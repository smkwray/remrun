from __future__ import annotations

import argparse
import json
import math
import os
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path

from . import __version__
from .config import (
    load_config, load_project_config, load_retention, offload_policy, offload_threshold,
    scheduler_config,
)
from .gitsync import HOOK_BEGIN
from .job_observation import (
    JobObservation, active_job_observation_enabled, controller_label,
)
from .memory_guard import MIB, MemoryAdmissionResult, MemoryReservation
from .models import RunPlan, WorkloadSpec
from .output import Reporter, emit_json_document
from .planner import make_run_plan
from .project import ProjectDetectionError, detect_project, find_project_config
from .profile import (
    LOCAL_DEVICE, WorkloadObservation, command_key, device_profile, load_job_costs,
    load_profiles, merge_job_costs, predict_job, profile_project_id,
    recommend_offload, update_profile, update_workload_profile,
)
from .protocol import (
    build_capabilities_document, build_error_document, format_capabilities_human,
)
from .scheduler import pick_by_load
from .reconcile import postrun_pullback, preflight_reconcile
from .resource_context import (
    MAX_RESOURCE_DOCUMENT_BYTES,
    ReceiptValidation,
    ResourceContextError,
    build_resource_envelope,
    build_run_context,
    envelope_meets_required_minimum,
    validate_workload_receipt_bytes,
    write_bounded_json,
)
from .resource_envelope import (
    MissingResourcePolicy,
    ResourcePolicy,
    ResourcePolicyError,
    parse_device_resource_policy,
)
from .resource_probe import probe_target_resources, unavailable_snapshot
from .runenv import resolve_run_env
from .scopes import configured_scope_names
from .state import (
    ProjectLock,
    LockError,
    RetentionPolicy,
    UnknownCompletionHazardError,
    cap_text,
    clear_unknown_completion_hazard,
    conflict_dir,
    default_state_root,
    new_run_id,
    prune_state,
    read_baseline,
    read_json,
    read_manifest,
    read_unknown_completion_hazard,
    run_dir,
    utc_now_iso,
    write_baseline,
    write_json,
    write_manifest,
    write_unknown_completion_hazard,
)
from .transport import (
    CommandNotStartedError, DurablePrestartError, DurableStateError, TelemetryRequest,
    TransportError, finalize_durable_result, make_transport,
)

KNOWN_COMMANDS = {
    "capabilities", "devices", "doctor", "plan", "run", "status", "logs", "clean", "bench", "sync",
    "git-sync", "runner", "action", "resolve-unknown", "resume",
}

# Exit codes (see docs/AGENT_OUTPUT_SPEC.md).
EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_CONFLICT = 2
EXIT_TRANSFER = 3
EXIT_INFRA = 4
EXIT_GUARD = 5
EXIT_CANCELLED = 130
RUN_CONTEXT_ENV = "REMRUN_RUN_CONTEXT"


@dataclass(frozen=True)
class _WorkloadRuntime:
    resources: dict
    remote_context_path: str | None
    remote_receipt_path: str | None
    context_staged: bool
    staging_error: str = ""


class _WorkloadAdmissionError(RuntimeError):
    """The selected workload cannot safely launch on the chosen target."""


class _PreflightConflict(Exception):
    """Preflight refused this candidate before mutating either side.

    Internal control flow only: it never escapes ``cmd_run``, which converts it to
    ``exit_code`` once no candidate is left. Carries the already-written conflict
    receipt so a skipped candidate stays diagnosable.
    """

    def __init__(
        self, *, exit_code: int, conflict_dir: Path, detail: str, retryable: bool,
    ) -> None:
        super().__init__(detail)
        self.exit_code = exit_code
        self.conflict_dir = conflict_dir
        self.detail = detail
        # Whether this conflict is candidate-LOCAL (try the next device) or a GLOBAL
        # condition like local-vanished, which must abort the whole run.
        self.retryable = retryable


class _LiveStdoutLog:
    """Best-effort streaming log whose file never exceeds its configured cap."""

    _MARKER = b"\n...[remrun truncated live log output]...\n"

    def __init__(self, path: Path, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        self.handle = None
        self.written = 0
        self.truncated = False
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            self.handle = path.open("w+b")
        except OSError:
            self.handle = None

    def append(self, chunk: str) -> None:
        if self.handle is None or self.truncated:
            return
        data = chunk.encode("utf-8", "replace")
        try:
            if self.max_bytes <= 0 or self.written + len(data) <= self.max_bytes:
                self.handle.write(data)
                self.handle.flush()
                self.written += len(data)
                return

            marker = self._MARKER
            if len(marker) > self.max_bytes:
                marker = b"...[remrun truncated]..."[:self.max_bytes]
                head_limit = 0
            else:
                head_limit = self.max_bytes - len(marker)
            self.handle.seek(0)
            head = self.handle.read(head_limit)
            if len(head) < head_limit:
                head += data[:head_limit - len(head)]
            head = head.decode("utf-8", "ignore").encode("utf-8")
            self.handle.seek(0)
            self.handle.write(head)
            self.handle.write(marker)
            self.handle.truncate()
            self.handle.flush()
            self.written = len(head) + len(marker)
            self.truncated = True
        except (OSError, ValueError):
            self.close()

    def close(self) -> None:
        if self.handle is None:
            return
        try:
            self.handle.close()
        except OSError:
            pass
        finally:
            self.handle = None


def split_command(argv: list[str]) -> tuple[list[str], list[str]]:
    """Split argv at the first standalone ``--`` separator.

    Everything before is remrun's own args (target + flags, order-independent);
    everything after is the verbatim command. This avoids argparse REMAINDER
    swallowing remrun flags that appear after the target.
    """
    if "--" in argv:
        i = argv.index("--")
        return argv[:i], argv[i + 1:]
    return argv, []


def _positive_mib(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer MiB value") from exc
    if result <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer MiB value")
    return result


def main(argv: list[str] | None = None) -> int:
    # On Windows the console defaults to a legacy codepage (e.g. cp1252).
    # Remote command output is decoded as UTF-8 and may contain characters that
    # codepage cannot encode, which would crash on write. Force UTF-8 on our
    # streams so captured remote output is emitted faithfully.
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass
    argv = list(sys.argv[1:] if argv is None else argv)

    # Fleet mode is a self-contained subcommand tree (plan/submit/run/status);
    # delegate before the bare-target alias rewrite so it isn't treated as a device.
    if argv and argv[0] == "fleet":
        from .fleet.cli import main as fleet_main
        return fleet_main(argv[1:])

    # Compatibility alias for future ergonomic use: remrun <device> -- cmd
    if argv and argv[0] not in KNOWN_COMMANDS and not argv[0].startswith("-"):
        argv = ["run", argv[0], *argv[1:]]

    head, cmd_tokens = split_command(argv)
    parser = build_parser()
    args = parser.parse_args(head)
    if args.command_name in {"run", "plan", "bench"}:
        args.cmd = cmd_tokens
    if args.command_name == "capabilities":
        try:
            return cmd_capabilities(args)
        except Exception as exc:
            emit_json_document(build_error_document(str(exc)), stream=sys.stderr)
            return EXIT_INTERNAL
    reporter = Reporter(json_events=getattr(args, "json", False),
                        quiet=getattr(args, "quiet", False))

    try:
        if args.command_name == "devices":
            return cmd_devices(args, reporter)
        if args.command_name == "doctor":
            return cmd_doctor(args, reporter)
        if args.command_name == "plan":
            return cmd_plan(args, reporter)
        if args.command_name == "run":
            return cmd_run(args, reporter)
        if args.command_name == "resume":
            return cmd_resume(args, reporter)
        if args.command_name == "status":
            return cmd_status(args, reporter)
        if args.command_name == "logs":
            return cmd_logs(args, reporter)
        if args.command_name == "clean":
            return cmd_clean(args, reporter)
        if args.command_name == "bench":
            return cmd_bench(args, reporter)
        if args.command_name == "sync":
            return cmd_sync(args, reporter)
        if args.command_name == "git-sync":
            return cmd_git_sync(args, reporter)
        if args.command_name == "runner":
            return cmd_runner(args, reporter)
        if args.command_name == "action":
            return cmd_action(args, reporter)
        if args.command_name == "resolve-unknown":
            return cmd_resolve_unknown(args, reporter)
        parser.print_help()
        return EXIT_INTERNAL
    except KeyboardInterrupt:
        reporter.event(
            "cancelled", message="interrupted by user", exit_code=EXIT_CANCELLED
        )
        return EXIT_CANCELLED
    except Exception as exc:  # Keep agent-visible error concise.
        reporter.event("error", type=type(exc).__name__, message=str(exc))
        return EXIT_INTERNAL


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="remrun")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command_name", required=True)

    p = sub.add_parser("capabilities", help="Show the public remrun protocol contract")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("devices", help="List configured devices")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("doctor", help="Check basic local configuration")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("plan", help="Plan a remote run without mutating files")
    p.add_argument("target", nargs="?", default="auto")
    p.add_argument("--auto", action="store_true", help="Pick the target device automatically")
    p.add_argument("--scope", help="declared [parallel.scopes.<name>] write scope for this run")
    p.add_argument("--workload", help="selected schema-1 project resource workload")
    p.add_argument("--probe", action="store_true",
                   help="probe each candidate for reachability and live CPU load "
                        "(costs a round-trip per device; off by default)")
    p.add_argument("--check-git", action="store_true",
                   help="also compare each candidate's project checkout against this one "
                        "(implies --probe; excluded paths are NOT reconciled, so a diverged "
                        "device can hold stale inputs)")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("run", help="Run a command remotely")
    p.add_argument("target", nargs="?", default="auto")
    p.add_argument("--json", action="store_true")
    p.add_argument("--auto", action="store_true", help="Pick the target device automatically")
    p.add_argument("--scope", help="declared [parallel.scopes.<name>] write scope for this run")
    p.add_argument("--workload", help="selected schema-1 project resource workload")
    p.add_argument("--dry-run", action="store_true", help="Print plan and do not execute")
    p.add_argument("--no-pullback", action="store_true", help="Skip post-run pullback")
    p.add_argument("--no-telemetry", action="store_true", help="Skip resource telemetry")
    p.add_argument(
        "--memory-limit-mib",
        type=_positive_mib,
        metavar="N",
        help="hard process-tree memory limit for this run; target safety limits still apply",
    )
    p.add_argument(
        "--durable", action="store_true",
        help="opt into a target-supervised run resumable by this controller; "
             "with --auto, selection freezes before launch",
    )

    p = sub.add_parser("resume", help="Resume an exact controller-owned durable run")
    p.add_argument("run_id", help="the exact durable run ID")
    p.add_argument("--no-wait", action="store_true", help="report a running state and return")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("status", help="Show recent remrun status")
    p.add_argument("device", nargs="?", help="show only runs targeting this device")
    p.add_argument("--json", action="store_true")
    p.add_argument("--limit", type=int, default=10)

    p = sub.add_parser("logs", help="Show run logs")
    p.add_argument("run", nargs="?", default="last")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("clean", help="Prune the run journal / state folder")
    p.add_argument("--older-than", metavar="DAYS",
                   help="Delete whole runs older than DAYS (e.g. 30 or 30d)")
    p.add_argument("--keep", type=int, metavar="N",
                   help="Keep only the most recent N runs; delete older")
    p.add_argument("--dry-run", action="store_true", help="Report what would be removed")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser(
        "resolve-unknown",
        help="Clear a controller-local unknown-completion hazard after confirming the run ended",
    )
    p.add_argument("run_id", help="the exact unknown run ID reported by remrun")
    p.add_argument(
        "--confirmed-ended",
        action="store_true",
        required=True,
        help="confirm a read-only check proved the remote command has ended",
    )
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("bench", help="Benchmark a command on multiple devices")
    p.add_argument("targets", nargs="?", default=None,
                   help="comma-separated devices; defaults to the configured scheduler order")
    p.add_argument("--no-local", action="store_true",
                   help="Skip the local baseline leg and assume offload (for jobs too "
                        "heavy to run on this machine); records only the remote round-trip")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("sync", help="Sync a folder with a device (pull-biased; "
                                    "local may be older than remote)")
    p.add_argument("folder", help="a configured [sync_roots] tree like 'outputs/audio', "
                                  "or a path under one")
    p.add_argument("device", help="configured device to sync with")
    p.add_argument("--remote", dest="remote",
                   help="explicit remote folder path (escape hatch for arbitrary folders)")
    p.add_argument("--pull", dest="direction", action="store_const", const="pull",
                   help="only pull remote→local (treat local as a read mirror)")
    p.add_argument("--push", dest="direction", action="store_const", const="push",
                   help="only push local→remote")
    p.add_argument("--both", dest="direction", action="store_const", const="both",
                   help="bidirectional, pull-biased (default)")
    p.add_argument("--exclude", action="append", default=[], metavar="PATTERN",
                   help="extra exclude pattern (repeatable)")
    p.add_argument("--dry-run", action="store_true", help="show the plan; change nothing")
    p.add_argument("--json", action="store_true")
    p.set_defaults(direction="both")

    p = sub.add_parser("action", help="Stage files and run an allowlisted target-side action")
    p.add_argument("device", help="configured target device")
    p.add_argument("action", help="action name configured under [devices.<name>.actions]")
    p.add_argument("--input", action="append", default=[], metavar="FILE",
                   help="file to place in the action inbox (repeatable)")
    p.add_argument("--key", help="idempotency key; required when there are no inputs")
    p.add_argument("--dry-run", action="store_true", help="validate and print the action plan")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("git-sync", help="Sync Git commits with a device without syncing .git/")
    p.add_argument("device", nargs="?", help="configured device to exchange git history with")
    p.add_argument("--pull", dest="direction", action="store_const", const="pull",
                   help="only pull peer commits into this repo")
    p.add_argument("--push", dest="direction", action="store_const", const="push",
                   help="only push this repo's commits to the peer")
    p.add_argument("--both", dest="direction", action="store_const", const="both",
                   help="pull then push (default)")
    p.add_argument(
        "--branch",
        help="transfer this branch plus tags and only fast-forward this branch",
    )
    p.add_argument("--bootstrap", action="store_true",
                   help="seed a repo-less project from the selected peer history (git init + "
                        "branch-plus-tags fetch with --branch, else full-history fetch); "
                        "the working tree is left untouched")
    p.add_argument("--dry-run", action="store_true",
                   help="verify both repos but do not fetch or fast-forward")
    p.add_argument("--status", action="store_true",
                   help="report branch state, dirty flags, hook, and diagnostics without mutating")
    p.add_argument(
        "--remote-memory-limit-mib",
        type=int,
        help=("hard per-command process-tree limit for repository-scaling Git work on "
              "a guarded target; overrides [git_sync].remote_memory_limit_mib"),
    )
    hook = p.add_mutually_exclusive_group()
    hook.add_argument("--install-hook", action="store_true",
                      help="install a non-blocking post-commit push hook")
    hook.add_argument("--uninstall-hook", action="store_true",
                      help="remove remrun's post-commit hook and restore any backup")
    p.add_argument("--quiet", action="store_true", help="suppress progress events")
    p.add_argument("--json", action="store_true")
    p.set_defaults(direction="both")

    p = sub.add_parser("runner", help="Install or probe the inert versioned remote runner")
    runner_sub = p.add_subparsers(dest="runner_command", required=True)
    for action in ("install", "probe"):
        rp = runner_sub.add_parser(action)
        rp.add_argument("device")
        rp.add_argument("--json", action="store_true")

    return parser


def cmd_capabilities(args: argparse.Namespace) -> int:
    document = build_capabilities_document()
    if args.json:
        emit_json_document(document)
    else:
        sys.stdout.write(format_capabilities_human(document))
        sys.stdout.flush()
    return EXIT_OK


def cmd_action(args: argparse.Namespace, reporter: Reporter) -> int:
    from .action import run_action

    result = run_action(load_config(), args.device, args.action, args.input,
                        key=args.key, dry_run=args.dry_run)
    reporter.event("action_result", **result.as_dict())
    return result.exit_code


def _import_legacy_job_costs(config) -> bool:
    """Honor the retired writer knob as read-only import for one compatibility release."""
    return config.defaults.get("profile", {}).get("store_in_project", False) is True


def normalize_cmd(cmd: list[str]) -> list[str]:
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        raise ValueError("missing command after --")
    return cmd


def cmd_devices(args: argparse.Namespace, reporter: Reporter) -> int:
    config = load_config()
    if args.json:
        print(json.dumps({"devices": {k: v.__dict__ for k, v in config.devices.items()}}, indent=2))
    else:
        for name, device in sorted(config.devices.items()):
            enabled = "enabled" if device.enabled else "disabled"
            print(f"{name}\t{enabled}\t{device.kind}\t{device.role}\t{','.join(device.tags)}")
    return EXIT_OK


def _offload_directive(host: str, policy: str, thr: dict) -> str:
    """One-line, agent-actionable summary of this host's offload posture."""
    bar = f"jobs over ~{thr['ram_gb']:g} GB RAM or ~{thr['wall_sec']}s (or that peg all cores)"
    if policy == "auto":
        msg = f"on {host}: auto-offload {bar} via `remrun run --auto` without asking; quick commands stay local"
    elif policy == "never":
        msg = f"on {host}: always run local (never offload)"
    else:
        msg = f"on {host}: run local unless the user asks; offload candidates are {bar}"
    note = thr.get("note")
    return f"{msg}. {note}" if note else msg


def _syncthing_candidates() -> list[str]:
    candidates: list[str] = []
    env_bin = os.environ.get("SYNCTHING_BIN")
    if env_bin:
        candidates.append(env_bin)
    found = shutil.which("syncthing")
    if found:
        candidates.append(found)
    if os.name == "nt":
        appdata = os.environ.get("APPDATA", "")
        localappdata = os.environ.get("LOCALAPPDATA", "")
        program_files = os.environ.get("ProgramFiles", "")
        candidates.extend([
            str(Path(appdata) / "SyncTrayzor" / "syncthing.exe") if appdata else "",
            str(Path(localappdata) / "Programs" / "Syncthing" / "syncthing.exe")
            if localappdata else "",
            str(Path(localappdata) / "Syncthing" / "syncthing.exe") if localappdata else "",
            str(Path(program_files) / "Syncthing" / "syncthing.exe") if program_files else "",
            r"C:\ProgramData\chocolatey\bin\syncthing.exe",
        ])
    seen: set[str] = set()
    out: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            out.append(candidate)
    return out


def _syncthing_info() -> dict[str, object]:
    for candidate in _syncthing_candidates():
        path = shutil.which(candidate) or candidate
        if not Path(path).exists():
            continue
        version = ""
        try:
            proc = subprocess.run([path, "--version"], check=False,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  text=True, timeout=5)
            version = (proc.stdout or "").strip().splitlines()[0] if proc.stdout else ""
        except (OSError, subprocess.SubprocessError):
            version = ""
        return {"available": True, "path": path, "version": version}
    return {"available": False, "path": None, "version": ""}


def _git_hook_path(repo: Path) -> Path | None:
    try:
        proc = subprocess.run(["git", "-C", str(repo), "rev-parse", "--git-path",
                               "hooks/post-commit"], check=False,
                              stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                              text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    raw = proc.stdout.strip()
    if not raw:
        return None
    path = Path(raw)
    return path if path.is_absolute() else repo / path


def _fleet_state_summary(state_root: Path) -> dict[str, object]:
    from .fleet.queue import _wal_reset_safe

    db = state_root / "fleet" / "fleet.db"
    summary: dict[str, object] = {
        "db": str(db),
        "exists": db.exists(),
        "sqlite_version": sqlite3.sqlite_version,
        "sqlite_wal_reset_safe": _wal_reset_safe(sqlite3.sqlite_version_info),
        "journal_mode": None,
    }
    if not db.exists():
        return summary
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        summary["error"] = str(exc)
        return summary
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()
        summary["journal_mode"] = str(mode[0]).lower() if mode else None
        tables = {
            r["name"] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "jobs" in tables:
            rows = conn.execute("SELECT state, COUNT(*) c FROM jobs GROUP BY state").fetchall()
            summary["counts"] = {r["state"]: r["c"] for r in rows}
            active = conn.execute(
                "SELECT assigned_device d, COUNT(*) c FROM jobs "
                "WHERE state IN ('leased','staging','running','fetching') "
                "AND assigned_device IS NOT NULL GROUP BY assigned_device"
            ).fetchall()
            summary["active_by_device"] = {r["d"]: r["c"] for r in active}
        if "resource_leases" in tables:
            summary["resource_leases"] = conn.execute(
                "SELECT COUNT(*) c FROM resource_leases"
            ).fetchone()["c"]
        if "cooldowns" in tables:
            summary["active_cooldowns"] = conn.execute(
                "SELECT COUNT(*) c FROM cooldowns WHERE until > ?", (utc_now_iso(),)
            ).fetchone()["c"]
    except sqlite3.Error as exc:
        summary["error"] = str(exc)
    finally:
        conn.close()
    return summary


def cmd_doctor(args: argparse.Namespace, reporter: Reporter) -> int:
    config = load_config()
    state_root = default_state_root()
    reporter.event("config_root", path=str(config.repo_root))
    reporter.event("devices_loaded", count=len(config.devices), names=sorted(config.devices))
    reporter.event("project_roots", roots=config.project_roots)
    reporter.event("coordination", mode=str(config.coordination.get("mode", "legacy")),
                   device=config.coordination.get("device"),
                   protocol=config.coordination.get("protocol"))
    reporter.event("state_root", path=str(state_root))
    host = socket.gethostname()
    policy = offload_policy(config)
    thr = offload_threshold(config)
    reporter.event("offload_policy", host=host, policy=policy)
    reporter.event("offload_threshold", host=host, ram_gb=thr["ram_gb"],
                   wall_sec=thr["wall_sec"], note=thr["note"])
    reporter.event("offload_directive", text=_offload_directive(host, policy, thr))
    reporter.event("syncthing", **_syncthing_info())
    reporter.event("fleet_state", **_fleet_state_summary(state_root))
    try:
        project = detect_project(Path.cwd(), config)
        project_config = load_project_config(find_project_config(project.local_project_root))
    except ProjectDetectionError as exc:
        reporter.event("project_context", available=False, reason=str(exc))
    else:
        scopes = configured_scope_names(project_config)
        reporter.event("project_write_scopes", project_id=project.project_id,
                       count=len(scopes), scopes=scopes,
                       default_lock="project")
        git_hook = _git_hook_path(project.local_project_root)
        installed = False
        if git_hook and git_hook.exists():
            text = git_hook.read_text(encoding="utf-8", errors="replace")
            installed = HOOK_BEGIN in text
        peers = (project_config.get("git_sync", {}) or {}).get(
            "peers", (config.git_sync or {}).get("peers", []))
        reporter.event("git_sync_hook", project_id=project.project_id,
                       installed=installed,
                       path=str(git_hook) if git_hook else None,
                       peers=peers if isinstance(peers, list) else [peers])
    return EXIT_OK


def cmd_runner(args: argparse.Namespace, reporter: Reporter) -> int:
    from .runner_client import RunnerClientError, ensure_versioned_runner

    config = load_config()
    try:
        info = ensure_versioned_runner(
            config, args.device, install=args.runner_command == "install")
    except (RunnerClientError, TransportError) as exc:
        reporter.event("runner_error", device=args.device, message=str(exc), exit_code=EXIT_INFRA)
        return EXIT_INFRA
    reporter.event(
        "runner_ready",
        device=info.device,
        installed_path=info.installed_path,
        source_sha256=info.source_sha256,
        reused=info.reused,
        schema_version=info.probe.get("schema_version"),
        device_id=info.probe.get("device_id"),
        filesystem=info.probe.get("filesystem"),
        sqlite_version=info.probe.get("sqlite_version"),
    )
    if args.json:
        print(json.dumps(info.as_dict(), indent=2, sort_keys=True))
    return EXIT_OK


def _candidate_probe_entry(device, sched: dict) -> dict:
    eff_w = float(sched.get("eff_core_weight", 1.0))
    capacity = device.cpu_capacity(eff_w)
    return {
        "name": device.name,
        "perf_cores": device.perf_cores,
        "eff_cores": device.eff_cores,
        "ram_gb": device.ram_gb,
        "capacity_perf_core_equiv": round(capacity, 2) or None,
        "reachable": None, "cpu_busy_pct": None, "spare_perf_core_equiv": None,
        "max_jobs": device.max_jobs,
        "recommended": False,
    }


def _job_prediction(config, plan: RunPlan) -> dict | None:
    """Read the exact learned inputs used by ordinary ``run --auto`` placement."""
    profiles = load_profiles(default_state_root())
    profile_id = profile_project_id(plan.project.project_id)
    profile_key = command_key(plan.command)
    if _import_legacy_job_costs(config):
        profiles = merge_job_costs(
            profiles, profile_id, load_job_costs(plan.project.local_project_root))
    return predict_job(profiles, profile_id, profile_key)


def _candidate_git_state(transport, device, plan: RunPlan) -> dict:
    """Whether this device's checkout of the project agrees with the controller's.

    Motivating hazard: remrun excludes `.git/**`, so it reconciles WORKING FILES and
    never history. That is normally the right call — but it only guarantees agreement
    for paths inside the transfer surface. A device whose checkout is behind can hold
    stale copies of EXCLUDED paths (bulk `data/**`, say) that no reconcile will fix and
    nothing currently warns about. Surfacing the head comparison lets an orchestrator
    refuse to route work to a diverged device.

    Advisory only: any failure returns `status: "unknown"` — never a bare False that
    would read as "in sync". Not a substitute for `remrun git-sync <device> --status`,
    which does the real branch comparison; this is the cheap head check an orchestrator
    can afford per routing decision.
    """
    from .gitsync import _local_git, _remote_git
    rev = ["rev-parse", "--verify", "--quiet", "HEAD"]
    try:
        local_proc = _local_git(plan.project.local_project_root, rev)
        mine = local_proc.stdout.strip() if local_proc.returncode == 0 else ""
    except (OSError, subprocess.SubprocessError) as exc:
        return {"status": "unknown", "detail": f"local git failed: {exc}"}
    try:
        remote_root = transport.remote_project_path(plan.project)
        remote_proc = _remote_git(transport, remote_root, rev)
        theirs = remote_proc.stdout.strip() if remote_proc.exit_code == 0 else ""
    except (TransportError, OSError) as exc:
        return {"status": "unknown", "local": mine or None,
                "detail": f"remote git failed: {exc}"}
    if not mine or not theirs:
        return {"status": "unknown", "local": mine or None, "remote": theirs or None,
                "detail": "one or both checkouts have no resolvable HEAD "
                          "(not a git repo, or an unborn branch)"}
    if mine == theirs:
        return {"status": "same", "local": mine, "remote": theirs}
    return {"status": "diverged", "local": mine, "remote": theirs,
            "detail": "device checkout differs from this one. remrun excludes .git/** and "
                      "reconciles WORKING FILES, so tracked in-surface paths still converge "
                      "— but EXCLUDED paths (bulk data, artifacts) are never reconciled and "
                      "may be stale here. Run `remrun git-sync <device> --status`."}


def cmd_plan(args: argparse.Namespace, reporter: Reporter) -> int:
    config = load_config()
    command = normalize_cmd(args.cmd)
    target_name = "auto" if args.auto else args.target
    plan = make_run_plan(
        cwd=Path.cwd(),
        config=config,
        target_name=target_name,
        command=command,
        scope_name=args.scope,
        json_events=args.json,
        requested_workload=args.workload,
    )
    candidates = None
    target_reason = None
    if args.probe or args.check_git:
        candidates = []
        selection = _resolve_targets(
            plan, target_name, scheduler_config(config), reporter,
            _job_prediction(config, plan), diagnostics=candidates,
            check_git=args.check_git, emit_events=False)
        if selection:
            plan = replace(plan, target=selection[0][0])
            target_reason = selection[0][3]
        else:
            target_reason = "unreachable"
    if args.json:
        payload = plan.as_dict()
        if candidates is not None:
            payload["target_reason"] = target_reason
            payload["candidates_probed"] = candidates
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        reporter.event("project", project_id=plan.project.project_id, relative_cwd=plan.project.relative_cwd)
        if target_reason is None:
            reporter.event("target", name=plan.target.name, kind=plan.target.kind)
        else:
            reporter.event("target", name=plan.target.name, kind=plan.target.kind,
                           reason=target_reason)
        reporter.event("command", argv=plan.command)
        reporter.event("transfer_mode", mode=plan.transfer_mode)
        reporter.event("write_scope", name=plan.write_scope or "project",
                       paths=plan.write_scope_paths)
        if plan.workload is not None:
            reporter.event("workload", **plan.workload.as_dict())
        reporter.event("active_surface", excludes=len(plan.excludes), hash_below_bytes=plan.hash_below_bytes)
        for entry in (candidates or []):
            reporter.event("candidate_probe", **entry)
        reporter.event("project_config", path=str(plan.project_config_path) if plan.project_config_path else None)
        rec = recommend_offload(
            load_profiles(default_state_root()),
            profile_project_id(plan.project.project_id),
            command_key(plan.command), devices=[d.name for d in plan.candidates],
            bias=float(scheduler_config(config).get("offload_bias", 1.25)))
        if rec.get("recommend") != "unknown":
            # Measured reality wins: a learned local-vs-trip comparison exists.
            reporter.event("offload_recommendation", **rec)
        else:
            # No bench data yet → fall back to this host's static offload posture
            # so `plan <cmd>` is still a self-sufficient "should I offload?" view.
            # A project `[run] offload` overrides the per-host policy. Run
            # `remrun bench <cmd>` to replace this with a measured recommendation.
            host = socket.gethostname()
            policy = offload_policy(config, project_config=plan.project_config)
            thr = offload_threshold(config)
            reporter.event("offload_policy", host=host, policy=policy,
                           basis="no-measurement",
                           hint="run `remrun bench` to measure local-vs-remote",
                           text=_offload_directive(host, policy, thr))
    return EXIT_OK


def _resolve_targets(plan: RunPlan, target_name: str | None, sched: dict, reporter: Reporter,
                     prediction: dict | None = None, *, diagnostics: list[dict] | None = None,
                     check_git: bool = False, emit_events: bool = True):
    """Probe and rank once for both ``run`` and opt-in, read-only ``plan`` diagnostics.

    Returns ``[(device, transport, probe, reason), ...]``. Diagnostics only record
    observations from this path; they never choose or mutate a target.
    """
    entries: dict[str, dict] = {}
    if diagnostics is not None:
        for device in plan.candidates:
            entry = _candidate_probe_entry(device, sched)
            entries[device.name] = entry
            diagnostics.append(entry)

    def event(event_name: str, **fields) -> None:
        if emit_events:
            reporter.event(event_name, **fields)

    def record(device, transport, probe, busy) -> None:
        entry = entries.get(device.name)
        if entry is None:
            return
        entry["reachable"] = bool(probe.reachable)
        entry["cpu_busy_pct"] = busy
        cap = device.cpu_capacity(float(sched.get("eff_core_weight", 1.0)))
        if busy is not None and cap:
            entry["spare_perf_core_equiv"] = round(
                cap * (1.0 - min(max(busy, 0.0), 100.0) / 100.0), 2)
        if check_git and probe.reachable:
            entry["git"] = _candidate_git_state(transport, device, plan)

    def probe_failure(device, exc) -> bool:
        entry = entries.get(device.name)
        if entry is None:
            return False
        entry["reachable"] = False
        entry["detail"] = str(exc)
        return True

    if target_name and target_name != "auto":
        device = plan.target
        try:
            transport = make_transport(device)
            probe = transport.probe()
        except (TransportError, OSError) as exc:
            if not probe_failure(device, exc):
                raise
            return []
        if not probe.reachable:
            record(device, transport, probe, None)
            entries.get(device.name, {})["detail"] = probe.detail
            event("unreachable", target=device.name, detail=probe.detail)
            return []
        busy = None
        if diagnostics is not None:
            try:
                busy = transport.sample_load()
            except (TransportError, OSError) as exc:
                entries[device.name]["detail"] = f"load probe failed: {exc}"
        record(device, transport, probe, busy)
        if device.name in entries:
            entries[device.name]["recommended"] = True
        return [(device, transport, probe, "explicit")]

    prefer_reachable = bool(sched.get("prefer_reachable_primary", True))
    load_balance = bool(sched.get("load_balance", True))
    candidates = plan.candidates if prefer_reachable else plan.candidates[:1]

    # Job-cost-aware adjustments (advisory; only when a learned profile exists).
    pred_rss = (prediction or {}).get("rss_mb")
    pred_dur = (prediction or {}).get("dur_s")
    if pred_rss:
        frac = float(sched.get("ram_headroom_pct", 80)) / 100.0
        fits = [d for d in candidates
                if d.ram_gb <= 0 or pred_rss <= d.ram_gb * 1024.0 * frac]
        if fits and len(fits) < len(candidates):
            for d in candidates:
                if d not in fits:
                    detail = f"predicted ~{pred_rss}MB > {frac:.0%} of {d.ram_gb}GB"
                    event("candidate_skipped", name=d.name, reason="insufficient_ram",
                          detail=detail)
                    if d.name in entries:
                        entries[d.name]["detail"] = detail
            candidates = fits
        elif not fits:
            event("ram_warning",
                  detail=f"predicted ~{pred_rss}MB exceeds all candidates; using largest-RAM")
            candidates = sorted(candidates, key=lambda d: d.ram_gb, reverse=True)
    if pred_dur is not None and pred_dur < float(sched.get("trivial_job_seconds", 30)):
        if load_balance:
            event("trivial_job", predicted_dur_s=pred_dur,
                  note="skipping load probe/reallocation")
        load_balance = False

    reachable: list = []  # (device, transport, probe, busy used by scheduler)
    for device in candidates:
        try:
            transport = make_transport(device)
            probe = transport.probe()
        except (TransportError, OSError) as exc:
            if not probe_failure(device, exc):
                raise
            continue
        if not probe.reachable:
            record(device, transport, probe, None)
            entries.get(device.name, {})["detail"] = probe.detail
            event("candidate_skipped", name=device.name, reason="unreachable",
                  detail=probe.detail)
            continue
        busy = None
        if load_balance or diagnostics is not None:
            try:
                observed_busy = transport.sample_load()
            except (TransportError, OSError) as exc:
                if diagnostics is None:
                    raise
                entries[device.name]["detail"] = f"load probe failed: {exc}"
                observed_busy = None
            record(device, transport, probe, observed_busy)
            if load_balance:
                busy = observed_busy
        else:
            record(device, transport, probe, None)
        event("candidate", name=device.name, cpu_busy_pct=busy,
              perf_cores=device.perf_cores, eff_cores=device.eff_cores)
        reachable.append((device, transport, probe, busy))

    if not reachable:
        first = plan.candidates[0].name if plan.candidates else "?"
        event("unreachable", target=first, detail="no candidate reachable")
        return []

    ranked = [(d, busy) for (d, _t, _pr, busy) in reachable]
    chosen_device, balance_reason = pick_by_load(ranked, sched)
    device, transport, probe, _busy = next(x for x in reachable if x[0] is chosen_device)
    reason = balance_reason
    if reason == "auto" and plan.candidates and device is not plan.candidates[0]:
        reason = "auto-failover"  # preferred candidate was unreachable
    if device.name in entries:
        entries[device.name]["recommended"] = True
    # pick_by_load names the winner; the rest stay in preference order behind it as
    # fallbacks. Their reason is "auto-failover": reaching them means the winner
    # failed for a candidate-local reason.
    rest = [(d, t, pr, "auto-failover") for (d, t, pr, _b) in reachable if d is not device]
    return [(device, transport, probe, reason), *rest]


def _unknown_completion_guidance(hazard: dict[str, object]) -> str:
    run_id = str(hazard["run_id"])
    return (
        f"project is blocked by controller-local unknown-completion hazard for run {run_id}; "
        "do not retry this project; use a read-only process/artifact probe to prove the prior "
        "command ended, then run "
        f"`remrun resolve-unknown {run_id} --confirmed-ended`. This legacy-mode hazard does "
        "not fence another controller that uses a different state root"
    )


def _report_unknown_completion_hazard(
    project_id: str,
    reporter: Reporter,
) -> dict[str, object] | None:
    hazard = read_unknown_completion_hazard(project_id)
    if hazard is None:
        return None
    reporter.event(
        "unknown_completion_hazard",
        project_id=project_id,
        target=hazard["target"],
        run_id=hazard["run_id"],
        created_at=hazard["created_at"],
        guidance=_unknown_completion_guidance(hazard),
    )
    return hazard


def _stage_workload_context(
    *,
    plan: RunPlan,
    transport,
    policy: ResourcePolicy,
    run_id: str,
    rdir: Path,
    reporter: Reporter,
) -> _WorkloadRuntime:
    """Capture and stage one selected workload's non-project launch context."""
    workload = plan.workload
    if workload is None:  # pragma: no cover - caller guards the inactive path
        raise AssertionError("workload context requested without a selected workload")

    if isinstance(policy, MissingResourcePolicy):
        snapshot = unavailable_snapshot(
            "unavailable",
            "device resource_policy is missing; live probe was not attempted",
        )
    else:
        snapshot = probe_target_resources(
            transport,
            plan.target,
            timeout_sec=policy.probe_timeout_sec,
        )
    captured_at = utc_now_iso()
    resources = build_resource_envelope(
        snapshot=snapshot,
        policy=policy,
        device=plan.target,
        captured_at=captured_at,
    )
    reporter.event(
        "resource_envelope",
        workload=workload.name,
        status=resources["status"],
        probe_status=resources["probe_status"],
        cpu_status=resources["offered"]["cpu"]["status"],
        ram_status=resources["offered"]["ram"]["status"],
    )

    if workload.require_envelope and not envelope_meets_required_minimum(resources):
        raise _WorkloadAdmissionError(
            "selected workload requires a usable resource envelope, but the chosen "
            f"target reported {resources['status']!r}"
        )
    remote_context_path: str | None = None
    remote_receipt_path: str | None = None
    try:
        if not plan.target.state_root.strip():
            raise ResourceContextError(
                f"device {plan.target.name} has no state_root for workload context"
            )
        state_root = transport.expand_remote(plan.target.state_root)
        if not state_root:
            raise ResourceContextError(
                f"device {plan.target.name} state_root resolved to an empty path"
            )
        remote_run_dir = transport.native_join(state_root, "runs", run_id)
        remote_context_path = transport.native_join(
            remote_run_dir, "run-context.v1.json"
        )
        remote_receipt_path = transport.native_join(
            remote_run_dir, "workload-receipt.v1.json"
        )
        context = build_run_context(
            run_id=run_id,
            created_at=utc_now_iso(),
            workload=workload,
            receipt_path=remote_receipt_path,
            resources=resources,
        )
        # Keep the controller copy under a distinct name: LocalSim may map target
        # state to this same run directory, and source==destination is not atomic.
        local_context = rdir / "run-context.controller.v1.json"
        write_bounded_json(local_context, context)
        transport.ensure_remote_dir(remote_run_dir)
        transport.push_file(local_context, remote_context_path)
    except (OSError, ResourceContextError, TransportError, ValueError) as exc:
        detail = f"{type(exc).__name__}: {exc}"
        reporter.event(
            "workload_context",
            workload=workload.name,
            staged=False,
            detail=detail,
        )
        if workload.require_envelope or workload.require_receipt:
            raise _WorkloadAdmissionError(
                "selected workload requires a staged run context: " + detail
            ) from exc
        return _WorkloadRuntime(
            resources=resources,
            remote_context_path=remote_context_path,
            remote_receipt_path=remote_receipt_path,
            context_staged=False,
            staging_error=detail,
        )

    reporter.event(
        "workload_context",
        workload=workload.name,
        staged=True,
        path=remote_context_path,
    )
    return _WorkloadRuntime(
        resources=resources,
        remote_context_path=remote_context_path,
        remote_receipt_path=remote_receipt_path,
        context_staged=True,
    )


def _collect_workload_receipt(
    *,
    plan: RunPlan,
    transport,
    runtime: _WorkloadRuntime,
    run_id: str,
    rdir: Path,
    reporter: Reporter,
) -> ReceiptValidation | None:
    """Collect and validate bounded adapter evidence after pullback."""
    workload = plan.workload
    if workload is None or not runtime.context_staged:
        return None
    receipt_path = runtime.remote_receipt_path
    if receipt_path is None:  # pragma: no cover - staged runtime invariant
        return ReceiptValidation("missing", detail="receipt path was not staged")
    try:
        raw = transport.read_small_file(
            receipt_path,
            MAX_RESOURCE_DOCUMENT_BYTES,
        )
    except TransportError as exc:
        detail = str(exc)
        status = "missing" if "small file missing:" in detail else "malformed"
        validation = ReceiptValidation(status, detail=detail)
    else:
        # Distinct from the target filename for LocalSim, whose target state root
        # may intentionally be the controller state root in production-path tests.
        local_receipt = rdir / "workload-receipt.controller.v1.json"
        try:
            local_receipt.write_bytes(raw)
        except OSError as exc:
            # Validation is over the transferred bytes; a controller-journal write
            # failure is evidence loss, so fail the receipt rather than claiming it.
            validation = ReceiptValidation(
                "malformed",
                detail=f"controller receipt write failed: {type(exc).__name__}: {exc}",
            )
        else:
            validation = validate_workload_receipt_bytes(
                raw,
                run_id=run_id,
                workload=workload,
            )
    reporter.event(
        "workload_receipt",
        workload=workload.name,
        status=validation.status,
        detail=validation.detail or None,
    )
    return validation


def _cleanup_workload_files(
    *,
    transport,
    runtime: _WorkloadRuntime,
    reporter: Reporter,
) -> dict[str, str]:
    """Best-effort exact-file cleanup; never remove an entire target run tree."""
    cleanup: dict[str, str] = {}
    for name, path in (
        ("context", runtime.remote_context_path),
        ("receipt", runtime.remote_receipt_path),
    ):
        if path is None:
            cleanup[name] = "not_staged"
            continue
        try:
            transport.delete_remote(path)
        except (OSError, TransportError) as exc:
            cleanup[name] = f"failed: {type(exc).__name__}: {exc}"
        else:
            cleanup[name] = "deleted"
    reporter.event("workload_cleanup", **cleanup)
    return cleanup


def _workload_observation_from_run(
    *,
    project_id: str,
    command: list[str],
    device: str,
    workload: WorkloadSpec,
    receipt: ReceiptValidation | None,
    telemetry: dict | None,
    trip_s: float,
    updated: str,
) -> tuple[WorkloadObservation | None, str]:
    """Build one setting-specific observation from already-admitted run evidence.

    Command/pullback admission is intentionally owned by the caller.  This helper
    validates only the generic receipt, work count, and normalized telemetry
    semantics.  Unknown metrics are never converted to plausible zeroes.
    """
    if receipt is None or not receipt.valid or not isinstance(receipt.data, dict):
        return None, "receipt is missing or invalid"
    data = receipt.data
    if data.get("status") == "blocked":
        return None, "adapter reported blocked"
    if not math.isfinite(trip_s) or trip_s <= 0:
        return None, "trip duration is not positive"

    work = data.get("work")
    count = work.get("count") if isinstance(work, dict) else None
    if (
        isinstance(count, bool)
        or not isinstance(count, (int, float))
        or not math.isfinite(count)
        or count <= 0
    ):
        return None, "receipt work count is not positive"

    if (
        not isinstance(telemetry, dict)
        or telemetry.get("schema") != 1
        or isinstance(telemetry.get("schema"), bool)
    ):
        return None, "detailed telemetry is unavailable"
    command_wall_s = telemetry.get("wall_sec")
    if (
        isinstance(command_wall_s, bool)
        or not isinstance(command_wall_s, (int, float))
        or not math.isfinite(command_wall_s)
        or command_wall_s <= 0
    ):
        return None, "target-measured command duration is not positive"
    if telemetry.get("process_tree_drained") is not True:
        return None, "process tree was not proven drained"
    memory = telemetry.get("memory")
    cpu = telemetry.get("cpu")
    gpu = telemetry.get("gpu")
    if not all(isinstance(section, dict) for section in (memory, cpu, gpu)):
        return None, "detailed telemetry sections are malformed"

    memory_metric = memory.get("metric")
    memory_coverage = memory.get("coverage")
    memory_peak = memory.get("peak_bytes")
    if (
        memory_metric not in {"rss_sum_sampled", "job_memory_peak"}
        or not isinstance(memory_coverage, str)
        or memory_coverage
        not in {"short_lived_sampled", "known_tree_drained", "job_object_drained"}
        or isinstance(memory_peak, bool)
        or not isinstance(memory_peak, (int, float))
        or not math.isfinite(memory_peak)
        or memory_peak < 0
    ):
        return None, "memory telemetry is not setting-comparable"

    cpu_coverage = cpu.get("coverage")
    cpu_sec = cpu.get("cpu_sec")
    avg_cpu_pct = cpu.get("avg_cpu_pct")
    if (
        not isinstance(cpu_coverage, str)
        or cpu_coverage
        not in {
            "wait4_known_tree_drained_detached_possible",
            "job_object_drained",
        }
        or any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
            for value in (cpu_sec, avg_cpu_pct)
        )
    ):
        return None, "CPU telemetry is not setting-comparable"

    gpu_scope = gpu.get("scope")
    gpu_status = gpu.get("status")
    if (
        gpu_scope != "whole_device"
        or gpu_status not in {"measured", "partial", "unavailable", "not_applicable"}
    ):
        return None, "GPU telemetry labels are malformed"
    for key in (
        "max_util_pct",
        "min_vram_free_bytes",
        "unified_memory_min_available_bytes",
    ):
        value = gpu.get(key)
        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            return None, f"GPU telemetry {key} is malformed"
    if (
        isinstance(gpu.get("max_util_pct"), (int, float))
        and not isinstance(gpu.get("max_util_pct"), bool)
        and gpu["max_util_pct"] > 100
    ):
        return None, "GPU telemetry max_util_pct is malformed"
    if gpu_status in {"unavailable", "not_applicable"} and any(
        gpu.get(key) is not None
        for key in ("max_util_pct", "min_vram_free_bytes")
    ):
        return None, "unavailable GPU telemetry carries plausible values"

    return (
        WorkloadObservation(
            project_id=project_id,
            command_key=command_key(command),
            device=device,
            workload_name=workload.name,
            adapter_version=workload.adapter_version,
            setting_fingerprint=data["setting_fingerprint"],
            receipt_status=data["status"],
            work_unit=workload.work_unit,
            evaluation=data["evaluation"],
            updated=updated,
            setting=data["setting"],
            constraints=data["constraints"],
            exec_s=float(command_wall_s),
            trip_s=trip_s,
            throughput=float(count) / float(command_wall_s),
            memory={
                "peak_bytes": memory_peak,
                "metric": memory_metric,
                "coverage": memory_coverage,
            },
            cpu={
                "cpu_sec": cpu_sec,
                "avg_cpu_pct": avg_cpu_pct,
                "coverage": cpu_coverage,
            },
            gpu={
                "scope": gpu_scope,
                "max_util_pct": gpu.get("max_util_pct"),
                "min_vram_free_bytes": gpu.get("min_vram_free_bytes"),
                "unified_memory_min_available_bytes": gpu.get(
                    "unified_memory_min_available_bytes"
                ),
                "status": gpu_status,
            },
        ),
        "",
    )


def cmd_run(args: argparse.Namespace, reporter: Reporter) -> int:
    config = load_config()
    policy = load_retention(config)
    telemetry_default = bool(config.defaults.get("telemetry", {}).get("enabled", True))
    command = normalize_cmd(args.cmd)
    target_name = "auto" if args.auto else args.target
    plan = make_run_plan(
        cwd=Path.cwd(),
        config=config,
        target_name=target_name,
        command=command,
        scope_name=getattr(args, "scope", None),
        json_events=args.json,
        requested_workload=getattr(args, "workload", None),
        allow_default_workload=not getattr(args, "suppress_default_workload", False),
    )

    if getattr(args, "durable", False):
        if (
            target_name != "auto"
            and plan.target.kind not in {"ssh-posix", "ssh-powershell"}
        ):
            raise ValueError(
                "durable runs support only built-in ssh-posix and ssh-powershell transports"
            )
        if plan.workload is not None:
            raise ValueError(
                "durable ordinary-run v1 does not support workload envelopes/receipts"
            )

    if not args.dry_run and _report_unknown_completion_hazard(
        plan.project.project_id, reporter
    ):
        return EXIT_INTERNAL

    if args.dry_run:
        run_id = new_run_id(plan.target.name, plan.project.project_id)
        reporter.event("run_id", run_id=run_id)
        reporter.event("project", project_id=plan.project.project_id, relative_cwd=plan.project.relative_cwd)
        reporter.event("target", name=plan.target.name, kind=plan.target.kind,
                       reason="explicit" if target_name != "auto" else "auto",
                       candidates=[d.name for d in plan.candidates])
        reporter.event("write_scope", name=plan.write_scope or "project",
                       paths=plan.write_scope_paths)
        reporter.event("dry_run", action="not executing")
        write_json(run_dir(run_id) / "summary.json",
                   {"run_id": run_id, "dry_run": True, "plan": plan.as_dict()})
        return EXIT_OK

    sched = scheduler_config(config)
    # When explicitly enabled, placement overlays the project's PORTABLE job costs so
    # --auto can use prior measurements from another controller.
    profile_id = profile_project_id(plan.project.project_id)
    profile_key = command_key(plan.command)
    prediction = _job_prediction(config, plan)
    memory_limit_mib = getattr(args, "memory_limit_mib", None)
    if prediction:
        reporter.event(
            "job_profile",
            project_id=profile_id,
            key=profile_key,
            predicted_rss_mb=(
                None if memory_limit_mib is not None else prediction.get("rss_mb")
            ),
            predicted_dur_s=prediction.get("dur_s"),
            explicit_memory_limit_mib=memory_limit_mib,
        )
    placement_prediction = prediction
    if memory_limit_mib is not None and prediction is not None:
        placement_prediction = {
            key: value for key, value in prediction.items() if key != "rss_mb"
        }
    selection = _resolve_targets(
        plan, target_name, sched, reporter, placement_prediction
    )
    if getattr(args, "durable", False) and target_name == "auto":
        durable_selection = []
        for candidate in selection:
            if candidate[0].kind not in {"ssh-posix", "ssh-powershell"}:
                reporter.event(
                    "candidate_skipped",
                    name=candidate[0].name,
                    reason="durable_unsupported_transport",
                    detail="durable runs require a built-in SSH transport",
                )
                continue
            durable_selection.append(candidate)
        selection = durable_selection
    if not selection:
        run_id = new_run_id(plan.target.name, plan.project.project_id)
        error = (
            "no reachable durable-capable target"
            if getattr(args, "durable", False)
            else "target unreachable"
        )
        write_json(run_dir(run_id) / "summary.json",
                   {"run_id": run_id, "error": error, "plan": plan.as_dict()})
        return EXIT_INFRA

    # Walk the ranked candidates. A preflight conflict is a property of ONE candidate's
    # tree, not of the job, and it is raised before that candidate mutates either side — so
    # with --auto we may try the next reachable candidate. Fallback preflight is stricter:
    # it may push local bytes outward but never pull/delete locally while choosing a device.
    # Each attempt keeps its own run_id, so skipped conflict evidence is retained rather than
    # overwritten. Only this pre-mutation case fails over: a transport error may have
    # already transferred files, and the project lock is keyed by project (not device), so
    # another candidate would meet the same lock.
    base_plan = plan
    last_conflict_exit = EXIT_CONFLICT
    # Every attempted run_id, so retention can't prune a skipped candidate's receipt.
    attempted_run_ids: set[str] = set()
    for attempt, (chosen, transport, probe, reason) in enumerate(selection):
        plan = replace(base_plan, target=chosen)
        remaining = len(selection) - attempt - 1

        run_id = new_run_id(plan.target.name, plan.project.project_id)
        attempted_run_ids.add(run_id)
        rdir = run_dir(run_id)
        summary_path = rdir / "summary.json"
        started_at = utc_now_iso()

        reporter.event("run_id", run_id=run_id)
        reporter.event("project", project_id=plan.project.project_id,
                       relative_cwd=plan.project.relative_cwd)
        reporter.event("target", name=plan.target.name, kind=plan.target.kind, reason=reason)
        reporter.event("target_reachable", address=probe.address, remote_os=probe.remote_os)
        reporter.event("write_scope", name=plan.write_scope or "project",
                       paths=plan.write_scope_paths)
        if plan.workload is not None:
            reporter.event("workload", **plan.workload.as_dict())
        # Create the receipt before any target mutation or other failure-prone phase.
        # If the controller is killed, status still has a durable attempt row rather
        # than silently implying that no run existed.
        write_json(summary_path, {
            "run_id": run_id,
            "project_id": plan.project.project_id,
            "target": plan.target.name,
            "command": plan.command,
            "started_at": started_at,
            "phase": "preflight",
            "completion_state": "not_started",
            "command_started": False,
            "terminal": False,
            "plan": plan.as_dict(),
        })

        resource_policy: ResourcePolicy | None = None
        if plan.workload is not None:
            try:
                resource_policy = parse_device_resource_policy(plan.target.resource_policy)
            except ResourcePolicyError as exc:
                reporter.event(
                    "resource_policy_error",
                    target=plan.target.name,
                    message=str(exc),
                )
                write_json(
                    summary_path,
                    {
                        "run_id": run_id,
                        "error": str(exc),
                        "phase": "resource_policy",
                        "plan": plan.as_dict(),
                    },
                )
                return EXIT_INTERNAL

        reservation: MemoryReservation | None = None
        admission: MemoryAdmissionResult | None = None
        if memory_limit_mib is not None and getattr(transport, "memory_guard", None) is None:
            admission = MemoryAdmissionResult.refused(
                "guard_not_configured",
                "explicit memory limit requires a configured target memory guard",
            )
        elif getattr(transport, "memory_guard", None) is not None:
            if memory_limit_mib is not None:
                admission = transport.reserve_memory_guard(
                    explicit_limit_mib=memory_limit_mib
                )
            else:
                predicted_rss_mb = prediction.get("rss_mb") if prediction else None
                admission = transport.reserve_memory_guard(
                    predicted_rss_mb=predicted_rss_mb
                )
        if admission is not None:
            reporter.event(
                "memory_admission",
                target=plan.target.name,
                status=admission.status,
                reason=admission.reason,
                detail=admission.detail,
                phase="pre_mutation",
            )
            if not admission.admitted:
                write_json(
                    summary_path,
                    {
                        "run_id": run_id,
                        "error": "no safe target capacity",
                        "phase": "memory_admission",
                        "memory_admission": admission.payload,
                        "job_profile": {
                            "status": (
                                "explicit_limit"
                                if memory_limit_mib is not None
                                else "learned" if prediction else "unprofiled"
                            ),
                            "project_id": profile_id,
                            "key": profile_key,
                            "predicted_rss_mb": (
                                None
                                if memory_limit_mib is not None
                                else prediction.get("rss_mb") if prediction else None
                            ),
                            "explicit_memory_limit_mib": memory_limit_mib,
                        },
                        "plan": plan.as_dict(),
                    },
                )
                if remaining:
                    reporter.event(
                        "candidate_skipped",
                        name=plan.target.name,
                        reason="memory_admission",
                        detail=admission.detail,
                    )
                    continue
                return EXIT_GUARD
            reservation = admission.reservation

        remote_root = transport.remote_project_path(plan.project)
        remote_cwd = transport.remote_join(remote_root, plan.project.relative_cwd)
        reporter.event("remote_cwd", path=remote_cwd)

        try:
            lock = ProjectLock(
                plan.project.project_id,
                plan.target.name,
                scope=plan.write_scope,
                run_id=run_id if getattr(args, "durable", False) else None,
            ).acquire()
        except LockError as exc:
            if reservation is not None:
                transport.release_memory_guard(reservation, reserved_only=True)
            reporter.event("locked", message=str(exc))
            write_json(summary_path, {"run_id": run_id, "error": str(exc), "plan": plan.as_dict()})
            return EXIT_INTERNAL

        t0 = time.monotonic()
        try:
            return _run_locked(args, reporter, plan, transport, remote_root, remote_cwd,
                               run_id, rdir, summary_path, started_at, t0, policy,
                               telemetry_default, attempt > 0, attempted_run_ids,
                               resource_policy, reservation)
        except _PreflightConflict as exc:
            last_conflict_exit = exc.exit_code
            # A non-retryable conflict (e.g. local-vanished) is global: stop the whole run.
            if not exc.retryable or not remaining:
                return exc.exit_code
            reporter.event("candidate_skipped", name=plan.target.name,
                           reason="preflight_conflict", detail=exc.detail,
                           conflict_state=str(exc.conflict_dir))
        except KeyboardInterrupt:
            checkpoint = read_json(summary_path) or {"run_id": run_id}
            checkpoint.update({
                "error": "cancelled by user",
                "terminal": True,
                "ended_at": utc_now_iso(),
            })
            completion_state = checkpoint.get("completion_state")
            if completion_state == "command_complete":
                # The command's checkpoint remains authoritative: cancellation while
                # finalizing must never turn a completed mutating command into a retry.
                checkpoint["completion_state"] = "finalization_failed"
            elif completion_state != "unknown":
                checkpoint["completion_state"] = "cancelled"
                checkpoint["command_started"] = False
            write_json(summary_path, checkpoint)
            raise
        except Exception as exc:
            # Normal Python finalization failures must still terminate the durable
            # receipt. BaseException deliberately escapes: an interrupted/killed
            # controller retains the last checkpoint and any unknown-completion fence.
            checkpoint = read_json(summary_path) or {"run_id": run_id}
            checkpoint.update({
                "error": f"{type(exc).__name__}: {exc}",
                "terminal": True,
                "ended_at": utc_now_iso(),
            })
            if checkpoint.get("completion_state") == "command_complete":
                checkpoint["completion_state"] = "finalization_failed"
            elif checkpoint.get("completion_state") != "unknown":
                checkpoint["completion_state"] = "failed_before_completion"
            write_json(summary_path, checkpoint)
            raise
        finally:
            lock.release()
            if reservation is not None:
                transport.release_memory_guard(reservation, reserved_only=True)
            # Prune even when a run errors out early (preflight/exec/pullback may already have
            # written backups), so repeated failures can't grow the state unbounded.
            try:
                # Keep every receipt from this command alive until candidate selection ends.
                prune_state(policy, exempt_run_ids=attempted_run_ids)
            except Exception:  # noqa: BLE001
                pass

    return last_conflict_exit


def _run_locked(
    args: argparse.Namespace,
    reporter: Reporter,
    plan: RunPlan,
    transport,
    remote_root: str,
    remote_cwd: str,
    run_id: str,
    rdir: Path,
    summary_path: Path,
    started_at: str,
    t0: float,
    policy: RetentionPolicy,
    telemetry_default: bool,
    is_fallback: bool,
    attempted_run_ids: set[str],
    resource_policy: ResourcePolicy | None,
    memory_reservation: MemoryReservation | None,
) -> int:
    local_root = plan.project.local_project_root
    prev_local, prev_remote = read_baseline(plan.target.name, plan.project.project_id)
    backup_root = conflict_dir(run_id) / "backup"

    # Validate a backend's declared argv boundary before reconciliation can mutate
    # either tree and before an UNKNOWN fence can be installed. PowerShell command
    # discovery depends on the resolved target PATH/environment, while direct
    # transport APIs still recheck the boundary at their own dispatch seam.
    runenv = resolve_run_env(
        device=plan.target, project=plan.project, project_config=plan.project_config
    )
    try:
        transport.validate_command_context(
            plan.command, env=runenv.env, path_prepend=runenv.path_prepend
        )
    except CommandNotStartedError as exc:
        reporter.event("command_rejected", phase="preflight", message=str(exc))
        write_json(summary_path, {
            "run_id": run_id,
            "project_id": plan.project.project_id,
            "target": plan.target.name,
            "command": plan.command,
            "started_at": started_at,
            "ended_at": utc_now_iso(),
            "error": str(exc),
            "phase": "preflight",
            "completion_state": "not_started",
            "command_started": False,
            "terminal": True,
            "plan": plan.as_dict(),
        })
        return EXIT_INFRA
    except TransportError as exc:
        reporter.event("transfer_error", phase="command_validation", message=str(exc))
        write_json(summary_path, {
            "run_id": run_id,
            "project_id": plan.project.project_id,
            "target": plan.target.name,
            "command": plan.command,
            "started_at": started_at,
            "ended_at": utc_now_iso(),
            "error": str(exc),
            "phase": "command_validation",
            "completion_state": "not_started",
            "command_started": False,
            "terminal": True,
            "plan": plan.as_dict(),
        })
        return EXIT_TRANSFER

    # --- preflight reconcile -------------------------------------------------
    def report_preflight_progress(
        completed: int, total: int, action_totals: dict[str, int]
    ) -> None:
        reporter.event(
            "preflight_progress",
            completed=completed,
            total=total,
            **action_totals,
        )

    try:
        pre = preflight_reconcile(
            transport=transport,
            local_root=local_root,
            remote_root=remote_root,
            excludes=plan.excludes,
            hash_below_bytes=plan.hash_below_bytes,
            prev_local=prev_local,
            prev_remote=prev_remote,
            backup_root=backup_root,
            backup_below_bytes=policy.backup_below_bytes,
            progress=report_preflight_progress,
            is_fallback=is_fallback,
        )
    except TransportError as exc:
        reporter.event("transfer_error", phase="preflight", message=str(exc))
        write_json(summary_path, {
            "run_id": run_id,
            "project_id": plan.project.project_id,
            "target": plan.target.name,
            "command": plan.command,
            "started_at": started_at,
            "ended_at": utc_now_iso(),
            "error": str(exc),
            "phase": "preflight",
            "completion_state": "not_started",
            "command_started": False,
            "terminal": True,
            "plan": plan.as_dict(),
        })
        return EXIT_TRANSFER

    if pre.has_conflicts:
        cdir = conflict_dir(run_id)
        write_json(cdir / "conflicts.json", {
            "run_id": run_id,
            "project_id": plan.project.project_id,
            "target": plan.target.name,
            "conflicts": [{"path": c.path, "state": c.state, "reason": c.reason} for c in pre.conflicts],
        })
        for c in pre.conflicts:
            reporter.event("conflict", path=c.path, state=c.state, reason=c.reason)
        reporter.event("action", value="not running command")
        reporter.event("conflict_state", path=str(cdir))
        write_json(summary_path, {
            "run_id": run_id, "exit_code": EXIT_CONFLICT, "conflicts": len(pre.conflicts),
            "conflict_state": str(cdir), "plan": plan.as_dict(),
        })
        # Signal rather than return so `cmd_run` can retry only conflicts positively
        # classified as candidate-local. Global failures (notably local-vanished) abort.
        # The receipt is exempted from pruning until the whole candidate loop exits.
        raise _PreflightConflict(
            exit_code=EXIT_CONFLICT,
            conflict_dir=cdir,
            detail=f"{len(pre.conflicts)} conflicting path(s); first: {pre.conflicts[0].path}",
            retryable=pre.conflicts_are_candidate_local,
        )

    write_manifest(rdir / "pre_local_manifest.json", pre.local_manifest)
    write_manifest(rdir / "pre_remote_manifest.json", pre.remote_manifest)
    reporter.event("preflight_summary", pulled=len(pre.pulled), pushed=len(pre.pushed),
                   deleted_remote=len(pre.deleted_remote), deleted_local=len(pre.deleted_local),
                   skipped_identical=len(pre.skipped_identical),
                   converged_conflicts=len(pre.converged_conflicts), conflicts=0)

    if getattr(args, "durable", False):
        reporter.event(
            "durable_target_bound",
            run_id=run_id,
            target=plan.target.name,
            note="selection is permanent; no candidate fallback after launch is attempted",
        )
        return _run_durable_locked(
            args=args, reporter=reporter, plan=plan, transport=transport,
            remote_root=remote_root, remote_cwd=remote_cwd, run_id=run_id,
            rdir=rdir, summary_path=summary_path, started_at=started_at, t0=t0,
            policy=policy, telemetry_default=telemetry_default, pre=pre,
            backup_root=backup_root, memory_reservation=memory_reservation,
        )

    # --- execute -------------------------------------------------------------
    if runenv.venv:
        reporter.event("venv", path=runenv.venv)
    exec_env = runenv.env
    workload_runtime: _WorkloadRuntime | None = None
    if plan.workload is not None:
        if resource_policy is None:  # pragma: no cover - cmd_run constructs it
            raise AssertionError("selected workload has no parsed resource policy")
        try:
            if RUN_CONTEXT_ENV in runenv.env:
                raise _WorkloadAdmissionError(
                    f"{RUN_CONTEXT_ENV} is reserved when a workload is selected"
                )
            workload_runtime = _stage_workload_context(
                plan=plan,
                transport=transport,
                policy=resource_policy,
                run_id=run_id,
                rdir=rdir,
                reporter=reporter,
            )
        except _WorkloadAdmissionError as exc:
            reporter.event("workload_admission_failed", message=str(exc))
            write_json(
                summary_path,
                {
                    "run_id": run_id,
                    "error": str(exc),
                    "phase": "workload_admission",
                    "workload": plan.workload.as_dict(),
                    "plan": plan.as_dict(),
                },
            )
            return EXIT_INTERNAL
        if workload_runtime.context_staged:
            exec_env = {**runenv.env, RUN_CONTEXT_ENV: workload_runtime.remote_context_path}
    if exec_env or runenv.path_prepend:
        reporter.event("run_env", vars=sorted(exec_env), path_prepend=len(runenv.path_prepend))
    telemetry_on = telemetry_default and not args.no_telemetry
    transport.ensure_remote_dir(remote_cwd)
    guarded_dispatch = (
        getattr(transport, "memory_guard", None) is not None
        or transport.command_start_requires_confirmation()
    )
    # Fence before dispatch, not after a transport exception. A controller can be
    # killed without Python regaining control; in that case the remote start/result
    # is unknowable and an automatic retry is unsafe.
    hazard = write_unknown_completion_hazard(
        plan.project.project_id,
        plan.target.name,
        run_id,
    )
    guidance = _unknown_completion_guidance(hazard)
    write_json(summary_path, {
        "run_id": run_id,
        "project_id": plan.project.project_id,
        "target": plan.target.name,
        "command": plan.command,
        "started_at": started_at,
        "phase": "exec",
        "completion_state": "unknown",
        "command_started": None,
        "terminal": False,
        "guidance": guidance,
        "plan": plan.as_dict(),
    })
    reporter.event(
        "command_dispatch" if guarded_dispatch else "command_started",
        run_id=run_id,
        command=" ".join(plan.command),
    )
    cmd_t0 = time.monotonic()
    # Tee stdout as it arrives without letting a verbose process exhaust controller
    # storage. Once the cap is reached, retain a bounded head plus a visible marker; the
    # completed result below is rewritten with cap_text's head+tail form. All live-log IO
    # is best-effort and cannot prevent the remote command from running.
    live_log = _LiveStdoutLog(rdir / "stdout.log", policy.max_log_bytes)

    def tee_stdout(chunk: str) -> None:
        live_log.append(chunk)

    try:
        exec_kwargs: dict[str, object] = {
            "env": exec_env,
            "path_prepend": runenv.path_prepend,
            "telemetry": telemetry_on,
            "on_stdout": tee_stdout,
        }
        if plan.workload is not None and telemetry_on:
            exec_kwargs["telemetry_request"] = TelemetryRequest()
        if memory_reservation is not None:
            exec_kwargs["memory_reservation"] = memory_reservation
        observed_exec = getattr(transport, "exec_observed", None)
        if not active_job_observation_enabled() or observed_exec is None:
            # Exact-base landing is dormant unless the controller explicitly opts in.
            # Third-party/test doubles continue through their established exec seam.
            result = transport.exec(plan.command, cwd=remote_cwd, **exec_kwargs)
        else:
            observation = JobObservation.for_command(
                job_id=run_id,
                project=plan.project.project_id,
                target=plan.target.name,
                phase="command",
                command=plan.command,
            )
            result = observed_exec(
                plan.command,
                cwd=remote_cwd,
                observation=observation,
                **exec_kwargs,
            )
    except CommandNotStartedError as exc:
        # The same target process that resolves a bare Windows command can prove it
        # refused the unsupported entry point before invoking user code. Remove the
        # conservative fence rather than manufacturing UNKNOWN.
        clear_unknown_completion_hazard(plan.project.project_id, run_id)
        reporter.event("command_rejected", phase="exec", message=str(exc))
        rejected_summary = {
            "run_id": run_id,
            "project_id": plan.project.project_id,
            "target": plan.target.name,
            "command": plan.command,
            "started_at": started_at,
            "ended_at": utc_now_iso(),
            "error": str(exc),
            "phase": "exec",
            "completion_state": "not_started",
            "command_started": False,
            "terminal": True,
            "plan": plan.as_dict(),
        }
        if plan.workload is not None and workload_runtime is not None:
            rejected_summary["workload"] = plan.workload.as_dict()
            rejected_summary["run_context"] = {
                "staged": workload_runtime.context_staged,
                "remote_path": workload_runtime.remote_context_path,
                "retained": True,
            }
            rejected_summary["resource_envelope"] = workload_runtime.resources
        write_json(summary_path, rejected_summary)
        return EXIT_INFRA
    except TransportError as exc:
        reporter.event("exec_error", message=str(exc))
        reporter.event("completion_unknown", guidance=guidance)
        unknown_summary = {
            "run_id": run_id,
            "error": str(exc),
            "phase": "exec",
            "completion_state": "unknown",
            "command_started": None,
            "terminal": True,
            "ended_at": utc_now_iso(),
            "guidance": guidance,
            "plan": plan.as_dict(),
        }
        if plan.workload is not None and workload_runtime is not None:
            unknown_summary["workload"] = plan.workload.as_dict()
            unknown_summary["run_context"] = {
                "staged": workload_runtime.context_staged,
                "remote_path": workload_runtime.remote_context_path,
                "retained": True,
            }
            unknown_summary["resource_envelope"] = workload_runtime.resources
        write_json(
            summary_path,
            unknown_summary,
        )
        # Whatever the command printed before the transport dropped is the only
        # evidence of how far it got; it is already capped on disk.
        return EXIT_INFRA
    finally:
        live_log.close()

    # The transport returned a conclusive exit status. Checkpoint that fact before
    # removing the pre-dispatch fence; pullback/receipt/profile finalization can still
    # fail, but it cannot make the command outcome unknown again.
    command_exit_code_checkpoint = (
        result.memory_guard.get("command_exit_code")
        if isinstance(result.memory_guard, dict)
        else result.exit_code
    )
    guarded_started = (
        result.memory_guard.get("command_started")
        if isinstance(result.memory_guard, dict)
        else True
    )
    write_json(summary_path, {
        "run_id": run_id,
        "project_id": plan.project.project_id,
        "target": plan.target.name,
        "command": plan.command,
        "started_at": started_at,
        "phase": "finalize" if args.no_pullback else "pullback",
        "completion_state": "command_complete",
        "command_started": guarded_started,
        "command_exit_code": command_exit_code_checkpoint,
        "terminal": False,
        "plan": plan.as_dict(),
    })
    clear_unknown_completion_hazard(plan.project.project_id, run_id)

    for log_name, text in (("stdout.log", result.stdout), ("stderr.log", result.stderr)):
        try:
            (rdir / log_name).write_text(
                cap_text(text, policy.max_log_bytes), encoding="utf-8"
            )
        except OSError:
            pass
    if result.stdout:
        sys.stdout.write(result.stdout)
        sys.stdout.flush()
    if result.stderr:
        sys.stderr.write(result.stderr)
        sys.stderr.flush()
    guard_result = result.memory_guard if isinstance(result.memory_guard, dict) else None
    guard_status = guard_result.get("status") if guard_result is not None else None
    guard_failed = guard_result is not None and guard_status != "ok"
    final_memory_reservation = (
        result.memory_reservation
        if result.memory_reservation is not None
        else memory_reservation
    )
    memory_limit_guidance: dict[str, object] | None = None
    if (
        guard_failed
        and guard_result is not None
        and guard_result.get("reason") == "command_memory_limit"
        and final_memory_reservation is not None
        and final_memory_reservation.allowance_basis
        == "unprofiled_available_backed"
    ):
        memory_limit_guidance = {
            "allowance_basis": final_memory_reservation.allowance_basis,
            "allocation_rule": final_memory_reservation.allocation_rule,
            "fair_share_limit_mib": final_memory_reservation.allowance_bytes // MIB,
            "observed_peak_lower_bound_bytes": guard_result.get(
                "peak_command_bytes"
            ),
            "policy_command_ceiling_bytes": (
                final_memory_reservation.max_command_bytes
            ),
            "partial_effects_may_exist": True,
            "profile_recorded": False,
            "retry_hint": (
                "inspect the workload, then intentionally rerun with "
                "--memory-limit-mib N if a larger hard limit is justified"
            ),
        }
        reporter.event("memory_limit_guidance", **memory_limit_guidance)
    command_exit_code = (
        guard_result.get("command_exit_code")
        if guard_result is not None
        else result.exit_code
    )
    reported_exit_code = EXIT_GUARD if guard_failed else result.exit_code
    if guarded_dispatch and guard_result is None:
        reporter.event(
            "command_started",
            run_id=run_id,
            command=" ".join(plan.command),
            confirmed_by="transport_result",
        )
    if guard_result is not None:
        if guard_result.get("command_started") is True:
            reporter.event(
                "command_started",
                run_id=run_id,
                command=" ".join(plan.command),
                confirmed_by="memory_guard",
            )
        reporter.event(
            "memory_guard",
            status=guard_status,
            reason=guard_result.get("reason"),
            command_started=guard_result.get("command_started"),
            command_exit_code=command_exit_code,
            peak_command_bytes=guard_result.get("peak_command_bytes"),
            min_host_available_bytes=guard_result.get("min_host_available_bytes"),
            cleanup_complete=guard_result.get("cleanup_complete"),
        )
    finished_fields = {
        "exit_code": reported_exit_code,
        "command_exit_code": command_exit_code,
        "duration_sec": round(time.monotonic() - t0, 3),
    }
    if result.telemetry:
        finished_fields["peak_rss_mb"] = result.telemetry.get("peak_rss_mb")
        finished_fields["avg_cpu_pct"] = result.telemetry.get("avg_cpu_pct")
    reporter.event("command_finished", **finished_fields)

    # Remote command exec time (excludes reconcile/pullback); folded into the
    # per-device profile after pullback so trip_s captures the full round-trip.
    exec_s = round(time.monotonic() - cmd_t0, 3)

    # --- post-run pullback ---------------------------------------------------
    post = None
    if not args.no_pullback:
        try:
            post = postrun_pullback(
                transport=transport,
                local_root=local_root,
                remote_root=remote_root,
                excludes=plan.excludes,
                hash_below_bytes=plan.hash_below_bytes,
                pre_remote_manifest=pre.remote_manifest,
                pre_local_manifest=pre.local_manifest,
                backup_root=backup_root,
                conflict_remote_root=conflict_dir(run_id) / "remote",
                backup_below_bytes=policy.backup_below_bytes,
                write_scope_paths=plan.write_scope_paths or None,
            )
        except TransportError as exc:
            reporter.event("transfer_error", phase="pullback", message=str(exc))
            pullback_summary = {
                "run_id": run_id,
                "exit_code": EXIT_GUARD if guard_failed else result.exit_code,
                "command_exit_code": command_exit_code,
                "error": str(exc),
                "phase": "pullback",
                "completion_state": "finalization_failed",
                "command_started": guarded_started,
                "terminal": True,
                "ended_at": utc_now_iso(),
                "plan": plan.as_dict(),
            }
            if plan.workload is not None and workload_runtime is not None:
                pullback_summary["workload"] = plan.workload.as_dict()
                pullback_summary["run_context"] = {
                    "staged": workload_runtime.context_staged,
                    "remote_path": workload_runtime.remote_context_path,
                    "retained": True,
                }
            write_json(summary_path, pullback_summary)
            # A later pullback failure must not erase the primary safety result.
            # The summary still records the transfer error, while the process exit
            # remains the distinct guard outcome required by automation.
            return EXIT_GUARD if guard_failed else EXIT_TRANSFER

        write_manifest(rdir / "post_remote_manifest.json", post.post_remote_manifest)
        if post.conflicts:
            for rel in post.conflicts:
                reporter.event("postrun_conflict", path=rel,
                               saved=str(conflict_dir(run_id) / "remote" / rel))
            reporter.event("postrun_conflict_unresolved", count=len(post.conflicts),
                           note="local edits during the run diverge from remote outputs; "
                                "baseline NOT advanced so the next run re-detects them")
            # Do NOT advance the baseline. Leaving the prior baseline in place makes the
            # next preflight classify these paths as both-changed and abort until the
            # user resolves them, instead of silently mtime-resolving the divergence.
        elif post.next_baseline is not None:
            # Reconciliation owns path attribution. Persist only its typed pair:
            # unrelated local work deliberately retains the pre-run generation and
            # remains visible to the next preflight.
            write_baseline(plan.target.name, plan.project.project_id,
                           post.next_baseline.local_manifest,
                           post.next_baseline.remote_manifest)
        else:  # defensive: uncertain attribution can never manufacture a baseline
            reporter.event(
                "baseline_not_advanced",
                note="postrun reconciliation returned no attributable baseline",
            )
    else:
        # Still record a baseline so future runs have delete evidence.
        write_baseline(plan.target.name, plan.project.project_id,
                       pre.local_manifest, pre.remote_manifest)

    receipt_validation: ReceiptValidation | None = None
    workload_cleanup: dict[str, str] | None = None
    if workload_runtime is not None:
        receipt_validation = _collect_workload_receipt(
            plan=plan,
            transport=transport,
            runtime=workload_runtime,
            run_id=run_id,
            rdir=rdir,
            reporter=reporter,
        )
        workload_cleanup = _cleanup_workload_files(
            transport=transport,
            runtime=workload_runtime,
            reporter=reporter,
        )

    duration = round(time.monotonic() - t0, 3)

    # A post-run conflict means the command ran but remrun could not converge local
    # state. If the command itself succeeded, surface that as a conflict (exit 2)
    # rather than reporting a clean success; the command's own code is preserved in
    # the summary as command_exit_code and in the command_finished event.
    postrun_unresolved = bool(post and post.conflicts)
    required_receipt_invalid = bool(
        plan.workload is not None
        and plan.workload.require_receipt
        and (receipt_validation is None or not receipt_validation.valid)
    )
    if required_receipt_invalid:
        reporter.event(
            "workload_contract_failed",
            workload=plan.workload.name if plan.workload is not None else None,
            receipt_status=(
                receipt_validation.status
                if receipt_validation is not None
                else "not_collected"
            ),
        )
    if guard_failed:
        final_exit = EXIT_GUARD
    elif result.exit_code != 0:
        final_exit = result.exit_code
    elif postrun_unresolved:
        final_exit = EXIT_CONFLICT
    elif required_receipt_invalid:
        final_exit = EXIT_INTERNAL
    else:
        final_exit = EXIT_OK

    # Scheduler-consumed profiles are admitted only after a successful command and
    # positively attributable pullback. Failed commands, disabled pullback, scope
    # escapes, invalid required receipts, and uncertain baselines are not evidence
    # of a reusable job cost. Best-effort storage must never break a valid run.
    profile_admissible = bool(
        not guard_failed
        and result.exit_code == 0
        and final_exit == EXIT_OK
        and not args.no_pullback
        and post is not None
        and not post.conflicts
        and post.next_baseline is not None
    )
    if profile_admissible:
        try:
            tel = result.telemetry or {}
            profile_peak_rss_mb = tel.get("peak_rss_mb")
            if not (
                isinstance(profile_peak_rss_mb, (int, float))
                and not isinstance(profile_peak_rss_mb, bool)
                and math.isfinite(float(profile_peak_rss_mb))
                and float(profile_peak_rss_mb) > 0
            ):
                guard_peak = (
                    guard_result.get("peak_command_bytes")
                    if guard_result is not None and guard_result.get("status") == "ok"
                    else None
                )
                profile_peak_rss_mb = (
                    guard_peak / MIB
                    if isinstance(guard_peak, int)
                    and not isinstance(guard_peak, bool)
                    and guard_peak > 0
                    else None
                )
            ckey = command_key(plan.command)
            profile_exec_s = exec_s
            target_wall = tel.get("wall_sec")
            if (
                plan.workload is not None
                and isinstance(target_wall, (int, float))
                and not isinstance(target_wall, bool)
                and math.isfinite(target_wall)
                and target_wall > 0
            ):
                profile_exec_s = float(target_wall)
            update_profile(
                default_state_root(),
                profile_project_id(plan.project.project_id),
                ckey,
                plan.target.name,
                peak_rss_mb=profile_peak_rss_mb,
                avg_cpu_pct=tel.get("avg_cpu_pct"),
                exec_s=profile_exec_s,
                trip_s=duration,
                now=utc_now_iso(),
            )
        except Exception:
            pass

    workload_profile_result: dict[str, str] | None = None
    if plan.workload is not None:
        observation: WorkloadObservation | None = None
        if final_exit != EXIT_OK or result.exit_code != 0:
            detail = "final run result is not successful"
        elif args.no_pullback:
            detail = "pullback was disabled"
        elif post is None or post.conflicts or post.next_baseline is None:
            detail = "pullback did not yield a verified attributable baseline"
        elif workload_runtime is None or not workload_runtime.context_staged:
            detail = "workload context was not staged"
        elif not telemetry_on:
            detail = "detailed telemetry was disabled"
        else:
            observation, detail = _workload_observation_from_run(
                project_id=plan.project.project_id,
                command=plan.command,
                device=plan.target.name,
                workload=plan.workload,
                receipt=receipt_validation,
                telemetry=result.telemetry,
                trip_s=duration,
                updated=utc_now_iso(),
            )
        if observation is not None:
            try:
                recorded = update_workload_profile(default_state_root(), observation)
            except Exception:
                recorded = False
            if recorded:
                workload_profile_result = {"status": "recorded", "detail": ""}
            else:
                workload_profile_result = {
                    "status": "not_recorded",
                    "detail": "controller profile store rejected the observation",
                }
        else:
            workload_profile_result = {"status": "not_recorded", "detail": detail}
        reporter.event(
            "workload_profile",
            workload=plan.workload.name,
            **workload_profile_result,
        )

    summary = {
        "run_id": run_id,
        "project_id": plan.project.project_id,
        "target": plan.target.name,
        "command": plan.command,
        "exit_code": final_exit,
        "command_exit_code": command_exit_code,
        "started_at": started_at,
        "ended_at": utc_now_iso(),
        "completion_state": "complete",
        "terminal": True,
        "duration_sec": duration,
        "files_pushed": len(pre.pushed),
        "files_pulled_pre": len(pre.pulled),
        "files_deleted_remote_pre": len(pre.deleted_remote),
        "files_deleted_local_pre": len(pre.deleted_local),
        "files_skipped_identical_pre": len(pre.skipped_identical),
        "files_converged_conflicts_pre": len(pre.converged_conflicts),
        "files_pulled_post": len(post.pulled) if post else 0,
        "files_deleted_local_post": len(post.deleted_local) if post else 0,
        "preflight_conflicts": 0,
        "postrun_conflicts": len(post.conflicts) if post else 0,
        "telemetry": result.telemetry,
        "memory_guard": guard_result,
        "memory_admission": (
            final_memory_reservation.as_dict()
            if final_memory_reservation is not None
            else None
        ),
        "memory_limit_guidance": memory_limit_guidance,
        "peak_rss_mb": result.telemetry.get("peak_rss_mb") if result.telemetry else None,
        "avg_cpu_pct": result.telemetry.get("avg_cpu_pct") if result.telemetry else None,
        "plan": plan.as_dict(),
    }
    if plan.workload is not None and workload_runtime is not None:
        summary["workload"] = plan.workload.as_dict()
        summary["run_context"] = {
            "staged": workload_runtime.context_staged,
            "remote_path": workload_runtime.remote_context_path,
            "staging_error": workload_runtime.staging_error or None,
        }
        summary["resource_envelope"] = workload_runtime.resources
        summary["receipt"] = {
            "status": (
                receipt_validation.status
                if receipt_validation is not None
                else "not_collected"
            ),
            "detail": (
                receipt_validation.detail
                if receipt_validation is not None
                else "workload context was not staged"
            ),
            "data": receipt_validation.data if receipt_validation is not None else None,
            "required": plan.workload.require_receipt,
        }
        summary["workload_cleanup"] = workload_cleanup
        summary["workload_profile"] = workload_profile_result
    write_json(summary_path, summary)
    reporter.event("summary", run_id=run_id, exit_code=final_exit,
                   files_pushed=summary["files_pushed"], files_pulled_post=summary["files_pulled_post"],
                   duration_sec=duration, summary_path=str(summary_path))

    # Self-limiting: apply the retention policy. Never let cleanup break a run.
    try:
        report = prune_state(policy, exempt_run_ids=attempted_run_ids)
        if report.runs_deleted or report.runs_trimmed:
            reporter.event("retention", runs_deleted=report.runs_deleted,
                           runs_trimmed=report.runs_trimmed,
                           freed_mb=round(report.bytes_freed / (1024 * 1024), 2))
    except Exception:  # pragma: no cover - cleanup must not affect exit code
        pass

    return final_exit


def cmd_bench(args: argparse.Namespace, reporter: Reporter) -> int:
    """Benchmark a command: time it locally and via the full remrun trip to each
    target, fold both into the profile, and print an offload recommendation.

    Gathers data + recommends only — it changes no run behavior. The local leg runs
    the job on THIS machine, so only bench where running locally is acceptable (not
    a heavy job on a low-power controller). For a job too heavy to run here, pass
    ``--no-local``: it skips the baseline leg, measures only the remote round-trip,
    and assumes offload (there is nothing to compare against).
    """
    config = load_config()
    command = normalize_cmd(args.cmd)
    if args.targets:
        targets = [t.strip() for t in str(args.targets).split(",") if t.strip()]
    else:
        sched = scheduler_config(config)
        ordered = [sched.get("primary"), *list(sched.get("fallback", []))]
        targets = [str(t) for t in ordered if t and t in config.devices]
        if not targets:
            targets = [name for name, device in config.devices.items() if device.enabled]
    skip_local = bool(getattr(args, "no_local", False))
    probe_plan = make_run_plan(
        cwd=Path.cwd(), config=config,
        target_name=targets[0] if targets else "auto",
        command=command, json_events=args.json,
        allow_default_workload=False,
    )
    project = probe_plan.project
    if _report_unknown_completion_hazard(project.project_id, reporter):
        return EXIT_INTERNAL
    key = command_key(command)
    profile_id = profile_project_id(project.project_id)
    state_root = default_state_root()

    # 1. Local baseline (timed wall on this machine) — unless --no-local.
    if skip_local:
        reporter.event("bench_local_skipped",
                       note="--no-local: not running the job here; assuming offload")
    else:
        reporter.event("bench_local_started", command=" ".join(command), cwd=str(project.local_cwd))
        t0 = time.monotonic()
        try:
            rc = subprocess.run(command, cwd=str(project.local_cwd)).returncode
        except OSError as exc:
            reporter.event("bench_local_error", message=str(exc))
            rc = None
        local_s = round(time.monotonic() - t0, 3)
        if rc == 0:
            update_profile(state_root, profile_id, key, LOCAL_DEVICE,
                           exec_s=local_s, trip_s=local_s, now=utc_now_iso())
            reporter.event("bench_local", exec_s=local_s, exit_code=rc)
        else:
            reporter.event("bench_local_unrecorded", exec_s=local_s, exit_code=rc,
                           note="non-zero/failed local run not folded into the baseline")

    # 2. Each target via the full remrun trip (cmd_run records its per-device row).
    # A leg only contributes to the verdict if its round-trip actually completed;
    # a remrun-level failure (unreachable/transfer/conflict/internal) recorded no
    # fresh row, so an unrelated stale row must not silently drive the recommendation.
    infra_failures = {
        EXIT_INTERNAL, EXIT_CONFLICT, EXIT_TRANSFER, EXIT_INFRA, EXIT_GUARD
    }
    ran: list[str] = []
    for dev in targets:
        reporter.event("bench_remote_started", device=dev)
        run_args = argparse.Namespace(
            target=dev, auto=False, cmd=command, json=args.json,
            dry_run=False, no_pullback=False, no_telemetry=False,
            workload=None, suppress_default_workload=True,
        )
        rc = cmd_run(run_args, reporter)
        reporter.event("bench_remote_finished", device=dev, exit_code=rc)
        if rc not in infra_failures:
            ran.append(dev)
    failed = [d for d in targets if d not in ran]
    if failed:
        reporter.event("bench_legs_failed", devices=failed,
                       note="round-trip did not complete; excluded from the verdict")

    # 3. Verdict — only from targets whose round-trip completed in THIS bench.
    bias = float(scheduler_config(config).get("offload_bias", 1.25))
    profiles = load_profiles(state_root)
    if skip_local:
        # No local baseline to compare against: recommend the fastest measured
        # target outright (the explicit point of --no-local).
        rec = _best_remote_verdict(profiles, profile_id, key, ran)
    else:
        rec = recommend_offload(profiles, profile_id, key, devices=ran, bias=bias)
    reporter.event("bench_verdict", **rec)
    # If no remote leg completed, the bench gathered no remote data — report failure.
    return EXIT_OK if ran else EXIT_INFRA


def _best_remote_verdict(profiles: dict, project_id: str, key: str,
                         targets: list[str]) -> dict:
    """Verdict for ``bench --no-local``: pick the fastest target by measured
    round-trip and recommend it. 'unknown' when no target produced a trip time
    (e.g. every target was unreachable)."""
    timed: dict[str, float] = {}
    for dev in targets:
        entry = device_profile(profiles, project_id, key, dev)
        if entry and entry.get("trip_s") is not None:
            timed[dev] = float(entry["trip_s"])
    if not timed:
        return {"recommend": "unknown", "basis": "no-local",
                "note": "no target round-trip recorded (targets unreachable?)"}
    best = min(timed, key=timed.get)
    return {"recommend": "remote", "basis": "no-local", "best_device": best,
            "best_trip_s": round(float(timed[best]), 3),
            "note": "local baseline skipped; recommending fastest measured target"}


def cmd_sync(args: argparse.Namespace, reporter: Reporter) -> int:
    """Pull-biased, project-less folder sync for configured output trees.

    Delegates to ``sync.run_sync``; the heavy lifting (manifest + classification)
    is the same engine ``run`` uses, but stateless (no baseline → additive, never
    proposes a destructive delete) and never clobbers a newer remote with an older
    local.
    """
    from . import sync as sync_mod

    config = load_config()
    return sync_mod.run_sync(
        config,
        arg=args.folder,
        device_name=args.device,
        remote_override=args.remote,
        direction=args.direction,
        dry_run=args.dry_run,
        extra_excludes=args.exclude,
        reporter=reporter,
        as_json=args.json,
    )


def cmd_git_sync(args: argparse.Namespace, reporter: Reporter) -> int:
    """Exchange Git history with a peer device using bundles over remrun transport.

    `.git/` stays out of remrun/Syncthing file reconciliation; only Git-created
    bundle files cross the transport, and branches advance by fast-forward only.
    """
    from . import gitsync as gitsync_mod

    config = load_config()
    if args.install_hook:
        return gitsync_mod.install_git_sync_hook(
            config, device_name=args.device, reporter=reporter)
    if args.uninstall_hook:
        return gitsync_mod.uninstall_git_sync_hook(config, reporter=reporter)
    if args.status:
        if not args.device:
            reporter.event("git_sync_error", message="missing device", exit_code=EXIT_INTERNAL)
            return EXIT_INTERNAL
        return gitsync_mod.run_git_sync_status(
            config,
            device_name=args.device,
            branch=args.branch,
            remote_memory_limit_mib=args.remote_memory_limit_mib,
            reporter=reporter,
            as_json=args.json,
        )
    if not args.device:
        reporter.event("git_sync_error", message="missing device", exit_code=EXIT_INTERNAL)
        return EXIT_INTERNAL
    return gitsync_mod.run_git_sync(
        config,
        device_name=args.device,
        direction=args.direction,
        dry_run=args.dry_run,
        branch=args.branch,
        bootstrap=args.bootstrap,
        remote_memory_limit_mib=args.remote_memory_limit_mib,
        reporter=reporter,
        as_json=args.json,
    )


def cmd_status(args: argparse.Namespace, reporter: Reporter) -> int:
    state_root = default_state_root()
    runs_root = state_root / "runs"
    fleet_state = _fleet_state_summary(state_root)
    if not runs_root.exists():
        if args.json:
            print(json.dumps({"runs": [], "fleet_state": fleet_state}, indent=2, sort_keys=True))
            return EXIT_OK
        reporter.event("no_runs", path=str(runs_root))
        return EXIT_OK
    summaries = []
    run_ids = sorted((d.name for d in runs_root.iterdir() if d.is_dir()), reverse=True)
    limit = max(0, args.limit)
    for rid in run_ids:
        if len(summaries) >= limit:
            break
        s = read_json(runs_root / rid / "summary.json") or {"run_id": rid}
        target = s.get("target") or ((s.get("plan") or {}).get("target") or {}).get("name")
        if args.device and target != args.device:
            continue
        summaries.append(s)
    if args.json:
        print(json.dumps({"runs": summaries, "fleet_state": fleet_state}, indent=2, sort_keys=True))
    else:
        if not summaries:
            reporter.event("no_runs", path=str(runs_root), device=args.device)
        for s in summaries:
            print(f"{s.get('run_id')}\texit={s.get('exit_code', '?')}\t"
                  f"pushed={s.get('files_pushed', '?')}\tpulled_post={s.get('files_pulled_post', '?')}\t"
                  f"conflicts={s.get('postrun_conflicts', s.get('conflicts', '?'))}\t"
                  f"peak_rss_mb={s.get('peak_rss_mb', '-')}")
    return EXIT_OK


def cmd_logs(args: argparse.Namespace, reporter: Reporter) -> int:
    runs_root = default_state_root() / "runs"
    if not runs_root.exists():
        reporter.event("no_runs", path=str(runs_root))
        return EXIT_INTERNAL
    if args.run == "last":
        dirs = sorted((d for d in runs_root.iterdir() if d.is_dir()), reverse=True)
        if not dirs:
            reporter.event("no_runs", path=str(runs_root))
            return EXIT_INTERNAL
        target = dirs[0]
    else:
        target = runs_root / args.run
        if not target.exists():
            reporter.event("unknown_run", run=args.run)
            return EXIT_INTERNAL

    if args.json:
        print(json.dumps(read_json(target / "summary.json") or {}, indent=2, sort_keys=True))
        return EXIT_OK
    for name in ("stdout.log", "stderr.log"):
        f = target / name
        if f.exists() and f.read_text(encoding="utf-8"):
            print(f"=== {name} ({target.name}) ===")
            stream = sys.stderr if name == "stderr.log" else sys.stdout
            stream.write(f.read_text(encoding="utf-8"))
    return EXIT_OK


def _durable_record_path(rdir: Path) -> Path:
    return rdir / "durable.json"


def _validate_durable_identity(
    status: dict[str, object], record: dict[str, object], *, require_complete: bool = False
) -> None:
    expected = {
        "run_id": record["run_id"],
        "project_id": record["project_id"],
        "target": record["target"],
        "controller": record["controller"],
        "command_sha256": record["command_sha256"],
    }
    for field, value in expected.items():
        if status.get(field) != value:
            raise DurableStateError(f"durable target state {field} mismatch")
    state = status.get("state")
    if state not in {"launching", "pending", "running", "complete", "failed"}:
        raise DurableStateError("durable target state is invalid")
    if require_complete and state != "complete":
        raise DurableStateError("durable target result is not complete")


def _detached_durable(
    reporter: Reporter,
    summary_path: Path,
    record: dict[str, object],
    status: dict[str, object],
    *,
    reason: str,
) -> int:
    summary = read_json(summary_path) or {"run_id": record["run_id"]}
    summary.update(
        phase="durable_wait",
        completion_state="detached",
        terminal=False,
        durable_state=status.get("state", "running_or_pending"),
        durable_acknowledged=True,
        detached_reason=reason,
        plan=record["plan"],
    )
    write_json(summary_path, summary)
    reporter.event(
        "durable_detached",
        run_id=record["run_id"],
        state=status.get("state", "running_or_pending"),
        note="target supervisor remains authoritative; resume with the exact run ID",
    )
    return EXIT_OK


def _running_observation_matches(transport, record: dict[str, object]) -> bool:  # noqa: ANN001
    payload = transport.query_observed_jobs(sample_interval=0.05, timeout=30.0)
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(jobs, list):
        raise DurableStateError("observer query returned malformed jobs")
    matches = [job for job in jobs if isinstance(job, dict) and job.get("job_id") == record["run_id"]]
    if len(matches) != 1:
        return False
    job = matches[0]
    expected = {
        "project": record["project_id"],
        "source_controller": record["controller"],
        "target": record["target"],
    }
    for field, value in expected.items():
        if job.get(field) != value:
            raise DurableStateError(f"durable observer {field} mismatch")
    command = job.get("command")
    if not isinstance(command, dict) or command.get("sha256") != record["command_sha256"]:
        raise DurableStateError("durable observer command digest mismatch")
    if job.get("observation_status") == "unknown":
        raise DurableStateError("durable observer ownership is unknown")
    return True


def _finalize_durable_run(
    *,
    args: argparse.Namespace,
    reporter: Reporter,
    plan: RunPlan,
    transport,
    remote_root: str,
    run_id: str,
    rdir: Path,
    summary_path: Path,
    policy: RetentionPolicy,
    record: dict[str, object],
) -> int:
    existing = read_json(summary_path)
    if (
        isinstance(existing, dict)
        and existing.get("completion_state") == "complete"
        and existing.get("terminal") is True
    ):
        code = existing.get("exit_code")
        if isinstance(code, int) and not isinstance(code, bool):
            return code
        raise UnknownCompletionHazardError("completed durable summary has no valid exit code")

    payload = transport.durable_status(run_id, str(record["resume_token"]), include_logs=True)
    if not isinstance(payload, dict) or not isinstance(payload.get("status"), dict):
        raise TransportError("durable result payload is malformed")
    status = payload["status"]
    _validate_durable_identity(status, record, require_complete=True)
    execution = record.get("execution")
    if not isinstance(execution, dict):
        raise UnknownCompletionHazardError("saved durable execution metadata is missing")
    try:
        result = finalize_durable_result(payload, execution)
    except CommandNotStartedError as exc:
        clear_unknown_completion_hazard(plan.project.project_id, run_id)
        reporter.event("command_rejected", phase="durable_exec", message=str(exc))
        write_json(summary_path, {
            "run_id": run_id,
            "project_id": plan.project.project_id,
            "target": plan.target.name,
            "command": plan.command,
            "started_at": record["started_at"],
            "ended_at": utc_now_iso(),
            "error": str(exc),
            "phase": "durable_exec",
            "completion_state": "not_started",
            "command_started": False,
            "terminal": True,
            "exit_code": EXIT_INFRA,
            "durable": True,
            "plan": plan.as_dict(),
        })
        try:
            transport.durable_cleanup(run_id, str(record["resume_token"]))
        except TransportError as cleanup_exc:
            reporter.event(
                "durable_cleanup_deferred", run_id=run_id, message=str(cleanup_exc)
            )
        return EXIT_INFRA

    command_exit_code_checkpoint = (
        result.memory_guard.get("command_exit_code")
        if isinstance(result.memory_guard, dict)
        else result.exit_code
    )
    guarded_started = (
        result.memory_guard.get("command_started")
        if isinstance(result.memory_guard, dict)
        else status.get("command_started")
    )
    write_json(summary_path, {
        "run_id": run_id,
        "project_id": plan.project.project_id,
        "target": plan.target.name,
        "command": plan.command,
        "started_at": record["started_at"],
        "phase": "finalize" if bool(record["no_pullback"]) else "pullback",
        "completion_state": "command_complete",
        "command_started": guarded_started,
        "command_exit_code": command_exit_code_checkpoint,
        "terminal": False,
        "durable": True,
        "plan": plan.as_dict(),
    })

    for log_name, text in (("stdout.log", result.stdout), ("stderr.log", result.stderr)):
        try:
            (rdir / log_name).write_text(cap_text(text, policy.max_log_bytes), encoding="utf-8")
        except OSError:
            pass
    if result.stdout:
        sys.stdout.write(result.stdout)
        sys.stdout.flush()
    if result.stderr:
        sys.stderr.write(result.stderr)
        sys.stderr.flush()

    guard_result = result.memory_guard if isinstance(result.memory_guard, dict) else None
    guard_status = guard_result.get("status") if guard_result is not None else None
    guard_failed = guard_result is not None and guard_status != "ok"
    command_exit_code = (
        guard_result.get("command_exit_code") if guard_result is not None else result.exit_code
    )
    if guard_result is not None:
        reporter.event(
            "memory_guard",
            status=guard_status,
            reason=guard_result.get("reason"),
            command_started=guard_result.get("command_started"),
            command_exit_code=command_exit_code,
            peak_command_bytes=guard_result.get("peak_command_bytes"),
            min_host_available_bytes=guard_result.get("min_host_available_bytes"),
            cleanup_complete=guard_result.get("cleanup_complete"),
        )

    pre_local = read_manifest(rdir / "pre_local_manifest.json")
    pre_remote = read_manifest(rdir / "pre_remote_manifest.json")
    backup_root = conflict_dir(run_id) / "backup"
    post = None
    if not bool(record["no_pullback"]):
        try:
            post = postrun_pullback(
                transport=transport,
                local_root=plan.project.local_project_root,
                remote_root=remote_root,
                excludes=plan.excludes,
                hash_below_bytes=plan.hash_below_bytes,
                pre_remote_manifest=pre_remote,
                pre_local_manifest=pre_local,
                backup_root=backup_root,
                conflict_remote_root=conflict_dir(run_id) / "remote",
                backup_below_bytes=policy.backup_below_bytes,
                write_scope_paths=plan.write_scope_paths or None,
            )
        except TransportError as exc:
            reporter.event("transfer_error", phase="pullback", message=str(exc))
            write_json(summary_path, {
                "run_id": run_id,
                "project_id": plan.project.project_id,
                "target": plan.target.name,
                "phase": "pullback",
                "completion_state": "finalization_failed",
                "terminal": True,
                "command_exit_code": command_exit_code,
                "error": str(exc),
                "durable": True,
                "plan": plan.as_dict(),
            })
            return EXIT_GUARD if guard_failed else EXIT_TRANSFER
        write_manifest(rdir / "post_remote_manifest.json", post.post_remote_manifest)
        if post.conflicts:
            for rel in post.conflicts:
                reporter.event(
                    "postrun_conflict", path=rel,
                    saved=str(conflict_dir(run_id) / "remote" / rel),
                )
        elif post.next_baseline is not None:
            write_baseline(
                plan.target.name,
                plan.project.project_id,
                post.next_baseline.local_manifest,
                post.next_baseline.remote_manifest,
            )
    else:
        write_baseline(plan.target.name, plan.project.project_id, pre_local, pre_remote)

    postrun_unresolved = bool(post and post.conflicts)
    if guard_failed:
        final_exit = EXIT_GUARD
    elif result.exit_code != 0:
        final_exit = result.exit_code
    elif postrun_unresolved:
        final_exit = EXIT_CONFLICT
    else:
        final_exit = EXIT_OK

    duration = max(0.0, time.time() - float(status.get("created_epoch", time.time())))
    checkpoint = {
        "run_id": run_id,
        "project_id": plan.project.project_id,
        "target": plan.target.name,
        "command": plan.command,
        "exit_code": final_exit,
        "command_exit_code": command_exit_code,
        "started_at": record["started_at"],
        "ended_at": utc_now_iso(),
        "completion_state": "finalization_complete",
        "terminal": False,
        "duration_sec": round(duration, 3),
        "files_pulled_post": len(post.pulled) if post else 0,
        "postrun_conflicts": len(post.conflicts) if post else 0,
        "telemetry": result.telemetry,
        "memory_guard": guard_result,
        "durable": True,
        "plan": plan.as_dict(),
    }
    write_json(summary_path, checkpoint)
    if not clear_unknown_completion_hazard(plan.project.project_id, run_id):
        raise UnknownCompletionHazardError(
            f"durable completion fence changed before finalization for {run_id!r}"
        )
    checkpoint["completion_state"] = "complete"
    checkpoint["terminal"] = True
    write_json(summary_path, checkpoint)
    reporter.event(
        "command_finished",
        exit_code=final_exit,
        command_exit_code=command_exit_code,
        durable=True,
    )
    try:
        transport.durable_cleanup(run_id, str(record["resume_token"]))
    except TransportError as exc:
        reporter.event("durable_cleanup_deferred", run_id=run_id, message=str(exc))
    else:
        checkpoint["durable_cleanup_complete"] = True
        write_json(summary_path, checkpoint)
    return final_exit


def _poll_durable(
    *,
    args: argparse.Namespace,
    reporter: Reporter,
    plan: RunPlan,
    transport,
    remote_root: str,
    run_id: str,
    rdir: Path,
    summary_path: Path,
    policy: RetentionPolicy,
    record: dict[str, object],
    initial_status: dict[str, object],
    no_wait: bool = False,
) -> int:
    status = initial_status
    acknowledged = bool(status.get("acknowledged"))
    ownership_checked = False
    if acknowledged:
        reporter.event("durable_acknowledged", run_id=run_id, state=status.get("state"))
    if status.get("detached_after_ack") and acknowledged:
        return _detached_durable(
            reporter, summary_path, record, status, reason="launch connection lost after acknowledgement"
        )
    while True:
        _validate_durable_identity(status, record)
        state = status.get("state")
        if state == "complete":
            return _finalize_durable_run(
                args=args, reporter=reporter, plan=plan, transport=transport,
                remote_root=remote_root, run_id=run_id, rdir=rdir,
                summary_path=summary_path, policy=policy, record=record,
            )
        if state == "failed":
            write_json(summary_path, {
                "run_id": run_id,
                "phase": "durable_wait",
                "completion_state": "unknown",
                "terminal": True,
                "error": status.get("error", "durable target state failed closed"),
                "durable": True,
                "plan": plan.as_dict(),
            })
            reporter.event(
                "completion_unknown",
                run_id=run_id,
                guidance=(
                    "target durable state is missing, corrupt, mismatched, or "
                    "ambiguous; do not retry user code"
                ),
            )
            return EXIT_INFRA
        if state == "running" and acknowledged and not ownership_checked:
            try:
                if not _running_observation_matches(transport, record):
                    status = transport.durable_status(
                        run_id, str(record["resume_token"])
                    )
                    _validate_durable_identity(status, record)
                    if status.get("state") != "complete":
                        raise DurableStateError(
                            "durable state says running but the exact observer "
                            "ownership row is absent"
                        )
                    continue
                ownership_checked = True
            except DurableStateError as exc:
                write_json(summary_path, {
                    "run_id": run_id,
                    "phase": "durable_wait",
                    "completion_state": "unknown",
                    "terminal": True,
                    "error": str(exc),
                    "durable": True,
                    "plan": plan.as_dict(),
                })
                reporter.event(
                    "completion_unknown", run_id=run_id,
                    guidance=(
                        "authenticated durable state or observer ownership failed "
                        "validation; do not retry user code"
                    ),
                )
                return EXIT_INFRA
            except TransportError as exc:
                return _detached_durable(
                    reporter, summary_path, record, status, reason=str(exc)
                )
        if no_wait and acknowledged:
            return _detached_durable(
                reporter, summary_path, record, status, reason="resume requested --no-wait"
            )
        try:
            time.sleep(1.0)
            next_status = transport.durable_status(
                run_id, str(record["resume_token"])
            )
            _validate_durable_identity(next_status, record)
            status = next_status
            if status.get("acknowledged") and not acknowledged:
                acknowledged = True
                reporter.event(
                    "durable_acknowledged",
                    run_id=run_id,
                    state=status.get("state"),
                )
        except KeyboardInterrupt:
            if acknowledged:
                return _detached_durable(
                    reporter, summary_path, record, status,
                    reason="controller interrupted after acknowledgement",
                )
            raise
        except DurableStateError as exc:
            write_json(summary_path, {
                "run_id": run_id,
                "phase": "durable_wait",
                "completion_state": "unknown",
                "terminal": True,
                "error": str(exc),
                "durable": True,
                "plan": plan.as_dict(),
            })
            reporter.event(
                "completion_unknown",
                run_id=run_id,
                guidance=(
                    "authenticated durable state or observer ownership failed "
                    "validation; do not retry user code"
                ),
            )
            return EXIT_INFRA
        except TransportError as exc:
            if acknowledged:
                return _detached_durable(
                    reporter, summary_path, record, status, reason=str(exc)
                )
            reporter.event(
                "completion_unknown",
                run_id=run_id,
                guidance=(
                    "connection lost before durable acknowledgement; do not retry "
                    "user code"
                ),
            )
            write_json(summary_path, {
                "run_id": run_id,
                "phase": "durable_launch",
                "completion_state": "unknown",
                "terminal": True,
                "error": str(exc),
                "durable": True,
                "plan": plan.as_dict(),
            })
            return EXIT_INFRA


def _run_durable_locked(
    *,
    args: argparse.Namespace,
    reporter: Reporter,
    plan: RunPlan,
    transport,
    remote_root: str,
    remote_cwd: str,
    run_id: str,
    rdir: Path,
    summary_path: Path,
    started_at: str,
    t0: float,
    policy: RetentionPolicy,
    telemetry_default: bool,
    pre,
    backup_root: Path,
    memory_reservation: MemoryReservation | None,
) -> int:
    del t0, pre, backup_root
    runenv = resolve_run_env(
        device=plan.target, project=plan.project, project_config=plan.project_config
    )
    transport.ensure_remote_dir(remote_cwd)
    resume_token = secrets.token_urlsafe(32)
    source_controller = controller_label()
    observation = JobObservation.for_command(
        job_id=run_id,
        project=plan.project.project_id,
        target=plan.target.name,
        phase="command",
        command=plan.command,
        source_controller=source_controller,
    )
    record: dict[str, object] = {
        "schema": 1,
        "run_id": run_id,
        "project_id": plan.project.project_id,
        "target": plan.target.name,
        "controller": source_controller,
        "command_sha256": observation.command_sha256,
        "resume_token": resume_token,
        "started_at": started_at,
        "remote_root": remote_root,
        "remote_cwd": remote_cwd,
        "no_pullback": bool(args.no_pullback),
        "plan": plan.as_dict(),
        "execution": None,
    }
    write_json(_durable_record_path(rdir), record)
    hazard = write_unknown_completion_hazard(
        plan.project.project_id, plan.target.name, run_id
    )
    write_json(summary_path, {
        "run_id": run_id,
        "project_id": plan.project.project_id,
        "target": plan.target.name,
        "command": plan.command,
        "started_at": started_at,
        "phase": "durable_launch",
        "completion_state": "unknown",
        "command_started": None,
        "terminal": False,
        "guidance": _unknown_completion_guidance(hazard),
        "durable": True,
        "plan": plan.as_dict(),
    })
    telemetry_on = telemetry_default and not args.no_telemetry
    reporter.event("durable_launch", run_id=run_id, target=plan.target.name)
    try:
        status, execution = transport.launch_durable(
            plan.command,
            remote_cwd,
            run_id=run_id,
            resume_token=resume_token,
            observation=observation,
            controller=source_controller,
            project_id=plan.project.project_id,
            max_log_bytes=policy.max_log_bytes,
            created_at=started_at,
            env=runenv.env,
            path_prepend=runenv.path_prepend,
            telemetry=telemetry_on,
            telemetry_request=None,
            memory_reservation=memory_reservation,
        )
    except CommandNotStartedError as exc:
        clear_unknown_completion_hazard(plan.project.project_id, run_id)
        reporter.event(
            "durable_prestart_refused", run_id=run_id, target=plan.target.name,
            detail=str(exc),
        )
        write_json(summary_path, {
            "run_id": run_id,
            "project_id": plan.project.project_id,
            "target": plan.target.name,
            "command": plan.command,
            "started_at": started_at,
            "phase": "durable_prestart_refused",
            "completion_state": "not_started",
            "command_started": False,
            "terminal": True,
            "exit_code": EXIT_INFRA,
            "error": str(exc),
            "durable": True,
            "plan": plan.as_dict(),
        })
        return EXIT_INFRA
    except DurablePrestartError as exc:
        clear_unknown_completion_hazard(plan.project.project_id, run_id)
        reporter.event(
            "durable_prestart_refused", run_id=run_id, target=plan.target.name,
            detail=str(exc),
        )
        write_json(summary_path, {
            "run_id": run_id,
            "project_id": plan.project.project_id,
            "target": plan.target.name,
            "command": plan.command,
            "started_at": started_at,
            "phase": "durable_prestart_refused",
            "completion_state": "not_started",
            "command_started": False,
            "terminal": True,
            "exit_code": exc.exit_code,
            "error": str(exc),
            "durable": True,
            "plan": plan.as_dict(),
        })
        return exc.exit_code
    except TransportError as exc:
        reporter.event(
            "completion_unknown",
            run_id=run_id,
            guidance=(
                "connection or target state failed before positive durable "
                "acknowledgement; do not retry user code"
            ),
        )
        write_json(summary_path, {
            "run_id": run_id,
            "project_id": plan.project.project_id,
            "target": plan.target.name,
            "command": plan.command,
            "phase": "durable_launch",
            "completion_state": "unknown",
            "terminal": True,
            "error": str(exc),
            "durable": True,
            "plan": plan.as_dict(),
        })
        return EXIT_INFRA
    record["execution"] = execution
    record["last_target_status"] = status
    write_json(_durable_record_path(rdir), record)
    return _poll_durable(
        args=args, reporter=reporter, plan=plan, transport=transport,
        remote_root=remote_root, run_id=run_id, rdir=rdir,
        summary_path=summary_path, policy=policy, record=record,
        initial_status=status,
    )


def _load_resume_plan(
    run_id: str,
) -> tuple[RunPlan, dict[str, object], Path, Path, object]:
    rdir = run_dir(run_id)
    record = read_json(_durable_record_path(rdir))
    if (
        not isinstance(record, dict)
        or record.get("schema") != 1
        or record.get("run_id") != run_id
    ):
        raise UnknownCompletionHazardError(
            f"durable controller state is missing or malformed for {run_id!r}"
        )
    saved_plan = record.get("plan")
    if not isinstance(saved_plan, dict):
        raise UnknownCompletionHazardError("saved durable plan is missing")
    saved_project = saved_plan.get("project")
    saved_target = saved_plan.get("target")
    command = saved_plan.get("command")
    if (
        not isinstance(saved_project, dict)
        or not isinstance(saved_target, dict)
        or not isinstance(command, list)
    ):
        raise UnknownCompletionHazardError("saved durable plan is malformed")
    config = load_config()
    local_cwd = Path(str(saved_project.get("local_cwd", "")))
    plan = make_run_plan(
        cwd=local_cwd,
        config=config,
        target_name=str(saved_target.get("name", "")),
        command=[str(token) for token in command],
        scope_name=saved_plan.get("write_scope"),
        json_events=False,
        requested_workload=None,
        allow_default_workload=False,
    )
    if plan.as_dict() != saved_plan:
        raise UnknownCompletionHazardError(
            "current project/config no longer matches the exact saved durable plan"
        )
    if plan.target.kind not in {"ssh-posix", "ssh-powershell"}:
        raise UnknownCompletionHazardError("saved durable target transport is unsupported")
    saved_controller = record.get("controller")
    if not isinstance(saved_controller, str) or controller_label() != saved_controller:
        raise UnknownCompletionHazardError(
            "resume is restricted to the controller that created the durable run"
        )
    current = detect_project(Path.cwd(), config)
    if (
        current.project_id != plan.project.project_id
        or current.local_project_root.resolve() != plan.project.local_project_root.resolve()
    ):
        raise UnknownCompletionHazardError(
            "resume must run from the same saved controller project"
        )
    summary_path = rdir / "summary.json"
    return plan, record, rdir, summary_path, config


def cmd_resume(args: argparse.Namespace, reporter: Reporter) -> int:
    plan, record, rdir, summary_path, _config = _load_resume_plan(args.run_id)
    summary = read_json(summary_path)
    if (
        isinstance(summary, dict)
        and summary.get("completion_state") == "complete"
        and summary.get("terminal") is True
    ):
        code = summary.get("exit_code")
        if isinstance(code, int) and not isinstance(code, bool):
            if summary.get("durable_cleanup_complete") is not True:
                try:
                    make_transport(plan.target).durable_cleanup(
                        args.run_id, str(record["resume_token"])
                    )
                except TransportError as exc:
                    reporter.event(
                        "durable_cleanup_deferred", run_id=args.run_id, message=str(exc)
                    )
                else:
                    summary["durable_cleanup_complete"] = True
                    write_json(summary_path, summary)
            reporter.event("durable_already_finalized", run_id=args.run_id, exit_code=code)
            return code
        raise UnknownCompletionHazardError("completed durable summary has invalid exit code")
    if (
        isinstance(summary, dict)
        and summary.get("completion_state") == "finalization_complete"
        and summary.get("terminal") is False
    ):
        code = summary.get("exit_code")
        if isinstance(code, bool) or not isinstance(code, int):
            raise UnknownCompletionHazardError(
                "durable finalization checkpoint has invalid exit code"
            )
        hazard = read_unknown_completion_hazard(plan.project.project_id)
        if hazard is not None:
            if (
                hazard.get("run_id") != args.run_id
                or hazard.get("target") != plan.target.name
            ):
                raise UnknownCompletionHazardError(
                    "durable completion fence changed after finalization checkpoint"
                )
            if not clear_unknown_completion_hazard(
                plan.project.project_id, args.run_id
            ):
                raise UnknownCompletionHazardError(
                    "durable completion fence could not be cleared"
                )
        summary["completion_state"] = "complete"
        summary["terminal"] = True
        write_json(summary_path, summary)
        reporter.event(
            "durable_finalization_recovered", run_id=args.run_id, exit_code=code
        )
        try:
            make_transport(plan.target).durable_cleanup(
                args.run_id, str(record["resume_token"])
            )
        except TransportError as exc:
            reporter.event(
                "durable_cleanup_deferred", run_id=args.run_id, message=str(exc)
            )
        else:
            summary["durable_cleanup_complete"] = True
            write_json(summary_path, summary)
        return code
    hazard = read_unknown_completion_hazard(plan.project.project_id)
    if hazard is None or hazard.get("run_id") != args.run_id or hazard.get("target") != plan.target.name:
        raise UnknownCompletionHazardError(
            "durable completion fence is missing or names a different run"
        )
    transport = make_transport(plan.target)
    lock = ProjectLock(
        plan.project.project_id,
        plan.target.name,
        scope=plan.write_scope,
        run_id=args.run_id,
        adopt_dead_run=True,
    ).acquire()
    try:
        status = transport.durable_status(args.run_id, str(record["resume_token"]))
        _validate_durable_identity(status, record)
        return _poll_durable(
            args=args, reporter=reporter, plan=plan, transport=transport,
            remote_root=str(record["remote_root"]), run_id=args.run_id,
            rdir=rdir, summary_path=summary_path, policy=load_retention(load_config()),
            record=record, initial_status=status, no_wait=bool(args.no_wait),
        )
    finally:
        lock.release()


def cmd_resolve_unknown(args: argparse.Namespace, reporter: Reporter) -> int:
    """Clear this project's local admission hazard after an explicit ended check.

    Resolution is deliberately narrow: it records the operator action in the
    original summary, then removes only the matching hazard. It neither infers
    outputs nor changes a baseline, so the next run performs normal preflight from
    the last positively completed generation.
    """
    if not args.confirmed_ended:  # argparse requires it; retain a direct-call guard.
        raise ValueError("--confirmed-ended is required")
    config = load_config()
    project = detect_project(Path.cwd(), config)
    hazard = read_unknown_completion_hazard(project.project_id)
    if hazard is None:
        raise UnknownCompletionHazardError(
            f"project {project.project_id!r} has no unknown-completion hazard"
        )
    if hazard["run_id"] != args.run_id:
        raise UnknownCompletionHazardError(
            f"project {project.project_id!r} is blocked by run {hazard['run_id']!r}, "
            f"not {args.run_id!r}"
        )

    summary_path = run_dir(args.run_id) / "summary.json"
    summary = read_json(summary_path)
    if not isinstance(summary, dict):
        raise UnknownCompletionHazardError(
            f"cannot record resolution because the original summary is missing at {summary_path}"
        )
    existing = summary.get("unknown_resolution")
    if not isinstance(existing, dict) or existing.get("action") != "confirmed-ended":
        summary["unknown_resolution"] = {
            "action": "confirmed-ended",
            "resolved_at": utc_now_iso(),
        }
        write_json(summary_path, summary)

    if not clear_unknown_completion_hazard(project.project_id, args.run_id):
        raise UnknownCompletionHazardError(
            f"unknown-completion hazard changed while resolving run {args.run_id!r}"
        )
    reporter.event(
        "unknown_completion_resolved",
        project_id=project.project_id,
        target=hazard["target"],
        run_id=args.run_id,
        note="baseline unchanged; next run will perform normal whole-project preflight",
    )
    return EXIT_OK


def _parse_days(value: str | None) -> int | None:
    if value is None:
        return None
    cleaned = value.strip().lower().rstrip("d")
    try:
        return int(cleaned)
    except ValueError as exc:
        raise ValueError(f"invalid --older-than value: {value!r}") from exc


def cmd_clean(args: argparse.Namespace, reporter: Reporter) -> int:
    config = load_config()
    policy = load_retention(config)
    report = prune_state(
        policy,
        older_than_days=_parse_days(args.older_than),
        keep=args.keep,
        dry_run=args.dry_run,
    )
    freed_mb = round(report.bytes_freed / (1024 * 1024), 2)
    if args.json:
        print(json.dumps({
            "dry_run": args.dry_run,
            "runs_deleted": report.runs_deleted,
            "runs_trimmed": report.runs_trimmed,
            "freed_mb": freed_mb,
            "details": report.details,
        }, indent=2, sort_keys=True))
    else:
        prefix = "would remove" if args.dry_run else "removed"
        reporter.event("clean", action=prefix, runs_deleted=report.runs_deleted,
                       runs_trimmed=report.runs_trimmed, freed_mb=freed_mb)
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
