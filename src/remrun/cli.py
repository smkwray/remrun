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
from functools import partial
from pathlib import Path
from typing import NoReturn

from . import __version__
from .bootstrap import (
    BootstrapConfigError,
    bootstrap_inputs_match,
    execute_bootstrap,
    inspect_bootstrap,
    validate_managed_command,
)
from .config import (
    admission_settings, load_config, load_project_config, load_retention, offload_policy,
    offload_threshold, scheduler_config,
)
from .gitsync import HOOK_BEGIN, GitSyncError
from .isolated import (
    CandidateRefusal,
    CommitSourceError,
    CommitWork,
    commit_source,
    discard_unstarted_work,
    finalize_commit_outputs,
    lease_conflicts,
    remove_commit_work,
    seed_commit_tree,
    work_paths,
)
from .job_observation import (
    JobObservation, command_label, controller_label, job_observation_enabled, local_device,
    local_registry_root, observed_job_id, observed_project_id,
)
from .manifest import sha256_file
from .memory_guard import MIB, MemoryAdmissionResult, MemoryReservation
from .models import RunPlan, WorkloadSpec
from .output import Reporter, emit_json_document
from .planner import TRANSFER_MODES, make_run_plan
from .project import (
    ProjectDetectionError, detect_project, find_project_config, project_root_base,
)
from .profile import (
    LOCAL_DEVICE, WorkloadObservation, command_key, device_profile, load_job_costs,
    load_profiles, merge_job_costs, predict_job, profile_project_id,
    recommend_offload, update_profile, update_workload_profile,
)
from .protocol import (
    build_capabilities_document, build_error_document, format_capabilities_human,
)
from .presence import PresencePolicy, load_presence, local_now
from .scheduler import pick_by_load
from .reconcile import estimate_push, postrun_pullback, preflight_reconcile
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
    per_run_completion_fences,
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
    CommandCancelledError, CommandNotStartedError, DurablePrestartError, DurableStateError, TelemetryRequest,
    TransportError, finalize_durable_result, make_transport,
)

KNOWN_COMMANDS = {
    "capabilities", "devices", "doctor", "plan", "run", "status", "logs", "clean", "bench", "sync",
    "git-sync", "runner", "action", "resolve-unknown", "resume", "observe", "cancel", "admit",
}

# Exit codes (see docs/AGENT_OUTPUT_SPEC.md).
EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_CONFLICT = 2
EXIT_TRANSFER = 3
EXIT_INFRA = 4
EXIT_GUARD = 5
EXIT_CANCELLED = 130
# `admit acquire`: the request is recorded and queued, not granted (sysexits EX_TEMPFAIL).
EXIT_ADMISSION_WAITING = 75
# How long a durable wait keeps retrying a failed status read (an SSH drop, a
# sleeping laptop's network coming back) before it stops waiting and reports the
# run as detached. Reads are read-only, so retrying is always safe.
DURABLE_STATUS_RETRY_SECONDS = 300.0
# How long `cancel` waits, after the target confirmed the stop, for the durable
# supervisor to record the command's exit before leaving completion unknown.
DURABLE_CANCEL_SETTLE_SECONDS = 60.0
# How long finalization waits for a pending `cancel` to record whether it stopped the
# job. The target observer's own cancel takes at most its 10 s grace, a 5 s settle
# and the round trips around them.
DURABLE_CANCEL_PENDING_SECONDS = 120.0
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


_JOB_LABEL_LIMIT = 64


def _job_label(value: str) -> str:
    """A short human label for ``fleet jobs``; spaces are stored as underscores."""
    text = value.strip()
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:@+- ")
    if not text or len(text) > _JOB_LABEL_LIMIT or not set(text) <= allowed:
        raise argparse.ArgumentTypeError(
            f"must be 1-{_JOB_LABEL_LIMIT} letters, digits, spaces or . _ : @ + -"
        )
    return text


def _positive_mib(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer MiB value") from exc
    if result <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer MiB value")
    return result


def _wait_seconds(value: str) -> float:
    try:
        result = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a non-negative number of seconds") from exc
    if not math.isfinite(result) or result < 0:
        raise argparse.ArgumentTypeError("must be a non-negative number of seconds")
    return result


def _declared_admission(args: argparse.Namespace) -> dict | None:
    """A run's admission declaration as request fields, or None when it declares nothing."""
    get = partial(getattr, args)
    declared = (get("cpu", None) is not None or get("ram_mib", None) is not None
                or get("key", None) or get("device_bound", False) or get("critical", False))
    if not declared:
        stray = [flag for flag, name in (("--thread", "thread"),
                                         ("--logical-thread", "logical_thread"),
                                         ("--admission-wait", "admission_wait"))
                 if get(name, None) is not None]
        if stray:
            raise ValueError(
                f"{', '.join(stray)} need a resource declaration "
                "(--cpu, --ram-gib, --key, --device-bound or --critical)"
            )
        return None
    return {
        "cpu_cores": get("cpu", None) or 0,
        "ram_mib": get("ram_mib", None) or 0,
        "keys": tuple(get("key", None) or ()),
        "device_bound": bool(get("device_bound", False)),
        "critical": bool(get("critical", False)),
        "owner_thread": (get("thread", None) or os.environ.get("T3CODE_THREAD_ID")
                         or f"{controller_label()}/pid-{os.getpid()}"),
        "logical_thread": get("logical_thread", None) or "",
    }


def _ram_gib_as_mib(value: str) -> int:
    try:
        result = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a non-negative number of GiB") from exc
    if not math.isfinite(result) or result < 0:
        raise argparse.ArgumentTypeError("must be a non-negative number of GiB")
    return math.ceil(result * 1024)


def _add_transfer_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--transfer", choices=TRANSFER_MODES,
        help="safe: reconcile the project tree before and after the command; none: run in "
             "the target's existing checkout and transfer nothing; commit: run a clean "
             "HEAD in a private tree on the target and return only --output paths "
             "(default: [transfer] mode)",
    )
    p.add_argument(
        "--verify", action="append", default=[], metavar="PATH",
        help="with --transfer none, refuse to run unless the target's copy of this "
             "project-relative file has the same SHA-256 as the local one (repeatable)",
    )
    p.add_argument(
        "--output", action="append", default=[], metavar="PATH",
        help="with --transfer commit, a project-relative file, or directory written dir/, "
             "that the command produces and remrun pulls back (repeatable, required)",
    )
    p.add_argument(
        "--input", action="append", default=[], metavar="PATH",
        help="with --transfer commit, a working-tree file copied over the commit before "
             "the command runs, recorded with its SHA-256 (repeatable)",
    )


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
    if args.command_name in {"run", "plan", "bench", "observe"}:
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
        if args.command_name == "observe":
            return cmd_observe(args, reporter)
        if args.command_name == "resume":
            return cmd_resume(args, reporter)
        if args.command_name == "cancel":
            return cmd_cancel(args, reporter)
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
        if args.command_name == "admit":
            return cmd_admit(args, reporter)
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
    _add_transfer_arguments(p)
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
    p.add_argument("--label", type=_job_label,
                   help="short label shown by `fleet jobs`, e.g. 'regen 2019'")
    _add_transfer_arguments(p)
    p.add_argument("--dry-run", action="store_true", help="Print plan and do not execute")
    p.add_argument("--no-pullback", action="store_true", help="Skip post-run pullback")
    p.add_argument("--no-telemetry", action="store_true", help="Skip resource telemetry")
    p.add_argument(
        "--allow-large-push", action="store_true",
        help="skip the [transfer] max_push_files / max_push_mb preflight guard "
             "(for a legitimate first sync of a large project)",
    )
    p.add_argument(
        "--memory-limit-mib",
        type=_positive_mib,
        metavar="N",
        help="hard process-tree memory limit for this run; target safety limits still apply",
    )
    p.add_argument(
        "--bypass-memory-guard",
        action="store_true",
        help="explicit-target owner override: skip memory admission, reserve, and command guard",
    )
    p.add_argument(
        "--durable", action="store_true",
        help="opt into a target-supervised run resumable by this controller; "
             "with --auto, selection freezes before launch",
    )
    p.add_argument(
        "--wait-lock", type=_wait_seconds, default=0.0, metavar="SECONDS",
        help="when another running remrun holds this project's lock, wait up to "
             "SECONDS for it instead of failing at once (default 0)",
    )
    # Declaring any resource makes the run take a turn from shared admission first.
    p.add_argument("--cpu", type=int, metavar="N",
                   help="admission: the run holds N cores of its target")
    p.add_argument("--ram-gib", dest="ram_mib", type=_ram_gib_as_mib, metavar="G",
                   help="admission: the run holds G GiB of its target's RAM")
    p.add_argument("--key", action="append", default=[], metavar="K",
                   help="admission: a capacity-one resource key the run holds, e.g. "
                        "pool/gpu (repeatable)")
    p.add_argument("--device-bound", action="store_true",
                   help="admission: the run needs something only its target has")
    p.add_argument("--critical", action="store_true",
                   help="admission: the run is on a blocked critical path")
    p.add_argument("--thread", help="admission: the owning agent thread "
                                    "(default: $T3CODE_THREAD_ID)")
    p.add_argument("--logical-thread",
                   help="admission: the logical work stream (default: the owning thread)")
    p.add_argument("--admission-wait", type=_wait_seconds, metavar="SECONDS",
                   help="admission: give up after SECONDS without a grant "
                        "(default: [admission] run_wait_seconds)")

    p = sub.add_parser(
        "observe",
        help="Run a command on this machine, registered so `fleet jobs` can see it",
    )
    p.add_argument("--label", type=_job_label,
                   help="short label shown by `fleet jobs`, e.g. 'regen 2019'")

    p = sub.add_parser("resume", help="Resume an exact controller-owned durable run")
    p.add_argument("run_id", help="the exact durable run ID")
    p.add_argument("--no-wait", action="store_true", help="report a running state and return")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser(
        "cancel",
        help="Stop an exact controller-owned durable run, then finalize it like resume",
    )
    p.add_argument("run_id", help="the exact durable run ID")
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

    p = sub.add_parser(
        "admit", help="Queue for, hold, and release shared compute on a device",
    )
    admit_sub = p.add_subparsers(dest="admit_command", required=True)
    for action, text in (
        ("acquire", "Queue a request, or replay one; exit 0 granted, 75 still waiting"),
        ("withdraw", "End your own request whatever its acquire did, even before it lands"),
    ):
        ap = admit_sub.add_parser(action, help=text)
        ap.add_argument("--device", required=True)
        ap.add_argument("--project", required=True, help="root project the work belongs to")
        ap.add_argument("--thread", required=True, help="agent thread that owns the request")
        ap.add_argument("--logical-thread", default="",
                        help="logical work stream the request belongs to (default: --thread)")
        ap.add_argument("--cpu", type=int, default=0, metavar="N", help="cores to hold")
        ap.add_argument("--ram-gib", dest="ram_mib", type=_ram_gib_as_mib, default=0,
                        metavar="G", help="RAM to hold, in GiB")
        ap.add_argument("--key", action="append", default=[], metavar="K",
                        help="capacity-one resource key to hold, e.g. pool/gpu (repeatable)")
        ap.add_argument("--device-bound", action="store_true",
                        help="the work needs something only this device has")
        ap.add_argument("--critical", action="store_true",
                        help="the work is on a blocked critical path")
        ap.add_argument("--label", default="")
        ap.add_argument("--json", action="store_true")
    ap.add_argument("--request-id", required=True, help="the id the acquire used")
    ap = admit_sub.choices["acquire"]
    ap.add_argument("--request-id", help="idempotency key; default: a new random id")
    ap.add_argument("--wait", type=_wait_seconds, default=0.0, metavar="SECONDS",
                    help="keep the request alive and retry until granted or SECONDS pass")
    for action, text in (("status", "Show one request"), ("release", "End a grant"),
                         ("cancel", "Withdraw a waiting request")):
        ap = admit_sub.add_parser(action, help=text)
        ap.add_argument("request_id")
        ap.add_argument("--json", action="store_true")
    ap = admit_sub.add_parser("list", help="Show granted and waiting requests in queue order")
    ap.add_argument("--device")
    ap.add_argument("--json", action="store_true")

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


def cmd_admit(args: argparse.Namespace, reporter: Reporter) -> int:
    """The deployment's admission ledger; see docs/SCHEDULING_AND_TELEMETRY.md."""
    from .admission import AdmissionClient, AdmissionError, AdmissionRequest, admit_first

    config = load_config()
    client = AdmissionClient.for_config(config)
    if args.admit_command == "list":
        documents = client.list_requests(args.device)
        if args.json:
            emit_json_document(
                {"schema": "remrun.admission-list", "version": 1, "requests": documents}
            )
        else:
            for document in documents:
                print(_admission_line(document))
        return EXIT_OK
    if args.admit_command in {"acquire", "withdraw"}:
        if args.device not in config.devices:
            raise AdmissionError(f"unknown device {args.device!r}")
        request = AdmissionRequest(
            request_id=args.request_id or secrets.token_hex(16), device=args.device,
            root_project=args.project, owner_thread=args.thread,
            logical_thread=args.logical_thread, label=args.label, cpu_cores=args.cpu,
            ram_mib=args.ram_mib, keys=tuple(args.key), device_bound=args.device_bound,
            critical=args.critical,
        )
    if args.admit_command == "acquire":
        document = admit_first(
            client, [request], wait_seconds=args.wait, keep_waiting=True,
            report=lambda doc: reporter.event("admission_waiting", **_admission_place(doc)),
            unresolved=lambda ids: _report_unresolved(reporter, client, ids),
        )
    elif args.admit_command == "withdraw":
        document = client.withdraw(request)
    else:
        document = getattr(client, args.admit_command)(args.request_id)
    if args.json:
        emit_json_document(document)
    else:
        print(_admission_line(document))
    if args.admit_command != "acquire" or document["state"] == "granted":
        return EXIT_OK
    if document["state"] == "waiting":
        return EXIT_ADMISSION_WAITING
    reporter.event(
        "admission_ended", request_id=document["request_id"], state=document["state"],
        reason=document["end_reason"], message="this request id is spent; acquire with a new one",
    )
    return EXIT_INTERNAL


def _report_unresolved(reporter: Reporter, client, request_ids: list[str]) -> None:  # noqa: ANN001
    """Name requests whose withdrawal could not be confirmed, and where they live."""
    reporter.event(
        "admission_unresolved", request_ids=request_ids, authority=client.authority,
        guidance="each may still hold a place or a grant; once that ledger answers, check "
                 "`remrun admit status ID` and release a grant or cancel a waiting request",
    )


def _admission_place(document: dict) -> dict:
    """Where a waiting request stands, and why the head of its queue is blocked."""
    blocked = (document.get("promotion") or {}).get("blocked") or {}
    return {
        "request_id": document["request_id"], "device": document["device"],
        "position": document["position"], "ahead": document["ahead"],
        "head_blocked": blocked.get("reason"),
    }


def _admission_line(document: dict) -> str:
    fields = [document["request_id"], document["state"], document["device"],
              document["root_project"]]
    if document["position"] is not None:
        fields.append(f"position={document['position']}")
        fields.append(f"ahead={','.join(document['ahead']) or '-'}")
    blocked = (document.get("promotion") or {}).get("blocked")
    if document["state"] == "waiting" and blocked:
        fields.append(f"head_blocked={blocked['reason']}")
    if document["end_reason"]:
        fields.append(f"reason={document['end_reason']}")
    return "\t".join(fields)


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
        transfer_mode=args.transfer,
        verify=args.verify,
        outputs=args.output,
        inputs=args.input,
    )
    candidates = None
    target_reason = None
    presence = None
    untransferred = plan.transfer_mode != "safe"
    # Measuring the push needs the target's manifest, a round-trip, so only with --probe.
    push: dict = (
        {"status": "not_applicable",
         "detail": f"--transfer {plan.transfer_mode} reconciles no project tree"}
        if untransferred else
        {"status": "not_measured", "max_files": plan.max_push_files,
         "max_bytes": plan.max_push_bytes, "hint": "add --probe to measure it"}
    )
    if args.probe or args.check_git:
        candidates = []
        presence = load_presence(config)
        selection = _resolve_targets(
            plan, target_name, scheduler_config(config), reporter,
            _job_prediction(config, plan), diagnostics=candidates,
            check_git=args.check_git, emit_events=False, presence=presence)
        if selection:
            plan = replace(plan, target=selection[0][0])
            target_reason = selection[0][3]
            if plan.bootstrap is not None:
                try:
                    bootstrap_transport = selection[0][1]
                    bootstrap_remote_root = bootstrap_transport.remote_project_path(
                        plan.project
                    )
                    bootstrap_runenv = resolve_run_env(
                        device=plan.target,
                        project=plan.project,
                        project_config=plan.project_config,
                    )
                    plan = replace(
                        plan,
                        bootstrap=inspect_bootstrap(
                            bootstrap_transport,
                            device=plan.target,
                            project=plan.project,
                            plan=plan.bootstrap,
                            remote_root=bootstrap_remote_root,
                            runenv=bootstrap_runenv,
                        ),
                    )
                except (OSError, TransportError, ValueError) as exc:
                    plan = replace(
                        plan,
                        bootstrap=replace(
                            plan.bootstrap, status="unsupported", detail=str(exc)
                        ),
                    )
            if not untransferred:
                push = _plan_push_estimate(plan, selection[0][1])
        else:
            target_reason = "unreachable"
            if not untransferred:
                push = {"status": "unavailable", "max_files": plan.max_push_files,
                        "max_bytes": plan.max_push_bytes, "error": "target unreachable"}
    if args.json:
        payload = plan.as_dict()
        if candidates is not None:
            payload["target_reason"] = target_reason
            payload["candidates_probed"] = candidates
            payload["owner_presence"] = presence.as_dict()
        payload["push"] = push
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        reporter.event("project", project_id=plan.project.project_id, relative_cwd=plan.project.relative_cwd)
        if target_reason is None:
            reporter.event("target", name=plan.target.name, kind=plan.target.kind)
        else:
            reporter.event("target", name=plan.target.name, kind=plan.target.kind,
                           reason=target_reason)
        reporter.event("command", argv=plan.command)
        reporter.event("transfer_mode", mode=plan.transfer_mode, **_mode_paths(plan))
        reporter.event("write_scope", name=plan.write_scope or "project",
                       paths=plan.write_scope_paths)
        if plan.workload is not None:
            reporter.event("workload", **plan.workload.as_dict())
        reporter.event("active_surface", excludes=len(plan.excludes), hash_below_bytes=plan.hash_below_bytes)
        if plan.bootstrap is not None:
            reporter.event("bootstrap", **plan.bootstrap.as_dict())
        reporter.event("push_estimate", **{
            k: v for k, v in push.items()
            if k not in {"message", "top_by_files", "top_by_bytes"}
        })
        if push.get("message"):
            reporter.event("push_would_refuse", message=push["message"])
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


def _plan_push_estimate(plan: RunPlan, transport) -> dict:
    """What a run's preflight would push to this target now, and whether the push
    guard would refuse it. Read-only; a failure is reported, never guessed."""
    bootstrap_inputs = (
        plan.bootstrap.lock_inputs
        if plan.bootstrap is not None and plan.bootstrap.status == "bootstrap-needed"
        else ()
    )
    prev_local, prev_remote = read_baseline(plan.target.name, plan.project.project_id)
    try:
        size = estimate_push(
            transport=transport,
            local_root=plan.project.local_project_root,
            remote_root=transport.remote_project_path(plan.project),
            excludes=plan.excludes,
            hash_below_bytes=plan.hash_below_bytes,
            prev_local=prev_local,
            prev_remote=prev_remote,
            max_push_files=plan.max_push_files,
            max_push_bytes=plan.max_push_bytes,
            include_paths=bootstrap_inputs,
        )
    except (OSError, TransportError) as exc:
        return {"status": "unavailable", "max_files": plan.max_push_files,
                "max_bytes": plan.max_push_bytes, "error": str(exc)}
    result = {"status": "measured", **size.as_dict()}
    if size.refused:
        result["message"] = size.message()
    return result


def _resolve_targets(plan: RunPlan, target_name: str | None, sched: dict, reporter: Reporter,
                     prediction: dict | None = None, *, diagnostics: list[dict] | None = None,
                     check_git: bool = False, emit_events: bool = True,
                     presence: PresencePolicy | None = None):
    """Probe and rank once for both ``run`` and opt-in, read-only ``plan`` diagnostics.

    Returns ``[(device, transport, probe, reason), ...]``. Diagnostics only record
    observations from this path; they never choose or mutate a target.

    A heavy job (predicted at least ``heavy_job_seconds``, or never measured) avoids a
    device inside an owner presence window whenever another reachable candidate exists;
    such a device stays a last resort, chosen with reason ``presence-fallback``.
    """
    entries: dict[str, dict] = {}
    pred_dur = (prediction or {}).get("dur_s")
    heavy = pred_dur is None or pred_dur >= float(sched.get("heavy_job_seconds", 300))
    now = local_now()
    owner_until: dict[str, str] = {}
    if presence is not None and heavy:
        for device in plan.candidates:
            end = presence.window_end(device.name, now)
            if end is not None:
                owner_until[device.name] = end.isoformat(timespec="minutes")
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

    if presence is not None and emit_events and heavy and (
            owner_until or presence.status in {"malformed", "unavailable"}):
        reporter.event("owner_presence", **presence.as_dict(), heavy=heavy,
                       predicted_dur_s=pred_dur, evaluated_at=now.isoformat(timespec="minutes"),
                       devices_in_window=owner_until or None)
    for name, until in owner_until.items():
        if name in entries:
            entries[name]["owner_window_until"] = until

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

    # Owner-present devices leave the ranking while any other candidate is reachable.
    clear = [x for x in reachable if x[0].name not in owner_until]
    pool = clear or reachable
    ranked = [(d, busy) for (d, _t, _pr, busy) in pool]
    chosen_device, balance_reason = pick_by_load(ranked, sched)
    device, transport, probe, _busy = next(x for x in reachable if x[0] is chosen_device)
    # Presence is named as the reason only when it changed the pick; otherwise the
    # reason is what ranking every reachable candidate would have said.
    unfiltered, reason = (
        pick_by_load([(d, busy) for (d, _t, _pr, busy) in reachable], sched)
        if pool is not reachable else (device, balance_reason)
    )
    if not clear:
        reason = "presence-fallback"
    elif unfiltered is not device:
        reason = "presence-avoided"
    elif reason == "auto" and plan.candidates and device is not plan.candidates[0]:
        reason = "auto-failover"  # preferred candidate was unreachable
    if device.name in entries:
        entries[device.name]["recommended"] = True
    # pick_by_load names the winner; the rest stay in preference order behind it as
    # fallbacks. Their reason is "auto-failover": reaching them means the winner
    # failed for a candidate-local reason. Owner-present devices come last.
    rest = [(d, t, pr, "auto-failover") for (d, t, pr, _b) in clear if d is not device]
    rest += [(d, t, pr, "presence-fallback") for (d, t, pr, _b) in reachable
             if d is not device and d.name in owner_until]
    return [(device, transport, probe, reason), *rest]


def _mode_paths(plan: RunPlan) -> dict[str, list[str]]:
    """The per-mode path lists a plan carries, for events."""
    if plan.transfer_mode == "none":
        return {"verify": list(plan.verify)}
    if plan.transfer_mode == "commit":
        return {"outputs": list(plan.outputs), "inputs": list(plan.inputs)}
    return {}


def _kept_lease_guidance(run_id: str, plan: RunPlan, tree: object, why: str) -> str:
    """How to end the --output lease a commit run keeps after its command ended.

    It keeps the lease while its outputs are not all in the project: an overlapping run
    would race the undelivered files, and the lease keeps the retained tree that holds
    them from being pruned.
    """
    return (
        f"{why}; its outputs stay in the retained private tree {tree} on "
        f"{plan.target.name}, and this run keeps its --output lease. Copy what you need "
        f"from there, then run `remrun resolve-unknown {run_id} --confirmed-ended` to "
        "release the lease"
    )


def _undelivered_outputs_why(command_exit_code: object, undelivered: list[str]) -> str:
    listed = ", ".join(undelivered[:5]) + (
        f" and {len(undelivered) - 5} more" if len(undelivered) > 5 else ""
    )
    return (
        f"the command ended (exit {command_exit_code}) but {len(undelivered)} path(s) at "
        f"or under --output were not cleanly delivered ({listed}): symlinks or special "
        "files remrun does not return, or output conflicts (a case-fold collision saves "
        "neither copy); the retained private tree still holds them"
    )


def _unknown_completion_guidance(hazard: dict[str, object], *, per_run: bool = False) -> str:
    run_id = str(hazard["run_id"])
    if per_run:
        # An untransferred run fences only itself; nothing was pushed and nothing will be
        # pulled back, so finalizing it changes no project files. A commit run's fence
        # also leases its declared outputs, which resume pulls back.
        outputs = hazard.get("outputs")
        first = (
            f"run `remrun resume {run_id}` from this project: it reattaches, waits and "
            "finalizes. Only if resume reports the target's durable state is missing or "
            "failed, use"
            if _durable_record_path(run_dir(run_id)).is_file()
            else "use"
        )
        kind, blocks = (
            ("commit run", f"holds its --output lease ({', '.join(map(str, outputs))}) "
                           "but blocks no other run of this project")
            if isinstance(outputs, list) else
            ("untransferred run", "blocks only that run, not other runs of this project")
        )
        return (
            f"completion of {kind} {run_id} is not confirmed; its fence {blocks}. To "
            f"clear it, {first} a read-only process/artifact probe to prove the command "
            f"ended, then run `remrun resolve-unknown {run_id} --confirmed-ended`"
        )
    if _durable_record_path(run_dir(run_id)).is_file():
        # A durable run's target supervisor may still own the job and its result.
        # Resuming finalizes it and pulls outputs back; resolving it as ended would
        # discard that finalization.
        return (
            f"project is blocked by durable run {run_id}, whose completion this controller "
            "has not yet confirmed; do not retry this project. First run "
            f"`remrun resume {run_id}` from this project: it reattaches, waits, finalizes "
            "and pulls outputs back. Only if resume reports the target's durable state is "
            "missing or failed, use a read-only process/artifact probe to prove the command "
            f"ended, then run `remrun resolve-unknown {run_id} --confirmed-ended`"
        )
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
    *,
    wait_for_live_owner: bool = False,
) -> dict[str, object] | None:
    """Report and return a hazard that blocks this project.

    With ``wait_for_live_owner``, a hazard owned by a run that still holds this
    project's lock with a running pid is the in-progress run's own fence: the
    caller waits on the lock and re-checks the hazard after acquiring it.
    """
    hazard = read_unknown_completion_hazard(project_id)
    if hazard is None:
        return None
    if wait_for_live_owner and hazard["run_id"] in ProjectLock(
        project_id, "", scope=None
    ).live_holder_run_ids():
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


def _local_verify_digests(plan: RunPlan) -> dict[str, str]:
    """SHA-256 of each ``--verify`` file in this controller's tree, taken once per run."""
    digests: dict[str, str] = {}
    for rel in plan.verify:
        path = plan.project.local_project_root.joinpath(*rel.split("/"))
        if not path.is_file():
            raise ValueError(f"--verify {rel}: no such file in this project")
        digests[rel] = sha256_file(path)
    return digests


def _verify_untransferred_target(
    transport, plan: RunPlan, remote_root: str, remote_cwd: str, local: dict[str, str],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Check an untransferred run's target before dispatch, writing nothing.

    Returns ``(verified, conflicts)``. The checkout, including the run's working
    directory, must already exist, and every ``--verify`` file must hash the same as the
    local copy. A digest the target cannot produce refuses too: equality is proved,
    never assumed.
    """
    if not transport.remote_path_exists(remote_cwd):
        return [], [{
            "path": plan.project.relative_cwd or ".",
            "state": "target-checkout-missing",
            "reason": f"{remote_cwd} does not exist on {plan.target.name}; --transfer none "
                      "runs only in the target's existing checkout",
        }]
    verified: list[dict[str, str]] = []
    conflicts: list[dict[str, str]] = []
    for rel, mine in local.items():
        try:
            theirs = transport.hash_file(transport.remote_join(remote_root, rel))
        except (TransportError, NotImplementedError, OSError, ValueError) as exc:
            conflicts.append({"path": rel, "state": "verify-unavailable",
                              "reason": f"target hash failed: {type(exc).__name__}: {exc}"})
            continue
        digest = theirs.strip().lower() if isinstance(theirs, str) else ""
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            conflicts.append({"path": rel, "state": "verify-unavailable",
                              "reason": "target returned no valid SHA-256"})
        elif digest != mine:
            conflicts.append({"path": rel, "state": "verify-mismatch",
                              "reason": f"local sha256 {mine}, target sha256 {digest}"})
        else:
            verified.append({"path": rel, "sha256": mine})
    return verified, conflicts


def _refuse_candidate(
    *,
    reporter: Reporter,
    plan: RunPlan,
    run_id: str,
    summary_path: Path,
    started_at: str,
    phase: str,
    conflicts: list[dict[str, str]],
    verified: list[dict[str, str]],
    retryable: bool = True,
) -> NoReturn:
    """Record why a run cannot start on this candidate, then skip it.

    Nothing was written to either tree and no fence exists yet, so ``--auto`` may try
    the next candidate, unless ``retryable`` is false (the refusal holds on every
    target).
    """
    cdir = conflict_dir(run_id)
    write_json(cdir / "conflicts.json", {
        "run_id": run_id,
        "project_id": plan.project.project_id,
        "target": plan.target.name,
        "conflicts": conflicts,
    })
    for item in conflicts:
        reporter.event("conflict", **item)
    reporter.event("action", value="not running command")
    reporter.event("conflict_state", path=str(cdir))
    write_json(summary_path, {
        "run_id": run_id,
        "project_id": plan.project.project_id,
        "target": plan.target.name,
        "command": plan.command,
        "started_at": started_at,
        "ended_at": utc_now_iso(),
        "exit_code": EXIT_CONFLICT,
        "phase": phase,
        "completion_state": "not_started",
        "command_started": False,
        "terminal": True,
        "conflicts": len(conflicts),
        "conflict_state": str(cdir),
        **({"verify": verified} if plan.transfer_mode == "none" else {}),
        "plan": plan.as_dict(),
    })
    first = conflicts[0]
    raise _PreflightConflict(
        exit_code=EXIT_CONFLICT,
        conflict_dir=cdir,
        detail=f"{len(conflicts)} refused path(s); first: {first['path']} ({first['state']})",
        retryable=retryable,
    )


def _publish_completion_fence(
    *,
    reporter: Reporter,
    plan: RunPlan,
    run_id: str,
    summary_path: Path,
    started_at: str,
    verified: list[dict[str, str]],
) -> dict[str, object]:
    """Write the fence that marks this run's completion unknown, just before dispatch.

    An ordinary run holds the project lock, so it writes the project-wide fence. An
    untransferred run takes no project lock: it checks for an ordinary run on its
    target and publishes its own fence in one step under the lock table's guard, which
    every project-lock acquisition also holds. Both orders are then safe. Lock first:
    the check below sees the holder and refuses this candidate. Fence first: the
    ordinary run takes its lock afterwards, and its per-run-fence check, made while it
    holds that lock, sees this fence and refuses. An ordinary durable run that detached
    wrote its project-wide fence before it released its lock, so the fence read below,
    which follows the lock read, sees it.
    """
    project_id = plan.project.project_id
    if plan.transfer_mode == "safe":
        return write_unknown_completion_hazard(project_id, plan.target.name, run_id)
    table = ProjectLock(project_id, plan.target.name)
    if plan.transfer_mode == "commit":
        # A commit run never touches the target's checkout, so an ordinary run working
        # there is no obstacle. Its fence is its lease on the declared outputs: checked
        # against every other live commit run of the project and taken in one step under
        # the guard. An overlap holds on every target, so it is not candidate-local.
        with table.guarded():
            leased = lease_conflicts(project_id, plan.outputs)
            if not leased and read_unknown_completion_hazard(project_id) is None:
                return write_unknown_completion_hazard(
                    project_id, plan.target.name, run_id, per_run=True,
                    outputs=plan.outputs,
                )
        if leased:
            _refuse_candidate(
                reporter=reporter, plan=plan, run_id=run_id, summary_path=summary_path,
                started_at=started_at, phase="dispatch", verified=verified,
                conflicts=leased, retryable=False,
            )
        hazard = _report_unknown_completion_hazard(project_id, reporter)
        raise UnknownCompletionHazardError(
            _unknown_completion_guidance(hazard) if hazard is not None
            else "an ordinary run's completion fence changed while this run was starting"
        )
    with table.guarded():
        busy = table.busy_on_target()
        if not busy and read_unknown_completion_hazard(project_id) is None:
            return write_unknown_completion_hazard(
                project_id, plan.target.name, run_id, per_run=True
            )
    if busy:
        _refuse_candidate(
            reporter=reporter, plan=plan, run_id=run_id, summary_path=summary_path,
            started_at=started_at, phase="dispatch", verified=verified,
            conflicts=[{
                "path": plan.project.relative_cwd or ".",
                "state": "target-busy-ordinary",
                "reason": (
                    f"an ordinary run of this project may be reconciling or running in "
                    f"{plan.target.name}'s checkout ({busy[0]}); wait for it or choose "
                    "another target"
                ),
            }],
        )
    # An ordinary run's fence appeared after this run's admission check; it blocks
    # every target, so this is not a candidate-local refusal.
    hazard = _report_unknown_completion_hazard(project_id, reporter)
    raise UnknownCompletionHazardError(
        _unknown_completion_guidance(hazard) if hazard is not None
        else "an ordinary run's completion fence changed while this run was starting"
    )


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
    held: dict = {}
    try:
        return _cmd_run(args, reporter, held)
    finally:
        # Whatever the run did, its grant goes once remrun knows its command is not running.
        if "admission" in held:
            _release_admission(held["admission"], held["project_id"],
                               sorted(held["run_ids"]), reporter, held["config"])


def _possibly_running(
    project_id: str, run_ids: list[str], *, confirmed_ended: bool = False,
) -> str | None:
    """The first run whose command remrun cannot prove has stopped, else None.

    An unknown-completion fence naming the run, or a summary left ``unknown`` or
    ``detached``, means the command may still be running; so does anything unreadable.
    ``confirmed_ended`` trusts an operator's `resolve-unknown --confirmed-ended` over the
    summary, never over a fence.
    """
    for run_id in run_ids:
        try:
            project_fence = read_unknown_completion_hazard(project_id) or {}
            if (project_fence.get("run_id") == run_id
                    or read_unknown_completion_hazard(project_id, run_id=run_id) is not None):
                return run_id
            summary = None if confirmed_ended else read_json(run_dir(run_id) / "summary.json")
        except (UnknownCompletionHazardError, OSError, ValueError):
            return run_id
        if summary is not None and (
            not isinstance(summary, dict)
            or summary.get("completion_state") in {"unknown", "detached"}
        ):
            return run_id
    return None


def _release_admission(
    admission: dict,
    project_id: str,
    run_ids: list[str],
    reporter: Reporter,
    config,  # noqa: ANN001
    *,
    confirmed_ended: bool = False,
) -> None:
    """Release a run's grant unless its command may still run; never raise."""
    request_id = admission.get("request_id")
    try:
        running = _possibly_running(project_id, run_ids, confirmed_ended=confirmed_ended)
        if running is not None:
            reporter.event(
                "admission_held", request_id=request_id, run_id=running,
                note=f"the grant is released when `remrun resume {running}` finalizes the run "
                     f"or `remrun resolve-unknown {running} --confirmed-ended` records its end",
            )
            return
        from .admission import AdmissionClient

        AdmissionClient(config, admission.get("coordinator")).release(str(request_id))
    except Exception as exc:  # noqa: BLE001 - never mask the run's own outcome
        reporter.event(
            "admission_release_failed", request_id=request_id, message=str(exc),
            guidance=f"once the run is known to have ended, `remrun admit release {request_id}`",
        )
        return
    reporter.event("admission_released", request_id=request_id)


def _release_after_recovery(
    plan: RunPlan, run_id: str, reporter: Reporter, config,  # noqa: ANN001
) -> None:
    """Release a recovered run's grant, but only on affirmative evidence that it ended.

    Callers have already validated the saved run and own its finalization. The summary
    must still be terminal and record that the command completed or never started; a
    missing, unreadable or nonterminal summary, or a fence naming the run, keeps it held.
    A nonterminal ``not_started`` receipt belongs to an owner that may still launch.
    """
    if plan.admission is None:
        return
    try:
        summary = read_json(run_dir(run_id) / "summary.json")
    except (OSError, ValueError):
        summary = None
    if not (isinstance(summary, dict) and summary.get("terminal") is True
            and summary.get("completion_state") in {
                "complete", "not_started", "finalization_failed"}):
        reporter.event(
            "admission_held", request_id=plan.admission.get("request_id"), run_id=run_id,
            note="the run's summary does not yet show that its command ended",
        )
        return
    _release_admission(plan.admission, plan.project.project_id, [run_id], reporter, config)


def _release_confirmed_ended(run_id: str, reporter: Reporter) -> None:
    """After `resolve-unknown --confirmed-ended`: release the grant the run recorded."""
    rdir = run_dir(run_id)
    try:
        record = read_json(_durable_record_path(rdir)) or read_json(rdir / "summary.json")
    except (OSError, ValueError):
        record = None
    plan_doc = record.get("plan") if isinstance(record, dict) else None
    admission = plan_doc.get("admission") if isinstance(plan_doc, dict) else None
    project = plan_doc.get("project") if isinstance(plan_doc, dict) else None
    if not isinstance(admission, dict) or not isinstance(project, dict):
        return
    try:
        config = load_config()
    except Exception as exc:  # noqa: BLE001
        reporter.event("admission_release_failed", request_id=admission.get("request_id"),
                       message=str(exc))
        return
    _release_admission(admission, str(project.get("project_id")), [run_id], reporter, config,
                       confirmed_ended=True)


def _cmd_run(args: argparse.Namespace, reporter: Reporter, held: dict) -> int:
    config = load_config()
    admission_fields = _declared_admission(args)
    policy = load_retention(config)
    telemetry_default = bool(config.defaults.get("telemetry", {}).get("enabled", True))
    command = normalize_cmd(args.cmd)
    target_name = "auto" if args.auto else args.target
    bypass_memory_guard = bool(getattr(args, "bypass_memory_guard", False))
    if bypass_memory_guard and target_name == "auto":
        raise ValueError("--bypass-memory-guard requires an explicitly named target")
    if bypass_memory_guard and getattr(args, "memory_limit_mib", None) is not None:
        raise ValueError(
            "--bypass-memory-guard cannot be combined with --memory-limit-mib"
        )
    plan = make_run_plan(
        cwd=Path.cwd(),
        config=config,
        target_name=target_name,
        command=command,
        scope_name=getattr(args, "scope", None),
        json_events=args.json,
        requested_workload=getattr(args, "workload", None),
        allow_default_workload=not getattr(args, "suppress_default_workload", False),
        transfer_mode=getattr(args, "transfer", None),
        verify=getattr(args, "verify", None) or (),
        outputs=getattr(args, "output", None) or (),
        inputs=getattr(args, "input", None) or (),
    )
    # An untransferred or commit run never reconciles the target's checkout, so it takes
    # no project lock and fences only itself (a commit run's fence also leases its
    # outputs); it still refuses while an ordinary run's project-wide fence stands.
    untransferred = plan.transfer_mode != "safe"
    if plan.transfer_mode == "commit" and getattr(args, "no_pullback", False):
        raise ValueError("--transfer commit returns its outputs by pullback; drop --no-pullback")

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
        plan.project.project_id, reporter,
        wait_for_live_owner=getattr(args, "wait_lock", 0.0) > 0 and not untransferred,
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

    verify_digests = _local_verify_digests(plan)
    source = None
    if plan.transfer_mode == "commit":
        # Both refusals hold on every target, so they come before any is chosen.
        try:
            source = commit_source(plan)
        except CommitSourceError as exc:
            for path in exc.paths[:200]:
                reporter.event("conflict", path=path, state="source-dirty",
                               reason="tracked file differs from HEAD")
            reporter.event("source_refused", message=str(exc), paths=len(exc.paths))
            return EXIT_CONFLICT
        leased = lease_conflicts(plan.project.project_id, plan.outputs)
        for item in leased:
            reporter.event("conflict", **item)
        if leased:
            reporter.event("action", value="not running command")
            return EXIT_CONFLICT
        reporter.event("source", commit=source.commit, inputs=list(source.inputs))
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
        plan, target_name, sched, reporter, placement_prediction,
        presence=load_presence(config),
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

    if admission_fields is not None:
        # A declared run takes its turn before anything that could start user code, keeps
        # the candidate order, and is then bound to the device that granted it.
        from .admission import (
            AdmissionClient, AdmissionError, AdmissionRequest, AdmissionTimeout,
            AdmissionUnavailable, admit_first,
        )

        settings = admission_settings(config)
        wait = getattr(args, "admission_wait", None)
        wait = settings["run_wait_seconds"] if wait is None else wait
        client = AdmissionClient(config, settings["coordinator"])
        token = secrets.token_hex(8)
        label = getattr(args, "label", None) or command_label(plan.command)
        requests = [
            AdmissionRequest(
                request_id=f"run-{token}-{index}", device=candidate[0].name,
                root_project=plan.project.project_id, label=label, **admission_fields,
            )
            for index, candidate in enumerate(selection)
        ]
        try:
            document = admit_first(
                client, requests, wait_seconds=wait,
                report=lambda doc: reporter.event("admission_waiting", **_admission_place(doc)),
                unresolved=lambda ids: _report_unresolved(reporter, client, ids),
            )
        except AdmissionTimeout as exc:
            reporter.event("admission_timeout", waited_seconds=wait,
                           exit_code=EXIT_ADMISSION_WAITING, **_admission_place(exc.document))
            return EXIT_ADMISSION_WAITING
        except AdmissionUnavailable as exc:
            reporter.event("admission_unavailable", message=str(exc), exit_code=EXIT_INFRA)
            return EXIT_INFRA
        if document["state"] != "granted":
            raise AdmissionError(
                f"admission request {document['request_id']} on {document['device']} "
                f"ended: {document['end_reason']}"
            )
        plan = replace(plan, admission={
            "request_id": document["request_id"], "coordinator": client.coordinator,
        })
        held.update(admission=plan.admission, project_id=plan.project.project_id,
                    config=config, run_ids=set())
        selection = [selection[[item.request_id for item in requests].index(
            document["request_id"])]]
        reporter.event("admission_granted", request_id=document["request_id"],
                       device=document["device"])

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
    attempted_run_ids: set[str] = held.get("run_ids", set())
    for attempt, (chosen, transport, probe, reason) in enumerate(selection):
        plan = replace(base_plan, target=chosen, target_reason=reason,
                       label=getattr(args, "label", None))
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
        if bypass_memory_guard:
            transport.memory_guard = None
            reporter.event(
                "memory_guard_bypassed",
                target=plan.target.name,
                reason="explicit_owner_override",
            )
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
        # A commit run's command runs in a private tree under the target's state root.
        work: CommitWork | None = None
        if source is not None:
            try:
                work = work_paths(transport, plan)
            except ValueError as exc:
                write_json(summary_path, {
                    "run_id": run_id, "error": str(exc), "phase": "preflight",
                    "completion_state": "not_started", "command_started": False,
                    "terminal": True, "plan": plan.as_dict(),
                })
                raise

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

        if work is not None:
            remote_root = work.tree
        else:
            remote_root = transport.remote_project_path(plan.project)
        remote_cwd = transport.remote_join(remote_root, plan.project.relative_cwd)
        reporter.event("remote_cwd", path=remote_cwd)

        lock: ProjectLock | None = None
        if untransferred and getattr(args, "durable", False):
            # Released, like the project lock, when this controller stops waiting.
            lock = _durable_run_lock(plan, run_id, reporter).acquire()
        if not untransferred:
            try:
                lock = ProjectLock(
                    plan.project.project_id,
                    plan.target.name,
                    scope=plan.write_scope,
                    run_id=run_id,
                    report=reporter.event,
                ).acquire(wait_seconds=getattr(args, "wait_lock", 0.0))
            except LockError as exc:
                if reservation is not None:
                    transport.release_memory_guard(reservation, reserved_only=True)
                reporter.event("locked", message=str(exc))
                write_json(summary_path, {"run_id": run_id, "error": str(exc), "plan": plan.as_dict()})
                return EXIT_INTERNAL
        # The pre-planning hazard check is stale by now: a holder waited on (or one
        # that finished while this run was planning) may have left its fence behind.
        def _drop_lock() -> None:
            if lock is not None:
                lock.release()
            if reservation is not None:
                transport.release_memory_guard(reservation, reserved_only=True)

        try:
            hazard = _report_unknown_completion_hazard(plan.project.project_id, reporter)
        except Exception:
            _drop_lock()
            raise
        if hazard is not None:
            _drop_lock()
            write_json(summary_path, {
                "run_id": run_id,
                "error": _unknown_completion_guidance(hazard),
                "plan": plan.as_dict(),
            })
            return EXIT_INTERNAL

        t0 = time.monotonic()
        reservation_ref: dict[str, MemoryReservation | None] = {"value": reservation}
        try:
            return _run_locked(args, reporter, plan, transport, remote_root, remote_cwd,
                               run_id, rdir, summary_path, started_at, t0, policy,
                               telemetry_default, attempt > 0, attempted_run_ids,
                               resource_policy, reservation,
                               memory_limit_mib=memory_limit_mib,
                               predicted_rss_mb=(prediction.get("rss_mb") if prediction else None),
                               memory_reservation_ref=reservation_ref,
                               verify_digests=verify_digests, work=work, source=source)
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
            if work is not None and work.seeded and not work.dispatched:
                discard_unstarted_work(transport, plan, work, reporter.event)
            if lock is not None:
                lock.release()
            current_reservation = reservation_ref["value"]
            if current_reservation is not None:
                transport.release_memory_guard(current_reservation, reserved_only=True)
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
    memory_limit_mib: int | None = None,
    predicted_rss_mb: object = None,
    memory_reservation_ref: dict[str, MemoryReservation | None] | None = None,
    verify_digests: dict[str, str] | None = None,
    work: CommitWork | None = None,
    source=None,  # noqa: ANN001 - isolated.CommitSource for a commit run
) -> int:
    local_root = plan.project.local_project_root
    # An untransferred run neither reconciles, nor reads or writes a baseline, nor pulls
    # back: the command runs on whatever the target's checkout holds.
    untransferred = plan.transfer_mode == "none"
    # A commit run neither reconciles nor writes a baseline: it seeds a private tree
    # from git and pulls back only its declared outputs.
    commit = work is not None
    # Bootstrap readiness is a property of the target's own checkout and environment.
    checkout_root = transport.remote_project_path(plan.project) if commit else remote_root
    backup_root = conflict_dir(run_id) / "backup"

    if plan.bootstrap is not None and plan.bootstrap.status == "unsupported":
        detail = plan.bootstrap.detail or "bootstrap declaration is unsupported"
        reporter.event("bootstrap_failed", status="unsupported", detail=detail)
        write_json(summary_path, {
            "run_id": run_id, "project_id": plan.project.project_id,
            "target": plan.target.name, "command": plan.command,
            "started_at": started_at, "ended_at": utc_now_iso(),
            "phase": "bootstrap", "completion_state": "not_started",
            "command_started": False, "terminal": True, "error": detail,
            "bootstrap": plan.bootstrap.as_dict(), "plan": plan.as_dict(),
        })
        return EXIT_INTERNAL

    runenv = resolve_run_env(
        device=plan.target, project=plan.project, project_config=plan.project_config
    )
    # Keep the owner-requested argv in ``plan`` and summaries, but dispatch a
    # managed bare entrypoint only after the target has resolved it to its
    # canonical absolute executable.  This prevents PATH from selecting a
    # system executable if the managed file is replaced between validation and
    # launch.
    dispatch_command = list(plan.command)
    bootstrap_inputs = (
        plan.bootstrap.lock_inputs
        if plan.bootstrap is not None and plan.bootstrap.status == "bootstrap-needed"
        else ()
    )
    bootstrap_result = None
    # Bootstrap must complete before resolving a managed bare command, even when
    # a declaration has no lock inputs: otherwise validation can reject the
    # interpreter before the setup step has had a chance to create it.
    defer_command_validation = (
        plan.bootstrap is not None
        and plan.bootstrap.status == "bootstrap-needed"
    )

    def validate_user_command() -> int | None:
        nonlocal dispatch_command
        try:
            dispatch_command = validate_managed_command(
                transport,
                device=plan.target,
                runenv=runenv,
                command=plan.command,
            )
            transport.validate_command_context(
                dispatch_command, env=runenv.env, path_prepend=runenv.path_prepend
            )
        except (BootstrapConfigError, CommandNotStartedError) as exc:
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
                "bootstrap": (
                    bootstrap_result.as_dict()
                    if bootstrap_result is not None
                    else None
                ),
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
                "bootstrap": (
                    bootstrap_result.as_dict()
                    if bootstrap_result is not None
                    else None
                ),
                "plan": plan.as_dict(),
            })
            return EXIT_TRANSFER
        return None

    # The untransferred target check runs first: a missing checkout or a differing
    # --verify file is this candidate's problem, so --auto may try the next one.
    verified: list[dict[str, str]] = []
    if untransferred:
        try:
            verified, problems = _verify_untransferred_target(
                transport, plan, remote_root, remote_cwd, verify_digests or {}
            )
        except TransportError as exc:
            reporter.event("transfer_error", phase="verify", message=str(exc))
            write_json(summary_path, {
                "run_id": run_id,
                "project_id": plan.project.project_id,
                "target": plan.target.name,
                "command": plan.command,
                "started_at": started_at,
                "ended_at": utc_now_iso(),
                "error": str(exc),
                "phase": "verify",
                "completion_state": "not_started",
                "command_started": False,
                "terminal": True,
                "plan": plan.as_dict(),
            })
            return EXIT_TRANSFER
        if problems:
            _refuse_candidate(
                reporter=reporter, plan=plan, run_id=run_id, summary_path=summary_path,
                started_at=started_at, phase="verify", conflicts=problems, verified=verified,
            )
        for item in verified:
            reporter.event("verify", path=item["path"], sha256=item["sha256"], status="match")
    # Untransferred summaries carry what --verify proved about the code that ran; commit
    # summaries carry the commit, the --input receipts and the private tree.
    verify_fields: dict[str, object] = {"verify": verified} if untransferred else {}
    if commit:
        verify_fields = {"source": source.as_dict(), "work": work.as_dict()}

    # Preserve the ordinary path's pre-mutation rejection boundary.  Only a
    # declared bootstrap may defer validation, because it can install the
    # requested entrypoint.
    if not defer_command_validation:
        validation_exit = validate_user_command()
        if validation_exit is not None:
            return validation_exit

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

    pre = None
    if untransferred:
        reporter.event(
            "transfer_skipped", mode=plan.transfer_mode,
            note="no reconcile, baseline or pullback; outputs stay on the target",
        )
    elif commit:
        # Seeding runs guarded git commands, each holding its own lease on a
        # memory-guarded target, while this run's reservation (held, unused until
        # dispatch) can occupy the target's only guarded slot. Release it for the
        # seed and admit the command again afterwards.
        reseat = memory_reservation is not None
        if reseat:
            transport.release_memory_guard(memory_reservation, reserved_only=True)
            memory_reservation = None
            if memory_reservation_ref is not None:
                memory_reservation_ref["value"] = None
        try:
            seed_commit_tree(
                transport=transport, plan=plan, config=load_config(), policy=policy,
                run_id=run_id, rdir=rdir, work=work, source=source, remote_cwd=remote_cwd,
                report=reporter.event,
            )
        except CandidateRefusal as exc:
            _refuse_candidate(
                reporter=reporter, plan=plan, run_id=run_id, summary_path=summary_path,
                started_at=started_at, phase="seed", verified=[],
                conflicts=[{"path": ".", "state": exc.state, "reason": exc.reason}],
            )
        except (TransportError, GitSyncError, CommitSourceError, OSError) as exc:
            reporter.event("transfer_error", phase="seed", message=str(exc))
            write_json(summary_path, {
                "run_id": run_id, "project_id": plan.project.project_id,
                "target": plan.target.name, "command": plan.command,
                "started_at": started_at, "ended_at": utc_now_iso(), "error": str(exc),
                "phase": "seed", "completion_state": "not_started",
                "command_started": False, "terminal": True, **verify_fields,
                "plan": plan.as_dict(),
            })
            return EXIT_TRANSFER
        reporter.event("work_seeded", path=work.tree, commit=source.commit,
                       inputs=len(source.inputs))
        if reseat:
            if memory_limit_mib is not None:
                admission = transport.reserve_memory_guard(explicit_limit_mib=memory_limit_mib)
            else:
                admission = transport.reserve_memory_guard(predicted_rss_mb=predicted_rss_mb)
            if not admission.admitted:
                # The seeded tree is removed by the caller: the command never started.
                detail = "target capacity was unavailable after seeding: " + admission.detail
                reporter.event("memory_admission", target=plan.target.name,
                               status=admission.status, reason=admission.reason,
                               detail=detail, phase="post_seed")
                write_json(summary_path, {
                    "run_id": run_id, "project_id": plan.project.project_id,
                    "target": plan.target.name, "command": plan.command,
                    "started_at": started_at, "ended_at": utc_now_iso(),
                    "phase": "memory_admission", "completion_state": "not_started",
                    "command_started": False, "terminal": True, "error": detail,
                    "memory_admission": admission.payload, "plan": plan.as_dict(),
                })
                return EXIT_GUARD
            memory_reservation = admission.reservation
            if memory_reservation_ref is not None:
                memory_reservation_ref["value"] = memory_reservation
    else:
        # Untransferred runs work in this target's checkout without the project lock.
        # Reconciling that tree under them could change files they are reading and pull
        # back their half-written outputs, so a reconciling run takes another target.
        # This check runs while this run holds the project lock, and an untransferred
        # run publishes its fence under that lock table's guard only when no live
        # holder is on its target (_publish_completion_fence). Whichever of the two
        # goes first, the other sees it.
        # A commit run's fence (it has outputs) names a private tree, not this checkout,
        # so it is no obstacle here. Nor does its lease need a guard in this run: both
        # runs write local files only under the local tree's write lock, re-checking
        # each file there, so whichever writes a leased output second saves its copy as
        # a conflict instead of overwriting the first (invariant 6).
        busy = [
            fence["run_id"]
            for fence in per_run_completion_fences(plan.project.project_id)
            if fence["target"] == plan.target.name and "outputs" not in fence
        ]
        if busy:
            _refuse_candidate(
                reporter=reporter, plan=plan, run_id=run_id, summary_path=summary_path,
                started_at=started_at, phase="preflight", verified=[],
                conflicts=[{
                    "path": plan.project.relative_cwd or ".",
                    "state": "target-busy-untransferred",
                    "reason": (
                        f"untransferred run(s) {', '.join(busy)} may still be working in "
                        f"{plan.target.name}'s checkout; wait for them (`remrun resume "
                        f"{busy[0]}`) or choose another target"
                    ),
                }],
            )
        prev_local, prev_remote = read_baseline(plan.target.name, plan.project.project_id)
        allow_large_push = getattr(args, "allow_large_push", False)
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
                include_paths=bootstrap_inputs,
                max_push_files=None if allow_large_push else plan.max_push_files,
                max_push_bytes=None if allow_large_push else plan.max_push_bytes,
                report=reporter.event,
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

        if pre.push_refused is not None:
            # Not a candidate-local conflict: the bulk is in the local tree, so failing over
            # would only repeat the same push elsewhere. Stop and let the user decide.
            message = pre.push_refused.message()
            reporter.event("push_refused", files=pre.push_refused.files,
                           bytes=pre.push_refused.bytes, max_files=pre.push_refused.max_files,
                           max_bytes=pre.push_refused.max_bytes, message=message)
            write_json(summary_path, {
                "run_id": run_id,
                "project_id": plan.project.project_id,
                "target": plan.target.name,
                "command": plan.command,
                "started_at": started_at,
                "ended_at": utc_now_iso(),
                "exit_code": EXIT_GUARD,
                "error": message,
                "phase": "preflight",
                "completion_state": "not_started",
                "command_started": False,
                "terminal": True,
                "push_guard": pre.push_refused.as_dict(),
                "plan": plan.as_dict(),
            })
            return EXIT_GUARD

        if pre.has_conflicts:
            cdir = conflict_dir(run_id)
            write_json(cdir / "conflicts.json", {
                "run_id": run_id,
                "project_id": plan.project.project_id,
                "target": plan.target.name,
                "conflicts": [
                    {"path": c.path, "state": c.state, "reason": c.reason}
                    for c in pre.conflicts
                ],
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
        # This positively reconciled generation survives bootstrap failure, transport
        # loss and cancellation. Later local edits must compare against the last push,
        # while command outputs remain unbaselined until verified pullback.
        write_baseline(plan.target.name, plan.project.project_id,
                       pre.local_manifest, pre.remote_manifest)
        reporter.event("preflight_summary", pulled=len(pre.pulled), pushed=len(pre.pushed),
                       deleted_remote=len(pre.deleted_remote), deleted_local=len(pre.deleted_local),
                       skipped_identical=len(pre.skipped_identical),
                       converged_conflicts=len(pre.converged_conflicts), conflicts=0)

    def fail_bootstrap_input_drift(detail: str) -> int:
        reporter.event("bootstrap_failed", status="input_changed", detail=detail)
        write_json(summary_path, {
            "run_id": run_id,
            "project_id": plan.project.project_id,
            "target": plan.target.name,
            "command": plan.command,
            "started_at": started_at,
            "ended_at": utc_now_iso(),
            "phase": "bootstrap",
            "completion_state": "not_started",
            "command_started": False,
            "terminal": True,
            "error": detail,
            "bootstrap": {
                "status": "input_changed",
                "detail": detail,
            },
            "plan": plan.as_dict(),
        })
        return EXIT_CONFLICT

    if plan.bootstrap is not None and plan.bootstrap.status == "bootstrap-needed":
        inputs_current, detail = bootstrap_inputs_match(
            plan.bootstrap, project_root=local_root
        )
        if not inputs_current:
            return fail_bootstrap_input_drift(detail)

    # Bootstrap is deliberately after reconciliation: declared lock inputs are now
    # present on the target, and no setup command can run against a stale project
    # surface.  It is also before both ordinary and durable dispatch, so a failed
    # or uncertain setup can never be mistaken for a user-command attempt.
    if plan.bootstrap is not None:
        try:
            target_bootstrap = inspect_bootstrap(
                transport,
                device=plan.target,
                project=plan.project,
                plan=plan.bootstrap,
                remote_root=checkout_root,
                runenv=runenv,
            )
        except (OSError, TransportError, ValueError) as exc:
            target_bootstrap = replace(plan.bootstrap, status="unsupported", detail=str(exc))
        reporter.event("bootstrap", **target_bootstrap.as_dict())
        if target_bootstrap.status == "disabled":
            pass
        elif target_bootstrap.status == "unsupported":
            detail = target_bootstrap.detail or "bootstrap declaration is unsupported"
            write_json(summary_path, {
                "run_id": run_id, "project_id": plan.project.project_id,
                "target": plan.target.name, "command": plan.command,
                "started_at": started_at, "ended_at": utc_now_iso(),
                "phase": "bootstrap", "completion_state": "not_started",
                "command_started": False, "terminal": True, "error": detail,
                "bootstrap": target_bootstrap.as_dict(), "plan": plan.as_dict(),
            })
            return EXIT_INTERNAL
        elif (untransferred or commit) and target_bootstrap.status != "ready":
            # Setup mutates an environment other in-flight runs may import from, and an
            # untransferred or commit run has not pushed the lock inputs it would build
            # from. (A commit run's lock inputs are its clean HEAD's, which are the
            # controller's working-tree bytes the environment's receipt is keyed to.)
            _refuse_candidate(
                reporter=reporter, plan=plan, run_id=run_id, summary_path=summary_path,
                started_at=started_at, phase="bootstrap", verified=verified,
                conflicts=[{
                    "path": ".",
                    "state": "bootstrap-not-ready",
                    "reason": (
                        f"the target's declared environment is not ready "
                        f"({target_bootstrap.detail or target_bootstrap.status}); "
                        f"--transfer {plan.transfer_mode} never runs setup, so run one "
                        "ordinary run first"
                    ),
                }],
            )
        elif target_bootstrap.status != "ready":
            used_existing_guard = (
                memory_reservation is not None
                and getattr(transport, "memory_guard", None) is not None
            )
            try:
                bootstrap_result = execute_bootstrap(
                    transport, device=plan.target, project=plan.project,
                    plan=target_bootstrap, remote_root=remote_root, runenv=runenv,
                    memory_reservation=memory_reservation if used_existing_guard else None,
                )
            except (
                BootstrapConfigError,
                OSError,
                TransportError,
                ValueError,
                subprocess.TimeoutExpired,
            ) as exc:
                bootstrap_result = None
                detail = f"{type(exc).__name__}: {exc}"
                reporter.event("bootstrap_failed", detail=detail)
                write_json(summary_path, {
                    "run_id": run_id, "project_id": plan.project.project_id,
                    "target": plan.target.name, "command": plan.command,
                    "started_at": started_at, "ended_at": utc_now_iso(),
                    "phase": "bootstrap", "completion_state": "unknown",
                    "command_started": False, "terminal": True, "error": detail,
                    "bootstrap": {"status": "unknown", "detail": detail},
                    "plan": plan.as_dict(),
                })
                return EXIT_INFRA
            # A transport consumes/releases a supplied lease when its bootstrap
            # helper returns.  Make the holder agree before any pre-dispatch
            # failure can be finalized by the outer cleanup.
            if used_existing_guard:
                memory_reservation = None
                if memory_reservation_ref is not None:
                    memory_reservation_ref["value"] = None
            if bootstrap_result is None or bootstrap_result.status not in {"ready", "bootstrapped"}:
                if bootstrap_result is not None:
                    for name, text in (
                        ("bootstrap.stdout.log", bootstrap_result.helper_stdout),
                        ("bootstrap.stderr.log", bootstrap_result.helper_stderr),
                    ):
                        (rdir / name).write_text(
                            cap_text(text, policy.max_log_bytes), encoding="utf-8"
                        )
                detail = (
                    bootstrap_result.detail if bootstrap_result is not None
                    else "bootstrap did not return an outcome"
                )
                reporter.event(
                    "bootstrap_failed",
                    status=bootstrap_result.status if bootstrap_result else "unknown",
                    detail=detail,
                )
                write_json(summary_path, {
                    "run_id": run_id, "project_id": plan.project.project_id,
                    "target": plan.target.name, "command": plan.command,
                    "started_at": started_at, "ended_at": utc_now_iso(),
                    "phase": "bootstrap", "completion_state": "not_started",
                    "command_started": False, "terminal": True, "error": detail,
                    "bootstrap": bootstrap_result.as_dict() if bootstrap_result else None,
                    "plan": plan.as_dict(),
                })
                return EXIT_INFRA
            if used_existing_guard:
                if memory_limit_mib is not None:
                    admission = transport.reserve_memory_guard(
                        explicit_limit_mib=memory_limit_mib
                    )
                else:
                    admission = transport.reserve_memory_guard(
                        predicted_rss_mb=predicted_rss_mb
                    )
                if not admission.admitted:
                    detail = "target capacity was unavailable after bootstrap: " + admission.detail
                    reporter.event("memory_admission", target=plan.target.name,
                                   status=admission.status, reason=admission.reason,
                                   detail=detail, phase="post_bootstrap")
                    write_json(summary_path, {
                        "run_id": run_id, "project_id": plan.project.project_id,
                        "target": plan.target.name, "command": plan.command,
                        "started_at": started_at, "ended_at": utc_now_iso(),
                        "phase": "memory_admission", "completion_state": "not_started",
                        "command_started": False, "terminal": True, "error": detail,
                        "bootstrap": bootstrap_result.as_dict(),
                        "memory_admission": admission.payload, "plan": plan.as_dict(),
                    })
                    return EXIT_GUARD
                memory_reservation = admission.reservation
                if memory_reservation_ref is not None:
                    memory_reservation_ref["value"] = memory_reservation
        elif target_bootstrap.status == "ready":
            bootstrap_result = None

    if plan.bootstrap is not None and plan.bootstrap.status == "bootstrap-needed":
        inputs_current, detail = bootstrap_inputs_match(
            plan.bootstrap, project_root=local_root
        )
        if not inputs_current:
            return fail_bootstrap_input_drift(detail)

    if defer_command_validation:
        validation_exit = validate_user_command()
        if validation_exit is not None:
            return validation_exit

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
            bootstrap_result=bootstrap_result, command=dispatch_command,
            verify_fields=verify_fields, work=work,
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
    if not untransferred:  # an untransferred run never writes into the target's tree
        transport.ensure_remote_dir(remote_cwd)
    guarded_dispatch = (
        getattr(transport, "memory_guard", None) is not None
        or transport.command_start_requires_confirmation()
    )
    # Fence before dispatch, not after a transport exception. A controller can be
    # killed without Python regaining control; in that case the remote start/result
    # is unknowable and an automatic retry is unsafe.
    hazard = _publish_completion_fence(
        reporter=reporter, plan=plan, run_id=run_id, summary_path=summary_path,
        started_at=started_at, verified=verified,
    )
    if work is not None:
        work.dispatched = True  # from here the command may run; its tree is kept
    guidance = _unknown_completion_guidance(hazard, per_run=untransferred or commit)
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
        **verify_fields,
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
        if not job_observation_enabled(plan.target) or observed_exec is None:
            # Observation is dormant unless the target declares observe_jobs.
            # Third-party/test doubles continue through their established exec seam.
            result = transport.exec(dispatch_command, cwd=remote_cwd, **exec_kwargs)
        else:
            observation = JobObservation.for_command(
                job_id=run_id,
                project=plan.project.project_id,
                target=plan.target.name,
                phase="command",
                command=dispatch_command,
                declared_label=getattr(args, "label", None),
            )
            result = observed_exec(
                dispatch_command,
                cwd=remote_cwd,
                observation=observation,
                **exec_kwargs,
            )
    except CommandCancelledError as exc:
        # The observer permits explicit breakaway for arbitrary-command compatibility.
        # Cancelling its exact Job cannot prove that escaped work has ended.
        reporter.event("completion_unknown", guidance=guidance)
        reporter.event("command_cancelled", owned_job_cancelled=exc.owned_job_cancelled,
                       message=str(exc))
        write_json(summary_path, {
            "run_id": run_id, "project_id": plan.project.project_id,
            "target": plan.target.name, "command": plan.command,
            "started_at": started_at, "ended_at": utc_now_iso(), "phase": "exec",
            "completion_state": "unknown", "command_started": None,
            "terminal": True, "error": str(exc),
            "owned_job_cancelled": exc.owned_job_cancelled,
            "guidance": guidance, **verify_fields, "plan": plan.as_dict(),
        })
        return EXIT_CANCELLED
    except CommandNotStartedError as exc:
        # The same target process that resolves a bare Windows command can prove it
        # refused the unsupported entry point before invoking user code. Remove the
        # conservative fence rather than manufacturing UNKNOWN.
        clear_unknown_completion_hazard(
            plan.project.project_id, run_id, per_run=untransferred or commit
        )
        if work is not None:
            work.dispatched = False  # proved never started: its tree is removed
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
            **verify_fields,
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
            **verify_fields,
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
        "phase": "finalize" if untransferred or args.no_pullback else "pullback",
        "completion_state": "command_complete",
        "command_started": guarded_started,
        "command_exit_code": command_exit_code_checkpoint,
        "terminal": False,
        **verify_fields,
        "plan": plan.as_dict(),
    })
    if not commit:
        # A commit run's fence is also its output lease: it stands through the pullback.
        clear_unknown_completion_hazard(
            plan.project.project_id, run_id, per_run=untransferred
        )

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
        and final_memory_reservation.effective_command_limit_bytes is not None
    ):
        memory_limit_guidance = {
            "allowance_basis": final_memory_reservation.allowance_basis,
            "allocation_rule": final_memory_reservation.allocation_rule,
            "enforced_command_limit_bytes": (
                final_memory_reservation.effective_command_limit_bytes
            ),
            "observed_peak_lower_bound_bytes": guard_result.get(
                "peak_command_bytes"
            ),
            "policy_command_ceiling_bytes": (
                final_memory_reservation.max_command_bytes
            ),
            "host_reserve_bytes": final_memory_reservation.min_available_bytes,
            "partial_effects_may_exist": True,
            "profile_recorded": False,
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
    commit_final = None
    lease_guidance: str | None = None
    if untransferred:
        # Outputs stay where the command wrote them in the target's tree. There are no
        # launch manifests to attribute changes against, and no baseline to advance.
        pass
    elif commit:
        try:
            commit_final = finalize_commit_outputs(
                transport=transport, plan=plan, run_id=run_id, rdir=rdir,
                work=work.as_dict(), policy=policy,
                command_clean=not guard_failed and result.exit_code == 0,
                command_note=(
                    f"memory guard {guard_status}" if guard_failed
                    else f"command exited {result.exit_code}"
                ),
                report=reporter.event,
            )
        except TransportError as exc:
            failed_exit = EXIT_GUARD if guard_failed else EXIT_TRANSFER
            # Delivery is incomplete, so the fence stays (see _kept_lease_guidance).
            guidance = _kept_lease_guidance(
                run_id, plan, work.tree,
                f"the command ended (exit {command_exit_code}) but its outputs were not "
                f"all returned ({exc})",
            )
            reporter.event("transfer_error", phase="pullback", message=str(exc))
            reporter.event("output_lease_kept", run_id=run_id, guidance=guidance)
            write_json(summary_path, {
                "run_id": run_id,
                "exit_code": failed_exit,
                "command_exit_code": command_exit_code,
                "error": str(exc),
                "guidance": guidance,
                "phase": "pullback",
                "completion_state": "finalization_failed",
                "command_started": guarded_started,
                "terminal": True,
                "ended_at": utc_now_iso(),
                **verify_fields,
                "work": {**work.as_dict(), "state": "retained", "reason": str(exc)},
                "plan": plan.as_dict(),
            })
            return failed_exit
        if commit_final.undelivered:
            # An output that was not cleanly delivered lives only in the retained tree:
            # the fence stays, so the tree is protected and overlapping runs wait.
            lease_guidance = _kept_lease_guidance(
                run_id, plan, work.tree,
                _undelivered_outputs_why(command_exit_code, commit_final.undelivered),
            )
            reporter.event("output_lease_kept", run_id=run_id, guidance=lease_guidance)
        else:
            # The outputs are delivered; the lease ends with the pullback.
            clear_unknown_completion_hazard(plan.project.project_id, run_id, per_run=True)
        post = commit_final.post
        verify_fields = {
            **verify_fields,
            "work": remove_commit_work(
                transport, plan.target.name, commit_final.work, reporter.event
            ),
        }
        for rel in post.conflicts:
            reporter.event("postrun_conflict", path=rel,
                           saved=str(conflict_dir(run_id) / "remote" / rel))
    elif not args.no_pullback:
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
                include_paths=bootstrap_inputs,
                report=reporter.event,
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
    postrun_unresolved = bool(post and post.conflicts) or bool(
        commit_final and commit_final.unresolved
    )
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
    # of a reusable job cost. An untransferred run has no pullback to attribute, so a
    # successful command under a clean guard is the whole evidence. Best-effort
    # storage must never break a valid run.
    profile_admissible = bool(
        not guard_failed
        and result.exit_code == 0
        and final_exit == EXIT_OK
        and (
            untransferred
            # A clean commit pullback (no conflicts, no unexpected writes) is attributable.
            or commit_final is not None
            or (
                not args.no_pullback
                and post is not None
                and not post.conflicts
                and post.next_baseline is not None
            )
        )
    )
    if profile_admissible:
        profile_exec_s = exec_s
        target_wall = (result.telemetry or {}).get("wall_sec")
        if plan.workload is not None and _positive_number(target_wall):
            profile_exec_s = float(target_wall)
        _record_job_profile(
            plan, result.telemetry, guard_result, exec_s=profile_exec_s, trip_s=duration
        )

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
        "files_pushed": len(pre.pushed) if pre else 0,
        "files_pulled_pre": len(pre.pulled) if pre else 0,
        "files_deleted_remote_pre": len(pre.deleted_remote) if pre else 0,
        "files_deleted_local_pre": len(pre.deleted_local) if pre else 0,
        "files_skipped_identical_pre": len(pre.skipped_identical) if pre else 0,
        "files_converged_conflicts_pre": len(pre.converged_conflicts) if pre else 0,
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
        "bootstrap": bootstrap_result.as_dict() if bootstrap_result is not None else None,
        "peak_rss_mb": result.telemetry.get("peak_rss_mb") if result.telemetry else None,
        "avg_cpu_pct": result.telemetry.get("avg_cpu_pct") if result.telemetry else None,
        **verify_fields,
        **({"guidance": lease_guidance} if lease_guidance else {}),
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


def _observed_project(cwd: Path, config, device) -> str:  # noqa: ANN001
    """The project directory a local command runs in, for display in ``fleet jobs``.

    Projects on this machine live under its own device ``project_root``; the
    controller project root is the fallback. A command started anywhere inside a
    project, including its ``.worktrees``, is that project's work.
    """
    here = cwd.resolve()
    bases = []
    if device is not None and device.project_root:
        bases.append(Path(device.project_root).expanduser())
    try:
        bases.append(project_root_base(config))
    except ProjectDetectionError:
        pass
    for base in bases:
        try:
            parts = here.relative_to(base.resolve()).parts
        except (OSError, ValueError):
            continue
        if parts:
            return parts[0]
    return here.name or "project"


def _exit_status(code: int) -> int:
    return (128 - code) if code < 0 else code


def _claim_terminal() -> tuple[int, int] | None:
    """Return this process's controlling terminal and group when it holds the foreground."""
    try:
        fd = os.open("/dev/tty", os.O_RDWR)
    except OSError:
        return None
    try:
        if os.tcgetpgrp(fd) == os.getpgrp():
            return fd, os.getpgrp()
    except OSError:
        pass
    os.close(fd)
    return None


def _set_foreground(fd: int, pgid: int) -> None:
    import signal

    previous = signal.signal(signal.SIGTTOU, signal.SIG_IGN)
    try:
        os.tcsetpgrp(fd, pgid)
    finally:
        signal.signal(signal.SIGTTOU, previous)


def _observe_posix(command: list[str], root: Path, observation: JobObservation) -> int:
    """Run ``command`` as its own process group, registered as that group.

    The group is what ``fleet jobs`` measures, and it is the foreground job when a
    terminal is attached, so Ctrl-C and terminal input reach the command exactly as
    they would without remrun. Signals sent to remrun are passed to the group, and
    remrun returns only when the command itself has exited, with the command's status:
    observation, terminal restoration and registry cleanup can only warn.
    """
    import signal

    from . import _job_observer as observer

    started: list[subprocess.Popen] = []
    pending: list[int] = []

    def signal_group(pid: int, signum: int) -> None:
        try:
            os.killpg(pid, signum)
        except (ProcessLookupError, PermissionError):
            pass  # the group has already exited

    def forward(signum, _frame) -> None:  # noqa: ANN001
        if not started:
            pending.append(signum)
            return
        signal_group(started[0].pid, signum)

    previous = {
        sig: signal.signal(sig, forward)
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    }
    terminal = None
    try:
        terminal = _claim_terminal()
        proc = subprocess.Popen(command, process_group=0)
        started.append(proc)
        if terminal is not None:
            try:
                _set_foreground(terminal[0], proc.pid)
            except OSError as exc:
                if proc.poll() is None:
                    # A command left outside the terminal stops on its first read and
                    # nothing would resume it, so end it rather than leave it running.
                    print(f"remrun observe: could not give the command the terminal ({exc}); "
                          "stopping it", file=sys.stderr, flush=True)
                    signal_group(proc.pid, signal.SIGTERM)
                    try:
                        return _exit_status(proc.wait(timeout=10))
                    except subprocess.TimeoutExpired:
                        signal_group(proc.pid, signal.SIGKILL)
                        return _exit_status(proc.wait())
            signal_group(proc.pid, signal.SIGCONT)
        for signum in pending:
            signal_group(proc.pid, signum)
        try:
            child = observer._child_row(proc)
            owner = observer._processes().get(os.getpid())
            if child is None or owner is None:
                raise RuntimeError("process identity is unavailable")
            token = observer._register(
                root, observation.payload(), child,
                owner_kind="posix_pgid", owner_key=str(proc.pid),
                owner_process=owner, witness=child,
            )
        except Exception as exc:  # noqa: BLE001 - observation never decides the run
            token = None
            print(f"remrun observation unavailable; command runs unobserved: {exc}",
                  file=sys.stderr, flush=True)
        code = proc.wait()
        if token is not None:
            # Descendants left in the group are still this job; keep one as its witness.
            try:
                survivors = [row for row in observer._processes().values()
                             if row.pgid == proc.pid and row.identity is not None]
                if survivors:
                    observer._update_record_processes(root, token, witness=survivors[0])
                else:
                    observer._unregister(root, token)
            except Exception as exc:  # noqa: BLE001
                print(f"remrun observation cleanup unavailable: {exc}", file=sys.stderr)
        return _exit_status(code)
    finally:
        try:
            if terminal is not None:
                try:
                    _set_foreground(*terminal)
                finally:
                    os.close(terminal[0])
        except OSError as exc:
            print(f"remrun observe: could not restore the terminal: {exc}", file=sys.stderr)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)


def _observe_windows(command: list[str], root: Path, observation: JobObservation) -> int:
    """Run ``command`` under the job-object observer; the console delivers Ctrl-C to it."""
    from . import _job_observer

    proc = subprocess.Popen([
        sys.executable, "-S", str(Path(_job_observer.__file__)), "run",
        "--state-root", str(root), "--metadata-b64", observation.encoded(), "--", *command,
    ])
    while True:
        try:
            return _exit_status(proc.wait())
        except KeyboardInterrupt:
            continue


def cmd_observe(args: argparse.Namespace, reporter: Reporter) -> int:
    """Run a command on this machine, registered in its active-job registry.

    Other controllers see it through ``fleet jobs`` exactly like a job launched here
    by ``run``. Observation is fail-open: the command runs and its exit status is
    returned whether or not it could be registered.
    """
    command = normalize_cmd(args.cmd)
    config = load_config()
    device = local_device(config)
    if not job_observation_enabled(device):
        reporter.event(
            "observation_unavailable",
            reason=(
                "this machine is not a configured remrun device" if device is None
                else f"{device.name} does not declare observe_jobs"
            ),
            detail="command runs unobserved",
        )
        return _exit_status(subprocess.call(command))
    project = _observed_project(Path.cwd(), config, device)
    observation = JobObservation.for_command(
        job_id=new_run_id(device.name, project),
        project=project,
        target=device.name,
        phase="native",
        command=command,
        declared_label=getattr(args, "label", None),
        source_controller=device.name,
    )
    root = local_registry_root(config)
    if os.name == "posix":
        return _observe_posix(command, root, observation)
    return _observe_windows(command, root, observation)


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
        ineligible = [
            name for name in targets
            if name not in config.devices or not config.devices[name].enabled
        ]
        if ineligible:
            raise ValueError(
                "bench requires enabled target device(s): " + ", ".join(ineligible)
                + "; allow_explicit_run applies only to ordinary plan/run"
            )
    else:
        sched = scheduler_config(config)
        ordered = [sched.get("primary"), *list(sched.get("fallback", []))]
        targets = [
            str(name) for name in ordered
            if name and str(name) in config.devices and config.devices[str(name)].enabled
        ]
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


def _positive_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _record_job_profile(
    plan: RunPlan,
    telemetry: dict | None,
    guard_result: dict | None,
    *,
    exec_s: float | None,
    trip_s: float | None,
) -> None:
    """Fold one admissible run's measured cost into the learned job profiles.

    Callers decide admissibility (successful command, attributable pullback).
    Best-effort storage must never break a valid run.
    """
    try:
        tel = telemetry or {}
        peak_rss_mb = tel.get("peak_rss_mb")
        if not _positive_number(peak_rss_mb):
            guard_peak = (
                guard_result.get("peak_command_bytes")
                if guard_result is not None and guard_result.get("status") == "ok"
                else None
            )
            peak_rss_mb = (
                guard_peak / MIB
                if isinstance(guard_peak, int)
                and not isinstance(guard_peak, bool)
                and guard_peak > 0
                else None
            )
        update_profile(
            default_state_root(),
            profile_project_id(plan.project.project_id),
            command_key(plan.command),
            plan.target.name,
            peak_rss_mb=peak_rss_mb,
            avg_cpu_pct=tel.get("avg_cpu_pct"),
            exec_s=exec_s,
            trip_s=trip_s,
            now=utc_now_iso(),
        )
    except Exception:
        pass


def _durable_command_seconds(status: dict[str, object]) -> float | None:
    """The command's runtime on the target's own clock, or None when unproven.

    The target records when the command was confirmed started (``started_at`` on
    the accepted path, ``acknowledged_at`` otherwise) and when it ended. Both are
    target epoch seconds, so controller clock skew and detached time do not count.
    """
    ended = status.get("ended_at")
    began = status.get("started_at")
    if not _positive_number(began):
        began = status.get("acknowledged_at")
    if not (_positive_number(ended) and _positive_number(began)):
        return None
    seconds = float(ended) - float(began)
    return round(seconds, 3) if seconds >= 0 else None


def _durable_record_path(rdir: Path) -> Path:
    return rdir / "durable.json"


def _durable_run_lock(
    plan: RunPlan, run_id: str, reporter: Reporter, *, adopt_dead_run: bool = False
) -> ProjectLock:
    """The lock held by the one process waiting on or finalizing a durable run.

    An ordinary run is serialized by its project lock. An untransferred or commit run
    takes none, so whoever waits on it (the launching ``run``, ``resume`` or
    ``cancel``) holds a lock keyed to that run alone. It never conflicts with the
    project's own lock, keeps two finalizations of one run from racing, and tells
    ``cancel`` that a controller is still waiting. Dead holders are reclaimed like any
    project lock.
    """
    if plan.transfer_mode == "safe":
        return ProjectLock(
            plan.project.project_id, plan.target.name, scope=plan.write_scope,
            run_id=run_id, adopt_dead_run=adopt_dead_run, report=reporter.event,
        )
    kind = "untransferred" if plan.transfer_mode == "none" else plan.transfer_mode
    return ProjectLock(
        f"{plan.project.project_id} {kind} run {run_id}", plan.target.name,
        run_id=run_id, adopt_dead_run=adopt_dead_run, report=reporter.event,
    )


def _durable_record_token(record: dict[str, object]) -> str:
    """Return the saved resume token, refusing to invent one from malformed state.

    ``str(record["resume_token"])`` turns a missing or null credential into the
    literal ``"None"`` — a non-empty string that satisfies every later validity
    check and reaches the target looking like a real token.  The target then
    reports an authentication failure, which reads as "the credential was
    rejected" rather than "this controller no longer has one".  A record that
    cannot produce its credential keeps its fence and says why.
    """
    token = record.get("resume_token")
    if not isinstance(token, str) or not token:
        raise TransportError(
            "saved durable record has no usable resume token; "
            "its target state must be resolved by hand"
        )
    return token


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
    exit_code: int,
) -> int:
    """Stop waiting on an acknowledged durable run without finalizing it.

    ``exit_code`` is EXIT_OK only when the caller asked to stop waiting (``--no-wait``
    or an interrupt); a lost connection is EXIT_INFRA so ``run --durable && next``
    never proceeds as if the job had finished.
    """
    guidance = (
        f"the job keeps running on {record['target']}; run `remrun resume {record['run_id']}` "
        "from this project to wait for it, finalize, and pull its outputs back"
    )
    summary = read_json(summary_path) or {"run_id": record["run_id"]}
    summary.update(
        phase="durable_wait",
        completion_state="detached",
        terminal=False,
        durable_state=status.get("state", "running_or_pending"),
        durable_acknowledged=True,
        detached_reason=reason,
        guidance=guidance,
        plan=record["plan"],
    )
    write_json(summary_path, summary)
    reporter.event(
        "durable_detached",
        run_id=record["run_id"],
        state=status.get("state", "running_or_pending"),
        exit_code=exit_code,
        note=guidance,
    )
    return exit_code


def _running_observation_matches(transport, record: dict[str, object]) -> bool:  # noqa: ANN001
    payload = transport.query_observed_jobs(sample_interval=0.05, timeout=30.0)
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(jobs, list):
        raise DurableStateError("observer query returned malformed jobs")
    run_id = observed_job_id(record["run_id"])
    matches = [job for job in jobs if isinstance(job, dict) and job.get("job_id") == run_id]
    if len(matches) != 1:
        return False
    job = matches[0]
    expected = {
        "project": observed_project_id(record["project_id"]),
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


def _cancel_gate(reporter: Reporter, rdir: Path, run_id: str) -> str | None:
    """None when no cancel stands between this run and finalization, else why one does.

    The target reports a cancelled run complete as soon as the observer wrapping it
    exits, which on POSIX is at the first SIGTERM; a member that ignores it runs on
    until the cancel's SIGKILL. So the saved cancel request, re-read from disk because
    another process may be making it, decides: none, or a confirmed stop, proceeds; a
    refusal that signalled nothing proves the completion genuine; a pending request is
    waited on for a bounded time; anything else leaves completion unknown.
    """
    deadline: float | None = None
    while True:
        record = read_json(_durable_record_path(rdir))
        if not isinstance(record, dict):
            return "the saved durable record is missing"
        cancel = record.get("cancel")
        if cancel is None:
            return None
        if not isinstance(cancel, dict):
            return "the saved cancel request is malformed"
        outcome = cancel.get("owned_job_cancelled")
        if outcome is True or (outcome is False and cancel.get("signals_sent") is False):
            return None
        if outcome is not None:
            return (
                "a cancel signalled the job but the target did not confirm that all of "
                "its processes are gone"
            )
        if deadline is None:
            deadline = time.monotonic() + DURABLE_CANCEL_PENDING_SECONDS
            reporter.event(
                "cancel_pending", run_id=run_id, wait_s=DURABLE_CANCEL_PENDING_SECONDS,
                note="waiting for the cancel to confirm the job's processes are gone",
            )
        if time.monotonic() >= deadline:
            return "a cancel request has not recorded whether it stopped the job"
        time.sleep(0.5)


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

    untransferred = plan.transfer_mode == "none"
    commit = plan.transfer_mode == "commit"
    per_run = plan.transfer_mode != "safe"
    blocked = _cancel_gate(reporter, rdir, run_id)
    if blocked is not None:
        guidance = (
            f"if that `remrun cancel {run_id}` is still running, run `remrun resume "
            f"{run_id}` from this project after it ends; otherwise use a read-only "
            "process/artifact probe to prove the command's processes ended, then run "
            f"`remrun resolve-unknown {run_id} --confirmed-ended`"
        )
        summary = existing if isinstance(existing, dict) else {"run_id": run_id}
        summary.update(
            phase="durable_wait", completion_state="unknown", terminal=True,
            error=blocked, guidance=guidance, durable=True, plan=plan.as_dict(),
        )
        write_json(summary_path, summary)
        reporter.event("completion_unknown", run_id=run_id, reason=blocked, guidance=guidance)
        return EXIT_INFRA

    payload = transport.durable_status(
        run_id, _durable_record_token(record), include_logs=True
    )
    if not isinstance(payload, dict) or not isinstance(payload.get("status"), dict):
        raise TransportError("durable result payload is malformed")
    status = payload["status"]
    _validate_durable_identity(status, record, require_complete=True)
    execution = record.get("execution")
    if not isinstance(execution, dict):
        raise UnknownCompletionHazardError("saved durable execution metadata is missing")
    verify_fields: dict[str, object] = (
        {"verify": record.get("verify")} if untransferred
        else {"source": record.get("source"), "work": record.get("work")} if commit
        else {}
    )
    work = record.get("work") if commit else None
    if commit and not (
        isinstance(work, dict)
        and all(isinstance(work.get(key), str) for key in ("id", "path", "tree"))
    ):
        raise UnknownCompletionHazardError("saved commit-run work record is malformed")
    try:
        result = finalize_durable_result(payload, execution)
    except CommandNotStartedError as exc:
        clear_unknown_completion_hazard(
            plan.project.project_id, run_id, per_run=per_run
        )
        if work is not None:  # proved never started: its tree goes
            remove_commit_work(
                transport, plan.target.name, {**work, "state": "pending_removal"},
                reporter.event,
            )
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
            transport.durable_cleanup(run_id, _durable_record_token(record))
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
        "phase": "finalize" if untransferred or bool(record["no_pullback"]) else "pullback",
        "completion_state": "command_complete",
        "command_started": guarded_started,
        "command_exit_code": command_exit_code_checkpoint,
        "terminal": False,
        "durable": True,
        "bootstrap": record.get("bootstrap"),
        **verify_fields,
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
    commit_final = None
    if untransferred:
        # No launch manifests exist for this run (both read back empty), so a pullback
        # here would treat every target path as a command output. Outputs stay put.
        pass
    elif commit:
        try:
            commit_final = finalize_commit_outputs(
                transport=transport, plan=plan, run_id=run_id, rdir=rdir, work=work,
                policy=policy, command_clean=not guard_failed and result.exit_code == 0,
                command_note=(
                    f"memory guard {guard_status}" if guard_failed
                    else f"command exited {result.exit_code}"
                ),
                report=reporter.event,
            )
        except TransportError as exc:
            # The fence (and the output lease) stands, so `resume` retries this pullback.
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
                **verify_fields,
                "plan": plan.as_dict(),
            })
            return EXIT_GUARD if guard_failed else EXIT_TRANSFER
        post = commit_final.post
        verify_fields = {**verify_fields, "work": commit_final.work}
        for rel in post.conflicts:
            reporter.event(
                "postrun_conflict", path=rel, saved=str(conflict_dir(run_id) / "remote" / rel),
            )
    elif not bool(record["no_pullback"]):
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
                include_paths=(
                    plan.bootstrap.lock_inputs
                    if plan.bootstrap is not None and plan.bootstrap.status == "bootstrap-needed"
                    else ()
                ),
                report=reporter.event,
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

    postrun_unresolved = bool(post and post.conflicts) or bool(
        commit_final and commit_final.unresolved
    )
    if guard_failed:
        final_exit = EXIT_GUARD
    elif result.exit_code != 0:
        final_exit = result.exit_code
    elif postrun_unresolved:
        final_exit = EXIT_CONFLICT
    else:
        final_exit = EXIT_OK

    # A durable run's controller may have been detached for hours, so its own wall
    # clock says nothing about the job. Report the command's runtime as the target
    # recorded it, or None when the target did not record both ends.
    duration = _durable_command_seconds(status)
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
        "duration_sec": duration,
        "files_pulled_post": len(post.pulled) if post else 0,
        "postrun_conflicts": len(post.conflicts) if post else 0,
        "telemetry": result.telemetry,
        "memory_guard": guard_result,
        "durable": True,
        "bootstrap": record.get("bootstrap"),
        **verify_fields,
        "plan": plan.as_dict(),
    }
    if commit_final is not None and commit_final.undelivered:
        # An output not cleanly delivered lives only in the retained tree: the fence
        # stays, and the summary goes straight to complete, so `resume` never reaches
        # the finalization_complete step that clears it.
        checkpoint["guidance"] = _kept_lease_guidance(
            run_id, plan, work["tree"],
            _undelivered_outputs_why(command_exit_code, commit_final.undelivered),
        )
        reporter.event("output_lease_kept", run_id=run_id, guidance=checkpoint["guidance"])
    else:
        write_json(summary_path, checkpoint)
        if not clear_unknown_completion_hazard(
            plan.project.project_id, run_id, per_run=per_run
        ):
            raise UnknownCompletionHazardError(
                f"durable completion fence changed before finalization for {run_id!r}"
            )
    checkpoint["completion_state"] = "complete"
    checkpoint["terminal"] = True
    write_json(summary_path, checkpoint)
    # Same admission rule as an ordinary run: only a successful command with a
    # positively attributable pullback (none for an untransferred run) is evidence of
    # a reusable job cost. The trip time is not comparable (the controller may have
    # been detached), so only the target-measured command time is learned.
    pullback_attributable = untransferred or commit_final is not None or (
        not bool(record["no_pullback"])
        and post is not None
        and not post.conflicts
        and post.next_baseline is not None
    )
    if (
        not guard_failed
        and result.exit_code == 0
        and final_exit == EXIT_OK
        and pullback_attributable
        and duration is not None
    ):
        _record_job_profile(
            plan, result.telemetry, guard_result, exec_s=duration, trip_s=None
        )
    reporter.event(
        "command_finished",
        exit_code=final_exit,
        command_exit_code=command_exit_code,
        durable=True,
    )
    if commit_final is not None:
        # After the checkpoint, so a crash here can never re-run the pullback against a
        # removed tree: resume sees the checkpoint and only retries this removal.
        checkpoint["work"] = remove_commit_work(
            transport, plan.target.name, commit_final.work, reporter.event
        )
        write_json(summary_path, checkpoint)
    try:
        transport.durable_cleanup(run_id, _durable_record_token(record))
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
        # The launch stream dropped, but an authenticated status read already
        # succeeded, so the target is reachable: keep waiting rather than detach.
        reporter.event(
            "durable_launch_reconnected", run_id=run_id, state=status.get("state")
        )
    # Separate budgets: a working status read must not reset the ownership check's.
    failure_since: dict[str, float] = {}
    failures: dict[str, int] = {}
    poll_delay = 1.0

    def retry_read(kind: str, exc: TransportError) -> bool:
        """Schedule a backed-off re-read; False once this read's retry budget is spent.

        The wait itself happens in the poll's sleep below, where an interrupt after
        acknowledgement still detaches cleanly.
        """
        nonlocal poll_delay
        now = time.monotonic()
        since = failure_since.setdefault(kind, now)
        failures[kind] = failures.get(kind, 0) + 1
        if now - since >= DURABLE_STATUS_RETRY_SECONDS:
            return False
        poll_delay = min(30.0, 2.0 ** min(failures[kind], 5))
        reporter.event(
            "durable_status_retry",
            run_id=run_id,
            read=kind,
            attempt=failures[kind],
            retry_in_s=poll_delay,
            message=str(exc),
        )
        return True

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
                        run_id, _durable_record_token(record)
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
                # Retried on the next pass, after the poll below waits out the backoff.
                if not retry_read("ownership", exc):
                    return _detached_durable(
                        reporter, summary_path, record, status, reason=str(exc),
                        exit_code=EXIT_INFRA,
                    )
        if no_wait and acknowledged:
            return _detached_durable(
                reporter, summary_path, record, status, reason="resume requested --no-wait",
                exit_code=EXIT_OK,
            )
        try:
            time.sleep(poll_delay)
            next_status = transport.durable_status(
                run_id, _durable_record_token(record)
            )
            _validate_durable_identity(next_status, record)
            status = next_status
            failure_since.pop("status", None)
            failures.pop("status", None)
            poll_delay = 1.0
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
                    exit_code=EXIT_OK,
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
            if retry_read("status", exc):
                continue
            if acknowledged:
                return _detached_durable(
                    reporter, summary_path, record, status, reason=str(exc),
                    exit_code=EXIT_INFRA,
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
    bootstrap_result=None,
    command: list[str] | None = None,
    verify_fields: dict[str, object] | None = None,
    work: CommitWork | None = None,
) -> int:
    del t0, pre, backup_root
    untransferred = plan.transfer_mode == "none"
    # Untransferred and commit runs fence only themselves.
    per_run = plan.transfer_mode != "safe"
    verify_fields = verify_fields or {}
    runenv = resolve_run_env(
        device=plan.target, project=plan.project, project_config=plan.project_config
    )
    dispatch_command = list(command if command is not None else plan.command)
    if not untransferred:  # an untransferred run never writes into the target's tree
        transport.ensure_remote_dir(remote_cwd)
    resume_token = secrets.token_urlsafe(32)
    source_controller = controller_label()
    observation = JobObservation.for_command(
        job_id=run_id,
        project=plan.project.project_id,
        target=plan.target.name,
        phase="command",
        command=dispatch_command,
        declared_label=getattr(args, "label", None),
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
        "bootstrap": bootstrap_result.as_dict() if bootstrap_result is not None else None,
        "execution": None,
        **verify_fields,
    }
    write_json(_durable_record_path(rdir), record)
    try:
        hazard = _publish_completion_fence(
            reporter=reporter, plan=plan, run_id=run_id, summary_path=summary_path,
            started_at=started_at, verified=list(verify_fields.get("verify") or []),
        )
    except (_PreflightConflict, LockError, UnknownCompletionHazardError):
        # Refused before launch: a run that never started keeps no launch record.
        _durable_record_path(rdir).unlink(missing_ok=True)
        raise
    if work is not None:
        work.dispatched = True  # from here the command may run; its tree is kept
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
        "guidance": _unknown_completion_guidance(hazard, per_run=per_run),
        "durable": True,
        **verify_fields,
        "plan": plan.as_dict(),
    })
    telemetry_on = telemetry_default and not args.no_telemetry
    reporter.event("durable_launch", run_id=run_id, target=plan.target.name)
    try:
        status, execution = transport.launch_durable(
            dispatch_command,
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
        clear_unknown_completion_hazard(
            plan.project.project_id, run_id, per_run=per_run
        )
        if work is not None:
            work.dispatched = False  # proved never started: its tree is removed
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
        clear_unknown_completion_hazard(
            plan.project.project_id, run_id, per_run=per_run
        )
        if work is not None:
            work.dispatched = False  # proved never started: its tree is removed
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
    # Why the target was chosen and the run's label describe the run; they are carried,
    # while every field that decides what runs where must still rebuild exactly.
    saved_reason, saved_label = saved_plan.get("target_reason"), saved_plan.get("label")
    if not all(value is None or isinstance(value, str) for value in (saved_reason, saved_label)):
        raise UnknownCompletionHazardError("saved durable plan is malformed")
    # So is the admission grant the run holds until it is proven to have ended.
    saved_admission = saved_plan.get("admission")
    if saved_admission is not None and not isinstance(saved_admission, dict):
        raise UnknownCompletionHazardError("saved durable plan is malformed")
    # The run's transfer mode and its verified, output and input files are part of what
    # ran, so they come from the saved plan, never from today's [transfer] mode default.
    saved_mode = saved_plan.get("transfer_mode")
    saved_paths = {
        key: saved_plan.get(key, []) for key in ("verify", "outputs", "inputs")
    }
    if not isinstance(saved_mode, str) or not all(
        isinstance(paths, list) and all(isinstance(path, str) for path in paths)
        for paths in saved_paths.values()
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
        transfer_mode=saved_mode,
        verify=saved_paths["verify"],
        outputs=saved_paths["outputs"],
        inputs=saved_paths["inputs"],
    )
    plan = replace(plan, target_reason=saved_reason, label=saved_label,
                   admission=saved_admission)
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


def _retry_commit_work_removal(
    plan: RunPlan, summary: dict, summary_path: Path, reporter: Reporter
) -> None:
    """Finish removing a finalized commit run's clean tree, or retry a failed removal."""
    work = summary.get("work")
    if (
        plan.transfer_mode != "commit"
        or not isinstance(work, dict)
        or work.get("state") not in {"pending_removal", "remove_failed"}
    ):
        return
    summary["work"] = remove_commit_work(
        make_transport(plan.target), plan.target.name, work, reporter.event
    )
    write_json(summary_path, summary)


def cmd_resume(args: argparse.Namespace, reporter: Reporter) -> int:
    plan, record, rdir, summary_path, config = _load_resume_plan(args.run_id)
    # An untransferred or commit run is fenced by its own record and never held the
    # project lock.
    untransferred = plan.transfer_mode != "safe"
    fence_run = args.run_id if untransferred else None
    summary = read_json(summary_path)
    if (
        isinstance(summary, dict)
        and summary.get("completion_state") == "complete"
        and summary.get("terminal") is True
    ):
        code = summary.get("exit_code")
        if isinstance(code, int) and not isinstance(code, bool):
            _retry_commit_work_removal(plan, summary, summary_path, reporter)
            if summary.get("durable_cleanup_complete") is not True:
                try:
                    make_transport(plan.target).durable_cleanup(
                        args.run_id, _durable_record_token(record)
                    )
                except TransportError as exc:
                    reporter.event(
                        "durable_cleanup_deferred", run_id=args.run_id, message=str(exc)
                    )
                else:
                    summary["durable_cleanup_complete"] = True
                    write_json(summary_path, summary)
            reporter.event("durable_already_finalized", run_id=args.run_id, exit_code=code)
            _release_after_recovery(plan, args.run_id, reporter, config)
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
        hazard = read_unknown_completion_hazard(plan.project.project_id, run_id=fence_run)
        if hazard is not None:
            if (
                hazard.get("run_id") != args.run_id
                or hazard.get("target") != plan.target.name
            ):
                raise UnknownCompletionHazardError(
                    "durable completion fence changed after finalization checkpoint"
                )
            if not clear_unknown_completion_hazard(
                plan.project.project_id, args.run_id, per_run=untransferred
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
        _retry_commit_work_removal(plan, summary, summary_path, reporter)
        try:
            make_transport(plan.target).durable_cleanup(
                args.run_id, _durable_record_token(record)
            )
        except TransportError as exc:
            reporter.event(
                "durable_cleanup_deferred", run_id=args.run_id, message=str(exc)
            )
        else:
            summary["durable_cleanup_complete"] = True
            write_json(summary_path, summary)
        _release_after_recovery(plan, args.run_id, reporter, config)
        return code
    hazard = read_unknown_completion_hazard(plan.project.project_id, run_id=fence_run)
    if hazard is None or hazard.get("run_id") != args.run_id or hazard.get("target") != plan.target.name:
        raise UnknownCompletionHazardError(
            "durable completion fence is missing or names a different run"
        )
    transport = make_transport(plan.target)
    lock = _durable_run_lock(plan, args.run_id, reporter, adopt_dead_run=True).acquire()
    try:
        status = transport.durable_status(args.run_id, _durable_record_token(record))
        _validate_durable_identity(status, record)
        code = _poll_durable(
            args=args, reporter=reporter, plan=plan, transport=transport,
            remote_root=str(record["remote_root"]), run_id=args.run_id,
            rdir=rdir, summary_path=summary_path, policy=load_retention(load_config()),
            record=record, initial_status=status, no_wait=bool(args.no_wait),
        )
        _release_after_recovery(plan, args.run_id, reporter, config)
        return code
    finally:
        lock.release()


def _durable_observation(record: dict[str, object], plan: RunPlan) -> JobObservation:
    """The exact observer identity this durable run registered at launch.

    ``_run_durable_locked`` registers durable runs in phase ``command``. The saved
    digest is authoritative because the dispatched argv can differ from the plan's.
    The label is display-only and plays no part in the observer's match.
    """
    return replace(
        JobObservation.for_command(
            job_id=str(record["run_id"]), project=str(record["project_id"]),
            target=str(record["target"]), phase="command", command=plan.command,
            source_controller=str(record["controller"]),
        ),
        command_sha256=str(record["command_sha256"]),
    )


def _request_durable_stop(
    *,
    reporter: Reporter,
    transport,  # noqa: ANN001
    plan: RunPlan,
    record: dict[str, object],
    rdir: Path,
    status: dict[str, object],
    hazard: dict[str, object],
    summary_path: Path | None,
) -> bool:
    """Record the request, then ask the target observer to stop exactly this run.

    Nothing is signalled until the target has acknowledged a running command and
    the saved launch record is complete: before that, ownership is not published.
    ``summary_path`` is None when another controller process is waiting on the run
    and owns its summary. True only when the observer confirms the tree is gone.
    """
    run_id = str(record["run_id"])
    guidance = _unknown_completion_guidance(hazard, per_run=plan.transfer_mode != "safe")
    if (
        status.get("state") != "running"
        or status.get("acknowledged") is not True
        or not isinstance(record.get("execution"), dict)
    ):
        reporter.event(
            "cancel_refused", run_id=run_id, state=status.get("state"),
            reason="the run is still starting; its process ownership is not published yet",
            guidance=guidance,
        )
        return False
    # One cancel of a run at a time: the earlier evidence is read, the job signalled
    # and the outcome saved as one step, so a concurrent cancel that signals nothing
    # can never overwrite another's "signalled but unconfirmed" with "nothing sent".
    cancel_lock = ProjectLock(
        f"{plan.project.project_id} cancel {run_id}", plan.target.name, run_id=run_id,
        report=reporter.event,
    )
    try:
        cancel_lock.acquire(wait_seconds=DURABLE_CANCEL_PENDING_SECONDS)
    except LockError as exc:
        reporter.event(
            "cancel_refused", run_id=run_id,
            reason=f"another cancel of this run is still in progress: {exc}",
            guidance=guidance,
        )
        return False
    try:
        return _request_durable_stop_locked(
            reporter=reporter, transport=transport, plan=plan, record=record, rdir=rdir,
            summary_path=summary_path, run_id=run_id, guidance=guidance,
        )
    finally:
        cancel_lock.release()


def _request_durable_stop_locked(
    *,
    reporter: Reporter,
    transport,  # noqa: ANN001
    plan: RunPlan,
    record: dict[str, object],
    rdir: Path,
    summary_path: Path | None,
    run_id: str,
    guidance: str,
) -> bool:
    # An earlier cancel that may have signalled the job without confirming the stop
    # stays unproven: this cancel signalling nothing does not show nothing was stopped.
    earlier = (read_json(_durable_record_path(rdir)) or {}).get("cancel")
    earlier_unproven = (
        isinstance(earlier, dict)
        and earlier.get("owned_job_cancelled") is not True
        and earlier.get("signals_sent") is not False
    )
    request: dict[str, object] = {"requested_at": utc_now_iso(), "owned_job_cancelled": None}

    def save() -> None:
        # Merge into the record as it is on disk now, not the copy loaded earlier.
        current = read_json(_durable_record_path(rdir))
        if not isinstance(current, dict):
            current = record
        current["cancel"] = request
        record["cancel"] = request
        write_json(_durable_record_path(rdir), current)
        if summary_path is not None:
            summary = read_json(summary_path) or {"run_id": run_id}
            summary["cancel"] = request
            write_json(summary_path, summary)

    save()
    reporter.event("cancel_requested", run_id=run_id, target=plan.target.name)
    try:
        cancelled, signals_sent, detail = transport.cancel_observed(
            _durable_observation(record, plan), cwd=str(record["remote_cwd"]), timeout=60.0
        )
    except TransportError as exc:
        cancelled, signals_sent, detail = False, None, str(exc)
    if not cancelled and signals_sent is False and earlier_unproven:
        signals_sent = None
    # A waiting controller finalizes only once this outcome is saved (_cancel_gate).
    request["owned_job_cancelled"] = cancelled
    request["signals_sent"] = signals_sent
    if detail:
        request["detail"] = detail
    save()
    if not cancelled:
        reporter.event(
            "completion_unknown", run_id=run_id,
            reason=f"the target did not confirm the stop: {detail}", guidance=guidance,
        )
    return cancelled


def cmd_cancel(args: argparse.Namespace, reporter: Reporter) -> int:
    """Stop a durable run's exact registered process tree, then finalize it as resume does.

    The stop goes to the target observer's registry row for this run (job id,
    command digest, project, target, source controller and phase), never to a
    process group read from ``ps``. A run whose completion is already recorded is
    handled exactly like ``resume``. When another controller process is still
    waiting on the run, this only requests the stop and that process finalizes.
    Anything that cannot be proved leaves completion unknown.
    """
    run_id = args.run_id
    resume_args = argparse.Namespace(run_id=run_id, no_wait=False, json=args.json)
    plan, record, rdir, summary_path, config = _load_resume_plan(run_id)
    summary = read_json(summary_path)
    if isinstance(summary, dict) and summary.get("completion_state") in {
        "complete", "finalization_complete",
    }:
        return cmd_resume(resume_args, reporter)
    # An untransferred or commit run is fenced by its own record, not the project's.
    hazard = read_unknown_completion_hazard(
        plan.project.project_id, run_id=run_id if plan.transfer_mode != "safe" else None
    )
    if hazard is None or hazard.get("run_id") != run_id or hazard.get("target") != plan.target.name:
        raise UnknownCompletionHazardError(
            "durable completion fence is missing or names a different run"
        )
    transport = make_transport(plan.target)
    token = _durable_record_token(record)
    stop = {
        "reporter": reporter, "transport": transport, "plan": plan, "record": record,
        "rdir": rdir, "hazard": hazard,
    }
    if run_id in _durable_run_lock(plan, run_id, reporter).live_holder_run_ids():
        # The original `run` (or a `resume`) is still waiting on this run and owns its
        # summary and finalization; racing it could pull outputs back twice.
        status = transport.durable_status(run_id, token)
        _validate_durable_identity(status, record)
        if status.get("state") == "failed":
            # Completion is unknown, exactly as the waiting controller will report it.
            reporter.event(
                "completion_unknown", run_id=run_id,
                reason="target durable state is missing, corrupt, mismatched, or ambiguous",
                guidance=_unknown_completion_guidance(
                    hazard, per_run=plan.transfer_mode != "safe"
                ),
            )
            return EXIT_INFRA
        if status.get("state") != "complete" and not _request_durable_stop(
            **stop, status=status, summary_path=None
        ):
            return EXIT_INFRA
        reporter.event(
            "cancel_handed_off", run_id=run_id, state=status.get("state"),
            note=(
                "the controller already waiting on this run finalizes it; if that "
                f"process stops waiting, run `remrun resume {run_id}`"
            ),
        )
        return EXIT_OK
    lock = _durable_run_lock(plan, run_id, reporter, adopt_dead_run=True).acquire()
    try:
        status = transport.durable_status(run_id, token)
        _validate_durable_identity(status, record)
        if status.get("state") not in {"complete", "failed"}:
            if not _request_durable_stop(**stop, status=status, summary_path=summary_path):
                return EXIT_INFRA
            deadline = time.monotonic() + DURABLE_CANCEL_SETTLE_SECONDS
            while status.get("state") not in {"complete", "failed"}:
                if time.monotonic() >= deadline:
                    reporter.event(
                        "completion_unknown", run_id=run_id,
                        reason="the stop was confirmed but the target has not recorded "
                               "the command's exit yet",
                        guidance=f"run `remrun resume {run_id}` from this project",
                    )
                    return EXIT_INFRA
                time.sleep(0.5)
                try:
                    status = transport.durable_status(run_id, token)
                except DurableStateError:
                    raise
                except TransportError:
                    continue
                _validate_durable_identity(status, record)
        code = _poll_durable(
            args=resume_args, reporter=reporter, plan=plan, transport=transport,
            remote_root=str(record["remote_root"]), run_id=run_id, rdir=rdir,
            summary_path=summary_path, policy=load_retention(config), record=record,
            initial_status=status,
        )
        if "cancel" in record:
            final = read_json(summary_path) or {"run_id": run_id}
            final["cancel"] = record["cancel"]
            write_json(summary_path, final)
        _release_after_recovery(plan, run_id, reporter, config)
        return code
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
    # An untransferred run's fence is its own record; it never blocks the project.
    per_run = hazard is None or hazard["run_id"] != args.run_id
    if per_run:
        run_fence = read_unknown_completion_hazard(project.project_id, run_id=args.run_id)
        if run_fence is not None:
            hazard = run_fence
        elif hazard is None:
            raise UnknownCompletionHazardError(
                f"project {project.project_id!r} has no unknown-completion hazard "
                f"for run {args.run_id!r}"
            )
        else:
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

    if not clear_unknown_completion_hazard(project.project_id, args.run_id, per_run=per_run):
        raise UnknownCompletionHazardError(
            f"unknown-completion hazard changed while resolving run {args.run_id!r}"
        )
    _release_confirmed_ended(args.run_id, reporter)
    reporter.event(
        "unknown_completion_resolved",
        project_id=project.project_id,
        target=hazard["target"],
        run_id=args.run_id,
        note=(
            "commit run: its output lease is released; resolving pulls nothing back, and "
            "its private tree stays on the target until a later commit run prunes it"
            if per_run and "outputs" in hazard
            else "untransferred run: nothing was transferred and no baseline exists"
            if per_run
            else "baseline unchanged; next run will perform normal whole-project preflight"
        ),
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
