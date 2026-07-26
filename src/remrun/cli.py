from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

from .config import (
    load_config, load_project_config, load_retention, offload_policy, offload_threshold,
    scheduler_config,
)
from .gitsync import HOOK_BEGIN
from .models import RunPlan
from .output import Reporter
from .planner import make_run_plan
from .project import ProjectDetectionError, detect_project, find_project_config
from .profile import (
    LOCAL_DEVICE, command_key, device_profile, load_job_costs, load_profiles,
    merge_job_costs, predict_job, recommend_offload, update_job_costs, update_profile,
)
from .scheduler import pick_by_load
from .reconcile import postrun_pullback, preflight_reconcile
from .runenv import resolve_run_env
from .scopes import configured_scope_names
from .state import (
    ProjectLock,
    LockError,
    RetentionPolicy,
    cap_text,
    conflict_dir,
    default_state_root,
    new_run_id,
    prune_state,
    read_baseline,
    read_json,
    run_dir,
    utc_now_iso,
    write_baseline,
    write_json,
    write_manifest,
)
from .transport import TransportError, make_transport

KNOWN_COMMANDS = {
    "devices", "doctor", "plan", "run", "status", "logs", "clean", "bench", "sync",
    "git-sync", "runner", "action",
}

# Exit codes (see docs/AGENT_OUTPUT_SPEC.md).
EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_CONFLICT = 2
EXIT_TRANSFER = 3
EXIT_INFRA = 4


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
        parser.print_help()
        return EXIT_INTERNAL
    except Exception as exc:  # Keep agent-visible error concise.
        reporter.event("error", type=type(exc).__name__, message=str(exc))
        return EXIT_INTERNAL


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="remrun")
    sub = parser.add_subparsers(dest="command_name", required=True)

    p = sub.add_parser("devices", help="List configured devices")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("doctor", help="Check basic local configuration")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("plan", help="Plan a remote run without mutating files")
    p.add_argument("target", nargs="?", default="auto")
    p.add_argument("--auto", action="store_true", help="Pick the target device automatically")
    p.add_argument("--scope", help="declared [parallel.scopes.<name>] write scope for this run")
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
    p.add_argument("--dry-run", action="store_true", help="Print plan and do not execute")
    p.add_argument("--no-pullback", action="store_true", help="Skip post-run pullback")
    p.add_argument("--no-telemetry", action="store_true", help="Skip resource telemetry")

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
    p.add_argument("--branch", help="only fast-forward this branch")
    p.add_argument("--bootstrap", action="store_true",
                   help="seed a repo-less project from the peer's history (git init + "
                        "full-history fetch); the working tree is left untouched")
    p.add_argument("--dry-run", action="store_true",
                   help="verify both repos but do not fetch or fast-forward")
    p.add_argument("--status", action="store_true",
                   help="report branch state, dirty flags, hook, and diagnostics without mutating")
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


def cmd_action(args: argparse.Namespace, reporter: Reporter) -> int:
    from .action import run_action

    result = run_action(load_config(), args.device, args.action, args.input,
                        key=args.key, dry_run=args.dry_run)
    reporter.event("action_result", **result.as_dict())
    return result.exit_code


def _store_costs_in_project(config) -> bool:
    """Whether to persist a job's PORTABLE resource costs into the project's
    do/remrun/job_costs.json (so they travel with the project, not the controller).
    Default on; disable with [profile] store_in_project = false in defaults.toml."""
    return bool(config.defaults.get("profile", {}).get("store_in_project", True))


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
    db = state_root / "fleet" / "fleet.db"
    summary: dict[str, object] = {"db": str(db), "exists": db.exists()}
    if not db.exists():
        return summary
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        summary["error"] = str(exc)
        return summary
    try:
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


def _probe_candidates(plan: RunPlan, config, *, check_git: bool) -> list[dict]:
    """Live per-candidate readiness for an orchestrator choosing a device.

    OPT-IN and never on the run path: probing costs a round-trip per device (the POSIX
    load sample runs `top -l 2`, ~2 s), which is why `plan` stays probe-free by default.

    Reports, per candidate: reachability, live CPU busy %, static capacity, and the
    derived spare perf-core-equivalents that `pick_by_load` actually ranks on — so the
    caller sees the same number the scheduler would use, not a raw percentage it would
    have to re-derive. Optionally adds git divergence.

    Every field is independently nullable: an unreachable device, a backend that cannot
    sample load, and a project with no git all degrade to `null` on that field alone
    rather than dropping the candidate. `null` means UNKNOWN, never "fine".
    """
    sched = scheduler_config(config)
    eff_w = float(sched.get("eff_core_weight", 1.0))
    out: list[dict] = []
    for device in plan.candidates:
        entry: dict = {
            "name": device.name,
            "perf_cores": device.perf_cores,
            "eff_cores": device.eff_cores,
            "ram_gb": device.ram_gb,
            "capacity_perf_core_equiv": round(device.cpu_capacity(eff_w), 2) or None,
            "reachable": None, "cpu_busy_pct": None, "spare_perf_core_equiv": None,
            "max_jobs": device.max_jobs,
        }
        try:
            transport = make_transport(device)
            probe = transport.probe()
            entry["reachable"] = bool(probe.reachable)
            if not probe.reachable:
                entry["detail"] = probe.detail
            else:
                busy = transport.sample_load()
                entry["cpu_busy_pct"] = busy
                cap = device.cpu_capacity(eff_w)
                if busy is not None and cap:
                    entry["spare_perf_core_equiv"] = round(
                        cap * (1.0 - min(max(busy, 0.0), 100.0) / 100.0), 2)
                if check_git:
                    entry["git"] = _candidate_git_state(transport, device, plan)
        except (TransportError, OSError) as exc:
            entry["reachable"] = False
            entry["detail"] = str(exc)
        out.append(entry)
    return out


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
    plan = make_run_plan(
        cwd=Path.cwd(),
        config=config,
        target_name="auto" if args.auto else args.target,
        command=command,
        scope_name=args.scope,
        json_events=args.json,
    )
    candidates = (_probe_candidates(plan, config, check_git=args.check_git)
                  if (args.probe or args.check_git) else None)
    if args.json:
        payload = plan.as_dict()
        if candidates is not None:
            payload["candidates_probed"] = candidates
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        reporter.event("project", project_id=plan.project.project_id, relative_cwd=plan.project.relative_cwd)
        reporter.event("target", name=plan.target.name, kind=plan.target.kind)
        reporter.event("command", argv=plan.command)
        reporter.event("transfer_mode", mode=plan.transfer_mode)
        reporter.event("write_scope", name=plan.write_scope or "project",
                       paths=plan.write_scope_paths)
        reporter.event("active_surface", excludes=len(plan.excludes), hash_below_bytes=plan.hash_below_bytes)
        for entry in (candidates or []):
            reporter.event("candidate_probe", **entry)
        reporter.event("project_config", path=str(plan.project_config_path) if plan.project_config_path else None)
        rec = recommend_offload(
            load_profiles(default_state_root()), plan.project.project_id,
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


def _resolve_target(plan: RunPlan, target_name: str | None, sched: dict, reporter: Reporter,
                    prediction: dict | None = None):
    """Probe candidates and pick the run target.

    Explicit target → probe it; reachable or fail (never fails over). For --auto,
    optionally narrow candidates by a job's learned RAM profile, walk them in
    preference order, probe reachability (and CPU load when load_balance is on),
    then pick via pick_by_load. Returns ``(device, transport, probe, reason)`` or
    ``None`` if nothing is reachable.
    """
    if target_name and target_name != "auto":
        device = plan.target
        transport = make_transport(device)
        probe = transport.probe()
        if not probe.reachable:
            reporter.event("unreachable", target=device.name, detail=probe.detail)
            return None
        return device, transport, probe, "explicit"

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
                    reporter.event("candidate_skipped", name=d.name, reason="insufficient_ram",
                                   detail=f"predicted ~{pred_rss}MB > {frac:.0%} of {d.ram_gb}GB")
            candidates = fits
        elif not fits:
            reporter.event("ram_warning",
                           detail=f"predicted ~{pred_rss}MB exceeds all candidates; using largest-RAM")
            candidates = sorted(candidates, key=lambda d: d.ram_gb, reverse=True)
    if pred_dur is not None and pred_dur < float(sched.get("trivial_job_seconds", 30)):
        if load_balance:
            reporter.event("trivial_job", predicted_dur_s=pred_dur,
                           note="skipping load probe/reallocation")
        load_balance = False

    reachable: list = []  # (device, transport, probe, busy%)
    for device in candidates:
        transport = make_transport(device)
        probe = transport.probe()
        if not probe.reachable:
            reporter.event("candidate_skipped", name=device.name, reason="unreachable",
                           detail=probe.detail)
            continue
        busy = transport.sample_load() if load_balance else None
        reporter.event("candidate", name=device.name, cpu_busy_pct=busy,
                       perf_cores=device.perf_cores, eff_cores=device.eff_cores)
        reachable.append((device, transport, probe, busy))

    if not reachable:
        first = plan.candidates[0].name if plan.candidates else "?"
        reporter.event("unreachable", target=first, detail="no candidate reachable")
        return None

    ranked = [(d, busy) for (d, _t, _pr, busy) in reachable]
    chosen_device, balance_reason = pick_by_load(ranked, sched)
    device, transport, probe, _busy = next(x for x in reachable if x[0] is chosen_device)
    reason = balance_reason
    if reason == "auto" and plan.candidates and device is not plan.candidates[0]:
        reason = "auto-failover"  # preferred candidate was unreachable
    return device, transport, probe, reason


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
    )

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
    # Placement reads the local profiles with the project's PORTABLE job costs overlaid,
    # so --auto is RAM-headroom/cost aware even on a controller that's never run this job
    # before (the project's do/remrun/job_costs.json travels with it).
    profiles = load_profiles(default_state_root())
    if _store_costs_in_project(config):
        profiles = merge_job_costs(profiles, plan.project.project_id,
                                   load_job_costs(plan.project.local_project_root))
    prediction = predict_job(profiles, plan.project.project_id, command_key(plan.command))
    if prediction:
        reporter.event("job_profile", key=command_key(plan.command),
                       predicted_rss_mb=prediction.get("rss_mb"),
                       predicted_dur_s=prediction.get("dur_s"))
    selection = _resolve_target(plan, target_name, sched, reporter, prediction)
    if selection is None:
        run_id = new_run_id(plan.target.name, plan.project.project_id)
        write_json(run_dir(run_id) / "summary.json",
                   {"run_id": run_id, "error": "target unreachable", "plan": plan.as_dict()})
        return EXIT_INFRA
    chosen, transport, probe, reason = selection
    plan = replace(plan, target=chosen)

    run_id = new_run_id(plan.target.name, plan.project.project_id)
    rdir = run_dir(run_id)
    summary_path = rdir / "summary.json"

    reporter.event("run_id", run_id=run_id)
    reporter.event("project", project_id=plan.project.project_id, relative_cwd=plan.project.relative_cwd)
    reporter.event("target", name=plan.target.name, kind=plan.target.kind, reason=reason)
    reporter.event("target_reachable", address=probe.address, remote_os=probe.remote_os)
    reporter.event("write_scope", name=plan.write_scope or "project",
                   paths=plan.write_scope_paths)

    remote_root = transport.remote_project_path(plan.project)
    remote_cwd = transport.remote_join(remote_root, plan.project.relative_cwd)
    reporter.event("remote_cwd", path=remote_cwd)

    try:
        lock = ProjectLock(plan.project.project_id, plan.target.name,
                           scope=plan.write_scope).acquire()
    except LockError as exc:
        reporter.event("locked", message=str(exc))
        write_json(summary_path, {"run_id": run_id, "error": str(exc), "plan": plan.as_dict()})
        return EXIT_INTERNAL

    started_at = utc_now_iso()
    t0 = time.monotonic()
    try:
        return _run_locked(args, reporter, plan, transport, remote_root, remote_cwd,
                           run_id, rdir, summary_path, started_at, t0, policy, telemetry_default)
    finally:
        lock.release()
        # Prune even when a run errors out early (preflight/exec/pullback may already have
        # written backups), so repeated failures can't grow the state unbounded.
        try:
            # Exempt THIS run's conflict/backup dir: a just-saved recovery copy must not be
            # pruned by the size-budget pass before the summary reports its path (audit B7).
            prune_state(policy, exempt_run_id=run_id)
        except Exception:  # noqa: BLE001
            pass


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
) -> int:
    local_root = plan.project.local_project_root
    prev_local, prev_remote = read_baseline(plan.target.name, plan.project.project_id)
    backup_root = conflict_dir(run_id) / "backup"

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
        )
    except TransportError as exc:
        reporter.event("transfer_error", phase="preflight", message=str(exc))
        write_json(summary_path, {"run_id": run_id, "error": str(exc), "phase": "preflight",
                                  "plan": plan.as_dict()})
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
        return EXIT_CONFLICT

    write_manifest(rdir / "pre_local_manifest.json", pre.local_manifest)
    write_manifest(rdir / "pre_remote_manifest.json", pre.remote_manifest)
    reporter.event("preflight_summary", pulled=len(pre.pulled), pushed=len(pre.pushed),
                   deleted_remote=len(pre.deleted_remote), deleted_local=len(pre.deleted_local),
                   skipped_identical=len(pre.skipped_identical),
                   converged_conflicts=len(pre.converged_conflicts), conflicts=0)

    # --- execute -------------------------------------------------------------
    runenv = resolve_run_env(
        device=plan.target, project=plan.project, project_config=plan.project_config
    )
    if runenv.venv:
        reporter.event("venv", path=runenv.venv)
    if runenv.env or runenv.path_prepend:
        reporter.event("run_env", vars=sorted(runenv.env), path_prepend=len(runenv.path_prepend))
    telemetry_on = telemetry_default and not args.no_telemetry
    transport.ensure_remote_dir(remote_cwd)
    reporter.event("command_started", run_id=run_id, command=" ".join(plan.command))
    cmd_t0 = time.monotonic()
    try:
        result = transport.exec(plan.command, cwd=remote_cwd,
                                env=runenv.env, path_prepend=runenv.path_prepend,
                                telemetry=telemetry_on)
    except TransportError as exc:
        reporter.event("exec_error", message=str(exc))
        guidance = (
            "remote start/completion is unknown; do not retry or remove the live project lock "
            "until a read-only process/artifact probe shows the prior command ended"
        )
        reporter.event("completion_unknown", guidance=guidance)
        write_json(
            summary_path,
            {
                "run_id": run_id,
                "error": str(exc),
                "phase": "exec",
                "completion_state": "unknown",
                "guidance": guidance,
                "plan": plan.as_dict(),
            },
        )
        return EXIT_INFRA

    (rdir / "stdout.log").write_text(cap_text(result.stdout, policy.max_log_bytes), encoding="utf-8")
    (rdir / "stderr.log").write_text(cap_text(result.stderr, policy.max_log_bytes), encoding="utf-8")
    if result.stdout:
        sys.stdout.write(result.stdout)
        sys.stdout.flush()
    if result.stderr:
        sys.stderr.write(result.stderr)
        sys.stderr.flush()
    finished_fields = {"exit_code": result.exit_code, "duration_sec": round(time.monotonic() - t0, 3)}
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
            write_json(summary_path, {"run_id": run_id, "exit_code": result.exit_code,
                                      "error": str(exc), "phase": "pullback", "plan": plan.as_dict()})
            return EXIT_TRANSFER

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
        else:
            write_baseline(plan.target.name, plan.project.project_id,
                           post.local_manifest_after, post.post_remote_manifest)
    else:
        # Still record a baseline so future runs have delete evidence.
        write_baseline(plan.target.name, plan.project.project_id,
                       pre.local_manifest, pre.remote_manifest)

    duration = round(time.monotonic() - t0, 3)

    # A post-run conflict means the command ran but remrun could not converge local
    # state. If the command itself succeeded, surface that as a conflict (exit 2)
    # rather than reporting a clean success; the command's own code is preserved in
    # the summary as command_exit_code and in the command_finished event.
    postrun_unresolved = bool(post and post.conflicts)
    final_exit = EXIT_CONFLICT if (postrun_unresolved and result.exit_code == 0) else result.exit_code

    # Per-device resource + timing profile (EWMA; advisory for placement and the
    # offload recommendation). exec_s = remote command; trip_s = full round-trip
    # (push+exec+pullback); overhead = trip - exec. The controller-specific timing
    # (trip/overhead) + the LOCAL baseline live in local state; the PORTABLE resource
    # costs (rss/cpu/exec) also go to the project's do/remrun/job_costs.json so they
    # travel with the project. Best-effort; never breaks a run.
    try:
        tel = result.telemetry or {}
        ckey = command_key(plan.command)
        update_profile(default_state_root(), plan.project.project_id, ckey, plan.target.name,
                       peak_rss_mb=tel.get("peak_rss_mb"),
                       avg_cpu_pct=tel.get("avg_cpu_pct"),
                       exec_s=exec_s, trip_s=duration, now=utc_now_iso())
        if _store_costs_in_project(load_config()):
            update_job_costs(plan.project.local_project_root, ckey, plan.target.name,
                             rss_mb=tel.get("peak_rss_mb"), cpu_pct=tel.get("avg_cpu_pct"),
                             exec_s=exec_s, now=utc_now_iso())
    except Exception:
        pass

    summary = {
        "run_id": run_id,
        "project_id": plan.project.project_id,
        "target": plan.target.name,
        "command": plan.command,
        "exit_code": final_exit,
        "command_exit_code": result.exit_code,
        "started_at": started_at,
        "ended_at": utc_now_iso(),
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
        "peak_rss_mb": result.telemetry.get("peak_rss_mb") if result.telemetry else None,
        "avg_cpu_pct": result.telemetry.get("avg_cpu_pct") if result.telemetry else None,
        "plan": plan.as_dict(),
    }
    write_json(summary_path, summary)
    reporter.event("summary", run_id=run_id, exit_code=final_exit,
                   files_pushed=summary["files_pushed"], files_pulled_post=summary["files_pulled_post"],
                   duration_sec=duration, summary_path=str(summary_path))

    # Self-limiting: apply the retention policy. Never let cleanup break a run.
    try:
        report = prune_state(policy)
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
    )
    project = probe_plan.project
    key = command_key(command)
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
            update_profile(state_root, project.project_id, key, LOCAL_DEVICE,
                           exec_s=local_s, trip_s=local_s, now=utc_now_iso())
            reporter.event("bench_local", exec_s=local_s, exit_code=rc)
        else:
            reporter.event("bench_local_unrecorded", exec_s=local_s, exit_code=rc,
                           note="non-zero/failed local run not folded into the baseline")

    # 2. Each target via the full remrun trip (cmd_run records its per-device row).
    # A leg only contributes to the verdict if its round-trip actually completed;
    # a remrun-level failure (unreachable/transfer/conflict/internal) recorded no
    # fresh row, so an unrelated stale row must not silently drive the recommendation.
    infra_failures = {EXIT_INTERNAL, EXIT_CONFLICT, EXIT_TRANSFER, EXIT_INFRA}
    ran: list[str] = []
    for dev in targets:
        reporter.event("bench_remote_started", device=dev)
        run_args = argparse.Namespace(
            target=dev, auto=False, cmd=command, json=args.json,
            dry_run=False, no_pullback=False, no_telemetry=False,
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
        rec = _best_remote_verdict(profiles, project.project_id, key, ran)
    else:
        rec = recommend_offload(profiles, project.project_id, key, devices=ran, bias=bias)
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
    """Pull-biased, project-less folder sync (e.g. fleet OCR/TTS output trees).

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
