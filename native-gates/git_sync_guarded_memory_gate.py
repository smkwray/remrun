#!/usr/bin/env python3
"""Read-only native gate for guarded Git-sync on one configured POSIX target.

Run from the project whose peer mapping should be checked::

    python native-gates/git_sync_guarded_memory_gate.py MACBOX \
        --remote-memory-limit-mib 2048

The gate exercises the repaired repository probe and complete ``git-sync --status``
path. It snapshots local and remote HEAD/refs/porcelain state before and after,
requires a quiescent schema-2 lease ledger, and fails if any lease remains. It
never invokes push, pull, bootstrap, update-ref, merge, reset, checkout, or clean.
A native push/pull gate belongs only in a repository explicitly created as
disposable; the focused exact-base tests cover those production call paths.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

MAX_LEDGER_BYTES = 1024 * 1024


class GateFailure(RuntimeError):
    """One native assertion failed."""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("device", help="configured guarded POSIX peer")
    parser.add_argument(
        "--remote-memory-limit-mib",
        type=int,
        required=True,
        help="explicit hard per-remote-Git-command process-tree limit",
    )
    parser.add_argument(
        "--project",
        type=Path,
        default=Path.cwd(),
        help="local project to inspect (default: current directory)",
    )
    parser.add_argument(
        "--remrun-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="remrun source/config root",
    )
    return parser


def _load_remrun(remrun_root: Path):  # noqa: ANN202
    source = remrun_root / "src"
    if not source.is_dir():
        raise GateFailure(f"remrun root has no src directory: {source}")
    source_text = str(source.resolve())
    if source_text not in sys.path:
        sys.path.insert(0, source_text)
    from remrun.config import load_config
    from remrun.gitsync import (
        _detect_git_project,
        _git_sync_config,
        git_sync_status_result,
    )
    from remrun.output import Reporter
    from remrun.transport import make_transport

    return (
        load_config,
        _detect_git_project,
        _git_sync_config,
        git_sync_status_result,
        Reporter,
        make_transport,
    )


def _git_snapshot(repo: Path) -> dict[str, str]:
    commands = {
        "head": ["rev-parse", "--verify", "HEAD"],
        "refs": ["for-each-ref", "--format=%(refname)%00%(objectname)"],
        "porcelain": ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    }
    snapshot: dict[str, str] = {}
    for key, args in commands.items():
        result = subprocess.run(
            ["git", *args],
            cwd=str(repo),
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise GateFailure(
                f"local git {' '.join(args)} failed: "
                f"{(result.stderr or result.stdout).strip()}"
            )
        snapshot[key] = result.stdout
    return snapshot


def _remote_git_snapshot(
    transport,
    remote_root: str,
    memory_limit_mib: int,
) -> dict[str, str]:  # noqa: ANN001
    commands = {
        "head": ["git", "rev-parse", "--verify", "HEAD"],
        "refs": ["git", "for-each-ref", "--format=%(refname)%00%(objectname)"],
        "porcelain": [
            "git", "status", "--porcelain=v1", "-z", "--untracked-files=all"
        ],
    }
    snapshot: dict[str, str] = {}
    for key, command in commands.items():
        result = transport.exec_with_memory_limit(
            command,
            cwd=remote_root,
            memory_limit_mib=memory_limit_mib,
        )
        guard = result.memory_guard if isinstance(result.memory_guard, dict) else {}
        if result.exit_code != 0 or guard.get("status") != "ok":
            raise GateFailure(
                f"remote {' '.join(command)} failed: exit={result.exit_code}; "
                f"guard={json.dumps(guard, sort_keys=True)}; "
                f"stderr={result.stderr!r}"
            )
        snapshot[key] = result.stdout
    return snapshot


def _ledger_leases(transport) -> list[dict[str, Any]]:  # noqa: ANN001
    state_root = transport.expand_remote(transport.device.state_root).rstrip("/\\")
    if not state_root:
        raise GateFailure("target state_root is empty")
    ledger_path = transport.native_join(
        state_root, "memory-guard", "v2", "ledger.json"
    )
    try:
        raw = transport.read_small_file(ledger_path, MAX_LEDGER_BYTES)
    except Exception as exc:
        if "missing" in str(exc).lower():
            return []
        raise GateFailure(f"cannot read guard ledger {ledger_path}: {exc}") from exc
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GateFailure(f"guard ledger is invalid JSON: {exc}") from exc
    leases = payload.get("leases")
    if not isinstance(leases, list) or not all(isinstance(row, dict) for row in leases):
        raise GateFailure("guard ledger has no valid leases list")
    return leases


def _changed(before: dict[str, str], after: dict[str, str]) -> list[str]:
    return [name for name in sorted(before) if before.get(name) != after.get(name)]


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.remote_memory_limit_mib <= 0:
        raise GateFailure("--remote-memory-limit-mib must be positive")
    remrun_root = args.remrun_root.expanduser().resolve()
    project = args.project.expanduser().resolve()
    if not project.is_dir():
        raise GateFailure(f"project directory does not exist: {project}")

    (
        load_config,
        detect_git_project,
        git_sync_config,
        git_sync_status_result,
        Reporter,
        make_transport,
    ) = _load_remrun(remrun_root)
    os.environ["REMRUN_ROOT"] = str(remrun_root)
    os.chdir(project)

    boundary_config = load_config(remrun_root)
    config = git_sync_config(boundary_config)
    if args.device not in config.devices:
        available = ", ".join(sorted(config.devices)) or "<none>"
        raise GateFailure(
            f"device {args.device!r} is not configured; available: {available}"
        )
    device = config.devices[args.device]
    transport = make_transport(device)
    probe = transport.probe()
    if not probe.reachable:
        raise GateFailure(f"{args.device} unreachable: {probe.detail}")
    if transport.memory_guard is None:
        raise GateFailure(f"{args.device} has no schema-2 memory guard")

    project_context, _project_config = detect_git_project(
        config, require_git=True, boundary_config=boundary_config
    )
    remote_root = transport.remote_project_path(project_context)
    leases_before = _ledger_leases(transport)
    if leases_before:
        raise GateFailure(
            "native gate requires a quiescent target ledger; active leases="
            + json.dumps(leases_before, sort_keys=True)
        )

    local_before = _git_snapshot(project_context.local_project_root)
    remote_before = _remote_git_snapshot(
        transport, remote_root, args.remote_memory_limit_mib
    )
    status = git_sync_status_result(
        boundary_config,
        device_name=args.device,
        remote_memory_limit_mib=args.remote_memory_limit_mib,
        reporter=Reporter(),
    )
    local_after = _git_snapshot(project_context.local_project_root)
    remote_after = _remote_git_snapshot(
        transport, remote_root, args.remote_memory_limit_mib
    )
    leases_after = _ledger_leases(transport)

    local_changes = _changed(local_before, local_after)
    remote_changes = _changed(remote_before, remote_after)
    if local_changes or remote_changes:
        raise GateFailure(
            f"status path changed Git/worktree observations: local={local_changes}; "
            f"remote={remote_changes}"
        )
    if leases_after:
        raise GateFailure(
            "guard lease remained after status: " + json.dumps(leases_after, sort_keys=True)
        )

    print(json.dumps({
        "status": "PASS",
        "device": args.device,
        "remote_memory_limit_mib": args.remote_memory_limit_mib,
        "remote_project": remote_root,
        "git_sync_exit_code": status.exit_code,
        "branch_states": [row.state for row in status.branches],
        "local_dirty": status.local_dirty,
        "remote_dirty": status.remote_dirty,
        "leases_after": 0,
        "working_tree_or_history_changed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except GateFailure as exc:
        print(json.dumps({"status": "FAIL", "error": str(exc)}, sort_keys=True),
              file=sys.stderr)
        raise SystemExit(1) from exc
