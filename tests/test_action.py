from __future__ import annotations

import sys
from pathlib import Path

from remrun.action import EXIT_CONFLICT, EXIT_INTERNAL, run_action
from remrun.config import RemrunConfig
from remrun.models import Device


def _config(tmp_path: Path, command: list[str]) -> RemrunConfig:
    device = Device(
        name="LOCAL_SIM",
        enabled=True,
        role="simulation",
        kind="local-sim",
        os="posix",
        address_candidates=["localhost"],
        project_root=str(tmp_path / "projects"),
        state_root=str(tmp_path / "state"),
        cache_root=str(tmp_path / "cache"),
        actions={"stage": {"inbox": str(tmp_path / "inbox"), "command": command}},
    )
    return RemrunConfig(tmp_path, {}, {"LOCAL_SIM": device}, {"posix": str(tmp_path)})


def test_action_stages_file_runs_once_and_replays_receipt(tmp_path):
    marker = tmp_path / "marker.txt"
    command = [
        sys.executable,
        "-c",
        "from pathlib import Path; p=Path(r'%s'); p.write_text(p.read_text()+'x' if p.exists() else 'x')"
        % marker,
    ]
    source = tmp_path / "prompt.md"
    source.write_text("dispatch", encoding="utf-8")
    config = _config(tmp_path, command)

    first = run_action(config, "LOCAL_SIM", "stage", [str(source)])
    second = run_action(config, "LOCAL_SIM", "stage", [str(source)])

    assert first.ok and first.status == "complete"
    assert second.ok and second.status == "already-complete"
    assert marker.read_text(encoding="utf-8") == "x"
    assert (tmp_path / "inbox" / "prompt.md").read_text(encoding="utf-8") == "dispatch"


def test_action_refuses_different_file_with_same_inbox_name(tmp_path):
    config = _config(tmp_path, [sys.executable, "-c", "pass"])
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.mkdir()
    right.mkdir()
    a = left / "bundle.zip"
    b = right / "bundle.zip"
    a.write_bytes(b"a")
    b.write_bytes(b"b")

    assert run_action(config, "LOCAL_SIM", "stage", [str(a)]).ok
    conflict = run_action(config, "LOCAL_SIM", "stage", [str(b)])
    assert not conflict.ok and conflict.exit_code == EXIT_CONFLICT


def test_action_without_input_requires_explicit_key(tmp_path):
    config = _config(tmp_path, [sys.executable, "-c", "pass"])
    result = run_action(config, "LOCAL_SIM", "stage", [])
    assert not result.ok and result.exit_code == EXIT_INTERNAL


def test_action_dry_run_has_no_remote_side_effect(tmp_path):
    config = _config(tmp_path, [sys.executable, "-c", "raise SystemExit(99)"])
    source = tmp_path / "x.txt"
    source.write_text("x", encoding="utf-8")
    result = run_action(config, "LOCAL_SIM", "stage", [str(source)], dry_run=True)
    assert result.ok and result.status == "planned"
    assert not (tmp_path / "inbox").exists()
