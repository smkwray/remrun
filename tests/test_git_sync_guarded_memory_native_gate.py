from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


GATE_PATH = (
    Path(__file__).resolve().parents[1]
    / "native-gates"
    / "git_sync_guarded_memory_gate.py"
)
SPEC = importlib.util.spec_from_file_location(
    "git_sync_guarded_memory_gate", GATE_PATH
)
assert SPEC is not None and SPEC.loader is not None
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def test_gate_requires_a_positive_explicit_limit():
    args = GATE._parser().parse_args(
        ["MACBOX", "--remote-memory-limit-mib", "512"]
    )
    assert args.device == "MACBOX"
    assert args.remote_memory_limit_mib == 512

    with pytest.raises(GATE.GateFailure, match="must be positive"):
        GATE.main(["MACBOX", "--remote-memory-limit-mib", "0"])


def test_remote_snapshot_uses_the_generic_limited_transport_seam():
    calls = []

    class Transport:
        def exec_with_memory_limit(
            self, command, cwd, *, memory_limit_mib
        ):
            calls.append((command, cwd, memory_limit_mib))
            return SimpleNamespace(
                exit_code=0,
                stdout="snapshot",
                stderr="",
                memory_guard={"status": "ok", "command_started": True},
            )

    result = GATE._remote_git_snapshot(Transport(), "/remote/repo", 640)

    assert result == {
        "head": "snapshot",
        "refs": "snapshot",
        "porcelain": "snapshot",
    }
    assert len(calls) == 3
    assert all(cwd == "/remote/repo" and limit == 640
               for _command, cwd, limit in calls)
    assert all(command[0] == "git" for command, _cwd, _limit in calls)


def test_remote_snapshot_rejects_a_classified_guard_stop():
    class Transport:
        def exec_with_memory_limit(self, *_args, **_kwargs):
            return SimpleNamespace(
                exit_code=125,
                stdout="",
                stderr="",
                memory_guard={
                    "status": "terminated",
                    "reason": "command_memory_limit",
                    "command_started": True,
                },
            )

    with pytest.raises(GATE.GateFailure, match="command_memory_limit"):
        GATE._remote_git_snapshot(Transport(), "/remote/repo", 256)


def test_main_runs_only_status_and_proves_snapshot_and_lease_invariants(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    project = tmp_path / "project"
    remrun_root = tmp_path / "remrun"
    project.mkdir()
    (remrun_root / "src").mkdir(parents=True)
    device = SimpleNamespace(name="MACBOX")
    config = SimpleNamespace(devices={"MACBOX": device})
    transport = SimpleNamespace(
        memory_guard=object(),
        device=device,
        probe=lambda: SimpleNamespace(reachable=True, detail="ok"),
        remote_project_path=lambda _project: "/remote/project",
    )
    calls = []

    def status_result(boundary_config, **kwargs):
        calls.append((boundary_config, kwargs))
        return SimpleNamespace(
            exit_code=0,
            branches=[SimpleNamespace(state="up_to_date")],
            local_dirty=False,
            remote_dirty=False,
        )

    monkeypatch.setattr(
        GATE,
        "_load_remrun",
        lambda _root: (
            lambda _root: config,
            lambda *_args, **_kwargs: (
                SimpleNamespace(local_project_root=project),
                {},
            ),
            lambda boundary: boundary,
            status_result,
            lambda: object(),
            lambda _device: transport,
        ),
    )
    monkeypatch.setattr(GATE, "_ledger_leases", lambda _transport: [])
    monkeypatch.setattr(
        GATE,
        "_git_snapshot",
        lambda _repo: {"head": "a", "refs": "r", "porcelain": ""},
    )
    monkeypatch.setattr(
        GATE,
        "_remote_git_snapshot",
        lambda _transport, _root, _limit: {
            "head": "b",
            "refs": "s",
            "porcelain": "",
        },
    )
    monkeypatch.setattr(GATE.os, "chdir", lambda path: calls.append(("chdir", path)))

    assert GATE.main([
        "MACBOX",
        "--remote-memory-limit-mib", "768",
        "--project", str(project),
        "--remrun-root", str(remrun_root),
    ]) == 0

    status_calls = [call for call in calls if call[0] is config]
    assert len(status_calls) == 1
    assert status_calls[0][1]["device_name"] == "MACBOX"
    assert status_calls[0][1]["remote_memory_limit_mib"] == 768
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "PASS"
    assert payload["working_tree_or_history_changed"] is False
    assert payload["leases_after"] == 0


def test_gate_source_has_no_push_pull_or_bootstrap_entrypoint():
    source = GATE_PATH.read_text(encoding="utf-8")

    assert "git_sync_status_result(" in source
    assert "run_git_sync_result(" not in source
    assert "run_git_sync(" not in source
