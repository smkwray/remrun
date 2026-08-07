from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

import remrun.gitsync as gitsync_module
from remrun import _posix_telemetry as telemetry
from remrun.cli import build_parser
from remrun.config import RemrunConfig
from remrun.gitsync import (
    EXIT_OK,
    GitSyncError,
    _branches_remote,
    git_sync_status_result,
    run_git_sync_result,
)
from remrun.models import Device
from remrun.output import Reporter
from remrun.transport import ExecResult, LocalSimTransport

MIB = 1024**2
GIB = 1024**3


def _posix(path: Path) -> str:
    return str(path).replace("\\", "/")


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=str(repo),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    return result.stdout.strip()


def _commit(repo: Path, name: str, text: str) -> str:
    (repo / name).write_text(text, encoding="utf-8")
    _git(repo, "add", name)
    _git(repo, "commit", "-m", f"add {name}")
    return _git(repo, "rev-parse", "HEAD")


class _RecordingGuardedTransport:
    """Make LocalSim look guarded while recording the selected execution seam.

    ``exec`` deliberately remains available so the exact base can demonstrate the
    bug: it reaches every Git command as an unknown workload. The repaired path must
    use ``exec_with_memory_limit`` for every tool-owned Git command instead.
    """

    def __init__(self, base: LocalSimTransport) -> None:
        self._base = base
        self.device = base.device
        self.memory_guard = object()
        self.unknown_calls: list[tuple[list[str], str]] = []
        self.limited_calls: list[tuple[list[str], str, int]] = []

    def __getattr__(self, name: str):
        return getattr(self._base, name)

    def exec(self, command: list[str], cwd: str, **kwargs) -> ExecResult:
        self.unknown_calls.append((list(command), cwd))
        return self._base.exec(command, cwd=cwd, **kwargs)

    def exec_with_memory_limit(
        self,
        command: list[str],
        cwd: str,
        *,
        memory_limit_mib: int,
        **kwargs,
    ) -> ExecResult:
        self.limited_calls.append((list(command), cwd, memory_limit_mib))
        return self._base.exec(command, cwd=cwd, **kwargs)


@pytest.fixture()
def repos(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    local_base = tmp_path / "local" / "proj"
    remote_base = tmp_path / "remote"
    cache = tmp_path / "cache"
    local = local_base / "demo"
    remote = remote_base / "demo"
    local.mkdir(parents=True)
    remote_base.mkdir(parents=True)
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(tmp_path / "state"))

    subprocess.run(
        ["git", "init", "-b", "main"],
        cwd=str(local),
        check=True,
        capture_output=True,
        text=True,
    )
    _git(local, "config", "user.email", "remrun-test@example.invalid")
    _git(local, "config", "user.name", "remrun Test")
    _commit(local, "base.txt", "base")
    subprocess.run(
        ["git", "clone", str(local), str(remote)],
        check=True,
        capture_output=True,
        text=True,
    )
    _git(remote, "config", "user.email", "remrun-test@example.invalid")
    _git(remote, "config", "user.name", "remrun Test")

    device = Device.from_mapping(
        "LOCAL_SIM",
        {
            "kind": "local-sim",
            "os": "posix",
            "project_root": _posix(remote_base),
            "state_root": _posix(tmp_path / "target-state"),
            "cache_root": _posix(cache),
        },
    )
    config = RemrunConfig(
        repo_root=tmp_path / "remrun",
        defaults={},
        devices={"LOCAL_SIM": device},
        project_roots={
            "default": _posix(local_base),
            "windows": _posix(local_base),
            "macos": _posix(local_base),
        },
    )
    base = LocalSimTransport(device)
    guarded = _RecordingGuardedTransport(base)
    monkeypatch.setattr(gitsync_module, "make_transport", lambda _device: guarded)
    monkeypatch.chdir(local)
    return config, local, remote, guarded


@pytest.fixture()
def bootstrap_repos(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    local_base = tmp_path / "local" / "proj"
    remote_base = tmp_path / "remote"
    local = local_base / "demo"
    remote = remote_base / "demo"
    local.mkdir(parents=True)
    remote.mkdir(parents=True)
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(tmp_path / "state"))

    subprocess.run(
        ["git", "init", "-b", "main"],
        cwd=str(remote),
        check=True,
        capture_output=True,
        text=True,
    )
    _git(remote, "config", "user.email", "remrun-test@example.invalid")
    _git(remote, "config", "user.name", "remrun Test")
    peer_head = _commit(remote, "base.txt", "peer base")
    (local / "base.txt").write_text("local unsaved work", encoding="utf-8")

    device = Device.from_mapping(
        "LOCAL_SIM",
        {
            "kind": "local-sim",
            "os": "posix",
            "project_root": _posix(remote_base),
            "state_root": _posix(tmp_path / "target-state"),
            "cache_root": _posix(tmp_path / "cache"),
        },
    )
    config = RemrunConfig(
        repo_root=tmp_path / "remrun",
        defaults={},
        devices={"LOCAL_SIM": device},
        project_roots={
            "default": _posix(local_base),
            "windows": _posix(local_base),
            "macos": _posix(local_base),
        },
    )
    base = LocalSimTransport(device)
    guarded = _RecordingGuardedTransport(base)
    monkeypatch.setattr(gitsync_module, "make_transport", lambda _device: guarded)
    monkeypatch.chdir(local)
    return config, local, remote, peer_head, guarded


def test_fixed_repository_probe_uses_bounded_limit_not_unknown_allowance(repos):
    config, _local, _remote, guarded = repos

    result = run_git_sync_result(
        config,
        device_name="LOCAL_SIM",
        direction="pull",
        dry_run=True,
        reporter=Reporter(),
    )

    assert result.exit_code == EXIT_OK
    assert guarded.unknown_calls == []
    assert guarded.limited_calls == [
        (
            ["git", "rev-parse", "--show-prefix", "--show-toplevel"],
            result.remote_project,
            128,
        )
    ]



def test_fixed_repository_probe_honors_a_stricter_operator_limit(repos):
    config, _local, _remote, guarded = repos

    result = run_git_sync_result(
        config,
        device_name="LOCAL_SIM",
        direction="pull",
        dry_run=True,
        remote_memory_limit_mib=64,
        reporter=Reporter(),
    )

    assert result.exit_code == EXIT_OK
    assert guarded.unknown_calls == []
    assert guarded.limited_calls == [(
        ["git", "rev-parse", "--show-prefix", "--show-toplevel"],
        result.remote_project,
        64,
    )]

def test_guarded_status_requires_explicit_limit_before_bundle_or_temp_mutation(repos):
    config, _local, _remote, guarded = repos

    with pytest.raises(GitSyncError, match="remote_memory_limit_mib"):
        git_sync_status_result(config, device_name="LOCAL_SIM", reporter=Reporter())

    assert guarded.unknown_calls == []
    assert [call[0] for call in guarded.limited_calls] == [
        ["git", "rev-parse", "--show-prefix", "--show-toplevel"]
    ]


def test_explicit_limit_covers_status_pull_push_and_every_remote_git_command(repos):
    config, local, remote, guarded = repos
    config = replace(config, git_sync={"remote_memory_limit_mib": 256})

    status = git_sync_status_result(config, device_name="LOCAL_SIM", reporter=Reporter())
    assert status.exit_code == EXIT_OK

    _commit(remote, "from-remote.txt", "remote")
    pulled = run_git_sync_result(
        config,
        device_name="LOCAL_SIM",
        direction="pull",
        reporter=Reporter(),
    )
    assert pulled.exit_code == EXIT_OK
    assert (local / "from-remote.txt").read_text(encoding="utf-8") == "remote"

    _commit(local, "from-local.txt", "local")
    pushed = run_git_sync_result(
        config,
        device_name="LOCAL_SIM",
        direction="push",
        reporter=Reporter(),
    )
    assert pushed.exit_code == EXIT_OK
    assert (remote / "from-local.txt").read_text(encoding="utf-8") == "local"

    assert guarded.unknown_calls == []
    assert guarded.limited_calls
    assert {limit for _command, _cwd, limit in guarded.limited_calls} == {128, 256}
    for command, _cwd, limit in guarded.limited_calls:
        if command[1:3] == ["rev-parse", "--show-prefix"]:
            assert limit == 128
        else:
            assert limit == 256
    commands = [command for command, _cwd, _limit in guarded.limited_calls]
    assert any(command[1:3] == ["bundle", "create"] for command in commands)
    assert any(command[1] == "fetch" for command in commands)
    assert any(command[1] == "status" for command in commands)
    assert any(command[1] == "diff" for command in commands)
    assert any(command[1] in {"merge", "update-ref", "reset"} for command in commands)


def test_bootstrap_requires_limit_before_creating_local_git_metadata(bootstrap_repos):
    config, local, _remote, _peer_head, guarded = bootstrap_repos

    with pytest.raises(GitSyncError, match="remote_memory_limit_mib"):
        run_git_sync_result(
            config,
            device_name="LOCAL_SIM",
            direction="pull",
            reporter=Reporter(),
        )

    assert not (local / ".git").exists()
    assert guarded.unknown_calls == []


def test_explicit_limit_covers_bootstrap_remote_git_path(bootstrap_repos):
    config, local, _remote, peer_head, guarded = bootstrap_repos
    config = replace(config, git_sync={"remote_memory_limit_mib": 192})

    result = run_git_sync_result(
        config,
        device_name="LOCAL_SIM",
        direction="pull",
        reporter=Reporter(),
    )

    assert result.bootstrap is not None
    assert result.bootstrap.head == peer_head
    assert _git(local, "rev-parse", "HEAD") == peer_head
    assert (local / "base.txt").read_text(encoding="utf-8") == "local unsaved work"
    assert guarded.unknown_calls == []
    assert guarded.limited_calls
    assert {limit for _command, _cwd, limit in guarded.limited_calls} == {128, 192}
    assert any(command[1:3] == ["bundle", "create"] and limit == 192
               for command, _cwd, limit in guarded.limited_calls)



def test_limit_precedence_is_cli_then_project_then_global(tmp_path: Path):
    config = RemrunConfig(
        repo_root=tmp_path,
        defaults={},
        devices={},
        project_roots={},
        git_sync={"remote_memory_limit_mib": 256},
    )

    assert gitsync_module._remote_memory_limit_mib(config, {}, None) == 256
    assert gitsync_module._remote_memory_limit_mib(
        config, {"git_sync": {"remote_memory_limit_mib": 384}}, None
    ) == 384
    assert gitsync_module._remote_memory_limit_mib(
        config, {"git_sync": {"remote_memory_limit_mib": 384}}, 512
    ) == 512
    with pytest.raises(GitSyncError, match="positive integer"):
        gitsync_module._remote_memory_limit_mib(config, {}, True)

def test_explicit_limit_is_labeled_as_limit_not_learned_measurement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    total = 64 * GIB
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (total, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: 128 * MIB)
    request = {
        "schema": 1,
        "op": "reserve",
        "state_root": str(tmp_path / "state"),
        "lease_id": "a" * 32,
        "lease_token": "b" * 32,
        "predicted_rss_bytes": None,
        "explicit_limit_bytes": 256 * MIB,
        "command_limit_fraction": 0.25,
        "host_reserve_fraction": 0.25,
        "max_jobs": 2,
        "reservation_ttl_seconds": 120.0,
    }

    result = telemetry._handle_admission_request(request)

    assert result["status"] == "admitted"
    assert result["detail"] == "explicit command limit reserved before mutation"
    assert result["capacity"]["allowance_basis"] == "explicit_command_limit"
    assert result["capacity"]["allowance_bytes"] == 256 * MIB
    assert result["capacity"]["explicit_limit_bytes"] == 256 * MIB
    assert result["capacity"]["predicted_rss_bytes"] is None



def test_explicit_limit_above_target_ceiling_is_refused_without_a_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    total = 64 * GIB
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (total, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: 128 * MIB)
    request = {
        "schema": 1,
        "op": "reserve",
        "state_root": str(tmp_path / "state"),
        "lease_id": "c" * 32,
        "lease_token": "d" * 32,
        "predicted_rss_bytes": None,
        "explicit_limit_bytes": 17 * GIB,
        "command_limit_fraction": 0.25,
        "host_reserve_fraction": 0.25,
        "max_jobs": 2,
        "reservation_ttl_seconds": 120.0,
    }

    result = telemetry._handle_admission_request(request)

    assert result["status"] == "refused"
    assert result["reason"] == "explicit_limit_exceeds_command_limit"
    ledger_path = tmp_path / "state" / "memory-guard" / "v2" / "ledger.json"
    assert not ledger_path.exists()


def test_guard_termination_is_not_misread_as_missing_ref_or_divergence():
    calls = []

    class GuardFailureTransport:
        memory_guard = object()

        def exec(self, command: list[str], cwd: str, **_kwargs) -> ExecResult:
            calls.append((command, cwd))
            return ExecResult(
                125,
                "",
                "",
                memory_guard={
                    "schema": 1,
                    "status": "terminated",
                    "reason": "command_memory_limit",
                    "detail": "sampled RSS exceeded the explicit command limit",
                    "command_started": True,
                },
            )

    with pytest.raises(GitSyncError, match=r"command_started=true"):
        _branches_remote(GuardFailureTransport(), "/repo", "main")

    assert calls == [
        (["git", "show-ref", "--verify", "--quiet", "refs/heads/main"], "/repo")
    ]


def test_limit_receipt_can_be_json_serialized_without_claiming_measurement(tmp_path: Path):
    """Keep the public evidence shape usable by reporters/native gates."""
    payload = {
        "allowance_basis": "explicit_command_limit",
        "allowance_bytes": 128 * MIB,
        "explicit_limit_bytes": 128 * MIB,
        "predicted_rss_bytes": None,
    }
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    assert json.loads(path.read_text(encoding="utf-8")) == payload


def test_cli_accepts_one_off_remote_memory_limit_override():
    args = build_parser().parse_args(
        ["git-sync", "MACBOX", "--status", "--remote-memory-limit-mib", "768"]
    )
    assert args.remote_memory_limit_mib == 768


def test_generic_explicit_limit_seam_cleans_reserved_lease_on_dispatch_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    pytest.importorskip("fcntl")
    total, _available = telemetry._host_memory()
    command_fraction = max(256 * MIB / total, 0.000001)
    reserve_fraction = max(MIB / total, 0.000001)
    device = Device.from_mapping(
        "GUARDED",
        {
            "kind": "local-sim",
            "os": "posix",
            "project_root": str(tmp_path / "remote"),
            "state_root": str(tmp_path / "state"),
            "cache_root": str(tmp_path / "cache"),
            "max_jobs": 1,
            "memory_guard": {
                "schema": 2,
                "command_limit_fraction": command_fraction,
                "host_reserve_fraction": reserve_fraction,
            },
        },
    )
    transport = LocalSimTransport(device)

    def fail_dispatch(*_args, **_kwargs):
        raise RuntimeError("dispatch failed before exec could own cleanup")

    monkeypatch.setattr(transport, "exec", fail_dispatch)
    with pytest.raises(RuntimeError, match="dispatch failed"):
        transport.exec_with_memory_limit(
            ["/usr/bin/true"],
            cwd=str(tmp_path / "remote"),
            memory_limit_mib=64,
        )

    ledger_path = tmp_path / "state" / "memory-guard" / "v2" / "ledger.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert ledger["leases"] == []



def test_generic_explicit_limit_seam_cleans_reserved_lease_on_returned_prestart_refusal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """A backend may classify address resolution as a returned no-start refusal."""
    pytest.importorskip("fcntl")
    total, _available = telemetry._host_memory()
    command_fraction = max(256 * MIB / total, 0.000001)
    reserve_fraction = max(MIB / total, 0.000001)
    device = Device.from_mapping(
        "GUARDED",
        {
            "kind": "local-sim",
            "os": "posix",
            "project_root": str(tmp_path / "remote"),
            "state_root": str(tmp_path / "state"),
            "cache_root": str(tmp_path / "cache"),
            "max_jobs": 1,
            "memory_guard": {
                "schema": 2,
                "command_limit_fraction": command_fraction,
                "host_reserve_fraction": reserve_fraction,
            },
        },
    )
    transport = LocalSimTransport(device)

    def refuse_before_dispatch(*_args, **_kwargs):
        return ExecResult(
            125,
            "",
            "transport unavailable\n",
            memory_guard={
                "schema": 1,
                "status": "refused",
                "reason": "transport_unavailable",
                "detail": "address resolution failed",
                "command_started": False,
            },
        )

    monkeypatch.setattr(transport, "exec", refuse_before_dispatch)
    result = transport.exec_with_memory_limit(
        ["/usr/bin/true"],
        cwd=str(tmp_path / "remote"),
        memory_limit_mib=64,
    )

    assert result.memory_guard is not None
    assert result.memory_guard["command_started"] is False
    ledger_path = tmp_path / "state" / "memory-guard" / "v2" / "ledger.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert ledger["leases"] == []

def test_generic_explicit_limit_seam_runs_once_and_reports_exact_cap(tmp_path: Path):
    pytest.importorskip("fcntl")
    total, _available = telemetry._host_memory()
    command_fraction = max(256 * MIB / total, 0.000001)
    reserve_fraction = max(MIB / total, 0.000001)
    device = Device.from_mapping(
        "GUARDED",
        {
            "kind": "local-sim",
            "os": "posix",
            "project_root": str(tmp_path / "remote"),
            "state_root": str(tmp_path / "state"),
            "cache_root": str(tmp_path / "cache"),
            "max_jobs": 1,
            "memory_guard": {
                "schema": 2,
                "command_limit_fraction": command_fraction,
                "host_reserve_fraction": reserve_fraction,
            },
        },
    )
    transport = LocalSimTransport(device)

    result = transport.exec_with_memory_limit(
        ["/usr/bin/true"],
        cwd=str(tmp_path / "remote"),
        memory_limit_mib=64,
    )

    assert result.exit_code == 0
    assert result.memory_guard is not None
    assert result.memory_guard["status"] == "ok"
    assert result.memory_guard["command_started"] is True
    assert result.memory_guard["max_command_bytes"] == 64 * MIB
    ledger_path = tmp_path / "state" / "memory-guard" / "v2" / "ledger.json"
    assert json.loads(ledger_path.read_text(encoding="utf-8"))["leases"] == []
