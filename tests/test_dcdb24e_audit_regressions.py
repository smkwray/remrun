from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path

import pytest

import remrun.gitsync as gitsync
from remrun.config import RemrunConfig
from remrun.fleet import probes
from remrun.models import Device
from remrun.output import Reporter
from remrun.transport import ExecResult, ProbeResult


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=repo, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr or result.stdout
    return result.stdout.strip()


def _local_os_backend() -> tuple[str, str]:
    if os.name == "nt":
        return "windows", "ssh-powershell"
    return ("macos" if __import__("sys").platform == "darwin" else "linux"), "ssh-posix"


def test_controller_name_collision_with_contradictory_address_uses_transport(tmp_path, monkeypatch):
    local_os, backend = _local_os_backend()
    host = (socket.gethostname() or "controller").split(".")[0]
    device = Device.from_mapping(host, {
        "role": "controller", "kind": backend, "os": local_os,
        "address_candidates": ["remote-target.example.invalid"],
        "project_root": str(tmp_path), "state_root": str(tmp_path / "state"),
        "cache_root": str(tmp_path / "cache"),
    })

    class RemoteEvidence:
        def probe(self):
            return ProbeResult(True, "remote-target.example.invalid", "remote evidence", device.os)

    monkeypatch.setattr(
        probes.local_resources, "local_view",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("collision used local evidence")),
    )
    monkeypatch.setattr(probes, "probe_target_resources", lambda *_a, **_k: None)
    snapshot = probes.build_snapshot(device, RemoteEvidence(), {}, adapter_specs=[])
    assert snapshot.detail == "remote evidence"


def test_controller_address_collision_without_name_match_uses_transport(tmp_path, monkeypatch):
    local_os, backend = _local_os_backend()
    device = Device.from_mapping("OTHER-CONTROLLER", {
        "role": "controller", "kind": backend, "os": local_os,
        "address_candidates": ["localhost"],
        "project_root": str(tmp_path), "state_root": str(tmp_path / "state"),
        "cache_root": str(tmp_path / "cache"),
    })

    class RemoteEvidence:
        def probe(self):
            return ProbeResult(True, "localhost", "remote evidence", device.os)

    monkeypatch.setattr(
        probes.local_resources, "local_view",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("collision used local evidence")),
    )
    monkeypatch.setattr(probes, "probe_target_resources", lambda *_a, **_k: None)
    snapshot = probes.build_snapshot(device, RemoteEvidence(), {}, adapter_specs=[])
    assert snapshot.detail == "remote evidence"


def test_local_alias_with_contradictory_resolution_is_not_local(monkeypatch):
    monkeypatch.setattr(
        probes.socket,
        "getaddrinfo",
        lambda *_a, **_k: [(probes.socket.AF_INET, 0, 0, "", ("203.0.113.10", 0))],
    )
    monkeypatch.setattr(probes, "_address_is_local", lambda *_a, **_k: False)

    assert not probes._host_token_is_local("controller", {"controller"}, set())


def test_unresolved_alias_is_not_address_evidence(monkeypatch):
    monkeypatch.setattr(
        probes.socket,
        "getaddrinfo",
        lambda *_a, **_k: (_ for _ in ()).throw(OSError("unresolved local alias")),
    )

    assert not probes._host_token_is_local("controller", {"controller"}, set())


def test_local_hostname_dns_answer_must_bind_before_local_substitution(
    tmp_path, monkeypatch,
):
    local_os, backend = _local_os_backend()
    device = Device.from_mapping("controller", {
        "role": "controller", "kind": backend, "os": local_os,
        "address_candidates": ["controller.example"],
        "project_root": str(tmp_path), "state_root": str(tmp_path / "state"),
        "cache_root": str(tmp_path / "cache"),
    })

    monkeypatch.setattr(probes.socket, "gethostname", lambda: "controller")
    monkeypatch.setattr(probes.socket, "getfqdn", lambda: "controller.example")
    monkeypatch.setattr(
        probes.socket,
        "getaddrinfo",
        lambda *_a, **_k: [
            (probes.socket.AF_INET, probes.socket.SOCK_DGRAM, 0, "", ("203.0.113.10", 0))
        ],
    )

    class NonlocalSocket:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def bind(self, _address):
            raise OSError("address is not assigned locally")

    monkeypatch.setattr(probes.socket, "socket", lambda *_a, **_k: NonlocalSocket())
    monkeypatch.setattr(
        probes.local_resources, "local_view",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("DNS drift used local evidence")),
    )
    monkeypatch.setattr(probes, "probe_target_resources", lambda *_a, **_k: None)

    class RemoteEvidence:
        def probe(self):
            return ProbeResult(True, "controller.example", "remote evidence", device.os)

    snapshot = probes.build_snapshot(device, RemoteEvidence(), {}, adapter_specs=[])
    assert snapshot.detail == "remote evidence"


@pytest.mark.parametrize("token", ["0.0.0.0", "::", "224.0.0.1", "255.255.255.255"])
def test_non_host_literal_is_not_positive_local_identity(token):
    assert not probes._address_is_local(token, set())


def test_selected_branch_show_ref_execution_failure_is_not_success(tmp_path, monkeypatch):
    local_base = tmp_path / "local" / "proj"
    remote_base = tmp_path / "remote"
    local = local_base / "demo"
    remote = remote_base / "demo"
    local.mkdir(parents=True)
    remote_base.mkdir(parents=True)
    subprocess.run(["git", "init", "-b", "main"], cwd=local, check=True, capture_output=True)
    _git(local, "config", "user.email", "test@example.invalid")
    _git(local, "config", "user.name", "Test")
    (local / "base.txt").write_text("base", encoding="utf-8")
    _git(local, "add", "base.txt")
    _git(local, "commit", "-m", "base")
    subprocess.run(["git", "clone", str(local), str(remote)], check=True, capture_output=True)
    _git(remote, "config", "user.email", "test@example.invalid")
    _git(remote, "config", "user.name", "Test")
    (remote / "ahead.txt").write_text("ahead", encoding="utf-8")
    _git(remote, "add", "ahead.txt")
    _git(remote, "commit", "-m", "ahead")

    device = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim", "os": "posix", "project_root": str(remote_base),
        "cache_root": str(tmp_path / "cache"),
    })
    cfg = RemrunConfig(
        repo_root=tmp_path / "remrun", defaults={}, devices={"LOCAL_SIM": device},
        project_roots={"default": str(local_base), "windows": str(local_base), "macos": str(local_base)},
    )
    monkeypatch.chdir(local)
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(tmp_path / "state"))
    original = gitsync._local_git

    def fail_selected_head(repo, args):
        if args == ["show-ref", "--verify", "--quiet", "refs/heads/main"]:
            return subprocess.CompletedProcess(["git", *args], 2, "", "simulated execution failure")
        return original(repo, args)

    monkeypatch.setattr(gitsync, "_local_git", fail_selected_head)
    with pytest.raises(gitsync.GitSyncError) as exc_info:
        gitsync.run_git_sync_result(
            cfg, device_name="LOCAL_SIM", direction="pull", branch="main", reporter=Reporter()
        )
    assert exc_info.value.exit_code == gitsync.EXIT_TRANSFER
    assert "show-ref" in str(exc_info.value)


def test_remote_selected_branch_show_ref_execution_failure_is_not_absence(monkeypatch):
    class DummyTransport:
        pass

    monkeypatch.setattr(
        gitsync,
        "_remote_git",
        lambda *_a, **_k: ExecResult(2, "", "simulated remote execution failure"),
    )
    with pytest.raises(gitsync.GitSyncError) as exc_info:
        gitsync._branches_remote(DummyTransport(), "/repo", "main")
    assert exc_info.value.exit_code == gitsync.EXIT_TRANSFER
    assert "show-ref" in str(exc_info.value)


def test_preview_route_help_states_json_prerequisite(capsys):
    from remrun.fleet import cli

    with pytest.raises(SystemExit) as exc_info:
        cli.build_parser().parse_args(["submit", "--help"])
    assert exc_info.value.code == 0
    help_text = capsys.readouterr().out
    assert "--preview-route" in help_text
    assert "requires --json" in help_text
