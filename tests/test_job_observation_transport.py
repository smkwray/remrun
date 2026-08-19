from __future__ import annotations

import base64
import hashlib
import json
import shlex
import subprocess
from pathlib import Path

import pytest

import remrun.transport as transport_module
from remrun import _job_observer as observer
from remrun.job_observation import JobObservation
from remrun.memory_guard import MemoryAdmissionResult, MemoryReservation
from remrun.models import Device
from remrun.transport import (
    BaseTransport,
    ExecResult,
    SSHPosixTransport,
    SSHPowerShellTransport,
)


def observation(target: str = "DEV") -> JobObservation:
    return JobObservation.for_command(
        job_id="j1", project="proj", source_controller="CTRL", target=target,
        phase="command", command=["echo", "hello"],
    )


def posix_device() -> Device:
    return Device.from_mapping("MAC", {
        "kind": "ssh-posix", "os": "macos", "address_candidates": ["host"],
        "project_root": "/work", "state_root": "/state", "cache_root": "/cache",
        "shell": "bash", "remote_python": "python3",
    })


def windows_device() -> Device:
    return Device.from_mapping("WIN", {
        "kind": "ssh-powershell", "os": "windows", "address_candidates": ["host"],
        "project_root": r"C:\work", "state_root": r"D:\state", "cache_root": r"D:\cache",
        "shell": "pwsh", "remote_python": "python",
    })


def test_posix_exec_observed_inserts_observer_outside_established_shell(monkeypatch):
    transport = SSHPosixTransport(posix_device())
    monkeypatch.setattr(transport, "_ensure_job_observer", lambda: ("/state", "/state/helper.py"))
    calls = []

    def fake_exec_posix(command, cwd, **kwargs):
        calls.append((command, cwd, kwargs))
        return ExecResult(0, "ok", "")

    monkeypatch.setattr(transport, "_exec_posix", fake_exec_posix)
    result = transport.exec_observed(
        ["python3", "-c", "print('ok')"], "/work", observation=observation("MAC"),
        env={"X": "1"}, telemetry=True,
    )
    assert result.exit_code == 0
    assert len(calls) == 1
    command, cwd, kwargs = calls[0]
    assert command == ["python3", "-c", "print('ok')"]
    assert cwd == "/work" and kwargs["env"] == {"X": "1"}
    state_root, helper, metadata = kwargs["observation_wrapper"]
    assert (state_root, helper) == ("/state", "/state/helper.py")
    assert isinstance(metadata, str) and metadata


def test_posix_setup_failure_runs_original_exactly_once_with_warning(monkeypatch):
    transport = SSHPosixTransport(posix_device())
    monkeypatch.setattr(
        transport, "_ensure_job_observer", lambda: (_ for _ in ()).throw(OSError("no helper"))
    )
    calls = []

    def fake_exec(command, cwd, **kwargs):
        calls.append((command, cwd, kwargs))
        return ExecResult(7, "", "user stderr\n")

    monkeypatch.setattr(transport, "exec", fake_exec)
    result = transport.exec_observed(["tool", "arg"], "/work", observation=observation())
    assert len(calls) == 1 and calls[0][0] == ["tool", "arg"]
    assert result.exit_code == 7
    assert "ran unobserved" in result.stderr
    assert result.stderr.endswith("user stderr\n")


def test_posix_query_uses_raw_control_path_not_exec(monkeypatch):
    transport = SSHPosixTransport(posix_device())
    transport._address = "host"
    monkeypatch.setattr(transport, "_ensure_job_observer", lambda: ("/state", "/state/helper.py"))
    monkeypatch.setattr(
        transport, "exec", lambda *a, **k: (_ for _ in ()).throw(AssertionError("exec touched"))
    )
    payload = {"schema": 1, "status": "ok", "jobs": [], "errors": []}
    seen = []

    def remote(address, script, **kwargs):
        seen.append((address, script, kwargs))
        return subprocess.CompletedProcess([], 0, json.dumps(payload).encode(), b"")

    monkeypatch.setattr(transport, "_remote", remote)
    assert transport.query_observed_jobs(sample_interval=0.15) == payload
    assert seen[0][0] == "host"
    assert "python3 -S" in seen[0][1]
    assert "query" in seen[0][1] and "0.15" in seen[0][1]


def test_powershell_exec_observed_preserves_shell_resolution_inside_child(monkeypatch):
    transport = SSHPowerShellTransport(windows_device())
    monkeypatch.setattr(
        transport, "_ensure_job_observer", lambda: (r"D:\state", r"D:\state\helper.py")
    )
    calls = []

    def fake_exec(command, cwd, **kwargs):
        calls.append((command, cwd, kwargs))
        return ExecResult(0, "ok", "")

    monkeypatch.setattr(transport, "exec", fake_exec)
    transport.exec_observed(
        [r"C:\work\adapter.ps1", "hello world"],
        r"C:\work",
        observation=observation("WIN"),
    )
    assert len(calls) == 1
    wrapped, _cwd, kwargs = calls[0]
    assert kwargs["_allow_observed_breakaway"] is True
    assert wrapped[:3] == ["python", "-S", r"D:\state\helper.py"]
    split = wrapped.index("--")
    child = wrapped[split + 1:]
    assert child[:4] == ["pwsh", "-NoProfile", "-NonInteractive", "-EncodedCommand"]
    script = base64.b64decode(child[4]).decode("utf-16-le")
    assert "& 'C:\\work\\adapter.ps1' 'hello world'" in script
    assert "GetCommand" in script and "cmd|bat" in script


def test_powershell_observer_setup_failure_uses_ordinary_exec_scope(monkeypatch):
    transport = SSHPowerShellTransport(windows_device())
    monkeypatch.setattr(
        transport,
        "_ensure_job_observer",
        lambda: (_ for _ in ()).throw(OSError("helper unavailable")),
    )
    calls = []

    def fake_exec(command, cwd, **kwargs):
        calls.append((command, cwd, kwargs))
        return ExecResult(9, "", "user stderr\n")

    monkeypatch.setattr(transport, "exec", fake_exec)

    result = transport.exec_observed(
        [r"C:\work\adapter.ps1", "hello"],
        r"C:\work",
        observation=observation("WIN"),
        telemetry=True,
    )

    assert result.exit_code == 9
    assert len(calls) == 1
    assert calls[0][0] == [r"C:\work\adapter.ps1", "hello"]
    assert calls[0][2]["telemetry"] is True
    assert "_allow_observed_breakaway" not in calls[0][2]
    assert "ran unobserved" in result.stderr


def test_powershell_query_uses_raw_control_path(monkeypatch):
    transport = SSHPowerShellTransport(windows_device())
    transport._address = "host"
    monkeypatch.setattr(
        transport, "_ensure_job_observer", lambda: (r"D:\state", r"D:\state\helper.py")
    )
    monkeypatch.setattr(
        transport, "exec", lambda *a, **k: (_ for _ in ()).throw(AssertionError("exec touched"))
    )
    payload = {"schema": 1, "status": "partial", "jobs": [], "errors": []}
    seen = []

    def remote(address, script, **kwargs):
        seen.append((address, script, kwargs))
        return subprocess.CompletedProcess([], 0, json.dumps(payload).encode(), b"")

    monkeypatch.setattr(transport, "_ps_remote", remote)
    assert transport.query_observed_jobs(sample_interval=0.25) == payload
    assert seen[0][0] == "host"
    assert "'-S'" in seen[0][1]
    assert "query" in seen[0][1] and "0.25" in seen[0][1]


def test_base_transport_default_is_explicitly_unsupported_and_executes_once():
    class ThirdParty(BaseTransport):
        def __init__(self):
            super().__init__(Device.from_mapping("X", {"kind": "third-party", "os": "x"}))
            self.calls = 0

        def exec(self, command, cwd, **kwargs):
            self.calls += 1
            return ExecResult(0, "", "")

    transport = ThirdParty()
    transport.exec_observed(["echo"], "/", observation=observation())
    assert transport.calls == 1
    payload = transport.query_observed_jobs()
    assert payload["status"] == "unsupported"
    assert payload["jobs"] == []
    assert payload["coverage"]["scope"] == "none"


def test_dispatch_reclaim_uses_observed_execution(monkeypatch):
    monkeypatch.setenv("REMRUN_FLEET_JOBS_OBSERVE", "1")
    from types import SimpleNamespace

    from remrun.fleet import dispatcher

    dev = Device.from_mapping("WIN", {
        "kind": "ssh-powershell", "os": "windows", "address_candidates": ["host"],
        "state_root": r"D:\state", "reclaim": {"command": [r"C:\tool.exe", "workingsets"]},
    })
    captured = []

    class FakeTransport:
        def probe(self):
            return SimpleNamespace(reachable=True)

        def expand_remote(self, value):
            return value

        def exec_observed(self, command, cwd, *, observation, **kwargs):
            captured.append((command, cwd, observation, kwargs))
            return ExecResult(0, "", "")

    monkeypatch.setattr(dispatcher, "make_transport", lambda _: FakeTransport())
    reporter = SimpleNamespace(event=lambda *a, **k: None)
    assert dispatcher._run_device_reclaim(dev, reporter) is True
    assert len(captured) == 1
    command, cwd, item, kwargs = captured[0]
    assert command == [r"C:\tool.exe", "workingsets"]
    assert cwd == "C:\\"
    assert item.project == "@fleet" and item.phase == "reclaim"
    assert item.command_label == "host-ram-reclaim"
    assert kwargs["timeout"] == 30


@pytest.mark.parametrize("factory", [posix_device, windows_device])
def test_job_observer_helper_replaces_corrupt_existing_bytes_and_verifies(factory, monkeypatch):
    transport = (
        SSHPosixTransport(factory())
        if factory is posix_device
        else SSHPowerShellTransport(factory())
    )
    source = Path(observer.__file__)
    expected = hashlib.sha256(source.read_bytes()).hexdigest()
    hashes = iter(["0" * 64, expected])
    pushed = []
    monkeypatch.setattr(transport, "_job_helper_sha256", lambda _path: next(hashes))
    monkeypatch.setattr(transport, "push_file", lambda local, remote: pushed.append((local, remote)))

    state_root, helper = transport._ensure_job_observer()

    assert state_root
    assert helper.endswith(".py")
    assert pushed == [(source, helper)]


@pytest.mark.parametrize("factory", [posix_device, windows_device])
def test_job_observer_helper_warm_path_hashes_once_without_push(factory, monkeypatch):
    transport = (
        SSHPosixTransport(factory())
        if factory is posix_device
        else SSHPowerShellTransport(factory())
    )
    expected = hashlib.sha256(Path(observer.__file__).read_bytes()).hexdigest()
    hashes = []
    monkeypatch.setattr(
        transport,
        "_job_helper_sha256",
        lambda path: hashes.append(path) or expected,
    )
    monkeypatch.setattr(
        transport,
        "push_file",
        lambda *_args: (_ for _ in ()).throw(AssertionError("exact helper must be reused")),
    )

    _, helper = transport._ensure_job_observer()

    assert hashes == [helper]


def test_posix_helper_integrity_probe_has_no_runner_payload(monkeypatch):
    transport = SSHPosixTransport(posix_device())
    transport._address = "host"
    expected = "a" * 64
    seen = []

    def remote(address, script, input_bytes=None, **kwargs):
        seen.append((address, script, input_bytes, kwargs))
        return subprocess.CompletedProcess([], 0, (expected + "\n").encode(), b"")

    monkeypatch.setattr(transport, "_remote", remote)
    monkeypatch.setattr(
        transport, "hash_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("generic runner-backed hash_file must not be used")
        ),
    )

    assert transport._job_helper_sha256("/state/helpers/job.py") == expected
    assert len(seen) == 1
    address, script, input_bytes, kwargs = seen[0]
    assert address == "host" and input_bytes is None and kwargs == {}
    assert "hashlib" in script and "/state/helpers/job.py" in script


def test_powershell_helper_integrity_probe_has_no_runner_payload(monkeypatch):
    transport = SSHPowerShellTransport(windows_device())
    transport._address = "host"
    expected = "b" * 64
    seen = []

    def remote(address, script, input_bytes=None, **kwargs):
        seen.append((address, script, input_bytes, kwargs))
        return subprocess.CompletedProcess([], 0, (expected + "\n").encode(), b"")

    monkeypatch.setattr(transport, "_ps_remote", remote)
    monkeypatch.setattr(
        transport, "hash_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("generic runner-backed hash_file must not be used")
        ),
    )

    assert transport._job_helper_sha256(r"D:\state\helpers\job.py") == expected
    assert len(seen) == 1
    address, script, input_bytes, kwargs = seen[0]
    assert address == "host" and input_bytes is None and kwargs == {}
    assert "hashlib" in script and r"D:\state\helpers\job.py" in script


def test_posix_wrapper_failure_after_launch_is_not_retried(monkeypatch):
    transport = SSHPosixTransport(posix_device())
    monkeypatch.setattr(transport, "_ensure_job_observer", lambda: ("/state", "/state/helper.py"))
    calls = []

    def failed_wrapper(command, cwd, **kwargs):
        calls.append((command, cwd, kwargs))
        return ExecResult(9, "partial", "wrapper failed after launch\n")

    monkeypatch.setattr(transport, "_exec_posix", failed_wrapper)
    result = transport.exec_observed(
        ["tool", "arg"], "/work", observation=observation("MAC")
    )

    assert result.exit_code == 9
    assert len(calls) == 1
    assert calls[0][0] == ["tool", "arg"]


def test_posix_observer_remains_inside_schema2_guard_with_one_login_shell(monkeypatch):
    guarded = Device.from_mapping("MAC", {
        "kind": "ssh-posix", "os": "macos", "address_candidates": ["host"],
        "project_root": "/work", "state_root": "/state", "cache_root": "/cache",
        "shell": "bash", "remote_python": "python3", "login_shell": True,
        "max_jobs": 1,
        "memory_guard": {
            "schema": 3, "command_limit_fraction": 0.25,
            "host_reserve_fraction": 0.25,
        },
    })
    transport = SSHPosixTransport(guarded)
    transport._address = "host"
    reservation = MemoryReservation(
        lease_id="a" * 32, lease_token="b" * 32, state_root="/state",
        allowance_bytes=1024**3, control_overhead_bytes=64 * 1024**2,
        capacity_bytes=1088 * 1024**2, max_command_bytes=4 * 1024**3,
        min_available_bytes=2 * 1024**3, host_total_bytes=8 * 1024**3,
        safe_concurrency=1, expires_at=9_999_999_999.0,
    )
    captured = []
    monkeypatch.setattr(transport, "_ensure_job_observer", lambda: ("/state", "/state/observer.py"))
    monkeypatch.setattr(transport, "_ensure_memory_guard_helper", lambda: "/state/guard.py")
    monkeypatch.setattr(
        transport, "renew_memory_guard",
        lambda current: MemoryAdmissionResult("admitted", "renewed", "test", {}, current),
    )
    monkeypatch.setattr(transport, "release_memory_guard", lambda *args, **kwargs: None)

    def remote(_address, script, **_kwargs):
        captured.append(script)
        return subprocess.CompletedProcess([], 0, b"", b"")

    monkeypatch.setattr(transport, "_remote", remote)
    monkeypatch.setattr(
        transport_module, "_finalize_guarded_result",
        lambda **_kwargs: ExecResult(0, "", ""),
    )

    result = transport.exec_observed(
        ["python3", "-c", "print('ok')"], "/work",
        observation=observation("MAC"), env={"X": "1"},
        path_prepend=["/opt/tool/bin"], memory_reservation=reservation,
    )

    assert result.exit_code == 0 and len(captured) == 1
    tokens = shlex.split(captured[0])
    guard_end = tokens.index("--")
    guarded_argv = tokens[guard_end + 1:]
    assert guarded_argv[:3] == ["python3", "-S", "/state/observer.py"]
    observer_end = guarded_argv.index("--")
    shell_argv = guarded_argv[observer_end + 1:]
    assert shell_argv[:2] == ["bash", "-lc"]
    assert shell_argv.count("-lc") == 1
    assert "export X=1" in shell_argv[2]
    assert 'export PATH=/opt/tool/bin:"$PATH"' in shell_argv[2]
    assert "cd /work && python3 -c" in shell_argv[2]


def test_observation_activation_switch_is_default_off_and_explicit(monkeypatch):
    from remrun.job_observation import active_job_observation_enabled

    monkeypatch.delenv("REMRUN_FLEET_JOBS_OBSERVE", raising=False)
    assert active_job_observation_enabled() is False
    for value in ("1", "true", "YES", "on"):
        monkeypatch.setenv("REMRUN_FLEET_JOBS_OBSERVE", value)
        assert active_job_observation_enabled() is True
    monkeypatch.setenv("REMRUN_FLEET_JOBS_OBSERVE", "0")
    assert active_job_observation_enabled() is False


def test_dispatch_reclaim_observation_is_dormant_by_default(monkeypatch):
    from types import SimpleNamespace

    from remrun.fleet import dispatcher

    monkeypatch.delenv("REMRUN_FLEET_JOBS_OBSERVE", raising=False)
    dev = Device.from_mapping(
        "WIN",
        {
            "kind": "ssh-powershell",
            "os": "windows",
            "address_candidates": ["host"],
            "state_root": r"D:\state",
            "reclaim": {"command": [r"C:\tool.exe", "workingsets"]},
        },
    )
    calls = []

    class FakeTransport:
        def probe(self):
            return SimpleNamespace(reachable=True)

        def expand_remote(self, value):
            return value

        def exec(self, command, cwd, **kwargs):
            calls.append(("exec", command, cwd, kwargs))
            return ExecResult(0, "", "")

        def exec_observed(self, *_args, **_kwargs):
            raise AssertionError("default-off reclaim must not enter observation code")

    def unexpected_metadata(*_args, **_kwargs):
        raise AssertionError("default-off reclaim must not construct observation metadata")

    monkeypatch.setattr(dispatcher.JobObservation, "for_command", classmethod(unexpected_metadata))
    monkeypatch.setattr(dispatcher, "make_transport", lambda _dev: FakeTransport())
    reporter = SimpleNamespace(event=lambda *args, **kwargs: None)
    assert dispatcher._run_device_reclaim(dev, reporter) is True
    assert [call[0] for call in calls] == ["exec"]
