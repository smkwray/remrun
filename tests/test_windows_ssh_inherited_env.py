"""Windows OpenSSH children must not inherit stale parent descriptor state."""
from __future__ import annotations

import hashlib
import io
import os
import subprocess
from pathlib import Path

import pytest

from remrun import transport as transport_mod
from remrun.models import Device
from remrun.transport import SSHPowerShellTransport


FD_STATE_KEY = "c28fc6f98a2c44abbbd89d6a3037d0d9_POSIX_FD_STATE"
NEAR_MATCH_KEY = FD_STATE_KEY + "_EXTRA"


def _transport() -> SSHPowerShellTransport:
    return SSHPowerShellTransport(Device.from_mapping("WINBOX", {
        "kind": "ssh-powershell",
        "os": "windows",
        "address_candidates": ["winbox"],
        "project_root": "C:\\Users\\you\\projects",
        "state_root": "C:\\Users\\you\\state",
        "cache_root": "C:\\Users\\you\\cache",
        "shell": "pwsh",
    }))


def _windows_environment(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    monkeypatch.setattr(transport_mod.sys, "platform", "win32")
    monkeypatch.setenv(FD_STATE_KEY.upper(), "inherited-fd-state")
    monkeypatch.setenv(NEAR_MATCH_KEY, "keep-near-match")
    monkeypatch.setenv("CHROOT", "keep-chroot")
    monkeypatch.setenv("OPENSSH_STDIO_MODE", "keep-stdio-mode")
    monkeypatch.setenv("PATH", "C:\\tools;C:\\Windows\\System32")
    return os.environ.copy()


def _assert_clean_child_env(child: object, parent: dict[str, str]) -> None:
    assert isinstance(child, dict)
    expected = {
        key: value for key, value in parent.items()
        if key.casefold() != FD_STATE_KEY.casefold()
    }
    assert child == expected
    assert any(
        key.casefold() == NEAR_MATCH_KEY.casefold() and value == "keep-near-match"
        for key, value in child.items()
    )
    assert child["CHROOT"] == "keep-chroot"
    assert child["OPENSSH_STDIO_MODE"] == "keep-stdio-mode"
    assert child["PATH"] == "C:\\tools;C:\\Windows\\System32"
    assert os.environ.copy() == parent


@pytest.mark.parametrize("spelling", [FD_STATE_KEY.upper(), FD_STATE_KEY])
def test_windows_ssh_child_env_removes_only_exact_key_case_insensitively(
    monkeypatch: pytest.MonkeyPatch, spelling: str,
) -> None:
    monkeypatch.setattr(transport_mod.sys, "platform", "win32")
    monkeypatch.setenv(spelling, "inherited-fd-state")
    monkeypatch.setenv(NEAR_MATCH_KEY, "keep-near-match")
    monkeypatch.setenv("CHROOT", "keep-chroot")
    monkeypatch.setenv("OPENSSH_STDIO_MODE", "keep-stdio-mode")
    monkeypatch.setenv("PATH", "C:\\tools;C:\\Windows\\System32")
    parent = os.environ.copy()

    child = transport_mod.ssh_child_env()

    _assert_clean_child_env(child, parent)


def test_ssh_child_env_is_none_on_unix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(transport_mod.sys, "platform", "darwin")
    assert transport_mod.ssh_child_env() is None


@pytest.mark.parametrize("payload", [None, b"", b"\x00\xffbinary"])
def test_buffered_ssh_run_uses_clean_child_env_and_preserves_input(
    monkeypatch: pytest.MonkeyPatch, payload: bytes | None,
) -> None:
    parent = _windows_environment(monkeypatch)
    seen: dict[str, object] = {}

    def fake_run(argv, **kwargs):  # noqa: ANN001, ANN003
        seen.update(argv=argv, kwargs=kwargs)
        return subprocess.CompletedProcess(argv, 0, b"ok", b"")

    monkeypatch.setattr(transport_mod.subprocess, "run", fake_run)
    result = _transport()._run(["ssh", "winbox"], input_bytes=payload)

    assert result.stdout == b"ok"
    kwargs = seen["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["input"] == payload
    _assert_clean_child_env(kwargs["env"], parent)


class _RecordingInput:
    def __init__(self) -> None:
        self.data = bytearray()
        self.closed = False

    def write(self, block: bytes) -> int:
        self.data.extend(block)
        return len(block)

    def close(self) -> None:
        self.closed = True


class _FakeProcess:
    def __init__(self) -> None:
        self.stdin = _RecordingInput()


@pytest.mark.parametrize("payload", [None, b"", b"\x00\xffstream"])
def test_streamed_ssh_run_uses_clean_child_env_and_preserves_streaming(
    monkeypatch: pytest.MonkeyPatch, payload: bytes | None,
) -> None:
    parent = _windows_environment(monkeypatch)
    seen: dict[str, object] = {}
    proc = _FakeProcess()

    def fake_popen(argv, **kwargs):  # noqa: ANN001, ANN003
        seen.update(argv=argv, kwargs=kwargs)
        return proc

    def fake_stream(process, on_stdout, timeout, *, process_group):  # noqa: ANN001
        assert process is proc
        assert process_group is True
        if on_stdout is not None:
            on_stdout("streamed output")
        return 0, "streamed output", "streamed error"

    monkeypatch.setattr(transport_mod.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(transport_mod, "_stream_process", fake_stream)
    streamed: list[str] = []
    result = _transport()._run(
        ["ssh", "winbox"], input_bytes=payload, on_stdout=streamed.append,
    )

    assert result.stdout == b"streamed output"
    assert result.stderr == b"streamed error"
    assert streamed == ["streamed output"]
    kwargs = seen["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["stdin"] is (subprocess.PIPE if payload is not None else None)
    _assert_clean_child_env(kwargs["env"], parent)
    assert bytes(proc.stdin.data) == (payload or b"")
    assert proc.stdin.closed is (payload is not None)


@pytest.mark.parametrize("payload", [b"", b"\x00\xffupload"])
def test_remote_file_upload_uses_clean_child_env_and_keeps_binary_bytes(
    monkeypatch: pytest.MonkeyPatch, payload: bytes,
) -> None:
    parent = _windows_environment(monkeypatch)
    seen: dict[str, object] = {}
    proc = _FakeProcess()

    def fake_popen(argv, **kwargs):  # noqa: ANN001, ANN003
        seen.update(argv=argv, kwargs=kwargs)
        return proc

    def fake_stream_file(process, source, timeout):  # noqa: ANN001
        data = source.read()
        process.stdin.write(data)
        process.stdin.close()
        return (
            subprocess.CompletedProcess(seen["argv"], 0, b"", b""),
            len(data),
            hashlib.sha256(data).hexdigest(),
        )

    monkeypatch.setattr(transport_mod.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(transport_mod, "_stream_file_process", fake_stream_file)
    result, count, digest = _transport()._remote_file(
        "winbox", "pwsh-upload", io.BytesIO(payload), timeout=3,
    )

    kwargs = seen["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["stdin"] is subprocess.PIPE
    _assert_clean_child_env(kwargs["env"], parent)
    assert result.returncode == 0
    assert count == len(payload)
    assert digest == hashlib.sha256(payload).hexdigest()
    assert bytes(proc.stdin.data) == payload


@pytest.mark.parametrize("payload", [b"", b"\x00\xffdownload"])
def test_remote_to_local_download_uses_clean_child_env_and_keeps_binary_bytes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, payload: bytes,
) -> None:
    parent = _windows_environment(monkeypatch)
    seen: dict[str, object] = {}

    def fake_run(argv, **kwargs):  # noqa: ANN001, ANN003
        seen.update(argv=argv, kwargs=kwargs)
        kwargs["stdout"].write(payload)
        return subprocess.CompletedProcess(argv, 0, b"", b"")

    monkeypatch.setattr(transport_mod.subprocess, "run", fake_run)
    destination = tmp_path / "output.bin"
    result = _transport()._remote_to_local_file(
        "winbox", "pwsh-download", destination,
    )

    kwargs = seen["kwargs"]
    assert isinstance(kwargs, dict)
    _assert_clean_child_env(kwargs["env"], parent)
    assert kwargs["stdin"] is subprocess.DEVNULL
    assert destination.read_bytes() == payload
    assert result.returncode == 0


def test_unix_ssh_run_passes_inherited_environment_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(transport_mod.sys, "platform", "darwin")
    seen: dict[str, object] = {}

    def fake_run(argv, **kwargs):  # noqa: ANN001, ANN003
        seen.update(kwargs=kwargs)
        return subprocess.CompletedProcess(argv, 0, b"ok", b"")

    monkeypatch.setattr(transport_mod.subprocess, "run", fake_run)
    assert _transport()._run(["ssh", "winbox"]).returncode == 0
    kwargs = seen["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["env"] is None


def test_runner_stream_env_is_clean_for_ssh_and_inherited_otherwise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A runner stream is a local ssh child for SSH transports and a plain process otherwise."""
    parent = _windows_environment(monkeypatch)
    _assert_clean_child_env(_transport().runner_stream_env(), parent)
    local = transport_mod.LocalSimTransport(Device.from_mapping("SIM", {
        "kind": "local-sim", "os": "posix", "project_root": "/tmp/sim",
    }))
    assert local.runner_stream_env() is None


def _capture_run(monkeypatch: pytest.MonkeyPatch, module) -> list[dict]:  # noqa: ANN001
    calls: list[dict] = []

    def fake_run(command, **kwargs):  # noqa: ANN001, ANN003
        calls.append({"command": command, **kwargs})
        return subprocess.CompletedProcess(command, 255, b"", b"Connection timed out")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    return calls


def test_reachability_diagnosis_ssh_uses_clean_child_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from remrun.fleet import resources as resources_mod

    parent = _windows_environment(monkeypatch)
    calls = _capture_run(monkeypatch, resources_mod)
    device = Device.from_mapping("WINBOX", {
        "kind": "ssh-powershell", "os": "windows", "address_candidates": ["winbox"],
        "project_root": "C:\\Users\\you\\projects",
    })
    resources_mod._diagnose(device, "")
    assert calls and all(call["command"][0] == "ssh" for call in calls)
    for call in calls:
        _assert_clean_child_env(call["env"], parent)


def test_mesh_probe_ssh_uses_clean_child_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from remrun.fleet import mesh as mesh_mod

    parent = _windows_environment(monkeypatch)
    calls = _capture_run(monkeypatch, mesh_mod)
    assert mesh_mod._run(["ssh", "winbox", "exit 0"], 5.0)[0] == 255
    _assert_clean_child_env(calls[0]["env"], parent)


def test_unix_direct_ssh_probes_inherit_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    from remrun.fleet import mesh as mesh_mod

    monkeypatch.setattr(transport_mod.sys, "platform", "darwin")
    calls = _capture_run(monkeypatch, mesh_mod)
    mesh_mod._run(["ssh", "macbox", "exit 0"], 5.0)
    assert calls[0]["env"] is None


class _StreamTransport:
    """Stands in for a transport whose runner stream needs a specific child environment."""

    def __init__(self, env: dict[str, str]) -> None:
        self.env = env

    def expand_remote(self, path: str) -> str:
        return path

    def runner_stream_argv(self, *args, **kwargs) -> list[str]:  # noqa: ANN002, ANN003
        return ["ssh", "host", "runner"]

    def runner_stream_env(self) -> dict[str, str]:
        return self.env


def test_key_relay_starts_each_runner_stream_with_its_transport_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from remrun import runner_client

    envs = {"COORD": {"SIDE": "coordinator"}, "TARGET": {"SIDE": "target"}}
    monkeypatch.setattr(runner_client, "ensure_versioned_runner",
                        lambda config, name, install: SimpleNamespace(
                            installed_path="/r", probe={"device_id": name}))
    monkeypatch.setattr(runner_client, "make_transport",
                        lambda device: _StreamTransport(envs[device.name]))
    monkeypatch.setattr(runner_client, "runner_rpc",
                        lambda *args, **kwargs: {"authority_epoch": 1, "key_id": "k"})
    started: list[dict] = []

    class FakePopen:
        def __init__(self, argv, **kwargs):  # noqa: ANN001, ANN003
            started.append(kwargs)
            if len(started) == 2:
                raise OSError("stop after both launches are observed")
            self.stdout = io.BytesIO()

        def poll(self) -> int:
            return 0

    monkeypatch.setattr(runner_client.subprocess, "Popen", FakePopen)
    devices = {name: SimpleNamespace(name=name, state_root="/s") for name in envs}
    config = SimpleNamespace(devices=devices)
    with pytest.raises(runner_client.RunnerClientError):
        runner_client.enroll_target_key(config, "COORD", "TARGET", "cluster")
    assert [call["env"] for call in started] == [envs["COORD"], envs["TARGET"]]


def test_target_owner_stream_starts_with_its_transport_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from remrun import target_resources

    env = {"SIDE": "target-owner"}
    started: list[dict] = []

    def fake_popen(argv, **kwargs):  # noqa: ANN001, ANN003
        started.append(kwargs)
        raise OSError("stop after the launch is observed")

    monkeypatch.setattr(target_resources.subprocess, "Popen", fake_popen)
    client = target_resources.TargetResourceClient(
        config=None, device_name="WINBOX", info=SimpleNamespace(installed_path="/r"),
        transport=_StreamTransport(env), state_root="/s",
    )
    reservation = target_resources.TargetReservation(
        receipt={"allocation_id": "a", "fence": 1, "policy_generation": 1,
                 "policy_digest": "d"},
        token="t",
    )
    with pytest.raises(OSError):
        client.owner_start(reservation, ["python", "-c", "pass"])
    assert started[0]["env"] == env


def test_native_gate_owner_stream_starts_with_its_transport_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The qualification gate launches the same runner stream and needs the same environment."""
    import importlib.util
    from types import SimpleNamespace

    path = Path(__file__).resolve().parents[1] / "native-gates" / "target_resources_native_gate.py"
    spec = importlib.util.spec_from_file_location("target_resources_native_gate", path)
    assert spec is not None and spec.loader is not None
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)

    env = {"SIDE": "native-gate-owner"}
    started: list[dict] = []

    def fake_popen(argv, **kwargs):  # noqa: ANN001, ANN003
        started.append(kwargs)
        raise OSError("stop after the launch is observed")

    monkeypatch.setattr(gate.subprocess, "Popen", fake_popen)
    client = SimpleNamespace(transport=_StreamTransport(env),
                             info=SimpleNamespace(installed_path="/r"), state_root="/s")
    with pytest.raises(OSError):
        gate._spawn_owner(client, None, ["python"], None, None)
    assert started[0]["env"] == env


def test_every_runner_stream_launch_passes_the_transport_env() -> None:
    """Tripwire: a new runner-stream caller that forgets the environment fails here, not on Windows."""
    root = Path(__file__).resolve().parents[1]
    callers = [path for folder in ("src", "scripts", "native-gates")
               for path in (root / folder).rglob("*.py")
               if path.name != "transport.py" and "runner_stream_argv(" in path.read_text(encoding="utf-8")]
    assert callers, "no runner-stream callers found; the tripwire is looking in the wrong place"
    for path in callers:
        text = path.read_text(encoding="utf-8")
        assert text.count("runner_stream_env()") >= text.count("runner_stream_argv("), path
