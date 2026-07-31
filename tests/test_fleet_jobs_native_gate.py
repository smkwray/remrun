from __future__ import annotations

import importlib.util
import signal
from pathlib import Path
from types import SimpleNamespace

import pytest


GATE_PATH = (
    Path(__file__).resolve().parents[1]
    / "native-gates"
    / "fleet_jobs_native_gate.py"
)
SPEC = importlib.util.spec_from_file_location("fleet_jobs_native_gate", GATE_PATH)
assert SPEC is not None and SPEC.loader is not None
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def test_windows_source_loss_gate_terminates_the_controller_request_not_getppid():
    source = GATE_PATH.read_text(encoding="utf-8")

    assert "os.getppid" not in source
    assert "_terminate_source_worker(worker)" in source
    assert "source SSH request terminated" in source
    assert "start_new_session" in source


def test_source_worker_argv_round_trips_a_bounded_private_payload():
    payload = {
        "repo": "/repo",
        "target": "windows-target",
        "command": ["python", "-S", "-c", "print('x')"],
        "predicted_rss_mb": 256.0,
    }

    argv = GATE._source_worker_argv(payload)

    assert argv[-2] == "--_worker-payload-b64"
    assert GATE._decode_worker_payload(argv[-1]) == payload


def test_marker_wait_accepts_windows_text_newlines(monkeypatch):
    monkeypatch.setattr(GATE, "_read_marker", lambda *_args: b"armed\r\n")

    GATE._wait_for_marker(
        device_name="windows-target",
        transport=object(),
        marker=r"C:\\state\\armed.marker",
        expected=b"armed\n",
        timeout=0.1,
    )


def test_posix_source_worker_termination_kills_its_local_ssh_process_group(
    monkeypatch,
):
    calls = []

    class Worker:
        pid = 4321
        returncode = None

        def poll(self):
            return None

        def wait(self, timeout):
            calls.append(("wait", timeout))
            self.returncode = -signal.SIGTERM
            return self.returncode

        def communicate(self, timeout):
            calls.append(("communicate", timeout))
            return "", ""

    monkeypatch.setattr(GATE.os, "name", "posix")
    monkeypatch.setattr(
        GATE.os,
        "killpg",
        lambda pid, sig: calls.append(("killpg", pid, sig)),
    )

    assert GATE._terminate_source_worker(Worker()) == ("", "")
    assert calls[0] == ("killpg", 4321, signal.SIGTERM)
    assert calls[1:] == [("wait", 2.0), ("communicate", 5.0)]


def test_guarded_probe_uses_declared_reservation_and_telemetry_on():
    reservation = object()
    admission_payload = {"status": "admitted", "predicted_rss_bytes": 256 * 1024**2}
    expected = object()
    calls = []

    class Transport:
        memory_guard = object()
        device = SimpleNamespace(name="guarded-target")

        def reserve_memory_guard(self, *, predicted_rss_mb):
            calls.append(("reserve", predicted_rss_mb))
            return SimpleNamespace(
                status="admitted",
                reason="capacity_available",
                admitted=True,
                reservation=reservation,
                payload=admission_payload,
            )

        def exec_observed(self, command, cwd, **kwargs):
            calls.append(("exec", command, cwd, kwargs))
            return expected

        def release_memory_guard(self, *_args, **_kwargs):
            raise AssertionError("successful exec owns reservation release")

    observation = object()
    result, payload = GATE._exec_observed_probe(
        Transport(),
        ["python3", "-S", "-c", "print('bounded')"],
        "/state",
        observation=observation,
        timeout=45.0,
        predicted_rss_mb=256.0,
    )

    assert result is expected
    assert payload == admission_payload
    assert calls[0] == ("reserve", 256.0)
    _, command, cwd, kwargs = calls[1]
    assert command[-1] == "print('bounded')"
    assert cwd == "/state"
    assert kwargs["observation"] is observation
    assert kwargs["timeout"] == 45.0
    assert kwargs["telemetry"] is True
    assert kwargs["memory_reservation"] is reservation


def test_guarded_probe_refusal_is_truthful_and_never_executes_user_code():
    class Transport:
        memory_guard = object()
        device = SimpleNamespace(name="guarded-target")

        def reserve_memory_guard(self, *, predicted_rss_mb):
            assert predicted_rss_mb == 256.0
            return SimpleNamespace(
                status="refused",
                reason="insufficient_live_memory",
                admitted=False,
                reservation=None,
                payload={"status": "refused", "reason": "insufficient_live_memory"},
            )

        def exec_observed(self, *_args, **_kwargs):
            raise AssertionError("refused reservation must not execute user code")

    with pytest.raises(GATE.GateFailure, match="bounded observer reservation refused"):
        GATE._exec_observed_probe(
            Transport(),
            ["python3", "-S", "-c", "print('bounded')"],
            "/state",
            observation=object(),
            timeout=45.0,
            predicted_rss_mb=256.0,
        )


def test_native_gate_keeps_workload_and_lifetime_bounds():
    source = GATE_PATH.read_text(encoding="utf-8")
    args = GATE._parser().parse_args(["--target", "windows-target"])

    assert GATE.MAX_CHILD_SECONDS == 35.0
    assert args.child_seconds == 35.0
    assert args.predicted_rss_mb == 256.0
    assert source.count("bytearray(4*1024*1024)") >= 3
    assert "os.environ[" not in source
    assert "os.putenv" not in source
