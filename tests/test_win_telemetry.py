from types import SimpleNamespace

from remrun import _win_telemetry as telemetry


def test_wait_job_empty_returns_immediately_when_drained(monkeypatch):
    monkeypatch.setattr(
        telemetry,
        "_query_basic",
        lambda _job: SimpleNamespace(ActiveProcesses=0),
    )

    assert telemetry._wait_job_empty(object(), timeout_s=0) == (True, 0)


def test_wait_job_empty_is_bounded_when_descendant_survives(monkeypatch):
    monkeypatch.setattr(
        telemetry,
        "_query_basic",
        lambda _job: SimpleNamespace(ActiveProcesses=2),
    )

    assert telemetry._wait_job_empty(object(), timeout_s=0) == (False, 2)


def test_createprocess_uses_no_window(monkeypatch):
    captured = {}

    class Kernel32:
        def CreateProcessW(self, _app, _command_line, _pa, _ta, _inherit, flags,
                           _environment, _cwd, _startup, _process_info):
            captured["flags"] = int(flags)
            return True

    monkeypatch.setattr(telemetry, "_kernel32", lambda: Kernel32())
    monkeypatch.setattr(telemetry, "_startupinfo", lambda: (telemetry.STARTUPINFOW(), False))
    monkeypatch.setattr(
        telemetry,
        "_command_for_createprocess",
        lambda _argv: (None, "example.exe"),
    )

    telemetry._create_suspended(["example.exe"])

    assert captured["flags"] & telemetry.CREATE_SUSPENDED
    assert captured["flags"] & telemetry.CREATE_NO_WINDOW
