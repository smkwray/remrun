import subprocess
import sys
from types import SimpleNamespace

import pytest

from remrun import _win_telemetry as telemetry


def test_createprocess_accepts_encoded_powershell_as_one_native_application(
    monkeypatch,
):
    argv = [
        "powershell",
        "-NoProfile",
        "-NonInteractive",
        "-EncodedCommand",
        "VwByAGkAdABlAC0ATwB1AHQAcAB1AHQAIAAxAA==",
    ]
    resolved = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
    monkeypatch.setattr(
        telemetry.shutil,
        "which",
        lambda command: resolved if command == "powershell" else None,
    )

    app, command_line = telemetry._command_for_createprocess(argv)

    assert app == resolved
    assert command_line == subprocess.list2cmdline([resolved, *argv[1:]])
    assert "cmd.exe" not in command_line.lower()


@pytest.mark.parametrize("extension", [".cmd", ".bat"])
def test_createprocess_never_invents_an_unsafe_cmd_quoting_layer(
    extension, monkeypatch
):
    batch = rf"C:\Tools\build{extension}"
    monkeypatch.setattr(telemetry.shutil, "which", lambda _command: batch)

    with pytest.raises(OSError, match="encoded PowerShell"):
        telemetry._command_for_createprocess(
            ["build", "literal&next", "100%", "caret^x"]
        )


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


def test_detailed_payload_preserves_job_memory_and_drain_qualification(monkeypatch):
    monkeypatch.setattr(
        telemetry,
        "_query_basic",
        lambda _job: SimpleNamespace(
            TotalUserTime=15_000_000,
            TotalKernelTime=5_000_000,
        ),
    )
    monkeypatch.setattr(
        telemetry,
        "_query_extended",
        lambda _job: SimpleNamespace(PeakJobMemoryUsed=3 * 1024 * 1024 * 1024),
    )
    samples = telemetry._PressureSamples()
    samples.count = 4
    samples.total_memory = 64 * 1024**3
    samples.min_available = 20 * 1024**3
    samples.max_used_pct = 68.75
    samples._cpu_busy = [25.0, 75.0]
    samples.gpu_status = "measured"
    samples.gpus["1"] = {
        "id": "1",
        "name": "GPU",
        "max_util_pct": 80.0,
        "min_vram_free_bytes": 2 * 1024**3,
        "vram_total_bytes": 16 * 1024**3,
    }

    payload = telemetry._detailed_payload(
        object(),
        1.0,
        samples,
        process_tree_drained=False,
        active_processes_at_cutoff=2,
    )

    assert payload["memory"]["metric"] == "job_memory_peak"
    assert payload["memory"]["peak_bytes"] == 3 * 1024**3
    assert payload["memory"]["coverage"] == "job_object_cutoff"
    assert payload["peak_rss_mb"] == 3072.0
    assert payload["cpu"]["cpu_sec"] == 2.0
    assert payload["cpu"]["coverage"] == "job_object_cutoff"
    assert payload["avg_cpu_pct"] == 200.0
    assert payload["process_tree_drained"] is False
    assert payload["active_processes_at_cutoff"] == 2
    assert payload["gpu"]["scope"] == "whole_device"
    assert payload["gpu"]["devices"][0]["id"] == "1"


def test_detailed_sampler_failure_is_explicit_unknown():
    payload = telemetry._unknown_detailed_payload("counter failed", 1.25)

    assert payload["status"] == "unavailable"
    assert payload["cpu"]["coverage"] == "sampler_failed"
    assert payload["memory"]["coverage"] == "sampler_failed"
    assert payload["memory"]["metric"] == "job_memory_peak"
    assert payload["peak_rss_mb"] is None
    assert payload["process_tree_drained"] is None


def test_outer_detailed_failure_never_executes_command_twice(
    tmp_path, monkeypatch
):
    marker = tmp_path / "executions.txt"

    def command_started_then_failed(_argv):
        marker.write_text(
            marker.read_text() + "x" if marker.exists() else "x",
            encoding="utf-8",
        )
        raise RuntimeError("late wrapper failure")

    monkeypatch.setattr(telemetry.os, "name", "nt")
    monkeypatch.setattr(
        telemetry,
        "_job_run_detailed",
        command_started_then_failed,
    )
    monkeypatch.setattr(
        telemetry,
        "_plain_run",
        lambda _argv: (_ for _ in ()).throw(
            AssertionError("post-start fallback must not execute argv again")
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["_win_telemetry.py", "--detailed", "--", "marker-command"],
    )

    assert telemetry.main() == 1
    assert marker.read_text(encoding="utf-8") == "x"


def test_prestart_detailed_failure_runs_the_encoded_shell_argv_once(monkeypatch):
    argv = [
        "powershell",
        "-NoProfile",
        "-NonInteractive",
        "-EncodedCommand",
        "encoded",
    ]
    calls = []

    monkeypatch.setattr(telemetry.os, "name", "nt")
    monkeypatch.setattr(
        telemetry,
        "_job_run_detailed",
        lambda _argv: (_ for _ in ()).throw(
            telemetry._CommandNotStarted("CreateProcessW failed")
        ),
    )
    monkeypatch.setattr(
        telemetry,
        "_plain_run",
        lambda plain_argv: calls.append(plain_argv) or 6,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["_win_telemetry.py", "--detailed", "--", *argv],
    )

    assert telemetry.main() == 6
    assert calls == [argv]


def test_broken_telemetry_stderr_cannot_replace_command_exit(monkeypatch):
    class BrokenBuffer:
        def write(self, _value):
            raise OSError("closed stderr")

        def flush(self):
            raise OSError("closed stderr")

    class BrokenStderr(BrokenBuffer):
        buffer = BrokenBuffer()

    def completed_with_payload(_argv):
        telemetry._emit_payload(telemetry._unknown_detailed_payload("test"))
        return 7

    monkeypatch.setattr(telemetry.os, "name", "nt")
    monkeypatch.setattr(telemetry, "_job_run_detailed", completed_with_payload)
    monkeypatch.setattr(
        sys,
        "argv",
        ["_win_telemetry.py", "--detailed", "--", "command"],
    )
    monkeypatch.setattr(sys, "stderr", BrokenStderr())

    assert telemetry.main() == 7


@pytest.mark.parametrize(
    "row",
    [
        "0, GPU, nan, 100, 200\n",
        "0, GPU, 101, 100, 200\n",
        "0, GPU, -1, 100, 200\n",
        "0, GPU, 50, -1, 200\n",
        "0, GPU, 50, 201, 200\n",
        "0, GPU, 50, 100, inf\n",
    ],
)
def test_nvidia_parser_rejects_nonfinite_and_out_of_range_values(
    row, monkeypatch
):
    monkeypatch.setattr(
        telemetry.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout=row),
    )

    status, devices = telemetry._gpu_reading()

    assert status == "unavailable"
    assert devices == []


def test_gpu_aggregate_downgrades_after_later_failed_sample(monkeypatch):
    valid = {
        "id": "GPU-a",
        "name": "GPU",
        "util_pct": 25.0,
        "vram_free_bytes": 100,
        "vram_total_bytes": 200,
    }
    readings = iter(
        [
            ("measured", [valid]),
            ("unavailable", []),
        ]
    )
    monkeypatch.setattr(telemetry, "_gpu_reading", lambda: next(readings))
    samples = telemetry._PressureSamples()

    samples._sample_gpu()
    samples._sample_gpu()
    payload = samples.gpu_payload()

    assert payload["status"] == "partial"
    assert payload["sample_count"] == 2
    assert payload["failed_sample_count"] == 1
    assert payload["max_util_pct"] == 25.0
    assert payload["min_vram_free_bytes"] == 100




def test_staged_observed_argv_is_bounded_validated_and_removed(tmp_path):
    request = tmp_path / "argv.json"
    request.write_text('["pwsh", "-EncodedCommand", "encoded"]', encoding="utf-8")

    assert telemetry._load_staged_argv(str(request)) == [
        "pwsh",
        "-EncodedCommand",
        "encoded",
    ]
    assert not request.exists()


def test_invalid_staged_observed_argv_is_removed_before_refusal(tmp_path):
    request = tmp_path / "argv.json"
    request.write_text('{"not": "argv"}', encoding="utf-8")

    with pytest.raises(ValueError, match="bounded string array"):
        telemetry._load_staged_argv(str(request))

    assert not request.exists()



def test_observed_breakaway_telemetry_is_explicit_unknown_not_wrapper_metrics():
    payload = telemetry._observed_breakaway_payload(1.25)

    assert payload["status"] == "unavailable"
    assert payload["coverage"] == "observer_wrapper_only"
    assert payload["cpu"]["coverage"] == "observer_wrapper_only"
    assert payload["memory"]["coverage"] == "observer_wrapper_only"
    assert payload["peak_rss_mb"] is None
    assert payload["avg_cpu_pct"] is None
    assert payload["cpu_sec"] is None
    assert payload["process_tree_drained"] is None
    assert "separate inner Job" in payload["detail"]


def test_observed_breakaway_programs_only_the_explicit_job_limit(monkeypatch):
    captured = {}

    class Kernel32:
        def SetInformationJobObject(self, _job, info_class, pointer, size):
            captured["class"] = info_class
            captured["size"] = int(size)
            info = telemetry.ctypes.cast(
                pointer,
                telemetry.ctypes.POINTER(
                    telemetry.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
                ),
            ).contents
            captured["flags"] = int(info.BasicLimitInformation.LimitFlags)
            return True

    monkeypatch.setattr(telemetry, "_kernel32", lambda: Kernel32())

    telemetry._enable_observed_breakaway(object())

    assert captured["class"] == telemetry.JobObjectExtendedLimitInformation
    assert captured["size"] == telemetry.ctypes.sizeof(
        telemetry.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
    )
    assert captured["flags"] == telemetry.JOB_OBJECT_LIMIT_BREAKAWAY_OK


def test_observed_breakaway_is_enabled_before_user_create(monkeypatch):
    order = []

    class Kernel32:
        def CreateJobObjectW(self, _security, _name):
            order.append("create-job")
            return telemetry.HANDLE(1)

    monkeypatch.setattr(telemetry, "_kernel32", lambda: Kernel32())
    monkeypatch.setattr(
        telemetry,
        "_enable_observed_breakaway",
        lambda _job: order.append("enable-breakaway"),
    )
    monkeypatch.setattr(
        telemetry,
        "_create_suspended",
        lambda _argv: order.append("create-user")
        or (_ for _ in ()).throw(OSError("pre-start failure")),
    )
    monkeypatch.setattr(telemetry, "_close_handle", lambda _handle: None)

    with pytest.raises(OSError, match="pre-start failure"):
        telemetry._job_run(["example.exe"], allow_observed_breakaway=True)

    assert order == ["create-job", "enable-breakaway", "create-user"]


def test_staged_observed_argv_routes_to_breakaway_job_once(tmp_path, monkeypatch):
    request = tmp_path / "argv.json"
    argv = ["pwsh", "-EncodedCommand", "encoded"]
    request.write_text(__import__("json").dumps(argv), encoding="utf-8")
    calls = []

    monkeypatch.setattr(telemetry.os, "name", "nt")
    monkeypatch.setattr(
        telemetry,
        "_job_run",
        lambda command, *, allow_observed_breakaway=False: calls.append(
            (command, allow_observed_breakaway)
        )
        or 7,
    )
    monkeypatch.setattr(
        telemetry,
        "_plain_run",
        lambda _command: (_ for _ in ()).throw(
            AssertionError("successful staged launch must not fall back")
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "_win_telemetry.py",
            "--allow-observed-breakaway",
            "--argv-json-file",
            str(request),
            "--",
        ],
    )

    assert telemetry.main() == 7
    assert calls == [(argv, True)]
    assert not request.exists()


def test_observed_breakaway_poststart_failure_never_retries_user_argv(monkeypatch):
    calls = []

    def fail_after_possible_start(command, *, allow_observed_breakaway=False):
        calls.append((command, allow_observed_breakaway))
        raise RuntimeError("late failure")

    monkeypatch.setattr(telemetry.os, "name", "nt")
    monkeypatch.setattr(telemetry, "_job_run_detailed", fail_after_possible_start)
    monkeypatch.setattr(
        telemetry,
        "_plain_run",
        lambda _command: (_ for _ in ()).throw(
            AssertionError("post-start uncertainty must never retry")
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "_win_telemetry.py",
            "--detailed",
            "--allow-observed-breakaway",
            "--",
            "pwsh",
            "-EncodedCommand",
            "encoded",
        ],
    )

    assert telemetry.main() == 1
    assert calls == [(["pwsh", "-EncodedCommand", "encoded"], True)]


def test_observed_breakaway_prestart_failure_falls_back_exactly_once(monkeypatch):
    argv = ["pwsh", "-EncodedCommand", "encoded"]
    calls = []

    def fail_before_start(command, *, allow_observed_breakaway=False):
        calls.append(("job", command, allow_observed_breakaway))
        raise OSError("CreateJobObjectW failed")

    monkeypatch.setattr(telemetry.os, "name", "nt")
    monkeypatch.setattr(telemetry, "_job_run", fail_before_start)
    monkeypatch.setattr(
        telemetry,
        "_plain_run",
        lambda command: calls.append(("plain", command)) or 6,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "_win_telemetry.py",
            "--allow-observed-breakaway",
            "--",
            *argv,
        ],
    )

    assert telemetry.main() == 6
    assert calls == [("job", argv, True), ("plain", argv)]


def test_observed_breakaway_is_rejected_on_the_memory_guard_path(
    monkeypatch, capsys
):
    monkeypatch.setattr(
        telemetry,
        "_guarded_job_run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("guarded user execution must not begin")
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "_win_telemetry.py",
            "--allow-observed-breakaway",
            "--guard-max-bytes",
            "1024",
            "--guard-min-available-bytes",
            "1024",
            "--guard-token",
            "a" * 32,
            "--",
            "example.exe",
        ],
    )

    assert telemetry.main() == 2
    assert "incompatible with the memory guard" in capsys.readouterr().err

def _guard_payload_from_stderr(stderr: str, token: str) -> dict:
    import json

    marker = f"__REMRUN_GUARD_RESULT_{token}__ "
    return json.loads(stderr.rsplit(marker, 1)[1].splitlines()[0])


def test_windows_guard_programs_job_memory_and_kill_on_close(monkeypatch):
    captured = {}

    class Kernel32:
        def SetInformationJobObject(self, _job, info_class, pointer, size):
            captured["class"] = info_class
            captured["size"] = int(size)
            info = telemetry.ctypes.cast(
                pointer,
                telemetry.ctypes.POINTER(
                    telemetry.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
                ),
            ).contents
            captured["flags"] = int(info.BasicLimitInformation.LimitFlags)
            captured["limit"] = int(info.JobMemoryLimit)
            return True

    monkeypatch.setattr(telemetry, "_kernel32", lambda: Kernel32())

    telemetry._set_job_guard_limits(object(), 40 * 1024**3)

    assert captured["class"] == telemetry.JobObjectExtendedLimitInformation
    assert captured["size"] == telemetry.ctypes.sizeof(
        telemetry.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
    )
    assert captured["flags"] & telemetry.JOB_OBJECT_LIMIT_JOB_MEMORY
    assert captured["flags"] & telemetry.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    assert captured["limit"] == 40 * 1024**3


def test_windows_guard_refuses_low_initial_memory_before_createprocess(
    monkeypatch, capsys
):
    token = "a" * 32

    class Kernel32:
        def CreateJobObjectW(self, _security, _name):
            return telemetry.HANDLE(1)

    monkeypatch.setattr(telemetry.os, "name", "nt")
    monkeypatch.setattr(telemetry, "_kernel32", lambda: Kernel32())
    monkeypatch.setattr(telemetry, "_set_job_guard_limits", lambda *_args: None)
    monkeypatch.setattr(
        telemetry, "_associate_guard_completion_port", lambda _job: telemetry.HANDLE(2)
    )
    monkeypatch.setattr(telemetry, "_system_memory", lambda: (1024, 100))
    monkeypatch.setattr(
        telemetry,
        "_create_suspended",
        lambda _argv: (_ for _ in ()).throw(
            AssertionError("CreateProcessW must not run below the reserve")
        ),
    )
    monkeypatch.setattr(telemetry, "_close_handle", lambda _handle: None)

    rc = telemetry._guarded_job_run(
        ["example.exe"],
        max_command_bytes=400,
        min_available_bytes=100,
        token=token,
        detailed=False,
        telemetry=False,
    )

    payload = _guard_payload_from_stderr(capsys.readouterr().err, token)
    assert rc == telemetry.GUARD_HELPER_EXIT
    assert payload["status"] == "refused"
    assert payload["reason"] == "host_memory_reserve"
    assert payload["command_started"] is False



def test_windows_guard_refuses_when_initial_memory_read_is_unavailable(
    monkeypatch, capsys
):
    token = "e" * 32

    class Kernel32:
        def CreateJobObjectW(self, _security, _name):
            return telemetry.HANDLE(1)

    monkeypatch.setattr(telemetry.os, "name", "nt")
    monkeypatch.setattr(telemetry, "_kernel32", lambda: Kernel32())
    monkeypatch.setattr(telemetry, "_set_job_guard_limits", lambda *_args: None)
    monkeypatch.setattr(
        telemetry, "_associate_guard_completion_port", lambda _job: telemetry.HANDLE(2)
    )
    monkeypatch.setattr(
        telemetry,
        "_system_memory",
        lambda: (_ for _ in ()).throw(OSError("host counters unavailable")),
    )
    monkeypatch.setattr(
        telemetry,
        "_create_suspended",
        lambda _argv: (_ for _ in ()).throw(
            AssertionError("CreateProcessW must not run without initial counters")
        ),
    )
    monkeypatch.setattr(telemetry, "_close_handle", lambda _handle: None)

    rc = telemetry._guarded_job_run(
        ["example.exe"],
        max_command_bytes=400,
        min_available_bytes=100,
        token=token,
        detailed=False,
        telemetry=False,
    )

    payload = _guard_payload_from_stderr(capsys.readouterr().err, token)
    assert rc == telemetry.GUARD_HELPER_EXIT
    assert payload["status"] == "refused"
    assert payload["reason"] == "guard_initialization_failed"
    assert payload["command_started"] is False

def test_windows_guard_assignment_failure_never_resumes_user_code(
    monkeypatch, capsys
):
    token = "b" * 32
    terminated = []

    class Kernel32:
        def CreateJobObjectW(self, _security, _name):
            return telemetry.HANDLE(1)

        def AssignProcessToJobObject(self, _job, _process):
            return False

        def TerminateProcess(self, process, code):
            terminated.append((process, code))
            return True

    pi = telemetry.PROCESS_INFORMATION()
    pi.hProcess = telemetry.HANDLE(3)
    pi.hThread = telemetry.HANDLE(4)
    monkeypatch.setattr(telemetry.os, "name", "nt")
    monkeypatch.setattr(telemetry, "_kernel32", lambda: Kernel32())
    monkeypatch.setattr(telemetry, "_set_job_guard_limits", lambda *_args: None)
    monkeypatch.setattr(
        telemetry, "_associate_guard_completion_port", lambda _job: telemetry.HANDLE(2)
    )
    monkeypatch.setattr(
        telemetry, "_system_memory", lambda: (64 * 1024**3, 48 * 1024**3)
    )
    monkeypatch.setattr(telemetry, "_create_suspended", lambda _argv: pi)
    monkeypatch.setattr(
        telemetry,
        "_resume",
        lambda _pi: (_ for _ in ()).throw(
            AssertionError("an unassigned process must remain suspended")
        ),
    )
    monkeypatch.setattr(telemetry, "_close_handle", lambda _handle: None)

    rc = telemetry._guarded_job_run(
        ["example.exe"],
        max_command_bytes=40 * 1024**3,
        min_available_bytes=16 * 1024**3,
        token=token,
        detailed=False,
        telemetry=False,
    )

    payload = _guard_payload_from_stderr(capsys.readouterr().err, token)
    assert rc == telemetry.GUARD_HELPER_EXIT
    assert payload["status"] == "refused"
    assert payload["command_started"] is False
    assert terminated and terminated[0][1] == telemetry.GUARD_HELPER_EXIT


def test_windows_guard_threshold_terminates_whole_job(monkeypatch, capsys):
    token = "c" * 32
    terminated = []

    class Kernel32:
        def CreateJobObjectW(self, _security, _name):
            return telemetry.HANDLE(1)

        def AssignProcessToJobObject(self, _job, _process):
            return True

    pi = telemetry.PROCESS_INFORMATION()
    pi.hProcess = telemetry.HANDLE(3)
    pi.hThread = telemetry.HANDLE(4)
    monkeypatch.setattr(telemetry.os, "name", "nt")
    monkeypatch.setattr(telemetry, "_kernel32", lambda: Kernel32())
    monkeypatch.setattr(telemetry, "_set_job_guard_limits", lambda *_args: None)
    monkeypatch.setattr(
        telemetry, "_associate_guard_completion_port", lambda _job: telemetry.HANDLE(2)
    )
    monkeypatch.setattr(
        telemetry, "_system_memory", lambda: (64 * 1024**3, 48 * 1024**3)
    )
    monkeypatch.setattr(telemetry, "_create_suspended", lambda _argv: pi)
    monkeypatch.setattr(telemetry, "_resume", lambda _pi: None)
    monkeypatch.setattr(
        telemetry,
        "_query_extended",
        lambda _job: SimpleNamespace(PeakJobMemoryUsed=40 * 1024**3),
    )
    monkeypatch.setattr(telemetry, "_poll_guard_messages", lambda _port: [])
    monkeypatch.setattr(
        telemetry,
        "_record_guard_pressure",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("optional pressure telemetry must stay off")
        ),
    )
    monkeypatch.setattr(
        telemetry,
        "_terminate_job_bounded",
        lambda job: terminated.append(job) or True,
    )
    monkeypatch.setattr(telemetry, "_query_basic", lambda _job: SimpleNamespace(ActiveProcesses=0))
    monkeypatch.setattr(telemetry, "_emit_guard_telemetry", lambda *_a, **_k: None)
    monkeypatch.setattr(telemetry, "_close_handle", lambda _handle: None)

    rc = telemetry._guarded_job_run(
        ["example.exe"],
        max_command_bytes=40 * 1024**3,
        min_available_bytes=16 * 1024**3,
        token=token,
        detailed=False,
        telemetry=False,
    )

    payload = _guard_payload_from_stderr(capsys.readouterr().err, token)
    assert rc == telemetry.GUARD_HELPER_EXIT
    assert payload["status"] == "terminated"
    assert payload["reason"] == "command_memory_limit"
    assert payload["cleanup_complete"] is True
    assert payload["detached_children_possible"] is True
    assert payload["ordinary_child_breakaway_possible"] is False
    assert payload["external_broker_escape_possible"] is True
    assert terminated



def test_windows_guard_reports_failed_safe_when_job_drain_is_unverified(
    monkeypatch, capsys
):
    token = "f" * 32

    class Kernel32:
        def CreateJobObjectW(self, _security, _name):
            return telemetry.HANDLE(1)

        def AssignProcessToJobObject(self, _job, _process):
            return True

    pi = telemetry.PROCESS_INFORMATION()
    pi.hProcess = telemetry.HANDLE(3)
    pi.hThread = telemetry.HANDLE(4)
    monkeypatch.setattr(telemetry.os, "name", "nt")
    monkeypatch.setattr(telemetry, "_kernel32", lambda: Kernel32())
    monkeypatch.setattr(telemetry, "_set_job_guard_limits", lambda *_args: None)
    monkeypatch.setattr(
        telemetry, "_associate_guard_completion_port", lambda _job: telemetry.HANDLE(2)
    )
    monkeypatch.setattr(
        telemetry, "_system_memory", lambda: (64 * 1024**3, 48 * 1024**3)
    )
    monkeypatch.setattr(telemetry, "_create_suspended", lambda _argv: pi)
    monkeypatch.setattr(telemetry, "_resume", lambda _pi: None)
    monkeypatch.setattr(
        telemetry,
        "_query_extended",
        lambda _job: SimpleNamespace(PeakJobMemoryUsed=40 * 1024**3),
    )
    monkeypatch.setattr(telemetry, "_poll_guard_messages", lambda _port: [])
    monkeypatch.setattr(telemetry, "_terminate_job_bounded", lambda _job: False)
    monkeypatch.setattr(
        telemetry, "_query_basic", lambda _job: SimpleNamespace(ActiveProcesses=1)
    )
    monkeypatch.setattr(telemetry, "_emit_guard_telemetry", lambda *_a, **_k: None)
    monkeypatch.setattr(telemetry, "_close_handle", lambda _handle: None)

    rc = telemetry._guarded_job_run(
        ["example.exe"],
        max_command_bytes=40 * 1024**3,
        min_available_bytes=16 * 1024**3,
        token=token,
        detailed=False,
        telemetry=False,
    )

    payload = _guard_payload_from_stderr(capsys.readouterr().err, token)
    assert rc == telemetry.GUARD_HELPER_EXIT
    assert payload["status"] == "failed_safe"
    assert payload["reason"] == "termination_cleanup_failed"
    assert payload["trigger_reason"] == "command_memory_limit"
    assert payload["cleanup_complete"] is False

def test_windows_guard_sampling_failure_after_resume_terminates_job(
    monkeypatch, capsys
):
    token = "d" * 32
    calls = 0
    terminated = []

    class Kernel32:
        def CreateJobObjectW(self, _security, _name):
            return telemetry.HANDLE(1)

        def AssignProcessToJobObject(self, _job, _process):
            return True

    def system_memory():
        nonlocal calls
        calls += 1
        if calls == 1:
            return 64 * 1024**3, 48 * 1024**3
        raise OSError("GlobalMemoryStatusEx failed")

    pi = telemetry.PROCESS_INFORMATION()
    pi.hProcess = telemetry.HANDLE(3)
    pi.hThread = telemetry.HANDLE(4)
    monkeypatch.setattr(telemetry.os, "name", "nt")
    monkeypatch.setattr(telemetry, "_kernel32", lambda: Kernel32())
    monkeypatch.setattr(telemetry, "_set_job_guard_limits", lambda *_args: None)
    monkeypatch.setattr(
        telemetry, "_associate_guard_completion_port", lambda _job: telemetry.HANDLE(2)
    )
    monkeypatch.setattr(telemetry, "_system_memory", system_memory)
    monkeypatch.setattr(telemetry, "_create_suspended", lambda _argv: pi)
    monkeypatch.setattr(telemetry, "_resume", lambda _pi: None)
    monkeypatch.setattr(
        telemetry,
        "_terminate_job_bounded",
        lambda job: terminated.append(job) or True,
    )
    monkeypatch.setattr(telemetry, "_query_basic", lambda _job: SimpleNamespace(ActiveProcesses=0))
    monkeypatch.setattr(telemetry, "_emit_guard_telemetry", lambda *_a, **_k: None)
    monkeypatch.setattr(telemetry, "_close_handle", lambda _handle: None)

    rc = telemetry._guarded_job_run(
        ["example.exe"],
        max_command_bytes=40 * 1024**3,
        min_available_bytes=16 * 1024**3,
        token=token,
        detailed=False,
        telemetry=False,
    )

    payload = _guard_payload_from_stderr(capsys.readouterr().err, token)
    assert rc == telemetry.GUARD_HELPER_EXIT
    assert payload["status"] == "failed_safe"
    assert payload["reason"] == "enforcement_sampling_failed"
    assert payload["cleanup_complete"] is True
    assert terminated
