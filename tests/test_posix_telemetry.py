from __future__ import annotations

import base64
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("resource")

from remrun import _posix_telemetry as telemetry


HELPER = Path(telemetry.__file__).resolve()


def _run(program: str, *, timeout: float = 10.0) -> tuple[subprocess.CompletedProcess, dict]:
    result = subprocess.run(
        [
            sys.executable,
            str(HELPER),
            "--detailed",
            "--",
            sys.executable,
            "-c",
            program,
        ],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    marker = "__REMRUN_TELEMETRY__ "
    payload = json.loads(result.stderr.rsplit(marker, 1)[1].splitlines()[0])
    return result, payload


def test_overlapping_children_report_concurrent_rss_sum_not_largest_child():
    child = "import time; x=bytearray(24*1024*1024); time.sleep(0.65)"
    concurrent = (
        "import subprocess,sys\n"
        f"code={child!r}\n"
        "children=[subprocess.Popen([sys.executable,'-c',code]) for _ in range(2)]\n"
        "[item.wait() for item in children]\n"
    )
    result, payload = _run(concurrent)

    assert result.returncode == 0
    assert payload["memory"]["metric"] == "rss_sum_sampled"
    assert payload["memory"]["sample_interval_ms"] == int(
        telemetry.SAMPLE_INTERVAL_S * 1000
    )
    assert payload["memory"]["sample_count"] >= 2
    assert payload["memory"]["shared_page_semantics"] == "may_double_count"
    # Two simultaneous 24 MiB private buffers plus interpreter RSS must be
    # materially larger than either child by itself.
    assert payload["memory"]["peak_bytes"] > 48 * 1024 * 1024
    assert payload["peak_rss_mb"] == round(payload["memory"]["peak_bytes"] / 1048576, 1)


def test_sequential_children_do_not_report_a_false_simultaneous_sum():
    child = "import time; x=bytearray(24*1024*1024); time.sleep(0.45)"
    sequential = (
        "import subprocess,sys\n"
        f"code={child!r}\n"
        "subprocess.run([sys.executable,'-c',code],check=True)\n"
        "subprocess.run([sys.executable,'-c',code],check=True)\n"
    )
    concurrent = (
        "import subprocess,sys\n"
        f"code={child!r}\n"
        "children=[subprocess.Popen([sys.executable,'-c',code]) for _ in range(2)]\n"
        "[item.wait() for item in children]\n"
    )

    sequential_result, sequential_payload = _run(sequential)
    concurrent_result, concurrent_payload = _run(concurrent)

    assert sequential_result.returncode == concurrent_result.returncode == 0
    assert (
        concurrent_payload["memory"]["peak_bytes"]
        > sequential_payload["memory"]["peak_bytes"] * 1.3
    )


def test_short_job_has_explicit_lower_confidence_coverage():
    result, payload = _run("pass")

    assert result.returncode == 0
    assert payload["memory"]["coverage"] in {
        "short_lived_sampled",
        "short_lived_unobserved",
    }
    assert payload["memory"]["coverage"] != "known_tree_drained"


def test_detached_child_wait_is_bounded_and_direct_exit_is_authoritative():
    program = (
        "import subprocess,sys,time\n"
        "subprocess.Popen([sys.executable,'-c','import time;time.sleep(3)'],"
        " start_new_session=True,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,"
        " stderr=subprocess.DEVNULL)\n"
        "time.sleep(0.3)\n"
        "sys.exit(7)\n"
    )
    result, payload = _run(program, timeout=4.0)

    assert result.returncode == 7
    assert payload["wall_sec"] < 2.0
    assert payload["process_tree_drained"] is False
    assert payload["active_processes_at_cutoff"] >= 1
    assert payload["detached_children_possible"] is True
    assert payload["memory"]["coverage"] == "known_tree_cutoff"


def test_sampler_failure_preserves_command_result_with_explicit_unknown(monkeypatch):
    monkeypatch.setattr(
        telemetry,
        "_processes",
        lambda: (_ for _ in ()).throw(RuntimeError("process table unavailable")),
    )

    rc, payload = telemetry._detailed_run([sys.executable, "-c", "raise SystemExit(5)"])

    assert rc == 5
    assert payload["status"] == "partial"
    assert payload["memory"]["peak_bytes"] is None
    assert payload["memory"]["coverage"] == "sampler_failed"
    assert payload["peak_rss_mb"] is None
    assert payload["cpu"]["cpu_sec"] is not None
    assert payload["cpu"]["coverage"] == "wait4_known_tree_cutoff"
    assert payload["process_tree_drained"] is False
    assert "active_processes_at_cutoff" not in payload


def test_post_command_payload_failure_never_executes_command_twice(
    tmp_path, monkeypatch
):
    marker = tmp_path / "executions.txt"
    program = (
        "from pathlib import Path\n"
        f"p=Path({str(marker)!r})\n"
        "p.write_text(p.read_text()+'x' if p.exists() else 'x')\n"
        "raise SystemExit(9)\n"
    )
    monkeypatch.setattr(
        telemetry.Samples,
        "gpu_payload",
        lambda _self: (_ for _ in ()).throw(RuntimeError("late payload failure")),
    )

    rc, payload = telemetry._detailed_run([sys.executable, "-c", program])

    assert rc == 9
    assert marker.read_text() == "x"
    assert payload["status"] == "unavailable"
    assert payload["memory"]["coverage"] == "sampler_failed"
    assert "after command start" in payload["detail"]


def test_broken_telemetry_stderr_cannot_replace_command_exit(monkeypatch):
    class BrokenStderr:
        def write(self, _value):
            raise OSError("closed stderr")

        def flush(self):
            raise OSError("closed stderr")

    monkeypatch.setattr(
        telemetry,
        "_detailed_run",
        lambda _argv: (7, telemetry._unknown_payload("test")),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["_posix_telemetry.py", "--detailed", "--", "command"],
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
    monkeypatch.setattr(telemetry.sys, "platform", "linux")
    monkeypatch.setattr(
        telemetry.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout=row),
    )

    kind, status, devices = telemetry._gpu_reading()

    assert kind == "unknown"
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
            ("discrete", "measured", [valid]),
            ("unknown", "unavailable", []),
        ]
    )
    monkeypatch.setattr(telemetry, "_gpu_reading", lambda: next(readings))
    samples = telemetry.Samples()

    samples._sample_gpu()
    samples._sample_gpu()
    payload = samples.gpu_payload()

    assert payload["status"] == "partial"
    assert payload["sample_count"] == 2
    assert payload["failed_sample_count"] == 1
    assert payload["max_util_pct"] == 25.0
    assert payload["min_vram_free_bytes"] == 100


def test_unified_gpu_pressure_uses_system_available_memory(monkeypatch):
    samples = telemetry.Samples()
    samples.min_available_bytes = 12_345
    monkeypatch.setattr(
        telemetry,
        "_gpu_reading",
        lambda: (
            "unified",
            "unavailable",
            [
                {
                    "id": "unified",
                    "name": "Apple GPU",
                    "util_pct": None,
                    "vram_free_bytes": None,
                    "vram_total_bytes": None,
                }
            ],
        ),
    )

    samples._sample_gpu()
    payload = samples.gpu_payload()

    assert payload["kind"] == "unified"
    assert payload["status"] == "unavailable"
    assert payload["min_vram_free_bytes"] is None
    assert payload["unified_memory_min_available_bytes"] == 12_345


def test_darwin_host_cpu_reuses_single_host_port(monkeypatch):
    calls = {"mach_host_self": 0, "host_statistics": 0}

    class FakeFunction:
        def __init__(self, function):
            self.function = function
            self.restype = None
            self.argtypes = None

        def __call__(self, *args):
            return self.function(*args)

    class FakeLib:
        def __init__(self):
            self.mach_host_self = FakeFunction(self._mach_host_self)
            self.host_statistics = FakeFunction(self._host_statistics)

        @staticmethod
        def _mach_host_self():
            calls["mach_host_self"] += 1
            return 77

        @staticmethod
        def _host_statistics(_port, _flavor, _values, _count):
            calls["host_statistics"] += 1
            return 0

    fake = FakeLib()
    monkeypatch.setattr(telemetry, "_DARWIN_HOST_PORT", None)
    monkeypatch.setattr(telemetry, "_DARWIN_LIBSYSTEM", fake)

    telemetry._darwin_host_cpu()
    telemetry._darwin_host_cpu()

    assert calls == {"mach_host_self": 1, "host_statistics": 2}


class _FakeDarwinProcessList:
    def __init__(self, estimate: int, count: int) -> None:
        self.estimate = estimate
        self.count = count

    def proc_listallpids(self, buffer, _size):  # noqa: ANN001
        return self.estimate if buffer is None else self.count


@pytest.mark.parametrize(
    ("estimate", "count", "message"),
    [
        (0, 0, "size query failed"),
        (1, 0, "proc_listallpids failed"),
        (1, 65, "result was truncated"),
    ],
)
def test_darwin_process_list_failures_are_not_empty_success(
    monkeypatch, estimate, count, message
):
    monkeypatch.setattr(
        telemetry,
        "_DARWIN_LIBSYSTEM",
        _FakeDarwinProcessList(estimate, count),
    )

    with pytest.raises(RuntimeError, match=message):
        telemetry._darwin_processes()


@pytest.mark.skipif(sys.platform != "darwin", reason="native macOS libproc gate")
def test_darwin_libproc_process_table_reports_current_process():
    rows = telemetry._darwin_processes()
    current = rows[os.getpid()]

    assert current.ppid == os.getppid()
    assert current.pgid > 0
    assert current.rss_bytes > 0
    assert current.identity.startswith(f"{os.getpid()}:")
    assert current.identity != str(os.getpid())


def _guard_payload_from_stderr(stderr: str, token: str) -> dict:
    marker = f"__REMRUN_GUARD_RESULT_{token}__ "
    return json.loads(stderr.rsplit(marker, 1)[1].splitlines()[0])


MIB = 1024**2


def _guard_request(
    state_root: Path,
    *,
    max_command_mib: int = 128,
    min_available_mib: int = 16,
    predicted_rss_bytes: int | None = None,
    max_jobs: int = 1,
) -> dict[str, object]:
    total, _available = telemetry._host_memory()
    command_fraction = ((max_command_mib + 0.25) * MIB) / total
    reserve_fraction = ((min_available_mib - 0.25) * MIB) / total
    assert 0 < command_fraction < 1
    assert 0 < reserve_fraction < 1
    assert command_fraction + reserve_fraction < 1
    return {
        "schema": 2,
        "op": "reserve",
        "state_root": str(state_root),
        "lease_id": uuid.uuid4().hex,
        "lease_token": uuid.uuid4().hex,
        "predicted_rss_bytes": predicted_rss_bytes,
        "command_limit_fraction": command_fraction,
        "host_reserve_fraction": reserve_fraction,
        "max_jobs": max_jobs,
        "reservation_ttl_seconds": 120.0,
    }


def _lease_request(payload: dict[str, object], *, op: str = "renew") -> dict[str, object]:
    lease = payload["lease"]
    policy = payload["policy"]
    assert isinstance(lease, dict) and isinstance(policy, dict)
    return {
        "schema": 2,
        "op": op,
        "state_root": lease["state_root"],
        "lease_id": lease["lease_id"],
        "lease_token": lease["lease_token"],
        "allowance_bytes": lease["allowance_bytes"],
        "control_overhead_bytes": lease["control_overhead_bytes"],
        "capacity_bytes": lease["capacity_bytes"],
        "command_limit_fraction": policy["command_limit_fraction"],
        "host_reserve_fraction": policy["host_reserve_fraction"],
        "max_jobs": policy["max_jobs"],
        "reservation_ttl_seconds": policy["reservation_ttl_seconds"],
    }


def _reserve_guard(
    state_root: Path,
    *,
    max_command_mib: int = 128,
    min_available_mib: int = 16,
    predicted_rss_bytes: int | None = None,
    max_jobs: int = 1,
) -> dict[str, object]:
    payload = telemetry._handle_admission_request(
        _guard_request(
            state_root,
            max_command_mib=max_command_mib,
            min_available_mib=min_available_mib,
            predicted_rss_bytes=predicted_rss_bytes,
            max_jobs=max_jobs,
        )
    )
    assert payload["status"] == "admitted", payload
    return payload


def _renew_guard(payload: dict[str, object]) -> dict[str, object]:
    renewed = telemetry._handle_admission_request(_lease_request(payload))
    assert renewed["status"] == "admitted", renewed
    return renewed


def _ledger_leases(state_root: Path) -> list[dict[str, object]]:
    ledger = state_root / "memory-guard" / "v2" / "ledger.json"
    if not ledger.exists():
        return []
    return json.loads(ledger.read_text(encoding="utf-8"))["leases"]


def _guarded_helper_argv(
    payload: dict[str, object],
    *,
    token: str,
    command: list[str],
    telemetry_enabled: bool = False,
) -> list[str]:
    lease = payload["lease"]
    assert isinstance(lease, dict)
    encoded = base64.urlsafe_b64encode(
        json.dumps(_lease_request(payload), separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    argv = [
        sys.executable,
        str(HELPER),
        "--guard-max-bytes",
        str(lease["allowance_bytes"]),
        "--guard-min-available-bytes",
        str(lease["min_available_bytes"]),
        "--guard-token",
        token,
        "--guard-lease-b64",
        encoded,
    ]
    if telemetry_enabled:
        argv.append("--telemetry")
    return [*argv, "--", *command]


def test_guard_refuses_low_initial_host_memory_before_user_code(
    tmp_path, monkeypatch, capsys
):
    state_root = tmp_path / "state"
    reserved = _reserve_guard(state_root)
    lease = reserved["lease"]
    assert isinstance(lease, dict)
    marker = tmp_path / "must-not-run.txt"
    total = int(lease["host_total_bytes"])
    reserve = int(lease["min_available_bytes"])
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (total, reserve))
    token = uuid.uuid4().hex

    rc = telemetry._guarded_run(
        [sys.executable, "-c", f"open({str(marker)!r},'w').write('ran')"],
        max_command_bytes=int(lease["allowance_bytes"]),
        min_available_bytes=reserve,
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=_lease_request(reserved),
    )

    payload = _guard_payload_from_stderr(capsys.readouterr().err, token)
    assert rc == 125
    assert payload["status"] == "refused"
    assert payload["reason"] == "host_memory_reserve"
    assert payload["command_started"] is False
    assert not marker.exists()
    assert _ledger_leases(state_root) == []


def test_guard_refuses_when_initial_host_memory_read_is_unavailable(
    tmp_path, monkeypatch, capsys
):
    state_root = tmp_path / "state"
    reserved = _reserve_guard(state_root)
    lease = reserved["lease"]
    assert isinstance(lease, dict)
    marker = tmp_path / "must-not-run.txt"
    monkeypatch.setattr(
        telemetry,
        "_host_memory",
        lambda: (_ for _ in ()).throw(OSError("host counters unavailable")),
    )
    token = uuid.uuid4().hex

    rc = telemetry._guarded_run(
        [sys.executable, "-c", f"open({str(marker)!r},'w').write('ran')"],
        max_command_bytes=int(lease["allowance_bytes"]),
        min_available_bytes=int(lease["min_available_bytes"]),
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=_lease_request(reserved),
    )

    payload = _guard_payload_from_stderr(capsys.readouterr().err, token)
    assert rc == 125
    assert payload["status"] == "refused"
    assert payload["reason"] == "guard_initialization_failed"
    assert payload["command_started"] is False
    assert not marker.exists()
    assert _ledger_leases(state_root) == []


def test_guard_sampling_failure_after_launch_kills_instead_of_waiting(
    tmp_path, monkeypatch, capsys
):
    state_root = tmp_path / "state"
    reserved = _reserve_guard(state_root)
    lease = reserved["lease"]
    assert isinstance(lease, dict)
    child_pid_file = tmp_path / "child.pid"
    real_processes = telemetry._processes

    def processes():
        if child_pid_file.exists():
            raise RuntimeError("process table unavailable")
        return real_processes()

    monkeypatch.setattr(telemetry, "_processes", processes)
    token = uuid.uuid4().hex
    started = time.monotonic()
    rc = telemetry._guarded_run(
        [
            sys.executable,
            "-c",
            (
                "import os,time,pathlib;"
                f"pathlib.Path({str(child_pid_file)!r}).write_text(str(os.getpid()));"
                "time.sleep(10)"
            ),
        ],
        max_command_bytes=int(lease["allowance_bytes"]),
        min_available_bytes=int(lease["min_available_bytes"]),
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=_lease_request(reserved),
    )
    elapsed = time.monotonic() - started

    payload = _guard_payload_from_stderr(capsys.readouterr().err, token)
    assert rc == 125
    assert elapsed < 6.0
    assert payload["status"] == "failed_safe"
    assert payload["reason"] == "enforcement_sampling_failed"
    assert payload["cleanup_complete"] is True
    if child_pid_file.exists():
        pid = int(child_pid_file.read_text())
        for _ in range(50):
            if telemetry._identity_for_pid(pid) is None:
                break
            time.sleep(0.02)
        assert telemetry._identity_for_pid(pid) is None
    assert _ledger_leases(state_root) == []


def test_guard_reports_failed_safe_when_process_tree_drain_is_unverified(
    tmp_path, monkeypatch, capsys
):
    state_root = tmp_path / "state"
    reserved = _reserve_guard(
        state_root, max_command_mib=64, predicted_rss_bytes=MIB
    )
    lease = reserved["lease"]
    assert isinstance(lease, dict)
    monkeypatch.setattr(telemetry, "_terminate_guarded_tree", lambda *_args: False)
    token = uuid.uuid4().hex

    rc = telemetry._guarded_run(
        [sys.executable, "-c", "import time; x=bytearray(8*1024*1024); time.sleep(30)"],
        max_command_bytes=int(lease["allowance_bytes"]),
        min_available_bytes=int(lease["min_available_bytes"]),
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=_lease_request(reserved),
    )

    payload = _guard_payload_from_stderr(capsys.readouterr().err, token)
    assert rc == 125
    assert payload["status"] == "failed_safe"
    assert payload["reason"] == "termination_cleanup_failed"
    assert payload["trigger_reason"] == "command_memory_limit"
    assert payload["cleanup_complete"] is False
    leases = _ledger_leases(state_root)
    assert len(leases) == 1 and leases[0]["state"] == "quarantined"
    pgid = int(leases[0]["pgid"])
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        os.waitpid(pgid, 0)
    except (ChildProcessError, ProcessLookupError):
        pass
    for _ in range(100):
        if not telemetry._group_alive(pgid):
            break
        time.sleep(0.02)
    later = _reserve_guard(
        state_root, max_command_mib=64, predicted_rss_bytes=MIB
    )
    assert later["stale_reaped"] == 1


def test_guard_threshold_terminates_allocating_descendant_and_reports_result(tmp_path):
    state_root = tmp_path / "state"
    reserved = _renew_guard(_reserve_guard(state_root, max_command_mib=32))
    token = uuid.uuid4().hex
    pid_file = tmp_path / "grandchild.pid"
    child_code = (
        "import os,time,pathlib;"
        f"pathlib.Path({str(pid_file)!r}).write_text(str(os.getpid()));"
        "x=bytearray(48*1024*1024);"
        "x[::4096]=b'x'*((len(x)+4095)//4096);"
        "time.sleep(10)"
    )
    parent_code = (
        "import subprocess,sys,time;"
        f"subprocess.Popen([sys.executable,'-c',{child_code!r}]);"
        "time.sleep(10)"
    )
    result = subprocess.run(
        _guarded_helper_argv(
            reserved,
            token=token,
            command=[sys.executable, "-c", parent_code],
        ),
        text=True,
        capture_output=True,
        timeout=8,
        check=False,
    )

    payload = _guard_payload_from_stderr(result.stderr, token)
    assert result.returncode == 125
    assert payload["status"] == "terminated"
    assert payload["reason"] == "command_memory_limit"
    assert payload["peak_command_bytes"] >= 32 * MIB
    assert payload["cleanup_complete"] is True
    if pid_file.exists():
        pid = int(pid_file.read_text())
        for _ in range(50):
            if telemetry._identity_for_pid(pid) is None:
                break
            time.sleep(0.02)
        assert telemetry._identity_for_pid(pid) is None
    assert _ledger_leases(state_root) == []


def test_guard_cleans_fast_parent_same_group_child_before_return(tmp_path):
    state_root = tmp_path / "state"
    reserved = _renew_guard(_reserve_guard(state_root, max_command_mib=128))
    token = uuid.uuid4().hex
    pid_file = tmp_path / "orphaned-child.pid"
    child_code = (
        "import os,time,pathlib;"
        f"pathlib.Path({str(pid_file)!r}).write_text(str(os.getpid()));"
        "time.sleep(10)"
    )
    parent_code = (
        "import os,subprocess,sys;"
        "subprocess.Popen("
        f"[sys.executable,'-c',{child_code!r}],"
        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,"
        "stderr=subprocess.DEVNULL,close_fds=True);"
        "os._exit(0)"
    )

    result = subprocess.run(
        _guarded_helper_argv(
            reserved,
            token=token,
            command=[sys.executable, "-c", parent_code],
        ),
        text=True,
        capture_output=True,
        timeout=8,
        check=False,
    )

    payload = _guard_payload_from_stderr(result.stderr, token)
    assert result.returncode == 0
    assert payload["status"] == "ok"
    assert payload["command_exit_code"] == 0
    assert payload["forced_descendant_cleanup"] is True
    assert payload["cleanup_complete"] is True
    assert pid_file.exists()
    pid = int(pid_file.read_text())
    for _ in range(50):
        if telemetry._identity_for_pid(pid) is None:
            break
        time.sleep(0.02)
    assert telemetry._identity_for_pid(pid) is None
    assert _ledger_leases(state_root) == []


def test_guard_preserves_real_exit_and_emits_telemetry_when_not_triggered(tmp_path):
    state_root = tmp_path / "state"
    reserved = _renew_guard(_reserve_guard(state_root, max_command_mib=128))
    token = uuid.uuid4().hex
    result = subprocess.run(
        _guarded_helper_argv(
            reserved,
            token=token,
            telemetry_enabled=True,
            command=[sys.executable, "-c", "print('guarded'); raise SystemExit(7)"],
        ),
        text=True,
        capture_output=True,
        timeout=8,
        check=False,
    )

    payload = _guard_payload_from_stderr(result.stderr, token)
    assert result.returncode == 7
    assert result.stdout == "guarded\n"
    assert payload["status"] == "ok"
    assert payload["command_exit_code"] == 7
    assert payload["sample_count"] >= 1
    assert "__REMRUN_TELEMETRY__ " in result.stderr
    assert _ledger_leases(state_root) == []


def test_guard_preserves_shell_signal_exit_semantics(tmp_path):
    state_root = tmp_path / "state"
    reserved = _renew_guard(_reserve_guard(state_root, max_command_mib=128))
    token = uuid.uuid4().hex
    result = subprocess.run(
        _guarded_helper_argv(
            reserved,
            token=token,
            command=[
                sys.executable,
                "-c",
                "import os,signal; os.kill(os.getpid(), signal.SIGTERM)",
            ],
        ),
        text=True,
        capture_output=True,
        timeout=8,
        check=False,
    )

    payload = _guard_payload_from_stderr(result.stderr, token)
    assert result.returncode == 128 + signal.SIGTERM
    assert payload["status"] == "ok"
    assert payload["command_exit_code"] == 128 + signal.SIGTERM
    assert payload["helper_exit_code"] == 128 + signal.SIGTERM
    assert _ledger_leases(state_root) == []
