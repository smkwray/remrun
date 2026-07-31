#!/usr/bin/env python3
"""Bounded native activation gate for the dormant remrun job observer.

Run from a controller with the production private device configuration::

    python native-gates/fleet_jobs_native_gate.py \
        --repo /path/to/remrun --target WINDOWS_TARGET \
        --target GUARDED_POSIX_TARGET

The gate calls ``exec_observed()`` directly and never changes
``REMRUN_FLEET_JOBS_OBSERVE``. Windows proves the telemetry-on direct-root case
and then kills the controller-side source SSH request while the ordinary user
root and descendant remain alive. A guarded POSIX target uses remrun's normal
predicted-RSS reservation seam, queries the live job from a fresh connection,
and verifies exact result/guard cleanup. Every user allocation is at most 4 MiB
and every user process lifetime is at most 35 seconds.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

DEFAULT_PREDICTED_RSS_MB = 256.0
MAX_CHILD_SECONDS = 35.0
MARKER_WAIT_SECONDS = 30.0


class GateFailure(RuntimeError):
    """A native assertion failed."""


def _compact(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _load_remrun(repo: Path):  # noqa: ANN202
    source = repo / "src"
    if not source.is_dir():
        raise GateFailure(f"repo has no src directory: {source}")
    source_text = str(source)
    if source_text not in sys.path:
        sys.path.insert(0, source_text)
    from remrun.config import load_config
    from remrun.job_observation import JobObservation
    from remrun.transport import make_transport

    return load_config, JobObservation, make_transport


def _resolve_device(config, requested: str):  # noqa: ANN001, ANN202
    if requested in config.devices:
        return config.devices[requested]
    matches = [
        device
        for name, device in config.devices.items()
        if name.casefold() == requested.casefold()
    ]
    if len(matches) == 1:
        return matches[0]
    available = ", ".join(sorted(config.devices)) or "<none>"
    raise GateFailure(f"target {requested!r} is not configured; available: {available}")


def _fresh_transport(device, make_transport):  # noqa: ANN001, ANN202
    transport = make_transport(device)
    probe = transport.probe()
    if not probe.reachable:
        raise GateFailure(f"{device.name}: unreachable: {probe.detail}")
    return transport


def _matching_jobs(payload: dict[str, Any], job_id: str) -> list[dict[str, Any]]:
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise GateFailure(f"query has no jobs list: {_compact(payload)}")
    return [
        row
        for row in jobs
        if isinstance(row, dict) and row.get("job_id") == job_id
    ]


def _assert_live_metrics(
    *,
    device_name: str,
    label: str,
    payload: dict[str, Any],
    job_id: str,
) -> None:
    matches = _matching_jobs(payload, job_id)
    registry = payload.get("registry") if isinstance(payload.get("registry"), dict) else {}
    print(
        f"[{device_name}] {label} status={payload.get('status')} matches={len(matches)} "
        f"records_seen={registry.get('records_seen')} "
        f"stale_hidden={registry.get('stale_hidden')}",
        flush=True,
    )
    if len(matches) != 1:
        raise GateFailure(
            f"{device_name}: {label} did not return exactly one job: {_compact(payload)}"
        )
    if int(registry.get("owned_v2_records_seen") or 0) < 1:
        raise GateFailure(
            f"{device_name}: {label} did not prove a schema-2 ownership row: "
            f"{_compact(registry)}"
        )
    row = matches[0]
    if row.get("state") != "RUNNING":
        raise GateFailure(f"{device_name}: {label} expected RUNNING: {_compact(row)}")
    if int((row.get("processes") or {}).get("current_count") or 0) < 1:
        raise GateFailure(f"{device_name}: {label} has no owned process: {_compact(row)}")
    if (row.get("cpu") or {}).get("current_pct_one_logical_cpu") is None:
        raise GateFailure(f"{device_name}: {label} CPU is unknown: {_compact(row)}")
    if int((row.get("threads") or {}).get("current_os_threads") or 0) < 1:
        raise GateFailure(f"{device_name}: {label} OS threads are absent: {_compact(row)}")
    if int((row.get("memory") or {}).get("current_bytes") or 0) <= 0:
        raise GateFailure(f"{device_name}: {label} current memory is absent: {_compact(row)}")
    if registry.get("query_mutated_registry") is not False:
        raise GateFailure(f"{device_name}: {label} query did not attest read-only behavior")


def _wait_until_hidden(
    *,
    device_name: str,
    transport,
    job_id: str,
    sample_interval: float,
    cleanup_timeout: float,
) -> None:  # noqa: ANN001
    deadline = time.monotonic() + cleanup_timeout
    last_payload: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        time.sleep(0.75)
        last_payload = transport.query_observed_jobs(
            sample_interval=max(0.05, min(sample_interval, 0.1)),
            timeout=45.0,
        )
        if not _matching_jobs(last_payload, job_id):
            print(f"[{device_name}] cleanup {job_id} visible_rows=0", flush=True)
            return
    raise GateFailure(
        f"{device_name}: {job_id} remained visibly active after bounded exit: "
        f"{_compact(last_payload)}"
    )


def _read_marker(transport, marker: str) -> bytes | None:  # noqa: ANN001
    try:
        return transport.read_small_file(marker, 64)
    except Exception:
        return None


def _wait_for_marker(
    *,
    device_name: str,
    transport,
    marker: str,
    expected: bytes,
    worker: subprocess.Popen[str] | None = None,
    timeout: float = MARKER_WAIT_SECONDS,
) -> None:  # noqa: ANN001
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        marker_data = _read_marker(transport, marker)
        # Python text writes use CRLF on Windows. Marker meaning is textual, but
        # read_small_file deliberately returns exact bytes, so normalize only the
        # platform newline before comparing the bounded sentinel.
        if (
            marker_data is not None
            and marker_data.replace(bytes((13, 10)), bytes((10,))) == expected
        ):
            return
        if worker is not None and worker.poll() is not None:
            stdout, stderr = worker.communicate()
            raise GateFailure(
                f"{device_name}: source worker exited before marker {expected!r}; "
                f"exit={worker.returncode} stdout={stdout!r} stderr={stderr!r}"
            )
        time.sleep(0.2)
    raise GateFailure(f"{device_name}: timed out waiting for marker {expected!r}")


def _best_effort_delete(transport, *paths: str) -> None:  # noqa: ANN001
    for path in paths:
        try:
            transport.delete_remote(path)
        except Exception:
            pass


def _exec_observed_probe(
    transport,
    command: list[str],
    cwd: str,
    *,
    observation,
    timeout: float,
    predicted_rss_mb: float,
):  # noqa: ANN001, ANN202
    """Use the normal declared reservation seam, then run telemetry-on observed."""
    reservation = None
    admission_payload = None
    if getattr(transport, "memory_guard", None) is not None:
        admission = transport.reserve_memory_guard(predicted_rss_mb=predicted_rss_mb)
        admission_payload = admission.payload
        print(
            f"[{transport.device.name}] memory admission "
            f"status={admission.status} reason={admission.reason} "
            f"predicted_rss_mb={predicted_rss_mb:g}",
            flush=True,
        )
        if not admission.admitted or admission.reservation is None:
            raise GateFailure(
                f"{transport.device.name}: bounded observer reservation refused: "
                f"{_compact(admission.payload)}"
            )
        reservation = admission.reservation
    try:
        result = transport.exec_observed(
            command,
            cwd,
            observation=observation,
            timeout=timeout,
            telemetry=True,
            memory_reservation=reservation,
        )
    except BaseException:
        if reservation is not None:
            transport.release_memory_guard(reservation, reserved_only=True)
        raise
    return result, admission_payload


def _assert_exact_result(
    *,
    device_name: str,
    label: str,
    result,
    require_guard: bool,
    require_observed_breakaway_telemetry: bool = False,
) -> None:  # noqa: ANN001
    print(
        f"[{device_name}] {label} exit={result.exit_code} "
        f"stdout={result.stdout!r} stderr={result.stderr!r}",
        flush=True,
    )
    if result.exit_code != 37:
        raise GateFailure(f"{device_name}: {label} expected exit 37, got {result.exit_code}")
    if result.stdout.count("ROOT_ONCE") != 1:
        raise GateFailure(
            f"{device_name}: {label} ROOT_ONCE count was "
            f"{result.stdout.count('ROOT_ONCE')}"
        )
    if result.stderr.count("ROOT_ERR") != 1:
        raise GateFailure(
            f"{device_name}: {label} ROOT_ERR count was "
            f"{result.stderr.count('ROOT_ERR')}"
        )
    if "observation unavailable" in result.stderr:
        raise GateFailure(f"{device_name}: {label} observer fell back: {result.stderr!r}")
    if result.telemetry is None:
        raise GateFailure(f"{device_name}: {label} telemetry=True produced no telemetry")
    if require_observed_breakaway_telemetry:
        telemetry = result.telemetry
        if (
            telemetry.get("status") != "unavailable"
            or telemetry.get("coverage") != "observer_wrapper_only"
            or telemetry.get("peak_rss_mb") is not None
        ):
            raise GateFailure(
                f"{device_name}: {label} returned misleading outer-Job telemetry: "
                f"{_compact(telemetry)}"
            )
    if require_guard:
        guard = result.memory_guard
        if not isinstance(guard, dict):
            raise GateFailure(f"{device_name}: {label} has no memory-guard result")
        if guard.get("status") != "ok" or guard.get("command_started") is not True:
            raise GateFailure(
                f"{device_name}: {label} memory guard was not a completed admitted run: "
                f"{_compact(guard)}"
            )
        if guard.get("cleanup_complete") is not True:
            raise GateFailure(
                f"{device_name}: {label} memory-guard cleanup is unproved: {_compact(guard)}"
            )


def _run_direct_root_gate(
    *,
    device,
    make_transport,
    JobObservation,
    child_seconds: float,
    sample_interval: float,
    cleanup_timeout: float,
    predicted_rss_mb: float,
) -> None:  # noqa: ANN001, N803
    transport = _fresh_transport(device, make_transport)
    cwd = transport.expand_remote(device.state_root)
    transport.ensure_remote_dir(cwd)
    job_id = f"native-last-handle-{device.name}-{time.time_ns()}"
    child_code = (
        "import time;"
        "blob=bytearray(4*1024*1024);"
        f"time.sleep({child_seconds!r})"
    )
    parent_code = "\n".join(
        [
            "import subprocess,sys",
            "subprocess.Popen(",
            f"    [sys.executable, '-S', '-c', {child_code!r}],",
            "    stdin=subprocess.DEVNULL,",
            "    stdout=subprocess.DEVNULL,",
            "    stderr=subprocess.DEVNULL,",
            "    close_fds=True,",
            ")",
            "print('ROOT_ONCE', flush=True)",
            "print('ROOT_ERR', file=sys.stderr, flush=True)",
            "raise SystemExit(37)",
        ]
    )
    command = [device.remote_python, "-S", "-c", parent_code]
    observation = JobObservation.for_command(
        job_id=job_id,
        project="@native-gate",
        target=device.name,
        phase="last-handle-telemetry-on",
        command=command,
        declared_label="native-last-handle-telemetry-on",
        source_controller="native-gate",
    )

    started = time.monotonic()
    result, _admission = _exec_observed_probe(
        transport,
        command,
        cwd,
        observation=observation,
        timeout=max(45.0, child_seconds + 15.0),
        predicted_rss_mb=predicted_rss_mb,
    )
    elapsed = time.monotonic() - started
    print(f"[{device.name}] last-handle elapsed={elapsed:.3f}", flush=True)
    _assert_exact_result(
        device_name=device.name,
        label="last-handle telemetry-on",
        result=result,
        require_guard=False,
        require_observed_breakaway_telemetry=_is_windows_device(device),
    )

    query_transport = _fresh_transport(device, make_transport)
    payload = query_transport.query_observed_jobs(
        sample_interval=sample_interval,
        timeout=45.0,
    )
    _assert_live_metrics(
        device_name=device.name,
        label="last-handle-reopen",
        payload=payload,
        job_id=job_id,
    )
    _wait_until_hidden(
        device_name=device.name,
        transport=query_transport,
        job_id=job_id,
        sample_interval=sample_interval,
        cleanup_timeout=cleanup_timeout,
    )


def _encode_worker_payload(payload: dict[str, object]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_worker_payload(encoded: str) -> dict[str, object]:
    try:
        payload = json.loads(base64.urlsafe_b64decode(encoded.encode("ascii")))
    except Exception as exc:
        raise GateFailure(f"invalid source-worker payload: {type(exc).__name__}: {exc}") from exc
    if not isinstance(payload, dict):
        raise GateFailure("source-worker payload is not an object")
    return payload


def _source_worker_argv(payload: dict[str, object]) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--_worker-payload-b64",
        _encode_worker_payload(payload),
    ]


def _start_source_worker(payload: dict[str, object]) -> subprocess.Popen[str]:
    kwargs: dict[str, object] = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
    }
    if os.name == "posix":
        kwargs["start_new_session"] = True
    elif os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    return subprocess.Popen(_source_worker_argv(payload), **kwargs)


def _terminate_source_worker(worker: subprocess.Popen[str]) -> tuple[str, str]:
    """End the worker and its local ssh child, which ends the source SSH request."""
    if worker.poll() is None:
        if os.name == "posix":
            try:
                os.killpg(worker.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        elif os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(worker.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        else:
            worker.terminate()
        try:
            worker.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            if os.name == "posix":
                try:
                    os.killpg(worker.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            else:
                worker.kill()
    stdout, stderr = worker.communicate(timeout=5.0)
    return stdout, stderr


def _worker_payload_from_output(stdout: str, stderr: str) -> dict[str, object]:
    for text in (stdout, stderr):
        for line in reversed(text.splitlines()):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and payload.get("worker_schema") == 1:
                return payload
    raise GateFailure(f"source worker emitted no result payload: stdout={stdout!r} stderr={stderr!r}")


def _source_worker_main(payload: dict[str, object]) -> int:
    repo = Path(str(payload["repo"])).expanduser().resolve()
    target_name = str(payload["target"])
    load_config, JobObservation, make_transport = _load_remrun(repo)
    device = _resolve_device(load_config(repo), target_name)
    transport = _fresh_transport(device, make_transport)
    command_value = payload.get("command")
    if not isinstance(command_value, list) or not all(
        isinstance(value, str) for value in command_value
    ):
        raise GateFailure("source-worker command is invalid")
    command = list(command_value)
    observation = JobObservation.for_command(
        job_id=str(payload["job_id"]),
        project="@native-gate",
        target=device.name,
        phase=str(payload["phase"]),
        command=command,
        declared_label=str(payload["declared_label"]),
        source_controller="native-gate",
    )
    result, admission = _exec_observed_probe(
        transport,
        command,
        str(payload["cwd"]),
        observation=observation,
        timeout=float(payload["timeout"]),
        predicted_rss_mb=float(payload["predicted_rss_mb"]),
    )
    print(
        _compact(
            {
                "worker_schema": 1,
                "status": "completed",
                "exit_code": result.exit_code,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "telemetry": result.telemetry,
                "memory_guard": result.memory_guard,
                "admission": admission,
            }
        ),
        flush=True,
    )
    return 0


def _run_windows_source_request_loss_gate(
    *,
    repo: Path,
    device,
    make_transport,
    root_seconds: float,
    child_seconds: float,
    sample_interval: float,
    cleanup_timeout: float,
    predicted_rss_mb: float,
) -> None:  # noqa: ANN001
    query_transport = _fresh_transport(device, make_transport)
    cwd = query_transport.expand_remote(device.state_root)
    marker_dir = query_transport.remote_join(cwd, "jobs/native-gates")
    query_transport.ensure_remote_dir(marker_dir)
    job_id = f"native-source-loss-{device.name}-{time.time_ns()}"
    armed = query_transport.remote_join(marker_dir, f"{job_id}.armed")
    survived = query_transport.remote_join(marker_dir, f"{job_id}.survived")
    post_loss_delay = min(5.0, max(2.0, root_seconds / 4.0))
    remaining = max(1.0, root_seconds - post_loss_delay)
    child_code = (
        "import time;"
        "blob=bytearray(4*1024*1024);"
        f"time.sleep({child_seconds!r})"
    )
    parent_code = "\n".join(
        [
            "import subprocess,sys,time",
            "from pathlib import Path",
            "subprocess.Popen(",
            f"    [sys.executable, '-S', '-c', {child_code!r}],",
            "    stdin=subprocess.DEVNULL,",
            "    stdout=subprocess.DEVNULL,",
            "    stderr=subprocess.DEVNULL,",
            "    close_fds=True,",
            ")",
            f"Path({armed!r}).write_text('armed\\n', encoding='ascii')",
            "print('LOSS_ARMED', flush=True)",
            "print('LOSS_ERR', file=sys.stderr, flush=True)",
            f"time.sleep({post_loss_delay!r})",
            f"Path({survived!r}).write_text('survived\\n', encoding='ascii')",
            f"time.sleep({remaining!r})",
        ]
    )
    command = [device.remote_python, "-S", "-c", parent_code]
    payload = {
        "repo": str(repo),
        "target": device.name,
        "job_id": job_id,
        "phase": "source-request-loss-telemetry-on",
        "declared_label": "native-source-request-loss",
        "cwd": cwd,
        "command": command,
        "timeout": max(45.0, child_seconds + 15.0),
        "predicted_rss_mb": predicted_rss_mb,
    }
    worker = _start_source_worker(payload)
    try:
        _wait_for_marker(
            device_name=device.name,
            transport=query_transport,
            marker=armed,
            expected=b"armed\n",
            worker=worker,
        )
        stdout, stderr = _terminate_source_worker(worker)
        print(
            f"[{device.name}] source SSH request terminated "
            f"worker_exit={worker.returncode} stdout={stdout!r} stderr={stderr!r}",
            flush=True,
        )
        if worker.returncode in (None, 0):
            raise GateFailure(
                f"{device.name}: source worker did not terminate as an interrupted request"
            )
        _wait_for_marker(
            device_name=device.name,
            transport=query_transport,
            marker=survived,
            expected=b"survived\n",
            timeout=max(10.0, post_loss_delay + 5.0),
        )
        payload_result = query_transport.query_observed_jobs(
            sample_interval=sample_interval,
            timeout=45.0,
        )
        _assert_live_metrics(
            device_name=device.name,
            label="source-request-loss-reopen",
            payload=payload_result,
            job_id=job_id,
        )
        _wait_until_hidden(
            device_name=device.name,
            transport=query_transport,
            job_id=job_id,
            sample_interval=sample_interval,
            cleanup_timeout=cleanup_timeout,
        )
    finally:
        if worker.poll() is None:
            _terminate_source_worker(worker)
        _best_effort_delete(query_transport, armed, survived)


def _run_guarded_posix_live_gate(
    *,
    repo: Path,
    device,
    make_transport,
    root_seconds: float,
    child_seconds: float,
    sample_interval: float,
    cleanup_timeout: float,
    predicted_rss_mb: float,
) -> None:  # noqa: ANN001
    query_transport = _fresh_transport(device, make_transport)
    cwd = query_transport.expand_remote(device.state_root)
    marker_dir = query_transport.remote_join(cwd, "jobs/native-gates")
    query_transport.ensure_remote_dir(marker_dir)
    job_id = f"native-guarded-live-{device.name}-{time.time_ns()}"
    armed = query_transport.remote_join(marker_dir, f"{job_id}.armed")
    child_code = (
        "import time;"
        "blob=bytearray(4*1024*1024);"
        f"time.sleep({child_seconds!r})"
    )
    parent_code = "\n".join(
        [
            "import subprocess,sys,time",
            "from pathlib import Path",
            "subprocess.Popen(",
            f"    [sys.executable, '-S', '-c', {child_code!r}],",
            "    stdin=subprocess.DEVNULL,",
            "    stdout=subprocess.DEVNULL,",
            "    stderr=subprocess.DEVNULL,",
            "    close_fds=True,",
            ")",
            f"Path({armed!r}).write_text('armed\\n', encoding='ascii')",
            "print('ROOT_ONCE', flush=True)",
            "print('ROOT_ERR', file=sys.stderr, flush=True)",
            f"time.sleep({root_seconds!r})",
            "raise SystemExit(37)",
        ]
    )
    command = [device.remote_python, "-S", "-c", parent_code]
    payload = {
        "repo": str(repo),
        "target": device.name,
        "job_id": job_id,
        "phase": "guarded-live-telemetry-on",
        "declared_label": "native-guarded-live",
        "cwd": cwd,
        "command": command,
        "timeout": max(45.0, child_seconds + 15.0),
        "predicted_rss_mb": predicted_rss_mb,
    }
    worker = _start_source_worker(payload)
    try:
        _wait_for_marker(
            device_name=device.name,
            transport=query_transport,
            marker=armed,
            expected=b"armed\n",
            worker=worker,
        )
        payload_result = query_transport.query_observed_jobs(
            sample_interval=sample_interval,
            timeout=45.0,
        )
        _assert_live_metrics(
            device_name=device.name,
            label="guarded-live-reopen",
            payload=payload_result,
            job_id=job_id,
        )
        stdout, stderr = worker.communicate(timeout=max(50.0, child_seconds + 20.0))
        worker_result = _worker_payload_from_output(stdout, stderr)
        if worker.returncode != 0 or worker_result.get("status") != "completed":
            raise GateFailure(
                f"{device.name}: guarded source worker failed: "
                f"exit={worker.returncode} payload={_compact(worker_result)}"
            )
        result = SimpleNamespace(**worker_result)
        _assert_exact_result(
            device_name=device.name,
            label="guarded-live telemetry-on",
            result=result,
            require_guard=True,
        )
        _wait_until_hidden(
            device_name=device.name,
            transport=query_transport,
            job_id=job_id,
            sample_interval=sample_interval,
            cleanup_timeout=cleanup_timeout,
        )
    finally:
        if worker.poll() is None:
            _terminate_source_worker(worker)
        _best_effort_delete(query_transport, armed)


def _is_windows_device(device) -> bool:  # noqa: ANN001
    return str(getattr(device, "os", "")).casefold() == "windows" or str(
        getattr(device, "kind", "")
    ).casefold() == "ssh-powershell"


def _run_one(
    *,
    repo: Path,
    target_name: str,
    child_seconds: float,
    sample_interval: float,
    cleanup_timeout: float,
    predicted_rss_mb: float,
) -> None:
    load_config, JobObservation, make_transport = _load_remrun(repo)
    device = _resolve_device(load_config(repo), target_name)
    configured_transport = make_transport(device)
    if _is_windows_device(device):
        _run_direct_root_gate(
            device=device,
            make_transport=make_transport,
            JobObservation=JobObservation,
            child_seconds=child_seconds,
            sample_interval=sample_interval,
            cleanup_timeout=cleanup_timeout,
            predicted_rss_mb=predicted_rss_mb,
        )
        loss_child_seconds = max(20.0, min(30.0, child_seconds - 5.0))
        loss_root_seconds = max(16.0, min(24.0, loss_child_seconds - 6.0))
        _run_windows_source_request_loss_gate(
            repo=repo,
            device=device,
            make_transport=make_transport,
            root_seconds=loss_root_seconds,
            child_seconds=loss_child_seconds,
            sample_interval=sample_interval,
            cleanup_timeout=cleanup_timeout,
            predicted_rss_mb=predicted_rss_mb,
        )
    elif getattr(configured_transport, "memory_guard", None) is not None:
        guarded_child_seconds = max(24.0, min(30.0, child_seconds - 5.0))
        guarded_root_seconds = max(18.0, min(24.0, guarded_child_seconds - 6.0))
        _run_guarded_posix_live_gate(
            repo=repo,
            device=device,
            make_transport=make_transport,
            root_seconds=guarded_root_seconds,
            child_seconds=guarded_child_seconds,
            sample_interval=sample_interval,
            cleanup_timeout=cleanup_timeout,
            predicted_rss_mb=predicted_rss_mb,
        )
    else:
        _run_direct_root_gate(
            device=device,
            make_transport=make_transport,
            JobObservation=JobObservation,
            child_seconds=child_seconds,
            sample_interval=sample_interval,
            cleanup_timeout=cleanup_timeout,
            predicted_rss_mb=predicted_rss_mb,
        )
    print(f"[{device.name}] PASS", flush=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd(), help="remrun repository root")
    parser.add_argument(
        "--target",
        action="append",
        help="configured target name; repeat for each target to verify",
    )
    parser.add_argument("--child-seconds", type=float, default=35.0)
    parser.add_argument("--sample-interval", type=float, default=0.25)
    parser.add_argument("--cleanup-timeout", type=float, default=55.0)
    parser.add_argument(
        "--predicted-rss-mb",
        type=float,
        default=DEFAULT_PREDICTED_RSS_MB,
        help="declared bounded native-probe RSS prediction for schema-2 admission",
    )
    parser.add_argument("--_worker-payload-b64", help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args._worker_payload_b64:
        try:
            return _source_worker_main(_decode_worker_payload(args._worker_payload_b64))
        except Exception as exc:
            print(
                _compact(
                    {
                        "worker_schema": 1,
                        "status": "error",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                ),
                file=sys.stderr,
                flush=True,
            )
            return 2

    if not args.target:
        raise SystemExit("at least one --target is required")
    repo = args.repo.expanduser().resolve()
    if args.child_seconds < 20.0 or args.child_seconds > MAX_CHILD_SECONDS:
        raise SystemExit(f"--child-seconds must be in 20..{MAX_CHILD_SECONDS:g}")
    if args.sample_interval < 0.05 or args.sample_interval > 2.0:
        raise SystemExit("--sample-interval must be in 0.05..2.0")
    if args.cleanup_timeout < 20.0 or args.cleanup_timeout > 150.0:
        raise SystemExit("--cleanup-timeout must be in 20..150")
    if args.predicted_rss_mb < 64.0 or args.predicted_rss_mb > 1024.0:
        raise SystemExit("--predicted-rss-mb must be in 64..1024")

    failures: list[str] = []
    for target in args.target:
        try:
            _run_one(
                repo=repo,
                target_name=target,
                child_seconds=args.child_seconds,
                sample_interval=args.sample_interval,
                cleanup_timeout=args.cleanup_timeout,
                predicted_rss_mb=args.predicted_rss_mb,
            )
        except Exception as exc:  # report every requested target in one bounded run
            detail = f"[{target}] FAIL {type(exc).__name__}: {exc}"
            print(detail, file=sys.stderr, flush=True)
            failures.append(detail)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
