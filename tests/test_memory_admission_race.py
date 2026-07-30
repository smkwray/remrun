from __future__ import annotations

import multiprocessing
import uuid
from pathlib import Path

import pytest

pytest.importorskip("fcntl")
pytest.importorskip("resource")

GIB = 1024**3


def _reserve_worker(
    state_root: str,
    command_fraction: float,
    barrier,
    queue,
) -> None:
    from remrun import _posix_telemetry as telemetry

    telemetry._host_memory = lambda: (64 * GIB, 64 * GIB)
    telemetry._processes = lambda: {}
    telemetry._control_overhead_budget_bytes = lambda: 64 * 1024**2
    request = {
        "schema": 1,
        "op": "reserve",
        "state_root": state_root,
        "lease_id": uuid.uuid4().hex,
        "lease_token": uuid.uuid4().hex,
        "predicted_rss_bytes": None,
        "command_limit_fraction": command_fraction,
        "host_reserve_fraction": 0.20,
        "max_jobs": 2,
        "reservation_ttl_seconds": 120.0,
    }
    barrier.wait(timeout=10)
    result = telemetry._handle_admission_request(request)
    queue.put((result["status"], result["reason"]))


def _race(state_root: Path, command_fraction: float) -> list[tuple[str, str]]:
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    queue = context.Queue()
    processes = [
        context.Process(
            target=_reserve_worker,
            args=(str(state_root), command_fraction, barrier, queue),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    results = [queue.get(timeout=15) for _ in processes]
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0
    return results


def test_cross_controller_ledger_atomically_refuses_unsafe_double_admission(
    tmp_path: Path,
):
    results = _race(tmp_path / "unsafe", 0.40)

    assert sorted(status for status, _reason in results) == ["admitted", "refused"]
    assert next(reason for status, reason in results if status == "refused") == (
        "insufficient_live_memory"
    )


def test_cross_controller_ledger_keeps_safe_jobs_concurrent(tmp_path: Path):
    results = _race(tmp_path / "safe", 0.15)

    assert [status for status, _reason in results].count("admitted") == 2
