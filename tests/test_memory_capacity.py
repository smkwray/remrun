from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("fcntl")

from remrun import _posix_telemetry as telemetry

MIB = 1024**2
GIB = 1024**3
TOTAL = 64 * GIB


def _running_lease(lease_id: str, *, capacity: int) -> dict[str, object]:
    return {
        "lease_id": lease_id,
        "token_hash": "0" * 64,
        "allowance_bytes": capacity,
        "control_overhead_bytes": MIB,
        "capacity_bytes": capacity,
        "state": "running",
        "helper_pid": 9000,
        "helper_identity": "9000:h",
        "root_pid": 100,
        "root_identity": "100:r",
        "pgid": 100,
    }


def _private(lease_id: str, value: int, *, rss: int | None = None):
    return {lease_id: {100: ("100:r", value, value if rss is None else rss)}}


def test_capacity_transaction_rejects_growth_between_host_and_private_reads(monkeypatch):
    lease_id = "a" * 32
    leases = [_running_lease(lease_id, capacity=32 * GIB)]
    hosts = iter(
        [
            (TOTAL, 40 * GIB),
            (TOTAL, 24 * GIB),
            (TOTAL, 24 * GIB),
        ]
    )
    private = iter([_private(lease_id, 0), _private(lease_id, 16 * GIB)])
    monkeypatch.setattr(telemetry, "_host_memory", lambda: next(hosts))
    monkeypatch.setattr(telemetry, "_lease_private_snapshot", lambda _leases: next(private))

    _total, result = telemetry._capacity_transaction(leases, reserve_bytes=16 * GIB)

    assert result["available_samples_bytes"] == [40 * GIB, 24 * GIB, 24 * GIB]
    assert result["private_bytes_by_lease"][lease_id] == 0
    assert result["required_available_bytes"] == 48 * GIB
    assert result["safe"] is False


def test_capacity_transaction_rejects_shrink_between_private_and_host_reads(monkeypatch):
    lease_id = "b" * 32
    leases = [_running_lease(lease_id, capacity=32 * GIB)]
    hosts = iter(
        [
            (TOTAL, 24 * GIB),
            (TOTAL, 40 * GIB),
            (TOTAL, 40 * GIB),
        ]
    )
    private = iter([_private(lease_id, 16 * GIB), _private(lease_id, 0)])
    monkeypatch.setattr(telemetry, "_host_memory", lambda: next(hosts))
    monkeypatch.setattr(telemetry, "_lease_private_snapshot", lambda _leases: next(private))

    _total, result = telemetry._capacity_transaction(leases, reserve_bytes=16 * GIB)

    assert result["available_floor_bytes"] == 24 * GIB
    assert result["private_bytes_by_lease"][lease_id] == 0
    assert result["required_available_bytes"] == 48 * GIB
    assert result["safe"] is False


def test_capacity_transaction_credits_stable_private_pages_once_and_keeps_safe_jobs_concurrent(
    monkeypatch,
):
    first_id = "c" * 32
    second_id = "d" * 32
    leases = [
        _running_lease(first_id, capacity=16 * GIB),
        {**_running_lease(second_id, capacity=16 * GIB), "root_pid": 101, "root_identity": "101:r", "pgid": 101},
    ]
    snapshot = {
        first_id: {100: ("100:r", 8 * GIB, 12 * GIB)},
        second_id: {101: ("101:r", 8 * GIB, 12 * GIB)},
    }
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 33 * GIB))
    monkeypatch.setattr(telemetry, "_lease_private_snapshot", lambda _leases: snapshot)

    _total, result = telemetry._capacity_transaction(leases, reserve_bytes=16 * GIB)

    assert result["current_guarded_private_bytes"] == 16 * GIB
    assert result["future_headroom_bytes"] == 16 * GIB
    assert result["required_available_bytes"] == 32 * GIB
    assert result["safe"] is True
    assert result["attribution"] == "private_resident_additive_two_snapshot_minimum"


def test_capacity_transaction_never_credits_one_process_identity_to_two_leases(
    monkeypatch,
):
    first_id = "e" * 32
    second_id = "f" * 32
    leases = [
        _running_lease(first_id, capacity=16 * GIB),
        {**_running_lease(second_id, capacity=16 * GIB), "root_pid": 100, "root_identity": "100:r", "pgid": 100},
    ]
    overlapping = {
        first_id: {100: ("100:r", 8 * GIB, 12 * GIB)},
        second_id: {100: ("100:r", 8 * GIB, 12 * GIB)},
    }
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 36 * GIB))
    monkeypatch.setattr(telemetry, "_lease_private_snapshot", lambda _leases: overlapping)

    _total, result = telemetry._capacity_transaction(leases, reserve_bytes=16 * GIB)

    assert result["current_guarded_private_bytes"] == 8 * GIB
    assert sorted(result["private_bytes_by_lease"].values()) == [0, 8 * GIB]
    assert result["future_headroom_bytes"] == 24 * GIB
    assert result["required_available_bytes"] == 40 * GIB
    assert result["safe"] is False


_WORKER = r"""
import mmap, os, sys
path, role = sys.argv[1], int(sys.argv[2])
current = None
print('READY', flush=True)
for line in sys.stdin:
    command = line.strip()
    if command == 'shared':
        current = mmap.mmap(os.open(path, os.O_RDWR), 0, access=mmap.ACCESS_WRITE)
        for offset in range(0, len(current), mmap.PAGESIZE):
            current[offset] = role + 1
        print('DONE', flush=True)
    elif command == 'close':
        if current is not None:
            current.close(); current = None
        print('DONE', flush=True)
    elif command == 'cow':
        current = mmap.mmap(os.open(path, os.O_RDONLY), 0, access=mmap.ACCESS_COPY)
        for offset in range(0, len(current), mmap.PAGESIZE):
            _ = current[offset]
        half = len(current) // 2
        start, stop = (0, half) if role == 0 else (half, len(current))
        for offset in range(start, stop, mmap.PAGESIZE):
            current[offset] = role + 3
        print('DONE', flush=True)
    elif command == 'exit':
        break
"""


def _command(proc: subprocess.Popen[str], command: str) -> None:
    assert proc.stdin is not None and proc.stdout is not None
    proc.stdin.write(command + "\n")
    proc.stdin.flush()
    assert proc.stdout.readline().strip() == "DONE"


@pytest.mark.skipif(
    not (sys.platform.startswith("linux") or sys.platform == "darwin"),
    reason="private-resident attribution is supported only on Linux/macOS",
)
def test_private_resident_metric_excludes_shared_pages_and_counts_cow_copies_once(
    tmp_path: Path,
):
    size = 8 * MIB
    backing = tmp_path / "pages.bin"
    backing.write_bytes(b"\0" * size)
    workers = [
        subprocess.Popen(
            [sys.executable, "-c", _WORKER, str(backing), str(role)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        for role in (0, 1)
    ]
    try:
        for proc in workers:
            assert proc.stdout is not None
            assert proc.stdout.readline().strip() == "READY"
        baseline = sum(telemetry._private_resident_bytes(proc.pid) for proc in workers)

        for proc in workers:
            _command(proc, "shared")
        shared = sum(telemetry._private_resident_bytes(proc.pid) for proc in workers)
        assert shared - baseline < 3 * MIB

        for proc in workers:
            _command(proc, "close")
        before_cow = sum(telemetry._private_resident_bytes(proc.pid) for proc in workers)
        for proc in workers:
            _command(proc, "cow")
        cow = sum(telemetry._private_resident_bytes(proc.pid) for proc in workers)
        private_growth = cow - before_cow
        if sys.platform == "darwin":
            # Each worker dirties a disjoint 4 MiB half. Darwin's
            # pri_private_pages_resident reports those physical COW copies,
            # without also classifying the untouched file-backed halves as
            # private resident.
            assert 7 * MIB <= private_growth <= 10 * MIB
        else:
            # Linux smaps_rollup also classifies the resident MAP_PRIVATE clean
            # halves in Private_Clean for this fixture.
            assert 14 * MIB <= private_growth <= 19 * MIB
    finally:
        for proc in workers:
            if proc.poll() is None and proc.stdin is not None:
                try:
                    proc.stdin.write("exit\n")
                    proc.stdin.flush()
                except BrokenPipeError:
                    pass
        for proc in workers:
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
