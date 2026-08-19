from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

pytest.importorskip("fcntl")

from remrun import _posix_telemetry as telemetry
from remrun.models import Device
from remrun.transport import LocalSimTransport

MIB = 1024**2


def _request(state_root: Path, *, lease_id: str | None = None, token: str | None = None):
    return {
        "schema": 2,
        "op": "reserve",
        "state_root": str(state_root),
        "lease_id": lease_id or uuid.uuid4().hex,
        "lease_token": token or uuid.uuid4().hex,
        "predicted_rss_bytes": None,
        "command_limit_fraction": 0.10,
        "host_reserve_fraction": 0.05,
        "max_jobs": 1,
        "reservation_ttl_seconds": 120.0,
    }


def _lease_request(result: dict[str, object], *, op: str) -> dict[str, object]:
    lease = result["lease"]
    policy = result["policy"]
    assert isinstance(lease, dict) and isinstance(policy, dict)
    request: dict[str, object] = {
        "schema": 2,
        "op": op,
        "state_root": lease["state_root"],
        "lease_id": lease["lease_id"],
        "lease_token": lease["lease_token"],
        "allowance_bytes": lease["allowance_bytes"],
        "command_limit_fraction": policy["command_limit_fraction"],
        "host_reserve_fraction": policy["host_reserve_fraction"],
        "max_jobs": policy["max_jobs"],
        "reservation_ttl_seconds": policy["reservation_ttl_seconds"],
    }
    for name in ("control_overhead_bytes", "capacity_bytes"):
        if name in lease:
            request[name] = lease[name]
    return request


def _ledger_leases(state_root: Path) -> list[dict[str, object]]:
    path = state_root / "memory-guard" / "v2" / "ledger.json"
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))["leases"]


def test_claimed_lease_with_live_survivor_is_quarantined_until_verified_dead(
    tmp_path: Path,
):
    state_root = tmp_path / "state"
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        start_new_session=True,
    )
    try:
        identity = None
        for _ in range(100):
            identity = telemetry._identity_for_pid(child.pid)
            if identity is not None:
                break
            time.sleep(0.01)
        assert identity is not None

        reserved = telemetry._handle_admission_request(_request(state_root))
        assert reserved["status"] == "admitted"
        lease_request = _lease_request(reserved, op="renew")
        claimed = telemetry._claim_memory_lease(
            lease_request,
            helper_pid=os.getpid(),
            root_pid=child.pid,
            root_identity=identity,
            pgid=child.pid,
        )
        assert claimed["status"] == "admitted"

        released = telemetry._release_memory_lease(
            {
                **lease_request,
                "op": "release",
                "reserved_only": False,
                "cleanup_complete": False,
            }
        )
        assert released["lease_released"] is False
        assert len(_ledger_leases(state_root)) == 1

        assert telemetry._quarantine_memory_lease(
            lease_request,
            survivors=[{"pid": child.pid, "identity": identity}],
            pgid=child.pid,
        )
        assert _ledger_leases(state_root)[0]["state"] == "quarantined"

        blocked = telemetry._handle_admission_request(_request(state_root))
        assert blocked["status"] == "refused"
        assert blocked["reason"] == "guarded_job_limit"

        os.killpg(child.pid, signal.SIGKILL)
        child.wait(timeout=5)
        for _ in range(100):
            if not telemetry._group_alive(child.pid):
                break
            time.sleep(0.01)

        later = telemetry._handle_admission_request(_request(state_root))
        assert later["status"] == "admitted"
        assert later["stale_reaped"] == 1
    finally:
        if child.poll() is None:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=5)


def test_status_pipe_eof_is_not_launch_confirmation():
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    with pytest.raises(telemetry._LaunchCompletionUnknown, match="without launch proof"):
        telemetry._confirm_gated_exec(read_fd)


def test_small_user_allowance_does_not_include_python_gate_overhead(tmp_path: Path):
    total, _available = telemetry._host_memory()
    tiny_fraction = max(2 * MIB / total, 0.000001)
    device = Device(
        name="LOCAL",
        enabled=True,
        role="runner",
        kind="local-sim",
        os="posix",
        address_candidates=[],
        project_root=str(tmp_path / "remote"),
        state_root=str(tmp_path / "state"),
        cache_root=str(tmp_path / "cache"),
        max_jobs=1,
        memory_guard={
            "schema": 3,
            "command_limit_fraction": tiny_fraction,
            "host_reserve_fraction": tiny_fraction,
        },
    )
    transport = LocalSimTransport(device)
    admission = transport.reserve_memory_guard(predicted_rss_mb=1)
    assert admission.admitted
    reservation = admission.reservation
    assert reservation is not None
    assert reservation.allowance_bytes == 2 * MIB
    assert reservation.control_overhead_bytes > reservation.allowance_bytes

    result = transport.exec(
        ["/usr/bin/true"],
        cwd=str(tmp_path / "remote" / "project"),
        telemetry=False,
        memory_reservation=reservation,
    )

    assert result.exit_code == 0
    assert result.memory_guard is not None
    assert result.memory_guard["status"] == "ok"
    assert result.memory_guard["peak_command_bytes"] < reservation.allowance_bytes
    assert _ledger_leases(tmp_path / "state") == []


def test_blank_local_state_root_is_refused_without_writing_in_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.chdir(tmp_path)
    device = Device(
        name="LOCAL",
        enabled=True,
        role="runner",
        kind="local-sim",
        os="posix",
        address_candidates=[],
        project_root=str(tmp_path / "remote"),
        state_root="",
        cache_root=str(tmp_path / "cache"),
        max_jobs=1,
        memory_guard={
            "schema": 3,
            "command_limit_fraction": 0.10,
            "host_reserve_fraction": 0.05,
        },
    )
    transport = LocalSimTransport(device)
    result = transport.reserve_memory_guard(predicted_rss_mb=1)
    try:
        assert not result.admitted
        assert result.reason == "admission_unavailable"
        assert "state root is empty" in result.detail
        assert not (tmp_path / "memory-guard" / "v2").exists()
    finally:
        if result.reservation is not None:
            transport.release_memory_guard(result.reservation)


def test_agent_output_contract_separates_admission_from_runtime_guard():
    text = (Path(__file__).parents[1] / "docs" / "AGENT_OUTPUT_SPEC.md").read_text(
        encoding="utf-8"
    )
    assert 'phase="memory_admission"' in text
    assert "structured memory_admission record" in text
    assert "Runtime enforcement has a structured" in text
