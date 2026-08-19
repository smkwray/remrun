from __future__ import annotations

import json
import os
import signal
import sys
import time
import uuid
from pathlib import Path

import pytest

pytest.importorskip("fcntl")
pytest.importorskip("resource")

from remrun import _posix_telemetry as telemetry
from remrun.memory_guard import MemoryReservation
from remrun.transport import TransportError, _finalize_guarded_result

MIB = 1024**2
GIB = 1024**3
TOTAL = 64 * GIB
AVAILABLE_SAFE = 60 * GIB
AVAILABLE_UNSAFE = 20 * GIB
CONTROL = 512 * MIB


def _request(
    state_root: Path,
    *,
    op: str = "reserve",
    lease_id: str | None = None,
    lease_token: str | None = None,
    predicted_rss_bytes: int | None = None,
    explicit_limit_bytes: int | None = None,
    max_jobs: int = 2,
    command_fraction: float = 0.25,
    reserve_fraction: float | None = 0.25,
    ttl: float = 120.0,
) -> dict[str, object]:
    return {
        "schema": 2,
        "op": op,
        "state_root": str(state_root),
        "lease_id": lease_id or uuid.uuid4().hex,
        "lease_token": lease_token or uuid.uuid4().hex,
        "predicted_rss_bytes": predicted_rss_bytes,
        "explicit_limit_bytes": explicit_limit_bytes,
        "command_limit_fraction": command_fraction,
        "host_reserve_fraction": reserve_fraction,
        "max_jobs": max_jobs,
        "reservation_ttl_seconds": ttl,
    }


def _lease_request(reserved: dict[str, object], *, op: str = "renew") -> dict[str, object]:
    lease = reserved["lease"]
    policy = reserved["policy"]
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


def _ledger_leases(state_root: Path) -> list[dict[str, object]]:
    path = state_root / "memory-guard" / "v2" / "ledger.json"
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))["leases"]


def _guard_result(stderr: str, token: str) -> dict[str, object]:
    marker = f"\n__REMRUN_GUARD_RESULT_{token}__ "
    return json.loads(stderr.rsplit(marker, 1)[1].splitlines()[0])


def test_memory_reservation_summary_names_policy_ceiling_and_omits_token():
    reservation = MemoryReservation(
        lease_id="lease",
        lease_token="private-token",
        state_root="/tmp/remrun-test",
        allowance_bytes=8 * GIB,
        control_overhead_bytes=CONTROL,
        capacity_bytes=8 * GIB + CONTROL,
        max_command_bytes=12 * GIB,
        min_available_bytes=4 * GIB,
        host_total_bytes=32 * GIB,
        safe_concurrency=2,
        expires_at=4_102_444_800.0,
    )

    summary = reservation.as_dict()

    assert summary["policy_command_ceiling_bytes"] == 12 * GIB
    assert "lease_token" not in summary


@pytest.mark.parametrize(
    ("total_gib", "reserve_gib"),
    [(16, 4), (24, 6), (32, 8), (64, 16), (128, 16)],
)
def test_automatic_host_reserve_is_proportional_and_bounded(
    total_gib: int, reserve_gib: int
):
    policy = telemetry._policy_from_request(
        {
            "command_limit_fraction": 0.25,
            "max_jobs": 2,
            "reservation_ttl_seconds": 120.0,
        },
        total_gib * GIB,
    )

    assert policy["host_reserve_basis"] == "auto_v1"
    assert policy["host_reserve_fraction"] is None
    assert policy["host_reserve_bytes"] == reserve_gib * GIB


def test_empty_schema_one_ledger_upgrades_but_active_one_fails_closed(tmp_path: Path):
    ledger = tmp_path / "ledger.json"
    ledger.write_text('{"schema":1,"policy":null,"leases":[]}', encoding="utf-8")
    assert telemetry._read_ledger(ledger) == {
        "schema": 2,
        "policy": None,
        "leases": [],
    }

    ledger.write_text(
        '{"schema":1,"policy":{},"leases":[{"lease_id":"old"}]}',
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="requires leases to finish"):
        telemetry._read_ledger(ledger)


def test_admission_labels_unprofiled_allowance_separately_from_learned_need(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 30 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"

    unknown = telemetry._handle_admission_request(_request(state_root))
    assert unknown["status"] == "admitted"
    assert unknown["detail"] == (
        "unprofiled live-capacity allowance reserved before mutation"
    )
    assert unknown["capacity"]["allowance_basis"] == (
        "unprofiled_available_backed"
    )
    assert unknown["capacity"]["allowance_bytes"] == 6655 * MIB
    assert unknown["capacity"]["allowance_bytes"] < 16 * GIB
    assert unknown["capacity"]["predicted_rss_bytes"] is None

    released = telemetry._handle_admission_request(_lease_request(unknown, op="release"))
    assert released["status"] == "released"

    learned = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=8 * GIB)
    )
    assert learned["status"] == "admitted"
    assert learned["detail"] == "learned allowance reserved before mutation"
    assert learned["capacity"]["allowance_basis"] == (
        "learned_profile_plus_25_percent"
    )
    assert learned["capacity"]["allowance_bytes"] == 10 * GIB
    assert learned["capacity"]["predicted_rss_bytes"] == 8 * GIB


def test_unprofiled_refusal_requires_actual_live_headroom_exhaustion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(
        telemetry,
        "_host_memory",
        lambda: (TOTAL, 16 * GIB + CONTROL + MIB // 2),
    )
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)

    result = telemetry._handle_admission_request(_request(tmp_path / "state"))

    assert result["status"] == "refused"
    assert result["reason"] == "insufficient_live_memory"
    assert result["detail"] == (
        "unprofiled command cannot receive the minimum live allowance while preserving host reserve"
    )
    assert result["capacity"]["allowance_basis"] == (
        "unprofiled_available_backed"
    )
    assert result["capacity"]["allowance_bytes"] < MIB


def test_unprofiled_open_slots_share_available_backed_capacity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 26 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: 128 * MIB)
    state_root = tmp_path / "state"

    def request() -> dict[str, object]:
        return _request(
            state_root,
            max_jobs=2,
            command_fraction=0.3125,
            reserve_fraction=None,
        )

    first = telemetry._handle_admission_request(request())
    second = telemetry._handle_admission_request(request())
    third = telemetry._handle_admission_request(request())

    assert first["status"] == "admitted"
    assert second["status"] == "admitted"
    assert int(first["lease"]["allowance_bytes"]) == 4991 * MIB
    assert int(second["lease"]["allowance_bytes"]) == 4992 * MIB
    assert first["capacity"]["allocation_rule"] == (
        "unprofiled_open_slot_fair_share_v1"
    )
    assert first["capacity"]["remaining_backed_capacity_bytes"] == 10239 * MIB
    assert first["capacity"]["open_slots_at_sizing"] == 2
    assert first["capacity"]["per_open_slot_capacity_bytes"] == 5119 * MIB
    assert second["capacity"]["open_slots_at_sizing"] == 1
    assert second["capacity"]["per_open_slot_capacity_bytes"] == 5120 * MIB
    assert third["status"] == "refused"
    assert third["reason"] == "guarded_job_limit"


def test_unprofiled_fair_share_preserves_single_slot_sizing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 26 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: 128 * MIB)

    result = telemetry._handle_admission_request(
        _request(
            tmp_path / "state",
            max_jobs=1,
            command_fraction=0.3125,
            reserve_fraction=None,
        )
    )

    assert result["status"] == "admitted"
    assert result["lease"]["allowance_bytes"] == 10111 * MIB
    assert result["capacity"]["open_slots_at_sizing"] == 1
    assert result["capacity"]["per_open_slot_capacity_bytes"] == 10239 * MIB


def test_unprofiled_final_renewal_rebalances_before_global_unsafety(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    available = 30 * GIB
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, available))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"

    request = lambda: _request(  # noqa: E731
        state_root,
        max_jobs=2,
        command_fraction=0.25,
        reserve_fraction=None,
    )
    first = telemetry._handle_admission_request(request())
    assert first["status"] == "admitted"
    assert first["lease"]["allowance_bytes"] == 6655 * MIB
    assert first["lease"]["capacity_bytes"] == 7167 * MIB

    available = 24 * GIB
    renewed = telemetry._handle_admission_request(_lease_request(first))
    assert renewed["status"] == "admitted"
    assert renewed["reason"] == "renewed_resized"
    assert renewed["lease"]["allowance_bytes"] == 3583 * MIB
    assert renewed["lease"]["capacity_bytes"] == 4095 * MIB

    second = telemetry._handle_admission_request(request())
    assert second["status"] == "admitted"
    assert second["lease"]["allowance_bytes"] == 3584 * MIB
    assert second["lease"]["capacity_bytes"] == 4096 * MIB


def test_actual_commitments_not_policy_ceiling_control_concurrency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"

    def request() -> dict[str, object]:
        return _request(
            state_root,
            predicted_rss_bytes=4 * GIB,
            command_fraction=0.50,
            reserve_fraction=0.25,
            max_jobs=2,
        )

    first = telemetry._handle_admission_request(request())
    second = telemetry._handle_admission_request(request())
    third = telemetry._handle_admission_request(request())

    assert first["status"] == "admitted"
    assert second["status"] == "admitted"
    assert third["status"] == "refused"
    assert third["reason"] == "guarded_job_limit"


def test_falling_live_memory_at_final_renewal_releases_and_later_controller_reclaims(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # reserve policy+bracket, renewal policy+bracket, later reserve policy+bracket
    readings = iter(
        [(TOTAL, AVAILABLE_SAFE)] * 4
        + [(TOTAL, AVAILABLE_UNSAFE)] * 4
        + [(TOTAL, AVAILABLE_SAFE)] * 4
    )
    monkeypatch.setattr(telemetry, "_host_memory", lambda: next(readings))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"

    reserved = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=8 * GIB)
    )
    assert reserved["status"] == "admitted"

    renewal = telemetry._handle_admission_request(_lease_request(reserved))
    assert renewal["status"] == "refused"
    assert renewal["reason"] == "live_memory_changed"
    assert renewal["lease_released"] is True
    assert _ledger_leases(state_root) == []

    later = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=8 * GIB)
    )
    assert later["status"] == "admitted"


def test_unprofiled_final_renewal_shrinks_to_current_backed_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    # Unknown reserve: host read + sizing and final transactions. Renewal uses
    # the same two-stage sizing at a lower but still usable floor.
    reads = 0

    def host_memory() -> tuple[int, int]:
        nonlocal reads
        reads += 1
        return (TOTAL, 30 * GIB if reads <= 7 else 29 * GIB)

    monkeypatch.setattr(telemetry, "_host_memory", host_memory)
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"

    reserved = telemetry._handle_admission_request(_request(state_root))
    assert reserved["status"] == "admitted"
    assert reserved["lease"]["allowance_bytes"] == 6655 * MIB

    renewal = telemetry._handle_admission_request(_lease_request(reserved))

    assert renewal["status"] == "admitted"
    assert renewal["reason"] == "renewed_resized"
    assert renewal["lease"]["allowance_basis"] == "unprofiled_available_backed"
    assert renewal["lease"]["allowance_bytes"] == 6143 * MIB
    assert renewal["lease"]["capacity_bytes"] == 6143 * MIB + CONTROL
    assert renewal["capacity"]["previous_allowance_bytes"] == 6655 * MIB
    leases = _ledger_leases(state_root)
    assert len(leases) == 1
    assert leases[0]["allowance_bytes"] == 6143 * MIB
    assert leases[0]["capacity_bytes"] == 6143 * MIB + CONTROL

    reservation = MemoryReservation.from_payload(renewal)
    token = uuid.uuid4().hex
    rc = telemetry._guarded_run(
        [sys.executable, "-c", "pass"],
        max_command_bytes=reservation.allowance_bytes,
        min_available_bytes=reservation.min_available_bytes,
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=_lease_request(renewal),
    )
    result = _guard_result(capsys.readouterr().err, token)
    assert rc == 0
    assert result["status"] == "ok"
    assert result["max_command_bytes"] == 6143 * MIB
    assert _ledger_leases(state_root) == []


def test_falling_live_memory_at_helper_claim_never_releases_gate_and_reclaims(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    # reserve (4), renew (4), helper initialization (1), claim (4), later reserve (4)
    readings = iter(
        [(TOTAL, AVAILABLE_SAFE)] * 9
        + [(TOTAL, AVAILABLE_UNSAFE)] * 4
        + [(TOTAL, AVAILABLE_SAFE)] * 4
    )
    monkeypatch.setattr(telemetry, "_host_memory", lambda: next(readings))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"
    sentinel = tmp_path / "argv-started"

    reserved = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=8 * GIB)
    )
    assert reserved["status"] == "admitted"
    renewed = telemetry._handle_admission_request(_lease_request(reserved))
    assert renewed["status"] == "admitted"
    lease = renewed["lease"]
    assert isinstance(lease, dict)
    token = uuid.uuid4().hex

    rc = telemetry._guarded_run(
        [
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({str(sentinel)!r}).write_text('started')",
        ],
        max_command_bytes=int(lease["allowance_bytes"]),
        min_available_bytes=int(lease["min_available_bytes"]),
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=_lease_request(renewed),
    )

    captured = capsys.readouterr()
    result = _guard_result(captured.err, token)
    assert rc == 125
    assert result["status"] == "refused"
    assert result["reason"] == "live_memory_changed"
    assert result["command_started"] is False
    assert result["memory_admission"]["lease_released"] is True
    assert not sentinel.exists()
    assert _ledger_leases(state_root) == []

    later = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=8 * GIB)
    )
    assert later["status"] == "admitted"


def test_gate_release_interruption_is_completion_unknown_never_false(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, AVAILABLE_SAFE))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"
    reserved = telemetry._handle_admission_request(_request(state_root))
    assert reserved["status"] == "admitted"
    reservation = MemoryReservation.from_payload(reserved)
    token = uuid.uuid4().hex

    def interrupt_after_release() -> None:
        raise telemetry._GuardInterrupted("deterministic post-gate interruption")

    monkeypatch.setattr(telemetry, "_after_gate_release", interrupt_after_release)
    rc = telemetry._guarded_run(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        max_command_bytes=reservation.allowance_bytes,
        min_available_bytes=reservation.min_available_bytes,
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=_lease_request(reserved),
    )

    captured = capsys.readouterr()
    assert rc == 125
    assert f"__REMRUN_GUARD_READY_{token}__" in captured.err
    assert f"__REMRUN_GUARD_RESULT_{token}__" not in captured.err
    assert '"command_started":false' not in captured.err
    assert _ledger_leases(state_root) == []

    with pytest.raises(TransportError, match="completion is unknown") as excinfo:
        _finalize_guarded_result(
            helper_exit_code=rc,
            stdout="",
            stderr=captured.err,
            token=token,
            reservation=reservation,
            telemetry=None,
            platform_name="test",
        )
    assert type(excinfo.value).__name__ == "GuardFinalizationError"
    assert getattr(excinfo.value, "command_started", "missing") is None
    assert getattr(excinfo.value, "memory_guard", "missing") is None


def test_missing_guard_markers_are_completion_unknown_not_prestart():
    reservation = MemoryReservation(
        lease_id="lease",
        lease_token="token",
        state_root="/tmp/remrun-test",
        allowance_bytes=8 * GIB,
        control_overhead_bytes=CONTROL,
        capacity_bytes=16 * GIB,
        max_command_bytes=8 * GIB,
        min_available_bytes=4 * GIB,
        host_total_bytes=32 * GIB,
        safe_concurrency=1,
        expires_at=4_102_444_800.0,
    )
    with pytest.raises(TransportError, match="completion is unknown") as excinfo:
        _finalize_guarded_result(
            helper_exit_code=255,
            stdout="",
            stderr="ssh connection closed before private guard records arrived",
            token="0" * 32,
            reservation=reservation,
            telemetry=None,
            platform_name="test",
        )
    assert type(excinfo.value).__name__ == "GuardFinalizationError"
    assert getattr(excinfo.value, "command_started", "missing") is None
    assert getattr(excinfo.value, "memory_guard", "missing") is None


def test_gate_status_eof_without_popen_proof_is_completion_unknown():
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    with pytest.raises(telemetry._LaunchCompletionUnknown, match="without launch proof"):
        telemetry._confirm_gated_exec(read_fd)


def test_cleanup_survivor_quarantines_lease_until_identity_verified_dead(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    state_root = tmp_path / "state"
    request = _request(
        state_root,
        predicted_rss_bytes=MIB,
        max_jobs=1,
        command_fraction=0.10,
        reserve_fraction=0.05,
    )
    reserved = telemetry._handle_admission_request(request)
    assert reserved["status"] == "admitted"
    reservation = MemoryReservation.from_payload(reserved)
    monkeypatch.setattr(telemetry, "_terminate_guarded_tree", lambda *_args: False)
    token = uuid.uuid4().hex

    rc = telemetry._guarded_run(
        [sys.executable, "-c", "import time; x=bytearray(8*1024*1024); time.sleep(30)"],
        max_command_bytes=reservation.allowance_bytes,
        min_available_bytes=reservation.min_available_bytes,
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=_lease_request(reserved),
    )

    payload = _guard_result(capsys.readouterr().err, token)
    assert rc == 125
    assert payload["cleanup_complete"] is False
    leases = _ledger_leases(state_root)
    assert len(leases) == 1
    assert leases[0]["state"] == "quarantined"
    pgid = int(leases[0]["pgid"])
    assert leases[0]["survivors"]

    blocked = telemetry._handle_admission_request(
        _request(
            state_root,
            predicted_rss_bytes=MIB,
            max_jobs=1,
            command_fraction=0.10,
            reserve_fraction=0.05,
        )
    )
    assert blocked["status"] == "refused"
    assert blocked["reason"] == "guarded_job_limit"

    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        os.waitpid(pgid, 0)
    except ChildProcessError:
        pass
    for _ in range(100):
        if not telemetry._group_alive(pgid):
            break
        time.sleep(0.01)

    later = telemetry._handle_admission_request(
        _request(
            state_root,
            predicted_rss_bytes=MIB,
            max_jobs=1,
            command_fraction=0.10,
            reserve_fraction=0.05,
        )
    )
    assert later["status"] == "admitted"
    assert later["stale_reaped"] == 1


def test_ledger_ttl_token_policy_max_jobs_and_prediction_headroom(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, AVAILABLE_SAFE))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"
    request = _request(
        state_root,
        predicted_rss_bytes=8 * GIB,
        max_jobs=1,
        command_fraction=0.25,
        reserve_fraction=0.10,
    )
    first = telemetry._handle_admission_request(request)
    assert first["status"] == "admitted"
    lease = first["lease"]
    assert isinstance(lease, dict)
    assert lease["allowance_bytes"] == 10 * GIB
    assert lease["capacity_bytes"] == 10 * GIB + CONTROL

    second = telemetry._handle_admission_request(
        _request(
            state_root,
            predicted_rss_bytes=MIB,
            max_jobs=1,
            command_fraction=0.25,
            reserve_fraction=0.10,
        )
    )
    assert second["status"] == "refused"
    assert second["reason"] == "guarded_job_limit"

    wrong_token = _lease_request(first)
    wrong_token["lease_token"] = "f" * 32
    assert telemetry._handle_admission_request(wrong_token)["reason"] == "reservation_missing"

    wrong_policy = _lease_request(first)
    wrong_policy["host_reserve_fraction"] = 0.11
    assert telemetry._handle_admission_request(wrong_policy)["reason"] == "policy_mismatch"

    ledger_path = state_root / "memory-guard" / "v2" / "ledger.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    ledger["leases"][0]["expires_at"] = time.time() - 1
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    replacement = telemetry._handle_admission_request(
        _request(
            state_root,
            predicted_rss_bytes=MIB,
            max_jobs=1,
            command_fraction=0.25,
            reserve_fraction=0.10,
        )
    )
    assert replacement["status"] == "admitted"
    assert replacement["stale_reaped"] == 1

    too_large = telemetry._handle_admission_request(
        _request(
            tmp_path / "other-state",
            predicted_rss_bytes=14 * GIB,
            max_jobs=1,
            command_fraction=0.25,
            reserve_fraction=0.10,
        )
    )
    assert too_large["status"] == "refused"
    assert too_large["reason"] == "prediction_exceeds_command_limit"


def test_local_transport_guarded_exit_uses_reserved_lease_end_to_end(tmp_path: Path):
    from remrun.models import Device
    from remrun.transport import LocalSimTransport

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

    result = transport.exec(
        ["/usr/bin/true"],
        cwd=str(tmp_path / "remote" / "project"),
        telemetry=False,
        memory_reservation=reservation,
    )

    assert result.exit_code == 0
    assert result.memory_guard is not None
    assert result.memory_guard["status"] == "ok"
    assert result.memory_guard["command_started"] is True
    assert result.memory_guard["peak_command_bytes"] < reservation.allowance_bytes
    assert result.memory_reservation is not None
    assert result.memory_reservation.lease_id == reservation.lease_id
    assert result.memory_reservation.lease_token == reservation.lease_token
    assert result.memory_guard["max_command_bytes"] == (
        result.memory_reservation.allowance_bytes
    )
    assert _ledger_leases(tmp_path / "state") == []
