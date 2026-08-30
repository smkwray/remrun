from __future__ import annotations

import json
import sys
from dataclasses import replace
import uuid
from pathlib import Path

import pytest

from remrun import _posix_telemetry as telemetry
from remrun.fleet import executor
from remrun.memory_guard import MemoryGuard, MemoryReservation
from remrun.transport import (
    GuardFinalizationError,
    _extract_memory_guard,
    _finalize_guarded_result,
    _guarded_helper_args,
    _lease_request,
)

MIB = 1024**2
GIB = 1024**3
TOTAL = 64 * GIB
CONTROL = 128 * MIB


def _request(
    state_root: Path,
    *,
    predicted_rss_bytes: int | None,
    max_jobs: int = 2,
    command_fraction: float = 0.3125,
) -> dict[str, object]:
    return {
        "schema": 2,
        "op": "reserve",
        "state_root": str(state_root),
        "lease_id": uuid.uuid4().hex,
        "lease_token": uuid.uuid4().hex,
        "predicted_rss_bytes": predicted_rss_bytes,
        "explicit_limit_bytes": None,
        "command_limit_fraction": command_fraction,
        "host_reserve_fraction": None,
        "max_jobs": max_jobs,
        "reservation_ttl_seconds": 120.0,
    }


def _production_request(
    payload: dict[str, object], *, op: str = "renew"
) -> tuple[MemoryGuard, MemoryReservation, dict[str, object]]:
    policy = payload["policy"]
    assert isinstance(policy, dict)
    guard = MemoryGuard(
        command_limit_fraction=policy["command_limit_fraction"],
        host_reserve_fraction=policy["host_reserve_fraction"],
        max_jobs=int(policy["max_jobs"]),
    )
    reservation = MemoryReservation.from_payload(payload)
    request = _lease_request(guard, reservation)
    request["op"] = op
    return guard, reservation, request


def _valid_guard_result(
    reservation: MemoryReservation, **overrides: object
) -> dict[str, object]:
    result: dict[str, object] = {
        "schema": 1,
        "status": "ok",
        "reason": "completed",
        "command_started": True,
        "command_exit_code": 0,
        "helper_exit_code": 0,
        "max_command_bytes": None,
        "min_available_bytes": reservation.min_available_bytes,
    }
    for name in (
        "allowance_bytes",
        "control_overhead_bytes",
        "capacity_bytes",
        "allowance_basis",
        "allocation_rule",
        "command_limit_bytes",
        "remaining_backed_capacity_bytes",
        "strict_margin_bytes",
        "learned_allowance_bytes",
        "live_backed_allowance_bytes",
        "learned_allowance_live_backed",
        "predicted_rss_bytes",
    ):
        result[name] = getattr(reservation, name)
    result.update(overrides)
    return result


def _finalize_guard_result(
    payload: dict[str, object], reservation: MemoryReservation, token: str
):
    stderr = f"\n__REMRUN_GUARD_RESULT_{token}__ " + json.dumps(payload) + "\n"
    return _finalize_guarded_result(
        helper_exit_code=0,
        stdout="",
        stderr=stderr,
        token=token,
        reservation=reservation,
        telemetry=None,
        platform_name="test",
    )


def test_running_inferred_allowance_is_not_a_concurrency_veto(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 26 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"

    first = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=8 * GIB)
    )
    assert first["status"] == "admitted"
    _guard, _reservation, first_request = _production_request(first)

    monkeypatch.setattr(telemetry, "_identity_for_pid", lambda pid: f"{pid}:audit")
    monkeypatch.setattr(telemetry, "_lease_private_snapshot", lambda leases: {})
    claim = telemetry._claim_memory_lease(
        first_request,
        helper_pid=111,
        root_pid=222,
        root_identity="222:audit",
        pgid=222,
    )
    assert claim["status"] == "admitted"
    assert claim["capacity"]["current_guarded_private_bytes"] == 0

    second = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=256 * MIB)
    )
    assert second["status"] == "admitted"
    assert second["lease"]["command_limit_bytes"] is None


def test_claim_clip_survives_transport_and_durable_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    state_root = tmp_path / "state"
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: 512 * MIB)
    reserved = telemetry._handle_admission_request(
        {
            **_request(
                state_root,
                predicted_rss_bytes=8 * GIB,
                command_fraction=0.25,
            ),
            "host_reserve_fraction": 0.25,
        }
    )
    guard, reservation, renew_request = _production_request(reserved)
    renewed = telemetry._handle_admission_request(renew_request)
    assert renewed["status"] == "admitted"
    _guard, reservation, claim_request = _production_request(renewed)
    original_allowance = reservation.allowance_bytes
    assert reservation.allowance_basis == "learned_profile_plus_25_percent"

    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 20 * GIB))
    token = uuid.uuid4().hex
    rc = telemetry._guarded_run(
        [sys.executable, "-c", "pass"],
        max_command_bytes=None,
        min_available_bytes=reservation.min_available_bytes,
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=claim_request,
    )
    captured = capsys.readouterr()
    _cleaned, raw_guard, _ready = _extract_memory_guard(captured.err, token)
    assert raw_guard is not None
    assert raw_guard["allowance_basis"] == "learned_profile_live_headroom"
    assert raw_guard["allowance_bytes"] < original_allowance
    assert raw_guard["allowance_bytes"] == raw_guard["live_backed_allowance_bytes"]
    assert raw_guard["learned_allowance_live_backed"] is False
    for name in (
        "allowance_bytes",
        "capacity_bytes",
        "control_overhead_bytes",
        "allowance_basis",
        "allocation_rule",
        "command_limit_bytes",
        "remaining_backed_capacity_bytes",
        "strict_margin_bytes",
        "learned_allowance_bytes",
        "live_backed_allowance_bytes",
        "learned_allowance_live_backed",
        "predicted_rss_bytes",
    ):
        assert name in raw_guard

    finalized = _finalize_guarded_result(
        helper_exit_code=rc,
        stdout=captured.out,
        stderr=captured.err,
        token=token,
        reservation=reservation,
        telemetry=None,
        platform_name="audit target",
    )
    assert finalized.memory_reservation is not None
    assert finalized.memory_reservation.allowance_basis == (
        "learned_profile_live_headroom"
    )
    assert finalized.memory_reservation.allowance_bytes == raw_guard["allowance_bytes"]
    assert finalized.memory_guard is not None
    assert finalized.memory_guard["allowance_bytes"] == raw_guard["allowance_bytes"]
    assert finalized.memory_guard["learned_allowance_bytes"] == raw_guard[
        "learned_allowance_bytes"
    ]
    assert finalized.memory_guard["command_limit_enforced"] is False

    receipt = executor._memory_limit_receipt(
        {"schema": 1}, admission=renewed, guard=finalized.memory_guard
    )
    assert receipt["admission"]["allowance_bytes"] == original_allowance
    assert receipt["outcome"]["allowance_bytes"] == raw_guard["allowance_bytes"]
    assert receipt["allowance_bytes"] == raw_guard["allowance_bytes"]
    assert receipt["allowance_basis"] == "learned_profile_live_headroom"
    assert receipt["outcome"]["learned_allowance_bytes"] == raw_guard[
        "learned_allowance_bytes"
    ]
    assert receipt.get("enforced_command_limit_bytes") is None

    claimed = finalized.memory_reservation
    assert claimed.learned_allowance_live_backed is False
    with pytest.raises(GuardFinalizationError, match="authenticated downward resize"):
        _finalize_guard_result(
            _valid_guard_result(claimed, learned_allowance_live_backed=True),
            claimed,
            token,
        )


def test_policy_capped_learned_allowance_finalizes_with_live_backed_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    state_root = tmp_path / "state"
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: 512 * MIB)
    reserved = telemetry._handle_admission_request(
        {
            **_request(
                state_root,
                predicted_rss_bytes=14 * GIB,
                command_fraction=0.25,
            ),
            "host_reserve_fraction": 0.25,
        }
    )
    assert reserved["status"] == "admitted"
    _guard, _reservation, renew_request = _production_request(reserved)
    renewed = telemetry._handle_admission_request(renew_request)
    assert renewed["status"] == "admitted"
    _guard, reservation, claim_request = _production_request(renewed)
    assert reservation.allowance_bytes == 16 * GIB
    assert reservation.learned_allowance_bytes == 17_920 * MIB
    assert reservation.live_backed_allowance_bytes is not None
    assert reservation.live_backed_allowance_bytes >= reservation.allowance_bytes
    assert reservation.learned_allowance_live_backed is True

    token = uuid.uuid4().hex
    rc = telemetry._guarded_run(
        [sys.executable, "-c", "pass"],
        max_command_bytes=None,
        min_available_bytes=reservation.min_available_bytes,
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=claim_request,
    )
    captured = capsys.readouterr()
    _cleaned, raw_guard, _ready = _extract_memory_guard(captured.err, token)
    assert raw_guard is not None
    assert raw_guard["allowance_bytes"] == reservation.allowance_bytes
    assert raw_guard["learned_allowance_bytes"] == 17_920 * MIB
    assert raw_guard["learned_allowance_live_backed"] is True

    finalized = _finalize_guarded_result(
        helper_exit_code=rc,
        stdout=captured.out,
        stderr=captured.err,
        token=token,
        reservation=reservation,
        telemetry=None,
        platform_name="audit target",
    )
    assert finalized.memory_reservation is not None
    assert finalized.memory_reservation.allowance_bytes == 16 * GIB
    assert finalized.memory_reservation.learned_allowance_bytes == 17_920 * MIB
    assert finalized.memory_reservation.learned_allowance_live_backed is True
    assert finalized.memory_guard is not None
    assert finalized.memory_guard["learned_allowance_live_backed"] is True


def test_finalization_rejects_incomplete_or_unauthenticated_claim_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    admitted = telemetry._handle_admission_request(
        _request(tmp_path / "state", predicted_rss_bytes=8 * GIB)
    )
    _guard, reservation, _request_payload = _production_request(admitted)

    token = uuid.uuid4().hex
    valid = _valid_guard_result(reservation, command_limit_bytes=None)

    def finalize(payload: dict[str, object]):
        return _finalize_guard_result(payload, reservation, token)

    incomplete = dict(valid)
    incomplete.pop("allowance_bytes")
    with pytest.raises(GuardFinalizationError, match="omitted claim-effective"):
        finalize(incomplete)

    increased = {**valid, "allowance_bytes": reservation.allowance_bytes + MIB,
                 "capacity_bytes": reservation.capacity_bytes + MIB}
    with pytest.raises(GuardFinalizationError, match="authenticated downward resize"):
        finalize(increased)

    mutated = {**valid, "allowance_basis": "unprofiled_available_backed"}
    with pytest.raises(GuardFinalizationError, match="authenticated downward resize"):
        finalize(mutated)

    command_mutated = {**valid, "command_limit_bytes": MIB}
    with pytest.raises(GuardFinalizationError, match="malformed effective reservation"):
        finalize(command_mutated)


def test_finalization_rejects_increased_or_erased_claim_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    admitted = telemetry._handle_admission_request(
        _request(tmp_path / "state", predicted_rss_bytes=8 * GIB)
    )
    assert admitted["status"] == "admitted"
    _guard, reservation, _request_payload = _production_request(admitted)
    assert reservation.remaining_backed_capacity_bytes is not None
    assert reservation.live_backed_allowance_bytes is not None

    token = uuid.uuid4().hex
    valid = _valid_guard_result(reservation)

    def finalize(payload: dict[str, object]):
        return _finalize_guard_result(payload, reservation, token)

    increased_headroom = {
        **valid,
        "remaining_backed_capacity_bytes": (
            reservation.remaining_backed_capacity_bytes + MIB
        ),
    }
    with pytest.raises(GuardFinalizationError, match="authenticated downward resize"):
        finalize(increased_headroom)

    erased_headroom = {**valid, "remaining_backed_capacity_bytes": None}
    with pytest.raises(GuardFinalizationError, match="authenticated downward resize"):
        finalize(erased_headroom)

    increased_live_allowance = {
        **valid,
        "live_backed_allowance_bytes": reservation.live_backed_allowance_bytes + MIB,
    }
    with pytest.raises(GuardFinalizationError, match="authenticated downward resize"):
        finalize(increased_live_allowance)

    erased_live_allowance = {**valid, "live_backed_allowance_bytes": None}
    with pytest.raises(GuardFinalizationError, match="authenticated downward resize"):
        finalize(erased_live_allowance)

    conservative = {
        **valid,
        "allowance_bytes": reservation.allowance_bytes - 2 * MIB,
        "capacity_bytes": reservation.capacity_bytes - 2 * MIB,
        "remaining_backed_capacity_bytes": (
            reservation.remaining_backed_capacity_bytes - MIB
        ),
        "live_backed_allowance_bytes": reservation.learned_allowance_bytes - MIB,
        "learned_allowance_live_backed": False,
    }
    finalized = finalize(conservative)
    assert finalized.memory_reservation is not None
    assert finalized.memory_reservation.allowance_bytes == (
        reservation.allowance_bytes - 2 * MIB
    )
    assert finalized.memory_reservation.learned_allowance_live_backed is False


def test_claim_projection_cannot_add_metadata_to_null_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    admitted = telemetry._handle_admission_request(
        _request(tmp_path / "state", predicted_rss_bytes=None)
    )
    assert admitted["status"] == "admitted"
    _guard, reservation, _request_payload = _production_request(admitted)
    original_null = replace(
        reservation,
        remaining_backed_capacity_bytes=None,
        live_backed_allowance_bytes=None,
        learned_allowance_live_backed=None,
    )

    token = uuid.uuid4().hex
    valid = _valid_guard_result(original_null, command_limit_bytes=None)

    def finalize(payload: dict[str, object]):
        return _finalize_guard_result(payload, original_null, token)

    with pytest.raises(GuardFinalizationError, match="authenticated downward resize"):
        finalize({**valid, "remaining_backed_capacity_bytes": MIB})
    with pytest.raises(GuardFinalizationError, match="authenticated downward resize"):
        finalize({**valid, "live_backed_allowance_bytes": MIB})
    with pytest.raises(GuardFinalizationError, match="authenticated downward resize"):
        finalize({**valid, "learned_allowance_live_backed": False})


def test_helper_construction_is_basis_authoritative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    admitted = telemetry._handle_admission_request(
        _request(tmp_path / "state", predicted_rss_bytes=8 * GIB)
    )
    guard, reservation, _request_payload = _production_request(admitted)

    contradictory = replace(
        reservation, command_limit_bytes=reservation.allowance_bytes
    )
    args = _guarded_helper_args(
        "helper.py", guard, contradictory, "e" * 32, detailed=False, telemetry=False
    )
    assert contradictory.effective_command_limit_bytes is None
    assert "--guard-max-bytes" not in args
    request = _lease_request(guard, contradictory)
    assert "command_limit_bytes" not in request

    explicit_legacy = replace(
        reservation,
        allowance_basis="explicit_command_limit",
        command_limit_bytes=None,
    )
    assert explicit_legacy.effective_command_limit_bytes == explicit_legacy.allowance_bytes
    assert _lease_request(guard, explicit_legacy)["command_limit_bytes"] == (
        explicit_legacy.allowance_bytes
    )


def test_inferred_wire_cannot_add_command_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    admitted = telemetry._handle_admission_request(
        _request(tmp_path / "state", predicted_rss_bytes=8 * GIB)
    )
    lease = admitted["lease"]
    assert isinstance(lease, dict)
    malformed = {
        **admitted,
        "lease": {**lease, "command_limit_bytes": lease["allowance_bytes"]},
    }
    with pytest.raises(ValueError, match="requires explicit_command_limit basis"):
        MemoryReservation.from_payload(malformed)


def test_ambiguous_legacy_guard_request_cannot_set_hard_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    admitted = telemetry._handle_admission_request(
        _request(tmp_path / "state", predicted_rss_bytes=8 * GIB)
    )
    _guard, reservation, production_request = _production_request(admitted)
    legacy_request = dict(production_request)
    legacy_request.pop("allowance_basis")
    legacy_request.pop("command_limit_bytes", None)
    marker = tmp_path / "must-not-run"
    token = uuid.uuid4().hex

    rc = telemetry._guarded_run(
        [sys.executable, "-c", f"open({str(marker)!r}, 'w').write('ran')"],
        max_command_bytes=reservation.allowance_bytes,
        min_available_bytes=reservation.min_available_bytes,
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=legacy_request,
    )
    _cleaned, guard_result, _ready = _extract_memory_guard(capsys.readouterr().err, token)
    assert rc == 125
    assert guard_result is not None
    assert guard_result["status"] == "refused"
    assert guard_result["reason"] == "guard_initialization_failed"
    assert guard_result["command_started"] is False
    assert "guard command limit does not match the reservation" in guard_result["detail"]
    assert not marker.exists()
