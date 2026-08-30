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

# _posix_telemetry imports fcntl at module scope, so this module cannot even be
# collected off POSIX. Skip the module rather than aborting the whole run.
pytest.importorskip("fcntl", reason="POSIX-only telemetry surface")

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
    command_fraction: float | None = 0.25,
    reserve_fraction: float | None = 0.25,
    ttl: float = 120.0,
) -> dict[str, object]:
    request = {
        "schema": 2,
        "op": op,
        "state_root": str(state_root),
        "lease_id": lease_id or uuid.uuid4().hex,
        "lease_token": lease_token or uuid.uuid4().hex,
        "predicted_rss_bytes": predicted_rss_bytes,
        "explicit_limit_bytes": explicit_limit_bytes,
        "host_reserve_fraction": reserve_fraction,
        "max_jobs": max_jobs,
        "reservation_ttl_seconds": ttl,
    }
    if command_fraction is not None:
        request["command_limit_fraction"] = command_fraction
    return request


def _lease_request(reserved: dict[str, object], *, op: str = "renew") -> dict[str, object]:
    lease = reserved["lease"]
    policy = reserved["policy"]
    assert isinstance(lease, dict) and isinstance(policy, dict)
    request = {
        "schema": 2,
        "op": op,
        "state_root": lease["state_root"],
        "lease_id": lease["lease_id"],
        "lease_token": lease["lease_token"],
        "allowance_bytes": lease["allowance_bytes"],
        "control_overhead_bytes": lease["control_overhead_bytes"],
        "capacity_bytes": lease["capacity_bytes"],
        "host_reserve_fraction": policy["host_reserve_fraction"],
        "max_jobs": policy["max_jobs"],
        "reservation_ttl_seconds": policy["reservation_ttl_seconds"],
    }
    if policy["command_limit_fraction"] is not None:
        request["command_limit_fraction"] = policy["command_limit_fraction"]
    if policy.get("allocation_rule") is not None:
        request["allocation_rule"] = policy["allocation_rule"]
    return request


def _production_lease_request(
    reserved: dict[str, object], *, op: str = "renew"
) -> dict[str, object]:
    request = _lease_request(reserved, op=op)
    lease = reserved["lease"]
    assert isinstance(lease, dict)
    request.update(
        {
            "allowance_basis": lease.get("allowance_basis"),
            "command_limit_bytes": lease.get("command_limit_bytes"),
            "remaining_backed_capacity_bytes": lease.get(
                "remaining_backed_capacity_bytes"
            ),
            "strict_margin_bytes": lease.get("strict_margin_bytes"),
            "learned_allowance_bytes": lease.get("learned_allowance_bytes"),
            "live_backed_allowance_bytes": lease.get(
                "live_backed_allowance_bytes"
            ),
            "learned_allowance_live_backed": lease.get(
                "learned_allowance_live_backed"
            ),
            "predicted_rss_bytes": lease.get("predicted_rss_bytes"),
        }
    )
    return request


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


def test_omitted_command_fraction_derives_ceiling_from_host_reserve():
    policy = telemetry._policy_from_request(
        {"max_jobs": 2, "reservation_ttl_seconds": 120.0},
        64 * GIB,
    )

    assert policy["command_limit_fraction"] is None
    assert policy["max_command_bytes"] == 48 * GIB
    assert policy["policy_command_ceiling_bytes"] == 48 * GIB


def test_null_command_fraction_is_not_an_alias_for_omission():
    with pytest.raises(ValueError, match="command_limit_fraction"):
        telemetry._policy_from_request(
            {
                "command_limit_fraction": None,
                "max_jobs": 2,
                "reservation_ttl_seconds": 120.0,
            },
            64 * GIB,
        )


def test_command_fraction_presence_is_part_of_active_policy_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, AVAILABLE_SAFE))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"

    omitted = telemetry._handle_admission_request(
        _request(state_root, command_fraction=None, reserve_fraction=0.25)
    )
    assert omitted["status"] == "admitted"
    present = telemetry._handle_admission_request(
        _request(state_root, command_fraction=0.25, reserve_fraction=0.25)
    )

    assert present["status"] == "refused"
    assert present["reason"] == "policy_mismatch"


def test_old_allocation_rule_cannot_mix_with_new_policy(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, AVAILABLE_SAFE))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"
    first = telemetry._handle_admission_request(_request(state_root))
    assert first["status"] == "admitted"

    old = _lease_request(first)
    old["allocation_rule"] = "unprofiled_open_slot_fair_share_v1"
    with pytest.raises(ValueError, match="allocation_rule is unsupported"):
        telemetry._handle_admission_request(old)


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
    assert unknown["capacity"]["allowance_bytes"] == 13823 * MIB
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


def test_learned_allowance_clips_to_live_headroom_instead_of_refusing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """A learned estimate cannot veto a command while reserve headroom remains."""
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 22 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: 45 * MIB)

    result = telemetry._handle_admission_request(
        _request(
            tmp_path / "state",
            predicted_rss_bytes=int(5452.5 * MIB),
        )
    )

    assert result["status"] == "admitted"
    assert result["reason"] == "reserved"
    assert result["capacity"]["allowance_basis"] == "learned_profile_live_headroom"
    # 22 GiB available - 16 GiB reserve - 1 MiB strict margin - 45 MiB overhead.
    assert result["capacity"]["allowance_bytes"] == 6_098 * MIB
    assert result["capacity"]["capacity_bytes"] == 6_098 * MIB + 45 * MIB
    assert result["capacity"]["learned_allowance_bytes"] == 6_816 * MIB
    assert result["capacity"]["live_backed_allowance_bytes"] == 6_098 * MIB
    assert result["capacity"]["learned_allowance_live_backed"] is False
    assert result["lease"]["allowance_bytes"] == 6_098 * MIB
    assert result["lease"]["command_limit_bytes"] is None


def test_healthy_host_learned_receipt_preserves_exact_admission_calculation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """A healthy 64-GiB/48-GiB host keeps the learned allowance exact."""
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 48 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: 45 * MIB)

    result = telemetry._handle_admission_request(
        _request(
            tmp_path / "state",
            predicted_rss_bytes=int(5452.5 * MIB),
            command_fraction=None,
            reserve_fraction=None,
        )
    )

    assert result["status"] == "admitted"
    assert result["reason"] == "reserved"
    capacity = result["capacity"]
    assert capacity["allowance_basis"] == "learned_profile_plus_25_percent"
    assert capacity["predicted_rss_bytes"] == int(5452.5 * MIB)
    assert capacity["learned_allowance_bytes"] == 6_816 * MIB
    assert capacity["allowance_bytes"] == 6_816 * MIB
    assert capacity["capacity_bytes"] == 6_816 * MIB + 45 * MIB
    assert capacity["command_limit_bytes"] is None
    assert capacity["enforced_command_limit_bytes"] is None
    assert result["lease"]["allowance_bytes"] == 6_816 * MIB
    assert result["lease"]["command_limit_bytes"] is None


def test_oversized_learned_profile_clips_before_physical_capacity_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """An inferred estimate above an omitted policy ceiling still gets a safe start."""
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 48 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: 45 * MIB)

    result = telemetry._handle_admission_request(
        _request(
            tmp_path / "state",
            predicted_rss_bytes=60 * GIB,
            command_fraction=None,
            reserve_fraction=None,
        )
    )

    assert result["status"] == "admitted"
    assert result["capacity"]["allowance_basis"] == (
        "learned_profile_live_headroom"
    )
    assert result["capacity"]["learned_allowance_bytes"] == 75 * GIB
    assert result["capacity"]["allowance_bytes"] < 48 * GIB
    assert result["capacity"]["command_limit_bytes"] is None
    assert result["capacity"]["enforced_command_limit_bytes"] is None


def _inferred_overrun_snapshot(lease_id: str) -> dict[str, dict[int, tuple[str, int, int]]]:
    return {lease_id: {100: ("100:r", 20 * GIB, MIB)}}


@pytest.mark.parametrize("available_gib", [48, 20])
def test_inferred_overrun_does_not_block_another_inferred_reservation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, available_gib: int
):
    state_root = tmp_path / "state"
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    first = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=8 * GIB)
    )
    assert first["status"] == "admitted"
    first_lease = first["lease"]
    assert isinstance(first_lease, dict)

    monkeypatch.setattr(
        telemetry, "_host_memory", lambda: (TOTAL, available_gib * GIB)
    )
    monkeypatch.setattr(
        telemetry,
        "_lease_private_snapshot",
        lambda _leases: _inferred_overrun_snapshot(str(first_lease["lease_id"])),
    )
    second = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=8 * GIB)
    )

    assert second["status"] == "admitted"
    assert second["capacity"]["inferred_capacity_overrun"] is True
    assert second["capacity"]["capacity_violation"] is False
    if available_gib == 48:
        assert second["capacity"]["required_available_bytes"] == 26 * GIB + CONTROL
        assert second["capacity"]["allowance_basis"] == (
            "learned_profile_plus_25_percent"
        )
    else:
        assert second["capacity"]["required_available_bytes"] == 20 * GIB - MIB
        assert second["capacity"]["allowance_bytes"] == 3_583 * MIB
        assert second["capacity"]["allowance_basis"] == (
            "learned_profile_live_headroom"
        )


@pytest.mark.parametrize("available_gib", [48, 20])
def test_inferred_overrun_does_not_release_lease_during_renewal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, available_gib: int
):
    state_root = tmp_path / "state"
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    first = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=8 * GIB)
    )
    assert first["status"] == "admitted"
    first_lease = first["lease"]
    assert isinstance(first_lease, dict)

    monkeypatch.setattr(
        telemetry, "_host_memory", lambda: (TOTAL, available_gib * GIB)
    )
    monkeypatch.setattr(
        telemetry,
        "_lease_private_snapshot",
        lambda _leases: _inferred_overrun_snapshot(str(first_lease["lease_id"])),
    )
    renewed = telemetry._handle_admission_request(_lease_request(first))

    assert renewed["status"] == "admitted"
    assert renewed["reason"] == (
        "renewed" if available_gib == 48 else "renewed_resized"
    )
    if available_gib == 20:
        assert renewed["lease"]["allowance_bytes"] == 3_583 * MIB
        assert renewed["lease"]["allowance_basis"] == (
            "learned_profile_live_headroom"
        )
    assert renewed["capacity"]["inferred_capacity_overrun"] is True
    assert renewed["capacity"]["capacity_violation"] is False


@pytest.mark.parametrize("available_gib", [48, 20])
def test_inferred_overrun_does_not_block_helper_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, available_gib: int
):
    state_root = tmp_path / "state"
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 60 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    first = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=8 * GIB)
    )
    assert first["status"] == "admitted"
    first_lease = first["lease"]
    assert isinstance(first_lease, dict)

    monkeypatch.setattr(
        telemetry, "_host_memory", lambda: (TOTAL, available_gib * GIB)
    )
    monkeypatch.setattr(
        telemetry,
        "_lease_private_snapshot",
        lambda _leases: _inferred_overrun_snapshot(str(first_lease["lease_id"])),
    )
    monkeypatch.setattr(telemetry, "_identity_for_pid", lambda pid: f"{pid}:x")
    claimed = telemetry._claim_memory_lease(
        _lease_request(first),
        helper_pid=123,
        root_pid=456,
        root_identity="456:x",
        pgid=456,
    )

    assert claimed["status"] == "admitted"
    assert claimed["reason"] == "claimed"
    assert claimed["capacity"]["inferred_capacity_overrun"] is True
    assert claimed["capacity"]["capacity_violation"] is False


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


def test_unprofiled_allowance_uses_all_available_backed_capacity(
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

    assert first["status"] == "admitted"
    assert second["status"] == "admitted"
    assert int(first["lease"]["allowance_bytes"]) == 10111 * MIB
    assert int(first["lease"]["allowance_bytes"]) > 5 * GIB
    assert first["capacity"]["allocation_rule"] == (
        "unprofiled_live_headroom_v1"
    )
    assert first["capacity"]["remaining_backed_capacity_bytes"] == 10239 * MIB
    assert "open_slots_at_sizing" not in first["capacity"]
    assert "per_open_slot_capacity_bytes" not in first["capacity"]
    assert second["lease"]["allowance_bytes"] == first["lease"]["allowance_bytes"]
    assert second["lease"]["command_limit_bytes"] is None


@pytest.mark.parametrize("mode", ["learned", "explicit"])
def test_unknown_inferred_allowance_does_not_veto_later_small_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
):
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 26 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: 128 * MIB)
    state_root = tmp_path / mode
    unknown = telemetry._handle_admission_request(
        _request(
            state_root,
            command_fraction=0.3125,
            reserve_fraction=None,
        )
    )
    assert unknown["status"] == "admitted"

    if mode == "learned":
        later = _request(
            state_root,
            predicted_rss_bytes=256 * MIB,
            command_fraction=0.3125,
            reserve_fraction=None,
        )
    else:
        later = _request(
            state_root,
            explicit_limit_bytes=256 * MIB,
            command_fraction=0.3125,
            reserve_fraction=None,
        )
    admitted = telemetry._handle_admission_request(later)

    assert admitted["status"] == "admitted"
    if mode == "learned":
        assert admitted["lease"]["command_limit_bytes"] is None
    else:
        assert admitted["lease"]["command_limit_bytes"] == 256 * MIB


def test_unprofiled_allowance_is_independent_of_max_jobs(
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
    assert result["capacity"]["allocation_rule"] == (
        "unprofiled_live_headroom_v1"
    )


def test_first_unprofiled_allowance_is_same_for_max_jobs_one_or_two(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, 26 * GIB))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: 128 * MIB)

    first = telemetry._handle_admission_request(
        _request(tmp_path / "one", max_jobs=1, command_fraction=0.3125,
                 reserve_fraction=None)
    )
    second = telemetry._handle_admission_request(
        _request(tmp_path / "two", max_jobs=2, command_fraction=0.3125,
                 reserve_fraction=None)
    )

    assert first["status"] == second["status"] == "admitted"
    assert first["lease"]["allowance_bytes"] == second["lease"]["allowance_bytes"]


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
    assert first["lease"]["allowance_bytes"] == 13823 * MIB
    assert first["lease"]["capacity_bytes"] == 14335 * MIB

    available = 24 * GIB
    renewed = telemetry._handle_admission_request(_lease_request(first))
    assert renewed["status"] == "admitted"
    assert renewed["reason"] == "renewed_resized"
    assert renewed["lease"]["allowance_bytes"] == 7679 * MIB
    assert renewed["lease"]["capacity_bytes"] == 8191 * MIB

    second = telemetry._handle_admission_request(request())
    assert second["status"] == "admitted"
    assert second["lease"]["allowance_bytes"] == 7679 * MIB
    assert second["lease"]["command_limit_bytes"] is None


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


def test_falling_live_memory_at_final_renewal_clips_and_releases_for_later_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # reserve policy+bracket, renewal policy+bracket, later reserve policy+bracket
    readings = iter(
        [(TOTAL, AVAILABLE_SAFE)] * 4
        + [(TOTAL, AVAILABLE_UNSAFE)] * 4
        + [(TOTAL, AVAILABLE_SAFE)] * 8
    )
    monkeypatch.setattr(telemetry, "_host_memory", lambda: next(readings))
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"

    reserved = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=8 * GIB)
    )
    assert reserved["status"] == "admitted"

    renewal = telemetry._handle_admission_request(_lease_request(reserved))
    assert renewal["status"] == "admitted"
    assert renewal["reason"] == "renewed_resized"
    assert renewal["lease"]["allowance_basis"] == "learned_profile_live_headroom"
    assert renewal["lease"]["allowance_bytes"] == 3_583 * MIB

    released = telemetry._handle_admission_request(
        _lease_request(renewal, op="release")
    )
    assert released["status"] == "released"
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
        return (
            TOTAL,
            30 * GIB if reads <= 7 else 29 * GIB if reads <= 11 else 24 * GIB,
        )

    monkeypatch.setattr(telemetry, "_host_memory", host_memory)
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"

    reserved = telemetry._handle_admission_request(_request(state_root))
    assert reserved["status"] == "admitted"
    assert reserved["lease"]["allowance_bytes"] == 13823 * MIB

    renewal = telemetry._handle_admission_request(_lease_request(reserved))

    assert renewal["status"] == "admitted"
    assert renewal["reason"] == "renewed_resized"
    assert renewal["lease"]["allowance_basis"] == "unprofiled_available_backed"
    assert renewal["lease"]["allowance_bytes"] == 7679 * MIB
    assert renewal["lease"]["capacity_bytes"] == 7679 * MIB + CONTROL
    assert renewal["lease"]["remaining_backed_capacity_bytes"] is not None
    assert renewal["capacity"]["previous_allowance_bytes"] == 13823 * MIB
    assert reads == 14
    leases = _ledger_leases(state_root)
    assert len(leases) == 1
    assert leases[0]["allowance_bytes"] == 7679 * MIB
    assert leases[0]["capacity_bytes"] == 7679 * MIB + CONTROL

    reservation = MemoryReservation.from_payload(renewal)
    token = uuid.uuid4().hex
    rc = telemetry._guarded_run(
        [sys.executable, "-c", "pass"],
        max_command_bytes=None,
        min_available_bytes=reservation.min_available_bytes,
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=_production_lease_request(renewal),
    )
    captured = capsys.readouterr()
    result = _guard_result(captured.err, token)
    assert rc == 0
    assert result["status"] == "ok"
    assert result["max_command_bytes"] is None

    finalized = _finalize_guarded_result(
        helper_exit_code=rc,
        stdout=captured.out,
        stderr=captured.err,
        token=token,
        reservation=reservation,
        telemetry=None,
        platform_name="test target",
    )
    assert finalized.memory_reservation is not None
    assert finalized.memory_reservation.allowance_basis == (
        "unprofiled_available_backed"
    )
    assert finalized.memory_reservation.allowance_bytes == 7679 * MIB
    assert finalized.memory_reservation.remaining_backed_capacity_bytes is not None
    assert finalized.memory_reservation.live_backed_allowance_bytes is None
    assert finalized.memory_reservation.learned_allowance_live_backed is None
    assert _ledger_leases(state_root) == []


def test_learned_final_renewal_refreshes_live_backed_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    """A learned renewal must carry its current live-backed projection."""
    available = AVAILABLE_SAFE
    monkeypatch.setattr(
        telemetry, "_host_memory", lambda: (TOTAL, available)
    )
    monkeypatch.setattr(telemetry, "_control_overhead_budget_bytes", lambda: CONTROL)
    state_root = tmp_path / "state"

    reserved = telemetry._handle_admission_request(
        _request(state_root, predicted_rss_bytes=8 * GIB)
    )
    assert reserved["status"] == "admitted"
    original_live_backed = reserved["lease"]["live_backed_allowance_bytes"]

    available = AVAILABLE_UNSAFE
    renewed = telemetry._handle_admission_request(_lease_request(reserved))
    assert renewed["status"] == "admitted"
    lease = renewed["lease"]
    assert lease["allowance_basis"] == "learned_profile_live_headroom"
    assert lease["allowance_bytes"] < reserved["lease"]["allowance_bytes"]
    assert original_live_backed > lease["allowance_bytes"]

    reservation = MemoryReservation.from_payload(renewed)
    token = uuid.uuid4().hex
    rc = telemetry._guarded_run(
        [sys.executable, "-c", "pass"],
        max_command_bytes=None,
        min_available_bytes=reservation.min_available_bytes,
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=_production_lease_request(renewed),
    )
    captured = capsys.readouterr()
    assert rc == 0
    finalized = _finalize_guarded_result(
        helper_exit_code=rc,
        stdout=captured.out,
        stderr=captured.err,
        token=token,
        reservation=reservation,
        telemetry=None,
        platform_name="test target",
    )
    assert finalized.memory_reservation is not None
    assert finalized.memory_reservation.live_backed_allowance_bytes == lease[
        "live_backed_allowance_bytes"
    ]
    assert _ledger_leases(state_root) == []


def test_learned_helper_claim_clips_without_rss_killing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, AVAILABLE_SAFE))
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

    # A controller reservation made at 60 GiB available is larger than the
    # 20-GiB claim-time headroom, but 20 GiB still safely exceeds the 16-GiB
    # host reserve. The target must clip the inferred lease and start argv.
    monkeypatch.setattr(telemetry, "_host_memory", lambda: (TOTAL, AVAILABLE_UNSAFE))

    rc = telemetry._guarded_run(
        [
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({str(sentinel)!r}).write_text('started')",
        ],
        max_command_bytes=None,
        min_available_bytes=int(lease["min_available_bytes"]),
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=_production_lease_request(renewed),
    )

    captured = capsys.readouterr()
    result = _guard_result(captured.err, token)
    assert rc == 0
    assert result["status"] == "ok"
    assert result["command_started"] is True
    assert result["max_command_bytes"] is None
    assert result["command_limit_enforced"] is False
    assert sentinel.exists()
    assert _ledger_leases(state_root) == []


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
        max_command_bytes=None,
        min_available_bytes=reservation.min_available_bytes,
        token=token,
        detailed=False,
        telemetry=False,
        lease_request=_production_lease_request(reserved),
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
        explicit_limit_bytes=2 * MIB,
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
        lease_request=_production_lease_request(reserved),
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
    assert too_large["status"] == "admitted"
    assert too_large["capacity"]["allowance_basis"] == (
        "learned_profile_plus_25_percent"
    )
    assert too_large["lease"]["allowance_bytes"] == 16 * GIB
    assert too_large["capacity"]["learned_allowance_bytes"] == 17_920 * MIB
    assert too_large["capacity"]["learned_allowance_live_backed"] is True


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
    assert result.memory_guard["max_command_bytes"] is None
    assert result.memory_guard["command_limit_enforced"] is False
    assert result.memory_guard["enforced_command_limit_bytes"] is None
    assert _ledger_leases(tmp_path / "state") == []
