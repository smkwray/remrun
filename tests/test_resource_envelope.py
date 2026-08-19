from __future__ import annotations

import math

import pytest

from remrun.resource_envelope import (
    MIB,
    DeviceResourcePolicy,
    Metric,
    MissingResourcePolicy,
    OfferedResource,
    ResourceEnvelopeError,
    ResourcePolicyError,
    offer_cpu,
    offer_gpu_vram,
    offer_ram,
    parse_device_resource_policy,
)


def _policy_raw(**overrides: object) -> dict[str, object]:
    raw: dict[str, object] = {
        "schema": 1,
        "mode": "unattended",
        "probe_timeout_sec": 5,
        "cpu_reserve_cores": 1,
        "cpu_max_fraction": 1.0,
        "ram_reserve_mib": 4096,
        "ram_max_fraction": 0.9,
        "gpu_busy_ceiling_pct": 100,
        "vram_reserve_mib": 1024,
        "vram_max_fraction": 0.95,
        "allow_static_fallback": False,
    }
    raw.update(overrides)
    return raw


def _policy(**overrides: object) -> DeviceResourcePolicy:
    parsed = parse_device_resource_policy(_policy_raw(**overrides))
    assert isinstance(parsed, DeviceResourcePolicy)
    return parsed


def _measured(value: int | float) -> Metric:
    return Metric(value, "measured", "test-probe", "exact")


def _unavailable(status: str = "unavailable") -> Metric:
    return Metric(None, status, "test-probe", "unknown")  # type: ignore[arg-type]


def test_missing_policy_is_explicit_and_offers_nothing() -> None:
    policy = parse_device_resource_policy(None)

    assert policy == MissingResourcePolicy(status="missing")
    assert offer_cpu(_measured(20), _measured(0), policy) == OfferedResource(
        None, "unavailable", "policy_missing"
    )


def test_present_policy_is_typed_and_normalized_to_bytes() -> None:
    policy = _policy(
        mode="interactive",
        cpu_reserve_cores=2.5,
        ram_reserve_mib=8192,
        vram_reserve_mib=2048,
        allow_static_fallback=True,
    )

    assert policy == DeviceResourcePolicy(
        schema=1,
        mode="interactive",
        probe_timeout_sec=5.0,
        cpu_reserve_cores=2.5,
        cpu_max_fraction=1.0,
        ram_reserve_bytes=8192 * MIB,
        ram_max_fraction=0.9,
        gpu_busy_ceiling_pct=100.0,
        vram_reserve_bytes=2048 * MIB,
        vram_max_fraction=0.95,
        allow_static_fallback=True,
    )


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"schema": 2}, "schema must be 1"),
        ({"schema": True}, "schema must be an integer"),
        ({"mode": "remote"}, "interactive.*unattended"),
        ({"probe_timeout_sec": 0}, "greater than"),
        ({"probe_timeout_sec": math.inf}, "finite number"),
        ({"cpu_reserve_cores": -1}, "at least"),
        ({"cpu_max_fraction": -0.01}, "at least"),
        ({"cpu_max_fraction": 1.01}, "at most"),
        ({"ram_reserve_mib": 1.5}, "must be an integer"),
        ({"ram_max_fraction": math.nan}, "finite number"),
        ({"gpu_busy_ceiling_pct": -1}, "at least"),
        ({"gpu_busy_ceiling_pct": 101}, "at most"),
        ({"vram_reserve_mib": -1}, "at least"),
        ({"vram_max_fraction": 2}, "at most"),
        ({"allow_static_fallback": 0}, "must be a boolean"),
    ],
)
def test_invalid_policy_values_raise(change: dict[str, object], message: str) -> None:
    with pytest.raises(ResourcePolicyError, match=message):
        parse_device_resource_policy(_policy_raw(**change))


def test_present_empty_policy_is_invalid_not_missing() -> None:
    with pytest.raises(ResourcePolicyError, match="missing keys"):
        parse_device_resource_policy({})


def test_mode_is_required_and_never_inferred_from_device_role() -> None:
    raw = _policy_raw()
    raw.pop("mode")

    with pytest.raises(ResourcePolicyError, match="mode"):
        parse_device_resource_policy(raw)


def test_unknown_policy_key_is_rejected() -> None:
    raw = _policy_raw(role="runner")

    with pytest.raises(ResourcePolicyError, match="unknown keys: role"):
        parse_device_resource_policy(raw)


@pytest.mark.parametrize(
    "status",
    ["measured", "configured", "unavailable", "malformed", "timeout", "not_applicable"],
)
def test_metric_accepts_each_explicit_status(status: str) -> None:
    value = 0 if status in {"measured", "configured"} else None

    metric = Metric(value, status, "probe", "exact")  # type: ignore[arg-type]

    assert metric.value == value


@pytest.mark.parametrize(
    ("metric", "message"),
    [
        (lambda: Metric(None, "measured", "probe", "exact"), "requires"),
        (lambda: Metric(0, "unavailable", "probe", "unknown"), "must use None"),
        (lambda: Metric(-1, "measured", "probe", "exact"), "negative"),
        (lambda: Metric(math.inf, "measured", "probe", "exact"), "finite"),
        (lambda: Metric(1, "bogus", "probe", "exact"), "unknown metric status"),
        (lambda: Metric(1, "measured", "", "exact"), "source"),
        (lambda: Metric(1, "measured", "probe", ""), "confidence"),
    ],
)
def test_metric_rejects_ambiguous_or_malformed_values(metric, message: str) -> None:
    with pytest.raises(ResourceEnvelopeError, match=message):
        metric()


def test_cpu_formula_matches_design_example() -> None:
    result = offer_cpu(_measured(20), _measured(7.1), _policy())

    assert result == OfferedResource(17, "usable")


def test_cpu_formula_applies_policy_cap_before_flooring() -> None:
    result = offer_cpu(
        _measured(10),
        _measured(0),
        _policy(cpu_reserve_cores=1, cpu_max_fraction=0.55),
    )

    assert result == OfferedResource(5, "usable")


def test_cpu_real_zero_is_not_unknown() -> None:
    result = offer_cpu(_measured(8), _measured(100), _policy())

    assert result == OfferedResource(0, "zero")


def test_cpu_unknown_stays_unknown() -> None:
    result = offer_cpu(_measured(8), _unavailable("timeout"), _policy())

    assert result == OfferedResource(None, "unavailable", "cpu_busy_timeout")


def test_configured_metric_requires_explicit_static_fallback() -> None:
    configured = Metric(8, "configured", "devices.toml", "configured")

    assert offer_cpu(configured, _measured(0), _policy()) == OfferedResource(
        None, "unavailable", "effective_cpu_static_fallback_disabled"
    )
    assert offer_cpu(
        configured,
        _measured(0),
        _policy(allow_static_fallback=True),
    ).status == "usable"


def test_out_of_range_cpu_utilization_raises() -> None:
    with pytest.raises(ResourceEnvelopeError, match="between 0 and 100"):
        offer_cpu(_measured(8), _measured(101), _policy())


def test_ram_formula_uses_tighter_live_headroom() -> None:
    result = offer_ram(
        _measured(64 * 1024**3),
        _measured(48 * 1024**3),
        _policy(ram_reserve_mib=4096, ram_max_fraction=0.9),
    )

    assert result == OfferedResource(44 * 1024**3, "usable")


def test_ram_formula_uses_policy_cap_and_whole_bytes() -> None:
    result = offer_ram(
        _measured(101),
        _measured(101),
        _policy(ram_reserve_mib=0, ram_max_fraction=0.5),
    )

    assert result == OfferedResource(50, "usable")


def test_ram_real_zero_is_not_unknown() -> None:
    result = offer_ram(
        _measured(16 * 1024**3),
        _measured(1 * MIB),
        _policy(ram_reserve_mib=2),
    )

    assert result == OfferedResource(0, "zero")


def test_ram_unknown_stays_unknown() -> None:
    result = offer_ram(_measured(1024), _unavailable("malformed"), _policy())

    assert result == OfferedResource(None, "unavailable", "ram_available_malformed")


def test_ram_available_cannot_exceed_total() -> None:
    with pytest.raises(ResourceEnvelopeError, match="cannot exceed"):
        offer_ram(_measured(100), _measured(101), _policy(ram_reserve_mib=0))


def test_discrete_gpu_formula_matches_design() -> None:
    result = offer_gpu_vram(
        "discrete",
        _measured(12 * 1024**3),
        _measured(10 * 1024**3),
        _measured(3),
        _policy(vram_reserve_mib=1024, vram_max_fraction=0.95),
    )

    assert result == OfferedResource(9 * 1024**3, "usable")


def test_gpu_above_busy_ceiling_is_unavailable() -> None:
    result = offer_gpu_vram(
        "discrete",
        _measured(12 * 1024**3),
        _measured(10 * 1024**3),
        _measured(15.1),
        _policy(gpu_busy_ceiling_pct=15),
    )

    assert result == OfferedResource(None, "unavailable", "gpu_busy")


def test_gpu_at_busy_ceiling_remains_eligible() -> None:
    result = offer_gpu_vram(
        "discrete",
        _measured(1024),
        _measured(1024),
        _measured(15),
        _policy(
            gpu_busy_ceiling_pct=15,
            vram_reserve_mib=0,
            vram_max_fraction=1,
        ),
    )

    assert result == OfferedResource(1024, "usable")


def test_gpu_real_zero_is_not_unknown() -> None:
    result = offer_gpu_vram(
        "discrete",
        _measured(1024),
        _measured(0),
        _measured(0),
        _policy(vram_reserve_mib=0),
    )

    assert result == OfferedResource(0, "zero")


def test_unified_memory_never_receives_fabricated_vram() -> None:
    result = offer_gpu_vram(
        "unified",
        _measured(64 * 1024**3),
        _measured(48 * 1024**3),
        _measured(0),
        _policy(),
    )

    assert result == OfferedResource(None, "unavailable", "unified_memory")


def test_discrete_gpu_unknown_stays_unknown() -> None:
    result = offer_gpu_vram(
        "discrete",
        _measured(1024),
        _unavailable("timeout"),
        _measured(0),
        _policy(vram_reserve_mib=0),
    )

    assert result == OfferedResource(None, "unavailable", "vram_free_timeout")


def test_offered_resource_enforces_zero_and_unknown_semantics() -> None:
    with pytest.raises(ResourceEnvelopeError, match="status='zero'"):
        OfferedResource(0, "usable")
    with pytest.raises(ResourceEnvelopeError, match="must use None"):
        OfferedResource(1, "unavailable", "probe_failed")
