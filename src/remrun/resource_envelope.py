"""Typed, dependency-free resource policy and launch-budget calculations.

This module deliberately contains no probing or command-launch behavior.  It
turns an explicit versioned device policy plus already-normalized measurements
into a conservative launch envelope.  Missing and unusable inputs stay
unavailable; they never become plausible-looking zeroes.
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, TypeAlias

MIB = 1024 * 1024

MetricStatus: TypeAlias = Literal[
    "measured",
    "configured",
    "unavailable",
    "malformed",
    "timeout",
    "not_applicable",
]
PolicyMode: TypeAlias = Literal["interactive", "unattended"]
OfferStatus: TypeAlias = Literal["usable", "zero", "unavailable"]

_METRIC_STATUSES = frozenset(
    {"measured", "configured", "unavailable", "malformed", "timeout", "not_applicable"}
)
_VALUE_STATUSES = frozenset({"measured", "configured"})
_POLICY_KEYS = frozenset(
    {
        "schema",
        "mode",
        "probe_timeout_sec",
        "cpu_reserve_cores",
        "cpu_max_fraction",
        "ram_reserve_mib",
        "ram_max_fraction",
        "gpu_busy_ceiling_pct",
        "vram_reserve_mib",
        "vram_max_fraction",
        "allow_static_fallback",
    }
)


class ResourcePolicyError(ValueError):
    """A present resource policy is not valid version-1 policy data."""


class ResourceEnvelopeError(ValueError):
    """A purportedly usable measurement is outside its physical range."""


@dataclass(frozen=True)
class Metric:
    """One normalized numeric measurement and its provenance.

    ``measured`` and ``configured`` metrics carry a finite, non-negative value.
    Every failure/not-applicable state carries ``None``.  Thus a genuine numeric
    zero remains distinguishable from an unknown value.
    """

    value: int | float | None
    status: MetricStatus
    source: str
    confidence: str

    def __post_init__(self) -> None:
        if self.status not in _METRIC_STATUSES:
            raise ResourceEnvelopeError(f"unknown metric status: {self.status!r}")
        if not self.source:
            raise ResourceEnvelopeError("metric source must be explicit")
        if not self.confidence:
            raise ResourceEnvelopeError("metric confidence must be explicit")
        if self.status in _VALUE_STATUSES:
            if not _is_number(self.value):
                raise ResourceEnvelopeError(
                    f"{self.status} metric requires a finite numeric value"
                )
            assert self.value is not None
            if self.value < 0:
                raise ResourceEnvelopeError(f"{self.status} metric cannot be negative")
        elif self.value is not None:
            raise ResourceEnvelopeError(
                f"{self.status} metric must use None rather than a plausible value"
            )


@dataclass(frozen=True)
class MissingResourcePolicy:
    """Explicit result for a device with no ``resource_policy`` table."""

    status: Literal["missing"] = "missing"


@dataclass(frozen=True)
class DeviceResourcePolicy:
    """Validated device policy, normalized to byte units."""

    schema: Literal[1]
    mode: PolicyMode
    probe_timeout_sec: float
    cpu_reserve_cores: float
    cpu_max_fraction: float
    ram_reserve_bytes: int
    ram_max_fraction: float
    gpu_busy_ceiling_pct: float
    vram_reserve_bytes: int
    vram_max_fraction: float
    allow_static_fallback: bool
    status: Literal["valid"] = "valid"


ResourcePolicy: TypeAlias = DeviceResourcePolicy | MissingResourcePolicy


@dataclass(frozen=True)
class OfferedResource:
    """A launch budget, preserving unavailable separately from genuine zero."""

    value: int | None
    status: OfferStatus
    reason: str = ""

    def __post_init__(self) -> None:
        if self.status == "unavailable":
            if self.value is not None:
                raise ResourceEnvelopeError("unavailable offer must use None")
            if not self.reason:
                raise ResourceEnvelopeError("unavailable offer must explain why")
            return
        if self.value is None or isinstance(self.value, bool) or self.value < 0:
            raise ResourceEnvelopeError(f"{self.status} offer requires a non-negative integer")
        if not isinstance(self.value, int):
            raise ResourceEnvelopeError(f"{self.status} offer requires whole resource units")
        if self.status == "zero" and self.value != 0:
            raise ResourceEnvelopeError("zero offer must carry numeric zero")
        if self.status == "usable" and self.value == 0:
            raise ResourceEnvelopeError("numeric zero must use status='zero'")


def parse_device_resource_policy(raw: Mapping[str, object] | None) -> ResourcePolicy:
    """Parse one optional ``[devices.NAME.resource_policy]`` table.

    No values are inferred from device role, transport, or remoteness.  A missing
    table returns an explicit missing object; a present malformed table raises.
    """

    if raw is None:
        return MissingResourcePolicy()
    if not isinstance(raw, Mapping):
        raise ResourcePolicyError("resource_policy must be a table")

    unknown = sorted(set(raw) - _POLICY_KEYS)
    missing = sorted(_POLICY_KEYS - set(raw))
    if unknown:
        raise ResourcePolicyError(f"resource_policy has unknown keys: {', '.join(unknown)}")
    if missing:
        raise ResourcePolicyError(f"resource_policy is missing keys: {', '.join(missing)}")

    schema = _integer(raw["schema"], "schema", minimum=1)
    if schema != 1:
        raise ResourcePolicyError(f"resource_policy.schema must be 1, got {schema}")

    mode_value = raw["mode"]
    if mode_value not in ("interactive", "unattended"):
        raise ResourcePolicyError(
            "resource_policy.mode must be exactly 'interactive' or 'unattended'"
        )
    mode: PolicyMode = mode_value

    timeout = _number(raw["probe_timeout_sec"], "probe_timeout_sec", minimum=0, strict=True)
    cpu_reserve = _number(raw["cpu_reserve_cores"], "cpu_reserve_cores", minimum=0)
    cpu_fraction = _fraction(raw["cpu_max_fraction"], "cpu_max_fraction")
    ram_reserve_mib = _integer(raw["ram_reserve_mib"], "ram_reserve_mib", minimum=0)
    ram_fraction = _fraction(raw["ram_max_fraction"], "ram_max_fraction")
    gpu_ceiling = _number(
        raw["gpu_busy_ceiling_pct"], "gpu_busy_ceiling_pct", minimum=0, maximum=100
    )
    vram_reserve_mib = _integer(raw["vram_reserve_mib"], "vram_reserve_mib", minimum=0)
    vram_fraction = _fraction(raw["vram_max_fraction"], "vram_max_fraction")

    allow_fallback = raw["allow_static_fallback"]
    if not isinstance(allow_fallback, bool):
        raise ResourcePolicyError("resource_policy.allow_static_fallback must be a boolean")

    return DeviceResourcePolicy(
        schema=1,
        mode=mode,
        probe_timeout_sec=timeout,
        cpu_reserve_cores=cpu_reserve,
        cpu_max_fraction=cpu_fraction,
        ram_reserve_bytes=ram_reserve_mib * MIB,
        ram_max_fraction=ram_fraction,
        gpu_busy_ceiling_pct=gpu_ceiling,
        vram_reserve_bytes=vram_reserve_mib * MIB,
        vram_max_fraction=vram_fraction,
        allow_static_fallback=allow_fallback,
    )


def offer_cpu(
    effective_cores: Metric,
    busy_pct: Metric,
    policy: ResourcePolicy,
) -> OfferedResource:
    """Compute the exact version-1 CPU launch budget in logical cores."""

    valid = _valid_policy(policy)
    if valid is None:
        return _unavailable("policy_missing")
    effective = _usable_metric(effective_cores, "effective_cpu", valid)
    if isinstance(effective, OfferedResource):
        return effective
    busy = _usable_metric(busy_pct, "cpu_busy", valid)
    if isinstance(busy, OfferedResource):
        return busy
    if busy > 100:
        raise ResourceEnvelopeError("cpu_busy must be between 0 and 100 percent")

    live_idle = effective * (1 - busy / 100)
    policy_cap = effective * valid.cpu_max_fraction
    offered = math.floor(max(0, min(policy_cap, live_idle - valid.cpu_reserve_cores)))
    return _offer(int(offered))


def offer_ram(
    total_bytes: Metric,
    available_bytes: Metric,
    policy: ResourcePolicy,
) -> OfferedResource:
    """Compute the exact version-1 RAM launch budget in whole bytes."""

    valid = _valid_policy(policy)
    if valid is None:
        return _unavailable("policy_missing")
    total = _usable_metric(total_bytes, "ram_total", valid)
    if isinstance(total, OfferedResource):
        return total
    available = _usable_metric(available_bytes, "ram_available", valid)
    if isinstance(available, OfferedResource):
        return available
    if available > total:
        raise ResourceEnvelopeError("ram_available cannot exceed ram_total")

    offered = max(
        0,
        min(
            available - valid.ram_reserve_bytes,
            total * valid.ram_max_fraction,
        ),
    )
    return _offer(math.floor(offered))


def offer_gpu_vram(
    kind: Literal["discrete", "unified"],
    total_bytes: Metric | None,
    free_bytes: Metric | None,
    util_pct: Metric | None,
    policy: ResourcePolicy,
) -> OfferedResource:
    """Compute per-device discrete VRAM; never invent VRAM for unified memory."""

    valid = _valid_policy(policy)
    if valid is None:
        return _unavailable("policy_missing")
    if kind == "unified":
        return _unavailable("unified_memory")
    if kind != "discrete":
        raise ResourceEnvelopeError(f"unknown GPU memory kind: {kind!r}")
    if total_bytes is None or free_bytes is None or util_pct is None:
        return _unavailable("gpu_metrics_unavailable")

    utilization = _usable_metric(util_pct, "gpu_util", valid)
    if isinstance(utilization, OfferedResource):
        return utilization
    if utilization > 100:
        raise ResourceEnvelopeError("gpu_util must be between 0 and 100 percent")
    if utilization > valid.gpu_busy_ceiling_pct:
        return _unavailable("gpu_busy")

    total = _usable_metric(total_bytes, "vram_total", valid)
    if isinstance(total, OfferedResource):
        return total
    free = _usable_metric(free_bytes, "vram_free", valid)
    if isinstance(free, OfferedResource):
        return free
    if free > total:
        raise ResourceEnvelopeError("vram_free cannot exceed vram_total")

    offered = max(
        0,
        min(
            free - valid.vram_reserve_bytes,
            total * valid.vram_max_fraction,
        ),
    )
    return _offer(math.floor(offered))


def _valid_policy(policy: ResourcePolicy) -> DeviceResourcePolicy | None:
    if isinstance(policy, MissingResourcePolicy):
        return None
    return policy


def _usable_metric(
    metric: Metric,
    label: str,
    policy: DeviceResourcePolicy,
) -> float | OfferedResource:
    if metric.value is None:
        return _unavailable(f"{label}_{metric.status}")
    if metric.status == "configured" and not policy.allow_static_fallback:
        return _unavailable(f"{label}_static_fallback_disabled")
    return float(metric.value)


def _offer(value: int) -> OfferedResource:
    return OfferedResource(value=value, status="zero" if value == 0 else "usable")


def _unavailable(reason: str) -> OfferedResource:
    return OfferedResource(value=None, status="unavailable", reason=reason)


def _is_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _number(
    value: object,
    name: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    strict: bool = False,
) -> float:
    if not _is_number(value):
        raise ResourcePolicyError(f"resource_policy.{name} must be a finite number")
    number = float(value)
    if minimum is not None and (number <= minimum if strict else number < minimum):
        qualifier = "greater than" if strict else "at least"
        raise ResourcePolicyError(f"resource_policy.{name} must be {qualifier} {minimum}")
    if maximum is not None and number > maximum:
        raise ResourcePolicyError(f"resource_policy.{name} must be at most {maximum}")
    return number


def _integer(value: object, name: str, *, minimum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ResourcePolicyError(f"resource_policy.{name} must be an integer")
    if value < minimum:
        raise ResourcePolicyError(f"resource_policy.{name} must be at least {minimum}")
    return value


def _fraction(value: object, name: str) -> float:
    return _number(value, name, minimum=0, maximum=1)
