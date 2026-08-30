"""Generic device-level GPU memory topology resolution.

The declaration is intentionally about hardware topology only.  It does not
name a workload, engine, model, or device family.  Probe parsers keep their
raw observations and use this module to resolve the normalized topology used
by consumers.
"""
from __future__ import annotations

from typing import Literal

GpuMemoryTopology = Literal["auto", "discrete", "unified"]
ObservedGpuTopology = Literal["auto", "discrete", "unified", "none", "unknown"]

GPU_MEMORY_TOPOLOGIES = frozenset({"auto", "discrete", "unified"})


class GpuTopologyError(ValueError):
    """A GPU memory topology declaration or observation is contradictory."""


def parse_gpu_memory_topology(
    value: object,
    *,
    field_name: str = "gpu_memory_topology",
) -> GpuMemoryTopology:
    """Validate one optional device declaration; an unset value means auto."""
    if value is None:
        return "auto"
    if not isinstance(value, str) or value not in GPU_MEMORY_TOPOLOGIES:
        values = ", ".join(sorted(GPU_MEMORY_TOPOLOGIES))
        raise GpuTopologyError(f"{field_name} must be one of {values}")
    return value  # type: ignore[return-value]


def resolve_gpu_memory_topology(
    declared: object,
    observed: object,
    *,
    context: str = "device",
    observed_authoritative: bool = False,
) -> ObservedGpuTopology:
    """Resolve a declaration against raw probe evidence, failing closed.

    ``unknown`` means the probe did not establish separate or shared memory;
    it is never silently treated as unified. Numeric NVIDIA counters establish
    a discrete telemetry shape for ``auto`` placement, but are not themselves
    authoritative physical-topology evidence. An explicit declaration may
    resolve that shape while it may not overwrite authoritative evidence.
    """
    configured = parse_gpu_memory_topology(declared)
    if observed not in {"auto", "discrete", "unified", "none", "unknown"}:
        raise GpuTopologyError(f"{context} reported an invalid GPU topology {observed!r}")
    raw = observed  # type: ignore[assignment]
    if (
        configured != "auto"
        and raw in {"discrete", "unified"}
        and configured != raw
        and observed_authoritative
    ):
        raise GpuTopologyError(
            f"{context} declares GPU memory topology {configured!r}, "
            f"but the probe reports {raw!r}"
        )
    if configured != "auto" and raw == "none":
        raise GpuTopologyError(
            f"{context} declares GPU memory topology {configured!r}, "
            "but the probe reports no GPU"
        )
    return configured if configured != "auto" else raw
