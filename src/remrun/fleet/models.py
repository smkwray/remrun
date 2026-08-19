"""Fleet data model. No warm-model state exists here by design (Invariant 0)."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


_CAPABILITY_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]*$")


def normalize_capabilities(raw, field_name: str) -> tuple[str, ...]:  # noqa: ANN001
    """Validate and canonicalize an opaque capability-token collection.

    Capability names are intentionally uninterpreted and case-sensitive. Rejecting a
    malformed declaration is safer than accidentally treating a string as an iterable
    of characters or a mapping as its keys. Sorting and de-duplicating gives queue and
    batch compatibility one stable set representation.
    """
    if raw is None:
        return ()
    if isinstance(raw, str) or not isinstance(raw, (list, tuple, set, frozenset)):
        raise ValueError(f"{field_name} must be a list of non-empty strings")
    values = tuple(raw)
    if any(
        not isinstance(value, str) or _CAPABILITY_TOKEN_RE.fullmatch(value) is None
        for value in values
    ):
        raise ValueError(
            f"{field_name} must be a list of capability tokens matching "
            "[A-Za-z0-9][A-Za-z0-9_.:/-]*"
        )
    return tuple(sorted(set(values)))


# Job lifecycle states (durable queue).
JOB_STATES = (
    "queued", "leased", "staging", "running", "fetching", "done",
    "failed_retryable", "failed_final",
    # A structured refusal the worker has already adjudicated. Terminal like
    # failed_final, but deliberately distinct: it is an answer awaiting a person,
    # not an error a different device or a later attempt could fix.
    "needs_review",
)


@dataclass(frozen=True)
class FleetTask:
    """One unit of work, independent of any project.

    ``prepared`` and ``resolved_spec`` carry the validated, frozen protocol
    records. ``task_name`` is the arbitrary configured name, not a core enum.
    ``requires`` contains opaque capability tokens derived from configuration,
    plus any additive caller restriction.
    """
    task_name: str
    text: str | None = None
    inputs: list[str] = field(default_factory=list)
    options: dict[str, Any] = field(default_factory=dict)
    force_device: str | None = None
    engine: str | None = None
    output_root: str | None = None         # explicit override; else adapter default
    idempotency_key: str = ""              # content+options hash, for safe retries
    # Keyword-only so the published positional constructor and __match_args__ stay unchanged.
    requires: tuple[str, ...] = field(default=(), kw_only=True)
    # Frozen protocol records. Configured work carries both; intrinsic commands
    # use the closed built-in command spec.
    prepared: dict[str, Any] | None = field(default=None, kw_only=True, repr=False)
    resolved_spec: dict[str, Any] | None = field(default=None, kw_only=True, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "requires", normalize_capabilities(self.requires, "task requires"),
        )


@dataclass(frozen=True)
class JobFeatures:
    """Cost-driving size features extracted from a task's payload."""
    input_bytes: int = 0
    file_count: int = 0
    text_chars: int = 0
    pages: int = 0
    pages_approx: bool = True
    prepared_units: float | None = None
    units_status: str = "unestimated"
    relative_uncertainty: float | None = None

    def units(self) -> float:
        """The frozen variable-cost unit count for a prepared job."""
        if self.prepared_units is not None:
            return float(self.prepared_units)
        return 0.0


@dataclass(frozen=True)
class DeviceSnapshot:
    """A live, best-effort view of one candidate device. Any field may be None
    when the probe could not measure it (callers degrade gracefully)."""
    name: str
    reachable: bool
    cpu_busy_pct: float | None = None
    ram_free_mb: float | None = None
    ram_total_mb: float | None = None
    vram_free_mb: float | None = None
    vram_total_mb: float | None = None
    active_jobs: int = 0
    max_jobs: int = 1
    # Free slots per configured resource pool (for example: {"gpu": 1, "cpu": 4}).
    # Adapters name the pool they need; no pool means no mutex.
    pool_free: dict[str, int] = field(default_factory=dict)
    # Per-engine live qualification: present, absent, or unknown.
    engine_status: dict[str, str] = field(default_factory=dict)
    detail: str = ""


@dataclass(frozen=True)
class FleetProfile:
    """Learned cost for one frozen task/adapter/device/bucket identity.

    ``fixed_load_s`` is the COLD model-load time (there is no warm variant —
    Invariant 0). ``var_per_unit_s`` is seconds per variable unit (see
    JobFeatures.units). ``peak_rss_mb`` / ``peak_vram_mb`` drive the fit check.
    EWMA-smoothed, bounded; a missing profile means "use the generic prior".
    """
    fixed_load_s: float | None = None
    var_per_unit_s: float | None = None
    peak_rss_mb: float | None = None
    peak_vram_mb: float | None = None
    n: int = 0
    updated: str = ""


@dataclass(frozen=True)
class PlacedBatch:
    """A placement decision: which jobs go to which device, and the estimate."""
    device: str
    job_indices: list[int]
    estimated_finish_s: float
    reason: str = ""


@dataclass(frozen=True)
class PlacementResult:
    batches: list[PlacedBatch] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)   # device -> why skipped
    makespan_s: float = 0.0
    note: str = ""
