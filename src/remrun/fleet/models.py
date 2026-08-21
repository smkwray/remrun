"""Fleet data model. No warm-model state exists here by design (Invariant 0)."""
from __future__ import annotations

import math
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
    # Launch was authorized but completion cannot be proved. This is deliberately
    # non-dispatchable and non-prunable, and it remains inside the active
    # idempotency fence so an identical submission cannot duplicate effects.
    "completion_unknown",
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
    Duration fields are rebuilt from the bounded SQLite observation window; a
    missing or unready duration profile means placement must not claim an ETA.
    """
    fixed_load_s: float | None = None
    var_per_unit_s: float | None = None
    peak_rss_mb: float | None = None
    peak_vram_mb: float | None = None
    n: int = 0
    duration_n: int = 0
    resource_n: int = 0
    min_units: float | None = None
    max_units: float | None = None
    normalized_rmse: float | None = None
    updated: str = ""


@dataclass(frozen=True)
class PlacedBatch:
    """A placement decision: which jobs go to which device, and the estimate."""
    device: str
    job_indices: list[int]
    estimated_finish_s: float | None
    reason: str = ""
    selection_basis: str = "estimated"
    estimate_reason: str | None = None

    def __post_init__(self) -> None:
        bases = {"forced", "sole_qualified", "cold_start", "exploration", "estimated"}
        reasons = {"uncalibrated", "model_unfit", "out_of_range", "backlog_unknown"}
        if self.selection_basis not in bases:
            raise ValueError(f"unknown selection_basis {self.selection_basis!r}")
        if self.estimated_finish_s is None:
            if self.estimate_reason not in reasons:
                raise ValueError("an unknown estimate requires a stable estimate_reason")
        else:
            if not math.isfinite(self.estimated_finish_s) or self.estimated_finish_s < 0:
                raise ValueError("estimated_finish_s must be finite and nonnegative")
            if self.estimate_reason is not None:
                raise ValueError("a numeric estimate may not carry estimate_reason")


@dataclass(frozen=True)
class PlacementResult:
    batches: list[PlacedBatch] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)   # device -> why skipped
    makespan_s: float | None = 0.0
    note: str = ""

    def __post_init__(self) -> None:
        if self.makespan_s is not None \
                and (not math.isfinite(self.makespan_s) or self.makespan_s < 0):
            raise ValueError("makespan_s must be null or finite and nonnegative")


@dataclass(frozen=True)
class DrainResultV1:
    """One stable final document for a dispatcher drain lifecycle."""

    status: str
    ran: int
    ok: int
    failed: int
    review: int
    queued: int
    active: int
    skipped: dict[str, str] = field(default_factory=dict)
    error: dict[str, str] | None = None
    schema: int = field(default=1, init=False)

    def __post_init__(self) -> None:
        statuses = {"drained", "stuck_unplaceable", "cancelled", "infrastructure_error"}
        if self.status not in statuses:
            raise ValueError(f"unknown drain status {self.status!r}")
        counters = (self.ran, self.ok, self.failed, self.review, self.queued, self.active)
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0
               for value in counters):
            raise ValueError("drain counters must be nonnegative integers")
        if self.status in {"cancelled", "infrastructure_error"}:
            if not isinstance(self.error, dict) or set(self.error) != {"kind", "message"}:
                raise ValueError(f"{self.status} requires a stable error object")
            if any(not isinstance(self.error[field], str) or not self.error[field]
                   for field in ("kind", "message")):
                raise ValueError(f"{self.status} error fields must be non-empty strings")
        elif self.error is not None:
            raise ValueError(f"{self.status} may not carry an error object")
        if self.status == "drained" and (self.queued or self.active):
            raise ValueError("drained requires an empty final queue")

    @property
    def exit_code(self) -> int:
        if self.status == "cancelled":
            return 130
        if self.status == "infrastructure_error":
            return 4
        if self.status == "stuck_unplaceable":
            return 2
        return 1 if self.failed or self.review else 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "status": self.status,
            "ran": self.ran,
            "ok": self.ok,
            "failed": self.failed,
            "review": self.review,
            "queued": self.queued,
            "active": self.active,
            "skipped": dict(self.skipped),
            "error": dict(self.error) if self.error is not None else None,
        }
