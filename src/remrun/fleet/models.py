"""Fleet data model. No warm-model state exists here by design (Invariant 0)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Task types the fleet understands. "cmd" is the generic project-less escape hatch.
TASK_TYPES = ("tts", "ocr", "cmd")

# Job lifecycle states (durable queue).
JOB_STATES = (
    "queued", "leased", "staging", "running", "fetching", "done",
    "failed_retryable", "failed_final",
)


@dataclass(frozen=True)
class FleetTask:
    """One unit of work, independent of any project.

    Exactly one of ``text`` or ``inputs`` is the payload source:
      * ``text`` — inline text (e.g. a TTS clipboard string).
      * ``inputs`` — local file paths (a folder is expanded to files by the caller).
    ``force_device`` pins placement to a configured runner; ``engine`` optionally
    pins the engine. ``options`` holds task knobs (for example speed, voice, or
    profile) and feeds the cost-profile ``option_bucket``.
    """
    task_type: str
    text: str | None = None
    inputs: list[str] = field(default_factory=list)
    options: dict[str, Any] = field(default_factory=dict)
    force_device: str | None = None
    engine: str | None = None
    output_root: str | None = None         # explicit override; else adapter default
    idempotency_key: str = ""              # content+options hash, for safe retries


@dataclass(frozen=True)
class JobFeatures:
    """Cost-driving size features extracted from a task's payload."""
    input_bytes: int = 0
    file_count: int = 0
    text_chars: int = 0
    pages: int = 0                          # OCR: approximate page/image count
    pages_approx: bool = True               # True when pages is a controller-side estimate

    def units(self, task_type: str) -> float:
        """The variable-cost unit count for ``task_type`` (kchars for tts, pages
        for ocr, input-MB for cmd)."""
        if task_type == "tts":
            return self.text_chars / 1000.0
        if task_type == "ocr":
            return float(self.pages)
        return self.input_bytes / (1024.0 * 1024.0)


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
    # Engines whose model/script were confirmed present (capability probe).
    engines_available: frozenset[str] = frozenset()
    detail: str = ""


@dataclass(frozen=True)
class FleetProfile:
    """Learned cost for one (task_type, engine, device, option_bucket).

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
