"""Per-(project, command, device) resource + timing profiles.

Records a rolling EWMA, from the telemetry remrun already captures, of each job's
typical peak RAM, CPU%, **remote exec time**, **full remrun round-trip time**, and
the **overhead** (trip − exec) — keyed by the device it ran on. A special ``LOCAL``
device row holds a locally-measured baseline (``remrun bench``).

Two consumers, both advisory (a missing profile just means default behavior):
  * scheduler placement inside ``--auto`` (RAM-headroom, trivial-skip) via ``predict_job``;
  * the offload decision (local vs. remote) via ``recommend_offload``.

One small bounded JSON file; never synced (local regenerable state).
"""
from __future__ import annotations

import json
import math
import os
import re
import shlex
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

PROFILE_FILE = "profiles.json"
DEFAULT_ALPHA = 0.4          # EWMA weight on the newest observation
DEFAULT_MAX_ENTRIES = 64     # cap total (project, command, device) rows; evict oldest
LOCAL_DEVICE = "LOCAL"       # pseudo-device key for the locally-measured baseline
WORKLOAD_PROFILES_KEY = "__remrun_workload_profiles_v1__"
WORKLOAD_PROFILE_VERSION = 1
MAX_WORKLOAD_METADATA_BYTES = 16 * 1024
_RECEIPT_STATUSES = frozenset({"applied", "fallback", "no_op", "blocked"})
_EVALUATIONS = frozenset({"baseline", "trial", "accepted", "fallback"})
_LEGACY_PROFILE_FIELDS = frozenset(
    {
        "n",
        "rss_mb",
        "rss_high_mb",
        "cpu_pct",
        "dur_s",
        "exec_s",
        "trip_s",
        "overhead_s",
        "updated",
    }
)
_LEGACY_NUMERIC_FIELDS = _LEGACY_PROFILE_FIELDS - {"n", "updated"}

_SCRIPT_EXTS = (".py", ".r", ".jl", ".sh", ".do", ".rb", ".pl", ".js", ".ts")
_WORKTREE_MARKERS = (
    (".worktrees",),
    (".claude", "worktrees"),
    (".delegate-worktrees",),
)
_PYTHON_NAME = re.compile(r"python(?:\d+(?:\.\d+)*)?(?:\.exe)?$", re.IGNORECASE)
_SHELL_META = re.compile(r"[|&;<>()\n]")


def profile_project_id(project_id: str) -> str:
    """Return the stable profile namespace for a project checkout.

    A nested agent worktree is a separate reconciliation tree, but its measured job
    costs belong to the same logical repository.  Only the profile namespace is
    collapsed; run IDs, locks, transfer paths, and completion fences keep the exact
    project identity.
    """
    parts = project_id.replace("\\", "/").split("/")
    for marker in _WORKTREE_MARKERS:
        width = len(marker)
        for index in range(1, len(parts) - width + 1):
            if tuple(parts[index : index + width]) == marker:
                return "/".join(parts[:index])
    return project_id


def _profile_tokens(command: list[str]) -> list[str]:
    """Conservatively unwrap common launchers without interpreting shell code."""
    tokens = [token for token in command if token]
    for _depth in range(3):
        if not tokens:
            return tokens
        lead = Path(tokens[0]).name.lower()
        if lead in {"sh", "bash", "zsh"} and len(tokens) == 3 and tokens[1] in {
            "-c",
            "-lc",
        }:
            payload = tokens[2]
            if _SHELL_META.search(payload):
                break
            try:
                parsed = shlex.split(payload)
            except ValueError:
                break
            if not parsed:
                break
            tokens = parsed
            continue
        if lead in {"uv", "uv.exe"} and len(tokens) >= 3 and tokens[1] == "run":
            rest = tokens[2:]
            if rest and rest[0] == "--":
                rest = rest[1:]
            # Unknown uv-run options may consume a following value.  Keep the outer
            # key rather than risk assigning the run to the wrong executable.
            if rest and not rest[0].startswith("-"):
                tokens = rest
                continue
        break
    return tokens


def _pytest_xdist_value(tokens: list[str]) -> str:
    for index, token in enumerate(tokens):
        if token in {"-n", "--numprocesses"}:
            if index + 1 < len(tokens) and tokens[index + 1]:
                return tokens[index + 1].lower()
            return "missing"
        if token.startswith("--numprocesses="):
            return token.split("=", 1)[1].lower() or "missing"
        if token.startswith("-n") and len(token) > 2:
            return token[2:].lower()
    return "default"


def _python_module(tokens: list[str]) -> str | None:
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token == "-m":
            return tokens[index + 1] if index + 1 < len(tokens) else None
        if token in {"-c", "--"} or not token.startswith("-"):
            return None
        if token in {"-W", "-X"}:
            index += 2
        else:
            index += 1
    return None


def command_key(command: list[str]) -> str:
    """Coarse signature that groups repeated invocations of the same job.

    Interpreter basename + first script-like arg basename, with paths and
    varying args/configs stripped: ["python", "-B", "do/run.py", "cfg.yaml"]
    -> "python:run.py". Falls back to the first non-flag arg, else the leading
    token. Intentionally fuzzy — same script over different data shares a key.
    """
    tokens = _profile_tokens(command)
    if not tokens:
        return "?"
    lead = Path(tokens[0]).name
    python_lead = _PYTHON_NAME.fullmatch(lead) is not None
    module = _python_module(tokens) if python_lead else None
    is_pytest = lead.lower() in {"pytest", "pytest.exe"} or (
        python_lead and module in {"pytest", "py.test"}
    )
    if is_pytest:
        return f"python:pytest[xdist={_pytest_xdist_value(tokens)}]"

    script = None
    for t in tokens[1:]:
        if t.startswith("-"):
            continue
        if Path(t).name.lower().endswith(_SCRIPT_EXTS):
            script = Path(t).name
            break
    if script is None:
        for t in tokens[1:]:
            if not t.startswith("-"):
                script = Path(t).name
                break
    return f"{lead}:{script}" if script else lead


def _path(state_root: Path) -> Path:
    return Path(state_root) / PROFILE_FILE


@dataclass(frozen=True)
class ProfileLoad:
    """Checked result that keeps absent separate from malformed existing bytes."""

    status: Literal["absent", "valid", "malformed"]
    profiles: dict[str, Any] = field(default_factory=dict)
    detail: str = ""


@dataclass(frozen=True)
class WorkloadObservation:
    """One gate-approved, setting-specific workload observation.

    The caller owns admission: this type has no acceptance or promotion
    semantics. Metric mappings carry the normalized telemetry labels alongside
    values so unlike physical-memory measures are never silently conflated:
    ``memory={peak_bytes, metric, coverage}``,
    ``cpu={cpu_sec, avg_cpu_pct, coverage}``, and
    ``gpu={scope, max_util_pct, min_vram_free_bytes,
    unified_memory_min_available_bytes, status}``.
    """

    project_id: str
    command_key: str
    device: str
    workload_name: str
    adapter_version: int
    setting_fingerprint: str
    receipt_status: str
    work_unit: str
    evaluation: str
    updated: str
    setting: Mapping[str, Any]
    constraints: Mapping[str, Any]
    exec_s: float | None = None
    trip_s: float | None = None
    throughput: float | None = None
    memory: Mapping[str, Any] = field(default_factory=dict)
    cpu: Mapping[str, Any] = field(default_factory=dict)
    gpu: Mapping[str, Any] = field(default_factory=dict)


def _workload_store_is_valid(data: dict[str, Any]) -> bool:
    store = data.get(WORKLOAD_PROFILES_KEY)
    if store is None:
        return True
    if not isinstance(store, dict):
        return False
    version = store.get("version")
    entries = store.get("entries")
    return (
        isinstance(version, int)
        and not isinstance(version, bool)
        and version == WORKLOAD_PROFILE_VERSION
        and isinstance(entries, dict)
        and all(
            isinstance(key, str) and _stored_workload_row_is_valid(row)
            for key, row in entries.items()
        )
    )


def _legacy_profiles_are_valid(data: dict[str, Any]) -> bool:
    """Validate current device rows while retaining the documented flat legacy row."""

    for project_id, commands in data.items():
        if project_id == WORKLOAD_PROFILES_KEY:
            continue
        if not isinstance(project_id, str) or not project_id or not isinstance(commands, dict):
            return False
        for key, node in commands.items():
            if not isinstance(key, str) or not key or not isinstance(node, dict):
                return False
            if not node:
                continue
            nested = [isinstance(value, dict) for value in node.values()]
            if all(nested):
                if any(
                    not isinstance(device, str)
                    or not device
                    or not _legacy_profile_row_is_valid(row)
                    for device, row in node.items()
                ):
                    return False
            elif any(nested) or not _legacy_profile_row_is_valid(node):
                return False
    return True


def _legacy_profile_row_is_valid(row: object) -> bool:
    if not isinstance(row, dict) or not set(row).issubset(_LEGACY_PROFILE_FIELDS):
        return False
    count = row.get("n")
    if count is not None and (
        not isinstance(count, int) or isinstance(count, bool) or count < 0
    ):
        return False
    if any(not _stored_number(row.get(key)) for key in _LEGACY_NUMERIC_FIELDS if key in row):
        return False
    updated = row.get("updated")
    return updated is None or isinstance(updated, str)


def _stored_workload_row_is_valid(row: object) -> bool:
    if not isinstance(row, dict):
        return False
    strings = (
        "project_id",
        "command_key",
        "device",
        "workload_name",
        "setting_fingerprint",
        "receipt_status",
        "work_unit",
        "evaluation",
        "updated",
    )
    if any(not isinstance(row.get(key), str) or not row[key] for key in strings):
        return False
    version = row.get("adapter_version")
    count = row.get("n")
    if (
        not isinstance(version, int)
        or isinstance(version, bool)
        or version < 1
        or not isinstance(count, int)
        or isinstance(count, bool)
        or count < 1
    ):
        return False
    if row["receipt_status"] not in _RECEIPT_STATUSES:
        return False
    if row["evaluation"] not in _EVALUATIONS:
        return False
    if not row["setting_fingerprint"].startswith("sha256:") or len(
        row["setting_fingerprint"]
    ) <= len("sha256:"):
        return False
    metadata = _normalized_workload_metadata(row.get("setting"), row.get("constraints"))
    if metadata is None:
        return False
    if any(not _stored_number(row.get(key)) for key in ("exec_s", "trip_s", "throughput")):
        return False
    metric_numbers = {
        "memory": ("peak_bytes",),
        "cpu": ("cpu_sec", "avg_cpu_pct"),
        "gpu": (
            "max_util_pct",
            "min_vram_free_bytes",
            "unified_memory_min_available_bytes",
        ),
    }
    metric_labels = {
        "memory": ("metric", "coverage"),
        "cpu": ("coverage",),
        "gpu": ("scope", "status"),
    }
    for section, keys in metric_numbers.items():
        value = row.get(section)
        if not isinstance(value, dict):
            return False
        if any(not _stored_number(value.get(key)) for key in keys):
            return False
        if any(
            not isinstance(value.get(key), str) or not value[key]
            for key in metric_labels[section]
        ):
            return False
    return True


def _stored_number(value: object) -> bool:
    return value is None or (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _json_value_is_valid(value: object, *, depth: int = 0) -> bool:
    if depth > 32:
        return False
    if value is None or isinstance(value, (str, bool)):
        return True
    if isinstance(value, int) and not isinstance(value, bool):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, list):
        return all(_json_value_is_valid(item, depth=depth + 1) for item in value)
    if isinstance(value, Mapping):
        return all(
            isinstance(key, str)
            and _json_value_is_valid(item, depth=depth + 1)
            for key, item in value.items()
        )
    return False


def _normalized_workload_metadata(
    setting: object,
    constraints: object,
) -> dict[str, dict[str, Any]] | None:
    if not isinstance(setting, Mapping) or not isinstance(constraints, Mapping):
        return None
    payload = {"setting": setting, "constraints": constraints}
    if not _json_value_is_valid(payload):
        return None
    try:
        encoded = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(encoded) > MAX_WORKLOAD_METADATA_BYTES:
            return None
        normalized = json.loads(encoded.decode("utf-8"))
    except (TypeError, ValueError, OverflowError, RecursionError):
        return None
    return normalized


def load_profiles_checked(state_root: Path) -> ProfileLoad:
    """Load ``profiles.json`` without erasing the distinction between file states."""

    p = _path(state_root)
    try:
        raw = p.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ProfileLoad("absent")
    except (OSError, UnicodeError) as exc:
        return ProfileLoad("malformed", detail=f"unreadable: {type(exc).__name__}")
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        return ProfileLoad("malformed", detail=f"invalid JSON: {type(exc).__name__}")
    if not isinstance(data, dict):
        return ProfileLoad("malformed", detail="profile root must be an object")
    if not _legacy_profiles_are_valid(data):
        return ProfileLoad("malformed", detail="legacy profile tree is malformed")
    if not _workload_store_is_valid(data):
        return ProfileLoad("malformed", detail="workload profile store is malformed")
    return ProfileLoad("valid", profiles=data)


def load_profiles(state_root: Path) -> dict[str, Any]:
    loaded = load_profiles_checked(state_root)
    return loaded.profiles if loaded.status == "valid" else {}


def _ewma(old, new, alpha: float):
    # Keep millisecond precision in the store; round only at display time. (Rounding
    # to 0.1 s here used to collapse sub-second overhead to 0.0.)
    if new is None:
        return old
    if old is None:
        return round(float(new), 3)
    return round(alpha * float(new) + (1.0 - alpha) * float(old), 3)


def _high_water(old, new):
    """Keep a monotone observed maximum for safety-sensitive admission."""
    if new is None:
        return old
    if old is None:
        return round(float(new), 3)
    return round(max(float(old), float(new)), 3)


def _devmap(profiles: dict, project_id: str, key: str) -> dict[str, dict]:
    """The ``{device: entry}`` map for (project, command-key).

    Tolerates legacy flat entries (pre per-device schema): those have scalar
    values, so they filter out to ``{}`` and are simply re-learned. profiles.json
    is local regenerable state, so no migration is needed.
    """
    node = (profiles.get(project_id) or {}).get(key)
    if not isinstance(node, dict):
        return {}
    return {d: e for d, e in node.items() if isinstance(e, dict)}


def device_profile(profiles: dict, project_id: str, key: str, device: str) -> dict | None:
    """The rolling estimate for one (project, command-key, device), or None."""
    return _devmap(profiles, project_id, key).get(device)


def predict_job(profiles: dict, project_id: str, key: str) -> dict | None:
    """Device-agnostic estimate for scheduler pre-selection.

    ``rss_mb`` = max observed RSS high-water mark (a job property, roughly
    device-independent).  Older rows without ``rss_high_mb`` fall back to their
    rolling ``rss_mb`` estimate. ``dur_s`` = min remote ``exec_s`` (best-case "is
    this quick?" for trivial-skip; ``LOCAL`` excluded). Returns None when nothing
    is known.
    """
    devs = _devmap(profiles, project_id, key)
    if not devs:
        return None
    rss_vals = [
        e.get("rss_high_mb", e.get("rss_mb"))
        for e in devs.values()
        if e.get("rss_high_mb", e.get("rss_mb")) is not None
    ]
    remote = {d: e for d, e in devs.items() if d != LOCAL_DEVICE}
    exec_src = remote or devs
    exec_vals = [e["exec_s"] for e in exec_src.values() if e.get("exec_s") is not None]
    rss = max(rss_vals) if rss_vals else None
    dur = min(exec_vals) if exec_vals else None
    if rss is None and dur is None:
        return None
    return {"rss_mb": rss, "dur_s": dur}


def recommend_offload(profiles: dict, project_id: str, key: str, *,
                      devices: list[str] | None = None, bias: float = 1.0) -> dict:
    """Recommend local vs. remote for a job, from measured local + trip times.

    Compares the ``LOCAL`` baseline ``exec_s`` against the best candidate device's
    full ``trip_s`` (push+exec+pullback). ``bias`` tilts toward remote for
    responsiveness: with bias 1.25, remote is chosen even when its trip is up to
    25% slower than local. ``devices`` optionally restricts the candidate set.

    Returns ``{recommend: 'remote'|'local'|'unknown', ...}``. 'unknown' means no
    bench data yet — the caller should fall back to the `[offload]` policy.
    """
    devs = _devmap(profiles, project_id, key)
    local = devs.get(LOCAL_DEVICE) or {}
    local_s = local.get("exec_s")
    remote = {d: e for d, e in devs.items()
              if d != LOCAL_DEVICE and e.get("trip_s") is not None
              and (devices is None or d in devices)}
    if not remote or local_s is None:
        return {"recommend": "unknown", "local_s": local_s,
                "have_remote": bool(remote), "have_local": local_s is not None}
    best_dev = min(remote, key=lambda d: remote[d]["trip_s"])
    best_trip = float(remote[best_dev]["trip_s"])
    local_s = float(local_s)
    rec = "remote" if best_trip <= local_s * bias else "local"
    return {"recommend": rec, "local_s": round(local_s, 3), "best_device": best_dev,
            "best_trip_s": round(best_trip, 3), "bias": bias}


# --- legacy per-project job-cost import (read-only compatibility) -------------
# Older releases optionally wrote these advisory rows into synced project trees.
# Keep import for one compatibility release; no remrun runtime path creates or
# updates them now that per-run state is controller-local.
JOB_COSTS_FILE = "job_costs.json"
_PORTABLE_FIELDS = ("rss_mb", "cpu_pct", "exec_s")
_LEGACY_JOB_COST_FIELDS = frozenset(
    {"n", "rss_mb", "cpu_pct", "exec_s", "updated"}
)


def _legacy_job_cost_row_is_valid(row: object) -> bool:
    if not isinstance(row, dict) or not set(row).issubset(_LEGACY_JOB_COST_FIELDS):
        return False
    count = row.get("n")
    if (
        not isinstance(count, int)
        or isinstance(count, bool)
        or count < 0
    ):
        return False
    if any(
        not _stored_number(row.get(field))
        for field in _PORTABLE_FIELDS
        if field in row
    ):
        return False
    return isinstance(row.get("updated", ""), str)


def _legacy_job_cost_document(data: object) -> dict[str, Any] | None:
    if (
        not isinstance(data, dict)
        or data.get("version") != 1
        or isinstance(data.get("version"), bool)
        or not isinstance(data.get("costs"), dict)
    ):
        return None
    costs = data["costs"]
    for key, devices in costs.items():
        if not isinstance(key, str) or not key or not isinstance(devices, dict):
            return None
        if any(
            not isinstance(device, str)
            or not device
            or not _legacy_job_cost_row_is_valid(row)
            for device, row in devices.items()
        ):
            return None
    return costs


def _job_costs_dir(project_root: Path) -> Path:
    return Path(project_root) / "do" / "remrun"


def load_job_costs(project_root: Path) -> dict[str, Any]:
    """Merge legacy job-cost files without mutating any project-tree bytes."""
    d = _job_costs_dir(project_root)
    if not d.is_dir():
        return {}
    paths: list[Path] = []
    legacy = d / JOB_COSTS_FILE
    if legacy.exists():
        paths.append(legacy)
    paths += sorted(d.glob("job_costs.*.json"))   # per-controller files (excludes the legacy single)
    merged: dict[str, dict[str, Any]] = {}
    for p in paths:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, ValueError):
            continue
        costs = _legacy_job_cost_document(data)
        if costs is None:
            continue
        for k, v in costs.items():
            if not isinstance(v, dict):
                continue
            row = merged.setdefault(k, {})
            for dev, e in v.items():
                # Defensive: a LOCAL (offload-baseline) row is controller-specific and must never
                # travel between controllers — strip it on read (also guards a conflicted file).
                if dev == LOCAL_DEVICE or not isinstance(e, dict):
                    continue
                cur = row.get(dev)
                if cur is None or str(e.get("updated", "")) >= str(cur.get("updated", "")):
                    row[dev] = e
    return merged


def merge_job_costs(profiles: dict, project_id: str, job_costs: dict) -> dict:
    """Overlay the portable project job-costs UNDER the local profiles for ``project_id``:
    a fresh controller (no local row yet) still gets rss/cpu/exec for placement, while a
    controller that has run the job keeps its own refined values. Controller-specific
    fields (trip_s/overhead_s/LOCAL) come only from the local store. Returns a new dict;
    does not mutate ``profiles``."""
    if not job_costs:
        return profiles
    merged = dict(profiles)
    proj = {k: ({d: dict(e) for d, e in v.items() if isinstance(e, dict)}
                if isinstance(v, dict) else v)
            for k, v in (merged.get(project_id) or {}).items()}
    changed = False
    for key, devmap in job_costs.items():
        if not isinstance(key, str) or not key or not isinstance(devmap, dict):
            continue
        for device, pentry in devmap.items():
            if (
                not isinstance(device, str)
                or not device
                or device == LOCAL_DEVICE
                or not _legacy_job_cost_row_is_valid(pentry)
            ):
                continue
            node = proj.get(key)
            if not isinstance(node, dict):
                node = {}
                proj[key] = node
            out = dict(node.get(device) if isinstance(node.get(device), dict) else {})
            for f in _PORTABLE_FIELDS:
                if out.get(f) is None and pentry.get(f) is not None:
                    out[f] = pentry[f]
            out.setdefault("n", int(pentry.get("n", 0)))
            node[device] = out
            changed = True
    if not changed:
        return profiles
    merged[project_id] = proj
    return merged


def _write_profiles(state_root: Path, profiles: dict[str, Any]) -> bool:
    """Atomically replace the one controller-local profile document."""

    try:
        p = _path(state_root)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + f".tmp{os.getpid()}")
        tmp.write_text(
            json.dumps(profiles, allow_nan=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(tmp, p)
    except (OSError, TypeError, ValueError):
        return False
    return True


def _workload_profile_key(observation: WorkloadObservation) -> str:
    """Collision-free JSON encoding of the six-part setting identity."""

    return json.dumps(
        [
            observation.project_id,
            observation.command_key,
            observation.device,
            observation.workload_name,
            observation.adapter_version,
            observation.setting_fingerprint,
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _observation_is_valid(observation: WorkloadObservation) -> bool:
    identities = (
        observation.project_id,
        observation.command_key,
        observation.device,
        observation.workload_name,
        observation.setting_fingerprint,
        observation.work_unit,
        observation.updated,
    )
    if any(not isinstance(value, str) or not value for value in identities):
        return False
    if (
        not isinstance(observation.adapter_version, int)
        or isinstance(observation.adapter_version, bool)
        or observation.adapter_version < 1
        or not isinstance(observation.receipt_status, str)
        or observation.receipt_status not in _RECEIPT_STATUSES
        or not isinstance(observation.evaluation, str)
        or observation.evaluation not in _EVALUATIONS
        or not observation.setting_fingerprint.startswith("sha256:")
        or len(observation.setting_fingerprint) <= len("sha256:")
    ):
        return False
    if any(
        not _stored_number(value)
        for value in (observation.exec_s, observation.trip_s, observation.throughput)
    ):
        return False
    return all(isinstance(section, Mapping) for section in (
        observation.setting,
        observation.constraints,
        observation.memory,
        observation.cpu,
        observation.gpu,
    ))


def _metric_label(section: Mapping[str, Any], key: str) -> str:
    value = section.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _metric_number(section: Mapping[str, Any], key: str) -> int | float | None:
    value = section.get(key)
    if not _stored_number(value):
        raise ValueError(f"{key} must be finite and non-negative or None")
    return value


def _workload_entries(
    profiles: dict[str, Any],
    *,
    create: bool,
) -> dict[str, dict[str, Any]] | None:
    store = profiles.get(WORKLOAD_PROFILES_KEY)
    if store is None:
        if not create:
            return None
        store = {"version": WORKLOAD_PROFILE_VERSION, "entries": {}}
        profiles[WORKLOAD_PROFILES_KEY] = store
    if (
        not isinstance(store, dict)
        or store.get("version") != WORKLOAD_PROFILE_VERSION
        or isinstance(store.get("version"), bool)
        or not isinstance(store.get("entries"), dict)
    ):
        return None
    return store["entries"]


def workload_profile(
    profiles: dict[str, Any],
    observation: WorkloadObservation,
) -> dict[str, Any] | None:
    """Return the row for an observation's setting identity, if one exists."""

    entries = _workload_entries(profiles, create=False)
    if entries is None:
        return None
    row = entries.get(_workload_profile_key(observation))
    return row if isinstance(row, dict) else None


def update_workload_profile(
    state_root: Path,
    observation: WorkloadObservation,
    *,
    alpha: float = DEFAULT_ALPHA,
    max_entries: int = DEFAULT_MAX_ENTRIES,
) -> bool:
    """Fold one admitted workload observation into the same bounded profile file.

    Admission remains the caller's responsibility. This function never promotes
    a setting and refuses malformed observations or malformed existing bytes.
    """

    if (
        not _observation_is_valid(observation)
        or not isinstance(alpha, (int, float))
        or isinstance(alpha, bool)
        or not math.isfinite(alpha)
        or not 0 <= alpha <= 1
        or not isinstance(max_entries, int)
        or isinstance(max_entries, bool)
        or max_entries < 1
    ):
        return False
    metadata = _normalized_workload_metadata(
        observation.setting,
        observation.constraints,
    )
    if metadata is None:
        return False
    loaded = load_profiles_checked(state_root)
    if loaded.status == "malformed":
        return False
    profiles = loaded.profiles
    entries = _workload_entries(profiles, create=True)
    if entries is None:
        return False

    profile_key = _workload_profile_key(observation)
    previous = entries.get(profile_key)
    if previous is not None and not _stored_workload_row_is_valid(previous):
        return False
    if previous is not None and previous.get("setting") != metadata["setting"]:
        # A setting fingerprint is the identity key, not an assertion remrun can
        # trust blindly. Refuse a collision rather than averaging measurements
        # from two project settings into one plausible-looking row.
        return False
    prev = previous or {}
    try:
        count = int(prev.get("n", 0)) + 1
        prev_memory = prev.get("memory") if isinstance(prev.get("memory"), dict) else {}
        prev_cpu = prev.get("cpu") if isinstance(prev.get("cpu"), dict) else {}
        prev_gpu = prev.get("gpu") if isinstance(prev.get("gpu"), dict) else {}
        memory_peak = _metric_number(observation.memory, "peak_bytes")
        cpu_sec = _metric_number(observation.cpu, "cpu_sec")
        avg_cpu = _metric_number(observation.cpu, "avg_cpu_pct")
        gpu_util = _metric_number(observation.gpu, "max_util_pct")
        gpu_vram = _metric_number(observation.gpu, "min_vram_free_bytes")
        unified_memory = _metric_number(
            observation.gpu,
            "unified_memory_min_available_bytes",
        )
        memory_metric = _metric_label(observation.memory, "metric")
        memory_coverage = _metric_label(observation.memory, "coverage")
        cpu_coverage = _metric_label(observation.cpu, "coverage")
        gpu_scope = _metric_label(observation.gpu, "scope")
        gpu_status = _metric_label(observation.gpu, "status")
        if prev_memory and (
            prev_memory.get("metric") != memory_metric
            or prev_memory.get("coverage") != memory_coverage
        ):
            prev_memory = {}
        if prev_cpu and prev_cpu.get("coverage") != cpu_coverage:
            prev_cpu = {}
        if prev_gpu and (
            prev_gpu.get("scope") != gpu_scope
            or prev_gpu.get("status") != gpu_status
        ):
            prev_gpu = {}
        row = {
            "project_id": observation.project_id,
            "command_key": observation.command_key,
            "device": observation.device,
            "workload_name": observation.workload_name,
            "adapter_version": observation.adapter_version,
            "setting_fingerprint": observation.setting_fingerprint,
            "work_unit": observation.work_unit,
            "n": count,
            "exec_s": _ewma(prev.get("exec_s"), observation.exec_s, alpha),
            "trip_s": _ewma(prev.get("trip_s"), observation.trip_s, alpha),
            "throughput": _ewma(prev.get("throughput"), observation.throughput, alpha),
            "memory": {
                "peak_bytes": _ewma(prev_memory.get("peak_bytes"), memory_peak, alpha),
                "metric": memory_metric,
                "coverage": memory_coverage,
            },
            "cpu": {
                "cpu_sec": _ewma(prev_cpu.get("cpu_sec"), cpu_sec, alpha),
                "avg_cpu_pct": _ewma(prev_cpu.get("avg_cpu_pct"), avg_cpu, alpha),
                "coverage": cpu_coverage,
            },
            "gpu": {
                "scope": gpu_scope,
                "max_util_pct": _ewma(prev_gpu.get("max_util_pct"), gpu_util, alpha),
                "min_vram_free_bytes": _ewma(
                    prev_gpu.get("min_vram_free_bytes"),
                    gpu_vram,
                    alpha,
                ),
                "unified_memory_min_available_bytes": _ewma(
                    prev_gpu.get("unified_memory_min_available_bytes"),
                    unified_memory,
                    alpha,
                ),
                "status": gpu_status,
            },
            "receipt_status": observation.receipt_status,
            "evaluation": observation.evaluation,
            "setting": metadata["setting"],
            "constraints": metadata["constraints"],
            "updated": observation.updated,
        }
    except (TypeError, ValueError, OverflowError):
        return False
    entries[profile_key] = row

    if len(entries) > max_entries:
        oldest = sorted(
            entries,
            key=lambda key: (str(entries[key].get("updated", "")), key),
        )
        for key in oldest[: len(entries) - max_entries]:
            entries.pop(key, None)
    return _write_profiles(state_root, profiles)


def update_profile(state_root: Path, project_id: str, key: str, device: str, *,
                   peak_rss_mb=None, avg_cpu_pct=None, exec_s=None, trip_s=None,
                   now: str = "", alpha: float = DEFAULT_ALPHA,
                   max_entries: int = DEFAULT_MAX_ENTRIES) -> None:
    """Fold one run's observed cost for (project, command-key, device) into the
    store (bounded). Typical cost/timing remains an EWMA; ``rss_high_mb`` is a
    monotone observed maximum for safety-sensitive admission. ``overhead_s`` is
    derived as ``trip_s - exec_s`` when both are present. Best-effort; never
    raises."""
    if (
        any(
            not isinstance(value, str) or not value
            for value in (project_id, key, device)
        )
        or not isinstance(now, str)
        or any(
            not _stored_number(value)
            for value in (peak_rss_mb, avg_cpu_pct, exec_s, trip_s)
        )
        or not isinstance(alpha, (int, float))
        or isinstance(alpha, bool)
        or not math.isfinite(alpha)
        or not 0 <= alpha <= 1
        or not isinstance(max_entries, int)
        or isinstance(max_entries, bool)
        or max_entries < 1
    ):
        return
    loaded = load_profiles_checked(state_root)
    if loaded.status == "malformed":
        return
    profiles = loaded.profiles
    proj = profiles.get(project_id)
    if proj is None:
        proj = {}
        profiles[project_id] = proj
    elif not isinstance(proj, dict):
        return
    node = proj.get(key)
    # Fresh, or a legacy flat entry (scalar values) → restart as a per-device map.
    if not isinstance(node, dict) or (node and not any(isinstance(v, dict) for v in node.values())):
        node = {}
    proj[key] = node
    previous = node.get(device)
    if previous is not None and not isinstance(previous, dict):
        return
    prev = previous or {}
    overhead = None
    if trip_s is not None and exec_s is not None:
        overhead = max(0.0, round(float(trip_s) - float(exec_s), 3))
    node[device] = {
        "n": int(prev.get("n", 0)) + 1,
        "rss_mb": _ewma(prev.get("rss_mb"), peak_rss_mb, alpha),
        "rss_high_mb": _high_water(
            prev.get("rss_high_mb", prev.get("rss_mb")), peak_rss_mb
        ),
        "cpu_pct": _ewma(prev.get("cpu_pct"), avg_cpu_pct, alpha),
        "exec_s": _ewma(prev.get("exec_s"), exec_s, alpha),
        "trip_s": _ewma(prev.get("trip_s"), trip_s, alpha),
        "overhead_s": _ewma(prev.get("overhead_s"), overhead, alpha),
        "updated": now,
    }
    # Bound total device-rows; evict oldest by 'updated', pruning empty parents.
    flat = [(pid, k, d, e.get("updated", ""))
            for pid, ks in profiles.items()
            if pid != WORKLOAD_PROFILES_KEY and isinstance(ks, dict)
            for k, ds in ks.items() if isinstance(ds, dict)
            for d, e in ds.items() if isinstance(e, dict)]
    if len(flat) > max_entries:
        flat.sort(key=lambda x: x[3])
        for pid, k, d, _ in flat[: len(flat) - max_entries]:
            ds = profiles.get(pid, {}).get(k)
            if isinstance(ds, dict):
                ds.pop(d, None)
                if not ds:
                    profiles[pid].pop(k, None)
            if not profiles.get(pid):
                profiles.pop(pid, None)
    # Atomic replace so a concurrent reader never sees a torn file (which
    # load_profiles would treat as empty and the next write would then clobber).
    # NOTE: this prevents corruption, not lost updates — two concurrent
    # read-modify-write runs can still drop one another's row. profiles.json is
    # local regenerable state, so the residual race is acceptable for now.
    _write_profiles(state_root, profiles)
