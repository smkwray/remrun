"""Fleet cost-profile store, keyed by frozen prepared-task identity.

Uses a fleet-specific key and a SQLite raw-observation journal; there is NO
warm-model field (Invariant 0). Derived fits and the small local memory-correction
cache are regenerable and never synced.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
from pathlib import Path
from typing import Any

from .models import FleetProfile, FleetTask
from .task_contract import sha256_id

FLEET_PROFILE_FILE = "fleet_profiles.json"
DEFAULT_MAX_ENTRIES = 256

_COST_FIELDS = ("fixed_load_s", "var_per_unit_s", "peak_rss_mb", "peak_vram_mb")


def prepared_profile_key(task, device: str) -> str:  # noqa: ANN001
    """Exact learned-cost identity for frozen prepared work."""
    prepared = task.prepared
    adapter = (task.resolved_spec.get("adapters") or {}).get(device) or {}
    cost = prepared["cost"]
    return sha256_id({
        "spec_id": prepared["spec_id"],
        "adapter_id": adapter.get("adapter_id") or "raw-command",
        "device": device,
        "unit": cost.get("unit"),
        "measure_id": cost.get("measure_id") or "legacy",
        "bucket_id": cost.get("bucket_id"),
    })


def prepared_profile_family_id(task) -> str:  # noqa: ANN001
    """Comparable-work identity; device and adapter are intentionally excluded."""
    prepared = task.prepared
    cost = prepared["cost"]
    return sha256_id({
        "spec_id": prepared["spec_id"],
        "unit": cost.get("unit"),
        "measure_id": cost.get("measure_id") or "legacy",
        "bucket_id": cost.get("bucket_id"),
    })


def profile_observation(tasks: list[FleetTask], device: str, result: dict[str, Any],
                        result_record: str | None) -> dict[str, Any]:
    """Build one raw generic observation; eligibility is explicit, never inferred later."""
    head = tasks[0]
    configured = bool(tasks) and all(
        task.prepared and task.prepared["kind"] == "task" for task in tasks
    )
    costs = [task.prepared["cost"] for task in tasks if task.prepared]
    rows = list(result.get("item_results") or [])
    performed = bool(rows) and all(
        row.get("outcome") == "succeeded" and row.get("work_performed") is True
        for row in rows
    )
    work_rows = [row.get("work_units") for row in rows]
    measured = bool(work_rows) and all(isinstance(value, dict) for value in work_rows)
    observed_units = (sum(float(value["value"]) for value in work_rows)
                      if measured else None)
    declared_exact = configured and all(cost.get("status") == "exact" for cost in costs)
    exact_costs = declared_exact and all(cost.get("measure_id") for cost in costs)
    prepared_units = (sum(float(cost["value"]) for cost in costs)
                      if exact_costs and all(cost.get("value") is not None for cost in costs)
                      else None)
    elapsed = result.get("elapsed_s")
    accepted = bool(
        configured
        and result.get("ok")
        and exact_costs
        and prepared_units is not None
        and performed
        and measured
        and isinstance(elapsed, (int, float))
        and not isinstance(elapsed, bool)
        and float(elapsed) > 0
    )
    if accepted:
        reject_reason = None
    elif not configured:
        reject_reason = "intrinsic_command"
    elif not declared_exact:
        reject_reason = "unestimated"
    elif not exact_costs:
        reject_reason = "unverified_work_measure"
    elif not result.get("ok"):
        reject_reason = "unsuccessful"
    elif any(row.get("failure_code") == "work_measure_mismatch" for row in rows):
        reject_reason = "work_measure_mismatch"
    elif not performed:
        reject_reason = "partial_or_no_work"
    elif not measured:
        reject_reason = "work_measure_missing"
    else:
        reject_reason = "elapsed_invalid"
    telemetry = result.get("telemetry") if isinstance(result.get("telemetry"), dict) else {}
    item_elapsed = [float(row["elapsed_s"]) for row in rows
                    if isinstance(row.get("elapsed_s"), (int, float))
                    and not isinstance(row.get("elapsed_s"), bool)]
    adapter_id = None
    profile_key = None
    family_id = None
    if configured:
        adapter = (head.resolved_spec.get("adapters") or {}).get(device) or {}
        adapter_id = adapter.get("adapter_id")
        profile_keys = {prepared_profile_key(task, device) for task in tasks}
        family_ids = {prepared_profile_family_id(task) for task in tasks}
        if len(profile_keys) == len(family_ids) == 1:
            profile_key = profile_keys.pop()
            family_id = family_ids.pop()
        else:
            accepted = False
            reject_reason = "mixed_profile_identity"
    digest_source = result_record or json.dumps(
        result, sort_keys=True, default=str, separators=(",", ":"),
    )
    result_digest = "sha256:" + hashlib.sha256(digest_source.encode("utf-8")).hexdigest()
    return {
        "profile_key": profile_key,
        "family_id": family_id,
        "device": device,
        "adapter_id": adapter_id,
        "prepared_units": float(prepared_units) if prepared_units is not None else None,
        "observed_units": observed_units,
        "controller_elapsed_s": float(elapsed) if isinstance(elapsed, (int, float))
        and not isinstance(elapsed, bool) else None,
        "worker_elapsed_s": sum(item_elapsed) if item_elapsed else None,
        "peak_rss_mb": telemetry.get("peak_rss_mb"),
        "peak_vram_mb": telemetry.get("peak_vram_mb"),
        "accepted_duration": accepted,
        "reject_reason": reject_reason,
        "result_digest": result_digest,
    }


def prepared_profile(profiles: dict, task, device: str) -> FleetProfile | None:  # noqa: ANN001
    if not task.prepared or task.prepared["cost"]["status"] == "unestimated":
        return None
    value = profiles.get(prepared_profile_key(task, device))
    if (not isinstance(value, dict) or value.get("fixed_load_s") is None
            or value.get("model_ready") is False):
        return None
    return FleetProfile(
        fixed_load_s=value.get("fixed_load_s"), var_per_unit_s=value.get("var_per_unit_s"),
        peak_rss_mb=value.get("peak_rss_mb"), peak_vram_mb=value.get("peak_vram_mb"),
        n=int(value.get("n", 0)), duration_n=int(value.get("duration_n", value.get("t_n", 0))),
        resource_n=int(value.get("resource_n", value.get("n", 0))),
        min_units=value.get("min_units"), max_units=value.get("max_units"),
        normalized_rmse=value.get("normalized_rmse"), updated=str(value.get("updated", "")),
    )


def prepared_resource_profile(profiles: dict, task, device: str) -> FleetProfile | None:  # noqa: ANN001
    """Return resource evidence even while the duration model is unavailable."""
    if not task.prepared:
        return None
    value = profiles.get(prepared_profile_key(task, device))
    if not isinstance(value, dict):
        return None
    return FleetProfile(
        fixed_load_s=value.get("fixed_load_s"), var_per_unit_s=value.get("var_per_unit_s"),
        peak_rss_mb=value.get("peak_rss_mb"), peak_vram_mb=value.get("peak_vram_mb"),
        n=int(value.get("n", 0)), duration_n=int(value.get("duration_n", value.get("t_n", 0))),
        resource_n=int(value.get("resource_n", value.get("n", 0))),
        min_units=value.get("min_units"), max_units=value.get("max_units"),
        normalized_rmse=value.get("normalized_rmse"), updated=str(value.get("updated", "")),
    )


def duration_observation_count(profiles: dict, task, device: str) -> int:  # noqa: ANN001
    """Accepted duration observations for deterministic calibration ordering."""
    value = profiles.get(prepared_profile_key(task, device))
    if not isinstance(value, dict):
        return 0
    return max(0, int(value.get("duration_n", value.get("t_n", 0)) or 0))


def estimate_unavailable_reason(profiles: dict, task, device: str,
                                prepared_units: float) -> str:  # noqa: ANN001
    value = profiles.get(prepared_profile_key(task, device))
    if not isinstance(value, dict) or duration_observation_count(profiles, task, device) < 4:
        return "uncalibrated"
    if value.get("model_ready") is False:
        return "model_unfit"
    low, high = value.get("min_units"), value.get("max_units")
    if low is not None and high is not None and not float(low) <= prepared_units <= float(high):
        return "out_of_range"
    return "uncalibrated"


def merge_costs(local: dict, shared: dict, observed: dict | None = None) -> dict:
    """Merge local resource corrections, shared seeds, and observation-derived fits.

    A local resource correction wins over an optional shared seed. A fit rebuilt
    from the SQLite observation journal then supplies duration evidence and may
    only increase retained peak-memory evidence. Returns the schema placement
    reads; no queue completion mutates this JSON cache directly.
    """
    local = local or {}
    shared = shared or {}
    out: dict[str, dict] = {}
    for key in set(shared) | set(local):
        s = shared.get(key) if isinstance(shared.get(key), dict) else {}
        loc = local.get(key) if isinstance(local.get(key), dict) else {}
        row = {f: (loc.get(f) if loc.get(f) is not None else s.get(f)) for f in _COST_FIELDS}
        row["n"] = int(loc.get("n", 0))
        row["updated"] = loc.get("updated") or s.get("updated", "")
        out[key] = row
    for key, raw in (observed or {}).items():
        if not isinstance(raw, dict):
            continue
        prior = out.get(key, {})
        row = dict(prior)
        row.update(raw)
        for field in ("peak_rss_mb", "peak_vram_mb"):
            values = [value for value in (prior.get(field), raw.get(field)) if value is not None]
            row[field] = max(values) if values else None
        out[key] = row
    return out


def _distinct_positive_levels(values: list[float]) -> int:
    levels: list[float] = []
    for value in sorted(item for item in values if item > 0):
        if not levels or value > levels[-1] * 1.01:
            levels.append(value)
    return len(levels)


def _duration_fit(rows: list[sqlite3.Row]) -> dict[str, Any]:
    """Conservative linear fit over the most recent 32 accepted observations."""
    recent = rows[-32:]
    units = [float(row["prepared_units"]) for row in recent]
    elapsed = [float(row["controller_elapsed_s"]) for row in recent]
    out: dict[str, Any] = {
        "duration_n": len(recent),
        "min_units": min(units) if units else None,
        "max_units": max(units) if units else None,
        "fixed_load_s": None,
        "var_per_unit_s": None,
        "normalized_rmse": None,
        "model_ready": False,
        "model_reason": "uncalibrated",
    }
    if len(recent) < 4:
        return out
    positive = [value for value in units if value > 0]
    if (_distinct_positive_levels(units) < 3 or not positive
            or max(positive) / min(positive) < 2.0):
        out["model_reason"] = "model_unfit"
        return out
    count = float(len(units))
    su = sum(units)
    se = sum(elapsed)
    suu = sum(value * value for value in units)
    sue = sum(value * duration for value, duration in zip(units, elapsed))
    denominator = count * suu - su * su
    if denominator <= 1e-12:
        out["model_reason"] = "model_unfit"
        return out
    slope = (count * sue - su * se) / denominator
    intercept = (se - slope * su) / count
    if not all(math.isfinite(value) and value >= 0 for value in (slope, intercept)):
        out["model_reason"] = "model_unfit"
        return out
    residual = [
        duration - (intercept + slope * value)
        for value, duration in zip(units, elapsed)
    ]
    mean_elapsed = se / count
    nrmse = ((sum(value * value for value in residual) / count) ** 0.5 / mean_elapsed
             if mean_elapsed > 0 else float("inf"))
    out["normalized_rmse"] = nrmse
    if not math.isfinite(nrmse) or nrmse > 0.25:
        out["model_reason"] = "model_unfit"
        return out
    out.update({
        "fixed_load_s": round(intercept, 6),
        "var_per_unit_s": round(slope, 6),
        "model_ready": True,
        "model_reason": None,
    })
    return out


def load_observation_profiles(db_path: Path) -> dict[str, dict[str, Any]]:
    """Derive placement profiles from the authoritative SQLite observation journal."""
    path = Path(db_path)
    if not path.exists():
        return {}
    uri = f"file:{path}?mode=ro"
    with sqlite3.connect(uri, uri=True) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT * FROM fleet_profile_observations ORDER BY recorded_at,batch_id"
        ).fetchall()
    grouped: dict[str, list[sqlite3.Row]] = {}
    resources: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        key = row["profile_key"]
        if not key:
            continue
        resources.setdefault(key, []).append(row)
        if row["accepted_duration"]:
            grouped.setdefault(key, []).append(row)
    out: dict[str, dict[str, Any]] = {}
    for key in set(resources) | set(grouped):
        fit = _duration_fit(grouped.get(key, []))
        resource_rows = resources.get(key, [])
        rss = [float(row["peak_rss_mb"]) for row in resource_rows
               if row["peak_rss_mb"] is not None]
        vram = [float(row["peak_vram_mb"]) for row in resource_rows
                if row["peak_vram_mb"] is not None]
        fit.update({
            "resource_n": sum(1 for row in resource_rows
                              if row["peak_rss_mb"] is not None
                              or row["peak_vram_mb"] is not None),
            "peak_rss_mb": max(rss) if rss else None,
            "peak_vram_mb": max(vram) if vram else None,
            "n": len(resource_rows),
            "updated": str(resource_rows[-1]["recorded_at"]) if resource_rows else "",
        })
        out[key] = fit
    return out


def _path(state_root: Path) -> Path:
    return Path(state_root) / FLEET_PROFILE_FILE


def load_profiles(state_root: Path) -> dict[str, Any]:
    p = _path(state_root)
    if not p.exists():
        return {}
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"fleet profile cache must contain an object: {p}")
    return data


def _write_profiles(state_root: Path, profiles: dict[str, Any]) -> None:
    p = _path(state_root)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(profiles, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, p)


def raise_prepared_memory_estimate(
        state_root: Path, task, device: str, *, memory_kind: str,  # noqa: ANN001
        factor: float = 1.25, min_delta_mb: float = 512.0, now: str = "",
        base_profiles: dict | None = None,
        max_entries: int = DEFAULT_MAX_ENTRIES) -> tuple[str, float]:
    """Raise OOM memory only in the exact frozen prepared-profile namespace."""
    profiles = load_profiles(state_root)
    key = prepared_profile_key(task, device)
    local = profiles.get(key) if isinstance(profiles.get(key), dict) else {}
    base = ((base_profiles or {}).get(key)
            if isinstance((base_profiles or {}).get(key), dict) else {})
    view = {**base, **local}
    field = "peak_vram_mb" if memory_kind == "gpu" else "peak_rss_mb"
    current = float(view.get(field) or 0.0)
    if current <= 0.0:
        raised = max(0.0, float(min_delta_mb))
    else:
        raised = max(current * max(1.0, float(factor)),
                     current + max(0.0, float(min_delta_mb)))
    entry = dict(view)
    for name in _COST_FIELDS:
        entry.setdefault(name, None)
    entry[field] = round(raised, 3)
    entry["n"] = int(entry.get("n", 0) or 0)
    entry["oom_n"] = int(entry.get("oom_n", 0) or 0) + 1
    entry["updated"] = now
    profiles[key] = entry
    if len(profiles) > max_entries:
        ordered = sorted(profiles.items(), key=lambda pair: pair[1].get("updated", "")
                         if isinstance(pair[1], dict) else "")
        for old_key, _ in ordered[:len(profiles) - max_entries]:
            profiles.pop(old_key, None)
    _write_profiles(state_root, profiles)
    return field, round(raised, 3)
