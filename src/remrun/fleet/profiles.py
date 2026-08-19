"""Fleet cost-profile store, keyed by frozen prepared-task identity.

Reuses remrun's profile mechanics (EWMA, atomic replace, bounded retention) but a
fleet-specific key and schema — there is NO warm-model field (Invariant 0); the
only fixed term is the cold model load. Local, regenerable, never synced.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from ..profile import _ewma
from .models import FleetProfile

FLEET_PROFILE_FILE = "fleet_profiles.json"
DEFAULT_ALPHA = 0.4
DEFAULT_MAX_ENTRIES = 256

_COST_FIELDS = ("fixed_load_s", "var_per_unit_s", "peak_rss_mb", "peak_vram_mb")


def prepared_profile_key(task, device: str) -> str:  # noqa: ANN001
    """Exact learned-cost identity for frozen prepared work."""
    prepared = task.prepared
    adapter = task.resolved_spec["adapters"][device]
    cost = prepared["cost"]
    return "|".join([
        prepared["spec_id"], adapter["adapter_id"], device,
        cost.get("unit") or "none",
        cost.get("bucket_id") or "none", cost.get("status") or "unestimated",
    ])


def prepared_profile(profiles: dict, task, device: str) -> FleetProfile | None:  # noqa: ANN001
    if not task.prepared or task.prepared["cost"]["status"] == "unestimated":
        return None
    value = profiles.get(prepared_profile_key(task, device))
    if not isinstance(value, dict) or value.get("fixed_load_s") is None:
        return None
    return FleetProfile(
        fixed_load_s=value.get("fixed_load_s"), var_per_unit_s=value.get("var_per_unit_s"),
        peak_rss_mb=value.get("peak_rss_mb"), peak_vram_mb=value.get("peak_vram_mb"),
        n=int(value.get("n", 0)), updated=str(value.get("updated", "")),
    )


def merge_costs(local: dict, shared: dict) -> dict:
    """Field-level merge of the local EWMA store over the shared measured costs.

    For each key/field, a real local observation wins; otherwise optional shared
    measured costs are used. This is the "don't forget" mechanism: a fresh
    controller with no local store can still estimate from shared costs, while
    local runs only *refine* (memory peaks today; fixed/slope if a regression is
    added later). Returns a dict in the same schema placement reads."""
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
    return out


def _path(state_root: Path) -> Path:
    return Path(state_root) / FLEET_PROFILE_FILE


def load_profiles(state_root: Path) -> dict[str, Any]:
    p = _path(state_root)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError, ValueError):
        return {}


def _write_profiles(state_root: Path, profiles: dict[str, Any]) -> None:
    p = _path(state_root)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(profiles, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, p)


MIN_FIT_N = 3   # least-squares needs a few points (with spread in units) before it's trustworthy


def update_prepared_profile(state_root: Path, task, device: str, *,  # noqa: ANN001
                            peak_rss_mb=None, peak_vram_mb=None,
                            observed_elapsed_s=None, observed_units=None,
                            now: str = "", alpha: float = DEFAULT_ALPHA,
                            max_entries: int = DEFAULT_MAX_ENTRIES) -> None:
    """Fold one successful structured observation into its exact frozen key."""
    data = load_profiles(state_root)
    key = prepared_profile_key(task, device)
    prev = data.get(key) if isinstance(data.get(key), dict) else {}
    entry = {
        "n": int(prev.get("n", 0)) + 1,
        "fixed_load_s": prev.get("fixed_load_s"),
        "var_per_unit_s": prev.get("var_per_unit_s"),
        "peak_rss_mb": _ewma(prev.get("peak_rss_mb"), peak_rss_mb, alpha),
        "peak_vram_mb": _ewma(prev.get("peak_vram_mb"), peak_vram_mb, alpha),
        "updated": now,
        "t_n": int(prev.get("t_n", 0)), "t_su": float(prev.get("t_su", 0.0)),
        "t_se": float(prev.get("t_se", 0.0)), "t_suu": float(prev.get("t_suu", 0.0)),
        "t_sue": float(prev.get("t_sue", 0.0)),
    }
    if observed_elapsed_s is not None and observed_units is not None:
        units, elapsed = float(observed_units), float(observed_elapsed_s)
        entry["t_n"] += 1
        entry["t_su"] += units
        entry["t_se"] += elapsed
        entry["t_suu"] += units * units
        entry["t_sue"] += units * elapsed
        count = entry["t_n"]
        denominator = count * entry["t_suu"] - entry["t_su"] ** 2
        if count >= MIN_FIT_N and denominator > 1e-6:
            slope = (count * entry["t_sue"] - entry["t_su"] * entry["t_se"]) / denominator
            intercept = (entry["t_se"] - slope * entry["t_su"]) / count
            if slope >= 0.0 and intercept >= 0.0:
                entry["var_per_unit_s"] = round(slope, 4)
                entry["fixed_load_s"] = round(intercept, 3)
    data[key] = entry
    if len(data) > max_entries:
        ordered = sorted(data.items(), key=lambda pair: pair[1].get("updated", "")
                         if isinstance(pair[1], dict) else "")
        for old_key, _ in ordered[:len(data) - max_entries]:
            data.pop(old_key, None)
    try:
        _write_profiles(state_root, data)
    except OSError:
        pass


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
    try:
        _write_profiles(state_root, profiles)
    except OSError:
        pass
    return field, round(raised, 3)
