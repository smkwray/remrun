"""Fleet cost-profile store, keyed (task_type, engine, device, option_bucket).

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

# Conservative generic priors used before any observation exists, per task type.
# These are deliberately rough bootstrap values, not tuned to a model or rig. Real
# runs, or optional shared ``config/fleet_costs.toml`` seed data, replace them via
# the EWMA/regression path. For GPU adapters, ``peak_vram_mb`` is the model
# footprint on a discrete-GPU box and the unified-memory need on a Mac;
# ``peak_rss_mb`` is host-side process overhead.
_GENERIC_PRIORS = {
    "tts": {
        "fixed_load_s": 30.0, "var_per_unit_s": 10.0,
        "peak_rss_mb": 1024.0, "peak_vram_mb": 4096.0,
    },
    "ocr": {
        "fixed_load_s": 30.0, "var_per_unit_s": 5.0,
        "peak_rss_mb": 1024.0, "peak_vram_mb": 4096.0,
    },
    "cmd": {
        "fixed_load_s": 1.0, "var_per_unit_s": 0.5,
        "peak_rss_mb": 512.0, "peak_vram_mb": 0.0,
    },
}


_COST_FIELDS = ("fixed_load_s", "var_per_unit_s", "peak_rss_mb", "peak_vram_mb")


def profile_key(task_type: str, engine: str, device: str, bucket: str) -> str:
    return f"{task_type}|{engine}|{device}|{bucket}"


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


def get_profile(profiles: dict, task_type: str, engine: str, device: str,
                bucket: str) -> FleetProfile | None:
    e = profiles.get(profile_key(task_type, engine, device, bucket))
    if not isinstance(e, dict):
        return None
    return FleetProfile(
        fixed_load_s=e.get("fixed_load_s"), var_per_unit_s=e.get("var_per_unit_s"),
        peak_rss_mb=e.get("peak_rss_mb"), peak_vram_mb=e.get("peak_vram_mb"),
        n=int(e.get("n", 0)), updated=str(e.get("updated", "")),
    )


def profile_or_prior(profiles: dict, task_type: str, engine: str, device: str,
                     bucket: str) -> FleetProfile:
    """The learned profile, or a conservative prior so placement always has numbers.
    A prior carries ``n=0`` so callers can flag low confidence."""
    p = get_profile(profiles, task_type, engine, device, bucket)
    if p is not None and p.fixed_load_s is not None:
        return p
    pr = _GENERIC_PRIORS.get(task_type, _GENERIC_PRIORS["cmd"])
    return FleetProfile(fixed_load_s=pr["fixed_load_s"], var_per_unit_s=pr["var_per_unit_s"],
                        peak_rss_mb=pr["peak_rss_mb"], peak_vram_mb=pr["peak_vram_mb"], n=0)


MIN_FIT_N = 3   # least-squares needs a few points (with spread in units) before it's trustworthy


def update_profile(state_root: Path, task_type: str, engine: str, device: str,
                   bucket: str, *, fixed_load_s=None, var_per_unit_s=None,
                   peak_rss_mb=None, peak_vram_mb=None,
                   observed_elapsed_s=None, observed_units=None, now: str = "",
                   alpha: float = DEFAULT_ALPHA,
                   max_entries: int = DEFAULT_MAX_ENTRIES) -> None:
    """Fold one observation into the store (bounded, atomic). Best-effort.

    Memory peaks (rss/vram) fold via EWMA. The TIME coefficients self-correct from real runs:
    pass ``observed_elapsed_s`` + ``observed_units`` and an online least-squares regression
    decomposes the worker's wall time into ``fixed_load_s`` (one cold model load, the intercept)
    + ``var_per_unit_s`` (per page/kchar, the slope). It needs SPREAD in units across runs to
    separate the two, so until ``MIN_FIT_N`` points with a non-degenerate fit accumulate, the
    prior/seed is kept. The explicit ``fixed_load_s``/``var_per_unit_s`` path (seed tooling)
    still EWMAs the supplied coefficients."""
    profiles = load_profiles(state_root)
    key = profile_key(task_type, engine, device, bucket)
    prev = profiles.get(key) if isinstance(profiles.get(key), dict) else {}
    entry = {
        "n": int(prev.get("n", 0)) + 1,
        "fixed_load_s": prev.get("fixed_load_s"),
        "var_per_unit_s": prev.get("var_per_unit_s"),
        "peak_rss_mb": _ewma(prev.get("peak_rss_mb"), peak_rss_mb, alpha),
        "peak_vram_mb": _ewma(prev.get("peak_vram_mb"), peak_vram_mb, alpha),
        "updated": now,
        # online least-squares accumulators for elapsed = fixed + slope*units (carried forward).
        "t_n": int(prev.get("t_n", 0)), "t_su": float(prev.get("t_su", 0.0)),
        "t_se": float(prev.get("t_se", 0.0)), "t_suu": float(prev.get("t_suu", 0.0)),
        "t_sue": float(prev.get("t_sue", 0.0)),
    }
    if observed_elapsed_s is not None and observed_units is not None:
        u, e = float(observed_units), float(observed_elapsed_s)
        entry["t_n"] += 1
        entry["t_su"] += u
        entry["t_se"] += e
        entry["t_suu"] += u * u
        entry["t_sue"] += u * e
        n = entry["t_n"]
        denom = n * entry["t_suu"] - entry["t_su"] ** 2     # 0 when units never vary (degenerate)
        if n >= MIN_FIT_N and denom > 1e-6:
            slope = (n * entry["t_sue"] - entry["t_su"] * entry["t_se"]) / denom
            intercept = (entry["t_se"] - slope * entry["t_su"]) / n
            if slope >= 0.0 and intercept >= 0.0:          # reject ill-conditioned/negative fits
                entry["var_per_unit_s"] = round(slope, 4)
                entry["fixed_load_s"] = round(intercept, 3)
    elif fixed_load_s is not None or var_per_unit_s is not None:
        entry["fixed_load_s"] = _ewma(prev.get("fixed_load_s"), fixed_load_s, alpha)
        entry["var_per_unit_s"] = _ewma(prev.get("var_per_unit_s"), var_per_unit_s, alpha)
    profiles[key] = entry
    if len(profiles) > max_entries:
        ordered = sorted(profiles.items(), key=lambda kv: kv[1].get("updated", "")
                         if isinstance(kv[1], dict) else "")
        for k, _ in ordered[: len(profiles) - max_entries]:
            profiles.pop(k, None)
    try:
        _write_profiles(state_root, profiles)
    except OSError:
        pass


def raise_memory_estimate(state_root: Path, task_type: str, engine: str, device: str,
                          bucket: str, *, memory_kind: str, factor: float = 1.25,
                          min_delta_mb: float = 512.0, now: str = "",
                          base_profiles: dict | None = None,
                          max_entries: int = DEFAULT_MAX_ENTRIES) -> tuple[str, float]:
    """Raise the learned memory estimate after an OOM.

    This is deliberately not a normal observation: no successful peak was measured, so ``n`` is
    unchanged and an ``oom_n`` counter records the failure-learning event. The starting point is
    the merged placement view when provided (shared seed/local/prior), then the raised field is
    written to the local store so the next placement tick can avoid repeating the same failure.
    """
    profiles = load_profiles(state_root)
    key = profile_key(task_type, engine, device, bucket)
    prev = profiles.get(key) if isinstance(profiles.get(key), dict) else {}
    view = dict(base_profiles or profiles)
    if prev:
        view[key] = {**view.get(key, {}), **prev} if isinstance(view.get(key), dict) else prev
    prof = profile_or_prior(view, task_type, engine, device, bucket)
    field = "peak_vram_mb" if memory_kind == "gpu" else "peak_rss_mb"
    current = float(getattr(prof, field) or 0.0)
    if current <= 0.0:
        raised = max(0.0, float(min_delta_mb))
    else:
        raised = max(current * max(1.0, float(factor)), current + max(0.0, float(min_delta_mb)))
    entry = dict(prev)
    for name in _COST_FIELDS:
        entry.setdefault(name, getattr(prof, name))
    entry[field] = round(raised, 3)
    entry["n"] = int(entry.get("n", getattr(prof, "n", 0)) or 0)
    entry["oom_n"] = int(entry.get("oom_n", 0) or 0) + 1
    entry["updated"] = now
    profiles[key] = entry
    if len(profiles) > max_entries:
        ordered = sorted(profiles.items(), key=lambda kv: kv[1].get("updated", "")
                         if isinstance(kv[1], dict) else "")
        for k, _ in ordered[: len(profiles) - max_entries]:
            profiles.pop(k, None)
    try:
        _write_profiles(state_root, profiles)
    except OSError:
        pass
    return field, round(raised, 3)
