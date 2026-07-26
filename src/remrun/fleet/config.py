"""Fleet configuration: the [fleet] block from defaults.toml, with fallbacks.

Kept small and separate from remrun's core config so fleet knobs don't entangle
the project-runner config.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from ..config import RemrunConfig, load_toml


def load_fleet_costs(config: RemrunConfig) -> dict[str, dict]:
    """Optional shared measured device costs (``config/fleet_costs.toml``).

    Portable across controllers in one deployment (device costs don't depend on
    who dispatched), so a new controller can avoid starting cost-blind. Public
    releases should ship an example file, not private measured costs. The local
    per-controller EWMA store refines on top (see ``profiles.merge_costs``).
    """
    doc = load_toml(Path(config.repo_root) / "config" / "fleet_costs.toml")
    costs = doc.get("costs", {})
    return {k: dict(v) for k, v in costs.items() if isinstance(v, dict)}


def load_costs(config: RemrunConfig, state_root: Path) -> dict:
    """Placement's cost view: shared measured costs merged under the local EWMA store."""
    from . import profiles
    return profiles.merge_costs(profiles.load_profiles(state_root), load_fleet_costs(config))


def fleet_config(config: RemrunConfig) -> dict[str, Any]:
    f = dict(config.defaults.get("fleet", {}))
    f.setdefault("idle_grace_s", 60)          # legacy idle grace; no resident daemon implied
    f.setdefault("safety_fraction", 0.90)     # usable share of free RAM/VRAM for fit checks
    f.setdefault("transfer_mbps", 200.0)      # pessimistic default throughput (LAN/Tailscale)
    f.setdefault("per_file_overhead_s", 0.05)
    f.setdefault("ssh_setup_s", 0.6)          # ~stage/exec/fetch connection overhead (no reconcile)
    # Adaptive split hysteresis (Phase 3c): margin a split must beat one batch by =
    # max(min_hysteresis_s, hysteresis_finish_frac*best_finish, OCR page-count uncertainty).
    # The legacy fixed `hysteresis_s` default was dropped on 2026-07-25. Seeding it here made
    # `min_hysteresis_s` always present, so placement's `get("min_hysteresis_s",
    # get("hysteresis_s", ...))` fallback could never reach a configured `hysteresis_s` — the
    # documented knob was dead. A config that still sets it now genuinely falls back below.
    f.setdefault("min_hysteresis_s", float(f.get("hysteresis_s", 5.0)))
    f.setdefault("hysteresis_finish_frac", 0.05)
    f.setdefault("page_uncertainty_frac", 0.25)
    # Confidence penalty for placement estimates. Priors/shared seeds carry n=0, so they get the
    # full fractional penalty; learned local profiles decay as 1/sqrt(n). This only affects
    # routing/makespan estimates, not memory gates.
    f.setdefault("confidence_penalty_frac", 0.35)
    # Two-part GPU memory gate. Per-engine VRAM reserve (MB) for can-ever-fit, plus
    # engines that do NOT elastically shrink and therefore must fit LIVE free VRAM
    # (can-launch-now) with a fragmentation reserve. Empty set = no engine gated
    # on live free VRAM.
    f.setdefault("vram_reserve_mb", {})
    f.setdefault("gpu_nonelastic_engines", [])
    f.setdefault("vram_frag_reserve_mb", 512.0)
    f.setdefault("health_audit", True)        # post-batch Invariant-0 configured-process check
    f.setdefault("health_cooldown_s", 300.0)
    # OOM failure learning: bump the relevant memory estimate so future placement can skip or
    # reroute instead of repeating the same too-small prior/profile.
    f.setdefault("oom_memory_raise_factor", 1.25)
    f.setdefault("oom_memory_raise_min_delta_mb", 512.0)
    pools = dict(f.get("pools", {}))
    pools.setdefault("gpu", 1)                # generic single-GPU resource pool
    f["pools"] = pools
    return f


def idle_grace_s(config: RemrunConfig) -> float:
    return float(fleet_config(config).get("idle_grace_s", 60))


def safety_fraction(config: RemrunConfig) -> float:
    return float(fleet_config(config).get("safety_fraction", 0.90))
