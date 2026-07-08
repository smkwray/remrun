"""remrun fleet mode — project-less, measured-placement job orchestration.

A generalized layer that runs device-specific jobs (TTS, OCR, and arbitrary
commands) across the configured device fleet, choosing placement by *measured*
cost (cold model-load + transfer + variable compute) and live RAM/VRAM/load,
batching a burst of jobs onto one device to amortize the one unavoidable model
load. It reuses remrun's device config, SSH transports, probes, and profile-store
mechanics, and intentionally skips remrun's project reconcile/conflict/baseline
path (fleet jobs produce artifacts in known output folders; they do not mutate a
synced project tree).

INVARIANT 0 — NO WARM MODELS. Models are loaded only while a job (or a queued
burst) is actively running and are unloaded on idle (immediately, or after a short
grace, default 60 s). There is no resident model server / warm pool / daemon, and
the cost model has no "warm" branch: model load is always the cold load, amortized
only across jobs that share one model lifetime (a coalesced burst).
"""
from __future__ import annotations

__all__ = ["models", "placement", "profiles", "adapters", "probes", "queue", "config"]
