"""remrun fleet mode — project-less, measured-placement job orchestration.

A generalized layer that runs configuration-defined work across the device
fleet, choosing placement by measured cost and live resources. It reuses
remrun's device config, transports, probes, and profile store, and intentionally
skips the project reconcile/conflict/baseline path.

INVARIANT 0 — NO WARM WORKERS. A configured worker exists only while a job or
compatible burst is active. There is no resident worker pool or daemon; fixed
startup cost may be amortized only across one compatible invocation.
"""
from __future__ import annotations

__all__ = ["models", "placement", "profiles", "adapters", "probes", "queue", "config"]
