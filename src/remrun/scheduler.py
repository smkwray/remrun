from __future__ import annotations

import re
import socket
from typing import Any

from .models import Device


class SchedulingError(RuntimeError):
    pass


def is_self(device: Device, hostname: str | None = None) -> bool:
    """Is this device the machine we are running on?

    Matched on the device NAME against the controller's short hostname, the same way
    `config._offload_entry` already resolves a per-host `[offload]` entry — config keys
    are device names, and a device's hostname is expected to match its name.
    `address_candidates` are also checked, since a box is often reachable by an alias
    that differs in case from its configured name.
    """
    host = (hostname or socket.gethostname() or "").split(".")[0].casefold()
    if not host:
        return False
    if device.name.casefold() == host:
        return True
    return any(str(a).split(".")[0].casefold() == host for a in device.address_candidates)


def _placement_order(project_config: dict[str, Any] | None, command: list[str]) -> list[str]:
    """Static device preference order from optional project [placement] hints.

    Command-match rules win first, then the configured primary, then fallbacks.
    Returns device names in preference order (may be empty when no hints exist).
    """
    placement = (project_config or {}).get("placement", {})
    if not placement:
        return []

    order: list[str] = []
    joined = " ".join(command)
    for rule in placement.get("rules", []):
        pattern = rule.get("match_command")
        prefer = rule.get("prefer")
        if not pattern or not prefer:
            continue
        try:
            if re.search(pattern, joined):
                order.append(prefer)
        except re.error:
            continue

    primary = placement.get("primary")
    if primary:
        order.append(primary)
    order.extend(placement.get("fallback", []))

    # De-duplicate while preserving order.
    seen: set[str] = set()
    deduped: list[str] = []
    for name in order:
        if name not in seen:
            deduped.append(name)
            seen.add(name)
    return deduped


def order_devices(
    devices: dict[str, Device],
    target: str | None,
    *,
    project_config: dict[str, Any] | None = None,
    command: list[str] | None = None,
    scheduler_cfg: dict[str, Any] | None = None,
) -> list[Device]:
    """Static preference-ordered list of candidate devices (enabled only).

    Explicit targets resolve to a single-element list. For ``auto`` the order is:
    project [placement] hints, then the configured ``[scheduler]`` primary and
    fallback, then any remaining enabled devices as a last resort. Reachability
    and load are evaluated later by the caller, walking this order.
    """
    enabled = {name: d for name, d in devices.items() if d.enabled}
    if target and target != "auto":
        device = enabled.get(target)
        if not device:
            raise SchedulingError(f"Unknown or disabled target device: {target}")
        return [device]

    sched = scheduler_cfg or {}
    order: list[str] = list(_placement_order(project_config, command or []))
    primary = sched.get("primary")
    if primary:
        order.append(primary)
    order.extend(sched.get("fallback", []))
    # Generic fallback if config is empty and no placement matched: preserve the
    # user's device order from devices.toml.
    if not any(name in enabled for name in order):
        order.extend(enabled.keys())

    result: list[Device] = []
    seen: set[str] = set()
    for name in order:
        if name in enabled and name not in seen:
            result.append(enabled[name])
            seen.add(name)
    if not result:
        # Last resort: any enabled device (excludes nothing, but auto rarely hits this).
        result = list(enabled.values())

    # Never auto-route to the machine we are already on. The controller is frequently ALSO
    # a configured device, and a device's remote project_root usually resolves to the SAME
    # absolute path as the local one — so a self-route makes reconcile compare a tree with
    # itself and transfer files onto the paths it is reading. That is a correctness hazard,
    # not merely the wasted SSH round trip. Explicit `run <DEVICE>` is untouched: naming your
    # own box is a deliberate act (and the local-sim backend exists for that), while `--auto`
    # picking it is never what the caller meant. Dropped only when something else remains.
    if len(result) > 1:
        others = [d for d in result if not is_self(d)]
        if others:
            result = others

    if not result:
        raise SchedulingError("No enabled devices configured")
    return result


def choose_device(
    devices: dict[str, Device],
    target: str | None,
    *,
    project_config: dict[str, Any] | None = None,
    command: list[str] | None = None,
    scheduler_cfg: dict[str, Any] | None = None,
) -> Device:
    """Back-compat: the single most-preferred device (first of ``order_devices``)."""
    return order_devices(
        devices, target, project_config=project_config, command=command,
        scheduler_cfg=scheduler_cfg,
    )[0]


def pick_by_load(
    ranked: list[tuple[Device, float | None]],
    scheduler_cfg: dict[str, Any] | None = None,
) -> tuple[Device, str]:
    """Choose among already-reachable candidates (in preference order).

    ``ranked`` is ``[(device, cpu_busy_pct_or_None), ...]`` for reachable devices,
    most-preferred first. Returns ``(device, balance_reason)`` where reason is
    ``"auto"`` (kept the preferred device) or ``"auto-loadbalance"`` (moved to a
    much-less-congested device). Pure: callers do the probing and final labeling.
    """
    primary, primary_busy = ranked[0]
    sched = scheduler_cfg or {}
    if not sched.get("load_balance", True):
        return primary, "auto"
    floor = float(sched.get("busy_floor_pct", 40))
    margin = float(sched.get("headroom_margin_cores", 4))
    eff_w = float(sched.get("eff_core_weight", 0.5))

    # Don't move off a primary that isn't busy (or whose load is unknown).
    if primary_busy is None or primary_busy < floor:
        return primary, "auto"

    def spare(device: Device, busy: float | None) -> float | None:
        """Free perf-core-equivalents: capacity * idle-fraction.

        Weighs perf vs efficiency cores (capacity) against live CPU usage. With no
        core spec, capacity falls back to 1 so comparison degrades to idle-fraction
        (and the core-count margin won't trigger a move — conservative).
        """
        if busy is None:
            return None
        cap = device.cpu_capacity(eff_w) or 1.0
        return cap * (1.0 - min(max(busy, 0.0), 100.0) / 100.0)

    primary_spare = spare(primary, primary_busy)
    best_device, best_spare = primary, primary_spare
    for device, busy in ranked:
        s = spare(device, busy)
        if s is not None and (best_spare is None or s > best_spare):
            best_device, best_spare = device, s
    if (best_device is not primary and primary_spare is not None
            and best_spare is not None
            and (best_spare - primary_spare) >= margin):
        return best_device, "auto-loadbalance"
    return primary, "auto"
