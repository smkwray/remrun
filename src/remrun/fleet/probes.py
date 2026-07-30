"""Build a live DeviceSnapshot from a transport. Best-effort: every probe is
wrapped so a failure yields None/empty (placement treats unknown as "don't veto"),
and nothing here ever raises.
"""
from __future__ import annotations

from ..models import Device
from ..resource_envelope import MIB, Metric
from ..resource_probe import GPUResourceSnapshot, ResourceSnapshot, probe_target_resources
from ..transport import BaseTransport, TransportError
from .adapters import ADAPTERS
from .models import DeviceSnapshot

_RESOURCE_PROBE_TIMEOUT_SEC = 20.0


def _metric_mb(metric: Metric) -> float | None:
    if metric.value is None:
        return None
    return float(metric.value) / MIB


def _first_discrete_gpu(snapshot: ResourceSnapshot) -> GPUResourceSnapshot | None:
    if snapshot.gpu_kind != "discrete" or not snapshot.gpus:
        return None
    # DeviceSnapshot is intentionally a legacy single-GPU view. Preserve its
    # established first-device behavior while the normalized snapshot retains
    # every GPU (with stable IDs) for newer consumers.
    return snapshot.gpus[0]


def _capability_engines(transport: BaseTransport, device: Device) -> frozenset[str]:
    """Engines whose required script/model paths exist on the device."""
    engines = set()
    for (task_type, dev), spec in ADAPTERS.items():
        if dev != device.name:
            continue
        try:
            present = all(transport.remote_path_exists(p) for p in spec.get("capability", []))
        except (TransportError, NotImplementedError, Exception):  # noqa: BLE001
            present = False
        if present:
            engines.add(spec["engine"])
    return frozenset(engines)


def build_snapshot(device: Device, transport: BaseTransport, fleet_cfg: dict, *,
                   active_jobs: int = 0, pool_used: dict[str, int] | None = None,
                   probe_capability: bool = True) -> DeviceSnapshot:
    """Probe one device. Reachability gates the rest; everything else degrades to
    None/empty on failure."""
    try:
        pr = transport.probe()
    except (TransportError, Exception):  # noqa: BLE001
        return DeviceSnapshot(name=device.name, reachable=False, detail="probe raised")
    if not pr.reachable:
        return DeviceSnapshot(name=device.name, reachable=False, detail=pr.detail)

    try:
        resources = probe_target_resources(
            transport,
            device,
            timeout_sec=_RESOURCE_PROBE_TIMEOUT_SEC,
        )
    except Exception:  # noqa: BLE001 - keep the placement probe no-throw
        resources = None

    cpu = (
        float(resources.cpu_busy_pct.value)
        if resources is not None and resources.cpu_busy_pct.value is not None
        else None
    )
    ram_free = (
        _metric_mb(resources.ram_available_bytes)
        if resources is not None
        else None
    )
    measured_ram_total = (
        _metric_mb(resources.ram_total_bytes)
        if resources is not None
        else None
    )
    gpu = _first_discrete_gpu(resources) if resources is not None else None
    vram_free = (
        float(gpu.vram_free_bytes.value) / MIB
        if gpu is not None and gpu.vram_free_bytes.value is not None
        else None
    )
    vram_total = (
        float(gpu.vram_total_bytes.value) / MIB
        if gpu is not None and gpu.vram_total_bytes.value is not None
        else None
    )
    engines = _capability_engines(transport, device) if probe_capability else frozenset()

    caps = dict(fleet_cfg.get("pools", {}))
    used = pool_used or {}
    pool_free = {p: int(cap) - int(used.get(p, 0)) for p, cap in caps.items()}

    return DeviceSnapshot(
        name=device.name, reachable=True, cpu_busy_pct=cpu,
        ram_free_mb=ram_free,
        ram_total_mb=(device.ram_gb * 1024.0 if device.ram_gb else measured_ram_total),
        vram_free_mb=vram_free,
        vram_total_mb=(vram_total if vram_total is not None
                       else (device.vram_gb * 1024.0 if device.vram_gb else None)),
        active_jobs=active_jobs, max_jobs=device.max_jobs, pool_free=pool_free,
        engines_available=engines, detail=pr.detail,
    )
