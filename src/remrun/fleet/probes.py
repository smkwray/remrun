"""Build live device snapshots with tri-state adapter qualification."""
from __future__ import annotations

from ..models import Device
from ..resource_envelope import MIB, Metric
from ..resource_probe import GPUResourceSnapshot, ResourceSnapshot, probe_target_resources
from ..transport import BaseTransport, TransportError
from .models import DeviceSnapshot

_RESOURCE_PROBE_TIMEOUT_SEC = 20.0


def _metric_mb(metric: Metric) -> float | None:
    if metric.value is None:
        return None
    return float(metric.value) / MIB


def _first_discrete_gpu(snapshot: ResourceSnapshot) -> GPUResourceSnapshot | None:
    if snapshot.gpu_kind != "discrete" or not snapshot.gpus:
        return None
    # DeviceSnapshot is intentionally a compact single-GPU view. Preserve its
    # established first-device behavior while the normalized snapshot retains
    # every GPU (with stable IDs) for newer consumers.
    return snapshot.gpus[0]


def _capability_engines(transport: BaseTransport,
                        adapter_specs: list[dict]) -> dict[str, str]:
    """Qualify every frozen adapter independently as present/absent/unknown."""
    status: dict[str, str] = {}
    for spec in adapter_specs:
        engine = spec["engine"]
        paths = list(spec.get("capability_paths") or [])
        if not paths:
            status[engine] = "unknown"
            continue
        try:
            present = all(transport.remote_path_exists(path) for path in paths)
        except Exception:  # noqa: BLE001 - unknown is distinct from absent
            status[engine] = "unknown"
        else:
            status[engine] = "present" if present else "absent"
    return status


def build_snapshot(device: Device, transport: BaseTransport, fleet_cfg: dict, *,
                   active_jobs: int = 0, pool_used: dict[str, int] | None = None,
                   probe_capability: bool = True,
                   adapter_specs: list[dict] | None = None) -> DeviceSnapshot:
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
    specs = list(adapter_specs or [])
    engines = (_capability_engines(transport, specs) if probe_capability else
               {spec["engine"]: "unknown" for spec in specs})

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
        engine_status=engines, detail=pr.detail,
    )
