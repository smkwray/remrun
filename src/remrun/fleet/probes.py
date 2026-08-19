"""Build live device snapshots with tri-state adapter qualification."""
from __future__ import annotations

import json
import os
from pathlib import Path

from ..models import Device
from ..resource_envelope import MIB, Metric
from ..resource_probe import GPUResourceSnapshot, ResourceSnapshot, probe_target_resources
from ..scheduler import is_self
from ..transport import BaseTransport, TransportError
from . import local_resources
from .models import DeviceSnapshot

_RESOURCE_PROBE_TIMEOUT_SEC = 20.0
_CAPABILITY_PROBE_PROGRAM = r"""
import json
import os
import sys

paths = json.loads(sys.argv[1])
result = []
for raw in paths:
    try:
        path = os.path.expanduser(os.path.expandvars(raw))
        result.append(os.path.exists(path))
    except Exception:
        result.append(None)
print(json.dumps(result, separators=(",", ":")))
""".strip()


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
                        adapter_specs: list[dict], *,
                        device: Device) -> dict[str, str]:
    """Qualify every frozen adapter independently as present/absent/unknown."""
    paths = list(dict.fromkeys(
        path
        for spec in adapter_specs
        for path in list(spec.get("capability_paths") or [])
    ))
    if not paths:
        return {spec["engine"]: "unknown" for spec in adapter_specs}
    try:
        result = transport.exec(
            [device.remote_python or ("python" if device.is_windows else "python3"),
             "-c", _CAPABILITY_PROBE_PROGRAM, json.dumps(paths)],
            cwd="C:\\" if device.is_windows else "/",
            timeout=_RESOURCE_PROBE_TIMEOUT_SEC,
            telemetry=False,
        )
        values = json.loads(result.stdout) if result.exit_code == 0 else None
        if (
            not isinstance(values, list)
            or len(values) != len(paths)
            or any(
                value is not True and value is not False and value is not None
                for value in values
            )
        ):
            raise ValueError("invalid capability probe result")
        observed = dict(zip(paths, values))
    except Exception:  # noqa: BLE001 - failed qualification is unknown, never absent
        return {spec["engine"]: "unknown" for spec in adapter_specs}

    status: dict[str, str] = {}
    for spec in adapter_specs:
        engine_paths = list(spec.get("capability_paths") or [])
        engine_values = [observed[path] for path in engine_paths]
        if not engine_values or any(value is None for value in engine_values):
            status[spec["engine"]] = "unknown"
        else:
            status[spec["engine"]] = "present" if all(engine_values) else "absent"
    return status


def _local_capability_engines(adapter_specs: list[dict]) -> dict[str, str]:
    """Qualify configured controller paths without opening an SSH connection."""
    status: dict[str, str] = {}
    for spec in adapter_specs:
        engine = spec["engine"]
        paths = list(spec.get("capability_paths") or [])
        if not paths:
            status[engine] = "unknown"
            continue
        try:
            present = all(
                Path(os.path.expandvars(path)).expanduser().exists()
                for path in paths
            )
        except (OSError, TypeError, ValueError):
            status[engine] = "unknown"
        else:
            status[engine] = "present" if present else "absent"
    return status


def _pool_free(fleet_cfg: dict, pool_used: dict[str, int] | None) -> dict[str, int]:
    caps = dict(fleet_cfg.get("pools", {}))
    used = pool_used or {}
    return {pool: int(cap) - int(used.get(pool, 0)) for pool, cap in caps.items()}


def _build_local_snapshot(
    device: Device,
    fleet_cfg: dict,
    *,
    active_jobs: int,
    pool_used: dict[str, int] | None,
    probe_capability: bool,
    adapter_specs: list[dict],
) -> DeviceSnapshot:
    view = local_resources.local_view(name=device.name, timeout=_RESOURCE_PROBE_TIMEOUT_SEC)
    engines = (
        _local_capability_engines(adapter_specs)
        if probe_capability
        else {spec["engine"]: "unknown" for spec in adapter_specs}
    )
    return DeviceSnapshot(
        name=device.name,
        reachable=view.reachable,
        cpu_busy_pct=view.cpu_busy_pct,
        ram_free_mb=view.ram_free_mb,
        ram_total_mb=(device.ram_gb * 1024.0 if device.ram_gb else view.ram_total_mb),
        vram_free_mb=view.vram_free_mb,
        vram_total_mb=(view.vram_total_mb if view.vram_total_mb is not None
                       else (device.vram_gb * 1024.0 if device.vram_gb else None)),
        active_jobs=active_jobs,
        max_jobs=device.max_jobs,
        pool_free=_pool_free(fleet_cfg, pool_used),
        engine_status=engines,
        detail=view.detail or "local controller",
    )


def build_snapshot(device: Device, transport: BaseTransport, fleet_cfg: dict, *,
                   active_jobs: int = 0, pool_used: dict[str, int] | None = None,
                   probe_capability: bool = True,
                   adapter_specs: list[dict] | None = None) -> DeviceSnapshot:
    """Probe one device. Reachability gates the rest; everything else degrades to
    None/empty on failure."""
    # A configured controller is still a device, but probing it through its own
    # SSH alias adds latency and can fail on controller-only key policy. LocalSim
    # remains transport-backed because it is a test target, not the controller.
    specs = list(adapter_specs or [])
    if device.kind != "local-sim" and is_self(device):
        return _build_local_snapshot(
            device, fleet_cfg, active_jobs=active_jobs, pool_used=pool_used,
            probe_capability=probe_capability, adapter_specs=specs,
        )
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
    engines = (_capability_engines(transport, specs, device=device) if probe_capability else
               {spec["engine"]: "unknown" for spec in specs})

    return DeviceSnapshot(
        name=device.name, reachable=True, cpu_busy_pct=cpu,
        ram_free_mb=ram_free,
        ram_total_mb=(device.ram_gb * 1024.0 if device.ram_gb else measured_ram_total),
        vram_free_mb=vram_free,
        vram_total_mb=(vram_total if vram_total is not None
                       else (device.vram_gb * 1024.0 if device.vram_gb else None)),
        active_jobs=active_jobs, max_jobs=device.max_jobs,
        pool_free=_pool_free(fleet_cfg, pool_used),
        engine_status=engines, detail=pr.detail,
    )
