"""Build live device snapshots with tri-state adapter qualification."""
from __future__ import annotations

import ipaddress
import json
import os
import socket
import sys
from dataclasses import replace
from pathlib import Path

from ..models import Device
from ..resource_envelope import MIB, Metric
from ..resource_probe import GPUResourceSnapshot, ResourceSnapshot, probe_target_resources
from ..transport import BaseTransport, TransportError, make_transport
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


def _controller_os_matches(device: Device) -> bool:
    """Require the configured controller marker to agree with this process' OS."""
    configured = device.os.strip().casefold()
    if os.name == "nt":
        return device.is_windows and device.kind == "ssh-powershell"
    if sys.platform == "darwin":
        return configured in {"macos", "darwin"} and device.kind == "ssh-posix"
    return configured in {"linux", "posix"} and device.kind == "ssh-posix"


def _normalize_host_token(value: str) -> str:
    token = str(value or "").strip().casefold().rstrip(".")
    if token.startswith("[") and token.endswith("]"):
        token = token[1:-1]
    return token.split("%", 1)[0]


def _local_host_identity() -> tuple[set[str], set[str]]:
    """Return local host aliases and addresses for positive controller evidence."""
    aliases: set[str] = {"localhost", "localhost.localdomain"}
    addresses: set[str] = {"127.0.0.1", "::1"}
    for raw in (socket.gethostname(), socket.getfqdn()):
        token = _normalize_host_token(raw)
        if not token:
            continue
        aliases.add(token)
        aliases.add(token.split(".", 1)[0])
    return aliases, addresses


def _address_is_local(token: str, addresses: set[str]) -> bool:
    """Return true only when an IP is loopback, resolved local, or locally bindable."""
    try:
        address = ipaddress.ip_address(token)
    except ValueError:
        return False
    if address.is_unspecified or address.is_multicast:
        return False
    if address.version == 4 and int(address) == (1 << 32) - 1:
        return False
    if address.is_loopback or token in addresses:
        return True
    family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
    bind_address = (token, 0, 0, 0) if family == socket.AF_INET6 else (token, 0)
    try:
        with socket.socket(family, socket.SOCK_DGRAM) as probe:
            probe.bind(bind_address)
    except OSError:
        return False
    return True


def _host_token_is_local(value: str, aliases: set[str], addresses: set[str]) -> bool:
    token = _normalize_host_token(value)
    if not token:
        return False
    if _address_is_local(token, addresses):
        return True
    try:
        resolved = {
            _normalize_host_token(info[4][0])
            for info in socket.getaddrinfo(token, None)
        }
    except OSError:
        return False
    return bool(resolved) and all(
        _address_is_local(address, addresses)
        for address in resolved
    )


def _controller_host_matches(device: Device) -> bool:
    """Require non-contradictory name and address evidence for this exact host."""
    aliases, addresses = _local_host_identity()
    if _normalize_host_token(device.name) not in aliases:
        return False
    candidates = device.all_addresses()
    return bool(candidates) and all(
        _host_token_is_local(candidate, aliases, addresses)
        for candidate in candidates
    )


def _is_local_controller(device: Device) -> bool:
    """Positive local substitution requires explicit and corroborating identity."""
    return (
        device.kind != "local-sim"
        and device.role.strip().casefold() == "controller"
        and _controller_os_matches(device)
        and _controller_host_matches(device)
    )


def _fixed_probe_transport(device: Device) -> BaseTransport:
    """Transport for fixed read-only probes; never stages the command memory guard."""
    return make_transport(replace(device, memory_guard=None))


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


def build_snapshot(device: Device, transport: BaseTransport | None, fleet_cfg: dict, *,
                   active_jobs: int = 0, pool_used: dict[str, int] | None = None,
                   probe_capability: bool = True,
                   adapter_specs: list[dict] | None = None) -> DeviceSnapshot:
    """Probe one device. Reachability gates the rest; everything else degrades to
    None/empty on failure."""
    # A configured controller is still a device, but probing it through its own
    # SSH alias adds latency and can fail on controller-only key policy. LocalSim
    # remains transport-backed because it is a test target, not the controller.
    specs = list(adapter_specs or [])
    if _is_local_controller(device):
        return _build_local_snapshot(
            device, fleet_cfg, active_jobs=active_jobs, pool_used=pool_used,
            probe_capability=probe_capability, adapter_specs=specs,
        )
    try:
        transport = transport or _fixed_probe_transport(device)
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
