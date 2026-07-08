"""Build a live DeviceSnapshot from a transport. Best-effort: every probe is
wrapped so a failure yields None/empty (placement treats unknown as "don't veto"),
and nothing here ever raises.
"""
from __future__ import annotations

from ..models import Device
from ..transport import BaseTransport, TransportError
from .adapters import ADAPTERS
from .models import DeviceSnapshot


def _root_cwd(device: Device) -> str:
    return "C:\\" if device.is_windows else "/"


def _exec_text(transport: BaseTransport, device: Device, tokens: list[str]) -> str | None:
    try:
        r = transport.exec(tokens, cwd=_root_cwd(device), telemetry=False, timeout=20)
    except (TransportError, Exception):  # noqa: BLE001 - probes must never raise
        return None
    if r.exit_code != 0:
        return None
    return (r.stdout or "").strip()


def _probe_vram(transport: BaseTransport, device: Device) -> tuple[float | None, float | None]:
    """(free_mb, total_mb) via nvidia-smi on a CUDA device; None on Macs / no GPU."""
    if not device.is_windows or device.vram_gb <= 0:
        return None, None
    out = _exec_text(transport, device,
                     ["nvidia-smi", "--query-gpu=memory.free,memory.total",
                      "--format=csv,noheader,nounits"])
    if not out:
        return None, None
    line = out.splitlines()[0]
    try:
        free, total = (float(x.strip()) for x in line.split(",")[:2])
        return free, total
    except (ValueError, IndexError):
        return None, None


def _probe_ram(transport: BaseTransport, device: Device) -> float | None:
    """AVAILABLE physical RAM in MB, best-effort, per OS.

    "Available" means what a new (model) process can claim WITHOUT paging out live memory — i.e.
    truly-free plus the instantly-reclaimable file cache. The two OS branches must agree on that
    definition or the same job is gated inconsistently across the fleet:
      * Windows: ``AvailableMBytes`` (free + standby/cache), NOT ``FreePhysicalMemory`` alone —
        the latter excludes the standby cache, so a box merely caching files reports far less RAM
        than it can actually hand out and gets wrongly RAM-skipped. Falls back to
        ``FreePhysicalMemory`` only if the perf class is unavailable.
      * macOS: ``free + inactive`` pages (below), inactive being the reclaimable half.
    """
    if device.is_windows:
        # Win32_PerfFormattedData_PerfOS_Memory.AvailableMBytes is language-neutral (unlike the
        # localized `\Memory\Available MBytes` Get-Counter path) and already in MB.
        out = _exec_text(transport, device,
                         ["powershell", "-NoProfile", "-Command",
                          "(Get-CimInstance Win32_PerfFormattedData_PerfOS_Memory).AvailableMBytes"])
        if out and out.strip():
            try:
                return float(out.split()[-1])            # already MB
            except (ValueError, IndexError):
                pass
        out = _exec_text(transport, device,
                         ["powershell", "-NoProfile", "-Command",
                          "(Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory"])
        if out:
            try:
                return float(out.split()[-1]) / 1024.0   # KB -> MB
            except (ValueError, IndexError):
                return None
        return None
    # POSIX (macOS): parse vm_stat (page size + free + inactive pages).
    out = _exec_text(transport, device, ["vm_stat"])
    if not out:
        return None
    try:
        page = 4096
        free_pages = inactive = 0
        for ln in out.splitlines():
            if "page size of" in ln:
                page = int("".join(c for c in ln if c.isdigit()))
            elif ln.startswith("Pages free:"):
                free_pages = int(ln.split(":")[1].strip().rstrip("."))
            elif ln.startswith("Pages inactive:"):
                inactive = int(ln.split(":")[1].strip().rstrip("."))
        return (free_pages + inactive) * page / (1024.0 * 1024.0)
    except (ValueError, IndexError):
        return None


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
        cpu = transport.sample_load()
    except (TransportError, Exception):  # noqa: BLE001
        cpu = None
    vram_free, vram_total = _probe_vram(transport, device)
    ram_free = _probe_ram(transport, device)
    engines = _capability_engines(transport, device) if probe_capability else frozenset()

    caps = dict(fleet_cfg.get("pools", {}))
    used = pool_used or {}
    pool_free = {p: int(cap) - int(used.get(p, 0)) for p, cap in caps.items()}

    return DeviceSnapshot(
        name=device.name, reachable=True, cpu_busy_pct=cpu,
        ram_free_mb=ram_free, ram_total_mb=(device.ram_gb * 1024.0 if device.ram_gb else None),
        vram_free_mb=vram_free,
        vram_total_mb=(vram_total if vram_total is not None
                       else (device.vram_gb * 1024.0 if device.vram_gb else None)),
        active_jobs=active_jobs, max_jobs=device.max_jobs, pool_free=pool_free,
        engines_available=engines, detail=pr.detail,
    )
