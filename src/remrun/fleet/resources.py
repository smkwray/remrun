"""Live hardware view of the fleet: CPU / RAM / GPU per device.

Separate from ``probes.py`` on purpose. ``probes.build_snapshot`` feeds the
placement gate, where a wrong number silently misroutes real jobs; this module
only ever renders a report, so it can ask for more (GPU utilization, live RAM
total, core counts) without widening the blast radius of a probe change.

Everything here is best-effort: one combined command per device, and any field
that could not be measured stays None rather than becoming a plausible-looking
zero. A device that cannot be reached carries the reason, not a bare dash.
"""
from __future__ import annotations

import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from threading import Lock

from ..models import Device
from ..transport import BaseTransport, TransportError, make_transport

# No console-window flash on Windows when invoked from a GUI trigger; 0 elsewhere.
_NO_WINDOW_FLAG = getattr(subprocess, "CREATE_NO_WINDOW", 0)

# One SSH round trip per device. Both scripts print `KEY=value` lines and must
# never fail as a whole: a missing tool yields a missing key, not a bad exit.
_POSIX_SCRIPT = r"""
echo "HOST=$(hostname 2>/dev/null)"
echo "NCPU=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null)"
echo "MEMTOTAL=$(sysctl -n hw.memsize 2>/dev/null)"
echo "CHIP=$(sysctl -n machdep.cpu.brand_string 2>/dev/null)"
echo "LOADAVG=$(sysctl -n vm.loadavg 2>/dev/null | tr -d '{}' || cat /proc/loadavg 2>/dev/null)"
# ACTUAL cpu busy. Load average is NOT this: it counts runnable AND blocked
# threads, so a box waiting on disk or network reports a high load while its
# cores idle. Measured on a 14-core laptop: load 10.84/14 = "77% busy", while top
# reported 66% idle. `top -l 2` discards the first sample, which is a since-boot
# average rather than a current reading.
top -l 2 -n 0 2>/dev/null | grep "CPU usage" | tail -1 | sed 's/^/CPUUSAGE:/'
vm_stat 2>/dev/null | sed 's/^/VMSTAT:/'
if [ -r /proc/meminfo ]; then sed 's/^/MEMINFO:/' /proc/meminfo 2>/dev/null | head -5; fi
# Apple GPU utilization, no sudo required.
ioreg -r -d 1 -w 0 -c AGXAccelerator 2>/dev/null \
  | grep -o '"Device Utilization %"=[0-9]*' | head -1 | sed 's/^/AGX:/'
# Discrete NVIDIA (Linux boxes in the mesh).
nvidia-smi --query-gpu=name,utilization.gpu,memory.free,memory.total \
  --format=csv,noheader,nounits 2>/dev/null | head -1 | sed 's/^/NVIDIA:/'
exit 0
"""

_WINDOWS_SCRIPT = r"""
$ErrorActionPreference = 'SilentlyContinue'
Write-Output "HOST=$env:COMPUTERNAME"
$os = Get-CimInstance Win32_OperatingSystem
Write-Output "MEMTOTAL_KB=$($os.TotalVisibleMemorySize)"
$m = Get-CimInstance Win32_PerfFormattedData_PerfOS_Memory
Write-Output "RAM_AVAIL_MB=$($m.AvailableMBytes)"
Write-Output "MEMFREE_KB=$($os.FreePhysicalMemory)"
$p = Get-CimInstance Win32_PerfFormattedData_PerfOS_Processor |
     Where-Object { $_.Name -eq '_Total' }
Write-Output "CPU_IDLE=$($p.PercentIdleTime)"
$c = @(Get-CimInstance Win32_Processor)
Write-Output "NCPU=$($c[0].NumberOfLogicalProcessors)"
Write-Output "CHIP=$($c[0].Name)"
$g = nvidia-smi --query-gpu=name,utilization.gpu,memory.free,memory.total --format=csv,noheader,nounits
if ($g) { Write-Output "NVIDIA:$(@($g)[0])" }
exit 0
"""


@dataclass
class ResourceView:
    """One device's live hardware state. Any field may be None (not measured)."""
    name: str
    reachable: bool
    # Reason the device could not be reached, in the operator's terms.
    detail: str = ""
    os: str = ""
    hostname: str = ""
    chip: str = ""
    cpu_busy_pct: float | None = None
    cpu_count: int | None = None
    # 1-minute load average and that figure per core. Distinct from cpu_busy_pct:
    # load counts runnable AND blocked threads, so it reveals queueing/contention
    # that a utilization percentage cannot. >1.0 per core means more work is
    # waiting than there are cores to run it.
    load1: float | None = None
    load_per_core: float | None = None
    ram_free_mb: float | None = None
    ram_total_mb: float | None = None
    gpu_name: str = ""
    gpu_util_pct: float | None = None
    vram_free_mb: float | None = None
    vram_total_mb: float | None = None
    # True when the GPU shares system RAM (Apple silicon), so a VRAM figure
    # would be a fiction rather than a measurement.
    gpu_unified: bool = False
    # Set when this row is the controller running the command.
    is_local: bool = False
    notes: list[str] = field(default_factory=list)

    @property
    def oversubscribed(self) -> bool | None:
        """More work queued than cores to run it (load per core > 1).

        None when unmeasured. A box can be oversubscribed while showing modest
        CPU busy — that is contention (threads waiting on each other or on I/O),
        and it is the state that makes a device a bad place to send more work.
        """
        if self.load_per_core is None:
            return None
        return self.load_per_core > 1.0

    @property
    def load_label(self) -> str:
        """Short human verdict on queueing pressure."""
        if self.load_per_core is None:
            return "-"
        ratio = self.load_per_core
        if ratio > 2.0:
            return f"{ratio:.1f}x OVERSUB"
        if ratio > 1.0:
            return f"{ratio:.1f}x busy"
        return f"{ratio:.2f}x"

    @property
    def ram_used_pct(self) -> float | None:
        if not self.ram_total_mb or self.ram_free_mb is None:
            return None
        return max(0.0, min(100.0, (1.0 - self.ram_free_mb / self.ram_total_mb) * 100.0))


def _kv(out: str) -> dict[str, str]:
    """Parse the `KEY=value` lines both probe scripts emit."""
    found: dict[str, str] = {}
    for line in out.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        # Keep the FIRST occurrence: prefixed lines (VMSTAT:, NVIDIA:) are
        # handled separately and must not clobber a real key.
        if key and key not in found:
            found[key] = value.strip()
    return found


def _num(text: str | None) -> float | None:
    """First number in `text`, or None. Never raises, never invents a zero."""
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group())
    except ValueError:
        return None


def _parse_nvidia(line: str, view: ResourceView) -> None:
    """`name, util%, free_mb, total_mb` from nvidia-smi's CSV row."""
    parts = [p.strip() for p in line.split(",")]
    if len(parts) < 4:
        return
    view.gpu_name = parts[0]
    view.gpu_util_pct = _num(parts[1])
    view.vram_free_mb = _num(parts[2])
    view.vram_total_mb = _num(parts[3])


def _parse_vm_stat(lines: list[str], view: ResourceView) -> None:
    """Available RAM = (free + inactive) pages.

    Matches ``probes._probe_ram``'s definition deliberately: "available" means
    what a new process can claim without paging out live memory, so the two
    surfaces cannot disagree about the same box.
    """
    page = 4096
    free_pages = inactive = 0
    seen = False
    for line in lines:
        if "page size of" in line:
            size = _num(line.split("page size of")[-1])
            if size:
                page = int(size)
        elif line.startswith("Pages free:"):
            free_pages = int(_num(line) or 0)
            seen = True
        elif line.startswith("Pages inactive:"):
            inactive = int(_num(line) or 0)
            seen = True
    if seen:
        view.ram_free_mb = (free_pages + inactive) * page / (1024.0 * 1024.0)


def _parse_posix(out: str, view: ResourceView) -> None:
    kv = _kv(out)
    view.hostname = kv.get("HOST", "")
    view.chip = kv.get("CHIP", "")
    ncpu = _num(kv.get("NCPU"))
    view.cpu_count = int(ncpu) if ncpu else None
    total = _num(kv.get("MEMTOTAL"))
    if total:
        view.ram_total_mb = total / (1024.0 * 1024.0)   # bytes -> MB

    vm_lines = [ln[len("VMSTAT:"):] for ln in out.splitlines()
                if ln.startswith("VMSTAT:")]
    if vm_lines:
        _parse_vm_stat(vm_lines, view)

    # Linux fallback when vm_stat is absent.
    if view.ram_free_mb is None:
        for line in out.splitlines():
            if not line.startswith("MEMINFO:"):
                continue
            body = line[len("MEMINFO:"):]
            if body.startswith("MemAvailable:"):
                kb = _num(body)
                if kb:
                    view.ram_free_mb = kb / 1024.0
            elif body.startswith("MemTotal:") and not view.ram_total_mb:
                kb = _num(body)
                if kb:
                    view.ram_total_mb = kb / 1024.0

    # Real utilization first: `CPU usage: 27.29% user, 6.40% sys, 66.29% idle`.
    for line in out.splitlines():
        if line.startswith("CPUUSAGE:") and "idle" in line:
            idle = _num(line.rsplit(",", 1)[-1])
            if idle is not None:
                view.cpu_busy_pct = max(0.0, min(100.0, 100.0 - idle))
            break

    # Load average is kept as a SEPARATE signal, never as cpu_busy_pct: it counts
    # blocked threads too, so it answers "is this box oversubscribed / is work
    # queueing" rather than "how busy are the cores".
    load1 = _num(kv.get("LOADAVG"))
    if load1 is not None:
        view.load1 = load1
        if view.cpu_count:
            view.load_per_core = load1 / view.cpu_count
        # Only fall back to load when top gave us nothing, and say so, since the
        # two are not the same measurement.
        if view.cpu_busy_pct is None and view.cpu_count:
            view.cpu_busy_pct = min(100.0, load1 / view.cpu_count * 100.0)
            view.notes.append("cpu from loadavg (top unavailable)")

    for line in out.splitlines():
        if line.startswith("AGX:"):
            view.gpu_unified = True
            view.gpu_util_pct = _num(line)
            if not view.gpu_name:
                view.gpu_name = view.chip or "Apple GPU"
        elif line.startswith("NVIDIA:"):
            _parse_nvidia(line[len("NVIDIA:"):], view)


def _parse_windows(out: str, view: ResourceView) -> None:
    kv = _kv(out)
    view.hostname = kv.get("HOST", "")
    view.chip = kv.get("CHIP", "")
    ncpu = _num(kv.get("NCPU"))
    view.cpu_count = int(ncpu) if ncpu else None

    total_kb = _num(kv.get("MEMTOTAL_KB"))
    if total_kb:
        view.ram_total_mb = total_kb / 1024.0
    # AvailableMBytes (free + standby) is the figure a new process can claim;
    # FreePhysicalMemory alone understates a box that is merely caching files.
    avail = _num(kv.get("RAM_AVAIL_MB"))
    if avail is None:
        free_kb = _num(kv.get("MEMFREE_KB"))
        avail = free_kb / 1024.0 if free_kb else None
    view.ram_free_mb = avail

    idle = _num(kv.get("CPU_IDLE"))
    if idle is not None:
        view.cpu_busy_pct = max(0.0, min(100.0, 100.0 - idle))

    for line in out.splitlines():
        if line.startswith("NVIDIA:"):
            _parse_nvidia(line[len("NVIDIA:"):], view)


# Ordered most- to least-diagnostic. The transport tries every address candidate
# and reports only the LAST failure, so a device that genuinely answered on its
# Tailscale IP but has an unresolvable trailing alias would otherwise be blamed
# on DNS. Reaching sshd and being refused is a far more specific fact than a name
# that did not resolve, so it wins regardless of candidate order.
_ERROR_RANK = (
    ("permission denied", "ssh auth refused (no authorized key for this controller)"),
    ("host key verification failed", "host key not trusted (connect once to accept it)"),
    ("connection refused", "connection refused (sshd not running / not enabled)"),
    # "offline" would be a guess: a heavily loaded but healthy box times out too.
    # Say what was observed (no answer in the budget) and name both causes.
    ("connection timed out", "no answer within timeout (offline, or too loaded to reply)"),
    ("operation timed out", "no answer within timeout (offline, or too loaded to reply)"),
    ("no route to host", "no route to host (device offline)"),
    ("could not resolve", "hostname did not resolve"),
    ("name or service not known", "hostname did not resolve"),
)


def _friendly_error(detail: str) -> str:
    """Turn ssh's noise into the one line that says what to fix.

    Scans the WHOLE detail (which may cover several address candidates) and
    reports the most specific failure found, not merely the last one.
    """
    text = (detail or "").strip()
    low = text.lower()

    # Most specific of all: the configured key does not exist on this box, so no
    # candidate could ever authenticate. Reported with the path, hence not tabled.
    if "not accessible" in low and "identity file" in low:
        match = re.search(r"Identity file (\S+) not accessible", text)
        missing = match.group(1) if match else "the configured key"
        return f"ssh key missing on this controller: {missing}"

    for needle, message in _ERROR_RANK:
        if needle in low:
            return message
    if "timed out" in low or "timeout" in low:
        return "no answer within timeout (offline, or too loaded to reply)"
    # Keep it to one line; ssh banners can run long.
    first = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")
    return first[:160] or "unreachable"


def _diagnose(device: Device, fallback: str, timeout: float = 10.0) -> str:
    """Most specific reason this device is unreachable, across ALL its addresses.

    Diagnosis only — never used to decide reachability, so a flaky extra probe
    cannot promote a dead device to live. Falls back to the transport's own
    detail if these attempts reveal nothing better.
    """
    best = _friendly_error(fallback)
    best_rank = _rank_of(best)
    if best_rank == 0:                      # already the most specific possible
        return best

    # Only trust the device's OWN addresses. A bare alias like `bmni` is
    # resolved by whatever DNS the controller happens to be using, and a router
    # that answers every unknown name with its own address (measured here:
    # `bmni`, `bmni.local`, `bmfs` all -> 192.168.42.1, the gateway) turns this
    # diagnosis into a report about the ROUTER — "host key not trusted" or
    # "hostname did not resolve" for a device that is perfectly reachable on its
    # tailnet IP. Prefer the IP; fall back to aliases only if there is no IP.
    candidates = [device.tailscale_ip] if device.tailscale_ip else device.all_addresses()

    for address in candidates:
        target = f"{device.user}@{address}" if device.user else address
        # `exit 0`, not `true`: a Windows sshd runs the command through
        # PowerShell, which has no `true` builtin and would fail a working login.
        command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                   "-o", "StrictHostKeyChecking=accept-new", target, "exit 0"]
        try:
            proc = subprocess.run(command, capture_output=True, timeout=timeout,
                                  check=False, creationflags=_NO_WINDOW_FLAG)
        except (OSError, subprocess.SubprocessError):
            continue
        if proc.returncode == 0:
            # Reachable by hand but not via the transport: report that honestly
            # rather than inventing a cause.
            return "reachable by ssh but the remrun probe failed (check device config)"
        text = (proc.stderr or proc.stdout).decode("utf-8", "replace").strip()
        message = _friendly_error(text)
        rank = _rank_of(message)
        if rank < best_rank:
            best, best_rank = message, rank
    return best


def _rank_of(message: str) -> int:
    """Lower is more specific. Unknown messages sort last."""
    for index, (_, candidate) in enumerate(_ERROR_RANK):
        if message == candidate:
            return index
    return 0 if message.startswith("ssh key missing") else len(_ERROR_RANK) + 1


def _is_timeout(detail: str) -> bool:
    low = (detail or "").lower()
    return "timed out" in low or "timeout" in low


def probe_device(device: Device, transport: BaseTransport | None = None,
                 timeout: float = 45.0, retries: int = 1) -> ResourceView:
    """Live hardware for one device. Never raises.

    A TIMEOUT is retried before the device is called unreachable. `probe()` uses
    a fixed 20 s ssh budget (transport.py:876-884), which a heavily loaded box
    can miss while being perfectly healthy — a workstation answered fine by hand at load
    average 8.4 while a concurrent fleet probe reported it offline. Reporting a
    critical device as down because it was busy is the worst failure this
    surface can have, so slowness must not be mistaken for absence.
    """
    view = ResourceView(name=device.name, reachable=False, os=device.os)
    try:
        transport = transport or make_transport(device)
    except Exception as exc:                                  # noqa: BLE001
        view.detail = _friendly_error(str(exc))
        return view

    probe = None
    for attempt in range(retries + 1):
        try:
            probe = transport.probe()
        except (TransportError, Exception) as exc:            # noqa: BLE001
            probe = None
            detail = str(exc)
        else:
            if probe.reachable:
                break
            detail = probe.detail
        # Only a timeout is worth retrying: auth refusal and DNS are immediate
        # and deterministic, so retrying them just doubles the wait.
        if attempt < retries and _is_timeout(detail):
            view.notes.append("slow to respond; retried")
            continue
        break

    if probe is None:
        view.detail = _friendly_error(detail)
        return view
    if not probe.reachable:
        # `probe()` keeps only the LAST candidate's error (transport.py:877-897
        # overwrites last_detail per attempt), so a device that answered on its
        # Tailscale IP and refused the key gets reported as a DNS failure by a
        # trailing unresolvable alias. Re-walk the candidates to find the most
        # specific reason before believing that.
        view.detail = _diagnose(device, probe.detail)
        return view

    if device.is_windows:
        command = ["powershell", "-NoProfile", "-Command", _WINDOWS_SCRIPT]
        cwd = "C:\\"
    else:
        command = [device.shell or "bash", "-lc", _POSIX_SCRIPT]
        cwd = "/"

    try:
        result = transport.exec(command, cwd=cwd, telemetry=False, timeout=timeout)
    except (TransportError, Exception) as exc:                # noqa: BLE001
        view.detail = _friendly_error(str(exc))
        return view

    view.reachable = True
    out = result.stdout or ""
    if device.is_windows:
        _parse_windows(out, view)
    else:
        _parse_posix(out, view)

    # Configured hardware fills only what the probe could not measure, and says
    # so; a static figure must never masquerade as a live reading.
    if view.ram_total_mb is None and device.ram_gb:
        view.ram_total_mb = device.ram_gb * 1024.0
        view.notes.append("ram_total from config")
    if view.vram_total_mb is None and device.vram_gb:
        view.vram_total_mb = device.vram_gb * 1024.0
        view.notes.append("vram_total from config")
    if not view.reachable and not view.detail:
        view.detail = "unreachable"
    return view


def probe_fleet(devices: list[Device], *, max_workers: int = 8,
                timeout: float = 45.0,
                on_event=None) -> list[ResourceView]:
    """Probe every device concurrently, preserving the given order.

    Mirrors the dispatcher's ThreadPoolExecutor contract: bounded workers, one
    transport per device (no shared connection), and workers that never raise —
    so one dead box cannot stall or crash the report.

    ``on_event(kind, name, view)`` is called as work starts and finishes, so a
    caller can show progress instead of a blank terminal. It is invoked from
    worker threads and must be cheap and thread-safe.
    """
    if not devices:
        return []
    workers = max(1, min(max_workers, len(devices)))
    lock = Lock()

    def run(device: Device) -> ResourceView:
        if on_event:
            with lock:
                on_event("start", device.name, None)
        view = probe_device(device, timeout=timeout)
        if on_event:
            with lock:
                on_event("done", device.name, view)
        return view

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(run, devices))
