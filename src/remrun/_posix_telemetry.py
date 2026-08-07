"""Detailed POSIX process-tree telemetry wrapper for remrun.

Usage: ``python _posix_telemetry.py --detailed -- <argv...>``

The command after ``--`` is launched with its argv unchanged.  The helper samples
the known descendant tree every 200 ms, then emits one JSON sentinel on stderr.
It is deliberately stdlib-only because transports stage it on the target.
"""
from __future__ import annotations

import base64
import binascii
import csv
import ctypes
import fcntl
import hashlib
import io
import json
import math
import os
import platform
import re
import resource
import select
import signal
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

TELEMETRY_MARKER = "\n__REMRUN_TELEMETRY__ "
MEMORY_ADMISSION_MARKER = "__REMRUN_MEMORY_ADMISSION__ "
SAMPLE_INTERVAL_S = 0.2
PRESSURE_SAMPLE_INTERVAL_S = 1.0
GPU_SAMPLE_INTERVAL_S = 5.0 if sys.platform == "darwin" else 1.0
DESCENDANT_GRACE_S = 1.0
MIB = 1024 * 1024
PREDICTION_HEADROOM_FACTOR = 1.25
CONTROL_OVERHEAD_HEADROOM_FACTOR = 2.0
DEFAULT_RESERVATION_TTL_S = 30 * 60
_ADMISSION_SCHEMA = 1
_LEDGER_SCHEMA = 1
_DARWIN_HOST_PORT: int | None = None
_DARWIN_TOTAL_MEMORY: int | None = None
_DARWIN_LIBSYSTEM = None

PROC_PIDTBSDINFO = 3
PROC_PIDTASKINFO = 4
PROC_PIDREGIONINFO = 7


class _ProcBSDInfo(ctypes.Structure):
    _fields_ = [
        ("pbi_flags", ctypes.c_uint32),
        ("pbi_status", ctypes.c_uint32),
        ("pbi_xstatus", ctypes.c_uint32),
        ("pbi_pid", ctypes.c_uint32),
        ("pbi_ppid", ctypes.c_uint32),
        ("pbi_uid", ctypes.c_uint32),
        ("pbi_gid", ctypes.c_uint32),
        ("pbi_ruid", ctypes.c_uint32),
        ("pbi_rgid", ctypes.c_uint32),
        ("pbi_svuid", ctypes.c_uint32),
        ("pbi_svgid", ctypes.c_uint32),
        ("rfu_1", ctypes.c_uint32),
        ("pbi_comm", ctypes.c_char * 16),
        ("pbi_name", ctypes.c_char * 32),
        ("pbi_nfiles", ctypes.c_uint32),
        ("pbi_pgid", ctypes.c_uint32),
        ("pbi_pjobc", ctypes.c_uint32),
        ("e_tdev", ctypes.c_uint32),
        ("e_tpgid", ctypes.c_uint32),
        ("pbi_nice", ctypes.c_int32),
        ("pbi_start_tvsec", ctypes.c_uint64),
        ("pbi_start_tvusec", ctypes.c_uint64),
    ]


class _ProcTaskInfo(ctypes.Structure):
    _fields_ = [
        ("pti_virtual_size", ctypes.c_uint64),
        ("pti_resident_size", ctypes.c_uint64),
        ("pti_total_user", ctypes.c_uint64),
        ("pti_total_system", ctypes.c_uint64),
        ("pti_threads_user", ctypes.c_uint64),
        ("pti_threads_system", ctypes.c_uint64),
        ("pti_policy", ctypes.c_int32),
        ("pti_faults", ctypes.c_int32),
        ("pti_pageins", ctypes.c_int32),
        ("pti_cow_faults", ctypes.c_int32),
        ("pti_messages_sent", ctypes.c_int32),
        ("pti_messages_received", ctypes.c_int32),
        ("pti_syscalls_mach", ctypes.c_int32),
        ("pti_syscalls_unix", ctypes.c_int32),
        ("pti_csw", ctypes.c_int32),
        ("pti_threadnum", ctypes.c_int32),
        ("pti_numrunning", ctypes.c_int32),
        ("pti_priority", ctypes.c_int32),
    ]


class _ProcRegionInfo(ctypes.Structure):
    _fields_ = [
        ("pri_protection", ctypes.c_uint32),
        ("pri_max_protection", ctypes.c_uint32),
        ("pri_inheritance", ctypes.c_uint32),
        ("pri_flags", ctypes.c_uint32),
        ("pri_offset", ctypes.c_uint64),
        ("pri_behavior", ctypes.c_uint32),
        ("pri_user_wired_count", ctypes.c_uint32),
        ("pri_user_tag", ctypes.c_uint32),
        ("pri_pages_resident", ctypes.c_uint32),
        ("pri_pages_shared_now_private", ctypes.c_uint32),
        ("pri_pages_swapped_out", ctypes.c_uint32),
        ("pri_pages_dirtied", ctypes.c_uint32),
        ("pri_ref_count", ctypes.c_uint32),
        ("pri_shadow_depth", ctypes.c_uint32),
        ("pri_share_mode", ctypes.c_uint32),
        ("pri_private_pages_resident", ctypes.c_uint32),
        ("pri_shared_pages_resident", ctypes.c_uint32),
        ("pri_obj_id", ctypes.c_uint32),
        ("pri_depth", ctypes.c_uint32),
        ("pri_address", ctypes.c_uint64),
        ("pri_size", ctypes.c_uint64),
    ]


class _VMStatistics64(ctypes.Structure):
    # xnu/osfmk/mach/vm_statistics.h: natural_t fields are 32-bit and
    # cumulative event counters are uint64_t.
    _fields_ = [
        ("free_count", ctypes.c_uint32),
        ("active_count", ctypes.c_uint32),
        ("inactive_count", ctypes.c_uint32),
        ("wire_count", ctypes.c_uint32),
        ("zero_fill_count", ctypes.c_uint64),
        ("reactivations", ctypes.c_uint64),
        ("pageins", ctypes.c_uint64),
        ("pageouts", ctypes.c_uint64),
        ("faults", ctypes.c_uint64),
        ("cow_faults", ctypes.c_uint64),
        ("lookups", ctypes.c_uint64),
        ("hits", ctypes.c_uint64),
        ("purges", ctypes.c_uint64),
        ("purgeable_count", ctypes.c_uint32),
        ("speculative_count", ctypes.c_uint32),
        ("decompressions", ctypes.c_uint64),
        ("compressions", ctypes.c_uint64),
        ("swapins", ctypes.c_uint64),
        ("swapouts", ctypes.c_uint64),
        ("compressor_page_count", ctypes.c_uint32),
        ("throttled_count", ctypes.c_uint32),
        ("external_page_count", ctypes.c_uint32),
        ("internal_page_count", ctypes.c_uint32),
        ("total_uncompressed_pages_in_compressor", ctypes.c_uint64),
        ("swapped_count", ctypes.c_uint64),
    ]


@dataclass(frozen=True)
class ProcessRow:
    pid: int
    ppid: int
    rss_bytes: int
    cpu_sec: float
    identity: str
    pgid: int = 0


class _CommandNotStarted(RuntimeError):
    """The detailed wrapper failed before the user command was launched."""


def _linux_processes() -> dict[int, ProcessRow]:
    rows: dict[int, ProcessRow] = {}
    page_size = int(os.sysconf("SC_PAGE_SIZE"))
    ticks = float(os.sysconf("SC_CLK_TCK"))
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            text = open(f"/proc/{name}/stat", encoding="ascii").read()
            end = text.rfind(")")
            fields = text[end + 2:].split()
            pid = int(name)
            ppid = int(fields[1])
            cpu_sec = (int(fields[11]) + int(fields[12])) / ticks
            start_ticks = fields[19]
            rss_bytes = max(0, int(fields[21])) * page_size
            rows[pid] = ProcessRow(
                pid=pid,
                ppid=ppid,
                rss_bytes=rss_bytes,
                cpu_sec=cpu_sec,
                identity=f"{pid}:{start_ticks}",
                pgid=int(fields[2]),
            )
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError, IndexError):
            continue
    return rows


def _parse_ps_time(text: str) -> float:
    days = 0
    value = text.strip()
    if "-" in value:
        day_text, value = value.split("-", 1)
        days = int(day_text)
    parts = [float(part) for part in value.split(":")]
    if len(parts) == 3:
        hours, minutes, seconds = parts
    elif len(parts) == 2:
        hours, (minutes, seconds) = 0.0, parts
    else:
        raise ValueError("unrecognized ps time")
    return days * 86400.0 + hours * 3600.0 + minutes * 60.0 + seconds


def _ps_processes() -> dict[int, ProcessRow]:
    result = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,pgid=,rss=,time="],
        text=True,
        capture_output=True,
        timeout=0.5,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError("ps process-table read failed")
    rows: dict[int, ProcessRow] = {}
    for line in result.stdout.splitlines():
        fields = line.split(None, 4)
        if len(fields) != 5:
            continue
        try:
            pid = int(fields[0])
            rows[pid] = ProcessRow(
                pid=pid,
                ppid=int(fields[1]),
                pgid=int(fields[2]),
                rss_bytes=max(0, int(fields[3])) * 1024,
                cpu_sec=_parse_ps_time(fields[4]),
                # Native macOS ps does not expose a cheap stable start token in
                # this batched table. PID reuse during the one-second grace is
                # possible, and is part of detached_children_possible.
                identity=str(pid),
            )
        except ValueError:
            continue
    return rows


def _darwin_libsystem():
    global _DARWIN_LIBSYSTEM
    if _DARWIN_LIBSYSTEM is None:
        lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
        lib.proc_listallpids.argtypes = (ctypes.c_void_p, ctypes.c_int)
        lib.proc_listallpids.restype = ctypes.c_int
        lib.proc_pidinfo.argtypes = (
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        )
        lib.proc_pidinfo.restype = ctypes.c_int
        _DARWIN_LIBSYSTEM = lib
    return _DARWIN_LIBSYSTEM


def _darwin_processes() -> dict[int, ProcessRow]:
    """Read the macOS process table without spawning ``ps`` every 200 ms."""
    lib = _darwin_libsystem()
    estimate = int(lib.proc_listallpids(None, 0))
    if estimate <= 0:
        raise RuntimeError("proc_listallpids size query failed")
    capacity = max(64, estimate + 64)
    pids = (ctypes.c_int * capacity)()
    count = int(lib.proc_listallpids(pids, ctypes.sizeof(pids)))
    if count <= 0:
        raise RuntimeError("proc_listallpids failed")
    if count >= capacity:
        raise RuntimeError("proc_listallpids result was truncated")
    rows: dict[int, ProcessRow] = {}
    for pid in pids[: min(count, capacity)]:
        if pid <= 0:
            continue
        bsd = _ProcBSDInfo()
        if lib.proc_pidinfo(
            pid,
            PROC_PIDTBSDINFO,
            0,
            ctypes.byref(bsd),
            ctypes.sizeof(bsd),
        ) != ctypes.sizeof(bsd):
            continue
        task = _ProcTaskInfo()
        if lib.proc_pidinfo(
            pid,
            PROC_PIDTASKINFO,
            0,
            ctypes.byref(task),
            ctypes.sizeof(task),
        ) != ctypes.sizeof(task):
            continue
        rows[pid] = ProcessRow(
            pid=pid,
            ppid=int(bsd.pbi_ppid),
            rss_bytes=max(0, int(task.pti_resident_size)),
            # proc_taskinfo reports these counters in nanoseconds. CPU
            # aggregation still comes from wait4; this field keeps the process
            # row complete and independently inspectable.
            cpu_sec=max(
                0.0,
                (int(task.pti_total_user) + int(task.pti_total_system)) / 1e9,
            ),
            identity=(
                f"{pid}:{int(bsd.pbi_start_tvsec)}:"
                f"{int(bsd.pbi_start_tvusec)}"
            ),
            pgid=int(bsd.pbi_pgid),
        )
    return rows


def _processes() -> dict[int, ProcessRow]:
    if sys.platform.startswith("linux") and os.path.exists("/proc/self/stat"):
        return _linux_processes()
    if sys.platform == "darwin":
        return _darwin_processes()
    return _ps_processes()


def _linux_private_resident_bytes(pid: int) -> int:
    """Return resident pages mapped privately by one Linux process.

    ``smaps_rollup`` is deliberately read in one large read. Private clean and
    dirty pages are additive across processes; shared pages and proportional
    estimates are not credited. Private hugetlb is also left uncredited because
    it is excluded from the ordinary RSS/MemAvailable accounting used here.
    """
    fd = os.open(f"/proc/{pid}/smaps_rollup", os.O_RDONLY)
    try:
        raw = os.read(fd, MIB)
        if len(raw) >= MIB:
            raise RuntimeError("smaps_rollup exceeds 1 MiB")
    finally:
        os.close(fd)
    values: dict[bytes, int] = {}
    for line in raw.splitlines():
        key, separator, rest = line.partition(b":")
        if not separator or key not in {b"Private_Clean", b"Private_Dirty"}:
            continue
        match = re.search(rb"\d+", rest)
        if match is not None:
            values[key] = int(match.group()) * 1024
    if b"Private_Clean" not in values or b"Private_Dirty" not in values:
        raise RuntimeError("smaps_rollup omitted private resident counters")
    return values[b"Private_Clean"] + values[b"Private_Dirty"]


def _darwin_private_resident_bytes(pid: int) -> int:
    """Return private resident pages for one macOS process.

    PROC_PIDREGIONINFO reports private and shared resident pages separately.
    Region addresses must advance monotonically; any inconsistent traversal is
    rejected rather than converted into capacity credit.
    """
    lib = _darwin_libsystem()
    page_size = int(os.sysconf("SC_PAGE_SIZE"))
    address = 0
    private_pages = 0
    for _ in range(1_000_000):
        info = _ProcRegionInfo()
        size = int(
            lib.proc_pidinfo(
                pid,
                PROC_PIDREGIONINFO,
                address,
                ctypes.byref(info),
                ctypes.sizeof(info),
            )
        )
        if size == 0:
            return private_pages * page_size
        if size != ctypes.sizeof(info):
            raise RuntimeError("proc_pidinfo region result was truncated")
        region_address = int(info.pri_address)
        region_size = int(info.pri_size)
        if region_size <= 0 or region_address < address:
            raise RuntimeError("proc_pidinfo region traversal did not advance")
        private_pages += max(0, int(info.pri_private_pages_resident))
        next_address = region_address + region_size
        if next_address <= address or next_address > (1 << 64) - 1:
            raise RuntimeError("proc_pidinfo region traversal overflowed")
        address = next_address
    raise RuntimeError("proc_pidinfo region traversal exceeded its bound")


def _private_resident_bytes(pid: int) -> int:
    if sys.platform.startswith("linux"):
        return _linux_private_resident_bytes(pid)
    if sys.platform == "darwin":
        return _darwin_private_resident_bytes(pid)
    raise RuntimeError("additive private-resident accounting unavailable")


def _known_tree(
    rows: dict[int, ProcessRow],
    root_pid: int,
    known: dict[int, str],
    *,
    direct_alive: bool,
) -> list[ProcessRow]:
    selected: set[int] = set()
    root = rows.get(root_pid)
    if direct_alive and root is not None:
        prior = known.get(root_pid)
        if prior is None or prior == root.identity:
            selected.add(root_pid)

    # start_new_session=True gives the direct command a process group whose ID
    # is root_pid. Include that group directly so an ordinary child remains
    # visible even when its short-lived parent exits and it reparents before the
    # next ancestry sample. A child that deliberately calls setsid/setpgid can
    # still escape this kernel grouping; that residual is reported explicitly.
    for row in rows.values():
        if row.pgid == root_pid:
            selected.add(row.pid)

    # Keep following descendants already observed even after they reparent.
    for pid, identity in tuple(known.items()):
        row = rows.get(pid)
        if row is not None and row.identity == identity:
            selected.add(pid)

    changed = True
    while changed:
        changed = False
        for row in rows.values():
            if row.pid not in selected and row.ppid in selected:
                selected.add(row.pid)
                changed = True

    tree = [rows[pid] for pid in selected if pid in rows]
    for row in tree:
        known[row.pid] = row.identity
    return tree


def _linux_host_cpu() -> tuple[int, int]:
    values = [
        int(value)
        for value in open("/proc/stat", encoding="ascii").readline().split()[1:]
    ]
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    return sum(values), idle


def _darwin_host_cpu() -> tuple[int, int]:
    global _DARWIN_HOST_PORT
    libc = _darwin_libsystem()
    libc.mach_host_self.restype = ctypes.c_uint
    libc.host_statistics.argtypes = (
        ctypes.c_uint,
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_uint),
        ctypes.POINTER(ctypes.c_uint),
    )
    values = (ctypes.c_uint * 4)()
    count = ctypes.c_uint(4)
    if _DARWIN_HOST_PORT is None:
        _DARWIN_HOST_PORT = int(libc.mach_host_self())
    if libc.host_statistics(_DARWIN_HOST_PORT, 3, values, ctypes.byref(count)) != 0:
        raise RuntimeError("host_statistics failed")
    # CPU_STATE_USER, SYSTEM, IDLE, NICE.
    return sum(int(value) for value in values), int(values[2])


def _host_cpu() -> tuple[int, int]:
    if sys.platform.startswith("linux"):
        return _linux_host_cpu()
    if sys.platform == "darwin":
        return _darwin_host_cpu()
    raise RuntimeError("whole-device CPU counters unavailable")


def _linux_host_memory() -> tuple[int, int]:
    values: dict[str, int] = {}
    for line in open("/proc/meminfo", encoding="ascii"):
        key, _, rest = line.partition(":")
        match = re.search(r"\d+", rest)
        if match:
            values[key] = int(match.group()) * 1024
    return values["MemTotal"], values["MemAvailable"]


def _darwin_host_memory() -> tuple[int, int]:
    """Read total and immediately reclaimable physical memory without subprocesses."""
    global _DARWIN_TOTAL_MEMORY, _DARWIN_HOST_PORT
    libc = _darwin_libsystem()
    if _DARWIN_HOST_PORT is None:
        libc.mach_host_self.restype = ctypes.c_uint
        _DARWIN_HOST_PORT = int(libc.mach_host_self())

    if _DARWIN_TOTAL_MEMORY is None:
        libc.sysctlbyname.argtypes = (
            ctypes.c_char_p,
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.c_void_p,
            ctypes.c_size_t,
        )
        libc.sysctlbyname.restype = ctypes.c_int
        total = ctypes.c_uint64()
        size = ctypes.c_size_t(ctypes.sizeof(total))
        if libc.sysctlbyname(
            b"hw.memsize", ctypes.byref(total), ctypes.byref(size), None, 0
        ) != 0 or total.value <= 0:
            raise RuntimeError("sysctlbyname hw.memsize failed")
        _DARWIN_TOTAL_MEMORY = int(total.value)

    libc.host_page_size.argtypes = (ctypes.c_uint, ctypes.POINTER(ctypes.c_size_t))
    libc.host_page_size.restype = ctypes.c_int
    page_size = ctypes.c_size_t()
    if libc.host_page_size(_DARWIN_HOST_PORT, ctypes.byref(page_size)) != 0:
        raise RuntimeError("host_page_size failed")

    libc.host_statistics64.argtypes = (
        ctypes.c_uint,
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_int32),
        ctypes.POINTER(ctypes.c_uint32),
    )
    libc.host_statistics64.restype = ctypes.c_int
    stats = _VMStatistics64()
    count = ctypes.c_uint32(ctypes.sizeof(stats) // ctypes.sizeof(ctypes.c_int32))
    if libc.host_statistics64(
        _DARWIN_HOST_PORT,
        4,  # HOST_VM_INFO64
        ctypes.cast(ctypes.byref(stats), ctypes.POINTER(ctypes.c_int32)),
        ctypes.byref(count),
    ) != 0:
        raise RuntimeError("host_statistics64 failed")
    # XNU documents speculative_count as already included in free_count.
    available = (int(stats.free_count) + int(stats.inactive_count)) * int(page_size.value)
    return int(_DARWIN_TOTAL_MEMORY), available


def _host_memory() -> tuple[int, int]:
    if sys.platform.startswith("linux"):
        return _linux_host_memory()
    if sys.platform == "darwin":
        return _darwin_host_memory()
    raise RuntimeError("whole-device memory counters unavailable")


def _ceil_mib(value: float) -> int:
    return max(MIB, int(math.ceil(value / MIB)) * MIB)


def _floor_mib(value: float) -> int:
    return max(MIB, int(math.floor(value / MIB)) * MIB)


def _control_overhead_budget_bytes() -> int:
    """Measure and reserve bounded capacity for the pre-exec control process.

    The admission helper and gate supervisor execute the same staged module and
    interpreter. Doubling the helper's additive private footprint gives the
    control process implementation headroom without increasing the user command
    allowance. The actual gate footprint is revalidated at helper claim.
    """
    pid = os.getpid()
    identity = _identity_for_pid(pid)
    if identity is None:
        raise RuntimeError("cannot identify admission helper for overhead accounting")
    row = _processes().get(pid)
    measured = row.rss_bytes if row is not None and row.identity == identity else 0
    if measured <= 0 or _identity_for_pid(pid) != identity:
        raise RuntimeError("cannot coherently measure admission helper overhead")
    return _ceil_mib(measured * CONTROL_OVERHEAD_HEADROOM_FACTOR)


def _strict_fraction(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite fraction")
    result = float(value)
    if not math.isfinite(result) or not 0.0 < result < 1.0:
        raise ValueError(f"{name} must be greater than 0 and below 1")
    return result


def _policy_from_request(request: dict[str, object], host_total: int) -> dict[str, object]:
    command_fraction = _strict_fraction(
        request.get("command_limit_fraction"), "command_limit_fraction"
    )
    reserve_fraction = _strict_fraction(
        request.get("host_reserve_fraction"), "host_reserve_fraction"
    )
    if command_fraction + reserve_fraction > 1.0:
        raise ValueError("command limit plus host reserve exceeds physical RAM")
    max_jobs = request.get("max_jobs")
    if isinstance(max_jobs, bool) or not isinstance(max_jobs, int) or max_jobs <= 0:
        raise ValueError("max_jobs must be a positive integer")
    if host_total <= 0:
        raise ValueError("physical RAM must be positive")
    reserve_bytes = _ceil_mib(host_total * reserve_fraction)
    max_command_bytes = _floor_mib(host_total * command_fraction)
    if reserve_bytes + max_command_bytes > host_total:
        raise ValueError("rounded command limit plus reserve exceeds physical RAM")
    safe_by_memory = (host_total - reserve_bytes) // max_command_bytes
    safe_concurrency = min(max_jobs, int(safe_by_memory))
    if safe_concurrency <= 0:
        raise ValueError("relative memory policy permits no guarded command")
    ttl = request.get("reservation_ttl_seconds", DEFAULT_RESERVATION_TTL_S)
    if isinstance(ttl, bool) or not isinstance(ttl, (int, float)):
        raise ValueError("reservation_ttl_seconds must be numeric")
    ttl_seconds = float(ttl)
    if not math.isfinite(ttl_seconds) or not 1.0 <= ttl_seconds <= 24 * 60 * 60:
        raise ValueError("reservation_ttl_seconds must be between 1 and 86400")
    return {
        "schema": 2,
        "command_limit_fraction": command_fraction,
        "host_reserve_fraction": reserve_fraction,
        "max_jobs": max_jobs,
        "host_total_bytes": host_total,
        "max_command_bytes": max_command_bytes,
        "min_available_bytes": reserve_bytes,
        "safe_concurrency": safe_concurrency,
        "reservation_ttl_seconds": ttl_seconds,
    }


def _policy_signature(policy: dict[str, object]) -> tuple[object, ...]:
    return (
        policy.get("schema"),
        policy.get("command_limit_fraction"),
        policy.get("host_reserve_fraction"),
        policy.get("max_jobs"),
        policy.get("host_total_bytes"),
        policy.get("max_command_bytes"),
        policy.get("min_available_bytes"),
        policy.get("safe_concurrency"),
    )


def _ledger_paths(state_root: str) -> tuple[Path, Path]:
    if not state_root.strip() or "\x00" in state_root:
        raise ValueError("state_root is empty or invalid")
    root = Path(state_root).expanduser()
    if not root.is_absolute():
        raise ValueError("state_root must be absolute and outside the project cwd")
    directory = root / "memory-guard" / "v2"
    return directory / "ledger.json", directory / "ledger.lock"


@contextmanager
def _locked_ledger(state_root: str):
    ledger_path, lock_path = _ledger_paths(state_root)
    ledger_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield ledger_path
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _empty_ledger() -> dict[str, object]:
    return {"schema": _LEDGER_SCHEMA, "policy": None, "leases": []}


def _read_ledger(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return _empty_ledger()
    if len(raw) > MIB:
        raise RuntimeError("memory admission ledger exceeds 1 MiB")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"memory admission ledger is invalid: {exc}") from exc
    if not isinstance(data, dict) or data.get("schema") != _LEDGER_SCHEMA:
        raise RuntimeError("memory admission ledger schema is invalid")
    leases = data.get("leases")
    if not isinstance(leases, list) or any(not isinstance(item, dict) for item in leases):
        raise RuntimeError("memory admission ledger leases are invalid")
    policy = data.get("policy")
    if policy is not None and not isinstance(policy, dict):
        raise RuntimeError("memory admission ledger policy is invalid")
    return data


def _write_ledger(path: Path, data: dict[str, object]) -> None:
    encoded = (json.dumps(data, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )
    tmp = path.parent / f".ledger-{uuid.uuid4().hex}.tmp"
    fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, "wb", closefd=True) as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        try:
            dfd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        tmp.unlink(missing_ok=True)
        raise


def _token_hash(token: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{32}", token):
        raise ValueError("lease_token must be 32 lowercase hexadecimal characters")
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def _lease_id(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise ValueError("lease_id must be 32 lowercase hexadecimal characters")
    return value


def _lease_matches(lease: dict[str, object], lease_id: str, token_hash: str) -> bool:
    return lease.get("lease_id") == lease_id and lease.get("token_hash") == token_hash


def _lease_owner_alive(lease: dict[str, object]) -> bool:
    """Conservatively retain a claimed/running lease while any recorded owner lives."""
    try:
        helper_pid = int(lease.get("helper_pid") or 0)
        helper_identity = str(lease.get("helper_identity") or "")
        if helper_pid > 0 and helper_identity and _identity_for_pid(helper_pid) == helper_identity:
            return True
        root_pid = int(lease.get("root_pid") or 0)
        root_identity = str(lease.get("root_identity") or "")
        if root_pid > 0 and root_identity and _identity_for_pid(root_pid) == root_identity:
            return True
        pgid = int(lease.get("pgid") or 0)
        if pgid > 0 and _group_alive(pgid):
            # A reused process-group ID may retain capacity longer than necessary,
            # but must never make stale reclamation unsafe.
            return True
        return False
    except Exception:
        return True


def _quarantined_owner_alive(lease: dict[str, object]) -> bool:
    """Retain a quarantine until every identity and its original group are dead."""
    try:
        survivors = lease.get("survivors")
        if not isinstance(survivors, list):
            return True
        for survivor in survivors:
            if not isinstance(survivor, dict):
                return True
            pid = int(survivor.get("pid") or 0)
            identity = str(survivor.get("identity") or "")
            if pid <= 0 or not identity:
                return True
            if _identity_for_pid(pid) == identity:
                return True
        pgid = int(lease.get("pgid") or 0)
        return pgid > 0 and _group_alive(pgid)
    except Exception:
        return True


def _reap_stale_leases(
    leases: list[dict[str, object]], now: float
) -> tuple[list[dict[str, object]], int]:
    kept: list[dict[str, object]] = []
    reaped = 0
    for lease in leases:
        state = lease.get("state")
        if state == "reserved":
            expires = lease.get("expires_at")
            if isinstance(expires, (int, float)) and not isinstance(expires, bool):
                if math.isfinite(float(expires)) and float(expires) <= now:
                    reaped += 1
                    continue
            kept.append(lease)
            continue
        if state in {"claimed", "running"}:
            if _lease_owner_alive(lease):
                kept.append(lease)
            else:
                reaped += 1
            continue
        if state == "quarantined":
            if _quarantined_owner_alive(lease):
                kept.append(lease)
            else:
                reaped += 1
            continue
        # Unknown state is retained rather than converted into free capacity.
        kept.append(lease)
    return kept, reaped


def _lease_process_rows(
    lease: dict[str, object], rows: dict[int, ProcessRow]
) -> dict[int, ProcessRow]:
    state = lease.get("state")
    if state == "reserved":
        return {}
    selected: dict[int, ProcessRow] = {}
    if state == "quarantined":
        survivors = lease.get("survivors")
        if not isinstance(survivors, list):
            raise RuntimeError("quarantined lease omitted survivor identities")
        for survivor in survivors:
            if not isinstance(survivor, dict):
                raise RuntimeError("quarantined lease survivor is invalid")
            pid = int(survivor.get("pid") or 0)
            identity = str(survivor.get("identity") or "")
            row = rows.get(pid)
            if pid > 0 and identity and row is not None and row.identity == identity:
                selected[pid] = row
        pgid = int(lease.get("pgid") or 0)
        if pgid > 0:
            selected.update({pid: row for pid, row in rows.items() if row.pgid == pgid})
    elif state in {"claimed", "running"}:
        root_pid = int(lease.get("root_pid") or 0)
        root_identity = str(lease.get("root_identity") or "")
        pgid = int(lease.get("pgid") or 0)
        if pgid > 0:
            selected.update({pid: row for pid, row in rows.items() if row.pgid == pgid})
        root = rows.get(root_pid)
        if root is not None and root.identity == root_identity:
            known = {root_pid: root_identity}
            for row in _known_tree(rows, root_pid, known, direct_alive=True):
                selected[row.pid] = row
    else:
        raise RuntimeError("ledger contains an unknown lease state")
    if not selected and _lease_owner_alive(lease):
        raise RuntimeError("cannot sample a live guarded process group")
    return selected


def _lease_private_snapshot(
    leases: list[dict[str, object]],
) -> dict[str, dict[int, tuple[str, int, int]]]:
    """Sample additive private resident bytes for every active lease.

    A PID contributes only when its identity is unchanged after the physical
    attribution read. A disappearing or changing process is left uncredited.
    """
    rows = _processes()
    snapshots: dict[str, dict[int, tuple[str, int, int]]] = {}
    for lease in leases:
        lease_name = str(lease.get("lease_id") or "unknown")
        selected = _lease_process_rows(lease, rows)
        values: dict[int, tuple[str, int, int]] = {}
        for pid, row in selected.items():
            try:
                private_bytes = _private_resident_bytes(pid)
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                continue
            if private_bytes < 0:
                raise RuntimeError("private resident accounting returned a negative value")
            if _identity_for_pid(pid) != row.identity:
                continue
            values[pid] = (row.identity, private_bytes, max(0, row.rss_bytes))
        if not values and selected and any(
            _identity_for_pid(pid) == row.identity for pid, row in selected.items()
        ):
            raise RuntimeError("private resident accounting failed for a live guarded tree")
        snapshots[lease_name] = values
    return snapshots


def _validated_host_memory() -> tuple[int, int]:
    total, available = _host_memory()
    if total <= 0 or not 0 <= available <= total:
        raise RuntimeError("invalid host-memory counters")
    return total, available


def _capacity_transaction(
    leases: list[dict[str, object]], *, reserve_bytes: int
) -> tuple[int, dict[str, object]]:
    """Conservatively bracket host availability and additive guarded memory.

    The transaction runs while the target ledger lock is held:

      H0 -> P0 -> H1 -> P1 -> H2

    ``available_floor`` is the minimum host-available reading. A guarded PID is
    credited only when the same process identity appears in both private-memory
    snapshots, and only by the smaller private-resident value. This rejects both
    growth-between-reads and shrink-between-reads double credit. A stable PID
    identity can receive credit in at most one lease, including under stale-ledger
    or process-group reuse ambiguity. Shared/COW pages are not credited until the
    kernel classifies a physically private resident copy, so the credited values
    are additive across processes.
    """
    host0 = _validated_host_memory()
    private0 = _lease_private_snapshot(leases)
    host1 = _validated_host_memory()
    private1 = _lease_private_snapshot(leases)
    host2 = _validated_host_memory()
    totals = {host0[0], host1[0], host2[0]}
    if len(totals) != 1:
        raise RuntimeError("physical-memory total changed during admission transaction")
    host_total = host0[0]
    available_floor = min(host0[1], host1[1], host2[1])

    future_headroom = 0
    current_private = 0
    private_by_lease: dict[str, int] = {}
    observed_private_peak_by_lease: dict[str, int] = {}
    observed_rss_peak_by_lease: dict[str, int] = {}
    capacity_violation = False
    credited_processes: set[tuple[int, str]] = set()
    for lease in leases:
        capacity = lease.get("capacity_bytes")
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise RuntimeError("ledger contains an invalid capacity commitment")
        lease_name = str(lease.get("lease_id") or "unknown")
        first = private0.get(lease_name, {})
        second = private1.get(lease_name, {})
        observed_private_peak_by_lease[lease_name] = max(
            sum(value for _identity, value, _rss in first.values()),
            sum(value for _identity, value, _rss in second.values()),
        )
        observed_rss_peak = max(
            sum(rss for _identity, _value, rss in first.values()),
            sum(rss for _identity, _value, rss in second.values()),
        )
        credited = 0
        for pid, (identity0, value0, _rss0) in first.items():
            later = second.get(pid)
            if later is None:
                continue
            identity1, value1, _rss1 = later
            process_key = (pid, identity0)
            if identity0 == identity1 and process_key not in credited_processes:
                credited_processes.add(process_key)
                credited += min(value0, value1)
        private_by_lease[lease_name] = credited
        current_private += credited
        if credited > capacity:
            capacity_violation = True
        future_headroom += max(0, capacity - credited)
        observed_rss_peak_by_lease[lease_name] = observed_rss_peak

    required_available = reserve_bytes + future_headroom
    capacity = {
        "safe": not capacity_violation and available_floor > required_available,
        "available_floor_bytes": available_floor,
        "available_samples_bytes": [host0[1], host1[1], host2[1]],
        "reserve_bytes": reserve_bytes,
        "future_headroom_bytes": future_headroom,
        "required_available_bytes": required_available,
        "current_guarded_private_bytes": current_private,
        "private_bytes_by_lease": private_by_lease,
        "observed_private_peak_by_lease": observed_private_peak_by_lease,
        "observed_rss_peak_by_lease": observed_rss_peak_by_lease,
        "capacity_violation": capacity_violation,
        "attribution": "private_resident_additive_two_snapshot_minimum",
    }
    return host_total, capacity

def _admission_result(
    status: str,
    reason: str,
    detail: str,
    *,
    policy: dict[str, object] | None = None,
    capacity: dict[str, object] | None = None,
    lease: dict[str, object] | None = None,
    lease_token: str | None = None,
    state_root: str | None = None,
    active_leases: int | None = None,
    stale_reaped: int = 0,
    lease_released: bool | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": _ADMISSION_SCHEMA,
        "status": status,
        "reason": reason,
        "detail": detail,
        "active_leases": active_leases,
        "stale_reaped": stale_reaped,
    }
    if policy is not None:
        payload["policy"] = policy
    if capacity is not None:
        payload["capacity"] = capacity
    if lease_released is not None:
        payload["lease_released"] = lease_released
    if lease is not None and lease_token is not None and state_root is not None:
        payload["lease"] = {
            "lease_id": lease["lease_id"],
            "lease_token": lease_token,
            "state_root": state_root,
            "allowance_bytes": lease["allowance_bytes"],
            "control_overhead_bytes": lease["control_overhead_bytes"],
            "capacity_bytes": lease["capacity_bytes"],
            "max_command_bytes": policy["max_command_bytes"] if policy else None,
            "min_available_bytes": policy["min_available_bytes"] if policy else None,
            "host_total_bytes": policy["host_total_bytes"] if policy else None,
            "safe_concurrency": policy["safe_concurrency"] if policy else None,
            "expires_at": lease["expires_at"],
        }
    return payload


def _validated_admission_request(request: object) -> dict[str, object]:
    if not isinstance(request, dict) or request.get("schema") != _ADMISSION_SCHEMA:
        raise ValueError("memory admission request schema must be 1")
    op = request.get("op")
    if op not in {"reserve", "renew", "release"}:
        raise ValueError("memory admission operation is invalid")
    state_root = request.get("state_root")
    if not isinstance(state_root, str) or not state_root:
        raise ValueError("state_root must be a non-empty string")
    _ledger_paths(state_root)
    _lease_id(request.get("lease_id"))
    _token_hash(str(request.get("lease_token") or ""))
    return request


def _reserve_memory_lease(request: dict[str, object]) -> dict[str, object]:
    state_root = str(request["state_root"])
    lease_id = _lease_id(request["lease_id"])
    lease_token = str(request["lease_token"])
    token_hash = _token_hash(lease_token)
    control_overhead = _control_overhead_budget_bytes()
    with _locked_ledger(state_root) as ledger_path:
        host_total, _ = _validated_host_memory()
        policy = _policy_from_request(request, host_total)
        ledger = _read_ledger(ledger_path)
        now = time.time()
        leases, stale_reaped = _reap_stale_leases(list(ledger["leases"]), now)
        existing_policy = ledger.get("policy")
        if leases and (
            not isinstance(existing_policy, dict)
            or _policy_signature(existing_policy) != _policy_signature(policy)
        ):
            return _admission_result(
                "refused",
                "policy_mismatch",
                "active guarded leases were created under a different target policy",
                policy=policy,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
            )
        if any(lease.get("lease_id") == lease_id for lease in leases):
            return _admission_result(
                "refused",
                "lease_id_collision",
                "lease_id already exists",
                policy=policy,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
            )
        predicted = request.get("predicted_rss_bytes")
        explicit_limit = request.get("explicit_limit_bytes")
        if predicted is not None and explicit_limit is not None:
            raise ValueError(
                "predicted_rss_bytes and explicit_limit_bytes are mutually exclusive"
            )
        if explicit_limit is not None:
            if isinstance(explicit_limit, bool) or not isinstance(explicit_limit, int):
                raise ValueError("explicit_limit_bytes must be an integer or null")
            if explicit_limit <= 0 or explicit_limit % MIB != 0:
                raise ValueError("explicit_limit_bytes must be a positive whole MiB")
            allowance = explicit_limit
            allowance_basis = "explicit_command_limit"
            predicted_value = None
            explicit_limit_value = explicit_limit
            if allowance > int(policy["max_command_bytes"]):
                return _admission_result(
                    "refused",
                    "explicit_limit_exceeds_command_limit",
                    "explicit memory limit exceeds the target per-command ceiling",
                    policy=policy,
                    active_leases=len(leases),
                    stale_reaped=stale_reaped,
                )
        elif predicted is None:
            allowance = int(policy["max_command_bytes"])
            allowance_basis = "unprofiled_command_ceiling"
            predicted_value = None
            explicit_limit_value = None
        else:
            if isinstance(predicted, bool) or not isinstance(predicted, (int, float)):
                raise ValueError("predicted_rss_bytes must be numeric or null")
            predicted_value = float(predicted)
            if not math.isfinite(predicted_value) or predicted_value <= 0:
                raise ValueError("predicted_rss_bytes must be positive")
            allowance = _ceil_mib(predicted_value * PREDICTION_HEADROOM_FACTOR)
            allowance_basis = "learned_profile_plus_25_percent"
            explicit_limit_value = None
            if allowance > int(policy["max_command_bytes"]):
                return _admission_result(
                    "refused",
                    "prediction_exceeds_command_limit",
                    "predicted RSS plus 25% headroom exceeds the per-command limit",
                    policy=policy,
                    active_leases=len(leases),
                    stale_reaped=stale_reaped,
                )
        capacity_bytes = allowance + control_overhead
        if capacity_bytes + int(policy["min_available_bytes"]) > host_total:
            return _admission_result(
                "refused",
                "control_overhead_exceeds_capacity",
                "user allowance plus measured control-process overhead cannot preserve the host reserve",
                policy=policy,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
            )
        if len(leases) >= int(policy["safe_concurrency"]):
            return _admission_result(
                "refused",
                "guarded_job_limit",
                "target already has the maximum safe number of guarded leases",
                policy=policy,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
            )
        expires_at = now + float(policy["reservation_ttl_seconds"])
        lease: dict[str, object] = {
            "lease_id": lease_id,
            "token_hash": token_hash,
            "allowance_bytes": allowance,
            "control_overhead_bytes": control_overhead,
            "capacity_bytes": capacity_bytes,
            "state": "reserved",
            "created_at": now,
            "expires_at": expires_at,
        }
        candidate_leases = [*leases, lease]
        transaction_total, capacity = _capacity_transaction(
            candidate_leases, reserve_bytes=int(policy["min_available_bytes"])
        )
        capacity.update(
            {
                "allowance_basis": allowance_basis,
                "allowance_bytes": allowance,
                "control_overhead_bytes": control_overhead,
                "predicted_rss_bytes": predicted_value,
                "explicit_limit_bytes": explicit_limit_value,
            }
        )
        if transaction_total != host_total:
            raise RuntimeError("physical-memory total changed while deriving admission policy")
        if not capacity["safe"]:
            if stale_reaped:
                _write_ledger(
                    ledger_path,
                    {
                        "schema": _LEDGER_SCHEMA,
                        "policy": existing_policy if leases else None,
                        "leases": leases,
                    },
                )
            return _admission_result(
                "refused",
                "insufficient_live_memory",
                (
                    "unprofiled allowance cannot preserve host reserve"
                    if predicted is None and explicit_limit is None
                    else "explicit command limit cannot preserve host reserve"
                    if explicit_limit is not None
                    else "learned allowance cannot preserve host reserve"
                ),
                policy=policy,
                capacity=capacity,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
            )
        _write_ledger(
            ledger_path,
            {"schema": _LEDGER_SCHEMA, "policy": policy, "leases": candidate_leases},
        )
        return _admission_result(
            "admitted",
            "reserved",
            (
                "unprofiled maximum allowance reserved before mutation"
                if predicted is None and explicit_limit is None
                else "explicit command limit reserved before mutation"
                if explicit_limit is not None
                else "learned allowance reserved before mutation"
            ),
            policy=policy,
            capacity=capacity,
            lease=lease,
            lease_token=lease_token,
            state_root=state_root,
            active_leases=len(candidate_leases),
            stale_reaped=stale_reaped,
        )


def _reservation_matches_request(
    lease: dict[str, object], request: dict[str, object]
) -> bool:
    return all(
        lease.get(name) == request.get(name)
        for name in ("allowance_bytes", "control_overhead_bytes", "capacity_bytes")
    )


def _renew_memory_lease(request: dict[str, object]) -> dict[str, object]:
    state_root = str(request["state_root"])
    lease_id = _lease_id(request["lease_id"])
    lease_token = str(request["lease_token"])
    token_hash = _token_hash(lease_token)
    with _locked_ledger(state_root) as ledger_path:
        host_total, _ = _validated_host_memory()
        policy = _policy_from_request(request, host_total)
        ledger = _read_ledger(ledger_path)
        now = time.time()
        leases, stale_reaped = _reap_stale_leases(list(ledger["leases"]), now)
        existing_policy = ledger.get("policy")
        if not isinstance(existing_policy, dict) or (
            _policy_signature(existing_policy) != _policy_signature(policy)
        ):
            return _admission_result(
                "refused",
                "policy_mismatch",
                "reservation policy no longer matches the target ledger",
                policy=policy,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
            )
        index = next(
            (i for i, lease in enumerate(leases) if _lease_matches(lease, lease_id, token_hash)),
            None,
        )
        if index is None:
            if stale_reaped:
                _write_ledger(
                    ledger_path,
                    {"schema": _LEDGER_SCHEMA, "policy": existing_policy, "leases": leases},
                )
            return _admission_result(
                "refused",
                "reservation_missing",
                "reservation expired, was reclaimed, or does not belong to this controller",
                policy=policy,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
            )
        lease = leases[index]
        if lease.get("state") != "reserved":
            return _admission_result(
                "refused",
                "reservation_state_changed",
                "only an unclaimed reservation can be renewed by the controller",
                policy=policy,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
            )
        if not _reservation_matches_request(lease, request):
            return _admission_result(
                "refused",
                "reservation_mismatch",
                "reservation capacity does not match the controller receipt",
                policy=policy,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
            )
        transaction_total, capacity = _capacity_transaction(
            leases, reserve_bytes=int(policy["min_available_bytes"])
        )
        if transaction_total != host_total:
            raise RuntimeError("physical-memory total changed while renewing reservation")
        if not capacity["safe"]:
            del leases[index]
            _write_ledger(
                ledger_path,
                {
                    "schema": _LEDGER_SCHEMA,
                    "policy": existing_policy if leases else None,
                    "leases": leases,
                },
            )
            return _admission_result(
                "refused",
                "live_memory_changed",
                "final renewal cannot preserve the reserve and active capacity commitments",
                policy=policy,
                capacity=capacity,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
                lease_released=True,
            )
        lease = dict(lease)
        lease["expires_at"] = now + float(policy["reservation_ttl_seconds"])
        leases[index] = lease
        _write_ledger(
            ledger_path,
            {"schema": _LEDGER_SCHEMA, "policy": existing_policy, "leases": leases},
        )
        return _admission_result(
            "admitted",
            "renewed",
            "reservation renewed after atomically revalidating live target memory",
            policy=policy,
            capacity=capacity,
            lease=lease,
            lease_token=lease_token,
            state_root=state_root,
            active_leases=len(leases),
            stale_reaped=stale_reaped,
        )


def _release_memory_lease(request: dict[str, object]) -> dict[str, object]:
    state_root = str(request["state_root"])
    lease_id = _lease_id(request["lease_id"])
    token_hash = _token_hash(str(request["lease_token"]))
    reserved_only = bool(request.get("reserved_only", False))
    cleanup_complete = bool(request.get("cleanup_complete", False))
    with _locked_ledger(state_root) as ledger_path:
        ledger = _read_ledger(ledger_path)
        now = time.time()
        leases, stale_reaped = _reap_stale_leases(list(ledger["leases"]), now)
        retained: list[dict[str, object]] = []
        released = False
        for lease in leases:
            matching = _lease_matches(lease, lease_id, token_hash)
            state = lease.get("state")
            may_release = state == "reserved" or (
                cleanup_complete and state in {"claimed", "running"}
            )
            if matching and may_release and (not reserved_only or state == "reserved"):
                released = True
                continue
            retained.append(lease)
        if released or stale_reaped:
            _write_ledger(
                ledger_path,
                {
                    "schema": _LEDGER_SCHEMA,
                    "policy": ledger.get("policy") if retained else None,
                    "leases": retained,
                },
            )
        return _admission_result(
            "released",
            "released" if released else "retained_or_absent",
            "reservation was released"
            if released
            else "reservation is absent or retained for a live/quarantined owner",
            policy=ledger.get("policy") if isinstance(ledger.get("policy"), dict) else None,
            active_leases=len(retained),
            stale_reaped=stale_reaped,
            lease_released=released,
        )


def _quarantine_memory_lease(
    request: dict[str, object],
    *,
    survivors: list[dict[str, object]],
    pgid: int,
) -> bool:
    """Persist identity-checked survivor ownership instead of freeing capacity."""
    state_root = str(request["state_root"])
    lease_id = _lease_id(request["lease_id"])
    token_hash = _token_hash(str(request["lease_token"]))
    normalized: list[dict[str, object]] = []
    for survivor in survivors:
        pid = int(survivor.get("pid") or 0)
        identity = str(survivor.get("identity") or "")
        if pid <= 0 or not identity:
            raise ValueError("quarantine survivor identity is invalid")
        normalized.append({"pid": pid, "identity": identity})
    if pgid <= 0:
        raise ValueError("quarantine pgid must be positive")
    with _locked_ledger(state_root) as ledger_path:
        ledger = _read_ledger(ledger_path)
        leases = list(ledger["leases"])
        index = next(
            (i for i, lease in enumerate(leases) if _lease_matches(lease, lease_id, token_hash)),
            None,
        )
        if index is None:
            return False
        lease = leases[index]
        if lease.get("state") not in {"claimed", "running", "quarantined"}:
            return False
        quarantined = dict(lease)
        quarantined.update(
            {
                "state": "quarantined",
                "survivors": normalized,
                "pgid": pgid,
                "quarantined_at": time.time(),
            }
        )
        leases[index] = quarantined
        _write_ledger(
            ledger_path,
            {"schema": _LEDGER_SCHEMA, "policy": ledger.get("policy"), "leases": leases},
        )
        return True


def _claim_memory_lease(
    request: dict[str, object],
    *,
    helper_pid: int,
    root_pid: int,
    root_identity: str,
    pgid: int,
) -> dict[str, object]:
    """Atomically revalidate and transfer one reservation to the guarded helper."""
    state_root = str(request["state_root"])
    lease_id = _lease_id(request["lease_id"])
    lease_token = str(request["lease_token"])
    token_hash = _token_hash(lease_token)
    with _locked_ledger(state_root) as ledger_path:
        host_total, _ = _validated_host_memory()
        policy = _policy_from_request(request, host_total)
        ledger = _read_ledger(ledger_path)
        now = time.time()
        leases, stale_reaped = _reap_stale_leases(list(ledger["leases"]), now)
        existing_policy = ledger.get("policy")
        if not isinstance(existing_policy, dict) or (
            _policy_signature(existing_policy) != _policy_signature(policy)
        ):
            return _admission_result(
                "refused",
                "policy_mismatch",
                "helper claim policy no longer matches the target ledger",
                policy=policy,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
            )
        index = next(
            (i for i, lease in enumerate(leases) if _lease_matches(lease, lease_id, token_hash)),
            None,
        )
        if index is None:
            if stale_reaped:
                _write_ledger(
                    ledger_path,
                    {"schema": _LEDGER_SCHEMA, "policy": existing_policy, "leases": leases},
                )
            return _admission_result(
                "refused",
                "reservation_missing",
                "helper cannot prove ownership of a live reservation",
                policy=policy,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
            )
        original = leases[index]
        if original.get("state") != "reserved" or not _reservation_matches_request(
            original, request
        ):
            return _admission_result(
                "refused",
                "reservation_mismatch",
                "helper claim does not match the reserved capacity/state",
                policy=policy,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
            )
        helper_identity = _identity_for_pid(helper_pid)
        if helper_identity is None:
            raise RuntimeError("cannot identify memory-guard helper")
        running = dict(original)
        running.update(
            {
                "state": "running",
                "helper_pid": helper_pid,
                "helper_identity": helper_identity,
                "root_pid": root_pid,
                "root_identity": root_identity,
                "pgid": pgid,
                "claimed_at": now,
                "expires_at": now + float(policy["reservation_ttl_seconds"]),
            }
        )
        candidate_leases = list(leases)
        candidate_leases[index] = running
        transaction_total, capacity = _capacity_transaction(
            candidate_leases, reserve_bytes=int(policy["min_available_bytes"])
        )
        if transaction_total != host_total:
            raise RuntimeError("physical-memory total changed while claiming reservation")
        observed_control = int(
            capacity.get("observed_rss_peak_by_lease", {}).get(lease_id, 0)
        )
        control_limit = int(original["control_overhead_bytes"])
        if observed_control > control_limit:
            capacity["safe"] = False
            capacity["control_overhead_violation"] = True
            capacity["observed_control_rss_bytes"] = observed_control
            capacity["control_overhead_bytes"] = control_limit
        if not capacity["safe"]:
            del leases[index]
            _write_ledger(
                ledger_path,
                {
                    "schema": _LEDGER_SCHEMA,
                    "policy": existing_policy if leases else None,
                    "leases": leases,
                },
            )
            return _admission_result(
                "refused",
                "live_memory_changed",
                "helper claim cannot preserve the reserve and active capacity commitments",
                policy=policy,
                capacity=capacity,
                active_leases=len(leases),
                stale_reaped=stale_reaped,
                lease_released=True,
            )
        _write_ledger(
            ledger_path,
            {"schema": _LEDGER_SCHEMA, "policy": existing_policy, "leases": candidate_leases},
        )
        return _admission_result(
            "admitted",
            "claimed",
            "guard helper owns the reservation and may release the launch gate",
            policy=policy,
            capacity=capacity,
            lease=running,
            lease_token=lease_token,
            state_root=state_root,
            active_leases=len(candidate_leases),
            stale_reaped=stale_reaped,
        )

def _handle_admission_request(request: object) -> dict[str, object]:
    validated = _validated_admission_request(request)
    op = validated["op"]
    if op == "reserve":
        return _reserve_memory_lease(validated)
    if op == "renew":
        return _renew_memory_lease(validated)
    return _release_memory_lease(validated)


def _emit_admission_result(payload: dict[str, object]) -> None:
    sys.stdout.write(
        MEMORY_ADMISSION_MARKER + json.dumps(payload, separators=(",", ":")) + "\n"
    )
    sys.stdout.flush()


def _gpu_reading() -> tuple[str, str, list[dict[str, object]]]:
    query = [
        "nvidia-smi",
        "--query-gpu=index,name,utilization.gpu,memory.free,memory.total",
        "--format=csv,noheader,nounits",
    ]
    try:
        result = subprocess.run(
            query,
            text=True,
            capture_output=True,
            timeout=0.5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        result = None

    devices: list[dict[str, object]] = []
    malformed = 0
    if result is not None and result.returncode == 0:
        for row in csv.reader(io.StringIO(result.stdout)):
            if len(row) < 5:
                malformed += 1
                continue
            try:
                util_pct = float(row[2])
                free_mib = float(row[3])
                total_mib = float(row[4])
                if (
                    not all(math.isfinite(value) for value in (util_pct, free_mib, total_mib))
                    or not 0 <= util_pct <= 100
                    or total_mib <= 0
                    or free_mib < 0
                    or free_mib > total_mib
                    or not row[0].strip()
                    or not row[1].strip()
                ):
                    raise ValueError("invalid GPU counters")
                devices.append(
                    {
                        "id": row[0].strip(),
                        "name": row[1].strip(),
                        "util_pct": util_pct,
                        "vram_free_bytes": int(free_mib * MIB),
                        "vram_total_bytes": int(total_mib * MIB),
                    }
                )
            except (ValueError, TypeError):
                malformed += 1
    if devices:
        return "discrete", ("partial" if malformed else "measured"), devices

    if sys.platform == "darwin" and platform.machine().lower() in {"arm64", "aarch64"}:
        try:
            ioreg = subprocess.run(
                ["ioreg", "-r", "-d", "1", "-w", "0", "-c", "AGXAccelerator"],
                text=True,
                capture_output=True,
                timeout=0.5,
                check=False,
            ).stdout
            match = re.search(r'"Device Utilization %"\s*=\s*(\d+)', ioreg)
            util = float(match.group(1)) if match else None
            if util is not None and not 0 <= util <= 100:
                util = None
        except (OSError, subprocess.TimeoutExpired):
            util = None
        return (
            "unified",
            "measured" if util is not None else "unavailable",
            [
                {
                    "id": "unified",
                    "name": "Apple GPU",
                    "util_pct": util,
                    "vram_free_bytes": None,
                    "vram_total_bytes": None,
                }
            ],
        )
    return "unknown", "unavailable", []


class Samples:
    def __init__(self) -> None:
        self.known: dict[int, str] = {}
        self.process_samples = 0
        self.observed_process_samples = 0
        self.process_table_errors = 0
        self.peak_rss_bytes: int | None = None
        self.errors: list[str] = []
        self._host_cpu_prior: tuple[int, int] | None = None
        self._host_busy: list[float] = []
        self.min_available_bytes: int | None = None
        self.host_total_bytes: int | None = None
        self.max_host_used_pct: float | None = None
        self.gpu_kind = "unknown"
        self.gpu_sample_count = 0
        self.gpu_failed_sample_count = 0
        self.gpus: dict[str, dict[str, object]] = {}
        self._next_pressure_at = 0.0
        self._next_gpu_at = 0.0

    def sample_process_tree(
        self,
        root_pid: int,
        *,
        direct_alive: bool,
        include_pressure: bool,
    ) -> int | None:
        active: int | None = None
        try:
            rows = _processes()
            tree = _known_tree(rows, root_pid, self.known, direct_alive=direct_alive)
            self.process_samples += 1
            active = len(tree)
            if tree:
                self.observed_process_samples += 1
                rss = sum(row.rss_bytes for row in tree)
                self.peak_rss_bytes = max(self.peak_rss_bytes or 0, rss)
        except Exception as exc:
            self.process_table_errors += 1
            self.errors.append(f"process table: {type(exc).__name__}")

        if include_pressure:
            now = time.monotonic()
            if now >= self._next_pressure_at:
                self._sample_host_pressure()
                self._next_pressure_at = now + PRESSURE_SAMPLE_INTERVAL_S
            if now >= self._next_gpu_at:
                self._sample_gpu()
                self._next_gpu_at = now + GPU_SAMPLE_INTERVAL_S
        return active

    def _sample_host_pressure(self) -> None:
        try:
            counter = _host_cpu()
            if self._host_cpu_prior is not None:
                total = counter[0] - self._host_cpu_prior[0]
                idle = counter[1] - self._host_cpu_prior[1]
                if total > 0 and 0 <= idle <= total:
                    self._host_busy.append(100.0 * (1.0 - idle / total))
            self._host_cpu_prior = counter
        except Exception as exc:
            self.errors.append(f"host cpu: {type(exc).__name__}")

        try:
            total, available = _host_memory()
            if total <= 0 or available < 0 or available > total:
                raise ValueError("invalid host memory counters")
            self.host_total_bytes = total
            self.min_available_bytes = min(self.min_available_bytes or available, available)
            used_pct = 100.0 * (1.0 - available / total)
            self.max_host_used_pct = max(self.max_host_used_pct or 0.0, used_pct)
        except Exception as exc:
            self.errors.append(f"host memory: {type(exc).__name__}")

    def _sample_gpu(self) -> None:
        kind, status, devices = _gpu_reading()
        self.gpu_sample_count += 1
        if status != "measured":
            self.gpu_failed_sample_count += 1
        if devices:
            self.gpu_kind = kind
        for device in devices:
            identifier = str(device["id"])
            aggregate = self.gpus.setdefault(
                identifier,
                {
                    "id": identifier,
                    "name": str(device["name"]),
                    "max_util_pct": None,
                    "min_vram_free_bytes": None,
                    "vram_total_bytes": device["vram_total_bytes"],
                },
            )
            util = device["util_pct"]
            if isinstance(util, (int, float)):
                current = aggregate["max_util_pct"]
                aggregate["max_util_pct"] = max(float(current or 0.0), float(util))
            free = device["vram_free_bytes"]
            if isinstance(free, int):
                current_free = aggregate["min_vram_free_bytes"]
                aggregate["min_vram_free_bytes"] = min(
                    int(current_free) if isinstance(current_free, int) else free,
                    free,
                )

    def host_cpu_payload(self) -> dict[str, object]:
        if not self._host_busy:
            return {
                "scope": "whole_device",
                "avg_busy_pct": None,
                "max_busy_pct": None,
                "status": "unavailable",
            }
        return {
            "scope": "whole_device",
            "avg_busy_pct": round(sum(self._host_busy) / len(self._host_busy), 1),
            "max_busy_pct": round(max(self._host_busy), 1),
            "status": "measured",
        }

    def host_memory_payload(self) -> dict[str, object]:
        status = "measured" if self.min_available_bytes is not None else "unavailable"
        return {
            "scope": "whole_device",
            "total_bytes": self.host_total_bytes,
            "min_available_bytes": self.min_available_bytes,
            "max_used_pct": (
                round(self.max_host_used_pct, 1)
                if self.max_host_used_pct is not None
                else None
            ),
            "status": status,
        }

    def gpu_payload(self) -> dict[str, object]:
        devices = [self.gpus[key] for key in sorted(self.gpus)]
        utils = [
            float(device["max_util_pct"])
            for device in devices
            if isinstance(device["max_util_pct"], (int, float))
        ]
        free_values = [
            int(device["min_vram_free_bytes"])
            for device in devices
            if isinstance(device["min_vram_free_bytes"], int)
        ]
        if not utils and not free_values:
            status = "unavailable"
        elif self.gpu_failed_sample_count:
            status = "partial"
        else:
            status = "measured"
        payload: dict[str, object] = {
            "scope": "whole_device",
            "kind": self.gpu_kind,
            "max_util_pct": round(max(utils), 1) if utils else None,
            "min_vram_free_bytes": min(free_values) if free_values else None,
            "status": status,
            "sample_count": self.gpu_sample_count,
            "failed_sample_count": self.gpu_failed_sample_count,
            "devices": devices,
        }
        if self.gpu_kind == "unified":
            payload["unified_memory_min_available_bytes"] = self.min_available_bytes
        return payload


def _wait4_nohang(child: subprocess.Popen) -> tuple[int, resource.struct_rusage] | None:
    pid, status, usage = os.wait4(child.pid, os.WNOHANG)
    if pid == 0:
        return None
    rc = os.waitstatus_to_exitcode(status)
    child.returncode = rc
    return rc, usage


def _shell_exit_code(returncode: int) -> int:
    """Map Popen's negative signal return to the status an SSH shell exposes."""
    if returncode < 0:
        return 128 + (-returncode)
    return returncode


def _unknown_payload(reason: str, *, wall_sec: float | None = None) -> dict[str, object]:
    return {
        "schema": 1,
        "status": "unavailable",
        "cpu": {
            "cpu_sec": None,
            "avg_cpu_pct": None,
            "coverage": "sampler_failed",
            "whole_device": {
                "scope": "whole_device",
                "avg_busy_pct": None,
                "max_busy_pct": None,
                "status": "unavailable",
            },
        },
        "memory": {
            "peak_bytes": None,
            "metric": "rss_sum_sampled",
            "sample_interval_ms": int(SAMPLE_INTERVAL_S * 1000),
            "sample_count": 0,
            "coverage": "sampler_failed",
            "shared_page_semantics": "may_double_count",
            "whole_device": {
                "scope": "whole_device",
                "total_bytes": None,
                "min_available_bytes": None,
                "max_used_pct": None,
                "status": "unavailable",
            },
        },
        "gpu": {
            "scope": "whole_device",
            "kind": "unknown",
            "max_util_pct": None,
            "min_vram_free_bytes": None,
            "status": "unavailable",
            "devices": [],
        },
        "peak_rss_mb": None,
        "avg_cpu_pct": None,
        "cpu_sec": None,
        "wall_sec": round(wall_sec, 3) if wall_sec is not None else None,
        "process_tree_drained": None,
        "detached_children_possible": True,
        "detail": reason,
    }


def _detailed_run(argv: list[str]) -> tuple[int, dict[str, object]]:
    try:
        child = subprocess.Popen(argv)
    except Exception as exc:
        raise _CommandNotStarted from exc
    started = time.monotonic()
    try:
        samples = Samples()
        usage = None
        rc = None
        next_sample = started

        while rc is None:
            now = time.monotonic()
            if now >= next_sample:
                samples.sample_process_tree(
                    child.pid,
                    direct_alive=True,
                    include_pressure=True,
                )
                next_sample = now + SAMPLE_INTERVAL_S

            waited = _wait4_nohang(child)
            if waited is not None:
                rc, usage = waited
                break
            time.sleep(min(0.02, max(0.001, next_sample - time.monotonic())))

        direct_wall = max(0.0, time.monotonic() - started)
        deadline = time.monotonic() + DESCENDANT_GRACE_S
        drained = False
        active: int | None = None
        while True:
            active = samples.sample_process_tree(
                child.pid,
                direct_alive=False,
                include_pressure=False,
            )
            if active == 0 and samples.process_table_errors == 0:
                drained = True
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(SAMPLE_INTERVAL_S, remaining))

        cpu_sec = (
            float(usage.ru_utime) + float(usage.ru_stime)
            if usage is not None
            else None
        )
        avg_cpu = (
            cpu_sec / direct_wall * 100.0
            if cpu_sec is not None and direct_wall > 0
            else None
        )

        if samples.process_table_errors:
            memory_coverage = (
                "sampler_failed"
                if samples.observed_process_samples == 0
                else "sampled_partial"
            )
        elif samples.observed_process_samples == 0:
            memory_coverage = (
                "sampler_failed" if samples.process_samples == 0 else "short_lived_unobserved"
            )
        elif samples.observed_process_samples < 2:
            memory_coverage = "short_lived_sampled"
        elif not drained:
            memory_coverage = "known_tree_cutoff"
        elif samples.errors:
            memory_coverage = "sampled_partial"
        else:
            memory_coverage = "known_tree_drained"

        peak = samples.peak_rss_bytes
        payload: dict[str, object] = {
            "schema": 1,
            "status": (
                "ok"
                if peak is not None and cpu_sec is not None and not samples.errors
                else "partial"
            ),
            "cpu": {
                "cpu_sec": round(cpu_sec, 3) if cpu_sec is not None else None,
                "avg_cpu_pct": round(avg_cpu, 1) if avg_cpu is not None else None,
                "coverage": (
                    "wait4_known_tree_drained_detached_possible"
                    if cpu_sec is not None and drained
                    else (
                        "wait4_known_tree_cutoff"
                        if cpu_sec is not None
                        else "sampler_failed"
                    )
                ),
                "whole_device": samples.host_cpu_payload(),
            },
            "memory": {
                "peak_bytes": peak,
                "metric": "rss_sum_sampled",
                "sample_interval_ms": int(SAMPLE_INTERVAL_S * 1000),
                "sample_count": samples.process_samples,
                "coverage": memory_coverage,
                "shared_page_semantics": "may_double_count",
                "whole_device": samples.host_memory_payload(),
            },
            "gpu": samples.gpu_payload(),
            # Deprecated compatibility aliases. POSIX now reports the sampled
            # concurrent RSS sum rather than largest-child ru_maxrss.
            "peak_rss_mb": round(peak / MIB, 1) if peak is not None else None,
            "avg_cpu_pct": round(avg_cpu, 1) if avg_cpu is not None else None,
            "cpu_sec": round(cpu_sec, 3) if cpu_sec is not None else None,
            "wall_sec": round(direct_wall, 3),
            "process_tree_drained": drained,
            "detached_children_possible": True,
        }
        if not drained and isinstance(active, int) and active > 0:
            payload["active_processes_at_cutoff"] = active
        if samples.errors:
            payload["sampling_error_count"] = len(samples.errors)
            payload["sampling_errors"] = sorted(set(samples.errors))[:8]
        return int(rc), payload
    except Exception as exc:
        # Popen succeeded. From this point on the command must never be executed
        # again merely because optional sampling or payload assembly failed.
        if child.returncode is None:
            try:
                child.wait()
            except Exception:
                pass
        rc = child.returncode if isinstance(child.returncode, int) else 1
        wall = time.monotonic() - started
        return rc, _unknown_payload(
            f"sampler failed after command start: {type(exc).__name__}",
            wall_sec=wall,
        )


def _plain_run(argv: list[str]) -> int:
    try:
        return int(subprocess.call(argv))
    except BaseException as exc:
        try:
            name = argv[0] if argv else "<empty command>"
            sys.stderr.write(
                f"remrun telemetry wrapper: failed to execute {name!r}: {exc}\n"
            )
        except Exception:
            pass
        return 127


def _emit(payload: dict[str, object]) -> None:
    try:
        sys.stderr.write(
            TELEMETRY_MARKER + json.dumps(payload, separators=(",", ":")) + "\n"
        )
        sys.stderr.flush()
    except Exception:
        pass


class _GuardInterrupted(RuntimeError):
    pass


class _LaunchCompletionUnknown(RuntimeError):
    pass


def _after_gate_release() -> None:
    """Private deterministic test seam for the release/exec-confirmation window."""


def _read_one_with_timeout(fd: int, timeout: float) -> bytes:
    ready, _, _ = select.select([fd], [], [], timeout)
    if not ready:
        raise TimeoutError("timed out waiting for guarded launch handshake")
    return os.read(fd, 65536)


def _gate_exec_child(
    argv: list[str], *, gate_fd: int, ready_fd: int, exec_status_fd: int
) -> int:
    """Supervise exact argv and report only a Popen-proved exec outcome.

    ``subprocess.Popen`` does not return until its private error pipe proves that
    exec succeeded or reports the explicit exec failure. The control process
    stays in the command process group and relays the user's shell-style exit.
    """
    try:
        os.set_inheritable(exec_status_fd, False)
        os.write(ready_fd, b"R")
    except OSError:
        return 125
    finally:
        try:
            os.close(ready_fd)
        except OSError:
            pass
    try:
        gate = os.read(gate_fd, 1)
    except OSError:
        gate = b""
    finally:
        try:
            os.close(gate_fd)
        except OSError:
            pass
    if gate != b"G":
        return 125
    try:
        command = subprocess.Popen(argv)
    except BaseException as exc:
        try:
            detail = json.dumps(
                {
                    "status": "exec_failed",
                    "error": f"{type(exc).__name__}: {exc}",
                },
                separators=(",", ":"),
            ).encode("utf-8")
            os.write(exec_status_fd, detail)
        except OSError:
            pass
        finally:
            try:
                os.close(exec_status_fd)
            except OSError:
                pass
        return 127
    try:
        detail = json.dumps(
            {"status": "exec_confirmed", "pid": command.pid},
            separators=(",", ":"),
        ).encode("utf-8")
        os.write(exec_status_fd, detail)
    except OSError:
        # The user command may be running, but the parent cannot prove launch.
        # Keep supervising; the parent will enter completion-unknown and kill us.
        pass
    finally:
        try:
            os.close(exec_status_fd)
        except OSError:
            pass
    try:
        return _shell_exit_code(int(command.wait()))
    except BaseException:
        try:
            command.kill()
        except BaseException:
            pass
        try:
            command.wait(timeout=1.0)
        except BaseException:
            pass
        return 125


def _spawn_gated_command(argv: list[str]) -> tuple[subprocess.Popen, int, int]:
    gate_read, gate_write = os.pipe()
    ready_read, ready_write = os.pipe()
    exec_read, exec_write = os.pipe()
    helper = str(Path(__file__).resolve())
    child_argv = [
        sys.executable,
        helper,
        "--guard-gate-read-fd",
        str(gate_read),
        "--guard-ready-fd",
        str(ready_write),
        "--guard-exec-status-fd",
        str(exec_write),
        "--",
        *argv,
    ]
    try:
        child = subprocess.Popen(
            child_argv,
            start_new_session=True,
            pass_fds=(gate_read, ready_write, exec_write),
        )
    except BaseException:
        for fd in (gate_read, gate_write, ready_read, ready_write, exec_read, exec_write):
            try:
                os.close(fd)
            except OSError:
                pass
        raise
    for fd in (gate_read, ready_write, exec_write):
        try:
            os.close(fd)
        except OSError:
            pass
    try:
        ready = _read_one_with_timeout(ready_read, 5.0)
    finally:
        os.close(ready_read)
    if ready != b"R":
        try:
            os.close(gate_write)
        except OSError:
            pass
        try:
            os.close(exec_read)
        except OSError:
            pass
        _terminate_guarded_tree(child, {})
        raise RuntimeError("guarded launch process did not reach the closed gate")
    return child, gate_write, exec_read


def _confirm_gated_exec(exec_status_fd: int) -> tuple[bool, str, int | None]:
    """Return a Popen-proved launch result; EOF alone is never confirmation."""
    try:
        data = _read_one_with_timeout(exec_status_fd, 5.0)
    except TimeoutError as exc:
        raise _LaunchCompletionUnknown(str(exc)) from exc
    finally:
        try:
            os.close(exec_status_fd)
        except OSError:
            pass
    if not data:
        raise _LaunchCompletionUnknown(
            "guard control process closed its status pipe without launch proof"
        )
    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _LaunchCompletionUnknown("guard launch status was malformed") from exc
    if not isinstance(payload, dict):
        raise _LaunchCompletionUnknown("guard launch status was not an object")
    status = payload.get("status")
    if status == "exec_confirmed":
        pid = payload.get("pid")
        if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
            raise _LaunchCompletionUnknown("guard launch proof omitted user pid")
        return True, "", pid
    if status == "exec_failed":
        return False, str(payload.get("error") or "guarded exec failed before argv started"), None
    raise _LaunchCompletionUnknown("guard launch status was unrecognized")

def _guard_base(
    *,
    status: str,
    reason: str,
    detail: str,
    command_started: bool,
    command_exit_code: int | None,
    helper_exit_code: int,
    max_command_bytes: int,
    min_available_bytes: int,
    host_total_bytes: int | None = None,
    initial_host_available_bytes: int | None = None,
    min_host_available_bytes: int | None = None,
    peak_command_bytes: int | None = None,
    sample_count: int = 0,
    process_tree_drained: bool | None = None,
    forced_descendant_cleanup: bool = False,
    cleanup_complete: bool | None = None,
) -> dict[str, object]:
    return {
        "schema": 1,
        "status": status,
        "reason": reason,
        "detail": detail,
        "command_started": command_started,
        "command_exit_code": command_exit_code,
        "helper_exit_code": helper_exit_code,
        "max_command_bytes": max_command_bytes,
        "min_available_bytes": min_available_bytes,
        "host_total_bytes": host_total_bytes,
        "initial_host_available_bytes": initial_host_available_bytes,
        "min_host_available_bytes": min_host_available_bytes,
        "peak_command_bytes": peak_command_bytes,
        "sample_count": sample_count,
        "sample_interval_ms": int(SAMPLE_INTERVAL_S * 1000),
        "memory_metric": "rss_sum_sampled",
        "process_tree_drained": process_tree_drained,
        "forced_descendant_cleanup": forced_descendant_cleanup,
        "cleanup_complete": cleanup_complete,
        "detached_children_possible": True,
        "platform": "darwin" if sys.platform == "darwin" else "posix",
    }


def _emit_guard_ready(token: str) -> None:
    sys.stderr.write(f"\n__REMRUN_GUARD_READY_{token}__\n")
    sys.stderr.flush()


def _emit_guard_result(token: str, payload: dict[str, object]) -> None:
    try:
        sys.stderr.write(
            f"\n__REMRUN_GUARD_RESULT_{token}__ "
            + json.dumps(payload, separators=(",", ":"))
            + "\n"
        )
        sys.stderr.flush()
    except Exception:
        pass


def _identity_for_pid(pid: int) -> str | None:
    if pid <= 0:
        return None
    if sys.platform.startswith("linux") and os.path.exists("/proc/self/stat"):
        try:
            text = open(f"/proc/{pid}/stat", encoding="ascii").read()
            end = text.rfind(")")
            fields = text[end + 2:].split()
            return f"{pid}:{fields[19]}"
        except (FileNotFoundError, PermissionError, ProcessLookupError, IndexError):
            return None
    if sys.platform == "darwin":
        try:
            bsd = _ProcBSDInfo()
            lib = _darwin_libsystem()
            if lib.proc_pidinfo(
                pid,
                PROC_PIDTBSDINFO,
                0,
                ctypes.byref(bsd),
                ctypes.sizeof(bsd),
            ) != ctypes.sizeof(bsd):
                return None
            return f"{pid}:{int(bsd.pbi_start_tvsec)}:{int(bsd.pbi_start_tvusec)}"
        except Exception:
            return None
    try:
        row = _processes().get(pid)
        return row.identity if row is not None else None
    except Exception:
        return None


def _known_alive(known: dict[int, str]) -> list[int]:
    alive: list[int] = []
    for pid, identity in tuple(known.items()):
        if _identity_for_pid(pid) == identity:
            alive.append(pid)
    return alive


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _signal_guarded_tree(pgid: int, known: dict[int, str], sig: int) -> None:
    try:
        os.killpg(pgid, sig)
    except (ProcessLookupError, PermissionError):
        pass
    for pid in _known_alive(known):
        try:
            os.kill(pid, sig)
        except (ProcessLookupError, PermissionError):
            pass


def _terminate_guarded_tree(
    child: subprocess.Popen,
    known: dict[int, str],
) -> bool:
    """Boundedly terminate the process group plus identity-verified known escapees."""
    _signal_guarded_tree(child.pid, known, signal.SIGTERM)
    deadline = time.monotonic() + 0.25
    while time.monotonic() < deadline:
        if not _group_alive(child.pid) and not _known_alive(known):
            break
        time.sleep(0.02)
    _signal_guarded_tree(child.pid, known, signal.SIGKILL)
    deadline = time.monotonic() + 0.75
    while time.monotonic() < deadline:
        if not _group_alive(child.pid) and not _known_alive(known):
            break
        time.sleep(0.02)
    try:
        child.wait(timeout=0.1)
    except (subprocess.TimeoutExpired, ChildProcessError):
        pass
    return not _group_alive(child.pid) and not _known_alive(known)


def _record_detailed_guard_sample(
    samples: Samples,
    tree: list[ProcessRow],
    *,
    host_total: int,
    host_available: int,
    now: float,
) -> None:
    samples.process_samples += 1
    if tree:
        samples.observed_process_samples += 1
        rss = sum(row.rss_bytes for row in tree)
        samples.peak_rss_bytes = max(samples.peak_rss_bytes or 0, rss)
    samples.host_total_bytes = host_total
    samples.min_available_bytes = min(
        samples.min_available_bytes
        if samples.min_available_bytes is not None
        else host_available,
        host_available,
    )
    used_pct = 100.0 * (1.0 - host_available / host_total)
    samples.max_host_used_pct = max(samples.max_host_used_pct or 0.0, used_pct)
    if now >= samples._next_pressure_at:
        try:
            counter = _host_cpu()
            if samples._host_cpu_prior is not None:
                total = counter[0] - samples._host_cpu_prior[0]
                idle = counter[1] - samples._host_cpu_prior[1]
                if total > 0 and 0 <= idle <= total:
                    samples._host_busy.append(100.0 * (1.0 - idle / total))
            samples._host_cpu_prior = counter
        except Exception as exc:
            samples.errors.append(f"host cpu: {type(exc).__name__}")
        samples._next_pressure_at = now + PRESSURE_SAMPLE_INTERVAL_S
    if now >= samples._next_gpu_at:
        samples._sample_gpu()
        samples._next_gpu_at = now + GPU_SAMPLE_INTERVAL_S


def _guard_telemetry_payload(
    *,
    detailed: bool,
    peak_bytes: int | None,
    sample_count: int,
    wall_sec: float,
    usage: resource.struct_rusage | None,
    host_total: int | None,
    min_host_available: int | None,
    process_tree_drained: bool,
    forced_cleanup: bool,
    samples: Samples | None,
) -> dict[str, object]:
    cpu_sec = (
        float(usage.ru_utime) + float(usage.ru_stime)
        if usage is not None
        else None
    )
    avg_cpu = (
        cpu_sec / wall_sec * 100.0
        if cpu_sec is not None and wall_sec > 0
        else None
    )
    if not detailed:
        return {
            "peak_rss_mb": round(peak_bytes / MIB, 1) if peak_bytes is not None else None,
            "avg_cpu_pct": round(avg_cpu, 1) if avg_cpu is not None else None,
            "cpu_sec": round(cpu_sec, 3) if cpu_sec is not None else None,
            "wall_sec": round(wall_sec, 3),
            "process_tree_drained": process_tree_drained,
            "detached_children_possible": True,
        }

    if sample_count == 0 or peak_bytes is None:
        memory_coverage = "sampler_failed"
    elif sample_count < 2:
        memory_coverage = "short_lived_sampled"
    elif process_tree_drained and not forced_cleanup:
        memory_coverage = "known_tree_drained"
    else:
        memory_coverage = "known_tree_cutoff"
    cpu_coverage = (
        "wait4_known_tree_drained_detached_possible"
        if cpu_sec is not None and process_tree_drained and not forced_cleanup
        else "wait4_known_tree_cutoff"
        if cpu_sec is not None
        else "sampler_failed"
    )
    gpu = samples.gpu_payload() if samples is not None else {
        "scope": "whole_device",
        "kind": "unknown",
        "max_util_pct": None,
        "min_vram_free_bytes": None,
        "status": "unavailable",
        "devices": [],
    }
    return {
        "schema": 1,
        "status": "ok" if peak_bytes is not None and cpu_sec is not None else "partial",
        "cpu": {
            "cpu_sec": round(cpu_sec, 3) if cpu_sec is not None else None,
            "avg_cpu_pct": round(avg_cpu, 1) if avg_cpu is not None else None,
            "coverage": cpu_coverage,
            "whole_device": (
                samples.host_cpu_payload()
                if samples is not None
                else {
                    "scope": "whole_device",
                    "avg_busy_pct": None,
                    "max_busy_pct": None,
                    "status": "unavailable",
                }
            ),
        },
        "memory": {
            "peak_bytes": peak_bytes,
            "metric": "rss_sum_sampled",
            "sample_interval_ms": int(SAMPLE_INTERVAL_S * 1000),
            "sample_count": sample_count,
            "coverage": memory_coverage,
            "shared_page_semantics": "may_double_count",
            "whole_device": {
                "scope": "whole_device",
                "total_bytes": host_total,
                "min_available_bytes": min_host_available,
                "max_used_pct": (
                    round(100.0 * (1.0 - min_host_available / host_total), 1)
                    if host_total and min_host_available is not None
                    else None
                ),
                "status": "measured" if min_host_available is not None else "unavailable",
            },
        },
        "gpu": gpu,
        "peak_rss_mb": round(peak_bytes / MIB, 1) if peak_bytes is not None else None,
        "avg_cpu_pct": round(avg_cpu, 1) if avg_cpu is not None else None,
        "cpu_sec": round(cpu_sec, 3) if cpu_sec is not None else None,
        "wall_sec": round(wall_sec, 3),
        "process_tree_drained": process_tree_drained and not forced_cleanup,
        "detached_children_possible": True,
        "forced_descendant_cleanup": forced_cleanup,
    }


def _best_effort_release_guard_lease(
    lease_request: dict[str, object] | None,
    *,
    reserved_only: bool,
    cleanup_complete: bool = False,
) -> None:
    if lease_request is None:
        return
    try:
        _release_memory_lease(
            {
                **lease_request,
                "schema": _ADMISSION_SCHEMA,
                "op": "release",
                "reserved_only": reserved_only,
                "cleanup_complete": cleanup_complete,
            }
        )
    except Exception:
        # A reserved lease remains bounded by its target-side expiry. A claimed
        # lease remains conservatively owned while its helper/process group lives.
        pass


def _survivor_records(pgid: int, known: dict[int, str]) -> list[dict[str, object]]:
    records: dict[int, str] = {}
    try:
        rows = _processes()
    except Exception:
        rows = {}
    for pid, identity in known.items():
        if _identity_for_pid(pid) == identity:
            records[pid] = identity
    for pid, row in rows.items():
        if row.pgid == pgid and _identity_for_pid(pid) == row.identity:
            records[pid] = row.identity
    return [
        {"pid": pid, "identity": identity}
        for pid, identity in sorted(records.items())
    ]


def _quarantine_or_hold_guard_lease(
    lease_request: dict[str, object] | None,
    *,
    pgid: int,
    known: dict[int, str],
) -> None:
    """Persist survivor ownership; if ledger I/O fails, keep this helper alive."""
    if lease_request is None:
        return
    while True:
        survivors = _survivor_records(pgid, known)
        if not survivors and not _group_alive(pgid):
            _best_effort_release_guard_lease(
                lease_request, reserved_only=False, cleanup_complete=True
            )
            return
        try:
            if _quarantine_memory_lease(
                lease_request, survivors=survivors, pgid=pgid
            ):
                return
        except Exception:
            pass
        # The still-live helper identity keeps the existing claimed lease owned
        # while persistence is retried. Never exit and let stale reaping free
        # capacity around a survivor that cleanup failed to terminate.
        time.sleep(SAMPLE_INTERVAL_S)


def _guarded_run(
    argv: list[str],
    *,
    max_command_bytes: int,
    min_available_bytes: int,
    token: str,
    detailed: bool,
    telemetry: bool,
    lease_request: dict[str, object] | None = None,
) -> int:
    helper_exit = 125
    initial_available: int | None = None
    host_total: int | None = None
    try:
        if not re.fullmatch(r"[0-9a-f]{32}", token):
            raise ValueError("guard token must be 32 lowercase hexadecimal characters")
        if max_command_bytes <= 0 or min_available_bytes <= 0:
            raise ValueError("guard thresholds must be positive")
        if lease_request is None:
            raise ValueError("guarded launch requires a target-local reservation")
        _validated_admission_request({**lease_request, "op": "renew"})
        if lease_request.get("allowance_bytes") != max_command_bytes:
            raise ValueError("guard allowance does not match the reservation")
        rows = _processes()
        if os.getpid() not in rows:
            raise RuntimeError("guard process is absent from the process table")
        host_total, initial_available = _host_memory()
        if host_total <= 0 or not 0 <= initial_available <= host_total:
            raise RuntimeError("invalid initial host-memory counters")
        if max_command_bytes + min_available_bytes > host_total:
            payload = _guard_base(
                status="refused",
                reason="thresholds_exceed_host",
                detail="configured command ceiling plus reserve exceeds measured physical memory",
                command_started=False,
                command_exit_code=None,
                helper_exit_code=helper_exit,
                max_command_bytes=max_command_bytes,
                min_available_bytes=min_available_bytes,
                host_total_bytes=host_total,
                initial_host_available_bytes=initial_available,
                min_host_available_bytes=initial_available,
            )
            _best_effort_release_guard_lease(lease_request, reserved_only=True)
            _emit_guard_result(token, payload)
            return helper_exit
        if initial_available <= min_available_bytes:
            payload = _guard_base(
                status="refused",
                reason="host_memory_reserve",
                detail="initial host available memory is at or below the configured reserve",
                command_started=False,
                command_exit_code=None,
                helper_exit_code=helper_exit,
                max_command_bytes=max_command_bytes,
                min_available_bytes=min_available_bytes,
                host_total_bytes=host_total,
                initial_host_available_bytes=initial_available,
                min_host_available_bytes=initial_available,
            )
            _best_effort_release_guard_lease(lease_request, reserved_only=True)
            _emit_guard_result(token, payload)
            return helper_exit
    except Exception as exc:
        payload = _guard_base(
            status="refused",
            reason="guard_initialization_failed",
            detail=f"{type(exc).__name__}: {exc}",
            command_started=False,
            command_exit_code=None,
            helper_exit_code=helper_exit,
            max_command_bytes=max_command_bytes,
            min_available_bytes=min_available_bytes,
            host_total_bytes=host_total,
            initial_host_available_bytes=initial_available,
            min_host_available_bytes=initial_available,
        )
        _best_effort_release_guard_lease(lease_request, reserved_only=True)
        _emit_guard_result(token, payload)
        return helper_exit

    child: subprocess.Popen | None = None
    known: dict[int, str] = {}
    peak_bytes: int | None = None
    min_host_available = initial_available
    sample_count = 0
    usage: resource.struct_rusage | None = None
    direct_rc: int | None = None
    started = time.monotonic()
    samples = Samples() if detailed else None
    old_handlers: dict[int, object] = {}
    gate_write: int | None = None
    exec_status_fd: int | None = None
    gate_release_attempted = False
    exec_confirmed = False
    lease_claimed = False

    def interrupted(signum, _frame):  # noqa: ANN001
        raise _GuardInterrupted(f"signal {signum}")

    def release_lease(
        *, reserved_only: bool = False, cleanup_complete: bool = False
    ) -> None:
        _best_effort_release_guard_lease(
            lease_request,
            reserved_only=reserved_only,
            cleanup_complete=cleanup_complete,
        )

    def finalize_claimed_lease(cleanup_complete: bool) -> None:
        if cleanup_complete:
            release_lease(cleanup_complete=True)
        elif child is not None:
            _quarantine_or_hold_guard_lease(
                lease_request, pgid=child.pid, known=known
            )

    try:
        for signum in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
            old_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, interrupted)
        # Start only a closed-gate control process. It cannot execute user argv
        # until the reservation is atomically claimed and the parent writes "G".
        child, gate_write, exec_status_fd = _spawn_gated_command(argv)
        root_identity = _identity_for_pid(child.pid)
        if root_identity is None:
            raise RuntimeError("cannot identify the guarded launch process")
        known[child.pid] = root_identity
        assert lease_request is not None
        claim = _claim_memory_lease(
            lease_request,
            helper_pid=os.getpid(),
            root_pid=child.pid,
            root_identity=root_identity,
            pgid=child.pid,
        )
        if claim.get("status") != "admitted":
            try:
                os.close(gate_write)
            except OSError:
                pass
            gate_write = None
            cleanup = _terminate_guarded_tree(child, known)
            release_lease(reserved_only=True)
            payload = _guard_base(
                status="refused",
                reason=str(claim.get("reason") or "memory_admission_refused"),
                detail=str(claim.get("detail") or "helper claim was refused"),
                command_started=False,
                command_exit_code=None,
                helper_exit_code=helper_exit,
                max_command_bytes=max_command_bytes,
                min_available_bytes=min_available_bytes,
                host_total_bytes=host_total,
                initial_host_available_bytes=initial_available,
                min_host_available_bytes=initial_available,
                process_tree_drained=cleanup,
                cleanup_complete=cleanup,
            )
            payload["memory_admission"] = claim
            _emit_guard_result(token, payload)
            return helper_exit
        lease_claimed = True
        # READY means all fail-safe machinery and the target-local claim exist.
        # User argv still cannot start until the following one-byte gate release.
        _emit_guard_ready(token)
        # From this assignment onward, a truthful false value is impossible unless
        # the private exec-status pipe explicitly proves that execvp failed.  Mark
        # the transition before the write so an interruption in the write/close
        # window is conservatively completion-unknown rather than falsely "not started".
        gate_release_attempted = True
        os.write(gate_write, b"G")
        os.close(gate_write)
        gate_write = None
        _after_gate_release()
        assert exec_status_fd is not None
        exec_confirmed, exec_failure, user_pid = _confirm_gated_exec(exec_status_fd)
        exec_status_fd = None
        if not exec_confirmed:
            cleanup = _terminate_guarded_tree(child, known)
            finalize_claimed_lease(cleanup)
            payload = _guard_base(
                status="refused",
                reason="command_launch_failed",
                detail=exec_failure,
                command_started=False,
                command_exit_code=None,
                helper_exit_code=helper_exit,
                max_command_bytes=max_command_bytes,
                min_available_bytes=min_available_bytes,
                host_total_bytes=host_total,
                initial_host_available_bytes=initial_available,
                min_host_available_bytes=initial_available,
                process_tree_drained=cleanup,
                cleanup_complete=cleanup,
            )
            _emit_guard_result(token, payload)
            return helper_exit
        if user_pid is not None:
            user_identity = _identity_for_pid(user_pid)
            if user_identity is not None:
                known[user_pid] = user_identity

        next_sample = time.monotonic()
        guard_status: str | None = None
        guard_reason = ""
        guard_detail = ""
        trigger_value: int | None = None

        while direct_rc is None and guard_status is None:
            now = time.monotonic()
            if now >= next_sample:
                try:
                    rows = _processes()
                    tree = _known_tree(rows, child.pid, known, direct_alive=True)
                    if child.pid not in known and child.pid in rows:
                        known[child.pid] = rows[child.pid].identity
                    user_tree = [row for row in tree if row.pid != child.pid]
                    rss = sum(row.rss_bytes for row in user_tree)
                    peak_bytes = max(peak_bytes or 0, rss)
                    sample_count += 1
                    current_total, available = _host_memory()
                    if (
                        current_total <= 0
                        or not 0 <= available <= current_total
                        or current_total != host_total
                    ):
                        raise RuntimeError("invalid or changed host-memory counters")
                    min_host_available = min(
                        min_host_available
                        if min_host_available is not None
                        else available,
                        available,
                    )
                    if samples is not None:
                        _record_detailed_guard_sample(
                            samples,
                            user_tree,
                            host_total=current_total,
                            host_available=available,
                            now=now,
                        )
                    if rss >= max_command_bytes:
                        guard_status = "terminated"
                        guard_reason = "command_memory_limit"
                        guard_detail = "sampled command-tree RSS reached the hard ceiling"
                        trigger_value = rss
                    elif available <= min_available_bytes:
                        guard_status = "terminated"
                        guard_reason = "host_memory_reserve"
                        guard_detail = "host available memory reached the hard reserve"
                        trigger_value = available
                except Exception as exc:
                    guard_status = "failed_safe"
                    guard_reason = "enforcement_sampling_failed"
                    guard_detail = f"{type(exc).__name__}: {exc}"
                next_sample = now + SAMPLE_INTERVAL_S

            if guard_status is None:
                try:
                    waited = _wait4_nohang(child)
                except ChildProcessError:
                    waited = None
                    if child.returncode is not None:
                        direct_rc = int(child.returncode)
                if waited is not None:
                    direct_rc, usage = waited
                    break
                time.sleep(min(0.02, max(0.001, next_sample - time.monotonic())))

        if guard_status is not None:
            cleanup = _terminate_guarded_tree(child, known)
            if not cleanup:
                trigger_reason = guard_reason
                guard_status = "failed_safe"
                guard_reason = "termination_cleanup_failed"
                guard_detail = (
                    f"{guard_detail}; bounded process-tree termination did not verify a drain"
                )
            else:
                trigger_reason = None
            payload = _guard_base(
                status=guard_status,
                reason=guard_reason,
                detail=guard_detail,
                command_started=True,
                command_exit_code=None,
                helper_exit_code=helper_exit,
                max_command_bytes=max_command_bytes,
                min_available_bytes=min_available_bytes,
                host_total_bytes=host_total,
                initial_host_available_bytes=initial_available,
                min_host_available_bytes=min_host_available,
                peak_command_bytes=peak_bytes,
                sample_count=sample_count,
                process_tree_drained=cleanup,
                cleanup_complete=cleanup,
            )
            if trigger_value is not None:
                payload["trigger_value_bytes"] = trigger_value
            if trigger_reason is not None:
                payload["trigger_reason"] = trigger_reason
            if detailed or telemetry:
                _emit(
                    _guard_telemetry_payload(
                        detailed=detailed,
                        peak_bytes=peak_bytes,
                        sample_count=sample_count,
                        wall_sec=max(0.0, time.monotonic() - started),
                        usage=usage,
                        host_total=host_total,
                        min_host_available=min_host_available,
                        process_tree_drained=cleanup,
                        forced_cleanup=True,
                        samples=samples,
                    )
                )
            finalize_claimed_lease(cleanup)
            _emit_guard_result(token, payload)
            return helper_exit

        direct_wall = max(0.0, time.monotonic() - started)
        deadline = time.monotonic() + DESCENDANT_GRACE_S
        drained = False
        active = 0
        guard_status = None
        guard_reason = ""
        guard_detail = ""
        trigger_value = None
        while True:
            try:
                rows = _processes()
                tree = _known_tree(rows, child.pid, known, direct_alive=False)
                user_tree = [row for row in tree if row.pid != child.pid]
                rss = sum(row.rss_bytes for row in user_tree)
                peak_bytes = max(peak_bytes or 0, rss)
                sample_count += 1
                active = len(user_tree)
                current_total, available = _host_memory()
                if (
                    current_total <= 0
                    or not 0 <= available <= current_total
                    or current_total != host_total
                ):
                    raise RuntimeError("invalid or changed host-memory counters")
                min_host_available = min(
                    min_host_available
                    if min_host_available is not None
                    else available,
                    available,
                )
                if samples is not None:
                    _record_detailed_guard_sample(
                        samples,
                        user_tree,
                        host_total=current_total,
                        host_available=available,
                        now=time.monotonic(),
                    )
                if rss >= max_command_bytes:
                    guard_status = "terminated"
                    guard_reason = "command_memory_limit"
                    guard_detail = "sampled descendant RSS reached the hard ceiling"
                    trigger_value = rss
                    break
                if available <= min_available_bytes:
                    guard_status = "terminated"
                    guard_reason = "host_memory_reserve"
                    guard_detail = "host available memory reached the hard reserve"
                    trigger_value = available
                    break
                if active == 0 and not _group_alive(child.pid):
                    drained = True
                    break
            except Exception as exc:
                guard_status = "failed_safe"
                guard_reason = "enforcement_sampling_failed"
                guard_detail = f"{type(exc).__name__}: {exc}"
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(SAMPLE_INTERVAL_S, remaining))

        forced_cleanup = not drained and (active > 0 or _group_alive(child.pid))
        cleanup = True
        if guard_status is not None or forced_cleanup:
            cleanup = _terminate_guarded_tree(child, known)
        trigger_reason = None
        if not cleanup:
            if guard_status is None:
                guard_status = "failed_safe"
                guard_reason = "descendant_cleanup_failed"
                guard_detail = "known descendants remained after bounded SIGKILL cleanup"
            else:
                trigger_reason = guard_reason
                guard_status = "failed_safe"
                guard_reason = "termination_cleanup_failed"
                guard_detail = (
                    f"{guard_detail}; bounded process-tree termination did not verify a drain"
                )
        if guard_status is not None:
            payload = _guard_base(
                status=guard_status,
                reason=guard_reason,
                detail=guard_detail,
                command_started=True,
                command_exit_code=None,
                helper_exit_code=helper_exit,
                max_command_bytes=max_command_bytes,
                min_available_bytes=min_available_bytes,
                host_total_bytes=host_total,
                initial_host_available_bytes=initial_available,
                min_host_available_bytes=min_host_available,
                peak_command_bytes=peak_bytes,
                sample_count=sample_count,
                process_tree_drained=cleanup,
                forced_descendant_cleanup=forced_cleanup,
                cleanup_complete=cleanup,
            )
            if trigger_value is not None:
                payload["trigger_value_bytes"] = trigger_value
            if trigger_reason is not None:
                payload["trigger_reason"] = trigger_reason
            if detailed or telemetry:
                _emit(
                    _guard_telemetry_payload(
                        detailed=detailed,
                        peak_bytes=peak_bytes,
                        sample_count=sample_count,
                        wall_sec=direct_wall,
                        usage=usage,
                        host_total=host_total,
                        min_host_available=min_host_available,
                        process_tree_drained=cleanup,
                        forced_cleanup=forced_cleanup,
                        samples=samples,
                    )
                )
            finalize_claimed_lease(cleanup)
            _emit_guard_result(token, payload)
            return helper_exit

        assert direct_rc is not None
        command_exit = _shell_exit_code(int(direct_rc))
        if detailed or telemetry:
            _emit(
                _guard_telemetry_payload(
                    detailed=detailed,
                    peak_bytes=peak_bytes,
                    sample_count=sample_count,
                    wall_sec=direct_wall,
                    usage=usage,
                    host_total=host_total,
                    min_host_available=min_host_available,
                    process_tree_drained=drained,
                    forced_cleanup=forced_cleanup,
                    samples=samples,
                )
            )
        payload = _guard_base(
            status="ok",
            reason="completed",
            detail="command completed without crossing the memory guard",
            command_started=True,
            command_exit_code=command_exit,
            helper_exit_code=command_exit,
            max_command_bytes=max_command_bytes,
            min_available_bytes=min_available_bytes,
            host_total_bytes=host_total,
            initial_host_available_bytes=initial_available,
            min_host_available_bytes=min_host_available,
            peak_command_bytes=peak_bytes,
            sample_count=sample_count,
            process_tree_drained=drained,
            forced_descendant_cleanup=forced_cleanup,
            cleanup_complete=cleanup,
        )
        finalize_claimed_lease(cleanup)
        _emit_guard_result(token, payload)
        return command_exit

    except BaseException as exc:
        if gate_write is not None:
            try:
                os.close(gate_write)
            except OSError:
                pass
            gate_write = None
        if exec_status_fd is not None:
            try:
                os.close(exec_status_fd)
            except OSError:
                pass
            exec_status_fd = None
        cleanup = True
        if child is not None:
            cleanup = _terminate_guarded_tree(child, known)
        if lease_claimed:
            finalize_claimed_lease(cleanup)
        else:
            release_lease(reserved_only=True)
        if gate_release_attempted and not exec_confirmed:
            # The gate-release transition began, but the CLOEXEC status pipe never
            # proved whether argv replaced the control process. Emitting false here would
            # be a lie; omit the private result so the transport enters its existing
            # completion-unknown fail-safe path.
            try:
                sys.stderr.write(
                    "remrun memory guard: launch completion unknown after gate release attempt: "
                    f"{type(exc).__name__}: {exc}\n"
                )
                sys.stderr.flush()
            except Exception:
                pass
            return helper_exit
        payload = _guard_base(
            status="failed_safe" if exec_confirmed else "refused",
            reason=(
                "guard_interrupted" if isinstance(exc, _GuardInterrupted)
                else "guard_runtime_failed" if exec_confirmed
                else "command_launch_failed"
            ),
            detail=f"{type(exc).__name__}: {exc}",
            command_started=exec_confirmed,
            command_exit_code=None,
            helper_exit_code=helper_exit,
            max_command_bytes=max_command_bytes,
            min_available_bytes=min_available_bytes,
            host_total_bytes=host_total,
            initial_host_available_bytes=initial_available,
            min_host_available_bytes=min_host_available,
            peak_command_bytes=peak_bytes,
            sample_count=sample_count,
            process_tree_drained=cleanup if child is not None else None,
            cleanup_complete=cleanup if child is not None else None,
        )
        _emit_guard_result(token, payload)
        return helper_exit
    finally:
        for fd in (gate_write, exec_status_fd):
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
        for signum, handler in old_handlers.items():
            try:
                signal.signal(signum, handler)
            except Exception:
                pass


def main() -> int:
    if len(sys.argv) == 3 and sys.argv[1] == "--memory-admission":
        try:
            raw = base64.urlsafe_b64decode(sys.argv[2].encode("ascii"))
            request = json.loads(raw.decode("utf-8"))
            payload = _handle_admission_request(request)
        except BaseException as exc:
            payload = _admission_result(
                "refused",
                "admission_failed_safe",
                f"{type(exc).__name__}: {exc}",
            )
        _emit_admission_result(payload)
        return 0

    try:
        separator = sys.argv.index("--")
        options = sys.argv[1:separator]
        argv = sys.argv[separator + 1:]
    except ValueError:
        sys.stderr.write(
            "usage: python _posix_telemetry.py [--detailed] "
            "[--guard-max-bytes N --guard-min-available-bytes N --guard-token TOKEN] "
            "-- <command> [args...]\n"
        )
        return 2

    gate_names = {
        "--guard-gate-read-fd",
        "--guard-ready-fd",
        "--guard-exec-status-fd",
    }
    if any(name in options for name in gate_names):
        try:
            values = {}
            for name in gate_names:
                index = options.index(name)
                values[name] = int(options[index + 1])
            if len(options) != 6 or not argv:
                raise ValueError("invalid guarded gate options")
            return _gate_exec_child(
                argv,
                gate_fd=values["--guard-gate-read-fd"],
                ready_fd=values["--guard-ready-fd"],
                exec_status_fd=values["--guard-exec-status-fd"],
            )
        except BaseException as exc:
            try:
                sys.stderr.write(f"remrun guarded gate failed: {type(exc).__name__}: {exc}\n")
            except Exception:
                pass
            return 125

    detailed = "--detailed" in options
    telemetry = "--telemetry" in options

    def option_value(name: str) -> str | None:
        if name not in options:
            return None
        index = options.index(name)
        if index + 1 >= len(options):
            raise ValueError(f"{name} requires a value")
        return options[index + 1]

    guard_names = {
        "--guard-max-bytes",
        "--guard-min-available-bytes",
        "--guard-token",
        "--guard-lease-b64",
    }
    guard_requested = any(name in options for name in guard_names)
    allowed = {"--detailed", "--telemetry", *guard_names}
    consumed_value_indexes = {
        index + 1
        for index, value in enumerate(options)
        if value in guard_names and index + 1 < len(options)
    }
    unknown = [
        value
        for index, value in enumerate(options)
        if index not in consumed_value_indexes and value not in allowed
    ]
    if unknown or not argv:
        sys.stderr.write("remrun telemetry wrapper: invalid options or empty command\n")
        return 2

    if guard_requested:
        token = option_value("--guard-token") or ""
        try:
            max_bytes = int(option_value("--guard-max-bytes") or "")
            reserve_bytes = int(option_value("--guard-min-available-bytes") or "")
            lease_raw = base64.urlsafe_b64decode(
                (option_value("--guard-lease-b64") or "").encode("ascii")
            )
            lease_request = json.loads(lease_raw.decode("utf-8"))
            if not isinstance(lease_request, dict):
                raise ValueError("guard lease request must be an object")
        except ValueError:
            max_bytes = reserve_bytes = -1
            lease_request = None
        except (UnicodeDecodeError, json.JSONDecodeError, binascii.Error):
            max_bytes = reserve_bytes = -1
            lease_request = None
        return _guarded_run(
            argv,
            max_command_bytes=max_bytes,
            min_available_bytes=reserve_bytes,
            token=token,
            detailed=detailed,
            telemetry=telemetry,
            lease_request=lease_request,
        )

    if not detailed:
        sys.stderr.write(
            "usage: python _posix_telemetry.py --detailed -- <command> [args...]\n"
        )
        return 2
    try:
        rc, payload = _detailed_run(argv)
    except _CommandNotStarted as exc:
        started = time.monotonic()
        rc = _plain_run(argv)
        payload = _unknown_payload(
            f"sampler unavailable before command start: {type(exc).__name__}",
            wall_sec=time.monotonic() - started,
        )
    _emit(payload)
    return rc


if __name__ == "__main__":
    sys.exit(main())
