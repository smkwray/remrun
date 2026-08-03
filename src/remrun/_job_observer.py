"""Stdlib-only target helper for ``remrun fleet jobs``.

The helper has two operations:

* ``run`` establishes a target-local ownership boundary, registers it before user
  code can start, launches one already-constructed argv, and retains the bounded
  row while a truthfully attributable process tree survives.
* ``query`` opens that registry read-only, takes at most two shared process-table
  snapshots, and emits one JSON document. It never deletes or repairs state.

POSIX ownership is a private process group with an exact generation witness.
Windows ownership is a named Job Object whose handle is retained by one detached,
per-active-job keeper process after the direct root exits. Deliberate POSIX
group/session changes and explicit Windows breakaway are outside coverage. The
file is staged onto targets by built-in transports and intentionally imports
nothing from remrun.
"""
from __future__ import annotations

import argparse
import base64
import ctypes
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

REGISTRY_SCHEMA = 1
OWNED_REGISTRY_SCHEMA = 2
QUERY_SCHEMA = 1
MAX_ACTIVE_JOBS = 256
MAX_DB_ROWS_READ = MAX_ACTIVE_JOBS + 1
MAX_MIXED_RECORDS = MAX_ACTIVE_JOBS * 2
DB_RELATIVE = ("jobs", "active-v1.sqlite3")
LEGACY_TABLE = "active_jobs"
OWNED_TABLE = "owned_jobs_v2"
_WIN_KEEPER_READY_TIMEOUT = 5.0
_WIN_KEEPER_POLL_SECONDS = 1.0
_WIN_KEEPER_CLEANUP_RETRIES = 50
_SCHEMA_READY_ATTEMPTS = 25
_SCHEMA_READY_DELAY_SECONDS = 0.02
_SAFE = re.compile(r"^[A-Za-z0-9._:@+-]+$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class ProcessRow:
    pid: int
    ppid: int
    identity: str | None
    start_order: int | None
    cpu_sec: float | None
    rss_bytes: int | None
    threads: int | None
    pgid: int | None = None


class RegistryError(RuntimeError):
    pass


class RegistryFull(RegistryError):
    pass


def _bounded_text(value: object, name: str, limit: int) -> str:
    if not isinstance(value, str) or not value or len(value) > limit:
        raise ValueError(f"{name} must be a non-empty string of at most {limit} characters")
    if not _SAFE.fullmatch(value):
        raise ValueError(f"{name} contains unsupported characters")
    return value


def _decode_metadata(encoded: str) -> dict[str, object]:
    if len(encoded) > 4096:
        raise ValueError("metadata is oversized")
    try:
        raw = base64.urlsafe_b64decode(encoded.encode("ascii"))
        data = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("metadata is not valid base64url JSON") from exc
    if not isinstance(data, dict) or data.get("schema") != REGISTRY_SCHEMA:
        raise ValueError("unsupported observation metadata schema")
    digest = data.get("command_sha256")
    if not isinstance(digest, str) or not _HEX64.fullmatch(digest):
        raise ValueError("command_sha256 must be lowercase SHA-256 hex")
    count = data.get("member_count")
    if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 100_000:
        raise ValueError("member_count must be in 1..100000")
    return {
        "schema": REGISTRY_SCHEMA,
        "job_id": _bounded_text(data.get("job_id"), "job_id", 128),
        "project": _bounded_text(data.get("project"), "project", 128),
        "source_controller": _bounded_text(
            data.get("source_controller"), "source_controller", 64
        ),
        "target": _bounded_text(data.get("target"), "target", 64),
        "phase": _bounded_text(data.get("phase"), "phase", 32),
        "command_label": _bounded_text(data.get("command_label"), "command_label", 64),
        "command_sha256": digest,
        "member_count": count,
    }


def _state_root(raw: str) -> Path:
    if not isinstance(raw, str) or not raw or "\x00" in raw or len(raw) > 4096:
        raise ValueError("target state root is invalid")
    root = Path(raw).expanduser()
    if not root.is_absolute():
        raise ValueError("target state root must be absolute")
    return root


def _db_path(root: Path) -> Path:
    return root.joinpath(*DB_RELATIVE)


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS registry_meta ("
        "key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID"
    )
    conn.execute(
        "INSERT OR IGNORE INTO registry_meta(key,value) VALUES('schema','1')"
    )
    row = conn.execute(
        "SELECT value FROM registry_meta WHERE key='schema'"
    ).fetchone()
    if row is None or row[0] != str(REGISTRY_SCHEMA):
        raise RegistryError("unsupported active-job registry schema")

    # Keep the legacy table byte-for-byte compatible with old launch writers.
    # They know only this table, so their launch-side cleanup cannot erase a row
    # written by the v2 ownership lifecycle below.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS active_jobs ("
        "token TEXT PRIMARY KEY, schema INTEGER NOT NULL, job_id TEXT NOT NULL, "
        "project TEXT NOT NULL, source_controller TEXT NOT NULL, target TEXT NOT NULL, "
        "phase TEXT NOT NULL, command_label TEXT NOT NULL, command_sha256 TEXT NOT NULL, "
        "member_count INTEGER NOT NULL, root_pid INTEGER NOT NULL, "
        "root_identity TEXT NOT NULL, started_at_ns INTEGER NOT NULL, "
        "owner_kind TEXT NOT NULL DEFAULT 'legacy_child', owner_key TEXT, "
        "owner_pid INTEGER, owner_identity TEXT, witness_pid INTEGER, witness_identity TEXT"
        ") WITHOUT ROWID"
    )
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(active_jobs)")}
    additions = (
        ("owner_kind", "TEXT NOT NULL DEFAULT 'legacy_child'"),
        ("owner_key", "TEXT"),
        ("owner_pid", "INTEGER"),
        ("owner_identity", "TEXT"),
        ("witness_pid", "INTEGER"),
        ("witness_identity", "TEXT"),
    )
    for name, declaration in additions:
        if name not in columns:
            conn.execute(f"ALTER TABLE active_jobs ADD COLUMN {name} {declaration}")

    conn.execute(
        "CREATE TABLE IF NOT EXISTS owned_jobs_v2 ("
        "token TEXT PRIMARY KEY, schema INTEGER NOT NULL, job_id TEXT NOT NULL, "
        "project TEXT NOT NULL, source_controller TEXT NOT NULL, target TEXT NOT NULL, "
        "phase TEXT NOT NULL, command_label TEXT NOT NULL, command_sha256 TEXT NOT NULL, "
        "member_count INTEGER NOT NULL, root_pid INTEGER NOT NULL, "
        "root_identity TEXT NOT NULL, started_at_ns INTEGER NOT NULL, "
        "owner_kind TEXT NOT NULL, owner_key TEXT, owner_pid INTEGER NOT NULL, "
        "owner_identity TEXT NOT NULL, witness_pid INTEGER NOT NULL, "
        "witness_identity TEXT NOT NULL"
        ") WITHOUT ROWID"
    )



def _writer(root: Path) -> sqlite3.Connection:
    db = _db_path(root)
    db.parent.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        try:
            os.chmod(db.parent, 0o700)
        except OSError:
            pass
    conn = sqlite3.connect(db, timeout=5.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=5000")
    _ensure_schema(conn)
    return conn


def _readonly(root: Path) -> sqlite3.Connection | None:
    db = _db_path(root)
    if not db.exists():
        return None
    uri = "file:" + quote(str(db), safe="/:\\") + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=2000")
    return conn


def _linux_processes() -> dict[int, ProcessRow]:
    rows: dict[int, ProcessRow] = {}
    page_size = int(os.sysconf("SC_PAGE_SIZE"))
    ticks = float(os.sysconf("SC_CLK_TCK"))
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            text = Path(f"/proc/{name}/stat").read_text(encoding="ascii")
            end = text.rfind(")")
            fields = text[end + 2 :].split()
            pid = int(name)
            start_ticks = int(fields[19])
            rows[pid] = ProcessRow(
                pid=pid,
                ppid=int(fields[1]),
                identity=f"linux:{pid}:{start_ticks}",
                start_order=start_ticks,
                cpu_sec=(int(fields[11]) + int(fields[12])) / ticks,
                rss_bytes=max(0, int(fields[21])) * page_size,
                threads=max(0, int(fields[17])),
                pgid=int(fields[2]),
            )
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError, IndexError):
            continue
    return rows


PROC_PIDTBSDINFO = 3
PROC_PIDTASKINFO = 4
_DARWIN_LIB = None


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


def _darwin_lib():
    global _DARWIN_LIB
    if _DARWIN_LIB is None:
        lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
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
        _DARWIN_LIB = lib
    return _DARWIN_LIB


def _darwin_processes() -> dict[int, ProcessRow]:
    lib = _darwin_lib()
    estimate = int(lib.proc_listallpids(None, 0))
    if estimate <= 0:
        raise RuntimeError("proc_listallpids size query failed")
    capacity = max(64, estimate + 64)
    pids = (ctypes.c_int * capacity)()
    count = int(lib.proc_listallpids(pids, ctypes.sizeof(pids)))
    if count <= 0 or count >= capacity:
        raise RuntimeError("proc_listallpids failed or was truncated")
    rows: dict[int, ProcessRow] = {}
    for pid in pids[:count]:
        if pid <= 0:
            continue
        bsd = _ProcBSDInfo()
        if lib.proc_pidinfo(
            pid, PROC_PIDTBSDINFO, 0, ctypes.byref(bsd), ctypes.sizeof(bsd)
        ) != ctypes.sizeof(bsd):
            continue
        task = _ProcTaskInfo()
        if lib.proc_pidinfo(
            pid, PROC_PIDTASKINFO, 0, ctypes.byref(task), ctypes.sizeof(task)
        ) != ctypes.sizeof(task):
            continue
        started = int(bsd.pbi_start_tvsec) * 1_000_000 + int(bsd.pbi_start_tvusec)
        rows[pid] = ProcessRow(
            pid=pid,
            ppid=int(bsd.pbi_ppid),
            identity=f"darwin:{pid}:{started}",
            start_order=started,
            cpu_sec=max(
                0.0,
                (int(task.pti_total_user) + int(task.pti_total_system)) / 1e9,
            ),
            rss_bytes=max(0, int(task.pti_resident_size)),
            threads=max(0, int(task.pti_threadnum)),
            pgid=int(bsd.pbi_pgid),
        )
    return rows


def _windows_processes() -> dict[int, ProcessRow]:
    from ctypes import wintypes

    th32cs_snapprocess = 0x00000002
    process_query_information = 0x0400
    process_query_limited_information = 0x1000
    process_vm_read = 0x0010
    invalid_handle = ctypes.c_void_p(-1).value

    class FileTime(ctypes.Structure):
        _fields_ = [("low", wintypes.DWORD), ("high", wintypes.DWORD)]

    class ProcessEntry32W(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("cntUsage", wintypes.DWORD),
            ("th32ProcessID", wintypes.DWORD),
            ("th32DefaultHeapID", ctypes.c_size_t),
            ("th32ModuleID", wintypes.DWORD),
            ("cntThreads", wintypes.DWORD),
            ("th32ParentProcessID", wintypes.DWORD),
            ("pcPriClassBase", wintypes.LONG),
            ("dwFlags", wintypes.DWORD),
            ("szExeFile", wintypes.WCHAR * 260),
        ]

    class ProcessMemoryCountersEx(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
            ("PrivateUsage", ctypes.c_size_t),
        ]

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    k32.CreateToolhelp32Snapshot.argtypes = (wintypes.DWORD, wintypes.DWORD)
    k32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    k32.Process32FirstW.argtypes = (wintypes.HANDLE, ctypes.POINTER(ProcessEntry32W))
    k32.Process32FirstW.restype = wintypes.BOOL
    k32.Process32NextW.argtypes = (wintypes.HANDLE, ctypes.POINTER(ProcessEntry32W))
    k32.Process32NextW.restype = wintypes.BOOL
    k32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    k32.OpenProcess.restype = wintypes.HANDLE
    k32.GetProcessTimes.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
    )
    k32.GetProcessTimes.restype = wintypes.BOOL
    k32.CloseHandle.argtypes = (wintypes.HANDLE,)
    k32.CloseHandle.restype = wintypes.BOOL
    psapi.GetProcessMemoryInfo.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(ProcessMemoryCountersEx),
        wintypes.DWORD,
    )
    psapi.GetProcessMemoryInfo.restype = wintypes.BOOL

    def ft(value: FileTime) -> int:
        return (int(value.high) << 32) | int(value.low)

    snap = k32.CreateToolhelp32Snapshot(th32cs_snapprocess, 0)
    snap_value = snap.value if hasattr(snap, "value") else snap
    if not snap_value or snap_value == invalid_handle:
        raise OSError(ctypes.get_last_error(), "CreateToolhelp32Snapshot failed")
    rows: dict[int, ProcessRow] = {}
    try:
        entry = ProcessEntry32W()
        entry.dwSize = ctypes.sizeof(entry)
        ok = bool(k32.Process32FirstW(snap, ctypes.byref(entry)))
        while ok:
            pid = int(entry.th32ProcessID)
            identity = None
            start_order = None
            cpu_sec = None
            rss_bytes = None
            handle = k32.OpenProcess(process_query_limited_information, False, pid)
            if handle:
                try:
                    created, exited, kernel, user = FileTime(), FileTime(), FileTime(), FileTime()
                    if k32.GetProcessTimes(
                        handle,
                        ctypes.byref(created),
                        ctypes.byref(exited),
                        ctypes.byref(kernel),
                        ctypes.byref(user),
                    ):
                        start_order = ft(created)
                        identity = f"windows:{pid}:{start_order}"
                        cpu_sec = (ft(kernel) + ft(user)) / 10_000_000.0
                    counters = ProcessMemoryCountersEx()
                    counters.cb = ctypes.sizeof(counters)
                    if psapi.GetProcessMemoryInfo(
                        handle, ctypes.byref(counters), ctypes.sizeof(counters)
                    ):
                        rss_bytes = max(0, int(counters.WorkingSetSize))
                finally:
                    k32.CloseHandle(handle)
            if rss_bytes is None:
                handle = k32.OpenProcess(
                    process_query_information | process_vm_read, False, pid
                )
                if handle:
                    try:
                        counters = ProcessMemoryCountersEx()
                        counters.cb = ctypes.sizeof(counters)
                        if psapi.GetProcessMemoryInfo(
                            handle, ctypes.byref(counters), ctypes.sizeof(counters)
                        ):
                            rss_bytes = max(0, int(counters.WorkingSetSize))
                    finally:
                        k32.CloseHandle(handle)
            rows[pid] = ProcessRow(
                pid=pid,
                ppid=int(entry.th32ParentProcessID),
                identity=identity,
                start_order=start_order,
                cpu_sec=cpu_sec,
                rss_bytes=rss_bytes,
                threads=max(0, int(entry.cntThreads)),
            )
            entry.dwSize = ctypes.sizeof(entry)
            ok = bool(k32.Process32NextW(snap, ctypes.byref(entry)))
    finally:
        k32.CloseHandle(snap)
    return rows


# Minimal Win32 ownership seam for descendant-stable observation.  These fixed-
# width aliases keep the staged helper importable on non-Windows test hosts.
_WIN_BYTE = ctypes.c_ubyte
_WIN_WORD = ctypes.c_uint16
_WIN_DWORD = ctypes.c_uint32
_WIN_BOOL = ctypes.c_int
_WIN_UINT = ctypes.c_uint
_WIN_HANDLE = ctypes.c_void_p
_WIN_LPVOID = ctypes.c_void_p
_WIN_LPCWSTR = ctypes.c_wchar_p
_WIN_LPWSTR = ctypes.c_wchar_p
_WIN_ULONG_PTR = ctypes.c_size_t
_WIN_LARGE_INTEGER = ctypes.c_int64
_WIN_INVALID_HANDLE = ctypes.c_void_p(-1).value
_WIN_CREATE_SUSPENDED = 0x00000004
_WIN_DETACHED_PROCESS = 0x00000008
_WIN_CREATE_BREAKAWAY_FROM_JOB = 0x01000000
_WIN_STARTF_USESTDHANDLES = 0x00000100
_WIN_HANDLE_FLAG_INHERIT = 0x00000001
_WIN_STD_INPUT_HANDLE = _WIN_DWORD(-10).value
_WIN_STD_OUTPUT_HANDLE = _WIN_DWORD(-11).value
_WIN_STD_ERROR_HANDLE = _WIN_DWORD(-12).value
_WIN_INFINITE = 0xFFFFFFFF
_WIN_WAIT_OBJECT_0 = 0
_WIN_WAIT_TIMEOUT = 258
_WIN_WAIT_FAILED = 0xFFFFFFFF
_WIN_DWORD_MINUS_ONE = 0xFFFFFFFF
_WIN_JOB_OBJECT_QUERY = 0x0004
_WIN_JOB_BASIC_LIMIT_INFORMATION = 2
_WIN_JOB_BASIC_PROCESS_ID_LIST = 3
_WIN_JOB_EXTENDED_LIMIT_INFORMATION = 9
_WIN_JOB_OBJECT_LIMIT_BREAKAWAY_OK = 0x00000800
_WIN_ERROR_ALREADY_EXISTS = 183
_WIN_ERROR_FILE_NOT_FOUND = 2
_WIN_ERROR_MORE_DATA = 234
_WIN_K32 = None


class _WinFileTime(ctypes.Structure):
    _fields_ = [("low", _WIN_DWORD), ("high", _WIN_DWORD)]


class _WinJobBasicLimitInformation(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", _WIN_LARGE_INTEGER),
        ("PerJobUserTimeLimit", _WIN_LARGE_INTEGER),
        ("LimitFlags", _WIN_DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", _WIN_DWORD),
        ("Affinity", _WIN_ULONG_PTR),
        ("PriorityClass", _WIN_DWORD),
        ("SchedulingClass", _WIN_DWORD),
    ]


class _WinIoCounters(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount", ctypes.c_uint64),
        ("WriteOperationCount", ctypes.c_uint64),
        ("OtherOperationCount", ctypes.c_uint64),
        ("ReadTransferCount", ctypes.c_uint64),
        ("WriteTransferCount", ctypes.c_uint64),
        ("OtherTransferCount", ctypes.c_uint64),
    ]


class _WinJobExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _WinJobBasicLimitInformation),
        ("IoInfo", _WinIoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


class _WinStartupInfo(ctypes.Structure):
    _fields_ = [
        ("cb", _WIN_DWORD),
        ("lpReserved", _WIN_LPWSTR),
        ("lpDesktop", _WIN_LPWSTR),
        ("lpTitle", _WIN_LPWSTR),
        ("dwX", _WIN_DWORD),
        ("dwY", _WIN_DWORD),
        ("dwXSize", _WIN_DWORD),
        ("dwYSize", _WIN_DWORD),
        ("dwXCountChars", _WIN_DWORD),
        ("dwYCountChars", _WIN_DWORD),
        ("dwFillAttribute", _WIN_DWORD),
        ("dwFlags", _WIN_DWORD),
        ("wShowWindow", _WIN_WORD),
        ("cbReserved2", _WIN_WORD),
        ("lpReserved2", ctypes.POINTER(_WIN_BYTE)),
        ("hStdInput", _WIN_HANDLE),
        ("hStdOutput", _WIN_HANDLE),
        ("hStdError", _WIN_HANDLE),
    ]


class _WinProcessInformation(ctypes.Structure):
    _fields_ = [
        ("hProcess", _WIN_HANDLE),
        ("hThread", _WIN_HANDLE),
        ("dwProcessId", _WIN_DWORD),
        ("dwThreadId", _WIN_DWORD),
    ]


def _win_kernel32():
    global _WIN_K32
    if _WIN_K32 is None:
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.CreateJobObjectW.argtypes = (_WIN_LPVOID, _WIN_LPCWSTR)
        k32.CreateJobObjectW.restype = _WIN_HANDLE
        k32.OpenJobObjectW.argtypes = (_WIN_DWORD, _WIN_BOOL, _WIN_LPCWSTR)
        k32.OpenJobObjectW.restype = _WIN_HANDLE
        k32.AssignProcessToJobObject.argtypes = (_WIN_HANDLE, _WIN_HANDLE)
        k32.AssignProcessToJobObject.restype = _WIN_BOOL
        k32.SetInformationJobObject.argtypes = (
            _WIN_HANDLE, ctypes.c_int, _WIN_LPVOID, _WIN_DWORD
        )
        k32.SetInformationJobObject.restype = _WIN_BOOL
        k32.QueryInformationJobObject.argtypes = (
            _WIN_HANDLE, ctypes.c_int, _WIN_LPVOID, _WIN_DWORD,
            ctypes.POINTER(_WIN_DWORD),
        )
        k32.QueryInformationJobObject.restype = _WIN_BOOL
        k32.CreateProcessW.argtypes = (
            _WIN_LPCWSTR, _WIN_LPWSTR, _WIN_LPVOID, _WIN_LPVOID, _WIN_BOOL,
            _WIN_DWORD, _WIN_LPVOID, _WIN_LPCWSTR,
            ctypes.POINTER(_WinStartupInfo), ctypes.POINTER(_WinProcessInformation),
        )
        k32.CreateProcessW.restype = _WIN_BOOL
        k32.ResumeThread.argtypes = (_WIN_HANDLE,)
        k32.ResumeThread.restype = _WIN_DWORD
        k32.WaitForSingleObject.argtypes = (_WIN_HANDLE, _WIN_DWORD)
        k32.WaitForSingleObject.restype = _WIN_DWORD
        k32.GetExitCodeProcess.argtypes = (_WIN_HANDLE, ctypes.POINTER(_WIN_DWORD))
        k32.GetExitCodeProcess.restype = _WIN_BOOL
        k32.GetProcessTimes.argtypes = (
            _WIN_HANDLE,
            ctypes.POINTER(_WinFileTime), ctypes.POINTER(_WinFileTime),
            ctypes.POINTER(_WinFileTime), ctypes.POINTER(_WinFileTime),
        )
        k32.GetProcessTimes.restype = _WIN_BOOL
        k32.TerminateProcess.argtypes = (_WIN_HANDLE, _WIN_UINT)
        k32.TerminateProcess.restype = _WIN_BOOL
        k32.CloseHandle.argtypes = (_WIN_HANDLE,)
        k32.CloseHandle.restype = _WIN_BOOL
        k32.GetStdHandle.argtypes = (_WIN_DWORD,)
        k32.GetStdHandle.restype = _WIN_HANDLE
        k32.SetHandleInformation.argtypes = (_WIN_HANDLE, _WIN_DWORD, _WIN_DWORD)
        k32.SetHandleInformation.restype = _WIN_BOOL
        _WIN_K32 = k32
    return _WIN_K32


def _win_error(where: str) -> OSError:
    code = ctypes.get_last_error()
    try:
        detail = ctypes.FormatError(code).strip()
    except Exception:
        detail = f"Win32 error {code}"
    return OSError(code, f"{where} failed: {detail}")


def _win_valid_handle(handle) -> bool:
    value = handle.value if hasattr(handle, "value") else handle
    return bool(value) and value != _WIN_INVALID_HANDLE


def _win_close(handle) -> None:
    if _win_valid_handle(handle):
        try:
            _win_kernel32().CloseHandle(handle)
        except Exception:
            pass


def _win_job_name(token: str) -> str:
    return f"Global\\remrun-job-observer-v1-{token}"


def _win_create_named_job(name: str):
    ctypes.set_last_error(0)
    handle = _win_kernel32().CreateJobObjectW(None, name)
    if not _win_valid_handle(handle):
        raise _win_error("CreateJobObjectW")
    if ctypes.get_last_error() == _WIN_ERROR_ALREADY_EXISTS:
        _win_close(handle)
        raise RegistryError("named observation job already exists")
    limits = _WinJobExtendedLimitInformation()
    limits.BasicLimitInformation.LimitFlags = _WIN_JOB_OBJECT_LIMIT_BREAKAWAY_OK
    if not _win_kernel32().SetInformationJobObject(
        handle,
        _WIN_JOB_EXTENDED_LIMIT_INFORMATION,
        ctypes.byref(limits),
        ctypes.sizeof(limits),
    ):
        error = _win_error("SetInformationJobObject")
        _win_close(handle)
        raise error
    return handle


def _win_open_job(name: str):
    ctypes.set_last_error(0)
    handle = _win_kernel32().OpenJobObjectW(_WIN_JOB_OBJECT_QUERY, False, name)
    if _win_valid_handle(handle):
        return handle
    code = ctypes.get_last_error()
    if code == _WIN_ERROR_FILE_NOT_FOUND:
        return None
    raise _win_error("OpenJobObjectW")


def _win_job_pids(handle) -> set[int]:
    capacity = 64
    while capacity <= 16_384:
        offset = ctypes.sizeof(_WIN_DWORD) * 2
        size = offset + ctypes.sizeof(_WIN_ULONG_PTR) * capacity
        buffer = ctypes.create_string_buffer(size)
        returned = _WIN_DWORD()
        ctypes.set_last_error(0)
        ok = _win_kernel32().QueryInformationJobObject(
            handle,
            _WIN_JOB_BASIC_PROCESS_ID_LIST,
            buffer,
            size,
            ctypes.byref(returned),
        )
        assigned = _WIN_DWORD.from_buffer(buffer, 0).value
        listed = _WIN_DWORD.from_buffer(buffer, ctypes.sizeof(_WIN_DWORD)).value
        if ok and assigned <= capacity and listed <= capacity:
            array_type = _WIN_ULONG_PTR * int(listed)
            values = array_type.from_buffer(buffer, offset)
            return {int(value) for value in values if int(value) > 0}
        if ctypes.get_last_error() != _WIN_ERROR_MORE_DATA and assigned <= capacity:
            raise _win_error("QueryInformationJobObject(ProcessIdList)")
        capacity = max(capacity * 2, int(assigned) + 16)
    raise RegistryError("Windows job process list exceeds observer bound")


def _windows_job_pids_by_name(name: str) -> set[int] | None:
    handle = _win_open_job(name)
    if handle is None:
        return None
    try:
        return _win_job_pids(handle)
    finally:
        _win_close(handle)


def _win_startup_info() -> tuple[_WinStartupInfo, bool]:
    k32 = _win_kernel32()
    info = _WinStartupInfo()
    info.cb = ctypes.sizeof(info)
    info.hStdInput = k32.GetStdHandle(_WIN_STD_INPUT_HANDLE)
    info.hStdOutput = k32.GetStdHandle(_WIN_STD_OUTPUT_HANDLE)
    info.hStdError = k32.GetStdHandle(_WIN_STD_ERROR_HANDLE)
    inherit = True
    for handle in (info.hStdInput, info.hStdOutput, info.hStdError):
        if not _win_valid_handle(handle) or not k32.SetHandleInformation(
            handle, _WIN_HANDLE_FLAG_INHERIT, _WIN_HANDLE_FLAG_INHERIT
        ):
            inherit = False
    if inherit:
        info.dwFlags |= _WIN_STARTF_USESTDHANDLES
    return info, inherit


def _win_create_suspended(command: list[str]) -> _WinProcessInformation:
    executable = shutil.which(command[0])
    application = executable or None
    argv = [executable, *command[1:]] if executable else command
    command_line = ctypes.create_unicode_buffer(subprocess.list2cmdline(argv))
    startup, inherit = _win_startup_info()
    process = _WinProcessInformation()
    if not _win_kernel32().CreateProcessW(
        application,
        command_line,
        None,
        None,
        bool(inherit),
        # Leave the enclosing OpenSSH request Job before assignment to remrun's
        # named Job. Otherwise sshd can tear down surviving descendants when the
        # direct request exits even though the detached keeper holds the inner Job.
        _WIN_CREATE_SUSPENDED | _WIN_CREATE_BREAKAWAY_FROM_JOB,
        None,
        None,
        ctypes.byref(startup),
        ctypes.byref(process),
    ):
        raise _win_error("CreateProcessW")
    return process


def _win_create_keeper_suspended(
    root: Path, token: str, job_name: str
) -> _WinProcessInformation:
    """Create the per-job handle keeper without inheriting SSH/command handles."""
    _bounded_text(token, "token", 64)
    helper = str(Path(__file__).resolve())
    command = [
        sys.executable,
        "-S",
        helper,
        "hold-windows-job",
        "--state-root",
        str(root),
        "--token",
        token,
        "--job-name",
        job_name,
    ]
    command_line = ctypes.create_unicode_buffer(subprocess.list2cmdline(command))
    startup = _WinStartupInfo()
    startup.cb = ctypes.sizeof(startup)
    process = _WinProcessInformation()
    flags = (
        _WIN_CREATE_SUSPENDED
        | _WIN_DETACHED_PROCESS
        | _WIN_CREATE_BREAKAWAY_FROM_JOB
    )
    if not _win_kernel32().CreateProcessW(
        sys.executable,
        command_line,
        None,
        None,
        False,
        flags,
        None,
        None,
        ctypes.byref(startup),
        ctypes.byref(process),
    ):
        raise _win_error("CreateProcessW(handle keeper)")
    return process


def _win_keeper_ready_path(root: Path, token: str) -> Path:
    _bounded_text(token, "token", 64)
    return root / "jobs" / "keeper-v2" / f"{token}.ready"


def _remove_keeper_ready(root: Path, token: str) -> None:
    """Best-effort cleanup that can never change command execution or exit."""
    try:
        _win_keeper_ready_path(root, token).unlink()
    except OSError:
        pass


def _write_keeper_ready(root: Path, token: str, job_name: str) -> None:
    path = _win_keeper_ready_path(root, token)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        {
            "schema": OWNED_REGISTRY_SCHEMA,
            "token": token,
            "job_name": job_name,
            "pid": os.getpid(),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    temp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        with temp.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _win_process_exit_if_done(process: _WinProcessInformation) -> int | None:
    wait = _win_kernel32().WaitForSingleObject(process.hProcess, 0)
    if wait == _WIN_WAIT_TIMEOUT:
        return None
    if wait == _WIN_WAIT_FAILED:
        raise _win_error("WaitForSingleObject(handle keeper)")
    if wait != _WIN_WAIT_OBJECT_0:
        raise OSError(f"WaitForSingleObject(handle keeper) returned {wait}")
    code = _WIN_DWORD()
    if not _win_kernel32().GetExitCodeProcess(process.hProcess, ctypes.byref(code)):
        raise _win_error("GetExitCodeProcess(handle keeper)")
    return int(code.value)


def _wait_for_keeper_ready(
    root: Path,
    token: str,
    job_name: str,
    keeper: _WinProcessInformation,
) -> None:
    path = _win_keeper_ready_path(root, token)
    deadline = time.monotonic() + _WIN_KEEPER_READY_TIMEOUT
    while time.monotonic() < deadline:
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            raw = ""
        if raw:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = None
            if (
                isinstance(payload, dict)
                and payload.get("schema") == OWNED_REGISTRY_SCHEMA
                and payload.get("token") == token
                and payload.get("job_name") == job_name
                and payload.get("pid") == int(keeper.dwProcessId)
            ):
                exit_code = _win_process_exit_if_done(keeper)
                if exit_code is not None:
                    raise RegistryError(
                        f"Windows handle keeper exited before launch (exit {exit_code})"
                    )
                return
        exit_code = _win_process_exit_if_done(keeper)
        if exit_code is not None:
            raise RegistryError(
                f"Windows handle keeper failed before readiness (exit {exit_code})"
            )
        time.sleep(0.02)
    raise RegistryError("Windows handle keeper did not become ready")


def _win_terminate_process(process: _WinProcessInformation) -> None:
    try:
        if _win_process_exit_if_done(process) is None:
            _win_kernel32().TerminateProcess(process.hProcess, 125)
            _win_kernel32().WaitForSingleObject(process.hProcess, 5_000)
    finally:
        _win_close(process.hThread)
        process.hThread = None
        _win_close(process.hProcess)
        process.hProcess = None


def _run_windows_handle_keeper(root: Path, token: str, job_name: str) -> int:
    """Hold one Job Object handle until its ordinary member set is empty."""
    if os.name != "nt":
        return 2
    job = _win_open_job(job_name)
    if job is None:
        return 3
    try:
        _write_keeper_ready(root, token, job_name)
        while True:
            try:
                pids = _win_job_pids(job)
            except Exception:
                # Keeping the handle is safer than converting an inspection failure
                # into false completion. A separate query will likewise be UNKNOWN.
                time.sleep(_WIN_KEEPER_POLL_SECONDS)
                continue
            if pids:
                time.sleep(_WIN_KEEPER_POLL_SECONDS)
                continue
            for _ in range(_WIN_KEEPER_CLEANUP_RETRIES):
                if _unregister(root, token):
                    return 0
                time.sleep(_WIN_KEEPER_POLL_SECONDS)
            return 4
    finally:
        _remove_keeper_ready(root, token)
        _win_close(job)


def _win_process_row(process: _WinProcessInformation) -> ProcessRow:
    created, exited, kernel, user = (
        _WinFileTime(), _WinFileTime(), _WinFileTime(), _WinFileTime()
    )
    if not _win_kernel32().GetProcessTimes(
        process.hProcess,
        ctypes.byref(created), ctypes.byref(exited),
        ctypes.byref(kernel), ctypes.byref(user),
    ):
        raise _win_error("GetProcessTimes")
    start = (int(created.high) << 32) | int(created.low)
    cpu = (
        ((int(kernel.high) << 32) | int(kernel.low))
        + ((int(user.high) << 32) | int(user.low))
    ) / 10_000_000.0
    pid = int(process.dwProcessId)
    return ProcessRow(
        pid=pid,
        ppid=os.getpid(),
        identity=f"windows:{pid}:{start}",
        start_order=start,
        cpu_sec=cpu,
        rss_bytes=None,
        threads=None,
    )


def _win_resume(process: _WinProcessInformation) -> None:
    previous = _win_kernel32().ResumeThread(process.hThread)
    if previous == _WIN_DWORD_MINUS_ONE:
        raise _win_error("ResumeThread")
    _win_close(process.hThread)
    process.hThread = None


def _win_wait_exit(process: _WinProcessInformation) -> int:
    wait = _win_kernel32().WaitForSingleObject(process.hProcess, _WIN_INFINITE)
    if wait == _WIN_WAIT_FAILED:
        raise _win_error("WaitForSingleObject")
    if wait != _WIN_WAIT_OBJECT_0:
        raise OSError(f"WaitForSingleObject returned {wait}")
    code = _WIN_DWORD()
    if not _win_kernel32().GetExitCodeProcess(process.hProcess, ctypes.byref(code)):
        raise _win_error("GetExitCodeProcess")
    return int(code.value)


def _win_discard_suspended(process: _WinProcessInformation) -> None:
    try:
        _win_kernel32().TerminateProcess(process.hProcess, 125)
        _win_kernel32().WaitForSingleObject(process.hProcess, 5_000)
    finally:
        _win_close(process.hThread)
        _win_close(process.hProcess)


def _win_assign_process(job, process: _WinProcessInformation) -> None:
    if not _win_kernel32().AssignProcessToJobObject(job, process.hProcess):
        raise _win_error("AssignProcessToJobObject")



def _platform_name() -> str:
    if sys.platform.startswith("linux") and Path("/proc/self/stat").exists():
        return "linux"
    if sys.platform == "darwin":
        return "macos"
    if os.name == "nt":
        return "windows"
    return "unsupported"

def _processes() -> dict[int, ProcessRow]:
    if sys.platform.startswith("linux") and Path("/proc/self/stat").exists():
        return _linux_processes()
    if sys.platform == "darwin":
        return _darwin_processes()
    if os.name == "nt":
        return _windows_processes()
    raise RuntimeError(f"unsupported target platform: {sys.platform}")


def _identity_state(
    pid: int, expected: str, snapshot: dict[int, ProcessRow] | None = None
) -> str:
    """Return match, mismatch, missing, or unknown without claiming false liveness.

    Callers that already took a process-table snapshot pass it here. This keeps
    query and launch-time reclamation to one shared scan rather than one scan per
    registered job. A missing row is checked conservatively because permission
    filtering can make a live process absent from a platform snapshot.
    """
    if snapshot is None:
        try:
            snapshot = _processes()
        except Exception:
            return "unknown"
    row = snapshot.get(pid)
    if row is not None:
        if row.identity is None:
            return "unknown"
        return "match" if row.identity == expected else "mismatch"
    if os.name == "posix":
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return "missing"
        except PermissionError:
            return "unknown"
        except OSError:
            return "unknown"
        return "unknown"
    # Toolhelp enumerates system processes independently of OpenProcess access.
    # Absence is therefore a truthful missing result on Windows.
    return "missing"


def _cleanup_stale_table_locked(
    conn: sqlite3.Connection, table: str, snapshot: dict[int, ProcessRow]
) -> int:
    if table not in {LEGACY_TABLE, OWNED_TABLE}:
        raise RegistryError("unsupported active-job table")
    rows = conn.execute(
        f"SELECT token,root_pid,root_identity,owner_kind,owner_key,"
        f"witness_pid,witness_identity FROM {table} LIMIT ?",
        (MAX_DB_ROWS_READ,),
    ).fetchall()
    if len(rows) > MAX_ACTIVE_JOBS:
        raise RegistryError(f"{table} exceeds its hard row bound")
    removed = 0
    for token, pid, identity, owner_kind, owner_key, witness_pid, witness_identity in rows:
        kind = str(owner_kind or "legacy_child")
        stale = False
        if kind == "legacy_child":
            stale = _identity_state(int(pid), str(identity), snapshot) in {"missing", "mismatch"}
        elif kind == "posix_pgid":
            try:
                pgid = int(str(owner_key))
            except (TypeError, ValueError):
                pgid = -1
            witness_state = _identity_state(
                int(witness_pid or pid), str(witness_identity or identity), snapshot
            )
            if witness_state == "match":
                stale = False
            elif pgid > 0 and any(row.pgid == pgid for row in snapshot.values()):
                # A surviving group without its exact witness is retained but will
                # be reported UNKNOWN; launch-time cleanup must not turn doubt into
                # a false completion claim.
                stale = False
            else:
                stale = witness_state in {"missing", "mismatch"}
        elif kind in {"windows_job", "windows_job_v2"}:
            if os.name != "nt":
                stale = False
            else:
                try:
                    pids = _windows_job_pids_by_name(str(owner_key))
                except Exception:
                    stale = False
                else:
                    # A missing name is not proof of completion. Windows removes a
                    # temporary object's namespace entry when the last handle closes
                    # even while process references keep the Job alive.
                    stale = pids is not None and not pids
        if stale:
            conn.execute(f"DELETE FROM {table} WHERE token=?", (token,))
            removed += 1
    return removed


def _cleanup_stale_locked(conn: sqlite3.Connection) -> int:
    try:
        snapshot = _processes()
    except Exception:
        # Reclamation is fail-safe: inability to inspect the process table is
        # never evidence that every registered job ended.
        return 0
    return sum(
        _cleanup_stale_table_locked(conn, table, snapshot)
        for table in (LEGACY_TABLE, OWNED_TABLE)
    )


def _register(
    root: Path,
    metadata: dict[str, object],
    process: ProcessRow,
    *,
    token: str | None = None,
    owner_kind: str = "legacy_child",
    owner_key: str | None = None,
    owner_process: ProcessRow | None = None,
    witness: ProcessRow | None = None,
) -> str:
    if process.identity is None:
        raise RegistryError("child process identity is unavailable")
    if owner_kind not in {
        "legacy_child", "posix_pgid", "windows_job", "windows_job_v2"
    }:
        raise RegistryError("unsupported process owner kind")
    if owner_kind != "legacy_child" and not owner_key:
        raise RegistryError("owned jobs require an owner key")
    owner_process = owner_process or process
    witness = witness or process
    if owner_process.identity is None or witness.identity is None:
        raise RegistryError("owner and witness identities must be exact")
    token = token or uuid.uuid4().hex
    _bounded_text(token, "token", 64)
    conn = _writer(root)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _cleanup_stale_locked(conn)
        legacy_count = int(conn.execute(f"SELECT COUNT(*) FROM {LEGACY_TABLE}").fetchone()[0])
        owned_count = int(conn.execute(f"SELECT COUNT(*) FROM {OWNED_TABLE}").fetchone()[0])
        if legacy_count + owned_count >= MAX_ACTIVE_JOBS:
            raise RegistryFull(f"active-job registry is full ({MAX_ACTIVE_JOBS})")
        conn.execute(
            f"INSERT INTO {OWNED_TABLE}("
            "token,schema,job_id,project,source_controller,target,phase,command_label,"
            "command_sha256,member_count,root_pid,root_identity,started_at_ns,"
            "owner_kind,owner_key,owner_pid,owner_identity,witness_pid,witness_identity"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                token,
                OWNED_REGISTRY_SCHEMA,
                metadata["job_id"],
                metadata["project"],
                metadata["source_controller"],
                metadata["target"],
                metadata["phase"],
                metadata["command_label"],
                metadata["command_sha256"],
                metadata["member_count"],
                process.pid,
                process.identity,
                time.time_ns(),
                owner_kind,
                owner_key,
                owner_process.pid,
                owner_process.identity,
                witness.pid,
                witness.identity,
            ),
        )
        conn.execute("COMMIT")
        return token
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


def _update_record_processes(
    root: Path,
    token: str,
    *,
    root_process: ProcessRow | None = None,
    witness: ProcessRow | None = None,
) -> None:
    assignments: list[str] = []
    values: list[object] = []
    if root_process is not None:
        if root_process.identity is None:
            raise RegistryError("root process identity is unavailable")
        assignments.extend(("root_pid=?", "root_identity=?"))
        values.extend((root_process.pid, root_process.identity))
    if witness is not None:
        if witness.identity is None:
            raise RegistryError("witness process identity is unavailable")
        assignments.extend(("witness_pid=?", "witness_identity=?"))
        values.extend((witness.pid, witness.identity))
    if not assignments:
        return
    conn = _writer(root)
    try:
        conn.execute("BEGIN IMMEDIATE")
        values.append(token)
        conn.execute(
            f"UPDATE {OWNED_TABLE} SET {','.join(assignments)} WHERE token=?",
            values,
        )
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


def _unregister(root: Path, token: str) -> bool:
    try:
        conn = _writer(root)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(f"DELETE FROM {OWNED_TABLE} WHERE token=?", (token,))
            conn.execute("COMMIT")
            return True
        finally:
            conn.close()
    except Exception:
        # Completion must never be changed by observability cleanup.
        return False



def _child_row(proc: subprocess.Popen[bytes] | subprocess.Popen[str]) -> ProcessRow | None:
    deadline = time.monotonic() + 0.25
    while True:
        try:
            row = _processes().get(proc.pid)
        except Exception:
            row = None
        if row is not None and row.identity is not None:
            return row
        if proc.poll() is not None or time.monotonic() >= deadline:
            return row
        time.sleep(0.01)


def _read_table_records(
    conn: sqlite3.Connection,
    table: str,
    *,
    expected_schema: int,
    legacy_compatible: bool,
) -> tuple[list[dict[str, object]], list[dict[str, str]]]:
    columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    if not columns:
        return [], []
    base_columns = (
        "token,schema,job_id,project,source_controller,target,phase,"
        "command_label,command_sha256,member_count,root_pid,root_identity,started_at_ns"
    )
    if "owner_kind" in columns:
        selected = (
            base_columns
            + ",owner_kind,owner_key,owner_pid,owner_identity,witness_pid,witness_identity"
        )
    elif legacy_compatible:
        selected = base_columns
    else:
        raise RegistryError(f"{table} is missing ownership columns")
    rows = conn.execute(
        f"SELECT {selected} FROM {table} "
        "ORDER BY project,started_at_ns,token LIMIT ?",
        (MAX_DB_ROWS_READ,),
    ).fetchall()
    if len(rows) > MAX_ACTIVE_JOBS:
        raise RegistryError(f"{table} exceeds its hard row bound")

    records: list[dict[str, object]] = []
    errors: list[dict[str, str]] = []
    for index, row in enumerate(rows):
        try:
            if row["schema"] != expected_schema:
                raise ValueError("record schema mismatch")
            root_pid = int(row["root_pid"])
            root_identity = str(row["root_identity"])
            owner_kind = str(row["owner_kind"]) if "owner_kind" in columns else "legacy_child"
            owner_key = row["owner_key"] if "owner_key" in columns else None
            owner_pid = int(row["owner_pid"] or root_pid) if "owner_pid" in columns else root_pid
            owner_identity = (
                str(row["owner_identity"] or root_identity)
                if "owner_identity" in columns
                else root_identity
            )
            witness_pid = (
                int(row["witness_pid"] or root_pid)
                if "witness_pid" in columns
                else root_pid
            )
            witness_identity = (
                str(row["witness_identity"] or root_identity)
                if "witness_identity" in columns
                else root_identity
            )
            token = _bounded_text(row["token"], "token", 64)
            record = {
                "token": token,
                "_record_key": f"{table}:{token}",
                "registry_table": table,
                "schema": expected_schema,
                "job_id": _bounded_text(row["job_id"], "job_id", 128),
                "project": _bounded_text(row["project"], "project", 128),
                "source_controller": _bounded_text(
                    row["source_controller"], "source_controller", 64
                ),
                "target": _bounded_text(row["target"], "target", 64),
                "phase": _bounded_text(row["phase"], "phase", 32),
                "command_label": _bounded_text(
                    row["command_label"], "command_label", 64
                ),
                "command_sha256": str(row["command_sha256"]),
                "member_count": int(row["member_count"]),
                "root_pid": root_pid,
                "root_identity": root_identity,
                "started_at_ns": int(row["started_at_ns"]),
                "owner_kind": owner_kind,
                "owner_key": str(owner_key) if owner_key is not None else None,
                "owner_pid": owner_pid,
                "owner_identity": owner_identity,
                "witness_pid": witness_pid,
                "witness_identity": witness_identity,
            }
            if not _HEX64.fullmatch(record["command_sha256"]):
                raise ValueError("command digest is invalid")
            if not 1 <= record["member_count"] <= 100_000:
                raise ValueError("member_count is invalid")
            if record["root_pid"] <= 0 or record["started_at_ns"] <= 0:
                raise ValueError("process identity fields are invalid")
            if owner_kind not in {
                "legacy_child", "posix_pgid", "windows_job", "windows_job_v2"
            }:
                raise ValueError("owner kind is invalid")
            if owner_kind != "legacy_child" and not record["owner_key"]:
                raise ValueError("owned record is missing its owner key")
            records.append(record)
        except (TypeError, ValueError) as exc:
            errors.append(
                {"kind": "invalid_record", "detail": f"{table} row {index}: {exc}"}
            )
    return records, errors


def _read_records(root: Path) -> tuple[list[dict[str, object]], list[dict[str, str]]]:
    conn = _readonly(root)
    if conn is None:
        return [], []
    try:
        # sqlite3.connect() creates the file before the first schema statement.
        # A concurrent read in that small initialization window must remain
        # read-only and bounded, but should not misclassify a healthy registry as
        # corrupt merely because its writer has not committed the metadata row yet.
        meta = None
        for attempt in range(_SCHEMA_READY_ATTEMPTS):
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='registry_meta'"
            ).fetchone()
            if table is not None:
                meta = conn.execute(
                    "SELECT value FROM registry_meta WHERE key='schema'"
                ).fetchone()
                if meta is not None:
                    break
            if attempt + 1 < _SCHEMA_READY_ATTEMPTS:
                time.sleep(_SCHEMA_READY_DELAY_SECONDS)
        if meta is None:
            raise RegistryError("active-job registry schema is unavailable")
        if meta[0] != str(REGISTRY_SCHEMA):
            raise RegistryError("unsupported active-job registry schema")
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN (?,?)",
                (LEGACY_TABLE, OWNED_TABLE),
            )
        }
        if not tables:
            raise RegistryError("active-job registry contains no recognized record table")
        records: list[dict[str, object]] = []
        errors: list[dict[str, str]] = []
        if LEGACY_TABLE in tables:
            found, invalid = _read_table_records(
                conn, LEGACY_TABLE, expected_schema=REGISTRY_SCHEMA, legacy_compatible=True
            )
            records.extend(found)
            errors.extend(invalid)
        if OWNED_TABLE in tables:
            found, invalid = _read_table_records(
                conn,
                OWNED_TABLE,
                expected_schema=OWNED_REGISTRY_SCHEMA,
                legacy_compatible=False,
            )
            records.extend(found)
            errors.extend(invalid)
        if len(records) + len(errors) > MAX_MIXED_RECORDS:
            raise RegistryError("mixed active-job registry exceeds its hard row bound")
        records.sort(
            key=lambda row: (
                str(row["project"]),
                int(row["started_at_ns"]),
                str(row["token"]),
                str(row["registry_table"]),
            )
        )
        return records, errors
    finally:
        conn.close()



def _nearest_root(
    row: ProcessRow,
    rows: dict[int, ProcessRow],
    roots: dict[int, tuple[str, str]],
) -> str | None:
    current = row
    seen: set[int] = set()
    while current.pid > 0 and current.pid not in seen:
        seen.add(current.pid)
        root = roots.get(current.pid)
        if root is not None and current.identity == root[0]:
            return root[1]
        parent = rows.get(current.ppid)
        if parent is None:
            return None
        # A parent created after its alleged child is a reused PID, not ancestry.
        if (
            parent.start_order is not None
            and current.start_order is not None
            and parent.start_order > current.start_order
        ):
            return None
        current = parent
    return None


def _assign(
    rows: dict[int, ProcessRow], records: list[dict[str, object]]
) -> dict[str, list[ProcessRow]]:
    roots = {
        int(record["root_pid"]): (
            str(record["root_identity"]),
            str(record.get("_record_key", record["token"])),
        )
        for record in records
    }
    assigned: dict[str, list[ProcessRow]] = {
        str(record.get("_record_key", record["token"])): [] for record in records
    }
    for row in rows.values():
        token = _nearest_root(row, rows, roots)
        if token is not None:
            assigned[token].append(row)
    return assigned


def _identity_map(rows: list[ProcessRow]) -> dict[str, ProcessRow] | None:
    result: dict[str, ProcessRow] = {}
    for row in rows:
        if row.identity is None or row.identity in result:
            return None
        result[row.identity] = row
    return result


def _job_payload(
    record: dict[str, object],
    first: list[ProcessRow],
    second: list[ProcessRow],
    elapsed: float,
    logical_cpus: int,
) -> dict[str, object]:
    errors: list[dict[str, str]] = []
    current_rss: int | None
    threads: int | None
    if any(row.rss_bytes is None for row in second):
        current_rss = None
        errors.append({"kind": "memory_partial", "detail": "one or more processes were inaccessible"})
    else:
        current_rss = sum(int(row.rss_bytes or 0) for row in second)
    if any(row.threads is None for row in second):
        threads = None
        errors.append({"kind": "threads_partial", "detail": "one or more processes were inaccessible"})
    else:
        threads = sum(int(row.threads or 0) for row in second)

    cpu_pct = None
    first_map = _identity_map(first)
    second_map = _identity_map(second)
    if first_map is None or second_map is None or set(first_map) != set(second_map):
        errors.append(
            {
                "kind": "cpu_churn",
                "detail": "the attributed process set changed during the sampling interval",
            }
        )
    elif any(
        first_map[key].cpu_sec is None or second_map[key].cpu_sec is None
        for key in first_map
    ):
        errors.append({"kind": "cpu_partial", "detail": "one or more CPU counters were inaccessible"})
    elif elapsed <= 0:
        errors.append({"kind": "cpu_interval", "detail": "sampling interval was not positive"})
    else:
        deltas = [
            float(second_map[key].cpu_sec or 0.0) - float(first_map[key].cpu_sec or 0.0)
            for key in first_map
        ]
        if any(delta < -1e-9 for delta in deltas):
            errors.append({"kind": "cpu_counter", "detail": "a CPU counter moved backwards"})
        else:
            cpu_pct = max(0.0, sum(max(0.0, delta) for delta in deltas) / elapsed * 100.0)

    now_ns = time.time_ns()
    observation_status = "ok" if not errors else "partial"
    return {
        "schema": QUERY_SCHEMA,
        "job_id": record["job_id"],
        "project": record["project"],
        "source_controller": record["source_controller"],
        "target": record["target"],
        "phase": record["phase"],
        "state": "RUNNING",
        "observation_status": observation_status,
        "age_seconds": max(0.0, (now_ns - int(record["started_at_ns"])) / 1e9),
        "started_at_unix_ns": record["started_at_ns"],
        "root_process": {
            "pid": record["root_pid"],
            "identity": record["root_identity"],
        },
        "member_count": record["member_count"],
        "command": {
            "label": record["command_label"],
            "sha256": record["command_sha256"],
        },
        "processes": {"current_count": len(second)},
        "cpu": {
            "current_pct_one_logical_cpu": cpu_pct,
            "normalized_host_pct": (cpu_pct / logical_cpus) if cpu_pct is not None else None,
            "logical_cpu_count": logical_cpus,
            "sample_elapsed_seconds": elapsed,
            "status": "ok" if cpu_pct is not None else "partial",
        },
        "threads": {
            "current_os_threads": threads,
            "status": "ok" if threads is not None else "partial",
        },
        "memory": {
            "current_bytes": current_rss,
            "current_kind": (
                "summed_working_set" if os.name == "nt" else "summed_rss"
            ),
            "peak_bytes": None,
            "peak_status": "unavailable",
            "peak_detail": "no cheap truthful cross-process aggregate peak is available",
            "status": "ok" if current_rss is not None else "partial",
        },
        "errors": errors,
    }


def _unknown_job(record: dict[str, object], detail: str) -> dict[str, object]:
    return {
        "schema": QUERY_SCHEMA,
        "job_id": record["job_id"],
        "project": record["project"],
        "source_controller": record["source_controller"],
        "target": record["target"],
        "phase": record["phase"],
        "state": "UNKNOWN",
        "observation_status": "unknown",
        "age_seconds": max(0.0, (time.time_ns() - int(record["started_at_ns"])) / 1e9),
        "started_at_unix_ns": record["started_at_ns"],
        "root_process": {
            "pid": record["root_pid"],
            "identity": record["root_identity"],
        },
        "member_count": record["member_count"],
        "command": {
            "label": record["command_label"],
            "sha256": record["command_sha256"],
        },
        "processes": {"current_count": None},
        "cpu": {
            "current_pct_one_logical_cpu": None,
            "normalized_host_pct": None,
            "logical_cpu_count": max(1, int(os.cpu_count() or 1)),
            "sample_elapsed_seconds": None,
            "status": "unknown",
        },
        "threads": {"current_os_threads": None, "status": "unknown"},
        "memory": {
            "current_bytes": None,
            "current_kind": "unknown",
            "peak_bytes": None,
            "peak_status": "unavailable",
            "peak_detail": "process ownership could not be verified",
            "status": "unknown",
        },
        "errors": [{"kind": "ownership_unknown", "detail": detail}],
    }


def _posix_owned_sample(
    record: dict[str, object], snapshot: dict[int, ProcessRow]
) -> tuple[str, list[ProcessRow], str]:
    try:
        pgid = int(str(record["owner_key"]))
    except (TypeError, ValueError):
        return "unknown", [], "the registered POSIX process-group key is invalid"
    owner_pid = int(record["owner_pid"])
    owner_identity = str(record["owner_identity"])
    members = [
        row
        for row in snapshot.values()
        if row.pgid == pgid
        and not (row.pid == owner_pid and row.identity == owner_identity)
    ]
    witness_pid = int(record["witness_pid"])
    witness_identity = str(record["witness_identity"])
    witness = snapshot.get(witness_pid)
    witness_state = _identity_state(witness_pid, witness_identity, snapshot)
    if witness_state == "match":
        if witness is not None and witness.pid == owner_pid and witness.identity == owner_identity:
            if members:
                return "live", members, ""
            return "unknown", [], "the command is registered but its child identity is not visible yet"
        if witness is not None and witness.pgid == pgid and members:
            return "live", members, ""
        if witness is not None and witness.pgid != pgid:
            return (
                "unknown",
                [],
                "the exact witness deliberately or unexpectedly left the registered process group",
            )
        return "stale", [], "the registered process group has no remaining user process"
    if witness_state == "unknown":
        return "unknown", [], "the exact POSIX ownership witness is inaccessible"
    if members:
        # The kernel group still exists, but a stale registry row could later see
        # the same numeric PGID after an empty interval. Without an exact live
        # witness, report UNKNOWN rather than a PID-reuse false RUNNING claim.
        return (
            "unknown",
            [],
            "the process group has members but its exact generation witness has ended",
        )
    return "stale", [], "the registered POSIX ownership group ended"


def _windows_owned_sample(
    record: dict[str, object], snapshot: dict[int, ProcessRow]
) -> tuple[str, list[ProcessRow], str]:
    if str(record.get("owner_kind")) == "windows_job_v2":
        keeper_state = _identity_state(
            int(record["owner_pid"]), str(record["owner_identity"]), snapshot
        )
        if keeper_state != "match":
            return (
                "unknown",
                [],
                "the exact Windows handle-keeper generation is unavailable; "
                "named-object ownership is unproved",
            )
    try:
        pids = _windows_job_pids_by_name(str(record["owner_key"]))
    except Exception as exc:
        return (
            "unknown",
            [],
            f"the Windows Job Object could not be queried: {type(exc).__name__}: {exc}",
        )
    if pids is None:
        return (
            "unknown",
            [],
            "the registered Windows Job Object name is unavailable; completion is unproved",
        )
    if not pids:
        return "stale", [], "the registered Windows Job Object has no live processes"
    rows: list[ProcessRow] = []
    for pid in sorted(pids):
        row = snapshot.get(pid)
        if row is None:
            rows.append(ProcessRow(pid, 0, None, None, None, None, None))
        else:
            rows.append(row)
    return "live", rows, ""


def _owned_sample(
    record: dict[str, object], snapshot: dict[int, ProcessRow]
) -> tuple[str, list[ProcessRow], str]:
    kind = str(record["owner_kind"])
    if kind == "posix_pgid":
        if os.name != "posix":
            return "unknown", [], "a POSIX ownership record is not queryable on this platform"
        return _posix_owned_sample(record, snapshot)
    if kind in {"windows_job", "windows_job_v2"}:
        if os.name != "nt":
            return "unknown", [], "a Windows ownership record is not queryable on this platform"
        return _windows_owned_sample(record, snapshot)
    return "unknown", [], "the record does not carry a supported owner kind"


def _query(root: Path, sample_interval: float) -> dict[str, object]:
    started = time.monotonic()
    records, record_errors = _read_records(root)
    logical = max(1, int(os.cpu_count() or 1))
    legacy_seen = sum(record.get("registry_table") == LEGACY_TABLE for record in records)
    owned_seen = sum(record.get("registry_table") == OWNED_TABLE for record in records)
    result: dict[str, object] = {
        "schema": QUERY_SCHEMA,
        "status": "ok",
        "platform": _platform_name(),
        "registry": {
            "schema": REGISTRY_SCHEMA,
            "path_kind": "target_state_root_relative",
            "max_active_jobs": MAX_ACTIVE_JOBS,
            "mixed_physical_row_bound": MAX_MIXED_RECORDS,
            "legacy_records_seen": legacy_seen,
            "owned_v2_records_seen": owned_seen,
            "records_seen": len(records) + len(record_errors),
            "invalid_records": len(record_errors),
            "stale_hidden": 0,
            "query_mutated_registry": False,
        },
        "coverage": {
            "scope": "registered_jobs_only",
            "mixed_version": True,
            "detail": (
                "enabled v2 helpers use POSIX process groups or a Windows Job Object "
                "with one detached per-active-job handle keeper; legacy/third-party "
                "launches may be direct-child-only, UNKNOWN, or invisible"
            ),
        },
        "semantics": {
            "cpu": "100 percent equals one logical CPU; normalized_host_pct divides by logical CPU count",
            "threads": "live OS threads summed across the kernel-owned member set",
            "memory": (
                "current summed RSS on POSIX or working set on Windows; shared pages may be double-counted"
            ),
            "attribution": (
                "POSIX process-group membership, Windows named Job Object membership retained "
                "by a per-active-job handle keeper, or legacy nearest-ancestor identity"
            ),
            "escape_boundary": (
                "POSIX descendants that deliberately change process group/session and Windows descendants "
                "created with explicit breakaway leave observation coverage"
            ),
            "peak_memory": "unavailable unless a cheap truthful aggregate exists",
        },
        "sample_interval_requested_seconds": sample_interval,
        "snapshot_elapsed_seconds": 0.0,
        "jobs": [],
        "errors": list(record_errors),
    }
    if record_errors:
        result["status"] = "partial"
    if result["platform"] == "unsupported":
        result["status"] = "unsupported"
        result["errors"].append({
            "kind": "unsupported_platform",
            "detail": f"no bounded process observer is implemented for {sys.platform}",
        })
        result["query_elapsed_seconds"] = time.monotonic() - started
        return result
    if not records:
        result["query_elapsed_seconds"] = time.monotonic() - started
        return result

    try:
        first_snapshot = _processes()
    except Exception as exc:
        result["status"] = "unknown"
        result["errors"].append({"kind": "snapshot_failed", "detail": f"{type(exc).__name__}: {exc}"})
        result["jobs"] = [
            _unknown_job(record, "the process table could not be read") for record in records
        ]
        result["query_elapsed_seconds"] = time.monotonic() - started
        return result

    legacy_first: list[dict[str, object]] = []
    owned_first: dict[str, tuple[dict[str, object], list[ProcessRow]]] = {}
    unknown: dict[str, tuple[dict[str, object], str]] = {}
    for record in records:
        token = str(record.get("_record_key", record["token"]))
        if record["owner_kind"] == "legacy_child":
            row = first_snapshot.get(int(record["root_pid"]))
            if row is not None and row.identity == record["root_identity"]:
                legacy_first.append(record)
                continue
            state = _identity_state(
                int(record["root_pid"]), str(record["root_identity"]), first_snapshot
            )
            if state in {"missing", "mismatch"}:
                result["registry"]["stale_hidden"] += 1
            else:
                unknown[token] = (record, "the legacy root identity was inaccessible")
            continue
        state, rows, detail = _owned_sample(record, first_snapshot)
        if state == "live":
            owned_first[token] = (record, rows)
        elif state == "stale":
            result["registry"]["stale_hidden"] += 1
        else:
            unknown[token] = (record, detail)

    if not legacy_first and not owned_first:
        result["jobs"] = [_unknown_job(record, detail) for record, detail in unknown.values()]
        if unknown:
            result["status"] = "partial"
        result["query_elapsed_seconds"] = time.monotonic() - started
        return result

    delay = min(2.0, max(0.05, sample_interval))
    sample_started = time.monotonic()
    time.sleep(delay)
    try:
        second_snapshot = _processes()
    except Exception as exc:
        result["status"] = "unknown"
        result["errors"].append({"kind": "snapshot_failed", "detail": f"{type(exc).__name__}: {exc}"})
        active_records = legacy_first + [item[0] for item in owned_first.values()]
        result["jobs"] = [
            _unknown_job(record, "the second process-table snapshot failed")
            for record in active_records
        ] + [_unknown_job(record, detail) for record, detail in unknown.values()]
        result["query_elapsed_seconds"] = time.monotonic() - started
        return result
    elapsed = max(0.0, time.monotonic() - sample_started)
    result["snapshot_elapsed_seconds"] = elapsed

    legacy_live: list[dict[str, object]] = []
    for record in legacy_first:
        token = str(record.get("_record_key", record["token"]))
        row = second_snapshot.get(int(record["root_pid"]))
        if row is not None and row.identity == record["root_identity"]:
            legacy_live.append(record)
            continue
        state = _identity_state(
            int(record["root_pid"]), str(record["root_identity"]), second_snapshot
        )
        if state in {"missing", "mismatch"}:
            result["registry"]["stale_hidden"] += 1
        else:
            unknown[token] = (record, "the legacy root identity became inaccessible")

    owned_live: dict[str, tuple[dict[str, object], list[ProcessRow], list[ProcessRow]]] = {}
    for token, (record, first_rows) in owned_first.items():
        state, second_rows, detail = _owned_sample(record, second_snapshot)
        if state == "live":
            owned_live[token] = (record, first_rows, second_rows)
        elif state == "stale":
            result["registry"]["stale_hidden"] += 1
        else:
            unknown[token] = (record, detail)

    first_assigned = _assign(first_snapshot, legacy_live)
    second_assigned = _assign(second_snapshot, legacy_live)
    jobs = [
        _job_payload(
            record,
            first_assigned.get(str(record.get("_record_key", record["token"])), []),
            second_assigned.get(str(record.get("_record_key", record["token"])), []),
            elapsed,
            logical,
        )
        for record in legacy_live
    ]
    jobs.extend(
        _job_payload(record, first_rows, second_rows, elapsed, logical)
        for record, first_rows, second_rows in owned_live.values()
    )
    jobs.extend(_unknown_job(record, detail) for record, detail in unknown.values())
    jobs.sort(key=lambda job: (str(job["project"]), int(job["started_at_unix_ns"]), str(job["job_id"])))
    result["jobs"] = jobs
    if unknown or any(job["observation_status"] != "ok" for job in jobs):
        result["status"] = "partial"
    result["query_elapsed_seconds"] = time.monotonic() - started
    return result


def _write_ready_file(
    root: Path, ready_file: str | None, metadata: dict[str, object], owner_kind: str
) -> None:
    """Durably acknowledge observation before durable user code may start."""
    if ready_file is None:
        return
    path = Path(ready_file)
    try:
        resolved_parent = path.parent.resolve()
        resolved_root = root.resolve()
        resolved_parent.relative_to(resolved_root)
    except (OSError, ValueError) as exc:
        raise RegistryError("observer ready file is outside the target state root") from exc
    payload = {
        "schema": 1,
        "job_id": metadata["job_id"],
        "command_sha256": metadata["command_sha256"],
        "owner_kind": owner_kind,
        "written_at_ns": time.time_ns(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + f".tmp-{os.getpid()}-{time.time_ns()}")
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    fd = os.open(str(temp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb", closefd=False) as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(fd)
    os.replace(temp, path)
    try:
        directory = os.open(str(path.parent), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(directory)
    except OSError:
        pass
    finally:
        os.close(directory)


def _warning(detail: str) -> None:
    print(
        "remrun observation unavailable; command continues unobserved: " + detail,
        file=sys.stderr,
        flush=True,
    )


def _wait_popen(proc: subprocess.Popen[bytes] | subprocess.Popen[str]) -> int:
    code = proc.wait()
    return (128 - code) if code < 0 else code


def _posix_owner_row() -> ProcessRow:
    pid = os.getpid()
    if os.getpgrp() != pid:
        os.setpgid(0, 0)
    pgid = os.getpgrp()
    if pgid != pid:
        raise RegistryError("observer could not establish a private POSIX process group")
    row = _processes().get(pid)
    if row is None or row.identity is None:
        raise RegistryError("observer process identity is unavailable")
    if row.pgid != pgid:
        row = ProcessRow(
            row.pid, row.ppid, row.identity, row.start_order,
            row.cpu_sec, row.rss_bytes, row.threads, pgid,
        )
    return row


def _posix_group_members(
    snapshot: dict[int, ProcessRow], pgid: int, owner: ProcessRow
) -> list[ProcessRow]:
    return [
        row
        for row in snapshot.values()
        if row.pgid == pgid
        and not (row.pid == owner.pid and row.identity == owner.identity)
    ]


def _best_witness(rows: list[ProcessRow], preferred: ProcessRow | None = None) -> ProcessRow | None:
    if preferred is not None:
        for row in rows:
            if row.pid == preferred.pid and row.identity == preferred.identity:
                return row
    exact = [row for row in rows if row.identity is not None]
    if not exact:
        return None
    return min(
        exact,
        key=lambda row: (
            row.start_order is None,
            row.start_order if row.start_order is not None else 0,
            row.pid,
        ),
    )


def _run_posix_command(
    root: Path, metadata: dict[str, object], command: list[str], ready_file: str | None = None
) -> int:
    try:
        owner = _posix_owner_row()
        token = _register(
            root,
            metadata,
            owner,
            owner_kind="posix_pgid",
            owner_key=str(owner.pid),
            owner_process=owner,
            witness=owner,
        )
    except Exception as exc:
        if ready_file is not None:
            raise
        _warning(f"{type(exc).__name__}: {exc}")
        return _wait_popen(subprocess.Popen(command))

    try:
        _write_ready_file(root, ready_file, metadata, "posix_pgid")
        proc = subprocess.Popen(command)
    except BaseException:
        _unregister(root, token)
        raise
    child = _child_row(proc)
    if child is not None and child.identity is not None:
        try:
            _update_record_processes(root, token, root_process=child, witness=child)
        except Exception as exc:
            _warning(f"registry identity update failed: {type(exc).__name__}: {exc}")
    else:
        _warning("child process identity was not readable")
    try:
        return_code = proc.wait()
    except BaseException:
        # The pre-registered owner row survives wrapper/source-controller loss.
        # Query remains exact while its witness survives, otherwise UNKNOWN.
        raise
    try:
        snapshot = _processes()
        survivors = _posix_group_members(snapshot, owner.pid, owner)
    except Exception:
        survivors = []
        # Fail safe: keep the row if completion-side ownership cannot be read.
        return (128 - return_code) if return_code < 0 else return_code
    witness = _best_witness(survivors, child)
    if witness is None:
        _unregister(root, token)
    else:
        try:
            _update_record_processes(root, token, witness=witness)
        except Exception as exc:
            _warning(f"survivor witness update failed: {type(exc).__name__}: {exc}")
    return (128 - return_code) if return_code < 0 else return_code


def _run_windows_command(
    root: Path, metadata: dict[str, object], command: list[str], ready_file: str | None = None
) -> int:
    token = uuid.uuid4().hex
    job_name = _win_job_name(token)
    job = None
    process: _WinProcessInformation | None = None
    keeper: _WinProcessInformation | None = None
    registered = False
    started = False
    keeper_started = False
    try:
        job = _win_create_named_job(job_name)
        process = _win_create_suspended(command)
        _win_assign_process(job, process)
        row = _win_process_row(process)

        # The keeper is a lifecycle witness, not a sampler or service. It is
        # detached from the SSH/controller lifetime, inherits no command handles,
        # opens this already-created named Job, and acknowledges that handle before
        # the registry row or user code can become visible.
        keeper = _win_create_keeper_suspended(root, token, job_name)
        keeper_row = _win_process_row(keeper)
        _win_resume(keeper)
        keeper_started = True
        _wait_for_keeper_ready(root, token, job_name, keeper)

        _register(
            root,
            metadata,
            row,
            token=token,
            owner_kind="windows_job_v2",
            owner_key=job_name,
            owner_process=keeper_row,
            witness=keeper_row,
        )
        registered = True
        _write_ready_file(root, ready_file, metadata, "windows_job_v2")
        _win_resume(process)
        started = True
        _remove_keeper_ready(root, token)
    except Exception as exc:
        if registered:
            _unregister(root, token)
        if process is not None and not started:
            _win_discard_suspended(process)
            process = None
        if keeper is not None:
            if keeper_started:
                _win_terminate_process(keeper)
            else:
                _win_discard_suspended(keeper)
            keeper = None
        _win_close(job)
        _remove_keeper_ready(root, token)
        if ready_file is not None:
            raise
        _warning(f"{type(exc).__name__}: {exc}")
        return _wait_popen(subprocess.Popen(command))

    assert process is not None
    try:
        return_code = _win_wait_exit(process)
    except BaseException:
        # The detached keeper retains the named Job handle if this helper or the
        # source SSH/controller disappears. Query remains exact while that kernel
        # ownership set is openable; keeper failure leaves the row UNKNOWN.
        raise
    else:
        try:
            pids = _win_job_pids(job)
        except Exception:
            pids = {1}  # fail-safe retention; query will report UNKNOWN if needed
        if not pids:
            _unregister(root, token)
        return return_code
    finally:
        _remove_keeper_ready(root, token)
        _win_close(process.hThread)
        _win_close(process.hProcess)
        if keeper is not None:
            _win_close(keeper.hThread)
            _win_close(keeper.hProcess)
        _win_close(job)



def _run_command(
    root: Path, metadata: dict[str, object], command: list[str], ready_file: str | None = None
) -> int:
    if not command:
        raise ValueError("run requires an argv after --")
    if os.name == "nt":
        return _run_windows_command(root, metadata, command, ready_file)
    if os.name == "posix":
        return _run_posix_command(root, metadata, command, ready_file)
    if ready_file is not None:
        raise RegistryError("durable observation is unsupported on this platform")
    proc = subprocess.Popen(command)
    row = _child_row(proc)
    token = None
    if row is not None and row.identity is not None:
        try:
            token = _register(root, metadata, row)
        except Exception as exc:
            _warning(f"{type(exc).__name__}: {exc}")
    try:
        return_code = proc.wait()
    except BaseException:
        if proc.poll() is not None and token is not None:
            _unregister(root, token)
        raise
    if token is not None:
        _unregister(root, token)
    return (128 - return_code) if return_code < 0 else return_code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="remrun-job-observer")
    sub = parser.add_subparsers(dest="operation", required=True)
    run = sub.add_parser("run")
    run.add_argument("--state-root", required=True)
    run.add_argument("--metadata-b64", required=True)
    run.add_argument("--ready-file")
    run.add_argument("command", nargs=argparse.REMAINDER)
    query_parser = sub.add_parser("query")
    query_parser.add_argument("--state-root", required=True)
    query_parser.add_argument("--sample-interval", type=float, default=0.2)
    keeper = sub.add_parser("hold-windows-job")
    keeper.add_argument("--state-root", required=True)
    keeper.add_argument("--token", required=True)
    keeper.add_argument("--job-name", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = _state_root(args.state_root)
    if args.operation == "run":
        command = list(args.command)
        if command and command[0] == "--":
            command = command[1:]
        metadata = _decode_metadata(args.metadata_b64)
        return _run_command(root, metadata, command, args.ready_file)
    if args.operation == "hold-windows-job":
        token = _bounded_text(args.token, "token", 64)
        expected = _win_job_name(token)
        if args.job_name != expected:
            raise ValueError("Windows keeper job name does not match its token")
        return _run_windows_handle_keeper(root, token, expected)
    try:
        payload = _query(root, args.sample_interval)
    except Exception as exc:
        payload = {
            "schema": QUERY_SCHEMA,
            "status": "unknown",
            "platform": sys.platform,
            "registry": {
                "schema": REGISTRY_SCHEMA,
                "max_active_jobs": MAX_ACTIVE_JOBS,
                "query_mutated_registry": False,
            },
            "coverage": {
                "scope": "registered_jobs_only",
                "mixed_version": True,
                "detail": "query failed before registry coverage could be established",
            },
            "jobs": [],
            "errors": [{"kind": "query_failed", "detail": f"{type(exc).__name__}: {exc}"}],
        }
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0 if payload.get("status") in {"ok", "partial"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
