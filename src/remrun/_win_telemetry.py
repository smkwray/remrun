# Windows process-tree telemetry wrapper for remrun.
# Usage: python _win_telemetry.py -- <argv...>
#
# Mirrors the POSIX getrusage sampler: runs the command, then reads kernel
# Job Object accounting for the whole process tree and emits the same
# "\n__REMRUN_TELEMETRY__ <json>\n" stderr sentinel remrun already parses.
from __future__ import annotations

import ctypes
import csv
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time

TELEMETRY_MARKER = "\n__REMRUN_TELEMETRY__ "
TICKS_PER_SECOND = 10_000_000.0  # FILETIME / LARGE_INTEGER job times are 100 ns ticks.

# Fixed-width Win32 aliases. Do not use ctypes.wintypes here: on non-Windows
# hosts its aliases track the build platform, while this script is Windows-only.
BYTE = ctypes.c_ubyte
WORD = ctypes.c_uint16
DWORD = ctypes.c_uint32
BOOL = ctypes.c_int
UINT = ctypes.c_uint
HANDLE = ctypes.c_void_p
LPVOID = ctypes.c_void_p
LPCWSTR = ctypes.c_wchar_p
LPWSTR = ctypes.c_wchar_p
LPBYTE = ctypes.POINTER(BYTE)
SIZE_T = ctypes.c_size_t
ULONG_PTR = ctypes.c_size_t
ULONGLONG = ctypes.c_uint64
LARGE_INTEGER = ctypes.c_int64

INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
CREATE_SUSPENDED = 0x00000004
CREATE_NO_WINDOW = 0x08000000
STARTF_USESTDHANDLES = 0x00000100
STD_INPUT_HANDLE = DWORD(-10).value
STD_OUTPUT_HANDLE = DWORD(-11).value
STD_ERROR_HANDLE = DWORD(-12).value
HANDLE_FLAG_INHERIT = 0x00000001
INFINITE = 0xFFFFFFFF
WAIT_OBJECT_0 = 0x00000000
WAIT_TIMEOUT = 0x00000102
WAIT_FAILED = 0xFFFFFFFF
DWORD_MINUS_ONE = 0xFFFFFFFF
ERROR_WAIT_TIMEOUT = 258
DETAILED_SAMPLE_MS = 200
GUARD_HELPER_EXIT = 125
JOB_OBJECT_LIMIT_JOB_MEMORY = 0x00000200
JOB_OBJECT_LIMIT_BREAKAWAY_OK = 0x00000800
JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
JOB_OBJECT_MSG_PROCESS_MEMORY_LIMIT = 9
JOB_OBJECT_MSG_JOB_MEMORY_LIMIT = 10
JOB_OBJECT_MSG_NOTIFICATION_LIMIT = 11

# Optional telemetry must not redefine command completion. Give descendants a
# short grace period to finish and contribute their final accounting, then
# return the direct command's exit code even if a daemon/helper remains alive.
JOB_DRAIN_GRACE_S = 1.0


class _CommandNotStarted(RuntimeError):
    """The detailed wrapper failed while the user process was still suspended."""

JobObjectBasicAccountingInformation = 1
JobObjectAssociateCompletionPortInformation = 7
JobObjectExtendedLimitInformation = 9


class IO_COUNTERS(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount", ULONGLONG),
        ("WriteOperationCount", ULONGLONG),
        ("OtherOperationCount", ULONGLONG),
        ("ReadTransferCount", ULONGLONG),
        ("WriteTransferCount", ULONGLONG),
        ("OtherTransferCount", ULONGLONG),
    ]


class JOBOBJECT_BASIC_ACCOUNTING_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("TotalUserTime", LARGE_INTEGER),
        ("TotalKernelTime", LARGE_INTEGER),
        ("ThisPeriodTotalUserTime", LARGE_INTEGER),
        ("ThisPeriodTotalKernelTime", LARGE_INTEGER),
        ("TotalPageFaultCount", DWORD),
        ("TotalProcesses", DWORD),
        ("ActiveProcesses", DWORD),
        ("TotalTerminatedProcesses", DWORD),
    ]


class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", LARGE_INTEGER),
        ("PerJobUserTimeLimit", LARGE_INTEGER),
        ("LimitFlags", DWORD),
        ("MinimumWorkingSetSize", SIZE_T),
        ("MaximumWorkingSetSize", SIZE_T),
        ("ActiveProcessLimit", DWORD),
        ("Affinity", ULONG_PTR),
        ("PriorityClass", DWORD),
        ("SchedulingClass", DWORD),
    ]


class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
        ("IoInfo", IO_COUNTERS),
        ("ProcessMemoryLimit", SIZE_T),
        ("JobMemoryLimit", SIZE_T),
        ("PeakProcessMemoryUsed", SIZE_T),
        ("PeakJobMemoryUsed", SIZE_T),
    ]


class JOBOBJECT_ASSOCIATE_COMPLETION_PORT(ctypes.Structure):
    _fields_ = [
        ("CompletionKey", LPVOID),
        ("CompletionPort", HANDLE),
    ]


class STARTUPINFOW(ctypes.Structure):
    _fields_ = [
        ("cb", DWORD),
        ("lpReserved", LPWSTR),
        ("lpDesktop", LPWSTR),
        ("lpTitle", LPWSTR),
        ("dwX", DWORD),
        ("dwY", DWORD),
        ("dwXSize", DWORD),
        ("dwYSize", DWORD),
        ("dwXCountChars", DWORD),
        ("dwYCountChars", DWORD),
        ("dwFillAttribute", DWORD),
        ("dwFlags", DWORD),
        ("wShowWindow", WORD),
        ("cbReserved2", WORD),
        ("lpReserved2", LPBYTE),
        ("hStdInput", HANDLE),
        ("hStdOutput", HANDLE),
        ("hStdError", HANDLE),
    ]


class PROCESS_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("hProcess", HANDLE),
        ("hThread", HANDLE),
        ("dwProcessId", DWORD),
        ("dwThreadId", DWORD),
    ]


class FILETIME(ctypes.Structure):
    _fields_ = [("low", DWORD), ("high", DWORD)]


class MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [
        ("length", DWORD),
        ("memory_load", DWORD),
        ("total_phys", ULONGLONG),
        ("avail_phys", ULONGLONG),
        ("total_page", ULONGLONG),
        ("avail_page", ULONGLONG),
        ("total_virtual", ULONGLONG),
        ("avail_virtual", ULONGLONG),
        ("avail_extended_virtual", ULONGLONG),
    ]


_K32 = None


def _kernel32():
    global _K32
    if _K32 is None:
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.CreateJobObjectW.argtypes = (LPVOID, LPCWSTR)
        k32.CreateJobObjectW.restype = HANDLE

        k32.AssignProcessToJobObject.argtypes = (HANDLE, HANDLE)
        k32.AssignProcessToJobObject.restype = BOOL

        k32.QueryInformationJobObject.argtypes = (
            HANDLE,
            ctypes.c_int,
            LPVOID,
            DWORD,
            ctypes.POINTER(DWORD),
        )
        k32.QueryInformationJobObject.restype = BOOL

        k32.SetInformationJobObject.argtypes = (
            HANDLE,
            ctypes.c_int,
            LPVOID,
            DWORD,
        )
        k32.SetInformationJobObject.restype = BOOL

        k32.TerminateJobObject.argtypes = (HANDLE, UINT)
        k32.TerminateJobObject.restype = BOOL

        k32.CreateIoCompletionPort.argtypes = (HANDLE, HANDLE, ULONG_PTR, DWORD)
        k32.CreateIoCompletionPort.restype = HANDLE

        k32.GetQueuedCompletionStatus.argtypes = (
            HANDLE,
            ctypes.POINTER(DWORD),
            ctypes.POINTER(ULONG_PTR),
            ctypes.POINTER(LPVOID),
            DWORD,
        )
        k32.GetQueuedCompletionStatus.restype = BOOL

        k32.CreateProcessW.argtypes = (
            LPCWSTR,                         # lpApplicationName
            LPWSTR,                          # lpCommandLine; mutable buffer
            LPVOID,                          # lpProcessAttributes
            LPVOID,                          # lpThreadAttributes
            BOOL,                            # bInheritHandles
            DWORD,                           # dwCreationFlags
            LPVOID,                          # lpEnvironment
            LPCWSTR,                         # lpCurrentDirectory
            ctypes.POINTER(STARTUPINFOW),    # lpStartupInfo
            ctypes.POINTER(PROCESS_INFORMATION),
        )
        k32.CreateProcessW.restype = BOOL

        k32.ResumeThread.argtypes = (HANDLE,)
        k32.ResumeThread.restype = DWORD

        k32.WaitForSingleObject.argtypes = (HANDLE, DWORD)
        k32.WaitForSingleObject.restype = DWORD

        k32.GetExitCodeProcess.argtypes = (HANDLE, ctypes.POINTER(DWORD))
        k32.GetExitCodeProcess.restype = BOOL

        k32.GetSystemTimes.argtypes = (
            ctypes.POINTER(FILETIME),
            ctypes.POINTER(FILETIME),
            ctypes.POINTER(FILETIME),
        )
        k32.GetSystemTimes.restype = BOOL

        k32.GlobalMemoryStatusEx.argtypes = (ctypes.POINTER(MEMORYSTATUSEX),)
        k32.GlobalMemoryStatusEx.restype = BOOL

        k32.TerminateProcess.argtypes = (HANDLE, UINT)
        k32.TerminateProcess.restype = BOOL

        k32.CloseHandle.argtypes = (HANDLE,)
        k32.CloseHandle.restype = BOOL

        k32.GetStdHandle.argtypes = (DWORD,)
        k32.GetStdHandle.restype = HANDLE

        k32.SetHandleInformation.argtypes = (HANDLE, DWORD, DWORD)
        k32.SetHandleInformation.restype = BOOL

        _K32 = k32
    return _K32


def _last_error(where: str) -> OSError:
    err = ctypes.get_last_error()
    try:
        msg = ctypes.FormatError(err).strip()
    except Exception:
        msg = f"Win32 error {err}"
    return OSError(err, f"{where} failed: {msg}")


def _valid_handle(h) -> bool:
    value = h.value if hasattr(h, "value") else h
    return bool(value) and value != INVALID_HANDLE_VALUE


def _close_handle(h) -> None:
    if _valid_handle(h):
        try:
            _kernel32().CloseHandle(h)
        except Exception:
            pass


def _mark_inheritable(h) -> bool:
    if not _valid_handle(h):
        return False
    try:
        return bool(_kernel32().SetHandleInformation(
            h,
            HANDLE_FLAG_INHERIT,
            HANDLE_FLAG_INHERIT,
        ))
    except Exception:
        return False


def _startupinfo() -> tuple[STARTUPINFOW, bool]:
    k32 = _kernel32()
    si = STARTUPINFOW()
    si.cb = ctypes.sizeof(STARTUPINFOW)

    si.hStdInput = k32.GetStdHandle(STD_INPUT_HANDLE)
    si.hStdOutput = k32.GetStdHandle(STD_OUTPUT_HANDLE)
    si.hStdError = k32.GetStdHandle(STD_ERROR_HANDLE)

    use_std = True
    for h in (si.hStdInput, si.hStdOutput, si.hStdError):
        if not _mark_inheritable(h):
            use_std = False

    if use_std:
        si.dwFlags |= STARTF_USESTDHANDLES

    return si, use_std


def _command_for_createprocess(argv: list[str]) -> tuple[str | None, str]:
    # The transport passes an encoded PowerShell application argv so PowerShell,
    # not this helper, remains the sole interpreter of the supported native/.ps1
    # command boundary. Never improvise a cmd.exe layer here: list2cmdline implements
    # the C-runtime grammar, not cmd.exe's metacharacter/percent-expansion rules.
    exe = shutil.which(argv[0])
    first = exe or argv[0]
    ext = os.path.splitext(first)[1].lower()

    if ext in (".bat", ".cmd"):
        raise OSError(
            "batch files must be launched through the encoded PowerShell command seam"
        )

    if exe:
        return first, subprocess.list2cmdline([first, *argv[1:]])

    return None, subprocess.list2cmdline(argv)


def _create_suspended(argv: list[str]) -> PROCESS_INFORMATION:
    k32 = _kernel32()
    app, command_line = _command_for_createprocess(argv)

    # CreateProcessW is documented as allowed to modify lpCommandLine, so pass
    # a mutable wchar buffer, not an immutable Python string.
    cmd_buf = ctypes.create_unicode_buffer(command_line)

    si, inherit_handles = _startupinfo()
    pi = PROCESS_INFORMATION()

    ok = k32.CreateProcessW(
        app,
        cmd_buf,
        None,
        None,
        bool(inherit_handles),
        CREATE_SUSPENDED | CREATE_NO_WINDOW,
        None,
        None,
        ctypes.byref(si),
        ctypes.byref(pi),
    )
    if not ok:
        raise _last_error("CreateProcessW")
    return pi


def _resume(pi: PROCESS_INFORMATION) -> None:
    prev = _kernel32().ResumeThread(pi.hThread)
    if prev == DWORD_MINUS_ONE:
        raise _last_error("ResumeThread")
    _close_handle(pi.hThread)
    pi.hThread = None


def _wait_exit_code(h_process) -> int:
    rc = _kernel32().WaitForSingleObject(h_process, INFINITE)
    if rc == WAIT_FAILED:
        raise _last_error("WaitForSingleObject(process)")
    if rc != WAIT_OBJECT_0:
        raise OSError(f"WaitForSingleObject(process) returned {rc}")

    code = DWORD()
    if not _kernel32().GetExitCodeProcess(h_process, ctypes.byref(code)):
        raise _last_error("GetExitCodeProcess")
    return int(code.value)


def _query_basic(h_job) -> JOBOBJECT_BASIC_ACCOUNTING_INFORMATION:
    info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION()
    ret = DWORD()
    if not _kernel32().QueryInformationJobObject(
        h_job,
        JobObjectBasicAccountingInformation,
        ctypes.byref(info),
        ctypes.sizeof(info),
        ctypes.byref(ret),
    ):
        raise _last_error("QueryInformationJobObject(BasicAccounting)")
    return info


def _query_extended(h_job) -> JOBOBJECT_EXTENDED_LIMIT_INFORMATION:
    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    ret = DWORD()
    if not _kernel32().QueryInformationJobObject(
        h_job,
        JobObjectExtendedLimitInformation,
        ctypes.byref(info),
        ctypes.sizeof(info),
        ctypes.byref(ret),
    ):
        raise _last_error("QueryInformationJobObject(ExtendedLimit)")
    return info


def _enable_observed_breakaway(h_job) -> None:
    """Permit only explicitly breakaway-created descendants of this telemetry Job."""
    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_BREAKAWAY_OK
    if not _kernel32().SetInformationJobObject(
        h_job,
        JobObjectExtendedLimitInformation,
        ctypes.byref(info),
        ctypes.sizeof(info),
    ):
        raise _last_error("SetInformationJobObject(ObservedBreakaway)")


def _enable_kill_on_close(h_job) -> None:
    """Make closing the one private job handle terminate every ordinary descendant."""
    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if not _kernel32().SetInformationJobObject(
        h_job,
        JobObjectExtendedLimitInformation,
        ctypes.byref(info),
        ctypes.sizeof(info),
    ):
        raise _last_error("SetInformationJobObject(KillOnClose)")


def _wait_job_empty(h_job, timeout_s: float = JOB_DRAIN_GRACE_S) -> tuple[bool, int]:
    # A job object is not signaled merely because all processes exited. Poll the
    # kernel-maintained ActiveProcesses accounting counter instead. This wait is
    # deliberately bounded: a command that successfully starts a detached helper
    # must retain the same completion semantics with telemetry on or off.
    deadline = time.monotonic() + max(0.0, timeout_s)
    delay = 0.01
    while True:
        active = int(_query_basic(h_job).ActiveProcesses)
        if active == 0:
            return True, 0
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False, active
        time.sleep(min(delay, remaining))
        if delay < 0.25:
            delay *= 2


def _filetime_value(value: FILETIME) -> int:
    return (int(value.high) << 32) | int(value.low)


def _system_cpu_counter() -> tuple[int, int]:
    idle, kernel, user = FILETIME(), FILETIME(), FILETIME()
    if not _kernel32().GetSystemTimes(
        ctypes.byref(idle),
        ctypes.byref(kernel),
        ctypes.byref(user),
    ):
        raise _last_error("GetSystemTimes")
    return _filetime_value(kernel) + _filetime_value(user), _filetime_value(idle)


def _system_memory() -> tuple[int, int]:
    status = MEMORYSTATUSEX()
    status.length = ctypes.sizeof(status)
    if not _kernel32().GlobalMemoryStatusEx(ctypes.byref(status)):
        raise _last_error("GlobalMemoryStatusEx")
    return int(status.total_phys), int(status.avail_phys)


def _gpu_reading() -> tuple[str, list[dict[str, object]]]:
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=index,name,utilization.gpu,memory.free,memory.total",
                "--format=csv,noheader,nounits",
            ],
            text=True,
            capture_output=True,
            timeout=0.5,
            check=False,
            creationflags=CREATE_NO_WINDOW,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unavailable", []
    if result.returncode != 0:
        return "unavailable", []
    devices = []
    malformed = 0
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
                    "vram_free_bytes": int(free_mib * 1048576),
                    "vram_total_bytes": int(total_mib * 1048576),
                }
            )
        except (TypeError, ValueError):
            malformed += 1
    if not devices:
        return "unavailable", []
    return ("partial" if malformed else "measured"), devices


class _PressureSamples:
    def __init__(self) -> None:
        self.count = 0
        self._cpu_prior: tuple[int, int] | None = None
        self._cpu_busy: list[float] = []
        self.total_memory: int | None = None
        self.min_available: int | None = None
        self.max_used_pct: float | None = None
        self.gpu_sample_count = 0
        self.gpu_failed_sample_count = 0
        self.gpus: dict[str, dict[str, object]] = {}
        self._next_gpu_at = 0.0
        self.errors = 0

    def sample(self) -> None:
        self.count += 1
        try:
            counter = _system_cpu_counter()
            if self._cpu_prior is not None:
                total = counter[0] - self._cpu_prior[0]
                idle = counter[1] - self._cpu_prior[1]
                if total > 0 and 0 <= idle <= total:
                    self._cpu_busy.append(100.0 * (1.0 - idle / total))
            self._cpu_prior = counter
        except Exception:
            self.errors += 1

        try:
            total, available = _system_memory()
            if total <= 0 or available < 0 or available > total:
                raise ValueError("invalid memory counters")
            self.total_memory = total
            self.min_available = min(self.min_available or available, available)
            used = 100.0 * (1.0 - available / total)
            self.max_used_pct = max(self.max_used_pct or 0.0, used)
        except Exception:
            self.errors += 1

        now = time.monotonic()
        if now >= self._next_gpu_at:
            self._sample_gpu()
            self._next_gpu_at = now + 1.0

    def _sample_gpu(self) -> None:
        status, devices = _gpu_reading()
        self.gpu_sample_count += 1
        if status != "measured":
            self.gpu_failed_sample_count += 1
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
                aggregate["max_util_pct"] = max(
                    float(aggregate["max_util_pct"] or 0.0),
                    float(util),
                )
            free = device["vram_free_bytes"]
            if isinstance(free, int):
                current = aggregate["min_vram_free_bytes"]
                aggregate["min_vram_free_bytes"] = min(
                    int(current) if isinstance(current, int) else free,
                    free,
                )

    def cpu_payload(self) -> dict[str, object]:
        if not self._cpu_busy:
            return {
                "scope": "whole_device",
                "avg_busy_pct": None,
                "max_busy_pct": None,
                "status": "unavailable",
            }
        return {
            "scope": "whole_device",
            "avg_busy_pct": round(sum(self._cpu_busy) / len(self._cpu_busy), 1),
            "max_busy_pct": round(max(self._cpu_busy), 1),
            "status": "measured",
        }

    def memory_payload(self) -> dict[str, object]:
        return {
            "scope": "whole_device",
            "total_bytes": self.total_memory,
            "min_available_bytes": self.min_available,
            "max_used_pct": (
                round(self.max_used_pct, 1) if self.max_used_pct is not None else None
            ),
            "status": "measured" if self.min_available is not None else "unavailable",
        }

    def gpu_payload(self) -> dict[str, object]:
        devices = [self.gpus[key] for key in sorted(self.gpus)]
        utils = [
            float(device["max_util_pct"])
            for device in devices
            if isinstance(device["max_util_pct"], (int, float))
        ]
        free = [
            int(device["min_vram_free_bytes"])
            for device in devices
            if isinstance(device["min_vram_free_bytes"], int)
        ]
        if not utils and not free:
            status = "unavailable"
        elif self.gpu_failed_sample_count:
            status = "partial"
        else:
            status = "measured"
        return {
            "scope": "whole_device",
            "kind": "discrete" if devices else "unknown",
            "max_util_pct": round(max(utils), 1) if utils else None,
            "min_vram_free_bytes": min(free) if free else None,
            "status": status,
            "sample_count": self.gpu_sample_count,
            "failed_sample_count": self.gpu_failed_sample_count,
            "devices": devices,
        }


def _wait_exit_code_sampled(h_process, samples: _PressureSamples) -> int:
    while True:
        samples.sample()
        wait = _kernel32().WaitForSingleObject(h_process, DETAILED_SAMPLE_MS)
        if wait == WAIT_OBJECT_0:
            code = DWORD()
            if not _kernel32().GetExitCodeProcess(h_process, ctypes.byref(code)):
                raise _last_error("GetExitCodeProcess")
            return int(code.value)
        if wait == WAIT_FAILED:
            raise _last_error("WaitForSingleObject(process)")
        if wait != WAIT_TIMEOUT:
            raise OSError(f"WaitForSingleObject(process) returned {wait}")


def _detailed_payload(
    h_job,
    wall: float,
    samples: _PressureSamples,
    *,
    process_tree_drained: bool,
    active_processes_at_cutoff: int,
) -> dict[str, object]:
    basic = _query_basic(h_job)
    extended = _query_extended(h_job)
    cpu_sec = (
        int(basic.TotalUserTime) + int(basic.TotalKernelTime)
    ) / TICKS_PER_SECOND
    peak_bytes = int(extended.PeakJobMemoryUsed)
    avg_cpu = cpu_sec / wall * 100.0 if wall > 0 else 0.0
    payload: dict[str, object] = {
        "schema": 1,
        "status": "ok",
        "cpu": {
            "cpu_sec": round(cpu_sec, 3),
            "avg_cpu_pct": round(avg_cpu, 1),
            "coverage": (
                "job_object_drained" if process_tree_drained else "job_object_cutoff"
            ),
            "whole_device": samples.cpu_payload(),
        },
        "memory": {
            "peak_bytes": peak_bytes,
            "metric": "job_memory_peak",
            "sample_interval_ms": DETAILED_SAMPLE_MS,
            "sample_count": samples.count,
            "coverage": (
                "job_object_drained" if process_tree_drained else "job_object_cutoff"
            ),
            "shared_page_semantics": "not_applicable",
            "whole_device": samples.memory_payload(),
        },
        "gpu": samples.gpu_payload(),
        "peak_rss_mb": round(peak_bytes / 1048576.0, 1),
        "avg_cpu_pct": round(avg_cpu, 1),
        "cpu_sec": round(cpu_sec, 3),
        "wall_sec": round(wall, 3),
        "process_tree_drained": process_tree_drained,
    }
    if not process_tree_drained:
        payload["active_processes_at_cutoff"] = active_processes_at_cutoff
    if samples.errors:
        payload["whole_device_sampling_error_count"] = samples.errors
    return payload


def _unknown_detailed_payload(reason: str, wall: float | None = None) -> dict[str, object]:
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
            "metric": "job_memory_peak",
            "sample_interval_ms": DETAILED_SAMPLE_MS,
            "sample_count": 0,
            "coverage": "sampler_failed",
            "shared_page_semantics": "not_applicable",
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
        "wall_sec": round(wall, 3) if wall is not None else None,
        "process_tree_drained": None,
        "detail": reason,
    }


def _emit_payload(payload: dict[str, object]) -> None:
    line = (TELEMETRY_MARKER + json.dumps(payload) + "\n").encode("utf-8", "replace")
    try:
        sys.stderr.buffer.write(line)
        sys.stderr.buffer.flush()
    except Exception:
        try:
            sys.stderr.write(TELEMETRY_MARKER + json.dumps(payload) + "\n")
            sys.stderr.flush()
        except Exception:
            pass


def _emit_telemetry(h_job, wall: float, *, process_tree_drained: bool = True,
                    active_processes_at_cutoff: int = 0) -> None:
    basic = _query_basic(h_job)
    extended = _query_extended(h_job)

    cpu = (int(basic.TotalUserTime) + int(basic.TotalKernelTime)) / TICKS_PER_SECOND
    peak_mb = int(extended.PeakJobMemoryUsed) / 1048576.0
    avg = round(cpu / wall * 100.0, 1) if wall > 0 else 0.0

    payload = {
        "peak_rss_mb": round(peak_mb, 1),
        "avg_cpu_pct": avg,
        "cpu_sec": round(cpu, 3),
        "wall_sec": round(wall, 3),
        "process_tree_drained": process_tree_drained,
    }
    if not process_tree_drained:
        payload["active_processes_at_cutoff"] = active_processes_at_cutoff

    # Write bytes so Windows text-mode newline translation cannot turn the
    # sentinel into CRLF. remrun's parser looks for "\n__REMRUN_TELEMETRY__ ".
    line = (TELEMETRY_MARKER + json.dumps(payload) + "\n").encode(
        "utf-8",
        "replace",
    )
    try:
        sys.stderr.buffer.write(line)
        sys.stderr.buffer.flush()
    except Exception:
        try:
            sys.stderr.write(TELEMETRY_MARKER + json.dumps(payload) + "\n")
            sys.stderr.flush()
        except Exception:
            pass


def _observed_breakaway_payload(wall: float | None) -> dict[str, object]:
    """Return explicit unknowns: the workload moved to remrun's inner Job."""
    payload = _unknown_detailed_payload(
        "observed workload runs in a separate inner Job after explicit breakaway",
        wall,
    )
    payload["coverage"] = "observer_wrapper_only"
    cpu = payload.get("cpu")
    if isinstance(cpu, dict):
        cpu["coverage"] = "observer_wrapper_only"
    memory = payload.get("memory")
    if isinstance(memory, dict):
        memory["coverage"] = "observer_wrapper_only"
    return payload


def _plain_run(argv: list[str]) -> int:
    try:
        return int(subprocess.call(argv))
    except BaseException as exc:  # last-ditch: do not print a traceback
        try:
            name = argv[0] if argv else "<empty command>"
            sys.stderr.write(
                f"remrun telemetry wrapper: failed to execute {name!r}: {exc}\n"
            )
        except Exception:
            pass
        return 127


def _bounded_helper_run(argv: list[str]) -> int:
    """Run an internal helper in a fail-closed Job Object until its whole tree exits."""
    job = None
    pi = PROCESS_INFORMATION()
    created = False
    resumed = False
    try:
        job = _kernel32().CreateJobObjectW(None, None)
        if not _valid_handle(job):
            raise _last_error("CreateJobObjectW")
        _enable_kill_on_close(job)
        pi = _create_suspended(argv)
        created = True
        if not _kernel32().AssignProcessToJobObject(job, pi.hProcess):
            raise _CommandNotStarted("AssignProcessToJobObject failed")
        _resume(pi)
        resumed = True
        direct_rc = _wait_exit_code(pi.hProcess)
        _close_handle(pi.hProcess)
        pi.hProcess = None
        while int(_query_basic(job).ActiveProcesses) > 0:
            time.sleep(0.01)
        return direct_rc
    except BaseException as exc:
        if created and not resumed:
            try:
                _kernel32().TerminateProcess(pi.hProcess, GUARD_HELPER_EXIT)
            except Exception:
                pass
        try:
            sys.stderr.write(
                f"remrun bounded helper failed: {type(exc).__name__}: {exc}\n"
            )
        except Exception:
            pass
        return GUARD_HELPER_EXIT
    finally:
        _close_handle(pi.hThread)
        _close_handle(pi.hProcess)
        # This is the final containment boundary when the outer deadline kills
        # this wrapper while an inherited-output descendant still exists.
        _close_handle(job)


def _job_run(argv: list[str], *, allow_observed_breakaway: bool = False) -> int:
    job = None
    pi = PROCESS_INFORMATION()
    created = False
    resumed = False
    child_rc = None

    try:
        job = _kernel32().CreateJobObjectW(None, None)
        if not _valid_handle(job):
            raise _last_error("CreateJobObjectW")
        if allow_observed_breakaway:
            _enable_observed_breakaway(job)

        pi = _create_suspended(argv)
        created = True

        if not _kernel32().AssignProcessToJobObject(job, pi.hProcess):
            # Telemetry failed, but the command is safely suspended. Resume and
            # run it normally, omitting the sentinel.
            _resume(pi)
            resumed = True
            return _wait_exit_code(pi.hProcess)

        t0 = time.time()

        _resume(pi)
        resumed = True

        child_rc = _wait_exit_code(pi.hProcess)
        _close_handle(pi.hProcess)
        pi.hProcess = None

        # Include descendants that outlive the direct child. If this query path
        # fails, the outer handler returns the direct child's exit code without a
        # sentinel rather than risking the run.
        drained, active = _wait_job_empty(job)

        wall = time.time() - t0
        if allow_observed_breakaway:
            _emit_payload(_observed_breakaway_payload(wall))
        else:
            _emit_telemetry(
                job,
                wall,
                process_tree_drained=drained,
                active_processes_at_cutoff=active,
            )
        return child_rc

    except Exception:
        if created and not resumed:
            try:
                _kernel32().TerminateProcess(pi.hProcess, 127)
            except Exception:
                pass
            _close_handle(pi.hThread)
            pi.hThread = None
            _close_handle(pi.hProcess)
            pi.hProcess = None
            raise

        if created and resumed:
            # The command has run or is running; do not start it a second time.
            if child_rc is not None:
                return child_rc
            try:
                return _wait_exit_code(pi.hProcess) if _valid_handle(pi.hProcess) else 1
            except Exception:
                return 1

        raise

    finally:
        _close_handle(pi.hThread)
        _close_handle(pi.hProcess)
        _close_handle(job)


def _job_run_detailed(
    argv: list[str], *, allow_observed_breakaway: bool = False
) -> int:
    job = None
    pi = PROCESS_INFORMATION()
    created = False
    resumed = False
    child_rc = None
    started = None

    try:
        job = _kernel32().CreateJobObjectW(None, None)
        if not _valid_handle(job):
            raise _last_error("CreateJobObjectW")
        if allow_observed_breakaway:
            _enable_observed_breakaway(job)

        pi = _create_suspended(argv)
        created = True
        if not _kernel32().AssignProcessToJobObject(job, pi.hProcess):
            _resume(pi)
            resumed = True
            started = time.time()
            child_rc = _wait_exit_code(pi.hProcess)
            _emit_payload(
                _unknown_detailed_payload(
                    "AssignProcessToJobObject failed",
                    time.time() - started,
                )
            )
            return child_rc

        samples = _PressureSamples()
        started = time.time()
        _resume(pi)
        resumed = True
        child_rc = _wait_exit_code_sampled(pi.hProcess, samples)
        _close_handle(pi.hProcess)
        pi.hProcess = None

        drained, active = _wait_job_empty(job)
        wall = time.time() - started
        if allow_observed_breakaway:
            payload = _observed_breakaway_payload(wall)
        else:
            payload = _detailed_payload(
                job,
                wall,
                samples,
                process_tree_drained=drained,
                active_processes_at_cutoff=active,
            )
        _emit_payload(payload)
        return child_rc

    except Exception as exc:
        if created and not resumed:
            try:
                _kernel32().TerminateProcess(pi.hProcess, 127)
            except Exception:
                pass
            _close_handle(pi.hThread)
            pi.hThread = None
            _close_handle(pi.hProcess)
            pi.hProcess = None
            raise _CommandNotStarted from exc

        if created and resumed:
            if child_rc is None:
                try:
                    child_rc = (
                        _wait_exit_code(pi.hProcess)
                        if _valid_handle(pi.hProcess)
                        else 1
                    )
                except Exception:
                    child_rc = 1
            wall = time.time() - started if started is not None else None
            _emit_payload(
                _unknown_detailed_payload(
                    f"detailed sampler failed after command start: {type(exc).__name__}",
                    wall,
                )
            )
            return child_rc
        raise _CommandNotStarted from exc
    finally:
        _close_handle(pi.hThread)
        _close_handle(pi.hProcess)
        _close_handle(job)




def _emit_guard_ready(token: str) -> None:
    line = f"\n__REMRUN_GUARD_READY_{token}__\n".encode("utf-8", "replace")
    try:
        sys.stderr.buffer.write(line)
        sys.stderr.buffer.flush()
    except Exception:
        try:
            sys.stderr.write(line.decode("utf-8"))
            sys.stderr.flush()
        except Exception:
            pass


def _emit_guard_result(token: str, payload: dict[str, object]) -> None:
    line = (
        f"\n__REMRUN_GUARD_RESULT_{token}__ "
        + json.dumps(payload, separators=(",", ":"))
        + "\n"
    ).encode("utf-8", "replace")
    try:
        sys.stderr.buffer.write(line)
        sys.stderr.buffer.flush()
    except Exception:
        try:
            sys.stderr.write(line.decode("utf-8"))
            sys.stderr.flush()
        except Exception:
            pass


def _guard_payload(
    *,
    status: str,
    reason: str,
    detail: str,
    command_started: bool,
    command_exit_code: int | None,
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
        "helper_exit_code": (
            command_exit_code if status == "ok" and command_exit_code is not None
            else GUARD_HELPER_EXIT
        ),
        "max_command_bytes": max_command_bytes,
        "min_available_bytes": min_available_bytes,
        "host_total_bytes": host_total_bytes,
        "initial_host_available_bytes": initial_host_available_bytes,
        "min_host_available_bytes": min_host_available_bytes,
        "peak_command_bytes": peak_command_bytes,
        "sample_count": sample_count,
        "sample_interval_ms": DETAILED_SAMPLE_MS,
        "memory_metric": "job_memory_peak",
        "process_tree_drained": process_tree_drained,
        "forced_descendant_cleanup": forced_descendant_cleanup,
        "cleanup_complete": cleanup_complete,
        # Ordinary CreateProcess descendants inherit this job because neither
        # breakaway limit is enabled. A process created by an external broker
        # (for example Win32_Process.Create) is outside that containment boundary.
        "detached_children_possible": True,
        "ordinary_child_breakaway_possible": False,
        "external_broker_escape_possible": True,
        "platform": "windows",
    }


def _set_job_guard_limits(h_job, max_command_bytes: int) -> None:
    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = (
        JOB_OBJECT_LIMIT_JOB_MEMORY | JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    )
    info.JobMemoryLimit = max_command_bytes
    if not _kernel32().SetInformationJobObject(
        h_job,
        JobObjectExtendedLimitInformation,
        ctypes.byref(info),
        ctypes.sizeof(info),
    ):
        raise _last_error("SetInformationJobObject(ExtendedLimit)")


def _associate_guard_completion_port(h_job):
    port = _kernel32().CreateIoCompletionPort(
        HANDLE(INVALID_HANDLE_VALUE), None, 0, 1
    )
    if not _valid_handle(port):
        raise _last_error("CreateIoCompletionPort")
    association = JOBOBJECT_ASSOCIATE_COMPLETION_PORT()
    association.CompletionKey = LPVOID(1)
    association.CompletionPort = port
    if not _kernel32().SetInformationJobObject(
        h_job,
        JobObjectAssociateCompletionPortInformation,
        ctypes.byref(association),
        ctypes.sizeof(association),
    ):
        _close_handle(port)
        raise _last_error("SetInformationJobObject(AssociateCompletionPort)")
    return port


def _poll_guard_messages(port) -> list[int]:
    messages: list[int] = []
    for _ in range(64):
        transferred = DWORD()
        key = ULONG_PTR()
        overlapped = LPVOID()
        ctypes.set_last_error(0)
        ok = _kernel32().GetQueuedCompletionStatus(
            port,
            ctypes.byref(transferred),
            ctypes.byref(key),
            ctypes.byref(overlapped),
            0,
        )
        if not ok:
            err = ctypes.get_last_error()
            if err == ERROR_WAIT_TIMEOUT:
                break
            raise _last_error("GetQueuedCompletionStatus")
        messages.append(int(transferred.value))
    return messages


def _terminate_job_bounded(h_job) -> bool:
    if not _kernel32().TerminateJobObject(h_job, GUARD_HELPER_EXIT):
        try:
            if int(_query_basic(h_job).ActiveProcesses) == 0:
                return True
        except Exception:
            pass
        return False
    deadline = time.monotonic() + 0.75
    while time.monotonic() < deadline:
        try:
            if int(_query_basic(h_job).ActiveProcesses) == 0:
                return True
        except Exception:
            return False
        time.sleep(0.02)
    try:
        return int(_query_basic(h_job).ActiveProcesses) == 0
    except Exception:
        return False


def _record_guard_pressure(
    samples: _PressureSamples,
    *,
    total: int,
    available: int,
) -> None:
    samples.count += 1
    samples.total_memory = total
    samples.min_available = min(
        samples.min_available if samples.min_available is not None else available,
        available,
    )
    used = 100.0 * (1.0 - available / total)
    samples.max_used_pct = max(samples.max_used_pct or 0.0, used)
    try:
        counter = _system_cpu_counter()
        if samples._cpu_prior is not None:
            delta_total = counter[0] - samples._cpu_prior[0]
            delta_idle = counter[1] - samples._cpu_prior[1]
            if delta_total > 0 and 0 <= delta_idle <= delta_total:
                samples._cpu_busy.append(100.0 * (1.0 - delta_idle / delta_total))
        samples._cpu_prior = counter
    except Exception:
        samples.errors += 1
    now = time.monotonic()
    if now >= samples._next_gpu_at:
        samples._sample_gpu()
        samples._next_gpu_at = now + 1.0


def _emit_guard_telemetry(
    h_job,
    *,
    detailed: bool,
    telemetry: bool,
    wall: float,
    samples: _PressureSamples,
    process_tree_drained: bool,
    active_processes_at_cutoff: int,
) -> None:
    if not (detailed or telemetry):
        return
    try:
        if detailed:
            _emit_payload(
                _detailed_payload(
                    h_job,
                    wall,
                    samples,
                    process_tree_drained=process_tree_drained,
                    active_processes_at_cutoff=active_processes_at_cutoff,
                )
            )
        else:
            _emit_telemetry(
                h_job,
                wall,
                process_tree_drained=process_tree_drained,
                active_processes_at_cutoff=active_processes_at_cutoff,
            )
    except Exception as exc:
        if detailed:
            _emit_payload(
                _unknown_detailed_payload(
                    f"guard telemetry assembly failed: {type(exc).__name__}", wall
                )
            )


def _guarded_job_run(
    argv: list[str],
    *,
    max_command_bytes: int,
    min_available_bytes: int,
    token: str,
    detailed: bool,
    telemetry: bool,
) -> int:
    job = None
    port = None
    pi = PROCESS_INFORMATION()
    created = False
    resumed = False
    host_total: int | None = None
    initial_available: int | None = None
    min_available: int | None = None
    peak: int | None = None
    sample_count = 0
    samples = _PressureSamples()
    started: float | None = None

    try:
        if not re.fullmatch(r"[0-9a-f]{32}", token):
            raise ValueError("guard token must be 32 lowercase hexadecimal characters")
        if max_command_bytes <= 0 or min_available_bytes <= 0:
            raise ValueError("guard thresholds must be positive")
        if os.name != "nt":
            raise RuntimeError("Windows Job Object guard is unavailable on this platform")

        job = _kernel32().CreateJobObjectW(None, None)
        if not _valid_handle(job):
            raise _last_error("CreateJobObjectW")
        _set_job_guard_limits(job, max_command_bytes)
        port = _associate_guard_completion_port(job)

        host_total, initial_available = _system_memory()
        if host_total <= 0 or not 0 <= initial_available <= host_total:
            raise RuntimeError("invalid initial host-memory counters")
        min_available = initial_available
        if max_command_bytes + min_available_bytes > host_total:
            payload = _guard_payload(
                status="refused",
                reason="thresholds_exceed_host",
                detail="configured command ceiling plus reserve exceeds measured physical memory",
                command_started=False,
                command_exit_code=None,
                max_command_bytes=max_command_bytes,
                min_available_bytes=min_available_bytes,
                host_total_bytes=host_total,
                initial_host_available_bytes=initial_available,
                min_host_available_bytes=initial_available,
            )
            _emit_guard_result(token, payload)
            return GUARD_HELPER_EXIT
        if initial_available <= min_available_bytes:
            payload = _guard_payload(
                status="refused",
                reason="host_memory_reserve",
                detail="initial host available memory is at or below the configured reserve",
                command_started=False,
                command_exit_code=None,
                max_command_bytes=max_command_bytes,
                min_available_bytes=min_available_bytes,
                host_total_bytes=host_total,
                initial_host_available_bytes=initial_available,
                min_host_available_bytes=initial_available,
            )
            _emit_guard_result(token, payload)
            return GUARD_HELPER_EXIT

        pi = _create_suspended(argv)
        created = True
        if not _kernel32().AssignProcessToJobObject(job, pi.hProcess):
            raise _CommandNotStarted("AssignProcessToJobObject failed")

        # The hard job limit, kill-on-close, completion port, and initial host
        # reading all exist before the suspended user process is resumed.
        _emit_guard_ready(token)
        started = time.time()
        _resume(pi)
        resumed = True

        direct_rc: int | None = None
        guard_status: str | None = None
        guard_reason = ""
        guard_detail = ""
        trigger_value: int | None = None

        while direct_rc is None and guard_status is None:
            try:
                total, available = _system_memory()
                if total <= 0 or total != host_total or not 0 <= available <= total:
                    raise RuntimeError("invalid or changed host-memory counters")
                min_available = min(
                    min_available if min_available is not None else available,
                    available,
                )
                extended = _query_extended(job)
                peak = max(peak or 0, int(extended.PeakJobMemoryUsed))
                sample_count += 1
                if detailed:
                    _record_guard_pressure(samples, total=total, available=available)
                messages = _poll_guard_messages(port)
                memory_message = next(
                    (
                        message
                        for message in messages
                        if message
                        in {
                            JOB_OBJECT_MSG_PROCESS_MEMORY_LIMIT,
                            JOB_OBJECT_MSG_JOB_MEMORY_LIMIT,
                            JOB_OBJECT_MSG_NOTIFICATION_LIMIT,
                        }
                    ),
                    None,
                )
                if memory_message is not None or peak >= max_command_bytes:
                    guard_status = "terminated"
                    guard_reason = "command_memory_limit"
                    guard_detail = "Windows Job Object memory limit was reached"
                    trigger_value = peak
                elif available <= min_available_bytes:
                    guard_status = "terminated"
                    guard_reason = "host_memory_reserve"
                    guard_detail = "host available memory reached the hard reserve"
                    trigger_value = available
            except Exception as exc:
                guard_status = "failed_safe"
                guard_reason = "enforcement_sampling_failed"
                guard_detail = f"{type(exc).__name__}: {exc}"

            if guard_status is None:
                wait = _kernel32().WaitForSingleObject(pi.hProcess, DETAILED_SAMPLE_MS)
                if wait == WAIT_OBJECT_0:
                    code = DWORD()
                    if not _kernel32().GetExitCodeProcess(pi.hProcess, ctypes.byref(code)):
                        guard_status = "failed_safe"
                        guard_reason = "enforcement_sampling_failed"
                        guard_detail = str(_last_error("GetExitCodeProcess"))
                    else:
                        direct_rc = int(code.value)
                elif wait == WAIT_FAILED:
                    guard_status = "failed_safe"
                    guard_reason = "enforcement_sampling_failed"
                    guard_detail = str(_last_error("WaitForSingleObject(process)"))
                elif wait != WAIT_TIMEOUT:
                    guard_status = "failed_safe"
                    guard_reason = "enforcement_sampling_failed"
                    guard_detail = f"WaitForSingleObject(process) returned {wait}"

        if guard_status is not None:
            cleanup = _terminate_job_bounded(job)
            if not cleanup:
                trigger_reason = guard_reason
                guard_status = "failed_safe"
                guard_reason = "termination_cleanup_failed"
                guard_detail = (
                    f"{guard_detail}; bounded job termination did not verify a drain"
                )
            else:
                trigger_reason = None
            wall = time.time() - started
            active = 0
            try:
                active = int(_query_basic(job).ActiveProcesses)
            except Exception:
                active = 1 if not cleanup else 0
            _emit_guard_telemetry(
                job,
                detailed=detailed,
                telemetry=telemetry,
                wall=wall,
                samples=samples,
                process_tree_drained=cleanup,
                active_processes_at_cutoff=active,
            )
            payload = _guard_payload(
                status=guard_status,
                reason=guard_reason,
                detail=guard_detail,
                command_started=True,
                command_exit_code=None,
                max_command_bytes=max_command_bytes,
                min_available_bytes=min_available_bytes,
                host_total_bytes=host_total,
                initial_host_available_bytes=initial_available,
                min_host_available_bytes=min_available,
                peak_command_bytes=peak,
                sample_count=sample_count,
                process_tree_drained=cleanup,
                cleanup_complete=cleanup,
            )
            if trigger_value is not None:
                payload["trigger_value_bytes"] = trigger_value
            if trigger_reason is not None:
                payload["trigger_reason"] = trigger_reason
            _emit_guard_result(token, payload)
            return GUARD_HELPER_EXIT

        assert direct_rc is not None
        deadline = time.monotonic() + JOB_DRAIN_GRACE_S
        drained = False
        active = 0
        guard_status = None
        guard_reason = ""
        guard_detail = ""
        trigger_value = None
        while True:
            try:
                total, available = _system_memory()
                if total <= 0 or total != host_total or not 0 <= available <= total:
                    raise RuntimeError("invalid or changed host-memory counters")
                min_available = min(
                    min_available if min_available is not None else available,
                    available,
                )
                extended = _query_extended(job)
                peak = max(peak or 0, int(extended.PeakJobMemoryUsed))
                sample_count += 1
                if detailed:
                    _record_guard_pressure(samples, total=total, available=available)
                messages = _poll_guard_messages(port)
                if any(
                    message
                    in {
                        JOB_OBJECT_MSG_PROCESS_MEMORY_LIMIT,
                        JOB_OBJECT_MSG_JOB_MEMORY_LIMIT,
                        JOB_OBJECT_MSG_NOTIFICATION_LIMIT,
                    }
                    for message in messages
                ) or peak >= max_command_bytes:
                    guard_status = "terminated"
                    guard_reason = "command_memory_limit"
                    guard_detail = "Windows Job Object memory limit was reached"
                    trigger_value = peak
                    break
                if available <= min_available_bytes:
                    guard_status = "terminated"
                    guard_reason = "host_memory_reserve"
                    guard_detail = "host available memory reached the hard reserve"
                    trigger_value = available
                    break
                active = int(_query_basic(job).ActiveProcesses)
                if active == 0:
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
            time.sleep(min(DETAILED_SAMPLE_MS / 1000.0, remaining))

        forced_cleanup = not drained and active > 0
        cleanup = True
        if guard_status is not None or forced_cleanup:
            cleanup = _terminate_job_bounded(job)
        trigger_reason = None
        if not cleanup:
            if guard_status is None:
                guard_status = "failed_safe"
                guard_reason = "descendant_cleanup_failed"
                guard_detail = "job descendants remained after bounded termination"
            else:
                trigger_reason = guard_reason
                guard_status = "failed_safe"
                guard_reason = "termination_cleanup_failed"
                guard_detail = (
                    f"{guard_detail}; bounded job termination did not verify a drain"
                )

        wall = time.time() - started
        _emit_guard_telemetry(
            job,
            detailed=detailed,
            telemetry=telemetry,
            wall=wall,
            samples=samples,
            process_tree_drained=drained and not forced_cleanup,
            active_processes_at_cutoff=active,
        )
        if guard_status is not None:
            payload = _guard_payload(
                status=guard_status,
                reason=guard_reason,
                detail=guard_detail,
                command_started=True,
                command_exit_code=None,
                max_command_bytes=max_command_bytes,
                min_available_bytes=min_available_bytes,
                host_total_bytes=host_total,
                initial_host_available_bytes=initial_available,
                min_host_available_bytes=min_available,
                peak_command_bytes=peak,
                sample_count=sample_count,
                process_tree_drained=cleanup,
                forced_descendant_cleanup=forced_cleanup,
                cleanup_complete=cleanup,
            )
            if trigger_value is not None:
                payload["trigger_value_bytes"] = trigger_value
            if trigger_reason is not None:
                payload["trigger_reason"] = trigger_reason
            _emit_guard_result(token, payload)
            return GUARD_HELPER_EXIT

        payload = _guard_payload(
            status="ok",
            reason="completed",
            detail="command completed without crossing the memory guard",
            command_started=True,
            command_exit_code=direct_rc,
            max_command_bytes=max_command_bytes,
            min_available_bytes=min_available_bytes,
            host_total_bytes=host_total,
            initial_host_available_bytes=initial_available,
            min_host_available_bytes=min_available,
            peak_command_bytes=peak,
            sample_count=sample_count,
            process_tree_drained=drained,
            forced_descendant_cleanup=forced_cleanup,
            cleanup_complete=cleanup,
        )
        _emit_guard_result(token, payload)
        return direct_rc

    except BaseException as exc:
        cleanup: bool | None = None
        if created and not resumed:
            try:
                _kernel32().TerminateProcess(pi.hProcess, GUARD_HELPER_EXIT)
            except Exception:
                pass
            cleanup = True
        elif resumed and _valid_handle(job):
            cleanup = _terminate_job_bounded(job)
        payload = _guard_payload(
            status="failed_safe" if resumed else "refused",
            reason="guard_runtime_failed" if resumed else "guard_initialization_failed",
            detail=f"{type(exc).__name__}: {exc}",
            command_started=resumed,
            command_exit_code=None,
            max_command_bytes=max_command_bytes,
            min_available_bytes=min_available_bytes,
            host_total_bytes=host_total,
            initial_host_available_bytes=initial_available,
            min_host_available_bytes=min_available,
            peak_command_bytes=peak,
            sample_count=sample_count,
            process_tree_drained=cleanup if resumed else None,
            cleanup_complete=cleanup,
        )
        _emit_guard_result(token, payload)
        return GUARD_HELPER_EXIT
    finally:
        _close_handle(pi.hThread)
        _close_handle(pi.hProcess)
        # KILL_ON_JOB_CLOSE is the final fail-safe if any process survives the
        # explicit bounded termination path or the helper itself unwinds.
        _close_handle(job)
        _close_handle(port)


_ARGV_FILE_MAX_BYTES = 1024 * 1024
_ARGV_FILE_MAX_ITEMS = 4096


def _load_staged_argv(path: str) -> list[str]:
    """Read one bounded private argv document and remove it before user start."""
    try:
        with open(path, "rb") as stream:
            data = stream.read(_ARGV_FILE_MAX_BYTES + 1)
    finally:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
    if len(data) > _ARGV_FILE_MAX_BYTES:
        raise ValueError("staged argv document exceeds the bounded size")
    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("staged argv document is invalid JSON") from exc
    if (
        not isinstance(payload, list)
        or not payload
        or len(payload) > _ARGV_FILE_MAX_ITEMS
        or not all(isinstance(value, str) and "\0" not in value for value in payload)
    ):
        raise ValueError("staged argv document is not a bounded string array")
    return payload


def main() -> int:
    try:
        idx = sys.argv.index("--")
        options = sys.argv[1:idx]
        argv = sys.argv[idx + 1:]
    except ValueError:
        sys.stderr.write(
            "usage: python _win_telemetry.py [--detailed] "
            "[--allow-observed-breakaway --argv-json-file PATH] "
            "[--guard-max-bytes N --guard-min-available-bytes N --guard-token TOKEN] "
            "-- <command> [args...]\n"
        )
        return 2

    detailed = "--detailed" in options
    telemetry = "--telemetry" in options
    allow_observed_breakaway = "--allow-observed-breakaway" in options
    bounded_helper = "--bounded-helper" in options
    argv_json_file = None

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
    }
    value_names = {*guard_names, "--argv-json-file"}
    guard_requested = any(name in options for name in guard_names)
    allowed = {
        "--detailed",
        "--telemetry",
        "--allow-observed-breakaway",
        "--bounded-helper",
        "--argv-json-file",
        *guard_names,
    }
    consumed_value_indexes = {
        index + 1
        for index, value in enumerate(options)
        if value in value_names and index + 1 < len(options)
    }
    unknown = [
        value
        for index, value in enumerate(options)
        if index not in consumed_value_indexes and value not in allowed
    ]
    if unknown:
        sys.stderr.write("remrun telemetry wrapper: invalid options or empty command\n")
        return 2

    try:
        argv_json_file = option_value("--argv-json-file")
    except ValueError:
        sys.stderr.write("remrun telemetry wrapper: invalid staged argv option\n")
        return 2
    if argv_json_file is not None:
        if not allow_observed_breakaway or argv:
            sys.stderr.write(
                "remrun telemetry wrapper: staged argv is observed-only and exclusive\n"
            )
            return 2
        try:
            argv = _load_staged_argv(argv_json_file)
        except (OSError, ValueError) as exc:
            sys.stderr.write(
                f"remrun telemetry wrapper: staged argv unavailable: {exc}\n"
            )
            return 2
    if not argv:
        sys.stderr.write("remrun telemetry wrapper: invalid options or empty command\n")
        return 2

    if bounded_helper:
        if options != ["--bounded-helper"] or os.name != "nt":
            sys.stderr.write("remrun telemetry wrapper: invalid bounded-helper invocation\n")
            return 2
        return _bounded_helper_run(argv)

    if guard_requested:
        if allow_observed_breakaway:
            sys.stderr.write(
                "remrun telemetry wrapper: observed breakaway is incompatible "
                "with the memory guard\n"
            )
            return 2
        token = option_value("--guard-token") or ""
        try:
            max_bytes = int(option_value("--guard-max-bytes") or "")
            reserve_bytes = int(option_value("--guard-min-available-bytes") or "")
        except ValueError:
            max_bytes = reserve_bytes = -1
        return _guarded_job_run(
            argv,
            max_command_bytes=max_bytes,
            min_available_bytes=reserve_bytes,
            token=token,
            detailed=detailed,
            telemetry=telemetry,
        )

    if not argv:
        sys.stderr.write("usage: python _win_telemetry.py -- <command> [args...]\n")
        return 2

    if os.name != "nt":
        started = time.time()
        rc = _plain_run(argv)
        if detailed:
            _emit_payload(
                _unknown_detailed_payload(
                    "Windows Job Object telemetry unavailable on this platform",
                    time.time() - started,
                )
            )
        return rc

    if not detailed:
        try:
            if allow_observed_breakaway:
                return _job_run(argv, allow_observed_breakaway=True)
            return _job_run(argv)
        except Exception:
            return _plain_run(argv)

    try:
        if allow_observed_breakaway:
            return _job_run_detailed(argv, allow_observed_breakaway=True)
        return _job_run_detailed(argv)
    except _CommandNotStarted as exc:
        started = time.time()
        rc = _plain_run(argv)
        _emit_payload(
            _unknown_detailed_payload(
                f"detailed sampler unavailable before command start: {type(exc).__name__}",
                time.time() - started,
            )
        )
        return rc
    except Exception as exc:
        _emit_payload(
            _unknown_detailed_payload(
                f"detailed sampler failed after command may have started: "
                f"{type(exc).__name__}"
            )
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
