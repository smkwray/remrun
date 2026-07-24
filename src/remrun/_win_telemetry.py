# Windows process-tree telemetry wrapper for remrun.
# Usage: python _win_telemetry.py -- <argv...>
#
# Mirrors the POSIX getrusage sampler: runs the command, then reads kernel
# Job Object accounting for the whole process tree and emits the same
# "\n__REMRUN_TELEMETRY__ <json>\n" stderr sentinel remrun already parses.
from __future__ import annotations

import ctypes
import json
import os
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
WAIT_FAILED = 0xFFFFFFFF
DWORD_MINUS_ONE = 0xFFFFFFFF

# Optional telemetry must not redefine command completion. Give descendants a
# short grace period to finish and contribute their final accounting, then
# return the direct command's exit code even if a daemon/helper remains alive.
JOB_DRAIN_GRACE_S = 1.0

JobObjectBasicAccountingInformation = 1
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
    # Resolve through PATHEXT so commands like pnpm -> pnpm.cmd work. If the
    # resolved command is a batch file, run it through cmd.exe explicitly because
    # CreateProcess cannot execute .bat/.cmd files as application images.
    exe = shutil.which(argv[0])
    first = exe or argv[0]
    ext = os.path.splitext(first)[1].lower()

    if ext in (".bat", ".cmd"):
        comspec = os.environ.get("COMSPEC") or os.path.join(
            os.environ.get("SystemRoot", r"C:\Windows"),
            "System32",
            "cmd.exe",
        )
        cmd_argv = [comspec, "/d", "/c", "call", first, *argv[1:]]
        return comspec, subprocess.list2cmdline(cmd_argv)

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
        sys.stderr.write(TELEMETRY_MARKER + json.dumps(payload) + "\n")
        try:
            sys.stderr.flush()
        except Exception:
            pass


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


def _job_run(argv: list[str]) -> int:
    job = None
    pi = PROCESS_INFORMATION()
    created = False
    resumed = False
    child_rc = None

    try:
        job = _kernel32().CreateJobObjectW(None, None)
        if not _valid_handle(job):
            raise _last_error("CreateJobObjectW")

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


def main() -> int:
    try:
        idx = sys.argv.index("--")
        argv = sys.argv[idx + 1:]
    except ValueError:
        sys.stderr.write(
            "usage: python _win_telemetry.py -- <command> [args...]\n"
        )
        return 2

    if not argv:
        sys.stderr.write(
            "usage: python _win_telemetry.py -- <command> [args...]\n"
        )
        return 2

    if os.name != "nt":
        return _plain_run(argv)

    try:
        return _job_run(argv)
    except Exception:
        return _plain_run(argv)


if __name__ == "__main__":
    sys.exit(main())
