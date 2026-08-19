"""Local controller hardware, measured the same way the remote probe measures.

The controller is usually NOT in devices.toml — it is the box you are sitting
at, not a run target — but leaving it out makes `fleet resources` a partial
picture exactly when you are deciding whether to offload. This measures the
local machine with the same definitions the remote probe uses (available RAM,
busy CPU) so the numbers in one table mean the same thing across rows.
"""
from __future__ import annotations

import os
import platform
import shutil
import socket
import subprocess

from .resources import (
    _NO_WINDOW_FLAG,
    _POSIX_SCRIPT,
    _WINDOWS_SCRIPT,
    ResourceView,
    _parse_posix,
    _parse_windows,
)


def _run(command: list[str], timeout: float) -> str | None:
    try:
        proc = subprocess.run(command, capture_output=True, text=True, timeout=timeout,
                              check=False, creationflags=_NO_WINDOW_FLAG)
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0 and not proc.stdout:
        return None
    return proc.stdout


def local_view(name: str = "", timeout: float = 20.0) -> ResourceView:
    """Measure this controller. Never raises."""
    host = ""
    try:
        host = socket.gethostname().split(".")[0]
    except OSError:
        pass
    view = ResourceView(
        name=(name or host or "LOCAL").upper(),
        reachable=True,
        os=("windows" if os.name == "nt" else
            "macos" if platform.system() == "Darwin" else "posix"),
        hostname=host,
        is_local=True,
    )

    if os.name == "nt":
        shell = shutil.which("powershell") or shutil.which("pwsh")
        out = _run([shell, "-NoProfile", "-Command", _WINDOWS_SCRIPT], timeout) if shell else None
        if out:
            _parse_windows(out, view)
    else:
        shell = shutil.which("bash") or "/bin/sh"
        out = _run([shell, "-lc", _POSIX_SCRIPT], timeout)
        if out:
            _parse_posix(out, view)

    if not view.cpu_count:
        view.cpu_count = os.cpu_count()
    if not view.chip:
        view.chip = platform.processor() or platform.machine()
    return view
