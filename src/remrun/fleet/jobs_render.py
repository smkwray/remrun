"""Compact rendering for target-local active jobs."""
from __future__ import annotations

from typing import Any

from .jobs import TargetJobsView

HEADERS = ("PROJECT", "FROM", "TO", "AGE", "CPU", "THR", "RAM", "STATE", "COMMAND")
DISPLAY_LIMITS = (16, 8, 6, 7, 6, 4, 7, 11, 22)


def _clip(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "~"


def _display_row(row: list[str]) -> list[str]:
    return [_clip(value, DISPLAY_LIMITS[index]) for index, value in enumerate(row)]


def _compact_age(seconds: object) -> str:
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        return "-"
    value = max(0, int(seconds))
    if value < 60:
        return f"{value}s"
    if value < 3600:
        return f"{value // 60}m"
    if value < 86400:
        return f"{value // 3600}h{(value % 3600) // 60:02d}m"
    return f"{value // 86400}d{(value % 86400) // 3600:02d}h"


def _compact_bytes(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return "-"
    amount = float(value)
    units = ("B", "K", "M", "G", "T")
    for unit in units:
        if amount < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(amount)}B"
            return f"{amount:.1f}{unit}" if amount < 10 else f"{amount:.0f}{unit}"
        amount /= 1024.0
    return "-"


def _cpu(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return "-"
    return f"{float(value):.0f}%" if value >= 10 else f"{float(value):.1f}%"


def _threads(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return "-"
    return str(int(value))


def _job_row(job: dict[str, Any], target: str) -> list[str]:
    cpu = job.get("cpu") if isinstance(job.get("cpu"), dict) else {}
    threads = job.get("threads") if isinstance(job.get("threads"), dict) else {}
    memory = job.get("memory") if isinstance(job.get("memory"), dict) else {}
    command = job.get("command") if isinstance(job.get("command"), dict) else {}
    reported_target = str(job.get("reported_target") or target)
    return _display_row([
        str(job.get("project") or "-"),
        str(job.get("source_controller") or "-"),
        reported_target,
        _compact_age(job.get("age_seconds")),
        _cpu(cpu.get("current_pct_one_logical_cpu")),
        _threads(threads.get("current_os_threads")),
        _compact_bytes(memory.get("current_bytes")),
        str(job.get("state") or "UNKNOWN"),
        str(command.get("label") or "command"),
    ])


def rows_for_view(view: TargetJobsView) -> list[list[str]]:
    if view.jobs:
        return [_job_row(job, view.name) for job in view.jobs]
    if view.status == "ok":
        return [_display_row(["-", "-", view.name, "-", "-", "-", "-", "IDLE", "-"])]
    state = "UNSUPPORTED" if view.status == "unsupported" else view.status.upper()
    detail = (view.detail or "target query failed").replace("\n", " ")[:64]
    return [_display_row(["-", "-", view.name, "-", "-", "-", "-", state, detail])]


def _widths(rows: list[list[str]]) -> list[int]:
    return [max(len(row[i]) for row in rows) for i in range(len(HEADERS))]


def _format(row: list[str], widths: list[int]) -> str:
    cells = []
    for index, value in enumerate(row):
        if index in {3, 4, 5, 6}:
            cells.append(value.rjust(widths[index]))
        else:
            cells.append(value.ljust(widths[index]))
    return "  ".join(cells).rstrip()


class IncrementalTable:
    """Fixed-width append-only table for completion-order TTY output."""

    def __init__(self, labels: list[str]) -> None:
        seed = [
            list(HEADERS),
            ["x" * limit for limit in DISPLAY_LIMITS],
        ]
        seed[1][2] = _clip(max(labels or ["target"], key=len), DISPLAY_LIMITS[2]).ljust(
            DISPLAY_LIMITS[2], "x"
        )
        self.widths = _widths(seed)

    def header(self) -> str:
        heading = _format(list(HEADERS), self.widths)
        rule = _format(["-" * width for width in self.widths], self.widths)
        return f"{heading}\n{rule}"

    def rows(self, view: TargetJobsView) -> list[str]:
        return [_format(row, self.widths) for row in rows_for_view(view)]


def render_table(views: list[TargetJobsView]) -> str:
    # Buffered output can honor the project grouping globally. Interactive TTY
    # output intentionally uses target-completion order instead.
    entries: list[tuple[tuple[object, ...], list[str]]] = []
    for view in views:
        if view.jobs:
            for job in view.jobs:
                entries.append((
                    (
                        0, str(job.get("project", "")), view.name,
                        int(job.get("started_at_unix_ns", 0) or 0),
                        str(job.get("job_id", "")),
                    ),
                    _job_row(job, view.name),
                ))
        else:
            entries.append(((1, "", view.name, 0, ""), rows_for_view(view)[0]))
    data = [list(HEADERS), *(row for _key, row in sorted(entries, key=lambda item: item[0]))]
    widths = _widths(data)
    lines = [_format(data[0], widths), _format(["-" * width for width in widths], widths)]
    lines.extend(_format(row, widths) for row in data[1:])
    return "\n".join(lines)
