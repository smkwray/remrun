"""Terminal rendering for `remrun fleet resources`.

Plain ASCII and no colour by default: this output is read in PowerShell, in
Terminal.app, and by agents parsing stdout, and the repo's contract asks for
agent-friendly output. Unmeasured values render as `-`, never as 0.
"""
from __future__ import annotations

from .resources import ResourceView

HEADERS = ("DEVICE", "CPU", "LOAD", "RAM", "GPU", "VRAM", "DISK", "STATUS")
USAGE_DISPLAYS = frozenset({"percent", "amounts"})
USAGE_ALERT_PCT = 85.0
LOAD_ALERT_PER_CORE = 1.0


def _gb(mb: float | None) -> str:
    if mb is None:
        return "-"
    return f"{mb / 1024.0:.0f}" if mb >= 1024 else f"{mb / 1024.0:.1f}"


def _pct(value: float | None) -> str:
    return "-" if value is None else f"{value:.0f}%"


def _usage_alert(value: float | None) -> bool:
    # Alert what the table displays: 84.6 renders as 85 and should not look
    # inexplicably unmarked beside an exactly-85.0 value.
    return value is not None and float(f"{value:.0f}") >= USAGE_ALERT_PCT


def _pct_cell(value: float | None) -> str:
    rendered = _pct(value)
    if _usage_alert(value):
        # Replace rather than append the percent sign, keeping 100* the same
        # width as 100%. The column header and footnote retain the unit.
        return rendered[:-1] + "*"
    return rendered


def _mark_amount(rendered: str, used_pct: float | None) -> str:
    return rendered + "*" if _usage_alert(used_pct) else rendered


def _load_value(view: ResourceView) -> float | None:
    if view.load_per_core is not None:
        return view.load_per_core
    return view.processor_queue_per_core


def _load_cell(view: ResourceView) -> str:
    rendered = view.load_label
    value = _load_value(view)
    if value is not None and value >= LOAD_ALERT_PER_CORE:
        return rendered + "*"
    return rendered


def _has_alert(view: ResourceView) -> bool:
    return view.reachable and (
        any(
            _usage_alert(value)
            for value in (
                view.cpu_busy_pct,
                view.ram_used_pct,
                view.gpu_util_pct,
                view.vram_used_pct,
                view.primary_disk.used_pct,
            )
        )
        or (
            _load_value(view) is not None
            and _load_value(view) >= LOAD_ALERT_PER_CORE
        )
    )


def _ram_amount_cell(view: ResourceView) -> str:
    if view.ram_free_mb is None and view.ram_total_mb is None:
        return "-"
    if view.ram_total_mb is None:
        return f"{_gb(view.ram_free_mb)} GB free"
    used = None
    if view.ram_free_mb is not None:
        used = max(0.0, view.ram_total_mb - view.ram_free_mb)
    return f"{_gb(used)}/{_gb(view.ram_total_mb)} GB"


def _vram_amount_cell(view: ResourceView) -> str:
    if view.gpu_unified or view.vram_total_mb is None:
        return "-"
    used = None
    if view.vram_free_mb is not None:
        used = max(0.0, view.vram_total_mb - view.vram_free_mb)
    return f"{_gb(used)}/{_gb(view.vram_total_mb)} GB"


def _disk_amount_cell(view: ResourceView) -> str:
    disk = view.primary_disk
    if disk.used_bytes is None or disk.total_bytes is None:
        return "-"
    # Disk vendors and macOS storage UIs use decimal GB.
    return f"{disk.used_bytes / 1_000_000_000:.0f}/{disk.total_bytes / 1_000_000_000:.0f} GB"


_SHORT_DETAILS = (
    ("ssh key missing", "key missing"),
    ("ssh auth refused", "auth refused"),
    ("host key not trusted", "host key"),
    ("connection refused", "sshd off"),
    ("timeout", "timeout"),
    ("no route to host", "offline"),
    ("hostname did not resolve", "DNS"),
    ("reachable by ssh", "probe failed"),
)

_SHORT_NOTES = {
    "slow to respond; retried": "retried",
    "ram_total from config": "RAM config",
    "vram_total from config": "VRAM config",
}


def _short_detail(detail: str) -> str:
    low = detail.casefold()
    for needle, label in _SHORT_DETAILS:
        if needle in low:
            return label
    return "unreachable" if detail else ""


def _status_cell(view: ResourceView) -> str:
    if not view.reachable:
        return _short_detail(view.detail) or "unreachable"
    bits = []
    if view.is_local:
        bits.append("local")
    if view.notes:
        bits.extend(_SHORT_NOTES.get(note, note[:20]) for note in view.notes)
    return ", ".join(bits)


def _view_row(view: ResourceView, usage_display: str) -> list[str]:
    label = view.name
    if not view.reachable:
        # A dash in every metric column would read as "measured zero".
        return [label, "-", "-", "-", "-", "-", "-", _status_cell(view)]
    if usage_display == "percent":
        ram = _pct_cell(view.ram_used_pct)
        vram = _pct_cell(view.vram_used_pct)
        disk = _pct_cell(view.primary_disk.used_pct)
    else:
        ram = _mark_amount(_ram_amount_cell(view), view.ram_used_pct)
        vram = _mark_amount(_vram_amount_cell(view), view.vram_used_pct)
        disk = _mark_amount(_disk_amount_cell(view), view.primary_disk.used_pct)
    return [
        label,
        _pct_cell(view.cpu_busy_pct),
        _load_cell(view),
        ram,
        _pct_cell(view.gpu_util_pct),
        vram,
        disk,
        _status_cell(view),
    ]


def _table_widths(rows: list[list[str]]) -> tuple[int, ...]:
    return tuple(max(len(row[index]) for row in rows) for index in range(len(HEADERS)))


def _format_row(row: list[str], widths: tuple[int, ...]) -> str:
    # Right pad all but the last column; trailing spaces are noise.
    cells = [row[index].ljust(widths[index]) for index in range(len(row) - 1)]
    return "  ".join([*cells, row[-1]]).rstrip()


def _footnotes(views: list[ResourceView]) -> list[str]:
    notes = []
    has_posix_load = any(view.load_per_core is not None for view in views)
    has_windows_queue = any(
        view.processor_queue_per_core is not None for view in views
    )
    if has_posix_load and has_windows_queue:
        notes.append("LOAD: x=1-min run queue/core; q=ready waiters/core")
    elif has_posix_load:
        notes.append("LOAD = 1-min demand/core; not CPU%")
    elif has_windows_queue:
        notes.append("LOAD: q=ready waiters/core; not CPU%")
    if any(_has_alert(view) for view in views):
        notes.append("* alert: usage >=85%; LOAD >=1.0x/q")
    return notes


class IncrementalTable:
    """Stable-width append-only renderer for interactive completion-order rows."""

    def __init__(self, device_labels: list[str], usage_display: str = "percent") -> None:
        if usage_display not in USAGE_DISPLAYS:
            raise ValueError(
                f"fleet.resources.usage_display must be one of {sorted(USAGE_DISPLAYS)}, "
                f"not {usage_display!r}"
            )
        self.usage_display = usage_display
        if usage_display == "percent":
            widest = ["", "100%", "9999.9x", "100%", "100%", "100%", "100%", ""]
        else:
            widest = [
                "",
                "100%",
                "9999.9x",
                "9999/9999 GB",
                "100%",
                "9999/9999 GB",
                "99999/99999 GB",
                "",
            ]
        widest[0] = max([*device_labels, HEADERS[0]], key=len)
        self.widths = _table_widths([list(HEADERS), widest])

    def header(self) -> str:
        heading = _format_row(list(HEADERS), self.widths)
        rule = _format_row(["-" * width for width in self.widths], self.widths)
        return f"{heading}\n{rule}"

    def row(self, view: ResourceView) -> str:
        return _format_row(_view_row(view, self.usage_display), self.widths)

    def footer(self, views: list[ResourceView]) -> str:
        notes = _footnotes(views)
        return "\n".join(["", *notes]) if notes else ""


def render_table(views: list[ResourceView], usage_display: str = "percent") -> str:
    if usage_display not in USAGE_DISPLAYS:
        raise ValueError(
            f"fleet.resources.usage_display must be one of {sorted(USAGE_DISPLAYS)}, "
            f"not {usage_display!r}"
        )
    rows = [list(HEADERS), *(_view_row(view, usage_display) for view in views)]
    widths = _table_widths(rows)
    lines = []
    for index, row in enumerate(rows):
        lines.append(_format_row(row, widths))
        if index == 0:
            lines.append(_format_row(["-" * width for width in widths], widths))

    footnotes = _footnotes(views)
    if footnotes:
        lines.append("")
        lines.extend(footnotes)
    return "\n".join(lines)


def to_dict(view: ResourceView) -> dict:
    """JSON payload for one device. Keys mirror DeviceSnapshot where they overlap."""
    return {
        "name": view.name,
        "reachable": view.reachable,
        "detail": view.detail,
        "os": view.os,
        "hostname": view.hostname,
        "chip": view.chip,
        "is_local": view.is_local,
        "cpu_busy_pct": view.cpu_busy_pct,
        "cpu_count": view.cpu_count,
        "load1": view.load1,
        "load_per_core": view.load_per_core,
        "processor_queue_length": view.processor_queue_length,
        "processor_queue_per_core": view.processor_queue_per_core,
        "ram_free_mb": view.ram_free_mb,
        "ram_total_mb": view.ram_total_mb,
        "ram_used_pct": view.ram_used_pct,
        "gpu_name": view.gpu_name,
        "gpu_util_pct": view.gpu_util_pct,
        "gpu_unified": view.gpu_unified,
        "vram_free_mb": view.vram_free_mb,
        "vram_total_mb": view.vram_total_mb,
        "vram_used_pct": view.vram_used_pct,
        "disk": {
            "mount": view.primary_disk.mount,
            "total_bytes": view.primary_disk.total_bytes,
            "available_bytes": view.primary_disk.available_bytes,
            "used_bytes": view.primary_disk.used_bytes,
            "used_pct": view.primary_disk.used_pct,
            "semantics": view.primary_disk.semantics,
            "source": view.primary_disk.source,
            "status": view.primary_disk.status,
            "detail": view.primary_disk.detail,
        },
        "notes": list(view.notes),
    }
