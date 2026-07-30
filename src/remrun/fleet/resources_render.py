"""Terminal rendering for `remrun fleet resources`.

Plain ASCII and no colour by default: this output is read in PowerShell, in
Terminal.app, and by agents parsing stdout, and the repo's contract asks for
agent-friendly output. Unmeasured values render as `-`, never as 0.
"""
from __future__ import annotations

from .resources import ResourceView

HEADERS = ("DEVICE", "CPU", "LOAD", "RAM", "GPU", "VRAM", "DISK", "STATUS")
USAGE_DISPLAYS = frozenset({"percent", "amounts"})


def _gb(mb: float | None) -> str:
    if mb is None:
        return "-"
    return f"{mb / 1024.0:.0f}" if mb >= 1024 else f"{mb / 1024.0:.1f}"


def _pct(value: float | None) -> str:
    return "-" if value is None else f"{value:.0f}%"


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


def render_table(views: list[ResourceView], usage_display: str = "percent") -> str:
    if usage_display not in USAGE_DISPLAYS:
        raise ValueError(
            f"fleet.resources.usage_display must be one of {sorted(USAGE_DISPLAYS)}, "
            f"not {usage_display!r}"
        )
    rows = [list(HEADERS)]
    for view in views:
        label = view.name + ("*" if view.is_local else "")
        if view.reachable:
            if usage_display == "percent":
                ram = _pct(view.ram_used_pct)
                vram = _pct(view.vram_used_pct)
                disk = _pct(view.primary_disk.used_pct)
            else:
                ram = _ram_amount_cell(view)
                vram = _vram_amount_cell(view)
                disk = _disk_amount_cell(view)
            rows.append([label, _pct(view.cpu_busy_pct), view.load_label,
                         ram, _pct(view.gpu_util_pct), vram, disk, _status_cell(view)])
        else:
            # A dash in every metric column would read as "measured zero".
            rows.append([label, "-", "-", "-", "-", "-", "-", _status_cell(view)])

    widths = [max(len(r[i]) for r in rows) for i in range(len(HEADERS))]
    lines = []
    for index, row in enumerate(rows):
        # Right pad all but the last column; trailing spaces are noise.
        cells = [row[i].ljust(widths[i]) for i in range(len(row) - 1)] + [row[-1]]
        lines.append("  ".join(cells).rstrip())
        if index == 0:
            lines.append("  ".join("-" * w for w in widths).rstrip())

    footnotes = []
    if any(v.is_local for v in views):
        footnotes.append("* this controller")
    if any(v.load_per_core is not None for v in views):
        footnotes.append("LOAD = 1-min demand/core; not CPU%")
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
