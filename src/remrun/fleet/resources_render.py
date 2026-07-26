"""Terminal rendering for `remrun fleet resources`.

Plain ASCII and no colour by default: this output is read in PowerShell, in
Terminal.app, and by agents parsing stdout, and the repo's contract asks for
agent-friendly output. Unmeasured values render as `-`, never as 0.
"""
from __future__ import annotations

from .resources import ResourceView

HEADERS = ("DEVICE", "CPU", "LOAD", "RAM", "GPU", "STATUS")


def _gb(mb: float | None) -> str:
    if mb is None:
        return "-"
    return f"{mb / 1024.0:.0f}" if mb >= 1024 else f"{mb / 1024.0:.1f}"


def _pct(value: float | None) -> str:
    return "-" if value is None else f"{value:.0f}%"


def _ram_cell(view: ResourceView) -> str:
    if view.ram_free_mb is None and view.ram_total_mb is None:
        return "-"
    if view.ram_total_mb is None:
        return f"{_gb(view.ram_free_mb)} GB free"
    used = None
    if view.ram_free_mb is not None:
        used = max(0.0, view.ram_total_mb - view.ram_free_mb)
    return f"{_gb(used)}/{_gb(view.ram_total_mb)} GB"


def _gpu_cell(view: ResourceView) -> str:
    """GPU utilization plus VRAM, or the reason there is no VRAM figure."""
    if view.gpu_unified:
        util = _pct(view.gpu_util_pct)
        return f"{util} (unified)" if view.gpu_util_pct is not None else "unified"
    if view.vram_total_mb is None and view.gpu_util_pct is None:
        return "-"
    util = _pct(view.gpu_util_pct)
    if view.vram_total_mb is None:
        return util
    used = None
    if view.vram_free_mb is not None:
        used = max(0.0, view.vram_total_mb - view.vram_free_mb)
    return f"{util}  {_gb(used)}/{_gb(view.vram_total_mb)} GB"


def _status_cell(view: ResourceView) -> str:
    if not view.reachable:
        return view.detail or "unreachable"
    bits = []
    if view.is_local:
        bits.append("local")
    if view.notes:
        bits.append("; ".join(view.notes))
    return ", ".join(bits)


def render_table(views: list[ResourceView]) -> str:
    rows = [list(HEADERS)]
    for view in views:
        label = view.name + ("*" if view.is_local else "")
        if view.reachable:
            rows.append([label, _pct(view.cpu_busy_pct), view.load_label,
                         _ram_cell(view), _gpu_cell(view), _status_cell(view)])
        else:
            # A dash in every metric column would read as "measured zero".
            rows.append([label, "-", "-", "-", "-", _status_cell(view)])

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
        footnotes.append("LOAD = 1-min load average per core; >1x means work is queueing "
                         "(counts blocked threads, so it can be high while CPU looks idle)")
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
        "oversubscribed": view.oversubscribed,
        "ram_free_mb": view.ram_free_mb,
        "ram_total_mb": view.ram_total_mb,
        "ram_used_pct": view.ram_used_pct,
        "gpu_name": view.gpu_name,
        "gpu_util_pct": view.gpu_util_pct,
        "gpu_unified": view.gpu_unified,
        "vram_free_mb": view.vram_free_mb,
        "vram_total_mb": view.vram_total_mb,
        "notes": list(view.notes),
    }
