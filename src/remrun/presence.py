"""Owner presence windows, read from the owner's routing file.

The owner states once, in the ``## Owner presence`` section of a shared routing file
(path: devices.toml ``[scheduler.routing_file]``), when they may be sitting at a device.
``--auto`` keeps heavy jobs off a device inside one of its windows whenever another
reachable device can take them; the device stays a last resort. One list item (``-``, ``*``
or ``+``) per device group, times in the controller's local wall-clock time::

    - macbox, winbox: Wed 04:30-09:30; Sat 00:00-20:00 (owner note)

Prose lines in the section are ignored. Any list item that does not parse (a numbered item
included), or names a device that is not configured, makes the whole policy ``malformed``:
placement then applies no presence rule and says so, rather than acting on a partly read
schedule.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from pathlib import Path

from .config import RemrunConfig, current_os_key, expand_path, scheduler_config

SECTION = "owner presence"
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_LIST_ITEM = re.compile(r"^(?:[-*+]|\d+[.)])\s")
_BULLET = re.compile(r"^[-*+]\s+(?P<names>[^:]+):(?P<rest>.*)$")
_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")
_WINDOW = re.compile(r"^(?P<day>[A-Za-z]{3})\s+(?P<start>\d{2}:\d{2})-(?P<end>\d{2}:\d{2})$")


@dataclass(frozen=True)
class Window:
    day: int  # 0 = Monday, matching datetime.weekday()
    start: time
    end: time | None  # None = midnight at the end of the day ("24:00")

    def contains(self, now: datetime) -> bool:
        clock = now.time()
        return (now.weekday() == self.day and self.start <= clock
                and (self.end is None or clock < self.end))

    def end_on(self, day: datetime) -> datetime:
        if self.end is None:
            return datetime.combine(day.date() + timedelta(days=1), time(0, 0))
        return datetime.combine(day.date(), self.end)


@dataclass(frozen=True)
class PresencePolicy:
    status: str  # valid | absent | unavailable | malformed
    source: str | None = None
    detail: str | None = None
    windows: dict[str, tuple[Window, ...]] = field(default_factory=dict)

    def window_end(self, device: str, now: datetime) -> datetime | None:
        """End of the owner window holding ``now`` on ``device``, or None outside one."""
        if self.status != "valid":
            return None
        ends = [w.end_on(now) for w in self.windows.get(device, ()) if w.contains(now)]
        return max(ends) if ends else None

    def next_window_start(self, device: str, now: datetime) -> datetime | None:
        """Start of the next owner window on ``device`` strictly after ``now`` (within a week)."""
        if self.status != "valid":
            return None
        starts = [
            datetime.combine(day, w.start)
            for offset in range(8)
            for day in [now.date() + timedelta(days=offset)]
            for w in self.windows.get(device, ())
            if w.day == day.weekday() and datetime.combine(day, w.start) > now
        ]
        return min(starts) if starts else None

    def as_dict(self) -> dict[str, object]:
        return {"status": self.status, "source": self.source, "detail": self.detail}


def local_now() -> datetime:
    return datetime.now()


def _clock(text: str, *, end: bool) -> time | None:
    hours, minutes = (int(part) for part in text.split(":"))
    if end and hours == 24 and minutes == 0:
        return None
    if hours > 23 or minutes > 59:
        raise ValueError(f"invalid time {text!r}")
    return time(hours, minutes)


def _parse_windows(text: str) -> tuple[Window, ...]:
    windows = []
    for item in text.split(";"):
        match = _WINDOW.match(item.strip())
        if not match:
            raise ValueError(f"window {item.strip()!r} is not 'Day HH:MM-HH:MM'")
        day = match["day"].casefold()
        if day not in DAYS:
            raise ValueError(f"unknown day {match['day']!r}")
        start = _clock(match["start"], end=False)
        end = _clock(match["end"], end=True)
        if end is not None and end <= start:
            raise ValueError(f"window {item.strip()!r} ends before it starts")
        windows.append(Window(DAYS.index(day), start, end))
    return tuple(windows)


def parse_presence(text: str, *, devices: list[str], source: str) -> PresencePolicy:
    """Parse the ``## Owner presence`` section of ``text`` against configured devices."""
    known = {name.casefold(): name for name in devices}
    windows: dict[str, list[Window]] = {}
    inside = found = False
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if line.startswith("#"):
            inside = line.lstrip("#").strip().casefold() == SECTION
            found = found or inside
            continue
        if not inside or not _LIST_ITEM.match(line):
            continue
        try:
            match = _BULLET.match(line)
            if not match:
                raise ValueError("expected '- DEVICE[, DEVICE]: Day HH:MM-HH:MM; ...'")
            names = [name.strip() for name in match["names"].split(",")]
            for name in names:
                if not _NAME.match(name) or name.casefold() not in known:
                    raise ValueError(f"unknown device {name!r}")
            parsed = _parse_windows(match["rest"].split("(", 1)[0].strip())
        except ValueError as exc:
            return PresencePolicy("malformed", source, f"line {number}: {exc}")
        for name in names:
            windows.setdefault(known[name.casefold()], []).extend(parsed)
    if not found:
        return PresencePolicy("absent", source, "no '## Owner presence' section")
    return PresencePolicy("valid", source, None,
                          {name: tuple(items) for name, items in windows.items()})


def load_presence(config: RemrunConfig) -> PresencePolicy:
    """Read the owner presence policy this controller is configured to follow."""
    paths = scheduler_config(config).get("routing_file")
    if paths is None:
        return PresencePolicy("unavailable", None, "no [scheduler.routing_file] configured")
    if not isinstance(paths, dict) or not all(isinstance(v, str) for v in paths.values()):
        return PresencePolicy("malformed", None,
                              "[scheduler.routing_file] must map OS keys to paths")
    raw = paths.get(current_os_key()) or paths.get("default")
    if not raw:
        return PresencePolicy("unavailable", None,
                              f"no [scheduler.routing_file] entry for {current_os_key()}")
    path: Path = expand_path(raw)
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return PresencePolicy("unavailable", str(path), str(exc))
    return parse_presence(text, devices=list(config.devices), source=str(path))
