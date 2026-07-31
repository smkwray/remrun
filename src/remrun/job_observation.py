"""Controller-side metadata for the target-local active-job observer.

Only bounded labels and digests cross the transport seam. Full argv, environment,
and project paths are deliberately excluded from the durable record.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import platform
import re
from dataclasses import dataclass
from pathlib import PurePath, PureWindowsPath

_SAFE = re.compile(r"[^A-Za-z0-9._:@+-]+")


_OBSERVATION_ENABLE_ENV = "REMRUN_FLEET_JOBS_OBSERVE"
_ENABLED_VALUES = frozenset({"1", "true", "yes", "on"})


def active_job_observation_enabled(environ: dict[str, str] | None = None) -> bool:
    """Return whether launch-side fleet-job observation is explicitly enabled.

    Querying remains available regardless of this switch.  The seam is evaluated
    on the controller, before any transport wrapper or target helper is selected,
    so an exact-base landing is genuinely dormant by default.
    """
    source = os.environ if environ is None else environ
    return str(source.get(_OBSERVATION_ENABLE_ENV, "")).strip().lower() in _ENABLED_VALUES


def _safe_text(value: object, *, fallback: str, limit: int) -> str:
    text = str(value or "").strip()
    text = _SAFE.sub("_", text).strip("_.")
    return (text or fallback)[:limit]


def controller_label() -> str:
    """Return a privacy-safe short label for this controller."""
    try:
        raw = platform.node().split(".", 1)[0]
    except OSError:
        raw = "controller"
    return _safe_text(raw, fallback="controller", limit=64)


def command_label(command: list[str], declared: str | None = None) -> str:
    """Derive a short display label without retaining command arguments."""
    if declared:
        return _safe_text(declared, fallback="command", limit=64)
    first = str(command[0]) if command else "command"
    # PurePath on POSIX does not split a Windows path. Prefer the shortest
    # non-empty basename produced by either path grammar.
    candidates = [PurePath(first).name, PureWindowsPath(first).name, first]
    raw = min((candidate for candidate in candidates if candidate), key=len, default="command")
    return _safe_text(raw, fallback="command", limit=64)


def command_digest(command: list[str]) -> str:
    """Digest the exact token vector without storing or displaying it."""
    raw = json.dumps(command, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class JobObservation:
    """Bounded metadata written beside a live target-side process identity."""

    job_id: str
    project: str
    source_controller: str
    target: str
    phase: str
    command_label: str
    command_sha256: str
    member_count: int = 1
    schema: int = 1

    @classmethod
    def for_command(
        cls,
        *,
        job_id: str,
        project: str,
        target: str,
        phase: str,
        command: list[str],
        declared_label: str | None = None,
        source_controller: str | None = None,
        member_count: int = 1,
    ) -> "JobObservation":
        if not command:
            raise ValueError("observed command must not be empty")
        count = int(member_count)
        if count < 1 or count > 100_000:
            raise ValueError("member_count must be in 1..100000")
        return cls(
            job_id=_safe_text(job_id, fallback="job", limit=128),
            project=_safe_text(project, fallback="project", limit=128),
            source_controller=_safe_text(
                source_controller or controller_label(), fallback="controller", limit=64
            ),
            target=_safe_text(target, fallback="target", limit=64),
            phase=_safe_text(phase, fallback="running", limit=32),
            command_label=command_label(command, declared_label),
            command_sha256=command_digest(command),
            member_count=count,
        )

    def payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "job_id": self.job_id,
            "project": self.project,
            "source_controller": self.source_controller,
            "target": self.target,
            "phase": self.phase,
            "command_label": self.command_label,
            "command_sha256": self.command_sha256,
            "member_count": self.member_count,
        }

    def encoded(self) -> str:
        raw = json.dumps(self.payload(), sort_keys=True, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii")


def observation_warning(result, detail: str):  # noqa: ANN001
    """Return an ExecResult-equivalent with a bounded observation warning."""
    from .transport import ExecResult  # local import avoids a module cycle

    message = _safe_text(detail, fallback="observer unavailable", limit=240)
    warning = f"remrun observation unavailable; command ran unobserved: {message}\n"
    return ExecResult(
        result.exit_code,
        result.stdout,
        warning + (result.stderr or ""),
        result.telemetry,
        result.memory_guard,
    )
