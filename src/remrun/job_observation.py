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
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePath, PureWindowsPath
from typing import Iterator

_SAFE = re.compile(r"[^A-Za-z0-9._:@+-]+")


def job_observation_enabled(device: object) -> bool:
    """Return whether runs on ``device`` register in its active-job registry.

    A target opts in with ``observe_jobs = true`` once its native gate passes. The
    decision is the target's, so every controller sharing the device configuration
    observes the same set of targets.
    """
    return getattr(device, "observe_jobs", False) is True


def local_device(config: object) -> object | None:
    """Return the configured device that is this machine, matched by short hostname."""
    controller = (platform.node().split(".", 1)[0] or "").casefold()
    if not controller:
        return None
    for device in getattr(config, "devices", {}).values():
        if device.name.casefold() == controller and device.kind != "local-sim":
            return device
    return None


def device_registry_root(device: object | None) -> Path:
    """Return the active-job registry root of ``device`` as seen on that device.

    It is the configured ``state_root`` other controllers query over SSH, so a job
    registered locally and a job launched there remotely land in one registry.
    """
    from . import _job_observer
    from .state import default_state_root

    raw = getattr(device, "state_root", "") if device is not None else ""
    return _job_observer._state_root(raw) if raw else default_state_root()


def local_registry_root(config: object) -> Path:
    """Return this machine's active-job registry root."""
    return device_registry_root(local_device(config))


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


def observed_project_id(project: object) -> str:
    """Return the bounded project text the registry stores for ``project``."""
    return _safe_text(project, fallback="project", limit=128)


def observed_job_id(job_id: object) -> str:
    """Return the bounded job id the registry stores for ``job_id``."""
    return _safe_text(job_id, fallback="job", limit=128)


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
            job_id=observed_job_id(job_id),
            project=observed_project_id(project),
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


@contextmanager
def observe_controller_operation(
    observation: JobObservation,
    root: Path,
) -> Iterator[bool]:
    """Register one controller-orchestrated operation for ``fleet jobs``.

    The exact initiating process is the liveness witness.  Registration is
    optional and fail-open: observability must never change the operation's
    result.
    """
    from . import _job_observer  # local imports avoid observer/transport cycles

    token: str | None = None
    try:
        process = _job_observer._processes().get(os.getpid())
        if process is None or process.identity is None:
            raise RuntimeError("controller process identity is unavailable")
        token = _job_observer._register(
            root,
            observation.payload(),
            process,
            owner_kind="controller_operation",
            owner_key=f"{process.pid}:{process.identity}",
            owner_process=process,
            witness=process,
        )
    except Exception as exc:
        detail = _safe_text(exc, fallback="observer unavailable", limit=240)
        print(
            f"remrun observation unavailable; operation ran unobserved: {detail}",
            file=sys.stderr,
        )
        yield False
        return

    try:
        yield True
    finally:
        if token is not None and not _job_observer._unregister(root, token):
            print(
                "remrun observation cleanup unavailable; the stale operation row "
                "will be hidden after its exact controller process ends",
                file=sys.stderr,
            )


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
        result.memory_reservation,
    )
