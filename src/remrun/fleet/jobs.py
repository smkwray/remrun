"""Cross-controller active-job observation for ``remrun fleet jobs``.

Each configured target is queried concurrently. The source of truth is the
bounded registry below that target's remrun state root, not the controller-local
fleet queue. Failures remain explicit UNKNOWN/UNSUPPORTED results and are never
collapsed into an empty list.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Callable

from .. import _job_observer
from ..models import Device
from ..state import default_state_root
from ..transport import TransportError, make_transport

_QUERY_STATUSES = {"ok", "partial", "unknown", "unsupported"}


@dataclass
class TargetJobsView:
    name: str
    reachable: bool
    status: str
    jobs: list[dict[str, Any]] = field(default_factory=list)
    detail: str = ""
    coverage: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)


def _bounded(value: object, limit: int, fallback: str = "-") -> str:
    text = str(value or "").replace("\n", " ").replace("\r", " ").strip()
    return (text or fallback)[:limit]


def _normalize_job(raw: object, target: str) -> dict[str, Any]:
    if not isinstance(raw, dict) or raw.get("schema") != 1:
        raise ValueError("job row has an invalid schema")
    command = raw.get("command")
    cpu = raw.get("cpu")
    threads = raw.get("threads")
    memory = raw.get("memory")
    if not all(isinstance(item, dict) for item in (command, cpu, threads, memory)):
        raise ValueError("job row is missing metric objects")
    started = raw.get("started_at_unix_ns")
    age = raw.get("age_seconds")
    if isinstance(started, bool) or not isinstance(started, int) or started <= 0:
        raise ValueError("job row has an invalid start timestamp")
    if isinstance(age, bool) or not isinstance(age, (int, float)) or age < 0:
        raise ValueError("job row has an invalid age")

    def optional_number(value: object) -> int | float | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("metric is not numeric")
        return value

    state = _bounded(raw.get("state"), 32, "UNKNOWN").upper()
    status = _bounded(raw.get("observation_status"), 32, "unknown").lower()
    reported_target = _bounded(raw.get("target"), 64, target)
    return {
        **raw,
        "job_id": _bounded(raw.get("job_id"), 128, "job"),
        "project": _bounded(raw.get("project"), 128, "project"),
        "source_controller": _bounded(raw.get("source_controller"), 64, "controller"),
        "target": target,
        "reported_target": reported_target,
        "phase": _bounded(raw.get("phase"), 32, "running"),
        "state": state,
        "observation_status": status,
        "age_seconds": float(age),
        "started_at_unix_ns": started,
        "command": {
            **command,
            "label": _bounded(command.get("label"), 64, "command"),
            "sha256": _bounded(command.get("sha256"), 64, "-"),
        },
        "cpu": {
            **cpu,
            "current_pct_one_logical_cpu": optional_number(
                cpu.get("current_pct_one_logical_cpu")
            ),
            "normalized_host_pct": optional_number(cpu.get("normalized_host_pct")),
        },
        "threads": {
            **threads,
            "current_os_threads": optional_number(threads.get("current_os_threads")),
        },
        "memory": {
            **memory,
            "current_bytes": optional_number(memory.get("current_bytes")),
            "peak_bytes": optional_number(memory.get("peak_bytes")),
        },
    }


def probe_device(
    device: Device,
    *,
    sample_interval: float = 0.2,
    timeout: float = 45.0,
    local: bool = False,
) -> TargetJobsView:
    """Query one target. Never raises and never maps failure to zero jobs."""
    try:
        if local:
            payload = _job_observer._query(default_state_root(), sample_interval)
        else:
            transport = make_transport(device)
            query = getattr(transport, "query_observed_jobs", None)
            if query is None:
                return TargetJobsView(
                    name=device.name,
                    reachable=False,
                    status="unsupported",
                    detail="transport has no target-local active-job query",
                )
            payload = query(sample_interval=sample_interval, timeout=timeout)
        if not isinstance(payload, dict) or payload.get("schema") != 1:
            raise ValueError("observer returned an invalid document")
        status = str(payload.get("status", "unknown")).lower()
        if status not in _QUERY_STATUSES:
            raise ValueError("observer returned an invalid status")
        raw_jobs = payload.get("jobs")
        if not isinstance(raw_jobs, list):
            raise ValueError("observer jobs is not a list")
        errors = payload.get("errors", [])
        if not isinstance(errors, list):
            raise ValueError("observer errors is not a list")
        normalized: list[dict[str, Any]] = []
        validation_errors: list[dict[str, Any]] = []
        for index, raw in enumerate(raw_jobs):
            try:
                normalized.append(_normalize_job(raw, device.name))
            except (TypeError, ValueError) as exc:
                validation_errors.append(
                    {"kind": "invalid_job_row", "detail": f"row {index}: {exc}"}
                )
        if validation_errors and status == "ok":
            status = "partial"
        all_errors = [e for e in errors if isinstance(e, dict)] + validation_errors
        normalized.sort(
            key=lambda row: (
                str(row["project"]),
                int(row["started_at_unix_ns"]),
                str(row["job_id"]),
            )
        )
        detail = ""
        if all_errors:
            detail = _bounded(all_errors[0].get("detail"), 200, status)
        view_payload = dict(payload)
        view_payload["jobs"] = normalized
        view_payload["errors"] = all_errors
        view_payload["status"] = status
        return TargetJobsView(
            name=device.name,
            reachable=status not in {"unknown"},
            status=status,
            jobs=normalized,
            detail=detail,
            coverage=payload.get("coverage") if isinstance(payload.get("coverage"), dict) else {},
            errors=all_errors,
            payload=view_payload,
        )
    except (TransportError, Exception) as exc:  # noqa: BLE001 - report per target
        detail = _bounded(f"{type(exc).__name__}: {exc}", 240, "query failed")
        payload = {
            "schema": 1,
            "status": "unknown",
            "jobs": [],
            "errors": [{"kind": "target_query_failed", "detail": detail}],
            "coverage": {
                "scope": "unknown",
                "mixed_version": True,
                "detail": "target registry could not be queried",
            },
        }
        return TargetJobsView(
            name=device.name,
            reachable=False,
            status="unknown",
            detail=detail,
            coverage=payload["coverage"],
            errors=payload["errors"],
            payload=payload,
        )


def probe_fleet(
    devices: list[Device],
    *,
    max_workers: int = 8,
    sample_interval: float = 0.2,
    timeout: float = 45.0,
    local_names: set[str] | None = None,
    on_event: Callable[[str, str, TargetJobsView | None], None] | None = None,
) -> list[TargetJobsView]:
    """Query targets concurrently while returning views in configured order."""
    if not devices:
        return []
    workers = max(1, min(max_workers, len(devices)))
    lock = Lock()

    def run(device: Device) -> TargetJobsView:
        if on_event:
            with lock:
                on_event("start", device.name, None)
        view = probe_device(
            device,
            sample_interval=sample_interval,
            timeout=timeout,
            local=device.name.casefold() in (local_names or set()),
        )
        if on_event:
            with lock:
                on_event("done", device.name, view)
        return view

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(run, devices))



def flatten_jobs(views: list[TargetJobsView]) -> list[dict[str, Any]]:
    """Return all observed jobs grouped deterministically by project."""
    rows = [job for view in views for job in view.jobs]
    rows.sort(
        key=lambda row: (
            str(row.get("project", "")),
            str(row.get("target", "")),
            int(row.get("started_at_unix_ns", 0) or 0),
            str(row.get("job_id", "")),
        )
    )
    return rows

def to_dict(view: TargetJobsView) -> dict[str, Any]:
    """Preserve the target observer's exact metric semantics and errors."""
    payload = dict(view.payload) if view.payload else {
        "schema": 1,
        "status": view.status,
        "jobs": list(view.jobs),
        "errors": list(view.errors),
        "coverage": dict(view.coverage),
    }
    return {
        "target": view.name,
        "reachable": view.reachable,
        "status": view.status,
        "detail": view.detail,
        **payload,
    }
