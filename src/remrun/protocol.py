from __future__ import annotations

from typing import Any

from . import __version__


def build_capabilities_document() -> dict[str, Any]:
    """Return the explicit public protocol manifest.

    Capability promotion is deliberate: implementation or configuration discovery must never
    change this document.
    """
    return {
        "schema": "remrun.capabilities",
        "version": 1,
        "protocol": {"major": 1, "minor": 0},
        "package_version": __version__,
        "documents": {
            "requests": [],
            "receipts": [],
            "errors": [{"schema": "remrun.error", "version": 1}],
        },
        "features": {
            "capabilities": "stable",
            "task_preparation": "unavailable",
            "target_fenced_admission": "unavailable",
            "durable_fleet_launch": "unavailable",
            "service_sessions": "unavailable",
        },
        "coordination": {
            "scope": "controller_local_queue",
            "accepted_work_recovery": "origin_controller_only",
            "unaccepted_queue_recovery": "origin_controller_only",
            "ambiguous_acceptance_retry_scope": "none",
            "global_ordering": False,
            "global_idempotency": False,
            "cross_target_exactly_once": False,
        },
    }


def build_error_document(message: str) -> dict[str, Any]:
    detail = message.strip() or "internal failure"
    return {
        "schema": "remrun.error",
        "version": 1,
        "code": "internal_error",
        "message": detail[:500],
        "retryable": False,
    }


def format_capabilities_human(document: dict[str, Any]) -> str:
    protocol = document["protocol"]
    lines = [
        f"remrun {document['package_version']}",
        f"protocol: {protocol['major']}.{protocol['minor']}",
        "features:",
    ]
    lines.extend(
        f"  {name}: {status}" for name, status in sorted(document["features"].items())
    )
    lines.extend(
        (
            "coordination:",
            f"  scope: {document['coordination']['scope']}",
            "  global ordering: "
            f"{str(document['coordination']['global_ordering']).lower()}",
            "  global idempotency: "
            f"{str(document['coordination']['global_idempotency']).lower()}",
            "  cross-target exactly-once: "
            f"{str(document['coordination']['cross_target_exactly_once']).lower()}",
        )
    )
    return "\n".join(lines) + "\n"
