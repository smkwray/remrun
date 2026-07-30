from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .models import Device, WorkloadSpec
from .resource_envelope import (
    DeviceResourcePolicy,
    Metric,
    MissingResourcePolicy,
    OfferedResource,
    ResourcePolicy,
    offer_cpu,
    offer_gpu_vram,
    offer_ram,
)
from .resource_probe import GPUResourceSnapshot, ResourceSnapshot
from .state import write_json

MAX_RESOURCE_DOCUMENT_BYTES = 64 * 1024
_RECEIPT_STATUSES = frozenset({"applied", "fallback", "no_op", "blocked"})
_RECEIPT_EVALUATIONS = frozenset({"baseline", "trial", "accepted", "fallback"})
_RESOURCE_KEYS = frozenset({"schema", "default_workload", "workloads"})
_WORKLOAD_KEYS = frozenset(
    {
        "protocol",
        "adapter_id",
        "adapter_version",
        "work_unit",
        "require_envelope",
        "require_receipt",
    }
)
_RECEIPT_KEYS = frozenset(
    {
        "schema",
        "version",
        "run_id",
        "workload",
        "adapter_id",
        "adapter_version",
        "status",
        "evaluation",
        "setting",
        "constraints",
        "work",
        "setting_fingerprint",
        "written_at",
    }
)


class ResourceContextError(ValueError):
    """A selected workload or its versioned document is invalid."""


@dataclass(frozen=True)
class ReceiptValidation:
    status: str
    data: dict[str, Any] | None = None
    detail: str = ""

    @property
    def valid(self) -> bool:
        return self.status == "valid"


def _reject_nonstandard_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant: {value}")


def _require_nonempty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResourceContextError(f"{field_name} must be a non-empty string")
    return value


def _require_version(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ResourceContextError(f"{field_name} must be a positive integer")
    return value


def select_workload(
    project_config: Mapping[str, Any] | None,
    requested: str | None,
) -> WorkloadSpec | None:
    """Resolve an explicit/default workload without touching inert config otherwise.

    A project with no selected or default workload takes the legacy execution path:
    even a stale ``[resources.default]`` example remains inert until adaptation is
    requested. Once selected, schema 1 is strict and the old documented tables get a
    specific migration error rather than a silent reinterpretation.
    """
    config = project_config or {}
    raw_resources = config.get("resources")
    resources = raw_resources if isinstance(raw_resources, Mapping) else {}
    selected = requested
    if selected is None:
        default = resources.get("default_workload")
        if default is None:
            return None
        selected = _require_nonempty_string(default, "resources.default_workload")
    else:
        selected = _require_nonempty_string(selected, "--workload")

    if not isinstance(raw_resources, Mapping):
        raise ResourceContextError(
            f"workload {selected!r} was requested but project [resources] schema 1 is missing"
        )
    if raw_resources.get("schema") != 1 or isinstance(raw_resources.get("schema"), bool):
        legacy = sorted(
            key for key in ("default", "heavy") if isinstance(raw_resources.get(key), Mapping)
        )
        if legacy:
            names = ", ".join(f"[resources.{name}]" for name in legacy)
            raise ResourceContextError(
                f"{names} are inert legacy examples; migrate to "
                "[resources.workloads.\"<name>\"] with schema = 1"
            )
        raise ResourceContextError("resources.schema must be 1 when a workload is selected")
    unknown_resource_keys = sorted(set(raw_resources) - _RESOURCE_KEYS)
    if unknown_resource_keys:
        legacy = [
            key
            for key in unknown_resource_keys
            if key in {"default", "heavy"} and isinstance(raw_resources.get(key), Mapping)
        ]
        if legacy:
            names = ", ".join(f"[resources.{name}]" for name in legacy)
            raise ResourceContextError(
                f"{names} are inert legacy examples; migrate to "
                "[resources.workloads.\"<name>\"] with schema = 1"
            )
        raise ResourceContextError(
            "resources has unknown keys: "
            + ", ".join(str(key) for key in unknown_resource_keys)
        )

    workloads = raw_resources.get("workloads")
    if not isinstance(workloads, Mapping):
        raise ResourceContextError("resources.workloads must be a table")
    raw_spec = workloads.get(selected)
    if not isinstance(raw_spec, Mapping):
        known = ", ".join(sorted(str(name) for name in workloads)) or "(none)"
        raise ResourceContextError(f"unknown workload {selected!r}; configured workloads: {known}")
    unknown_workload_keys = sorted(set(raw_spec) - _WORKLOAD_KEYS)
    if unknown_workload_keys:
        raise ResourceContextError(
            f"resources.workloads.{selected} has unknown keys: "
            + ", ".join(str(key) for key in unknown_workload_keys)
        )

    protocol = raw_spec.get("protocol")
    if protocol != 1 or isinstance(protocol, bool):
        raise ResourceContextError(
            f"resources.workloads.{selected}.protocol must be 1"
        )
    for boolean_field in ("require_envelope", "require_receipt"):
        value = raw_spec.get(boolean_field, False)
        if not isinstance(value, bool):
            raise ResourceContextError(
                f"resources.workloads.{selected}.{boolean_field} must be boolean"
            )

    return WorkloadSpec(
        name=selected,
        protocol=1,
        adapter_id=_require_nonempty_string(
            raw_spec.get("adapter_id"),
            f"resources.workloads.{selected}.adapter_id",
        ),
        adapter_version=_require_version(
            raw_spec.get("adapter_version"),
            f"resources.workloads.{selected}.adapter_version",
        ),
        work_unit=_require_nonempty_string(
            raw_spec.get("work_unit"),
            f"resources.workloads.{selected}.work_unit",
        ),
        require_envelope=raw_spec.get("require_envelope", False),
        require_receipt=raw_spec.get("require_receipt", False),
    )


def build_run_context(
    *,
    run_id: str,
    created_at: str,
    workload: WorkloadSpec,
    receipt_path: str,
    resources: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema": "remrun.run-context",
        "version": 1,
        "run_id": _require_nonempty_string(run_id, "run_id"),
        "created_at": _require_nonempty_string(created_at, "created_at"),
        "workload": {
            "name": workload.name,
            "adapter_id": workload.adapter_id,
            "adapter_version": workload.adapter_version,
            "work_unit": workload.work_unit,
            "receipt": {
                "schema": "remrun.workload-receipt",
                "version": 1,
                "path": _require_nonempty_string(receipt_path, "receipt_path"),
            },
        },
        "resources": dict(resources),
    }


def _metric_document(name: str, metric: Metric) -> dict[str, Any]:
    return {
        name: metric.value,
        "status": metric.status,
        "source": metric.source,
        "confidence": metric.confidence,
    }


def _cpu_static_document(logical: Metric, effective: Metric) -> dict[str, Any]:
    """Represent the schema-v1 CPU pair with conservative shared provenance."""
    statuses = (logical.status, effective.status)
    precedence = (
        "malformed",
        "timeout",
        "unavailable",
        "configured",
        "measured",
        "not_applicable",
    )
    status = next(candidate for candidate in precedence if candidate in statuses)
    sources = list(dict.fromkeys((logical.source, effective.source)))
    confidences = list(dict.fromkeys((logical.confidence, effective.confidence)))
    return {
        "logical_cores": logical.value,
        "effective_cores": effective.value,
        "status": status,
        "source": "+".join(sources),
        "confidence": confidences[0] if len(confidences) == 1 else "mixed",
    }


def _configured_metric(value: int | float | None, source: str) -> Metric:
    if value is None or value <= 0:
        return Metric(None, "unavailable", source, "unknown")
    return Metric(value, "configured", source, "configured")


def _offer_document(name: str, offer: OfferedResource) -> dict[str, Any]:
    result = {name: offer.value, "status": offer.status}
    if offer.reason:
        result["reason"] = offer.reason
    return result


def _policy_document(policy: ResourcePolicy) -> dict[str, Any]:
    if isinstance(policy, MissingResourcePolicy):
        return {"status": "missing"}
    return {
        "mode": policy.mode,
        "cpu_reserve_cores": policy.cpu_reserve_cores,
        "cpu_max_fraction": policy.cpu_max_fraction,
        "ram_reserve_bytes": policy.ram_reserve_bytes,
        "ram_max_fraction": policy.ram_max_fraction,
        "gpu_busy_ceiling_pct": policy.gpu_busy_ceiling_pct,
        "vram_reserve_bytes": policy.vram_reserve_bytes,
        "vram_max_fraction": policy.vram_max_fraction,
        "allow_static_fallback": policy.allow_static_fallback,
        "status": "valid",
    }


def _static_or_measured(
    measured: Metric,
    configured: int | float | None,
    source: str,
) -> Metric:
    return measured if measured.value is not None else _configured_metric(configured, source)


def build_resource_envelope(
    *,
    snapshot: ResourceSnapshot,
    policy: ResourcePolicy,
    device: Device,
    captured_at: str,
) -> dict[str, Any]:
    """Turn a normalized snapshot into the version-1 launch envelope document."""
    configured_cores = device.perf_cores + device.eff_cores
    logical = _static_or_measured(
        snapshot.logical_cores,
        configured_cores or None,
        "devices.toml",
    )
    effective = _static_or_measured(
        snapshot.effective_cores,
        configured_cores or None,
        "devices.toml",
    )
    ram_total = _static_or_measured(
        snapshot.ram_total_bytes,
        int(device.ram_gb * 1024**3) if device.ram_gb > 0 else None,
        "devices.toml",
    )

    allow_fallback = (
        isinstance(policy, DeviceResourcePolicy) and policy.allow_static_fallback
    )
    busy = snapshot.cpu_busy_pct
    if busy.value is None and allow_fallback:
        busy = Metric(0, "configured", "static-fallback", "assumed-idle")
    available = snapshot.ram_available_bytes
    if available.value is None and allow_fallback and ram_total.value is not None:
        available = Metric(
            ram_total.value,
            "configured",
            "static-fallback",
            "configured-capacity",
        )

    gpus = list(snapshot.gpus)
    gpu_kind = snapshot.gpu_kind
    if not gpus and gpu_kind != "unified" and device.vram_gb > 0:
        configured_vram = int(device.vram_gb * 1024**3)
        gpus = [
            GPUResourceSnapshot(
                id="configured",
                name="Configured discrete GPU",
                util_pct=Metric(0, "configured", "static-fallback", "assumed-idle"),
                vram_free_bytes=Metric(
                    configured_vram,
                    "configured",
                    "devices.toml",
                    "configured",
                ),
                vram_total_bytes=Metric(
                    configured_vram,
                    "configured",
                    "devices.toml",
                    "configured",
                ),
            )
        ]
        gpu_kind = "discrete"

    cpu_offer = offer_cpu(effective, busy, policy)
    ram_offer = offer_ram(ram_total, available, policy)
    gpu_offers: list[tuple[GPUResourceSnapshot, OfferedResource]] = []
    if gpu_kind == "discrete":
        for gpu in gpus:
            gpu_offers.append(
                (
                    gpu,
                    offer_gpu_vram(
                        "discrete",
                        gpu.vram_total_bytes,
                        gpu.vram_free_bytes,
                        gpu.util_pct,
                        policy,
                    ),
                )
            )

    if isinstance(policy, MissingResourcePolicy):
        status = "policy_missing"
    elif cpu_offer.status == "unavailable" or ram_offer.status == "unavailable":
        status = "partial"
    else:
        status = "ok"

    static_gpus = []
    live_gpus = []
    offered_gpus = []
    for gpu in gpus:
        static_entry = {"id": gpu.id, "name": gpu.name}
        if gpu_kind == "discrete":
            static_entry.update(
                _metric_document("vram_total_bytes", gpu.vram_total_bytes)
            )
        static_gpus.append(static_entry)
        live_entry = {
            "id": gpu.id,
            **_metric_document("util_pct", gpu.util_pct),
        }
        if gpu_kind == "discrete":
            live_entry.update(_metric_document("vram_free_bytes", gpu.vram_free_bytes))
        live_gpus.append(live_entry)
    for gpu, offer in gpu_offers:
        offered_gpus.append({"id": gpu.id, **_offer_document("vram_bytes", offer)})

    return {
        "schema": "remrun.resource-envelope",
        "version": 1,
        "status": status,
        "probe_status": snapshot.status,
        "static": {
            "cpu": _cpu_static_document(logical, effective),
            "ram": _metric_document("total_bytes", ram_total),
            "gpu": {
                "kind": gpu_kind,
                "devices": static_gpus,
            },
        },
        "live": {
            "captured_at": captured_at,
            "cpu": {
                **_metric_document("busy_pct", busy),
                "sample_interval_ms": snapshot.cpu_sample_interval_ms,
            },
            "ram": _metric_document("available_bytes", available),
            "gpu": live_gpus,
        },
        "policy": _policy_document(policy),
        "offered": {
            "cpu": _offer_document("cores", cpu_offer),
            "ram": _offer_document("bytes", ram_offer),
            "gpu": offered_gpus,
        },
    }


def envelope_meets_required_minimum(resources: Mapping[str, Any]) -> bool:
    offered = resources.get("offered")
    if not isinstance(offered, Mapping):
        return False
    cpu = offered.get("cpu")
    ram = offered.get("ram")
    return (
        resources.get("status") == "ok"
        and isinstance(cpu, Mapping)
        and cpu.get("status") == "usable"
        and isinstance(ram, Mapping)
        and ram.get("status") == "usable"
    )


def write_bounded_json(path: Path, payload: Mapping[str, Any]) -> int:
    encoded = json.dumps(dict(payload), indent=2, sort_keys=True).encode("utf-8")
    if len(encoded) > MAX_RESOURCE_DOCUMENT_BYTES:
        raise ResourceContextError(
            f"resource document is {len(encoded)} bytes; limit is "
            f"{MAX_RESOURCE_DOCUMENT_BYTES}"
        )
    write_json(path, dict(payload))
    return len(encoded)


def validate_workload_receipt(
    path: Path,
    *,
    run_id: str,
    workload: WorkloadSpec,
) -> ReceiptValidation:
    if not path.exists():
        return ReceiptValidation("missing", detail="receipt file was not written")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        return ReceiptValidation("malformed", detail=f"receipt unreadable: {type(exc).__name__}")
    return validate_workload_receipt_bytes(raw, run_id=run_id, workload=workload)


def validate_workload_receipt_bytes(
    raw: bytes,
    *,
    run_id: str,
    workload: WorkloadSpec,
) -> ReceiptValidation:
    if len(raw) > MAX_RESOURCE_DOCUMENT_BYTES:
        return ReceiptValidation(
            "malformed",
            detail=f"receipt exceeds {MAX_RESOURCE_DOCUMENT_BYTES} byte limit",
        )
    try:
        data = json.loads(
            raw.decode("utf-8"),
            parse_constant=_reject_nonstandard_json_constant,
        )
    except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
        return ReceiptValidation("malformed", detail=f"receipt is not valid JSON: {type(exc).__name__}")
    if not isinstance(data, dict):
        return ReceiptValidation("malformed", detail="receipt must be a JSON object")
    unknown_keys = sorted(set(data) - _RECEIPT_KEYS)
    if unknown_keys:
        return ReceiptValidation(
            "malformed",
            detail="receipt has unknown keys: " + ", ".join(unknown_keys),
        )

    expected = {
        "schema": "remrun.workload-receipt",
        "version": 1,
        "run_id": run_id,
        "workload": workload.name,
        "adapter_id": workload.adapter_id,
        "adapter_version": workload.adapter_version,
    }
    for key, value in expected.items():
        if data.get(key) != value or (
            key in {"version", "adapter_version"} and isinstance(data.get(key), bool)
        ):
            return ReceiptValidation(
                "malformed",
                detail=f"receipt {key} does not match the run context",
            )
    if data.get("status") not in _RECEIPT_STATUSES:
        return ReceiptValidation("malformed", detail="receipt status is invalid")
    if data.get("evaluation") not in _RECEIPT_EVALUATIONS:
        return ReceiptValidation("malformed", detail="receipt evaluation is invalid")
    for key in ("setting", "constraints", "work"):
        if not isinstance(data.get(key), dict):
            return ReceiptValidation("malformed", detail=f"receipt {key} must be an object")
    if data["work"].get("unit") != workload.work_unit:
        return ReceiptValidation("malformed", detail="receipt work.unit does not match")
    unknown_work_keys = sorted(set(data["work"]) - {"unit", "count"})
    if unknown_work_keys:
        return ReceiptValidation(
            "malformed",
            detail="receipt work has unknown keys: " + ", ".join(unknown_work_keys),
        )
    count = data["work"].get("count")
    if (
        isinstance(count, bool)
        or not isinstance(count, (int, float))
        or not math.isfinite(count)
        or count < 0
    ):
        return ReceiptValidation("malformed", detail="receipt work.count must be non-negative")
    for key in ("setting_fingerprint", "written_at"):
        if not isinstance(data.get(key), str) or not data[key]:
            return ReceiptValidation("malformed", detail=f"receipt {key} must be non-empty")
    if not data["setting_fingerprint"].startswith("sha256:"):
        return ReceiptValidation(
            "malformed",
            detail="receipt setting_fingerprint must start with sha256:",
        )
    return ReceiptValidation("valid", data=data)
