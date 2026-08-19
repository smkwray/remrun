"""Closed generic result protocol for configured fleet workers."""
from __future__ import annotations

import math
import json
import re
from pathlib import PurePosixPath
from typing import Any, Mapping

from .task_contract import TaskContractError, verify_id


class ResultProtocolError(ValueError):
    """A worker result is malformed, incoherent, or attributed to other work."""


_OUTCOMES = {"succeeded", "review", "failed"}
_DISPOSITIONS = {"none", "final", "retry", "elsewhere", "once-elsewhere", "retry-later"}
_PUBLICATIONS = {"none", "produced", "reused"}
_RESOURCES = {"none", "memory", "scratch"}
_ITEM_FIELDS = {
    "job_id", "prepared_id", "index", "outcome", "disposition", "retry_after_s",
    "publication", "work_performed", "outputs", "companion", "message", "failure_code",
    "resource", "work_units", "elapsed_s", "details",
}
_REQUIRED_ITEM_FIELDS = _ITEM_FIELDS - {"details"}
_TOKEN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")


def _finite(value: Any, field: str, *, positive: bool = False) -> float:
    if type(value) not in (int, float) or not math.isfinite(float(value)):
        raise ResultProtocolError(f"{field} must be finite")
    result = float(value)
    if (positive and result <= 0) or (not positive and result < 0):
        raise ResultProtocolError(f"{field} must be {'positive' if positive else 'nonnegative'}")
    return result


def _result_id(value: Any, field: str) -> str:
    try:
        return verify_id(value, field)
    except TaskContractError as exc:
        raise ResultProtocolError(str(exc)) from exc


def _safe_path(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or "\\" in value:
        raise ResultProtocolError(f"{field} must be a canonical safe relative path")
    raw_parts = value.split("/")
    path = PurePosixPath(value)
    if (path.is_absolute() or any(part in {"", ".", ".."} for part in raw_parts)
            or path.as_posix() != value):
        raise ResultProtocolError(f"{field} must be a canonical safe relative path")
    return value


def _validate_item(item: Any, expected: Mapping[str, Any], completion: Mapping[str, Any]) -> dict:
    if (not isinstance(item, dict) or not _REQUIRED_ITEM_FIELDS.issubset(item)
            or not set(item).issubset(_ITEM_FIELDS)):
        raise ResultProtocolError("result item has unknown or missing fields")
    item = dict(item)
    item.setdefault("details", {})
    if item["job_id"] != expected["job_id"]:
        raise ResultProtocolError("result job_id does not match expected work")
    _result_id(item["prepared_id"], "result prepared_id")
    if item["prepared_id"] != expected["prepared_id"]:
        raise ResultProtocolError("result prepared_id does not match expected work")
    if type(item["index"]) is not int or item["index"] < 0 or item["index"] != expected["index"]:
        raise ResultProtocolError("result item index does not match expected work")
    outcome = item["outcome"]
    disposition = item["disposition"]
    publication = item["publication"]
    resource = item["resource"]
    if (not isinstance(outcome, str) or outcome not in _OUTCOMES
            or not isinstance(disposition, str) or disposition not in _DISPOSITIONS):
        raise ResultProtocolError("result outcome or disposition is invalid")
    if (not isinstance(publication, str) or publication not in _PUBLICATIONS
            or publication not in completion["allowed_publication"]):
        raise ResultProtocolError("result publication is not allowed")
    if not isinstance(resource, str) or resource not in _RESOURCES:
        raise ResultProtocolError("result resource is invalid")
    if type(item["work_performed"]) is not bool:
        raise ResultProtocolError("result work_performed must be boolean")
    if item["message"] is not None and not isinstance(item["message"], str):
        raise ResultProtocolError("result message must be null or a string")
    if outcome == "failed" and publication != "none":
        # Published bytes make retry/final unsafe: preserve the worker evidence
        # and force this one row into durable human review.
        outcome = item["outcome"] = "review"
        disposition = item["disposition"] = "none"
        item["retry_after_s"] = None
    if outcome in {"succeeded", "review"} and disposition != "none":
        raise ResultProtocolError("successful/review result requires disposition none")
    if outcome == "failed" and (disposition == "none" or not isinstance(item["message"], str)
                                or not item["message"]):
        raise ResultProtocolError("failed result requires a disposition and message")
    retry_after = item["retry_after_s"]
    if disposition == "retry-later":
        _finite(retry_after, "retry_after_s", positive=True)
    elif retry_after is not None:
        raise ResultProtocolError("retry_after_s is permitted only for retry-later")
    outputs = item["outputs"]
    if not isinstance(outputs, list):
        raise ResultProtocolError("result outputs must be a list")
    outputs = [_safe_path(value, "result output") for value in outputs]
    if len(outputs) != len(set(outputs)):
        raise ResultProtocolError("result outputs contain duplicates")
    companion = item["companion"]
    if companion is not None:
        companion = _safe_path(companion, "result companion")
    if publication == "none" and (outputs or companion is not None):
        raise ResultProtocolError("publication none forbids outputs and companion")
    if publication in {"produced", "reused"} and not outputs:
        raise ResultProtocolError("output-bearing publication requires an output")
    reservations = expected.get("reservations") or []
    allowed_stems = {str(row["stem"]) for row in reservations}
    if outputs and not allowed_stems:
        raise ResultProtocolError("result output has no prepared reservation")
    for output in outputs:
        name = PurePosixPath(output).name
        if not any(name == stem or name.startswith(stem + ".") for stem in allowed_stems):
            raise ResultProtocolError("result output does not match a prepared reservation stem")
    if companion is not None:
        name = PurePosixPath(companion).name
        if not allowed_stems or not any(
                name == stem or name.startswith(stem + ".") for stem in allowed_stems):
            raise ResultProtocolError(
                "result companion does not match a prepared reservation stem")
    companion_rule = completion["companion"]
    if companion_rule == "forbidden" and companion is not None:
        raise ResultProtocolError("companion is forbidden")
    if companion_rule == "required" and publication in {"produced", "reused"} and companion is None:
        raise ResultProtocolError("companion is required")
    failure_code = item["failure_code"]
    if failure_code is not None and (not isinstance(failure_code, str)
                                     or _TOKEN.fullmatch(failure_code) is None):
        raise ResultProtocolError("failure_code must be null or a token")
    work_units = item["work_units"]
    if work_units is not None:
        if not isinstance(work_units, dict) or set(work_units) != {"unit", "value"}:
            raise ResultProtocolError("work_units has the wrong fields")
        if (not isinstance(work_units["unit"], str) or not work_units["unit"]
                or expected.get("cost_unit") is None
                or work_units["unit"] != expected.get("cost_unit")):
            raise ResultProtocolError("work_units unit does not match prepared cost unit")
        _finite(work_units["value"], "work_units.value")
    elapsed = item["elapsed_s"]
    if elapsed is not None:
        _finite(elapsed, "elapsed_s")
    details = item["details"]
    if not isinstance(details, dict):
        raise ResultProtocolError("result details must be an object")
    try:
        details_bytes = json.dumps(
            details, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ResultProtocolError("result details must be canonical JSON data") from exc
    if len(details_bytes) > 65536:
        raise ResultProtocolError("result details exceed the byte limit")
    return item


def validate_result_envelope(envelope: Any, *, batch_id: str, spec_id: str,
                             adapter_id: str, expected_items: list[Mapping[str, Any]],
                             completion: Mapping[str, Any]) -> list[dict]:
    """Validate exact attribution and coherence for one ``ResultEnvelopeV2``."""
    if not isinstance(envelope, dict) or set(envelope) != {
            "schema", "batch_id", "spec_id", "adapter_id", "items"}:
        raise ResultProtocolError("result envelope has unknown or missing fields")
    if (type(envelope["schema"]) is not int or envelope["schema"] != 2
            or not isinstance(envelope["batch_id"], str)
            or envelope["batch_id"] != batch_id):
        raise ResultProtocolError("result schema or batch_id does not match")
    _result_id(envelope["spec_id"], "result spec_id")
    _result_id(envelope["adapter_id"], "result adapter_id")
    if envelope["spec_id"] != spec_id or envelope["adapter_id"] != adapter_id:
        raise ResultProtocolError("result spec_id or adapter_id does not match")
    items = envelope["items"]
    if not isinstance(items, list):
        raise ResultProtocolError("result items must be a list")
    expected_by_id = {row["job_id"]: row for row in expected_items}
    if len(expected_by_id) != len(expected_items):
        raise ResultProtocolError("expected job IDs are not unique")
    if len(items) != len(expected_items):
        raise ResultProtocolError("result item count does not match expected work")
    seen: set[str] = set()
    claimed_paths: set[str] = set()
    out = []
    for item in items:
        job_id = item.get("job_id") if isinstance(item, dict) else None
        if not isinstance(job_id, str) or job_id not in expected_by_id:
            raise ResultProtocolError("result contains an unknown job")
        if job_id in seen:
            raise ResultProtocolError("result contains a duplicate job")
        seen.add(job_id)
        normalized = _validate_item(item, expected_by_id[job_id], completion)
        paths = [*normalized["outputs"]]
        if normalized["companion"] is not None:
            paths.append(normalized["companion"])
        if len(paths) != len(set(paths)):
            raise ResultProtocolError("result paths are not globally unique")
        overlap = claimed_paths.intersection(paths)
        if overlap:
            raise ResultProtocolError(
                f"result paths are not globally unique: {sorted(overlap)[0]}")
        claimed_paths.update(paths)
        out.append(normalized)
    if seen != set(expected_by_id):
        raise ResultProtocolError("result omitted expected work")
    return out
