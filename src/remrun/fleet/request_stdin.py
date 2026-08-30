"""The bounded versioned request document used for inline fleet payloads.

The document is deliberately independent of a task name. The task definition remains the sole
authority for which payload modes are accepted; this module only validates the transport envelope
before preparation starts.
"""
from __future__ import annotations

import json
import sys
from typing import Any, BinaryIO

from .task_contract import verify_id

SCHEMA = "remrun.fleet.request-stdin"
VERSION = 1
# A 1 MiB UTF-8 text limit is above the required 512 KiB Windows payload while keeping both the
# decoded value and the input document bounded. The document limit includes JSON framing/escaping.
MAX_TEXT_BYTES = 1024 * 1024
MAX_DOCUMENT_BYTES = 8 * 1024 * 1024


class RequestStdinError(ValueError):
    """The request stream is absent, malformed, or outside its closed bounds."""


def _no_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RequestStdinError(f"request document repeats field {key!r}")
        result[key] = value
    return result


def _reject_constant(value: str) -> Any:
    raise RequestStdinError(f"request document has invalid constant {value}")


def _read_bounded(stream: BinaryIO) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = stream.read(64 * 1024)
        if not chunk:
            return b"".join(chunks)
        total += len(chunk)
        if total > MAX_DOCUMENT_BYTES:
            raise RequestStdinError(
                f"request document exceeds the {MAX_DOCUMENT_BYTES}-byte limit"
            )
        chunks.append(chunk)


def parse_request_document(raw: bytes) -> dict[str, Any]:
    """Parse and normalize one exact v1 request document.

    The returned value contains only the validated task, spec identity, and text payload. JSON
    duplicate keys, unknown fields, invalid UTF-8, NULs, non-text payloads, and oversized text are
    rejected before the task preparer can inspect filesystem inputs or create a prepared record.
    """
    if not isinstance(raw, bytes):
        raise RequestStdinError("request document must be bytes")
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise RequestStdinError(
            f"request document exceeds the {MAX_DOCUMENT_BYTES}-byte limit"
        )
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise RequestStdinError("request document is not valid UTF-8") from exc
    try:
        document = json.loads(text, object_pairs_hook=_no_duplicate_pairs,
                              parse_constant=_reject_constant)
    except RequestStdinError:
        raise
    except (TypeError, json.JSONDecodeError) as exc:
        raise RequestStdinError("request document is malformed JSON") from exc
    if not isinstance(document, dict):
        raise RequestStdinError("request document must be an object")
    if set(document) != {"schema", "version", "task", "spec_id", "payload"}:
        raise RequestStdinError("request document has unknown or missing fields")
    if document["schema"] != SCHEMA or type(document["version"]) is not int \
            or document["version"] != VERSION:
        raise RequestStdinError("request document schema or version is unsupported")

    task = document["task"]
    if not isinstance(task, str) or not task or task.strip() != task or "\x00" in task:
        raise RequestStdinError("request task must be a trimmed non-empty string")
    spec_id = document["spec_id"]
    try:
        verify_id(spec_id, "request spec_id")
    except Exception as exc:  # keep the transport boundary's public error type closed
        raise RequestStdinError(str(exc)) from exc

    payload = document["payload"]
    if not isinstance(payload, dict) or set(payload) != {"kind", "value"} \
            or payload["kind"] != "text":
        raise RequestStdinError("request payload must be the closed text form")
    value = payload["value"]
    if not isinstance(value, str) or not value or "\x00" in value:
        raise RequestStdinError("request text must be a non-empty string")
    value_bytes = value.encode("utf-8")
    if len(value_bytes) > MAX_TEXT_BYTES:
        raise RequestStdinError(
            f"request text exceeds the {MAX_TEXT_BYTES}-byte limit"
        )
    return {"task": task, "spec_id": spec_id, "text": value}


def read_request_document(stream: BinaryIO | None = None) -> dict[str, Any]:
    """Read one bounded request document from a binary stdin stream."""
    if stream is None:
        stream = getattr(sys.stdin, "buffer", None)
    if stream is None:
        raise RequestStdinError("request stdin is not a binary stream")
    return parse_request_document(_read_bounded(stream))
