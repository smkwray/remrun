from __future__ import annotations

import io
import json
import sys
from types import SimpleNamespace

import pytest

from remrun.fleet import cli
from remrun.fleet.request_stdin import (
    MAX_DOCUMENT_BYTES,
    MAX_TEXT_BYTES,
    RequestStdinError,
    SCHEMA,
    VERSION,
    parse_request_document,
)


SPEC_ID = "sha256:" + "a" * 64


def _document(text: str, **changes: object) -> bytes:
    document: dict[str, object] = {
        "schema": SCHEMA,
        "version": VERSION,
        "task": "generic.workflow",
        "spec_id": SPEC_ID,
        "payload": {"kind": "text", "value": text},
    }
    document.update(changes)
    return json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _stdin(raw: bytes) -> None:
    sys.stdin = io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8")


def test_request_document_preserves_exact_complex_utf8_bytes() -> None:
    value = "header\nπ — café\t" + "🙂" * 100
    raw = _document(value)
    parsed = parse_request_document(raw)
    assert parsed == {"task": "generic.workflow", "spec_id": SPEC_ID, "text": value}
    assert parsed["text"].encode("utf-8") == value.encode("utf-8")


def test_cli_payload_reader_accepts_the_required_512_kib_text(monkeypatch) -> None:  # noqa: ANN001
    value = "🙂" * (512 * 1024 // len("🙂".encode("utf-8")))
    _stdin(_document(value))
    args = SimpleNamespace(stdin_request=True, text=None, input=None, clipboard=False)
    text, inputs = cli._configured_payload(
        args, {"input": {"mode": "text"}},
        task_name="generic.workflow", spec_id=SPEC_ID,
    )
    assert text == value
    assert text.encode("utf-8") == value.encode("utf-8")
    assert inputs == []


@pytest.mark.parametrize(
    "raw",
    [
        _document("ok", extra=True),
        _document("ok", version=True),
        _document("ok", schema="other.schema"),
        _document("ok", spec_id="not-a-digest"),
        _document("ok", payload={"kind": "files", "value": "ok"}),
        _document("ok", task=" generic.workflow"),
    ],
)
def test_request_document_is_closed_and_fails_before_preparation(raw: bytes) -> None:
    with pytest.raises(RequestStdinError):
        parse_request_document(raw)


def test_request_document_rejects_duplicate_fields() -> None:
    raw = (
        b'{"schema":"' + SCHEMA.encode() + b'","version":1,'
        b'"task":"generic.workflow","task":"other",'
        b'"spec_id":"' + SPEC_ID.encode() + b'",'
        b'"payload":{"kind":"text","value":"ok"}}'
    )
    with pytest.raises(RequestStdinError, match="repeats field"):
        parse_request_document(raw)


def test_request_document_rejects_invalid_utf8_nul_and_oversized_text() -> None:
    with pytest.raises(RequestStdinError, match="valid UTF-8"):
        parse_request_document(b"\xff")
    with pytest.raises(RequestStdinError, match="non-empty string"):
        parse_request_document(_document("\x00"))
    oversized = "x" * (MAX_TEXT_BYTES + 1)
    with pytest.raises(RequestStdinError, match="text exceeds"):
        parse_request_document(_document(oversized))
    with pytest.raises(RequestStdinError, match="document exceeds"):
        parse_request_document(b"x" * (MAX_DOCUMENT_BYTES + 1))


def test_cli_stdin_request_is_consumed_before_task_preparation(monkeypatch) -> None:  # noqa: ANN001
    spec = {"task_name": "generic.workflow", "spec_id": SPEC_ID,
            "definition": {"input": {"mode": "text"}}}
    monkeypatch.setattr(cli, "resolve_tasks", lambda _config: {spec["task_name"]: spec})
    monkeypatch.setattr(cli, "load_config", lambda: object())
    monkeypatch.setattr(
        cli, "prepare_task_jobs", lambda **_kwargs: pytest.fail("malformed input was prepared"),
    )
    _stdin(_document("ok", payload={"kind": "files", "value": "not-text"}))
    args = SimpleNamespace(
        task_name=spec["task_name"], stdin_request=True, text=None, input=None, clipboard=False,
    )
    with pytest.raises(ValueError, match="closed text form"):
        cli._prepare_configured(args, object())


def test_cli_stdin_request_checks_task_and_projected_spec_before_preparation(monkeypatch) -> None:  # noqa: ANN001
    spec = {"task_name": "generic.workflow", "spec_id": SPEC_ID,
            "definition": {"input": {"mode": "text"}}}
    monkeypatch.setattr(cli, "resolve_tasks", lambda _config: {spec["task_name"]: spec})
    monkeypatch.setattr(cli, "load_config", lambda: object())
    monkeypatch.setattr(
        cli, "prepare_task_jobs", lambda **_kwargs: pytest.fail("mismatched input was prepared"),
    )
    args = SimpleNamespace(
        task_name=spec["task_name"], stdin_request=True, text=None, input=None, clipboard=False,
    )
    _stdin(_document("ok", task="other.workflow"))
    with pytest.raises(ValueError, match="task does not match"):
        cli._prepare_configured(args, object())

    _stdin(_document("ok", spec_id="sha256:" + "b" * 64))
    with pytest.raises(ValueError, match="spec_id does not match"):
        cli._prepare_configured(args, object())
