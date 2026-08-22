from __future__ import annotations

import json
import sys
from typing import Any, TextIO


def emit_json_document(document: dict[str, Any], *, stream: TextIO | None = None) -> None:
    """Write one deterministic compact JSON document and one trailing newline."""
    target = sys.stdout if stream is None else stream
    print(
        json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        file=target,
        flush=True,
    )


class Reporter:
    def __init__(self, json_events: bool = False, quiet: bool = False, *,
                 event_schema: str | None = None,
                 event_version: int | None = None) -> None:
        self.json_events = json_events
        self.quiet = quiet
        if (event_schema is None) != (event_version is None):
            raise ValueError("event schema and version must be supplied together")
        self.event_schema = event_schema
        self.event_version = event_version

    def event(self, event: str, **fields: Any) -> None:
        if self.quiet:
            return
        if self.json_events:
            payload = {"event": event, **fields}
            if self.event_schema is not None:
                payload = {
                    "schema": self.event_schema,
                    "version": self.event_version,
                    **payload,
                }
            print(json.dumps(payload, sort_keys=True), file=sys.stderr, flush=True)
        else:
            suffix = " ".join(f"{k}={_fmt(v)}" for k, v in fields.items())
            if suffix:
                print(f"remrun: {event} {suffix}", file=sys.stderr, flush=True)
            else:
                print(f"remrun: {event}", file=sys.stderr, flush=True)


def _fmt(value: Any) -> str:
    if isinstance(value, str):
        if " " in value or value == "":
            return json.dumps(value)
        return value
    return json.dumps(value)
