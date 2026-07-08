from __future__ import annotations

import json
import sys
from typing import Any


class Reporter:
    def __init__(self, json_events: bool = False, quiet: bool = False) -> None:
        self.json_events = json_events
        self.quiet = quiet

    def event(self, event: str, **fields: Any) -> None:
        if self.quiet:
            return
        if self.json_events:
            payload = {"event": event, **fields}
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
