#!/usr/bin/env python3
"""Later-local source-loss gate for one configured remrun SSH target.

Run from the project root after applying the patch. The gate waits for the
positive durable acknowledgement, kills that source controller process, resumes
from the same controller state, verifies exact exit/log/pullback behavior, and
then performs a small ordinary run to reconcile deletion of its marker.
"""
from __future__ import annotations

import argparse
import json
import queue
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

EXPECTED_EXIT = 23


def _drain(stream, sink: list[str], events: queue.Queue[dict]) -> None:  # noqa: ANN001
    for line in iter(stream.readline, ""):
        sink.append(line)
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            events.put(payload)
    stream.close()


def _cli(*args: str) -> list[str]:
    return [sys.executable, "-m", "remrun.cli", *args]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("device")
    parser.add_argument(
        "--remote-python",
        default="python3",
        help="remote interpreter command (use python for ssh-powershell)",
    )
    parser.add_argument("--sleep-seconds", type=float, default=20.0)
    parser.add_argument("--ack-timeout", type=float, default=60.0)
    args = parser.parse_args()

    nonce = uuid.uuid4().hex
    # `.remrun-*` is a global transfer exclusion, so use an ordinary project
    # filename or the gate would falsely report a missing pullback.
    marker = Path(f"durable-native-gate-{nonce}.json")
    script = (
        "import json,pathlib,sys,time; "
        f"print('native-start-{nonce}', flush=True); "
        f"time.sleep({args.sleep_seconds!r}); "
        f"pathlib.Path({str(marker)!r}).write_text(json.dumps({{'nonce':{nonce!r},'exit':23}})); "
        f"print('native-finish-{nonce}', flush=True); "
        "sys.exit(23)"
    )
    command = _cli(
        "run", "--json", "--durable", args.device, "--",
        args.remote_python, "-c", script,
    )
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None and process.stderr is not None
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    events: queue.Queue[dict] = queue.Queue()
    threads = [
        threading.Thread(target=_drain, args=(process.stdout, stdout_lines, events), daemon=True),
        threading.Thread(target=_drain, args=(process.stderr, stderr_lines, events), daemon=True),
    ]
    for thread in threads:
        thread.start()

    run_id = None
    acknowledged = False
    deadline = time.monotonic() + args.ack_timeout
    while time.monotonic() < deadline and not acknowledged:
        if process.poll() is not None:
            break
        try:
            event = events.get(timeout=0.2)
        except queue.Empty:
            continue
        if event.get("event") == "run_id":
            run_id = event.get("run_id")
        if event.get("event") == "durable_acknowledged":
            acknowledged = True
            run_id = event.get("run_id") or run_id

    if not acknowledged or not isinstance(run_id, str) or not run_id:
        process.kill()
        process.wait(timeout=10)
        sys.stderr.write("positive durable acknowledgement was not observed\n")
        sys.stderr.write("".join(stderr_lines))
        return 1

    # Simulate source sleep/network disappearance after the positive acknowledgement.
    process.kill()
    process.wait(timeout=10)

    resumed = subprocess.run(
        _cli("resume", run_id, "--json"),
        text=True,
        capture_output=True,
        check=False,
        timeout=max(120.0, args.sleep_seconds + 90.0),
    )
    if resumed.returncode != EXPECTED_EXIT:
        sys.stderr.write(
            f"resume returned {resumed.returncode}, expected {EXPECTED_EXIT}\n"
        )
        sys.stderr.write(resumed.stderr)
        return 1
    if not marker.is_file():
        sys.stderr.write(f"pullback marker missing after resume: {marker}\n")
        return 1
    payload = json.loads(marker.read_text(encoding="utf-8"))
    if payload != {"nonce": nonce, "exit": EXPECTED_EXIT}:
        sys.stderr.write(f"marker mismatch: {payload!r}\n")
        return 1
    if f"native-finish-{nonce}" not in resumed.stdout:
        sys.stderr.write("recovered stdout omitted the completion marker\n")
        return 1

    marker.unlink()
    cleanup = subprocess.run(
        _cli(
            "run", args.device, "--", args.remote_python, "-c", "pass"
        ),
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )
    if cleanup.returncode != 0:
        sys.stderr.write("marker deletion reconciliation failed\n")
        sys.stderr.write(cleanup.stderr)
        return 1

    print(json.dumps({"status": "PASS", "device": args.device, "run_id": run_id}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
