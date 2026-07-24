"""Allowlisted target-side actions with persistent inputs and fail-closed receipts."""
from __future__ import annotations

import hashlib
import json
import re
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .config import RemrunConfig
from .manifest import sha256_file
from .transport import TransportError, make_transport

EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_CONFLICT = 2
EXIT_TRANSFER = 3
EXIT_INFRA = 4

_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_INPUT_TOKEN = re.compile(r"\{input:([^{}]+)\}")


@dataclass(frozen=True)
class ActionResult:
    ok: bool
    exit_code: int
    device: str
    action: str
    action_id: str = ""
    status: str = ""
    inbox: str = ""
    inputs: tuple[str, ...] = ()
    message: str = ""
    stdout_tail: str = ""
    stderr_tail: str = ""

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["inputs"] = list(self.inputs)
        return data


def _fail(device: str, action: str, message: str, exit_code: int = EXIT_INTERNAL,
          *, action_id: str = "", status: str = "error", inbox: str = "") -> ActionResult:
    return ActionResult(False, exit_code, device, action, action_id, status, inbox,
                        message=message)


def _normalized_inputs(paths: list[str]) -> tuple[list[Path], str | None]:
    inputs: list[Path] = []
    names: set[str] = set()
    for raw in paths:
        path = Path(raw).expanduser().resolve()
        if not path.is_file():
            return [], f"action input is not a file: {raw}"
        if path.name in names:
            return [], f"action inputs have duplicate basename: {path.name}"
        names.add(path.name)
        inputs.append(path)
    return inputs, None


def _action_id(name: str, spec: dict[str, Any], inputs: list[Path], key: str | None) -> str:
    if key:
        if not _SAFE_NAME.fullmatch(key):
            raise ValueError("--key must be 1-128 letters, digits, dots, underscores, or hyphens")
        return key
    if not inputs:
        raise ValueError("an action without inputs requires --key")
    digest = hashlib.sha256()
    digest.update(name.encode())
    digest.update(b"\0")
    digest.update(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode())
    for path in inputs:
        digest.update(b"\0")
        digest.update(path.name.encode())
        digest.update(b"\0")
        digest.update(sha256_file(path).encode())
    return digest.hexdigest()[:24]


def _push_json(transport, remote_path: str, payload: dict[str, Any]) -> None:  # noqa: ANN001
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(payload, tmp, sort_keys=True)
        tmp.write("\n")
        local = Path(tmp.name)
    try:
        transport.push_file(local, remote_path)
    finally:
        local.unlink(missing_ok=True)


def _pull_json(transport, remote_path: str) -> dict[str, Any]:  # noqa: ANN001
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        local = Path(tmp.name)
    try:
        transport.pull_file(remote_path, local)
        return json.loads(local.read_text(encoding="utf-8"))
    finally:
        local.unlink(missing_ok=True)


def _render_command(command: list[str], inbox: str, action_id: str,
                    destinations: dict[str, str]) -> list[str]:
    rendered: list[str] = []
    for raw in command:
        token = str(raw).replace("{inbox}", inbox).replace("{action_id}", action_id)

        def replace_input(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in destinations:
                raise ValueError(f"action command names an unstaged input: {name}")
            return destinations[name]

        rendered.append(_INPUT_TOKEN.sub(replace_input, token))
    return rendered


def run_action(config: RemrunConfig, device_name: str, action_name: str, paths: list[str],
               *, key: str | None = None, dry_run: bool = False) -> ActionResult:
    """Stage explicit files to an action inbox and invoke its configured argv once.

    A ``running`` receipt is published before execution. If transport drops after the
    command starts, a retry refuses to guess whether the side effect happened.
    """
    device = config.devices.get(device_name)
    if device is None:
        return _fail(device_name, action_name, f"unknown device: {device_name}")
    if not device.enabled:
        return _fail(device_name, action_name, f"device disabled: {device_name}", EXIT_INFRA)
    if not _SAFE_NAME.fullmatch(action_name):
        return _fail(device_name, action_name, "invalid action name")
    spec = device.actions.get(action_name)
    if not spec:
        return _fail(device_name, action_name,
                     f"action {action_name!r} is not configured for {device_name}")
    inbox_raw = str(spec.get("inbox", "")).strip()
    command_raw = spec.get("command", [])
    if not inbox_raw or not isinstance(command_raw, list) or not command_raw:
        return _fail(device_name, action_name, "action requires inbox and non-empty command")
    inputs, error = _normalized_inputs(paths)
    if error:
        return _fail(device_name, action_name, error)
    try:
        action_id = _action_id(action_name, spec, inputs, key)
    except ValueError as exc:
        return _fail(device_name, action_name, str(exc))
    if dry_run:
        return ActionResult(True, EXIT_OK, device_name, action_name, action_id, "planned",
                            inbox_raw, tuple(str(p) for p in inputs))

    transport = make_transport(device)
    try:
        probe = transport.probe()
        if not probe.reachable:
            return _fail(device_name, action_name, f"{device_name} unreachable: {probe.detail}",
                         EXIT_INFRA, action_id=action_id, status="unreachable")
        inbox = transport.expand_remote(inbox_raw)
        state_root = transport.expand_remote(device.state_root)
        receipt_dir = transport.native_join(state_root, "actions", action_name)
        receipt_path = transport.native_join(receipt_dir, f"{action_id}.json")
        transport.ensure_remote_dir(inbox)
        transport.ensure_remote_dir(receipt_dir)

        if transport.remote_path_exists(receipt_path):
            receipt = _pull_json(transport, receipt_path)
            status = str(receipt.get("status", "unknown"))
            if status == "complete":
                return ActionResult(True, EXIT_OK, device_name, action_name, action_id,
                                    "already-complete", inbox,
                                    tuple(str(p) for p in inputs),
                                    message="matching action receipt already complete")
            return _fail(device_name, action_name,
                         f"existing {status} receipt; refusing an ambiguous retry",
                         EXIT_CONFLICT, action_id=action_id, status=status, inbox=inbox)

        destinations: dict[str, str] = {}
        for path in inputs:
            destination = transport.native_join(inbox, path.name)
            destinations[path.name] = destination
            if transport.remote_path_exists(destination):
                if transport.hash_file(destination) != sha256_file(path):
                    return _fail(device_name, action_name,
                                 f"refusing to overwrite different inbox file: {path.name}",
                                 EXIT_CONFLICT, action_id=action_id, status="conflict", inbox=inbox)
                continue
            transport.push_file(path, destination)

        command = _render_command([str(x) for x in command_raw], inbox, action_id, destinations)
        receipt = {
            "version": 1,
            "status": "running",
            "device": device_name,
            "action": action_name,
            "action_id": action_id,
            "inbox": inbox,
            "inputs": destinations,
            "command": command,
        }
        _push_json(transport, receipt_path, receipt)
        result = transport.exec(
            command,
            cwd=inbox,
            env={
                "REMRUN_ACTION": action_name,
                "REMRUN_ACTION_ID": action_id,
                "REMRUN_ACTION_INBOX": inbox,
            },
            timeout=float(spec["timeout_seconds"]) if spec.get("timeout_seconds") else None,
        )
        receipt.update({
            "status": "complete" if result.exit_code == 0 else "failed",
            "exit_code": result.exit_code,
            "stdout_tail": (result.stdout or "")[-2000:],
            "stderr_tail": (result.stderr or "")[-2000:],
        })
        _push_json(transport, receipt_path, receipt)
        return ActionResult(
            result.exit_code == 0,
            result.exit_code,
            device_name,
            action_name,
            action_id,
            receipt["status"],
            inbox,
            tuple(str(p) for p in inputs),
            stdout_tail=(result.stdout or "")[-500:],
            stderr_tail=(result.stderr or "")[-500:],
        )
    except (TransportError, OSError, ValueError, json.JSONDecodeError) as exc:
        return _fail(device_name, action_name, str(exc), EXIT_INFRA,
                     action_id=action_id, status="ambiguous")
