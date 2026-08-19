"""Controller-side versioned runner installer/RPC client (design Step 3).

This module is opt-in groundwork. Legacy run/plan/sync paths do not import it and
continue piping the helper exactly as before.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path

from .config import RemrunConfig
from .frame import FrameError, decode_frame, encode_frame
from .transport import BaseTransport, TransportError, make_transport

RUNNER_PROTOCOL = 1


class RunnerClientError(RuntimeError):
    pass


@dataclass(frozen=True)
class RunnerInfo:
    device: str
    installed_path: str
    source_sha256: str
    reused: bool
    probe: dict

    def as_dict(self) -> dict:
        return {
            "device": self.device,
            "installed_path": self.installed_path,
            "source_sha256": self.source_sha256,
            "reused": self.reused,
            "probe": self.probe,
        }


def runner_source() -> bytes:
    path = Path(__file__).resolve().parent / "remote" / "runner.py"
    return path.read_bytes()


def runner_rpc(
    transport: BaseTransport,
    runner_path: str,
    state_root: str,
    operation: str,
    body: dict | None = None,
    *,
    rpc_id: str | None = None,
) -> dict:
    request = {"operation": operation, "body": body or {}}
    request_bytes = json.dumps(
        request, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    request_sha = hashlib.sha256(request_bytes).hexdigest()
    identity = rpc_id or str(uuid.uuid4())
    frame = encode_frame({
        "v": 2,
        "kind": "rpc-request",
        "protocol": RUNNER_PROTOCOL,
        "rpc_id": identity,
        "operation": operation,
        "request_sha256": request_sha,
    }, request_bytes)
    raw = transport.runner_rpc(runner_path, state_root, frame)
    try:
        header, response_bytes = decode_frame(raw)
        response = json.loads(response_bytes.decode("utf-8"))
    except (FrameError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RunnerClientError(f"invalid versioned runner response: {exc}") from exc
    if header.get("kind") != "rpc-response" or header.get("v") != 2:
        raise RunnerClientError("versioned runner returned the wrong response type")
    if header.get("rpc_id") != identity or header.get("request_sha256") != request_sha:
        raise RunnerClientError("versioned runner response identity mismatch")
    if not isinstance(response, dict) or not response.get("ok"):
        detail = response.get("error", "unknown runner error") if isinstance(response, dict) \
            else "response is not an object"
        raise RunnerClientError(str(detail))
    return response


def ensure_versioned_runner(
    config: RemrunConfig,
    device_name: str,
    *,
    install: bool,
) -> RunnerInfo:
    if device_name not in config.devices:
        raise RunnerClientError(f"unknown device: {device_name}")
    device = config.devices[device_name]
    if not device.enabled:
        raise RunnerClientError(f"device disabled: {device_name}")
    if not device.state_root:
        raise RunnerClientError(f"device {device_name} has no state_root configured")

    transport = make_transport(device)
    reachability = transport.probe()
    if not reachability.reachable:
        raise RunnerClientError(f"{device_name} unreachable: {reachability.detail}")
    state_root = transport.expand_remote(device.state_root)
    source = runner_source()
    source_sha = hashlib.sha256(source).hexdigest()
    installed_path = transport.native_join(
        state_root, "runner", "v1", "bin", f"remrun-runner-{source_sha}.py")

    exists = transport.remote_path_exists(installed_path)
    reused = False
    if exists:
        try:
            reused = transport.hash_file(installed_path) == source_sha
        except TransportError as exc:
            if not install:
                raise RunnerClientError(f"cannot verify installed runner: {exc}") from exc
    if not reused:
        if not install:
            state = "corrupt" if exists else "missing"
            raise RunnerClientError(
                f"versioned runner {state} on {device_name}; run `remrun runner install {device_name}`"
            )
        transport.install_versioned_runner(source, installed_path, source_sha)
        if not transport.remote_path_exists(installed_path) \
                or transport.hash_file(installed_path) != source_sha:
            raise RunnerClientError("versioned runner failed post-install source verification")

    probe = runner_rpc(
        transport, installed_path, state_root, "participant_probe",
        {"expected_source_sha256": source_sha},
    )
    if probe.get("runner_source_sha256") != source_sha:
        raise RunnerClientError("installed runner executed bytes that do not match the pinned source")
    if RUNNER_PROTOCOL not in probe.get("protocols", []):
        raise RunnerClientError(f"runner does not support protocol {RUNNER_PROTOCOL}")
    return RunnerInfo(
        device=device_name,
        installed_path=installed_path,
        source_sha256=source_sha,
        reused=reused,
        probe=probe,
    )


def enroll_target_key(
    config: RemrunConfig,
    coordinator_device: str,
    target_device: str,
    cluster_id: str,
) -> dict:
    """Relay one prepared authority key directly between two runner processes.

    The controller connects coordinator stdout to target stdin. It observes only
    the target's signed enrollment receipt; the key frame is never read into or
    written by controller code.
    """
    coordinator = ensure_versioned_runner(config, coordinator_device, install=True)
    target = ensure_versioned_runner(config, target_device, install=True)
    coordinator_transport = make_transport(config.devices[coordinator_device])
    target_transport = make_transport(config.devices[target_device])
    coordinator_root = coordinator_transport.expand_remote(
        config.devices[coordinator_device].state_root
    )
    target_root = target_transport.expand_remote(config.devices[target_device].state_root)
    target_id = str(target.probe["device_id"])
    prepared = runner_rpc(
        coordinator_transport, coordinator.installed_path, coordinator_root,
        "authority_target_key_create",
        {"cluster_id": cluster_id, "target_device_id": target_id},
    )
    export_argv = coordinator_transport.runner_stream_argv(
        coordinator.installed_path, coordinator_root, "key-export", [
            cluster_id, target_id, str(prepared["authority_epoch"]), prepared["key_id"],
        ]
    )
    import_argv = target_transport.runner_stream_argv(
        target.installed_path, target_root, "key-import"
    )
    no_window = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    source = None
    destination = None
    try:
        source = subprocess.Popen(
            export_argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            creationflags=no_window,
        )
        destination = subprocess.Popen(
            import_argv, stdin=source.stdout, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, creationflags=no_window,
        )
        if source.stdout is not None:
            source.stdout.close()
        target_stdout, target_stderr = destination.communicate(timeout=60)
        source_returncode = source.wait(timeout=60)
        source_stderr = source.stderr.read() if source.stderr is not None else b""
    except (OSError, subprocess.SubprocessError) as exc:
        for process in (destination, source):
            if process is not None and process.poll() is None:
                process.kill()
        raise RunnerClientError(f"target key relay failed: {exc}") from exc
    if source_returncode != 0:
        raise RunnerClientError(
            "coordinator key export failed: "
            + source_stderr.decode("utf-8", "replace").strip()
        )
    if destination.returncode != 0:
        raise RunnerClientError(
            "target key import failed: "
            + target_stderr.decode("utf-8", "replace").strip()
        )
    try:
        imported = json.loads(target_stdout.decode("utf-8"))
        receipt = imported["receipt"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise RunnerClientError("target key import returned an invalid receipt") from exc
    finalized = runner_rpc(
        coordinator_transport, coordinator.installed_path, coordinator_root,
        "authority_target_key_finalize",
        {"cluster_id": cluster_id, "receipt": receipt},
    )
    if finalized.get("status") != "ENROLLED":
        raise RunnerClientError(f"target key enrollment was not finalized: {finalized}")
    return {
        "coordinator": coordinator.as_dict(), "target": target.as_dict(),
        "prepared": prepared, "imported": imported, "finalized": finalized,
    }
