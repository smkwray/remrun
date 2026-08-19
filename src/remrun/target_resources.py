"""Internal controller client for target-local resource admission.

RWO 5 deliberately does not connect this module to fleet execution or advertise it
through the public capabilities document. Later consumers must compose this client
with durable target acceptance before enabling production launch paths.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any

from .config import RemrunConfig
from .frame import MAGIC, FrameError, decode_frame, encode_frame
from .runner_client import RunnerClientError, RunnerInfo, ensure_versioned_runner, runner_rpc
from .transport import BaseTransport, make_transport

POLICY_SCHEMA = "remrun.target-resource-policy"
RECEIPT_SCHEMA = "remrun.target-resource-receipt"
OWNER_REQUEST_SCHEMA = "remrun.target-resource-owner-request"


class TargetResourceError(RuntimeError):
    pass


def canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def policy_digest(document: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(document)).hexdigest()


def _receipt(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TargetResourceError("target resource receipt is not an object")
    if value.get("schema") != RECEIPT_SCHEMA or value.get("version") != 1:
        raise TargetResourceError("target resource receipt schema/version mismatch")
    required = {
        "status", "allocation_id", "operation_id", "request_sha256", "resource_keys",
        "policy_generation", "policy_digest", "fence", "target_boot_id",
        "reservation_expires_mono_ns", "state", "command_start_state", "owner",
        "terminal_reason", "updated_at_ns",
    }
    if not required <= set(value):
        raise TargetResourceError("target resource receipt is incomplete")
    return dict(value)


@dataclass(frozen=True)
class TargetReservation:
    receipt: dict[str, Any]
    token: str

    @property
    def allocation_id(self) -> str:
        return str(self.receipt["allocation_id"])

    @property
    def fence(self) -> int:
        return int(self.receipt["fence"])


def _read_stream_frame(stream) -> tuple[dict[str, Any], bytes]:
    first = stream.readline(256)
    if not first or len(first) >= 256 or not first.endswith(b"\n"):
        raise TargetResourceError("target owner stream header is missing or oversized")
    parts = first[:-1].split(b" ")
    if len(parts) != 3 or parts[0] != MAGIC:
        raise TargetResourceError("target owner stream frame magic is invalid")
    try:
        header_len = int(parts[1])
        body_len = int(parts[2])
    except ValueError as exc:
        raise TargetResourceError("target owner stream frame lengths are invalid") from exc
    if header_len < 0 or header_len > (1 << 20) or body_len < 0 or body_len > (1 << 28):
        raise TargetResourceError("target owner stream frame length exceeds bound")
    remaining = header_len + body_len
    chunks = []
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            raise TargetResourceError("target owner stream frame is truncated")
        chunks.append(chunk)
        remaining -= len(chunk)
    try:
        return decode_frame(first + b"".join(chunks))
    except FrameError as exc:
        raise TargetResourceError(f"invalid target owner stream frame: {exc}") from exc


def _read_stream_frame_with_timeout(
    process: subprocess.Popen, timeout: float | None
) -> tuple[dict[str, Any], bytes]:
    assert process.stdout is not None
    if timeout is None:
        return _read_stream_frame(process.stdout)
    result = []
    failure = []

    def read() -> None:
        try:
            result.append(_read_stream_frame(process.stdout))
        except BaseException as exc:
            failure.append(exc)

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    reader.join(timeout)
    if reader.is_alive():
        process.kill()
        process.wait(timeout=10)
        raise subprocess.TimeoutExpired(process.args, timeout)
    if failure:
        raise failure[0]
    return result[0]


@dataclass
class TargetOwnerHandle:
    process: subprocess.Popen
    request_sha256: str
    claim_receipt: dict[str, Any]

    def finish(self, *, timeout: float | None = None) -> dict[str, Any]:
        stdout, stderr = self.process.communicate(timeout=timeout)
        try:
            header, payload = decode_frame(stdout)
            response = json.loads(payload.decode("utf-8"))
        except (FrameError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            detail = stderr.decode("utf-8", "replace").strip()
            raise TargetResourceError(
                f"target owner terminal response is invalid: {detail or exc}"
            ) from exc
        if header.get("kind") != "target-resource-owner-response" \
                or header.get("request_sha256") != self.request_sha256:
            raise TargetResourceError("target owner terminal response identity mismatch")
        return_code = self.process.wait(timeout=timeout)
        if return_code not in (0, 1) or not isinstance(response, dict) \
                or not response.get("ok"):
            raise TargetResourceError(str(response.get("error", "target owner failed")))
        response["claim_receipt"] = _receipt(response.get("claim_receipt"))
        response["receipt"] = _receipt(response.get("receipt"))
        return response


@dataclass
class TargetResourceClient:
    config: RemrunConfig
    device_name: str
    info: RunnerInfo
    transport: BaseTransport
    state_root: str

    @classmethod
    def connect(
        cls, config: RemrunConfig, device_name: str, *, install: bool = True
    ) -> "TargetResourceClient":
        try:
            info = ensure_versioned_runner(config, device_name, install=install)
        except RunnerClientError as exc:
            raise TargetResourceError(str(exc)) from exc
        transport = make_transport(config.devices[device_name])
        state_root = transport.expand_remote(config.devices[device_name].state_root)
        return cls(config, device_name, info, transport, state_root)

    def _rpc(
        self, operation: str, body: dict[str, Any] | None = None, *, rpc_id: str | None = None
    ) -> dict[str, Any]:
        try:
            return runner_rpc(
                self.transport,
                self.info.installed_path,
                self.state_root,
                operation,
                body,
                rpc_id=rpc_id,
            )
        except RunnerClientError as exc:
            raise TargetResourceError(str(exc)) from exc

    def policy_get(self) -> dict[str, Any]:
        return self._rpc("target_resource_policy_get")

    def policy_install(
        self,
        document: dict[str, Any],
        *,
        expected_generation: int | None,
        expected_digest: str | None,
        rpc_id: str | None = None,
    ) -> dict[str, Any]:
        return self._rpc(
            "target_resource_policy_install",
            {
                "expected_generation": expected_generation,
                "expected_digest": expected_digest,
                "policy_document": document,
                "supplied_digest": policy_digest(document),
            },
            rpc_id=rpc_id,
        )

    def reserve(
        self,
        *,
        allocation_id: str,
        operation_id: str,
        request_sha256: str,
        resource_keys: list[str],
        expected_policy_generation: int,
        expected_policy_digest: str,
        rpc_id: str | None = None,
    ) -> TargetReservation | dict[str, Any]:
        response = self._rpc(
            "target_resource_reserve",
            {
                "allocation_id": allocation_id,
                "operation_id": operation_id,
                "request_sha256": request_sha256,
                "resource_keys": resource_keys,
                "expected_policy_generation": expected_policy_generation,
                "expected_policy_digest": expected_policy_digest,
            },
            rpc_id=rpc_id,
        )
        if response.get("status") != "reserved":
            if "receipt" in response:
                response = {**response, "receipt": _receipt(response["receipt"])}
            return response
        token = response.get("token")
        if not isinstance(token, str) or not token:
            raise TargetResourceError("reservation response omitted its one-time token")
        return TargetReservation(_receipt(response.get("receipt")), token)

    def renew(
        self,
        reservation: TargetReservation,
        *,
        expected_policy_generation: int,
        expected_policy_digest: str,
        rpc_id: str | None = None,
    ) -> dict[str, Any]:
        response = self._rpc(
            "target_resource_renew",
            {
                "allocation_id": reservation.allocation_id,
                "fence": reservation.fence,
                "token": reservation.token,
                "expected_policy_generation": expected_policy_generation,
                "expected_policy_digest": expected_policy_digest,
            },
            rpc_id=rpc_id,
        )
        response["receipt"] = _receipt(response.get("receipt"))
        return response

    def cancel(
        self, reservation: TargetReservation, *, rpc_id: str | None = None
    ) -> dict[str, Any]:
        response = self._rpc(
            "target_resource_cancel",
            {
                "allocation_id": reservation.allocation_id,
                "fence": reservation.fence,
                "token": reservation.token,
            },
            rpc_id=rpc_id,
        )
        response["receipt"] = _receipt(response.get("receipt"))
        return response

    def status(
        self, reservation: TargetReservation, *, rpc_id: str | None = None
    ) -> dict[str, Any]:
        response = self._rpc(
            "target_resource_status",
            {"allocation_id": reservation.allocation_id, "token": reservation.token},
            rpc_id=rpc_id,
        )
        response["receipt"] = _receipt(response.get("receipt"))
        return response

    def owner_run(
        self,
        reservation: TargetReservation,
        argv: list[str],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        handle = self.owner_start(
            reservation, argv, cwd=cwd, env=env, timeout=timeout
        )
        remaining = None
        if timeout is not None:
            remaining = max(0.0, timeout - (time.monotonic() - started))
        return handle.finish(timeout=remaining)

    def owner_start(
        self,
        reservation: TargetReservation,
        argv: list[str],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> TargetOwnerHandle:
        if not argv or not all(isinstance(token, str) and token for token in argv):
            raise TargetResourceError("owner-run argv must be a non-empty token array")
        request = {
            "schema": OWNER_REQUEST_SCHEMA,
            "version": 1,
            "reservation": {
                "allocation_id": reservation.allocation_id,
                "fence": reservation.fence,
                "token": reservation.token,
                "policy_generation": reservation.receipt["policy_generation"],
                "policy_digest": reservation.receipt["policy_digest"],
            },
            "argv": argv,
            "cwd": cwd,
            "env": env or {},
        }
        request_bytes = canonical_json(request)
        request_sha = hashlib.sha256(request_bytes).hexdigest()
        frame = encode_frame(
            {
                "v": 2,
                "kind": "target-resource-owner-request",
                "request_sha256": request_sha,
            },
            request_bytes,
        )
        command = self.transport.runner_stream_argv(
            self.info.installed_path, self.state_root, "resource-owner-run"
        )
        proc = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert proc.stdin is not None and proc.stdout is not None
        proc.stdin.write(frame)
        proc.stdin.close()
        proc.stdin = None
        try:
            header, payload = _read_stream_frame_with_timeout(proc, timeout)
            response = json.loads(payload.decode("utf-8"))
        except (TargetResourceError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            detail = ""
            if proc.stderr is not None and proc.poll() is not None:
                detail = proc.stderr.read().decode("utf-8", "replace").strip()
            raise TargetResourceError(
                f"target owner returned no valid receipt: {detail or exc}"
            ) from exc
        if header.get("kind") == "target-resource-owner-response":
            proc.wait(timeout=10)
            raise TargetResourceError(str(response.get("error", "target owner failed")))
        if header.get("kind") != "target-resource-claim-receipt" \
                or header.get("request_sha256") != request_sha:
            raise TargetResourceError("target owner claim response identity mismatch")
        if not isinstance(response, dict) or not response.get("ok"):
            raise TargetResourceError(str(response.get("error", "target owner claim failed")))
        return TargetOwnerHandle(
            process=proc,
            request_sha256=request_sha,
            claim_receipt=_receipt(response.get("claim_receipt")),
        )


def new_allocation_id() -> str:
    return str(uuid.uuid4())
