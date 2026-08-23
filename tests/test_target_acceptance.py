from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun import _job_observer as observer
from remrun import _durable_runner as durable_runner
from remrun.config import RemrunConfig
from remrun.fleet import cli as fleet_cli
from remrun.fleet import executor as fleet_executor
from remrun.fleet.prepared import (
    RAW_COMMAND_SPEC,
    RAW_COMMAND_SPEC_ID,
    as_fleet_task,
    prepare_raw_command,
)
from remrun.job_observation import JobObservation
from remrun.models import Device
from remrun.transport import LocalSimTransport, TransportError
from remrun.fleet.queue import FleetQueue
from remrun.target_resources import (
    EMPTY_POLICY_DIGEST,
    TargetReservation,
    TargetResourceClient,
    TargetResourceError,
    policy_digest,
)


def _wait_json(path: Path, timeout: float = 5.0) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            time.sleep(0.02)
            continue
        if isinstance(value, dict):
            return value
    raise AssertionError(f"timed out waiting for {path.name}")


def _wait_terminal(root: Path, operation_id: str, token: str) -> dict[str, object]:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        status = durable_runner._status(root, operation_id, token, False)
        if status["state"] in {"complete", "failed"}:
            return status
        time.sleep(0.03)
    raise AssertionError("accepted operation did not become terminal")


def _local_client(tmp_path: Path) -> TargetResourceClient:
    device = Device.from_mapping(
        "LOCAL",
        {
            "kind": "local-sim",
            "os": "posix",
            "project_root": str(tmp_path / "projects"),
            "state_root": str(tmp_path / "state"),
            "cache_root": str(tmp_path / "cache"),
        },
    )
    config = RemrunConfig(
        repo_root=tmp_path,
        defaults={},
        devices={"LOCAL": device},
        project_roots={"default": str(tmp_path / "projects")},
    )
    return TargetResourceClient.connect(config, "LOCAL", install=True)


@pytest.mark.skipif(os.name != "posix", reason="POSIX launch-gate proof")
def test_observer_holds_user_code_until_exact_start_gate(tmp_path: Path) -> None:
    root = tmp_path / "state"
    ready = root / "operation" / "ready.json"
    gate = root / "operation" / "start.json"
    started = root / "operation" / "started.json"
    sentinel = tmp_path / "user-code-ran"
    command = [
        sys.executable,
        "-S",
        "-c",
        f"from pathlib import Path; Path({str(sentinel)!r}).write_text('ran')",
    ]
    metadata = JobObservation.for_command(
        job_id="operation-acceptance-proof",
        project="@fleet",
        source_controller="controller-a",
        target="target-a",
        phase="fleet-worker",
        command=command,
    )
    process = subprocess.Popen(
        [
            sys.executable,
            str(Path(observer.__file__)),
            "run",
            "--state-root",
            str(root),
            "--metadata-b64",
            metadata.encoded(),
            "--ready-file",
            str(ready),
            "--start-gate-file",
            str(gate),
            "--started-file",
            str(started),
            "--",
            *command,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        receipt = _wait_json(ready)
        assert receipt["job_id"] == "operation-acceptance-proof"
        assert receipt["command_sha256"] == metadata.command_sha256
        assert isinstance(receipt.get("owner"), dict)
        time.sleep(0.15)
        assert not sentinel.exists()
        assert not started.exists()

        gate.parent.mkdir(parents=True, exist_ok=True)
        gate.write_text(
            json.dumps(
                {
                    "schema": 1,
                    "job_id": metadata.job_id,
                    "command_sha256": metadata.command_sha256,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        assert process.wait(timeout=10) == 0
        assert sentinel.read_text(encoding="utf-8") == "ran"
        start_receipt = _wait_json(started)
        assert start_receipt["job_id"] == metadata.job_id
        assert start_receipt["command_sha256"] == metadata.command_sha256
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


@pytest.mark.skipif(os.name != "posix", reason="POSIX accepted-operation proof")
def test_durable_acceptance_claims_before_start_and_reconciles_terminal_result(
    tmp_path: Path,
) -> None:
    client = _local_client(tmp_path)
    policy = {
        "schema": "remrun.target-resource-policy",
        "version": 1,
        "generation": 1,
        "resources": [{"key": "pool/gpu", "capacity": 1}],
    }
    digest = policy_digest(policy)
    client.policy_install(
        policy, expected_generation=None, expected_digest=None, rpc_id="install-policy"
    )
    operation_id = "fleet-acceptance-proof"
    request_sha = hashlib.sha256(b"immutable-prepared-batch").hexdigest()
    reserved = client.reserve(
        allocation_id=operation_id,
        operation_id=operation_id,
        request_sha256=request_sha,
        resource_keys=["pool/gpu"],
        expected_policy_generation=1,
        expected_policy_digest=digest,
        rpc_id="reserve-fleet-acceptance-proof",
    )
    assert isinstance(reserved, TargetReservation)

    root = Path(client.state_root).resolve()
    rdir = root / "durable-runs" / operation_id
    marker = tmp_path / "accepted-user-code-ran"
    command = [
        sys.executable,
        "-S",
        "-c",
        f"from pathlib import Path; Path({str(marker)!r}).write_text('ran'); print('done')",
    ]
    metadata = JobObservation.for_command(
        job_id=operation_id,
        project="@fleet",
        source_controller="controller-a",
        target="LOCAL",
        phase="fleet-worker",
        command=command,
    )
    ready = rdir / "observer-ready.json"
    gate = rdir / "start-gate.json"
    started = rdir / "started.json"
    spec = {
        "schema": 1,
        "run_id": operation_id,
        "resume_token": reserved.token,
        "controller": "controller-a",
        "project_id": "@fleet",
        "target": "LOCAL",
        "command_sha256": metadata.command_sha256,
        "argv": [
            sys.executable,
            "-S",
            str(Path(observer.__file__)),
            "run",
            "--state-root",
            str(root),
            "--metadata-b64",
            metadata.encoded(),
            "--ready-file",
            str(ready),
            "--start-gate-file",
            str(gate),
            "--started-file",
            str(started),
            "--",
            *command,
        ],
        "ready_path": str(ready),
        "max_log_bytes": 1024 * 1024,
        "created_at": "2026-08-23T00:00:00Z",
        "acceptance": {
            "schema": 1,
            "runner_path": client.info.installed_path,
            "state_root": client.state_root,
            "operation_id": operation_id,
            "request_sha256": request_sha,
            "reservation": {
                "allocation_id": reserved.allocation_id,
                "fence": reserved.fence,
                "policy_generation": 1,
                "policy_digest": digest,
            },
            "start_gate_path": str(gate),
            "started_path": str(started),
        },
    }

    accepted = durable_runner._launch(
        root, json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()
    )
    assert accepted["acknowledged"] is True
    assert accepted["command_started"] is False
    assert accepted["target_acceptance"]["state"] == "CLAIMED"
    assert accepted["target_acceptance"]["command_start_state"] == "NO"

    terminal = _wait_terminal(root, operation_id, reserved.token)
    assert terminal["state"] == "complete"
    assert terminal["command_started"] is True
    assert terminal["wrapper_exit_code"] == 0
    assert terminal["target_cleanup"]["state"] == "RELEASED"
    assert terminal["target_cleanup"]["command_start_state"] == "YES"
    assert marker.read_text(encoding="utf-8") == "ran"
    result = durable_runner._status(root, operation_id, reserved.token, True)
    assert "done" in base64.b64decode(result["stdout_b64"]).decode()
    assert client.status(reserved)["receipt"]["state"] == "RELEASED"
    replay = durable_runner._launch(
        root, json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()
    )
    assert replay["wrapper_exit_code"] == 0
    assert replay["operation_id"] == operation_id
    durable_runner._cleanup(root, operation_id, reserved.token)


def test_opened_gate_without_started_receipt_remains_maybe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "state"
    run_id = "fleet-missing-started-receipt"
    token = "private-operation-token"
    rdir = root / "durable-runs" / run_id
    ready = rdir / "observer-ready.json"
    gate = rdir / "start-gate.json"
    started = rdir / "started.json"
    command_sha = hashlib.sha256(b"fast-user-command").hexdigest()
    request_sha = hashlib.sha256(b"frozen-request").hexdigest()
    spec = {
        "schema": 1,
        "run_id": run_id,
        "token_sha256": durable_runner._token_hash(token),
        "controller": "controller-a",
        "project_id": "@fleet",
        "target": "TARGET",
        "command_sha256": command_sha,
        "argv": ["synthetic-observer"],
        "ready_path": str(ready),
        "max_log_bytes": 1024,
        "created_at": "2026-08-23T00:00:00Z",
        "acceptance": {
            "schema": 1,
            "runner_path": str(tmp_path / "runner.py"),
            "state_root": str(root),
            "operation_id": run_id,
            "request_sha256": request_sha,
            "reservation": {
                "allocation_id": run_id,
                "fence": 1,
                "policy_generation": 0,
                "policy_digest": EMPTY_POLICY_DIGEST,
            },
            "start_gate_path": str(gate),
            "started_path": str(started),
        },
    }
    rdir.mkdir(parents=True)
    durable_runner._atomic_json(rdir / "spec.json", spec)
    durable_runner._atomic_json(
        rdir / "auth.json",
        {"schema": 1, "run_id": run_id, "token_sha256": spec["token_sha256"]},
    )
    durable_runner._atomic_json(
        durable_runner._claim_auth_path(root, run_id),
        {"schema": 1, "run_id": run_id, "claim_token": token},
    )

    class FastObserver:
        pid = os.getpid()
        stdout = io.BytesIO()
        stderr = io.BytesIO()

        def poll(self):  # noqa: ANN201
            if not ready.exists():
                durable_runner._atomic_json(
                    ready,
                    {
                        "schema": 1,
                        "job_id": run_id,
                        "command_sha256": command_sha,
                        "owner_kind": "posix_pgid",
                        "owner": {
                            "kind": "posix_pgid",
                            "key": str(self.pid),
                            "pid": self.pid,
                            "start_id": "owner-start",
                            "root_pid": self.pid,
                            "root_start_id": "owner-start",
                        },
                    },
                )
            return 0 if gate.exists() else None

        def wait(self):  # noqa: ANN201
            return 0

    monkeypatch.setattr(durable_runner.subprocess, "Popen", lambda *_args, **_kwargs: FastObserver())
    monkeypatch.setattr(durable_runner, "_boot_marker", lambda: "test-boot")
    transitions: list[tuple[str, dict[str, object]]] = []
    command_start_state = "NO"

    def transition(_acceptance, _token, operation, values):  # noqa: ANN001
        nonlocal command_start_state
        transitions.append((operation, dict(values)))
        if operation == "start":
            command_start_state = str(values["state"])
        return {
            "operation_id": run_id,
            "request_sha256": request_sha,
            "state": "RELEASED" if operation == "finish" else "CLAIMED",
            "command_start_state": command_start_state,
        }

    monkeypatch.setattr(durable_runner, "_resource_transition", transition)

    supervisor_code = durable_runner._supervise(root, run_id)
    terminal = json.loads((rdir / "status.json").read_text(encoding="utf-8"))
    assert supervisor_code == 0, terminal
    assert terminal["command_started"] is None
    assert terminal["target_cleanup"]["command_start_state"] == "MAYBE"
    assert transitions == [
        ("claim", {"owner": json.loads(ready.read_text())["owner"]}),
        ("start", {"state": "MAYBE", "explicit_no_start": False}),
        ("finish", {}),
    ]


def test_durable_cleanup_refuses_to_erase_quarantined_target_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "state"
    run_id = "fleet-quarantined-cleanup"
    token = "private-operation-token"
    rdir = root / "durable-runs" / run_id
    request_sha = hashlib.sha256(b"request").hexdigest()
    spec = {
        "schema": 1,
        "run_id": run_id,
        "token_sha256": durable_runner._token_hash(token),
        "controller": "controller-a",
        "project_id": "@fleet",
        "target": "TARGET",
        "command_sha256": hashlib.sha256(b"command").hexdigest(),
        "argv": ["synthetic-observer"],
        "ready_path": str(rdir / "observer-ready.json"),
        "max_log_bytes": 1024,
        "created_at": "2026-08-23T00:00:00Z",
        "acceptance": {
            "schema": 1,
            "runner_path": str(tmp_path / "runner.py"),
            "state_root": str(root),
            "operation_id": run_id,
            "request_sha256": request_sha,
            "reservation": {
                "allocation_id": run_id,
                "fence": 1,
                "policy_generation": 0,
                "policy_digest": EMPTY_POLICY_DIGEST,
            },
            "start_gate_path": str(rdir / "start-gate.json"),
            "started_path": str(rdir / "started.json"),
        },
    }
    rdir.mkdir(parents=True)
    durable_runner._atomic_json(rdir / "spec.json", spec)
    durable_runner._atomic_json(
        rdir / "auth.json",
        {"schema": 1, "run_id": run_id, "token_sha256": spec["token_sha256"]},
    )
    status = durable_runner._base_status(spec, "complete")
    status["command_started"] = False
    status["target_cleanup"] = {
        "operation_id": run_id,
        "request_sha256": request_sha,
        "state": "QUARANTINED",
        "command_start_state": "MAYBE",
    }
    durable_runner._atomic_json(rdir / "status.json", status)
    monkeypatch.setattr(
        durable_runner,
        "_resource_transition",
        lambda *_args, **_kwargs: status["target_cleanup"],
    )

    refreshed = durable_runner._status(root, run_id, token, False)
    assert refreshed["command_started"] is None

    with pytest.raises(durable_runner.DurableError, match="target cleanup"):
        durable_runner._cleanup(root, run_id, token)
    assert rdir.exists()


def test_queue_persists_private_target_credential_and_public_acceptance_receipt(
    tmp_path: Path,
) -> None:
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        queue.db.execute(
            "INSERT INTO jobs(job_id,task_name,state,created_at,updated_at) "
            "VALUES('job-a','command','queued','2099-01-01T00:00:00Z',"
            "'2099-01-01T00:00:00Z')"
        )
        queue.db.commit()
        owner = queue.claim_many(
            ["job-a"], "TARGET", batch_id="batch-a", pool=None,
            lease_until="2099-01-01T01:00:00Z", now="2099-01-01T00:00:00Z",
        )
        assert isinstance(owner, str)
        assert queue.set_batch_state(
            "batch-a", "running", expected_state="leased", owner_token=owner,
            now="2099-01-01T00:00:01Z",
        )
        assert queue.record_target_reservation(
            "batch-a",
            operation_id="fleet-batch-a",
            request_sha256="a" * 64,
            resume_token="private-target-token",
            expected_state="running",
            owner_token=owner,
            now="2099-01-01T00:00:02Z",
        )
        assert queue.record_target_acceptance(
            "batch-a",
            operation_id="fleet-batch-a",
            request_sha256="a" * 64,
            expected_state="running",
            owner_token=owner,
            now="2099-01-01T00:00:03Z",
        )

        public = queue.target_operation("batch-a")
        private = queue.target_operation("batch-a", include_token=True)
        assert public == {
            "schema": 1,
            "operation_id": "fleet-batch-a",
            "request_sha256": "a" * 64,
            "device": "TARGET",
            "reserved_at": "2099-01-01T00:00:02Z",
            "accepted": True,
            "accepted_at": "2099-01-01T00:00:03Z",
        }
        assert private == {**public, "resume_token": "private-target-token"}
        assert "private-target-token" not in json.dumps(queue.get_batch("batch-a"))

        assert not queue.record_target_reservation(
            "batch-a",
            operation_id="fleet-batch-a",
            request_sha256="b" * 64,
            resume_token="replacement-token",
            expected_state="running",
            owner_token=owner,
            now="2099-01-01T00:00:04Z",
        )
        assert queue.target_operation("batch-a", include_token=True) == private

        assert queue.recover_stale(now="2099-01-01T02:00:00Z") == 1
        assert queue.get("job-a")["state"] == "completion_unknown"
        assert queue.target_operation("batch-a", include_token=True) == private
    finally:
        queue.close()


def test_exact_target_status_uses_private_credential_but_returns_no_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        queue.db.execute(
            "INSERT INTO jobs(job_id,task_name,state,created_at,updated_at) "
            "VALUES('job-a','command','queued','2099-01-01T00:00:00Z',"
            "'2099-01-01T00:00:00Z')"
        )
        queue.db.commit()
        owner = queue.claim_many(
            ["job-a"], "TARGET", batch_id="batch-a", pool=None,
            lease_until="2099-01-01T01:00:00Z", now="2099-01-01T00:00:00Z",
        )
        assert isinstance(owner, str)
        assert queue.set_batch_state(
            "batch-a", "running", expected_state="leased", owner_token=owner,
            now="2099-01-01T00:00:01Z",
        )
        assert queue.record_target_reservation(
            "batch-a", operation_id="fleet-batch-a", request_sha256="c" * 64,
            resume_token="private-target-token", expected_state="running",
            owner_token=owner, now="2099-01-01T00:00:02Z",
        )
        assert queue.record_target_acceptance(
            "batch-a", operation_id="fleet-batch-a", request_sha256="c" * 64,
            expected_state="running", owner_token=owner,
            now="2099-01-01T00:00:03Z",
        )

        class _StatusTransport:
            def durable_status(self, operation_id, token):  # noqa: ANN001
                assert operation_id == "fleet-batch-a"
                assert token == "private-target-token"
                return {
                    "schema": 1,
                    "operation_id": operation_id,
                    "request_sha256": "c" * 64,
                    "state": "complete",
                    "command_started": True,
                    "wrapper_exit_code": 0,
                    "target_cleanup": {"state": "RELEASED"},
                }

        monkeypatch.setattr(fleet_cli, "make_transport", lambda _device: _StatusTransport())
        config = SimpleNamespace(devices={"TARGET": object()})
        operation, ok = fleet_cli._target_operation_status(
            queue, queue.get("job-a"), refresh=True, config=config,
        )
        assert ok is True
        assert operation["query_status"] == "ok"
        assert operation["target_status"]["state"] == "complete"
        assert "private-target-token" not in json.dumps(operation)
    finally:
        queue.close()


def test_exact_status_uses_finalized_queue_receipt_after_target_retention_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        queue.db.execute(
            "INSERT INTO jobs(job_id,task_name,state,created_at,updated_at,last_result) "
            "VALUES(?,?,?,?,?,?)",
            (
                "job-a", "command", "queued", "2099-01-01T00:00:00Z",
                "2099-01-01T00:00:00Z", json.dumps({
                    "schema": 1,
                    "kind": "fleet-attempt-receipt",
                    "target_operation": {
                        "schema": 1,
                        "operation_id": "fleet-batch-a",
                        "request_sha256": "f" * 64,
                        "accepted": True,
                        "state": "complete",
                        "command_started": True,
                        "cleanup": {"state": "RELEASED"},
                    },
                }),
            ),
        )
        queue.db.commit()
        owner = queue.claim_many(
            ["job-a"], "TARGET", batch_id="batch-a", pool=None,
            lease_until="2099-01-01T01:00:00Z", now="2099-01-01T00:00:00Z",
        )
        assert isinstance(owner, str)
        assert queue.set_batch_state(
            "batch-a", "running", expected_state="leased", owner_token=owner,
            now="2099-01-01T00:00:01Z",
        )
        assert queue.record_target_reservation(
            "batch-a", operation_id="fleet-batch-a", request_sha256="f" * 64,
            resume_token="private-target-token", expected_state="running",
            owner_token=owner, now="2099-01-01T00:00:02Z",
        )
        assert queue.record_target_acceptance(
            "batch-a", operation_id="fleet-batch-a", request_sha256="f" * 64,
            expected_state="running", owner_token=owner,
            now="2099-01-01T00:00:03Z",
        )

        class _CleanedTransport:
            def durable_status(self, _operation_id, _token):  # noqa: ANN001
                raise TransportError("terminal target record was cleaned")

        monkeypatch.setattr(fleet_cli, "make_transport", lambda _device: _CleanedTransport())
        config = SimpleNamespace(devices={"TARGET": object()})
        operation, ok = fleet_cli._target_operation_status(
            queue, queue.get("job-a"), refresh=True, config=config,
        )
        assert ok is True
        assert operation["query_status"] == "finalized_from_queue_receipt"
        assert operation["target_status"]["cleanup"]["state"] == "RELEASED"
        assert "private-target-token" not in json.dumps(operation)
    finally:
        queue.close()


def test_durable_attempt_receipt_retains_target_identity_without_token() -> None:
    task = SimpleNamespace(prepared={"schema": 1})
    operation = {
        "schema": 1,
        "operation_id": "fleet-batch-a",
        "request_sha256": "d" * 64,
        "accepted": True,
        "state": "complete",
    }
    result = {
        "target_operation": operation,
        "resume_token": "must-not-persist",
    }
    receipt = fleet_executor.durable_attempt_record(task, result)
    assert json.loads(receipt) == {
        "schema": 1,
        "kind": "fleet-attempt-receipt",
        "target_operation": operation,
    }
    assert "must-not-persist" not in receipt


def test_resource_free_operation_reserves_and_renews_without_invented_policy(
    tmp_path: Path,
) -> None:
    client = _local_client(tmp_path)
    operation_id = "fleet-resource-free"
    reserved = client.reserve(
        allocation_id=operation_id,
        operation_id=operation_id,
        request_sha256="e" * 64,
        resource_keys=[],
        expected_policy_generation=0,
        expected_policy_digest=EMPTY_POLICY_DIGEST,
        rpc_id="reserve-resource-free",
    )
    assert isinstance(reserved, TargetReservation)
    renewed = client.renew(
        reserved,
        expected_policy_generation=0,
        expected_policy_digest=EMPTY_POLICY_DIGEST,
        rpc_id="renew-resource-free",
    )
    assert renewed["status"] == "reserved"
    assert renewed["receipt"]["resource_keys"] == []
    assert client.cancel(reserved, rpc_id="cancel-resource-free")["status"] == "cancelled"


def test_target_policy_cli_installs_explicit_opaque_keys_idempotently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    config = _local_client(tmp_path).config
    monkeypatch.setattr(fleet_cli, "load_config", lambda: config)

    argv = [
        "target-policy", "install", "--device", "LOCAL",
        "--resource", "tcp/8188", "--resource", "pool/gpu", "--json",
    ]
    assert fleet_cli.main(argv) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["status"] == "installed"
    assert first["generation"] == 1
    assert first["resources"] == ["pool/gpu", "tcp/8188"]

    assert fleet_cli.main(argv) == 0
    replay = json.loads(capsys.readouterr().out)
    assert replay["idempotent"] is True
    assert replay["generation"] == 1

    assert fleet_cli.main(["target-policy", "show", "--device", "LOCAL", "--json"]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["policy"]["document"]["resources"] == [
        {"capacity": 1, "key": "pool/gpu"},
        {"capacity": 1, "key": "tcp/8188"},
    ]


def test_durable_status_atomic_replace_retries_transient_permission_denial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "status.json"
    real_replace = durable_runner.os.replace
    attempts = 0

    def transient_replace(source, destination):  # noqa: ANN001
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError("transient reader still closing")
        real_replace(source, destination)

    monkeypatch.setattr(durable_runner.os, "replace", transient_replace)
    durable_runner._atomic_json(path, {"schema": 1, "state": "complete"})
    assert json.loads(path.read_text(encoding="utf-8"))["state"] == "complete"
    assert attempts == 3
    assert not list(tmp_path.glob("status.json.tmp-*"))


@pytest.mark.parametrize("cleanup_state", ["RELEASED", "UNKNOWN"])
def test_live_executor_orders_acceptance_and_respects_target_cleanup_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_state: str,
) -> None:
    events: list[str] = []
    device = Device.from_mapping(
        "TARGET",
        {
            "kind": "ssh-posix",
            "os": "posix",
            "project_root": str(tmp_path / "projects"),
            "state_root": str(tmp_path / "target-state"),
            "cache_root": str(tmp_path / "cache"),
        },
    )
    config = RemrunConfig(
        repo_root=tmp_path,
        defaults={"fleet": {"pools": {}}},
        devices={"TARGET": device},
        project_roots={},
    )
    prepared = prepare_raw_command([sys.executable, "-c", "print('ok')"], device="TARGET")
    task = as_fleet_task(prepared, {**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID})
    reservation = TargetReservation(
        {
            "allocation_id": "fleet-batch-a",
            "operation_id": "fleet-batch-a",
            "fence": 1,
            "policy_generation": 0,
            "policy_digest": EMPTY_POLICY_DIGEST,
            "resource_keys": [],
        },
        "private-target-token",
    )

    class _Client:
        state_root = str(tmp_path / "target-state")
        info = SimpleNamespace(installed_path=str(tmp_path / "runner.py"))

        def reserve(self, **_kwargs):  # noqa: ANN003
            return reservation

        def cancel(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
            return {"status": "cancelled"}

    class _Transport(LocalSimTransport):
        def launch_durable(self, _command, _cwd, **_kwargs):  # noqa: ANN001, ANN003
            assert events == ["reserved"]
            events.append("launched")
            return (
                {
                    "acknowledged": True,
                    "command_started": False,
                    "state": "pending",
                    "target_acceptance": {"state": "CLAIMED"},
                },
                {"platform": "POSIX", "telemetry": "none"},
            )

        def durable_status(self, _run_id, _token, *, include_logs=False):  # noqa: ANN001
            assert events == ["reserved", "launched", "accepted"]
            status = {
                "state": "complete",
                "command_started": True,
                "wrapper_exit_code": 0,
                "target_cleanup": {"state": cleanup_state},
            }
            if include_logs:
                return {
                    "status": status,
                    "stdout_b64": base64.b64encode(b"ok\n").decode("ascii"),
                    "stderr_b64": base64.b64encode(b"").decode("ascii"),
                }
            return status

        def durable_cleanup(self, _run_id, _token):  # noqa: ANN001
            events.append("cleaned")
            return {"cleaned": True}

    transport = _Transport(device)
    monkeypatch.setattr(fleet_executor, "make_transport", lambda _device: transport)
    monkeypatch.setattr(
        fleet_executor.TargetResourceClient,
        "connect",
        lambda *_args, **_kwargs: _Client(),
    )

    def record_reservation(receipt, token):  # noqa: ANN001
        assert receipt["accepted"] is False
        assert token == "private-target-token"
        events.append("reserved")
        return True

    def record_acceptance(receipt):  # noqa: ANN001
        assert receipt["accepted"] is True
        events.append("accepted")
        return True

    result = fleet_executor.run_batch(
        "TARGET", [task], config,
        state_root=tmp_path / "controller-state",
        observation_id="batch-a",
        on_target_reservation=record_reservation,
        on_target_acceptance=record_acceptance,
    )
    assert result["ok"] is True
    assert result["stdout_tail"] == "ok\n"
    operation_root = tmp_path / "target-state" / "fleet-operations" / "fleet-batch-a"
    if cleanup_state == "RELEASED":
        assert events == ["reserved", "launched", "accepted", "cleaned"]
        assert not operation_root.exists()
    else:
        assert events == ["reserved", "launched", "accepted"]
        assert operation_root.exists()
        assert result["cleanup_deferred"] is True


def test_lost_launch_response_with_claimed_target_remains_completion_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    device = Device.from_mapping(
        "TARGET",
        {
            "kind": "ssh-posix",
            "os": "posix",
            "project_root": str(tmp_path / "projects"),
            "state_root": str(tmp_path / "target-state"),
            "cache_root": str(tmp_path / "cache"),
        },
    )
    config = RemrunConfig(
        repo_root=tmp_path,
        defaults={"fleet": {"pools": {}}},
        devices={"TARGET": device},
        project_roots={},
    )
    prepared = prepare_raw_command([sys.executable, "-c", "print('ok')"], device="TARGET")
    task = as_fleet_task(prepared, {**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID})
    reservation = TargetReservation(
        {
            "allocation_id": "fleet-batch-ambiguous",
            "operation_id": "fleet-batch-ambiguous",
            "fence": 1,
            "policy_generation": 0,
            "policy_digest": EMPTY_POLICY_DIGEST,
            "resource_keys": [],
        },
        "private-target-token",
    )

    class ClaimedClient:
        state_root = str(tmp_path / "target-state")
        info = SimpleNamespace(installed_path=str(tmp_path / "runner.py"))

        def reserve(self, **_kwargs):  # noqa: ANN003
            return reservation

        def status(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
            return {
                "status": "found",
                "receipt": {
                    **reservation.receipt,
                    "state": "CLAIMED",
                    "command_start_state": "MAYBE",
                },
            }

        def cancel(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
            raise TargetResourceError("only a reserved allocation may be cancelled")

    class LostResponseTransport(LocalSimTransport):
        def launch_durable(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
            raise TransportError("launch and first status response were lost")

    monkeypatch.setattr(
        fleet_executor, "make_transport", lambda _device: LostResponseTransport(device),
    )
    monkeypatch.setattr(
        fleet_executor.TargetResourceClient,
        "connect",
        lambda *_args, **_kwargs: ClaimedClient(),
    )
    accepted: list[dict[str, object]] = []
    result = fleet_executor.run_batch(
        "TARGET",
        [task],
        config,
        state_root=tmp_path / "controller-state",
        observation_id="batch-ambiguous",
        on_target_reservation=lambda _receipt, _token: True,
        on_target_acceptance=lambda receipt: accepted.append(receipt) is None,
    )

    assert result["completion_state"] == "unknown"
    assert result["command_started"] is None
    assert result["cleanup_deferred"] is True
    assert Path(result["stage_dir"]).exists()
    assert result["target_operation"]["accepted"] is True
    assert result["target_operation"]["cleanup"]["state"] == "CLAIMED"
    assert accepted == [result["target_operation"]]
