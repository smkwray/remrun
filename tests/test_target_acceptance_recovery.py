"""Target-acceptance crash recovery, cleanup ordering, and queue retention."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun import _durable_runner as durable
from remrun.config import RemrunConfig
from remrun.fleet import executor as fleet_executor
from remrun.fleet import dispatcher as fleet_dispatcher
from remrun.fleet import queue as queue_mod
from remrun.fleet.prepared import (
    RAW_COMMAND_SPEC,
    RAW_COMMAND_SPEC_ID,
    as_fleet_task,
    prepare_raw_command,
)
from remrun.models import Device
from remrun.target_resources import (
    EMPTY_POLICY_DIGEST,
    TargetReservation,
    TargetResourceError,
)
from remrun.transport import LocalSimTransport, TransportError


def _synthetic_ssh_device(root: Path) -> Device:
    windows = os.name == "nt"
    return Device.from_mapping(
        "TARGET",
        {
            "kind": "ssh-powershell" if windows else "ssh-posix",
            "os": "windows" if windows else "posix",
            "project_root": str(root / "projects"),
            "state_root": str(root / "target-state"),
            "cache_root": str(root / "cache"),
        },
    )


def _raw_task(root: Path) -> tuple[Device, RemrunConfig, object]:
    device = _synthetic_ssh_device(root)
    config = RemrunConfig(
        repo_root=root,
        defaults={"fleet": {"pools": {}}},
        devices={"TARGET": device},
        project_roots={},
    )
    prepared = prepare_raw_command(
        [sys.executable, "-c", "print('ok')"], device="TARGET"
    )
    task = as_fleet_task(
        prepared, {**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID}
    )
    return device, config, task


def _reservation(operation_id: str, token: str = "private-target-token") -> TargetReservation:
    return TargetReservation(
        {
            "allocation_id": operation_id,
            "operation_id": operation_id,
            "fence": 1,
            "policy_generation": 0,
            "policy_digest": EMPTY_POLICY_DIGEST,
            "resource_keys": [],
        },
        token,
    )


def test_target_reservation_renews_during_staging() -> None:
    reservation = _reservation("fleet-renewed")
    calls: list[str] = []

    class Client:
        def renew(self, _reservation, **kwargs):  # noqa: ANN001, ANN003
            calls.append(str(kwargs["rpc_id"]))
            return {"receipt": {"state": "RESERVED"}}

    with fleet_executor._TargetReservationHeartbeat(
        Client(),
        reservation,
        policy_generation=0,
        policy_digest=EMPTY_POLICY_DIGEST,
        operation_id="fleet-renewed",
        interval_s=0.01,
    ) as heartbeat:
        deadline = time.monotonic() + 1.0
        while len(calls) < 2 and time.monotonic() < deadline:
            time.sleep(0.005)
        heartbeat.require_live()

    assert len(calls) >= 2
    assert calls == [f"fleet-renew-fleet-renewed-{i}" for i in range(1, len(calls) + 1)]


def test_ledger_claim_alone_does_not_emit_positive_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    device, config, task = _raw_task(tmp_path)
    operation_id = "fleet-batch-ambiguous"
    reservation = _reservation(operation_id)

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
            raise TransportError("launch response lost after target claim")

    monkeypatch.setattr(
        fleet_executor, "make_transport", lambda _device: LostResponseTransport(device)
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
    # A CLAIMED ledger row proves that replay is unsafe; it does not prove that
    # durable status persisted positive acceptance while the gate was closed.
    assert result["target_operation"]["accepted"] is False
    assert result["target_acceptance_unknown"] is True
    assert accepted == []


def test_terminal_preaccept_failure_reconciles_cleanup_before_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path.resolve()
    run_id = "fleet-preclaim-crash"
    token = "private-resume-token"
    rdir = root / "durable-runs" / run_id
    rdir.mkdir(parents=True)
    request_sha = hashlib.sha256(b"request").hexdigest()
    command_sha = hashlib.sha256(b"command").hexdigest()
    spec = {
        "schema": 1,
        "run_id": run_id,
        "token_sha256": durable._token_hash(token),
        "controller": "controller-a",
        "project_id": "@fleet",
        "target": "TARGET",
        "command_sha256": command_sha,
        "argv": [str(root / "definitely-missing-observer")],
        "ready_path": str(rdir / "observer-ready.json"),
        "max_log_bytes": 1024,
        "created_at": "2026-08-24T00:00:00Z",
        "acceptance": {
            "schema": 1,
            "runner_path": str(root / "runner.py"),
            "state_root": str(root),
            "operation_id": run_id,
            "request_sha256": request_sha,
            "reservation": {
                "allocation_id": run_id,
                "fence": 1,
                "policy_generation": 0,
                "policy_digest": "0" * 64,
            },
            "start_gate_path": str(rdir / "start-gate.json"),
            "started_path": str(rdir / "started.json"),
        },
    }
    durable._atomic_json(rdir / "spec.json", spec)
    durable._atomic_json(
        rdir / "auth.json",
        {"schema": 1, "run_id": run_id, "token_sha256": spec["token_sha256"]},
    )
    durable._atomic_json(
        durable._claim_auth_path(root, run_id),
        {"schema": 1, "run_id": run_id, "claim_token": token},
    )
    for name in ("stdout.log", "stderr.log"):
        durable._atomic_bytes(rdir / name, b"")
    durable._atomic_json(rdir / "status.json", durable._base_status(spec, "launching"))

    assert durable._supervise(root, run_id) == 1
    assert json.loads((rdir / "status.json").read_text())["target_cleanup"] is None
    calls: list[str] = []

    def expired_status(*_args, **_kwargs):  # noqa: ANN002, ANN003
        calls.append("status")
        return {
            "schema": 1,
            "operation_id": run_id,
            "request_sha256": request_sha,
            "state": "EXPIRED",
            "command_start_state": "NO",
        }

    monkeypatch.setattr(durable, "_resource_transition", expired_status)
    refreshed = durable._status(root, run_id, token, False)
    assert calls == ["status"]
    assert refreshed["target_cleanup"]["state"] == "EXPIRED"
    assert durable._cleanup(root, run_id, token)["cleaned"] is True
    assert not rdir.exists()


def test_stage_delete_is_proved_before_durable_evidence_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    device, config, task = _raw_task(tmp_path)
    reservation = _reservation("fleet-batch-a")
    events: list[str] = []

    class Client:
        state_root = str(tmp_path / "target-state")
        info = SimpleNamespace(installed_path=str(tmp_path / "runner.py"))

        def reserve(self, **_kwargs):  # noqa: ANN003
            return reservation

    class Transport(LocalSimTransport):
        def launch_durable(self, _command, _cwd, **_kwargs):  # noqa: ANN001, ANN003
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
            status = {
                "state": "complete",
                "command_started": True,
                "wrapper_exit_code": 0,
                "target_cleanup": {"state": "RELEASED"},
            }
            if include_logs:
                return {
                    "status": status,
                    "stdout_b64": base64.b64encode(b"ok\n").decode(),
                    "stderr_b64": "",
                }
            return status

        def durable_cleanup(self, _run_id, _token):  # noqa: ANN001
            events.append("durable_cleanup")
            return {"cleaned": True}

        def remove_remote_tree(self, _path):  # noqa: ANN001
            events.append("stage_delete")
            raise TransportError("simulated stage deletion failure")

    monkeypatch.setattr(fleet_executor, "make_transport", lambda _device: Transport(device))
    monkeypatch.setattr(
        fleet_executor.TargetResourceClient,
        "connect",
        lambda *_args, **_kwargs: Client(),
    )
    result = fleet_executor.run_batch(
        "TARGET",
        [task],
        config,
        state_root=tmp_path / "controller-state",
        observation_id="batch-a",
        on_target_reservation=lambda _receipt, _token: True,
        on_target_acceptance=lambda _receipt: True,
        before_target_cleanup=lambda _receipt: True,
    )

    assert events == ["stage_delete"]
    assert result["cleanup_deferred"] is True
    assert result["stage_dir"].endswith("fleet-batch-a")
    assert result["target_cleanup_deferred"] == (
        "target stage deletion failed: simulated stage deletion failure"
    )


def test_target_reservation_is_persisted_before_first_stage_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    device, config, task = _raw_task(tmp_path)
    reservation = _reservation("fleet-batch-before-stage")
    events: list[str] = []

    class Client:
        state_root = str(tmp_path / "target-state")
        info = SimpleNamespace(installed_path=str(tmp_path / "runner.py"))

        def reserve(self, **_kwargs):  # noqa: ANN003
            events.append("reserve")
            return reservation

        def cancel(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
            events.append("cancel")
            return {"receipt": {"state": "CANCELLED"}}

    class Transport(LocalSimTransport):
        def ensure_remote_dir(self, path):  # noqa: ANN001
            assert "persist" in events
            if "stage_write" not in events:
                events.append("stage_write")
            return super().ensure_remote_dir(path)

        def remove_remote_tree(self, path):  # noqa: ANN001
            events.append("stage_delete")
            return super().remove_remote_tree(path)

        def durable_cleanup(self, _run_id, _token):  # noqa: ANN001
            events.append("durable_cleanup")
            return {"cleaned": False, "absent": True}

    monkeypatch.setattr(fleet_executor, "make_transport", lambda _device: Transport(device))
    monkeypatch.setattr(
        fleet_executor.TargetResourceClient,
        "connect",
        lambda *_args, **_kwargs: Client(),
    )
    finalized: list[dict[str, object]] = []
    result = fleet_executor.run_batch(
        "TARGET",
        [task],
        config,
        state_root=tmp_path / "controller-state",
        observation_id="batch-before-stage",
        prelaunch_gate=lambda: False,
        on_target_reservation=lambda _receipt, _token: events.append("persist") is None,
        before_target_cleanup=lambda _receipt: events.append("authorize") is None,
        on_target_finalization=lambda receipt: (
            events.append("finalize") or finalized.append(receipt) or True
        ),
    )

    assert result["definition_drift"] is True
    assert events.index("reserve") < events.index("persist") < events.index("stage_write")
    assert events[-5:] == [
        "cancel", "authorize", "stage_delete", "durable_cleanup", "finalize",
    ]
    assert finalized[0]["cleanup_state"] == "CANCELLED"


def test_terminal_prestart_failure_finalizes_before_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    device, config, task = _raw_task(tmp_path)
    reservation = _reservation("fleet-batch-prestart-failed")
    events: list[str] = []

    class Client:
        state_root = str(tmp_path / "target-state")
        info = SimpleNamespace(installed_path=str(tmp_path / "runner.py"))

        def reserve(self, **_kwargs):  # noqa: ANN003
            return reservation

    class Transport(LocalSimTransport):
        def launch_durable(self, _command, _cwd, **_kwargs):  # noqa: ANN001, ANN003
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
            return {
                "state": "failed",
                "command_started": False,
                "error": "observer failed before command start",
                "target_cleanup": {"state": "RELEASED"},
            }

        def remove_remote_tree(self, path):  # noqa: ANN001
            events.append("stage_delete")
            return super().remove_remote_tree(path)

        def durable_cleanup(self, _run_id, _token):  # noqa: ANN001
            events.append("durable_cleanup")
            return {"cleaned": True}

    monkeypatch.setattr(fleet_executor, "make_transport", lambda _device: Transport(device))
    monkeypatch.setattr(
        fleet_executor.TargetResourceClient,
        "connect",
        lambda *_args, **_kwargs: Client(),
    )
    result = fleet_executor.run_batch(
        "TARGET",
        [task],
        config,
        state_root=tmp_path / "controller-state",
        observation_id="batch-prestart-failed",
        on_target_reservation=lambda _receipt, _token: True,
        on_target_acceptance=lambda _receipt: True,
        before_target_cleanup=lambda _receipt: events.append("authorize") is None,
        on_target_finalization=lambda _receipt: events.append("finalize") is None,
    )

    assert result["completion_state"] == "not_started"
    assert result["command_started"] is False
    assert result.get("cleanup_deferred") is not True
    assert events == ["authorize", "stage_delete", "durable_cleanup", "finalize"]


def test_guard_finalization_prestart_cleanup_is_authorized_before_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guard receipt failure uses the same pre-delete owner authorization gate."""
    device, config, task = _raw_task(tmp_path)
    reservation = _reservation("fleet-batch-guard-finalization")
    events: list[str] = []

    class Client:
        state_root = str(tmp_path / "target-state")
        info = SimpleNamespace(installed_path=str(tmp_path / "runner.py"))

        def reserve(self, **_kwargs):  # noqa: ANN003
            return reservation

    class Transport(LocalSimTransport):
        def launch_durable(self, _command, _cwd, **_kwargs):  # noqa: ANN001, ANN003
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
            status = {
                "state": "complete",
                "command_started": False,
                "wrapper_exit_code": 0,
                "target_cleanup": {"state": "RELEASED"},
            }
            return {"status": status} if include_logs else status

        def remove_remote_tree(self, path):  # noqa: ANN001
            events.append("stage_delete")
            return super().remove_remote_tree(path)

        def durable_cleanup(self, _run_id, _token):  # noqa: ANN001
            events.append("durable_cleanup")
            return {"cleaned": True}

    monkeypatch.setattr(fleet_executor, "make_transport", lambda _device: Transport(device))
    monkeypatch.setattr(
        fleet_executor.TargetResourceClient,
        "connect",
        lambda *_args, **_kwargs: Client(),
    )
    monkeypatch.setattr(
        fleet_executor,
        "finalize_durable_result",
        lambda _terminal, _execution: (_ for _ in ()).throw(
            fleet_executor.GuardFinalizationError(
                "simulated guard finalization loss", command_started=False,
            )
        ),
    )

    result = fleet_executor.run_batch(
        "TARGET",
        [task],
        config,
        state_root=tmp_path / "controller-state",
        observation_id="batch-guard-finalization",
        on_target_reservation=lambda _receipt, _token: True,
        on_target_acceptance=lambda _receipt: True,
        before_target_cleanup=lambda _receipt: events.append("authorize") is None,
        on_target_finalization=lambda _receipt: events.append("finalize") is None,
    )

    assert result["completion_state"] == "not_started"
    assert result["command_started"] is False
    assert events == ["authorize", "stage_delete", "durable_cleanup", "finalize"]


def _open_queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):  # noqa: ANN202
    # Some test runtimes predate remrun's required patched SQLite builds;
    # queue semantics are independent of the WAL reset capability gate.
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    return queue_mod.FleetQueue(tmp_path / "fleet.db")


def _insert_unfinalized_done(q, *, batch: str, job: str) -> None:  # noqa: ANN001
    now = "2099-01-01T00:00:00Z"
    result = {
        "ok": True,
        "target_operation": {
            "schema": 1,
            "operation_id": f"fleet-{batch}",
            "request_sha256": "a" * 64,
            "accepted": True,
            "cleanup": {"state": "QUARANTINED"},
        },
        "cleanup_deferred": True,
    }
    q.db.execute(
        "INSERT INTO jobs(job_id,task_name,state,created_at,updated_at,batch_id,last_result) "
        "VALUES(?,?,?,?,?,?,?)",
        (job, "command", "done", now, now, batch, json.dumps(result)),
    )
    q.db.execute(
        "INSERT INTO batches(batch_id,owner_token,state,device,created_at,updated_at,"
        "lease_until,target_operation_id,target_request_sha256,target_resume_token,"
        "target_reserved_at,target_accepted_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            batch,
            "owner",
            "done",
            "TARGET",
            now,
            now,
            "2100-01-01T00:00:00Z",
            f"fleet-{batch}",
            "a" * 64,
            "resume-token-only-copy",
            now,
            now,
        ),
    )
    q.db.commit()


def test_prune_and_clear_preserve_unfinalized_target_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    q = _open_queue(tmp_path, monkeypatch)
    try:
        _insert_unfinalized_done(q, batch="batch-a", job="job-a")
        q.prune_final(keep=0)
        assert q.db.execute("SELECT 1 FROM jobs WHERE job_id='job-a'").fetchone()
        private = q.target_operation("batch-a", include_token=True)
        assert private is not None
        assert private["resume_token"] == "resume-token-only-copy"

        _insert_unfinalized_done(q, batch="batch-b", job="job-b")
        q.db.execute(
            "INSERT INTO jobs(job_id,task_name,state,created_at,updated_at) "
            "VALUES('job-active','command','queued','2099-01-01T00:00:00Z',"
            "'2099-01-01T00:00:00Z')"
        )
        q.db.commit()
        owner = q.claim_many(
            ["job-active"], "TARGET", batch_id="batch-active", pool="gpu",
            lease_until="2100-01-01T00:00:00Z", now="2099-01-01T00:00:00Z",
            target_protocol_version=1,
        )
        assert isinstance(owner, str)
        assert q.record_target_reservation(
            "batch-active",
            operation_id="fleet-batch-active",
            request_sha256="b" * 64,
            resume_token="active-token-only-copy",
            expected_state="leased",
            owner_token=owner,
            now="2099-01-01T00:00:01Z",
        )
        q.clear(include_final=True)
        assert q.db.execute("SELECT 1 FROM jobs WHERE job_id='job-b'").fetchone()
        private = q.target_operation("batch-b", include_token=True)
        assert private is not None
        assert private["resume_token"] == "resume-token-only-copy"
        assert q.db.execute(
            "SELECT 1 FROM resource_leases WHERE batch_id='batch-active'"
        ).fetchone()
    finally:
        q.close()


def test_stale_pre_reservation_rows_do_not_leak_stage_or_claim_uncertain_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    q = _open_queue(tmp_path, monkeypatch)
    try:
        old = "2000-01-01T00:00:00Z"
        now = "2099-01-01T00:00:00Z"
        stages: dict[str, Path] = {}
        for state in ("staging", "running"):
            batch = f"batch-{state}"
            job = f"job-{state}"
            stage = (
                tmp_path
                / "target-state"
                / "fleet-operations"
                / f"fleet-{batch}"
            )
            stage.mkdir(parents=True)
            (stage / "partial-input.bin").write_bytes(b"partial")
            stages[state] = stage
            q.db.execute(
                "INSERT INTO jobs(job_id,task_name,state,attempts,batch_id,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (job, "command", state, 1, batch, old, old),
            )
            q.db.execute(
                "INSERT INTO batches(batch_id,owner_token,state,device,created_at,updated_at,"
                "lease_until,target_protocol_version,target_operation_id) "
                "VALUES(?,?,?,?,?,?,?,?,NULL)",
                (batch, "dead-owner", state, "TARGET", old, old, old, 1),
            )
        q.db.commit()

        device = _synthetic_ssh_device(tmp_path)
        config = RemrunConfig(
            repo_root=tmp_path,
            defaults={"fleet": {"pools": {}}},
            devices={"TARGET": device},
            project_roots={},
        )
        monkeypatch.setattr(
            fleet_dispatcher, "make_transport", lambda _device: LocalSimTransport(device)
        )
        assert q.recover_stale(now) == 0
        assert fleet_dispatcher._recover_target_stale(
            config,
            q,
            now,
            reporter=SimpleNamespace(event=lambda *_args, **_kwargs: None),
        ) == 2
        staging = q.db.execute(
            "SELECT state FROM jobs WHERE job_id='job-staging'"
        ).fetchone()
        running = q.db.execute(
            "SELECT state FROM jobs WHERE job_id='job-running'"
        ).fetchone()
        assert staging["state"] == "queued"
        # With no persisted target operation, launch was never authorized; the
        # recovery must not invent completion uncertainty.
        assert running["state"] == "queued"
        # Exact pre-launch stages must be removed or retained under a recoverable,
        # owner-fenced operation credential; these rows have no such credential.
        assert not stages["staging"].exists()
        assert not stages["running"].exists()
    finally:
        q.close()


def test_unleased_execution_still_cleans_target_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unmanaged execution must clean up, not silently retain target state.

    The shared cleanup helper refuses to delete without authorization, which is
    correct for queue-managed runs where a targeted cancellation can commit
    between the decision and the delete. The unleased path has no queue owner and
    no cancellation transaction, so it must grant that authority explicitly. When
    it did not, every successful `--no-lease` run against a protocol-v1 target
    left its stage and durable record behind, and repeated runs accumulated them
    on the target -- while still reporting ok.
    """
    device, config, task = _raw_task(tmp_path)
    reservation = _reservation("fleet-batch-unleased")
    events: list[str] = []

    class Client:
        state_root = str(tmp_path / "target-state")
        info = SimpleNamespace(installed_path=str(tmp_path / "runner.py"))

        def reserve(self, **_kwargs):  # noqa: ANN003
            return reservation

    class Transport(LocalSimTransport):
        def launch_durable(self, _command, _cwd, **_kwargs):  # noqa: ANN001, ANN003
            return (
                {
                    "acknowledged": True,
                    "command_started": True,
                    "state": "running",
                    "target_acceptance": {"state": "CLAIMED"},
                },
                {"platform": "POSIX", "telemetry": "none"},
            )

        def durable_status(self, _run_id, _token, *, include_logs=False):  # noqa: ANN001
            status = {
                "state": "complete",
                "command_started": True,
                "wrapper_exit_code": 0,
                "target_cleanup": {"state": "RELEASED"},
            }
            if include_logs:
                return {
                    "status": status,
                    "stdout_b64": base64.b64encode(b"ok\n").decode("ascii"),
                    "stderr_b64": base64.b64encode(b"").decode("ascii"),
                }
            return status

        def remove_remote_tree(self, path):  # noqa: ANN001
            events.append("stage_delete")
            return super().remove_remote_tree(path)

        def durable_cleanup(self, _run_id, _token):  # noqa: ANN001
            events.append("durable_cleanup")
            return {"cleaned": True}

    monkeypatch.setattr(fleet_executor, "make_transport", lambda _device: Transport(device))
    monkeypatch.setattr(
        fleet_executor.TargetResourceClient,
        "connect",
        lambda *_args, **_kwargs: Client(),
    )

    # Placement probes the real device; force it so the test exercises the
    # unleased execution path rather than device selection.
    monkeypatch.setattr(
        fleet_executor, "_choose_device", lambda *_args, **_kwargs: ("TARGET", {}),
    )
    result = fleet_executor.run_group(
        [task], config, state_root=tmp_path / "controller-state", use_lease=False,
    )

    assert result.get("error") is None
    # The whole point: an unmanaged run reaches the shared helper with authority,
    # so both classes of target evidence are removed rather than retained forever.
    assert events == ["stage_delete", "durable_cleanup"]
    assert result.get("cleanup_deferred") is not True
