"""Focused red/green gate for successor recovery seams missed by 0b71e228.

The tests intentionally specify required behavior. They fail on exact tree
5b85431bf374ef4f6889ccf1b47b6215b4b018ac and should pass only after a
successor makes cleanup finalization recoverable from terminal rows and routes
predecessor target rows through target-aware migration/recovery.
"""
from __future__ import annotations

import sqlite3
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun.config import RemrunConfig
from remrun.fleet import dispatcher, executor, queue as queue_mod
from remrun.fleet.prepared import (
    RAW_COMMAND_SPEC,
    RAW_COMMAND_SPEC_ID,
    as_fleet_task,
    prepare_raw_command,
)
from remrun.models import Device
from remrun.output import Reporter
from remrun.transport import LocalSimTransport


OLD = "2000-01-01T00:00:00Z"
FUTURE = "2099-01-01T00:00:00Z"


def _device(root: Path) -> Device:
    return Device.from_mapping(
        "TARGET",
        {
            # The target state root below is built from tmp_path, so it is a
            # native path. Declare the matching family: _target_state_root checks
            # absoluteness with PureWindowsPath/PurePosixPath per device.os, and a
            # posix declaration cannot validate a C:\... path.
            "kind": "ssh-powershell" if os.name == "nt" else "ssh-posix",
            "os": "windows" if os.name == "nt" else "posix",
            "project_root": str(root / "projects"),
            "state_root": str(root / "target-state"),
            "cache_root": str(root / "cache"),
        },
    )


def _config(root: Path) -> RemrunConfig:
    device = _device(root)
    return RemrunConfig(
        repo_root=root,
        defaults={"fleet": {"pools": {}}},
        devices={"TARGET": device},
        project_roots={},
    )


def _reporter() -> Reporter:
    return Reporter(json_events=False)


def test_known_terminal_cleanup_deferred_is_recoverable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A known result must not strand its finalization after the batch is terminal."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    config = _config(tmp_path)
    device = config.devices["TARGET"]
    state_root = tmp_path / "controller-state"
    db_path = state_root / "fleet" / "fleet.db"
    operation_id = "fleet-batch-a"
    request_sha = "a" * 64
    token = "private-token"

    prepared = prepare_raw_command(
        [sys.executable, "-c", "print('ok')"], device="TARGET"
    )
    task = as_fleet_task(
        prepared, {**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID}
    )
    q = queue_mod.FleetQueue(db_path)
    job_id = q.enqueue_prepared(
        prepared, spec=None, job_id="job-a", now="2026-08-24T00:00:00Z"
    )
    owner = q.claim_many(
        [job_id],
        "TARGET",
        batch_id="batch-a",
        lease_until=FUTURE,
        pool=None,
        task_name="command",
        engine="raw",
        bucket="",
        now="2026-08-24T00:00:00Z",
        target_protocol_version=1,
        current_spec_ids={job_id: RAW_COMMAND_SPEC_ID},
    )
    q.close()
    assert isinstance(owner, str)

    stage = tmp_path / "target-state" / "fleet-operations" / operation_id
    stage.mkdir(parents=True)
    (stage / "partial-input.bin").write_bytes(b"payload")

    def fake_run_batch(_device_name, _tasks, _config, **kwargs):  # noqa: ANN001, ANN003
        assert kwargs["on_target_reservation"](
            {"operation_id": operation_id, "request_sha256": request_sha}, token
        )
        assert kwargs["prelaunch_gate"]()
        assert kwargs["on_target_acceptance"](
            {"operation_id": operation_id, "request_sha256": request_sha}
        )
        # The command result is known, but a checked stage deletion failed. The
        # executor therefore correctly withholds target finalization.
        return {
            "ok": True,
            "exit_code": 0,
            "elapsed_s": 0.01,
            "staged": 1,
            "cleanup_deferred": True,
            "stage_dir": str(stage),
            "target_cleanup_deferred": "target stage deletion failed",
            "target_operation": {
                "schema": 1,
                "operation_id": operation_id,
                "request_sha256": request_sha,
                "accepted": True,
                "command_started": True,
                "state": "complete",
                "cleanup": {"state": "RELEASED"},
            },
            "stdout_tail": "ok\n",
            "stderr_tail": "",
            "item_results": [],
        }

    monkeypatch.setattr(executor, "run_batch", fake_run_batch)
    monkeypatch.setattr(dispatcher, "_remote_output_mtimes", lambda *_a, **_k: {})
    result = dispatcher._run_claimed_batch(
        config,
        state_root,
        {
            "batch_id": "batch-a",
            "device": "TARGET",
            "owner_token": owner,
            "btasks": [task],
            "engine": "raw",
            "job_ids": [job_id],
        },
        60,
        _reporter(),
    )
    assert result["ok"] == 1

    # Let a different controller own recovery rather than relying on the old
    # owner's still-live timestamp.
    q = queue_mod.FleetQueue(db_path)
    q.db.execute("UPDATE batches SET lease_until=? WHERE batch_id='batch-a'", (OLD,))
    q.db.commit()
    before = dict(
        q.db.execute(
            "SELECT state,target_finalized_at FROM batches WHERE batch_id='batch-a'"
        ).fetchone()
    )
    q.close()
    assert before == {"state": "done", "target_finalized_at": None}

    class Client:
        def status_identity(self, op, supplied_token, **_kwargs):  # noqa: ANN001, ANN003
            assert (op, supplied_token) == (operation_id, token)
            return {
                "status": "found",
                "receipt": {
                    "schema": 1,
                    "operation_id": operation_id,
                    "request_sha256": request_sha,
                    "state": "RELEASED",
                    "command_start_state": "YES",
                },
            }

    class Transport(LocalSimTransport):
        def durable_status(self, run_id, supplied_token, *, include_logs=False):  # noqa: ANN001
            assert (run_id, supplied_token) == (operation_id, token)
            return {
                "state": "complete",
                "acknowledged": True,
                "command_started": True,
                "target_acceptance": {
                    "operation_id": operation_id,
                    "request_sha256": request_sha,
                },
                "target_cleanup": {"state": "RELEASED"},
            }

        def durable_cleanup(self, run_id, supplied_token):  # noqa: ANN001
            assert (run_id, supplied_token) == (operation_id, token)
            return {"cleaned": True}

    monkeypatch.setattr(
        dispatcher.TargetResourceClient, "connect", lambda *_a, **_k: Client()
    )
    monkeypatch.setattr(dispatcher, "make_transport", lambda _d: Transport(device))

    dispatcher.drain_once(config, state_root=state_root, reporter=_reporter())

    q = queue_mod.FleetQueue(db_path)
    try:
        after = q.target_operation("batch-a", include_token=True)
        job = q.get(job_id)
    finally:
        q.close()
    assert after is not None and after["finalized"] is True
    assert job is not None and job["state"] == "done"
    assert not stage.exists()


def test_predecessor_target_row_migrates_into_target_truth_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rows made by e92bb1 must not be treated as non-target legacy batches."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    db_path = tmp_path / "controller-state" / "fleet" / "fleet.db"
    q = queue_mod.FleetQueue(db_path)
    q.close()

    # Exact predecessor batches schema: target identity/credential columns exist,
    # but target_protocol_version and finalization columns do not.
    conn = sqlite3.connect(db_path)
    conn.execute("DROP TABLE batches")
    conn.execute(
        """
        CREATE TABLE batches (
            batch_id TEXT PRIMARY KEY,
            owner_token TEXT,
            state TEXT NOT NULL,
            device TEXT NOT NULL,
            task_name TEXT,
            engine TEXT,
            bucket TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            lease_until TEXT NOT NULL,
            heartbeat_at TEXT,
            estimated_finish_s REAL,
            target_operation_id TEXT,
            target_request_sha256 TEXT,
            target_resume_token TEXT,
            target_reserved_at TEXT,
            target_accepted_at TEXT,
            error TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO jobs(job_id,task_name,state,attempts,batch_id,created_at,updated_at) "
        "VALUES(?,?,?,?,?,?,?)",
        ("job-old", "command", "running", 1, "batch-old", OLD, OLD),
    )
    conn.execute(
        "INSERT INTO batches(batch_id,owner_token,state,device,created_at,updated_at,"
        "lease_until,target_operation_id,target_request_sha256,target_resume_token,"
        "target_reserved_at,target_accepted_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "batch-old",
            "dead-owner",
            "running",
            "TARGET",
            OLD,
            OLD,
            OLD,
            "fleet-batch-old",
            "b" * 64,
            "old-private-token",
            OLD,
            OLD,
        ),
    )
    conn.commit()
    conn.close()

    stage = (
        tmp_path
        / "target-state"
        / "fleet-operations"
        / "fleet-batch-old"
    )
    stage.mkdir(parents=True)
    (stage / "partial-input.bin").write_bytes(b"partial")

    config = _config(tmp_path)
    device = config.devices["TARGET"]

    class Client:
        def status_identity(self, op, supplied_token, **_kwargs):  # noqa: ANN001, ANN003
            assert (op, supplied_token) == ("fleet-batch-old", "old-private-token")
            return {
                "status": "found",
                "receipt": {
                    "schema": 1,
                    "operation_id": "fleet-batch-old",
                    "request_sha256": "b" * 64,
                    "state": "RELEASED",
                    "command_start_state": "NO",
                },
            }

    class Transport(LocalSimTransport):
        def durable_cleanup(self, run_id, supplied_token):  # noqa: ANN001
            assert (run_id, supplied_token) == (
                "fleet-batch-old",
                "old-private-token",
            )
            return {"cleaned": False, "absent": True}

    monkeypatch.setattr(
        dispatcher.TargetResourceClient, "connect", lambda *_a, **_k: Client()
    )
    monkeypatch.setattr(dispatcher, "make_transport", lambda _d: Transport(device))

    q = queue_mod.FleetQueue(db_path)  # applies the successor migration
    try:
        migrated = dict(
            q.db.execute(
                "SELECT target_protocol_version,target_operation_id,target_finalized_at "
                "FROM batches WHERE batch_id='batch-old'"
            ).fetchone()
        )
        # The migration must route a predecessor target identity through the exact
        # target-aware coordinator, not generic queue-state inference.
        recovered = dispatcher._recover_target_stale(
            config,
            q,
            FUTURE,
            reporter=SimpleNamespace(event=lambda *_a, **_k: None),
        )
        job = q.get("job-old")
        target = q.target_operation("batch-old", include_token=True)
    finally:
        q.close()

    assert migrated["target_operation_id"] == "fleet-batch-old"
    assert recovered == 1
    assert job is not None and job["state"] == "queued"
    assert target is not None and target["finalized"] is True
    assert not stage.exists()
