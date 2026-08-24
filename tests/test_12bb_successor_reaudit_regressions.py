from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import pytest

from remrun.config import RemrunConfig
from remrun.fleet import dispatcher, queue as queue_mod
from remrun.fleet.prepared import RAW_COMMAND_SPEC_ID, prepare_raw_command
from remrun.models import Device
from remrun.output import Reporter
from remrun.transport import LocalSimTransport

OLD = "2000-01-01T00:00:00Z"
NOW = "2026-08-24T00:00:00Z"
FUTURE = "2099-01-01T00:00:00Z"


def _device(root: Path) -> Device:
    return Device.from_mapping(
        "TARGET",
        {
            # Same platform rule as the other target-acceptance fixtures: the
            # state root below is built from tmp_path, so declare the matching
            # family. _target_state_root checks absoluteness with
            # PureWindowsPath/PurePosixPath per device.os, and a posix
            # declaration cannot validate a C:\... path.
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


def _install_predecessor_batches_schema(db_path: Path) -> sqlite3.Connection:
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
    return conn


def test_migration_fences_predecessor_completion_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    db_path = tmp_path / "fleet.db"
    q = queue_mod.FleetQueue(db_path)
    q.close()
    conn = _install_predecessor_batches_schema(db_path)
    conn.execute(
        "INSERT INTO jobs(job_id,task_name,state,attempts,batch_id,created_at,updated_at) "
        "VALUES(?,?,?,?,?,?,?)",
        ("job-unknown", "command", "completion_unknown", 1, "batch-unknown", OLD, OLD),
    )
    conn.execute(
        "INSERT INTO batches(batch_id,owner_token,state,device,created_at,updated_at,"
        "lease_until,target_operation_id,target_request_sha256,target_resume_token,"
        "target_reserved_at,target_accepted_at,error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "batch-unknown", "dead-owner", "failed", "TARGET", OLD, OLD, OLD,
            "fleet-batch-unknown", "a" * 64, "private-token", OLD, OLD,
            "completion unknown",
        ),
    )
    conn.commit()
    conn.close()

    q = queue_mod.FleetQueue(db_path)
    try:
        row = q.get_batch("batch-unknown")
        assert row is not None
        assert row["target_finalization_disposition"] == queue_mod.FINALIZATION_FENCED
        assert q.terminal_target_batches(FUTURE) == []
        assert q.stale_target_batches(FUTURE, include_terminal=True) == []
        assert q.get("job-unknown")["state"] == "completion_unknown"
    finally:
        q.close()


def test_partial_predecessor_without_operation_id_is_fenced_without_target_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    db_path = tmp_path / "controller-state" / "fleet" / "fleet.db"
    q = queue_mod.FleetQueue(db_path)
    q.close()
    conn = _install_predecessor_batches_schema(db_path)
    conn.execute(
        "INSERT INTO jobs(job_id,task_name,state,attempts,batch_id,created_at,updated_at) "
        "VALUES(?,?,?,?,?,?,?)",
        ("job-partial", "command", "running", 1, "batch-partial", OLD, OLD),
    )
    conn.execute(
        "INSERT INTO batches(batch_id,owner_token,state,device,created_at,updated_at,"
        "lease_until,target_operation_id,target_request_sha256,target_resume_token,"
        "target_reserved_at,target_accepted_at,error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "batch-partial", "dead-owner", "running", "TARGET", OLD, OLD, OLD,
            None, "a" * 64, "private-token", OLD, None, None,
        ),
    )
    conn.commit()
    conn.close()

    stage = tmp_path / "target-state" / "fleet-operations" / "fleet-batch-partial"
    stage.mkdir(parents=True)
    (stage / "evidence.bin").write_bytes(b"partial identity evidence")
    monkeypatch.setattr(
        dispatcher,
        "make_transport",
        lambda _device: (_ for _ in ()).throw(AssertionError("target I/O is forbidden")),
    )
    monkeypatch.setattr(
        dispatcher.TargetResourceClient,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("target I/O is forbidden")
        ),
    )

    q = queue_mod.FleetQueue(db_path)
    try:
        migrated = q.get_batch("batch-partial")
        assert migrated is not None
        assert migrated["target_finalization_disposition"] == queue_mod.FINALIZATION_MALFORMED
        assert dispatcher._recover_target_stale(
            _config(tmp_path), q, FUTURE, reporter=Reporter(json_events=False),
        ) == 1
        batch = q.get_batch("batch-partial")
        job = q.get("job-partial")
    finally:
        q.close()
    assert batch is not None
    assert batch["target_finalization_disposition"] == queue_mod.FINALIZATION_FENCED
    assert job is not None and job["state"] == "completion_unknown"
    assert stage.exists()



def test_stale_selector_requires_explicit_terminal_opt_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    q = queue_mod.FleetQueue(tmp_path / "fleet.db")
    prepared = prepare_raw_command([sys.executable, "-c", "raise SystemExit(1)"], device="TARGET")
    job_id = q.enqueue_prepared(prepared, spec=None, job_id="job-selector", now=NOW)
    owner = q.claim_many(
        [job_id], "TARGET", batch_id="batch-selector", lease_until=FUTURE,
        pool=None, task_name="command", engine="raw", bucket="", now=NOW,
        target_protocol_version=1, current_spec_ids={job_id: RAW_COMMAND_SPEC_ID},
    )
    assert owner
    assert q.record_target_reservation(
        "batch-selector", operation_id="fleet-batch-selector", request_sha256="d" * 64,
        resume_token="private-token", expected_state="leased", owner_token=owner, now=NOW,
    )
    assert q.fail_batch(
        "batch-selector", "prestart known failure", expected_state="leased",
        owner_token=owner, now=NOW, max_attempts=1,
        finalization_disposition=queue_mod.FINALIZATION_PENDING,
    )
    q.db.execute("UPDATE batches SET lease_until=? WHERE batch_id='batch-selector'", (OLD,))
    q.db.commit()
    try:
        assert q.stale_target_batches(FUTURE) == []
        assert [row["batch_id"] for row in q.stale_target_batches(
            FUTURE, include_terminal=True,
        )] == ["batch-selector"]
    finally:
        q.close()

def test_scoped_drain_still_recovers_detached_historical_terminal_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    state_root = tmp_path / "controller-state"
    db_path = state_root / "fleet" / "fleet.db"
    q = queue_mod.FleetQueue(db_path)
    prepared = prepare_raw_command([sys.executable, "-c", "raise SystemExit(1)"], device="TARGET")
    job_id = q.enqueue_prepared(prepared, spec=None, job_id="job-a", now=NOW)
    owner = q.claim_many(
        [job_id], "TARGET", batch_id="batch-old", lease_until=FUTURE,
        pool=None, task_name="command", engine="raw", bucket="", now=NOW,
        target_protocol_version=1, current_spec_ids={job_id: RAW_COMMAND_SPEC_ID},
    )
    assert owner
    assert q.record_target_reservation(
        "batch-old", operation_id="fleet-batch-old", request_sha256="a" * 64,
        resume_token="private-token", expected_state="leased", owner_token=owner, now=NOW,
    )
    assert q.set_batch_state(
        "batch-old", "staging", expected_state="leased", owner_token=owner, now=NOW,
    )
    assert q.set_batch_state(
        "batch-old", "running", expected_state="staging", owner_token=owner, now=NOW,
    )
    assert q.record_target_acceptance(
        "batch-old", operation_id="fleet-batch-old", request_sha256="a" * 64,
        expected_state="running", owner_token=owner, now=NOW,
    )
    assert q.fail_batch(
        "batch-old", "exit 1", expected_state="running", owner_token=owner, now=NOW,
        finalization_disposition=queue_mod.FINALIZATION_PENDING,
    )
    q.db.execute("UPDATE batches SET lease_until=? WHERE batch_id='batch-old'", (OLD,))
    q.db.commit()
    assert q.get(job_id)["batch_id"] is None
    assert q.terminal_target_batches(FUTURE, job_ids=[job_id]) == []
    assert [row["batch_id"] for row in q.stale_target_batches(
        FUTURE, include_terminal=True,
    )] == ["batch-old"]
    q.close()

    operation_id = "fleet-batch-old"
    request_sha = "a" * 64
    token = "private-token"
    stage = tmp_path / "target-state" / "fleet-operations" / operation_id
    stage.mkdir(parents=True)
    (stage / "evidence.bin").write_bytes(b"known failure evidence")
    device = _device(tmp_path)

    class Client:
        def status_identity(self, operation, supplied_token, **_kwargs):  # noqa: ANN001
            assert (operation, supplied_token) == (operation_id, token)
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
        def durable_status(self, operation, supplied_token, *, include_logs=False):  # noqa: ANN001
            assert (operation, supplied_token) == (operation_id, token)
            return {
                "state": "failed",
                "acknowledged": True,
                "command_started": True,
                "target_acceptance": {
                    "operation_id": operation_id,
                    "request_sha256": request_sha,
                },
            }

        def durable_cleanup(self, operation, supplied_token):  # noqa: ANN001
            assert (operation, supplied_token) == (operation_id, token)
            return {"cleaned": True}

    monkeypatch.setattr(dispatcher.TargetResourceClient, "connect", lambda *_a, **_k: Client())
    monkeypatch.setattr(dispatcher, "make_transport", lambda _device: Transport(device))
    # Keep the requeued job unclaimed; this test is solely about cleanup recovery.
    monkeypatch.setattr(dispatcher, "_candidate_names", lambda *_args, **_kwargs: [])

    summary = dispatcher.drain_once(
        _config(tmp_path), state_root=state_root, job_ids=[job_id],
        reporter=Reporter(json_events=False),
    )
    assert summary["recovered"] == 1
    q = queue_mod.FleetQueue(db_path)
    try:
        batch = q.get_batch("batch-old")
        job = q.get(job_id)
    finally:
        q.close()
    assert batch is not None and batch["target_finalized_at"]
    assert batch["target_finalization_disposition"] == queue_mod.FINALIZATION_FINALIZED
    assert job is not None and job["state"] == "queued" and job["batch_id"] is None
    assert not stage.exists()
