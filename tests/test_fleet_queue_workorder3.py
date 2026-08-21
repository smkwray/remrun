"""Database-enforced queue idempotency, ownership fencing, and WAL gates."""
from __future__ import annotations

import concurrent.futures
import sqlite3
import threading

import pytest

from remrun.fleet import queue as queue_mod
from remrun.fleet.prepared import prepare_raw_command
from remrun.fleet.queue import FleetQueue, QueueMigrationError


_FAR = "2099-01-01T00:00:00Z"
_NOW = "2026-08-15T12:00:00Z"
_EXPIRED = "2026-08-15T11:59:59Z"


def _record(marker: str = "same") -> dict:
    return prepare_raw_command(["python", "-c", "pass", marker], device="DEVICE")


def _enqueue(queue: FleetQueue, key: str, marker: str = "same") -> str:
    return queue.enqueue_prepared(
        _record(marker), spec=None, idempotency_key=key, now=_NOW,
    )


def test_active_idempotency_is_unique_and_100_enqueues_converge(tmp_path) -> None:
    db = tmp_path / "fleet.db"
    queue = FleetQueue(db)
    indexes = queue.db.execute("PRAGMA index_list(jobs)").fetchall()
    assert any(row[1] == "ux_jobs_active_idem" and row[2] == 1 for row in indexes)
    queue.close()
    barrier = threading.Barrier(100)

    def submit(_: int) -> str:
        contender = FleetQueue(db)
        try:
            barrier.wait(timeout=20)
            return _enqueue(contender, "one-key")
        finally:
            contender.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as pool:
        job_ids = list(pool.map(submit, range(100)))

    assert len(set(job_ids)) == 1
    queue = FleetQueue(db)
    try:
        assert queue.db.execute(
            "SELECT COUNT(*) FROM jobs WHERE idempotency_key='one-key' "
            "AND state NOT IN ('done','failed_final','needs_review')"
        ).fetchone()[0] == 1
    finally:
        queue.close()


def test_distinct_idempotency_keys_enqueue_concurrently(tmp_path) -> None:
    db = tmp_path / "fleet.db"
    FleetQueue(db).close()
    count = 32
    barrier = threading.Barrier(count)

    def submit(index: int) -> str:
        contender = FleetQueue(db)
        try:
            barrier.wait(timeout=20)
            return _enqueue(contender, f"key-{index}")
        finally:
            contender.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
        job_ids = list(pool.map(submit, range(count)))
    assert len(set(job_ids)) == count


def test_same_key_with_different_prepared_meaning_fails_closed(tmp_path) -> None:
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        _enqueue(queue, "opaque-key", "first")
        with pytest.raises(ValueError, match="idempotency_key collision"):
            _enqueue(queue, "opaque-key", "second")
    finally:
        queue.close()


def test_active_duplicate_migration_preserves_history_and_fails_closed(tmp_path) -> None:
    db = tmp_path / "fleet.db"
    FleetQueue(db).close()
    conn = sqlite3.connect(db)
    conn.execute("DROP INDEX ux_jobs_active_idem")
    for job_id in ("duplicate-a", "duplicate-b"):
        conn.execute(
            "INSERT INTO jobs(job_id,task_name,idempotency_key,state,created_at,updated_at) "
            "VALUES(?, '__command__', 'duplicate-key', 'queued', ?, ?)",
            (job_id, _NOW, _NOW),
        )
    conn.commit()
    conn.close()

    with pytest.raises(QueueMigrationError, match=r"duplicate-key.*duplicate-a.*duplicate-b"):
        FleetQueue(db)

    conn = sqlite3.connect(db)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE idempotency_key='duplicate-key'"
        ).fetchone()[0] == 2
    finally:
        conn.close()


def test_legacy_migration_preserves_payload_bytes_but_never_reinterprets_them(tmp_path) -> None:
    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY, task_type TEXT NOT NULL, payload_json TEXT NOT NULL,
            force_device TEXT, engine TEXT, priority INTEGER NOT NULL DEFAULT 0,
            idempotency_key TEXT, state TEXT NOT NULL DEFAULT 'queued',
            attempts INTEGER NOT NULL DEFAULT 0, leased_until TEXT, assigned_device TEXT,
            batch_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            last_error TEXT, output_manifest TEXT
        );
        CREATE TABLE batches (
            batch_id TEXT PRIMARY KEY, state TEXT NOT NULL, device TEXT NOT NULL,
            task_type TEXT, engine TEXT, bucket TEXT, created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL, lease_until TEXT NOT NULL, heartbeat_at TEXT,
            estimated_finish_s REAL NOT NULL DEFAULT 0, error TEXT
        );
    """)
    active_bytes = '{"legacy":"active","opaque":[1,2,3]}'
    done_bytes = '{"legacy":"done","opaque":"keep exactly"}'
    conn.execute(
        "INSERT INTO jobs(job_id,task_type,payload_json,state,created_at,updated_at) "
        "VALUES('active','old-name',?,'queued',?,?)", (active_bytes, _NOW, _NOW),
    )
    conn.execute(
        "INSERT INTO jobs(job_id,task_type,payload_json,state,created_at,updated_at) "
        "VALUES('done','old-name',?,'done',?,?)", (done_bytes, _NOW, _NOW),
    )
    conn.commit()
    conn.close()

    queue = FleetQueue(db)
    try:
        active = queue.get("active")
        done = queue.get("done")
        assert active["state"] == "needs_review"
        assert active["prepared_json"] == active_bytes
        assert done["state"] == "done" and done["prepared_json"] == done_bytes
        assert active["prepared_id"] is None and done["prepared_id"] is None
        assert queue.prepared_record("active") is None
    finally:
        queue.close()


def test_two_concurrent_same_pool_claimants_have_one_winner(tmp_path) -> None:
    db = tmp_path / "fleet.db"
    queue = FleetQueue(db)
    jobs = [_enqueue(queue, f"claim-{index}", str(index)) for index in range(2)]
    queue.close()
    barrier = threading.Barrier(2)

    def claim(index: int) -> str | None:
        contender = FleetQueue(db)
        try:
            barrier.wait(timeout=10)
            return contender.claim_many(
                [jobs[index]], "DEVICE", batch_id=f"batch-{index}",
                lease_until=_FAR, pool="gpu", now=_NOW,
            )
        finally:
            contender.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        winners = list(pool.map(claim, range(2)))
    assert sum(bool(winner) for winner in winners) == 1


def _claimed(queue: FleetQueue, key: str, *, lease_until: str = _FAR) -> tuple[str, str]:
    job_id = _enqueue(queue, key, key)
    owner = queue.claim_many(
        [job_id], "DEVICE", batch_id=key, lease_until=lease_until,
        pool=None, now="2026-08-15T11:00:00Z",
    )
    assert owner is not None
    return job_id, owner


def test_expired_recovered_batch_cannot_be_revived_or_completed(tmp_path) -> None:
    queue = FleetQueue(tmp_path / "fleet.db")
    job_id, owner = _claimed(queue, "revive", lease_until=_EXPIRED)
    assert queue.set_batch_state(
        "revive", "running", expected_state="leased", owner_token=owner,
        now="2026-08-15T11:30:00Z",
    )
    assert queue.recover_stale(now=_NOW) == 1
    assert not queue.set_batch_state(
        "revive", "running", expected_state="running", owner_token=owner, now=_NOW,
    )
    assert not queue.complete_batch(
        "revive", expected_state="running", owner_token=owner, now=_NOW,
    )
    assert queue.get_batch("revive")["state"] == "failed"
    assert queue.get(job_id)["state"] == "completion_unknown"
    queue.close()


def test_completion_after_expiry_and_wrong_owner_are_rejected(tmp_path) -> None:
    queue = FleetQueue(tmp_path / "fleet.db")
    job_id, owner = _claimed(queue, "expired", lease_until=_EXPIRED)
    assert not queue.complete_batch(
        "expired", expected_state="leased", owner_token=owner, now=_NOW,
    )
    assert not queue.set_batch_state(
        "expired", "running", expected_state="leased", owner_token="wrong", now=_NOW,
    )
    assert queue.get(job_id)["state"] == "leased"
    queue.close()


def test_active_batch_count_survives_a_new_scheduling_tick(tmp_path) -> None:
    db = tmp_path / "fleet.db"
    queue = FleetQueue(db)
    _claimed(queue, "existing")
    queue.close()

    next_tick = FleetQueue(db)
    try:
        assert next_tick.active_batches_by_device(now=_NOW) == {"DEVICE": 1}
    finally:
        next_tick.close()


def test_queue_reports_verified_wal_and_refuses_vulnerable_runtime(
        tmp_path, monkeypatch) -> None:
    queue = FleetQueue(tmp_path / "safe.db")
    try:
        assert queue.journal_mode == "wal"
        assert queue.sqlite_version == sqlite3.sqlite_version
    finally:
        queue.close()

    monkeypatch.setattr(sqlite3, "sqlite_version", "3.50.4")
    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 50, 4))
    with pytest.raises(RuntimeError, match=r"3\.50\.4.*WAL-reset"):
        FleetQueue(tmp_path / "vulnerable.db")


def test_queue_refuses_a_journal_mode_fallback(tmp_path, monkeypatch) -> None:
    real_connect = sqlite3.connect

    class ModeCursor:
        @staticmethod
        def fetchone():
            return ("delete",)

    class Connection:
        def __init__(self, *args, **kwargs):
            self.inner = real_connect(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self.inner, name)

        def execute(self, sql, *args, **kwargs):
            if sql.strip().lower() == "pragma journal_mode=wal":
                return ModeCursor()
            return self.inner.execute(sql, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", Connection)
    with pytest.raises(RuntimeError, match=r"requires journal_mode=wal.*delete"):
        FleetQueue(tmp_path / "fallback.db")


def test_background_heartbeat_queue_open_failure_is_ownership_loss(
        tmp_path, monkeypatch) -> None:
    heartbeat = queue_mod.BatchHeartbeat(
        tmp_path / "fleet.db", "batch", "owner", "running", 60,
    )

    def fail_open(_path):
        raise RuntimeError("queue unavailable")

    monkeypatch.setattr(queue_mod, "FleetQueue", fail_open)
    heartbeat._loop()
    assert heartbeat.ownership_lost.is_set()
