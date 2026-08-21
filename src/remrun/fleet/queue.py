"""Durable local fleet job queue (SQLite). Local regenerable state under remrun's
state root; never synced. Provides enqueue (with idempotent dedupe), atomic claim,
completion/failure with bounded retry, and per-device/per-state counts for the
dispatcher's concurrency control. The dispatcher/executor that *uses* this is built
on top; the worker model-lifetime contract (load -> drain -> unload, Invariant 0)
lives in the device workers, not here.
"""
from __future__ import annotations

import contextlib
import json
import math
import sqlite3
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from ..state import iso_plus_seconds, utc_now_iso
_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id           TEXT PRIMARY KEY,
    task_name        TEXT NOT NULL,
    prepared_json    TEXT,
    prepared_id      TEXT,
    spec_id          TEXT,
    force_device     TEXT,
    priority         INTEGER NOT NULL DEFAULT 0,
    idempotency_key  TEXT,
    state            TEXT NOT NULL DEFAULT 'queued',
    attempts         INTEGER NOT NULL DEFAULT 0,
    leased_until     TEXT,
    assigned_device  TEXT,
    batch_id         TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    last_error       TEXT,
    output_manifest  TEXT,
    exclude_devices  TEXT,          -- devices that proved they cannot serve it
    last_result      TEXT           -- latest completed attempt's structured record
);
CREATE INDEX IF NOT EXISTS ix_jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS ix_jobs_batch ON jobs(batch_id);

-- A batch = one worker invocation over a compatible burst (one cold model load).
CREATE TABLE IF NOT EXISTS batches (
    batch_id     TEXT PRIMARY KEY,
    owner_token  TEXT,                   -- per-claim fence; NULL only on legacy rows
    state        TEXT NOT NULL,          -- leased|staging|running|fetching|done|failed
    device       TEXT NOT NULL,
    task_name    TEXT, engine TEXT, bucket TEXT,
    created_at   TEXT NOT NULL, updated_at TEXT NOT NULL,
    lease_until  TEXT NOT NULL, heartbeat_at TEXT,
    estimated_finish_s REAL,                      -- NULL means duration is honestly unknown
    error        TEXT
);
-- One row per held resource slot. UNIQUE(device,pool) makes a configured pool a
-- hard mutex: a second batch cannot lease the same resource while one is held.
CREATE TABLE IF NOT EXISTS resource_leases (
    device       TEXT NOT NULL,
    pool         TEXT NOT NULL,
    batch_id     TEXT NOT NULL,
    lease_until  TEXT NOT NULL,
    PRIMARY KEY (device, pool)
);
-- Device (or device+engine) cooldowns after a failure. engine='' = device-wide
-- (e.g. SSH transport failure backs off the whole box); a specific engine = that engine
-- on that device (e.g. a model OOM cools only that engine). Placement skips a
-- candidate while its cooldown is active.
CREATE TABLE IF NOT EXISTS cooldowns (
    device       TEXT NOT NULL,
    engine       TEXT NOT NULL DEFAULT '',
    until        TEXT NOT NULL,
    kind         TEXT,
    reason       TEXT,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (device, engine)
);
-- Content-addressed configured-task meaning.
CREATE TABLE IF NOT EXISTS prepared_specs (
    spec_id        TEXT PRIMARY KEY,
    schema         INTEGER NOT NULL,
    canonical_json TEXT NOT NULL,
    created_at     TEXT NOT NULL
);
-- Global durable output namespace. A stem may be reused only for the exact
-- same semantic work (for example a terminal rerun), never by unrelated work.
CREATE TABLE IF NOT EXISTS prepared_output_reservations (
    stem           TEXT PRIMARY KEY,
    work_id        TEXT NOT NULL,
    created_at     TEXT NOT NULL
);
-- Raw execution observations are authoritative and are committed atomically
-- with the queue's terminal transition. Derived profile caches are rebuildable.
CREATE TABLE IF NOT EXISTS fleet_profile_observations (
    batch_id             TEXT PRIMARY KEY,
    profile_key          TEXT,
    family_id            TEXT,
    device               TEXT NOT NULL,
    adapter_id           TEXT,
    prepared_units       REAL,
    observed_units       REAL,
    controller_elapsed_s REAL,
    worker_elapsed_s     REAL,
    peak_rss_mb          REAL,
    peak_vram_mb         REAL,
    accepted_duration    INTEGER NOT NULL,
    reject_reason        TEXT,
    result_digest        TEXT,
    recorded_at          TEXT NOT NULL
);
"""

# Terminal states. ``needs_review`` is a durable ANSWER — the worker examined the
# work and refused it — not an error to retry: it must never be reopened by a
# late completer and must not block a fresh submission of the same key.
_FINAL = ("done", "failed_final", "needs_review")
_FINAL_Q = ",".join("?" * len(_FINAL))
# History that volume may evict. A review answer is waiting for a PERSON, so it
# is never discarded to make room for ordinary throughput.
_PRUNABLE = ("done", "failed_final")
_BATCH_ACTIVE = ("leased", "staging", "running", "fetching")
MAX_ATTEMPTS = 3


class QueueConfigurationError(RuntimeError):
    """The local SQLite runtime or database cannot honor the queue contract."""


class QueueMigrationError(RuntimeError):
    """Existing queue state needs an explicit owner-directed repair."""


def _wal_reset_safe(version: tuple[int, ...]) -> bool:
    """Whether ``version`` contains SQLite's March-2026 WAL-reset repair.

    SQLite documents the race in every WAL-capable release from 3.7.0 through
    3.51.2. It is fixed on trunk in 3.51.3 and in the maintained backports
    3.50.7 and 3.44.6. Fail closed for every other older line: this queue opens
    multiple writer connections by design, so it exercises the affected shape.
    """
    v = tuple(version[:3]) + (0,) * max(0, 3 - len(version))
    if v >= (3, 51, 3):
        return True
    return ((v[0], v[1]) == (3, 50) and v[2] >= 7) or (
        (v[0], v[1]) == (3, 44) and v[2] >= 6
    )


def _active_idempotency_index_sql() -> str:
    terminal = ",".join(f"'{state}'" for state in _FINAL)
    return (
        "CREATE UNIQUE INDEX ux_jobs_active_idem ON jobs(idempotency_key) "
        "WHERE idempotency_key IS NOT NULL AND idempotency_key <> '' "
        f"AND state NOT IN ({terminal})"
    )


def _parse_iso(s: str | None) -> float:
    """Parse a utc_now_iso() timestamp (…Z) to epoch seconds; 0.0 if unparseable."""
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except (ValueError, AttributeError):
        return 0.0


class FleetQueue:
    def __init__(self, db_path: Path) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        # Autocommit (isolation_level=None) so the multi-statement transitions below can
        # use explicit BEGIN IMMEDIATE transactions (all-or-nothing, race-safe).
        self.db = sqlite3.connect(str(db_path), isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.sqlite_version = sqlite3.sqlite_version
        self.journal_mode = ""
        try:
            if not _wal_reset_safe(sqlite3.sqlite_version_info):
                raise QueueConfigurationError(
                    f"SQLite {self.sqlite_version} is vulnerable to the WAL-reset race; "
                    "the fleet queue requires SQLite 3.51.3+, 3.50.7, or 3.44.6"
                )
            row = self.db.execute("PRAGMA journal_mode=WAL").fetchone()
            self.journal_mode = str(row[0] if row else "").lower()
            if self.journal_mode != "wal":
                raise QueueConfigurationError(
                    "fleet queue requires journal_mode=wal; "
                    f"SQLite reported {self.journal_mode or 'no mode'}"
                )
            self.db.execute("PRAGMA busy_timeout=5000")
            self.db.executescript(_SCHEMA)
            self._migrate()
            self._migrate_nullable_batch_estimates()
            self._migrate_prepared_output_reservations()
        except BaseException:
            self.db.close()
            raise

    def _migrate(self) -> None:
        """Upgrade the local queue without inventing or deleting queue history."""
        have_jobs = {r["name"] for r in self.db.execute("PRAGMA table_info(jobs)")}
        have_batches = {r["name"] for r in self.db.execute("PRAGMA table_info(batches)")}
        current_jobs = {"job_id", "task_name", "prepared_json", "prepared_id", "spec_id",
                        "force_device", "priority", "idempotency_key", "state", "attempts",
                        "leased_until", "assigned_device", "batch_id", "created_at",
                        "updated_at", "last_error", "output_manifest", "exclude_devices",
                        "last_result"}
        current_batches = {"batch_id", "owner_token", "state", "device", "task_name",
                           "engine", "bucket", "created_at", "updated_at", "lease_until",
                           "heartbeat_at", "estimated_finish_s", "error"}
        index_row = self.db.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' "
            "AND name='ux_jobs_active_idem'"
        ).fetchone()
        expected_index = " ".join(_active_idempotency_index_sql().lower().split())
        have_unique = bool(
            index_row and " ".join(str(index_row["sql"] or "").lower().split()) == expected_index
        )
        if have_jobs == current_jobs and have_batches == current_batches and have_unique:
            return

        with self._immediate():
            have_jobs = {r["name"] for r in self.db.execute("PRAGMA table_info(jobs)")}
            for column in ("prepared_id", "spec_id", "exclude_devices", "last_result",
                           "output_manifest", "assigned_device", "leased_until", "batch_id"):
                if column not in have_jobs:
                    self.db.execute(f"ALTER TABLE jobs ADD COLUMN {column} TEXT")
            have_batches = {r["name"] for r in self.db.execute("PRAGMA table_info(batches)")}
            if "owner_token" not in have_batches:
                self.db.execute("ALTER TABLE batches ADD COLUMN owner_token TEXT")
            if "estimated_finish_s" not in have_batches:
                self.db.execute(
                    "ALTER TABLE batches ADD COLUMN estimated_finish_s REAL")

            duplicate_rows = self.db.execute(
                f"SELECT idempotency_key, job_id, state FROM jobs "
                "WHERE idempotency_key IS NOT NULL AND idempotency_key <> '' "
                f"AND state NOT IN ({_FINAL_Q}) "
                "ORDER BY idempotency_key, created_at, job_id",
                _FINAL,
            ).fetchall()
            duplicates: dict[str, list[str]] = {}
            for row in duplicate_rows:
                duplicates.setdefault(row["idempotency_key"], []).append(
                    f"{row['job_id']}:{row['state']}"
                )
            duplicates = {key: rows for key, rows in duplicates.items() if len(rows) > 1}
            if duplicates:
                detail = "; ".join(
                    f"{key} -> {', '.join(rows)}" for key, rows in duplicates.items()
                )
                raise QueueMigrationError(
                    "active idempotency duplicates prevent queue migration: "
                    f"{detail}. Repair explicitly by naming one canonical job_id per key "
                    "and moving each other row to a terminal state; no queue history was changed"
                )

            # Rows from any pre-cutover schema cannot be interpreted under the
            # final frozen-record protocol. Preserve their payload bytes as
            # opaque history, move every active row to review, and clear the old
            # semantic IDs so no later reader mistakes those bytes for V1.
            if "prepared_json" not in have_jobs:
                self.db.execute(
                    f"UPDATE jobs SET state='needs_review',last_error="
                    "COALESCE(last_error,'unprepared row requires explicit resubmission'),"
                    "leased_until=NULL,assigned_device=NULL,batch_id=NULL,updated_at=? "
                    f"WHERE state NOT IN ({_FINAL_Q})",
                    (utc_now_iso(), *_FINAL),
                )

                self.db.execute("DROP TABLE IF EXISTS jobs_v2")
                self.db.execute("""
                    CREATE TABLE jobs_v2 (
                        job_id TEXT PRIMARY KEY, task_name TEXT NOT NULL,
                        prepared_json TEXT, prepared_id TEXT, spec_id TEXT,
                        force_device TEXT, priority INTEGER NOT NULL DEFAULT 0,
                        idempotency_key TEXT, state TEXT NOT NULL DEFAULT 'queued',
                        attempts INTEGER NOT NULL DEFAULT 0, leased_until TEXT,
                        assigned_device TEXT, batch_id TEXT, created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL, last_error TEXT, output_manifest TEXT,
                        exclude_devices TEXT, last_result TEXT
                    )
                """)
                task_column = "task_type" if "task_type" in have_jobs else "'unknown'"
                payload_column = "payload_json" if "payload_json" in have_jobs else "NULL"
                self.db.execute(f"""
                    INSERT INTO jobs_v2
                    SELECT job_id,{task_column},
                           {payload_column},NULL,NULL,force_device,priority,idempotency_key,state,attempts,
                           leased_until,assigned_device,batch_id,created_at,updated_at,last_error,
                           output_manifest,exclude_devices,last_result FROM jobs
                """)
                self.db.execute("DROP TABLE jobs")
                self.db.execute("ALTER TABLE jobs_v2 RENAME TO jobs")

            if "task_name" not in have_batches:
                self.db.execute("ALTER TABLE batches RENAME COLUMN task_type TO task_name")

            self.db.execute("DROP INDEX IF EXISTS ix_jobs_idem")
            self.db.execute("DROP INDEX IF EXISTS ux_jobs_active_idem")
            self.db.execute("CREATE INDEX IF NOT EXISTS ix_jobs_state ON jobs(state)")
            self.db.execute("CREATE INDEX IF NOT EXISTS ix_jobs_batch ON jobs(batch_id)")
            self.db.execute(_active_idempotency_index_sql())

    def _migrate_nullable_batch_estimates(self) -> None:
        """Rebuild only the local batch ledger when its old ETA column is NOT NULL.

        SQLite cannot remove a NOT NULL constraint in place. Queue history is
        copied exactly; zero remains a real historical value, while new unknown
        estimates can be stored as NULL.
        """
        columns = self.db.execute("PRAGMA table_info(batches)").fetchall()
        eta = next((row for row in columns if row["name"] == "estimated_finish_s"), None)
        if eta is None or not int(eta["notnull"]):
            return
        with self._immediate():
            self.db.execute("DROP TABLE IF EXISTS batches_nullable")
            self.db.execute("""
                CREATE TABLE batches_nullable (
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
                    error TEXT
                )
            """)
            names = (
                "batch_id,owner_token,state,device,task_name,engine,bucket,created_at,"
                "updated_at,lease_until,heartbeat_at,estimated_finish_s,error"
            )
            self.db.execute(
                f"INSERT INTO batches_nullable ({names}) SELECT {names} FROM batches"
            )
            self.db.execute("DROP TABLE batches")
            self.db.execute("ALTER TABLE batches_nullable RENAME TO batches")

    def _migrate_prepared_output_reservations(self) -> None:
        """Backfill the durable namespace or fail closed on historical collisions."""
        from .prepared import validate_prepared_job

        with self._immediate():
            for row in self.db.execute(
                    "SELECT job_id,prepared_json FROM jobs WHERE prepared_id IS NOT NULL"):
                try:
                    prepared = json.loads(row["prepared_json"])
                    validate_prepared_job(prepared)
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise QueueMigrationError(
                        f"prepared job {row['job_id']} cannot seed output reservations: {exc}"
                    ) from exc
                for reservation in prepared["output"]["reservations"]:
                    stem = reservation["stem"]
                    owner = self.db.execute(
                        "SELECT work_id FROM prepared_output_reservations WHERE stem=?",
                        (stem,),
                    ).fetchone()
                    if owner is not None and owner["work_id"] != prepared["work_id"]:
                        raise QueueMigrationError(
                            f"prepared output reservation collision at {stem!r}; "
                            f"job {row['job_id']} conflicts with durable work {owner['work_id']}; "
                            "repair explicitly without deleting queue history"
                        )
                    if owner is None:
                        self.db.execute(
                            "INSERT INTO prepared_output_reservations(stem,work_id,created_at) "
                            "VALUES(?,?,?)", (stem, prepared["work_id"], utc_now_iso()),
                        )

    @contextlib.contextmanager
    def _immediate(self):
        """An explicit BEGIN IMMEDIATE … COMMIT (ROLLBACK on error) so a multi-statement
        lifecycle transition is atomic — a crash mid-transition can't leave split-brain
        state (e.g. jobs done but the resource lease still held)."""
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            try:
                self.db.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    def close(self) -> None:
        self.db.close()

    # --- enqueue ----------------------------------------------------------
    def enqueue_prepared(self, prepared: dict[str, Any], *, spec: dict[str, Any] | None,
                         priority: int = 0, idempotency_key: str | None = None,
                         now: str | None = None, job_id: str | None = None,
                         current_spec_id: Callable[[], str | None] | None = None) -> str:
        """Insert one fully prepared job without re-resolving any task meaning.

        Configured tasks default to prepared-id idempotency. Raw commands
        deliberately do not: two identical command submissions are two process
        invocations. The content-addressed spec row is inserted in the same
        transaction as the job.
        """
        return self.enqueue_prepared_many(
            [prepared], spec=spec, priority=priority,
            idempotency_keys=[idempotency_key], now=now, job_ids=[job_id],
            current_spec_id=current_spec_id,
        )[0]

    def enqueue_prepared_many(
        self, prepared_records: list[dict[str, Any]], *, spec: dict[str, Any] | None,
        priority: int = 0, idempotency_keys: list[str | None] | None = None,
        now: str | None = None, job_ids: list[str | None] | None = None,
        current_spec_id: Callable[[], str | None] | None = None,
    ) -> list[str]:
        """Atomically store one prepared submission, including every split row.

        A callable equality gate is evaluated *inside* ``BEGIN IMMEDIATE`` after
        route preview and preparation. A changed or unreadable definition raises
        before any spec, job, or reservation row exists.
        """
        from .prepared import (
            RAW_COMMAND_SPEC, RAW_COMMAND_SPEC_ID, validate_prepared_against_spec,
            validate_prepared_job,
        )
        from .task_contract import canonical_json, sha256_id

        if not prepared_records:
            raise ValueError("prepared enqueue requires at least one record")
        for prepared in prepared_records:
            validate_prepared_job(prepared)
        kinds = {prepared["kind"] for prepared in prepared_records}
        spec_ids = {prepared["spec_id"] for prepared in prepared_records}
        if len(kinds) != 1 or len(spec_ids) != 1:
            raise ValueError("one prepared enqueue must share one kind and spec_id")
        kind = next(iter(kinds))
        spec_id = next(iter(spec_ids))
        if kind == "task":
            if not callable(current_spec_id):
                raise ValueError(
                    "configured prepared enqueue requires a live definition authority callable")
            if spec is None or spec.get("spec_id") != spec_id:
                raise ValueError("prepared task requires its exact resolved spec")
            for prepared in prepared_records:
                validate_prepared_against_spec(prepared, spec)
            spec_blob = canonical_json(spec)
            if sha256_id({k: v for k, v in spec.items() if k != "spec_id"}) != spec_id:
                raise ValueError("resolved spec_id does not match canonical spec bytes")
        else:
            if spec is not None:
                raise ValueError("raw command does not accept a configured task spec")
            spec_blob = canonical_json({**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID})
        if idempotency_keys is None:
            idempotency_keys = [None] * len(prepared_records)
        if job_ids is None:
            job_ids = [None] * len(prepared_records)
        if len(idempotency_keys) != len(prepared_records) or len(job_ids) != len(prepared_records):
            raise ValueError("prepared enqueue metadata length mismatch")
        now = now or utc_now_iso()
        allocated = [job_id or uuid.uuid4().hex[:12] for job_id in job_ids]
        result: list[str] = []
        with self._immediate():
            if kind == "task":
                live_id = current_spec_id()
                if live_id != spec_id:
                    raise ValueError("task definition changed before atomic enqueue; no job was enqueued")
            existing_spec = self.db.execute(
                "SELECT canonical_json FROM prepared_specs WHERE spec_id=?", (spec_id,),
            ).fetchone()
            if existing_spec and existing_spec["canonical_json"] != spec_blob:
                raise QueueMigrationError(
                    f"prepared spec {spec_id} exists with different canonical bytes")
            if not existing_spec:
                self.db.execute(
                    "INSERT INTO prepared_specs(spec_id,schema,canonical_json,created_at) "
                    "VALUES(?,?,?,?)", (spec_id, 1, spec_blob, now))
            for prepared, requested_key, jid in zip(
                    prepared_records, idempotency_keys, allocated):
                prepared_id = prepared["prepared_id"]
                key = ((prepared_id if requested_key is None else requested_key)
                       if kind == "task" else ("" if requested_key is None else requested_key))
                for reservation in prepared["output"]["reservations"]:
                    stem = reservation["stem"]
                    owner = self.db.execute(
                        "SELECT work_id FROM prepared_output_reservations WHERE stem=?",
                        (stem,),
                    ).fetchone()
                    if owner is not None and owner["work_id"] != prepared["work_id"]:
                        raise ValueError(
                            f"output reservation collision for {stem!r} with different work")
                    if owner is None:
                        self.db.execute(
                            "INSERT INTO prepared_output_reservations(stem,work_id,created_at) "
                            "VALUES(?,?,?)", (stem, prepared["work_id"], now),
                        )
                if key:
                    row = self.db.execute(
                        "SELECT job_id, prepared_id FROM jobs WHERE idempotency_key=? "
                        f"AND state NOT IN ({_FINAL_Q})", (key, *_FINAL),
                    ).fetchone()
                    if row:
                        if row["prepared_id"] != prepared_id:
                            raise ValueError(
                                "idempotency_key collision with different prepared work")
                        result.append(row["job_id"])
                        continue
                label = prepared["task"]["name"] if kind == "task" else "__command__"
                try:
                    self.db.execute(
                        "INSERT INTO jobs (job_id,task_name,prepared_json,force_device,priority,"
                        "idempotency_key,state,created_at,updated_at,prepared_id,spec_id) "
                        "VALUES(?,?,?,?,?,?, 'queued',?,?,?,?)",
                        (jid, label, canonical_json(prepared),
                         prepared["routing"]["force_device"], priority, key,
                         now, now, prepared_id, spec_id),
                    )
                except sqlite3.IntegrityError:
                    if not key:
                        raise
                    row = self.db.execute(
                        "SELECT job_id, prepared_id FROM jobs WHERE idempotency_key=? "
                        f"AND state NOT IN ({_FINAL_Q})", (key, *_FINAL),
                    ).fetchone()
                    if row is None or row["prepared_id"] != prepared_id:
                        raise
                    jid = row["job_id"]
                result.append(jid)
        return result

    def prepared_record(self, job_id: str) -> dict[str, Any] | None:
        """Return a verified prepared record, or fail closed on corrupt bytes."""
        from .prepared import validate_prepared_job

        row = self.db.execute(
            "SELECT prepared_json, prepared_id FROM jobs "
            "WHERE job_id=? AND prepared_id IS NOT NULL",
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            record = json.loads(row["prepared_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise QueueMigrationError(f"prepared job {job_id} has malformed bytes") from exc
        validate_prepared_job(record)
        if record["prepared_id"] != row["prepared_id"]:
            raise QueueMigrationError(f"prepared job {job_id} identity columns disagree")
        return record

    def prepared_spec(self, spec_id: str) -> dict[str, Any] | None:
        from .prepared import RAW_COMMAND_SPEC, RAW_COMMAND_SPEC_ID
        from .task_contract import TaskContractError, validate_resolved_task_spec

        row = self.db.execute(
            "SELECT canonical_json FROM prepared_specs WHERE spec_id=?", (spec_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            spec = json.loads(row["canonical_json"])
        except json.JSONDecodeError as exc:
            raise QueueMigrationError(f"prepared spec {spec_id} has malformed bytes") from exc
        if spec_id == RAW_COMMAND_SPEC_ID:
            if spec != {**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID}:
                raise QueueMigrationError("raw command spec fails its content identity")
        else:
            try:
                validate_resolved_task_spec(spec)
            except TaskContractError as exc:
                raise QueueMigrationError(
                    f"prepared spec {spec_id} fails its content identity or shape: {exc}"
                ) from exc
        if spec.get("spec_id") != spec_id:
            raise QueueMigrationError(f"prepared spec {spec_id} identity column disagrees")
        return spec

    # --- read -------------------------------------------------------------
    def get(self, job_id: str) -> dict[str, Any] | None:
        row = self.db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def list(self, state: str | None = None) -> list[dict[str, Any]]:
        if state:
            rows = self.db.execute("SELECT * FROM jobs WHERE state=? ORDER BY priority DESC, created_at",
                                   (state,)).fetchall()
        else:
            rows = self.db.execute("SELECT * FROM jobs ORDER BY priority DESC, created_at").fetchall()
        return [dict(r) for r in rows]

    def counts(self) -> dict[str, int]:
        rows = self.db.execute("SELECT state, COUNT(*) c FROM jobs GROUP BY state").fetchall()
        return {r["state"]: r["c"] for r in rows}

    def active_by_device(self) -> dict[str, int]:
        """In-flight (leased/staging/running/fetching) job count per device."""
        rows = self.db.execute(
            "SELECT assigned_device d, COUNT(*) c FROM jobs "
            "WHERE state IN ('leased','staging','running','fetching') AND assigned_device IS NOT NULL "
            "GROUP BY assigned_device").fetchall()
        return {r["d"]: r["c"] for r in rows}

    def active_backlog(self, now: str | None = None) -> dict[str, float | None]:
        """Per-device REMAINING compute (seconds) of in-flight batches — the backlog the scheduler
        adds to a new job's finish estimate so it won't pile onto a busy device when an idle one
        would finish sooner. Per batch: max(0, estimated_finish_s - elapsed_since_claim).
        A configured exclusive resource pool usually means one active batch per
        device for that pool, so these do not double-count a shared wait."""
        now = now or utc_now_iso()
        now_t = _parse_iso(now)
        out: dict[str, float | None] = {}
        rows = self.db.execute(
            "SELECT device, created_at, estimated_finish_s FROM batches "
            "WHERE state IN ('leased','staging','running','fetching')").fetchall()
        for r in rows:
            if r["estimated_finish_s"] is None:
                out[r["device"]] = None
                continue
            est = float(r["estimated_finish_s"])
            if est < 0:
                continue
            started = _parse_iso(r["created_at"])
            elapsed = max(0.0, now_t - started) if (now_t and started) else 0.0
            if r["device"] in out and out[r["device"]] is None:
                continue
            out[r["device"]] = float(out.get(r["device"], 0.0)) + max(0.0, est - elapsed)
        return out

    # --- state transitions ------------------------------------------------
    def finalize_queued(self, job_id: str, error: str, *,
                        now: str | None = None) -> bool:
        """Finalize a job ONLY while it is still queued.

        The caller reads queued rows outside a transaction, so a concurrent
        dispatcher or ad-hoc run may have claimed one since. An unfenced write
        would mark a RUNNING job final and leave its batch with no active rows,
        which would also skip the resource-lease release and leak the device's
        pool mutex until the lease expired.
        """
        now = now or utc_now_iso()
        cur = self.db.execute(
            "UPDATE jobs SET state='failed_final', last_error=?, updated_at=? "
            "WHERE job_id=? AND state='queued'", (error, now, job_id))
        self.db.commit()
        return cur.rowcount == 1

    def review_queued(self, job_id: str, error: str, *,
                      now: str | None = None) -> bool:
        """Fail closed on corrupt or revoked queued semantics without racing a claimant."""
        now = now or utc_now_iso()
        cur = self.db.execute(
            "UPDATE jobs SET state='needs_review', last_error=?, updated_at=? "
            "WHERE job_id=? AND state='queued'", (error, now, job_id))
        self.db.commit()
        return cur.rowcount == 1

    # --- batch + resource-lease primitives (dispatcher) -------------------
    def claim_many(self, job_ids: list[str], device: str, *, batch_id: str,
                   lease_until: str, pool: str | None = "gpu", task_name: str = "",
                   engine: str = "", bucket: str = "", estimated_finish_s: float | None = 0.0,
                   now: str | None = None,
                   current_spec_ids: dict[str, str | None] |
                   Callable[[], dict[str, str | None]] | None = None,
                   ) -> str | None:
        """Atomically lease a whole compatible group to one device+batch.

        If ``pool`` is truthy, also acquire the per-(device,pool) resource lease —
        ALL-OR-NOTHING (BEGIN IMMEDIATE). Returns a new opaque owner token on
        success, or ``None`` without changing anything if the pool slot is held
        or any job is no longer ``queued``.
        """
        now = now or utc_now_iso()
        if not job_ids or lease_until <= now:   # never grant an already-expired lease
            return None
        owner_token = uuid.uuid4().hex
        qmarks = ",".join("?" * len(job_ids))
        try:
            self.db.execute("BEGIN IMMEDIATE")   # inside the try: lock contention -> False, not a crash
            try:
                # A live lease on this (device,pool) blocks the claim; an expired one is freed.
                if pool:
                    self.db.execute(
                        "DELETE FROM resource_leases WHERE device=? AND pool=? AND lease_until<?",
                        (device, pool, now),
                    )
                    if self.db.execute(
                        "SELECT 1 FROM resource_leases WHERE device=? AND pool=?",
                        (device, pool),
                    ).fetchone():
                        self.db.execute("ROLLBACK")
                        return None
                rows = self.db.execute(
                    f"SELECT job_id,state,spec_id,prepared_id FROM jobs WHERE job_id IN ({qmarks})",
                    tuple(job_ids)).fetchall()
                if len(rows) != len(job_ids) or any(row["state"] != "queued" for row in rows):
                    self.db.execute("ROLLBACK")
                    return None
                for row in rows:
                    if not row["prepared_id"]:
                        continue
                    try:
                        record = self.prepared_record(row["job_id"])
                        spec = self.prepared_spec(row["spec_id"])
                        if record is None or spec is None:
                            raise QueueMigrationError("missing frozen prepared bytes")
                        if record["kind"] == "task":
                            from .prepared import validate_prepared_against_spec
                            validate_prepared_against_spec(record, spec)
                    except (ValueError, QueueMigrationError) as exc:
                        self.db.execute(
                            "UPDATE jobs SET state='needs_review',last_error=?,updated_at=? "
                            "WHERE job_id=? AND state='queued'",
                            (f"prepared_integrity: {exc}", now, row["job_id"]),
                        )
                        self.db.execute("COMMIT")
                        return None
                configured_rows = []
                for row in rows:
                    if not row["prepared_id"]:
                        continue
                    record = self.prepared_record(row["job_id"])
                    if record is not None and record["kind"] == "task":
                        configured_rows.append(row)
                if configured_rows and not callable(current_spec_ids):
                    for row in configured_rows:
                        self.db.execute(
                            "UPDATE jobs SET state='needs_review',last_error=?,updated_at=? "
                            "WHERE job_id=? AND state='queued'",
                            ("definition_authority_not_live", now, row["job_id"]),
                        )
                    self.db.execute("COMMIT")
                    return None
                if configured_rows:
                    authority_unreadable = False
                    try:
                        live_spec_ids = current_spec_ids()
                    except Exception:  # noqa: BLE001 - unreadable live config is revocation
                        live_spec_ids = {row["job_id"]: None for row in configured_rows}
                        authority_unreadable = True
                    if not isinstance(live_spec_ids, dict):
                        live_spec_ids = {row["job_id"]: None for row in configured_rows}
                        authority_unreadable = True
                    drifted = [row for row in configured_rows if
                               live_spec_ids.get(row["job_id"]) != row["spec_id"]]
                    if drifted:
                        for row in drifted:
                            current = live_spec_ids.get(row["job_id"])
                            if authority_unreadable:
                                reason = "definition_authority_unreadable"
                            else:
                                reason = ("definition_missing" if current is None
                                          else "definition_changed")
                            self.db.execute(
                                "UPDATE jobs SET state='needs_review',last_error=?,updated_at=? "
                                "WHERE job_id=? AND state='queued'", (reason, now, row["job_id"]),
                            )
                        self.db.execute("COMMIT")
                        return None
                self.db.execute(
                    "INSERT INTO batches (batch_id,owner_token,state,device,task_name,engine,bucket,"
                    "created_at,updated_at,lease_until,heartbeat_at,estimated_finish_s) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (batch_id, owner_token, "leased", device, task_name, engine, bucket, now, now,
                     lease_until, now,
                     None if estimated_finish_s is None else float(estimated_finish_s)))
                if pool:
                    self.db.execute(
                        "INSERT INTO resource_leases (device,pool,batch_id,lease_until) "
                        "VALUES (?,?,?,?)",
                        (device, pool, batch_id, lease_until),
                    )
                cur = self.db.execute(
                    f"UPDATE jobs SET state='leased', assigned_device=?, batch_id=?, leased_until=?, "
                    f"attempts=attempts+1, updated_at=? WHERE job_id IN ({qmarks})",
                    (device, batch_id, lease_until, now, *job_ids))
                if cur.rowcount != len(job_ids):   # invariant guard: every job must have flipped
                    self.db.execute("ROLLBACK")
                    return None
                self.db.execute("COMMIT")
                return owner_token
            except sqlite3.Error:
                self.db.execute("ROLLBACK")
                return None
        except sqlite3.Error:
            return None   # couldn't even BEGIN (lock contention past busy_timeout)

    def revoke_prelaunch_batch(self, batch_id: str, *, owner_token: str,
                               reason: str, now: str | None = None) -> bool:
        """Definition drift recovery before the user-process launch gate."""
        now = now or utc_now_iso()
        with self._immediate():
            cur = self.db.execute(
                "UPDATE batches SET state='failed',error=?,updated_at=? WHERE batch_id=? "
                "AND state IN ('leased','staging','running') AND owner_token=? AND lease_until>?",
                (reason, now, batch_id, owner_token, now),
            )
            if not cur.rowcount:
                return False
            self.db.execute(
                "UPDATE jobs SET state='needs_review',last_error=?,leased_until=NULL,"
                "assigned_device=NULL,updated_at=? WHERE batch_id=? "
                "AND state IN ('leased','staging','running')", (reason, now, batch_id),
            )
            self.db.execute("DELETE FROM resource_leases WHERE batch_id=?", (batch_id,))
            return True

    def record_revoked_prelaunch_result(self, batch_id: str, *, owner_token: str,
                                        result_record: str,
                                        now: str | None = None) -> bool:
        """Persist a sanitized result after this owner revoked a prelaunch batch.

        The target admission/release receipt does not exist until the executor's
        launch gate returns, while :meth:`revoke_prelaunch_batch` must fence the
        batch inside that gate.  The failed batch retains its owner token, so the
        same fenced owner can attach the completed receipt afterwards without
        reviving the batch or changing the terminal ``needs_review`` disposition.
        """
        now = now or utc_now_iso()
        with self._immediate():
            owner = self.db.execute(
                "SELECT 1 FROM batches WHERE batch_id=? AND state='failed' "
                "AND owner_token=?",
                (batch_id, owner_token),
            ).fetchone()
            if owner is None:
                return False
            cur = self.db.execute(
                "UPDATE jobs SET last_result=?,updated_at=? WHERE batch_id=? "
                "AND state='needs_review'",
                (result_record, now, batch_id),
            )
            return bool(cur.rowcount)

    def set_batch_state(self, batch_id: str, state: str, *, expected_state: str,
                        owner_token: str, now: str | None = None) -> bool:
        """Perform one owner-side state transition while its fence is live."""
        now = now or utc_now_iso()
        with self._immediate():
            cur = self.db.execute(
                "UPDATE batches SET state=?, updated_at=? WHERE batch_id=? "
                "AND state=? AND owner_token=? AND lease_until>?",
                (state, now, batch_id, expected_state, owner_token, now),
            )
            if not cur.rowcount:
                return False
            self.db.execute(
                "UPDATE jobs SET state=?, updated_at=? WHERE batch_id=? AND state=?",
                (state, now, batch_id, expected_state),
            )
            return True

    def heartbeat(self, batch_id: str, lease_until: str, *, expected_state: str,
                  owner_token: str, now: str | None = None) -> bool:
        """Atomically extend a batch, its jobs, and its resource lease.

        Long-running work can outlive the initial lease. The exact owner, state,
        and still-live old lease must all match.
        """
        now = now or utc_now_iso()
        if lease_until <= now:
            return False
        with self._immediate():
            cur = self.db.execute(
                "UPDATE batches SET lease_until=?, heartbeat_at=?, updated_at=? "
                "WHERE batch_id=? AND state=? AND owner_token=? AND lease_until>?",
                (lease_until, now, now, batch_id, expected_state, owner_token, now),
            )
            if not cur.rowcount:
                return False
            self.db.execute("UPDATE resource_leases SET lease_until=? WHERE batch_id=?",
                            (lease_until, batch_id))
            self.db.execute(
                "UPDATE jobs SET leased_until=? WHERE batch_id=? AND state=?",
                (lease_until, batch_id, expected_state),
            )
            return True

    def complete_batch(self, batch_id: str, *, expected_state: str,
                       owner_token: str, now: str | None = None,
                       result_record: str | None = None,
                       observation: dict[str, Any] | None = None) -> bool:
        """Mark a batch done only for its current, unexpired owner."""
        now = now or utc_now_iso()
        with self._immediate():
            cur = self.db.execute(
                "UPDATE batches SET state='done', updated_at=? WHERE batch_id=? "
                "AND state=? AND owner_token=? AND lease_until>?",
                (now, batch_id, expected_state, owner_token, now),
            )
            if not cur.rowcount:        # already terminal / recovered by someone else — no-op
                return False
            self.db.execute("UPDATE jobs SET state='done', leased_until=NULL, "
                            # A finished row must not still explain why an
                            # earlier attempt failed; a structured completed-attempt
                            # receipt may replace it.
                            "last_error=NULL, last_result=?, updated_at=? "
                            "WHERE batch_id=? AND state=?",
                            (result_record, now, batch_id, expected_state))
            self._insert_profile_observation(batch_id, observation, now)
            self.db.execute("DELETE FROM resource_leases WHERE batch_id=?", (batch_id,))
            return True

    def complete_batch_items(self, batch_id: str, succeeded: dict[str, str | None],
                             failed: dict[str, str], *, now: str | None = None,
                             expected_state: str, owner_token: str,
                             max_attempts: int = MAX_ATTEMPTS,
                             clear_force_device: bool = False,
                             dispositions: dict[str, str] | None = None,
                             results: dict[str, str] | None = None,
                             result_record: str | None = None,
                             observation: dict[str, Any] | None = None) -> bool:
        """Finish an ACTIVE batch from per-item worker results.

        ``succeeded`` maps job_id -> optional output_manifest JSON; those jobs become ``done``.
        ``failed`` maps job_id -> error; those jobs requeue until attempts are exhausted. Any
        active job in the batch that is not mentioned is treated as failed, because a partial
        metrics file is not enough evidence to mark it done. ``clear_force_device`` turns a
        retryable forced-device failure into an auto-placed retry (used only by opt-in fallback).
        Returns True iff this call performed the transition (False = already terminal/recovered).
        """
        now = now or utc_now_iso()
        dispositions = dispositions or {}
        results = results or {}
        with self._immediate():
            owner = self.db.execute(
                "SELECT 1 FROM batches WHERE batch_id=? AND state=? "
                "AND owner_token=? AND lease_until>?",
                (batch_id, expected_state, owner_token, now),
            ).fetchone()
            if owner is None:
                return False
            rows = self.db.execute(
                "SELECT job_id, attempts, force_device, assigned_device, "
                "exclude_devices FROM jobs WHERE batch_id=? AND state=?",
                (batch_id, expected_state)).fetchall()
            active_rows = {r["job_id"]: r for r in rows}
            active_ids = set(active_rows)
            if not active_ids:
                return False
            explicit_failed = active_ids & set(failed)
            done_ids = (active_ids & set(succeeded)) - explicit_failed
            failed_ids = (active_ids - done_ids) | explicit_failed
            batch_state = "failed" if failed_ids else "done"
            batch_error = "; ".join(
                f"{jid}: {failed.get(jid) or 'missing item result'}"
                for jid in sorted(failed_ids)[:5])
            cur = self.db.execute(
                "UPDATE batches SET state=?, error=?, updated_at=? WHERE batch_id=? "
                "AND state=? AND owner_token=? AND lease_until>?",
                (batch_state, batch_error or None, now, batch_id,
                 expected_state, owner_token, now))
            if not cur.rowcount:
                return False
            for jid in done_ids:
                self.db.execute(
                    "UPDATE jobs SET state='done', leased_until=NULL, output_manifest=?, "
                    # A row that eventually succeeded must not still carry the
                    # record of why an earlier attempt failed; retain a sanitized
                    # completed-attempt receipt when one exists.
                    "last_error=NULL, last_result=?, updated_at=? "
                    "WHERE job_id=? AND batch_id=? AND state=?",
                    (succeeded.get(jid), result_record, now, jid, batch_id, expected_state))
            for jid in failed_ids:
                self._finish_failed_item(
                    active_rows[jid], batch_id,
                    failed.get(jid) or "missing item result",
                    dispositions.get(jid, "retry"), results.get(jid), now,
                    max_attempts, clear_force_device)
            self._insert_profile_observation(batch_id, observation, now)
            self.db.execute("DELETE FROM resource_leases WHERE batch_id=?", (batch_id,))
            return True

    @staticmethod
    def _finite_optional(value: Any, name: str) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"profile observation {name} must be numeric or null")
        number = float(value)
        if not math.isfinite(number) or number < 0:
            raise ValueError(f"profile observation {name} must be finite and nonnegative")
        return number

    def _insert_profile_observation(self, batch_id: str,
                                    observation: dict[str, Any] | None,
                                    recorded_at: str) -> None:
        """Insert one raw observation inside the caller's terminal transaction."""
        if observation is None:
            return
        expected = {
            "profile_key", "family_id", "device", "adapter_id", "prepared_units",
            "observed_units", "controller_elapsed_s", "worker_elapsed_s", "peak_rss_mb",
            "peak_vram_mb", "accepted_duration", "reject_reason", "result_digest",
        }
        if not isinstance(observation, dict) or set(observation) != expected:
            raise ValueError("profile observation has unknown or missing fields")
        device = observation["device"]
        if not isinstance(device, str) or not device:
            raise ValueError("profile observation device must be non-empty")
        accepted = observation["accepted_duration"]
        if type(accepted) is not bool:
            raise ValueError("profile observation accepted_duration must be boolean")
        values = {
            name: self._finite_optional(observation[name], name)
            for name in (
                "prepared_units", "observed_units", "controller_elapsed_s", "worker_elapsed_s",
                "peak_rss_mb", "peak_vram_mb",
            )
        }
        digest = observation["result_digest"]
        is_digest = lambda value: (  # noqa: E731
            isinstance(value, str) and len(value) == 71 and value.startswith("sha256:")
            and all(char in "0123456789abcdef" for char in value[7:])
        )
        if digest is not None and not is_digest(digest):
            raise ValueError("profile observation result_digest must be a sha256 identity or null")
        if accepted and (
                values["prepared_units"] is None
                or values["observed_units"] is None
                or values["controller_elapsed_s"] is None
                or values["controller_elapsed_s"] <= 0
                or not is_digest(observation["profile_key"])
                or not is_digest(observation["family_id"])
                or not is_digest(observation["adapter_id"])
                or observation["reject_reason"] is not None):
            raise ValueError(
                "accepted duration requires verified work, profile identities, and positive elapsed time"
            )
        if not accepted and (not isinstance(observation["reject_reason"], str)
                             or not observation["reject_reason"]):
            raise ValueError("rejected duration requires a reason")
        self.db.execute(
            "INSERT INTO fleet_profile_observations ("
            "batch_id,profile_key,family_id,device,adapter_id,prepared_units,observed_units,"
            "controller_elapsed_s,worker_elapsed_s,peak_rss_mb,peak_vram_mb,accepted_duration,"
            "reject_reason,result_digest,recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                batch_id,
                observation["profile_key"],
                observation["family_id"],
                device,
                observation["adapter_id"],
                values["prepared_units"],
                values["observed_units"],
                values["controller_elapsed_s"],
                values["worker_elapsed_s"],
                values["peak_rss_mb"],
                values["peak_vram_mb"],
                1 if accepted else 0,
                observation["reject_reason"],
                observation["result_digest"],
                recorded_at,
            ),
        )

    def profile_observations(self, *, batch_id: str | None = None,
                             profile_key: str | None = None) -> list[dict[str, Any]]:
        """Read retained raw observations for deterministic profile rebuilding."""
        clauses: list[str] = []
        values: list[str] = []
        if batch_id is not None:
            clauses.append("batch_id=?")
            values.append(batch_id)
        if profile_key is not None:
            clauses.append("profile_key=?")
            values.append(profile_key)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.db.execute(
            "SELECT * FROM fleet_profile_observations" + where + " ORDER BY recorded_at,batch_id",
            tuple(values),
        ).fetchall()
        return [dict(row) for row in rows]

    def _finish_failed_item(self, row: sqlite3.Row, batch_id: str, error: str,
                            disposition: str, record: str | None, now: str,
                            max_attempts: int, clear_force_device: bool) -> None:
        """Apply ONE failed item's disposition, inside the caller's transaction.

        Five rules in SQL CASEs would be unreadable, and the row is already
        fetched inside BEGIN IMMEDIATE, so deciding the next state here is
        equivalent and legible. An unrecognized disposition keeps the historical
        bounded retry, so an unstructured worker behaves exactly as before.
        """
        device = row["assigned_device"]
        attempts = int(row["attempts"] or 0)
        force_device = row["force_device"]
        exclude = [d for d in str(row["exclude_devices"] or "").split(",") if d]
        elsewhere = disposition in ("elsewhere", "once_elsewhere")
        if elsewhere and device and device not in exclude:
            exclude.append(device)
        # "At most one more attempt, and not here" — deliberately NOT a guarantee
        # of one further encode. `attempts` counts CLAIMS, including any lost to
        # stale-lease recovery or a transport failure, so a job that has already
        # burned two claims finalizes on its first encode failure. That is
        # conservative availability loss, not an accounting error, and it is the
        # reason this needs no extra durable counter.
        cap = min(max_attempts, 2) if disposition == "once_elsewhere" else max_attempts

        if disposition == "review":
            state = "needs_review"
        elif disposition == "final" or attempts >= cap:
            state = "failed_final"
        elif elsewhere and force_device and force_device == device \
                and not clear_force_device:
            # Pinned to the one device that has proven it cannot serve this job:
            # requeueing would leave a row no placement can ever satisfy.
            state = "failed_final"
        else:
            state = "queued"
        requeued = state == "queued"
        active = ",".join("?" * len(_BATCH_ACTIVE))
        self.db.execute(
            "UPDATE jobs SET state=?, assigned_device=?, batch_id=?, "
            "force_device=CASE WHEN ? THEN NULL ELSE force_device END, "
            "exclude_devices=?, leased_until=NULL, last_error=?, last_result=?, "
            f"updated_at=? WHERE job_id=? AND batch_id=? AND state IN ({active})",
            (state, (None if requeued else device),
             (None if requeued else batch_id),
             1 if (requeued and clear_force_device) else 0,
             ",".join(exclude) or None, error, record, now,
             row["job_id"], batch_id, *_BATCH_ACTIVE))

    def fail_batch(self, batch_id: str, error: str, *, expected_state: str,
                   owner_token: str, now: str | None = None,
                   max_attempts: int = MAX_ATTEMPTS,
                   clear_force_device: bool = False,
                   result_record: str | None = None,
                   observation: dict[str, Any] | None = None) -> bool:
        """Fail a batch atomically only for its current, unexpired owner.

        Requeue jobs that have attempts left (clearing their batch + device),
        finalize the rest, and drop the resource lease so the device frees up.
        ``clear_force_device`` unpins retryable rows for explicit fallback. Returns True iff this
        call performed the transition (False = already terminal/recovered)."""
        now = now or utc_now_iso()
        with self._immediate():
            cur = self.db.execute(
                "UPDATE batches SET state='failed', error=?, updated_at=? WHERE batch_id=? "
                "AND state=? AND owner_token=? AND lease_until>?",
                (error, now, batch_id, expected_state, owner_token, now),
            )
            if not cur.rowcount:        # already terminal / recovered by someone else — no-op
                return False
            self._finish_batch_failure(
                batch_id, error, now, max_attempts, clear_force_device, result_record,
            )
            self._insert_profile_observation(batch_id, observation, now)
            return True

    def mark_completion_unknown(self, batch_id: str, error: str, *, expected_state: str,
                                owner_token: str, now: str | None = None,
                                result_record: str | None = None,
                                observation: dict[str, Any] | None = None) -> bool:
        """Fence ambiguous post-launch work without authorizing an automatic replay."""
        if expected_state not in {"running", "fetching"}:
            return False
        now = now or utc_now_iso()
        with self._immediate():
            cur = self.db.execute(
                "UPDATE batches SET state='failed',error=?,updated_at=? WHERE batch_id=? "
                "AND state=? AND owner_token=? AND lease_until>?",
                (error, now, batch_id, expected_state, owner_token, now),
            )
            if not cur.rowcount:
                return False
            self.db.execute(
                "UPDATE jobs SET state='completion_unknown',leased_until=NULL,last_error=?,"
                "last_result=?,updated_at=? WHERE batch_id=? AND state=?",
                (error, result_record, now, batch_id, expected_state),
            )
            self._insert_profile_observation(batch_id, observation, now)
            self.db.execute("DELETE FROM resource_leases WHERE batch_id=?", (batch_id,))
            return True

    def _finish_batch_failure(self, batch_id: str, error: str, now: str,
                              max_attempts: int,
                              clear_force_device: bool = False,
                              result_record: str | None = None) -> None:
        """Apply job/lease failure effects after the batch row was fenced."""
        self.db.execute(
            "UPDATE jobs SET "
            "state=CASE WHEN attempts < ? THEN 'queued' ELSE 'failed_final' END, "
            "assigned_device=CASE WHEN attempts < ? THEN NULL ELSE assigned_device END, "
            "batch_id=CASE WHEN attempts < ? THEN NULL ELSE batch_id END, "
            "force_device=CASE WHEN attempts < ? AND ? THEN NULL ELSE force_device END, "
            "leased_until=NULL, last_error=?, last_result=?, updated_at=? "
            f"WHERE batch_id=? AND state NOT IN ({_FINAL_Q})",
            (max_attempts, max_attempts, max_attempts, max_attempts,
             1 if clear_force_device else 0, error, result_record, now, batch_id, *_FINAL),
        )
        self.db.execute("DELETE FROM resource_leases WHERE batch_id=?", (batch_id,))

    def _expire_batch(self, batch_id: str, *, now: str,
                      max_attempts: int = MAX_ATTEMPTS) -> bool:
        """Recovery-only transition authorized solely by an expired lease."""
        error = "lease expired (stale recovery)"
        with self._immediate():
            row = self.db.execute(
                "SELECT state FROM batches WHERE batch_id=? AND lease_until<?",
                (batch_id, now),
            ).fetchone()
            if row is None or row["state"] not in _BATCH_ACTIVE:
                return False
            state = str(row["state"])
            cur = self.db.execute(
                "UPDATE batches SET state='failed', error=?, updated_at=? WHERE batch_id=? "
                "AND state=? AND lease_until<?",
                (error, now, batch_id, state, now),
            )
            if not cur.rowcount:
                return False
            if state in {"leased", "staging"}:
                # Launch was never authorized, so bounded retry is safe.
                self._finish_batch_failure(batch_id, error, now, max_attempts)
            elif self._batch_replay_policy(batch_id) == "idempotent-v1":
                self._finish_batch_failure(batch_id, error, now, max_attempts)
            else:
                # Running/fetching means launch happened or may have happened.
                # Without a target-side deduplication receipt, replay could repeat
                # external effects. Hold the idempotency key until explicit review.
                self.db.execute(
                    "UPDATE jobs SET state='completion_unknown',leased_until=NULL,"
                    "last_error=?,updated_at=? WHERE batch_id=? "
                    "AND state IN ('running','fetching')",
                    ("completion unknown after lease expiry", now, batch_id),
                )
                self.db.execute("DELETE FROM resource_leases WHERE batch_id=?", (batch_id,))
            return True

    def _batch_replay_policy(self, batch_id: str) -> str:
        """Frozen replay policy for a compatible batch; commands are at-most-once."""
        row = self.db.execute(
            "SELECT j.spec_id,s.canonical_json FROM jobs j "
            "LEFT JOIN prepared_specs s ON s.spec_id=j.spec_id "
            "WHERE j.batch_id=? LIMIT 1",
            (batch_id,),
        ).fetchone()
        if row is None:
            return "at-most-once-v1"
        try:
            spec = json.loads(row["canonical_json"] or "{}")
            if spec.get("kind") == "command":
                return "at-most-once-v1"
            replay = spec["definition"]["execution"]["replay"]
        except (KeyError, TypeError, json.JSONDecodeError):
            return "at-most-once-v1"
        return replay if replay in {"at-most-once-v1", "idempotent-v1"} \
            else "at-most-once-v1"

    def batch_replay_policy(self, batch_id: str) -> str:
        """Return the frozen replay declaration for one claimed batch."""
        return self._batch_replay_policy(batch_id)

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        row = self.db.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        return dict(row) if row else None

    def lease_usage(self, now: str | None = None) -> dict[str, dict[str, int]]:
        """Held resource-pool slots per device: ``{device: {pool: count}}`` for leases still
        in effect. The dispatcher feeds this into ``build_snapshot(pool_used=...)`` so placement
        won't pick a device whose configured pool is already leased by another dispatcher/run."""
        now = now or utc_now_iso()
        rows = self.db.execute(
            "SELECT device, pool, COUNT(*) c FROM resource_leases WHERE lease_until>? "
            "GROUP BY device, pool", (now,)).fetchall()
        out: dict[str, dict[str, int]] = {}
        for r in rows:
            out.setdefault(r["device"], {})[r["pool"]] = int(r["c"])
        return out

    def active_batches_by_device(self, now: str | None = None) -> dict[str, int]:
        """Live execution claims per device, counted by worker invocation."""
        now = now or utc_now_iso()
        active = ",".join("?" * len(_BATCH_ACTIVE))
        rows = self.db.execute(
            f"SELECT device, COUNT(*) c FROM batches WHERE state IN ({active}) "
            "AND lease_until>? GROUP BY device",
            (*_BATCH_ACTIVE, now),
        ).fetchall()
        return {row["device"]: int(row["c"]) for row in rows}

    def batch_attempts(self, batch_id: str) -> int:
        """Max attempt count among a batch's jobs (for failure backoff scaling)."""
        row = self.db.execute("SELECT MAX(attempts) m FROM jobs WHERE batch_id=?",
                              (batch_id,)).fetchone()
        return int(row["m"]) if row and row["m"] is not None else 1

    def recover_stale(self, now: str | None = None) -> int:
        """Fail any batch whose lease expired (controller/worker died mid-flight) — which
        requeues its jobs and frees the device — plus orphan leases and singly-claimed
        stale jobs. Returns the number of stale batches recovered."""
        now = now or utc_now_iso()
        stale = self.db.execute(
            "SELECT batch_id FROM batches WHERE state NOT IN ('done','failed') AND lease_until<?",
            (now,)).fetchall()
        recovered = 0
        for r in stale:
            recovered += int(self._expire_batch(r["batch_id"], now=now))
        self.db.execute("DELETE FROM resource_leases WHERE lease_until<?", (now,))
        return recovered

    # --- cooldowns (Phase 3d failure backoff) -----------------------------
    def set_cooldown(self, device: str, until: str, *, engine: str = "", kind: str = "",
                     reason: str = "", now: str | None = None) -> None:
        """Set/extend a (device, engine) cooldown. ``engine=''`` is device-wide. Idempotent
        upsert; a later/longer ``until`` always wins so re-failures only push it further out."""
        now = now or utc_now_iso()
        self.db.execute(
            "INSERT INTO cooldowns (device,engine,until,kind,reason,created_at) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(device,engine) DO UPDATE SET "
            "until=MAX(excluded.until, cooldowns.until), kind=excluded.kind, "
            "reason=excluded.reason, created_at=excluded.created_at",
            (device, engine, until, kind, reason, now))
        self.db.commit()

    def active_cooldowns(self, now: str | None = None) -> list[dict[str, Any]]:
        """All cooldown rows still in effect (``until`` in the future)."""
        now = now or utc_now_iso()
        rows = self.db.execute("SELECT * FROM cooldowns WHERE until > ?", (now,)).fetchall()
        return [dict(r) for r in rows]

    def is_cooled(self, device: str, engine: str, now: str | None = None) -> bool:
        """True if a device-wide OR (device, engine) cooldown is active."""
        now = now or utc_now_iso()
        row = self.db.execute(
            "SELECT 1 FROM cooldowns WHERE device=? AND engine IN ('', ?) AND until > ? LIMIT 1",
            (device, engine, now)).fetchone()
        return row is not None

    def prune_cooldowns(self, now: str | None = None) -> int:
        now = now or utc_now_iso()
        cur = self.db.execute("DELETE FROM cooldowns WHERE until <= ?", (now,))
        return cur.rowcount

    def clear(self, *, include_final: bool = False) -> dict[str, int]:
        """Manual reset to unstick the fleet (e.g. a job forced to a device that can't fit it, or
        a dispatcher that died holding a lease). Removes all QUEUED + in-flight jobs and their
        batches, releases ALL resource leases, and clears ALL cooldowns. ``include_final`` also
        drops done/failed_final history. Returns counts of what was removed. Does NOT touch any
        worker already running on a device — kill that separately if needed."""
        with self._immediate():
            if include_final:
                jobs = self.db.execute("DELETE FROM jobs").rowcount
                self.db.execute("DELETE FROM batches")
            else:
                jobs = self.db.execute(
                    f"DELETE FROM jobs WHERE state NOT IN ({_FINAL_Q})", _FINAL).rowcount
                self.db.execute("DELETE FROM batches WHERE state NOT IN ('done','failed')")
            leases = self.db.execute("DELETE FROM resource_leases").rowcount
            cooldowns = self.db.execute("DELETE FROM cooldowns").rowcount
        return {"jobs": jobs, "leases": leases, "cooldowns": cooldowns}

    def prune_final(self, keep: int = 500) -> int:
        """Bound the table: drop all but the most-recent ``keep`` final (done/failed_final)
        jobs, and any batch with no remaining jobs. Returns rows deleted."""
        rows = self.db.execute(
            f"SELECT job_id FROM jobs WHERE state IN "
            f"({','.join('?' * len(_PRUNABLE))}) ORDER BY updated_at DESC",
            _PRUNABLE).fetchall()
        victims = [r["job_id"] for r in rows[keep:]]
        with self._immediate():
            for jid in victims:
                self.db.execute("DELETE FROM jobs WHERE job_id=?", (jid,))
            # Never delete a batch that still holds a live resource lease (would orphan it).
            self.db.execute(
                "DELETE FROM batches WHERE state IN ('done','failed') "
                "AND batch_id NOT IN (SELECT DISTINCT batch_id FROM jobs WHERE batch_id IS NOT NULL) "
                "AND batch_id NOT IN (SELECT batch_id FROM resource_leases)")
        return len(victims)


class BatchHeartbeat:
    """Keep one exact owner/state lease live around synchronous remote work.

    Each heartbeat uses its own SQLite connection because connections are not
    thread-safe. Queue-open, renewal, or fence failure all mean ownership can no
    longer be proved; callers must stop any later remote work and must not record
    completion. The running remote call itself is synchronous and cannot be
    cancelled here.
    """

    def __init__(self, db_path: Path, batch_id: str, owner_token: str,
                 expected_state: str, lease_seconds: int, *,
                 interval_s: float | None = None) -> None:
        self.db_path = db_path
        self.batch_id = batch_id
        self.owner_token = owner_token
        self.expected_state = expected_state
        self.lease_seconds = lease_seconds
        self.interval = (
            interval_s if interval_s is not None
            else max(0.1, min(60.0, lease_seconds / 3.0))
        )
        self._stop = threading.Event()
        self.ownership_lost = threading.Event()
        self._state_lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def _renew(self, queue: FleetQueue) -> bool:
        with self._state_lock:
            now = utc_now_iso()
            # Queue timestamps have one-second resolution. A two-second floor keeps
            # a deliberately tiny one-second test lease from becoming equal to
            # ``now`` at the next tick before the sub-second heartbeat can advance it.
            extension = max(2, self.lease_seconds)
            return queue.heartbeat(
                self.batch_id, iso_plus_seconds(now, extension),
                expected_state=self.expected_state,
                owner_token=self.owner_token, now=now,
            )

    def transition(self, queue: FleetQueue, state: str) -> bool:
        """Move the fenced batch and heartbeat expectation as one local critical section."""
        with self._state_lock:
            if self.ownership_lost.is_set():
                return False
            if not queue.set_batch_state(
                self.batch_id,
                state,
                expected_state=self.expected_state,
                owner_token=self.owner_token,
            ):
                self.ownership_lost.set()
                return False
            self.expected_state = state
            return True

    def __enter__(self) -> "BatchHeartbeat":
        queue: FleetQueue | None = None
        try:
            queue = FleetQueue(self.db_path)
            if not self._renew(queue):
                self.ownership_lost.set()
                return self
        except Exception:  # noqa: BLE001 - inability to prove ownership is loss
            self.ownership_lost.set()
            return self
        finally:
            if queue is not None:
                queue.close()
        self._thread = threading.Thread(
            target=self._loop, name=f"hb-{self.batch_id}", daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval + 5.0)

    def _loop(self) -> None:
        queue: FleetQueue | None = None
        try:
            queue = FleetQueue(self.db_path)
            while not self._stop.wait(self.interval):
                if not self._renew(queue):
                    self.ownership_lost.set()
                    return
        except Exception:  # noqa: BLE001 - inability to prove ownership is loss
            self.ownership_lost.set()
        finally:
            if queue is not None:
                queue.close()
