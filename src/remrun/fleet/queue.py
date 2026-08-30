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
from .models import MAX_PLACEMENT_EXPLANATION_BYTES, validate_placement_explanation
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
    state        TEXT NOT NULL,          -- leased|staging|running|fetching|cancelling|done|failed
    device       TEXT NOT NULL,
    task_name    TEXT, engine TEXT, bucket TEXT,
    created_at   TEXT NOT NULL, updated_at TEXT NOT NULL,
    lease_until  TEXT NOT NULL, heartbeat_at TEXT,
    estimated_finish_s REAL,                      -- NULL means duration is honestly unknown
    placement_json TEXT,                          -- bounded schema-1 decision explanation
    target_protocol_version INTEGER,
    target_operation_id TEXT,
    target_request_sha256 TEXT,
    target_resume_token TEXT,
    target_reserved_at TEXT,
    target_accepted_at TEXT,
    target_cleanup_state TEXT,
    target_cleanup_at TEXT,
    target_stage_cleaned_at TEXT,
    target_durable_cleaned_at TEXT,
    target_finalized_at TEXT,
    target_finalization_disposition TEXT,
    target_release_authorized_at TEXT,
    target_release_effects_acknowledged_at TEXT,
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
-- One caller action may create several queue rows.  The submission is the
-- stable controller-local identity; request_id is an optional caller-owned
-- retry key and plan_id identifies an exact saved placement.
CREATE TABLE IF NOT EXISTS submissions (
    submission_id TEXT PRIMARY KEY NOT NULL,
    request_id    TEXT UNIQUE,
    plan_id       TEXT UNIQUE,
    fingerprint   TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS submission_jobs (
    submission_id   TEXT NOT NULL,
    item_index      INTEGER NOT NULL,
    job_id          TEXT NOT NULL,
    prepared_id     TEXT NOT NULL,
    PRIMARY KEY (submission_id, item_index)
);
CREATE INDEX IF NOT EXISTS ix_submission_jobs_job ON submission_jobs(job_id);
-- Saved plans are controller-local immutable prepared records with their
-- selected devices already frozen into normal PreparedJob identities.
CREATE TABLE IF NOT EXISTS submission_plans (
    plan_id                 TEXT PRIMARY KEY NOT NULL,
    plan_digest             TEXT NOT NULL,
    plan_json               TEXT NOT NULL,
    created_at              TEXT NOT NULL,
    consumed_submission_id  TEXT
);
-- A saved plan that reached the live definition fence and was refused.  This
-- is deliberately separate from submissions: the caller must be able to
-- recover a terminal refusal without manufacturing a fake submission row.
CREATE TABLE IF NOT EXISTS submission_refusals (
    refusal_id       TEXT PRIMARY KEY NOT NULL,
    plan_id          TEXT NOT NULL,
    request_id       TEXT UNIQUE,
    fingerprint      TEXT NOT NULL,
    reason_code      TEXT NOT NULL,
    created_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_submission_refusals_plan ON submission_refusals(plan_id);
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
    memory_metric        TEXT,
    accepted_duration    INTEGER NOT NULL,
    reject_reason        TEXT,
    result_digest        TEXT,
    recorded_at          TEXT NOT NULL
);
"""
_SCHEMA_OBJECTS = {
    "jobs", "ix_jobs_state", "ix_jobs_batch", "batches", "resource_leases",
    "cooldowns", "prepared_specs", "prepared_output_reservations", "submissions",
    "submission_jobs", "ix_submission_jobs_job", "submission_plans",
    "submission_refusals", "ix_submission_refusals_plan",
    "fleet_profile_observations",
}

# Terminal rows do not participate in active idempotency. ``needs_review`` is a
# durable worker answer; ``cancelled`` is retained evidence that permits an
# explicitly new submission while never being selected by dispatch.
_FINAL = ("done", "failed_final", "needs_review", "cancelled")
_FINAL_Q = ",".join("?" * len(_FINAL))
# History that volume may evict. A review answer is waiting for a PERSON, so it
# is never discarded to make room for ordinary throughput.
_PRUNABLE = ("done", "failed_final")
_BATCH_EXECUTING = ("leased", "staging", "running", "fetching")
# ``cancelling`` is a durable controller intent that revokes the execution
# owner before the target-side stop RPC. It remains active only for retry and
# dedicated cancellation recovery.
_BATCH_ACTIVE = (*_BATCH_EXECUTING, "cancelling")
MAX_ATTEMPTS = 3
_BUSY_TIMEOUT_MS = 30_000

FINALIZATION_PENDING = "known_outcome_cleanup_pending"
FINALIZATION_REPLAYABLE = "proven_no_start_replayable"
FINALIZATION_FENCED = "completion_unknown_or_started"
FINALIZATION_FINALIZED = "fully_finalized"
FINALIZATION_MALFORMED = "malformed_target_identity"
FINALIZATION_RECONCILE = "target_reconciliation_pending"
_FINALIZATION_DISPOSITIONS = {
    FINALIZATION_PENDING,
    FINALIZATION_REPLAYABLE,
    FINALIZATION_FENCED,
    FINALIZATION_FINALIZED,
    FINALIZATION_MALFORMED,
    FINALIZATION_RECONCILE,
}
_TARGET_COLUMNS = (
    "target_protocol_version", "target_operation_id", "target_request_sha256",
    "target_resume_token", "target_reserved_at", "target_accepted_at",
    "target_cleanup_state", "target_cleanup_at", "target_stage_cleaned_at",
    "target_durable_cleaned_at", "target_finalized_at",
    "target_finalization_disposition", "target_release_authorized_at",
    "target_release_effects_acknowledged_at",
)

_SUBMISSION_REFUSAL_SCHEMA = "remrun.fleet.submission-refusal"
_SUBMISSION_REFUSAL_VERSION = 1
_TASK_SPEC_DRIFT = "task_spec_drift"


class QueueConfigurationError(RuntimeError):
    """The local SQLite runtime or database cannot honor the queue contract."""


class QueueMigrationError(RuntimeError):
    """Existing queue state needs an explicit owner-directed repair."""


class TaskSpecDriftRefusal(ValueError):
    """A saved-plan identity was durably refused because its task spec drifted.

    The document is the closed protocol returned by both submit and exact
    request-status lookup.  It intentionally contains no task definition,
    payload, or live spec value.
    """

    def __init__(self, document: dict[str, Any]) -> None:
        self.document = dict(document)
        super().__init__("saved plan refused: task_spec_drift")


def _encode_placement_explanation(value: dict[str, Any] | None, device: str) -> str | None:
    if value is None:
        return None
    validate_placement_explanation(value, device)
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    if len(encoded.encode("utf-8")) > MAX_PLACEMENT_EXPLANATION_BYTES:
        raise ValueError("placement explanation exceeds the durable byte limit")
    return encoded


def _decode_placement_explanation(value: Any, device: str | None = None) -> dict | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value.encode("utf-8")) > MAX_PLACEMENT_EXPLANATION_BYTES:
        raise QueueMigrationError("durable placement explanation is malformed or oversized")
    try:
        explanation = json.loads(value)
        validate_placement_explanation(explanation, device)
    except (json.JSONDecodeError, ValueError) as exc:
        raise QueueMigrationError("durable placement explanation is malformed") from exc
    return explanation


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
            self.db.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
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
            self._ensure_schema()
            self._migrate()
            self._migrate_nullable_batch_estimates()
            self._migrate_prepared_output_reservations()
            self._migrate_profile_observations()
            self._verify_submission_control_schema()
        except BaseException:
            self.db.close()
            raise

    def _ensure_schema(self) -> None:
        """Create missing schema objects without taking a DDL lock on every open."""
        present = {
            row["name"]
            for row in self.db.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','index')"
            )
        }
        if not _SCHEMA_OBJECTS <= present:
            self.db.executescript(_SCHEMA)

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
                           "heartbeat_at", "estimated_finish_s", "placement_json",
                           "target_protocol_version",
                           "target_operation_id",
                           "target_request_sha256", "target_resume_token",
                           "target_reserved_at", "target_accepted_at",
                           "target_cleanup_state", "target_cleanup_at",
                           "target_stage_cleaned_at", "target_durable_cleaned_at",
                           "target_finalized_at", "target_finalization_disposition",
                           "target_release_authorized_at",
                           "target_release_effects_acknowledged_at",
                           "error"}
        index_row = self.db.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' "
            "AND name='ux_jobs_active_idem'"
        ).fetchone()
        expected_index = " ".join(_active_idempotency_index_sql().lower().split())
        have_unique = bool(
            index_row and " ".join(str(index_row["sql"] or "").lower().split()) == expected_index
        )
        if not (have_jobs == current_jobs and have_batches == current_batches and have_unique):
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
                if "placement_json" not in have_batches:
                    self.db.execute("ALTER TABLE batches ADD COLUMN placement_json TEXT")
                if "target_protocol_version" not in have_batches:
                    self.db.execute(
                        "ALTER TABLE batches ADD COLUMN target_protocol_version INTEGER"
                    )
                for column in (
                    "target_operation_id",
                    "target_request_sha256",
                    "target_resume_token",
                    "target_reserved_at",
                    "target_accepted_at",
                    "target_cleanup_state",
                    "target_cleanup_at",
                    "target_stage_cleaned_at",
                    "target_durable_cleaned_at",
                    "target_finalized_at",
                    "target_finalization_disposition",
                    "target_release_authorized_at",
                    "target_release_effects_acknowledged_at",
                ):
                    if column not in have_batches:
                        self.db.execute(f"ALTER TABLE batches ADD COLUMN {column} TEXT")

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

        self._migrate_target_rows()

    @staticmethod
    def _valid_target_digest(value: Any) -> bool:
        return isinstance(value, str) and len(value) == 64 and all(
            char in "0123456789abcdef" for char in value
        )

    @classmethod
    def _target_identity_complete(cls, row: sqlite3.Row) -> bool:
        required = (
            row["target_operation_id"], row["target_resume_token"],
            row["target_reserved_at"],
        )
        if not all(isinstance(value, str) and value for value in required):
            return False
        if not cls._valid_target_digest(row["target_request_sha256"]):
            return False
        accepted = row["target_accepted_at"]
        return accepted is None or (isinstance(accepted, str) and bool(accepted))

    @classmethod
    def _target_identity_present(cls, row: sqlite3.Row) -> bool:
        return any(row[name] is not None for name in _TARGET_COLUMNS[1:])

    def _migrate_target_rows(self) -> None:
        """Classify predecessor target rows without fabricating missing identity.

        A NULL protocol marker is legacy only when no target field exists. Any
        predecessor target payload, including a malformed or partial one, is
        routed away from generic stale recovery and retained for target-aware
        reconciliation or owner review.
        """
        with self._immediate():
            rows = self.db.execute(
                "SELECT * FROM batches WHERE target_protocol_version IS NULL"
            ).fetchall()
            now = utc_now_iso()
            for row in rows:
                if not self._target_identity_present(row):
                    continue
                if self._target_identity_complete(row):
                    if row["state"] in {"done", "failed"}:
                        # Batch terminality alone is not a known job outcome.  The
                        # predecessor represented acceptance/completion ambiguity as
                        # a failed batch whose attached jobs were completion_unknown.
                        # Fail closed for every still-associated non-final job state;
                        # ordinary retryable known failures have already detached
                        # their queued jobs from this historical batch.
                        unresolved = self.db.execute(
                            f"SELECT 1 FROM jobs WHERE batch_id=? "
                            f"AND state NOT IN ({_FINAL_Q}) LIMIT 1",
                            (row["batch_id"], *_FINAL),
                        ).fetchone()
                        if unresolved is not None:
                            disposition = FINALIZATION_FENCED
                        elif row["target_finalized_at"] is not None:
                            disposition = FINALIZATION_FINALIZED
                        else:
                            disposition = FINALIZATION_PENDING
                    else:
                        disposition = FINALIZATION_RECONCILE
                else:
                    disposition = FINALIZATION_MALFORMED
                self.db.execute(
                    "UPDATE batches SET target_protocol_version=1,"
                    "target_finalization_disposition=?,updated_at=? "
                    "WHERE batch_id=? AND target_protocol_version IS NULL",
                    (disposition, now, row["batch_id"]),
                )

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
                    placement_json TEXT,
                    target_protocol_version INTEGER,
                    target_operation_id TEXT,
                    target_request_sha256 TEXT,
                    target_resume_token TEXT,
                    target_reserved_at TEXT,
                    target_accepted_at TEXT,
                    target_cleanup_state TEXT,
                    target_cleanup_at TEXT,
                    target_stage_cleaned_at TEXT,
                    target_durable_cleaned_at TEXT,
                    target_finalized_at TEXT,
                    target_finalization_disposition TEXT,
                    target_release_authorized_at TEXT,
                    target_release_effects_acknowledged_at TEXT,
                    error TEXT
                )
            """)
            names = (
                "batch_id,owner_token,state,device,task_name,engine,bucket,created_at,"
                "updated_at,lease_until,heartbeat_at,estimated_finish_s,"
                "placement_json,"
                "target_protocol_version,"
                "target_operation_id,target_request_sha256,target_resume_token,"
                "target_reserved_at,target_accepted_at,target_cleanup_state,"
                "target_cleanup_at,target_stage_cleaned_at,target_durable_cleaned_at,"
                "target_finalized_at,target_finalization_disposition,"
                "target_release_authorized_at,"
                "target_release_effects_acknowledged_at,error"
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

    def _migrate_profile_observations(self) -> None:
        """Add optional resource-metric provenance without rewriting raw history.

        Old rows remain valid duration evidence but have a NULL metric marker;
        the profile reader consequently excludes their memory values until a
        producer emits the versioned, shared-page-safe metric.
        """
        columns = {row["name"] for row in self.db.execute(
            "PRAGMA table_info(fleet_profile_observations)"
        )}
        if "memory_metric" not in columns:
            with self._immediate():
                self.db.execute(
                    "ALTER TABLE fleet_profile_observations ADD COLUMN memory_metric TEXT"
                )

    def _verify_submission_control_schema(self) -> None:
        """Fail closed unless identity tables enforce the exact durable-key contract."""
        expected_columns = {
            "submissions": (
                ("submission_id", "TEXT", 1, None, 1),
                ("request_id", "TEXT", 0, None, 0),
                ("plan_id", "TEXT", 0, None, 0),
                ("fingerprint", "TEXT", 1, None, 0),
                ("created_at", "TEXT", 1, None, 0),
            ),
            "submission_jobs": (
                ("submission_id", "TEXT", 1, None, 1),
                ("item_index", "INTEGER", 1, None, 2),
                ("job_id", "TEXT", 1, None, 0),
                ("prepared_id", "TEXT", 1, None, 0),
            ),
            "submission_plans": (
                ("plan_id", "TEXT", 1, None, 1),
                ("plan_digest", "TEXT", 1, None, 0),
                ("plan_json", "TEXT", 1, None, 0),
                ("created_at", "TEXT", 1, None, 0),
                ("consumed_submission_id", "TEXT", 0, None, 0),
            ),
            "submission_refusals": (
                ("refusal_id", "TEXT", 1, None, 1),
                ("plan_id", "TEXT", 1, None, 0),
                ("request_id", "TEXT", 0, None, 0),
                ("fingerprint", "TEXT", 1, None, 0),
                ("reason_code", "TEXT", 1, None, 0),
                ("created_at", "TEXT", 1, None, 0),
            ),
        }
        for table, expected in expected_columns.items():
            actual = tuple(
                (
                    str(row["name"]), str(row["type"]).upper(), int(row["notnull"]),
                    row["dflt_value"], int(row["pk"]),
                )
                for row in self.db.execute(f"PRAGMA table_xinfo({table})")
                if int(row["hidden"]) == 0
            )
            if actual != expected:
                raise QueueMigrationError(
                    f"{table} schema cannot enforce durable submission identity; "
                    f"expected {expected!r}, found {actual!r}"
                )

        required_unique = {
            "submissions": {("submission_id",), ("request_id",), ("plan_id",)},
            "submission_jobs": {("submission_id", "item_index")},
            "submission_plans": {("plan_id",)},
            "submission_refusals": {("refusal_id",), ("request_id",)},
        }
        for table, required in required_unique.items():
            unique_columns: set[tuple[str, ...]] = set()
            for index in self.db.execute(f"PRAGMA index_list({table})"):
                if not int(index["unique"]) or int(index["partial"]):
                    continue
                name = str(index["name"]).replace('"', '""')
                key_rows = sorted(
                    (
                        row for row in self.db.execute(f'PRAGMA index_xinfo("{name}")')
                        if int(row["key"])
                    ),
                    key=lambda row: int(row["seqno"]),
                )
                if any(
                    row["name"] is None or str(row["coll"]).upper() != "BINARY"
                    or int(row["desc"])
                    for row in key_rows
                ):
                    continue
                unique_columns.add(tuple(str(row["name"]) for row in key_rows))
            if not required <= unique_columns:
                raise QueueMigrationError(
                    f"{table} schema lacks a full binary database uniqueness fence for identity"
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

    @staticmethod
    def _submission_fingerprint(
        prepared_records: list[dict[str, Any]],
        *,
        spec_id: str,
        priority: int,
        idempotency_keys: list[str | None],
    ) -> str:
        """Content identity used by both accepted submissions and refusals."""
        from .task_contract import sha256_id

        kind = prepared_records[0]["kind"]
        resolved_keys = [
            ((prepared["prepared_id"] if requested_key is None else requested_key)
             if kind == "task" else ("" if requested_key is None else requested_key))
            for prepared, requested_key in zip(prepared_records, idempotency_keys)
        ]
        return sha256_id({
            "schema": 1,
            "spec_id": spec_id,
            "prepared_ids": [record["prepared_id"] for record in prepared_records],
            "priority": priority,
            "idempotency_keys": resolved_keys,
        })

    @staticmethod
    def _refusal_key(plan_id: str, request_id: str | None, fingerprint: str) -> str:
        from .task_contract import sha256_id

        return sha256_id({
            "schema": _SUBMISSION_REFUSAL_VERSION,
            "plan_id": plan_id,
            "request_id": request_id,
            "fingerprint": fingerprint,
        })

    @staticmethod
    def _refusal_document(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        """Return the closed refusal wire document, never the frozen task bytes."""
        if row["reason_code"] != _TASK_SPEC_DRIFT:
            raise QueueMigrationError("durable submission refusal has an unknown reason")
        return {
            "schema": _SUBMISSION_REFUSAL_SCHEMA,
            "version": _SUBMISSION_REFUSAL_VERSION,
            "status": "refused",
            "reason": _TASK_SPEC_DRIFT,
            "plan_id": row["plan_id"],
            "request_id": row["request_id"],
            "fingerprint": row["fingerprint"],
        }

    def _find_submission_refusal(
        self, *, plan_id: str, request_id: str | None, fingerprint: str,
    ) -> dict[str, Any] | None:
        refusal_id = self._refusal_key(plan_id, request_id, fingerprint)
        row = self.db.execute(
            "SELECT * FROM submission_refusals WHERE refusal_id=?", (refusal_id,),
        ).fetchone()
        if row is None:
            return None
        if (row["plan_id"], row["request_id"], row["fingerprint"]) != (
            plan_id, request_id, fingerprint
        ):
            raise QueueMigrationError("durable submission refusal identity disagrees")
        return self._refusal_document(row)

    def _find_submission_refusal_by_request(
        self, *, plan_id: str | None, request_id: str, fingerprint: str,
    ) -> dict[str, Any] | None:
        """Find a refusal by its caller request and fence identity rebinding."""
        row = self.db.execute(
            "SELECT * FROM submission_refusals WHERE request_id=?", (request_id,),
        ).fetchone()
        if row is None:
            return None
        if (row["plan_id"], row["request_id"], row["fingerprint"]) != (
            plan_id, request_id, fingerprint
        ):
            raise QueueMigrationError(
                "request identity already names a different durable refusal"
            )
        if row["refusal_id"] != self._refusal_key(plan_id, request_id, fingerprint):
            raise QueueMigrationError("durable submission refusal identity disagrees")
        return self._refusal_document(row)

    def _record_submission_refusal(
        self, *, plan_id: str, request_id: str | None, fingerprint: str,
        now: str,
    ) -> dict[str, Any]:
        refusal_id = self._refusal_key(plan_id, request_id, fingerprint)
        row = self.db.execute(
            "SELECT * FROM submission_refusals WHERE refusal_id=?", (refusal_id,),
        ).fetchone()
        if row is None:
            if request_id is not None:
                request_row = self.db.execute(
                    "SELECT * FROM submission_refusals WHERE request_id=?", (request_id,),
                ).fetchone()
                if request_row is not None:
                    if (request_row["plan_id"], request_row["request_id"],
                            request_row["fingerprint"]) == (plan_id, request_id, fingerprint):
                        return self._refusal_document(request_row)
                    raise QueueMigrationError(
                        "request identity already names a different durable refusal"
                    )
            try:
                self.db.execute(
                    "INSERT INTO submission_refusals "
                    "(refusal_id,plan_id,request_id,fingerprint,reason_code,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (refusal_id, plan_id, request_id, fingerprint, _TASK_SPEC_DRIFT, now),
                )
            except sqlite3.IntegrityError:
                # Another controller may have committed the same request identity while this
                # transaction was waiting. Re-read it below; a different row is a hard conflict.
                row = self.db.execute(
                    "SELECT * FROM submission_refusals WHERE refusal_id=?", (refusal_id,),
                ).fetchone()
                if row is None:
                    if request_id is not None:
                        request_row = self.db.execute(
                            "SELECT * FROM submission_refusals WHERE request_id=?",
                            (request_id,),
                        ).fetchone()
                        if request_row is not None:
                            if (request_row["plan_id"], request_row["request_id"],
                                    request_row["fingerprint"]) == (
                                        plan_id, request_id, fingerprint):
                                row = request_row
                            else:
                                raise QueueMigrationError(
                                    "request identity already names a different durable refusal"
                                )
                    if row is None:
                        raise
        if row is None:
            row = self.db.execute(
                "SELECT * FROM submission_refusals WHERE refusal_id=?", (refusal_id,),
            ).fetchone()
        if row is None or (row["plan_id"], row["request_id"], row["fingerprint"]) != (
            plan_id, request_id, fingerprint
        ):
            raise QueueMigrationError("durable submission refusal identity disagrees")
        return self._refusal_document(row)

    def get_submission_refusal(
        self, *, plan_id: str | None = None, request_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Read one terminal refusal by an exact saved-plan or request identity."""
        selected = [(field, value) for field, value in (
            ("plan_id", plan_id), ("request_id", request_id)
        ) if value is not None]
        if len(selected) != 1:
            raise ValueError("refusal lookup requires exactly one plan or request identity")
        field, value = selected[0]
        rows = self.db.execute(
            f"SELECT * FROM submission_refusals WHERE {field}=? ORDER BY refusal_id",
            (value,),
        ).fetchall()
        if len(rows) > 1:
            raise QueueMigrationError(
                f"durable refusal lookup by {field} is ambiguous"
            )
        return self._refusal_document(rows[0]) if rows else None

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
        """Compatibility wrapper returning only the jobs in a new submission."""
        return self.enqueue_submission(
            prepared_records, spec=spec, priority=priority,
            idempotency_keys=idempotency_keys, now=now, job_ids=job_ids,
            current_spec_id=current_spec_id,
        )["job_ids"]

    def enqueue_submission(
        self, prepared_records: list[dict[str, Any]], *, spec: dict[str, Any] | None,
        priority: int = 0, idempotency_keys: list[str | None] | None = None,
        now: str | None = None, job_ids: list[str | None] | None = None,
        current_spec_id: Callable[[], str | None] | None = None,
        request_id: str | None = None, plan_id: str | None = None,
        submission_id: str | None = None,
    ) -> dict[str, Any]:
        """Atomically store and identify one prepared submission.

        A callable equality gate is evaluated *inside* ``BEGIN IMMEDIATE`` after
        route preview and preparation. A changed or unreadable definition raises
        before any spec, job, or reservation row exists.

        ``request_id`` is a caller-owned retry/correlation token. Reusing it for
        the same exact prepared submission returns the first receipt; reusing it
        for different work fails closed. ``plan_id`` has the same one-shot replay
        semantics for an immutable saved placement.
        """
        from .prepared import (
            RAW_COMMAND_SPEC, RAW_COMMAND_SPEC_ID, validate_prepared_against_spec,
            validate_prepared_job,
        )
        from .task_contract import canonical_json, sha256_id

        if not prepared_records:
            raise ValueError("prepared enqueue requires at least one record")
        if request_id is not None and (
                not isinstance(request_id, str) or not request_id
                or request_id.strip() != request_id or "\x00" in request_id
                or len(request_id) > 200):
            raise ValueError("request_id must be 1-200 non-whitespace-surrounded characters")
        if plan_id is not None and (
                not isinstance(plan_id, str) or not plan_id or len(plan_id) > 200):
            raise ValueError("plan_id must be a non-empty token")
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
        submission_id = submission_id or uuid.uuid4().hex[:16]
        resolved_keys = [
            ((prepared["prepared_id"] if requested_key is None else requested_key)
             if kind == "task" else ("" if requested_key is None else requested_key))
            for prepared, requested_key in zip(prepared_records, idempotency_keys)
        ]
        fingerprint = self._submission_fingerprint(
            prepared_records,
            spec_id=spec_id,
            priority=priority,
            idempotency_keys=idempotency_keys,
        )
        result: list[str] = []
        durable_refusal: dict[str, Any] | None = None
        with self._immediate():
            existing = None
            if plan_id is not None:
                existing = self.db.execute(
                    "SELECT * FROM submissions WHERE plan_id=?", (plan_id,),
                ).fetchone()
            if existing is None and request_id is not None:
                existing = self.db.execute(
                    "SELECT * FROM submissions WHERE request_id=?", (request_id,),
                ).fetchone()
            if request_id is not None:
                durable_refusal = self._find_submission_refusal_by_request(
                    plan_id=plan_id, request_id=request_id, fingerprint=fingerprint,
                )
            if existing is not None:
                if durable_refusal is not None:
                    raise QueueMigrationError(
                        "request identity exists as both accepted submission and "
                        "durable refusal"
                    )
                if plan_id is not None and existing["plan_id"] != plan_id:
                    raise ValueError(
                        "request and plan identities do not name the same submission"
                    )
                if request_id is not None and existing["request_id"] != request_id:
                    raise ValueError(
                        "request and plan identities do not name the same submission"
                    )
                if existing["fingerprint"] != fingerprint:
                    raise ValueError(
                        "request or plan identity already names a different prepared submission"
                    )
                return self._submission_receipt(existing, replayed=True)
            if durable_refusal is None and plan_id is not None:
                durable_refusal = self._find_submission_refusal(
                    plan_id=plan_id, request_id=request_id, fingerprint=fingerprint,
                )
                if durable_refusal is not None:
                    # The refusal is terminal and idempotent. Do not consult mutable config or
                    # create any queue rows when the exact identity is replayed.
                    pass
            if durable_refusal is None and kind == "task":
                live_id = current_spec_id()
                if live_id != spec_id:
                    if plan_id is None:
                        raise ValueError(
                            "task definition changed before atomic enqueue; no job was enqueued"
                        )
                    durable_refusal = self._record_submission_refusal(
                        plan_id=plan_id, request_id=request_id,
                        fingerprint=fingerprint, now=now,
                    )
            if durable_refusal is None:
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
                for prepared, key, jid in zip(prepared_records, resolved_keys, allocated):
                    prepared_id = prepared["prepared_id"]
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
                self.db.execute(
                    "INSERT INTO submissions(submission_id,request_id,plan_id,fingerprint,created_at) "
                    "VALUES(?,?,?,?,?)",
                    (submission_id, request_id, plan_id, fingerprint, now),
                )
                for index, (jid, prepared) in enumerate(zip(result, prepared_records)):
                    self.db.execute(
                        "INSERT INTO submission_jobs(submission_id,item_index,job_id,prepared_id) "
                        "VALUES(?,?,?,?)",
                        (submission_id, index, jid, prepared["prepared_id"]),
                    )
                if plan_id is not None:
                    cur = self.db.execute(
                        "UPDATE submission_plans SET consumed_submission_id=? "
                        "WHERE plan_id=? AND consumed_submission_id IS NULL",
                        (submission_id, plan_id),
                    )
                    if cur.rowcount != 1:
                        raise ValueError("saved plan is missing or already consumed inconsistently")
                row = self.db.execute(
                    "SELECT * FROM submissions WHERE submission_id=?", (submission_id,),
                ).fetchone()
                return self._submission_receipt(row, replayed=False)
        if durable_refusal is not None:
            raise TaskSpecDriftRefusal(durable_refusal)
        raise AssertionError("submission transaction produced neither receipt nor refusal")

    def _submission_receipt(self, row: sqlite3.Row, *, replayed: bool) -> dict[str, Any]:
        jobs = self.db.execute(
            "SELECT sj.item_index,sj.job_id,sj.prepared_id,"
            "j.job_id AS live_job_id,j.prepared_id AS live_prepared_id "
            "FROM submission_jobs sj LEFT JOIN jobs j ON j.job_id=sj.job_id "
            "WHERE sj.submission_id=? ORDER BY sj.item_index",
            (row["submission_id"],),
        ).fetchall()
        if not jobs or [job["item_index"] for job in jobs] != list(range(len(jobs))):
            raise QueueMigrationError(
                f"submission {row['submission_id']} has incomplete durable membership"
            )
        if any(
            job["live_job_id"] is not None
            and job["live_prepared_id"] != job["prepared_id"]
            for job in jobs
        ):
            raise QueueMigrationError(
                f"submission {row['submission_id']} membership identity disagrees with its job row"
            )
        return {
            "schema": 1,
            "submission_id": row["submission_id"],
            "request_id": row["request_id"],
            "plan_id": row["plan_id"],
            "job_ids": [job["job_id"] for job in jobs],
            "prepared_ids": [job["prepared_id"] for job in jobs],
            "created_at": row["created_at"],
            "replayed": bool(replayed),
        }

    def get_submission(self, *, submission_id: str | None = None,
                       request_id: str | None = None,
                       plan_id: str | None = None) -> dict[str, Any] | None:
        selectors = [("submission_id", submission_id), ("request_id", request_id),
                     ("plan_id", plan_id)]
        selected = [(field, value) for field, value in selectors if value is not None]
        if len(selected) != 1:
            raise ValueError("submission lookup requires exactly one identity")
        field, value = selected[0]
        row = self.db.execute(
            f"SELECT * FROM submissions WHERE {field}=?", (value,),
        ).fetchone()
        if field == "request_id" and row is not None and self.db.execute(
            "SELECT 1 FROM submission_refusals WHERE request_id=?", (value,),
        ).fetchone() is not None:
            raise QueueMigrationError(
                "request identity exists as both accepted submission and durable refusal"
            )
        return self._submission_receipt(row, replayed=True) if row is not None else None

    def jobs_for_submission(self, submission_id: str) -> list[dict[str, Any]]:
        """Exact durable submission membership in caller item order.

        A left join keeps the accepted job IDs visible even if an explicit
        maintenance command later prunes their detailed queue rows.
        """
        rows = self.db.execute(
            "SELECT sj.item_index,sj.job_id AS submitted_job_id,"
            "sj.prepared_id AS submitted_prepared_id,j.* "
            "FROM submission_jobs sj LEFT JOIN jobs j ON j.job_id=sj.job_id "
            "WHERE sj.submission_id=? ORDER BY sj.item_index",
            (submission_id,),
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["job_id"] = item.pop("submitted_job_id")
            if item.get("prepared_id") is not None \
                    and item["prepared_id"] != item["submitted_prepared_id"]:
                raise QueueMigrationError(
                    f"submission {submission_id} membership identity disagrees with its job row"
                )
            if item.get("prepared_id") is None:
                item["prepared_id"] = item["submitted_prepared_id"]
            out.append(self._with_placement_explanation(item))
        return out

    def save_submission_plan(self, *, spec: dict[str, Any],
                             prepared_records: list[dict[str, Any]],
                             current_spec_id: Callable[[], str | None],
                             priority: int = 0,
                             now: str | None = None) -> dict[str, Any]:
        """Persist one immutable, fully placed configured submission plan."""
        from .prepared import validate_prepared_against_spec, validate_prepared_job
        from .task_contract import canonical_json, sha256_id

        if not prepared_records:
            raise ValueError("saved plan requires at least one prepared record")
        for record in prepared_records:
            validate_prepared_job(record)
            if record["kind"] != "task" or not record["routing"]["force_device"]:
                raise ValueError("saved plan requires every prepared job to have a pinned device")
            if record["routing"]["allow_fallback"]:
                raise ValueError("saved plan may not permit placement fallback")
            validate_prepared_against_spec(record, spec)
        planned_devices = {record["routing"]["force_device"] for record in prepared_records}
        if len(planned_devices) > 1 and any(
                record["output"]["root_override"] is not None
                for record in prepared_records):
            raise ValueError(
                "a multi-device saved plan cannot carry one controller-supplied output-root; "
                "use each adapter's configured root and verified return-root instead"
            )
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise ValueError("saved plan priority must be an integer")
        plan = {
            "schema": 1,
            "spec": spec,
            "prepared": prepared_records,
            "priority": priority,
        }
        blob = canonical_json(plan)
        digest = sha256_id(plan)
        plan_id = uuid.uuid4().hex[:20]
        now = now or utc_now_iso()
        with self._immediate():
            if not callable(current_spec_id) or current_spec_id() != spec.get("spec_id"):
                raise ValueError(
                    "task definition changed before atomic plan save; no plan was saved"
                )
            self.db.execute(
                "INSERT INTO submission_plans(plan_id,plan_digest,plan_json,created_at) "
                "VALUES(?,?,?,?)", (plan_id, digest, blob, now),
            )
        return {
            "schema": 1,
            "plan_id": plan_id,
            "plan_digest": digest,
            "prepared_ids": [record["prepared_id"] for record in prepared_records],
            "devices": [record["routing"]["force_device"] for record in prepared_records],
            "priority": priority,
            "created_at": now,
        }

    def get_submission_plan(self, plan_id: str) -> dict[str, Any] | None:
        from .prepared import validate_prepared_against_spec, validate_prepared_job
        from .task_contract import sha256_id

        row = self.db.execute(
            "SELECT * FROM submission_plans WHERE plan_id=?", (plan_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            plan = json.loads(row["plan_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise QueueMigrationError(f"saved plan {plan_id} has malformed bytes") from exc
        if not isinstance(plan, dict) \
                or set(plan) != {"schema", "spec", "prepared", "priority"} \
                or plan["schema"] != 1 or not isinstance(plan["spec"], dict) \
                or not isinstance(plan["prepared"], list) or not plan["prepared"] \
                or isinstance(plan["priority"], bool) or not isinstance(plan["priority"], int):
            raise QueueMigrationError(f"saved plan {plan_id} has an unsupported shape")
        if sha256_id(plan) != row["plan_digest"]:
            raise QueueMigrationError(f"saved plan {plan_id} fails its content identity")
        try:
            for record in plan["prepared"]:
                validate_prepared_job(record)
                if record["kind"] != "task" or not record["routing"]["force_device"] \
                        or record["routing"]["allow_fallback"]:
                    raise ValueError("prepared record is not pinned")
                validate_prepared_against_spec(record, plan["spec"])
        except ValueError as exc:
            raise QueueMigrationError(f"saved plan {plan_id} is invalid: {exc}") from exc
        return {
            "plan_id": plan_id,
            "plan_digest": row["plan_digest"],
            "created_at": row["created_at"],
            "consumed_submission_id": row["consumed_submission_id"],
            **plan,
        }

    def enqueue_saved_plan(self, plan_id: str, *, request_id: str | None = None,
                           current_spec_id: Callable[[], str | None]) -> dict[str, Any]:
        """Consume an exact saved plan, replaying its first receipt after response loss."""
        # Submission insertion and plan consumption commit atomically. Read the immutable
        # receipt first so a concurrent commit cannot pair a stale plan with a fresh receipt.
        existing = self.get_submission(plan_id=plan_id)
        plan = self.get_submission_plan(plan_id)
        if plan is None:
            raise ValueError(f"unknown saved plan {plan_id!r}")
        if existing is not None:
            if plan["consumed_submission_id"] != existing["submission_id"]:
                raise QueueMigrationError(
                    f"saved plan {plan_id} disagrees with its accepted submission"
                )
            if request_id is not None and request_id != existing["request_id"]:
                raise ValueError(
                    "saved plan replay cannot change its caller request identity"
                )
            return existing
        return self.enqueue_submission(
            plan["prepared"], spec=plan["spec"], priority=plan["priority"],
            request_id=request_id, plan_id=plan_id,
            current_spec_id=current_spec_id,
        )

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
    def _with_placement_explanation(self, record: dict[str, Any]) -> dict[str, Any]:
        batch_id = record.get("batch_id")
        if not batch_id:
            return record
        batch = self.db.execute(
            "SELECT device,placement_json FROM batches WHERE batch_id=?", (batch_id,),
        ).fetchone()
        if batch is None or batch["placement_json"] is None:
            return record
        record["placement_explanation"] = _decode_placement_explanation(
            batch["placement_json"], batch["device"],
        )
        return record

    def get(self, job_id: str) -> dict[str, Any] | None:
        row = self.db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return self._with_placement_explanation(dict(row)) if row else None

    def jobs_for_batch(self, batch_id: str) -> list[dict[str, Any]]:
        """Return the retained job rows attached to one worker invocation."""
        rows = self.db.execute(
            "SELECT * FROM jobs WHERE batch_id=? ORDER BY created_at,job_id", (batch_id,),
        ).fetchall()
        return [self._with_placement_explanation(dict(row)) for row in rows]

    def list(self, state: str | None = None,
             job_ids: list[str] | tuple[str, ...] | set[str] | None = None,
             ) -> list[dict[str, Any]]:
        if job_ids is not None and not job_ids:
            return []
        clauses: list[str] = []
        values: list[Any] = []
        if state:
            clauses.append("state=?")
            values.append(state)
        if job_ids is not None:
            ordered_ids = sorted(set(job_ids))
            clauses.append(f"job_id IN ({','.join('?' * len(ordered_ids))})")
            values.extend(ordered_ids)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.db.execute(
            "SELECT * FROM jobs" + where + " ORDER BY priority DESC, created_at",
            tuple(values),
        ).fetchall()
        return [self._with_placement_explanation(dict(r)) for r in rows]

    def counts(self, job_ids: list[str] | tuple[str, ...] | set[str] | None = None,
               ) -> dict[str, int]:
        if job_ids is not None and not job_ids:
            return {}
        values: tuple[str, ...] = ()
        where = ""
        if job_ids is not None:
            ordered_ids = tuple(sorted(set(job_ids)))
            where = f" WHERE job_id IN ({','.join('?' * len(ordered_ids))})"
            values = ordered_ids
        rows = self.db.execute(
            "SELECT state, COUNT(*) c FROM jobs" + where + " GROUP BY state", values,
        ).fetchall()
        return {r["state"]: r["c"] for r in rows}

    def active_by_device(
        self, job_ids: list[str] | tuple[str, ...] | set[str] | None = None,
    ) -> dict[str, int]:
        """In-flight (leased/staging/running/fetching) job count per device."""
        if job_ids is not None and not job_ids:
            return {}
        values: tuple[str, ...] = ()
        scope = ""
        if job_ids is not None:
            ordered_ids = tuple(sorted(set(job_ids)))
            scope = f" AND job_id IN ({','.join('?' * len(ordered_ids))})"
            values = ordered_ids
        rows = self.db.execute(
            "SELECT assigned_device d, COUNT(*) c FROM jobs "
            "WHERE state IN ('leased','staging','running','fetching') AND assigned_device IS NOT NULL "
            + scope + " GROUP BY assigned_device", values).fetchall()
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
                   placement_explanation: dict[str, Any] | None = None,
                   target_protocol_version: int | None = None,
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
        placement_json = _encode_placement_explanation(placement_explanation, device)
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
                    "created_at,updated_at,lease_until,heartbeat_at,estimated_finish_s,placement_json,"
                    "target_protocol_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (batch_id, owner_token, "leased", device, task_name, engine, bucket, now, now,
                     lease_until, now,
                     None if estimated_finish_s is None else float(estimated_finish_s),
                     placement_json, target_protocol_version))
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
                               reason: str, now: str | None = None,
                               finalization_disposition: str | None = None) -> bool:
        """Definition drift recovery before the user-process launch gate."""
        now = now or utc_now_iso()
        with self._immediate():
            disposition = self._terminal_disposition(
                batch_id, finalization_disposition, default=FINALIZATION_PENDING,
            )
            cur = self.db.execute(
                "UPDATE batches SET state='failed',error=?,"
                "target_finalization_disposition=?,updated_at=? WHERE batch_id=? "
                "AND state IN ('leased','staging','running') AND owner_token=? AND lease_until>?",
                (reason, disposition, now, batch_id, owner_token, now),
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

        Remote reservation and cleanup proof are finalized while the batch is
        still owner-fenced; the same owner then revokes the batch and attaches
        the completed receipt without reviving it or changing the terminal
        ``needs_review`` disposition.
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

    def cancel_queued(self, job_id: str, *, now: str | None = None) -> bool:
        """Atomically fence one queued job without deleting any durable record."""
        now = now or utc_now_iso()
        with self._immediate():
            cur = self.db.execute(
                "UPDATE jobs SET state='cancelled',last_error=?,updated_at=? "
                "WHERE job_id=? AND state='queued'",
                ("cancelled by operator", now, job_id),
            )
            return bool(cur.rowcount)

    def begin_active_cancellation(
        self,
        batch_id: str,
        job_ids: list[str] | tuple[str, ...] | set[str],
        *,
        now: str | None = None,
    ) -> bool:
        """Durably revoke an execution owner before a target stop side effect.

        The exact target identity remains attached. Changing the batch state to
        ``cancelling`` invalidates the owner's state and heartbeat CAS while the
        finalization fence prevents ordinary stale recovery from authorizing a
        replay or deleting target evidence.
        """
        selected = set(job_ids)
        if not selected:
            return False
        now = now or utc_now_iso()
        with self._immediate():
            batch = self.db.execute(
                "SELECT state,target_protocol_version,"
                "target_cleanup_state,target_cleanup_at,target_stage_cleaned_at,"
                "target_durable_cleaned_at,target_finalized_at,"
                "target_finalization_disposition "
                "FROM batches WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
            if (
                batch is None
                or batch["target_protocol_version"] != 1
                or any(
                    batch[name] is not None
                    for name in (
                        "target_cleanup_state",
                        "target_cleanup_at",
                        "target_stage_cleaned_at",
                        "target_durable_cleaned_at",
                        "target_finalized_at",
                    )
                )
                or batch["target_finalization_disposition"]
                == FINALIZATION_FINALIZED
            ):
                return False
            active_rows = self.db.execute(
                "SELECT job_id FROM jobs WHERE batch_id=? "
                "AND state IN ('leased','staging','running','fetching')",
                (batch_id,),
            ).fetchall()
            active_ids = {str(row["job_id"]) for row in active_rows}
            if not active_ids or active_ids != selected:
                return False
            if batch["state"] == "cancelling":
                return (
                    batch["target_finalization_disposition"]
                    == FINALIZATION_FENCED
                )
            if batch["state"] not in _BATCH_EXECUTING:
                return False
            cur = self.db.execute(
                "UPDATE batches SET state='cancelling',error=?,"
                "target_finalization_disposition=?,updated_at=? "
                "WHERE batch_id=? AND state=? AND target_protocol_version=1",
                (
                    "cancellation requested by operator",
                    FINALIZATION_FENCED,
                    now,
                    batch_id,
                    batch["state"],
                ),
            )
            return cur.rowcount == 1

    def mark_active_cancellation_unknown(
        self,
        batch_id: str,
        job_ids: list[str] | tuple[str, ...] | set[str],
        *,
        reason: str,
        now: str | None = None,
    ) -> bool:
        """Close a cancellation that raced target completion, retaining evidence."""
        selected = set(job_ids)
        if not selected or not reason:
            return False
        now = now or utc_now_iso()
        with self._immediate():
            batch = self.db.execute(
                "SELECT state,target_protocol_version,"
                "target_finalization_disposition FROM batches WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
            if (
                batch is None
                or batch["state"] != "cancelling"
                or batch["target_protocol_version"] != 1
                or batch["target_finalization_disposition"]
                != FINALIZATION_FENCED
            ):
                return False
            active_rows = self.db.execute(
                "SELECT job_id FROM jobs WHERE batch_id=? "
                "AND state IN ('leased','staging','running','fetching')",
                (batch_id,),
            ).fetchall()
            active_ids = {str(row["job_id"]) for row in active_rows}
            if active_ids != selected:
                return False
            cur = self.db.execute(
                "UPDATE batches SET state='failed',error=?,updated_at=? "
                "WHERE batch_id=? AND state='cancelling'",
                (reason, now, batch_id),
            )
            if cur.rowcount != 1:
                return False
            ordered = sorted(selected)
            qmarks = ",".join("?" * len(ordered))
            updated = self.db.execute(
                "UPDATE jobs SET state='completion_unknown',leased_until=NULL,"
                "last_error=?,updated_at=? WHERE batch_id=? "
                f"AND job_id IN ({qmarks}) "
                "AND state IN ('leased','staging','running','fetching')",
                (reason, now, batch_id, *ordered),
            ).rowcount
            if updated != len(ordered):
                raise QueueMigrationError(
                    f"batch {batch_id!r} cancellation changed incomplete job membership"
                )
            self.db.execute(
                "DELETE FROM resource_leases WHERE batch_id=?", (batch_id,),
            )
            return True

    def cancel_active_batch(
        self,
        batch_id: str,
        job_ids: list[str] | tuple[str, ...] | set[str],
        *,
        cleanup_state: str,
        now: str | None = None,
    ) -> bool:
        """Record a verified exact process-tree stop without deleting its evidence.

        A worker invocation is the smallest killable unit. The transition is
        refused unless every still-active job in that batch was explicitly named.
        Target cleanup/finalization remains fenced: cancellation proves the tree
        stopped, not that an already-started operation produced no external effect.
        """
        selected = set(job_ids)
        if not selected or not cleanup_state:
            return False
        now = now or utc_now_iso()
        with self._immediate():
            batch = self.db.execute(
                "SELECT state,target_protocol_version,target_finalized_at,"
                "target_finalization_disposition FROM batches WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
            if (
                batch is None
                or batch["state"] != "cancelling"
                or batch["target_protocol_version"] != 1
                or batch["target_finalized_at"] is not None
                or batch["target_finalization_disposition"]
                != FINALIZATION_FENCED
            ):
                return False
            active_rows = self.db.execute(
                "SELECT job_id,state FROM jobs WHERE batch_id=? "
                "AND state IN ('leased','staging','running','fetching')",
                (batch_id,),
            ).fetchall()
            active_ids = {str(row["job_id"]) for row in active_rows}
            if not active_ids or active_ids != selected:
                return False
            cur = self.db.execute(
                "UPDATE batches SET state='failed',error=?,target_cleanup_state=?,"
                "target_cleanup_at=?,target_finalization_disposition=?,updated_at=? "
                "WHERE batch_id=? AND state=?",
                (
                    "cancelled by operator", cleanup_state, now,
                    FINALIZATION_FENCED, now,
                    batch_id, batch["state"],
                ),
            )
            if cur.rowcount != 1:
                return False
            ordered = sorted(selected)
            qmarks = ",".join("?" * len(ordered))
            updated = self.db.execute(
                "UPDATE jobs SET state='cancelled',leased_until=NULL,last_error=?,"
                f"updated_at=? WHERE batch_id=? AND job_id IN ({qmarks}) "
                "AND state IN ('leased','staging','running','fetching')",
                ("cancelled by operator", now, batch_id, *ordered),
            ).rowcount
            if updated != len(ordered):
                raise QueueMigrationError(
                    f"batch {batch_id!r} cancellation changed incomplete job membership"
                )
            self.db.execute("DELETE FROM resource_leases WHERE batch_id=?", (batch_id,))
            return True

    def record_fenced_cancellation(
        self, batch_id: str, *, cleanup_state: str, now: str | None = None,
    ) -> bool:
        """Retain an unknown-completion fence while recording a verified tree stop."""
        if not cleanup_state:
            return False
        now = now or utc_now_iso()
        with self._immediate():
            cur = self.db.execute(
                "UPDATE batches SET target_cleanup_state=?,target_cleanup_at=?,updated_at=? "
                "WHERE batch_id=? AND state='failed' AND target_protocol_version=1 "
                "AND target_finalization_disposition=? AND target_finalized_at IS NULL",
                (cleanup_state, now, now, batch_id, FINALIZATION_FENCED),
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

    def _terminal_disposition(
        self, batch_id: str, supplied: str | None, *, default: str,
    ) -> str | None:
        """Resolve the normalized target disposition inside a terminal transition."""
        row = self.db.execute(
            "SELECT target_protocol_version,target_operation_id,"
            "target_finalization_disposition FROM batches WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        if row is None or row["target_protocol_version"] != 1:
            return None
        disposition = supplied or row["target_finalization_disposition"] or default
        if disposition not in _FINALIZATION_DISPOSITIONS:
            raise ValueError(f"unsupported target finalization disposition {disposition!r}")
        if row["target_finalization_disposition"] == FINALIZATION_FINALIZED:
            return FINALIZATION_FINALIZED
        if supplied is None and row["target_operation_id"] is None:
            return FINALIZATION_FINALIZED
        return disposition

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
                       observation: dict[str, Any] | None = None,
                       finalization_disposition: str | None = None) -> bool:
        """Mark a batch done only for its current, unexpired owner."""
        now = now or utc_now_iso()
        with self._immediate():
            disposition = self._terminal_disposition(
                batch_id, finalization_disposition, default=FINALIZATION_PENDING,
            )
            cur = self.db.execute(
                "UPDATE batches SET state='done',target_finalization_disposition=?,"
                "updated_at=? WHERE batch_id=? "
                "AND state=? AND owner_token=? AND lease_until>?",
                (disposition, now, batch_id, expected_state, owner_token, now),
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
                             observation: dict[str, Any] | None = None,
                             finalization_disposition: str | None = None) -> bool:
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
            disposition = self._terminal_disposition(
                batch_id, finalization_disposition, default=FINALIZATION_PENDING,
            )
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
                "UPDATE batches SET state=?,error=?,target_finalization_disposition=?,"
                "updated_at=? WHERE batch_id=? "
                "AND state=? AND owner_token=? AND lease_until>?",
                (batch_state, batch_error or None, disposition, now, batch_id,
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
        if not isinstance(observation, dict) or set(observation) not in (
            expected, expected | {"memory_metric"}
        ):
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
        memory_metric = observation.get("memory_metric")
        if memory_metric is not None and (
            not isinstance(memory_metric, str) or not memory_metric
        ):
            raise ValueError("profile observation memory_metric must be a non-empty string or null")
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
            "controller_elapsed_s,worker_elapsed_s,peak_rss_mb,peak_vram_mb,memory_metric,"
            "accepted_duration,reject_reason,result_digest,recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
                memory_metric,
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
                   observation: dict[str, Any] | None = None,
                   finalization_disposition: str | None = None) -> bool:
        """Fail a batch atomically only for its current, unexpired owner.

        Requeue jobs that have attempts left (clearing their batch + device),
        finalize the rest, and drop the resource lease so the device frees up.
        ``clear_force_device`` unpins retryable rows for explicit fallback. Returns True iff this
        call performed the transition (False = already terminal/recovered)."""
        now = now or utc_now_iso()
        with self._immediate():
            disposition = self._terminal_disposition(
                batch_id, finalization_disposition, default=FINALIZATION_PENDING,
            )
            cur = self.db.execute(
                "UPDATE batches SET state='failed',error=?,"
                "target_finalization_disposition=?,updated_at=? WHERE batch_id=? "
                "AND state=? AND owner_token=? AND lease_until>?",
                (error, disposition, now, batch_id, expected_state, owner_token, now),
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
                                observation: dict[str, Any] | None = None,
                                finalization_disposition: str = FINALIZATION_FENCED) -> bool:
        """Fence ambiguous post-launch work without authorizing an automatic replay."""
        if expected_state not in {"running", "fetching"}:
            return False
        if finalization_disposition not in _FINALIZATION_DISPOSITIONS:
            return False
        now = now or utc_now_iso()
        with self._immediate():
            cur = self.db.execute(
                "UPDATE batches SET state='failed',error=?,"
                "target_finalization_disposition=?,updated_at=? WHERE batch_id=? "
                "AND state=? AND owner_token=? AND lease_until>?",
                (error, finalization_disposition, now, batch_id, expected_state,
                 owner_token, now),
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
                      max_attempts: int = MAX_ATTEMPTS,
                      replay_safe: bool | None = None) -> bool:
        """Recovery-only transition authorized solely by an expired lease."""
        error = "lease expired (stale recovery)"
        with self._immediate():
            row = self.db.execute(
                "SELECT state,target_protocol_version,target_operation_id,target_finalized_at "
                "FROM batches WHERE batch_id=? AND lease_until<?",
                (batch_id, now),
            ).fetchone()
            if row is None or row["state"] not in _BATCH_ACTIVE:
                return False
            if (
                replay_safe is True
                and row["target_protocol_version"] == 1
                and row["target_operation_id"] is not None
                and row["target_finalized_at"] is None
            ):
                # A target-bearing row may only be replayed after its exact
                # stage and durable evidence cleanup has been finalized.
                return False
            state = str(row["state"])
            disposition = None
            if row["target_protocol_version"] == 1:
                disposition = (
                    FINALIZATION_FINALIZED if replay_safe is True
                    else FINALIZATION_FENCED if replay_safe is False
                    else FINALIZATION_PENDING
                )
            cur = self.db.execute(
                "UPDATE batches SET state='failed',error=?,"
                "target_finalization_disposition=COALESCE(?,target_finalization_disposition),"
                "updated_at=? WHERE batch_id=? AND state=? AND lease_until<?",
                (error, disposition, now, batch_id, state, now),
            )
            if not cur.rowcount:
                return False
            if replay_safe is True:
                self._finish_batch_failure(batch_id, error, now, max_attempts)
            elif replay_safe is False:
                self.db.execute(
                    "UPDATE jobs SET state='completion_unknown',leased_until=NULL,"
                    "last_error=?,updated_at=? WHERE batch_id=? "
                    f"AND state NOT IN ({_FINAL_Q})",
                    ("completion unknown after lease expiry", now, batch_id, *_FINAL),
                )
                self.db.execute("DELETE FROM resource_leases WHERE batch_id=?", (batch_id,))
            elif state in {"leased", "staging"}:
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

    def expire_stale_batch(
        self, batch_id: str, *, now: str, replay_safe: bool,
        max_attempts: int = MAX_ATTEMPTS,
    ) -> bool:
        """Apply a separately-authorized expired-lease transition.

        Target-aware callers must first reconcile target truth and remote stage
        cleanup, then state explicitly whether replay is proven safe.  The queue
        never infers remote facts and never performs remote I/O itself.
        """
        return self._expire_batch(
            batch_id, now=now, max_attempts=max_attempts, replay_safe=replay_safe,
        )

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
        if row is None:
            return None
        result = dict(row)
        result.pop("target_resume_token", None)
        placement_json = result.pop("placement_json", None)
        if placement_json is not None:
            result["placement_explanation"] = _decode_placement_explanation(
                placement_json, result["device"],
            )
        return result

    def record_target_reservation(
        self,
        batch_id: str,
        *,
        operation_id: str,
        request_sha256: str,
        resume_token: str,
        expected_state: str,
        owner_token: str,
        now: str | None = None,
    ) -> bool:
        """Persist one exact target reservation under the live batch-owner fence.

        The raw resume credential stays private inside the controller-local queue
        database. Public queue/status receipts use :meth:`target_operation`, which
        omits it. This happens before launch so a controller crash cannot lose the
        credential for an operation that the target may subsequently accept. An
        exact replay is idempotent; a different identity can never replace it.
        """
        if not operation_id or not resume_token \
                or len(request_sha256) != 64 \
                or any(ch not in "0123456789abcdef" for ch in request_sha256):
            return False
        now = now or utc_now_iso()
        with self._immediate():
            row = self.db.execute(
                "SELECT target_operation_id,target_request_sha256,target_resume_token,"
                "target_reserved_at,target_accepted_at FROM batches "
                "WHERE batch_id=? AND state=? "
                "AND owner_token=? AND lease_until>?",
                (batch_id, expected_state, owner_token, now),
            ).fetchone()
            if row is None:
                return False
            existing = row["target_operation_id"]
            if existing is not None:
                return (
                    existing == operation_id
                    and row["target_request_sha256"] == request_sha256
                    and row["target_resume_token"] == resume_token
                    and isinstance(row["target_reserved_at"], str)
                    and bool(row["target_reserved_at"])
                )
            if any(
                row[name] is not None
                for name in (
                    "target_request_sha256", "target_resume_token", "target_reserved_at",
                    "target_accepted_at",
                )
            ):
                return False
            cur = self.db.execute(
                "UPDATE batches SET target_operation_id=?,target_request_sha256=?,"
                "target_resume_token=?,target_reserved_at=?,updated_at=? "
                "WHERE batch_id=? AND state=? AND owner_token=? AND lease_until>? "
                "AND target_operation_id IS NULL AND target_request_sha256 IS NULL "
                "AND target_resume_token IS NULL AND target_reserved_at IS NULL "
                "AND target_accepted_at IS NULL",
                (
                    operation_id, request_sha256, resume_token, now, now,
                    batch_id, expected_state, owner_token, now,
                ),
            )
            return cur.rowcount == 1

    def record_target_acceptance(
        self,
        batch_id: str,
        *,
        operation_id: str,
        request_sha256: str,
        expected_state: str,
        owner_token: str,
        now: str | None = None,
    ) -> bool:
        """Mark the exact pre-recorded reservation positively accepted by its target."""
        now = now or utc_now_iso()
        with self._immediate():
            row = self.db.execute(
                "SELECT target_operation_id,target_request_sha256,target_reserved_at,"
                "target_accepted_at FROM batches WHERE batch_id=? AND state=? "
                "AND owner_token=? AND lease_until>?",
                (batch_id, expected_state, owner_token, now),
            ).fetchone()
            if row is None \
                    or row["target_operation_id"] != operation_id \
                    or row["target_request_sha256"] != request_sha256 \
                    or not isinstance(row["target_reserved_at"], str) \
                    or not row["target_reserved_at"]:
                return False
            if row["target_accepted_at"] is not None:
                return isinstance(row["target_accepted_at"], str) \
                    and bool(row["target_accepted_at"])
            cur = self.db.execute(
                "UPDATE batches SET target_accepted_at=?,updated_at=? "
                "WHERE batch_id=? AND state=? AND owner_token=? AND lease_until>? "
                "AND target_operation_id=? AND target_request_sha256=? "
                "AND target_reserved_at IS NOT NULL AND target_accepted_at IS NULL",
                (
                    now, now, batch_id, expected_state, owner_token, now,
                    operation_id, request_sha256,
                ),
            )
            return cur.rowcount == 1

    def authorize_target_cleanup(
        self,
        batch_id: str,
        *,
        operation_id: str,
        request_sha256: str,
        cleanup_state: str,
        expected_state: str,
        owner_token: str,
        now: str | None = None,
    ) -> bool:
        """Linearize execution-owner cleanup against operator cancellation.

        The terminal cleanup observation is the durable authorization marker.
        Destructive target cleanup may begin only after this live-owner CAS;
        finalization remains a separate post-deletion proof.
        """
        if (
            not operation_id
            or not self._valid_target_digest(request_sha256)
            or cleanup_state not in {"RELEASED", "CANCELLED", "EXPIRED", "REBOOTED"}
        ):
            return False
        now = now or utc_now_iso()
        with self._immediate():
            row = self.db.execute(
                "SELECT target_protocol_version,target_operation_id,"
                "target_request_sha256,target_reserved_at,target_cleanup_state,"
                "target_cleanup_at,target_stage_cleaned_at,target_durable_cleaned_at,"
                "target_finalized_at,target_finalization_disposition "
                "FROM batches WHERE batch_id=? AND state=? AND owner_token=? "
                "AND lease_until>?",
                (batch_id, expected_state, owner_token, now),
            ).fetchone()
            if (
                row is None
                or row["target_protocol_version"] != 1
                or row["target_operation_id"] != operation_id
                or row["target_request_sha256"] != request_sha256
                or not isinstance(row["target_reserved_at"], str)
                or not row["target_reserved_at"]
                or row["target_finalized_at"] is not None
                or row["target_finalization_disposition"]
                in {FINALIZATION_FENCED, FINALIZATION_MALFORMED, FINALIZATION_FINALIZED}
            ):
                return False
            existing_state = row["target_cleanup_state"]
            existing_at = row["target_cleanup_at"]
            if existing_state is not None or existing_at is not None:
                return (
                    existing_state == cleanup_state
                    and isinstance(existing_at, str)
                    and bool(existing_at)
                    and row["target_stage_cleaned_at"] is None
                    and row["target_durable_cleaned_at"] is None
                    and row["target_finalization_disposition"] == FINALIZATION_PENDING
                )
            if (
                row["target_stage_cleaned_at"] is not None
                or row["target_durable_cleaned_at"] is not None
                or row["target_finalization_disposition"] is not None
            ):
                return False
            cur = self.db.execute(
                "UPDATE batches SET target_cleanup_state=?,target_cleanup_at=?,"
                "target_finalization_disposition=?,updated_at=? WHERE batch_id=? "
                "AND state=? AND owner_token=? AND lease_until>? "
                "AND target_protocol_version=1 AND target_operation_id=? "
                "AND target_request_sha256=? AND target_reserved_at IS NOT NULL "
                "AND target_cleanup_state IS NULL AND target_cleanup_at IS NULL "
                "AND target_stage_cleaned_at IS NULL "
                "AND target_durable_cleaned_at IS NULL AND target_finalized_at IS NULL "
                "AND target_finalization_disposition IS NULL",
                (
                    cleanup_state,
                    now,
                    FINALIZATION_PENDING,
                    now,
                    batch_id,
                    expected_state,
                    owner_token,
                    now,
                    operation_id,
                    request_sha256,
                ),
            )
            return cur.rowcount == 1

    def record_target_finalization(
        self,
        batch_id: str,
        *,
        operation_id: str,
        request_sha256: str,
        cleanup_state: str,
        stage_cleaned: bool,
        durable_cleaned: bool,
        expected_state: str,
        owner_token: str,
        now: str | None = None,
    ) -> bool:
        """Persist monotonic target cleanup proof under the live owner fence."""
        terminal = {"RELEASED", "CANCELLED", "EXPIRED", "REBOOTED"}
        if cleanup_state not in terminal or not stage_cleaned or not durable_cleaned:
            return False
        now = now or utc_now_iso()
        with self._immediate():
            row = self.db.execute(
                "SELECT target_operation_id,target_request_sha256,target_cleanup_state,"
                "target_cleanup_at,target_stage_cleaned_at,target_durable_cleaned_at,"
                "target_finalized_at FROM batches WHERE batch_id=? AND state=? "
                "AND owner_token=? AND lease_until>?",
                (batch_id, expected_state, owner_token, now),
            ).fetchone()
            if row is None \
                    or row["target_operation_id"] != operation_id \
                    or row["target_request_sha256"] != request_sha256:
                return False
            existing_state = row["target_cleanup_state"]
            if existing_state is not None and existing_state != cleanup_state:
                return False
            if row["target_finalized_at"] is not None:
                return all(
                    isinstance(row[name], str) and bool(row[name])
                    for name in (
                        "target_cleanup_at", "target_stage_cleaned_at",
                        "target_durable_cleaned_at", "target_finalized_at",
                    )
                )
            cur = self.db.execute(
                "UPDATE batches SET target_cleanup_state=?,"
                "target_cleanup_at=COALESCE(target_cleanup_at,?),"
                "target_stage_cleaned_at=?,target_durable_cleaned_at=?,"
                "target_finalized_at=?,target_finalization_disposition=?,updated_at=? "
                "WHERE batch_id=? AND state=? "
                "AND owner_token=? AND lease_until>? AND target_operation_id=? "
                "AND target_request_sha256=? AND target_finalized_at IS NULL",
                (
                    cleanup_state, now, now, now, now, FINALIZATION_FINALIZED, now,
                    batch_id, expected_state, owner_token, now,
                    operation_id, request_sha256,
                ),
            )
            return cur.rowcount == 1

    def record_target_cleanup_progress(
        self,
        batch_id: str,
        *,
        operation_id: str,
        request_sha256: str,
        cleanup_state: str | None = None,
        stage_cleaned: bool = False,
        durable_cleaned: bool = False,
        replayable: bool = False,
        owner_release: bool = False,
        now: str,
    ) -> bool:
        """CAS-record one cleanup proof for an expired or terminal target row.

        Each proof is monotonic and exact-identity fenced. Remote deletion is
        intentionally performed before its corresponding proof is written, so
        a controller crash only causes an absence-aware repeat of that step.
        """
        terminal = {"RELEASED", "CANCELLED", "EXPIRED", "REBOOTED"}
        if cleanup_state is not None and cleanup_state not in terminal:
            return False
        if not stage_cleaned and not durable_cleaned and cleanup_state is None:
            return False
        with self._immediate():
            row = self.db.execute(
                "SELECT target_protocol_version,target_operation_id,target_request_sha256,"
                "target_cleanup_state,target_cleanup_at,target_stage_cleaned_at,"
                "target_durable_cleaned_at,target_finalized_at,target_finalization_disposition,"
                "target_release_authorized_at "
                "FROM batches WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
            if row is None or row["target_protocol_version"] != 1 \
                    or row["target_operation_id"] != operation_id \
                    or row["target_request_sha256"] != request_sha256:
                return False
            current_state = row["target_cleanup_state"]
            if current_state is not None and cleanup_state is not None \
                    and current_state != cleanup_state:
                return False
            disposition = row["target_finalization_disposition"]
            if disposition == FINALIZATION_MALFORMED:
                return False
            if disposition == FINALIZATION_FENCED and (
                not owner_release or row["target_release_authorized_at"] is None
            ):
                return False
            if replayable:
                disposition = FINALIZATION_REPLAYABLE
            elif disposition is None:
                disposition = FINALIZATION_PENDING
            self.db.execute(
                "UPDATE batches SET target_cleanup_state=COALESCE(?,target_cleanup_state),"
                "target_cleanup_at=CASE WHEN ? IS NOT NULL THEN "
                "COALESCE(target_cleanup_at,?) ELSE target_cleanup_at END,"
                "target_stage_cleaned_at=CASE WHEN ? THEN COALESCE(target_stage_cleaned_at,?) "
                "ELSE target_stage_cleaned_at END,"
                "target_durable_cleaned_at=CASE WHEN ? THEN "
                "COALESCE(target_durable_cleaned_at,?) ELSE target_durable_cleaned_at END,"
                "target_finalization_disposition=?,updated_at=? WHERE batch_id=? "
                "AND target_protocol_version=1 AND target_operation_id=? "
                "AND target_request_sha256=?",
                (
                    cleanup_state, cleanup_state, now, stage_cleaned, now,
                    durable_cleaned, now, disposition, now, batch_id,
                    operation_id, request_sha256,
                ),
            )
            return True

    def record_expired_target_finalization(
        self,
        batch_id: str,
        *,
        operation_id: str,
        request_sha256: str,
        cleanup_state: str,
        owner_release: bool = False,
        now: str,
    ) -> bool:
        """Persist complete cleanup proof under the expired-lease recovery fence."""
        if cleanup_state not in {"RELEASED", "CANCELLED", "EXPIRED", "REBOOTED"}:
            return False
        release_fence = (
            "AND target_release_authorized_at IS NOT NULL "
            "AND target_finalization_disposition=?"
            if owner_release else "AND lease_until<?"
        )
        release_values: tuple[str, ...] = (
            (FINALIZATION_FENCED,) if owner_release else (now,)
        )
        with self._immediate():
            cur = self.db.execute(
                "UPDATE batches SET target_cleanup_state=?,target_cleanup_at="
                "COALESCE(target_cleanup_at,?),target_stage_cleaned_at="
                "COALESCE(target_stage_cleaned_at,?),target_durable_cleaned_at="
                "COALESCE(target_durable_cleaned_at,?),target_finalized_at="
                "COALESCE(target_finalized_at,?),target_finalization_disposition=?,"
                "updated_at=? WHERE batch_id=? AND target_protocol_version=1 "
                f"{release_fence} AND target_operation_id=? AND target_request_sha256=? "
                "AND (target_cleanup_state IS NULL OR target_cleanup_state=?)",
                (
                    cleanup_state, now, now, now, now, FINALIZATION_FINALIZED, now,
                    batch_id, *release_values, operation_id, request_sha256, cleanup_state,
                ),
            )
            return cur.rowcount == 1

    def target_release_operation(
        self, operation_id: str, request_sha256: str,
    ) -> dict[str, Any] | None:
        """Return one exact owner-release row, including its private resume token."""
        if not operation_id or not self._valid_target_digest(request_sha256):
            return None
        rows = self.db.execute(
            "SELECT batch_id,state,device,target_protocol_version,target_operation_id,"
            "target_request_sha256,target_resume_token,target_reserved_at,"
            "target_accepted_at,target_cleanup_state,target_stage_cleaned_at,"
            "target_durable_cleaned_at,target_finalized_at,"
            "target_finalization_disposition,target_release_authorized_at,"
            "target_release_effects_acknowledged_at FROM batches "
            "WHERE target_operation_id=? AND target_request_sha256=?",
            (operation_id, request_sha256),
        ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise QueueMigrationError(
                f"target operation {operation_id!r} is not unique in the batch ledger"
            )
        return dict(rows[0])

    def _authorize_target_release(
        self,
        operation_id: str,
        request_sha256: str,
        *,
        acknowledge_effects: bool,
        now: str | None = None,
    ) -> bool:
        """Record exact owner authority without changing job or cleanup state."""
        if not operation_id or not self._valid_target_digest(request_sha256):
            return False
        now = now or utc_now_iso()
        with self._immediate():
            rows = self.db.execute(
                "SELECT batch_id,state,target_protocol_version,target_finalized_at,"
                "target_finalization_disposition,target_release_authorized_at,"
                "target_release_effects_acknowledged_at FROM batches "
                "WHERE target_operation_id=? AND target_request_sha256=?",
                (operation_id, request_sha256),
            ).fetchall()
            if len(rows) != 1:
                return False
            row = rows[0]
            if (
                row["state"] != "failed"
                or row["target_protocol_version"] != 1
                or row["target_finalized_at"] is not None
                or row["target_finalization_disposition"] != FINALIZATION_FENCED
            ):
                return False
            job_states = {
                str(job["state"])
                for job in self.db.execute(
                    "SELECT state FROM jobs WHERE batch_id=?", (row["batch_id"],),
                )
            }
            if not job_states or not job_states <= {"cancelled", "completion_unknown"}:
                return False
            cur = self.db.execute(
                "UPDATE batches SET target_release_authorized_at="
                "COALESCE(target_release_authorized_at,?),"
                "target_release_effects_acknowledged_at=CASE WHEN ? THEN "
                "COALESCE(target_release_effects_acknowledged_at,?) "
                "ELSE target_release_effects_acknowledged_at END,updated_at=? "
                "WHERE batch_id=? AND target_operation_id=? AND target_request_sha256=? "
                "AND target_finalized_at IS NULL AND target_finalization_disposition=?",
                (
                    now, acknowledge_effects, now, now, row["batch_id"], operation_id,
                    request_sha256, FINALIZATION_FENCED,
                ),
            )
            return cur.rowcount == 1

    def authorize_target_release_no_start(
        self, operation_id: str, request_sha256: str, *, now: str | None = None,
    ) -> bool:
        """Record release authority after exact target truth proves no command start."""
        return self._authorize_target_release(
            operation_id, request_sha256, acknowledge_effects=False, now=now,
        )

    def acknowledge_target_release_effects(
        self, operation_id: str, request_sha256: str, *, now: str | None = None,
    ) -> bool:
        """Distinct owner act accepting that the cancelled operation may have effects."""
        return self._authorize_target_release(
            operation_id, request_sha256, acknowledge_effects=True, now=now,
        )

    def target_operation(
        self, batch_id: str, *, include_token: bool = False
    ) -> dict[str, Any] | None:
        """Return the accepted target identity, redacting its credential by default."""
        row = self.db.execute(
            "SELECT device,target_operation_id,target_request_sha256,target_resume_token,"
            "target_reserved_at,target_accepted_at,target_cleanup_state,"
            "target_cleanup_at,target_stage_cleaned_at,target_durable_cleaned_at,"
            "target_finalized_at,target_finalization_disposition FROM batches WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        if row is None or row["target_operation_id"] is None:
            return None
        required = (
            row["target_operation_id"], row["target_request_sha256"],
            row["target_resume_token"], row["target_reserved_at"],
        )
        if not all(isinstance(value, str) and value for value in required):
            raise QueueMigrationError(
                f"batch {batch_id!r} has a partial target-acceptance receipt"
            )
        result = {
            "schema": 1,
            "operation_id": row["target_operation_id"],
            "request_sha256": row["target_request_sha256"],
            "device": row["device"],
            "reserved_at": row["target_reserved_at"],
            "accepted": row["target_accepted_at"] is not None,
            "cleanup_state": row["target_cleanup_state"],
            "stage_cleaned": row["target_stage_cleaned_at"] is not None,
            "durable_cleaned": row["target_durable_cleaned_at"] is not None,
            "finalized": row["target_finalized_at"] is not None,
        }
        if row["target_accepted_at"] is not None:
            if not isinstance(row["target_accepted_at"], str) or not row["target_accepted_at"]:
                raise QueueMigrationError(
                    f"batch {batch_id!r} has a malformed target acceptance time"
                )
            result["accepted_at"] = row["target_accepted_at"]
        if include_token:
            result["resume_token"] = row["target_resume_token"]
        return result

    def target_operation_for_job(
        self, job_id: str, *, include_token: bool = False
    ) -> dict[str, Any] | None:
        row = self.db.execute(
            "SELECT batch_id FROM jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if row is None or row["batch_id"] is None:
            return None
        return self.target_operation(str(row["batch_id"]), include_token=include_token)

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

    def recover_stale(
        self, now: str | None = None,
        job_ids: list[str] | tuple[str, ...] | set[str] | None = None,
    ) -> int:
        """Recover expired legacy batches without inventing target-side facts.

        Protocol-v1 target batches are intentionally left for the dispatcher's
        target-aware recovery coordinator, which can inspect and clean the exact
        device stage before authorizing replay.
        """
        now = now or utc_now_iso()
        if job_ids is not None and not job_ids:
            return 0
        values: list[str] = [now]
        scope = ""
        if job_ids is not None:
            ordered_ids = sorted(set(job_ids))
            scope = (
                " AND batch_id IN (SELECT DISTINCT batch_id FROM jobs WHERE job_id IN ("
                + ",".join("?" * len(ordered_ids)) + ") AND batch_id IS NOT NULL)"
            )
            values.extend(ordered_ids)
        stale = self.db.execute(
            "SELECT batch_id FROM batches WHERE state NOT IN ('done','failed') "
            "AND lease_until<? AND target_protocol_version IS NULL" + scope,
            tuple(values),
        ).fetchall()
        recovered = 0
        for r in stale:
            recovered += int(self._expire_batch(r["batch_id"], now=now))
        if job_ids is None:
            self.db.execute("DELETE FROM resource_leases WHERE lease_until<?", (now,))
        return recovered

    def stale_target_batches(
        self, now: str | None = None,
        job_ids: list[str] | tuple[str, ...] | set[str] | None = None,
        *, include_terminal: bool = False,
    ) -> list[dict[str, Any]]:
        """Return expired protocol-v1 batches for target-aware reconciliation.

        The private resume token is exposed only to this controller-internal
        recovery surface. Public status receipts remain redacted. Terminal
        known-outcome rows require an explicit opt-in; production uses the dedicated
        cleanup-only selector instead of mixing them into active recovery.
        """
        now = now or utc_now_iso()
        if job_ids is not None and not job_ids:
            return []
        values: list[str] = [now]
        scope = ""
        if job_ids is not None:
            ordered_ids = sorted(set(job_ids))
            scope = (
                " AND batch_id IN (SELECT DISTINCT batch_id FROM jobs WHERE job_id IN ("
                + ",".join("?" * len(ordered_ids)) + ") AND batch_id IS NOT NULL)"
            )
            values.extend(ordered_ids)
        state_clause = (
            "(state NOT IN ('done','failed') OR "
            "(state IN ('done','failed') AND target_finalized_at IS NULL "
            "AND target_finalization_disposition IN (?,?)))"
            if include_terminal else "state NOT IN ('done','failed')"
        )
        state_values = (
            FINALIZATION_PENDING, FINALIZATION_REPLAYABLE
        ) if include_terminal else ()
        rows = self.db.execute(
            "SELECT batch_id,state,device,target_operation_id,target_request_sha256,"
            "target_resume_token,target_reserved_at,target_accepted_at,"
            "target_cleanup_state,target_stage_cleaned_at,"
            "target_durable_cleaned_at,target_finalized_at,target_finalization_disposition "
            "FROM batches WHERE " + state_clause + " AND lease_until<? "
            "AND target_protocol_version=1" + scope,
            (*state_values, *values),
        ).fetchall()
        return [dict(row) for row in rows]

    def terminal_target_batches(
        self, now: str | None = None,
        job_ids: list[str] | tuple[str, ...] | set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return known-outcome terminal rows whose target cleanup is unfinished."""
        now = now or utc_now_iso()
        if job_ids is not None and not job_ids:
            return []
        values: list[str] = [now]
        scope = ""
        if job_ids is not None:
            ordered_ids = sorted(set(job_ids))
            scope = (
                " AND batch_id IN (SELECT DISTINCT batch_id FROM jobs WHERE job_id IN ("
                + ",".join("?" * len(ordered_ids)) + ") AND batch_id IS NOT NULL)"
            )
            values.extend(ordered_ids)
        rows = self.db.execute(
            "SELECT batch_id,state,device,target_operation_id,target_request_sha256,"
            "target_resume_token,target_reserved_at,target_accepted_at,"
            "target_cleanup_state,target_stage_cleaned_at,"
            "target_durable_cleaned_at,target_finalized_at,target_finalization_disposition "
            "FROM batches WHERE state IN ('done','failed') AND target_protocol_version=1 "
            "AND target_finalized_at IS NULL AND target_finalization_disposition IN (?,?) "
            "AND lease_until<?" + scope,
            (FINALIZATION_PENDING, FINALIZATION_REPLAYABLE, *values),
        ).fetchall()
        return [dict(row) for row in rows]

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
        protected_target = (
            "((target_protocol_version=1 AND "
            "COALESCE(target_finalization_disposition,'') <> 'fully_finalized') OR "
            "target_operation_id IS NOT NULL OR "
            "target_request_sha256 IS NOT NULL OR target_resume_token IS NOT NULL OR "
            "target_reserved_at IS NOT NULL OR target_accepted_at IS NOT NULL)"
        )
        with self._immediate():
            protected = self.db.execute(
                f"SELECT COUNT(*) c FROM batches WHERE {protected_target} "
                "AND target_finalized_at IS NULL"
            ).fetchone()["c"]
            removable = (
                "(batch_id IS NULL OR batch_id NOT IN ("
                f"SELECT batch_id FROM batches WHERE {protected_target} "
                "AND target_finalized_at IS NULL))"
            )
            if include_final:
                jobs = self.db.execute(f"DELETE FROM jobs WHERE {removable}").rowcount
                self.db.execute(
                    f"DELETE FROM batches WHERE NOT {protected_target} "
                    "OR target_finalized_at IS NOT NULL"
                )
            else:
                jobs = self.db.execute(
                    f"DELETE FROM jobs WHERE state NOT IN ({_FINAL_Q}) AND {removable}",
                    _FINAL,
                ).rowcount
                self.db.execute(
                    "DELETE FROM batches WHERE state NOT IN ('done','failed') "
                    f"AND (NOT {protected_target} OR target_finalized_at IS NOT NULL)"
                )
            leases = self.db.execute(
                "DELETE FROM resource_leases WHERE batch_id NOT IN ("
                f"SELECT batch_id FROM batches WHERE {protected_target} "
                "AND target_finalized_at IS NULL)"
            ).rowcount
            cooldowns = self.db.execute("DELETE FROM cooldowns").rowcount
        return {
            "jobs": jobs,
            "leases": leases,
            "cooldowns": cooldowns,
            "protected_unfinalized": int(protected),
        }

    def prune_final(self, keep: int = 500) -> int:
        """Bound the table: drop all but the most-recent ``keep`` final (done/failed_final)
        jobs, and any batch with no remaining jobs. Returns rows deleted."""
        rows = self.db.execute(
            f"SELECT j.job_id FROM jobs j LEFT JOIN batches b ON b.batch_id=j.batch_id "
            f"WHERE j.state IN ({','.join('?' * len(_PRUNABLE))}) "
            "AND (j.batch_id IS NULL OR NOT ((b.target_protocol_version=1 AND "
            "COALESCE(b.target_finalization_disposition,'') <> 'fully_finalized') OR "
            "b.target_operation_id IS NOT NULL OR b.target_request_sha256 IS NOT NULL OR "
            "b.target_resume_token IS NOT NULL OR b.target_reserved_at IS NOT NULL OR "
            "b.target_accepted_at IS NOT NULL) "
            "OR b.target_finalized_at IS NOT NULL) ORDER BY j.updated_at DESC",
            _PRUNABLE).fetchall()
        victims = [r["job_id"] for r in rows[keep:]]
        with self._immediate():
            for jid in victims:
                self.db.execute("DELETE FROM jobs WHERE job_id=?", (jid,))
            # Never delete a batch that still holds a live resource lease (would orphan it).
            self.db.execute(
                "DELETE FROM batches WHERE state IN ('done','failed') "
                "AND batch_id NOT IN (SELECT DISTINCT batch_id FROM jobs WHERE batch_id IS NOT NULL) "
                "AND batch_id NOT IN (SELECT batch_id FROM resource_leases) "
                "AND (NOT ((target_protocol_version=1 AND "
                "COALESCE(target_finalization_disposition,'') <> 'fully_finalized') OR "
                "target_operation_id IS NOT NULL OR target_request_sha256 IS NOT NULL OR "
                "target_resume_token IS NOT NULL OR "
                "target_reserved_at IS NOT NULL OR target_accepted_at IS NOT NULL) "
                "OR target_finalized_at IS NOT NULL)")
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
