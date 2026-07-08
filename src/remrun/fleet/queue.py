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
import re
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from ..state import utc_now_iso
from .models import FleetTask

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id           TEXT PRIMARY KEY,
    task_type        TEXT NOT NULL,
    payload_json     TEXT NOT NULL,
    force_device     TEXT,
    engine           TEXT,
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
    output_manifest  TEXT
);
CREATE INDEX IF NOT EXISTS ix_jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS ix_jobs_idem  ON jobs(idempotency_key);
CREATE INDEX IF NOT EXISTS ix_jobs_batch ON jobs(batch_id);

-- A batch = one worker invocation over a compatible burst (one cold model load).
CREATE TABLE IF NOT EXISTS batches (
    batch_id     TEXT PRIMARY KEY,
    state        TEXT NOT NULL,          -- leased|staging|running|fetching|done|failed
    device       TEXT NOT NULL,
    task_type    TEXT, engine TEXT, bucket TEXT,
    created_at   TEXT NOT NULL, updated_at TEXT NOT NULL,
    lease_until  TEXT NOT NULL, heartbeat_at TEXT,
    estimated_finish_s REAL NOT NULL DEFAULT 0,   -- own compute estimate, for backlog-aware placement
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
"""

_FINAL = ("done", "failed_final")
_BATCH_ACTIVE = ("leased", "staging", "running", "fetching")
MAX_ATTEMPTS = 3
_RESERVED_OUTPUTS_KEY = "_reserved_outputs"
_RESERVED_OUTPUT_STEM_KEY = "_reserved_output_stem"


def _parse_iso(s: str | None) -> float:
    """Parse a utc_now_iso() timestamp (…Z) to epoch seconds; 0.0 if unparseable."""
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except (ValueError, AttributeError):
        return 0.0


def _safe_output_stem(raw: str, fallback: str = "output", maxlen: int = 40) -> str:
    stem = re.sub(r"\s+", " ", (raw or "").strip())[:maxlen]
    stem = re.sub(r"[^0-9A-Za-z ._-]+", "", stem).strip(" ._-")
    stem = stem.replace(" ", "_")
    return stem or fallback


def _reserved_outputs(task: FleetTask, job_id: str) -> list[dict[str, str]]:
    """Per-input output stems reserved before dispatch.

    Workers derive final output names from staged input stems, so we reserve the input's OWN
    stem — the produced file matches the source name exactly (``notes.md`` -> ``notes.m4a``),
    which is what a folder/multi-file batch is expected to yield. This is stable across retries
    and cross-device reruns (the stem is a pure function of the input), so no timestamp/job tag is
    needed for convergence. Distinct inputs that share a basename WITHIN one job are disambiguated
    with a ``-2`` suffix; the executor additionally de-dupes staged names across a batch so two
    same-named inputs from different jobs can't clobber each other. ``cmd`` stays passthrough.
    (``job_id`` is retained for call-site stability; the reservation no longer depends on it.)
    """
    if task.task_type not in ("ocr", "tts"):
        return []
    used: set[str] = set()

    def pick(base: str, fallback: str) -> str:
        stem = _safe_output_stem(base, fallback)
        candidate = stem
        n = 2
        while candidate in used:
            candidate = f"{stem}-{n}"
            n += 1
        used.add(candidate)
        return candidate

    if task.text is not None:
        return [{"source": "text", "stem": pick(task.text, "clip")}]
    out = []
    for src in task.inputs:
        out.append({"source": str(src), "stem": pick(Path(src).stem, "input")})
    return out


class FleetQueue:
    def __init__(self, db_path: Path) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        # Autocommit (isolation_level=None) so the multi-statement transitions below can
        # use explicit BEGIN IMMEDIATE transactions (all-or-nothing, race-safe).
        self.db = sqlite3.connect(str(db_path), isolation_level=None)
        self.db.row_factory = sqlite3.Row
        for pragma in ("journal_mode=WAL", "busy_timeout=5000"):
            try:
                self.db.execute(f"PRAGMA {pragma}")
            except sqlite3.Error:
                pass
        self.db.executescript(_SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """Add columns missing from pre-existing tables (fleet.db is regenerable local state, so
        this is a best-effort forward migration)."""
        for table, cols in (
            ("jobs", (("batch_id", "TEXT"), ("output_manifest", "TEXT"),
                      ("assigned_device", "TEXT"), ("leased_until", "TEXT"))),
            ("batches", (("estimated_finish_s", "REAL NOT NULL DEFAULT 0"),)),
        ):
            try:
                have = {r["name"] for r in self.db.execute(f"PRAGMA table_info({table})")}
            except sqlite3.Error:
                continue
            for col, decl in cols:
                if col not in have:
                    try:
                        self.db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
                    except sqlite3.Error:
                        pass

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
    def enqueue(self, task: FleetTask, *, priority: int = 0, now: str | None = None,
                job_id: str | None = None) -> str:
        """Add a job. If a non-final job with the same idempotency_key exists,
        return that job's id instead of creating a duplicate."""
        now = now or utc_now_iso()
        if task.idempotency_key:
            row = self.db.execute(
                "SELECT job_id FROM jobs WHERE idempotency_key=? AND state NOT IN (?,?)",
                (task.idempotency_key, *_FINAL)).fetchone()
            if row:
                return row["job_id"]
        jid = job_id or uuid.uuid4().hex[:12]
        options = dict(task.options)
        reserved = options.get(_RESERVED_OUTPUTS_KEY)
        if not reserved:
            reserved = _reserved_outputs(task, jid)
            if reserved:
                options[_RESERVED_OUTPUTS_KEY] = reserved
                options[_RESERVED_OUTPUT_STEM_KEY] = reserved[0]["stem"]
        payload = json.dumps({"text": task.text, "inputs": task.inputs,
                              "options": options, "output_root": task.output_root})
        self.db.execute(
            "INSERT INTO jobs (job_id, task_type, payload_json, force_device, engine, "
            "priority, idempotency_key, state, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?, 'queued', ?, ?)",
            (jid, task.task_type, payload, task.force_device, task.engine, priority,
             task.idempotency_key, now, now))
        self.db.commit()
        return jid

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

    def active_backlog(self, now: str | None = None) -> dict[str, float]:
        """Per-device REMAINING compute (seconds) of in-flight batches — the backlog the scheduler
        adds to a new job's finish estimate so it won't pile onto a busy device when an idle one
        would finish sooner. Per batch: max(0, estimated_finish_s - elapsed_since_claim).
        A configured exclusive resource pool usually means one active batch per
        device for that pool, so these do not double-count a shared wait."""
        now = now or utc_now_iso()
        now_t = _parse_iso(now)
        out: dict[str, float] = {}
        rows = self.db.execute(
            "SELECT device, created_at, estimated_finish_s FROM batches "
            "WHERE state IN ('leased','staging','running','fetching')").fetchall()
        for r in rows:
            est = float(r["estimated_finish_s"] or 0.0)
            if est <= 0:
                continue
            started = _parse_iso(r["created_at"])
            elapsed = max(0.0, now_t - started) if (now_t and started) else 0.0
            out[r["device"]] = out.get(r["device"], 0.0) + max(0.0, est - elapsed)
        return out

    # --- state transitions ------------------------------------------------
    def claim(self, job_id: str, device: str, *, lease_until: str = "",
              batch_id: str = "", now: str | None = None) -> bool:
        """Atomically move a queued job to 'leased' on a device. Returns False if
        it was already taken (lost the race)."""
        now = now or utc_now_iso()
        cur = self.db.execute(
            "UPDATE jobs SET state='leased', assigned_device=?, leased_until=?, "
            "batch_id=?, attempts=attempts+1, updated_at=? "
            "WHERE job_id=? AND state='queued'",
            (device, lease_until, batch_id, now, job_id))
        self.db.commit()
        return cur.rowcount == 1

    def set_state(self, job_id: str, state: str, *, now: str | None = None,
                  error: str | None = None, output_manifest: str | None = None) -> None:
        now = now or utc_now_iso()
        self.db.execute(
            "UPDATE jobs SET state=?, updated_at=?, "
            "last_error=COALESCE(?, last_error), output_manifest=COALESCE(?, output_manifest) "
            "WHERE job_id=?", (state, now, error, output_manifest, job_id))
        self.db.commit()

    def fail(self, job_id: str, error: str, *, now: str | None = None,
             max_attempts: int = MAX_ATTEMPTS) -> str:
        """Mark a failure; requeue if attempts remain, else final. Returns the new state."""
        now = now or utc_now_iso()
        row = self.get(job_id)
        attempts = int(row["attempts"]) if row else max_attempts
        new_state = "queued" if attempts < max_attempts else "failed_final"
        self.db.execute(
            "UPDATE jobs SET state=?, last_error=?, updated_at=?, "
            "assigned_device=CASE WHEN ?='queued' THEN NULL ELSE assigned_device END, "
            "leased_until=NULL WHERE job_id=?",
            (new_state, error, now, new_state, job_id))
        self.db.commit()
        return new_state

    def recover_stale_leases(self, now: str | None = None) -> int:
        """Requeue jobs whose lease expired (worker/device died mid-flight). Clears
        ``batch_id`` too so a requeued job is never left tied to a defunct batch."""
        now = now or utc_now_iso()
        cur = self.db.execute(
            "UPDATE jobs SET state='queued', assigned_device=NULL, batch_id=NULL, "
            "leased_until=NULL, updated_at=? WHERE state IN ('leased','staging','running',"
            "'fetching') AND leased_until IS NOT NULL AND leased_until < ?", (now, now))
        return cur.rowcount

    # --- batch + resource-lease primitives (dispatcher) -------------------
    def claim_many(self, job_ids: list[str], device: str, *, batch_id: str,
                   lease_until: str, pool: str | None = "gpu", task_type: str = "",
                   engine: str = "", bucket: str = "", estimated_finish_s: float = 0.0,
                   now: str | None = None) -> bool:
        """Atomically lease a whole compatible group to one device+batch.

        If ``pool`` is truthy, also acquire the per-(device,pool) resource lease —
        ALL-OR-NOTHING (BEGIN IMMEDIATE). Returns False and changes nothing if
        the pool slot is held or any job is no longer 'queued'.
        """
        now = now or utc_now_iso()
        if not job_ids or lease_until <= now:   # never grant an already-expired lease
            return False
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
                        return False
                n = self.db.execute(
                    f"SELECT COUNT(*) c FROM jobs WHERE job_id IN ({qmarks}) AND state='queued'",
                    tuple(job_ids)).fetchone()["c"]
                if n != len(job_ids):
                    self.db.execute("ROLLBACK")
                    return False
                self.db.execute(
                    "INSERT INTO batches (batch_id,state,device,task_type,engine,bucket,"
                    "created_at,updated_at,lease_until,heartbeat_at,estimated_finish_s) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (batch_id, "leased", device, task_type, engine, bucket, now, now,
                     lease_until, now, float(estimated_finish_s)))
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
                    return False
                self.db.execute("COMMIT")
                return True
            except sqlite3.Error:
                self.db.execute("ROLLBACK")
                return False
        except sqlite3.Error:
            return False   # couldn't even BEGIN (lock contention past busy_timeout)

    def set_batch_state(self, batch_id: str, state: str, *, now: str | None = None) -> None:
        now = now or utc_now_iso()
        with self._immediate():
            self.db.execute("UPDATE batches SET state=?, updated_at=? WHERE batch_id=?",
                            (state, now, batch_id))
            self.db.execute(
                f"UPDATE jobs SET state=?, updated_at=? WHERE batch_id=? AND state IN "
                f"({','.join('?' * len(_BATCH_ACTIVE))})", (state, now, batch_id, *_BATCH_ACTIVE))

    def heartbeat(self, batch_id: str, lease_until: str, *, now: str | None = None) -> None:
        """Atomically extend the batch + its jobs' + its resource lease's expiry (long OCR
        batches can outlive the initial lease; without this, recover_stale would requeue
        them). Only an ACTIVE batch is extended — a completed/failed one stays put even if
        a late heartbeat thread fires."""
        now = now or utc_now_iso()
        active = ",".join("?" * len(_BATCH_ACTIVE))
        with self._immediate():
            cur = self.db.execute(
                f"UPDATE batches SET lease_until=?, heartbeat_at=?, updated_at=? "
                f"WHERE batch_id=? AND state IN ({active})",
                (lease_until, now, now, batch_id, *_BATCH_ACTIVE))
            if cur.rowcount:   # batch still active: keep its lease + jobs in sync
                self.db.execute("UPDATE resource_leases SET lease_until=? WHERE batch_id=?",
                                (lease_until, batch_id))
                self.db.execute(
                    f"UPDATE jobs SET leased_until=? WHERE batch_id=? AND state IN ({active})",
                    (lease_until, batch_id, *_BATCH_ACTIVE))

    def complete_batch(self, batch_id: str, *, now: str | None = None) -> bool:
        """Mark a batch done — but ONLY if it is still ACTIVE. Fencing (audit Finding 3): a
        late completer that lost its lease to stale recovery must NOT resurrect a recovered/
        terminal batch row or its jobs. Returns True iff this call performed the transition."""
        now = now or utc_now_iso()
        active = ",".join("?" * len(_BATCH_ACTIVE))
        with self._immediate():
            cur = self.db.execute(
                f"UPDATE batches SET state='done', updated_at=? WHERE batch_id=? "
                f"AND state IN ({active})", (now, batch_id, *_BATCH_ACTIVE))
            if not cur.rowcount:        # already terminal / recovered by someone else — no-op
                return False
            self.db.execute("UPDATE jobs SET state='done', leased_until=NULL, updated_at=? "
                            "WHERE batch_id=? AND state NOT IN (?,?)", (now, batch_id, *_FINAL))
            self.db.execute("DELETE FROM resource_leases WHERE batch_id=?", (batch_id,))
            return True

    def complete_batch_items(self, batch_id: str, succeeded: dict[str, str | None],
                             failed: dict[str, str], *, now: str | None = None,
                             max_attempts: int = MAX_ATTEMPTS,
                             clear_force_device: bool = False) -> bool:
        """Finish an ACTIVE batch from per-item worker results.

        ``succeeded`` maps job_id -> optional output_manifest JSON; those jobs become ``done``.
        ``failed`` maps job_id -> error; those jobs requeue until attempts are exhausted. Any
        active job in the batch that is not mentioned is treated as failed, because a partial
        metrics file is not enough evidence to mark it done. ``clear_force_device`` turns a
        retryable forced-device failure into an auto-placed retry (used only by opt-in fallback).
        Returns True iff this call performed the transition (False = already terminal/recovered).
        """
        now = now or utc_now_iso()
        active = ",".join("?" * len(_BATCH_ACTIVE))
        with self._immediate():
            rows = self.db.execute(
                f"SELECT job_id FROM jobs WHERE batch_id=? AND state IN ({active})",
                (batch_id, *_BATCH_ACTIVE)).fetchall()
            active_ids = {r["job_id"] for r in rows}
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
                f"UPDATE batches SET state=?, error=?, updated_at=? WHERE batch_id=? "
                f"AND state IN ({active})", (batch_state, batch_error or None,
                                             now, batch_id, *_BATCH_ACTIVE))
            if not cur.rowcount:
                return False
            for jid in done_ids:
                self.db.execute(
                    "UPDATE jobs SET state='done', leased_until=NULL, output_manifest=?, "
                    "updated_at=? WHERE job_id=? AND batch_id=? AND state IN "
                    f"({active})",
                    (succeeded.get(jid), now, jid, batch_id, *_BATCH_ACTIVE))
            for jid in failed_ids:
                err = failed.get(jid) or "missing item result"
                self.db.execute(
                    "UPDATE jobs SET "
                    "state=CASE WHEN attempts < ? THEN 'queued' ELSE 'failed_final' END, "
                    "assigned_device=CASE WHEN attempts < ? THEN NULL ELSE assigned_device END, "
                    "batch_id=CASE WHEN attempts < ? THEN NULL ELSE batch_id END, "
                    "force_device=CASE WHEN attempts < ? AND ? THEN NULL ELSE force_device END, "
                    "leased_until=NULL, last_error=?, updated_at=? "
                    "WHERE job_id=? AND batch_id=? AND state IN "
                    f"({active})",
                    (max_attempts, max_attempts, max_attempts, max_attempts,
                     1 if clear_force_device else 0, err, now,
                     jid, batch_id, *_BATCH_ACTIVE))
            self.db.execute("DELETE FROM resource_leases WHERE batch_id=?", (batch_id,))
            return True

    def fail_batch(self, batch_id: str, error: str, *, now: str | None = None,
                   max_attempts: int = MAX_ATTEMPTS,
                   clear_force_device: bool = False) -> bool:
        """Fail a batch atomically — but ONLY if it is still ACTIVE (same fencing as
        ``complete_batch``): requeue its jobs that have attempts left (clearing their batch +
        device), finalize the rest, drop the resource lease so the device frees up.
        ``clear_force_device`` unpins retryable rows for explicit fallback. Returns True iff this
        call performed the transition (False = already terminal/recovered)."""
        now = now or utc_now_iso()
        active = ",".join("?" * len(_BATCH_ACTIVE))
        with self._immediate():
            cur = self.db.execute(
                f"UPDATE batches SET state='failed', error=?, updated_at=? WHERE batch_id=? "
                f"AND state IN ({active})", (error, now, batch_id, *_BATCH_ACTIVE))
            if not cur.rowcount:        # already terminal / recovered by someone else — no-op
                return False
            self.db.execute(
                "UPDATE jobs SET "
                "state=CASE WHEN attempts < ? THEN 'queued' ELSE 'failed_final' END, "
                "assigned_device=CASE WHEN attempts < ? THEN NULL ELSE assigned_device END, "
                "batch_id=CASE WHEN attempts < ? THEN NULL ELSE batch_id END, "
                "force_device=CASE WHEN attempts < ? AND ? THEN NULL ELSE force_device END, "
                "leased_until=NULL, last_error=?, updated_at=? "
                "WHERE batch_id=? AND state NOT IN (?,?)",
                (max_attempts, max_attempts, max_attempts, max_attempts,
                 1 if clear_force_device else 0, error, now, batch_id, *_FINAL))
            self.db.execute("DELETE FROM resource_leases WHERE batch_id=?", (batch_id,))
            return True

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
        for r in stale:
            self.fail_batch(r["batch_id"], "lease expired (stale recovery)", now=now)
        self.db.execute("DELETE FROM resource_leases WHERE lease_until<?", (now,))
        self.recover_stale_leases(now)
        return len(stale)

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
                jobs = self.db.execute("DELETE FROM jobs WHERE state NOT IN (?,?)", _FINAL).rowcount
                self.db.execute("DELETE FROM batches WHERE state NOT IN ('done','failed')")
            leases = self.db.execute("DELETE FROM resource_leases").rowcount
            cooldowns = self.db.execute("DELETE FROM cooldowns").rowcount
        return {"jobs": jobs, "leases": leases, "cooldowns": cooldowns}

    def prune_final(self, keep: int = 500) -> int:
        """Bound the table: drop all but the most-recent ``keep`` final (done/failed_final)
        jobs, and any batch with no remaining jobs. Returns rows deleted."""
        rows = self.db.execute(
            "SELECT job_id FROM jobs WHERE state IN (?,?) ORDER BY updated_at DESC",
            _FINAL).fetchall()
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
