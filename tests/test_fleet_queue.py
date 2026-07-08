"""Durable fleet queue: enqueue/dedupe, claim race, retry/final, stale recovery."""
from __future__ import annotations

from remrun.fleet.models import FleetTask
from remrun.fleet.queue import FleetQueue
import json


def q(tmp_path):
    return FleetQueue(tmp_path / "fleet.db")


def task(key="k1", device=None):
    return FleetTask(task_type="ocr", inputs=["a.pdf"], idempotency_key=key, force_device=device)


def test_enqueue_dedupes_on_idempotency_key(tmp_path):
    fq = q(tmp_path)
    a = fq.enqueue(task("same"), now="t1")
    b = fq.enqueue(task("same"), now="t2")
    assert a == b
    assert len(fq.list()) == 1
    fq.close()


def test_enqueue_reserves_output_stems_for_model_tasks(tmp_path):
    fq = q(tmp_path)
    inp = tmp_path / "scan 1.pdf"
    inp.write_text("x")
    jid = fq.enqueue(FleetTask(task_type="ocr", inputs=[str(inp)], idempotency_key="reserve"),
                     now="t1")
    row = fq.get(jid)
    payload = json.loads(row["payload_json"])
    reserved = payload["options"]["_reserved_outputs"]
    # The reserved stem is the input's own name, so the produced file matches the source exactly
    # (scan 1.pdf -> scan_1.<out>) — no job tag, which would corrupt the "same name as input" rule.
    assert reserved == [{"source": str(inp), "stem": "scan_1"}]
    # Dedupe returns the existing job and therefore preserves the same reservation.
    assert fq.enqueue(FleetTask(task_type="ocr", inputs=[str(inp)], idempotency_key="reserve"),
                      now="t2") == jid
    assert json.loads(fq.get(jid)["payload_json"])["options"]["_reserved_outputs"] == reserved
    fq.close()


def test_enqueue_does_not_reserve_outputs_for_cmd(tmp_path):
    fq = q(tmp_path)
    jid = fq.enqueue(FleetTask(task_type="cmd", inputs=["x.txt"], idempotency_key="cmd"))
    payload = json.loads(fq.get(jid)["payload_json"])
    assert "_reserved_outputs" not in payload["options"]
    fq.close()


def test_claim_is_atomic_single_winner(tmp_path):
    fq = q(tmp_path)
    jid = fq.enqueue(task("c"), now="t1")
    assert fq.claim(jid, "WINBOX", now="t2") is True
    assert fq.claim(jid, "MACBOX", now="t3") is False    # already leased
    assert fq.get(jid)["assigned_device"] == "WINBOX"
    assert fq.active_by_device() == {"WINBOX": 1}
    fq.close()


def test_fail_retries_then_finalizes(tmp_path):
    fq = q(tmp_path)
    jid = fq.enqueue(task("f"), now="t1")
    fq.claim(jid, "WINBOX", now="t2")                    # attempts=1
    assert fq.fail(jid, "boom", now="t3", max_attempts=2) == "queued"
    assert fq.get(jid)["assigned_device"] is None      # requeued, device cleared
    fq.claim(jid, "WINBOX", now="t4")                    # attempts=2
    assert fq.fail(jid, "boom again", now="t5", max_attempts=2) == "failed_final"
    fq.close()


def test_clear_unsticks_queue_leases_and_cooldowns(tmp_path):
    fq = q(tmp_path)
    queued = fq.enqueue(task("queued"), now="t1")          # stuck/queued
    leased = fq.enqueue(task("leased"), now="t1")          # in-flight (lease held)
    assert fq.claim_many([leased], "WINBOX", batch_id="b1", lease_until="t9", now="t2")
    fq.set_cooldown("WINBOX", "t9", engine="ocr", kind="oom")
    done = fq.enqueue(task("done"), now="t1")              # finished history (kept by default)
    fq.claim_many([done], "MACBOX", batch_id="b2", lease_until="t9", now="t2")
    fq.complete_batch("b2", now="t3")
    assert fq.lease_usage().get("WINBOX", {}).get("gpu", 0) == 1
    assert fq.active_cooldowns() != []

    res = fq.clear()                                        # keep history
    assert res["jobs"] == 2                                 # queued + leased dropped
    assert res["leases"] >= 1 and res["cooldowns"] == 1
    c = fq.counts()
    assert c.get("queued", 0) == 0 and c.get("leased", 0) == 0
    assert c.get("done", 0) == 1                            # done history retained
    assert fq.lease_usage() == {} and fq.active_cooldowns() == []
    assert {queued, leased}                                 # (ids referenced; jobs gone)

    assert fq.clear(include_final=True)["jobs"] == 1        # --all wipes the done job too
    assert fq.counts() == {}
    fq.close()


def test_active_backlog_tracks_remaining_compute(tmp_path):
    fq = q(tmp_path)
    jid = fq.enqueue(task("b"), now="2026-01-01T00:00:00Z")
    assert fq.claim_many([jid], "MACBOX", batch_id="bk", lease_until="2026-01-01T01:00:00Z",
                         estimated_finish_s=100.0, now="2026-01-01T00:00:00Z")
    bl = fq.active_backlog(now="2026-01-01T00:00:30Z")   # 30 s in -> ~70 s remaining
    assert abs(bl["MACBOX"] - 70.0) < 1.0
    bl2 = fq.active_backlog(now="2026-01-01T00:05:00Z")  # past the estimate -> clamped to 0
    assert bl2.get("MACBOX", 0.0) == 0.0
    fq.close()


def test_recover_stale_leases(tmp_path):
    fq = q(tmp_path)
    jid = fq.enqueue(task("s"), now="t1")
    fq.claim(jid, "WINBOX", lease_until="2020-01-01T00:00:00Z", now="t2")
    n = fq.recover_stale_leases(now="2026-01-01T00:00:00Z")
    assert n == 1 and fq.get(jid)["state"] == "queued"
    fq.close()


def test_counts_by_state(tmp_path):
    fq = q(tmp_path)
    fq.enqueue(task("a"), now="t1")
    j = fq.enqueue(task("b"), now="t2")
    fq.claim(j, "MACBOX", now="t3")
    fq.set_state(j, "done", now="t4")
    counts = fq.counts()
    assert counts.get("queued") == 1 and counts.get("done") == 1
    fq.close()


# --- batch + resource-lease primitives (dispatcher) ---------------------------

_FAR = "2099-01-01T00:00:00Z"


def test_claim_many_all_or_nothing_and_gpu_mutex(tmp_path):
    fq = q(tmp_path)
    a = fq.enqueue(task("a"), now="t1")
    b = fq.enqueue(task("b"), now="t2")
    assert fq.claim_many([a, b], "MACBOX", batch_id="B1", lease_until=_FAR) is True
    # A second batch can't take MACBOX's gpu slot while B1 holds it.
    c = fq.enqueue(task("c"), now="t3")
    assert fq.claim_many([c], "MACBOX", batch_id="B2", lease_until=_FAR) is False
    assert fq.get(c)["state"] == "queued"           # untouched
    assert fq.get_batch("B2") is None                 # nothing created


def test_claim_many_rejects_if_a_job_not_queued(tmp_path):
    fq = q(tmp_path)
    a = fq.enqueue(task("a"), now="t1")
    b = fq.enqueue(task("b"), now="t2")
    fq.set_state(a, "running")           # a no longer queued
    assert fq.claim_many([a, b], "MACBOX", batch_id="B1", lease_until=_FAR) is False
    assert fq.get(b)["state"] == "queued"           # all-or-nothing: b untouched


def test_complete_batch_frees_lease_and_marks_done(tmp_path):
    fq = q(tmp_path)
    a = fq.enqueue(task("a"), now="t1")
    fq.claim_many([a], "MACBOX", batch_id="B1", lease_until=_FAR)
    fq.complete_batch("B1")
    assert fq.get(a)["state"] == "done"
    # lease freed -> a new batch can take MACBOX now.
    b = fq.enqueue(task("b"), now="t2")
    assert fq.claim_many([b], "MACBOX", batch_id="B2", lease_until=_FAR) is True


def test_complete_batch_items_done_and_requeue_failed(tmp_path):
    fq = q(tmp_path)
    a = fq.enqueue(task("a"), now="t1")
    b = fq.enqueue(task("b"), now="t1")
    assert fq.claim_many([a, b], "MACBOX", batch_id="B1", lease_until=_FAR)
    assert fq.complete_batch_items("B1", {a: '{"ok": true}'}, {b: "bad page"})
    assert fq.get(a)["state"] == "done"
    assert fq.get(a)["output_manifest"] == '{"ok": true}'
    assert fq.get(b)["state"] == "queued"
    assert fq.get(b)["assigned_device"] is None and fq.get(b)["batch_id"] is None
    assert fq.lease_usage() == {}
    c = fq.enqueue(task("c"), now="t2")
    assert fq.claim_many([c], "MACBOX", batch_id="B2", lease_until=_FAR) is True


def test_complete_batch_items_missing_result_requeues_job(tmp_path):
    fq = q(tmp_path)
    a = fq.enqueue(task("a"), now="t1")
    b = fq.enqueue(task("b"), now="t1")
    assert fq.claim_many([a, b], "MACBOX", batch_id="B1", lease_until=_FAR)
    assert fq.complete_batch_items("B1", {a: None}, {})
    assert fq.get(a)["state"] == "done"
    assert fq.get(b)["state"] == "queued"
    assert "missing item result" in fq.get(b)["last_error"]


def test_fail_batch_requeues_then_finalizes(tmp_path):
    fq = q(tmp_path)
    a = fq.enqueue(task("a"), now="t1")
    for i in range(1, 4):                              # MAX_ATTEMPTS = 3
        bid = f"B{i}"
        assert fq.claim_many([a], "MACBOX", batch_id=bid, lease_until=_FAR) is True
        fq.fail_batch(bid, "boom")
    assert fq.get(a)["state"] == "failed_final"      # out of attempts


def test_claim_many_rejects_an_already_expired_lease(tmp_path):
    fq = q(tmp_path)
    a = fq.enqueue(task("a"))
    assert fq.claim_many([a], "MACBOX", batch_id="B1",
                         lease_until="2000-01-01T00:00:00Z", now="2026-06-29T00:00:00Z") is False
    assert fq.get(a)["state"] == "queued"   # untouched


def test_heartbeat_extends_and_recover_stale_requeues(tmp_path):
    fq = q(tmp_path)
    a = fq.enqueue(task("a"), now="t1")
    assert fq.claim_many([a], "MACBOX", batch_id="B1", lease_until=_FAR) is True  # valid lease
    # heartbeat keeps it far out -> recover_stale at a time before _FAR leaves it alone.
    fq.heartbeat("B1", _FAR)
    assert fq.recover_stale(now="2026-06-29T00:00:00Z") == 0
    assert fq.get(a)["state"] == "leased"
    # push the lease into the past -> recover_stale fails the batch and requeues the job.
    fq.heartbeat("B1", "2000-01-01T00:00:00Z")
    assert fq.recover_stale(now="2026-06-29T00:00:00Z") == 1
    assert fq.get(a)["state"] == "queued" and fq.get(a)["assigned_device"] is None


# --- batch-state fencing (audit Finding 3) ------------------------------------

def test_late_complete_after_stale_recovery_is_noop(tmp_path):
    fq = q(tmp_path)
    a = fq.enqueue(task("a"), now="t1")
    assert fq.claim_many([a], "MACBOX", batch_id="B1", lease_until=_FAR) is True
    # stale recovery fails the batch (requeues the job, clears its batch_id)
    assert fq.fail_batch("B1", "stale recovery") is True
    assert fq.get(a)["state"] == "queued" and fq.get(a)["batch_id"] is None
    # a LATE completer for the same (now-terminal) batch must not resurrect it
    assert fq.complete_batch("B1") is False              # fenced: no-op
    assert fq.get_batch("B1")["state"] == "failed"        # batch row unchanged
    assert fq.get(a)["state"] == "queued"                 # job still queued, not flipped to done


def test_fail_batch_can_clear_force_device_for_fallback(tmp_path):
    fq = FleetQueue(tmp_path / "fleet" / "fleet.db")
    try:
        a = fq.enqueue(task("a", device="WINBOX"), now="t1")
        assert fq.claim_many([a], "WINBOX", batch_id="B1", lease_until=_FAR, now=_NOW)
        assert fq.fail_batch("B1", "boom", now="t2", clear_force_device=True)
        row = fq.get(a)
        assert row["state"] == "queued"
        assert row["force_device"] is None
    finally:
        fq.close()


def test_late_fail_after_complete_is_noop(tmp_path):
    fq = q(tmp_path)
    a = fq.enqueue(task("a"), now="t1")
    fq.claim_many([a], "MACBOX", batch_id="B1", lease_until=_FAR)
    assert fq.complete_batch("B1") is True
    assert fq.get(a)["state"] == "done"
    # a LATE failer (e.g. a stale-recovery sweep that selected B1 just before completion)
    assert fq.fail_batch("B1", "lease expired") is False  # fenced: no-op
    assert fq.get_batch("B1")["state"] == "done"          # stays done
    assert fq.get(a)["state"] == "done"                   # job stays done
    fq.close()


# --- cooldowns + lease-usage helpers (Phase 3d / 3a) --------------------------

_NOW = "2026-06-29T00:00:00Z"     # cooldowns compare until > now, so use real ISO timestamps


def test_cooldown_set_active_is_cooled_and_prune(tmp_path):
    fq = q(tmp_path)
    fq.set_cooldown("WINBOX", _FAR, engine="ocr-remote", kind="oom", reason="oom", now=_NOW)
    fq.set_cooldown("MACBOX", _FAR, kind="transport", reason="ssh", now=_NOW)   # device-wide
    # engine-specific: only that engine on that device is cooled
    assert fq.is_cooled("WINBOX", "ocr-remote", now=_NOW) is True
    assert fq.is_cooled("WINBOX", "tts-remote", now=_NOW) is False
    # device-wide ('') cools any engine on the device
    assert fq.is_cooled("MACBOX", "ocr-local", now=_NOW) is True
    assert {r["device"] for r in fq.active_cooldowns(now=_NOW)} == {"WINBOX", "MACBOX"}
    fq.close()


def test_cooldown_expires_and_prunes(tmp_path):
    fq = q(tmp_path)
    fq.set_cooldown("WINBOX", "2000-01-01T00:00:00Z", engine="ocr-remote", now="t1")  # already past
    assert fq.is_cooled("WINBOX", "ocr-remote", now="2026-06-29T00:00:00Z") is False
    assert fq.active_cooldowns(now="2026-06-29T00:00:00Z") == []
    assert fq.prune_cooldowns(now="2026-06-29T00:00:00Z") == 1
    fq.close()


def test_cooldown_upsert_keeps_the_later_until(tmp_path):
    fq = q(tmp_path)
    fq.set_cooldown("WINBOX", "2026-06-29T00:00:10Z", engine="e", now="t1")
    fq.set_cooldown("WINBOX", "2026-06-29T00:00:05Z", engine="e", now="t2")  # earlier — must not shrink
    rows = fq.active_cooldowns(now="2026-06-29T00:00:00Z")
    assert len(rows) == 1 and rows[0]["until"] == "2026-06-29T00:00:10Z"
    fq.close()


def test_lease_usage_and_batch_attempts(tmp_path):
    fq = q(tmp_path)
    a = fq.enqueue(task("a"), now="t1")
    b = fq.enqueue(task("b"), now="t2")
    fq.claim_many([a], "MACBOX", batch_id="B1", lease_until=_FAR)
    fq.claim_many([b], "WINBOX", batch_id="B2", lease_until=_FAR)
    usage = fq.lease_usage(now="2026-06-29T00:00:00Z")
    assert usage == {"MACBOX": {"gpu": 1}, "WINBOX": {"gpu": 1}}
    assert fq.batch_attempts("B1") == 1                    # claimed once
    # an expired lease drops out of usage
    fq.heartbeat("B1", "2000-01-01T00:00:00Z")
    assert "MACBOX" not in fq.lease_usage(now="2026-06-29T00:00:00Z")
    fq.close()


def test_claim_many_returns_false_under_write_lock_not_crash(tmp_path):
    import sqlite3
    fq = q(tmp_path)
    a = fq.enqueue(task("a"))
    fq.db.execute("PRAGMA busy_timeout=100")          # don't wait the full 5s in the test
    other = sqlite3.connect(str(tmp_path / "fleet.db"), isolation_level=None)
    other.execute("PRAGMA busy_timeout=100")
    other.execute("BEGIN IMMEDIATE")
    other.execute("INSERT INTO resource_leases (device,pool,batch_id,lease_until) "
                  "VALUES ('X','p','b',?)", (_FAR,))   # holds the write lock
    try:
        assert fq.claim_many([a], "MACBOX", batch_id="B1", lease_until=_FAR) is False  # no crash
    finally:
        other.execute("ROLLBACK")
        other.close()
    assert fq.get(a)["state"] == "queued"             # untouched
