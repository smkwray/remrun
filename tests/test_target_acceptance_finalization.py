from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun.config import RemrunConfig
from remrun.fleet import dispatcher, queue as queue_mod
from remrun.fleet.prepared import RAW_COMMAND_SPEC_ID, prepare_raw_command
from remrun.models import Device
from remrun.output import Reporter
from remrun.transport import LocalSimTransport

from conftest import native_target_device


OLD = "2000-01-01T00:00:00Z"
NOW = "2026-08-24T00:00:00Z"
FUTURE = "2099-01-01T00:00:00Z"


def _config(root: Path) -> RemrunConfig:
    device = native_target_device(root)
    return RemrunConfig(
        repo_root=root,
        defaults={"fleet": {"pools": {}}},
        devices={"TARGET": device},
        project_roots={},
    )


def _terminal_batch(
    tmp_path: Path, *, suffix: str = "finalization",
) -> tuple[Path, Path, Device, str, str, str]:
    db_path = tmp_path / "controller-state" / "fleet" / "fleet.db"
    batch_id = f"batch-{suffix}"
    job_id = f"job-{suffix}"
    operation_id = f"fleet-{batch_id}"
    request_sha = "c" * 64
    token = "terminal-private-token"
    q = queue_mod.FleetQueue(db_path)
    prepared = prepare_raw_command([sys.executable, "-c", "print('ok')"], device="TARGET")
    job_id = q.enqueue_prepared(prepared, spec=None, job_id=job_id, now=NOW)
    owner = q.claim_many(
        [job_id], "TARGET", batch_id=batch_id, lease_until=FUTURE,
        pool=None, task_name="command", engine="raw", bucket="", now=NOW,
        target_protocol_version=1, current_spec_ids={job_id: RAW_COMMAND_SPEC_ID},
    )
    assert owner is not None
    assert q.record_target_reservation(
        batch_id, operation_id=operation_id, request_sha256=request_sha,
        resume_token=token, expected_state="leased", owner_token=owner, now=NOW,
    )
    assert q.set_batch_state(
        batch_id, "staging", expected_state="leased", owner_token=owner, now=NOW,
    )
    assert q.set_batch_state(
        batch_id, "running", expected_state="staging", owner_token=owner, now=NOW,
    )
    assert q.record_target_acceptance(
        batch_id, operation_id=operation_id, request_sha256=request_sha,
        expected_state="running", owner_token=owner, now=NOW,
    )
    assert q.set_batch_state(
        batch_id, "fetching", expected_state="running", owner_token=owner, now=NOW,
    )
    assert q.complete_batch(
        batch_id, expected_state="fetching", owner_token=owner, now=NOW,
        finalization_disposition=queue_mod.FINALIZATION_PENDING,
    )
    q.db.execute(
        "UPDATE batches SET lease_until=? WHERE batch_id=?", (OLD, batch_id)
    )
    q.db.commit()
    q.close()
    stage = tmp_path / "target-state" / "fleet-operations" / operation_id
    stage.mkdir(parents=True)
    (stage / "evidence.bin").write_bytes(b"evidence")
    device = _config(tmp_path).devices["TARGET"]
    return db_path, stage, device, operation_id, request_sha, token


def _recovery_fakes(
    monkeypatch: pytest.MonkeyPatch,
    device: Device,
    operation_id: str,
    request_sha: str,
    token: str,
    events: list[str],
    crash_after: str | None = None,
) -> None:
    fired = {"value": False}
    durable_present = {"value": True}

    class Client:
        def status_identity(self, operation, supplied_token, **_kwargs):  # noqa: ANN001, ANN003
            assert (operation, supplied_token) == (operation_id, token)
            events.append("target_terminality")
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
                "state": "complete",
                "target_acceptance": {
                    "operation_id": operation_id,
                    "request_sha256": request_sha,
                },
                "target_cleanup": {"state": "RELEASED"},
            }

        def remove_remote_tree(self, path):  # noqa: ANN001
            events.append("stage_delete")
            super().remove_remote_tree(path)
            if crash_after == "stage" and not fired["value"]:
                fired["value"] = True
                raise KeyboardInterrupt("controller loss after stage deletion")

        def durable_cleanup(self, operation, supplied_token):  # noqa: ANN001
            assert (operation, supplied_token) == (operation_id, token)
            events.append("durable_delete")
            if not durable_present["value"]:
                return {"cleaned": False, "absent": True}
            durable_present["value"] = False
            if crash_after == "durable" and not fired["value"]:
                fired["value"] = True
                raise KeyboardInterrupt("controller loss after durable deletion")
            return {"cleaned": True}

    monkeypatch.setattr(dispatcher.TargetResourceClient, "connect", lambda *_a, **_k: Client())
    monkeypatch.setattr(dispatcher, "make_transport", lambda _device: Transport(device))


def _event_reporter(events: list[tuple[str, dict]]) -> SimpleNamespace:
    return SimpleNamespace(
        event=lambda name, **fields: events.append((name, fields)),
    )


def test_terminal_cleanup_active_transport_cooldown_skips_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A global cleanup obligation must not probe a device already known to be unreachable."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    db_path, stage, device, _operation_id, _request_sha, _token = _terminal_batch(tmp_path)
    q = queue_mod.FleetQueue(db_path)
    q.set_cooldown(
        "TARGET", FUTURE, kind="transport", reason="transport", now=NOW,
    )
    q.close()

    probes: list[str] = []

    class OfflineTransport(LocalSimTransport):
        def probe(self):  # noqa: ANN201
            probes.append(self.device.name)
            return SimpleNamespace(reachable=False, detail="offline")

    monkeypatch.setattr(dispatcher, "utc_now_iso", lambda: NOW)
    monkeypatch.setattr(dispatcher, "make_transport", lambda _device: OfflineTransport(device))
    events: list[tuple[str, dict]] = []
    summary = dispatcher.drain_once(
        _config(tmp_path), state_root=tmp_path / "controller-state",
        reporter=_event_reporter(events),
    )

    assert summary["recovered"] == 0
    assert probes == []
    q = queue_mod.FleetQueue(db_path)
    try:
        batch = q.get_batch("batch-finalization")
        assert [row["batch_id"] for row in q.terminal_target_batches(FUTURE)] == [
            "batch-finalization"
        ]
    finally:
        q.close()
    assert batch is not None and batch["state"] == "done"
    assert batch["target_finalized_at"] is None
    assert stage.exists()
    assert (
        "dispatch_stale_deferred",
        {
            "batch": "batch-finalization",
            "device": "TARGET",
            "reason": "active transport cooldown",
            "until": FUTURE,
        },
    ) in events


def test_terminal_cleanup_resumes_after_transport_cooldown_expires(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cooldown defers cleanup; expiry exposes the same obligation to normal finalization."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    db_path, stage, device, operation_id, request_sha, token = _terminal_batch(tmp_path)
    cooldown_until = "2026-08-24T00:01:00Z"
    after_cooldown = "2026-08-24T00:02:00Z"
    q = queue_mod.FleetQueue(db_path)
    q.set_cooldown(
        "TARGET", cooldown_until, kind="transport", reason="transport", now=NOW,
    )
    q.close()
    recovery_events: list[str] = []
    _recovery_fakes(
        monkeypatch, device, operation_id, request_sha, token, recovery_events,
    )
    clock = {"now": NOW}
    monkeypatch.setattr(dispatcher, "utc_now_iso", lambda: clock["now"])

    deferred = dispatcher.drain_once(
        _config(tmp_path), state_root=tmp_path / "controller-state",
        reporter=Reporter(json_events=False),
    )
    assert deferred["recovered"] == 0
    assert recovery_events == []
    assert stage.exists()

    clock["now"] = after_cooldown
    resumed = dispatcher.drain_once(
        _config(tmp_path), state_root=tmp_path / "controller-state",
        reporter=Reporter(json_events=False),
    )
    assert resumed["recovered"] == 1
    q = queue_mod.FleetQueue(db_path)
    try:
        batch = q.get_batch("batch-finalization")
        job = q.get("job-finalization")
    finally:
        q.close()
    assert batch is not None and batch["state"] == "done"
    assert batch["target_finalization_disposition"] == queue_mod.FINALIZATION_FINALIZED
    assert batch["target_finalized_at"] == after_cooldown
    assert job is not None and job["state"] == "done"
    assert recovery_events == ["target_terminality", "stage_delete", "durable_delete"]
    assert not stage.exists()


def test_terminal_cleanup_probe_failure_sets_transport_cooldown_and_retains_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One failed cleanup probe backs off the device without changing or dropping known truth."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    db_path, stage, device, _operation_id, _request_sha, _token = _terminal_batch(tmp_path)
    probes: list[str] = []

    class OfflineTransport(LocalSimTransport):
        def probe(self):  # noqa: ANN201
            probes.append(self.device.name)
            return SimpleNamespace(reachable=False, detail="offline")

    monkeypatch.setattr(dispatcher, "utc_now_iso", lambda: NOW)
    monkeypatch.setattr(dispatcher, "make_transport", lambda _device: OfflineTransport(device))
    events: list[tuple[str, dict]] = []
    summary = dispatcher.drain_once(
        _config(tmp_path), state_root=tmp_path / "controller-state",
        reporter=_event_reporter(events),
    )

    assert summary["recovered"] == 0
    assert probes == ["TARGET"]
    q = queue_mod.FleetQueue(db_path)
    try:
        batch = q.get_batch("batch-finalization")
        job = q.get("job-finalization")
        cooldowns = q.active_cooldowns(NOW)
        obligations = q.terminal_target_batches(FUTURE)
    finally:
        q.close()
    assert batch is not None and batch["state"] == "done"
    assert batch["target_finalized_at"] is None
    assert job is not None and job["state"] == "done"
    assert [row["batch_id"] for row in obligations] == ["batch-finalization"]
    assert len(cooldowns) == 1
    assert cooldowns[0]["device"] == "TARGET"
    assert cooldowns[0]["engine"] == ""
    assert cooldowns[0]["kind"] == "transport"
    assert cooldowns[0]["until"] > NOW
    assert any(name == "dispatch_cooldown" for name, _fields in events)
    assert any(name == "dispatch_stale_deferred" for name, _fields in events)
    assert stage.exists()


def test_terminal_cleanup_first_probe_failure_defers_later_rows_on_same_device(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One dead-device probe per tick is enough even when several cleanup rows target it."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    db_path, stage_one, device, _operation_id, _request_sha, _token = _terminal_batch(
        tmp_path, suffix="one",
    )
    _db_path, stage_two, _device, _operation_id, _request_sha, _token = _terminal_batch(
        tmp_path, suffix="two",
    )
    probes: list[str] = []

    class OfflineTransport(LocalSimTransport):
        def probe(self):  # noqa: ANN201
            probes.append(self.device.name)
            return SimpleNamespace(reachable=False, detail="offline")

    monkeypatch.setattr(dispatcher, "utc_now_iso", lambda: NOW)
    monkeypatch.setattr(dispatcher, "make_transport", lambda _device: OfflineTransport(device))
    summary = dispatcher.drain_once(
        _config(tmp_path), state_root=tmp_path / "controller-state",
        reporter=Reporter(json_events=False),
    )

    assert summary["recovered"] == 0
    assert probes == ["TARGET"]
    q = queue_mod.FleetQueue(db_path)
    try:
        obligations = q.terminal_target_batches(FUTURE)
    finally:
        q.close()
    assert sorted(row["batch_id"] for row in obligations) == ["batch-one", "batch-two"]
    assert stage_one.exists()
    assert stage_two.exists()


@pytest.mark.parametrize("crash_after", ["target", "stage", "durable", "finalization"])
def test_terminal_cleanup_crash_points_resume_monotonically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, crash_after: str,
) -> None:
    """Each proof is durable before the next destructive step, so controller loss cannot strand a done job."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    db_path, stage, device, operation_id, request_sha, token = _terminal_batch(tmp_path)
    events: list[str] = []
    _recovery_fakes(monkeypatch, device, operation_id, request_sha, token, events, crash_after)
    q = queue_mod.FleetQueue(db_path)
    original_progress = q.record_target_cleanup_progress
    original_finalization = q.record_expired_target_finalization
    fired = {"value": False}

    def progress(*args, **kwargs):  # noqa: ANN002, ANN003
        result = original_progress(*args, **kwargs)
        if crash_after == "target" and kwargs.get("cleanup_state") and not fired["value"]:
            fired["value"] = True
            raise KeyboardInterrupt("controller loss after target terminality proof")
        return result

    def finalization(*args, **kwargs):  # noqa: ANN002, ANN003
        if crash_after == "finalization" and not fired["value"]:
            fired["value"] = True
            raise KeyboardInterrupt("controller loss before queue finalization")
        return original_finalization(*args, **kwargs)

    q.record_target_cleanup_progress = progress
    q.record_expired_target_finalization = finalization
    with pytest.raises(KeyboardInterrupt):
        dispatcher._recover_terminal_target_finalization(
            _config(tmp_path), q, FUTURE,
            reporter=Reporter(json_events=False),
        )
    q.close()

    q = queue_mod.FleetQueue(db_path)
    try:
        assert dispatcher._recover_terminal_target_finalization(
            _config(tmp_path), q, FUTURE, reporter=Reporter(json_events=False),
        ) == 1
        batch = dict(q.db.execute(
            "SELECT state,target_cleanup_state,target_stage_cleaned_at,"
            "target_durable_cleaned_at,target_finalized_at,target_finalization_disposition "
            "FROM batches WHERE batch_id='batch-finalization'"
        ).fetchone())
        job = q.get("job-finalization")
    finally:
        q.close()
    assert batch["state"] == "done"
    assert batch["target_cleanup_state"] == "RELEASED"
    assert batch["target_stage_cleaned_at"]
    assert batch["target_durable_cleaned_at"]
    assert batch["target_finalized_at"]
    assert batch["target_finalization_disposition"] == queue_mod.FINALIZATION_FINALIZED
    assert job is not None and job["state"] == "done"
    assert not stage.exists()


def test_two_controllers_repeated_terminal_recovery_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exact identity CAS plus absence-aware deletion makes repeated controllers harmless."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    db_path, stage, device, operation_id, request_sha, token = _terminal_batch(tmp_path)
    events: list[str] = []
    _recovery_fakes(monkeypatch, device, operation_id, request_sha, token, events)
    q1 = queue_mod.FleetQueue(db_path)
    q2 = queue_mod.FleetQueue(db_path)
    try:
        row = q1.terminal_target_batches(FUTURE)[0]
        assert dispatcher._recover_target_row(
            _config(tmp_path), q1, row, FUTURE,
            reporter=Reporter(json_events=False), terminal_outcome=True,
        ) == 1
        # q2 deliberately uses the first controller's stale snapshot. Its
        # exact-identity writes may repeat, but it cannot alter the done outcome.
        assert dispatcher._recover_target_row(
            _config(tmp_path), q2, row, FUTURE,
            reporter=Reporter(json_events=False), terminal_outcome=True,
        ) == 1
        batch = dict(q2.db.execute(
            "SELECT state,target_finalized_at,target_finalization_disposition "
            "FROM batches WHERE batch_id='batch-finalization'"
        ).fetchone())
    finally:
        q1.close()
        q2.close()
    assert batch == {
        "state": "done",
        "target_finalized_at": batch["target_finalized_at"],
        "target_finalization_disposition": queue_mod.FINALIZATION_FINALIZED,
    }
    assert len([event for event in events if event == "stage_delete"]) == 2
    assert len([event for event in events if event == "durable_delete"]) == 2
    assert not stage.exists()


@pytest.mark.parametrize("label", ["completion_unknown", "acceptance_unknown"])
def test_fenced_terminal_work_is_not_selected_for_cleanup_or_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, label: str,
) -> None:
    """Unknown or started work is protected evidence, not a cleanup-only success."""
    del label
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    db_path, stage, _device, _operation_id, _request_sha, _token = _terminal_batch(tmp_path)
    q = queue_mod.FleetQueue(db_path)
    q.db.execute(
        "UPDATE batches SET target_finalization_disposition=? WHERE batch_id=?",
        (queue_mod.FINALIZATION_FENCED, "batch-finalization"),
    )
    q.db.commit()
    assert q.terminal_target_batches(FUTURE) == []
    assert q.stale_target_batches(FUTURE) == []
    q.close()
    assert stage.exists()


def test_malformed_and_started_active_target_work_remain_fenced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed identity and target YES truth never enter the no-start replay path."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    db_path, stage, device, operation_id, request_sha, token = _terminal_batch(tmp_path)
    q = queue_mod.FleetQueue(db_path)
    # Reopen a fresh active claim-shaped row while retaining exact target bytes.
    q.db.execute(
        "UPDATE batches SET state='running',lease_until=?,target_finalized_at=NULL,"
        "target_finalization_disposition=?,target_request_sha256=? WHERE batch_id=?",
        (OLD, queue_mod.FINALIZATION_RECONCILE, request_sha, "batch-finalization"),
    )
    q.db.execute(
        "UPDATE jobs SET state='running',batch_id='batch-finalization' "
        "WHERE job_id='job-finalization'"
    )
    q.db.commit()
    q.close()

    events: list[str] = []
    _recovery_fakes(monkeypatch, device, operation_id, request_sha, token, events)
    q = queue_mod.FleetQueue(db_path)
    try:
        assert dispatcher._recover_target_stale(
            _config(tmp_path), q, FUTURE, reporter=Reporter(json_events=False),
        ) == 1
        job = q.get("job-finalization")
        batch = q.get_batch("batch-finalization")
    finally:
        q.close()
    assert job is not None and job["state"] == "completion_unknown"
    assert batch is not None and batch["target_finalization_disposition"] == queue_mod.FINALIZATION_FENCED
    assert stage.exists()
    assert "stage_delete" not in events and "durable_delete" not in events

    # A partial predecessor identity is preserved and fenced without any target I/O.
    db_path2, stage2, _device2, _op2, _sha2, _token2 = _terminal_batch(tmp_path / "malformed")
    q = queue_mod.FleetQueue(db_path2)
    q.db.execute(
        "UPDATE batches SET state='running',lease_until=?,target_request_sha256=NULL,"
        "target_finalization_disposition=?,target_resume_token=NULL WHERE batch_id=?",
        (OLD, queue_mod.FINALIZATION_MALFORMED, "batch-finalization"),
    )
    q.db.execute(
        "UPDATE jobs SET state='running' WHERE job_id='job-finalization'"
    )
    q.db.commit()
    assert dispatcher._recover_target_stale(
        _config(tmp_path / "malformed"), q, FUTURE, reporter=Reporter(json_events=False),
    ) == 1
    assert q.get("job-finalization")["state"] == "completion_unknown"
    q.close()
    assert stage2.exists()
