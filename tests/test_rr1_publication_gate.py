from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun.config import RemrunConfig
from remrun.fleet import cli, dispatcher, executor, queue as queue_mod
from remrun.fleet.prepared import RAW_COMMAND_SPEC_ID, prepare_raw_command
from remrun.models import Device
from remrun.output import Reporter
from remrun.remote import runner as remote_runner

NOW = "2026-08-24T00:00:00Z"
FUTURE = "2099-01-01T00:00:00Z"
EXPIRED = "2000-01-01T00:00:00Z"
RECOVER = "2099-02-01T00:00:00Z"


def _device(root: Path) -> Device:
    return Device.from_mapping("TARGET", {
        "kind": "ssh-posix",
        "os": "posix",
        "host": "target.invalid",
        "project_root": "/target/projects",
        "state_root": "/target/state",
        "cache_root": "/target/cache",
    })


def _config(root: Path, device: Device) -> RemrunConfig:
    return RemrunConfig(
        repo_root=root,
        defaults={"fleet": {"pools": {}}},
        devices={device.name: device},
        project_roots={},
    )


def _prepared() -> dict:
    return prepare_raw_command(
        [sys.executable, "-c", "print('work')"], device="TARGET",
    )


def _active_target_job(q: queue_mod.FleetQueue) -> tuple[str, str, str, str]:
    job_id = "job-a"
    batch_id = "batch-a"
    operation_id = executor._target_operation_identity(batch_id)
    request_sha = hashlib.sha256(operation_id.encode()).hexdigest()
    token = "private-target-token"
    q.enqueue_prepared(_prepared(), spec=None, job_id=job_id, now=NOW)
    owner = q.claim_many(
        [job_id], "TARGET", batch_id=batch_id, lease_until=FUTURE,
        pool=None, task_name="command", engine="raw", bucket="", now=NOW,
        target_protocol_version=1,
        current_spec_ids={job_id: RAW_COMMAND_SPEC_ID},
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
    return job_id, batch_id, request_sha, token


def test_cancel_crash_after_remote_stop_cannot_replay_or_delete_target_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A remote CANCELLED side effect must have a durable local fence first."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    state_root = tmp_path / "controller-state"
    q = queue_mod.FleetQueue(state_root / "fleet" / "fleet.db")
    job_id, batch_id, request_sha, _token = _active_target_job(q)
    device = _device(tmp_path)
    config = _config(tmp_path, device)

    monkeypatch.setattr(
        cli, "_stop_target_operation",
        lambda _config, _operation: ("stopped", None, "CANCELLED"),
    )

    class SimulatedControllerLoss(BaseException):
        pass

    original_cancel_active_batch = q.cancel_active_batch
    crash_once = True

    def crash_before_local_commit(*args, **kwargs):  # noqa: ANN002, ANN003
        nonlocal crash_once
        if crash_once:
            crash_once = False
            raise SimulatedControllerLoss()
        return original_cancel_active_batch(*args, **kwargs)

    monkeypatch.setattr(q, "cancel_active_batch", crash_before_local_commit)
    with pytest.raises(SimulatedControllerLoss):
        cli._cancel_active_batch(config, q, batch_id, [job_id])

    # The target-side stop has already succeeded.  Let the local owner lease expire.
    q.db.execute(
        "UPDATE batches SET lease_until=? WHERE batch_id=?", (EXPIRED, batch_id),
    )
    q.db.execute(
        "UPDATE jobs SET leased_until=? WHERE job_id=?", (EXPIRED, job_id),
    )
    q.db.commit()

    removed_stages: list[str] = []
    removed_durable: list[tuple[str, str]] = []

    class Transport:
        def probe(self):  # noqa: ANN201
            return SimpleNamespace(reachable=True)

        def expand_remote(self, value: str) -> str:
            return value

        def native_join(self, *parts: str) -> str:
            return "/".join(part.strip("/") for part in parts)

        def remove_remote_tree(self, path: str) -> None:
            removed_stages.append(path)

        def durable_cleanup(self, operation_id: str, token: str) -> None:
            removed_durable.append((operation_id, token))

    class Client:
        def status_identity(self, operation_id: str, token: str, **_kwargs):  # noqa: ANN003, ANN201
            return {
                "status": "found",
                "receipt": {
                    "operation_id": operation_id,
                    "request_sha256": request_sha,
                    "state": "CANCELLED",
                    "command_start_state": "NO",
                    "fence": 1,
                },
            }

    monkeypatch.setattr(dispatcher, "make_transport", lambda _device: Transport())
    monkeypatch.setattr(
        dispatcher.TargetResourceClient, "connect", lambda *_args, **_kwargs: Client(),
    )
    recovered = dispatcher._recover_target_stale(
        config, q, RECOVER, reporter=Reporter(json_events=False),
    )

    job = q.get(job_id)
    batch = q.get_batch(batch_id)
    assert recovered == 1
    assert job is not None and job["state"] in {"cancelled", "completion_unknown"}
    assert job["state"] != "queued"
    assert batch is not None
    assert batch["target_finalization_disposition"] == queue_mod.FINALIZATION_FENCED
    assert batch["target_finalized_at"] is None
    assert removed_stages == []
    assert removed_durable == []
    q.close()


def test_posix_cancel_refuses_to_kill_group_when_authenticated_root_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reused PGID is not the allocation's exact process identity."""
    row = {
        "owner_kind": "posix_pgid_v1",
        "owner_key": "321",
        "root_pid": 321,
        "root_start_id": "linux:321:original",
    }
    states = iter(["live", "gone"])
    killed: list[int] = []
    monkeypatch.setattr(remote_runner, "_resource_owner_cleanup_state", lambda _row: next(states))
    monkeypatch.setattr(remote_runner, "_posix_group_members", lambda _pgid: [999])
    monkeypatch.setattr(remote_runner, "_kill_posix_group", killed.append)

    assert remote_runner._terminate_resource_owner(row) is False
    assert killed == []
