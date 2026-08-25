"""Publication-gate regression for cancellation versus execution-owner cleanup.

This test is expected to FAIL on remrun tree
abab3bc71c12c6b213746b8eee0d6f9507ef74d8 (claimed commit 355c1e5).
It must pass before publication.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun.config import RemrunConfig
from remrun.fleet import executor as fleet_executor
from remrun.fleet import queue as queue_mod
from remrun.fleet.prepared import (
    RAW_COMMAND_SPEC,
    RAW_COMMAND_SPEC_ID,
    as_fleet_task,
    prepare_raw_command,
)
from remrun.models import Device
from remrun.target_resources import EMPTY_POLICY_DIGEST, TargetReservation
from remrun.transport import LocalSimTransport

NOW = "2026-08-25T12:00:00Z"
FUTURE = "2099-01-01T00:00:00Z"


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


def _raw_task(root: Path):  # noqa: ANN202
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


def test_cancellation_fence_prevents_execution_owner_from_deleting_target_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If cancellation wins, the revoked execution owner must retain all evidence."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    device, config, task = _raw_task(tmp_path)
    q = queue_mod.FleetQueue(tmp_path / "controller-state" / "fleet" / "fleet.db")
    job_id, batch_id = "job-a", "batch-a"
    operation_id = fleet_executor._target_operation_identity(batch_id)

    q.enqueue_prepared(task.prepared, spec=None, job_id=job_id, now=NOW)
    owner = q.claim_many(
        [job_id],
        "TARGET",
        batch_id=batch_id,
        lease_until=FUTURE,
        pool=None,
        task_name="command",
        engine="raw",
        bucket="",
        now=NOW,
        target_protocol_version=1,
        current_spec_ids={job_id: RAW_COMMAND_SPEC_ID},
    )
    assert owner is not None
    assert q.set_batch_state(
        batch_id,
        "staging",
        expected_state="leased",
        owner_token=owner,
        now=NOW,
    )

    reservation = _reservation(operation_id)
    events: list[str] = []
    cancellation_committed = False

    class Client:
        state_root = str(tmp_path / "target-state")
        info = SimpleNamespace(installed_path=str(tmp_path / "runner.py"))

        def reserve(self, **_kwargs):  # noqa: ANN003, ANN202
            return reservation

        def renew(self, _reservation, **_kwargs):  # noqa: ANN001, ANN003, ANN202
            return {"receipt": {**reservation.receipt, "state": "RESERVED"}}

    class Transport(LocalSimTransport):
        def launch_durable(self, _command, _cwd, **_kwargs):  # noqa: ANN001, ANN003, ANN202
            return (
                {
                    "acknowledged": True,
                    "command_started": False,
                    "state": "pending",
                    "target_acceptance": {"state": "CLAIMED"},
                },
                {"platform": "POSIX", "telemetry": "none"},
            )

        def durable_status(  # noqa: ANN001, ANN202
            self, _run_id, _token, *, include_logs=False
        ):
            nonlocal cancellation_committed
            assert not include_logs
            if not cancellation_committed:
                assert q.begin_active_cancellation(batch_id, [job_id], now=NOW)
                events.append("cancellation_committed")
                cancellation_committed = True
            return {
                "state": "failed",
                "command_started": False,
                "error": "cancelled before command start",
                "target_cleanup": {"state": "CANCELLED"},
            }

        def remove_remote_tree(self, path):  # noqa: ANN001, ANN202
            events.append("stage_delete")
            return super().remove_remote_tree(path)

        def durable_cleanup(self, _run_id, _token):  # noqa: ANN001, ANN202
            events.append("durable_delete")
            return {"cleaned": True}

    monkeypatch.setattr(
        fleet_executor, "make_transport", lambda _device: Transport(device)
    )
    monkeypatch.setattr(
        fleet_executor.TargetResourceClient,
        "connect",
        lambda *_args, **_kwargs: Client(),
    )

    def reservation_cb(receipt, token):  # noqa: ANN001, ANN202
        return q.record_target_reservation(
            batch_id,
            operation_id=receipt["operation_id"],
            request_sha256=receipt["request_sha256"],
            resume_token=token,
            expected_state="staging",
            owner_token=owner,
            now=NOW,
        )

    def launch_gate() -> bool:
        return q.set_batch_state(
            batch_id,
            "running",
            expected_state="staging",
            owner_token=owner,
            now=NOW,
        )

    def acceptance_cb(receipt):  # noqa: ANN001, ANN202
        return q.record_target_acceptance(
            batch_id,
            operation_id=receipt["operation_id"],
            request_sha256=receipt["request_sha256"],
            expected_state="running",
            owner_token=owner,
            now=NOW,
        )

    def finalization_cb(receipt):  # noqa: ANN001, ANN202
        accepted = q.record_target_finalization(
            batch_id,
            operation_id=receipt["operation_id"],
            request_sha256=receipt["request_sha256"],
            cleanup_state=receipt["cleanup_state"],
            stage_cleaned=receipt["stage_cleaned"],
            durable_cleaned=receipt["durable_cleaned"],
            expected_state="running",
            owner_token=owner,
            now=NOW,
        )
        events.append(f"finalization_accepted={accepted}")
        return accepted

    result = fleet_executor.run_batch(
        "TARGET",
        [task],
        config,
        state_root=tmp_path / "controller-state",
        cleanup=True,
        job_ids=[job_id],
        observation_id=batch_id,
        prelaunch_gate=launch_gate,
        on_target_reservation=reservation_cb,
        on_target_acceptance=acceptance_cb,
        on_target_finalization=finalization_cb,
    )

    assert q.get_batch(batch_id)["state"] == "cancelling"
    assert result["target_finalization_deferred"] == (
        "controller queue rejected target finalization proof"
    )
    assert "finalization_accepted=False" in events

    # Safety contract: once the cancellation fence wins, the old owner may not
    # erase either class of target evidence before or after its rejected CAS.
    assert "stage_delete" not in events
    assert "durable_delete" not in events
