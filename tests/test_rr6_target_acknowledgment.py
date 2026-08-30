"""RR-6: target acceptance is a positive, versioned lifecycle event."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun.config import RemrunConfig
from remrun import _durable_runner as durable_runner
from remrun import _job_observer as observer
from remrun.fleet import dispatcher, executor, queue as queue_mod
from remrun.fleet.prepared import (
    RAW_COMMAND_SPEC,
    RAW_COMMAND_SPEC_ID,
    as_fleet_task,
    prepare_raw_command,
)
from remrun.output import Reporter
from remrun.target_resources import EMPTY_POLICY_DIGEST, TargetReservation
from remrun.transport import LocalSimTransport, SSHPosixTransport

from conftest import native_target_device


def _config(root: Path) -> RemrunConfig:
    device = native_target_device(root)
    return RemrunConfig(
        repo_root=root,
        defaults={"fleet": {"pools": {}}},
        devices={"TARGET": device},
        project_roots={},
    )


def _run_claimed_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    command_started: bool | None,
    acknowledge: bool,
) -> None:
    config = _config(tmp_path)
    state_root = tmp_path / "controller-state"
    db_path = state_root / "fleet" / "fleet.db"
    operation_id = "fleet-batch-rr6"
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
        prepared, spec=None, job_id="job-rr6", now="2026-08-25T00:00:00Z"
    )
    owner = q.claim_many(
        [job_id],
        "TARGET",
        batch_id="batch-rr6",
        lease_until="2099-01-01T00:00:00Z",
        pool=None,
        task_name="command",
        engine="raw",
        bucket="",
        now="2026-08-25T00:00:00Z",
        target_protocol_version=1,
        current_spec_ids={job_id: RAW_COMMAND_SPEC_ID},
    )
    q.close()
    assert isinstance(owner, str)

    def fake_run_batch(_device_name, _tasks, _config, **kwargs):  # noqa: ANN001, ANN003
        assert kwargs["on_target_reservation"](
            {"operation_id": operation_id, "request_sha256": request_sha}, token
        )
        assert kwargs["prelaunch_gate"]()
        if acknowledge:
            assert kwargs["on_target_acceptance"](
                {
                    "schema": 1,
                    "operation_id": operation_id,
                    "request_sha256": request_sha,
                    "accepted": True,
                    "command_started": command_started,
                    "state": "running",
                }
            )
        return {
            "ok": True,
            "exit_code": 0,
            "elapsed_s": 0.01,
            "staged": 1,
            "item_results": [],
            **(
                {
                    "target_operation": {
                        "schema": 1,
                        "operation_id": operation_id,
                        "request_sha256": request_sha,
                        "accepted": True,
                        "command_started": command_started,
                        "state": "complete",
                    }
                }
                if acknowledge
                else {}
            ),
        }

    monkeypatch.setattr(dispatcher.executor, "run_batch", fake_run_batch)
    monkeypatch.setattr(dispatcher, "_remote_output_mtimes", lambda *_a, **_k: {})
    reporter = Reporter(
        json_events=True,
        event_schema="remrun.fleet.lifecycle",
        event_version=1,
    )
    dispatcher._run_claimed_batch(
        config,
        state_root,
        {
            "batch_id": "batch-rr6",
            "device": "TARGET",
            "owner_token": owner,
            "btasks": [task],
            "engine": "raw",
            "job_ids": [job_id],
        },
        60,
        reporter,
    )


@pytest.mark.skipif(os.name != "posix", reason="POSIX live durable acceptance path")
def test_live_executor_ack_requires_started_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    prepared = prepare_raw_command(
        [sys.executable, "-c", "import time; time.sleep(0.5); print('live-rr6')"],
        device="TARGET",
    )
    task = as_fleet_task(
        prepared, {**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID}
    )
    target_root = tmp_path / "target-state"
    durable_root = target_root / "jobs" / "v1"
    resource_runner = tmp_path / "resource-runner.py"
    resource_runner.write_text(
        """from __future__ import annotations
import json
import sys
import time
from pathlib import Path

request = json.loads(sys.stdin.buffer.read().decode("utf-8"))
ledger_path = Path(sys.argv[2]) / "rr6-ledger.json"
receipt = json.loads(ledger_path.read_text(encoding="utf-8"))
operation = request["operation"]
if operation == "claim":
    receipt.update(state="CLAIMED", command_start_state="NO")
elif operation == "start":
    time.sleep(0.3)
    receipt["command_start_state"] = request["values"]["state"]
elif operation == "exec_confirm":
    receipt["command_start_state"] = "YES"
elif operation == "finish":
    receipt["state"] = "RELEASED"
elif operation == "quarantine":
    receipt["state"] = "QUARANTINED"
ledger_path.write_text(json.dumps(receipt), encoding="utf-8")
sys.stdout.write(json.dumps({"ok": True, "receipt": receipt}))
""",
        encoding="utf-8",
    )

    class Client:
        state_root = str(target_root)
        info = SimpleNamespace(installed_path=str(resource_runner))

        def reserve(self, **kwargs):  # noqa: ANN003, ANN201
            receipt = {
                "allocation_id": kwargs["allocation_id"],
                "operation_id": kwargs["operation_id"],
                "request_sha256": kwargs["request_sha256"],
                "fence": 1,
                "policy_generation": 0,
                "policy_digest": EMPTY_POLICY_DIGEST,
                "resource_keys": [],
                "state": "RESERVED",
                "command_start_state": "NO",
            }
            target_root.mkdir(parents=True, exist_ok=True)
            (target_root / "rr6-ledger.json").write_text(
                json.dumps(receipt), encoding="utf-8",
            )
            return TargetReservation(receipt, "private-target-token")

        def cancel(self, *_args, **_kwargs):  # noqa: ANN002, ANN003, ANN201
            return {"status": "cancelled"}

        def renew(self, *_args, **_kwargs):  # noqa: ANN002, ANN003, ANN201
            return {"receipt": {"state": "RESERVED"}}

    class LiveDurableTransport(LocalSimTransport):
        def _expand_remote(self, path: str) -> str:
            return path

        def _ensure_job_observer(self) -> tuple[str, str]:
            return str(durable_root), str(Path(observer.__file__).resolve())

        def _ensure_durable_runner(self) -> tuple[str, str]:
            return str(durable_root), str(Path(durable_runner.__file__).resolve())

        def _durable_control(self, operation: str, **kwargs):  # noqa: ANN003, ANN201
            root = Path(durable_root)
            if operation == "launch":
                return durable_runner._launch(root, kwargs["input_bytes"])
            if operation == "status":
                return durable_runner._status(
                    root, kwargs["run_id"], kwargs["resume_token"],
                    kwargs.get("include_logs", False),
                )
            if operation == "cleanup":
                return durable_runner._cleanup(
                    root, kwargs["run_id"], kwargs["resume_token"],
                )
            raise AssertionError(f"unexpected durable operation {operation}")

        def launch_durable(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
            return SSHPosixTransport.launch_durable(self, *args, **kwargs)

        def durable_status(
            self, run_id: str, resume_token: str, *, include_logs: bool = False,
        ) -> dict[str, object]:
            return self._durable_control(
                "status", run_id=run_id, resume_token=resume_token,
                include_logs=include_logs,
            )

        def durable_cleanup(self, run_id: str, resume_token: str) -> dict[str, object]:
            return self._durable_control(
                "cleanup", run_id=run_id, resume_token=resume_token,
            )

    transport = LiveDurableTransport(config.devices["TARGET"])
    accepted: list[dict[str, object]] = []
    monkeypatch.setattr(executor, "make_transport", lambda _device: transport)
    monkeypatch.setattr(
        executor.TargetResourceClient, "connect", lambda *_args, **_kwargs: Client(),
    )

    result = executor.run_batch(
        "TARGET",
        [task],
        config,
        state_root=tmp_path / "controller-state",
        observation_id="rr6-live-path",
        on_target_reservation=lambda _receipt, _token: True,
        on_target_acceptance=lambda receipt: accepted.append(dict(receipt)) or True,
        before_target_cleanup=lambda _receipt: True,
        on_target_finalization=lambda _receipt: True,
    )

    assert result["ok"] is True, result
    assert len(accepted) == 1
    assert accepted[0]["accepted"] is True
    assert accepted[0]["command_started"] is True


def test_positive_target_ack_is_emitted_after_dispatch_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _run_claimed_batch(
        tmp_path, monkeypatch, command_started=True, acknowledge=True
    )
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    ack = next(event for event in events if event["event"] == "dispatch_target_acknowledged")
    assert events.index(ack) > next(
        index for index, event in enumerate(events) if event["event"] == "dispatch_run"
    )
    assert ack == {
        "event": "dispatch_target_acknowledged",
        "schema": "remrun.fleet.lifecycle",
        "version": 1,
        "batch": "batch-rr6",
        "device": "TARGET",
        "operation_id": "fleet-batch-rr6",
        "request_sha256": "a" * 64,
        "accepted": True,
        "command_start_state": "YES",
    }


def test_sent_without_target_ack_emits_no_acknowledgment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _run_claimed_batch(
        tmp_path, monkeypatch, command_started=None, acknowledge=False
    )
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert not any(event["event"] == "dispatch_target_acknowledged" for event in events)


def test_ambiguous_target_start_is_emitted_as_maybe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _run_claimed_batch(
        tmp_path, monkeypatch, command_started=None, acknowledge=True
    )
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    ack = next(event for event in events if event["event"] == "dispatch_target_acknowledged")
    assert ack["command_start_state"] == "MAYBE"
    assert "started" not in ack
    assert ack["batch"] == "batch-rr6"
    assert ack["operation_id"] == "fleet-batch-rr6"
    assert ack["request_sha256"] == "a" * 64
