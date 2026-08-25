from __future__ import annotations

import hashlib
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

import pytest

from remrun.config import RemrunConfig
from remrun.fleet import cli, queue as queue_mod
from remrun.fleet.prepared import RAW_COMMAND_SPEC_ID, prepare_raw_command
from remrun.models import Device
from remrun.output import Reporter
from remrun.remote import runner as remote_runner
from remrun.target_resources import (
    EMPTY_POLICY_DIGEST,
    TargetResourceError,
)


NOW = "2026-08-24T00:00:00Z"
FUTURE = "2099-01-01T00:00:00Z"


def _device(root: Path, name: str = "TARGET", *, kind: str = "local-sim") -> Device:
    return Device.from_mapping(name, {
        "kind": kind,
        "os": "posix",
        "project_root": str(root / name / "projects"),
        "state_root": str(root / name / "state"),
        "cache_root": str(root / name / "cache"),
    })


def _config(root: Path, *devices: Device) -> RemrunConfig:
    return RemrunConfig(
        repo_root=root,
        defaults={"fleet": {"pools": {}}},
        devices={device.name: device for device in devices},
        project_roots={},
    )


def _state_root(tmp_path: Path) -> Path:
    return tmp_path / "controller-state"


def _queue(tmp_path: Path) -> queue_mod.FleetQueue:
    return queue_mod.FleetQueue(_state_root(tmp_path) / "fleet" / "fleet.db")


def _prepared(label: str = "work") -> dict:
    return prepare_raw_command(
        [sys.executable, "-c", f"print({label!r})"], device="TARGET",
    )


def _active_target_job(
    q: queue_mod.FleetQueue,
    job_id: str,
    *,
    device: str = "TARGET",
    batch_id: str | None = None,
    operation_id: str | None = None,
    request_sha: str | None = None,
    token: str = "private-target-token",
) -> tuple[str, str, str, str]:
    batch_id = batch_id or f"batch-{job_id}"
    operation_id = operation_id or f"operation-{job_id}"
    request_sha = request_sha or hashlib.sha256(operation_id.encode()).hexdigest()
    q.enqueue_prepared(_prepared(job_id), spec=None, job_id=job_id, now=NOW)
    owner = q.claim_many(
        [job_id], device, batch_id=batch_id, lease_until=FUTURE,
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
    return batch_id, operation_id, request_sha, owner


def _patch_cli_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, config: RemrunConfig,
) -> None:
    monkeypatch.setattr(cli, "default_state_root", lambda: _state_root(tmp_path))
    monkeypatch.setattr(cli, "load_config", lambda: config)


def _cancel(argv: list[str]) -> tuple[int, object]:
    args = cli.build_parser().parse_args(["cancel", *argv, "--json"])
    code = cli.cmd_cancel(args, Reporter(json_events=False))
    return code, args


def test_targeted_submission_cancel_stops_only_named_queue_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """A scoped stop is a durable queue fence; unrelated work remains claimable."""
    q = _queue(tmp_path)
    targeted = q.enqueue_submission(
        [_prepared("target-a"), _prepared("target-b")], spec=None,
        request_id="request-targeted", job_ids=["target-a", "target-b"], now=NOW,
    )
    q.enqueue_prepared(_prepared("unrelated"), spec=None, job_id="unrelated", now=NOW)
    q.close()
    _patch_cli_state(monkeypatch, tmp_path, _config(tmp_path))

    code, _args = _cancel(["--submission", targeted["submission_id"]])
    payload = json.loads(capsys.readouterr().out)

    assert code == cli.EXIT_OK
    assert payload == {
        "schema": 1,
        "jobs": [
            {"disposition": "stopped", "job_id": "target-a"},
            {"disposition": "stopped", "job_id": "target-b"},
        ],
    }
    q = _queue(tmp_path)
    try:
        assert q.get("target-a")["state"] == "cancelled"
        assert q.get("target-b")["state"] == "cancelled"
        assert q.get("unrelated")["state"] == "queued"
        assert q.claim_many(
            ["unrelated"], "TARGET", batch_id="unrelated-batch",
            lease_until=FUTURE, pool=None, task_name="command", engine="raw",
            bucket="", now=NOW,
            current_spec_ids={"unrelated": RAW_COMMAND_SPEC_ID},
        ) is not None
        assert q.claim_many(
            ["target-a"], "TARGET", batch_id="cancelled-batch",
            lease_until=FUTURE, pool=None, task_name="command", engine="raw",
            bucket="", now=NOW,
            current_spec_ids={"target-a": RAW_COMMAND_SPEC_ID},
        ) is None
    finally:
        q.close()


def test_cancel_preserves_records_results_and_allows_explicit_resubmission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation keeps the old row intact while releasing its idempotency key for retry."""
    evidence = tmp_path / "result.json"
    evidence.write_text('{"kept":true}\n', encoding="utf-8")
    prepared = _prepared("retryable")
    q = _queue(tmp_path)
    q.enqueue_prepared(
        prepared, spec=None, idempotency_key="retryable-work",
        job_id="first-attempt", now=NOW,
    )
    q.db.execute(
        "UPDATE jobs SET last_result=?,output_manifest=? WHERE job_id='first-attempt'",
        ('{"outcome":"partial"}', str(evidence)),
    )
    q.db.commit()
    q.close()
    _patch_cli_state(monkeypatch, tmp_path, _config(tmp_path))

    code, _args = _cancel(["--job", "first-attempt"])
    assert code == cli.EXIT_OK

    q = _queue(tmp_path)
    try:
        original = q.get("first-attempt")
        assert original is not None
        assert original["state"] == "cancelled"
        assert original["last_result"] == '{"outcome":"partial"}'
        assert original["output_manifest"] == str(evidence)
        retried = q.enqueue_prepared(
            prepared, spec=None, idempotency_key="retryable-work",
            job_id="second-attempt", now="2026-08-24T00:01:00Z",
        )
        assert retried == "second-attempt"
        assert q.get(retried)["state"] == "queued"
    finally:
        q.close()
    assert evidence.read_text(encoding="utf-8") == '{"kept":true}\n'


def test_per_job_dispositions_and_partial_failure_exit_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    q = _queue(tmp_path)
    q.enqueue_prepared(_prepared("queued"), spec=None, job_id="queued", now=NOW)
    q.enqueue_prepared(_prepared("finished"), spec=None, job_id="finished", now=NOW)
    q.db.execute("UPDATE jobs SET state='done' WHERE job_id='finished'")
    _active_target_job(q, "unstoppable")
    q.close()

    class Client:
        def status_identity(self, operation_id, token, **_kwargs):  # noqa: ANN001, ANN003
            assert (operation_id, token) == ("operation-unstoppable", "private-target-token")
            return {
                "status": "found",
                "receipt": {
                    "operation_id": operation_id,
                    "request_sha256": hashlib.sha256(operation_id.encode()).hexdigest(),
                    "state": "CLAIMED",
                    "command_start_state": "YES",
                    "fence": 1,
                },
            }

        def cancel(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
            raise TargetResourceError("process-tree termination could not be verified")

    config = _config(tmp_path, _device(tmp_path))
    _patch_cli_state(monkeypatch, tmp_path, config)
    monkeypatch.setattr(cli.TargetResourceClient, "connect", lambda *_a, **_k: Client())

    code, _args = _cancel([
        "--job", "queued", "--job", "unstoppable", "--job", "finished",
    ])
    payload = json.loads(capsys.readouterr().out)

    assert code == cli.EXIT_ERROR
    assert payload == {
        "schema": 1,
        "jobs": [
            {"disposition": "stopped", "job_id": "queued"},
            {
                "disposition": "could_not_stop",
                "error": "process-tree termination could not be verified",
                "job_id": "unstoppable",
            },
            {"disposition": "already_finished", "job_id": "finished"},
        ],
    }
    q = _queue(tmp_path)
    try:
        assert q.get("queued")["state"] == "cancelled"
        assert q.get("unstoppable")["state"] == "running"
        assert q.get("finished")["state"] == "done"
    finally:
        q.close()


def test_targeted_cancel_refuses_to_kill_a_batch_with_unrelated_active_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    q = _queue(tmp_path)
    job_ids = ["targeted-running", "unrelated-running"]
    for job_id in job_ids:
        q.enqueue_prepared(_prepared(job_id), spec=None, job_id=job_id, now=NOW)
    owner = q.claim_many(
        job_ids, "TARGET", batch_id="shared-batch", lease_until=FUTURE,
        pool=None, task_name="command", engine="raw", bucket="", now=NOW,
        target_protocol_version=1,
        current_spec_ids={job_id: RAW_COMMAND_SPEC_ID for job_id in job_ids},
    )
    assert owner is not None
    request_sha = hashlib.sha256(b"shared-operation").hexdigest()
    assert q.record_target_reservation(
        "shared-batch", operation_id="shared-operation", request_sha256=request_sha,
        resume_token="shared-token", expected_state="leased", owner_token=owner, now=NOW,
    )
    assert q.set_batch_state(
        "shared-batch", "staging", expected_state="leased", owner_token=owner, now=NOW,
    )
    assert q.set_batch_state(
        "shared-batch", "running", expected_state="staging", owner_token=owner, now=NOW,
    )
    q.close()
    _patch_cli_state(monkeypatch, tmp_path, _config(tmp_path, _device(tmp_path)))
    monkeypatch.setattr(
        cli.TargetResourceClient,
        "connect",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("a shared active batch must not be killed")
        ),
    )

    code, _args = _cancel(["--job", "targeted-running"])
    payload = json.loads(capsys.readouterr().out)

    assert code == cli.EXIT_ERROR
    assert payload["jobs"] == [{
        "disposition": "could_not_stop",
        "error": "active batch also contains untargeted jobs",
        "job_id": "targeted-running",
    }]
    q = _queue(tmp_path)
    try:
        assert q.get("targeted-running")["state"] == "running"
        assert q.get("unrelated-running")["state"] == "running"
    finally:
        q.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-tree proof")
def test_targeted_cancel_terminates_the_exact_active_process_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    config = _config(tmp_path, _device(tmp_path))
    operation_id = "operation-live-tree"
    request_sha = hashlib.sha256(operation_id.encode()).hexdigest()
    target_state = str(tmp_path / "target-runtime")
    pids = tmp_path / "tree.pids"
    monkeypatch.setattr(
        remote_runner, "filesystem_probe",
        lambda path: {"local": True, "kind": "test-local", "path": path},
    )
    monkeypatch.setattr(remote_runner, "_strict_boot_id", lambda: "test:boot-session")

    def exact_test_group_members(pgid: int) -> list[int]:
        candidates = [pgid]
        if pids.exists():
            candidates.extend(int(value) for value in pids.read_text().split())
        return sorted({pid for pid in candidates if _process_live(pid)})

    monkeypatch.setattr(remote_runner, "_posix_group_members", exact_test_group_members)

    def target_call(operation: str, body: dict) -> dict:
        conn, _root, _meta = remote_runner.open_participant_store(target_state)
        try:
            conn.execute("BEGIN IMMEDIATE")
            boot_id = remote_runner._strict_boot_id()
            mono_ns = remote_runner._strict_monotonic_ns()
            if operation == "reserve":
                response = remote_runner._resource_reserve(conn, body, boot_id, mono_ns)
            elif operation == "status":
                response = remote_runner._resource_status(conn, body, boot_id, mono_ns)
            else:
                response = remote_runner._resource_cancel(conn, body, boot_id, mono_ns)
            conn.execute("COMMIT")
            return response
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    reserved = target_call("reserve", {
        "allocation_id": operation_id,
        "operation_id": operation_id,
        "request_sha256": request_sha,
        "resource_keys": [],
        "expected_policy_generation": 0,
        "expected_policy_digest": EMPTY_POLICY_DIGEST,
    })
    token = reserved["token"]
    fence = reserved["receipt"]["fence"]
    owner_result: list[dict] = []
    owner_error: list[BaseException] = []
    claim: list[dict] = []
    pgid = None

    def own_tree() -> None:
        try:
            owner_result.append(remote_runner._run_posix_resource_owner(
                target_state,
                {
                    "reservation": {
                        "allocation_id": operation_id,
                        "fence": fence,
                        "token": token,
                        "policy_generation": 0,
                        "policy_digest": EMPTY_POLICY_DIGEST,
                    },
                    "argv": [
                        sys.executable,
                        "-c",
                        "import os,subprocess,sys,time; from pathlib import Path; "
                        "print('log-before-cancel',flush=True); "
                        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
                        "Path(sys.argv[1]).write_text(f'{os.getpid()} {child.pid}'); time.sleep(60)",
                        str(pids),
                    ],
                    "cwd": None,
                    "env": {},
                },
                claim_callback=claim.append,
            ))
        except BaseException as exc:
            owner_error.append(exc)

    owner_thread = threading.Thread(target=own_tree)
    try:
        q = _queue(tmp_path)
        _batch, _operation, _sha, owner = _active_target_job(
            q, "live-tree", operation_id=operation_id,
            request_sha=request_sha, token=token,
        )
        owner_thread.start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and (not claim or not pids.exists()):
            time.sleep(0.02)
        assert claim
        assert pids.exists()
        pgid = int(claim[0]["owner"]["key"])
        assert q.record_target_acceptance(
            "batch-live-tree", operation_id=operation_id,
            request_sha256=request_sha, expected_state="running",
            owner_token=owner, now=NOW,
        )
        q.close()
        _patch_cli_state(monkeypatch, tmp_path, config)

        class Client:
            def status_identity(self, allocation_id, supplied_token, **_kwargs):  # noqa: ANN001, ANN003
                assert (allocation_id, supplied_token) == (operation_id, token)
                return target_call("status", {
                    "allocation_id": allocation_id,
                    "token": supplied_token,
                })

            def cancel(self, reservation, **_kwargs):  # noqa: ANN001, ANN003
                return target_call("cancel", {
                    "allocation_id": reservation.allocation_id,
                    "fence": reservation.fence,
                    "token": reservation.token,
                })

        monkeypatch.setattr(cli.TargetResourceClient, "connect", lambda *_a, **_k: Client())

        code, _args = _cancel(["--job", "live-tree"])
        payload = json.loads(capsys.readouterr().out)

        assert code == cli.EXIT_OK, payload
        assert payload["jobs"] == [
            {"disposition": "stopped", "job_id": "live-tree"}
        ]
        parent_pid, child_pid = (int(value) for value in pids.read_text().split())
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and (
            _process_live(parent_pid) or _process_live(child_pid)
        ):
            time.sleep(0.02)
        assert not _process_live(parent_pid)
        assert not _process_live(child_pid)
        owner_thread.join(timeout=5)
        assert not owner_thread.is_alive()
        assert not owner_error
        assert owner_result and owner_result[0]["receipt"]["state"] == "CANCELLED"
        assert owner_result[0]["stdout_b64"]
        q = _queue(tmp_path)
        try:
            assert q.get("live-tree")["state"] == "cancelled"
            batch = dict(q.db.execute(
                "SELECT target_resume_token,target_accepted_at,target_finalized_at,"
                "target_finalization_disposition FROM batches "
                "WHERE batch_id='batch-live-tree'"
            ).fetchone())
            assert batch["target_resume_token"] == token
            assert batch["target_accepted_at"] is not None
            assert batch["target_finalized_at"] is None
            assert batch["target_finalization_disposition"] == queue_mod.FINALIZATION_FENCED
        finally:
            q.close()
        assert pids.exists()
    finally:
        if pgid is not None:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if owner_thread.is_alive():
            owner_thread.join(timeout=10)


def _process_live(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    if sys.platform.startswith("linux"):
        try:
            state = Path(f"/proc/{pid}/stat").read_text(encoding="ascii").split(") ", 1)[1][0]
        except (OSError, IndexError):
            return False
        return state != "Z"
    return True


def test_fenced_completion_unknown_is_not_replayed_or_cleaned_by_cancel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    evidence = tmp_path / "target-evidence.bin"
    evidence.write_bytes(b"must survive")
    q = _queue(tmp_path)
    batch_id, _operation, _sha, owner = _active_target_job(q, "fenced")
    assert q.mark_completion_unknown(
        batch_id, "completion is unknown", expected_state="running",
        owner_token=owner, now=NOW, result_record=str(evidence),
        finalization_disposition=queue_mod.FINALIZATION_FENCED,
    )
    before_batch = dict(q.db.execute(
        "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
    ).fetchone())
    before_job = q.get("fenced")
    q.close()
    _patch_cli_state(monkeypatch, tmp_path, _config(tmp_path, _device(tmp_path)))

    class Client:
        def status_identity(self, operation_id, token, **_kwargs):  # noqa: ANN001, ANN003
            assert (operation_id, token) == ("operation-fenced", "private-target-token")
            return {
                "status": "found",
                "receipt": {
                    "operation_id": operation_id,
                    "request_sha256": hashlib.sha256(operation_id.encode()).hexdigest(),
                    "state": "CLAIMED",
                    "command_start_state": "YES",
                    "fence": 7,
                },
            }

        def cancel(self, reservation, **_kwargs):  # noqa: ANN001, ANN003
            return {
                "status": "cancelled",
                "receipt": {
                    **reservation.receipt,
                    "state": "CANCELLED",
                    "terminal_reason": "controller_cancelled",
                },
            }

    monkeypatch.setattr(cli.TargetResourceClient, "connect", lambda *_a, **_k: Client())

    code, _args = _cancel(["--job", "fenced"])
    payload = json.loads(capsys.readouterr().out)

    assert code == cli.EXIT_OK
    assert payload["jobs"] == [
        {"disposition": "stopped", "job_id": "fenced"}
    ]
    q = _queue(tmp_path)
    try:
        assert q.get("fenced") == before_job
        after_batch = dict(q.db.execute(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone())
        for field in (
            "state", "target_operation_id", "target_request_sha256", "target_resume_token",
            "target_reserved_at", "target_accepted_at", "target_finalized_at",
            "target_finalization_disposition", "error",
        ):
            assert after_batch[field] == before_batch[field]
        assert after_batch["target_cleanup_state"] == "CANCELLED"
        assert after_batch["target_cleanup_at"] is not None
        assert q.list(state="queued", job_ids=["fenced"]) == []
    finally:
        q.close()
    assert evidence.read_bytes() == b"must survive"


def test_global_cancel_sweep_remains_available_and_reports_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    q = _queue(tmp_path)
    q.enqueue_prepared(_prepared("global"), spec=None, job_id="global", now=NOW)
    q.close()
    first = _device(tmp_path, "FIRST", kind="ssh-posix")
    second = _device(tmp_path, "SECOND", kind="ssh-posix")
    _patch_cli_state(monkeypatch, tmp_path, _config(tmp_path, first, second))
    monkeypatch.setattr(cli.platform, "system", lambda: "Other")
    called: list[str] = []

    class Transport:
        def __init__(self, device):  # noqa: ANN001
            self.device = device

        def kill_workers(self) -> bool:
            called.append(self.device.name)
            return self.device.name == "FIRST"

    monkeypatch.setattr(cli, "make_transport", lambda device: Transport(device))

    code, _args = _cancel([])
    payload = json.loads(capsys.readouterr().out)

    assert called == ["FIRST", "SECOND"]
    assert payload["stopped_workers"] == {"FIRST": True, "SECOND": False}
    assert code == cli.EXIT_ERROR
    q = _queue(tmp_path)
    try:
        assert q.get("global") is None
    finally:
        q.close()
