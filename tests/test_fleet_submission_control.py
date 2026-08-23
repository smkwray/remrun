from __future__ import annotations

import copy
import json
import os
import sqlite3
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun.config import RemrunConfig
from remrun.fleet import cli, dispatcher
from remrun.fleet.models import DeviceSnapshot, DrainResultV1, PlacedBatch, PlacementResult
from remrun.fleet.prepared import as_fleet_task, pin_prepared_job, prepare_task_job
from remrun.fleet.queue import FleetQueue, QueueMigrationError
from remrun.fleet.task_contract import resolve_task_spec
from remrun.output import Reporter


def _spec(tmp_path: Path, *, allow_root_override: bool = False) -> dict:
    worker = tmp_path / "worker.py"
    worker.write_text("raise SystemExit(0)\n", encoding="utf-8")
    devices = {
        "A": _device("A", tmp_path),
        "B": _device("B", tmp_path),
    }
    definition = {
        "input": {
            "mode": "files",
            "extensions": [".unit"],
            "split": "per-item",
            "file_identity": "sha256",
        },
        "prepare": {"mode": "none"},
        "routing": {"requirements": [], "requirements_by_option": {}},
        "execution": {"batching": "never", "replay": "at-most-once-v1"},
        "cost": {
            "measure": "item-count",
            "unit": "items",
            "divisor": 1,
            "bucket_options": [],
        },
        "output": {
            "reservation": "none",
            "allow_root_override": allow_root_override,
            "verification": "none",
        },
        "completion": {
            "protocol": "exit-code-v1",
            "evidence": "never",
            "companion": "forbidden",
            "allowed_publication": ["none"],
            "unstructured_memory": "ignore",
        },
        "options": {},
        "adapters": {
            name: {
                "engine": "generic",
                "argv": [sys.executable, str(worker)],
                "output_root": str(tmp_path / name / "out"),
                "pool": False,
                "memory_kind": "cpu",
                "capability_paths": [str(worker)],
                "provides": [],
            }
            for name in devices
        },
    }
    return resolve_task_spec(
        "arbitrary-unit", definition, devices=devices, repo_root=tmp_path,
    )


def _device(name: str, tmp_path: Path):
    from remrun.models import Device

    return Device.from_mapping(name, {
        "kind": "local-sim",
        "os": "windows" if os.name == "nt" else "posix",
        "address_candidates": ["localhost"],
        "project_root": str(tmp_path / name / "project"),
        "cache_root": str(tmp_path / name / "cache"),
        "state_root": str(tmp_path / name / "state"),
    })


def _prepared_pair(tmp_path: Path) -> tuple[dict, list[dict]]:
    spec = _spec(tmp_path)
    records = []
    for index in range(2):
        source = tmp_path / f"input-{index}.unit"
        source.write_text(f"input {index}", encoding="utf-8")
        records.append(prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)]))
    return spec, records


def test_request_identity_replays_one_atomic_submission(tmp_path: Path) -> None:
    spec, records = _prepared_pair(tmp_path)
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        first = queue.enqueue_submission(
            records,
            spec=spec,
            request_id="consumer-request-17",
            current_spec_id=lambda: spec["spec_id"],
        )
        replay = queue.enqueue_submission(
            records,
            spec=spec,
            request_id="consumer-request-17",
            current_spec_id=lambda: spec["spec_id"],
        )

        assert replay["submission_id"] == first["submission_id"]
        assert replay["job_ids"] == first["job_ids"]
        assert replay["replayed"] is True
        exact = queue.get_submission(request_id="consumer-request-17")
        assert exact == replay
        assert [row["job_id"] for row in queue.jobs_for_submission(
            first["submission_id"]
        )] == first["job_ids"]

        changed = copy.deepcopy(records)
        changed[0] = pin_prepared_job(changed[0], "A", spec)
        with pytest.raises(ValueError, match="different prepared submission"):
            queue.enqueue_submission(
                changed,
                spec=spec,
                request_id="consumer-request-17",
                current_spec_id=lambda: spec["spec_id"],
            )
    finally:
        queue.close()


def test_concurrent_request_retries_produce_one_submission(tmp_path: Path) -> None:
    spec, records = _prepared_pair(tmp_path)
    db_path = tmp_path / "fleet.db"
    FleetQueue(db_path).close()
    barrier = threading.Barrier(20)

    def submit() -> tuple[str, tuple[str, ...]]:
        queue = FleetQueue(db_path)
        try:
            barrier.wait(timeout=20)
            receipt = queue.enqueue_submission(
                records,
                spec=spec,
                request_id="same-controller-request",
                current_spec_id=lambda: spec["spec_id"],
            )
            return receipt["submission_id"], tuple(receipt["job_ids"])
        finally:
            queue.close()

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(lambda _index: submit(), range(20)))

    assert len(set(results)) == 1
    queue = FleetQueue(db_path)
    try:
        assert queue.db.execute("SELECT COUNT(*) FROM submissions").fetchone()[0] == 1
        assert queue.db.execute("SELECT COUNT(*) FROM submission_jobs").fetchone()[0] == 2
    finally:
        queue.close()


def test_saved_plan_consumes_exact_pinned_records_once(tmp_path: Path) -> None:
    spec, records = _prepared_pair(tmp_path)
    pinned = [
        pin_prepared_job(records[0], "A", spec),
        pin_prepared_job(records[1], "B", spec),
    ]
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        plan = queue.save_submission_plan(
            spec=spec, prepared_records=pinned,
            current_spec_id=lambda: spec["spec_id"],
        )
        first = queue.enqueue_saved_plan(
            plan["plan_id"], current_spec_id=lambda: spec["spec_id"],
        )
        replay = queue.enqueue_saved_plan(
            plan["plan_id"], current_spec_id=lambda: (_ for _ in ()).throw(
                AssertionError("an accepted plan replay must not consult mutable config")
            ),
        )

        assert replay["submission_id"] == first["submission_id"]
        assert replay["replayed"] is True
        rows = queue.jobs_for_submission(first["submission_id"])
        assert [json.loads(row["prepared_json"])["routing"]["force_device"] for row in rows] == [
            "A", "B",
        ]
        assert [row["prepared_id"] for row in rows] == [
            record["prepared_id"] for record in pinned
        ]
    finally:
        queue.close()


def test_saved_plan_freezes_priority_for_response_loss_replay(tmp_path: Path) -> None:
    spec, records = _prepared_pair(tmp_path)
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        plan = queue.save_submission_plan(
            spec=spec,
            prepared_records=[pin_prepared_job(records[0], "A", spec)],
            priority=7,
            current_spec_id=lambda: spec["spec_id"],
        )
        first = queue.enqueue_saved_plan(
            plan["plan_id"], current_spec_id=lambda: spec["spec_id"],
        )
        replay = queue.enqueue_saved_plan(
            plan["plan_id"], current_spec_id=lambda: None,
        )
        assert first["job_ids"] == replay["job_ids"]
        assert queue.get(first["job_ids"][0])["priority"] == 7
        with pytest.raises(ValueError, match="cannot change.*request identity"):
            queue.enqueue_saved_plan(
                plan["plan_id"], request_id="late-new-identity",
                current_spec_id=lambda: None,
            )
    finally:
        queue.close()


def test_submission_scope_selects_only_its_jobs(tmp_path: Path) -> None:
    spec, records = _prepared_pair(tmp_path)
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        one = queue.enqueue_submission(
            [records[0]], spec=spec, request_id="one",
            current_spec_id=lambda: spec["spec_id"],
        )
        two = queue.enqueue_submission(
            [records[1]], spec=spec, request_id="two",
            current_spec_id=lambda: spec["spec_id"],
        )

        scoped = queue.list("queued", job_ids=one["job_ids"])
        assert [row["job_id"] for row in scoped] == one["job_ids"]
        assert two["job_ids"][0] not in {row["job_id"] for row in scoped}
        assert queue.counts(job_ids=one["job_ids"]) == {"queued": 1}
    finally:
        queue.close()


def test_submission_membership_corruption_fails_closed(tmp_path: Path) -> None:
    spec, records = _prepared_pair(tmp_path)
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        receipt = queue.enqueue_submission(
            [records[0]], spec=spec,
            current_spec_id=lambda: spec["spec_id"],
        )
        queue.db.execute(
            "UPDATE submission_jobs SET prepared_id='sha256:' || printf('%064d', 0) "
            "WHERE submission_id=?",
            (receipt["submission_id"],),
        )
        with pytest.raises(QueueMigrationError, match="membership identity"):
            queue.jobs_for_submission(receipt["submission_id"])
    finally:
        queue.close()


def test_malformed_submission_schema_fails_at_queue_open(tmp_path: Path) -> None:
    db_path = tmp_path / "fleet.db"
    db = sqlite3.connect(db_path)
    try:
        db.execute("CREATE TABLE submissions (wrong TEXT)")
        db.commit()
    finally:
        db.close()
    with pytest.raises(QueueMigrationError, match="submissions schema"):
        FleetQueue(db_path)


def test_plan_rejects_unpinned_or_tampered_prepared_records(tmp_path: Path) -> None:
    spec, records = _prepared_pair(tmp_path)
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        with pytest.raises(ValueError, match="pinned device"):
            queue.save_submission_plan(
                spec=spec, prepared_records=records,
                current_spec_id=lambda: spec["spec_id"],
            )

        pinned = pin_prepared_job(records[0], "A", spec)
        pinned["routing"]["force_device"] = "B"
        with pytest.raises(ValueError, match="prepared_id"):
            queue.save_submission_plan(
                spec=spec, prepared_records=[pinned],
                current_spec_id=lambda: spec["spec_id"],
            )
        with pytest.raises(ValueError, match="definition changed"):
            queue.save_submission_plan(
                spec=spec,
                prepared_records=[pin_prepared_job(records[0], "A", spec)],
                current_spec_id=lambda: None,
            )
        override_spec = _spec(tmp_path, allow_root_override=True)
        override_records = [
            prepare_task_job(
                override_spec,
                repo_root=tmp_path,
                inputs=[str(tmp_path / f"input-{index}.unit")],
                output_root=str((tmp_path / "controller-output").resolve()),
            )
            for index in range(2)
        ]
        with pytest.raises(ValueError, match="multi-device.*output-root"):
            queue.save_submission_plan(
                spec=override_spec,
                prepared_records=[
                    pin_prepared_job(override_records[0], "A", override_spec),
                    pin_prepared_job(override_records[1], "B", override_spec),
                ],
                current_spec_id=lambda: override_spec["spec_id"],
            )
    finally:
        queue.close()


def test_cli_saved_plan_submission_and_exact_lookup(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    spec, records = _prepared_pair(tmp_path)
    tasks = [as_fleet_task(record, spec) for record in records]
    devices = {"A": _device("A", tmp_path), "B": _device("B", tmp_path)}
    config = SimpleNamespace(
        repo_root=tmp_path,
        devices=devices,
        defaults={"fleet": {"pools": {}}},
        fleet_tasks={},
    )
    state_root = tmp_path / "state"
    monkeypatch.setattr(cli, "default_state_root", lambda: state_root)
    monkeypatch.setattr(cli, "load_config", lambda _root=None: config)
    monkeypatch.setattr(cli, "resolve_tasks", lambda _config: {"arbitrary-unit": spec})
    monkeypatch.setattr(
        cli, "_prepare_configured", lambda _args, _config: (spec, records, tasks),
    )
    monkeypatch.setattr(
        cli.probes,
        "build_snapshot",
        lambda device, *_args, **_kwargs: DeviceSnapshot(
            name=device.name, reachable=True, max_jobs=1,
            engine_status={"generic": "present"},
        ),
    )
    monkeypatch.setattr(cli, "load_costs", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        cli.placement,
        "plan_jobs",
        lambda *_args, **_kwargs: PlacementResult(batches=[
            PlacedBatch(device="A", job_indices=[0], estimated_finish_s=1.0),
            PlacedBatch(device="B", job_indices=[1], estimated_finish_s=1.0),
        ], makespan_s=1.0),
    )

    plan_args = cli.build_parser().parse_args([
        "plan", "arbitrary-unit", "--save", "--json",
    ])
    assert cli.cmd_plan(plan_args, Reporter()) == 0
    planned = json.loads(capsys.readouterr().out)
    assert planned["plan_id"]
    assert planned["prepared_ids"] != [record["prepared_id"] for record in records]

    text_plan_args = cli.build_parser().parse_args([
        "plan", "arbitrary-unit", "--save", "--priority", "4",
    ])
    assert cli.cmd_plan(text_plan_args, Reporter()) == 0
    text_events = capsys.readouterr().err
    assert "plan_saved" in text_events
    assert "priority=4" in text_events
    inert_priority_args = cli.build_parser().parse_args([
        "plan", "arbitrary-unit", "--priority", "4",
    ])
    with pytest.raises(ValueError, match="requires --save"):
        cli.cmd_plan(inert_priority_args, Reporter())

    submit_args = cli.build_parser().parse_args([
        "submit", "--plan", planned["plan_id"],
        "--request-id", "ui-action-22", "--json",
    ])
    assert cli.cmd_submit(submit_args, Reporter()) == 0
    submitted = json.loads(capsys.readouterr().out)
    assert submitted["submission_id"]
    assert submitted["request_id"] == "ui-action-22"
    assert len(submitted["job_ids"]) == 2

    status_args = cli.build_parser().parse_args([
        "status", "--request-id", "ui-action-22", "--json",
    ])
    assert cli.cmd_status(status_args, Reporter()) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["submission"]["submission_id"] == submitted["submission_id"]
    assert [job["job_id"] for job in status["jobs"]] == submitted["job_ids"]

    changed_request = cli.build_parser().parse_args([
        "submit", "--plan", planned["plan_id"],
        "--request-id", "different-action", "--json",
    ])
    with pytest.raises(ValueError, match="cannot change.*request identity"):
        cli.cmd_submit(changed_request, Reporter())


def test_scoped_dispatch_passes_only_submission_members(
    tmp_path: Path, monkeypatch,
) -> None:
    spec, records = _prepared_pair(tmp_path)
    state_root = tmp_path / "state"
    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        wanted = queue.enqueue_submission(
            [records[0]], spec=spec, request_id="wanted",
            current_spec_id=lambda: spec["spec_id"],
        )
        queue.enqueue_submission(
            [records[1]], spec=spec, request_id="unrelated",
            current_spec_id=lambda: spec["spec_id"],
        )
    finally:
        queue.close()

    seen = []
    monkeypatch.setattr(cli, "default_state_root", lambda: state_root)
    monkeypatch.setattr(cli, "load_config", lambda: object())

    def fake_drain_once(*_args, job_ids, **_kwargs):  # noqa: ANN001
        seen.extend(job_ids)
        return {"recovered": 0, "placed": 0, "ran": 0, "ok": 0,
                "failed": 0, "review": 0, "skipped": {}, "cooled": []}

    monkeypatch.setattr(dispatcher, "drain_once", fake_drain_once)
    args = cli.build_parser().parse_args([
        "dispatch", "--once", "--submission", wanted["submission_id"],
    ])
    assert cli.cmd_dispatch(args, Reporter()) == 0
    assert seen == wanted["job_ids"]


def test_dispatcher_scope_executes_wanted_and_leaves_unrelated_queued(
    tmp_path: Path, monkeypatch,
) -> None:
    spec, records = _prepared_pair(tmp_path)
    config = RemrunConfig(
        repo_root=tmp_path,
        defaults={"fleet": {"pools": {}}},
        devices={"A": _device("A", tmp_path), "B": _device("B", tmp_path)},
        project_roots={},
        offload={},
        fleet_tasks={"arbitrary-unit": spec["definition"]},
    )
    state_root = tmp_path / "state"
    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        wanted = queue.enqueue_submission(
            [pin_prepared_job(records[0], "A", spec)], spec=spec,
            request_id="wanted", current_spec_id=lambda: spec["spec_id"],
        )
        unrelated = queue.enqueue_submission(
            [pin_prepared_job(records[1], "B", spec)], spec=spec,
            request_id="unrelated", current_spec_id=lambda: spec["spec_id"],
        )
    finally:
        queue.close()

    monkeypatch.setattr(dispatcher, "load_config", lambda _root=None: config)
    monkeypatch.setattr(
        dispatcher, "resolve_tasks", lambda _config: {"arbitrary-unit": spec},
    )
    result = dispatcher.drain_once(
        config, state_root=state_root, job_ids=wanted["job_ids"], max_parallel=1,
    )
    assert result["ran"] == 1

    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        assert queue.get(wanted["job_ids"][0])["state"] == "done"
        assert queue.get(unrelated["job_ids"][0])["state"] == "queued"
    finally:
        queue.close()


def test_dispatch_json_events_are_versioned_on_the_live_cli_path(
    monkeypatch, capsys,
) -> None:
    monkeypatch.setattr(cli, "load_config", lambda: object())

    def fake_run(*_args, reporter, **_kwargs):  # noqa: ANN001
        reporter.event("dispatch_run", batch="b1")
        return DrainResultV1(
            status="drained", ran=1, ok=1, failed=0, review=0,
            queued=0, active=0,
        )

    monkeypatch.setattr(dispatcher, "run", fake_run)
    assert cli.main(["dispatch", "--drain", "--json"]) == 0
    captured = capsys.readouterr()
    event = json.loads(captured.err)
    assert event == {
        "schema": "remrun.fleet.lifecycle",
        "version": 1,
        "event": "dispatch_run",
        "batch": "b1",
    }
    assert json.loads(captured.out)["status"] == "drained"


def test_lifecycle_schema_and_version_cannot_be_overridden(capsys) -> None:
    reporter = Reporter(
        json_events=True,
        event_schema="remrun.fleet.lifecycle",
        event_version=1,
    )
    reporter.event("dispatch_probe", schema="wrong", version=999)
    assert json.loads(capsys.readouterr().err) == {
        "schema": "remrun.fleet.lifecycle",
        "version": 1,
        "event": "dispatch_probe",
    }
