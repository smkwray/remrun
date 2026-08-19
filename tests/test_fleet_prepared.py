from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path

import pytest

from remrun.fleet import dispatcher, executor
from remrun.fleet.prepared import (
    PreparationError,
    as_fleet_task,
    prepare_raw_command,
    prepare_task_job,
    prepare_task_jobs,
    snapshot_prepared_input,
    validate_prepared_job,
)
from remrun.fleet.queue import FleetQueue, QueueMigrationError
from remrun.fleet.task_contract import resolve_task_spec


def _task(*, split: str = "per-item") -> dict:
    return {
        "input": {"mode": "files", "extensions": [".zot"], "split": split,
                  "file_identity": "sha256"},
        "prepare": {"mode": "none"},
        "routing": {"requirements": [], "requirements_by_option": {}},
        "execution": {"batching": "compatible"},
        "cost": {"measure": "input-bytes", "unit": "bytes", "divisor": 1,
                 "bucket_options": ["quality"]},
        "output": {"reservation": "content-work-stem-v1", "allow_root_override": False,
                   "verification": "mapped-tree-change-v1", "missing_mapping": "final",
                   "no_change": "final"},
        "completion": {"protocol": "item-result-v2", "evidence": "always",
                       "companion": "forbidden", "allowed_publication": ["produced"],
                       "unstructured_memory": "ignore"},
        "options": {"quality": {"type": "integer", "required": False, "default": 2,
                                "values": [1, 2, 3]}},
        "adapters": {"BOX": {"engine": "zot", "argv": ["zot", "{manifest}",
                                                                  "{output_root}"],
                              "output_root": "/out", "pool": "gpu",
                              "memory_kind": "gpu", "capability_paths": [],
                              "provides": ["zot.v1"]}},
    }


def _spec(tmp_path: Path, raw: dict | None = None) -> dict:
    return resolve_task_spec("zotomatic", raw or _task(), devices={"BOX"}, repo_root=tmp_path)


def test_prepare_freezes_payload_options_cost_routing_and_output(tmp_path: Path) -> None:
    source = tmp_path / "item.zot"
    source.write_bytes(b"abc")
    record = prepare_task_job(
        _spec(tmp_path), repo_root=tmp_path, inputs=[str(source)],
        caller_requirements=["zot.v1"], force_device="BOX",
    )

    validate_prepared_job(record)
    assert record["payload"]["items"][0]["identity"]["sha256"].startswith("sha256:")
    assert record["task"]["options"] == {"quality": 2}
    assert record["routing"]["requirements"] == ["zot.v1"]
    assert record["cost"] == {
        "status": "exact", "unit": "bytes", "value": 3.0,
        "relative_uncertainty": 0.0, "provenance": "input-bytes",
        "bucket_id": record["cost"]["bucket_id"],
    }
    assert record["output"]["reservations"][0]["stem"].startswith("item-0000-")


def test_direct_and_folder_files_obey_same_extension_policy(tmp_path: Path) -> None:
    allowed = tmp_path / "good.zot"
    denied = tmp_path / "bad.txt"
    allowed.write_text("ok", encoding="utf-8")
    denied.write_text("no", encoding="utf-8")
    spec = _spec(tmp_path)

    with pytest.raises(PreparationError, match="extension is not allowed"):
        prepare_task_job(spec, repo_root=tmp_path, inputs=[str(denied)])
    record = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(tmp_path)])
    assert [Path(row["source_path"]).name for row in record["payload"]["items"]] == ["good.zot"]


def test_per_item_split_prepares_one_frozen_job_per_file(tmp_path: Path) -> None:
    for name in ("a.zot", "b.zot"):
        (tmp_path / name).write_text(name, encoding="utf-8")
    records = prepare_task_jobs(
        _spec(tmp_path), repo_root=tmp_path,
        inputs=[str(tmp_path / "a.zot"), str(tmp_path / "b.zot")],
    )
    assert len(records) == 2
    assert records[0]["prepared_id"] != records[1]["prepared_id"]


def test_same_basenames_receive_globally_distinct_full_digest_reservations(
        tmp_path: Path) -> None:
    left = tmp_path / "left" / "same.zot"
    right = tmp_path / "right" / "same.zot"
    left.parent.mkdir()
    right.parent.mkdir()
    left.write_text("left", encoding="utf-8")
    right.write_text("right", encoding="utf-8")
    records = prepare_task_jobs(
        _spec(tmp_path), repo_root=tmp_path, inputs=[str(left), str(right)],
    )

    stems = [record["output"]["reservations"][0]["stem"] for record in records]
    assert len(set(stems)) == 2
    assert all(len(stem.rsplit("-", 1)[-1]) == 64 for stem in stems)


def test_per_item_split_expands_a_directory_before_deciding_job_count(tmp_path: Path) -> None:
    folder = tmp_path / "inputs"
    folder.mkdir()
    for name in ("a.zot", "b.zot"):
        (folder / name).write_text(name, encoding="utf-8")
    records = prepare_task_jobs(
        _spec(tmp_path), repo_root=tmp_path, inputs=[str(folder)],
    )
    assert len(records) == 2
    assert [Path(row["payload"]["items"][0]["source_path"]).name for row in records] == [
        "a.zot", "b.zot",
    ]


def test_raw_command_is_exact_unestimated_and_nonsemantic(tmp_path: Path) -> None:
    argv = ["tool", "", "a b", "*.txt", ">", "--flag", "☃"]
    first = prepare_raw_command(argv, device="BOX")
    second = prepare_raw_command(argv, device="BOX")
    validate_prepared_job(first)
    assert first["command"]["argv"] == argv
    assert first["task"] is None
    assert first["cost"]["status"] == "unestimated"
    assert first["prepared_id"] == second["prepared_id"]


def test_prepared_integrity_rejects_changed_bytes(tmp_path: Path) -> None:
    record = prepare_raw_command(["echo", "ok"], device="BOX")
    record["command"]["argv"][1] = "changed"
    with pytest.raises(PreparationError, match="prepared_id"):
        validate_prepared_job(record)


@pytest.mark.parametrize("identity_mode", ["sha256", "metadata"])
def test_frozen_input_snapshot_rejects_changed_source(tmp_path: Path,
                                                      identity_mode: str) -> None:
    source = tmp_path / "item.zot"
    source.write_bytes(b"original")
    raw = _task()
    raw["input"]["file_identity"] = identity_mode
    if identity_mode == "metadata":
        raw["output"]["reservation"] = "source-stem-v1"
    record = prepare_task_job(_spec(tmp_path, raw), repo_root=tmp_path, inputs=[str(source)])
    source.write_bytes(b"changed!")
    if identity_mode == "metadata":
        frozen = record["payload"]["items"][0]["identity"]["mtime_ns"]
        os.utime(source, ns=(frozen + 1, frozen + 1))

    with pytest.raises(PreparationError, match="source_changed"):
        snapshot_prepared_input(record["payload"]["items"][0])


def test_queue_stores_spec_once_and_dedupes_only_prepared_tasks(tmp_path: Path) -> None:
    source = tmp_path / "item.zot"
    source.write_text("x", encoding="utf-8")
    spec = _spec(tmp_path)
    task = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        def live_spec() -> str:
            return spec["spec_id"]

        first = queue.enqueue_prepared(task, spec=spec, current_spec_id=live_spec)
        second = queue.enqueue_prepared(task, spec=spec, current_spec_id=live_spec)
        command = prepare_raw_command(["echo", "same"], device="BOX")
        cmd1 = queue.enqueue_prepared(command, spec=None)
        cmd2 = queue.enqueue_prepared(command, spec=None)

        assert first == second
        assert cmd1 != cmd2
        assert queue.prepared_record(first) == task
        assert queue.prepared_spec(spec["spec_id"]) == spec
    finally:
        queue.close()


def test_queue_rejects_idempotency_collision_with_different_prepared_work(tmp_path: Path) -> None:
    first = prepare_raw_command(["echo", "one"], device="BOX")
    second = prepare_raw_command(["echo", "two"], device="BOX")
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        queue.enqueue_prepared(first, spec=None, idempotency_key="same")
        with pytest.raises(ValueError, match="collision"):
            queue.enqueue_prepared(second, spec=None, idempotency_key="same")
    finally:
        queue.close()


def test_atomic_enqueue_rejects_definition_drift_with_zero_rows(tmp_path: Path) -> None:
    source = tmp_path / "item.zot"
    source.write_text("x", encoding="utf-8")
    spec = _spec(tmp_path)
    record = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        with pytest.raises(ValueError, match="no job was enqueued"):
            queue.enqueue_prepared_many(
                [record], spec=spec, current_spec_id=lambda: "sha256:" + "0" * 64,
            )
        assert queue.db.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0
        assert queue.db.execute("SELECT count(*) FROM prepared_specs").fetchone()[0] == 0
        assert queue.db.execute(
            "SELECT count(*) FROM prepared_output_reservations").fetchone()[0] == 0
    finally:
        queue.close()


def test_configured_enqueue_requires_live_definition_authority(tmp_path: Path) -> None:
    source = tmp_path / "item.zot"
    source.write_text("x", encoding="utf-8")
    spec = _spec(tmp_path)
    record = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        with pytest.raises(ValueError, match="live definition authority callable"):
            queue.enqueue_prepared(record, spec=spec)
        with pytest.raises(ValueError, match="live definition authority callable"):
            queue.enqueue_prepared_many(
                [record], spec=spec, current_spec_id=spec["spec_id"],
            )

        def unreadable() -> str:
            raise OSError("config unavailable")

        with pytest.raises(OSError, match="config unavailable"):
            queue.enqueue_prepared(record, spec=spec, current_spec_id=unreadable)
        assert queue.db.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0
        assert queue.db.execute("SELECT count(*) FROM prepared_specs").fetchone()[0] == 0
    finally:
        queue.close()


def test_configured_claim_without_live_authority_fails_closed_before_lease(
        tmp_path: Path) -> None:
    source = tmp_path / "item.zot"
    source.write_text("x", encoding="utf-8")
    spec = _spec(tmp_path)
    record = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        jid = queue.enqueue_prepared(
            record, spec=spec, current_spec_id=lambda: spec["spec_id"],
        )
        assert queue.claim_many(
            [jid], "BOX", batch_id="no-authority",
            lease_until="2099-01-01T00:00:00Z", pool=None,
        ) is None
        row = queue.get(jid)
        assert row["state"] == "needs_review"
        assert row["last_error"] == "definition_authority_not_live"
        assert row["attempts"] == 0
        assert queue.get_batch("no-authority") is None
    finally:
        queue.close()


def test_configured_claim_with_unreadable_authority_fails_closed_before_lease(
        tmp_path: Path) -> None:
    source = tmp_path / "item.zot"
    source.write_text("x", encoding="utf-8")
    spec = _spec(tmp_path)
    record = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        jid = queue.enqueue_prepared(
            record, spec=spec, current_spec_id=lambda: spec["spec_id"],
        )

        def unreadable() -> dict[str, str | None]:
            raise OSError("config unavailable")

        assert queue.claim_many(
            [jid], "BOX", batch_id="unreadable-authority",
            lease_until="2099-01-01T00:00:00Z", pool=None,
            current_spec_ids=unreadable,
        ) is None
        row = queue.get(jid)
        assert row["state"] == "needs_review"
        assert row["last_error"] == "definition_authority_unreadable"
        assert row["attempts"] == 0
        assert queue.get_batch("unreadable-authority") is None
    finally:
        queue.close()


def test_output_reservation_collision_rolls_back_whole_multirow_enqueue(
        tmp_path: Path) -> None:
    paths = []
    for name in ("owner", "first", "second"):
        path = tmp_path / f"{name}.zot"
        path.write_text(name, encoding="utf-8")
        paths.append(path)
    spec = _spec(tmp_path)
    owner, first, second = [
        prepare_task_job(spec, repo_root=tmp_path, inputs=[str(path)]) for path in paths
    ]
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        def live_spec() -> str:
            return spec["spec_id"]

        queue.enqueue_prepared(owner, spec=spec, current_spec_id=live_spec)
        queue.db.execute(
            "INSERT INTO prepared_output_reservations(stem,work_id,created_at) VALUES(?,?,?)",
            (second["output"]["reservations"][0]["stem"], owner["work_id"], "t0"),
        )
        with pytest.raises(ValueError, match="reservation collision"):
            queue.enqueue_prepared_many(
                [first, second], spec=spec, current_spec_id=live_spec,
            )
        rows = queue.db.execute("SELECT prepared_id FROM jobs ORDER BY created_at").fetchall()
        assert [row["prepared_id"] for row in rows] == [owner["prepared_id"]]
        stems = queue.db.execute(
            "SELECT stem FROM prepared_output_reservations").fetchall()
        assert {row["stem"] for row in stems} == {
            owner["output"]["reservations"][0]["stem"],
            second["output"]["reservations"][0]["stem"],
        }
    finally:
        queue.close()


def test_corrupt_prepared_spec_fails_closed(tmp_path: Path) -> None:
    command = prepare_raw_command(["echo", "ok"], device="BOX")
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        jid = queue.enqueue_prepared(command, spec=None)
        queue.db.execute("UPDATE prepared_specs SET canonical_json='{}' WHERE spec_id=?",
                         (command["spec_id"],))
        with pytest.raises(QueueMigrationError, match="content identity"):
            queue.prepared_spec(command["spec_id"])
        assert queue.prepared_record(jid) == command
    finally:
        queue.close()


def test_atomic_claim_moves_corrupt_prepared_work_to_review(tmp_path: Path) -> None:
    command = prepare_raw_command(["echo", "ok"], device="BOX")
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        jid = queue.enqueue_prepared(command, spec=None)
        queue.db.execute("UPDATE prepared_specs SET canonical_json='{}' WHERE spec_id=?",
                         (command["spec_id"],))
        assert queue.claim_many(
            [jid], "BOX", batch_id="batch", lease_until="2099-01-01T00:00:00Z",
            pool=None, current_spec_ids={jid: command["spec_id"]},
        ) is None
        row = queue.get(jid)
        assert row["state"] == "needs_review"
        assert "prepared_integrity" in row["last_error"]
    finally:
        queue.close()


def test_immediate_prelaunch_gate_can_revoke_running_state_before_exec(tmp_path: Path) -> None:
    command = prepare_raw_command(["echo", "ok"], device="BOX")
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        jid = queue.enqueue_prepared(command, spec=None)
        owner = queue.claim_many(
            [jid], "BOX", batch_id="batch", lease_until="2099-01-01T00:00:00Z",
            pool=None, current_spec_ids={jid: command["spec_id"]},
        )
        assert owner is not None
        assert queue.set_batch_state(
            "batch", "running", expected_state="leased", owner_token=owner,
        )
        assert queue.revoke_prelaunch_batch(
            "batch", owner_token=owner, reason="definition_changed",
        )
        assert queue.get(jid)["state"] == "needs_review"
    finally:
        queue.close()


def test_prepared_batch_compatibility_pins_frozen_routing_policy(tmp_path: Path) -> None:
    source = tmp_path / "item.zot"
    source.write_bytes(b"abc")
    spec = _spec(tmp_path)
    base = dict(
        repo_root=tmp_path, inputs=[str(source)], caller_requirements=["zot.v1"],
    )
    forced = as_fleet_task(
        prepare_task_job(spec, force_device="BOX", allow_fallback=False, **base), spec,
    )
    automatic = as_fleet_task(
        prepare_task_job(spec, force_device=None, allow_fallback=False, **base), spec,
    )
    fallback = as_fleet_task(
        prepare_task_job(spec, force_device="BOX", allow_fallback=True, **base), spec,
    )

    assert dispatcher._compat_key(forced) != dispatcher._compat_key(automatic)
    assert dispatcher._compat_key(forced) != dispatcher._compat_key(fallback)
    assert executor._group_contract_error([forced, automatic]) is not None
    assert executor._group_contract_error([forced, fallback]) is not None


def test_nonbatchable_configured_jobs_never_share_a_dispatch_group(tmp_path: Path) -> None:
    first = tmp_path / "first.zot"
    second = tmp_path / "second.zot"
    first.write_bytes(b"a")
    second.write_bytes(b"b")
    raw = _task()
    raw["execution"]["batching"] = "never"
    raw["completion"] = {
        "protocol": "exit-code-v1", "evidence": "never", "companion": "forbidden",
        "allowed_publication": ["none"], "unstructured_memory": "ignore",
    }
    raw["output"] = {
        "reservation": "none", "allow_root_override": False, "verification": "none",
    }
    spec = _spec(tmp_path, raw)
    tasks = [as_fleet_task(
        prepare_task_job(spec, repo_root=tmp_path, inputs=[str(path)]), spec,
    ) for path in (first, second)]
    keys = []
    for index, task in enumerate(tasks):
        options = dict(task.options)
        options["_queue_job_id"] = f"job-{index}"
        keys.append(dispatcher._compat_key(replace(task, options=options)))
    assert keys[0] != keys[1]


def test_opted_in_fallback_updates_placement_without_rewriting_prepared_identity(
        tmp_path: Path) -> None:
    source = tmp_path / "item.zot"
    source.write_bytes(b"abc")
    raw = _task()
    raw["adapters"]["ALT"] = dict(raw["adapters"]["BOX"])
    raw["adapters"]["ALT"]["engine"] = "zot-alt"
    spec = resolve_task_spec(
        "zotomatic", raw, devices={"BOX", "ALT"}, repo_root=tmp_path,
    )
    record = prepare_task_job(
        spec, repo_root=tmp_path, inputs=[str(source)],
        force_device="BOX", allow_fallback=True,
    )
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        job_id = queue.enqueue_prepared(
            record, spec=spec, current_spec_id=lambda: spec["spec_id"],
        )
        owner = queue.claim_many(
            [job_id], "BOX", batch_id="first", lease_until="2099-01-01T00:00:00Z",
            pool=None, current_spec_ids=lambda: {job_id: spec["spec_id"]},
        )
        assert owner is not None
        assert queue.fail_batch(
            "first", "target refused", expected_state="leased", owner_token=owner,
            clear_force_device=True,
        )
        row = queue.get(job_id)
        task = dispatcher._row_to_task(row, queue)
        assert task.force_device is None
        assert task.prepared["routing"]["force_device"] == "BOX"
        assert task.prepared["prepared_id"] == record["prepared_id"]
    finally:
        queue.close()
