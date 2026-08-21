from __future__ import annotations

from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sqlite3
import sys
import threading
from types import SimpleNamespace

import pytest

from remrun.config import RemrunConfig
from remrun.fleet import cli, dispatcher, executor, placement, profiles
from remrun.fleet.models import DeviceSnapshot, DrainResultV1, PlacedBatch, PlacementResult
from remrun.fleet.prepared import as_fleet_task, prepare_raw_command, prepare_task_job, prepared_features
from remrun.fleet.prepared import validate_prepared_against_spec
from remrun.fleet.profiles import prepared_profile_key
from remrun.fleet.queue import FleetQueue
from remrun.fleet.task_contract import resolve_task_spec, sha256_id
from remrun.output import Reporter
from remrun.models import Device


_NOW = "2026-08-21T12:00:00Z"
_BEFORE = "2026-08-21T11:00:00Z"
_EXPIRED = "2026-08-21T11:59:59Z"


def _definition() -> dict:
    adapter = {
        "engine": "engine-v1",
        "argv": ["/worker", "{manifest}"],
        "output_root": None,
        "pool": "gpu",
        "memory_kind": "cpu",
        "capability_paths": ["/worker"],
        "provides": ["worker.v1"],
    }
    return {
        "input": {"mode": "text", "split": "never"},
        "prepare": {"mode": "none"},
        "routing": {"requirements": ["worker.v1"], "requirements_by_option": {}},
        "execution": {"batching": "never", "replay": "at-most-once-v1"},
        "cost": {
            "measure": "none",
            "bucket_options": [],
        },
        "output": {
            "reservation": "none",
            "allow_root_override": False,
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
        "adapters": {"A": deepcopy(adapter), "B": deepcopy(adapter)},
    }


def _task(tmp_path):  # noqa: ANN001
    spec = resolve_task_spec(
        "novel-work", _definition(), devices={"A", "B"}, repo_root=tmp_path,
    )
    record = prepare_task_job(spec, repo_root=tmp_path, text="hello")
    return as_fleet_task(record, spec)


def _exact_definition() -> dict:
    definition = _definition()
    definition["input"] = {
        "mode": "files", "extensions": [".zot"], "split": "never",
        "file_identity": "sha256",
    }
    definition["cost"] = {
        "measure": "input-bytes", "unit": "synthetic-work-v1", "divisor": 1,
        "bucket_options": [],
    }
    return definition


def _exact_task(tmp_path, spec, size: int, label: str, **kwargs):  # noqa: ANN001
    path = tmp_path / f"{label}.zot"
    path.write_bytes(b"x" * size)
    record = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(path)], **kwargs)
    return as_fleet_task(record, spec)


def _profile_row(*, count: int, slope: float | None = None,
                 low: float = 1.0, high: float = 8.0) -> dict:
    return {
        "duration_n": count,
        "model_ready": slope is not None,
        "fixed_load_s": 2.0 if slope is not None else None,
        "var_per_unit_s": slope,
        "min_units": low,
        "max_units": high,
        "peak_rss_mb": 100.0,
        "resource_n": count,
    }


def _dispatch_fixture(tmp_path: Path) -> tuple[RemrunConfig, dict, Path, Path]:
    ledger = tmp_path / "invocations.jsonl"
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import json,os,pathlib\n"
        "m=json.loads(pathlib.Path(os.environ['REMRUN_BATCH_MANIFEST']).read_text())\n"
        f"ledger=pathlib.Path({str(ledger)!r})\n"
        "with ledger.open('a',encoding='utf-8') as out:\n"
        "  [out.write(json.dumps({'prepared_id':i['prepared_id'],'device':m['device']})+'\\n') "
        "for i in m['items']]\n"
        "items=[]\n"
        "for i in m['items']:\n"
        " c=i['cost']; items.append({'job_id':i['job_id'],'prepared_id':i['prepared_id'],"
        "'index':i['index'],'outcome':'succeeded','disposition':'none','retry_after_s':None,"
        "'publication':'none','work_performed':True,'outputs':[],'companion':None,"
        "'message':None,'failure_code':None,'resource':'none',"
        "'work_units':{'unit':c['unit'],'value':c['value'],'measure_id':c['measure_id']},"
        "'elapsed_s':0.01,'details':{}})\n"
        "r={'schema':2,'batch_id':m['batch_id'],'spec_id':m['spec_id'],"
        "'adapter_id':m['adapter_id'],'items':items}\n"
        "pathlib.Path(os.environ['REMRUN_BATCH_METRICS']).write_text(json.dumps(r))\n",
        encoding="utf-8",
    )
    adapter = {
        "engine": "engine-v1", "argv": [sys.executable, str(worker), "{manifest}"],
        "pool": False, "memory_kind": "cpu", "capability_paths": [str(worker)],
        "provides": ["worker.v1"],
    }
    definition = _exact_definition()
    definition["execution"] = {"batching": "never", "replay": "at-most-once-v1"}
    definition["output"] = {
        "reservation": "none", "allow_root_override": False, "verification": "none",
    }
    definition["completion"] = {
        "protocol": "item-result-v2", "evidence": "always", "companion": "forbidden",
        "allowed_publication": ["none"], "unstructured_memory": "ignore",
    }
    definition["adapters"] = {"A": dict(adapter), "B": dict(adapter)}
    devices = {
        name: Device.from_mapping(name, {
            "kind": "local-sim", "os": "windows" if os.name == "nt" else "posix",
            "address_candidates": ["localhost"],
            "project_root": str(tmp_path / f"remote-{name}"),
            "cache_root": str(tmp_path / f"cache-{name}"),
            "state_root": str(tmp_path / f"device-state-{name}"), "max_jobs": 1,
        })
        for name in ("A", "B")
    }
    config = RemrunConfig(
        repo_root=tmp_path, defaults={"fleet": {"pools": {}}}, devices=devices,
        project_roots={}, offload={}, fleet_tasks={"novel-work": definition},
    )
    spec = resolve_task_spec(
        "novel-work", definition, devices=devices, repo_root=tmp_path,
    )
    return config, spec, ledger, worker


def _fixture_capability_snapshot(device, *_args, **kwargs) -> DeviceSnapshot:  # noqa: ANN001
    """Keep queue/dispatch courts independent of platform-specific probe syntax."""
    return DeviceSnapshot(
        name=device.name,
        reachable=True,
        active_jobs=int(kwargs.get("active_jobs", 0)),
        max_jobs=int(getattr(device, "max_jobs", 1)),
        engine_status={"engine-v1": "present"},
    )


def _snap(name: str, *, active_jobs: int = 0) -> DeviceSnapshot:
    return DeviceSnapshot(
        name=name,
        reachable=True,
        active_jobs=active_jobs,
        max_jobs=2,
        pool_free={"gpu": 1},
        engine_status={"engine-v1": "present"},
        ram_free_mb=32000.0,
    )


def _cfg() -> dict:
    return {
        "transfer_mbps": 200.0,
        "ssh_setup_s": 0.0,
        "per_file_overhead_s": 0.0,
        "pools": {"gpu": 1},
    }


def test_two_qualified_unestimated_devices_choose_one_cold_start(tmp_path) -> None:
    task = _task(tmp_path)
    result = placement.plan_jobs(
        [task],
        [prepared_features(task.prepared)],
        {"B": _snap("B"), "A": _snap("A")},
        {},
        _cfg(),
    )

    assert result.skipped == {}
    assert result.makespan_s is None
    assert len(result.batches) == 1
    batch = result.batches[0]
    assert batch.device == "A"
    assert batch.job_indices == [0]
    assert batch.reason == "batched"
    assert batch.selection_basis == "cold_start"
    assert batch.estimated_finish_s is None
    assert batch.estimate_reason == "uncalibrated"


def test_cold_start_prefers_fewer_observations_then_active_jobs_then_name(tmp_path) -> None:
    task = _task(tmp_path)
    profiles = {
        prepared_profile_key(task, "A"): {"duration_n": 2},
        prepared_profile_key(task, "B"): {"duration_n": 1},
    }
    result = placement.plan_jobs(
        [task],
        [prepared_features(task.prepared)],
        {"A": _snap("A"), "B": _snap("B", active_jobs=1)},
        profiles,
        _cfg(),
    )
    assert result.batches[0].device == "B"


def test_profile_readiness_requires_variation_span_and_fit(tmp_path) -> None:
    queue = FleetQueue(tmp_path / "fleet.db")
    key = "sha256:" + "1" * 64
    try:
        def add(batch: str, units: float, elapsed: float) -> None:
            queue.db.execute(
                "INSERT INTO fleet_profile_observations ("
                "batch_id,profile_key,family_id,device,adapter_id,prepared_units,"
                "observed_units,controller_elapsed_s,worker_elapsed_s,peak_rss_mb,"
                "peak_vram_mb,accepted_duration,reject_reason,result_digest,recorded_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (batch, key, "family", "A", "adapter", units, units, elapsed, elapsed,
                 100.0, None, 1, None, "sha256:" + "2" * 64,
                 f"2026-08-21T12:00:{int(batch):02d}Z"),
            )

        for index, (units, elapsed) in enumerate([(1, 3), (2, 4), (4, 6)], 1):
            add(str(index), units, elapsed)
        row = profiles.load_observation_profiles(tmp_path / "fleet.db")[key]
        assert row["duration_n"] == 3 and row["model_ready"] is False

        add("4", 8, 10)
        row = profiles.load_observation_profiles(tmp_path / "fleet.db")[key]
        assert row["model_ready"] is True
        assert row["fixed_load_s"] == pytest.approx(2.0)
        assert row["var_per_unit_s"] == pytest.approx(1.0)

        queue.db.execute("DELETE FROM fleet_profile_observations")
        for index, elapsed in enumerate((3.0, 3.1, 2.9, 3.0), 1):
            add(str(index), 1.0, elapsed)
        same = profiles.load_observation_profiles(tmp_path / "fleet.db")[key]
        assert same["duration_n"] == 4 and same["model_ready"] is False

        queue.db.execute("DELETE FROM fleet_profile_observations")
        for index, pair in enumerate(((1, 1), (2, 20), (4, 2), (8, 40)), 1):
            add(str(index), *pair)
        noisy = profiles.load_observation_profiles(tmp_path / "fleet.db")[key]
        assert noisy["model_ready"] is False
        assert noisy["model_reason"] == "model_unfit"
    finally:
        queue.close()


def test_profile_store_corruption_is_not_silently_treated_as_no_evidence(tmp_path) -> None:
    path = tmp_path / "fleet.db"
    path.write_bytes(b"not a sqlite database")
    with pytest.raises(sqlite3.DatabaseError):
        profiles.load_observation_profiles(path)

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    (cache_root / profiles.FLEET_PROFILE_FILE).write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="must contain an object"):
        profiles.load_profiles(cache_root)


def test_success_without_performed_work_is_not_duration_evidence(tmp_path) -> None:
    definition = _exact_definition()
    spec = resolve_task_spec(
        "novel-work", definition, devices={"A", "B"}, repo_root=tmp_path,
    )
    task = _exact_task(tmp_path, spec, 4, "no-work")
    observation = dispatcher._profile_observation(  # noqa: SLF001
        [task], "A", {
            "ok": True,
            "elapsed_s": 1.0,
            "item_results": [{
                "outcome": "succeeded",
                "work_performed": False,
                "work_units": {
                    "unit": task.prepared["cost"]["unit"],
                    "value": task.prepared["cost"]["value"],
                    "measure_id": task.prepared["cost"]["measure_id"],
                },
            }],
        }, None,
    )
    assert observation["accepted_duration"] is False
    assert observation["reject_reason"] == "partial_or_no_work"


def test_frozen_legacy_prepared_job_remains_readable_and_defaults_to_at_most_once(
    tmp_path,
) -> None:
    spec = resolve_task_spec(
        "novel-work", _exact_definition(), devices={"A", "B"}, repo_root=tmp_path,
    )
    task = _exact_task(tmp_path, spec, 4, "legacy")
    legacy_spec = deepcopy(spec)
    legacy_spec["definition"]["execution"].pop("replay")
    legacy_spec["definition"]["cost"].pop("verify_relative_tolerance")
    legacy_spec["spec_id"] = sha256_id({
        key: value for key, value in legacy_spec.items() if key != "spec_id"
    })
    legacy = deepcopy(task.prepared)
    legacy["schema"] = 1
    legacy["spec_id"] = legacy_spec["spec_id"]
    legacy["cost"] = {
        "status": "exact", "unit": "synthetic-work-v1", "value": 4.0,
        "relative_uncertainty": 0.0, "provenance": "input-bytes",
        "bucket_id": legacy["cost"]["bucket_id"],
    }
    semantic = {
        "spec_id": legacy["spec_id"], "payload": legacy["payload"],
        "options": legacy["task"]["options"],
        "requirements": legacy["routing"]["requirements"],
        "output_root": legacy["output"]["root_override"],
    }
    legacy["work_id"] = sha256_id(semantic)
    legacy["prepared_id"] = sha256_id({
        key: value for key, value in legacy.items() if key != "prepared_id"
    })
    validate_prepared_against_spec(legacy, legacy_spec)

    queue = FleetQueue(tmp_path / "legacy.db")
    try:
        job_id = queue.enqueue_prepared(
            legacy, spec=legacy_spec,
            current_spec_id=lambda: legacy_spec["spec_id"],
        )
        owner = queue.claim_many(
            [job_id], "A", batch_id="legacy-batch", lease_until="2099-01-01T00:00:00Z",
            pool=None, current_spec_ids=lambda: {job_id: legacy_spec["spec_id"]},
        )
        assert owner is not None
        assert queue.batch_replay_policy("legacy-batch") == "at-most-once-v1"
    finally:
        queue.close()


def test_eight_cold_starts_calibrate_both_devices_then_prefer_faster(tmp_path) -> None:
    spec = resolve_task_spec(
        "novel-work", _exact_definition(), devices={"A", "B"}, repo_root=tmp_path,
    )
    learned: dict[str, dict] = {}
    assignments: list[tuple[str, int]] = []
    counts = {"A": 0, "B": 0}
    for index, size in enumerate((1, 1, 2, 2, 4, 4, 8, 8), 1):
        task = _exact_task(tmp_path, spec, size, f"cal-{index}")
        for device in ("A", "B"):
            learned[prepared_profile_key(task, device)] = _profile_row(count=counts[device])
        result = placement.plan_jobs(
            [task], [prepared_features(task.prepared)],
            {"A": _snap("A"), "B": _snap("B")}, learned, _cfg(),
        )
        chosen = result.batches[0].device
        assignments.append((chosen, size))
        counts[chosen] += 1
    assert assignments == [
        ("A", 1), ("B", 1), ("A", 2), ("B", 2),
        ("A", 4), ("B", 4), ("A", 8), ("B", 8),
    ]

    ninth = _exact_task(tmp_path, spec, 4, "ninth")
    learned = {
        prepared_profile_key(ninth, "A"): _profile_row(count=4, slope=1.0),
        prepared_profile_key(ninth, "B"): _profile_row(count=4, slope=2.0),
    }
    result = placement.plan_jobs(
        [ninth], [prepared_features(ninth.prepared)],
        {"A": _snap("A"), "B": _snap("B")}, learned, _cfg(),
    )
    assert result.batches[0].device == "A"
    assert result.batches[0].selection_basis == "estimated"


def test_mixed_state_exploration_is_prompt_bounded_slotted_and_parked(tmp_path) -> None:
    spec = resolve_task_spec(
        "novel-work", _exact_definition(), devices={"A", "B"}, repo_root=tmp_path,
    )

    def plan(task, b_count: int, *, b_ready: bool = False, b_range=(1.0, 8.0)):  # noqa: ANN001
        learned = {
            prepared_profile_key(task, "A"): _profile_row(count=8, slope=1.0),
            prepared_profile_key(task, "B"): _profile_row(
                count=b_count, slope=(2.0 if b_ready else None),
                low=b_range[0], high=b_range[1],
            ),
        }
        return placement.plan_jobs(
            [task], [prepared_features(task.prepared)],
            {"A": _snap("A"), "B": _snap("B")}, learned, _cfg(),
        ).batches[0]

    prompt = _exact_task(tmp_path, spec, 4, "prompt")
    for count in range(4):
        assert plan(prompt, count).device == "B"
        assert plan(prompt, count).selection_basis == "exploration"

    slot_zero = None
    nonzero = None
    for index in range(100):
        candidate = _exact_task(tmp_path, spec, 4, f"slot-{index}")
        if placement._exploration_slot(candidate) == 0:  # noqa: SLF001
            slot_zero = slot_zero or candidate
        else:
            nonzero = nonzero or candidate
        if slot_zero is not None and nonzero is not None:
            break
    assert slot_zero is not None and nonzero is not None
    assert plan(slot_zero, 4).device == "B"
    assert plan(slot_zero, 4).selection_basis == "exploration"
    assert plan(nonzero, 4).device == "A"
    assert plan(nonzero, 4).selection_basis == "estimated"
    assert plan(slot_zero, 8).device == "A"  # invalid profile is parked
    assert plan(slot_zero, 8, b_ready=True, b_range=(1.0, 2.0)).device == "B"
    assert plan(slot_zero, 8, b_ready=True, b_range=(1.0, 2.0)).estimate_reason == "out_of_range"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"estimated_finish_s": 1.0, "estimate_reason": "uncalibrated"},
        {"estimated_finish_s": None, "estimate_reason": None},
        {"estimated_finish_s": None, "estimate_reason": "mystery"},
        {"estimated_finish_s": 1.0, "selection_basis": "mystery"},
    ],
)
def test_placed_batch_rejects_incoherent_estimate_metadata(kwargs) -> None:  # noqa: ANN001
    values = {
        "device": "A",
        "job_indices": [0],
        "estimated_finish_s": 1.0,
        "reason": "batched",
        "selection_basis": "estimated",
        "estimate_reason": None,
    }
    values.update(kwargs)
    with pytest.raises(ValueError):
        PlacedBatch(**values)


@pytest.mark.parametrize("value", [-1.0, float("inf"), float("nan")])
def test_estimate_fields_reject_invalid_numeric_values(value: float) -> None:
    with pytest.raises(ValueError):
        PlacedBatch(
            device="A", job_indices=[0], estimated_finish_s=value,
            selection_basis="estimated",
        )
    with pytest.raises(ValueError):
        PlacementResult(makespan_s=value)


@pytest.mark.parametrize("value", [-1, 1.5, True])
def test_drain_counters_are_nonnegative_integers(value) -> None:  # noqa: ANN001
    with pytest.raises(ValueError):
        DrainResultV1(
            status="drained", ran=value, ok=0, failed=0, review=0,
            queued=0, active=0,
        )


@pytest.mark.parametrize(
    ("status", "failed", "review", "exit_code"),
    [
        ("drained", 0, 0, 0),
        ("drained", 1, 0, 1),
        ("drained", 0, 1, 1),
        ("stuck_unplaceable", 0, 0, 2),
        ("infrastructure_error", 0, 0, 4),
        ("cancelled", 0, 0, 130),
    ],
)
def test_drain_result_has_stable_status_and_exit_code(
    status: str, failed: int, review: int, exit_code: int,
) -> None:
    result = DrainResultV1(
        status=status,
        ran=failed + review,
        ok=0,
        failed=failed,
        review=review,
        queued=0,
        active=0,
        skipped={},
        error=(
            {"kind": "interrupted", "message": "stopped"}
            if status == "cancelled"
            else {"kind": "transport", "message": "offline"}
            if status == "infrastructure_error"
            else None
        ),
    )
    assert result.to_dict()["schema"] == 1
    assert result.exit_code == exit_code


@pytest.mark.parametrize(
    ("status", "failed", "review", "exit_code"),
    [
        ("drained", 0, 0, 0),
        ("drained", 1, 0, 1),
        ("stuck_unplaceable", 0, 0, 2),
        ("infrastructure_error", 0, 0, 4),
        ("cancelled", 0, 0, 130),
    ],
)
def test_dispatch_drain_json_is_one_stdout_document_with_exact_exit(
    monkeypatch, capsys, status: str, failed: int, review: int, exit_code: int,
) -> None:
    error = ({"kind": "test", "message": "stop"}
             if status in {"infrastructure_error", "cancelled"} else None)
    result = DrainResultV1(
        status=status, ran=failed + review, ok=0, failed=failed, review=review,
        queued=2 if status == "stuck_unplaceable" else 0, active=0,
        skipped={"A": "unreachable"} if status == "stuck_unplaceable" else {},
        error=error,
    )
    monkeypatch.setattr(cli, "load_config", lambda: object())

    def fake_run(*_args, reporter, **_kwargs):  # noqa: ANN001
        reporter.event("dispatch_probe", device="A")
        return result

    monkeypatch.setattr(dispatcher, "run", fake_run)
    args = SimpleNamespace(once=False, json=True, debounce=0.0, poll=0.0, drain=True)
    assert cli.cmd_dispatch(args, Reporter(json_events=True)) == exit_code
    captured = capsys.readouterr()
    assert captured.out.endswith("\n") and captured.out.count("\n") == 1
    assert json.loads(captured.out) == result.to_dict()
    assert json.loads(captured.err)["event"] == "dispatch_probe"


@pytest.mark.parametrize(
    ("raised", "status", "exit_code"),
    [(RuntimeError("broken queue"), "infrastructure_error", 4),
     (KeyboardInterrupt(), "cancelled", 130)],
)
def test_dispatch_result_survives_failure_to_read_final_queue_counts(
    tmp_path: Path, monkeypatch, raised: BaseException, status: str, exit_code: int,
) -> None:
    def fail_tick(*_args, **_kwargs):  # noqa: ANN001
        raise raised

    class BrokenQueue:
        def __init__(self, *_args, **_kwargs) -> None:
            raise RuntimeError("queue unreadable")

    monkeypatch.setattr(dispatcher, "drain_once", fail_tick)
    monkeypatch.setattr(dispatcher, "FleetQueue", BrokenQueue)
    result = dispatcher.run(
        SimpleNamespace(defaults={}), state_root=tmp_path, until_empty=True, max_ticks=1,
        reporter=SimpleNamespace(event=lambda *_args, **_kwargs: None),
    )
    assert result.status == status
    assert result.queued == result.active == 0
    assert result.exit_code == exit_code


def test_plan_json_and_human_output_surface_null_eta_reason(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    task = _task(tmp_path)
    spec = task.resolved_spec
    prepared = task.prepared
    config = SimpleNamespace(devices={"A": object()})
    planned = PlacementResult(
        batches=[PlacedBatch(
            device="A", job_indices=[0], estimated_finish_s=None,
            reason="cold-start calibration", selection_basis="cold_start",
            estimate_reason="uncalibrated",
        )],
        makespan_s=None,
    )
    monkeypatch.setattr(cli, "load_config", lambda: config)
    monkeypatch.setattr(cli, "default_state_root", lambda: tmp_path / "state")
    monkeypatch.setattr(cli, "_prepare_configured", lambda _args, _config: (
        spec, [prepared], [task],
    ))
    monkeypatch.setattr(cli, "fleet_config", lambda _config: {})
    monkeypatch.setattr(cli, "safety_fraction", lambda _config: 0.9)
    monkeypatch.setattr(cli, "load_costs", lambda *_args: {})
    monkeypatch.setattr(
        cli.probes, "build_snapshot",
        lambda *_args, **_kwargs: DeviceSnapshot(name="A", reachable=True),
    )
    monkeypatch.setattr(cli.placement, "plan_jobs", lambda *_args, **_kwargs: planned)

    args = SimpleNamespace(json=True)
    assert cli.cmd_plan(args, Reporter()) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["makespan_s"] is None
    assert payload["batches"][0]["estimated_finish_s"] is None
    assert payload["batches"][0]["selection_basis"] == "cold_start"
    assert payload["batches"][0]["estimate_reason"] == "uncalibrated"

    events = []
    args.json = False
    assert cli.cmd_plan(args, SimpleNamespace(
        event=lambda name, **fields: events.append((name, fields)),
    )) == 0
    placement_event = next(fields for name, fields in events if name == "placement")
    assert placement_event["estimated_finish_s"] is None
    assert placement_event["message"] == "Uncalibrated placement; no duration estimate."


def test_two_dispatchers_cold_start_one_job_only_once_with_one_observation(
    tmp_path: Path, monkeypatch,
) -> None:
    config, spec, ledger, _worker = _dispatch_fixture(tmp_path)
    source = tmp_path / "one.zot"
    source.write_bytes(b"one")
    prepared = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
    state_root = tmp_path / "state"
    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        job_id = queue.enqueue_prepared(
            prepared, spec=spec, current_spec_id=lambda: spec["spec_id"],
        )
    finally:
        queue.close()
    monkeypatch.setattr(dispatcher, "load_config", lambda _root=None: config)
    monkeypatch.setattr(dispatcher.probes, "build_snapshot", _fixture_capability_snapshot)
    barrier = threading.Barrier(2)

    def one_tick() -> dict:
        barrier.wait()
        return dispatcher.drain_once(
            config, state_root=state_root, debounce_s=0.0, max_parallel=1,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _index: one_tick(), range(2)))
    assert sum(result["ran"] for result in results) == 1
    assert len(ledger.read_text(encoding="utf-8").splitlines()) == 1
    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        row = queue.get(job_id)
        assert row["state"] == "done" and row["attempts"] == 1
        assert len(queue.profile_observations()) == 1
        assert queue.profile_observations()[0]["accepted_duration"] == 1
    finally:
        queue.close()


def test_nonbatchable_jobs_each_execute_once_without_benchmark_duplication(
    tmp_path: Path, monkeypatch,
) -> None:
    config, spec, ledger, _worker = _dispatch_fixture(tmp_path)
    state_root = tmp_path / "state"
    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    prepared_ids: list[str] = []
    try:
        for index, size in enumerate((1, 2, 4, 8)):
            source = tmp_path / f"work-{index}.zot"
            source.write_bytes(b"x" * size)
            prepared = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
            prepared_ids.append(prepared["prepared_id"])
            queue.enqueue_prepared(
                prepared, spec=spec, current_spec_id=lambda: spec["spec_id"],
            )
    finally:
        queue.close()
    monkeypatch.setattr(dispatcher, "load_config", lambda _root=None: config)
    monkeypatch.setattr(dispatcher.probes, "build_snapshot", _fixture_capability_snapshot)
    for _tick in range(4):
        summary = dispatcher.drain_once(
            config, state_root=state_root, debounce_s=0.0, max_parallel=2,
        )
        if summary["ran"] == 0:
            break
    invocations = [json.loads(line)["prepared_id"]
                   for line in ledger.read_text(encoding="utf-8").splitlines()]
    assert sorted(invocations) == sorted(prepared_ids)
    assert len(invocations) == len(set(invocations)) == 4


def test_idempotent_replay_requires_and_uses_target_atomic_work_id_dedup(
    tmp_path: Path, monkeypatch,
) -> None:
    config, _old_spec, invocation_log, worker = _dispatch_fixture(tmp_path)
    definition = config.fleet_tasks["novel-work"]
    definition["execution"]["replay"] = "idempotent-v1"
    effect_log = tmp_path / "effects.jsonl"
    ledger_dir = tmp_path / "target-ledger"
    worker.write_text(
        "import json,os,pathlib\n"
        "m=json.loads(pathlib.Path(os.environ['REMRUN_BATCH_MANIFEST']).read_text())\n"
        f"invocations=pathlib.Path({str(invocation_log)!r})\n"
        f"effects=pathlib.Path({str(effect_log)!r})\n"
        f"ledger=pathlib.Path({str(ledger_dir)!r}); ledger.mkdir(exist_ok=True)\n"
        "items=[]\n"
        "for i in m['items']:\n"
        " with invocations.open('a',encoding='utf-8') as out: out.write(i['work_id']+'\\n')\n"
        " receipt=ledger/i['work_id'].split(':',1)[1]\n"
        " try:\n"
        "  fd=os.open(receipt,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600); os.close(fd)\n"
        "  with effects.open('a',encoding='utf-8') as out: out.write(i['work_id']+'\\n')\n"
        " except FileExistsError: pass\n"
        " c=i['cost']; items.append({'job_id':i['job_id'],'prepared_id':i['prepared_id'],"
        "'index':i['index'],'outcome':'succeeded','disposition':'none','retry_after_s':None,"
        "'publication':'none','work_performed':True,'outputs':[],'companion':None,"
        "'message':None,'failure_code':None,'resource':'none',"
        "'work_units':{'unit':c['unit'],'value':c['value'],'measure_id':c['measure_id']},"
        "'elapsed_s':0.01,'details':{}})\n"
        "r={'schema':2,'batch_id':m['batch_id'],'spec_id':m['spec_id'],"
        "'adapter_id':m['adapter_id'],'items':items}\n"
        "pathlib.Path(os.environ['REMRUN_BATCH_METRICS']).write_text(json.dumps(r))\n",
        encoding="utf-8",
    )
    spec = resolve_task_spec(
        "novel-work", definition, devices=config.devices, repo_root=tmp_path,
    )
    source = tmp_path / "dedup.zot"
    source.write_bytes(b"work")
    prepared = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
    task = as_fleet_task(prepared, spec)
    state_root = tmp_path / "state"
    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        job_id = queue.enqueue_prepared(
            prepared, spec=spec, current_spec_id=lambda: spec["spec_id"],
        )
        owner = queue.claim_many(
            [job_id], "A", batch_id="lost", lease_until=_EXPIRED,
            estimated_finish_s=None, now=_BEFORE,
            current_spec_ids=lambda: {job_id: spec["spec_id"]},
        )
        assert owner is not None
        assert queue.set_batch_state(
            "lost", "staging", expected_state="leased", owner_token=owner,
            now="2026-08-21T11:30:00Z",
        )
        assert queue.set_batch_state(
            "lost", "running", expected_state="staging", owner_token=owner,
            now="2026-08-21T11:31:00Z",
        )
    finally:
        queue.close()

    first = executor.run_batch(
        "A", [task], config, state_root=state_root, job_ids=[job_id],
        observation_id="lost", prelaunch_gate=lambda: True,
    )
    assert first["ok"] is True, first
    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        assert queue.recover_stale(now=_NOW) == 1
        assert queue.get(job_id)["state"] == "queued"
    finally:
        queue.close()

    monkeypatch.setattr(dispatcher, "load_config", lambda _root=None: config)
    monkeypatch.setattr(dispatcher.probes, "build_snapshot", _fixture_capability_snapshot)
    second = dispatcher.drain_once(
        config, state_root=state_root, debounce_s=0.0, max_parallel=1,
    )
    assert second["ok"] == 1
    assert len(invocation_log.read_text(encoding="utf-8").splitlines()) == 2
    assert effect_log.read_text(encoding="utf-8").splitlines() == [prepared["work_id"]]


def _enqueue_command(queue: FleetQueue, key: str) -> str:
    return queue.enqueue_prepared(
        prepare_raw_command(["python", "-c", "pass", key], device="DEVICE"),
        spec=None,
        idempotency_key=key,
        now=_BEFORE,
    )


def _claim(queue: FleetQueue, key: str) -> tuple[str, str]:
    job_id = _enqueue_command(queue, key)
    owner = queue.claim_many(
        [job_id],
        "DEVICE",
        batch_id=key,
        lease_until=_EXPIRED,
        pool=None,
        estimated_finish_s=None,
        now=_BEFORE,
    )
    assert owner is not None
    return job_id, owner


def test_unknown_active_batch_is_not_reported_as_zero_backlog(tmp_path) -> None:
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        _claim(queue, "unknown-backlog")
        assert queue.active_backlog(now="2026-08-21T11:30:00Z") == {"DEVICE": None}
    finally:
        queue.close()


def test_stale_staging_requeues_but_stale_running_becomes_completion_unknown(tmp_path) -> None:
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        staging_job, staging_owner = _claim(queue, "staging-job")
        assert queue.set_batch_state(
            "staging-job",
            "staging",
            expected_state="leased",
            owner_token=staging_owner,
            now="2026-08-21T11:30:00Z",
        )

        running_job, running_owner = _claim(queue, "running-job")
        assert queue.set_batch_state(
            "running-job",
            "staging",
            expected_state="leased",
            owner_token=running_owner,
            now="2026-08-21T11:30:00Z",
        )
        assert queue.set_batch_state(
            "running-job",
            "running",
            expected_state="staging",
            owner_token=running_owner,
            now="2026-08-21T11:31:00Z",
        )

        assert queue.recover_stale(now=_NOW) == 2
        assert queue.get(staging_job)["state"] == "queued"
        assert queue.get(running_job)["state"] == "completion_unknown"
        assert _enqueue_command(queue, "running-job") == running_job
        assert queue.get(running_job)["attempts"] == 1
    finally:
        queue.close()


def test_not_null_eta_migration_preserves_batch_and_resource_lease(tmp_path) -> None:
    path = tmp_path / "fleet.db"
    db = sqlite3.connect(path)
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("""
            CREATE TABLE batches (
                batch_id TEXT PRIMARY KEY, owner_token TEXT, state TEXT NOT NULL,
                device TEXT NOT NULL, task_name TEXT, engine TEXT, bucket TEXT,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                lease_until TEXT NOT NULL, heartbeat_at TEXT,
                estimated_finish_s REAL NOT NULL DEFAULT 0, error TEXT
            )
        """)
        db.execute("""
            CREATE TABLE resource_leases (
                device TEXT NOT NULL, pool TEXT NOT NULL, batch_id TEXT NOT NULL,
                lease_until TEXT NOT NULL, PRIMARY KEY (device,pool)
            )
        """)
        db.execute(
            "INSERT INTO batches VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("old", "owner", "running", "DEVICE", "task", "engine", "bucket",
             _BEFORE, _BEFORE, "2099-01-01T00:00:00Z", _BEFORE, 0.0, None),
        )
        db.execute(
            "INSERT INTO resource_leases VALUES (?,?,?,?)",
            ("DEVICE", "gpu", "old", "2099-01-01T00:00:00Z"),
        )
        db.commit()
    finally:
        db.close()

    queue = FleetQueue(path)
    try:
        eta = next(
            row for row in queue.db.execute("PRAGMA table_info(batches)")
            if row["name"] == "estimated_finish_s"
        )
        assert eta["notnull"] == 0
        assert queue.get_batch("old")["estimated_finish_s"] == 0.0
        assert queue.lease_usage(now=_NOW) == {"DEVICE": {"gpu": 1}}
    finally:
        queue.close()


def test_terminal_completion_and_observation_are_one_atomic_idempotent_transition(tmp_path) -> None:
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        job_id, owner = _claim(queue, "observed")
        assert queue.set_batch_state(
            "observed",
            "staging",
            expected_state="leased",
            owner_token=owner,
            now="2026-08-21T11:30:00Z",
        )
        assert queue.set_batch_state(
            "observed",
            "running",
            expected_state="staging",
            owner_token=owner,
            now="2026-08-21T11:31:00Z",
        )
        observation = {
            "profile_key": None,
            "family_id": None,
            "device": "DEVICE",
            "adapter_id": None,
            "prepared_units": None,
            "observed_units": None,
            "controller_elapsed_s": 2.5,
            "worker_elapsed_s": None,
            "peak_rss_mb": 12.0,
            "peak_vram_mb": None,
            "accepted_duration": False,
            "reject_reason": "unestimated",
            "result_digest": "sha256:" + "a" * 64,
        }
        assert queue.complete_batch(
            "observed",
            expected_state="running",
            owner_token=owner,
            now="2026-08-21T11:32:00Z",
            observation=observation,
        )
        assert not queue.complete_batch(
            "observed",
            expected_state="running",
            owner_token=owner,
            now="2026-08-21T11:32:01Z",
            observation=observation,
        )
        rows = queue.profile_observations(batch_id="observed")
        assert len(rows) == 1
        assert rows[0]["accepted_duration"] == 0
        assert queue.get(job_id)["state"] == "done"
    finally:
        queue.close()


def test_conflicting_preexisting_observation_rolls_back_terminal_transition(tmp_path) -> None:
    queue = FleetQueue(tmp_path / "fleet.db")
    try:
        job_id, owner = _claim(queue, "conflict")
        assert queue.set_batch_state(
            "conflict", "staging", expected_state="leased", owner_token=owner,
            now="2026-08-21T11:30:00Z",
        )
        assert queue.set_batch_state(
            "conflict", "running", expected_state="staging", owner_token=owner,
            now="2026-08-21T11:31:00Z",
        )
        queue.db.execute(
            "INSERT INTO fleet_profile_observations "
            "(batch_id,device,accepted_duration,reject_reason,recorded_at) VALUES (?,?,?,?,?)",
            ("conflict", "DEVICE", 0, "preexisting", _BEFORE),
        )
        observation = {
            "profile_key": None, "family_id": None, "device": "DEVICE",
            "adapter_id": None, "prepared_units": None, "observed_units": None,
            "controller_elapsed_s": 1.0, "worker_elapsed_s": None,
            "peak_rss_mb": None, "peak_vram_mb": None,
            "accepted_duration": False, "reject_reason": "unestimated",
            "result_digest": "sha256:" + "c" * 64,
        }
        with pytest.raises(sqlite3.IntegrityError):
            queue.complete_batch(
                "conflict", expected_state="running", owner_token=owner,
                now="2026-08-21T11:32:00Z", observation=observation,
            )
        assert queue.get_batch("conflict")["state"] == "running"
        assert queue.get(job_id)["state"] == "running"
    finally:
        queue.close()


def test_concurrent_terminal_batches_lose_no_profile_observations(tmp_path: Path) -> None:
    db_path = tmp_path / "fleet.db"
    queue = FleetQueue(db_path)
    owners: dict[str, str] = {}
    try:
        for batch in ("first", "second"):
            _job, owner = _claim(queue, batch)
            assert queue.set_batch_state(
                batch, "staging", expected_state="leased", owner_token=owner,
                now="2026-08-21T11:30:00Z",
            )
            assert queue.set_batch_state(
                batch, "running", expected_state="staging", owner_token=owner,
                now="2026-08-21T11:31:00Z",
            )
            owners[batch] = owner
    finally:
        queue.close()

    def complete(batch: str) -> bool:
        connection = FleetQueue(db_path)
        try:
            observation = {
                "profile_key": "sha256:" + ("1" if batch == "first" else "2") * 64,
                "family_id": "sha256:" + "f" * 64,
                "device": "DEVICE", "adapter_id": "sha256:" + "a" * 64,
                "prepared_units": 1.0, "observed_units": 1.0,
                "controller_elapsed_s": 2.0, "worker_elapsed_s": 1.5,
                "peak_rss_mb": 10.0, "peak_vram_mb": None,
                "accepted_duration": True, "reject_reason": None,
                "result_digest": "sha256:" + "d" * 64,
            }
            return connection.complete_batch(
                batch, expected_state="running", owner_token=owners[batch],
                now="2026-08-21T11:32:00Z", observation=observation,
            )
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert all(pool.map(complete, ("first", "second")))
    queue = FleetQueue(db_path)
    try:
        assert {row["batch_id"] for row in queue.profile_observations()} == {"first", "second"}
    finally:
        queue.close()
