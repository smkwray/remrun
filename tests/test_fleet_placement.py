from __future__ import annotations

from copy import deepcopy

import pytest

from remrun.fleet import placement
from remrun.fleet.models import DeviceSnapshot
from remrun.fleet.prepared import (
    PreparationError,
    as_fleet_task,
    prepare_task_job,
    prepared_features,
)
from remrun.fleet.profiles import prepared_profile_key
from remrun.fleet.task_contract import resolve_task_spec


def _definition() -> dict:
    adapter = {
        "engine": "engine-v1", "argv": ["worker", "{manifest}"],
        "output_root": None, "pool": "gpu", "memory_kind": "cpu",
        "capability_paths": ["/worker"], "provides": ["worker.v1"],
    }
    return {
        "input": {"mode": "text", "split": "never"},
        "prepare": {"mode": "none"},
        "routing": {"requirements": ["worker.v1"], "requirements_by_option": {}},
        "execution": {"batching": "never", "replay": "at-most-once-v1"},
        "cost": {"measure": "text-codepoints", "unit": "chars", "divisor": 1,
                 "bucket_options": []},
        "output": {"reservation": "none", "allow_root_override": False,
                   "verification": "none"},
        "completion": {"protocol": "exit-code-v1", "evidence": "never",
                       "companion": "forbidden", "allowed_publication": ["none"],
                       "unstructured_memory": "ignore"},
        "options": {},
        "adapters": {"A": deepcopy(adapter), "B": deepcopy(adapter)},
    }


def _task(tmp_path, *, forced: str | None = None):  # noqa: ANN001
    spec = resolve_task_spec(
        "nonsensical-work", _definition(), devices={"A", "B"}, repo_root=tmp_path,
    )
    record = prepare_task_job(
        spec, repo_root=tmp_path, text="hello", force_device=forced,
    )
    return as_fleet_task(record, spec)


def _snap(name: str, status: str = "present", **kwargs) -> DeviceSnapshot:
    values = {"reachable": True, "max_jobs": 2, "pool_free": {"gpu": 1},
              "engine_status": {"engine-v1": status}, "ram_free_mb": 32000.0}
    values.update(kwargs)
    return DeviceSnapshot(name=name, **values)


def _profiles(task, **fixed):  # noqa: ANN001
    return {
        prepared_profile_key(task, device): {
            "fixed_load_s": seconds, "var_per_unit_s": 1.0,
            "peak_rss_mb": 1000.0, "peak_vram_mb": 0.0, "n": 5,
        }
        for device, seconds in fixed.items()
    }


def _cfg() -> dict:
    return {"transfer_mbps": 200.0, "ssh_setup_s": 0.0,
            "per_file_overhead_s": 0.0, "min_hysteresis_s": 1.0,
            "pools": {"gpu": 1}}


def test_automatic_route_requires_positive_capability_qualification(tmp_path) -> None:
    task = _task(tmp_path)
    result = placement.plan_jobs(
        [task], [prepared_features(task.prepared)], {"A": _snap("A", "unknown")},
        _profiles(task, A=2.0), _cfg(),
    )
    assert not result.batches
    assert result.skipped["A"] == "engine engine-v1 qualification unknown"


def test_confirmed_absent_engine_is_refused(tmp_path) -> None:
    task = _task(tmp_path)
    result = placement.plan_jobs(
        [task], [prepared_features(task.prepared)], {"A": _snap("A", "absent")},
        _profiles(task, A=2.0), _cfg(),
    )
    assert not result.batches
    assert "not installed" in result.skipped["A"]


def test_explicit_device_may_proceed_to_target_preflight_when_unknown(tmp_path) -> None:
    task = _task(tmp_path, forced="A")
    result = placement.plan_jobs(
        [task], [prepared_features(task.prepared)], {"A": _snap("A", "unknown")},
        {}, _cfg(),
    )
    assert [batch.device for batch in result.batches] == ["A"]
    assert result.batches[0].reason == "forced"


def test_multiple_devices_without_comparable_profiles_calibrate_one_device(tmp_path) -> None:
    task = _task(tmp_path)
    result = placement.plan_jobs(
        [task], [prepared_features(task.prepared)],
        {"A": _snap("A"), "B": _snap("B")}, _profiles(task, A=2.0), _cfg(),
    )
    assert result.skipped == {}
    assert result.makespan_s is None
    assert len(result.batches) == 1
    assert result.batches[0].device == "B"
    assert result.batches[0].selection_basis == "exploration"
    assert result.batches[0].estimated_finish_s is None
    assert result.batches[0].estimate_reason == "uncalibrated"


def test_observed_profiles_choose_faster_device_and_include_backlog(tmp_path) -> None:
    task = _task(tmp_path)
    snapshots = {"A": _snap("A"), "B": _snap("B")}
    costs = _profiles(task, A=2.0, B=8.0)
    first = placement.plan_jobs(
        [task], [prepared_features(task.prepared)], snapshots, costs, _cfg(),
    )
    assert first.batches[0].device == "A"
    second = placement.plan_jobs(
        [task], [prepared_features(task.prepared)], snapshots, costs, _cfg(),
        device_backlog={"A": 20.0},
    )
    assert second.batches[0].device == "B"


def test_max_jobs_and_pool_slots_are_enforced(tmp_path) -> None:
    task = _task(tmp_path)
    result = placement.plan_jobs(
        [task], [prepared_features(task.prepared)],
        {"A": _snap("A", active_jobs=2, max_jobs=2),
         "B": _snap("B", pool_free={"gpu": 0})},
        _profiles(task, A=2.0, B=2.0), _cfg(),
    )
    assert not result.batches
    assert result.skipped["A"] == "at max_jobs"
    assert result.skipped["B"] == "no gpu slot free"


def test_engine_selector_filters_automatic_candidates(tmp_path) -> None:
    raw = _definition()
    raw["adapters"]["B"]["engine"] = "engine-v2"
    spec = resolve_task_spec(
        "nonsensical-work", raw, devices={"A", "B"}, repo_root=tmp_path,
    )
    record = prepare_task_job(
        spec, repo_root=tmp_path, text="hello", engine="engine-v1",
    )
    task = as_fleet_task(record, spec)
    snapshots = {
        "A": _snap("A"),
        "B": DeviceSnapshot(
            name="B", reachable=True, max_jobs=2, pool_free={"gpu": 1},
            engine_status={"engine-v2": "present"}, ram_free_mb=32000.0,
        ),
    }
    result = placement.plan_jobs(
        [task], [prepared_features(task.prepared)], snapshots, _profiles(task, A=2.0), _cfg(),
    )
    assert [batch.device for batch in result.batches] == ["A"]
    assert "B" not in result.skipped


def test_forced_device_cannot_bypass_requested_engine(tmp_path) -> None:
    raw = _definition()
    raw["adapters"]["B"]["engine"] = "engine-v2"
    spec = resolve_task_spec(
        "nonsensical-work", raw, devices={"A", "B"}, repo_root=tmp_path,
    )
    record = prepare_task_job(
        spec, repo_root=tmp_path, text="hello", force_device="B", engine="engine-v1",
    )
    with pytest.raises(PreparationError, match="force_device does not provide"):
        as_fleet_task(record, spec)
