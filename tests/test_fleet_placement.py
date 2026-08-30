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


def _task_for_devices(tmp_path, devices: list[str]):  # noqa: ANN001
    definition = _definition()
    adapter = definition["adapters"]["A"]
    definition["adapters"] = {name: deepcopy(adapter) for name in devices}
    spec = resolve_task_spec(
        "nonsensical-work", definition, devices=set(devices), repo_root=tmp_path,
    )
    record = prepare_task_job(spec, repo_root=tmp_path, text="hello")
    return as_fleet_task(record, spec)


def _snap(name: str, status: str = "present", **kwargs) -> DeviceSnapshot:
    values = {"reachable": True, "enabled": True, "max_jobs": 2, "pool_free": {"gpu": 1},
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


def _gpu_task(tmp_path):  # noqa: ANN001
    definition = _definition()
    for adapter in definition["adapters"].values():
        adapter["memory_kind"] = "gpu"
    spec = resolve_task_spec(
        "opaque-work", definition, devices={"A", "B"}, repo_root=tmp_path,
    )
    return as_fleet_task(
        prepare_task_job(spec, repo_root=tmp_path, text="hello"), spec,
    )


def test_unified_gpu_work_is_charged_once_against_host_memory(tmp_path) -> None:
    task = _gpu_task(tmp_path)
    profiles = _profiles(task, A=2.0)
    profile = profiles[prepared_profile_key(task, "A")]
    profile["peak_rss_mb"] = 4000.0
    profile["peak_vram_mb"] = 9000.0
    snapshot = _snap(
        "A", gpu_memory_topology="unified", ram_free_mb=10000.0,
        vram_free_mb=None, vram_total_mb=None,
    )

    profile["peak_vram_mb"] = 9500.0
    assert placement.fits(task, "A", snapshot, profiles, 0.9, _cfg()) == (
        False, "insufficient unified memory (~9500MB > 90% of free)",
    )
    leftover_vram = _snap(
        "A", gpu_memory_topology="unified", ram_free_mb=10000.0,
        vram_free_mb=None, vram_total_mb=12000.0,
    )
    assert placement.fits(task, "A", leftover_vram, profiles, 0.9, _cfg()) == (
        False, "insufficient unified memory (~9500MB > 90% of free)",
    )
    profile["peak_vram_mb"] = 9000.0
    assert placement.fits(task, "A", snapshot, profiles, 0.9, _cfg()) == (True, "ok")
    unknown = _snap("A", gpu_memory_topology="unknown")
    assert placement.fits(task, "A", unknown, profiles, 0.9, _cfg()) == (
        False, "GPU memory topology unknown",
    )


@pytest.mark.parametrize(
    ("min_hysteresis_s", "hysteresis_finish_frac", "batch_count"),
    [(1.0, 0.05, 2), (3.0, 0.05, 1), (1.0, 0.30, 1)],
)
def test_split_placement_applies_floor_and_finish_fraction(
    tmp_path,
    min_hysteresis_s: float,
    hysteresis_finish_frac: float,
    batch_count: int,
) -> None:
    tasks = [_task(tmp_path), _task(tmp_path)]
    features = [prepared_features(task.prepared) for task in tasks]
    profiles = {}
    for task in tasks:
        profiles[prepared_profile_key(task, "A")] = {
            "fixed_load_s": 0.0, "var_per_unit_s": 1.0,
            "peak_rss_mb": 1000.0, "peak_vram_mb": 0.0, "n": 5,
        }
        profiles[prepared_profile_key(task, "B")] = {
            "fixed_load_s": 0.0, "var_per_unit_s": 1.6,
            "peak_rss_mb": 1000.0, "peak_vram_mb": 0.0, "n": 5,
        }
    config = {
        **_cfg(),
        "min_hysteresis_s": min_hysteresis_s,
        "hysteresis_finish_frac": hysteresis_finish_frac,
    }

    result = placement.plan_jobs(
        tasks, features, {"A": _snap("A"), "B": _snap("B")}, profiles, config,
        device_backlog={"A": 0.0, "B": 0.0},
    )

    assert len(result.batches) == batch_count


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


def test_explanation_distinguishes_ineligible_from_eligible_loser_with_deciding_quantity(
    tmp_path,
) -> None:
    task = _task_for_devices(tmp_path, ["A", "B", "C"])
    result = placement.plan_jobs(
        [task], [prepared_features(task.prepared)],
        {"A": _snap("A"), "B": _snap("B"), "C": _snap("C", "absent")},
        _profiles(task, A=2.0, B=8.0, C=1.0), _cfg(),
        device_backlog={"A": 1.0, "B": 4.0, "C": 0.0},
    )

    explanation = result.batches[0].explanation
    assert explanation["schema"] == 1
    assert explanation["selected"]["device"] == "A"
    assert explanation["selected"]["selection_basis"] == "estimated"
    alternatives = {item["device"]: item for item in explanation["alternatives"]}
    assert alternatives["B"]["status"] == "eligible_not_selected"
    assert alternatives["B"]["reason"] == "higher_estimated_finish"
    assert alternatives["B"]["quantities"]["effective_finish_s"] \
        > explanation["selected"]["quantities"]["effective_finish_s"]
    assert alternatives["C"]["status"] == "ineligible"
    assert alternatives["C"]["reason"] == "fit_rejected"
    assert "not installed" in alternatives["C"]["detail"]


def test_cold_start_explanation_uses_observed_order_not_fabricated_comparison(tmp_path) -> None:
    task = _task(tmp_path)
    result = placement.plan_jobs(
        [task], [prepared_features(task.prepared)],
        {"A": _snap("A"), "B": _snap("B")}, {}, _cfg(),
    )

    explanation = result.batches[0].explanation
    assert explanation["selected"]["selection_basis"] == "cold_start"
    assert explanation["selected"]["estimated_finish_s"] is None
    loser = explanation["alternatives"][0]
    assert loser["status"] == "eligible_not_selected"
    assert loser["reason"] == "cold_start_order"
    assert set(loser["quantities"]) == {"active_jobs", "duration_observations"}


def test_equal_estimates_are_reported_as_a_tie_break_not_a_speed_difference(tmp_path) -> None:
    task = _task(tmp_path)
    result = placement.plan_jobs(
        [task], [prepared_features(task.prepared)],
        {"A": _snap("A"), "B": _snap("B")}, _profiles(task, A=2.0, B=2.0), _cfg(),
    )

    alternative = result.batches[0].explanation["alternatives"][0]
    assert alternative["reason"] == "estimated_tie_break"
    assert alternative["quantities"]["effective_finish_s"] \
        == result.batches[0].explanation["selected"]["quantities"]["effective_finish_s"]


def test_placement_explanation_retains_a_bounded_number_of_alternatives(tmp_path) -> None:
    devices = [f"D{index:02d}" for index in range(40)]
    task = _task_for_devices(tmp_path, devices)
    snapshots = {device: _snap(device) for device in devices}
    profiles = _profiles(task, **{
        device: float(index + 1) for index, device in enumerate(devices)
    })

    result = placement.plan_jobs(
        [task], [prepared_features(task.prepared)], snapshots, profiles, _cfg(),
    )
    explanation = result.batches[0].explanation

    assert len(explanation["alternatives"]) == placement.MAX_PLACEMENT_ALTERNATIVES
    assert explanation["retention"] == {
        "alternative_limit": placement.MAX_PLACEMENT_ALTERNATIVES,
        "total_alternatives": 39,
        "retained_alternatives": placement.MAX_PLACEMENT_ALTERNATIVES,
        "omitted": {"eligible_not_selected": 7},
    }


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
