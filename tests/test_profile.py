"""Tests for the per-(project, command, device) job profile store."""
import json
import math
from dataclasses import replace

from remrun.profile import (
    LOCAL_DEVICE, MAX_WORKLOAD_METADATA_BYTES, WORKLOAD_PROFILES_KEY,
    WorkloadObservation, command_key, device_profile,
    load_job_costs, load_profiles, load_profiles_checked, merge_job_costs,
    predict_job, profile_project_id, recommend_offload, update_profile,
    update_workload_profile,
    workload_profile,
)


def test_command_key_python_script():
    assert command_key(["python3", "do/run.py", "cfg.yaml"]) == "python3:run.py"


def test_command_key_strips_flags_and_paths():
    assert command_key(["/U/venvs/p/bin/python", "-B", "run.py", "a.yaml"]) == "python:run.py"


def test_command_key_rscript():
    assert command_key(["Rscript", "do/analysis.R"]) == "Rscript:analysis.R"


def test_command_key_fallback_no_script():
    assert command_key(["pnpm", "run", "tauri:build"]) == "pnpm:run"


def test_command_key_empty():
    assert command_key([]) == "?"


def test_profile_project_id_collapses_only_known_nested_worktree_roots():
    assert profile_project_id("ratewall/.worktrees/v1-builder") == "ratewall"
    assert profile_project_id("ratewall/.claude/worktrees/agent-a") == "ratewall"
    assert profile_project_id("ratewall/.delegate-worktrees/a") == "ratewall"
    assert profile_project_id("group/ratewall") == "group/ratewall"
    assert profile_project_id(".worktrees/example") == ".worktrees/example"


def test_command_key_normalizes_pytest_launchers_and_preserves_xdist_shape():
    expected = "python:pytest[xdist=4]"
    assert command_key(["python3.14", "-m", "pytest", "-q", "-n", "4"]) == expected
    assert command_key(["pytest", "--numprocesses=4", "tests/test_a.py"]) == expected
    assert command_key(["uv", "run", "python", "-m", "pytest", "-n4"]) == expected
    assert command_key(["sh", "-lc", "python -m pytest -q -n 4 tests/test_a.py"]) == expected
    assert command_key(["python", "-m", "pytest", "-q"]) == (
        "python:pytest[xdist=default]"
    )


def test_command_key_does_not_interpret_shell_pipeline():
    key = command_key(["sh", "-lc", "python -m pytest -n 4 | tail -1"])
    assert key.startswith("sh:")
    assert key != "python:pytest[xdist=4]"
    assert command_key(["python", "-c", "print('x')", "-m", "pytest"]) != (
        "python:pytest[xdist=default]"
    )


def test_update_and_get_device(tmp_path):
    update_profile(tmp_path, "proj", "python:run.py", "MACBOX",
                   peak_rss_mb=1000, avg_cpu_pct=90, exec_s=100, trip_s=110, now="t1")
    e = device_profile(load_profiles(tmp_path), "proj", "python:run.py", "MACBOX")
    assert e["n"] == 1 and e["rss_mb"] == 1000.0
    assert e["exec_s"] == 100.0 and e["trip_s"] == 110.0 and e["overhead_s"] == 10.0


def test_ewma_merge(tmp_path):
    update_profile(tmp_path, "proj", "k", "MACBOX", peak_rss_mb=1000, exec_s=100, trip_s=120, now="t1")
    update_profile(tmp_path, "proj", "k", "MACBOX", peak_rss_mb=2000, exec_s=200, trip_s=240, now="t2", alpha=0.5)
    e = device_profile(load_profiles(tmp_path), "proj", "k", "MACBOX")
    assert e["n"] == 2 and e["rss_mb"] == 1500.0 and e["exec_s"] == 150.0 and e["trip_s"] == 180.0
    assert e["rss_high_mb"] == 2000.0
    assert predict_job(load_profiles(tmp_path), "proj", "k")["rss_mb"] == 2000.0


def test_rss_high_water_never_falls_after_a_smaller_run(tmp_path):
    update_profile(tmp_path, "proj", "k", "MACBOX", peak_rss_mb=2000, now="t1")
    update_profile(
        tmp_path,
        "proj",
        "k",
        "MACBOX",
        peak_rss_mb=500,
        now="t2",
        alpha=0.5,
    )
    entry = device_profile(load_profiles(tmp_path), "proj", "k", "MACBOX")
    assert entry["rss_mb"] == 1250.0
    assert entry["rss_high_mb"] == 2000.0
    assert predict_job(load_profiles(tmp_path), "proj", "k")["rss_mb"] == 2000.0


def test_devices_are_separate_rows(tmp_path):
    update_profile(tmp_path, "proj", "k", "MACBOX", exec_s=10, trip_s=12, now="t1")
    update_profile(tmp_path, "proj", "k", "WINBOX", exec_s=20, trip_s=30, now="t2")
    profs = load_profiles(tmp_path)
    assert device_profile(profs, "proj", "k", "MACBOX")["trip_s"] == 12.0
    assert device_profile(profs, "proj", "k", "WINBOX")["trip_s"] == 30.0


def test_predict_job_aggregates(tmp_path):
    update_profile(tmp_path, "proj", "k", "MACBOX", peak_rss_mb=800, exec_s=10, trip_s=12, now="t1")
    update_profile(tmp_path, "proj", "k", "WINBOX", peak_rss_mb=1200, exec_s=25, trip_s=30, now="t2")
    update_profile(tmp_path, "proj", "k", LOCAL_DEVICE, exec_s=99, trip_s=99, now="t3")
    p = predict_job(load_profiles(tmp_path), "proj", "k")
    assert p["rss_mb"] == 1200.0     # max peak RSS across rows
    assert p["dur_s"] == 10.0        # min remote exec_s (LOCAL excluded)


def test_predict_job_missing():
    assert predict_job({}, "proj", "k") is None


def test_none_observation_keeps_old(tmp_path):
    update_profile(tmp_path, "proj", "k", "MACBOX", peak_rss_mb=1000, exec_s=10, trip_s=12, now="t1")
    update_profile(tmp_path, "proj", "k", "MACBOX", peak_rss_mb=None, exec_s=20, trip_s=24, now="t2", alpha=0.5)
    e = device_profile(load_profiles(tmp_path), "proj", "k", "MACBOX")
    assert e["rss_mb"] == 1000.0 and e["exec_s"] == 15.0


def test_recommend_remote_when_trip_under_local(tmp_path):
    update_profile(tmp_path, "proj", "k", LOCAL_DEVICE, exec_s=100, trip_s=100, now="t1")
    update_profile(tmp_path, "proj", "k", "MACBOX", exec_s=10, trip_s=20, now="t2")
    r = recommend_offload(load_profiles(tmp_path), "proj", "k", bias=1.0)
    assert r["recommend"] == "remote" and r["best_device"] == "MACBOX"


def test_recommend_bias_keeps_remote_when_marginally_slower(tmp_path):
    # local 100s, best remote trip 120s.
    update_profile(tmp_path, "proj", "k", LOCAL_DEVICE, exec_s=100, trip_s=100, now="t1")
    update_profile(tmp_path, "proj", "k", "MACBOX", exec_s=110, trip_s=120, now="t2")
    profs = load_profiles(tmp_path)
    assert recommend_offload(profs, "proj", "k", bias=1.25)["recommend"] == "remote"  # within 25%
    assert recommend_offload(profs, "proj", "k", bias=1.0)["recommend"] == "local"     # strictly slower


def test_recommend_unknown_without_local_baseline(tmp_path):
    update_profile(tmp_path, "proj", "k", "MACBOX", exec_s=10, trip_s=20, now="t1")  # no LOCAL row
    assert recommend_offload(load_profiles(tmp_path), "proj", "k")["recommend"] == "unknown"


def test_recommend_respects_device_filter(tmp_path):
    update_profile(tmp_path, "proj", "k", LOCAL_DEVICE, exec_s=100, trip_s=100, now="t1")
    update_profile(tmp_path, "proj", "k", "WINBOX", exec_s=10, trip_s=20, now="t2")
    # Restrict to MACBOX (no data for it) → unknown.
    r = recommend_offload(load_profiles(tmp_path), "proj", "k", devices=["MACBOX"])
    assert r["recommend"] == "unknown"


def test_bounding_evicts_oldest(tmp_path):
    for i in range(5):
        update_profile(tmp_path, "proj", f"k{i}", "MACBOX", exec_s=1, trip_s=1, now=f"t{i}", max_entries=3)
    keys = list(load_profiles(tmp_path).get("proj", {}).keys())
    assert len(keys) == 3 and "k0" not in keys and "k4" in keys


def test_legacy_flat_entry_is_relearned(tmp_path):
    # An old flat (pre per-device) row: scalar values. update_profile restarts it
    # as a per-device map; readers tolerate it as no-profile until then.
    (tmp_path / "profiles.json").write_text(
        json.dumps({"proj": {"k": {"n": 3, "rss_mb": 500, "dur_s": 9, "updated": "old"}}}),
        encoding="utf-8")
    assert predict_job(load_profiles(tmp_path), "proj", "k") is None
    update_profile(tmp_path, "proj", "k", "MACBOX", exec_s=10, trip_s=12, now="t1")
    e = device_profile(load_profiles(tmp_path), "proj", "k", "MACBOX")
    assert e is not None and e["n"] == 1 and e["trip_s"] == 12.0


def test_checked_profile_load_distinguishes_absent_valid_and_malformed(tmp_path):
    assert load_profiles_checked(tmp_path).status == "absent"

    path = tmp_path / "profiles.json"
    path.write_text('{"proj": {}}', encoding="utf-8")
    loaded = load_profiles_checked(tmp_path)
    assert loaded.status == "valid" and loaded.profiles == {"proj": {}}

    path.write_text('{"proj":', encoding="utf-8")
    assert load_profiles_checked(tmp_path).status == "malformed"
    assert load_profiles(tmp_path) == {}

    path.write_text("[]", encoding="utf-8")
    assert load_profiles_checked(tmp_path).status == "malformed"


def test_legacy_update_preserves_malformed_existing_profile_bytes(tmp_path):
    path = tmp_path / "profiles.json"
    torn = b'{"proj":{"k":{"MACBOX":{"n":4'
    path.write_bytes(torn)

    update_profile(tmp_path, "proj", "other", "MACBOX", exec_s=2, trip_s=3, now="t2")

    assert path.read_bytes() == torn


def test_legacy_update_rejects_nonfinite_metrics_without_poisoning_store(tmp_path):
    update_profile(
        tmp_path,
        "proj",
        "k",
        "MACBOX",
        peak_rss_mb=100,
        avg_cpu_pct=50,
        exec_s=2,
        trip_s=3,
        now="t1",
    )
    path = tmp_path / "profiles.json"
    before = path.read_bytes()

    update_profile(
        tmp_path,
        "proj",
        "k",
        "MACBOX",
        peak_rss_mb=math.nan,
        avg_cpu_pct=math.inf,
        exec_s=4,
        trip_s=5,
        now="t2",
    )

    assert path.read_bytes() == before
    assert load_profiles_checked(tmp_path).status == "valid"


def _observation(**overrides):
    values = {
        "project_id": "proj",
        "command_key": "python:run.py",
        "device": "MACBOX",
        "workload_name": "demo.build",
        "adapter_version": 1,
        "setting_fingerprint": "sha256:a",
        "receipt_status": "applied",
        "work_unit": "case",
        "evaluation": "accepted",
        "updated": "t1",
        "setting": {
            "outer_workers": 2,
            "inner_workers": 3,
            "native_threads": 1,
        },
        "constraints": {
            "concurrent_process_cap": 8,
            "project_memory_cap_bytes": 4096,
        },
        "exec_s": 10,
        "trip_s": 12,
        "throughput": 20,
        "memory": {
            "peak_bytes": 1000,
            "metric": "rss_sum_sampled",
            "coverage": "complete",
        },
        "cpu": {
            "cpu_sec": 40,
            "avg_cpu_pct": 400,
            "coverage": "complete",
        },
        "gpu": {
            "scope": "whole_device",
            "max_util_pct": 80,
            "min_vram_free_bytes": 2000,
            "unified_memory_min_available_bytes": None,
            "status": "measured",
        },
    }
    values.update(overrides)
    return WorkloadObservation(**values)


def test_workload_profile_keys_separate_setting_identity(tmp_path):
    observations = (
        _observation(),
        _observation(project_id="other-project", updated="t2"),
        _observation(command_key="python:other.py", updated="t3"),
        _observation(device="WINBOX", updated="t4"),
        _observation(setting_fingerprint="sha256:b", updated="t2"),
        _observation(adapter_version=2, updated="t3"),
        _observation(workload_name="demo.test", updated="t4"),
    )

    for observation in observations:
        assert update_workload_profile(tmp_path, observation)

    profiles = load_profiles(tmp_path)
    entries = profiles[WORKLOAD_PROFILES_KEY]["entries"]
    assert len(entries) == 7
    assert all(workload_profile(profiles, observation)["n"] == 1 for observation in observations)


def test_workload_profile_ewma_keeps_metric_kinds_and_last_receipt_status(tmp_path):
    first = _observation()
    second = replace(
        first,
        receipt_status="fallback",
        updated="t2",
        exec_s=20,
        trip_s=24,
        throughput=10,
        memory={
            "peak_bytes": 3000,
            "metric": "rss_sum_sampled",
            "coverage": "complete",
        },
        cpu={"cpu_sec": 80, "avg_cpu_pct": 500, "coverage": "complete"},
        gpu={
            "scope": "whole_device",
            "max_util_pct": 90,
            "min_vram_free_bytes": 1000,
            "unified_memory_min_available_bytes": None,
            "status": "measured",
        },
    )

    assert update_workload_profile(tmp_path, first, alpha=0.5)
    assert update_workload_profile(tmp_path, second, alpha=0.5)

    row = workload_profile(load_profiles(tmp_path), first)
    assert row["n"] == 2
    assert row["exec_s"] == 15.0 and row["trip_s"] == 18.0
    assert row["throughput"] == 15.0
    assert row["memory"] == {
        "peak_bytes": 2000.0,
        "metric": "rss_sum_sampled",
        "coverage": "complete",
    }
    assert row["cpu"]["avg_cpu_pct"] == 450.0
    assert row["gpu"]["min_vram_free_bytes"] == 1500.0
    assert row["gpu"]["unified_memory_min_available_bytes"] is None
    assert row["receipt_status"] == "fallback"
    assert row["evaluation"] == "accepted"
    assert row["work_unit"] == "case"
    assert row["setting"]["outer_workers"] == 2
    assert row["constraints"]["concurrent_process_cap"] == 8
    assert "promoted" not in row


def test_workload_profile_keeps_unified_memory_pressure_without_fabricated_vram(
    tmp_path,
):
    first = _observation(
        gpu={
            "scope": "whole_device",
            "max_util_pct": None,
            "min_vram_free_bytes": None,
            "unified_memory_min_available_bytes": 8_000,
            "status": "unavailable",
        }
    )
    second = replace(
        first,
        updated="t2",
        gpu={
            "scope": "whole_device",
            "max_util_pct": None,
            "min_vram_free_bytes": None,
            "unified_memory_min_available_bytes": 4_000,
            "status": "unavailable",
        },
    )

    assert update_workload_profile(tmp_path, first, alpha=0.5)
    assert update_workload_profile(tmp_path, second, alpha=0.5)

    gpu = workload_profile(load_profiles(tmp_path), first)["gpu"]
    assert gpu["min_vram_free_bytes"] is None
    assert gpu["unified_memory_min_available_bytes"] == 6_000.0


def test_workload_profile_updates_are_bounded(tmp_path):
    observations = [
        _observation(setting_fingerprint=f"sha256:{i}", updated=f"t{i}")
        for i in range(5)
    ]
    for observation in observations:
        assert update_workload_profile(tmp_path, observation, max_entries=3)

    profiles = load_profiles(tmp_path)
    assert workload_profile(profiles, observations[0]) is None
    assert workload_profile(profiles, observations[1]) is None
    assert workload_profile(profiles, observations[-1]) is not None
    assert len(profiles[WORKLOAD_PROFILES_KEY]["entries"]) == 3


def test_workload_and_legacy_profiles_coexist_in_one_file(tmp_path):
    observation = _observation()
    update_profile(tmp_path, "proj", "legacy", "MACBOX", exec_s=4, trip_s=5, now="t1")
    assert update_workload_profile(tmp_path, observation)
    update_profile(
        tmp_path,
        "proj",
        "legacy",
        "MACBOX",
        exec_s=6,
        trip_s=7,
        now="t2",
        max_entries=1,
    )

    profiles = load_profiles(tmp_path)
    assert device_profile(profiles, "proj", "legacy", "MACBOX")["n"] == 2
    assert workload_profile(profiles, observation)["n"] == 1
    assert (tmp_path / "profiles.json").is_file()
    assert not (tmp_path / "workload_profiles.json").exists()


def test_workload_profile_resets_sections_when_telemetry_semantics_change(tmp_path):
    observation = _observation()
    assert update_workload_profile(tmp_path, observation)

    incompatible = replace(
        observation,
        updated="t2",
        memory={
            "peak_bytes": 3000,
            "metric": "job_memory_peak",
            "coverage": "job_object_complete",
        },
        cpu={"cpu_sec": 80, "avg_cpu_pct": 500, "coverage": "partial"},
        gpu={
            "scope": "unified_system",
            "max_util_pct": 90,
            "min_vram_free_bytes": 1000,
            "unified_memory_min_available_bytes": 2048,
            "status": "partial",
        },
    )
    assert update_workload_profile(tmp_path, incompatible, alpha=0.5)

    row = workload_profile(load_profiles(tmp_path), observation)
    assert row["n"] == 2
    assert row["memory"]["peak_bytes"] == 3000.0
    assert row["memory"]["metric"] == "job_memory_peak"
    assert row["cpu"]["avg_cpu_pct"] == 500.0
    assert row["cpu"]["coverage"] == "partial"
    assert row["gpu"]["min_vram_free_bytes"] == 1000.0
    assert row["gpu"]["unified_memory_min_available_bytes"] == 2048.0
    assert row["gpu"]["scope"] == "unified_system"


def test_workload_profile_rejects_setting_fingerprint_collision(tmp_path):
    original = _observation(setting_fingerprint="sha256:same")
    collision = replace(
        original,
        setting={"outer_workers": 99, "inner_workers": 1, "native_threads": 1},
        exec_s=30,
        updated="t2",
    )

    assert update_workload_profile(tmp_path, original)
    before = (tmp_path / "profiles.json").read_bytes()
    assert not update_workload_profile(tmp_path, collision)

    assert (tmp_path / "profiles.json").read_bytes() == before
    row = workload_profile(load_profiles(tmp_path), original)
    assert row["n"] == 1
    assert row["setting"]["outer_workers"] == 2
    assert row["exec_s"] == 10.0


def test_workload_update_rejects_bad_identity_and_nonfinite_metrics(tmp_path):
    assert not update_workload_profile(
        tmp_path,
        _observation(setting_fingerprint=""),
    )
    assert not update_workload_profile(
        tmp_path,
        _observation(setting_fingerprint="not-a-sha"),
    )
    assert not update_workload_profile(
        tmp_path,
        _observation(adapter_version=True),
    )
    assert not update_workload_profile(
        tmp_path,
        _observation(throughput=math.nan),
    )
    assert not update_workload_profile(
        tmp_path,
        _observation(receipt_status=[]),
    )
    assert not update_workload_profile(
        tmp_path,
        _observation(setting={"bad": math.inf}),
    )
    assert not update_workload_profile(
        tmp_path,
        _observation(setting={"too_large": "x" * MAX_WORKLOAD_METADATA_BYTES}),
    )
    assert not (tmp_path / "profiles.json").exists()


def test_workload_update_preserves_malformed_existing_profile_bytes(tmp_path):
    path = tmp_path / "profiles.json"
    malformed = json.dumps({
        WORKLOAD_PROFILES_KEY: {"version": 2, "entries": {}},
    }).encode()
    path.write_bytes(malformed)

    assert not update_workload_profile(tmp_path, _observation())
    update_profile(tmp_path, "proj", "legacy", "MACBOX", exec_s=1, now="t1")

    assert path.read_bytes() == malformed


def test_either_writer_preserves_structurally_bad_legacy_profile_bytes(tmp_path):
    path = tmp_path / "profiles.json"
    malformed_documents = (
        b'{"proj":{"k":{"MACBOX":{"n":1,"exec_s":"fast","updated":"t1"}}}}',
        b'{"proj":{"k":{"MACBOX":{"n":1,"exec_s":NaN,"updated":"t1"}}}}',
    )
    for original in malformed_documents:
        path.write_bytes(original)
        assert load_profiles_checked(tmp_path).status == "malformed"

        update_profile(tmp_path, "proj", "other", "MACBOX", exec_s=1, now="t2")
        assert not update_workload_profile(tmp_path, _observation())

        assert path.read_bytes() == original


# --- legacy per-project job costs (read-only compatibility) ------------------

def test_load_job_costs_merges_per_controller_files(tmp_path):
    import json
    d = tmp_path / "proj" / "do" / "remrun"
    d.mkdir(parents=True)
    (d / "job_costs.winbox.json").write_text(json.dumps({"version": 1, "costs": {
        "k": {"MACBOX": {"rss_mb": 100.0, "exec_s": 10.0, "n": 1, "updated": "t1"}}}}))
    (d / "job_costs.macbox.json").write_text(json.dumps({"version": 1, "costs": {
        "k": {"WINBOX": {"rss_mb": 200.0, "exec_s": 20.0, "n": 1, "updated": "t1"}}}}))
    (d / "job_costs.laptop.json").write_text(json.dumps({"version": 1, "costs": {
        "k": {"MACBOX": {"rss_mb": 999.0, "exec_s": 99.0, "n": 5, "updated": "t9"}}}}))  # newer MACBOX
    before = {path.name: path.read_bytes() for path in d.iterdir()}

    merged = load_job_costs(tmp_path / "proj")

    assert set(merged["k"]) == {"MACBOX", "WINBOX"}          # union across controllers
    assert merged["k"]["MACBOX"]["rss_mb"] == 999.0        # newest `updated` (t9) wins on overlap
    assert merged["k"]["WINBOX"]["rss_mb"] == 200.0
    assert {path.name: path.read_bytes() for path in d.iterdir()} == before


def test_load_job_costs_rejects_malformed_rows_without_coercion(tmp_path):
    d = tmp_path / "proj" / "do" / "remrun"
    d.mkdir(parents=True)
    path = d / "job_costs.old.json"
    original = json.dumps(
        {
            "version": 1,
            "costs": {
                "k": {
                    "MACBOX": {
                        "rss_mb": "huge",
                        "exec_s": "fast",
                        "n": "bad",
                        "updated": "t1",
                    }
                }
            },
        }
    ).encode()
    path.write_bytes(original)

    assert load_job_costs(tmp_path / "proj") == {}
    assert path.read_bytes() == original
    assert merge_job_costs(
        {},
        "proj",
        {"k": {"MACBOX": {"rss_mb": "huge", "n": "bad"}}},
    ) == {}


def test_merge_gives_fresh_controller_the_project_costs(tmp_path):
    # A controller with NO local profile still predicts rss/dur from project job_costs.
    job_costs = {"k": {"MACBOX": {"rss_mb": 8000.0, "cpu_pct": 250.0, "exec_s": 40.0, "n": 3}}}
    merged = merge_job_costs({}, "proj", job_costs)
    pred = predict_job(merged, "proj", "k")
    assert pred == {"rss_mb": 8000.0, "dur_s": 40.0}


def test_merge_local_refinement_wins_over_project(tmp_path):
    update_profile(tmp_path, "proj", "k", "MACBOX", peak_rss_mb=9000, exec_s=50,
                   trip_s=60, now="t1")
    local = load_profiles(tmp_path)
    job_costs = {"k": {"MACBOX": {"rss_mb": 8000.0, "exec_s": 40.0, "n": 5}}}
    merged = merge_job_costs(local, "proj", job_costs)
    e = merged["proj"]["k"]["MACBOX"]
    assert e["rss_mb"] == 9000.0 and e["exec_s"] == 50.0   # local refinement kept
    assert e["trip_s"] == 60.0                              # controller-specific preserved
