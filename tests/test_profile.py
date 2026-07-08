"""Tests for the per-(project, command, device) job profile store."""
import json

from remrun.profile import (
    LOCAL_DEVICE, command_key, device_profile, job_costs_path, load_job_costs,
    load_profiles, merge_job_costs, predict_job, recommend_offload, update_job_costs,
    update_profile,
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


# --- portable per-project job costs (do/remrun/job_costs.json) -----------------

def test_job_costs_written_to_project_do_remrun(tmp_path):
    proj = tmp_path / "proj"
    proj.mkdir()
    update_job_costs(proj, "Rscript:model.R", "MACBOX", rss_mb=8000, cpu_pct=300,
                     exec_s=42, now="t1")
    p = job_costs_path(proj)
    # Per-controller file (job_costs.<controller-id>.json) to avoid the shared-writer conflict,
    # NOT the legacy single job_costs.json.
    assert p.parent == proj / "do" / "remrun"
    assert p.name.startswith("job_costs.") and p.name.endswith(".json") and p.name != "job_costs.json"
    assert p.exists()
    costs = load_job_costs(proj)
    e = costs["Rscript:model.R"]["MACBOX"]
    assert e["rss_mb"] == 8000.0 and e["exec_s"] == 42.0 and e["n"] == 1
    # No controller-specific fields leak into the portable project file.
    assert "trip_s" not in e and "overhead_s" not in e


def test_load_job_costs_merges_per_controller_files(tmp_path):
    # Multiple controllers each write their own job_costs.<id>.json (no shared-writer conflict);
    # a reader merges them all (union of (command,device) rows, newest `updated` wins on overlap).
    import json
    d = tmp_path / "proj" / "do" / "remrun"
    d.mkdir(parents=True)
    (d / "job_costs.winbox.json").write_text(json.dumps({"version": 1, "costs": {
        "k": {"MACBOX": {"rss_mb": 100.0, "exec_s": 10.0, "n": 1, "updated": "t1"}}}}))
    (d / "job_costs.macbox.json").write_text(json.dumps({"version": 1, "costs": {
        "k": {"WINBOX": {"rss_mb": 200.0, "exec_s": 20.0, "n": 1, "updated": "t1"}}}}))
    (d / "job_costs.laptop.json").write_text(json.dumps({"version": 1, "costs": {
        "k": {"MACBOX": {"rss_mb": 999.0, "exec_s": 99.0, "n": 5, "updated": "t9"}}}}))  # newer MACBOX
    merged = load_job_costs(tmp_path / "proj")
    assert set(merged["k"]) == {"MACBOX", "WINBOX"}          # union across controllers
    assert merged["k"]["MACBOX"]["rss_mb"] == 999.0        # newest `updated` (t9) wins on overlap
    assert merged["k"]["WINBOX"]["rss_mb"] == 200.0


def test_job_costs_skips_local_baseline(tmp_path):
    proj = tmp_path / "proj"
    proj.mkdir()
    update_job_costs(proj, "k", LOCAL_DEVICE, rss_mb=5000, exec_s=9, now="t1")
    assert load_job_costs(proj) == {}  # LOCAL baseline is controller-specific, not stored


def test_job_costs_ewma_across_runs(tmp_path):
    proj = tmp_path / "proj"
    proj.mkdir()
    update_job_costs(proj, "k", "MACBOX", rss_mb=10000, exec_s=30, now="t1")
    update_job_costs(proj, "k", "MACBOX", rss_mb=0, exec_s=10, now="t2", alpha=0.5)
    e = load_job_costs(proj)["k"]["MACBOX"]
    assert e["n"] == 2 and e["rss_mb"] == 5000.0 and e["exec_s"] == 20.0


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
