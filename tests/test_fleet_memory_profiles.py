from __future__ import annotations

import sqlite3
from types import SimpleNamespace

from remrun.fleet import profiles


def test_legacy_summed_rss_is_not_reintroduced_by_cost_merge() -> None:
    merged = profiles.merge_costs(
        {
            "work": {
                "fixed_load_s": 1.0,
                "peak_rss_mb": 8600.0,
                "peak_vram_mb": 512.0,
                "n": 4,
            }
        },
        {},
    )

    assert merged["work"]["peak_rss_mb"] is None
    assert merged["work"]["peak_vram_mb"] == 512.0
    assert merged["work"]["memory_metric"] is None


def test_profile_observation_requires_versioned_private_memory_evidence() -> None:
    task = SimpleNamespace(
        prepared={
            "kind": "task",
            "spec_id": "spec",
            "cost": {
                "status": "exact",
                "measure_id": "measure",
                "unit": "units",
                "value": 1.0,
                "bucket_id": "bucket",
            },
        },
        resolved_spec={"adapters": {"A": {"adapter_id": "adapter"}}},
    )
    result = {
        "ok": True,
        "elapsed_s": 1.0,
        "item_results": [
            {
                "outcome": "succeeded",
                "work_performed": True,
                "work_units": {
                    "unit": "units",
                    "value": 1.0,
                    "measure_id": "measure",
                },
            }
        ],
        # The old compatibility alias deliberately contains the stale/bogus
        # peak. It must never seed a future admission profile.
        "telemetry": {
            "peak_rss_mb": 8600.0,
            "memory_profile": {
                "metric": profiles.RESOURCE_MEMORY_METRIC,
                "peak_bytes": 4 * 1024 * 1024,
            },
        },
    }

    observation = profiles.profile_observation([task], "A", result, None)

    assert observation["peak_rss_mb"] == 4.0
    assert observation["memory_metric"] == profiles.RESOURCE_MEMORY_METRIC

    legacy = dict(result)
    legacy["telemetry"] = {"peak_rss_mb": 8600.0}
    legacy_observation = profiles.profile_observation([task], "A", legacy, None)
    assert legacy_observation["peak_rss_mb"] is None
    assert legacy_observation["memory_metric"] is None


def test_observation_profiles_ignore_unversioned_peak_max(tmp_path) -> None:
    db_path = tmp_path / "fleet.db"
    db = sqlite3.connect(db_path)
    db.execute(
        """CREATE TABLE fleet_profile_observations (
            batch_id TEXT PRIMARY KEY, profile_key TEXT, family_id TEXT,
            device TEXT NOT NULL, adapter_id TEXT, prepared_units REAL,
            observed_units REAL, controller_elapsed_s REAL, worker_elapsed_s REAL,
            peak_rss_mb REAL, peak_vram_mb REAL, memory_metric TEXT,
            accepted_duration INTEGER NOT NULL, reject_reason TEXT,
            result_digest TEXT, recorded_at TEXT NOT NULL
        )"""
    )
    key = "sha256:" + "a" * 64
    db.executemany(
        "INSERT INTO fleet_profile_observations "
        "(batch_id,profile_key,device,peak_rss_mb,memory_metric,"
        "accepted_duration,recorded_at) VALUES (?,?,?,?,?,?,?)",
        [
            ("old", key, "A", 8600.0, None, 0, "2026-08-26T17:00:00Z"),
            (
                "new",
                key,
                "A",
                12.0,
                profiles.RESOURCE_MEMORY_METRIC,
                0,
                "2026-08-26T18:00:00Z",
            ),
        ],
    )
    db.commit()
    db.close()

    loaded = profiles.load_observation_profiles(db_path)

    assert loaded[key]["peak_rss_mb"] == 12.0
    assert loaded[key]["resource_n"] == 1
    assert loaded[key]["memory_metric"] == profiles.RESOURCE_MEMORY_METRIC


def test_unversioned_host_peak_does_not_discard_vram_profile(tmp_path) -> None:
    db_path = tmp_path / "fleet.db"
    db = sqlite3.connect(db_path)
    db.execute(
        """CREATE TABLE fleet_profile_observations (
            batch_id TEXT PRIMARY KEY, profile_key TEXT, family_id TEXT,
            device TEXT NOT NULL, adapter_id TEXT, prepared_units REAL,
            observed_units REAL, controller_elapsed_s REAL, worker_elapsed_s REAL,
            peak_rss_mb REAL, peak_vram_mb REAL, memory_metric TEXT,
            accepted_duration INTEGER NOT NULL, reject_reason TEXT,
            result_digest TEXT, recorded_at TEXT NOT NULL
        )"""
    )
    key = "sha256:" + "b" * 64
    db.execute(
        "INSERT INTO fleet_profile_observations "
        "(batch_id,profile_key,device,peak_rss_mb,peak_vram_mb,memory_metric,"
        "accepted_duration,recorded_at) VALUES (?,?,?,?,?,?,?,?)",
        ("legacy", key, "A", 8600.0, 768.0, None, 0, "2026-08-26T17:00:00Z"),
    )
    db.commit()
    db.close()

    loaded = profiles.load_observation_profiles(db_path)

    assert loaded[key]["peak_rss_mb"] is None
    assert loaded[key]["peak_vram_mb"] == 768.0
    assert loaded[key]["resource_n"] == 1
