"""Fleet profile store: priors, EWMA update, no warm fields (Invariant 0)."""
from __future__ import annotations

from remrun.fleet import profiles
from remrun.fleet.models import FleetProfile


def test_prior_when_unseen(tmp_path):
    p = profiles.profile_or_prior({}, "ocr", "ocr-remote", "WINBOX", "default")
    assert isinstance(p, FleetProfile) and p.n == 0
    assert p.fixed_load_s and p.peak_vram_mb        # priors populated


def test_update_and_ewma(tmp_path):
    profiles.update_profile(tmp_path, "ocr", "ocr-remote", "WINBOX", "default",
                            fixed_load_s=30, var_per_unit_s=3, peak_rss_mb=6000,
                            peak_vram_mb=8000, now="t1")
    profiles.update_profile(tmp_path, "ocr", "ocr-remote", "WINBOX", "default",
                            fixed_load_s=10, var_per_unit_s=1, peak_vram_mb=4000,
                            now="t2", alpha=0.5)
    p = profiles.get_profile(profiles.load_profiles(tmp_path), "ocr", "ocr-remote",
                             "WINBOX", "default")
    assert p.n == 2
    assert p.fixed_load_s == 20.0 and p.var_per_unit_s == 2.0    # EWMA alpha 0.5
    assert p.peak_vram_mb == 6000.0
    assert p.peak_rss_mb == 6000.0                               # None obs keeps old


def test_time_regression_learns_fixed_and_slope(tmp_path):
    # elapsed = 10 + 2*units; with SPREAD in units the online least-squares recovers fixed=10,
    # slope=2 from observed runs (no hand-seeded coefficients) — the self-correcting cost model.
    for units, elapsed in [(1, 12.0), (5, 20.0), (10, 30.0), (20, 50.0)]:
        profiles.update_profile(tmp_path, "ocr", "ocr-remote", "WINBOX", "default",
                                observed_units=units, observed_elapsed_s=elapsed, now="t")
    p = profiles.get_profile(profiles.load_profiles(tmp_path), "ocr", "ocr-remote",
                             "WINBOX", "default")
    assert abs(p.fixed_load_s - 10.0) < 0.01
    assert abs(p.var_per_unit_s - 2.0) < 0.001


def test_time_regression_keeps_prior_when_units_constant(tmp_path):
    # Runs that all process the SAME unit count give no spread -> fixed vs slope can't be separated
    # -> the seeded coefficients are kept, not corrupted by a degenerate fit.
    profiles.update_profile(tmp_path, "ocr", "ocr-remote", "WINBOX", "default",
                            fixed_load_s=30, var_per_unit_s=3, now="t0")
    for _ in range(5):
        profiles.update_profile(tmp_path, "ocr", "ocr-remote", "WINBOX", "default",
                                observed_units=10, observed_elapsed_s=999.0, now="t")
    p = profiles.get_profile(profiles.load_profiles(tmp_path), "ocr", "ocr-remote",
                             "WINBOX", "default")
    assert p.fixed_load_s == 30.0 and p.var_per_unit_s == 3.0


def test_no_warm_fields_in_store(tmp_path):
    profiles.update_profile(tmp_path, "tts", "tts-remote", "WINBOX", "default",
                            fixed_load_s=20, var_per_unit_s=6, now="t1")
    raw = profiles.load_profiles(tmp_path)
    entry = next(iter(raw.values()))
    assert "fixed_warm_s" not in entry and "warm_engines" not in entry
    assert "fixed_load_s" in entry


def test_keys_are_distinct_per_device_and_bucket(tmp_path):
    profiles.update_profile(tmp_path, "tts", "tts-remote", "WINBOX", "voice=a", fixed_load_s=5, now="t1")
    profiles.update_profile(tmp_path, "tts", "tts-local", "MACBOX", "voice=a", fixed_load_s=9, now="t2")
    raw = profiles.load_profiles(tmp_path)
    assert len(raw) == 2


def test_merge_costs_local_refines_shared_field_by_field():
    # Portability: shared measured costs are the base; a local observation refines only
    # the fields it actually measured (here memory), keeping shared fixed/slope.
    shared = {"ocr|e|MACBOX|v=vision": {"fixed_load_s": 6.0, "var_per_unit_s": 2.4,
                                      "peak_rss_mb": 6900.0, "peak_vram_mb": 6900.0}}
    local = {"ocr|e|MACBOX|v=vision": {"peak_rss_mb": 6875.0, "n": 3, "updated": "t"}}
    m = profiles.merge_costs(local, shared)["ocr|e|MACBOX|v=vision"]
    assert m["fixed_load_s"] == 6.0 and m["var_per_unit_s"] == 2.4   # from shared
    assert m["peak_rss_mb"] == 6875.0                                # refined locally
    assert m["n"] == 3


def test_merge_costs_shared_only_when_no_local():
    # A fresh controller (empty local store) still has full estimates from shared costs.
    shared = {"tts|e|WINBOX|v=norm": {"fixed_load_s": 68.0, "var_per_unit_s": 280.0,
                                    "peak_rss_mb": 15000.0, "peak_vram_mb": 11700.0}}
    m = profiles.merge_costs({}, shared)["tts|e|WINBOX|v=norm"]
    assert m["fixed_load_s"] == 68.0 and m["peak_rss_mb"] == 15000.0 and m["n"] == 0


def test_raise_memory_estimate_after_oom_starts_from_merged_cost_view(tmp_path):
    shared = {"ocr|e|WINBOX|default": {"fixed_load_s": 8.0, "var_per_unit_s": 1.0,
                                     "peak_rss_mb": 1000.0, "peak_vram_mb": 6000.0, "n": 0}}
    field, raised = profiles.raise_memory_estimate(
        tmp_path, "ocr", "e", "WINBOX", "default", memory_kind="gpu",
        factor=1.25, min_delta_mb=512.0, now="t1", base_profiles=shared)
    raw = profiles.load_profiles(tmp_path)["ocr|e|WINBOX|default"]
    assert field == "peak_vram_mb" and raised == 7500.0
    assert raw["peak_vram_mb"] == 7500.0 and raw["peak_rss_mb"] == 1000.0
    assert raw["fixed_load_s"] == 8.0 and raw["oom_n"] == 1
