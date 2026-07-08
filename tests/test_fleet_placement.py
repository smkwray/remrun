"""Cost model + placement: fit/skip, batch-vs-split, greedy, memory veto, force."""
from __future__ import annotations

from remrun.fleet import placement
from remrun.fleet.models import DeviceSnapshot, FleetTask, JobFeatures
from remrun.fleet.profiles import profile_key


def fcfg():
    return {"transfer_mbps": 200.0, "ssh_setup_s": 0.6, "per_file_overhead_s": 0.05,
            "hysteresis_s": 1.0, "pools": {"gpu": 1}}


def snap(name, **kw):
    base = dict(reachable=True, max_jobs=2, pool_free={"gpu": 1},
                engines_available=frozenset(), ram_free_mb=None, vram_free_mb=None)
    base.update(kw)
    return DeviceSnapshot(name=name, **base)


def ocr_profs(fixed=10.0, var=1.0, vram=2000.0):
    # Seed both OCR devices with identical cost so the batch/split decision is
    # driven purely by job sizes (no transfer: features carry 0 bytes/files).
    return {
        profile_key("ocr", "ocr-remote", "WINBOX", "default"):
            {"fixed_load_s": fixed, "var_per_unit_s": var, "peak_rss_mb": 1000.0,
             "peak_vram_mb": vram, "n": 5},
        profile_key("ocr", "ocr-local", "MACBOX", "default"):
            {"fixed_load_s": fixed, "var_per_unit_s": var, "peak_rss_mb": 1000.0,
             "peak_vram_mb": 0.0, "n": 5},
    }


def ocr_task():
    return FleetTask(task_type="ocr")


def feat(pages):
    return JobFeatures(input_bytes=0, file_count=0, pages=pages, pages_approx=False)


def test_estimate_is_fixed_plus_variable_no_transfer():
    e = placement.estimate_finish([0], "WINBOX", [ocr_task()], [feat(10)], ocr_profs(), fcfg())
    assert e == 10.0 + 1.0 * 10            # fixed + var*pages, transfer 0


def test_confidence_penalty_prefers_learned_profile_when_raw_costs_close():
    cfg = fcfg()
    cfg["confidence_penalty_frac"] = 0.4
    profs = {
        profile_key("ocr", "ocr-remote", "WINBOX", "default"):
            {"fixed_load_s": 10.0, "var_per_unit_s": 1.0, "peak_rss_mb": 1000.0,
             "peak_vram_mb": 2000.0, "n": 0},
        profile_key("ocr", "ocr-local", "MACBOX", "default"):
            {"fixed_load_s": 10.0, "var_per_unit_s": 1.0, "peak_rss_mb": 1000.0,
             "peak_vram_mb": 0.0, "n": 16},
    }
    r = placement.plan_jobs([ocr_task()], [feat(10)],
                            {"WINBOX": snap("WINBOX"), "MACBOX": snap("MACBOX")},
                            profs, cfg, 0.90)
    assert r.batches[0].device == "MACBOX"


def test_confidence_penalty_does_not_swamp_much_faster_prior():
    cfg = fcfg()
    cfg["confidence_penalty_frac"] = 0.4
    profs = {
        profile_key("ocr", "ocr-remote", "WINBOX", "default"):
            {"fixed_load_s": 1.0, "var_per_unit_s": 0.2, "peak_rss_mb": 1000.0,
             "peak_vram_mb": 2000.0, "n": 0},
        profile_key("ocr", "ocr-local", "MACBOX", "default"):
            {"fixed_load_s": 20.0, "var_per_unit_s": 2.0, "peak_rss_mb": 1000.0,
             "peak_vram_mb": 0.0, "n": 16},
    }
    r = placement.plan_jobs([ocr_task()], [feat(10)],
                            {"WINBOX": snap("WINBOX"), "MACBOX": snap("MACBOX")},
                            profs, cfg, 0.90)
    assert r.batches[0].device == "WINBOX"


def test_backlog_reroutes_to_idle_device_over_busy_faster_one():
    # The first device is cheaper (lower load), so an idle fleet sends the job there. But once MACBOX
    # has a big backlog, the SAME job must route to the idle WINBOX, which now finishes sooner.
    profs = {
        profile_key("ocr", "ocr-remote", "WINBOX", "default"):
            {"fixed_load_s": 20.0, "var_per_unit_s": 1.0, "peak_rss_mb": 1000.0,
             "peak_vram_mb": 2000.0, "n": 5},
        profile_key("ocr", "ocr-local", "MACBOX", "default"):
            {"fixed_load_s": 6.0, "var_per_unit_s": 1.0, "peak_rss_mb": 1000.0,
             "peak_vram_mb": 0.0, "n": 5},
    }
    tasks, feats = [ocr_task()], [feat(10)]
    snaps = {"WINBOX": snap("WINBOX"), "MACBOX": snap("MACBOX")}
    # idle: MACBOX wins (6+10=16 < 20+10=30)
    r0 = placement.plan_jobs(tasks, feats, snaps, profs, fcfg(), 0.90)
    assert r0.batches[0].device == "MACBOX"
    # the first device busy with 50 s of work -> eff 66 > WINBOX 30 -> reroute to WINBOX
    r1 = placement.plan_jobs(tasks, feats, snaps, profs, fcfg(), 0.90, device_backlog={"MACBOX": 50.0})
    assert r1.batches[0].device == "WINBOX"
    assert r1.batches[0].estimated_finish_s == 30.0   # stored estimate is OWN compute, not backlog-inflated


def test_large_pair_splits_across_devices():
    tasks = [ocr_task(), ocr_task()]
    feats = [feat(10), feat(10)]
    snaps = {"WINBOX": snap("WINBOX"), "MACBOX": snap("MACBOX")}
    r = placement.plan_jobs(tasks, feats, snaps, ocr_profs(), fcfg(), 0.90)
    # same-device = 10 + 20 = 30; split = max(20, 20) = 20 -> split wins.
    assert len(r.batches) == 2
    assert {b.device for b in r.batches} == {"WINBOX", "MACBOX"}
    assert r.makespan_s == 20.0


def test_tiny_pair_stays_batched_via_hysteresis():
    tasks = [ocr_task(), ocr_task()]
    feats = [feat(1), feat(1)]
    snaps = {"WINBOX": snap("WINBOX"), "MACBOX": snap("MACBOX")}
    r = placement.plan_jobs(tasks, feats, snaps, ocr_profs(), fcfg(), 0.90)
    # same = 10 + 2 = 12; split makespan = 11; 11 + 1 (hysteresis) not < 12 -> batch.
    assert len(r.batches) == 1
    assert sorted(r.batches[0].job_indices) == [0, 1]


def test_gpu_job_gated_on_total_vram_capacity():
    # A model bigger than the card's capacity (x sf) is skipped; one that fits is not,
    # even if transient free VRAM is low (the gpu pool gives it the whole card).
    big = ocr_profs(vram=20000.0)          # 20 GB model > 0.9*16 GB card
    snaps = {"WINBOX": snap("WINBOX", vram_total_mb=16384.0, vram_free_mb=14000.0, ram_free_mb=64000.0),
             "MACBOX": snap("MACBOX", ram_free_mb=64000.0)}
    r = placement.plan_jobs([ocr_task()], [feat(5)], snaps, big, fcfg(), 0.90)
    assert [b.device for b in r.batches] == ["MACBOX"]
    assert "VRAM" in r.skipped["WINBOX"]


def test_gpu_job_fits_on_card_despite_low_free_vram_and_ram():
    # The measured reality: a ~13 GB OCR model fits a 16 GB card via exclusivity even
    # when transient free VRAM (and system RAM) are low.
    p = ocr_profs(vram=13000.0)
    snaps = {"WINBOX": snap("WINBOX", vram_total_mb=16384.0, vram_free_mb=2000.0, ram_free_mb=4000.0)}
    r = placement.plan_jobs([ocr_task()], [feat(5)], snaps, p, fcfg(), 0.90)
    assert [b.device for b in r.batches] == ["WINBOX"]


def test_gpu_job_on_unified_device_gated_on_ram():
    # MACBOX (Mac, unified memory, no discrete VRAM): the model footprint hits RAM.
    tasks = [ocr_task()]
    snaps = {"MACBOX": snap("MACBOX", ram_free_mb=1000.0)}  # model 2000 MB > 0.9*1000
    r = placement.plan_jobs(tasks, [feat(5)], snaps, ocr_profs(), fcfg(), 0.90)
    assert not r.batches and "unified" in r.skipped["MACBOX"]


def test_cpu_job_gated_on_ram_not_vram():
    # A cmd (CPU) job ignores VRAM and is gated on system RAM.
    t = FleetTask(task_type="cmd", options={"argv": ["x"]})
    fits_snap = snap("WINBOX", vram_total_mb=16384.0, vram_free_mb=10.0, ram_free_mb=8000.0)
    r = placement.plan_jobs([t], [JobFeatures(input_bytes=0)], {"WINBOX": fits_snap}, {}, fcfg(), 0.90)
    assert [b.device for b in r.batches] == ["WINBOX"]    # tiny VRAM ignored for CPU job
    starved = snap("WINBOX", vram_free_mb=16000.0, ram_free_mb=100.0)
    r2 = placement.plan_jobs([t], [JobFeatures(input_bytes=0)], {"WINBOX": starved}, {}, fcfg(), 0.90)
    assert not r2.batches and "RAM" in r2.skipped["WINBOX"]


def test_at_max_jobs_and_no_pool_slot_skip():
    snaps = {"WINBOX": snap("WINBOX", active_jobs=2, max_jobs=2),
             "MACBOX": snap("MACBOX", pool_free={"gpu": 0})}
    r = placement.plan_jobs([ocr_task()], [feat(3)], snaps, ocr_profs(), fcfg(), 0.90)
    assert not r.batches
    assert r.skipped["WINBOX"] == "at_max_jobs" or "max_jobs" in r.skipped["WINBOX"]
    assert "gpu" in r.skipped["MACBOX"]


def test_force_device_pins_placement():
    t = FleetTask(task_type="ocr", force_device="MACBOX")
    snaps = {"WINBOX": snap("WINBOX"), "MACBOX": snap("MACBOX")}
    r = placement.plan_jobs([t], [feat(5)], snaps, ocr_profs(), fcfg(), 0.90)
    assert len(r.batches) == 1 and r.batches[0].device == "MACBOX"
    assert r.batches[0].reason == "forced"


def test_force_device_unreachable_fails():
    t = FleetTask(task_type="ocr", force_device="MACBOX")
    snaps = {"MACBOX": snap("MACBOX", reachable=False, detail="down")}
    r = placement.plan_jobs([t], [feat(5)], snaps, ocr_profs(), fcfg(), 0.90)
    assert not r.batches and "MACBOX" in r.skipped


def test_unreachable_all_is_unplaceable():
    snaps = {"WINBOX": snap("WINBOX", reachable=False), "MACBOX": snap("MACBOX", reachable=False)}
    r = placement.plan_jobs([ocr_task()], [feat(2)], snaps, ocr_profs(), fcfg(), 0.90)
    assert not r.batches and r.note


# --- Phase 3b: two-part GPU memory gate ------------------------------------------

def test_vram_engine_reserve_can_push_a_model_over_card():
    # 15 GB model fits a 16 GB card with reserve 0 (default), but a 2 GB engine reserve
    # pushes can-ever-fit over the top -> skipped.
    cfg = fcfg()
    p = ocr_profs(vram=15000.0)
    snaps = {"WINBOX": snap("WINBOX", vram_total_mb=16384.0, vram_free_mb=15000.0, ram_free_mb=64000.0)}
    assert placement.plan_jobs([ocr_task()], [feat(5)], snaps, p, cfg, 0.90).batches  # reserve 0 -> fits
    cfg["vram_reserve_mb"] = {"ocr-remote": 2000.0}
    r = placement.plan_jobs([ocr_task()], [feat(5)], snaps, p, cfg, 0.90)
    assert not r.batches and "VRAM" in r.skipped["WINBOX"]


def test_nonelastic_engine_gated_on_live_free_vram():
    # An elastic engine (default) fits the card even with low free VRAM; marking it
    # non-elastic makes it additionally require LIVE free VRAM (can-launch-now).
    p = ocr_profs(vram=13000.0)
    snaps = {"WINBOX": snap("WINBOX", vram_total_mb=16384.0, vram_free_mb=2000.0, ram_free_mb=64000.0)}
    assert placement.plan_jobs([ocr_task()], [feat(5)], snaps, p, fcfg(), 0.90).batches  # elastic -> fits
    cfg = fcfg()
    cfg["gpu_nonelastic_engines"] = ["ocr-remote"]
    r = placement.plan_jobs([ocr_task()], [feat(5)], snaps, p, cfg, 0.90)
    assert not r.batches and "free VRAM now" in r.skipped["WINBOX"]


# --- Phase 3c: adaptive split hysteresis -----------------------------------------

def test_adaptive_hysteresis_holds_batch_near_crossover_when_pages_approx():
    # A split that wins by a hair is suppressed when the page counts are APPROXIMATE
    # (the per-page time uncertainty exceeds the tiny win) — no flip-flop.
    cfg = fcfg()
    cfg.update({"min_hysteresis_s": 5.0, "hysteresis_finish_frac": 0.05,
                "page_uncertainty_frac": 0.25})
    tasks = [ocr_task(), ocr_task()]
    approx = [JobFeatures(pages=40, pages_approx=True), JobFeatures(pages=40, pages_approx=True)]
    # var=1.0: same = 10 + 80 = 90; split makespan = 10 + 40 = 50. Plain win = 40s.
    # uncertainty margin = var(1.0) * approx_pages(80) * 0.25 = 20s; 50 + 20 = 70 < 90 -> still split.
    r = placement.plan_jobs(tasks, approx, {"WINBOX": snap("WINBOX"), "MACBOX": snap("MACBOX")},
                            ocr_profs(), cfg, 0.90)
    assert len(r.batches) == 2          # big win survives even the uncertainty margin
    # A tiny job pair whose split win is below the hysteresis floor: same=16, split=13 (win 3);
    # margin = max(5, 0.05*13, 1.0*6*0.25=1.5) = 5; 13 + 5 = 18 NOT < 16 -> stays batched.
    tiny = [JobFeatures(pages=3, pages_approx=True), JobFeatures(pages=3, pages_approx=True)]
    r2 = placement.plan_jobs(tasks, tiny, {"WINBOX": snap("WINBOX"), "MACBOX": snap("MACBOX")},
                             ocr_profs(), cfg, 0.90)
    assert len(r2.batches) == 1         # tiny win < hysteresis floor -> stays batched
