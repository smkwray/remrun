"""Dispatcher drain_once end-to-end via LOCAL_SIM (no SSH, no models)."""
from __future__ import annotations

import dataclasses
import time
from pathlib import Path

from remrun.config import RemrunConfig
from remrun.fleet import dispatcher
from remrun.fleet.models import DeviceSnapshot, FleetTask
from remrun.fleet.queue import FleetQueue
from remrun.models import Device
from remrun.output import Reporter
from remrun.state import utc_now_iso

_COUNT_ARGV = ["python", "-c",
               "import os,sys,pathlib;"
               "(pathlib.Path(sys.argv[2])/'count.txt').write_text(str(len(os.listdir(sys.argv[1]))))",
               "{stage}", "{output_root}"]


def _config(tmp_path) -> RemrunConfig:
    dev = Device.from_mapping("LOCAL_SIM", {"kind": "local-sim", "os": "posix",
                                            "address_candidates": ["localhost"],
                                            "project_root": str(tmp_path / "remote"),
                                            "cache_root": str(tmp_path / "cache"), "max_jobs": 2})
    return RemrunConfig(
        repo_root=tmp_path, defaults={}, devices={"LOCAL_SIM": dev},
        project_roots={}, offload={},
        fleet_adapters={"cmd": {"LOCAL_SIM": {"engine": "cmd", "output_root": "", "pool": "gpu",
                                               "memory_kind": "cpu"}}},
    )


def _config_ocr(tmp_path) -> RemrunConfig:
    return dataclasses.replace(_config(tmp_path), fleet_adapters={
        "ocr": {"LOCAL_SIM": {"engine": "ocr-test", "output_root": "", "pool": "gpu",
                              "memory_kind": "gpu", "cmd": ["unused"]}},
    }, defaults={"fleet": {"oom_memory_raise_factor": 1.5,
                           "oom_memory_raise_min_delta_mb": 100.0}})


def _queue(state: Path) -> FleetQueue:
    return FleetQueue(state / "fleet" / "fleet.db")


def test_drain_once_empty_queue(tmp_path):
    s = dispatcher.drain_once(_config(tmp_path), state_root=tmp_path / "state")
    assert s["ran"] == 0 and s["ok"] == 0


def test_drain_once_runs_compatible_burst_as_one_batch(tmp_path):
    state = tmp_path / "state"
    out = tmp_path / "out"
    out.mkdir()
    a, b = tmp_path / "a.txt", tmp_path / "b.txt"
    a.write_text("A")
    b.write_text("B")
    q = _queue(state)
    for inp in (a, b):
        q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", inputs=[str(inp)],
                            output_root=str(out), options={"argv": _COUNT_ARGV}))
    q.close()

    s = dispatcher.drain_once(_config(tmp_path), state_root=state, debounce_s=0)
    assert s["placed"] == 1 and s["ran"] == 1 and s["ok"] == 1 and s["failed"] == 0
    # Both inputs were staged into ONE input dir and processed by ONE invocation.
    assert (out / "count.txt").read_text() == "2"
    q = _queue(state)
    try:
        assert q.counts().get("done") == 2 and q.counts().get("queued", 0) == 0
    finally:
        q.close()


def test_drain_once_failed_batch_requeues(tmp_path):
    state = tmp_path / "state"
    q = _queue(state)
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM",
                        options={"argv": ["python", "-c", "import sys;sys.exit(2)"]}))
    q.close()
    s = dispatcher.drain_once(_config(tmp_path), state_root=state, debounce_s=0)
    assert s["ran"] == 1 and s["failed"] == 1
    q = _queue(state)
    try:
        # attempts<MAX -> requeued for another try (assigned_device cleared).
        assert q.counts().get("queued") == 1
        job = q.list("queued")[0]
        assert job["assigned_device"] is None and job["batch_id"] is None
    finally:
        q.close()


def test_drain_once_per_item_metrics_complete_successes_requeue_failures(tmp_path, monkeypatch):
    state = tmp_path / "state"
    q = _queue(state)
    j1 = q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM",
                             options={"argv": ["python", "-c", "pass"]}))
    j2 = q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM",
                             options={"argv": ["python", "-c", "pass"]}))
    q.close()

    def fake_run_batch(_device, _tasks, _config, **kwargs):
        assert kwargs["job_ids"] == [j1, j2]
        assert kwargs["observation_id"]
        return {"ok": True, "exit_code": 0, "elapsed_s": 1.0, "item_results": [
            {"job_id": j1, "ok": True, "outputs": ["a.md"]},
            {"job_id": j2, "ok": False, "error": "bad page"},
        ]}

    monkeypatch.setattr(dispatcher.executor, "run_batch", fake_run_batch)
    s = dispatcher.drain_once(_config(tmp_path), state_root=state, debounce_s=0)
    assert s["ran"] == 1 and s["ok"] == 1 and s["failed"] == 1
    q = _queue(state)
    try:
        assert q.get(j1)["state"] == "done"
        assert q.get(j2)["state"] == "queued"
        assert q.get(j2)["last_error"] == "bad page"
    finally:
        q.close()


def test_drain_once_nonzero_with_item_metrics_is_partial_not_whole_batch(tmp_path, monkeypatch):
    state = tmp_path / "state"
    q = _queue(state)
    j1 = q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM",
                             options={"argv": ["python", "-c", "pass"]}))
    j2 = q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM",
                             options={"argv": ["python", "-c", "pass"]}))
    q.close()

    monkeypatch.setattr(dispatcher.executor, "run_batch", lambda *_a, **_k: {
        "ok": False, "exit_code": 2, "error": "partial failure", "item_results": [
            {"job_id": j1, "ok": True, "outputs": ["a.md"]},
            {"job_id": j2, "ok": False, "error": "bad page"},
        ],
    })
    s = dispatcher.drain_once(_config(tmp_path), state_root=state, debounce_s=0)
    assert s["ran"] == 1 and s["ok"] == 1 and s["failed"] == 1
    q = _queue(state)
    try:
        assert q.get(j1)["state"] == "done"
        assert q.get(j2)["state"] == "queued"
    finally:
        q.close()


def test_drain_once_missing_model_item_evidence_is_final_not_retried(tmp_path, monkeypatch):
    state = tmp_path / "state"
    q = _queue(state)
    job_ids = [
        q.enqueue(FleetTask(task_type="ocr", force_device="LOCAL_SIM",
                            inputs=[str(tmp_path / f"{name}.pdf")]))
        for name in ("a", "b")
    ]
    assert q.claim_many(job_ids, "LOCAL_SIM", batch_id="B-EVIDENCE",
                        lease_until="2099-01-01T00:00:00Z", pool="gpu",
                        task_type="ocr", engine="ocr-test")
    q.close()
    monkeypatch.setattr(dispatcher, "_remote_output_mtimes", lambda *_a, **_k: {})
    monkeypatch.setattr(dispatcher.executor, "run_batch", lambda *_a, **_k: {
        "ok": False,
        "exit_code": 0,
        "no_retry": True,
        "error": "worker succeeded without complete per-file completion evidence",
        "item_results": [],
    })

    tasks = [FleetTask(task_type="ocr", force_device="LOCAL_SIM",
                       inputs=[str(tmp_path / f"{name}.pdf")]) for name in ("a", "b")]
    summary = dispatcher._run_claimed_batch(
        _config_ocr(tmp_path), state,
        {"device": "LOCAL_SIM", "batch_id": "B-EVIDENCE", "btasks": tasks,
         "engine": "ocr-test", "job_ids": job_ids},
        lease_seconds=300, reporter=Reporter(json_events=False),
    )
    assert summary["failed"] == 1
    q = _queue(state)
    try:
        assert all(q.get(job_id)["state"] == "failed_final" for job_id in job_ids)
    finally:
        q.close()


def test_compat_key_separates_mixed_cmd_argv_and_output_roots():
    from remrun.fleet.dispatcher import _compat_key
    a = FleetTask(task_type="cmd", options={"argv": ["python", "x.py"]}, output_root="/o1")
    b = FleetTask(task_type="cmd", options={"argv": ["python", "y.py"]}, output_root="/o1")
    c = FleetTask(task_type="cmd", options={"argv": ["python", "x.py"]}, output_root="/o2")
    d = FleetTask(task_type="cmd", options={"argv": ["python", "x.py"]}, output_root="/o1")
    assert _compat_key(a) != _compat_key(b)   # different argv -> separate batch
    assert _compat_key(a) != _compat_key(c)   # different output root -> separate batch
    assert _compat_key(a) == _compat_key(d)   # identical -> same batch


def test_drain_once_skips_unplaceable_group_and_runs_a_placeable_one(tmp_path):
    state = tmp_path / "state"
    out = tmp_path / "out"
    out.mkdir()
    f = tmp_path / "x.txt"
    f.write_text("x")
    q = _queue(state)
    # First in queue order: forced to an unknown device -> unplaceable (must not starve the rest).
    q.enqueue(FleetTask(task_type="cmd", force_device="NOPE", options={"argv": ["python", "-c", "pass"]}))
    # Second: a placeable LOCAL_SIM job.
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", inputs=[str(f)],
                        output_root=str(out),
                        options={"argv": ["python", "-c",
                                          "import sys,pathlib;(pathlib.Path(sys.argv[2])/'ok.txt').write_text('1')",
                                          "{stage}", "{output_root}"]}))
    q.close()
    s = dispatcher.drain_once(_config(tmp_path), state_root=state, debounce_s=0)
    assert s["ran"] == 1 and s["ok"] == 1 and (out / "ok.txt").exists()


# --- Phase 2b: deterministic output fetch ----------------------------------------

_WRITE_OUT = ["python", "-c",
              "import sys,pathlib;(pathlib.Path(sys.argv[2])/'out.md').write_text('done')",
              "{stage}", "{output_root}"]


def _config_with_tree(tmp_path, local_base: Path, remote_base: Path) -> RemrunConfig:
    """Config whose 'outputs' tree maps the macos base (local controller, monkeypatched
    current_os_key) to ``local_base`` and the default base (the posix LOCAL_SIM device)
    to ``remote_base`` — so a pull copies remote_base -> local_base on the local fs."""
    return dataclasses.replace(_config(tmp_path), sync_roots={
        "outputs": {"macos": str(local_base), "default": str(remote_base)}})


def test_drain_once_verifies_output_then_completes(tmp_path, monkeypatch):
    # The worker writes to the mapped tree base; the dispatcher VERIFIES new output appeared
    # there (Phase 2b) then marks the job done — it does NOT copy the files (Syncthing delivers,
    # so remrun must not race it). The local base stays untouched by remrun.
    monkeypatch.setattr("remrun.sync.current_os_key", lambda: "macos")
    state = tmp_path / "state"
    local_base = tmp_path / "local_outputs"
    local_base.mkdir()
    remote_base = tmp_path / "remote_outputs"
    remote_base.mkdir()
    inp = tmp_path / "in.txt"
    inp.write_text("hi")
    cfg = _config_with_tree(tmp_path, local_base, remote_base)
    q = _queue(state)
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", inputs=[str(inp)],
                        output_root=str(remote_base), options={"argv": _WRITE_OUT}))
    q.close()
    s = dispatcher.drain_once(cfg, state_root=state, debounce_s=0)
    assert s["ran"] == 1 and s["ok"] == 1 and s["failed"] == 0
    assert (remote_base / "out.md").read_text() == "done"        # worker wrote it on the runner
    assert not (local_base / "out.md").exists()                  # remrun did NOT copy it (Syncthing does)
    q = _queue(state)
    try:
        assert q.counts().get("done") == 1
    finally:
        q.close()


def test_verify_zero_new_files_is_final(tmp_path, monkeypatch):
    # OCR/TTS output root maps to a tree but the worker added nothing there (wrote elsewhere —
    # a worker writing outside the configured output root. That must be a FINAL failure.
    monkeypatch.setattr("remrun.sync.current_os_key", lambda: "macos")
    remote_base = tmp_path / "remote"
    remote_base.mkdir()   # empty
    cfg = _config_with_tree(tmp_path, tmp_path / "local", remote_base)
    head = FleetTask(task_type="ocr", force_device="LOCAL_SIM", output_root=str(remote_base))
    out = dispatcher._verify_batch_output(cfg, "LOCAL_SIM", head, Reporter(json_events=False))
    assert out["status"] == "final"


def test_verify_new_file_is_ok_without_copying(tmp_path, monkeypatch):
    # A file the worker ADDS after the pre-run baseline counts as this batch's output -> ok;
    # no local copy is made (Syncthing delivers).
    monkeypatch.setattr("remrun.sync.current_os_key", lambda: "macos")
    local_base = tmp_path / "local"
    local_base.mkdir()
    remote_base = tmp_path / "remote"
    remote_base.mkdir()
    cfg = _config_with_tree(tmp_path, local_base, remote_base)
    head = FleetTask(task_type="ocr", force_device="LOCAL_SIM", output_root=str(remote_base))
    pre = dispatcher._remote_output_mtimes(cfg, "LOCAL_SIM", head)   # baseline (empty) BEFORE the run
    (remote_base / "doc.md").write_text("x")                        # the worker "produces" output
    out = dispatcher._verify_batch_output(cfg, "LOCAL_SIM", head, Reporter(json_events=False), pre)
    assert out["status"] == "ok" and out["new_files"] == 1
    assert not (local_base / "doc.md").exists()                     # verify-only: nothing copied local


def test_verify_preexisting_file_does_not_mask_mismatch(tmp_path, monkeypatch):
    # A STALE pre-existing file under the mapped root must NOT be read as this batch's success
    # when the worker added/updated nothing (audit independent issue).
    monkeypatch.setattr("remrun.sync.current_os_key", lambda: "macos")
    remote_base = tmp_path / "remote"
    remote_base.mkdir()
    (remote_base / "old.md").write_text("stale from a prior run")
    cfg = _config_with_tree(tmp_path, tmp_path / "local", remote_base)
    head = FleetTask(task_type="ocr", force_device="LOCAL_SIM", output_root=str(remote_base))
    pre = dispatcher._remote_output_mtimes(cfg, "LOCAL_SIM", head)   # captures old.md's mtime
    # the worker adds/updates nothing under the mapped tree -> FINAL (it wrote elsewhere)
    out = dispatcher._verify_batch_output(cfg, "LOCAL_SIM", head, Reporter(json_events=False), pre)
    assert out["status"] == "final" and "no new/updated output" in out["detail"]


def test_verify_no_tree_skips(tmp_path):
    # A cmd job with a custom output root that maps to no [sync_roots] tree -> skip + complete.
    cfg = _config(tmp_path)   # no sync_roots
    head = FleetTask(task_type="cmd", force_device="LOCAL_SIM",
                     output_root=str(tmp_path / "whatever"))
    out = dispatcher._verify_batch_output(cfg, "LOCAL_SIM", head, Reporter(json_events=False))
    assert out["status"] == "skip"


def test_verify_no_tree_is_final_for_model_tasks(tmp_path):
    cfg = _config(tmp_path)   # no sync_roots
    head = FleetTask(task_type="tts", force_device="LOCAL_SIM",
                     output_root=str(tmp_path / "whatever"))
    out = dispatcher._verify_batch_output(cfg, "LOCAL_SIM", head, Reporter(json_events=False))
    assert out["status"] == "final"
    assert "no [sync_roots] tree" in out["detail"]


# --- Phase 2c: heartbeat keeps a long batch's lease alive ------------------------

def _config_2dev(tmp_path) -> RemrunConfig:
    def mk(name, sub):
        return Device.from_mapping(name, {"kind": "local-sim", "os": "posix",
                                          "address_candidates": ["localhost"],
                                          "project_root": str(tmp_path / sub / "remote"),
                                          "cache_root": str(tmp_path / sub / "cache"), "max_jobs": 2})
    return RemrunConfig(repo_root=tmp_path, defaults={},
                        devices={"LOCAL_SIM": mk("LOCAL_SIM", "a"), "LOCAL_SIM2": mk("LOCAL_SIM2", "b")},
                        project_roots={}, offload={})


def _config_2dev_cmd_adapters(tmp_path) -> RemrunConfig:
    return dataclasses.replace(_config_2dev(tmp_path), fleet_adapters={
        "cmd": {
            "LOCAL_SIM": {"engine": "cmd-a", "output_root": "", "pool": "gpu",
                          "memory_kind": "cpu"},
            "LOCAL_SIM2": {"engine": "cmd-b", "output_root": "", "pool": "gpu",
                           "memory_kind": "cpu"},
        },
    })


def _writes(name):
    return ["python", "-c",
            f"import sys,pathlib;(pathlib.Path(sys.argv[2])/'{name}').write_text('1')",
            "{stage}", "{output_root}"]


# --- Phase 3d: failure classification + cooldown -------------------------------

def test_classify_failure_kinds():
    from remrun.fleet.dispatcher import _classify_failure
    # OOM is only honored for model tasks (ocr/tts), whose stderr we understand (audit F10).
    assert _classify_failure("CUDA error: out of memory", task_type="ocr")["kind"] == "oom"
    assert _classify_failure("torch.cuda.OutOfMemory: ...", task_type="tts")["kind"] == "oom"
    # transport is recognized from the CONTROLLER's own error string, not worker stderr.
    assert _classify_failure("exec failed: ssh: connect timed out", exit_code=255)["kind"] == "transport"
    assert _classify_failure("stage failed: ...")["kind"] == "transport"
    assert _classify_failure("engine ocr-remote not installed")["kind"] == "capability"
    assert _classify_failure("nonzero exit", exit_code=2)["kind"] == "other"


def test_classify_failure_ignores_arbitrary_cmd_stderr():
    # A cmd job whose OWN stderr mentions transport/OOM words must NOT cool the device (F10):
    # that text is in stderr (not the controller's error) and the task isn't a model task.
    from remrun.fleet.dispatcher import _classify_failure
    assert _classify_failure("", "connection refused; timed out; MemoryError",
                             exit_code=1, task_type="cmd")["kind"] == "other"
    # ...but a real OCR worker OOM in stderr IS honored.
    assert _classify_failure("", "RuntimeError: CUDA out of memory",
                             exit_code=1, task_type="ocr")["kind"] == "oom"


def test_apply_cooldown_scopes(tmp_path):
    from remrun.fleet.dispatcher import _apply_cooldown, _classify_failure
    from remrun.output import Reporter as Rep
    q = FleetQueue(tmp_path / "fleet" / "fleet.db")
    rep = Rep(json_events=False)
    # transport -> device-wide cooldown
    _apply_cooldown(q, "WINBOX", "ocr-remote", _classify_failure("exec failed: ssh timed out", exit_code=255),
                    1, "2026-06-29T00:00:00Z", rep)
    assert q.is_cooled("WINBOX", "model-win", now="2026-06-29T00:00:01Z")    # whole box cooled
    # oom (model task) -> only that engine
    _apply_cooldown(q, "MACBOX", "ocr-mac", _classify_failure("CUDA out of memory", task_type="ocr"),
                    1, "2026-06-29T00:00:00Z", rep)
    assert q.is_cooled("MACBOX", "ocr-mac", now="2026-06-29T00:00:01Z")
    assert q.is_cooled("MACBOX", "model-mac", now="2026-06-29T00:00:01Z") is False
    # other -> no cooldown
    _apply_cooldown(q, "MACBOX", "x", _classify_failure("boom", exit_code=2), 1,
                    "2026-06-29T00:00:00Z", rep)
    q.close()


def test_health_audit_kills_leak_and_cools_engine(tmp_path, monkeypatch):
    cfg = _config(tmp_path)
    dev = dataclasses.replace(cfg.devices["LOCAL_SIM"], cancel={"process_patterns": ["worker"]})
    cfg = dataclasses.replace(cfg, devices={"LOCAL_SIM": dev})
    q = _queue(tmp_path / "state")
    calls = {"killed": 0}

    class FakeTransport:
        def workers_running(self):
            return True

        def kill_workers(self):
            calls["killed"] += 1
            return True

    monkeypatch.setattr(dispatcher, "make_transport", lambda _dev: FakeTransport())
    try:
        assert dispatcher._health_audit(cfg, q, "LOCAL_SIM", "engine1",
                                        Reporter(json_events=False)) is True
        assert calls["killed"] == 1
        assert q.is_cooled("LOCAL_SIM", "engine1")
    finally:
        q.close()


def test_health_audit_no_patterns_is_noop(tmp_path, monkeypatch):
    q = _queue(tmp_path / "state")
    called = []
    monkeypatch.setattr(dispatcher, "make_transport", lambda _dev: called.append(1))
    try:
        assert dispatcher._health_audit(_config(tmp_path), q, "LOCAL_SIM", "engine1",
                                        Reporter(json_events=False)) is False
        assert called == []
    finally:
        q.close()


def test_cooldown_skips_device_in_placement(tmp_path):
    state = tmp_path / "state"
    out = tmp_path / "out"
    out.mkdir()
    q = _queue(state)
    # Cool the only device device-wide -> the job can't be placed this tick.
    q.set_cooldown("LOCAL_SIM", "2099-01-01T00:00:00Z", kind="transport", reason="ssh")
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", output_root=str(out),
                        options={"argv": _writes("a.txt")}))
    q.close()
    s = dispatcher.drain_once(_config(tmp_path), state_root=state, debounce_s=0)
    assert s["ran"] == 0 and s["placed"] == 0
    q = _queue(state)
    try:
        assert q.counts().get("queued") == 1            # still queued, backed off
        assert any("LOCAL_SIM" in d for d in s["cooled"])
    finally:
        q.close()


def test_oom_failure_raises_memory_estimate_for_future_placement(tmp_path, monkeypatch):
    from remrun.fleet import profiles

    state = tmp_path / "state"
    monkeypatch.setattr("remrun.fleet.executor.run_batch",
                        lambda *a, **k: {"ok": False, "exit_code": 1, "error": "worker failed",
                                         "stderr_tail": "RuntimeError: CUDA out of memory"})
    # This test exercises OOM learning after placement; host memory pressure on the
    # test controller must not prevent the mocked worker from reaching that branch.
    monkeypatch.setattr(dispatcher.probes, "build_snapshot", lambda dev, *_a, **_k:
                        DeviceSnapshot(name=dev.name, reachable=True,
                                       ram_free_mb=64_000.0, ram_total_mb=64_000.0,
                                       vram_free_mb=16_000.0, vram_total_mb=16_000.0,
                                       max_jobs=dev.max_jobs, pool_free={"gpu": 1},
                                       engines_available=frozenset({"ocr-test"})))
    q = _queue(state)
    q.enqueue(FleetTask(task_type="ocr", force_device="LOCAL_SIM"))
    q.close()
    s = dispatcher.drain_once(_config_ocr(tmp_path), state_root=state, debounce_s=0)
    assert s["ran"] == 1 and s["failed"] == 1
    raw = profiles.load_profiles(state)
    entry = raw["ocr|ocr-test|LOCAL_SIM|default"]
    assert entry["peak_vram_mb"] == 6144.0        # generic 4096MB prior raised by 1.5x
    assert entry["oom_n"] == 1


# --- Phase 3a: lease-aware + parallel multi-group ------------------------------

def test_single_device_serializes_two_groups(tmp_path):
    # Two incompatible groups (different argv) both want the one device -> only one is claimed
    # this tick (gpu mutex + claimed-this-tick exclusion); the other stays queued.
    state = tmp_path / "state"
    out = tmp_path / "out"
    out.mkdir()
    q = _queue(state)
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", output_root=str(out),
                        options={"argv": _writes("a.txt")}))
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", output_root=str(out),
                        options={"argv": _writes("b.txt")}))
    q.close()
    s = dispatcher.drain_once(_config(tmp_path), state_root=state, debounce_s=0)
    assert s["placed"] == 1 and s["ran"] == 1 and s["ok"] == 1
    q = _queue(state)
    try:
        assert q.counts().get("queued") == 1            # the second group waits for the next tick
    finally:
        q.close()


def test_timeline_scheduler_does_not_let_flexible_group_steal_forced_device(tmp_path):
    # Queue order is flexible first, forced second. A queue-order greedy dispatcher would put the
    # flexible group on LOCAL_SIM (the first/cheaper candidate) and leave the forced group queued.
    # The shared-timeline scheduler plans the forced/constrained group too and routes the flexible
    # group to LOCAL_SIM2, so both run in one tick.
    state = tmp_path / "state"
    out1, out2 = tmp_path / "o1", tmp_path / "o2"
    out1.mkdir()
    out2.mkdir()
    q = _queue(state)
    q.enqueue(FleetTask(task_type="cmd", output_root=str(out1),
                        options={"argv": _writes("flexible.txt")}))
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", output_root=str(out2),
                        options={"argv": _writes("forced.txt")}))
    q.close()

    s = dispatcher.drain_once(_config_2dev_cmd_adapters(tmp_path), state_root=state, debounce_s=0)

    assert s["placed"] == 2 and s["ran"] == 2 and s["ok"] == 2
    assert (out1 / "flexible.txt").exists()
    assert (out2 / "forced.txt").exists()
    q = _queue(state)
    try:
        assert q.counts().get("done") == 2 and q.counts().get("queued", 0) == 0
    finally:
        q.close()


def test_capability_failure_on_forced_device_is_final(tmp_path, monkeypatch):
    # A forced device missing the capability can't be fixed by retrying it (audit F9) -> final.
    state = tmp_path / "state"
    monkeypatch.setattr("remrun.fleet.executor.run_batch",
                        lambda *a, **k: {"ok": False, "error": "engine foo not installed",
                                         "exit_code": 1})
    q = _queue(state)
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM",
                        options={"argv": ["python", "-c", "pass"]}))
    q.close()
    s = dispatcher.drain_once(_config(tmp_path), state_root=state, debounce_s=0)
    assert s["ran"] == 1 and s["failed"] == 1
    q = _queue(state)
    try:
        assert q.counts().get("failed_final") == 1     # finalized, NOT requeued
        assert q.counts().get("queued", 0) == 0
    finally:
        q.close()


def test_allow_fallback_unpins_retry_after_forced_device_failure(tmp_path, monkeypatch):
    state = tmp_path / "state"
    out = tmp_path / "out"
    out.mkdir()
    calls = []

    def fake_run_batch(device, *_a, **_k):
        calls.append(device)
        if device == "LOCAL_SIM":
            return {"ok": False, "exit_code": 255, "error": "exec failed: ssh timed out"}
        return {"ok": True, "exit_code": 0, "elapsed_s": 1.0}

    monkeypatch.setattr("remrun.fleet.executor.run_batch", fake_run_batch)
    q = _queue(state)
    jid = q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", output_root=str(out),
                              options={"argv": ["python", "-c", "pass"],
                                       "_allow_fallback": True,
                                       "_preferred_device": "LOCAL_SIM"}))
    q.close()

    s1 = dispatcher.drain_once(_config_2dev(tmp_path), state_root=state, debounce_s=0)
    assert s1["ran"] == 1 and s1["failed"] == 1
    q = _queue(state)
    try:
        row = q.get(jid)
        assert row["state"] == "queued"
        assert row["force_device"] is None
        assert q.is_cooled("LOCAL_SIM", "default")
    finally:
        q.close()

    s2 = dispatcher.drain_once(_config_2dev(tmp_path), state_root=state, debounce_s=0)
    assert s2["ran"] == 1 and s2["ok"] == 1
    assert calls == ["LOCAL_SIM", "LOCAL_SIM2"]


def test_max_parallel_caps_claims(tmp_path):
    # With two free devices but max_parallel=1, claim only one batch this tick (audit F4) so we
    # never leave a claimed batch leased-but-not-heartbeating.
    state = tmp_path / "state"
    o1, o2 = tmp_path / "o1", tmp_path / "o2"
    o1.mkdir()
    o2.mkdir()
    q = _queue(state)
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", output_root=str(o1),
                        options={"argv": _writes("a.txt")}))
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM2", output_root=str(o2),
                        options={"argv": _writes("b.txt")}))
    q.close()
    s = dispatcher.drain_once(_config_2dev(tmp_path), state_root=state, debounce_s=0, max_parallel=1)
    assert s["placed"] == 1 and s["ran"] == 1       # only one claimed despite two free devices
    q = _queue(state)
    try:
        assert q.counts().get("queued") == 1         # the other waits for the next tick
    finally:
        q.close()


def test_parallel_multigroup_two_devices(tmp_path):
    # Two groups forced to two different devices -> both claimed + run CONCURRENTLY in one tick.
    state = tmp_path / "state"
    out1, out2 = tmp_path / "o1", tmp_path / "o2"
    out1.mkdir()
    out2.mkdir()
    q = _queue(state)
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", output_root=str(out1),
                        options={"argv": _writes("a.txt")}))
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM2", output_root=str(out2),
                        options={"argv": _writes("b.txt")}))
    q.close()
    s = dispatcher.drain_once(_config_2dev(tmp_path), state_root=state, debounce_s=0)
    assert s["placed"] == 2 and s["ran"] == 2 and s["ok"] == 2 and s["failed"] == 0
    assert (out1 / "a.txt").exists() and (out2 / "b.txt").exists()
    q = _queue(state)
    try:
        assert q.counts().get("done") == 2 and q.counts().get("queued", 0) == 0
    finally:
        q.close()


def test_run_until_empty_drains_then_returns(tmp_path):
    # dispatch --drain with zero idle grace: run the queue empty, then RETURN immediately
    # (don't loop forever).
    state = tmp_path / "state"
    out = tmp_path / "out"
    out.mkdir()
    q = _queue(state)
    q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", output_root=str(out),
                        options={"argv": _writes("a.txt")}))
    q.close()
    rc = dispatcher.run(_config(tmp_path), state_root=state, debounce_s=0, poll_s=0,
                        max_ticks=5, until_empty=True, idle_grace_s=0, sleep=lambda _s: None)
    assert rc == 0 and (out / "a.txt").exists()
    q = _queue(state)
    try:
        assert q.counts().get("done") == 1 and q.counts().get("queued", 0) == 0
    finally:
        q.close()


def test_run_until_empty_grace_catches_followup_job(tmp_path):
    # The supervised drain stays alive briefly after the queue first goes empty, so a second
    # trigger can enqueue during the idle grace and still be drained by the same process.
    state = tmp_path / "state"
    out = tmp_path / "out"
    out.mkdir()
    cfg = _config(tmp_path)
    enqueued = False

    def sleepy(_seconds):
        nonlocal enqueued
        if not enqueued:
            q = _queue(state)
            try:
                q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM",
                                    output_root=str(out), options={"argv": _writes("late.txt")}))
                enqueued = True
            finally:
                q.close()
        time.sleep(0.001)

    rc = dispatcher.run(cfg, state_root=state, debounce_s=0, poll_s=0.001, max_ticks=20,
                        until_empty=True, idle_grace_s=0.003, sleep=sleepy)
    assert rc == 0 and (out / "late.txt").exists()
    q = _queue(state)
    try:
        assert q.counts().get("done") == 1 and q.counts().get("queued", 0) == 0
    finally:
        q.close()


def test_idle_grace_is_capped(tmp_path):
    assert dispatcher._bounded_idle_grace_s(_config(tmp_path), 999) == 240.0


def test_run_until_empty_exits_when_jobs_are_stuck(tmp_path):
    # A job forced to a device that isn't in the config is permanently unplaceable. --drain must
    # NOT spin forever on it (the bug that left orphaned `dispatch --drain` processes after a
    # forced force-win/ocr-force-win the device couldn't fit): it leaves the job queued and returns.
    state = tmp_path / "state"
    q = _queue(state)
    q.enqueue(FleetTask(task_type="cmd", force_device="NOPE", options={"argv": ["x"]}))
    q.close()
    sleeps = []
    rc = dispatcher.run(_config(tmp_path), state_root=state, debounce_s=0, poll_s=0,
                        max_ticks=50, until_empty=True, sleep=lambda s: sleeps.append(s))
    assert rc == 0
    assert sleeps == []                     # exited on the first (stuck) tick, did not spin
    q = _queue(state)
    try:
        assert q.counts().get("queued") == 1    # left queued for a later dispatch
    finally:
        q.close()


def test_route_preview_predicts_device_and_busy(tmp_path):
        # fleet submit --json routing preview: predicts the device + whether it's
    # busy, for the trigger UI.
    from remrun.fleet import cli
    state = tmp_path / "state"
    cfg = _config(tmp_path)
    q = _queue(state)
    try:
        task = FleetTask(task_type="cmd", options={"argv": ["python", "-c", "pass"]})
        route = cli._route_preview(task, cfg, q, state)
        assert route["device"] == "LOCAL_SIM" and route["device_busy"] is False
        # hold the gpu lease on LOCAL_SIM -> the preview reports it busy
        jid = q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", options={"argv": ["x"]}))
        assert q.claim_many([jid], "LOCAL_SIM", batch_id="B1", lease_until="2099-01-01T00:00:00Z")
        route2 = cli._route_preview(task, cfg, q, state)
        assert route2["device_busy"] is True and route2["active_on_device"] >= 1
    finally:
        q.close()


def test_route_preview_forced_busy_device_reports_queued_not_no_device(tmp_path, monkeypatch):
    # A second force-win while the forced device is busy: placement can't fit it RIGHT NOW (RAM taken
    # by the running job), but it's forced + busy -> queued-behind-busy, NOT "no device". (The
    # second force-win showed a 99-char "no device (insufficient host RAM)" tooltip though it queued.)
    from types import SimpleNamespace

    from remrun.fleet import cli
    state = tmp_path / "state"
    cfg = _config(tmp_path)
    q = _queue(state)
    try:
        jid = q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM", options={"argv": ["x"]}))
        assert q.claim_many([jid], "LOCAL_SIM", batch_id="B1", lease_until="2099-01-01T00:00:00Z")
        monkeypatch.setattr(cli.placement, "plan_jobs", lambda *a, **k: SimpleNamespace(
            batches=[], note="no eligible device for any job",
            skipped={"LOCAL_SIM": "insufficient host RAM"}, makespan_s=0.0))
        task = FleetTask(task_type="cmd", force_device="LOCAL_SIM", options={"argv": ["y"]})
        route = cli._route_preview(task, cfg, q, state)
        assert route["device"] == "LOCAL_SIM" and route["device_busy"] is True
        line = cli._route_line("cmd", route, False, 1)
        assert "queued" in line and "no device" not in line
    finally:
        q.close()


def test_batch_heartbeat_keeps_lease_alive_vs_recover_stale(tmp_path):
    db = tmp_path / "fleet" / "fleet.db"
    q = FleetQueue(db)
    try:
        jid = q.enqueue(FleetTask(task_type="cmd", force_device="LOCAL_SIM",
                                  options={"argv": ["x"]}))
        now = utc_now_iso()
        # Claim with a SHORT 1 s lease — without a heartbeat this goes stale almost immediately.
        assert q.claim_many([jid], "LOCAL_SIM", batch_id="b1",
                            lease_until=dispatcher._lease_until(now, 1), now=now)
        q.set_batch_state("b1", "running")
        before = q.get_batch("b1")["lease_until"]
        with dispatcher.BatchHeartbeat(db, "b1", lease_seconds=60, interval_s=0.2):
            time.sleep(2.0)                                   # well past the 1 s claim lease
            after = q.get_batch("b1")["lease_until"]
            assert after > before                              # lease was extended
            assert q.recover_stale(utc_now_iso()) == 0         # not requeued — it's alive
            assert q.get_batch("b1")["state"] == "running"
            assert q.get(jid)["state"] == "running"
    finally:
        q.close()


# --- host-RAM reclaim gate (_reclaim_marginal_devices) ------------------------------------------


def _reclaim_dev(name="RCL", with_command=True):
    data = {"kind": "ssh-powershell", "os": "windows", "address_candidates": [name], "max_jobs": 2}
    if with_command:
        data["reclaim"] = {"command": ["~\\tool\\EmptyStandbyList.exe", "workingsets"]}
    return Device.from_mapping(name, data)


def _reclaim_config(tmp_path, dev):
    return RemrunConfig(
        repo_root=tmp_path, defaults={}, devices={dev.name: dev},
        project_roots={}, offload={},
        fleet_adapters={"tts": {dev.name: {"engine": "e", "output_root": "", "pool": "gpu",
                                           "memory_kind": "gpu", "cmd": ["x"]}}})


def _patch_probe(monkeypatch, free_sequence):
    """build_snapshot returns snapshots whose ram_free_mb walks free_sequence (per call)."""
    calls = {"n": 0}

    def fake(dev, transport, fcfg, **kw):
        i = min(calls["n"], len(free_sequence) - 1)
        calls["n"] += 1
        return DeviceSnapshot(name=dev.name, reachable=True, ram_free_mb=free_sequence[i],
                              pool_free={"gpu": 1})
    monkeypatch.setattr(dispatcher.probes, "build_snapshot", fake)
    return calls


def _groups(cfg, need_rss, monkeypatch):
    from remrun.fleet import adapters
    adapters.configure(cfg)
    monkeypatch.setattr(dispatcher.placement, "predicted_resources",
                        lambda task, device, profs: (need_rss, 0.0))
    task = FleetTask(task_type="tts", inputs=["a.md"], idempotency_key="k")
    return [{"tasks": [task], "features": [], "indices": [0], "job_ids": ["j"]}]


def test_reclaim_fires_when_idle_and_marginal(tmp_path, monkeypatch):
    # Idle, reclaim-configured device that a queued job would NOT fit in host RAM -> reclaim runs,
    # and snap_cache is refreshed to the post-reclaim (roomier) snapshot for the placement below.
    cfg = _reclaim_config(tmp_path, _reclaim_dev())
    _patch_probe(monkeypatch, [8000.0, 18000.0])   # before reclaim 8 GB, after 18 GB
    ran = {"n": 0}
    monkeypatch.setattr(dispatcher, "_run_device_reclaim", lambda dev, rep: (ran.__setitem__("n", 1) or True))
    snap_cache = {}
    dispatcher._reclaim_marginal_devices(cfg, _groups(cfg, 9700.0, monkeypatch), snap_cache,
                                         lease_used={}, fcfg={}, profs={}, sf=0.90,
                                         reporter=Reporter(json_events=False))
    assert ran["n"] == 1                                   # reclaim was invoked
    assert snap_cache["RCL"].ram_free_mb == 18000.0        # refreshed to post-reclaim snapshot


def test_reclaim_skipped_when_already_fits(tmp_path, monkeypatch):
    cfg = _reclaim_config(tmp_path, _reclaim_dev())
    _patch_probe(monkeypatch, [18000.0])                   # 18 GB free, job needs 9.7 GB -> fits
    ran = {"n": 0}
    monkeypatch.setattr(dispatcher, "_run_device_reclaim", lambda dev, rep: (ran.__setitem__("n", 1) or True))
    dispatcher._reclaim_marginal_devices(cfg, _groups(cfg, 9700.0, monkeypatch), {}, lease_used={},
                                         fcfg={}, profs={}, sf=0.90, reporter=Reporter(json_events=False))
    assert ran["n"] == 0                                   # comfortably fits -> no reclaim


def test_reclaim_skipped_when_device_busy(tmp_path, monkeypatch):
    # A held pool lease means a model is running there; never trim a live job's working set.
    cfg = _reclaim_config(tmp_path, _reclaim_dev())
    _patch_probe(monkeypatch, [8000.0])
    ran = {"n": 0}
    monkeypatch.setattr(dispatcher, "_run_device_reclaim", lambda dev, rep: (ran.__setitem__("n", 1) or True))
    dispatcher._reclaim_marginal_devices(cfg, _groups(cfg, 9700.0, monkeypatch), {},
                                         lease_used={"RCL": {"gpu": 1}}, fcfg={}, profs={}, sf=0.90,
                                         reporter=Reporter(json_events=False))
    assert ran["n"] == 0


def test_reclaim_noop_without_command(tmp_path, monkeypatch):
    # No [reclaim] command -> function must not even probe the device (zero side effects / SSH).
    cfg = _reclaim_config(tmp_path, _reclaim_dev(with_command=False))
    probed = {"n": 0}
    monkeypatch.setattr(dispatcher.probes, "build_snapshot",
                        lambda *a, **k: probed.__setitem__("n", probed["n"] + 1))
    ran = {"n": 0}
    monkeypatch.setattr(dispatcher, "_run_device_reclaim", lambda dev, rep: (ran.__setitem__("n", 1) or True))
    dispatcher._reclaim_marginal_devices(cfg, _groups(cfg, 9700.0, monkeypatch), {}, lease_used={},
                                         fcfg={}, profs={}, sf=0.90, reporter=Reporter(json_events=False))
    assert probed["n"] == 0 and ran["n"] == 0
