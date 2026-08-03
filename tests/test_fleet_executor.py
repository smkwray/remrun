"""Executor end-to-end via the LOCAL_SIM transport (no SSH, no models)."""
from __future__ import annotations

from pathlib import Path

from remrun.config import RemrunConfig
from remrun.fleet import adapters, executor
from remrun.fleet.models import FleetTask
from remrun.fleet.queue import FleetQueue
from remrun.models import Device

_FAR = "2099-01-01T00:00:00Z"


def _config(tmp_path) -> RemrunConfig:
    dev = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim", "os": "posix", "address_candidates": ["localhost"],
        "project_root": str(tmp_path / "remote"), "cache_root": str(tmp_path / "cache"),
        "state_root": str(tmp_path / "devstate"), "max_jobs": 2,
    })
    return RemrunConfig(
        repo_root=tmp_path, defaults={}, devices={"LOCAL_SIM": dev},
        project_roots={}, offload={},
        fleet_adapters={"cmd": {"LOCAL_SIM": {"engine": "cmd", "output_root": "", "pool": "gpu",
                                               "memory_kind": "cpu"}}},
    )


def _config_tts(tmp_path, cmd: list[str]) -> RemrunConfig:
    cfg = _config(tmp_path)
    return RemrunConfig(
        repo_root=cfg.repo_root, defaults=cfg.defaults, devices=cfg.devices,
        project_roots=cfg.project_roots, offload=cfg.offload,
        fleet_adapters={"tts": {"LOCAL_SIM": {"engine": "tts", "output_root": "",
                                               "pool": "gpu", "memory_kind": "cpu",
                                               "cmd": cmd}}},
    )


def _config_ocr(tmp_path, cmd: list[str]) -> RemrunConfig:
    cfg = _config(tmp_path)
    return RemrunConfig(
        repo_root=cfg.repo_root, defaults=cfg.defaults, devices=cfg.devices,
        project_roots=cfg.project_roots, offload=cfg.offload,
        fleet_adapters={"ocr": {"LOCAL_SIM": {"engine": "ocr", "output_root": "",
                                               "pool": "gpu", "memory_kind": "cpu",
                                               "cmd": cmd}}},
    )


def test_run_once_cmd_stages_runs_and_writes_output(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    argv = ["python", "-c",
            "import sys,pathlib;(pathlib.Path(sys.argv[1])/'done.txt').write_text('ok')",
            "{output_root}"]
    task = FleetTask(task_type="cmd", force_device="LOCAL_SIM",
                     output_root=str(out), options={"argv": argv})
    res = executor.run_once(task, _config(tmp_path), state_root=tmp_path / "state")
    assert res["ok"] is True
    assert res["device"] == "LOCAL_SIM" and res["exit_code"] == 0
    assert (out / "done.txt").read_text() == "ok"


def test_run_once_stages_text_input(tmp_path):
    # A cmd job that echoes the staged clip back into the output root proves text staging works.
    # The staged file is named from a slug of the text ("hello fleet" -> hello_fleet.txt) so a
    # clipboard TTS's output is named after its content, not a generic "clip".
    out = tmp_path / "out2"
    out.mkdir()
    argv = ["python", "-c",
            "import sys,pathlib;"
            "src=pathlib.Path(sys.argv[1])/'hello_fleet.txt';"
            "(pathlib.Path(sys.argv[2])/'echo.txt').write_text(src.read_text())",
            "{stage}", "{output_root}"]
    task = FleetTask(task_type="cmd", force_device="LOCAL_SIM", text="hello fleet",
                     output_root=str(out), options={"argv": argv})
    res = executor.run_once(task, _config(tmp_path), state_root=tmp_path / "state")
    assert res["ok"] is True and res["staged"] == 1
    assert (out / "echo.txt").read_text() == "hello fleet"


def test_text_slug():
    assert executor._text_slug("hello fleet") == "hello_fleet"
    # first 30 chars, symbols dropped, spaces -> underscores
    assert executor._text_slug("The 2026 Q3 report: a summary of everything, in detail.") \
        == "The_2026_Q3_report_a_summary"
    # hyphens are KEPT (so 'downward-sloping' isn't merged into 'downwardsloping')
    assert executor._text_slug("The blue downward-sloping curve here") \
        == "The_blue_downward-sloping_curv"
    assert executor._text_slug("   ") == "clip"
    assert len(executor._text_slug("x" * 100)) == 30


def test_run_once_success_updates_profile(tmp_path):
    from remrun.fleet import profiles
    out = tmp_path / "outp"
    out.mkdir()
    state = tmp_path / "state"
    task = FleetTask(task_type="cmd", force_device="LOCAL_SIM",
                     output_root=str(out), options={"argv": ["python", "-c", "pass"]})
    res = executor.run_once(task, _config(tmp_path), state_root=state)
    assert res["ok"] is True
    assert any("LOCAL_SIM" in k for k in profiles.load_profiles(state))


def test_run_once_failed_run_does_not_update_profile(tmp_path):
    # A non-zero exit must NOT fold its (garbage) telemetry into the cost profile.
    from remrun.fleet import profiles
    out = tmp_path / "outf"
    out.mkdir()
    state = tmp_path / "state"
    task = FleetTask(task_type="cmd", force_device="LOCAL_SIM", output_root=str(out),
                     options={"argv": ["python", "-c", "import sys; sys.exit(3)"]})
    res = executor.run_once(task, _config(tmp_path), state_root=state)
    assert res["ok"] is False and res["exit_code"] == 3
    assert profiles.load_profiles(state) == {}


def test_run_batch_stages_all_jobs_one_invocation(tmp_path):
    # Two jobs whose inputs share a basename must both be staged (collision-free) and
    # processed by ONE worker invocation. The "worker" counts files in the input dir.
    out = tmp_path / "out"
    out.mkdir()
    a, b = tmp_path / "da", tmp_path / "db"
    a.mkdir()
    b.mkdir()
    (a / "data.txt").write_text("A")
    (b / "data.txt").write_text("B")           # same basename as the other job's input
    argv = ["python", "-c",
            "import os,sys,pathlib;"
            "(pathlib.Path(sys.argv[2])/'count.txt').write_text(str(len(os.listdir(sys.argv[1]))))",
            "{stage}", "{output_root}"]
    jobs = [
        FleetTask(task_type="cmd", inputs=[str(a / "data.txt")], output_root=str(out),
                  options={"argv": argv}),
        FleetTask(task_type="cmd", inputs=[str(b / "data.txt")], output_root=str(out),
                  options={"argv": argv}),
    ]
    res = executor.run_batch("LOCAL_SIM", jobs, _config(tmp_path), state_root=tmp_path / "state")
    assert res["ok"] is True and res["jobs"] == 2 and res["staged"] == 2
    assert (out / "count.txt").read_text() == "2"   # both inputs landed in one dir


def test_run_batch_reads_per_item_metrics_and_exposes_manifest(tmp_path):
    out = tmp_path / "outm"
    out.mkdir()
    a = tmp_path / "a.txt"
    a.write_text("A")
    argv = ["python", "-c",
            "import json,os,pathlib;"
            "m=json.loads(pathlib.Path(os.environ['REMRUN_BATCH_MANIFEST']).read_text());"
            "assert m['items'][0]['job_id']=='j1';"
            "pathlib.Path(os.environ['REMRUN_BATCH_METRICS']).write_text(json.dumps({"
            "'items':[{'job_id':'j1','ok':True,'units':2,'outputs':['a.out']},"
            "{'job_id':'j2','ok':False,'error':'bad input'}]}))"]
    jobs = [
        FleetTask(task_type="cmd", inputs=[str(a)], output_root=str(out), options={"argv": argv}),
        FleetTask(task_type="cmd", output_root=str(out), options={"argv": argv}),
    ]
    res = executor.run_batch("LOCAL_SIM", jobs, _config(tmp_path), state_root=tmp_path / "state",
                             job_ids=["j1", "j2"])
    assert res["ok"] is True
    assert [item["job_id"] for item in res["item_results"]] == ["j1", "j2"]
    assert res["item_results"][0]["ok"] is True
    assert res["item_results"][1]["ok"] is False
    assert res["item_results"][1]["error"] == "bad input"


def test_run_batch_rejects_controller_native_output_path_for_posix_target(tmp_path):
    task = FleetTask(
        task_type="cmd", force_device="LOCAL_SIM", output_root=r"D:\sync\out",
        options={"argv": ["python", "-c", "raise AssertionError('must not run')"]},
    )
    res = executor.run_batch("LOCAL_SIM", [task], _config(tmp_path),
                             state_root=tmp_path / "state")
    assert res["ok"] is False
    assert res["phase"] == "output_root"
    assert "Windows path" in res["error"] and "POSIX target" in res["error"]
    assert not (tmp_path / "cache").exists()


def test_multi_item_ocr_requires_complete_per_file_evidence(tmp_path):
    out = tmp_path / "ocr-out"
    out.mkdir()
    files = []
    for name in ("a.pdf", "b.pdf"):
        path = tmp_path / name
        path.write_text(name)
        files.append(path)
    cmd = ["python", "-c", "pass"]
    tasks = [FleetTask(task_type="ocr", inputs=[str(path)], output_root=str(out))
             for path in files]
    cfg = _config_ocr(tmp_path, cmd)
    adapters.configure(cfg)
    res = executor.run_batch("LOCAL_SIM", tasks, cfg,
                             state_root=tmp_path / "state", job_ids=["j1", "j2"])
    assert res["ok"] is False
    assert res["no_retry"] is True
    assert res["completion_evidence"] == "missing"
    assert "per-file completion evidence" in res["error"]


def test_multi_item_ocr_evidence_matches_each_staged_stem(tmp_path):
    out = tmp_path / "ocr-evidence"
    out.mkdir()
    files = []
    for name in ("a.pdf", "b.pdf"):
        path = tmp_path / name
        path.write_text(name)
        files.append(path)
    cmd = [
        "python", "-c",
        "import json,os,pathlib;"
        "m=json.loads(pathlib.Path(os.environ['REMRUN_BATCH_MANIFEST']).read_text());"
        "items=[{'job_id':x['job_id'],'index':x['index'],'ok':True,"
        "'outputs':[x['reserved_output_stem']+'.md']} for x in m['items']];"
        "pathlib.Path(os.environ['REMRUN_BATCH_METRICS']).write_text(json.dumps({'items':items}))",
    ]
    tasks = [FleetTask(task_type="ocr", inputs=[str(path)], output_root=str(out))
             for path in files]
    cfg = _config_ocr(tmp_path, cmd)
    adapters.configure(cfg)
    res = executor.run_batch("LOCAL_SIM", tasks, cfg,
                             state_root=tmp_path / "state", job_ids=["j1", "j2"])
    assert res["ok"] is True
    assert res["completion_evidence"] == "complete"
    assert [item["job_id"] for item in res["item_results"]] == ["j1", "j2"]
    assert executor._portable_stem(r"C:\output\a.md") == "a"


def test_run_batch_stages_tts_with_reserved_output_stem(tmp_path):
    out = tmp_path / "reserved"
    out.mkdir()
    argv = ["python", "-c",
            "import os,sys,pathlib;"
            "names=','.join(sorted(os.listdir(sys.argv[1])));"
            "(pathlib.Path(sys.argv[2])/'names.txt').write_text(names)",
            "{stage}", "{output_root}"]
    task = FleetTask(task_type="tts", text="hello fleet",
                     output_root=str(out), options={
                         "_reserved_outputs": [{"source": "text",
                                                "stem": "hello_fleet"}],
                     })
    cfg = _config_tts(tmp_path, argv)
    adapters.configure(cfg)
    res = executor.run_batch("LOCAL_SIM", [task], cfg,
                             state_root=tmp_path / "state", job_ids=["abc12345ffff"])
    assert res["ok"] is True
    assert (out / "names.txt").read_text() == "hello_fleet.txt"
    assert res["batch_metrics"] is None


def test_run_batch_cmd_ignores_reserved_output_stem(tmp_path):
    out = tmp_path / "cmdreserved"
    out.mkdir()
    argv = ["python", "-c",
            "import os,sys,pathlib;"
            "(pathlib.Path(sys.argv[2])/'names.txt').write_text(','.join(os.listdir(sys.argv[1])))",
            "{stage}", "{output_root}"]
    task = FleetTask(task_type="cmd", text="hello fleet",
                     output_root=str(out), options={
                         "argv": argv,
                         "_reserved_outputs": [{"source": "text", "stem": "reserved"}],
                     })
    res = executor.run_batch("LOCAL_SIM", [task], _config(tmp_path),
                             state_root=tmp_path / "state")
    assert res["ok"] is True
    assert (out / "names.txt").read_text() == "hello_fleet.txt"


def test_run_once_with_lease_fails_fast_when_gpu_busy(tmp_path, monkeypatch):
    state = tmp_path / "state"
    out = tmp_path / "out"
    out.mkdir()
    # Pre-hold the LOCAL_SIM gpu lease (as a dispatcher batch would).
    q = FleetQueue(state / "fleet" / "fleet.db")
    held = q.enqueue(FleetTask(task_type="cmd", options={"argv": ["python", "-c", "pass"]}))
    assert q.claim_many([held], "LOCAL_SIM", batch_id="HELD", lease_until=_FAR) is True
    q.close()
    called = []
    monkeypatch.setattr(executor, "run_batch", lambda *a, **k: called.append(1) or {"ok": True})
    task = FleetTask(task_type="cmd", force_device="LOCAL_SIM", output_root=str(out),
                     options={"argv": ["python", "-c", "pass"]})
    res = executor.run_once(task, _config(tmp_path), state_root=state, use_lease=True)
    assert res["ok"] is False and res.get("lease_busy") is True
    assert called == []   # never ran while the GPU was leased


def test_run_once_with_lease_completes_and_frees_lease(tmp_path):
    state = tmp_path / "state"
    out = tmp_path / "out"
    out.mkdir()
    task = FleetTask(task_type="cmd", force_device="LOCAL_SIM", output_root=str(out),
                     options={"argv": ["python", "-c", "pass"]})
    res = executor.run_once(task, _config(tmp_path), state_root=state, use_lease=True)
    assert res["ok"] is True
    # lease freed -> a fresh claim on LOCAL_SIM succeeds, and no ad-hoc job lingers queued.
    q = FleetQueue(state / "fleet" / "fleet.db")
    try:
        assert q.counts().get("queued", 0) == 0
        j = q.enqueue(FleetTask(task_type="cmd", options={"argv": ["python", "-c", "pass"]}))
        assert q.claim_many([j], "LOCAL_SIM", batch_id="X", lease_until=_FAR) is True
    finally:
        q.close()


def test_run_batch_unknown_device_and_empty():
    from remrun.config import RemrunConfig
    cfg = RemrunConfig(repo_root=Path("."), defaults={}, devices={}, project_roots={}, offload={})
    assert executor.run_batch("NOPE", [FleetTask(task_type="cmd")], cfg)["ok"] is False
    dev = {"LOCAL_SIM": Device.from_mapping("LOCAL_SIM", {"kind": "local-sim", "os": "posix",
                                                          "project_root": "/x", "cache_root": "/x/c"})}
    cfg2 = RemrunConfig(repo_root=Path("."), defaults={}, devices=dev, project_roots={}, offload={})
    assert executor.run_batch("LOCAL_SIM", [], cfg2)["error"] == "empty batch"


def test_run_once_no_eligible_device(tmp_path):
    dev = Device.from_mapping("MACBOX", {"kind": "ssh-posix", "os": "macos",
                                       "address_candidates": [], "project_root": "/x"})
    config = RemrunConfig(repo_root=tmp_path, defaults={}, devices={"MACBOX": dev},
                          project_roots={}, offload={})
    # Force an OCR job at a device with no address -> unreachable -> not placeable.
    task = FleetTask(task_type="ocr", force_device="MACBOX", inputs=[])
    res = executor.run_once(task, config, state_root=tmp_path / "state")
    assert res["ok"] is False and "no eligible device" in res["error"]
