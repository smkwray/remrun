"""One arbitrary configured workflow through every supported fleet boundary."""
from __future__ import annotations

from pathlib import Path

from remrun.config import RemrunConfig
from remrun.fleet import adapters, dispatcher, placement, probes
from remrun.fleet.models import DeviceSnapshot
from remrun.fleet.prepared import as_fleet_task, prepare_task_job
from remrun.fleet.queue import FleetQueue
from remrun.fleet.task_contract import resolve_task_spec
from remrun.models import Device


def _definition(worker: Path, output_root: Path) -> dict:
    return {
        "input": {"mode": "files", "extensions": [".zot"], "split": "per-item",
                  "file_identity": "sha256"},
        "prepare": {"mode": "none"},
        "routing": {"requirements": ["zot.v1"], "requirements_by_option": {
            "quality": {"compact": [], "archival": ["zot.archive"]},
        }},
        "execution": {"batching": "compatible"},
        "cost": {"measure": "input-bytes", "unit": "bytes", "divisor": 1,
                 "bucket_options": ["quality"]},
        "output": {"reservation": "content-work-stem-v1", "allow_root_override": False,
                   "verification": "none"},
        "completion": {"protocol": "item-result-v2", "evidence": "always",
                       "companion": "forbidden", "allowed_publication": ["produced"],
                       "unstructured_memory": "ignore"},
        "options": {"quality": {"type": "string", "required": False,
                                  "default": "compact", "values": ["compact", "archival"]}},
        "adapters": {"LOCAL_SIM": {
            "engine": "zot-engine", "argv": ["python", str(worker), "{opt:quality}"],
            "output_root": str(output_root), "pool": False, "memory_kind": "cpu",
            "capability_paths": [str(worker)], "provides": ["zot.v1", "zot.archive"],
        }},
    }


def _config(tmp_path: Path, task: dict) -> RemrunConfig:
    device = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim", "os": "posix", "address_candidates": ["localhost"],
        "project_root": str(tmp_path / "remote"), "cache_root": str(tmp_path / "cache"),
        "state_root": str(tmp_path / "device-state"), "max_jobs": 1,
    })
    return RemrunConfig(
        repo_root=tmp_path, defaults={"fleet": {"pools": {}}},
        devices={"LOCAL_SIM": device}, project_roots={}, offload={},
        fleet_tasks={"zotomatic": task},
    )


def test_novel_name_submits_claims_executes_closes_and_validates(
    tmp_path, monkeypatch,
) -> None:
    output_root = tmp_path / "outputs"
    output_root.mkdir()
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import json,os,pathlib,sys\n"
        "m=json.loads(pathlib.Path(os.environ['REMRUN_BATCH_MANIFEST']).read_text())\n"
        "assert sys.argv[1]=='compact'\n"
        "i=m['items'][0]; out=i['reservations'][0]['stem']+'.zout'\n"
        "(pathlib.Path(m['output_root'])/out).write_text('ok')\n"
        "r={'schema':2,'batch_id':m['batch_id'],'spec_id':m['spec_id'],"
        "'adapter_id':m['adapter_id'],'items':[{'job_id':i['job_id'],"
        "'prepared_id':i['prepared_id'],'index':i['index'],'outcome':'succeeded',"
        "'disposition':'none','retry_after_s':None,'publication':'produced',"
        "'work_performed':True,'outputs':[out],'companion':None,'message':None,"
        "'failure_code':None,'resource':'none','work_units':{'unit':'bytes','value':3},"
        "'elapsed_s':0.01,'details':{}}]}\n"
        "pathlib.Path(os.environ['REMRUN_BATCH_METRICS']).write_text(json.dumps(r))\n",
        encoding="utf-8",
    )
    definition = _definition(worker, output_root)
    config = _config(tmp_path, definition)
    spec = resolve_task_spec(
        "zotomatic", definition, devices=config.devices, repo_root=tmp_path,
    )
    source = tmp_path / "sample.zot"
    source.write_bytes(b"zot")
    prepared = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
    task = as_fleet_task(prepared, spec)

    state_root = tmp_path / "state"
    queue_path = state_root / "fleet" / "fleet.db"
    queue = FleetQueue(queue_path)
    try:
        job_id = queue.enqueue_prepared(
            prepared, spec=spec, current_spec_id=lambda: spec["spec_id"],
        )
        assert queue.get(job_id)["state"] == "queued"
    finally:
        queue.close()

    snapshot = DeviceSnapshot(
        name="LOCAL_SIM", reachable=True, max_jobs=1,
        engine_status={"zot-engine": "present"},
    )
    plan = placement.plan_jobs(
        [task], [adapters.extract_features(task)], {"LOCAL_SIM": snapshot}, {},
        {"pools": {}},
    )
    assert plan.batches[0].device == "LOCAL_SIM"
    rendered = adapters.render_command(task, "LOCAL_SIM", "/stage", str(output_root))
    assert rendered == ["python", str(worker), "compact"]

    monkeypatch.setattr(dispatcher, "load_config", lambda _repo_root=None: config)
    result = dispatcher.drain_once(
        config, state_root=state_root, max_parallel=1,
    )
    assert result["placed"] == 1 and result["ran"] == 1 and result["ok"] == 1

    queue = FleetQueue(queue_path)
    try:
        row = queue.get(job_id)
        assert row["state"] == "done"
        batch = queue.get_batch(row["batch_id"])
        assert batch["state"] == "done"
    finally:
        queue.close()
    assert [path.read_text() for path in output_root.glob("*.zout")] == ["ok"]


class _CapabilityTransport:
    def remote_path_exists(self, path: str) -> bool:
        if path == "/raises":
            raise OSError("probe failed")
        return path == "/present"


def test_capability_probe_preserves_mixed_per_engine_outcomes() -> None:
    status = probes._capability_engines(_CapabilityTransport(), [
        {"engine": "yes", "capability_paths": ["/present"]},
        {"engine": "no", "capability_paths": ["/missing"]},
        {"engine": "maybe", "capability_paths": ["/raises"]},
        {"engine": "unprobed", "capability_paths": []},
    ])
    assert status == {
        "yes": "present", "no": "absent", "maybe": "unknown", "unprobed": "unknown",
    }
