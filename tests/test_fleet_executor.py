"""Prepared fleet execution end to end through the LOCAL_SIM transport."""
from __future__ import annotations

import json
import os
from pathlib import Path

from remrun.config import RemrunConfig
from remrun.fleet import executor, profiles
from remrun.fleet.prepared import (
    RAW_COMMAND_SPEC,
    RAW_COMMAND_SPEC_ID,
    as_fleet_task,
    prepare_raw_command,
    prepare_task_job,
)
from remrun.fleet.task_contract import resolve_task_spec
from remrun.models import Device
from remrun.transport import TransportError


def _config(tmp_path) -> RemrunConfig:  # noqa: ANN001
    device = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim", "os": "windows" if os.name == "nt" else "posix",
        "address_candidates": ["localhost"],
        "project_root": str(tmp_path / "remote"), "cache_root": str(tmp_path / "cache"),
        "state_root": str(tmp_path / "devstate"), "max_jobs": 2,
    })
    return RemrunConfig(
        repo_root=tmp_path, defaults={}, devices={"LOCAL_SIM": device},
        project_roots={}, offload={},
    )


def _definition(worker: str, output_root: str) -> dict:
    return {
        "input": {"mode": "files", "extensions": [".zot"], "split": "per-item",
                  "file_identity": "sha256"},
        "prepare": {"mode": "none"},
        "routing": {"requirements": [], "requirements_by_option": {}},
        "execution": {"batching": "compatible"},
        "cost": {"measure": "item-count", "unit": "items", "divisor": 1,
                 "bucket_options": []},
        "output": {"reservation": "content-work-stem-v1", "allow_root_override": False,
                   "verification": "none"},
        "completion": {"protocol": "item-result-v2", "evidence": "always",
                       "companion": "forbidden", "allowed_publication": ["produced"],
                       "unstructured_memory": "ignore"},
        "options": {},
        "adapters": {"LOCAL_SIM": {
            "engine": "zot", "argv": ["python", worker, "{stage}", "{manifest}"],
            "output_root": output_root,
            "pool": False, "memory_kind": "cpu", "capability_paths": [], "provides": [],
        }},
    }


def test_prepared_raw_command_runs_exact_argv_once_per_submission(tmp_path) -> None:
    marker = tmp_path / "raw-invocations.jsonl"
    hostile = ["", "a b", "*.txt", ">", "--flag", "☃", "~/must-remain-literal"]
    argv = [
        "python", "-c",
        "import json,pathlib,sys; p=pathlib.Path(sys.argv[1]); "
        "p.open('a',encoding='utf-8').write(json.dumps(sys.argv[2:])+'\\n')",
        str(marker), *hostile,
    ]
    spec = {**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID}
    for _ in range(2):
        task = as_fleet_task(prepare_raw_command(argv, device="LOCAL_SIM"), spec)
        result = executor.run_batch(
            "LOCAL_SIM", [task], _config(tmp_path), state_root=tmp_path / "state",
        )
        assert result["ok"] is True
    assert [json.loads(line) for line in marker.read_text(encoding="utf-8").splitlines()] == [
        hostile, hostile,
    ]


def test_prepared_source_change_stops_before_worker_launch(tmp_path) -> None:
    source = tmp_path / "source.bin"
    marker = tmp_path / "worker-launched"
    source.write_bytes(b"original")
    argv = ["python", "-c", f"import pathlib; pathlib.Path({str(marker)!r}).write_text('yes')"]
    spec = {**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID}
    task = as_fleet_task(
        prepare_raw_command(argv, device="LOCAL_SIM", inputs=[str(source)]), spec,
    )
    source.write_bytes(b"changed!")

    result = executor.run_batch(
        "LOCAL_SIM", [task], _config(tmp_path), state_root=tmp_path / "state",
        job_ids=["j1"],
    )

    assert result["ok"] is False and result["error"] == "source_changed"
    assert result["item_results"][0]["disposition"] == "review"
    assert not marker.exists()


def test_configured_task_executes_frozen_manifest_and_result_v2(tmp_path) -> None:
    output_root = tmp_path / "prepared-output"
    output_root.mkdir()
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import json,os,pathlib,sys\n"
        "assert pathlib.Path(sys.argv[1]).resolve()==pathlib.Path(os.environ['REMRUN_STAGE_IN']).resolve()\n"
        "assert pathlib.Path(sys.argv[2]).resolve()==pathlib.Path(os.environ['REMRUN_BATCH_MANIFEST']).resolve()\n"
        "assert pathlib.Path(sys.argv[2]).parent.resolve()==pathlib.Path(sys.argv[1]).parent.resolve()\n"
        "m=json.loads(pathlib.Path(os.environ['REMRUN_BATCH_MANIFEST']).read_text())\n"
        "i=m['items'][0]; out=i['reservations'][0]['stem']+'.bin'\n"
        "(pathlib.Path(m['output_root'])/out).write_text('done')\n"
        "r={'schema':2,'batch_id':m['batch_id'],'spec_id':m['spec_id'],"
        "'adapter_id':m['adapter_id'],'items':[{'job_id':i['job_id'],"
        "'prepared_id':i['prepared_id'],'index':i['index'],'outcome':'succeeded',"
        "'disposition':'none','retry_after_s':None,'publication':'produced',"
        "'work_performed':True,'outputs':[out],'companion':None,'message':None,"
        "'failure_code':None,'resource':'none','work_units':{'unit':'items','value':1},"
        "'elapsed_s':0.01,'details':{}}]}\n"
        "pathlib.Path(os.environ['REMRUN_BATCH_METRICS']).write_text(json.dumps(r))\n",
        encoding="utf-8",
    )
    spec = resolve_task_spec(
        "zotomatic", _definition(str(worker), str(output_root)),
        devices={"LOCAL_SIM"}, repo_root=tmp_path,
    )
    source = tmp_path / "input.zot"
    source.write_text("payload", encoding="utf-8")
    prepared = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
    task = as_fleet_task(prepared, spec)

    result = executor.run_batch(
        "LOCAL_SIM", [task], _config(tmp_path), state_root=tmp_path / "state",
        job_ids=["j1"], observation_id="batch1", prelaunch_gate=lambda: True,
    )

    assert result["ok"] is True and result["completion_evidence"] == "complete"
    assert result["item_results"][0]["prepared_id"] == prepared["prepared_id"]
    assert (output_root / result["item_results"][0]["outputs"][0]).read_text() == "done"
    assert profiles.prepared_profile_key(task, "LOCAL_SIM") in profiles.load_profiles(
        tmp_path / "state"
    )


def test_exit_code_worker_is_not_asked_for_structured_metrics(tmp_path) -> None:
    """Legacy exit-only workers must not see the item-result-v2 output contract."""
    output_root = tmp_path / "exit-only-output"
    output_root.mkdir()
    worker = tmp_path / "exit-only-worker.py"
    worker.write_text(
        "import os\n"
        "assert 'REMRUN_BATCH_MANIFEST' in os.environ\n"
        "assert 'REMRUN_BATCH_METRICS' not in os.environ\n",
        encoding="utf-8",
    )
    definition = _definition(str(worker), str(output_root))
    definition["execution"] = {"batching": "never"}
    definition["output"] = {
        "reservation": "none", "allow_root_override": False, "verification": "none",
    }
    definition["completion"] = {
        "protocol": "exit-code-v1", "evidence": "never", "companion": "forbidden",
        "allowed_publication": ["none"], "unstructured_memory": "ignore",
    }
    spec = resolve_task_spec(
        "zotomatic", definition, devices={"LOCAL_SIM"}, repo_root=tmp_path,
    )
    source = tmp_path / "input.zot"
    source.write_text("payload", encoding="utf-8")
    task = as_fleet_task(
        prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)]), spec,
    )

    result = executor.run_batch(
        "LOCAL_SIM", [task], _config(tmp_path), state_root=tmp_path / "state",
        job_ids=["j1"], observation_id="batch1", prelaunch_gate=lambda: True,
    )

    assert result["ok"] is True
    assert result["item_results"] == []


def test_malformed_result_digest_is_terminal_and_not_learned(tmp_path) -> None:
    output_root = tmp_path / "malformed-result-output"
    output_root.mkdir()
    worker = tmp_path / "malformed-result-worker.py"
    worker.write_text(
        "import json,os,pathlib\n"
        "m=json.loads(pathlib.Path(os.environ['REMRUN_BATCH_MANIFEST']).read_text())\n"
        "i=m['items'][0]\n"
        "r={'schema':2,'batch_id':m['batch_id'],'spec_id':m['spec_id'],"
        "'adapter_id':m['adapter_id'],'items':[{'job_id':i['job_id'],"
        "'prepared_id':[],'index':i['index'],'outcome':'succeeded',"
        "'disposition':'none','retry_after_s':None,'publication':'produced',"
        "'work_performed':True,'outputs':[i['reservations'][0]['stem']+'.bin'],"
        "'companion':None,'message':None,'failure_code':None,'resource':'none',"
        "'work_units':{'unit':'items','value':1},'elapsed_s':0.01,'details':{}}]}\n"
        "pathlib.Path(os.environ['REMRUN_BATCH_METRICS']).write_text(json.dumps(r))\n",
        encoding="utf-8",
    )
    spec = resolve_task_spec(
        "zotomatic", _definition(str(worker), str(output_root)),
        devices={"LOCAL_SIM"}, repo_root=tmp_path,
    )
    source = tmp_path / "input.zot"
    source.write_text("payload", encoding="utf-8")
    prepared = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
    task = as_fleet_task(prepared, spec)

    result = executor.run_batch(
        "LOCAL_SIM", [task], _config(tmp_path), state_root=tmp_path / "state",
        job_ids=["j1"], observation_id="batch1", prelaunch_gate=lambda: True,
    )

    assert result["ok"] is False and result["no_retry"] is True
    assert result["item_results"] == [] and "prepared_id" in result["error"]
    assert profiles.load_profiles(tmp_path / "state") == {}


def test_stage_setup_failure_discards_snapshots_and_remote_root(
    tmp_path, monkeypatch,
) -> None:
    output_root = tmp_path / "prepared-output"
    output_root.mkdir()
    worker = tmp_path / "worker.py"
    worker.write_text("raise SystemExit(99)", encoding="utf-8")
    spec = resolve_task_spec(
        "zotomatic", _definition(str(worker), str(output_root)),
        devices={"LOCAL_SIM"}, repo_root=tmp_path,
    )
    source = tmp_path / "input.zot"
    source.write_bytes(b"private payload")
    task = as_fleet_task(
        prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)]), spec,
    )

    snapshots: list[Path] = []
    real_snapshot = executor.snapshot_prepared_input

    def recording_snapshot(item: dict) -> Path:
        snapshot = real_snapshot(item)
        snapshots.append(snapshot)
        return snapshot

    stage = str(tmp_path / "remote-stage")
    removed: list[str] = []

    class FailingTransport:
        def remote_temp_dir(self, _prefix: str) -> str:
            return stage

        def native_join(self, *parts: str) -> str:
            return str(Path(*parts))

        def ensure_remote_dir(self, _path: str) -> None:
            raise TransportError("synthetic stage failure")

        def remove_remote_tree(self, path: str) -> None:
            removed.append(path)

    monkeypatch.setattr(executor, "snapshot_prepared_input", recording_snapshot)
    monkeypatch.setattr(executor, "make_transport", lambda _device: FailingTransport())

    result = executor.run_batch(
        "LOCAL_SIM", [task], _config(tmp_path), state_root=tmp_path / "state",
    )

    assert result["ok"] is False and result["error"].startswith("stage failed:")
    assert snapshots and all(not snapshot.exists() for snapshot in snapshots)
    assert removed == [stage]
