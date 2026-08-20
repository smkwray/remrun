"""One arbitrary configured workflow through every supported fleet boundary."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from remrun.config import RemrunConfig
from remrun.fleet import adapters, cli, dispatcher, placement, probes
from remrun.fleet.models import DeviceSnapshot
from remrun.fleet.prepared import as_fleet_task, prepare_task_job
from remrun.fleet.queue import FleetQueue
from remrun.fleet.resources import ResourceView
from remrun.fleet.task_contract import resolve_task_spec
from remrun.models import Device
from remrun.output import Reporter
from remrun.transport import ExecResult, ProbeResult


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
    def __init__(self) -> None:
        self.calls = []

    def exec(self, command, cwd, **kwargs):
        self.calls.append((command, cwd, kwargs))
        paths = json.loads(command[3])
        values = {
            "/present": True,
            "/missing": False,
            "/raises": None,
        }
        return ExecResult(0, json.dumps([values[path] for path in paths]), "")


def test_capability_probe_preserves_mixed_per_engine_outcomes() -> None:
    transport = _CapabilityTransport()
    device = Device.from_mapping("REMOTE", {
        "kind": "ssh-posix", "os": "posix", "address": "example.invalid",
        "project_root": "/work", "cache_root": "/cache",
    })
    status = probes._capability_engines(transport, [
        {"engine": "yes", "capability_paths": ["/present"]},
        {"engine": "no", "capability_paths": ["/missing"]},
        {"engine": "maybe", "capability_paths": ["/raises"]},
        {"engine": "unprobed", "capability_paths": []},
    ], device=device)
    assert status == {
        "yes": "present", "no": "absent", "maybe": "unknown", "unprobed": "unknown",
    }
    assert len(transport.calls) == 1
    assert transport.calls[0][2]["telemetry"] is False


def test_json_submit_enqueues_without_speculative_route_probe(
    tmp_path, monkeypatch, capsys,
) -> None:
    worker = tmp_path / "worker.py"
    worker.write_text("pass\n", encoding="utf-8")
    definition = _definition(worker, tmp_path / "outputs")
    config = _config(tmp_path, definition)
    spec = resolve_task_spec(
        "zotomatic", definition, devices=config.devices, repo_root=tmp_path,
    )
    source = tmp_path / "sample.zot"
    source.write_bytes(b"zot")
    prepared = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
    task = as_fleet_task(prepared, spec)
    state_root = tmp_path / "state"

    monkeypatch.setattr(cli, "load_config", lambda _repo_root=None: config)
    monkeypatch.setattr(cli, "default_state_root", lambda: state_root)
    monkeypatch.setattr(cli, "_prepare_configured", lambda _args, _config: (
        spec, [prepared], [task],
    ))

    def unexpected_preview(*_args, **_kwargs):
        raise AssertionError("queue-only JSON submission must not probe placement")

    monkeypatch.setattr(cli, "_route_preview", unexpected_preview)
    args = cli.build_parser().parse_args([
        "submit", "zotomatic", "--input", str(source), "--json",
    ])

    assert cli.cmd_submit(args, Reporter()) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["queued_total"] == 1
    assert payload["route_preview"] is False
    assert "device_busy" not in payload

    monkeypatch.setattr(
        cli, "_route_preview",
        lambda *_args, **_kwargs: {
            "device": "LOCAL_SIM", "device_busy": False,
            "active_on_device": 0, "estimated_finish_s": 1.0,
        },
    )
    preview_args = cli.build_parser().parse_args([
        "submit", "zotomatic", "--input", str(source), "--json", "--preview-route",
    ])
    assert cli.cmd_submit(preview_args, Reporter()) == 0
    preview_payload = json.loads(capsys.readouterr().out)
    assert preview_payload["route_preview"] is True
    assert preview_payload["device"] == "LOCAL_SIM"


def test_configured_controller_snapshot_is_local_and_never_uses_ssh(
    tmp_path, monkeypatch,
) -> None:
    worker = tmp_path / "worker.py"
    worker.write_text("pass\n", encoding="utf-8")
    local_windows = os.name == "nt"
    local_os = "windows" if local_windows else ("macos" if sys.platform == "darwin" else "linux")
    device = Device.from_mapping("SELF", {
        "role": "controller",
        "kind": "ssh-powershell" if local_windows else "ssh-posix",
        "os": local_os, "address_candidates": ["localhost"],
        "project_root": str(tmp_path), "cache_root": str(tmp_path / "cache"),
        "max_jobs": 2,
    })

    class NoSSH:
        def __getattr__(self, name):
            raise AssertionError(f"self snapshot attempted transport operation {name}")

    monkeypatch.setattr(
        "remrun.fleet.local_resources.local_view",
        lambda name="", timeout=20.0: ResourceView(
            name=name, reachable=True, detail="local controller", cpu_busy_pct=12.5,
            ram_free_mb=8192, ram_total_mb=16384, is_local=True,
        ),
    )
    snapshot = probes.build_snapshot(
        device, NoSSH(), {"pools": {"gpu": 1}}, active_jobs=1,
        pool_used={"gpu": 1},
        adapter_specs=[{"engine": "zot-engine", "capability_paths": [str(worker)]}],
    )

    assert snapshot.reachable is True
    assert snapshot.cpu_busy_pct == 12.5
    assert snapshot.ram_free_mb == 8192
    assert snapshot.active_jobs == 1
    assert snapshot.pool_free == {"gpu": 0}
    assert snapshot.engine_status == {"zot-engine": "present"}
    assert snapshot.detail == "local controller"


def test_controller_local_substitution_fails_closed_on_os_collision(
    tmp_path, monkeypatch,
) -> None:
    cross_windows = os.name != "nt"
    device = Device.from_mapping("SELF", {
        "role": "controller",
        "kind": "ssh-powershell" if cross_windows else "ssh-posix",
        "os": "windows" if cross_windows else "linux",
        "address_candidates": ["localhost"],
        "project_root": str(tmp_path), "cache_root": str(tmp_path / "cache"),
    })

    class RemoteEvidence:
        def probe(self):
            return ProbeResult(True, "remote", "remote evidence", device.os)

    monkeypatch.setattr(
        "remrun.fleet.local_resources.local_view",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cross-platform target must not use controller evidence")
        ),
    )
    monkeypatch.setattr(probes, "probe_target_resources", lambda *_args, **_kwargs: None)

    snapshot = probes.build_snapshot(device, RemoteEvidence(), {}, adapter_specs=[])

    assert snapshot.reachable is True
    assert snapshot.detail == "remote evidence"


def test_controller_local_substitution_requires_explicit_marker(
    tmp_path, monkeypatch,
) -> None:
    local_windows = os.name == "nt"
    local_os = "windows" if local_windows else ("macos" if sys.platform == "darwin" else "linux")
    device = Device.from_mapping("SELF", {
        "role": "runner",
        "kind": "ssh-powershell" if local_windows else "ssh-posix",
        "os": local_os, "address_candidates": ["localhost"],
        "project_root": str(tmp_path), "cache_root": str(tmp_path / "cache"),
    })

    class RemoteEvidence:
        def probe(self):
            return ProbeResult(True, "remote", "remote evidence", device.os)

    monkeypatch.setattr(
        "remrun.fleet.local_resources.local_view",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unmarked target must not use controller evidence")
        ),
    )
    monkeypatch.setattr(probes, "probe_target_resources", lambda *_args, **_kwargs: None)

    snapshot = probes.build_snapshot(device, RemoteEvidence(), {}, adapter_specs=[])

    assert snapshot.reachable is True
    assert snapshot.detail == "remote evidence"


def test_fixed_snapshot_probe_strips_memory_guard_before_transport_creation(
    tmp_path, monkeypatch,
) -> None:
    device = Device.from_mapping("REMOTE", {
        "kind": "ssh-posix", "os": "linux",
        "address_candidates": ["remote.invalid"],
        "project_root": str(tmp_path), "cache_root": str(tmp_path / "cache"),
        "memory_guard": {"schema": 3, "command_limit_fraction": 0.25},
    })
    seen = []

    class FixedProbeTransport:
        def probe(self):
            return ProbeResult(True, "remote.invalid", "fixed probe", "linux")

    def fake_make_transport(probe_device):
        seen.append(probe_device)
        return FixedProbeTransport()

    monkeypatch.setattr(probes, "make_transport", fake_make_transport)
    monkeypatch.setattr(probes, "probe_target_resources", lambda *_args, **_kwargs: None)

    snapshot = probes.build_snapshot(device, None, {}, adapter_specs=[])

    assert snapshot.reachable is True
    assert len(seen) == 1
    assert seen[0].memory_guard is None


def test_preview_route_requires_json_before_prepare_or_enqueue(monkeypatch) -> None:
    monkeypatch.setattr(
        cli, "load_config",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must fail first")),
    )
    args = cli.build_parser().parse_args(["submit", "zotomatic", "--preview-route"])

    try:
        cli.cmd_submit(args, Reporter())
    except ValueError as exc:
        assert str(exc) == "--preview-route requires --json"
    else:
        raise AssertionError("preview without JSON must be rejected")
