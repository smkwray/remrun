from __future__ import annotations

import json
import time
from types import SimpleNamespace

from remrun.fleet import cli, jobs
from remrun.fleet.jobs_render import IncrementalTable, render_table
from remrun.models import Device


def device(name: str = "DEV") -> Device:
    return Device.from_mapping(name, {
        "kind": "ssh-posix", "os": "linux", "address_candidates": ["host"],
        "project_root": "/work", "state_root": "/state", "cache_root": "/cache",
    })


def job_row(**over):
    row = {
        "schema": 1,
        "job_id": "j1",
        "project": "proj",
        "source_controller": "CTRL",
        "target": "DEV",
        "phase": "command",
        "state": "RUNNING",
        "observation_status": "ok",
        "age_seconds": 65.0,
        "started_at_unix_ns": 1,
        "command": {"label": "python", "sha256": "a" * 64},
        "cpu": {"current_pct_one_logical_cpu": 125.0, "normalized_host_pct": 12.5},
        "threads": {"current_os_threads": 4},
        "memory": {"current_bytes": 1_572_864, "peak_bytes": None},
    }
    row.update(over)
    return row


def test_probe_device_preserves_exact_metrics(monkeypatch):
    payload = {
        "schema": 1, "status": "ok", "jobs": [job_row()], "errors": [],
        "coverage": {"scope": "registered_jobs_only", "mixed_version": True},
        "semantics": {"cpu": "100 percent equals one logical CPU"},
    }
    fake = SimpleNamespace(query_observed_jobs=lambda **_: payload)
    monkeypatch.setattr(jobs, "make_transport", lambda _: fake)
    view = jobs.probe_device(device())
    assert view.reachable and view.status == "ok"
    assert view.jobs[0]["cpu"]["current_pct_one_logical_cpu"] == 125.0
    out = jobs.to_dict(view)
    assert out["semantics"] == payload["semantics"]
    assert out["coverage"]["mixed_version"] is True


def test_probe_device_local_reads_registry_without_transport(monkeypatch, tmp_path):
    payload = {
        "schema": 1, "status": "ok", "jobs": [job_row(target="LOCAL")], "errors": [],
        "coverage": {"scope": "registered_jobs_only", "mixed_version": False},
    }
    seen = []
    monkeypatch.setattr(jobs, "default_state_root", lambda: tmp_path)
    monkeypatch.setattr(
        jobs._job_observer,
        "_query",
        lambda root, interval: seen.append((root, interval)) or payload,
    )
    monkeypatch.setattr(
        jobs,
        "make_transport",
        lambda _: (_ for _ in ()).throw(AssertionError("local query must not use SSH")),
    )

    view = jobs.probe_device(device("LOCAL"), sample_interval=0.05, local=True)

    assert view.reachable and view.status == "ok"
    assert seen == [(tmp_path, 0.05)]


def test_probe_failure_is_unknown_not_empty_success(monkeypatch):
    class Broken:
        def query_observed_jobs(self, **_):
            raise RuntimeError("offline")

    monkeypatch.setattr(jobs, "make_transport", lambda _: Broken())
    view = jobs.probe_device(device())
    assert not view.reachable
    assert view.status == "unknown"
    assert view.jobs == []
    assert view.errors[0]["kind"] == "target_query_failed"


def test_invalid_row_makes_target_partial(monkeypatch):
    payload = {"schema": 1, "status": "ok", "jobs": [{"schema": 9}], "errors": []}
    monkeypatch.setattr(
        jobs, "make_transport", lambda _: SimpleNamespace(query_observed_jobs=lambda **_: payload)
    )
    view = jobs.probe_device(device())
    assert view.status == "partial"
    assert view.jobs == []
    assert view.errors[0]["kind"] == "invalid_job_row"


def test_probe_fleet_returns_config_order_but_events_complete_as_ready(monkeypatch):
    devices = [device("SLOW"), device("FAST")]

    def fake(dev, **_):
        if dev.name == "SLOW":
            time.sleep(0.05)
        return jobs.TargetJobsView(dev.name, True, "ok")

    monkeypatch.setattr(jobs, "probe_device", fake)
    done = []
    views = jobs.probe_fleet(
        devices, on_event=lambda kind, name, view: done.append(name) if kind == "done" else None
    )
    assert [v.name for v in views] == ["SLOW", "FAST"]
    assert done == ["FAST", "SLOW"]


def test_probe_fleet_marks_named_controller_as_local(monkeypatch):
    devices = [device("LOCAL"), device("REMOTE")]
    seen = {}

    def fake(dev, **kwargs):
        seen[dev.name] = kwargs["local"]
        return jobs.TargetJobsView(dev.name, True, "ok")

    monkeypatch.setattr(jobs, "probe_device", fake)
    jobs.probe_fleet(devices, local_names={"local"})

    assert seen == {"LOCAL": True, "REMOTE": False}


def test_render_distinguishes_job_idle_unknown_and_unsupported():
    views = [
        jobs.TargetJobsView("A", True, "ok", jobs=[job_row(target="A")]),
        jobs.TargetJobsView("B", True, "ok"),
        jobs.TargetJobsView("C", False, "unknown", detail="offline"),
        jobs.TargetJobsView("D", True, "unsupported", detail="old helper"),
    ]
    text = render_table(views)
    assert "PROJECT" in text and "FROM" in text and "COMMAND" in text
    assert "125%" in text and "1.5M" in text and "1m" in text
    assert "IDLE" in text and "UNKNOWN" in text and "UNSUPPORTED" in text
    assert "no registered jobs" not in text


def test_incremental_header_precedes_completion_rows():
    table = IncrementalTable(["DEV"])
    assert table.header().splitlines()[0].startswith("PROJECT")
    rows = table.rows(jobs.TargetJobsView("DEV", True, "ok", jobs=[job_row()]))
    assert len(rows) == 1 and "proj" in rows[0]


def test_render_caps_long_labels_and_preserves_full_json_elsewhere():
    row = job_row(
        project="project-name-that-is-too-long",
        source_controller="controller-name-that-is-too-long",
        command={"label": "command-label-that-is-much-too-long", "sha256": "b" * 64},
    )
    text = render_table([jobs.TargetJobsView("DEVICE-NAME", True, "ok", jobs=[row])])

    assert "project-name-th~" in text
    assert "control~" in text
    assert "DEVIC~" in text
    assert "command-label-that-is~" in text
    assert max(map(len, text.splitlines())) <= 103


def test_cmd_jobs_json_is_deterministic_and_exit_success(monkeypatch, capsys):
    config = SimpleNamespace(devices={"B": device("B"), "A": device("A")})
    monkeypatch.setattr(cli, "load_config", lambda: config)
    views = [
        jobs.TargetJobsView("B", True, "ok", payload={"schema": 1, "status": "ok", "jobs": [], "errors": []}),
        jobs.TargetJobsView("A", False, "unknown", payload={"schema": 1, "status": "unknown", "jobs": [], "errors": []}),
    ]
    monkeypatch.setattr(jobs, "probe_fleet", lambda *a, **k: views)
    args = SimpleNamespace(device=None, enabled_only=False, json=True, no_progress=False,
                           timeout=1.0, sample_interval=0.05)
    assert cli.cmd_jobs(args, SimpleNamespace(event=lambda *a, **k: None)) == 0
    parsed = json.loads(capsys.readouterr().out)
    assert [item["target"] for item in parsed["targets"]] == ["B", "A"]


def test_cmd_jobs_queries_matching_controller_locally(monkeypatch, capsys):
    config = SimpleNamespace(devices={"CTRL": device("CTRL"), "OTHER": device("OTHER")})
    monkeypatch.setattr(cli, "load_config", lambda: config)
    monkeypatch.setattr(cli.platform, "node", lambda: "ctrl.local")
    seen = {}

    def fake_probe(*_args, **kwargs):
        seen["local_names"] = kwargs["local_names"]
        return [
            jobs.TargetJobsView("CTRL", True, "ok"),
            jobs.TargetJobsView("OTHER", True, "ok"),
        ]

    monkeypatch.setattr(jobs, "probe_fleet", fake_probe)
    args = SimpleNamespace(device=None, enabled_only=False, json=False, no_progress=True,
                           timeout=1.0, sample_interval=0.05)

    assert cli.cmd_jobs(args, SimpleNamespace(event=lambda *a, **k: None)) == 0
    assert seen["local_names"] == {"ctrl"}
    assert "IDLE" in capsys.readouterr().out


def test_cmd_jobs_fails_when_every_target_unknown_or_unsupported(monkeypatch, capsys):
    config = SimpleNamespace(devices={"A": device("A")})
    monkeypatch.setattr(cli, "load_config", lambda: config)
    monkeypatch.setattr(
        jobs,
        "probe_fleet",
        lambda *a, **k: [jobs.TargetJobsView("A", False, "unknown", detail="offline")],
    )
    args = SimpleNamespace(device=None, enabled_only=False, json=False, no_progress=True,
                           timeout=1.0, sample_interval=0.05)
    assert cli.cmd_jobs(args, SimpleNamespace(event=lambda *a, **k: None)) == 1
    assert "UNKNOWN" in capsys.readouterr().out


def test_parser_exposes_jobs_options():
    args = cli.build_parser().parse_args([
        "jobs", "--device", "DEV", "--sample-interval", "0.1", "--no-progress"
    ])
    assert args.fleet_command == "jobs"
    assert args.device == ["DEV"]
    assert args.sample_interval == 0.1
    assert args.no_progress is True


def test_executor_registers_one_observation_for_a_batch(tmp_path, monkeypatch):
    monkeypatch.setenv("REMRUN_FLEET_JOBS_OBSERVE", "1")
    from remrun.config import RemrunConfig
    from remrun.fleet import executor
    from remrun.fleet.models import FleetTask
    from remrun.transport import LocalSimTransport

    dev = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim", "os": "posix", "address_candidates": ["localhost"],
        "project_root": str(tmp_path / "remote"), "state_root": str(tmp_path / "devstate"),
        "cache_root": str(tmp_path / "cache"),
    })
    cfg = RemrunConfig(
        repo_root=tmp_path, defaults={}, devices={"LOCAL_SIM": dev}, project_roots={}, offload={},
        fleet_adapters={"cmd": {"LOCAL_SIM": {"engine": "cmd", "output_root": "",
                                                  "pool": "cpu", "memory_kind": "cpu"}}},
    )
    captured = []
    original = LocalSimTransport.exec_observed

    def record(self, command, cwd, *, observation, **kwargs):
        captured.append(observation)
        return original(self, command, cwd, observation=observation, **kwargs)

    monkeypatch.setattr(LocalSimTransport, "exec_observed", record)
    task = FleetTask(task_type="cmd", force_device="LOCAL_SIM",
                     options={"argv": ["python", "-c", "pass"]})
    result = executor.run_batch(
        "LOCAL_SIM", [task, task], cfg, state_root=tmp_path / "state",
        job_ids=["a", "b"], observation_id="batch-7",
    )
    assert result["ok"] is True
    assert len(captured) == 1
    assert captured[0].job_id == "batch-7"
    assert captured[0].project == "@fleet"
    assert captured[0].member_count == 2


def test_buffered_jobs_are_grouped_globally_by_project():
    views = [
        jobs.TargetJobsView("B", True, "ok", jobs=[job_row(project="zeta", target="B")]),
        jobs.TargetJobsView("A", True, "ok", jobs=[job_row(project="alpha", target="A")]),
    ]
    flat = jobs.flatten_jobs(views)
    assert [item["project"] for item in flat] == ["alpha", "zeta"]
    text = render_table(views)
    assert text.index("alpha") < text.index("zeta")


def test_executor_observation_is_dormant_by_default(tmp_path, monkeypatch):
    from remrun.config import RemrunConfig
    from remrun.fleet import executor
    from remrun.fleet.models import FleetTask
    from remrun.transport import LocalSimTransport

    monkeypatch.delenv("REMRUN_FLEET_JOBS_OBSERVE", raising=False)
    dev = Device.from_mapping(
        "LOCAL_SIM",
        {
            "kind": "local-sim",
            "os": "posix",
            "address_candidates": ["localhost"],
            "project_root": str(tmp_path / "remote"),
            "state_root": str(tmp_path / "devstate"),
            "cache_root": str(tmp_path / "cache"),
        },
    )
    cfg = RemrunConfig(
        repo_root=tmp_path,
        defaults={},
        devices={"LOCAL_SIM": dev},
        project_roots={},
        offload={},
        fleet_adapters={
            "cmd": {
                "LOCAL_SIM": {
                    "engine": "cmd",
                    "output_root": "",
                    "pool": "cpu",
                    "memory_kind": "cpu",
                }
            }
        },
    )

    def unexpected(*_args, **_kwargs):
        raise AssertionError("default-off fleet worker must not enter observation code")

    monkeypatch.setattr(LocalSimTransport, "exec_observed", unexpected)
    monkeypatch.setattr(executor.JobObservation, "for_command", classmethod(unexpected))
    task = FleetTask(
        task_type="cmd",
        force_device="LOCAL_SIM",
        options={"argv": ["python", "-c", "pass"]},
    )
    result = executor.run_batch(
        "LOCAL_SIM",
        [task],
        cfg,
        state_root=tmp_path / "state",
        job_ids=["a"],
        observation_id="batch-default-off",
    )
    assert result["ok"] is True
