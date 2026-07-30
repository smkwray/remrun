from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from remrun.config import RemrunConfig
from remrun.models import Device, ProjectContext, WorkloadSpec
from remrun.planner import make_run_plan


def _config(tmp_path: Path) -> RemrunConfig:
    device = Device.from_mapping(
        "BOX",
        {
            "kind": "ssh-posix",
            "os": "macos",
            "project_root": "/projects",
            "state_root": "/state",
        },
    )
    return RemrunConfig(
        repo_root=tmp_path,
        defaults={"transfer": {"mode": "safe"}},
        devices={"BOX": device},
        project_roots={"default": str(tmp_path)},
    )


def _patch_planning_dependencies(monkeypatch, tmp_path: Path, project_config: dict) -> None:
    project = ProjectContext(
        local_project_root=tmp_path / "project",
        project_id="project",
        relative_cwd=".",
        local_cwd=tmp_path / "project",
    )
    monkeypatch.setattr("remrun.planner.detect_project", lambda cwd, config: project)
    monkeypatch.setattr("remrun.planner.find_project_config", lambda root: None)
    monkeypatch.setattr("remrun.planner.load_project_config", lambda path: project_config)
    monkeypatch.setattr(
        "remrun.planner.resolve_write_scope",
        lambda config, name: SimpleNamespace(name=None, paths=[]),
    )


def test_workload_selection_precedes_runtime_targeting(monkeypatch, tmp_path: Path) -> None:
    events: list[tuple[str, object]] = []
    selected = WorkloadSpec(
        name="demo.build",
        adapter_id="demo.policy",
        adapter_version=1,
        work_unit="case",
        require_envelope=True,
        require_receipt=True,
        protocol=1,
    )
    _patch_planning_dependencies(monkeypatch, tmp_path, {})

    def select(project_config, requested):
        events.append(("workload", requested))
        return selected

    def target(devices, target_name, **kwargs):
        events.append(("target", target_name))
        return [devices["BOX"]]

    monkeypatch.setattr("remrun.planner.select_workload", select)
    monkeypatch.setattr("remrun.planner.order_devices", target)

    plan = make_run_plan(
        cwd=tmp_path,
        config=_config(tmp_path),
        target_name="BOX",
        command=["python", "run.py"],
        requested_workload="demo.build",
    )

    assert events == [("workload", "demo.build"), ("target", "BOX")]
    assert plan.workload == WorkloadSpec(
        name="demo.build",
        adapter_id="demo.policy",
        adapter_version=1,
        work_unit="case",
        require_envelope=True,
        require_receipt=True,
    )


def test_unselected_workload_preserves_legacy_plan(monkeypatch, tmp_path: Path) -> None:
    _patch_planning_dependencies(monkeypatch, tmp_path, {"resources": {"default": {}}})
    monkeypatch.setattr(
        "remrun.planner.order_devices",
        lambda devices, target_name, **kwargs: [devices["BOX"]],
    )

    plan = make_run_plan(
        cwd=tmp_path,
        config=_config(tmp_path),
        target_name="BOX",
        command=["python", "run.py"],
    )

    assert plan.workload is None
    assert "workload" not in plan.as_dict()


def test_bench_can_suppress_project_default_workload(monkeypatch, tmp_path: Path) -> None:
    project_config = {
        "resources": {
            "schema": 1,
            "default_workload": "demo.build",
            "workloads": {
                "demo.build": {
                    "protocol": 1,
                    "adapter_id": "demo.policy",
                    "adapter_version": 1,
                    "work_unit": "case",
                }
            },
        }
    }
    _patch_planning_dependencies(monkeypatch, tmp_path, project_config)
    monkeypatch.setattr(
        "remrun.planner.order_devices",
        lambda devices, target_name, **kwargs: [devices["BOX"]],
    )

    ordinary = make_run_plan(
        cwd=tmp_path,
        config=_config(tmp_path),
        target_name="BOX",
        command=["python", "run.py"],
    )
    bench_remote_leg = make_run_plan(
        cwd=tmp_path,
        config=_config(tmp_path),
        target_name="BOX",
        command=["python", "run.py"],
        allow_default_workload=False,
    )

    assert ordinary.workload is not None
    assert ordinary.workload.name == "demo.build"
    assert bench_remote_leg.workload is None
