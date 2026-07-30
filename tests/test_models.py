from __future__ import annotations

from pathlib import Path

from remrun.models import Device, ProjectContext, RunPlan, WorkloadSpec


def _device(**overrides: object) -> Device:
    data: dict[str, object] = {
        "kind": "ssh-posix",
        "os": "macos",
        "address_candidates": ["box.example"],
        "project_root": "/projects",
        "state_root": "/state",
        "tags": ["compute"],
        "max_jobs": 2,
    }
    data.update(overrides)
    return Device.from_mapping("BOX", data)


def _plan(*, workload: WorkloadSpec | None = None) -> RunPlan:
    project = ProjectContext(
        local_project_root=Path("/local/project"),
        project_id="project",
        relative_cwd="analysis",
        local_cwd=Path("/local/project/analysis"),
    )
    return RunPlan(
        target=_device(),
        project=project,
        command=["python", "run.py"],
        transfer_mode="safe",
        project_config_path=Path("/local/project/do/remrun/remrun.toml"),
        excludes=[".git/**"],
        hash_below_bytes=1024,
        write_scope="outputs",
        write_scope_paths=["results/**"],
        workload=workload,
    )


def test_device_preserves_resource_policy_for_opt_in_validation() -> None:
    raw_policy = {"schema": 1, "mode": "interactive", "unexpected": "preserved"}

    assert _device(resource_policy=raw_policy).resource_policy is raw_policy
    assert _device(resource_policy="malformed").resource_policy == "malformed"
    assert _device().resource_policy is None


def test_run_plan_legacy_serialization_is_exactly_unchanged_without_workload() -> None:
    result = _plan().as_dict()

    assert result == {
        "target": {
            "name": "BOX",
            "kind": "ssh-posix",
            "os": "macos",
            "address_candidates": ["box.example"],
            "project_root": "/projects",
            "state_root": "/state",
            "tags": ["compute"],
            "max_jobs": 2,
        },
        "project": {
            "local_project_root": "/local/project",
            "project_id": "project",
            "relative_cwd": "analysis",
            "local_cwd": "/local/project/analysis",
        },
        "command": ["python", "run.py"],
        "transfer_mode": "safe",
        "project_config_path": "/local/project/do/remrun/remrun.toml",
        "excludes": [".git/**"],
        "hash_below_bytes": 1024,
        "write_scope": "outputs",
        "write_scope_paths": ["results/**"],
    }
    assert "workload" not in result


def test_run_plan_serializes_selected_workload_only_when_present() -> None:
    workload = WorkloadSpec(
        name="demo.build",
        adapter_id="demo.policy",
        adapter_version=2,
        work_unit="case",
        require_envelope=True,
        require_receipt=True,
    )

    assert _plan(workload=workload).as_dict()["workload"] == {
        "name": "demo.build",
        "adapter_id": "demo.policy",
        "adapter_version": 2,
        "work_unit": "case",
        "require_envelope": True,
        "require_receipt": True,
        "protocol": 1,
    }
