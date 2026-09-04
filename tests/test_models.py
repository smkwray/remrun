from __future__ import annotations

from pathlib import Path

import pytest

from remrun.config import load_config
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


def test_device_explicit_run_flag_defaults_false_and_is_closed_boolean() -> None:
    assert _device().allow_explicit_run is False
    assert _device(allow_explicit_run=True).allow_explicit_run is True

    with pytest.raises(ValueError, match="allow_explicit_run must be a boolean"):
        _device(allow_explicit_run="true")


def test_device_automatic_placement_defaults_true_and_is_closed_boolean() -> None:
    assert _device().automatic_placement is True
    assert _device(automatic_placement=False).automatic_placement is False

    with pytest.raises(ValueError, match="automatic_placement must be a boolean"):
        _device(automatic_placement="false")


def test_load_config_rejects_non_boolean_explicit_run_flag(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "defaults.toml").write_text("", encoding="utf-8")
    (config_dir / "devices.toml").write_text(
        "[devices.BOX]\nallow_explicit_run = 1\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="allow_explicit_run must be a boolean"):
        load_config(tmp_path)


def test_load_config_rejects_non_boolean_automatic_placement(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "defaults.toml").write_text("", encoding="utf-8")
    (config_dir / "devices.toml").write_text(
        "[devices.BOX]\nautomatic_placement = 1\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="automatic_placement must be a boolean"):
        load_config(tmp_path)


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
