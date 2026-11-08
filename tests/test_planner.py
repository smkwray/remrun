from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

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


def test_transfer_mode_is_one_validated_value_set(monkeypatch, tmp_path: Path) -> None:
    """[transfer] mode used to be echoed and never obeyed. Now that it decides whether
    files move, a value remrun does not implement must stop the run instead of quietly
    running as something else, and the per-run flag must beat the default."""
    _patch_planning_dependencies(monkeypatch, tmp_path, {})
    monkeypatch.setattr(
        "remrun.planner.order_devices",
        lambda devices, target_name, **kwargs: [devices["BOX"]],
    )

    def plan(mode: object = "safe", **kwargs):
        config = _config(tmp_path)
        config.defaults["transfer"]["mode"] = mode
        return make_run_plan(
            cwd=tmp_path, config=config, target_name="BOX", command=["true"], **kwargs
        )

    assert plan().transfer_mode == "safe"
    assert plan("none").transfer_mode == "none"
    assert plan("none", transfer_mode="safe").transfer_mode == "safe"
    assert plan(transfer_mode="none").as_dict()["verify"] == []
    for bad in ("isolated", "", "SAFE", 1, None):
        with pytest.raises(ValueError, match=r"\[transfer\] mode"):
            plan(bad)
    with pytest.raises(ValueError, match="--transfer"):
        plan(transfer_mode="isolated")

    checked = plan(transfer_mode="none", verify=["src/model.py", "src\\model.py", "data.csv"])
    assert checked.verify == ("src/model.py", "data.csv")
    assert checked.as_dict()["verify"] == ["src/model.py", "data.csv"]
    for bad_path in ("../escape.py", "/etc/passwd", "src/", "src/*.py", "./model.py", "C:x", ""):
        with pytest.raises(ValueError, match="--verify"):
            plan(transfer_mode="none", verify=[bad_path])
    with pytest.raises(ValueError, match="only to --transfer none"):
        plan(verify=["src/model.py"])
    with pytest.raises(ValueError, match="--scope"):
        plan(transfer_mode="none", scope_name="outputs")


def test_commit_paths_are_normalized_prefixes_and_overlap_by_prefix(
    monkeypatch, tmp_path: Path
) -> None:
    """Output leases are compared by prefix, so every path must be root-anchored,
    normalized and glob-free; a trailing / is the only way to name a directory."""
    from remrun.planner import paths_overlap

    _patch_planning_dependencies(monkeypatch, tmp_path, {})
    monkeypatch.setattr(
        "remrun.planner.order_devices",
        lambda devices, target_name, **kwargs: [devices["BOX"]],
    )

    def plan(**kwargs):
        return make_run_plan(
            cwd=tmp_path, config=_config(tmp_path), target_name="BOX", command=["true"],
            transfer_mode="commit", **kwargs,
        )

    made = plan(outputs=["out/", "out\\", "res/a.csv"], inputs=["data/in.csv"])
    assert made.outputs == ("out/", "res/a.csv") and made.inputs == ("data/in.csv",)
    assert made.as_dict()["outputs"] == ["out/", "res/a.csv"]
    assert made.as_dict()["inputs"] == ["data/in.csv"]
    with pytest.raises(ValueError, match="at least one --output"):
        plan()
    for bad in ("../x", "/abs", "out/*.csv", "./out/", "C:x", "", "/"):
        with pytest.raises(ValueError, match="--output"):
            plan(outputs=[bad])
    with pytest.raises(ValueError, match="--input"):
        plan(outputs=["out/"], inputs=["data/"])
    with pytest.raises(ValueError, match="only to --transfer commit"):
        make_run_plan(cwd=tmp_path, config=_config(tmp_path), target_name="BOX",
                      command=["true"], outputs=["out/"])
    assert paths_overlap("out/", "out/a.txt") and paths_overlap("out/a/", "out/")
    assert paths_overlap("out", "out/") and paths_overlap("a.txt", "a.txt")
    assert not paths_overlap("out/", "output/") and not paths_overlap("out/a", "out/ab")
    # Case- and normalization-insensitive, as APFS and NTFS are.
    assert paths_overlap("out/", "OUT/a.txt") and paths_overlap("Out", "oUT/")
    assert paths_overlap("caf\u00e9/", "cafe\u0301/a") and not paths_overlap("OUT/", "output/")

