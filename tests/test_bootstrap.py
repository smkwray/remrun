from __future__ import annotations

import json
import shutil
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from remrun.bootstrap import (
    BootstrapConfigError,
    execute_bootstrap,
    inspect_bootstrap,
    parse_bootstrap,
    render_steps,
    validate_managed_command,
)
from remrun import bootstrap as bootstrap_module
from remrun.models import Device, ProjectContext
from remrun.runenv import resolve_run_env
from remrun.transport import ExecResult, LocalSimTransport


def _project(tmp_path: Path) -> ProjectContext:
    root = tmp_path / "project"
    root.mkdir()
    return ProjectContext(root, "project", ".", root)


def _device(tmp_path: Path) -> Device:
    return Device.from_mapping(
        "LOCAL_SIM",
        {
            "kind": "local-sim",
            "project_root": str(tmp_path / "remote"),
            "state_root": str(tmp_path / "target-state"),
            "remote_python": sys.executable,
            "venv_root": str(tmp_path / "venvs"),
        },
    )


def _plan(project: ProjectContext):
    lock = project.local_project_root / "uv.lock"
    if not lock.exists():
        lock.write_text("lock-v1", encoding="utf-8")
    return parse_bootstrap(
        {
            "run": {"use_venv": True, "venv_layout": "external", "bootstrap": {
                "schema": 1,
                "steps": [
                    ["{python}", "-c", "import sys; from pathlib import Path; p=Path(sys.argv[1]); (p / 'bin').mkdir(parents=True, exist_ok=True); entry=p / 'bin' / 'python'; entry.exists() or entry.symlink_to(sys.executable); Path('booted').write_text('yes')", "{venv}"],
                    ["{python}", "-c", "assert Path if False else True"],
                ],
                "lock_inputs": ["uv.lock"],
            }}
        },
        project_root=project.local_project_root,
    )


def test_parse_fingerprint_covers_declaration_and_lock_bytes(tmp_path: Path):
    project = _project(tmp_path)
    first = _plan(project)
    assert first is not None and first.status == "bootstrap-needed"
    (project.local_project_root / "uv.lock").write_text("lock-v2", encoding="utf-8")
    second = _plan(project)
    assert second is not None
    assert second.fingerprint != first.fingerprint
    changed = parse_bootstrap(
        {
            "run": {"use_venv": True, "venv_layout": "external", "bootstrap": {
                "schema": 1,
                "argv": [sys.executable, "-c", "print('changed')"],
                "lock_inputs": ["uv.lock"],
            }}
        },
        project_root=project.local_project_root,
    )
    assert changed is not None and changed.fingerprint != second.fingerprint


def test_parse_rejects_shell_strings_and_unsafe_lock_paths(tmp_path: Path):
    project = _project(tmp_path)
    (project.local_project_root / "uv.lock").write_text("lock", encoding="utf-8")
    shell = parse_bootstrap(
        {"run": {"use_venv": True, "venv_layout": "external", "bootstrap": {"schema": 1, "argv": ["sh -c 'touch pwned'"], "lock_inputs": ["uv.lock"]}}},
        project_root=project.local_project_root,
    )
    assert shell is not None and shell.status == "bootstrap-needed"
    unsafe = parse_bootstrap(
        {"run": {"use_venv": True, "venv_layout": "external", "bootstrap": {"schema": 1, "steps": [["python"]], "lock_inputs": ["../uv.lock"]}}},
        project_root=project.local_project_root,
    )
    assert unsafe is not None and unsafe.status == "unsupported"
    assert "inside the project" in unsafe.detail

    old_location = parse_bootstrap(
        {"bootstrap": {"schema": 1, "argv": ["python"], "lock_inputs": ["uv.lock"]}},
        project_root=project.local_project_root,
    )
    assert old_location is not None and old_location.status == "unsupported"
    unknown = parse_bootstrap(
        {"run": {"use_venv": True, "venv_layout": "external", "bootstrap": {
            "schema": 1, "argv": ["python"], "lock_inputs": ["uv.lock"], "shell": "no",
        }}},
        project_root=project.local_project_root,
    )
    assert unknown is not None and unknown.status == "unsupported"
    assert "unknown" in unknown.detail
    boolean_schema = parse_bootstrap(
        {"run": {"use_venv": True, "venv_layout": "external", "bootstrap": {
            "schema": True,
            "argv": ["python"],
            "lock_inputs": ["uv.lock"],
        }}},
        project_root=project.local_project_root,
    )
    assert boolean_schema is not None and boolean_schema.status == "unsupported"


def test_target_bootstrap_is_locked_idempotent_and_receipted(tmp_path: Path):
    project = _project(tmp_path)
    plan = _plan(project)
    assert plan is not None
    device = Device.from_mapping(
        "LOCAL_SIM",
        {
            "kind": "local-sim",
            "project_root": str(tmp_path / "remote"),
            "state_root": str(tmp_path / "target-state"),
            "remote_python": sys.executable,
            "venv_root": "~/venvs",
        },
    )
    transport = LocalSimTransport(device)
    remote_root = Path(transport.remote_project_path(project))
    remote_root.mkdir(parents=True)
    (remote_root / "uv.lock").write_text("lock-v1", encoding="utf-8")
    runenv = resolve_run_env(
        device=device,
        project=project,
        project_config={"run": {
            "use_venv": True,
            "venv_layout": "external",
            "bootstrap": {
                "schema": 1,
                "steps": [
                    ["{python}", "-c", "import sys; from pathlib import Path; p=Path(sys.argv[1]); (p / 'bin').mkdir(parents=True, exist_ok=True); entry=p / 'bin' / 'python'; entry.exists() or entry.symlink_to(sys.executable); Path('booted').write_text('yes')", "{venv}"],
                    ["{python}", "-c", "assert Path if False else True"],
                ],
                "lock_inputs": ["uv.lock"],
            },
        }},
    )

    first = execute_bootstrap(
        transport, device=device, project=project, plan=plan,
        remote_root=str(remote_root), runenv=runenv,
    )
    assert first is not None and first.status == "bootstrapped"
    assert (remote_root / "booted").read_text(encoding="utf-8") == "yes"
    assert Path(first.receipt_path).is_file()

    second = execute_bootstrap(
        transport, device=device, project=project, plan=plan,
        remote_root=str(remote_root), runenv=runenv,
    )
    assert second is not None and second.status == "ready"
    receipt = Path(second.receipt_path)
    receipt_data = json.loads(receipt.read_text(encoding="utf-8"))
    receipt_data["status"] = "running"
    receipt.write_text(json.dumps(receipt_data), encoding="utf-8")
    retried = execute_bootstrap(
        transport, device=device, project=project, plan=plan,
        remote_root=str(remote_root), runenv=runenv,
    )
    assert retried is not None and retried.status == "bootstrapped"
    (remote_root / "uv.lock").write_text("tampered", encoding="utf-8")
    repaired = execute_bootstrap(
        transport, device=device, project=project, plan=plan,
        remote_root=str(remote_root), runenv=runenv,
    )
    assert repaired is not None and repaired.status == "unsupported"
    (remote_root / "uv.lock").write_text("lock-v1", encoding="utf-8")
    inspected = inspect_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )
    assert inspected is not None and inspected.status == "ready"
    assert not Path(first.receipt_path).is_relative_to(remote_root)


def test_receipt_is_bound_to_rendered_target_environment(tmp_path: Path):
    project = _project(tmp_path)
    (project.local_project_root / "uv.lock").write_text("lock", encoding="utf-8")
    plan = parse_bootstrap(
        {"run": {"use_venv": True, "venv_layout": "external", "bootstrap": {
            "schema": 1,
            "argv": [
                "{python}",
                "-c",
                "import sys; from pathlib import Path; p=Path(sys.argv[1]); p.mkdir(parents=True, exist_ok=True); (p / 'bin').mkdir(exist_ok=True); entry=p / 'bin' / 'python'; entry.exists() or entry.symlink_to(sys.executable); Path('context').write_text(sys.argv[1])",
                "{venv}",
            ],
            "lock_inputs": ["uv.lock"],
        }}},
        project_root=project.local_project_root,
    )
    assert plan is not None
    device = _device(tmp_path)
    transport = LocalSimTransport(device)
    remote_root = Path(transport.remote_project_path(project))
    remote_root.mkdir(parents=True)
    (remote_root / "uv.lock").write_text("lock", encoding="utf-8")
    first_env = resolve_run_env(
        device=device,
        project=project,
        project_config={"run": {
            "use_venv": True,
            "venv_layout": "external",
            "bootstrap": {
                "schema": 1,
                "argv": [
                    "{python}",
                    "-c",
                    "import sys; from pathlib import Path; p=Path(sys.argv[1]); p.mkdir(parents=True, exist_ok=True); (p / 'bin').mkdir(exist_ok=True); entry=p / 'bin' / 'python'; entry.exists() or entry.symlink_to(sys.executable); Path('context').write_text(sys.argv[1])",
                    "{venv}",
                ],
                "lock_inputs": ["uv.lock"],
            },
        }},
    )
    first = execute_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=first_env,
    )
    assert first is not None and first.status == "bootstrapped"
    # A different explicit environment must not reuse the first target receipt.
    second_env = resolve_run_env(
        device=device,
        project=project,
        project_config={"run": {
            "use_venv": True,
            "venv_layout": "external",
            "venv": {"LOCAL_SIM": str(tmp_path / "venvs" / "other-venv")},
            "bootstrap": {
                "schema": 1,
                "argv": ["{python}", "-c", "print('ok')"],
                "lock_inputs": ["uv.lock"],
            },
        }},
    )
    inspected = inspect_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=second_env,
    )
    assert inspected is not None and inspected.status == "bootstrap-needed"


def test_home_relative_external_venv_expands_before_direct_bootstrap_argv(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = _project(tmp_path)
    (project.local_project_root / "uv.lock").write_text("lock", encoding="utf-8")
    plan = parse_bootstrap(
        {"run": {"use_venv": True, "venv_layout": "external", "bootstrap": {
            "schema": 1,
            "argv": [
                "{python}",
                "-c",
                (
                    "import sys; from pathlib import Path; "
                    "p=Path(sys.argv[1]); p.parent.mkdir(parents=True, exist_ok=True); "
                    "(p.parent / 'bin').mkdir(parents=True, exist_ok=True); "
                    "entry=p.parent / 'bin' / 'python'; "
                    "entry.exists() or entry.symlink_to(sys.executable); "
                    "p.write_text('outside')"
                ),
                "{venv}/bootstrap-marker",
            ],
            "lock_inputs": ["uv.lock"],
        }}},
        project_root=project.local_project_root,
    )
    assert plan is not None
    device = Device.from_mapping(
        "LOCAL_SIM",
        {
            "kind": "local-sim",
            "project_root": str(tmp_path / "remote"),
            "state_root": str(tmp_path / "target-state"),
            "remote_python": sys.executable,
            "venv_root": "~/venvs",
        },
    )
    transport = LocalSimTransport(device)
    target_home = tmp_path / "target-home"
    monkeypatch.setattr(
        transport,
        "expand_remote",
        lambda path: str(target_home / path[2:]) if path.startswith("~/") else path,
    )
    remote_root = Path(transport.remote_project_path(project))
    remote_root.mkdir(parents=True)
    (remote_root / "uv.lock").write_text("lock", encoding="utf-8")
    runenv = resolve_run_env(
        device=device,
        project=project,
        project_config={
            "run": {
                "use_venv": True,
                "venv_layout": "external",
                "venv_name": "sample-project",
                "bootstrap": {
                    "schema": 1,
                    "argv": ["{python}", "-c", "print('ok')"],
                    "lock_inputs": ["uv.lock"],
                },
            }
        },
    )

    rendered = render_steps(
        plan,
        device=device,
        project=project,
        runenv=runenv,
        remote_root=str(remote_root),
        state_root=str(tmp_path / "target-state"),
        transport=transport,
    )
    expected_venv = target_home / "venvs" / "sample-project"
    assert rendered[0][-1] == str(expected_venv / "bootstrap-marker")
    assert not any("~" in token for step in rendered for token in step)

    result = execute_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )
    assert result is not None and result.status == "bootstrapped"
    assert (expected_venv / "bootstrap-marker").read_text(encoding="utf-8") == "outside"
    assert not (remote_root / "~").exists()


def test_bootstrap_rejects_unexpanded_home_relative_venv(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = _project(tmp_path)
    (project.local_project_root / "uv.lock").write_text("lock", encoding="utf-8")
    plan = parse_bootstrap(
        {"run": {"use_venv": True, "venv_layout": "external", "bootstrap": {
            "schema": 1,
            "steps": [["{venv_python}", "-m", "pip", "--version"]],
            "lock_inputs": ["uv.lock"],
        }}},
        project_root=project.local_project_root,
    )
    assert plan is not None
    device = Device.from_mapping(
        "LOCAL_SIM",
        {
            "kind": "local-sim",
            "project_root": str(tmp_path / "remote"),
            "state_root": str(tmp_path / "target-state"),
            "remote_python": sys.executable,
            "venv_root": "~/venvs",
        },
    )
    transport = LocalSimTransport(device)
    monkeypatch.setattr(transport, "expand_remote", lambda path: path)
    runenv = resolve_run_env(
        device=device,
        project=project,
        project_config={"run": {
            "use_venv": True,
            "venv_layout": "external",
            "venv": {"LOCAL_SIM": "~/venvs/sample-project"},
            "bootstrap": {
                "schema": 1,
                "steps": [["{venv_python}", "-m", "pip", "--version"]],
                "lock_inputs": ["uv.lock"],
            },
        }},
    )
    with pytest.raises(
        BootstrapConfigError,
            match="target environment root was not expanded",
    ):
        render_steps(
            plan,
            device=device,
            project=project,
            runenv=runenv,
            remote_root=str(tmp_path / "remote"),
            state_root=str(tmp_path / "target-state"),
            transport=transport,
        )


def test_render_steps_requires_explicit_virtualenv_placeholder(tmp_path: Path):
    project = _project(tmp_path)
    (project.local_project_root / "uv.lock").write_text("lock", encoding="utf-8")
    plan = parse_bootstrap(
        {"run": {"use_venv": True, "venv_layout": "external", "bootstrap": {"schema": 1, "steps": [["{venv_python}", "-m", "pip"],], "lock_inputs": ["uv.lock"]}}},
        project_root=project.local_project_root,
    )
    assert plan is not None
    device = _device(tmp_path)
    env = resolve_run_env(device=device, project=project, project_config={})
    try:
        render_steps(
            plan, device=device, project=project, runenv=env,
            remote_root="/remote/project", state_root="/state",
            transport=LocalSimTransport(device),
        )
    except BootstrapConfigError as exc:
        assert "configured external virtualenv" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("missing venv should not render a bootstrap step")


def test_bootstrap_rejects_a_step_that_changes_declared_input(tmp_path: Path):
    project = _project(tmp_path)
    (project.local_project_root / "uv.lock").write_text("lock", encoding="utf-8")
    plan = parse_bootstrap(
        {"run": {"use_venv": True, "venv_layout": "external", "bootstrap": {
            "schema": 1,
            "argv": [
                "{python}",
                "-c",
                "from pathlib import Path; Path('uv.lock').write_text('changed')",
            ],
            "lock_inputs": ["uv.lock"],
        }}},
        project_root=project.local_project_root,
    )
    assert plan is not None
    device = _device(tmp_path)
    transport = LocalSimTransport(device)
    remote_root = Path(transport.remote_project_path(project))
    remote_root.mkdir(parents=True)
    (remote_root / "uv.lock").write_text("lock", encoding="utf-8")
    runenv = resolve_run_env(
        device=device,
        project=project,
        project_config={"run": {
            "use_venv": True,
            "venv_layout": "external",
            "bootstrap": {
                "schema": 1,
                "argv": [
                    "{python}",
                    "-c",
                    "import sys; from pathlib import Path; Path('context').write_text(sys.argv[1])",
                    "{venv}",
                ],
                "lock_inputs": ["uv.lock"],
            },
        }},
    )

    result = execute_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )

    assert result is not None and result.status == "failed"
    receipt = json.loads(Path(result.receipt_path).read_text(encoding="utf-8"))
    assert receipt["status"] == "failed"
    assert "changed a declared lock input" in receipt["detail"]


def _valid_bootstrap_config() -> dict:
    return {
        "run": {
            "use_venv": True,
            "venv_layout": "external",
            "bootstrap": {
                "schema": 1,
                "argv": ["{python}", "-c", "print('ok')"],
                "lock_inputs": ["uv.lock"],
            },
        }
    }


@pytest.mark.parametrize(
    ("run_overrides", "expected"),
    [
        ({}, "use_venv = true"),
        ({"use_venv": True}, "venv_layout = 'external'"),
        ({"use_venv": True, "venv_layout": "local"}, "venv_layout = 'external'"),
        ({"use_venv": True, "venv_layout": "typo"}, "venv_layout = 'external'"),
        (
            {"use_venv": True, "venv_layout": "external", "venv_name": "../escape"},
            "safe path component",
        ),
    ],
)
def test_parse_bootstrap_requires_external_managed_environment(
    tmp_path: Path, run_overrides: dict, expected: str
):
    project = _project(tmp_path)
    (project.local_project_root / "uv.lock").write_text("lock", encoding="utf-8")
    config = _valid_bootstrap_config()
    if "use_venv" not in run_overrides:
        config["run"].pop("use_venv")
    if "venv_layout" not in run_overrides:
        config["run"].pop("venv_layout")
    config["run"].update(run_overrides)
    plan = parse_bootstrap(config, project_root=project.local_project_root)
    assert plan is not None and plan.status == "unsupported"
    assert expected in plan.detail


@pytest.mark.parametrize(
    ("device_root", "run_overrides", "expected"),
    [
        ("", {}, "device.venv_root"),
        ("/tmp/venvs", {"venv_name": "../escape"}, "safe path component"),
        ("/tmp/venvs", {"venv_name": "nested/name"}, "safe path component"),
        ("/tmp/venvs", {"venv": {"LOCAL_SIM": "relative-env"}}, "absolute"),
        ("/tmp/venvs", {"venv": {"LOCAL_SIM": "/tmp/other-env"}}, "strictly under"),
    ],
)
def test_resolve_bootstrap_environment_fails_closed_before_mutation(
    tmp_path: Path, device_root: str, run_overrides: dict, expected: str
):
    project = _project(tmp_path)
    device = Device.from_mapping(
        "LOCAL_SIM",
        {
            "kind": "local-sim",
            "project_root": str(tmp_path / "remote"),
            "state_root": str(tmp_path / "target-state"),
            "remote_python": sys.executable,
            "venv_root": device_root,
        },
    )
    config = _valid_bootstrap_config()
    config["run"].update(run_overrides)
    with pytest.raises(ValueError, match=expected):
        resolve_run_env(device=device, project=project, project_config=config)
    assert not (tmp_path / "remote").exists()


def test_bootstrap_readiness_requires_live_environment_marker_and_interpreter(
    tmp_path: Path,
):
    project = _project(tmp_path)
    plan = _plan(project)
    assert plan is not None
    device = _device(tmp_path)
    transport = LocalSimTransport(device)
    remote_root = Path(transport.remote_project_path(project))
    remote_root.mkdir(parents=True)
    (remote_root / "uv.lock").write_text("lock-v1", encoding="utf-8")
    config = _valid_bootstrap_config()
    runenv = resolve_run_env(device=device, project=project, project_config=config)
    result = execute_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )
    assert result is not None and result.status == "bootstrapped"
    marker = Path(runenv.venv) / ".remrun-bootstrap-v1.json"
    interpreter = Path(runenv.venv) / "bin" / "python"
    marker.unlink()
    inspected = inspect_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )
    assert inspected is not None and inspected.status == "bootstrap-needed"
    result = execute_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )
    assert result is not None and result.status == "bootstrapped"
    interpreter.unlink()
    inspected = inspect_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )
    assert inspected is not None and inspected.status == "bootstrap-needed"
    result = execute_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )
    assert result is not None and result.status == "bootstrapped"
    shutil.rmtree(Path(runenv.venv))
    inspected = inspect_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )
    assert inspected is not None and inspected.status == "bootstrap-needed"
    result = execute_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )
    assert result is not None and result.status == "bootstrapped"


def test_target_helper_rejects_symlink_escape_from_environment_root(tmp_path: Path):
    project = _project(tmp_path)
    plan = _plan(project)
    assert plan is not None
    device = _device(tmp_path)
    transport = LocalSimTransport(device)
    remote_root = Path(transport.remote_project_path(project))
    remote_root.mkdir(parents=True)
    (remote_root / "uv.lock").write_text("lock-v1", encoding="utf-8")
    venv_root = Path(device.venv_root)
    venv_root.mkdir()
    escaped = tmp_path / "escaped"
    escaped.mkdir()
    (venv_root / project.project_id).symlink_to(escaped, target_is_directory=True)
    runenv = resolve_run_env(
        device=device,
        project=project,
        project_config=_valid_bootstrap_config(),
    )

    result = execute_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )

    assert result is not None and result.status == "unsupported"
    assert "outside its configured root" in result.detail
    assert not (escaped / "bin").exists()
    assert not (remote_root / "booted").exists()


def test_managed_command_cannot_fall_back_to_system_path(tmp_path: Path):
    project = _project(tmp_path)
    device = _device(tmp_path)
    transport = LocalSimTransport(device)
    runenv = resolve_run_env(
        device=device,
        project=project,
        project_config=_valid_bootstrap_config(),
    )
    from remrun.bootstrap import validate_managed_command

    with pytest.raises(BootstrapConfigError, match="system PATH fallback"):
        validate_managed_command(
            transport, device=device, runenv=runenv, command=["python"]
        )
    bindir = Path(runenv.path_prepend[0])
    bindir.mkdir(parents=True)
    (bindir / "python").write_text("", encoding="utf-8")
    with pytest.raises(BootstrapConfigError, match="not executable"):
        validate_managed_command(
            transport, device=device, runenv=runenv, command=["python"]
        )
    (bindir / "python").chmod(0o700)
    assert validate_managed_command(
        transport, device=device, runenv=runenv, command=["python", "-V"]
    )[0] == str((bindir / "python").resolve())


def test_managed_directory_cannot_satisfy_bare_command_resolution(tmp_path: Path):
    project = _project(tmp_path)
    device = _device(tmp_path)
    transport = LocalSimTransport(device)
    runenv = resolve_run_env(
        device=device,
        project=project,
        project_config=_valid_bootstrap_config(),
    )
    bindir = Path(runenv.path_prepend[0])
    bindir.mkdir(parents=True)
    (bindir / "python").mkdir()

    with pytest.raises(BootstrapConfigError, match="not executable"):
        validate_managed_command(
            transport, device=device, runenv=runenv, command=["python"]
        )


def test_controller_rejects_post_bootstrap_environment_symlink_escape(tmp_path: Path):
    """A completed receipt cannot bless an environment moved outside its root."""
    project = _project(tmp_path)
    plan = _plan(project)
    assert plan is not None
    device = _device(tmp_path)
    transport = LocalSimTransport(device)
    remote_root = Path(transport.remote_project_path(project))
    remote_root.mkdir(parents=True)
    (remote_root / "uv.lock").write_text("lock-v1", encoding="utf-8")
    runenv = resolve_run_env(
        device=device, project=project, project_config=_valid_bootstrap_config()
    )
    first = execute_bootstrap(
        transport, device=device, project=project, plan=plan,
        remote_root=str(remote_root), runenv=runenv,
    )
    assert first is not None and first.status == "bootstrapped"
    managed = Path(runenv.venv)
    escaped = tmp_path / "escaped-environment"
    managed.rename(escaped)
    managed.symlink_to(escaped, target_is_directory=True)

    inspected = inspect_bootstrap(
        transport, device=device, project=project, plan=plan,
        remote_root=str(remote_root), runenv=runenv,
    )
    assert inspected is not None and inspected.status == "unsupported"
    assert "outside" in inspected.detail or "escapes" in inspected.detail


def test_readiness_rejects_nonexecutable_expected_interpreter(tmp_path: Path):
    project = _project(tmp_path)
    plan = _plan(project)
    assert plan is not None
    device = _device(tmp_path)
    transport = LocalSimTransport(device)
    remote_root = Path(transport.remote_project_path(project))
    remote_root.mkdir(parents=True)
    (remote_root / "uv.lock").write_text("lock-v1", encoding="utf-8")
    runenv = resolve_run_env(
        device=device, project=project, project_config=_valid_bootstrap_config()
    )
    result = execute_bootstrap(
        transport, device=device, project=project, plan=plan,
        remote_root=str(remote_root), runenv=runenv,
    )
    assert result is not None and result.status == "bootstrapped"
    interpreter = Path(runenv.venv) / "bin" / "python"
    interpreter.unlink()
    interpreter.write_text("not executable", encoding="utf-8")
    interpreter.chmod(0o644)

    inspected = inspect_bootstrap(
        transport, device=device, project=project, plan=plan,
        remote_root=str(remote_root), runenv=runenv,
    )
    assert inspected is not None and inspected.status == "bootstrap-needed"
    assert "not executable" in inspected.detail


def test_managed_command_dispatches_absolute_environment_entrypoint(tmp_path: Path):
    project = _project(tmp_path)
    device = _device(tmp_path)
    transport = LocalSimTransport(device)
    runenv = resolve_run_env(
        device=device, project=project, project_config=_valid_bootstrap_config()
    )
    bindir = Path(runenv.path_prepend[0])
    bindir.mkdir(parents=True)
    interpreter = bindir / "python"
    interpreter.symlink_to(sys.executable)
    dispatch = validate_managed_command(
        transport, device=device, runenv=runenv,
        command=["python", "-c", "print('ok')"],
    )
    assert dispatch[0] == str(interpreter.absolute())
    assert Path(dispatch[0]).is_absolute()


def test_removed_managed_entrypoint_fails_without_path_fallback(tmp_path: Path):
    project = _project(tmp_path)
    device = _device(tmp_path)
    transport = LocalSimTransport(device)
    runenv = resolve_run_env(
        device=device, project=project, project_config=_valid_bootstrap_config()
    )
    bindir = Path(runenv.path_prepend[0])
    bindir.mkdir(parents=True)
    interpreter = bindir / "python"
    interpreter.symlink_to(sys.executable)
    dispatch = validate_managed_command(
        transport, device=device, runenv=runenv, command=["python", "-V"]
    )
    interpreter.unlink()
    with pytest.raises((OSError, RuntimeError)):
        transport.exec(dispatch, cwd=str(project.local_project_root), path_prepend=runenv.path_prepend)


def test_standard_environment_interpreter_symlink_remains_usable(tmp_path: Path):
    """A normal venv may link its entrypoint to the target's installed Python."""
    project = _project(tmp_path)
    plan = _plan(project)
    assert plan is not None
    device = _device(tmp_path)
    transport = LocalSimTransport(device)
    remote_root = Path(transport.remote_project_path(project))
    remote_root.mkdir(parents=True)
    (remote_root / "uv.lock").write_text("lock-v1", encoding="utf-8")
    runenv = resolve_run_env(
        device=device, project=project, project_config=_valid_bootstrap_config()
    )
    result = execute_bootstrap(
        transport, device=device, project=project, plan=plan,
        remote_root=str(remote_root), runenv=runenv,
    )
    assert result is not None and result.status == "bootstrapped"
    interpreter = Path(runenv.venv) / "bin" / "python"
    interpreter.unlink()
    interpreter.symlink_to(sys.executable)
    inspected = inspect_bootstrap(
        transport, device=device, project=project, plan=plan,
        remote_root=str(remote_root), runenv=runenv,
    )
    assert inspected is not None and inspected.status == "ready"


def test_bootstrap_helper_never_uses_managed_python_path_entry(tmp_path: Path):
    """A forged managed ``python3`` cannot counterfeit inspect or setup."""
    project = _project(tmp_path)
    plan = _plan(project)
    assert plan is not None
    forged_called = tmp_path / "forged-helper-called"
    device_path = tmp_path / "device-bin"
    device = replace(
        _device(tmp_path),
        remote_python="python3",
        path=[str(device_path)],
        env={"FORGED_SENTINEL": str(forged_called)},
    )
    transport = LocalSimTransport(device)
    remote_root = Path(transport.remote_project_path(project))
    remote_root.mkdir(parents=True)
    (remote_root / "uv.lock").write_text("lock-v1", encoding="utf-8")
    runenv = resolve_run_env(
        device=device,
        project=project,
        project_config=_valid_bootstrap_config(),
    )
    first = execute_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )
    assert first is not None and first.status == "bootstrapped"

    forged = Path(runenv.path_prepend[0]) / "python3"
    forged.write_text(
        "#!/bin/sh\n"
        "printf forged > \"$FORGED_SENTINEL\"\n"
        "printf '%s' '{\"schema\":\"remrun.bootstrap\",\"version\":1,\"status\":\"ready\"}'\n",
        encoding="utf-8",
    )
    forged.chmod(0o700)
    assert runenv.path_prepend == [str(forged.parent), str(device_path)]
    Path(runenv.venv, ".remrun-bootstrap-v1.json").unlink()

    seen: dict[str, object] = {}
    original_control = transport.exec_control

    def record_control(*args, **kwargs):  # noqa: ANN002, ANN003
        seen["path_prepend"] = kwargs.get("path_prepend")
        return original_control(*args, **kwargs)

    transport.exec_control = record_control
    inspected = inspect_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=str(remote_root),
        runenv=runenv,
    )
    assert inspected is not None and inspected.status == "bootstrap-needed"
    assert "marker" in inspected.detail
    assert seen["path_prepend"] == [str(device_path)]
    assert not forged_called.exists()

    result = execute_bootstrap(
        transport,
        device=device,
        project=project,
        plan=inspected,
        remote_root=str(remote_root),
        runenv=runenv,
    )
    assert result is not None and result.status == "bootstrapped"
    assert Path(runenv.venv, ".remrun-bootstrap-v1.json").is_file()
    assert not forged_called.exists()


def test_windows_bootstrap_helper_keeps_device_path_but_skips_scripts(
    tmp_path: Path,
):
    """Windows helper probes must not resolve ``python`` from Scripts first."""
    project = _project(tmp_path)
    (project.local_project_root / "uv.lock").write_text("lock-v1", encoding="utf-8")
    config = _valid_bootstrap_config()
    plan = parse_bootstrap(config, project_root=project.local_project_root)
    assert plan is not None
    device = Device.from_mapping(
        "WIN",
        {
            "kind": "local-sim",
            "os": "windows",
            "project_root": r"C:\remote",
            "state_root": r"C:\state",
            "cache_root": r"C:\cache",
            "remote_python": "python",
            "path": [r"C:\device-bin"],
            "venv_root": r"C:\venvs",
        },
    )

    class ProbeTransport(LocalSimTransport):
        def expand_remote(self, value: str) -> str:
            return value

        def native_join(self, *parts: str) -> str:
            return "\\".join(str(part).rstrip("\\/") for part in parts if part)

        def remote_path_exists(self, _path: str) -> bool:
            return True

        def hash_file(self, _path: str) -> str:
            return bootstrap_module.sha256_file(
                Path(bootstrap_module.__file__).with_name("_bootstrap.py")
            )

        def exec_control(self, _command, cwd, **kwargs):  # noqa: ANN001, ANN003
            del cwd
            self.path_seen = kwargs.get("path_prepend")
            return ExecResult(
                1,
                json.dumps({"status": "bootstrap-needed", "detail": "marker missing"}),
                "",
            )

    transport = ProbeTransport(device)
    runenv = resolve_run_env(
        device=device, project=project, project_config=config
    )
    inspected = inspect_bootstrap(
        transport,
        device=device,
        project=project,
        plan=plan,
        remote_root=r"C:\remote\project",
        runenv=runenv,
    )
    assert inspected is not None and inspected.status == "bootstrap-needed"
    assert transport.path_seen == [r"C:\device-bin"]
