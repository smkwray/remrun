from __future__ import annotations

from pathlib import Path

from remrun.models import Device, ProjectContext
from remrun.runenv import resolve_run_env
from remrun.transport import LocalSimTransport, SSHPosixTransport


def dev(name="MACBOX", os_="macos", **over) -> Device:
    data = {"kind": "ssh-posix", "os": os_, "project_root": "~/projects"}
    data.update(over)
    return Device.from_mapping(name, data)


def proj(pid="remrun-test") -> ProjectContext:
    return ProjectContext(
        local_project_root=Path("/x") / pid, project_id=pid, relative_cwd=".",
        local_cwd=Path("/x") / pid,
    )


def test_device_env_and_path():
    d = dev(env={"OMP_NUM_THREADS": "4"}, path=["~/extra/bin"])
    r = resolve_run_env(device=d, project=proj(), project_config={})
    assert r.env["OMP_NUM_THREADS"] == "4"
    assert r.path_prepend == ["~/extra/bin"]
    assert r.venv is None


def test_project_env_overrides_device():
    d = dev(env={"X": "device"})
    cfg = {"env": {"X": "project", "Y": "y"}}
    r = resolve_run_env(device=d, project=proj(), project_config=cfg)
    assert r.env["X"] == "project"
    assert r.env["Y"] == "y"


def test_use_venv_posix_project_local_default():
    # Default layout: project-local .venv beside the project on the device.
    d = dev()  # project_root="~/projects"
    cfg = {"run": {"use_venv": True}}
    r = resolve_run_env(device=d, project=proj("remrun-test"), project_config=cfg)
    assert r.venv == "~/projects/remrun-test/.venv"
    assert r.path_prepend[0] == "~/projects/remrun-test/.venv/bin"
    assert r.env["VIRTUAL_ENV"] == "~/projects/remrun-test/.venv"


def test_use_venv_windows_uses_scripts_and_backslash():
    d = dev(name="WINBOX", os_="windows", project_root="C:\\Users\\you\\projects")
    cfg = {"run": {"use_venv": True}}
    r = resolve_run_env(device=d, project=proj("foo"), project_config=cfg)
    assert r.venv == "C:\\Users\\you\\projects\\foo\\.venv"
    assert r.path_prepend[0] == "C:\\Users\\you\\projects\\foo\\.venv\\Scripts"


def test_nested_project_local_uses_full_project_path():
    d = dev()
    cfg = {"run": {"use_venv": True}}
    r = resolve_run_env(device=d, project=proj("client/foo"), project_config=cfg)
    assert r.venv == "~/projects/client/foo/.venv"


def test_external_layout_uses_venv_root():
    d = dev(venv_root="~/venvs")
    cfg = {"run": {"use_venv": True, "venv_layout": "external"}}
    r = resolve_run_env(device=d, project=proj("remrun-test"), project_config=cfg)
    assert r.venv == "~/venvs/remrun-test"
    assert r.path_prepend[0] == "~/venvs/remrun-test/bin"


def test_external_layout_nested_uses_leaf_name():
    d = dev(venv_root="~/venvs")
    cfg = {"run": {"use_venv": True, "venv_layout": "external"}}
    r = resolve_run_env(device=d, project=proj("client/foo"), project_config=cfg)
    assert r.venv == "~/venvs/foo"


def test_explicit_per_device_venv_wins():
    d = dev(name="MACBOX", venv_root="~/venvs")
    cfg = {"run": {"use_venv": True, "venv": {"MACBOX": "~/special/env"}}}
    r = resolve_run_env(device=d, project=proj("remrun-test"), project_config=cfg)
    assert r.venv == "~/special/env"
    assert r.path_prepend[0] == "~/special/env/bin"


def test_external_venv_name_override():
    d = dev(venv_root="~/venvs")
    cfg = {"run": {"use_venv": True, "venv_layout": "external", "venv_name": "shared"}}
    r = resolve_run_env(device=d, project=proj("remrun-test"), project_config=cfg)
    assert r.venv == "~/venvs/shared"


# --- application by transports ------------------------------------------------

def test_localsim_exec_applies_env_and_path(tmp_path: Path):
    d = Device.from_mapping("LOCAL_SIM", {"kind": "local-sim", "project_root": str(tmp_path)})
    t = LocalSimTransport(d)
    cwd = tmp_path / "p"
    cwd.mkdir()
    fakebin = tmp_path / "fakebin"
    fakebin.mkdir()
    res = t.exec(
        ["python", "-c", "import os;print(os.environ['MYVAR']);print(os.environ['PATH'])"],
        cwd=str(cwd), env={"MYVAR": "hello"}, path_prepend=[str(fakebin)],
    )
    assert res.exit_code == 0
    lines = res.stdout.splitlines()
    assert lines[0] == "hello"
    assert str(fakebin) in lines[1]


def test_ssh_posix_exec_exports_env_and_path(monkeypatch):
    import subprocess

    t = SSHPosixTransport(dev(login_shell=False))
    t._address = "h"
    t._remote_home = "/Users/alice"
    calls = {}

    def fake_run(argv, input_bytes=None, timeout=None):
        calls["script"] = argv[-1]
        return subprocess.CompletedProcess(argv, 0, b"", b"")

    monkeypatch.setattr(t, "_run", fake_run)
    t.exec(["python", "x.py"], cwd="/p",
           env={"VIRTUAL_ENV": "~/venvs/foo"}, path_prepend=["~/venvs/foo/bin"])
    script = calls["script"]
    # ~ expanded to remote home, venv bin prepended before $PATH.
    assert "export VIRTUAL_ENV=/Users/alice/venvs/foo" in script
    assert 'export PATH=/Users/alice/venvs/foo/bin:"$PATH"' in script
    assert script.endswith("cd /p && python x.py")
