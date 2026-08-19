"""Integration tests driving remrun.cli.main end-to-end via LOCAL_SIM."""
from __future__ import annotations

import json
import shutil
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path

import pytest

from remrun import __version__
from remrun.cli import (
    EXIT_CONFLICT, EXIT_GUARD, EXIT_INFRA, EXIT_INTERNAL, EXIT_OK, EXIT_TRANSFER,
    _best_remote_verdict,
    _workload_observation_from_run, main,
)
from remrun.models import WorkloadSpec
from remrun.profile import (
    LOCAL_DEVICE, WORKLOAD_PROFILES_KEY, command_key, device_profile, load_profiles,
)
from remrun.resource_context import ReceiptValidation
from remrun.state import ProjectLock
from remrun.transport import ExecResult, LocalSimTransport, TransportError


def test_cli_reports_package_version(capsys) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["--version"])

    assert exc_info.value.code == 0
    assert capsys.readouterr().out == f"remrun {__version__}\n"


def posix(p: Path) -> str:
    return str(p).replace("\\", "/")


def set_log_cap(env: dict, max_bytes: int) -> None:
    defaults = env["remrun_root"] / "config" / "defaults.toml"
    defaults.write_text(
        defaults.read_text(encoding="utf-8")
        + f'\n[logging]\nmax_full_log_mb = {max_bytes / (1024 * 1024)!r}\n',
        encoding="utf-8",
    )


def configure_workload(
    env: dict,
    *,
    require_envelope: bool = False,
    require_receipt: bool = False,
    default: bool = False,
    device_policy: bool = True,
) -> None:
    cfgdir = env["proj"] / "do" / "remrun"
    cfgdir.mkdir(parents=True, exist_ok=True)
    default_line = 'default_workload = "demo.work"\n' if default else ""
    (cfgdir / "remrun.toml").write_text(
        "[resources]\n"
        "schema = 1\n"
        f"{default_line}"
        '[resources.workloads."demo.work"]\n'
        "protocol = 1\n"
        'adapter_id = "demo.policy"\n'
        "adapter_version = 1\n"
        'work_unit = "case"\n'
        f"require_envelope = {str(require_envelope).lower()}\n"
        f"require_receipt = {str(require_receipt).lower()}\n",
        encoding="utf-8",
    )
    if device_policy:
        devices = env["remrun_root"] / "config" / "devices.toml"
        devices.write_text(
            devices.read_text(encoding="utf-8")
            + "\n[devices.LOCAL_SIM.resource_policy]\n"
            + "schema = 1\n"
            + 'mode = "unattended"\n'
            + "probe_timeout_sec = 5\n"
            + "cpu_reserve_cores = 0\n"
            + "cpu_max_fraction = 1.0\n"
            + "ram_reserve_mib = 0\n"
            + "ram_max_fraction = 1.0\n"
            + "gpu_busy_ceiling_pct = 100\n"
            + "vram_reserve_mib = 0\n"
            + "vram_max_fraction = 1.0\n"
            + "allow_static_fallback = false\n",
            encoding="utf-8",
        )


def configure_memory_guard(
    env: dict,
    *,
    ram_gb: int = 8,
    max_command_mib: int = 512,
    min_available_mib: int = 16,
) -> None:
    # Schema 2 derives exact rounded test thresholds from measured physical RAM.
    # ``ram_gb`` remains in the helper signature only to keep older callers clear
    # that declared placement RAM is not a protection authority.
    del ram_gb
    from remrun import _posix_telemetry as telemetry

    total_bytes, _available = telemetry._host_memory()
    command_fraction = ((max_command_mib + 0.25) * 1024**2) / total_bytes
    reserve_fraction = ((min_available_mib - 0.25) * 1024**2) / total_bytes
    assert 0 < command_fraction < 1
    assert 0 < reserve_fraction < 1
    assert command_fraction + reserve_fraction < 1
    devices = env["remrun_root"] / "config" / "devices.toml"
    devices.write_text(
        devices.read_text(encoding="utf-8")
        + "\n[devices.LOCAL_SIM.memory_guard]\n"
        + "schema = 3\n"
        + f"command_limit_fraction = {command_fraction!r}\n"
        + f"host_reserve_fraction = {reserve_fraction!r}\n",
        encoding="utf-8",
    )


def receipt_writing_command(output: str = "adapted") -> list[str]:
    script = (
        "import json,os,pathlib\n"
        "ctx=json.load(open(os.environ['REMRUN_RUN_CONTEXT'],encoding='utf-8'))\n"
        "receipt={"
        "'schema':'remrun.workload-receipt','version':1,"
        "'run_id':ctx['run_id'],'workload':ctx['workload']['name'],"
        "'adapter_id':ctx['workload']['adapter_id'],"
        "'adapter_version':ctx['workload']['adapter_version'],"
        "'status':'applied','evaluation':'accepted',"
        "'setting':{'workers':1},'constraints':{'process_cap':1},"
        "'work':{'unit':ctx['workload']['work_unit'],'count':1},"
        "'setting_fingerprint':'sha256:test','written_at':'2026-07-28T23:18:21Z'}\n"
        "dest=pathlib.Path(ctx['workload']['receipt']['path'])\n"
        "tmp=dest.with_suffix(dest.suffix+'.tmp')\n"
        "tmp.write_text(json.dumps(receipt),encoding='utf-8')\n"
        "tmp.replace(dest)\n"
        f"pathlib.Path('adapted.txt').write_text({output!r},encoding='utf-8')\n"
    )
    return ["python", "-c", script]


@pytest.fixture()
def env(tmp_path: Path, monkeypatch):
    remrun_root = tmp_path / "remrun"
    (remrun_root / "config").mkdir(parents=True)
    local_base = tmp_path / "local" / "proj"
    remote_base = tmp_path / "remote"
    state_root = tmp_path / "state"
    local_base.mkdir(parents=True)
    remote_base.mkdir(parents=True)

    (remrun_root / "config" / "defaults.toml").write_text(
        '[transfer]\n'
        'mode = "safe"\n'
        'global_exclude = ["node_modules/**", ".git/**"]\n'
        'hash_small_files_below_mb = 8\n'
    )
    (remrun_root / "config" / "devices.toml").write_text(
        '[project_roots]\n'
        f'default = "{posix(local_base)}"\n'
        f'macos = "{posix(local_base)}"\n'
        f'windows = "{posix(local_base)}"\n'
        '\n'
        '[devices.LOCAL_SIM]\n'
        'enabled = true\n'
        'role = "simulation"\n'
        'kind = "local-sim"\n'
        'os = "posix"\n'
        f'project_root = "{posix(remote_base)}"\n'
        f'state_root = "{posix(state_root)}"\n'
        f'cache_root = "{posix(tmp_path / "cache")}"\n'
    )

    monkeypatch.setenv("REMRUN_ROOT", str(remrun_root))
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(state_root))

    proj = local_base / "proj1"
    proj.mkdir()
    monkeypatch.chdir(proj)
    return {
        "proj": proj,
        "remote_proj": remote_base / "proj1",
        "state": state_root,
        "remrun_root": remrun_root,
    }


def test_run_happy_path_pulls_output(env, capsys):
    (env["proj"] / "input.txt").write_text("in")
    code = main(["run", "LOCAL_SIM", "--",
                 "python", "-c", "open('result.txt','w').write('ok')"])
    assert code == EXIT_OK
    # Output created remotely is pulled back to the local project path.
    assert (env["proj"] / "result.txt").read_text() == "ok"
    assert (env["remote_proj"] / "input.txt").read_text() == "in"
    assert not (env["proj"] / "do").exists()
    err = capsys.readouterr().err
    assert "preflight_progress completed=0 total=1 pulls=0 pushes=1" in err
    assert "preflight_progress completed=1 total=1 pulls=0 pushes=1" in err


def test_run_passes_through_nonzero_exit(env):
    code = main(["run", "LOCAL_SIM", "--", "python", "-c", "import sys; sys.exit(7)"])
    assert code == 7
    assert load_profiles(env["state"]) == {}


def test_ctrl_c_during_preflight_returns_130_and_terminalizes_not_started_receipt(
    env, monkeypatch, capsys
):
    (env["proj"] / "input.txt").write_text("push me", encoding="utf-8")

    def interrupt_push(*_args, **_kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(LocalSimTransport, "push_file", interrupt_push)
    try:
        code = main([
            "run", "LOCAL_SIM", "--", "python", "-c",
            "open('must-not-run.txt','w').write('ran')",
        ])
    except KeyboardInterrupt:
        pytest.fail("ordinary-run preflight leaked KeyboardInterrupt")

    assert code == 130
    assert not (env["remote_proj"] / "must-not-run.txt").exists()
    err = capsys.readouterr().err
    assert "cancelled" in err
    assert "KeyboardInterrupt" not in err
    summaries = list((env["state"] / "runs").glob("*/summary.json"))
    assert len(summaries) == 1
    summary = json.loads(summaries[0].read_text(encoding="utf-8"))
    assert summary["phase"] == "preflight"
    assert summary["completion_state"] == "cancelled"
    assert summary["command_started"] is False
    assert summary["terminal"] is True
    assert not list((env["state"] / "hazards" / "project").glob("*/unknown.json"))


def test_disappearing_file_during_preflight_hash_is_concise_retryable_failure(
    env, monkeypatch, capsys
):
    import remrun.manifest as manifest_mod

    transient = env["proj"] / "generated.tmp"
    transient.write_text("transient", encoding="utf-8")
    real_sha256 = manifest_mod.sha256_file
    injected = False

    def disappear_while_hashing(path: Path) -> str:
        nonlocal injected
        if Path(path) == transient and not injected:
            injected = True
            transient.unlink()
            raise FileNotFoundError(2, "generated file vanished", str(path))
        return real_sha256(path)

    monkeypatch.setattr(manifest_mod, "sha256_file", disappear_while_hashing)
    code = main([
        "run", "LOCAL_SIM", "--", "python", "-c",
        "open('must-not-run.txt','w').write('ran')",
    ])

    assert code == EXIT_TRANSFER
    assert not (env["remote_proj"] / "must-not-run.txt").exists()
    err = capsys.readouterr().err
    assert "transfer_error" in err
    assert "changed while hashing generated.tmp" in err
    assert "retry" in err
    assert "FileNotFoundError" not in err
    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["phase"] == "preflight"
    assert summary["completion_state"] == "not_started"
    assert summary["command_started"] is False
    assert summary["terminal"] is True


def test_conclusive_predispatch_rejection_never_installs_unknown_fence(
    env, monkeypatch, capsys
):
    import remrun.cli as cli_mod
    import remrun.transport as transport_mod

    rejection = getattr(transport_mod, "CommandNotStartedError", TransportError)
    real_make_transport = cli_mod.make_transport

    def make_rejecting_transport(device):
        transport = real_make_transport(device)

        def reject(_command, *, env=None, path_prepend=None):
            assert env == {}
            assert path_prepend == []
            raise rejection("unsupported top-level .cmd/.bat")

        monkeypatch.setattr(
            transport, "validate_command_context", reject, raising=False
        )
        monkeypatch.setattr(
            transport,
            "exec",
            lambda *_args, **_kwargs: pytest.fail("rejected argv reached dispatch"),
        )
        return transport

    monkeypatch.setattr(cli_mod, "make_transport", make_rejecting_transport)
    code = main(["run", "LOCAL_SIM", "--", "native_probe.cmd", "literal&arg"])

    assert code == EXIT_INFRA
    err = capsys.readouterr().err
    assert "command_rejected" in err
    assert "command_started" not in err
    assert "completion_unknown" not in err
    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["completion_state"] == "not_started"
    assert summary["command_started"] is False
    assert summary["terminal"] is True
    assert not list((env["state"] / "hazards" / "project").glob("*/unknown.json"))


def test_command_validation_transport_failure_is_conclusive_not_started(
    env, monkeypatch, capsys
):
    import remrun.cli as cli_mod

    real_make_transport = cli_mod.make_transport

    def make_failing_transport(device):
        transport = real_make_transport(device)

        def fail(_command, *, env=None, path_prepend=None):
            del env, path_prepend
            raise TransportError("injected command discovery disconnect")

        monkeypatch.setattr(
            transport, "validate_command_context", fail, raising=False
        )
        monkeypatch.setattr(
            transport,
            "exec",
            lambda *_args, **_kwargs: pytest.fail("failed validation reached dispatch"),
        )
        return transport

    monkeypatch.setattr(cli_mod, "make_transport", make_failing_transport)
    code = main(["run", "LOCAL_SIM", "--", "native_probe", "literal&arg"])

    assert code == EXIT_TRANSFER
    err = capsys.readouterr().err
    assert "transfer_error" in err
    assert "command_validation" in err
    assert "command_started" not in err
    assert "completion_unknown" not in err
    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["phase"] == "command_validation"
    assert summary["completion_state"] == "not_started"
    assert summary["command_started"] is False
    assert summary["terminal"] is True
    assert not list((env["state"] / "hazards" / "project").glob("*/unknown.json"))


def test_conclusive_remote_boundary_rejection_clears_fence_without_false_unknown(
    env, monkeypatch, capsys
):
    import remrun.cli as cli_mod
    import remrun.transport as transport_mod

    rejection = getattr(transport_mod, "CommandNotStartedError", TransportError)
    real_make_transport = cli_mod.make_transport

    def make_guarded_transport(device):
        transport = real_make_transport(device)
        monkeypatch.setattr(
            transport,
            "command_start_requires_confirmation",
            lambda: True,
            raising=False,
        )

        def reject_after_remote_resolution(*_args, **_kwargs):
            raise rejection("PATH command resolved to unsupported .cmd/.bat")

        monkeypatch.setattr(transport, "exec", reject_after_remote_resolution)
        return transport

    monkeypatch.setattr(cli_mod, "make_transport", make_guarded_transport)
    code = main(["run", "LOCAL_SIM", "--", "native_probe", "literal&arg"])

    assert code == EXIT_INFRA
    err = capsys.readouterr().err
    assert "command_dispatch" in err
    assert "command_started" not in err
    assert "command_rejected" in err
    assert "completion_unknown" not in err
    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["completion_state"] == "not_started"
    assert summary["command_started"] is False
    assert summary["terminal"] is True
    assert not list((env["state"] / "hazards" / "project").glob("*/unknown.json"))


def test_exec_transport_failure_records_unknown_completion_guidance(
    env, monkeypatch, capsys
):
    def disconnect_after_start(*_args, **_kwargs):
        raise TransportError("injected connection reset")

    real_release = ProjectLock.release

    def release_after_hazard(self):
        hazards = list((env["state"] / "hazards" / "project").glob("*/unknown.json"))
        assert len(hazards) == 1
        real_release(self)

    monkeypatch.setattr(LocalSimTransport, "exec", disconnect_after_start)
    monkeypatch.setattr(ProjectLock, "release", release_after_hazard)
    code = main(["run", "LOCAL_SIM", "--", "python", "-c", "print('maybe ran')"])

    assert code == EXIT_INFRA
    err = capsys.readouterr().err
    assert "completion_unknown" in err
    assert "do not retry" in err
    summaries = list((env["state"] / "runs").glob("*/summary.json"))
    assert len(summaries) == 1
    summary = json.loads(summaries[0].read_text(encoding="utf-8"))
    assert summary["phase"] == "exec"
    assert summary["completion_state"] == "unknown"
    assert summary["terminal"] is True
    assert "read-only process/artifact probe" in summary["guidance"]
    assert not list((env["state"] / "locks").glob("**/*.lock"))


def test_unknown_completion_hazard_blocks_clean_and_resolves_explicitly(
    env, monkeypatch, capsys
):
    tracked = env["proj"] / "tracked.txt"
    tracked.write_text("before")
    assert main(["run", "LOCAL_SIM", "--", "python", "-c", "print('seed')"]) == EXIT_OK
    capsys.readouterr()

    real_exec = LocalSimTransport.exec

    def disconnect_after_start(*_args, **_kwargs):
        raise TransportError("injected connection reset")

    monkeypatch.setattr(LocalSimTransport, "exec", disconnect_after_start)
    assert main(["run", "LOCAL_SIM", "--", "python", "-c", "print('maybe ran')"]) == EXIT_INFRA

    hazards = list((env["state"] / "hazards" / "project").glob("*/unknown.json"))
    assert len(hazards) == 1
    hazard = json.loads(hazards[0].read_text(encoding="utf-8"))
    assert hazard["version"] == 1
    assert hazard["project_id"] == "proj1"
    assert hazard["target"] == "LOCAL_SIM"
    unknown_run_id = hazard["run_id"]

    # A new invocation (the controller process may have restarted) must fail before
    # preflight or execution.  Another controller state root is intentionally not fenced.
    monkeypatch.setattr(LocalSimTransport, "exec", real_exec)
    assert main([
        "run", "LOCAL_SIM", "--", "python", "-c",
        "open('must-not-run.txt','w').write('unsafe')",
    ]) == EXIT_INTERNAL
    assert not (env["remote_proj"] / "must-not-run.txt").exists()
    err = capsys.readouterr().err
    assert "unknown_completion_hazard" in err
    assert "controller-local" in err

    local_bench_called = False

    def fail_if_local_bench_runs(*_args, **_kwargs):
        nonlocal local_bench_called
        local_bench_called = True
        raise AssertionError("bench local leg ran despite unknown-completion hazard")

    with monkeypatch.context() as bench_patch:
        bench_patch.setattr("remrun.cli.subprocess.run", fail_if_local_bench_runs)
        assert main([
            "bench", "LOCAL_SIM", "--", "python", "-c", "print('must-not-bench')",
        ]) == EXIT_INTERNAL
    assert not local_bench_called

    # Ordinary cleanup cannot erase the admission hazard or its resolution evidence.
    assert main(["clean", "--keep", "0"]) == EXIT_OK
    assert hazards[0].exists()
    summary_path = env["state"] / "runs" / unknown_run_id / "summary.json"
    assert summary_path.exists()

    # Resolution records only the operator's confirmed-ended action.  It does not
    # infer outputs or advance the last good baseline: a remote edit made while the
    # outcome was unknown must still be discovered by the next normal preflight.
    (env["remote_proj"] / "tracked.txt").write_text("remote-after-unknown")
    assert main(["resolve-unknown", unknown_run_id, "--confirmed-ended"]) == EXIT_OK
    assert not hazards[0].exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["unknown_resolution"]["action"] == "confirmed-ended"

    assert main(["run", "LOCAL_SIM", "--", "python", "-c", "print('safe-next')"]) == EXIT_OK
    assert tracked.read_text() == "remote-after-unknown"


def test_controller_loss_during_dispatch_leaves_receipt_and_unknown_hazard(
    env, monkeypatch
):
    class ControllerLost(BaseException):
        pass

    def vanish_during_dispatch(*_args, **_kwargs):
        raise ControllerLost("injected controller loss")

    monkeypatch.setattr(LocalSimTransport, "exec", vanish_during_dispatch)
    with pytest.raises(ControllerLost):
        main(["run", "LOCAL_SIM", "--", "python", "-c", "print('maybe')"])

    summaries = list((env["state"] / "runs").glob("*/summary.json"))
    assert len(summaries) == 1
    summary = json.loads(summaries[0].read_text(encoding="utf-8"))
    assert summary["phase"] == "exec"
    assert summary["completion_state"] == "unknown"
    assert summary["terminal"] is False
    hazards = list((env["state"] / "hazards" / "project").glob("*/unknown.json"))
    assert len(hazards) == 1


def test_finalization_exception_after_known_completion_writes_terminal_receipt(
    env, monkeypatch
):
    import remrun.cli as climod

    def fail_finalization(**_kwargs):
        raise RuntimeError("injected finalization failure")

    monkeypatch.setattr(climod, "postrun_pullback", fail_finalization)
    assert main(["run", "LOCAL_SIM", "--", "python", "-c", "print('done')"]) == EXIT_INTERNAL

    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["terminal"] is True
    assert summary["completion_state"] == "finalization_failed"
    assert summary["command_exit_code"] == 0
    assert summary["phase"] == "pullback"
    assert not list((env["state"] / "hazards" / "project").glob("*/unknown.json"))


def test_pullback_transport_failure_keeps_completed_command_conclusive(
    env, monkeypatch, capsys
):
    import remrun.cli as climod

    def fail_pullback(**_kwargs):
        raise TransportError("injected locked local temp")

    monkeypatch.setattr(climod, "postrun_pullback", fail_pullback)
    code = main([
        "run", "LOCAL_SIM", "--", "python", "-c",
        "open('ran-once.txt','a').write('x')",
    ])

    assert code == EXIT_TRANSFER
    assert (env["remote_proj"] / "ran-once.txt").read_text(encoding="utf-8") == "x"
    err = capsys.readouterr().err
    assert "transfer_error" in err
    assert "completion_unknown" not in err
    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["phase"] == "pullback"
    assert summary["completion_state"] == "finalization_failed"
    assert summary["command_started"] is True
    assert summary["command_exit_code"] == 0
    assert summary["terminal"] is True
    assert not list((env["state"] / "hazards" / "project").glob("*/unknown.json"))


def test_auto_resolves_target(env):
    # With only LOCAL_SIM configured, --auto resolves to it.
    code = main(["run", "--auto", "--", "python", "-c", "open('auto.txt','w').write('ok')"])
    assert code == EXIT_OK
    assert (env["proj"] / "auto.txt").read_text() == "ok"


def test_dry_run_does_not_execute(env):
    code = main(["run", "LOCAL_SIM", "--dry-run", "--",
                 "python", "-c", "open('nope.txt','w').write('x')"])
    assert code == EXIT_OK
    assert not (env["proj"] / "nope.txt").exists()
    assert not (env["remote_proj"]).exists() or not (env["remote_proj"] / "nope.txt").exists()


def test_no_workload_uses_exact_legacy_exec_path(env, monkeypatch):
    import remrun.cli as climod

    def forbidden_probe(*_args, **_kwargs):
        raise AssertionError("ordinary run must not probe resources")

    real_exec = LocalSimTransport.exec
    calls: list[tuple[list[str], dict]] = []

    def capture_exec(self, command, **kwargs):
        calls.append((command, kwargs))
        return real_exec(self, command, **kwargs)

    monkeypatch.setattr(climod, "probe_target_resources", forbidden_probe)
    monkeypatch.setattr(LocalSimTransport, "exec", capture_exec)
    command = ["python", "-c", "open('legacy.txt','w').write('ok')"]

    assert main(["run", "LOCAL_SIM", "--", *command]) == EXIT_OK
    assert calls == [
        (
            command,
            {
                "cwd": str(env["remote_proj"]),
                "env": {},
                "path_prepend": [],
                "telemetry": True,
                "on_stdout": calls[0][1]["on_stdout"],
            },
        )
    ]
    assert "REMRUN_RUN_CONTEXT" not in calls[0][1]["env"]
    run = next((env["state"] / "runs").iterdir())
    assert not list(run.glob("run-context*"))
    summary = json.loads((run / "summary.json").read_text(encoding="utf-8"))
    assert "workload" not in summary
    assert "workload" not in summary["plan"]


def test_unselected_malformed_resources_remain_inert(env, monkeypatch):
    cfgdir = env["proj"] / "do" / "remrun"
    cfgdir.mkdir(parents=True)
    (cfgdir / "remrun.toml").write_text(
        '[resources.default]\ncores = "not-an-integer"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "remrun.cli.probe_target_resources",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("unselected legacy config must remain inert")
        ),
    )
    assert main(["run", "LOCAL_SIM", "--", "python", "-c", "print('legacy')"]) == EXIT_OK


def test_workload_context_is_target_native_and_argv_is_unchanged(
    env, monkeypatch
):
    from remrun.resource_envelope import Metric
    from remrun.resource_probe import ResourceSnapshot

    configure_workload(env, require_receipt=True)

    def measured(value):
        return Metric(value, "measured", "test", "exact")

    monkeypatch.setattr(
        "remrun.cli.probe_target_resources",
        lambda *_a, **_k: ResourceSnapshot(
            status="ok",
            platform="test",
            machine="test",
            logical_cores=measured(8),
            effective_cores=measured(8),
            cpu_busy_pct=measured(25),
            cpu_sample_interval_ms=500,
            ram_total_bytes=measured(16 * 1024**3),
            ram_available_bytes=measured(8 * 1024**3),
            gpu_kind="none",
        ),
    )
    real_exec = LocalSimTransport.exec
    calls: list[tuple[list[str], dict]] = []

    def capture_exec(self, command, **kwargs):
        calls.append((command, kwargs))
        return real_exec(self, command, **kwargs)

    monkeypatch.setattr(LocalSimTransport, "exec", capture_exec)
    command = receipt_writing_command()
    assert main(["run", "LOCAL_SIM", "--workload", "demo.work", "--", *command]) == EXIT_OK

    assert calls[-1][0] == command
    context_path = calls[-1][1]["env"]["REMRUN_RUN_CONTEXT"]
    assert context_path.startswith(str(env["state"] / "runs"))
    assert not context_path.startswith(str(env["proj"]))
    run = next((env["state"] / "runs").iterdir())
    context = json.loads(
        (run / "run-context.controller.v1.json").read_text(encoding="utf-8")
    )
    assert context["workload"]["receipt"]["path"].endswith(
        "workload-receipt.v1.json"
    )
    assert context["resources"]["status"] == "ok"
    assert (env["proj"] / "adapted.txt").read_text() == "adapted"
    summary = json.loads((run / "summary.json").read_text(encoding="utf-8"))
    assert summary["receipt"]["status"] == "valid"
    assert summary["command"] == command
    assert summary["telemetry"]["memory"]["metric"] == "rss_sum_sampled"
    assert summary["workload_profile"]["status"] == "recorded"
    assert summary["workload_cleanup"] == {
        "context": "deleted",
        "receipt": "deleted",
    }
    workload_rows = load_profiles(env["state"])[WORKLOAD_PROFILES_KEY]["entries"]
    assert len(workload_rows) == 1
    row = next(iter(workload_rows.values()))
    assert row["setting"] == {"workers": 1}
    assert row["work_unit"] == "case"
    assert row["throughput"] > 0
    assert (run / "workload-receipt.controller.v1.json").exists()
    assert not Path(context_path).exists()


def test_workload_profiles_use_target_wall_not_controller_staging_time(
    env, monkeypatch
):
    from remrun.resource_probe import unavailable_snapshot

    configure_workload(env, require_receipt=True)
    monkeypatch.setattr(
        "remrun.cli.probe_target_resources",
        lambda *_a, **_k: unavailable_snapshot("unavailable", "test"),
    )
    real_exec = LocalSimTransport.exec

    def delayed_exec(self, *args, **kwargs):
        time.sleep(0.35)
        return real_exec(self, *args, **kwargs)

    monkeypatch.setattr(LocalSimTransport, "exec", delayed_exec)
    command = receipt_writing_command("timed")

    assert main([
        "run", "LOCAL_SIM", "--workload", "demo.work", "--", *command
    ]) == EXIT_OK

    summary = json.loads(
        next((env["state"] / "runs").glob("*/summary.json")).read_text(
            encoding="utf-8"
        )
    )
    target_wall = summary["telemetry"]["wall_sec"]
    assert summary["duration_sec"] - target_wall >= 0.3
    profiles = load_profiles(env["state"])
    generic = device_profile(
        profiles,
        "proj1",
        command_key(command),
        "LOCAL_SIM",
    )
    workload_row = next(
        iter(profiles[WORKLOAD_PROFILES_KEY]["entries"].values())
    )
    assert generic["exec_s"] == pytest.approx(target_wall, abs=0.001)
    assert workload_row["exec_s"] == pytest.approx(target_wall, abs=0.001)
    assert workload_row["throughput"] == pytest.approx(
        1 / target_wall,
        abs=0.001,
    )


def test_default_workload_is_explicit_project_opt_in(env):
    configure_workload(env, require_receipt=True, default=True)
    command = receipt_writing_command("default")
    assert main(["run", "LOCAL_SIM", "--", *command]) == EXIT_OK
    assert (env["proj"] / "adapted.txt").read_text() == "default"


def test_required_envelope_missing_policy_aborts_before_command(env):
    configure_workload(env, require_envelope=True, device_policy=False)
    code = main([
        "run",
        "LOCAL_SIM",
        "--workload",
        "demo.work",
        "--",
        "python",
        "-c",
        "open('must-not-run.txt','w').write('x')",
    ])
    assert code == EXIT_INTERNAL
    assert not (env["remote_proj"] / "must-not-run.txt").exists()
    summary = json.loads(
        next((env["state"] / "runs").glob("*/summary.json")).read_text(
            encoding="utf-8"
        )
    )
    assert summary["phase"] == "workload_admission"


def test_optional_missing_policy_stages_explicit_fallback_context(env):
    configure_workload(env, device_policy=False)
    script = (
        "import json,os,pathlib;"
        "ctx=json.load(open(os.environ['REMRUN_RUN_CONTEXT']));"
        "pathlib.Path('policy.txt').write_text(ctx['resources']['status'])"
    )
    assert main([
        "run", "LOCAL_SIM", "--workload", "demo.work", "--", "python", "-c", script
    ]) == EXIT_OK
    assert (env["proj"] / "policy.txt").read_text() == "policy_missing"


@pytest.mark.parametrize(
    "require_receipt,expected",
    [(False, EXIT_OK), (True, EXIT_INTERNAL)],
)
def test_context_staging_failure_obeys_required_boundary(
    env, monkeypatch, require_receipt, expected
):
    configure_workload(env, require_receipt=require_receipt)
    real_push = LocalSimTransport.push_file

    def fail_context(self, local_path, remote_path):
        if remote_path.endswith("run-context.v1.json"):
            raise TransportError("injected context staging failure")
        return real_push(self, local_path, remote_path)

    monkeypatch.setattr(LocalSimTransport, "push_file", fail_context)
    command = (
        "import os,pathlib;"
        "pathlib.Path('staging.txt').write_text("
        "'context' if 'REMRUN_RUN_CONTEXT' in os.environ else 'legacy')"
    )
    code = main([
        "run", "LOCAL_SIM", "--workload", "demo.work", "--", "python", "-c", command
    ])
    assert code == expected
    if require_receipt:
        assert not (env["remote_proj"] / "staging.txt").exists()
    else:
        assert (env["proj"] / "staging.txt").read_text() == "legacy"


@pytest.mark.parametrize(
    "require_receipt,expected",
    [(False, EXIT_OK), (True, EXIT_INTERNAL)],
)
def test_missing_receipt_is_checked_only_after_pullback(
    env, require_receipt, expected
):
    configure_workload(env, require_receipt=require_receipt)
    code = main([
        "run",
        "LOCAL_SIM",
        "--workload",
        "demo.work",
        "--",
        "python",
        "-c",
        "open('pulled-before-contract.txt','w').write('yes')",
    ])
    assert code == expected
    assert (env["proj"] / "pulled-before-contract.txt").read_text() == "yes"
    summary = json.loads(
        next((env["state"] / "runs").glob("*/summary.json")).read_text(
            encoding="utf-8"
        )
    )
    assert summary["command_exit_code"] == 0
    assert summary["receipt"]["status"] == "missing"
    assert summary["workload_profile"]["status"] == "not_recorded"
    profiles = load_profiles(env["state"])
    assert WORKLOAD_PROFILES_KEY not in profiles
    if require_receipt:
        assert profiles == {}


@pytest.mark.parametrize(
    "extra_arg,detail",
    [
        ("--no-telemetry", "detailed telemetry was disabled"),
        ("--no-pullback", "pullback was disabled"),
    ],
)
def test_workload_profile_requires_telemetry_and_verified_pullback(
    env, extra_arg, detail
):
    configure_workload(env, require_receipt=True)
    command = receipt_writing_command()

    assert main([
        "run",
        "LOCAL_SIM",
        "--workload",
        "demo.work",
        extra_arg,
        "--",
        *command,
    ]) == EXIT_OK

    summary = json.loads(
        next((env["state"] / "runs").glob("*/summary.json")).read_text(
            encoding="utf-8"
        )
    )
    assert summary["receipt"]["status"] == "valid"
    assert summary["workload_profile"] == {
        "status": "not_recorded",
        "detail": detail,
    }
    profiles = load_profiles(env["state"])
    assert WORKLOAD_PROFILES_KEY not in profiles
    if extra_arg == "--no-pullback":
        assert profiles == {}


def test_workload_profile_rejects_failed_command_even_with_valid_receipt(env):
    configure_workload(env, require_receipt=True)
    command = receipt_writing_command()
    command[-1] += "\nraise SystemExit(7)\n"

    assert main([
        "run", "LOCAL_SIM", "--workload", "demo.work", "--", *command
    ]) == 7

    summary = json.loads(
        next((env["state"] / "runs").glob("*/summary.json")).read_text(
            encoding="utf-8"
        )
    )
    assert summary["receipt"]["status"] == "valid"
    assert summary["workload_profile"]["status"] == "not_recorded"
    assert load_profiles(env["state"]) == {}


def test_workload_observation_rejects_unknown_metrics_and_zero_work():
    workload = WorkloadSpec(
        name="demo.work",
        protocol=1,
        adapter_id="demo.policy",
        adapter_version=1,
        work_unit="case",
        require_envelope=False,
        require_receipt=False,
    )
    receipt_data = {
        "status": "applied",
        "evaluation": "trial",
        "setting": {"workers": 2},
        "constraints": {"process_cap": 2},
        "work": {"unit": "case", "count": 1},
        "setting_fingerprint": "sha256:test",
    }
    telemetry = {
        "schema": 1,
        "wall_sec": 1.0,
        "process_tree_drained": True,
        "memory": {
            "peak_bytes": None,
            "metric": "rss_sum_sampled",
            "coverage": "sampler_failed",
        },
        "cpu": {
            "cpu_sec": None,
            "avg_cpu_pct": None,
            "coverage": "sampler_failed",
        },
        "gpu": {
            "scope": "whole_device",
            "max_util_pct": None,
            "min_vram_free_bytes": None,
            "unified_memory_min_available_bytes": None,
            "status": "unavailable",
        },
    }

    observation, detail = _workload_observation_from_run(
        project_id="proj",
        command=["python", "run.py"],
        device="LOCAL_SIM",
        workload=workload,
        receipt=ReceiptValidation("valid", data=receipt_data),
        telemetry=telemetry,
        trip_s=2,
        updated="now",
    )
    assert observation is None
    assert "memory telemetry" in detail

    telemetry["memory"] = {
        "peak_bytes": 1024,
        "metric": "rss_sum_sampled",
        "coverage": "known_tree_drained",
    }
    telemetry["cpu"] = {
        "cpu_sec": 1,
        "avg_cpu_pct": 100,
        "coverage": "wait4_known_tree_drained_detached_possible",
    }
    telemetry["process_tree_drained"] = False
    observation, detail = _workload_observation_from_run(
        project_id="proj",
        command=["python", "run.py"],
        device="LOCAL_SIM",
        workload=workload,
        receipt=ReceiptValidation("valid", data=receipt_data),
        telemetry=telemetry,
        trip_s=2,
        updated="now",
    )
    assert observation is None
    assert "not proven drained" in detail

    telemetry["process_tree_drained"] = True
    telemetry["gpu"] = {
        "scope": "garbage",
        "max_util_pct": 999,
        "min_vram_free_bytes": 1,
        "status": "fabricated",
    }
    observation, detail = _workload_observation_from_run(
        project_id="proj",
        command=["python", "run.py"],
        device="LOCAL_SIM",
        workload=workload,
        receipt=ReceiptValidation("valid", data=receipt_data),
        telemetry=telemetry,
        trip_s=2,
        updated="now",
    )
    assert observation is None
    assert "GPU telemetry" in detail

    receipt_data["work"]["count"] = 0
    observation, detail = _workload_observation_from_run(
        project_id="proj",
        command=["python", "run.py"],
        device="LOCAL_SIM",
        workload=workload,
        receipt=ReceiptValidation("valid", data=receipt_data),
        telemetry=telemetry,
        trip_s=2,
        updated="now",
    )
    assert observation is None
    assert "work count" in detail


def test_workload_observation_keeps_unified_memory_pressure():
    workload = WorkloadSpec(
        name="demo.work",
        protocol=1,
        adapter_id="demo.policy",
        adapter_version=1,
        work_unit="case",
        require_envelope=False,
        require_receipt=False,
    )
    receipt = ReceiptValidation(
        "valid",
        data={
            "status": "applied",
            "evaluation": "trial",
            "setting": {"workers": 2},
            "constraints": {"process_cap": 2},
            "work": {"unit": "case", "count": 2},
            "setting_fingerprint": "sha256:test",
        },
    )
    telemetry = {
        "schema": 1,
        "wall_sec": 1.0,
        "process_tree_drained": True,
        "memory": {
            "peak_bytes": 1024,
            "metric": "rss_sum_sampled",
            "coverage": "known_tree_drained",
        },
        "cpu": {
            "cpu_sec": 1,
            "avg_cpu_pct": 100,
            "coverage": "wait4_known_tree_drained_detached_possible",
        },
        "gpu": {
            "scope": "whole_device",
            "max_util_pct": None,
            "min_vram_free_bytes": None,
            "unified_memory_min_available_bytes": 4096,
            "status": "unavailable",
        },
    }

    observation, detail = _workload_observation_from_run(
        project_id="proj",
        command=["python", "run.py"],
        device="LOCAL_SIM",
        workload=workload,
        receipt=receipt,
        telemetry=telemetry,
        trip_s=2,
        updated="now",
    )

    assert detail == ""
    assert observation is not None
    assert observation.gpu["min_vram_free_bytes"] is None
    assert observation.gpu["unified_memory_min_available_bytes"] == 4096


def test_workload_dry_run_does_not_probe_or_stage(env, monkeypatch):
    configure_workload(env, require_receipt=True)
    monkeypatch.setattr(
        "remrun.cli.probe_target_resources",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("dry run must not sample resources")
        ),
    )
    assert main([
        "run",
        "LOCAL_SIM",
        "--workload",
        "demo.work",
        "--dry-run",
        "--",
        "python",
        "-c",
        "print('dry')",
    ]) == EXIT_OK
    run = next((env["state"] / "runs").iterdir())
    assert not list(run.glob("run-context*"))


def test_scoped_run_pulls_declared_output(env):
    cfgdir = env["proj"] / "do" / "remrun"
    cfgdir.mkdir(parents=True)
    (cfgdir / "remrun.toml").write_text(
        '[parallel.scopes.spec_a]\npaths = ["results/spec_a/**"]\n',
        encoding="utf-8",
    )
    code = main(["run", "LOCAL_SIM", "--scope", "spec_a", "--",
                 "python", "-c",
                 "import pathlib; p=pathlib.Path('results/spec_a/out.txt'); "
                 "p.parent.mkdir(parents=True, exist_ok=True); p.write_text('ok')"])
    assert code == EXIT_OK
    assert (env["proj"] / "results" / "spec_a" / "out.txt").read_text() == "ok"


def test_scoped_run_rejects_output_outside_declared_paths(env):
    cfgdir = env["proj"] / "do" / "remrun"
    cfgdir.mkdir(parents=True)
    (cfgdir / "remrun.toml").write_text(
        '[parallel.scopes.spec_a]\npaths = ["results/spec_a/**"]\n',
        encoding="utf-8",
    )
    code = main(["run", "LOCAL_SIM", "--scope", "spec_a", "--",
                 "python", "-c",
                 "import pathlib; p=pathlib.Path('results/spec_b/out.txt'); "
                 "p.parent.mkdir(parents=True, exist_ok=True); p.write_text('escaped')"])
    assert code == EXIT_CONFLICT
    assert not (env["proj"] / "results" / "spec_b" / "out.txt").exists()
    saved = list((env["state"] / "conflicts").glob("*/remote/results/spec_b/out.txt"))
    assert saved and saved[0].read_text() == "escaped"


def test_unknown_scope_is_rejected_before_execution(env):
    code = main(["run", "LOCAL_SIM", "--scope", "missing", "--",
                 "python", "-c", "open('nope.txt','w').write('x')"])
    assert code == EXIT_INTERNAL
    assert not (env["proj"] / "nope.txt").exists()


def test_conflict_aborts_with_exit_2(env):
    # First run establishes a baseline with shared.txt on both sides.
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "LOCAL_SIM", "--", "python", "-c", "print('first')"]) == EXIT_OK

    # Diverge both sides, then a run must abort before executing the command.
    (env["proj"] / "shared.txt").write_text("local-edit")
    (env["remote_proj"] / "shared.txt").write_text("remote-edit")
    code = main(["run", "LOCAL_SIM", "--",
                 "python", "-c", "open('should_not_exist.txt','w').write('x')"])
    assert code == EXIT_CONFLICT
    assert not (env["remote_proj"] / "should_not_exist.txt").exists()
    # Conflict metadata recorded outside the project tree.
    conflicts = list((env["state"] / "conflicts").glob("*/conflicts.json"))
    assert conflicts


def test_status_and_logs(env, capsys):
    main(["run", "LOCAL_SIM", "--", "python", "-c", "print('hello-logs')"])
    capsys.readouterr()
    assert main(["status"]) == EXIT_OK
    assert main(["logs", "last"]) == EXIT_OK
    out = capsys.readouterr().out
    assert "hello-logs" in out


def test_status_json_includes_fleet_state(env, capsys):
    assert main(["status", "--json"]) == EXIT_OK
    payload = capsys.readouterr().out
    assert '"fleet_state"' in payload
    assert '"runs"' in payload


def test_status_device_filter_applies_before_limit(env, capsys):
    runs = env["state"] / "runs"
    for run_id, target in (
        ("20260724T030000Z-WINBOX-demo-3", "WINBOX"),
        ("20260724T020000Z-MACBOX-demo-2", "MACBOX"),
        ("20260724T010000Z-WINBOX-demo-1", "WINBOX"),
    ):
        run = runs / run_id
        run.mkdir(parents=True)
        (run / "summary.json").write_text(
            json.dumps({"run_id": run_id, "target": target, "exit_code": 0}),
            encoding="utf-8",
        )

    assert main(["status", "WINBOX", "--limit", "2", "--json"]) == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert [run["run_id"] for run in payload["runs"]] == [
        "20260724T030000Z-WINBOX-demo-3",
        "20260724T010000Z-WINBOX-demo-1",
    ]
    assert main(["status", "WINBOX", "--limit", "0", "--json"]) == EXIT_OK
    assert json.loads(capsys.readouterr().out)["runs"] == []


def test_bench_records_local_and_remote_rows(env, capsys):
    cmd = ["python", "-c", "print('bench')"]
    code = main(["bench", "LOCAL_SIM", "--", *cmd])
    assert code == EXIT_OK
    key = command_key(cmd)
    profs = load_profiles(env["state"])
    # Both a LOCAL baseline row and the per-target trip row land in the profile.
    assert device_profile(profs, "proj1", key, LOCAL_DEVICE) is not None
    sim = device_profile(profs, "proj1", key, "LOCAL_SIM")
    assert sim is not None and sim["trip_s"] is not None
    assert "bench_verdict" in capsys.readouterr().err


def test_bench_no_local_skips_baseline_and_recommends_remote(env, capsys):
    cmd = ["python", "-c", "print('nl')"]
    code = main(["bench", "LOCAL_SIM", "--no-local", "--", *cmd])
    assert code == EXIT_OK
    key = command_key(cmd)
    profs = load_profiles(env["state"])
    # No local leg ran → no LOCAL baseline row, but the target trip is recorded.
    assert device_profile(profs, "proj1", key, LOCAL_DEVICE) is None
    assert device_profile(profs, "proj1", key, "LOCAL_SIM") is not None
    err = capsys.readouterr().err
    assert "bench_local_skipped" in err
    assert "recommend=remote" in err and "basis=no-local" in err


def test_plan_offload_policy_fallback_without_profile(env, capsys):
    # No bench data yet → plan still emits actionable offload guidance, falling
    # back to the host's static policy (the empty [offload] table → "ask").
    code = main(["plan", "LOCAL_SIM", "--", "python", "-c", "print('p')"])
    assert code == EXIT_OK
    err = capsys.readouterr().err
    assert "offload_policy" in err and "basis=no-measurement" in err


def test_plan_is_probe_free_by_default(env, capsys, monkeypatch):
    # Probing costs a round-trip per device, so `plan` must not do it unless asked.
    from remrun import transport as transport_mod

    def boom(self):
        raise AssertionError("plan sampled load without --probe")

    monkeypatch.setattr(transport_mod.LocalSimTransport, "sample_load", boom, raising=False)
    code = main(["plan", "LOCAL_SIM", "--json", "--", "python", "-c", "print('p')"])
    assert code == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert "candidates_probed" not in payload


def test_plan_probe_reports_live_load_and_spare_capacity(env, capsys, monkeypatch):
    # --probe exposes the same `spare` figure pick_by_load ranks on, so an orchestrator
    # sees the scheduler's own number rather than re-deriving it from a raw percentage.
    from remrun import transport as transport_mod

    monkeypatch.setattr(transport_mod.LocalSimTransport, "sample_load",
                        lambda self: 25.0, raising=False)
    code = main(["plan", "LOCAL_SIM", "--probe", "--json", "--", "python", "-c", "print('p')"])
    assert code == EXIT_OK
    probed = json.loads(capsys.readouterr().out)["candidates_probed"]
    entry = next(e for e in probed if e["name"] == "LOCAL_SIM")
    assert entry["reachable"] is True
    assert entry["cpu_busy_pct"] == 25.0
    # Nothing was asked about git, so the key is absent rather than a misleading default.
    assert "git" not in entry


def test_plan_auto_probe_displays_load_balanced_target_read_only(
    env, capsys, monkeypatch
):
    from remrun import transport as transport_mod

    devices = env["remrun_root"] / "config" / "devices.toml"
    devices.write_text(
        devices.read_text(encoding="utf-8")
        + "\n[scheduler]\n"
        + 'primary = "SIM_A"\n'
        + 'fallback = ["SIM_B"]\n'
        + "busy_floor_pct = 40\n"
        + "headroom_margin_cores = 4\n"
        + "eff_core_weight = 1.0\n"
        + "\n[devices.SIM_A]\n"
        + 'kind = "local-sim"\n'
        + 'os = "posix"\n'
        + f'project_root = "{posix(env["proj"].parent.parent / "remote-a")}"\n'
        + "perf_cores = 8\n"
        + "\n[devices.SIM_B]\n"
        + 'kind = "local-sim"\n'
        + 'os = "posix"\n'
        + f'project_root = "{posix(env["proj"].parent.parent / "remote-b")}"\n'
        + "perf_cores = 8\n",
        encoding="utf-8",
    )
    sentinel = env["proj"] / "sentinel.txt"
    sentinel.write_text("unchanged", encoding="utf-8")

    busy = {"SIM_A": 95.0, "SIM_B": 0.0, "LOCAL_SIM": 50.0}
    monkeypatch.setattr(
        transport_mod.LocalSimTransport,
        "sample_load",
        lambda self: busy[self.device.name],
        raising=False,
    )

    code = main([
        "plan", "--auto", "--probe", "--json", "--",
        "python", "-c", "print('p')",
    ])

    assert code == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert payload["target"]["name"] == "SIM_B"
    assert payload["target_reason"] == "auto-loadbalance"
    recommended = {entry["name"]: entry["recommended"]
                   for entry in payload["candidates_probed"]}
    assert recommended["SIM_A"] is False
    assert recommended["SIM_B"] is True
    assert sentinel.read_text(encoding="utf-8") == "unchanged"
    assert not (env["proj"].parent.parent / "remote-a").exists()
    assert not (env["proj"].parent.parent / "remote-b").exists()


def test_plan_probe_reports_unknown_load_as_null_not_zero(env, capsys, monkeypatch):
    # A backend that cannot measure must yield null. Zero would read as "totally idle"
    # and attract every routing decision to the device we know least about.
    from remrun import transport as transport_mod

    monkeypatch.setattr(transport_mod.LocalSimTransport, "sample_load",
                        lambda self: None, raising=False)
    code = main(["plan", "LOCAL_SIM", "--probe", "--json", "--", "python", "-c", "print('p')"])
    assert code == EXIT_OK
    entry = json.loads(capsys.readouterr().out)["candidates_probed"][0]
    assert entry["cpu_busy_pct"] is None
    assert entry["spare_perf_core_equiv"] is None


def test_plan_check_git_reports_unknown_for_a_non_git_checkout(env, capsys, monkeypatch):
    # The live-observed case: a Syncthing-delivered working tree with no .git at all.
    # It must report `unknown`, never `same` — remrun excludes .git/**, so EXCLUDED
    # paths on such a device are unreconciled and may be stale.
    from remrun import transport as transport_mod

    monkeypatch.setattr(transport_mod.LocalSimTransport, "sample_load",
                        lambda self: 10.0, raising=False)
    code = main(["plan", "LOCAL_SIM", "--check-git", "--json", "--",
                 "python", "-c", "print('p')"])
    assert code == EXIT_OK
    entry = json.loads(capsys.readouterr().out)["candidates_probed"][0]
    assert entry["git"]["status"] == "unknown"
    assert entry["reachable"] is True          # unknown git != unreachable device


def test_doctor_reports_project_scopes_and_fleet_state(env, capsys):
    from remrun.fleet.queue import FleetQueue

    cfgdir = env["proj"] / "do" / "remrun"
    cfgdir.mkdir(parents=True)
    (cfgdir / "remrun.toml").write_text(
        '[parallel.scopes.spec_a]\npaths = ["results/spec_a/**"]\n'
        '[git_sync]\npeers = ["LOCAL_SIM"]\n',
        encoding="utf-8",
    )
    FleetQueue(env["state"] / "fleet" / "fleet.db").close()
    code = main(["doctor"])
    assert code == EXIT_OK
    err = capsys.readouterr().err
    assert "syncthing" in err
    assert "fleet_state" in err
    assert "sqlite_version" in err and "sqlite_wal_reset_safe=true" in err
    assert "journal_mode=wal" in err
    assert "project_write_scopes" in err and "spec_a" in err
    assert "git_sync_hook" in err and "LOCAL_SIM" in err


def test_postrun_conflict_is_terminal_and_preserves_baseline(env):
    # Establish a baseline with conflict.txt identical on both sides.
    (env["proj"] / "conflict.txt").write_text("orig")
    assert main(["run", "LOCAL_SIM", "--", "python", "-c", "print('A')"]) == EXIT_OK
    profile_path = env["state"] / "profiles.json"
    admitted_profile_bytes = profile_path.read_bytes()

    # A run whose command rewrites the remote copy while the local copy also
    # changes (simulated via absolute-path write) is an unresolved post-run conflict.
    lp = posix(env["proj"])
    code = main(["run", "LOCAL_SIM", "--", "python", "-c",
                 f"open('conflict.txt','w').write('REMOTE'); "
                 f"open(r'{lp}/conflict.txt','w').write('LOCAL')"])
    # Command exited 0, but remrun could not converge -> reported as a conflict.
    assert code == EXIT_CONFLICT
    assert profile_path.read_bytes() == admitted_profile_bytes
    assert (env["proj"] / "conflict.txt").read_text() == "LOCAL"      # local not clobbered
    saved = list((env["state"] / "conflicts").glob("*/remote/conflict.txt"))
    assert saved and saved[0].read_text() == "REMOTE"                  # remote copy saved aside

    # Baseline was NOT advanced: the next plain run sees both sides diverged from the
    # preserved baseline and aborts in preflight (proves the baseline wasn't poisoned).
    assert main(["run", "LOCAL_SIM", "--", "python", "-c",
                 "open('should_not_exist.txt','w').write('x')"]) == EXIT_CONFLICT
    assert not (env["remote_proj"] / "should_not_exist.txt").exists()


@pytest.mark.parametrize("local_action", ["create", "modify", "delete"])
def test_postrun_baseline_advances_only_command_attributed_paths(
    env, monkeypatch, local_action
):
    """A local-only edit during exec must remain visible to the next preflight.

    The remote command changes ``remote-output.txt``.  The injected local change to
    ``unrelated.txt`` happens only after preflight, inside the production exec seam.
    The old implementation wrote the whole postrun local and remote manifests as one
    baseline pair, teaching each half a different value for the unrelated path.  The
    next preflight then called that divergence unchanged and did nothing.
    """
    unrelated = env["proj"] / "unrelated.txt"
    if local_action != "create":
        unrelated.write_text("before")
    assert main(["run", "LOCAL_SIM", "--", "python", "-c", "print('seed')"]) == EXIT_OK

    real_exec = LocalSimTransport.exec

    def exec_after_unrelated_local_change(self, *args, **kwargs):
        if local_action == "delete":
            unrelated.unlink()
        else:
            unrelated.write_text("local-during-run")
        return real_exec(self, *args, **kwargs)

    monkeypatch.setattr(LocalSimTransport, "exec", exec_after_unrelated_local_change)
    assert main([
        "run", "LOCAL_SIM", "--", "python", "-c",
        "open('remote-output.txt','w').write('remote-command')",
    ]) == EXIT_OK
    assert (env["proj"] / "remote-output.txt").read_text() == "remote-command"

    # The injection was for the prior run only.  This next run must reconcile the
    # still-unattributed local edit from the retained pre-run baseline.
    monkeypatch.setattr(LocalSimTransport, "exec", real_exec)
    assert main(["run", "LOCAL_SIM", "--", "python", "-c", "print('reconcile')"]) == EXIT_OK
    remote_unrelated = env["remote_proj"] / "unrelated.txt"
    if local_action == "delete":
        assert not remote_unrelated.exists()
    else:
        assert remote_unrelated.read_text() == "local-during-run"


def test_best_remote_verdict_ignores_excluded_devices():
    profs = {"p": {"k": {"MACBOX": {"trip_s": 5.0}, "WINBOX": {"trip_s": 9.0}}}}
    # Only WINBOX completed this bench; MACBOX's row is stale and must be ignored.
    rec = _best_remote_verdict(profs, "p", "k", ["WINBOX"])
    assert rec["recommend"] == "remote" and rec["best_device"] == "WINBOX"
    # No target completed -> unknown, never a stale recommendation.
    assert _best_remote_verdict(profs, "p", "k", [])["recommend"] == "unknown"


def test_bench_returns_infra_when_no_remote_leg_completes(env, monkeypatch, capsys):
    import remrun.cli as climod
    # Simulate every remote leg failing the round-trip.
    monkeypatch.setattr(climod, "cmd_run", lambda a, r: climod.EXIT_INFRA)
    code = main(["bench", "LOCAL_SIM", "--no-local", "--", "python", "-c", "print(1)"])
    assert code == EXIT_INFRA
    err = capsys.readouterr().err
    assert "bench_legs_failed" in err and "recommend=unknown" in err


def test_invalid_env_var_name_is_rejected(env):
    # A project config carrying a shell-injecting env var name must be refused.
    cfgdir = env["proj"] / "do" / "remrun"
    cfgdir.mkdir(parents=True)
    (cfgdir / "remrun.toml").write_text('[env]\n"BAD; rm -rf x" = "1"\n')
    code = main(["run", "LOCAL_SIM", "--", "python", "-c", "print(1)"])
    assert code == EXIT_INTERNAL   # rejected, not executed


def test_clean_keep_prunes_old_runs(env):
    for i in range(3):
        main(["run", "LOCAL_SIM", "--", "python", "-c", f"print({i})"])
    runs_root = env["state"] / "runs"
    assert len(list(runs_root.iterdir())) == 3

    # Dry-run keeps everything.
    assert main(["clean", "--keep", "1", "--dry-run"]) == EXIT_OK
    assert len(list(runs_root.iterdir())) == 3

    # Real clean keeps only the newest run.
    assert main(["clean", "--keep", "1"]) == EXIT_OK
    assert len(list(runs_root.iterdir())) == 1


@pytest.fixture()
def two_device_env(tmp_path: Path, monkeypatch):
    """Two reachable sim devices, SIM_A preferred, each with its own remote tree.

    Mirrors the field topology behind the 2026-07-27 reports: --auto ranks one device
    first, that device's tree has a conflict, and a second reachable device is clean.
    """
    remrun_root = tmp_path / "remrun"
    (remrun_root / "config").mkdir(parents=True)
    local_base = tmp_path / "local" / "proj"
    remote_a = tmp_path / "remote_a"
    remote_b = tmp_path / "remote_b"
    state_root = tmp_path / "state"
    for d in (local_base, remote_a, remote_b):
        d.mkdir(parents=True)

    (remrun_root / "config" / "defaults.toml").write_text(
        '[transfer]\n'
        'mode = "safe"\n'
        'global_exclude = ["node_modules/**", ".git/**"]\n'
        'hash_small_files_below_mb = 8\n'
        '\n'
        '[scheduler]\n'
        'primary = "SIM_A"\n'
        'fallback = ["SIM_B"]\n'
        'load_balance = false\n'
        '\n'
        '[logging]\n'
        'backup_below_mb = 1\n'
    )
    devices = '[project_roots]\n' + "".join(
        f'{k} = "{posix(local_base)}"\n' for k in ("default", "macos", "windows")
    )
    for name, root in (("SIM_A", remote_a), ("SIM_B", remote_b)):
        devices += (
            f'\n[devices.{name}]\n'
            'enabled = true\n'
            'role = "simulation"\n'
            'kind = "local-sim"\n'
            'os = "posix"\n'
            f'project_root = "{posix(root)}"\n'
            f'state_root = "{posix(state_root)}"\n'
            f'cache_root = "{posix(tmp_path / ("cache_" + name))}"\n'
        )
    (remrun_root / "config" / "devices.toml").write_text(devices)

    monkeypatch.setenv("REMRUN_ROOT", str(remrun_root))
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(state_root))

    proj = local_base / "proj1"
    proj.mkdir()
    monkeypatch.chdir(proj)
    return {
        "proj": proj,
        "remote_a": remote_a / "proj1",
        "remote_b": remote_b / "proj1",
        "state": state_root,
        "remrun_root": remrun_root,
    }


@pytest.fixture()
def three_device_env(two_device_env):
    env = two_device_env
    root = env["remrun_root"].parent
    remote_c = root / "remote_c"
    remote_c.mkdir()
    defaults = env["remrun_root"] / "config" / "defaults.toml"
    defaults.write_text(
        defaults.read_text(encoding="utf-8").replace(
            'fallback = ["SIM_B"]', 'fallback = ["SIM_B", "SIM_C"]'
        ),
        encoding="utf-8",
    )
    devices = env["remrun_root"] / "config" / "devices.toml"
    with devices.open("a", encoding="utf-8") as handle:
        handle.write(
            '\n[devices.SIM_C]\n'
            'enabled = true\n'
            'role = "simulation"\n'
            'kind = "local-sim"\n'
            'os = "posix"\n'
            f'project_root = "{posix(remote_c)}"\n'
            f'state_root = "{posix(env["state"])}"\n'
            f'cache_root = "{posix(root / "cache_SIM_C")}"\n'
        )
    return {**env, "remote_c": remote_c / "proj1"}


def test_auto_fails_over_to_next_candidate_on_preflight_conflict(two_device_env, capsys):
    """A conflict on the first-ranked candidate must not abandon the run.

    Reported three times from separate projects: --auto stopped on the first candidate's
    `both-changed` paths while a reachable, conflict-free device sat unused. The conflict
    is a property of ONE candidate's tree and is raised before any mutation, so the next
    ranked candidate must be tried.
    """
    env = two_device_env
    # Baseline shared.txt on the local tree and on BOTH remotes.
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    assert main(["run", "SIM_B", "--", "python", "-c", "print('seed-b')"]) == EXIT_OK
    capsys.readouterr()

    # Diverge ONLY SIM_A: local and SIM_A both changed since their shared baseline.
    # SIM_B still matches its own baseline, so it can reconcile cleanly.
    (env["proj"] / "shared.txt").write_text("local-edit")
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")

    code = main(["run", "--auto", "--",
                 "python", "-c", "open('ran.txt','w').write('ok')"])

    # The run completed on the fallback rather than aborting on the preferred device.
    assert code == EXIT_OK
    assert (env["remote_b"] / "ran.txt").exists()
    assert not (env["remote_a"] / "ran.txt").exists()
    assert (env["proj"] / "ran.txt").read_text() == "ok"

    err = capsys.readouterr().err
    assert "candidate_skipped name=SIM_A reason=preflight_conflict" in err
    assert "target name=SIM_B" in err

    # The skipped candidate's conflict evidence is retained, not discarded.
    receipts = [json.loads(p.read_text())
                for p in (env["state"] / "conflicts").glob("*/conflicts.json")]
    assert [r for r in receipts if r["target"] == "SIM_A"
            and any(c["path"] == "shared.txt" for c in r["conflicts"])]
    # SIM_A's diverged bytes were left alone — failover must not "fix" the skipped device.
    assert (env["remote_a"] / "shared.txt").read_text() == "remote-a-edit"


def test_auto_casefold_collision_then_fallback_pull_leaves_local_unchanged(
    two_device_env, monkeypatch, capsys
):
    env = two_device_env
    local_path = env["proj"] / "Foo"
    local_path.write_text("LOCAL")
    assert main(["run", "SIM_B", "--", "python", "-c", "print('seed-b')"]) == EXIT_OK
    (env["remote_b"] / "Foo").write_text("REMOTE-B")
    env["remote_a"].mkdir(parents=True)
    (env["remote_a"] / "foo").write_text("REMOTE-A")
    monkeypatch.setattr("remrun.reconcile.current_os_key", lambda: "macos")
    capsys.readouterr()

    code = main(["run", "--auto", "--", "python", "-c", "print('must-not-run')"])

    assert code == EXIT_CONFLICT
    assert local_path.read_text() == "LOCAL"
    receipts = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (env["state"] / "conflicts").glob("*/conflicts.json")
    ]
    first = next(receipt for receipt in receipts if receipt["target"] == "SIM_A")
    assert any(
        conflict["path"] == "Foo | foo" and conflict["state"] == "casefold-collision"
        for conflict in first["conflicts"]
    )
    fallback = next(receipt for receipt in receipts if receipt["target"] == "SIM_B")
    assert any(
        conflict["path"] == "Foo" and conflict["state"] == "fallback-local-mutation"
        for conflict in fallback["conflicts"]
    )


def test_auto_retries_after_fallback_local_mutation_and_reaches_safe_third_candidate(
    three_device_env, capsys
):
    env = three_device_env
    shared = env["proj"] / "shared.txt"
    shared.write_text("v0")
    for name in ("SIM_A", "SIM_B", "SIM_C"):
        assert main(["run", name, "--", "python", "-c", f"print('seed-{name}')"]) == EXIT_OK

    shared.write_text("local-disputed")
    assert main(["run", "SIM_B", "--", "python", "-c", "print('baseline-b')"]) == EXIT_OK
    (env["remote_b"] / "shared.txt").write_text("remote-b-new")
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")
    capsys.readouterr()

    code = main([
        "run", "--auto", "--", "python", "-c", "open('ran.txt','w').write('safe')"
    ])

    assert code == EXIT_OK
    assert shared.read_text() == "local-disputed"
    assert (env["remote_b"] / "shared.txt").read_text() == "remote-b-new"
    assert not (env["remote_b"] / "ran.txt").exists()
    assert (env["remote_c"] / "shared.txt").read_text() == "local-disputed"
    assert (env["remote_c"] / "ran.txt").read_text() == "safe"
    err = capsys.readouterr().err
    assert "candidate_skipped name=SIM_B reason=preflight_conflict" in err
    assert "target name=SIM_C" in err
    receipts = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (env["state"] / "conflicts").glob("*/conflicts.json")
    ]
    fallback = next(receipt for receipt in receipts if receipt["target"] == "SIM_B")
    assert any(
        conflict["path"] == "shared.txt"
        and conflict["state"] == "fallback-local-mutation"
        for conflict in fallback["conflicts"]
    )


def test_auto_skips_fallback_that_would_pull_any_path(two_device_env, capsys):
    """A fallback may not pull even a path unrelated to the first candidate's conflict."""
    env = two_device_env
    shared = env["proj"] / "shared.txt"
    unrelated = env["proj"] / "unrelated.txt"
    shared.write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    assert main(["run", "SIM_B", "--", "python", "-c", "print('seed-b')"]) == EXIT_OK

    shared.write_text("local-disputed")
    unrelated.write_text("local-baseline")
    assert main(["run", "SIM_B", "--", "python", "-c", "print('baseline-b')"]) == EXIT_OK
    (env["remote_b"] / "unrelated.txt").write_text("remote-b-new")
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")
    capsys.readouterr()

    code = main(["run", "--auto", "--",
                 "python", "-c", "open('ran.txt','w').write('unsafe')"])

    assert code == EXIT_CONFLICT
    assert shared.read_text() == "local-disputed"
    assert unrelated.read_text() == "local-baseline"
    assert (env["remote_b"] / "unrelated.txt").read_text() == "remote-b-new"
    assert not (env["remote_b"] / "ran.txt").exists()
    err = capsys.readouterr().err
    assert "candidate_skipped name=SIM_A reason=preflight_conflict" in err
    receipts = [json.loads(p.read_text())
                for p in (env["state"] / "conflicts").glob("*/conflicts.json")]
    fallback = next(r for r in receipts if r["target"] == "SIM_B")
    assert any(c["path"] == "unrelated.txt" and c["state"] == "fallback-local-mutation"
               for c in fallback["conflicts"])


def test_auto_skips_fallback_that_would_delete_local_earlier_conflict_above_backup_cap(
    two_device_env, capsys
):
    """Failover must preserve a disputed local file even when no backup would be retained."""
    env = two_device_env
    disputed = "L" * (2 * 1024 * 1024)
    path = env["proj"] / "shared.txt"
    path.write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    assert main(["run", "SIM_B", "--", "python", "-c", "print('seed-b')"]) == EXIT_OK

    # SIM_B records the large local edit, then sees a clean remote-side deletion.
    path.write_text(disputed)
    assert main(["run", "SIM_B", "--", "python", "-c", "print('baseline-b')"]) == EXIT_OK
    (env["remote_b"] / "shared.txt").unlink()

    # SIM_A still has the old baseline and independently changed bytes: a real conflict.
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")
    capsys.readouterr()
    code = main(["run", "--auto", "--",
                 "python", "-c", "open('ran.txt','w').write('unsafe')"])

    assert code == EXIT_CONFLICT
    assert path.exists()
    assert path.read_text() == disputed
    assert not list((env["state"] / "conflicts").glob("*/backup/shared.txt"))
    assert not (env["remote_b"] / "ran.txt").exists()
    receipts = [json.loads(p.read_text())
                for p in (env["state"] / "conflicts").glob("*/conflicts.json")]
    fallback = next(r for r in receipts if r["target"] == "SIM_B")
    assert any(c["path"] == "shared.txt" and c["state"] == "fallback-local-mutation"
               for c in fallback["conflicts"])


def test_auto_returns_conflict_when_every_candidate_conflicts(two_device_env, capsys):
    """Failover must not mask a genuine all-candidates-conflicted state as success."""
    env = two_device_env
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    assert main(["run", "SIM_B", "--", "python", "-c", "print('seed-b')"]) == EXIT_OK
    capsys.readouterr()

    (env["proj"] / "shared.txt").write_text("local-edit")
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")
    (env["remote_b"] / "shared.txt").write_text("remote-b-edit")

    code = main(["run", "--auto", "--",
                 "python", "-c", "open('ran.txt','w').write('ok')"])
    assert code == EXIT_CONFLICT
    assert not (env["remote_a"] / "ran.txt").exists()
    assert not (env["remote_b"] / "ran.txt").exists()


def test_explicit_target_never_fails_over_on_conflict(two_device_env, capsys):
    """An explicitly named device is a user instruction, not a placement hint."""
    env = two_device_env
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    capsys.readouterr()

    (env["proj"] / "shared.txt").write_text("local-edit")
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")

    code = main(["run", "SIM_A", "--", "python", "-c", "open('ran.txt','w').write('ok')"])
    assert code == EXIT_CONFLICT
    # SIM_B was never touched: naming a device must not silently redirect the work.
    assert not (env["remote_b"] / "ran.txt").exists()
    assert "SIM_B" not in capsys.readouterr().err


def test_auto_local_vanished_aborts_without_trying_another_candidate(
    two_device_env, monkeypatch, capsys
):
    env = two_device_env
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    env["remote_b"].mkdir(parents=True)
    (env["remote_b"] / "from-b.txt").write_text("must not be pulled")
    capsys.readouterr()

    from remrun import cli as cli_mod

    real_resolve = cli_mod._resolve_targets
    vanished = env["proj"].with_name("proj1-vanished")

    def resolve_then_vanish(*args, **kwargs):
        selection = real_resolve(*args, **kwargs)
        # Windows will not rename a directory while it is the process cwd. Move
        # outside first, then create the same live product condition: the project
        # resolved at command start has vanished before preflight.
        monkeypatch.chdir(env["proj"].parent)
        env["proj"].rename(vanished)
        return selection

    monkeypatch.setattr(cli_mod, "_resolve_targets", resolve_then_vanish)
    try:
        code = main([
            "run", "--auto", "--", "python", "-c", "open('ran.txt','w').write('wrong')"
        ])
        err = capsys.readouterr().err
        assert code == EXIT_CONFLICT
        assert not env["proj"].exists()
        assert not (env["remote_b"] / "ran.txt").exists()
        assert "target name=SIM_B" not in err
    finally:
        if env["proj"].exists():
            shutil.rmtree(env["proj"])
        if vanished.exists():
            vanished.rename(env["proj"])


def test_auto_remote_vanished_still_fails_over(two_device_env, capsys):
    # Guard, not a regression test: remote-vanished failover already worked before this fix set.
    env = two_device_env
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    shutil.rmtree(env["remote_a"])
    capsys.readouterr()

    code = main([
        "run", "--auto", "--", "python", "-c", "open('ran.txt','w').write('ok')"
    ])

    assert code == EXIT_OK
    assert (env["remote_b"] / "ran.txt").read_text() == "ok"
    err = capsys.readouterr().err
    assert "candidate_skipped name=SIM_A reason=preflight_conflict" in err
    assert "target name=SIM_B" in err


def test_skipped_candidate_conflict_receipt_survives_successful_fallback_retention(
    two_device_env, capsys
):
    env = two_device_env
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    assert main(["run", "SIM_B", "--", "python", "-c", "print('seed-b')"]) == EXIT_OK
    defaults = env["remrun_root"] / "config" / "defaults.toml"
    defaults.write_text(
        defaults.read_text(encoding="utf-8")
        + "\nbackup_retention_days = 999\nmax_backup_mb = 0.000001\n",
        encoding="utf-8",
    )
    (env["proj"] / "shared.txt").write_text("local-edit")
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")
    capsys.readouterr()

    code = main(["run", "--auto", "--", "python", "-c", "print('fallback')"])

    assert code == EXIT_OK
    receipts = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (env["state"] / "conflicts").glob("*/conflicts.json")
    ]
    assert any(receipt["target"] == "SIM_A" for receipt in receipts)


def test_stdout_reaches_the_log_before_the_command_exits(env):
    """A long run must not be indistinguishable from a hang.

    Before streaming, `stdout.log` was written only after transport.exec returned, so a
    multi-hour run left a zero-byte log and an agent watching it could not tell live from
    dead (two independent field reports). Here a watcher thread
    releases the remote command only once it has seen the marker IN THE LOG — so the
    command can only exit if the bytes were really flushed mid-run, and the buffered
    implementation deadlocks until its own timeout instead of passing.
    """
    seen: dict[str, object] = {}
    state_root = env["state"]

    def watch() -> None:
        deadline = time.time() + 30
        while time.time() < deadline:
            for log in (state_root / "runs").glob("*/stdout.log"):
                if "LIVE-MARKER" in log.read_text(errors="replace"):
                    seen["mid_run"] = True
                    (env["proj"] / "observed.flag").write_text("1")
                    return
            time.sleep(0.05)

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    code = main(["run", "LOCAL_SIM", "--", "python", "-c",
                 "import time,os\n"
                 "print('LIVE-MARKER', flush=True)\n"
                 "deadline = time.time() + 30\n"
                 f"while not os.path.exists({str(env['proj'] / 'observed.flag')!r}):\n"
                 "    if time.time() > deadline: raise SystemExit('log never observed')\n"
                 "    time.sleep(0.05)\n"])
    watcher.join(timeout=5)

    assert code == EXIT_OK
    # The marker was in the log while the remote command was still running.
    assert seen.get("mid_run") is True


def test_streaming_stdout_log_is_capped_before_transport_returns(env, monkeypatch):
    max_bytes = 256
    set_log_cap(env, max_bytes)
    observed: dict[str, object] = {}
    payload = "x" * 10_000

    def verbose_exec(self, command, cwd, **kwargs):
        on_stdout = kwargs["on_stdout"]
        on_stdout(payload)
        log = next((env["state"] / "runs").glob("*/stdout.log"))
        data = log.read_bytes()
        observed["size"] = len(data)
        observed["truncated"] = b"remrun truncated" in data
        return ExecResult(0, payload, "")

    monkeypatch.setattr(LocalSimTransport, "exec", verbose_exec)

    code = main(["run", "LOCAL_SIM", "--", "python", "-c", "print('unused')"])

    assert code == EXIT_OK
    assert 0 < int(observed["size"]) <= max_bytes
    assert observed["truncated"] is True


def test_streaming_stdout_log_stays_capped_after_transport_error(env, monkeypatch):
    max_bytes = 256
    set_log_cap(env, max_bytes)

    def failing_exec(self, command, cwd, **kwargs):
        kwargs["on_stdout"]("x" * 10_000)
        raise TransportError("injected disconnect after output")

    monkeypatch.setattr(LocalSimTransport, "exec", failing_exec)

    code = main(["run", "LOCAL_SIM", "--", "python", "-c", "print('unused')"])

    assert code == EXIT_INFRA
    log = next((env["state"] / "runs").glob("*/stdout.log"))
    assert log.stat().st_size <= max_bytes
    assert "remrun truncated" in log.read_text(encoding="utf-8", errors="replace")


def test_unwritable_stdout_log_does_not_abort_run(env, monkeypatch):
    real_open = Path.open

    def deny_stdout_log(self, mode="r", *args, **kwargs):
        if self.name == "stdout.log" and any(flag in mode for flag in "wax+"):
            raise PermissionError("injected unwritable stdout log")
        return real_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", deny_stdout_log)

    code = main([
        "run", "LOCAL_SIM", "--", "python", "-c", "open('ran.txt','w').write('ok')"
    ])

    assert code == EXIT_OK
    assert (env["proj"] / "ran.txt").read_text() == "ok"


def test_guarded_ordinary_run_preserves_real_exit_with_no_telemetry(env):
    configure_memory_guard(env)

    code = main(
        [
            "run",
            "LOCAL_SIM",
            "--no-telemetry",
            "--",
            sys.executable,
            "-c",
            "print('guarded ordinary'); raise SystemExit(7)",
        ]
    )

    assert code == 7
    summaries = list((env["state"] / "runs").glob("*/summary.json"))
    assert len(summaries) == 1
    summary = json.loads(summaries[0].read_text(encoding="utf-8"))
    assert summary["exit_code"] == 7
    assert summary["command_exit_code"] == 7
    assert summary["telemetry"] is None
    assert summary["memory_guard"]["status"] == "ok"
    assert summary["memory_guard"]["command_started"] is True


def test_no_telemetry_cannot_disable_guard_threshold_termination(env, capsys):
    configure_memory_guard(env, max_command_mib=160)
    program = (
        "import time;"
        "x=bytearray(160*1024*1024);"
        "[x.__setitem__(i,1) for i in range(0,len(x),4096)];"
        "time.sleep(10)"
    )

    code = main(
        [
            "run",
            "LOCAL_SIM",
            "--no-telemetry",
            "--",
            sys.executable,
            "-c",
            program,
        ]
    )

    assert code == EXIT_GUARD
    summaries = list((env["state"] / "runs").glob("*/summary.json"))
    assert len(summaries) == 1
    summary = json.loads(summaries[0].read_text(encoding="utf-8"))
    assert summary["exit_code"] == EXIT_GUARD
    assert summary["command_exit_code"] is None
    assert summary["telemetry"] is None
    assert summary["memory_guard"]["status"] == "terminated"
    assert summary["memory_guard"]["reason"] == "command_memory_limit"
    assert summary["memory_guard"]["cleanup_complete"] is True
    assert summary["memory_limit_guidance"] == {
        "allowance_basis": "unprofiled_available_backed",
        "allocation_rule": "unprofiled_open_slot_fair_share_v1",
        "fair_share_limit_mib": 160,
        "observed_peak_lower_bound_bytes": summary["memory_guard"][
            "peak_command_bytes"
        ],
        "policy_command_ceiling_bytes": 160 * 1024**2,
        "partial_effects_may_exist": True,
        "profile_recorded": False,
        "retry_hint": (
            "inspect the workload, then intentionally rerun with "
            "--memory-limit-mib N if a larger hard limit is justified"
        ),
    }
    assert load_profiles(env["state"]) == {}
    assert not list((env["state"] / "hazards" / "project").glob("*/unknown.json"))
    events = capsys.readouterr().err
    assert "memory_guard status=terminated" in events
    assert "memory_limit_guidance" in events
    assert "profile_recorded=false" in events
    assert "--memory-limit-mib N" in events


def test_guard_prelaunch_refusal_is_distinct_and_user_code_never_runs(
    env, monkeypatch
):
    from remrun.memory_guard import MemoryAdmissionResult

    configure_memory_guard(env)
    monkeypatch.setattr(
        LocalSimTransport,
        "reserve_memory_guard",
        lambda self, *, predicted_rss_mb=None: MemoryAdmissionResult.refused(
            "insufficient_live_memory", "deterministic pre-mutation refusal"
        ),
    )

    code = main(
        [
            "run",
            "LOCAL_SIM",
            "--no-telemetry",
            "--",
            sys.executable,
            "-c",
            "open('must-not-run.txt','w').write('unsafe')",
        ]
    )

    assert code == EXIT_GUARD
    assert not (env["remote_proj"] / "must-not-run.txt").exists()
    summaries = list((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summaries[0].read_text(encoding="utf-8"))
    assert summary["phase"] == "memory_admission"
    assert summary["error"] == "no safe target capacity"
    assert summary["memory_admission"]["status"] == "refused"
    assert summary["memory_admission"]["reason"] == "insufficient_live_memory"
    assert summary["job_profile"]["status"] == "unprofiled"
    assert summary["job_profile"]["project_id"] == "proj1"
    assert summary["job_profile"]["predicted_rss_mb"] is None


def test_run_memory_limit_is_remrun_option_and_same_post_separator_token_is_user_argv(
    env, capsys
):
    configure_memory_guard(env, max_command_mib=256)
    program = (
        "import json,sys; from pathlib import Path; "
        "Path('explicit-argv.json').write_text(json.dumps(sys.argv[1:]))"
    )

    code = main(
        [
            "run",
            "LOCAL_SIM",
            "--memory-limit-mib",
            "64",
            "--no-telemetry",
            "--",
            sys.executable,
            "-c",
            program,
            "--memory-limit-mib",
            "777",
        ]
    )

    assert code == EXIT_OK
    assert json.loads((env["proj"] / "explicit-argv.json").read_text()) == [
        "--memory-limit-mib",
        "777",
    ]
    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["memory_admission"]["allowance_basis"] == "explicit_command_limit"
    assert summary["memory_admission"]["allowance_bytes"] == 64 * 1024**2
    assert "lease_token" not in summary["memory_admission"]
    events = capsys.readouterr().err
    assert "remrun: memory_admission" in events
    assert "status=admitted" in events


def test_run_memory_limit_refuses_unguarded_target_before_project_mutation(env):
    code = main(
        [
            "run",
            "LOCAL_SIM",
            "--memory-limit-mib",
            "64",
            "--",
            sys.executable,
            "-c",
            "open('must-not-run.txt','w').write('unsafe')",
        ]
    )

    assert code == EXIT_GUARD
    assert not env["remote_proj"].exists()
    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["phase"] == "memory_admission"
    assert summary["memory_admission"]["reason"] == "guard_not_configured"


def test_run_memory_limit_over_target_ceiling_refuses_before_project_mutation(env):
    configure_memory_guard(env, max_command_mib=64)

    code = main(
        [
            "run",
            "LOCAL_SIM",
            "--memory-limit-mib",
            "65",
            "--",
            sys.executable,
            "-c",
            "open('must-not-run.txt','w').write('unsafe')",
        ]
    )

    assert code == EXIT_GUARD
    assert not env["remote_proj"].exists()
    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["memory_admission"]["reason"] == (
        "explicit_limit_exceeds_command_limit"
    )


def test_run_memory_limit_overrides_learned_rss_for_placement_and_admission(
    env, monkeypatch
):
    from remrun import cli as cli_mod

    configure_memory_guard(env, max_command_mib=256)
    learned = {"rss_mb": 4096.0, "dur_s": 12.5}
    monkeypatch.setattr(cli_mod, "_job_prediction", lambda config, plan: learned)

    placement_predictions: list[dict | None] = []
    real_resolve = cli_mod._resolve_targets

    def resolve(plan, target_name, sched, reporter, prediction=None, **kwargs):
        placement_predictions.append(prediction)
        return real_resolve(
            plan, target_name, sched, reporter, prediction, **kwargs
        )

    reserve_calls: list[dict[str, object]] = []
    real_reserve = LocalSimTransport.reserve_memory_guard

    def reserve(self, **kwargs):
        reserve_calls.append(kwargs)
        return real_reserve(self, **kwargs)

    monkeypatch.setattr(cli_mod, "_resolve_targets", resolve)
    monkeypatch.setattr(LocalSimTransport, "reserve_memory_guard", reserve)

    code = main(
        [
            "run",
            "LOCAL_SIM",
            "--memory-limit-mib",
            "64",
            "--",
            sys.executable,
            "-c",
            "from pathlib import Path; Path('explicit-won.txt').write_text('ok')",
        ]
    )

    assert code == EXIT_OK
    assert placement_predictions == [{"dur_s": 12.5}]
    assert reserve_calls == [{"explicit_limit_mib": 64}]
    assert (env["proj"] / "explicit-won.txt").read_text() == "ok"


def test_successful_explicit_no_telemetry_run_seeds_learned_rss(env):
    configure_memory_guard(env, max_command_mib=256)
    program = (
        "from pathlib import Path; "
        "x=bytearray(8*1024*1024); "
        "[x.__setitem__(i,1) for i in range(0,len(x),4096)]; "
        "Path('explicit-profile.txt').write_text(str(len(x)),encoding='utf-8')"
    )
    argv = [sys.executable, "-c", program]

    first = main(
        [
            "run",
            "LOCAL_SIM",
            "--memory-limit-mib",
            "224",
            "--no-telemetry",
            "--",
            *argv,
        ]
    )

    assert first == EXIT_OK
    profiles = load_profiles(env["state"])
    row = profiles["proj1"][command_key(argv)]["LOCAL_SIM"]
    assert row["rss_mb"] > 0
    assert row["rss_high_mb"] > 0

    second = main(["run", "LOCAL_SIM", "--no-telemetry", "--", *argv])

    assert second == EXIT_OK
    summaries = sorted(
        (env["state"] / "runs").glob("*/summary.json"),
        key=lambda path: path.stat().st_mtime_ns,
    )
    summary = json.loads(summaries[-1].read_text(encoding="utf-8"))
    assert summary["memory_admission"]["allowance_basis"] == (
        "learned_profile_plus_25_percent"
    )


def test_final_summary_uses_transport_authenticated_renewed_reservation(
    env, monkeypatch
):
    configure_memory_guard(env, max_command_mib=512)
    renewed_allowance = 128 * 1024**2

    def execute(self, command, cwd, **kwargs):
        del self, command
        original = kwargs["memory_reservation"]
        renewed = replace(
            original,
            allowance_bytes=renewed_allowance,
            capacity_bytes=renewed_allowance + original.control_overhead_bytes,
            per_open_slot_capacity_bytes=(
                renewed_allowance + original.control_overhead_bytes
            ),
        )
        Path(cwd, "renewed-summary.txt").write_text("ok", encoding="utf-8")
        guard = {
            "schema": 1,
            "status": "ok",
            "reason": "completed",
            "detail": "test",
            "command_started": True,
            "command_exit_code": 0,
            "helper_exit_code": 0,
            "max_command_bytes": renewed.allowance_bytes,
            "min_available_bytes": renewed.min_available_bytes,
            "peak_command_bytes": 1,
            "min_host_available_bytes": renewed.min_available_bytes + 1,
            "cleanup_complete": True,
        }
        return ExecResult(
            0,
            "",
            "",
            None,
            guard,
            memory_reservation=renewed,
        )

    monkeypatch.setattr(LocalSimTransport, "exec", execute)

    code = main(
        [
            "run",
            "LOCAL_SIM",
            "--no-telemetry",
            "--",
            sys.executable,
            "-c",
            "print('unused')",
        ]
    )

    assert code == EXIT_OK
    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["memory_guard"]["max_command_bytes"] == renewed_allowance
    assert summary["memory_admission"]["allowance_bytes"] == renewed_allowance


def test_memory_limit_guidance_uses_transport_authenticated_renewed_reservation(
    env, monkeypatch
):
    configure_memory_guard(env, max_command_mib=512)
    renewed_allowance = 192 * 1024**2
    observed_peak = 208 * 1024**2

    def execute(self, command, cwd, **kwargs):
        del self, command, cwd
        original = kwargs["memory_reservation"]
        renewed = replace(
            original,
            allowance_bytes=renewed_allowance,
            capacity_bytes=renewed_allowance + original.control_overhead_bytes,
            per_open_slot_capacity_bytes=(
                renewed_allowance + original.control_overhead_bytes
            ),
        )
        guard = {
            "schema": 1,
            "status": "terminated",
            "reason": "command_memory_limit",
            "detail": "test",
            "command_started": True,
            "command_exit_code": None,
            "helper_exit_code": 125,
            "max_command_bytes": renewed.allowance_bytes,
            "min_available_bytes": renewed.min_available_bytes,
            "peak_command_bytes": observed_peak,
            "min_host_available_bytes": renewed.min_available_bytes + 1,
            "cleanup_complete": True,
        }
        return ExecResult(
            125,
            "",
            "",
            None,
            guard,
            memory_reservation=renewed,
        )

    monkeypatch.setattr(LocalSimTransport, "exec", execute)

    code = main(
        [
            "run",
            "LOCAL_SIM",
            "--no-telemetry",
            "--",
            sys.executable,
            "-c",
            "print('unused')",
        ]
    )

    assert code == EXIT_GUARD
    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["memory_admission"]["allowance_bytes"] == renewed_allowance
    assert summary["memory_limit_guidance"] == {
        "allowance_basis": "unprofiled_available_backed",
        "allocation_rule": "unprofiled_open_slot_fair_share_v1",
        "fair_share_limit_mib": 192,
        "observed_peak_lower_bound_bytes": observed_peak,
        "policy_command_ceiling_bytes": 512 * 1024**2,
        "partial_effects_may_exist": True,
        "profile_recorded": False,
        "retry_hint": (
            "inspect the workload, then intentionally rerun with "
            "--memory-limit-mib N if a larger hard limit is justified"
        ),
    }


def test_auto_memory_limit_skips_unguarded_target_before_mutation(two_device_env):
    env = two_device_env
    devices = env["remrun_root"] / "config" / "devices.toml"
    with devices.open("a", encoding="utf-8") as handle:
        handle.write(
            "\n[devices.SIM_B.memory_guard]\n"
            "schema = 3\n"
            "command_limit_fraction = 0.25\n"
            "host_reserve_fraction = 0.25\n"
        )
    (env["proj"] / "input.txt").write_text("source", encoding="utf-8")

    code = main(
        [
            "run",
            "--auto",
            "--memory-limit-mib",
            "64",
            "--",
            sys.executable,
            "-c",
            "from pathlib import Path; Path('explicit-auto.txt').write_text('ok')",
        ]
    )

    assert code == EXIT_OK
    assert not env["remote_a"].exists()
    assert (env["remote_b"] / "input.txt").read_text(encoding="utf-8") == "source"
    assert (env["proj"] / "explicit-auto.txt").read_text(encoding="utf-8") == "ok"
    summaries = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (env["state"] / "runs").glob("*/summary.json")
    ]
    refused = next(
        item for item in summaries if item["plan"]["target"]["name"] == "SIM_A"
    )
    assert refused["phase"] == "memory_admission"
    assert refused["memory_admission"]["reason"] == "guard_not_configured"


def test_selected_workload_is_guarded_even_with_no_telemetry(env):
    configure_workload(env, require_receipt=False)
    configure_memory_guard(env, max_command_mib=160)
    program = (
        "import time;"
        "x=bytearray(160*1024*1024);"
        "[x.__setitem__(i,1) for i in range(0,len(x),4096)];"
        "time.sleep(10)"
    )

    code = main(
        [
            "run",
            "LOCAL_SIM",
            "--workload",
            "demo.work",
            "--no-telemetry",
            "--",
            sys.executable,
            "-c",
            program,
        ]
    )

    assert code == EXIT_GUARD
    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["workload"]["name"] == "demo.work"
    assert summary["telemetry"] is None
    assert summary["memory_guard"]["status"] == "terminated"
    assert summary["memory_guard"]["reason"] == "command_memory_limit"


def _enable_relative_guards_for_two_sim_devices(env: dict) -> None:
    devices = env["remrun_root"] / "config" / "devices.toml"
    with devices.open("a", encoding="utf-8") as handle:
        for name in ("SIM_A", "SIM_B"):
            handle.write(
                f"\n[devices.{name}.memory_guard]\n"
                "schema = 3\n"
                "command_limit_fraction = 0.25\n"
                "host_reserve_fraction = 0.25\n"
            )


def test_auto_memory_admission_skips_unsafe_candidate_before_project_mutation(
    two_device_env, monkeypatch
):
    from remrun.memory_guard import MemoryAdmissionResult, MemoryReservation

    env = two_device_env
    _enable_relative_guards_for_two_sim_devices(env)
    (env["proj"] / "input.txt").write_text("source", encoding="utf-8")

    def reserve(self, *, predicted_rss_mb=None):
        del predicted_rss_mb
        if self.device.name == "SIM_A":
            return MemoryAdmissionResult.refused(
                "insufficient_live_memory", "deterministic unsafe candidate"
            )
        reservation = MemoryReservation(
            lease_id="b" * 32,
            lease_token="c" * 32,
            state_root=self.device.state_root,
            allowance_bytes=16 * 1024**3,
            control_overhead_bytes=256 * 1024**2,
            capacity_bytes=16 * 1024**3 + 256 * 1024**2,
            max_command_bytes=16 * 1024**3,
            min_available_bytes=16 * 1024**3,
            host_total_bytes=64 * 1024**3,
            safe_concurrency=2,
            expires_at=time.time() + 60,
        )
        return MemoryAdmissionResult(
            "admitted", "reserved", "safe", {"schema": 1, "status": "admitted"},
            reservation,
        )

    def release(self, reservation, *, reserved_only=True):
        del self, reservation, reserved_only
        return MemoryAdmissionResult(
            "released", "released", "released",
            {"schema": 1, "status": "released", "reason": "released", "detail": "released"},
        )

    def execute(self, command, cwd, **kwargs):
        del command
        assert self.device.name == "SIM_B"
        assert kwargs["memory_reservation"].lease_id == "b" * 32
        Path(cwd, "ran.txt").write_text("ok", encoding="utf-8")
        guard = {
            "schema": 1,
            "status": "ok",
            "reason": "completed",
            "detail": "test",
            "command_started": True,
            "command_exit_code": 0,
            "helper_exit_code": 0,
            "max_command_bytes": 16 * 1024**3,
            "min_available_bytes": 16 * 1024**3,
            "peak_command_bytes": 1,
            "min_host_available_bytes": 32 * 1024**3,
            "cleanup_complete": True,
        }
        return ExecResult(0, "", "", None, guard)

    monkeypatch.setattr(LocalSimTransport, "reserve_memory_guard", reserve)
    monkeypatch.setattr(LocalSimTransport, "release_memory_guard", release)
    monkeypatch.setattr(LocalSimTransport, "exec", execute)

    code = main(["run", "--auto", "--", "python", "-c", "print('unused')"])

    assert code == EXIT_OK
    assert not env["remote_a"].exists()
    assert (env["remote_b"] / "input.txt").read_text(encoding="utf-8") == "source"
    assert (env["proj"] / "ran.txt").read_text(encoding="utf-8") == "ok"
    summaries = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (env["state"] / "runs").glob("*/summary.json")
    ]
    refused = next(item for item in summaries if item["plan"]["target"]["name"] == "SIM_A")
    assert refused["phase"] == "memory_admission"
    assert refused["error"] == "no safe target capacity"


def test_auto_no_safe_memory_candidate_returns_structured_exit_five_without_mutation(
    two_device_env, monkeypatch
):
    from remrun.memory_guard import MemoryAdmissionResult

    env = two_device_env
    _enable_relative_guards_for_two_sim_devices(env)
    (env["proj"] / "input.txt").write_text("source", encoding="utf-8")

    def refuse(self, *, predicted_rss_mb=None):
        del predicted_rss_mb
        return MemoryAdmissionResult.refused(
            "insufficient_live_memory", f"{self.device.name} has no safe capacity"
        )

    monkeypatch.setattr(LocalSimTransport, "reserve_memory_guard", refuse)

    code = main(["run", "--auto", "--", "python", "-c", "print('must not run')"])

    assert code == EXIT_GUARD
    assert not env["remote_a"].exists()
    assert not env["remote_b"].exists()
    summaries = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (env["state"] / "runs").glob("*/summary.json")
    ]
    assert len(summaries) == 2
    assert all(item["phase"] == "memory_admission" for item in summaries)
    assert all(item["error"] == "no safe target capacity" for item in summaries)


def test_main_memory_guard_live_path_reserves_renews_executes_and_releases(
    env, monkeypatch
):
    """Exercise the real CLI/admission/helper/argv/result/ledger-release path."""
    configure_memory_guard(env, max_command_mib=512, min_available_mib=16)
    operations: list[str] = []
    real_invoke = LocalSimTransport._invoke_memory_admission

    def recording_invoke(self, request):
        operations.append(str(request.get("op")))
        return real_invoke(self, request)

    monkeypatch.setattr(LocalSimTransport, "_invoke_memory_admission", recording_invoke)

    code = main(
        [
            "run",
            "LOCAL_SIM",
            "--no-telemetry",
            "--",
            sys.executable,
            "-c",
            "from pathlib import Path; Path('guard-e2e.txt').write_text('ok')",
        ]
    )

    assert code == EXIT_OK
    assert (env["proj"] / "guard-e2e.txt").read_text(encoding="utf-8") == "ok"
    assert operations[:2] == ["reserve", "renew"]
    summary_path = next((env["state"] / "runs").glob("*/summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["command_exit_code"] == 0
    assert summary["memory_guard"]["status"] == "ok"
    assert summary["memory_guard"]["command_started"] is True
    ledger_path = env["state"] / "memory-guard" / "v2" / "ledger.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert ledger["leases"] == []


def test_run_routes_command_through_observation_seam(env, monkeypatch):
    monkeypatch.setenv("REMRUN_FLEET_JOBS_OBSERVE", "1")
    captured = []
    original = LocalSimTransport.exec_observed

    def record(self, command, cwd, *, observation, **kwargs):
        captured.append(observation)
        return original(self, command, cwd, observation=observation, **kwargs)

    monkeypatch.setattr(LocalSimTransport, "exec_observed", record)
    code = main(["run", "LOCAL_SIM", "--", "python", "-c", "print('observed')"])
    assert code == EXIT_OK
    assert len(captured) == 1
    item = captured[0]
    assert item.project == "proj1"
    assert item.target == "LOCAL_SIM"
    assert item.phase == "command"
    assert item.command_label == "python"
    assert item.job_id


def test_run_observation_is_dormant_by_default(env, monkeypatch):
    from remrun import cli

    monkeypatch.delenv("REMRUN_FLEET_JOBS_OBSERVE", raising=False)

    def unexpected(*_args, **_kwargs):
        raise AssertionError("default-off run must not enter observation code")

    monkeypatch.setattr(LocalSimTransport, "exec_observed", unexpected)
    monkeypatch.setattr(cli.JobObservation, "for_command", classmethod(unexpected))
    code = main(["run", "LOCAL_SIM", "--", "python", "-c", "print('plain')"])
    assert code == EXIT_OK
