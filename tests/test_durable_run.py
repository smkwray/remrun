"""Focused regressions for opt-in durable ordinary runs."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest

import remrun.cli as cli
import remrun._durable_runner as durable_runner
import remrun.state as state_module
from remrun.cli import EXIT_CONFLICT, EXIT_GUARD, EXIT_INFRA, EXIT_INTERNAL, EXIT_OK, main
from remrun._durable_runner import TRUNCATION_MARKER, _BoundedWriter, _boot_marker
from remrun.job_observation import JobObservation, command_label
from remrun.memory_guard import MemoryAdmissionResult, MemoryReservation
from remrun.state import LockError, ProjectLock
from remrun.transport import (
    DurablePrestartError,
    LocalSimTransport,
    ProbeResult,
    SSHPosixTransport,
    SSHPowerShellTransport,
    TransportError,
)
from remrun.models import Device
from remrun.profile import command_key, load_profiles


def _posix(path: Path) -> str:
    return str(path).replace("\\", "/")


@pytest.fixture()
def durable_env(tmp_path: Path, monkeypatch):
    remrun_root = tmp_path / "remrun"
    (remrun_root / "config").mkdir(parents=True)
    local_base = tmp_path / "local" / "proj"
    remote_base = tmp_path / "remote"
    remote_other = tmp_path / "remote-other"
    state_root = tmp_path / "state"
    local_base.mkdir(parents=True)
    remote_base.mkdir(parents=True)
    remote_other.mkdir(parents=True)
    (remrun_root / "config" / "defaults.toml").write_text(
        '[transfer]\nmode = "safe"\n'
        'global_exclude = ["node_modules/**", ".git/**"]\n'
        'hash_small_files_below_mb = 8\n'
        '[telemetry]\nenabled = false\n',
        encoding="utf-8",
    )
    (remrun_root / "config" / "devices.toml").write_text(
        '[project_roots]\n'
        f'default = "{_posix(local_base)}"\n'
        f'macos = "{_posix(local_base)}"\n'
        f'windows = "{_posix(local_base)}"\n\n'
        '[devices.TEST]\n'
        'enabled = true\nrole = "runner"\nkind = "ssh-posix"\nos = "posix"\n'
        'address_candidates = ["test.invalid"]\n'
        f'project_root = "{_posix(remote_base)}"\n'
        f'state_root = "{_posix(state_root / "target")}"\n'
        f'cache_root = "{_posix(tmp_path / "cache")}"\n'
        'perf_cores = 8\n'
        '[devices.OTHER]\n'
        'enabled = true\nrole = "runner"\nkind = "ssh-posix"\nos = "posix"\n'
        'address_candidates = ["other.invalid"]\n'
        f'project_root = "{_posix(remote_other)}"\n'
        f'state_root = "{_posix(state_root / "target-other")}"\n'
        f'cache_root = "{_posix(tmp_path / "cache-other")}"\n'
        'perf_cores = 8\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("REMRUN_ROOT", str(remrun_root))
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(state_root))
    project = local_base / "proj1"
    project.mkdir()
    monkeypatch.chdir(project)
    return {
        "project": project,
        "remote_project": remote_base / "proj1",
        "state": state_root,
    }


class _FakeDurableTransport(LocalSimTransport):
    def __init__(self, device: Device, mode: str = "complete") -> None:
        super().__init__(device)
        self.mode = mode
        self.launch_calls = 0
        self.cleanup_calls = 0
        self.exec_calls = 0
        self.status: dict[str, object] | None = None
        self.stdout = ""
        self.stderr = ""
        self.command_seen: list[str] = []
        self.reservation_seen: MemoryReservation | None = None
        self.load: float | None = None
        # Connection-loss simulation for the controller's wait loop: status and
        # observer reads raise while ``unreachable`` is set, or for the next
        # ``fail_status_reads`` status reads.
        self.unreachable = False
        self.fail_status_reads = 0
        self.status_reads = 0
        self.final_status: dict[str, object] | None = None
        if mode == "flaky":
            self.fail_status_reads = 2
        # The target observer's registry row for the launched run, and how the
        # target answers `cancel`: "confirm" (the supervisor then records the
        # stop), "confirm_unsettled" (stop confirmed, exit not yet recorded),
        # or "refuse" (no exact row).
        self.observation_seen: JobObservation | None = None
        self.cancel_mode = "confirm"
        self.cancel_calls: list[tuple[JobObservation, str]] = []

    def probe(self) -> ProbeResult:
        return ProbeResult(True, "fake", remote_os="posix")

    def sample_load(self) -> float | None:
        return self.load

    def exec(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.exec_calls += 1
        return super().exec(*args, **kwargs)

    def launch_durable(
        self,
        command: list[str],
        cwd: str,
        *,
        run_id: str,
        resume_token: str,
        observation: JobObservation,
        controller: str,
        project_id: str,
        max_log_bytes: int,
        created_at: str,
        env: dict[str, str] | None = None,
        path_prepend: list[str] | None = None,
        telemetry: bool = False,
        telemetry_request=None,  # noqa: ANN001
        memory_reservation: MemoryReservation | None = None,
    ) -> tuple[dict[str, object], dict[str, object]]:
        del resume_token, max_log_bytes, created_at, telemetry, telemetry_request
        self.launch_calls += 1
        self.command_seen = list(command)
        self.observation_seen = observation
        self.reservation_seen = memory_reservation
        identity = {
            "schema": 1,
            "run_id": run_id,
            "project_id": project_id,
            "target": self.device.name,
            "controller": controller,
            "command_sha256": observation.command_sha256,
            "wrapper_exit_code": None,
        }
        if self.mode == "preack_loss":
            raise TransportError("injected request loss before acknowledgement")
        if self.mode == "prestart_refusal":
            raise DurablePrestartError("injected guard renewal refusal")
        if self.mode == "detached":
            # The launch stream drops after acknowledgement and the target then
            # stays unreachable for the controller.
            self.status = {
                **identity,
                "state": "running",
                "acknowledged": True,
                "command_started": True,
                "detached_after_ack": True,
            }
            self.unreachable = True
            return dict(self.status), {"platform": "POSIX", "telemetry": "none"}
        result = super().exec(
            command,
            cwd,
            env=env,
            path_prepend=path_prepend,
            telemetry=False,
        )
        self.stdout, self.stderr = result.stdout, result.stderr
        complete = {
            **identity,
            "state": "complete",
            "acknowledged": True,
            "command_started": True,
            "wrapper_exit_code": result.exit_code,
            # Target-clock timestamps, as the durable supervisor records them.
            "acknowledged_at": 1000.0,
            "ended_at": 1012.5,
        }
        if self.mode in ("reconnect", "flaky"):
            # The command finished on the target, but the launch stream dropped
            # after acknowledgement while the target stays reachable.
            self.final_status = complete
            self.status = {
                **identity,
                "state": "running",
                "acknowledged": True,
                "command_started": True,
                "detached_after_ack": True,
            }
            return dict(self.status), {"platform": "POSIX", "telemetry": "none"}
        self.status = complete
        return dict(self.status), {"platform": "POSIX", "telemetry": "none"}

    def durable_status(
        self, run_id: str, resume_token: str, *, include_logs: bool = False
    ) -> dict[str, object]:
        del run_id, resume_token
        if self.status is None:
            raise TransportError("no target durable state")
        if not include_logs:
            self.status_reads += 1
            if self.unreachable:
                raise TransportError("injected connection loss during status read")
            if self.fail_status_reads > 0:
                self.fail_status_reads -= 1
                raise TransportError("injected transient status read failure")
            if self.final_status is not None:
                # The first successful status read observes the finished command.
                self.status, self.final_status = self.final_status, None
        if include_logs:
            return {
                "status": dict(self.status),
                "stdout_b64": base64.b64encode(self.stdout.encode()).decode(),
                "stderr_b64": base64.b64encode(self.stderr.encode()).decode(),
            }
        return dict(self.status)

    def durable_cleanup(self, run_id: str, resume_token: str) -> dict[str, object]:
        del run_id, resume_token
        self.cleanup_calls += 1
        return {"cleaned": True}

    def cancel_observed(self, observation, *, cwd, timeout=30.0):  # noqa: ANN001
        del timeout
        self.cancel_calls.append((observation, cwd))
        registered = self.observation_seen
        exact = ("job_id", "project", "source_controller", "target", "phase", "command_sha256")
        if self.cancel_mode == "refuse" or registered is None or any(
            getattr(observation, key) != getattr(registered, key) for key in exact
        ):
            return False, "remrun cancel refused: no registered job has this exact identity"
        # The command had written partial output before it was stopped.
        (Path(cwd) / "partial.txt").write_text("partial", encoding="utf-8")
        if self.cancel_mode == "confirm":
            assert self.status is not None
            self.status = {
                **self.status, "state": "complete", "wrapper_exit_code": 137,
                "acknowledged_at": 1000.0, "ended_at": 1001.0,
            }
        return True, ""

    def query_observed_jobs(self, *, sample_interval=0.05, timeout=30.0):  # noqa: ANN001
        del sample_interval, timeout
        assert self.status is not None
        if self.unreachable:
            raise TransportError("injected connection loss during observer query")
        return {
            "status": "ok",
            "jobs": [{
                "job_id": self.status["run_id"],
                "project": self.status["project_id"],
                "source_controller": self.status["controller"],
                "target": self.status["target"],
                "command": {"sha256": self.status["command_sha256"]},
                "observation_status": "observed",
            }],
        }


def _install_fake(monkeypatch, mode: str = "complete"):
    holder: dict[str, _FakeDurableTransport] = {}

    def factory(device: Device):
        if "transport" not in holder:
            holder["transport"] = _FakeDurableTransport(device, mode)
        return holder["transport"]

    monkeypatch.setattr(cli, "make_transport", factory)
    return holder


def _only_run_id(state: Path) -> str:
    runs = [path.name for path in (state / "runs").iterdir() if path.is_dir()]
    assert len(runs) == 1
    return runs[0]


def test_complete_result_logs_and_pullback_finalize_exactly_once(
    durable_env, monkeypatch, capsys
):
    holder = _install_fake(monkeypatch)
    pullbacks = 0
    real_pullback = cli.postrun_pullback

    def counted_pullback(*args, **kwargs):  # noqa: ANN002, ANN003
        nonlocal pullbacks
        pullbacks += 1
        return real_pullback(*args, **kwargs)

    monkeypatch.setattr(cli, "postrun_pullback", counted_pullback)
    script = (
        "import pathlib,sys; "
        "pathlib.Path('result.txt').write_text('durable'); "
        "print('durable-out'); print('durable-err', file=sys.stderr); sys.exit(7)"
    )
    assert main(["run", "--durable", "TEST", "--", sys.executable, "-c", script]) == 7
    fake = holder["transport"]
    assert fake.launch_calls == 1
    assert fake.cleanup_calls == 1
    assert pullbacks == 1
    assert (durable_env["project"] / "result.txt").read_text() == "durable"
    captured = capsys.readouterr()
    assert "durable-out" in captured.out
    assert "durable-err" in captured.err

    run_id = _only_run_id(durable_env["state"])
    assert main(["resume", run_id]) == 7
    assert fake.launch_calls == 1
    assert fake.cleanup_calls == 1
    assert pullbacks == 1


def test_a_labelled_durable_run_resumes_with_its_recorded_reason_and_label(
    durable_env, monkeypatch, capsys
):
    """The run record names why its device was chosen; resume must still accept it,
    while every field that decides what runs where is rebuilt and compared exactly."""
    _install_fake(monkeypatch)
    assert main(["run", "--durable", "TEST", "--label", "regen 2019", "--",
                 sys.executable, "-c", "print('ok')"]) == 0
    run_id = _only_run_id(durable_env["state"])
    summary = json.loads((durable_env["state"] / "runs" / run_id / "summary.json").read_text())
    assert (summary["plan"]["target_reason"], summary["plan"]["label"]) == ("explicit", "regen 2019")
    capsys.readouterr()
    assert main(["resume", run_id]) == 0


def test_durable_managed_command_dispatches_absolute_entrypoint(
    durable_env, monkeypatch, capsys
):
    devices = Path(durable_env["state"]).parent / "remrun" / "config" / "devices.toml"
    device_config = devices.read_text(encoding="utf-8")
    venv_root = Path(durable_env["state"]).parent / "venvs"
    devices.write_text(
        device_config.replace(
            'address_candidates = ["test.invalid"]\n',
            'address_candidates = ["test.invalid"]\n'
            f'venv_root = "{_posix(venv_root)}"\n',
            1,
        ),
        encoding="utf-8",
    )
    (durable_env["project"] / "uv.lock").write_text("lock-v1", encoding="utf-8")
    cfgdir = durable_env["project"] / "do" / "remrun"
    cfgdir.mkdir(parents=True)
    step = (
        "import sys; from pathlib import Path; "
        "v=Path(sys.argv[1]); b=v/'bin'; b.mkdir(parents=True,exist_ok=True); "
        "p=b/'python'; p.exists() or p.symlink_to(sys.executable)"
    )
    (cfgdir / "remrun.toml").write_text(
        "[run]\n"
        "use_venv = true\n"
        "venv_layout = \"external\"\n"
        "[run.bootstrap]\n"
        "schema = 1\n"
        'lock_inputs = ["uv.lock"]\n'
        f"argv = [\"{{python}}\", \"-c\", {json.dumps(step)}, \"{{venv}}\"]\n",
        encoding="utf-8",
    )
    holder = _install_fake(monkeypatch)

    assert main([
        "run", "--durable", "TEST", "--", "python", "-c", "print('ok')"
    ]) == EXIT_OK
    capsys.readouterr()
    fake = holder["transport"]
    assert fake.command_seen == [
        str(venv_root / "proj1" / "bin" / "python"), "-c", "print('ok')"
    ]


def test_resume_completes_finalization_checkpoint_without_second_pullback(
    durable_env, monkeypatch
):
    holder = _install_fake(monkeypatch)
    pullbacks = 0
    real_pullback = cli.postrun_pullback

    def counted_pullback(*args, **kwargs):  # noqa: ANN002, ANN003
        nonlocal pullbacks
        pullbacks += 1
        return real_pullback(*args, **kwargs)

    monkeypatch.setattr(cli, "postrun_pullback", counted_pullback)
    assert main(["run", "--durable", "TEST", "--", sys.executable, "-c", "print('ok')"]) == EXIT_OK
    run_id = _only_run_id(durable_env["state"])
    summary_path = durable_env["state"] / "runs" / run_id / "summary.json"
    summary = json.loads(summary_path.read_text())
    summary["completion_state"] = "finalization_complete"
    summary["terminal"] = False
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    cli.write_unknown_completion_hazard("proj1", "TEST", run_id)

    assert main(["resume", run_id]) == EXIT_OK
    assert pullbacks == 1
    assert holder["transport"].launch_calls == 1
    recovered = json.loads(summary_path.read_text())
    assert recovered["completion_state"] == "complete"
    assert recovered["terminal"] is True
    assert not list((durable_env["state"] / "hazards" / "project").glob("*/unknown.json"))


def test_lost_connection_after_ack_detaches_nonzero_blocks_new_run_and_rejects_mismatch(
    durable_env, monkeypatch, capsys
):
    """Losing the target after acknowledgement is not success: `run --durable && next`
    must not proceed. The job keeps running, so every message points at `resume`."""
    holder = _install_fake(monkeypatch, "detached")
    monkeypatch.setattr(cli, "DURABLE_STATUS_RETRY_SECONDS", 0.0)
    assert main(["run", "--durable", "TEST", "--", sys.executable, "-c", "print(1)"]) == EXIT_INFRA
    fake = holder["transport"]
    assert fake.launch_calls == 1
    run_id = _only_run_id(durable_env["state"])
    assert "durable_detached" in capsys.readouterr().err
    summary = json.loads((durable_env["state"] / "runs" / run_id / "summary.json").read_text())
    assert summary["completion_state"] == "detached"
    assert f"remrun resume {run_id}" in summary["guidance"]

    # The existing one-writer completion fence blocks any new mutating run, and its
    # guidance sends the operator to resume rather than to resolve-unknown first.
    assert main(["run", "TEST", "--", sys.executable, "-c", "print('duplicate')"]) == EXIT_INTERNAL
    assert f"remrun resume {run_id}" in capsys.readouterr().err
    assert fake.launch_calls == 1
    assert fake.exec_calls == 0

    fake.unreachable = False
    assert main(["resume", run_id, "--no-wait"]) == EXIT_OK
    assert fake.launch_calls == 1
    assert fake.status is not None
    fake.status["target"] = "OTHER"
    assert main(["resume", run_id]) == EXIT_INTERNAL


def test_resume_rejects_a_different_controller_identity(durable_env, monkeypatch):
    holder = _install_fake(monkeypatch, "detached")
    monkeypatch.setattr(cli, "DURABLE_STATUS_RETRY_SECONDS", 0.0)
    monkeypatch.setattr(cli, "controller_label", lambda: "controller-a")
    assert main(["run", "--durable", "TEST", "--", sys.executable, "-c", "print(1)"]) == EXIT_INFRA
    run_id = _only_run_id(durable_env["state"])
    holder["transport"].unreachable = False
    monkeypatch.setattr(cli, "controller_label", lambda: "controller-b")
    assert main(["resume", run_id, "--no-wait"]) == EXIT_INTERNAL
    assert holder["transport"].launch_calls == 1


def test_launch_stream_loss_after_ack_keeps_waiting_and_finalizes(
    durable_env, monkeypatch, capsys
):
    """A dropped launch stream is not completion: while the target stays reachable
    the controller keeps waiting, then finalizes once and pulls outputs back."""
    holder = _install_fake(monkeypatch, "reconnect")
    script = "import pathlib,sys; pathlib.Path('result.txt').write_text('late'); sys.exit(7)"
    assert main(["run", "--durable", "TEST", "--", sys.executable, "-c", script]) == 7
    assert (durable_env["project"] / "result.txt").read_text() == "late"
    assert holder["transport"].cleanup_calls == 1
    err = capsys.readouterr().err
    assert "durable_launch_reconnected" in err
    assert "durable_detached" not in err


def test_transient_status_read_failures_are_retried_with_backoff(
    durable_env, monkeypatch, capsys
):
    """An SSH blip during the wait is retried (read-only, always safe) with a growing
    delay instead of detaching on the first failed read."""
    holder = _install_fake(monkeypatch, "flaky")
    sleeps: list[float] = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)
    script = "import pathlib,sys; pathlib.Path('result.txt').write_text('ok'); sys.exit(7)"
    assert main(["run", "--durable", "TEST", "--", sys.executable, "-c", script]) == 7
    assert holder["transport"].fail_status_reads == 0
    assert sleeps[:3] == [1.0, 2.0, 4.0]
    err = capsys.readouterr().err
    assert err.count("durable_status_retry") == 2
    assert "durable_detached" not in err
    assert (durable_env["project"] / "result.txt").read_text() == "ok"


def test_durable_run_records_target_measured_duration_and_learns_its_cost(
    durable_env, monkeypatch
):
    """Durable jobs must feed the learned costs that admission and --auto use, timed
    by the target's own clock; a failed command teaches nothing."""
    _install_fake(monkeypatch)
    ok_cmd = [sys.executable, "-c", "print('ok')"]
    assert main(["run", "--durable", "TEST", "--", *ok_cmd]) == EXIT_OK
    run_id = _only_run_id(durable_env["state"])
    summary = json.loads((durable_env["state"] / "runs" / run_id / "summary.json").read_text())
    assert summary["duration_sec"] == 12.5
    learned = load_profiles(durable_env["state"])["proj1"][command_key(ok_cmd)]["TEST"]
    assert learned["exec_s"] == 12.5

    fail_cmd = [sys.executable, "-c", "raise SystemExit(3)"]
    assert main(["run", "--durable", "TEST", "--", *fail_cmd]) == 3
    assert command_key(fail_cmd) not in load_profiles(durable_env["state"])["proj1"]


def test_loss_before_positive_ack_is_unknown_and_never_retried(durable_env, monkeypatch):
    holder = _install_fake(monkeypatch, "preack_loss")
    assert main(["run", "--durable", "TEST", "--", sys.executable, "-c", "print(1)"]) == EXIT_INFRA
    fake = holder["transport"]
    assert fake.launch_calls == 1
    assert fake.exec_calls == 0
    run_id = _only_run_id(durable_env["state"])
    summary = json.loads((durable_env["state"] / "runs" / run_id / "summary.json").read_text())
    assert summary["completion_state"] == "unknown"
    assert main(["resume", run_id]) == EXIT_INTERNAL
    assert fake.launch_calls == 1


def test_auto_selects_once_then_binds_the_durable_target(
    durable_env, monkeypatch, capsys
):
    transports: dict[str, _FakeDurableTransport] = {}

    def factory(device: Device):
        transport = transports.setdefault(device.name, _FakeDurableTransport(device))
        transport.load = 90.0 if device.name == "TEST" else 0.0
        return transport

    monkeypatch.setattr(cli, "make_transport", factory)
    assert main([
        "run", "--durable", "--auto", "--", sys.executable, "-c", "print('bound')",
    ]) == EXIT_OK

    assert transports["TEST"].launch_calls == 0
    assert transports["OTHER"].launch_calls == 1
    run_id = _only_run_id(durable_env["state"])
    record = json.loads(
        (durable_env["state"] / "runs" / run_id / "durable.json").read_text()
    )
    assert record["target"] == "OTHER"
    assert record["plan"]["target"]["name"] == "OTHER"
    assert "durable_target_bound" in capsys.readouterr().err


def test_auto_never_falls_back_after_a_durable_launch_attempt(
    durable_env, monkeypatch
):
    transports: dict[str, _FakeDurableTransport] = {}

    def factory(device: Device):
        mode = "preack_loss" if device.name == "TEST" else "complete"
        transport = transports.setdefault(device.name, _FakeDurableTransport(device, mode))
        transport.load = None
        return transport

    monkeypatch.setattr(cli, "make_transport", factory)
    assert main([
        "run", "--durable", "--auto", "--", sys.executable, "-c", "print('once')",
    ]) == EXIT_INFRA

    assert transports["TEST"].launch_calls == 1
    assert transports["OTHER"].launch_calls == 0
    run_id = _only_run_id(durable_env["state"])
    summary = json.loads(
        (durable_env["state"] / "runs" / run_id / "summary.json").read_text()
    )
    assert summary["target"] == "TEST"
    assert summary["completion_state"] == "unknown"


def test_auto_skips_candidates_without_a_durable_transport(
    durable_env, monkeypatch, capsys
):
    unsupported = Device(
        name="LOCAL",
        enabled=True,
        role="runner",
        kind="local-sim",
        os="posix",
        address_candidates=(),
        project_root=str(durable_env["remote_project"].parent),
        state_root=str(durable_env["state"] / "local-target"),
        cache_root=str(durable_env["state"] / "local-cache"),
    )
    transport = _FakeDurableTransport(unsupported)
    monkeypatch.setattr(
        cli,
        "_resolve_targets",
        lambda *_args, **_kwargs: [
            (unsupported, transport, ProbeResult(True, "local", remote_os="posix"), "auto")
        ],
    )

    assert main([
        "run", "--durable", "--auto", "--", sys.executable, "-c", "print('no')",
    ]) == EXIT_INFRA
    assert transport.launch_calls == 0
    assert "durable_unsupported_transport" in capsys.readouterr().err


def test_conclusive_prestart_refusal_clears_completion_fence(durable_env, monkeypatch):
    holder = _install_fake(monkeypatch, "prestart_refusal")
    assert main(["run", "--durable", "TEST", "--", sys.executable, "-c", "print(1)"]) == EXIT_GUARD
    fake = holder["transport"]
    assert fake.launch_calls == 1
    assert not list((durable_env["state"] / "hazards" / "project").glob("*/unknown.json"))
    run_id = _only_run_id(durable_env["state"])
    summary = json.loads((durable_env["state"] / "runs" / run_id / "summary.json").read_text())
    assert summary["completion_state"] == "not_started"
    assert summary["command_started"] is False


def test_ordinary_run_without_flag_does_not_enter_durable_path(durable_env, monkeypatch):
    holder = _install_fake(monkeypatch)
    assert main([
        "run", "TEST", "--", sys.executable, "-c",
        "import pathlib; pathlib.Path('ordinary.txt').write_text('ordinary')",
    ]) == EXIT_OK
    fake = holder["transport"]
    assert fake.launch_calls == 0
    assert fake.exec_calls == 1
    assert (durable_env["project"] / "ordinary.txt").read_text() == "ordinary"


# --- untransferred runs (--transfer none) ---------------------------------------


class _ConcurrentDurableTransport(_FakeDurableTransport):
    """A target that keeps every launched run's durable state, as a real one does.

    Launches detach after acknowledgement (the target then looks unreachable) until a
    test calls ``finish`` and clears ``unreachable``.
    """

    def __init__(self, device: Device) -> None:
        super().__init__(device, "detached")
        self.runs: dict[str, dict[str, object]] = {}
        self.observations: list[JobObservation] = []
        self.pulls = 0

    def launch_durable(self, command, cwd, *, run_id, observation, **kwargs):  # noqa: ANN001, ANN003
        status, execution = super().launch_durable(
            command, cwd, run_id=run_id, observation=observation, **kwargs
        )
        self.runs[run_id] = dict(status)
        self.observations.append(observation)
        return status, execution

    def exec_observed(self, command, cwd, *, observation, **kwargs):  # noqa: ANN001, ANN003
        self.observations.append(observation)
        return self.exec(command, cwd, **kwargs)

    def finish(self, run_id: str, exit_code: int = 0) -> None:
        self.runs[run_id].update(
            state="complete", wrapper_exit_code=exit_code,
            acknowledged_at=1000.0, ended_at=1012.5,
        )

    def durable_status(self, run_id, resume_token, *, include_logs=False):  # noqa: ANN001
        del resume_token
        if self.unreachable:
            raise TransportError("injected connection loss during status read")
        status = dict(self.runs[run_id])
        if include_logs:
            return {"status": status, "stdout_b64": "", "stderr_b64": ""}
        return status

    def query_observed_jobs(self, *, sample_interval=0.05, timeout=30.0):  # noqa: ANN001
        del sample_interval, timeout
        if self.unreachable:
            raise TransportError("injected connection loss during observer query")
        return {"status": "ok", "jobs": [
            {
                "job_id": status["run_id"], "project": status["project_id"],
                "source_controller": status["controller"], "target": status["target"],
                "command": {"sha256": status["command_sha256"]},
                "observation_status": "observed",
            }
            for status in self.runs.values() if status["state"] != "complete"
        ]}

    def pull_file(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.pulls += 1
        return super().pull_file(*args, **kwargs)

    def pull_files(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.pulls += 1
        return super().pull_files(*args, **kwargs)


def _install_concurrent(monkeypatch) -> dict[str, _ConcurrentDurableTransport]:
    transports: dict[str, _ConcurrentDurableTransport] = {}
    monkeypatch.setattr(
        cli, "make_transport",
        lambda device: transports.setdefault(device.name, _ConcurrentDurableTransport(device)),
    )
    monkeypatch.setattr(cli, "DURABLE_STATUS_RETRY_SECONDS", 0.0)
    return transports


def _set_transfer_mode(env: dict, mode: str) -> None:
    """Set the deployment default ``[transfer] mode``, as an owner would."""
    defaults = env["state"].parent / "remrun" / "config" / "defaults.toml"
    text = defaults.read_text(encoding="utf-8")
    defaults.write_text(
        re.sub(r'(?m)^mode = "[a-z]+"$', f'mode = "{mode}"', text), encoding="utf-8"
    )


def _run_ids(state: Path) -> set[str]:
    runs = state / "runs"
    return {path.name for path in runs.iterdir() if path.is_dir()} if runs.exists() else set()


def _launch(state: Path, argv: list[str]) -> tuple[int, str | None]:
    """Run ``main(argv)``; return its exit code and the one run it recorded, if any."""
    before = _run_ids(state)
    code = main(argv)
    new = _run_ids(state) - before
    assert len(new) <= 1
    return code, next(iter(new), None)


def _summary(state: Path, run_id: str) -> dict:
    return json.loads((state / "runs" / run_id / "summary.json").read_text(encoding="utf-8"))


def _run_fences(state: Path) -> set[str]:
    return {
        json.loads(path.read_text(encoding="utf-8"))["run_id"]
        for path in (state / "hazards" / "project").glob("*/runs/*.json")
    }


def _project_fences(state: Path) -> list[Path]:
    return list((state / "hazards" / "project").glob("*/unknown.json"))


def _counted_pullbacks(monkeypatch) -> list[int]:
    calls: list[int] = []
    real_pullback = cli.postrun_pullback

    def counted(*args, **kwargs):  # noqa: ANN002, ANN003
        calls.append(1)
        return real_pullback(*args, **kwargs)

    monkeypatch.setattr(cli, "postrun_pullback", counted)
    return calls


LONG_JOB = [sys.executable, "-c", "print('long job')"]


def test_two_untransferred_durable_runs_on_one_project_launch_concurrently(
    durable_env, monkeypatch
):
    """Two long jobs of one project must both be in flight at once. The per-project
    completion fence, not the lock, used to refuse the second for as long as the first
    ran. Neither job moved a file, so neither may take the project lock or write a
    baseline that a later ordinary run would trust."""
    state = durable_env["state"]
    _set_transfer_mode(durable_env, "none")
    durable_env["remote_project"].mkdir()
    transports = _install_concurrent(monkeypatch)

    first_code, first = _launch(state, ["run", "--durable", "TEST", "--", *LONG_JOB])
    second_code, second = _launch(state, ["run", "--durable", "TEST", "--", *LONG_JOB])

    # Both detached while still running on the target (exit 4 is "not finished").
    assert (first_code, second_code) == (EXIT_INFRA, EXIT_INFRA)
    assert transports["TEST"].launch_calls == 2
    assert _run_fences(state) == {first, second}
    assert not _project_fences(state)
    for run_id in (first, second):
        summary = _summary(state, run_id)
        assert summary["completion_state"] == "detached"
        assert summary["plan"]["transfer_mode"] == "none"
    # Neither took the project's own lock (only per-run waiter locks exist); they only
    # held its table's guard while publishing their fences.
    project_locks = state / "locks" / "project" / hashlib.sha256(b"proj1").hexdigest()[:16]
    assert not (project_locks / "whole.lock").exists()
    assert not (project_locks / "scopes").exists()
    assert not (state / "manifests").exists()


def test_reconciling_run_refuses_a_target_where_an_untransferred_run_works(
    durable_env, monkeypatch, capsys
):
    """An untransferred job works in the target's checkout without the project lock.
    A reconciling run there could change files under it and pull back its half-written
    outputs, so that target is refused (a candidate-local conflict, so --auto moves on);
    other targets stay usable."""
    state = durable_env["state"]
    _set_transfer_mode(durable_env, "none")
    durable_env["remote_project"].mkdir()
    transports = _install_concurrent(monkeypatch)
    code, untransferred = _launch(state, ["run", "--durable", "TEST", "--", *LONG_JOB])
    assert code == EXIT_INFRA and _run_fences(state) == {untransferred}
    capsys.readouterr()

    code, refused = _launch(state, ["run", "--transfer", "safe", "TEST", "--", *LONG_JOB])
    assert code == cli.EXIT_CONFLICT
    assert "target-busy-untransferred" in capsys.readouterr().err
    assert _summary(state, refused)["completion_state"] == "not_started"
    assert transports["TEST"].exec_calls == 0
    assert _run_fences(state) == {untransferred}

    code, _other = _launch(state, ["run", "--transfer", "safe", "OTHER", "--", *LONG_JOB])
    assert code == EXIT_OK


_MARK_JOB = [sys.executable, "-c", "import pathlib; pathlib.Path('ran.txt').write_text('ran')"]


def _ranked_fakes(monkeypatch) -> dict[str, _FakeDurableTransport]:  # noqa: ANN001
    """One fake per device, with TEST ranked first under --auto."""
    transports: dict[str, _FakeDurableTransport] = {}

    def factory(device: Device):
        transport = transports.setdefault(device.name, _FakeDurableTransport(device))
        transport.load = 0.0 if device.name == "TEST" else 90.0
        return transport

    monkeypatch.setattr(cli, "make_transport", factory)
    return transports


@pytest.mark.parametrize(
    ("holder_target", "liveness", "refused"),
    [
        ("TEST", True, True),
        ("TEST", None, True),  # a holder not proven dead may still be reconciling
        ("TEST", False, False),
        ("OTHER", True, False),
    ],
)
def test_untransferred_run_refuses_a_target_an_ordinary_run_holds(
    durable_env, monkeypatch, capsys, holder_target, liveness, refused
):
    """An ordinary run holding the project lock may be reconciling the target's checkout
    or pulling it back. An untransferred job launched there would read files as they
    change and leave half-written outputs for that pullback, so the target is refused
    before anything is dispatched, as a candidate-local conflict. A dead holder, or one
    working on another target, is no reason to refuse."""
    state = durable_env["state"]
    remote = durable_env["remote_project"]
    other_remote = remote.parent.parent / "remote-other" / "proj1"
    for root in (remote, other_remote):
        root.mkdir()
    transports = _ranked_fakes(monkeypatch)
    owner = ProjectLock("proj1", holder_target, run_id="ordinary-run").acquire()
    info_path = owner.path / "info.json"
    info_path.write_text(
        json.dumps({**json.loads(info_path.read_text()), "pid": 4242}), encoding="utf-8"
    )
    real_liveness = state_module.pid_liveness
    monkeypatch.setattr(
        state_module, "pid_liveness",
        lambda pid: liveness if pid == 4242 else real_liveness(pid),
    )
    try:
        capsys.readouterr()
        code, run_id = _launch(state, ["run", "--transfer", "none", "TEST", "--", *_MARK_JOB])
        if not refused:
            assert code == EXIT_OK
            assert (remote / "ran.txt").read_text() == "ran"
            return
        assert code == EXIT_CONFLICT
        assert "target-busy-ordinary" in capsys.readouterr().err
        assert transports["TEST"].exec_calls == 0
        assert not (remote / "ran.txt").exists()
        assert not list((state / "hazards").glob("**/*.json"))
        assert _summary(state, run_id)["completion_state"] == "not_started"
        conflicts = json.loads((state / "conflicts" / run_id / "conflicts.json").read_text())
        assert [c["state"] for c in conflicts["conflicts"]] == ["target-busy-ordinary"]

        code, run_id = _launch(
            state, ["run", "--durable", "--transfer", "none", "TEST", "--", *_MARK_JOB]
        )
        assert code == EXIT_CONFLICT
        assert transports["TEST"].launch_calls == 0
        assert not (state / "runs" / run_id / "durable.json").exists()
        assert not list((state / "hazards").glob("**/*.json"))

        # Candidate-local: --auto moves on to a target the ordinary run is not using.
        assert main(["run", "--auto", "--transfer", "none", "--", *_MARK_JOB]) == EXIT_OK
        assert transports["TEST"].exec_calls == 0
        assert (other_remote / "ran.txt").read_text() == "ran"
        assert not (remote / "ran.txt").exists()
    finally:
        owner.release()


def test_untransferred_run_started_mid_reconcile_is_refused(durable_env, monkeypatch, capsys):
    """The race itself: an ordinary run has taken the lock and passed its per-run-fence
    check, and is reconciling, when an untransferred run of the project starts on the
    same target. Checking for the ordinary run and publishing the untransferred fence
    happen under the lock table's guard, so the late starter is refused."""
    durable_env["remote_project"].mkdir()
    transports = _ranked_fakes(monkeypatch)
    real_preflight = cli.preflight_reconcile
    nested: list[int] = []

    def preflight(*args, **kwargs):  # noqa: ANN002, ANN003
        if not nested:
            nested.append(main(["run", "--transfer", "none", "TEST", "--", *_MARK_JOB]))
        return real_preflight(*args, **kwargs)

    monkeypatch.setattr(cli, "preflight_reconcile", preflight)
    capsys.readouterr()

    assert main(["run", "--transfer", "safe", "TEST", "--", *LONG_JOB]) == EXIT_OK

    assert nested == [EXIT_CONFLICT]
    assert "target-busy-ordinary" in capsys.readouterr().err
    assert transports["TEST"].exec_calls == 1  # only the ordinary run's command
    assert not (durable_env["project"] / "ran.txt").exists()
    assert not (durable_env["remote_project"] / "ran.txt").exists()


def test_ordinary_run_started_after_an_untransferred_fence_is_refused(
    durable_env, monkeypatch, capsys
):
    """The other order: an untransferred run has published its fence and is running when
    an ordinary run starts on its target; the ordinary run's fence check sees it."""
    durable_env["remote_project"].mkdir()
    transports = _ranked_fakes(monkeypatch)
    fake_exec = _FakeDurableTransport.exec
    nested: list[int] = []

    def exec_and_race(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        if not nested:
            nested.append(main(["run", "--transfer", "safe", "TEST", "--", *LONG_JOB]))
        return fake_exec(self, *args, **kwargs)

    monkeypatch.setattr(_FakeDurableTransport, "exec", exec_and_race)
    capsys.readouterr()

    assert main(["run", "--transfer", "none", "TEST", "--", *_MARK_JOB]) == EXIT_OK

    assert nested == [EXIT_CONFLICT]
    assert "target-busy-untransferred" in capsys.readouterr().err
    assert transports["TEST"].exec_calls == 1
    assert (durable_env["remote_project"] / "ran.txt").read_text() == "ran"


def test_verify_mismatch_refuses_before_dispatch_without_fence(
    durable_env, monkeypatch, capsys
):
    """An untransferred run trusts the target's checkout, so --verify is the proof of
    which code runs. A differing or unreadable copy must stop the command before it
    starts and leave no fence to resolve; under --auto the job moves on to a target
    whose copy matches."""
    state = durable_env["state"]
    stale = durable_env["remote_project"]
    current = stale.parent.parent / "remote-other" / "proj1"
    for root in (stale, current):
        root.mkdir()
    (durable_env["project"] / "model.py").write_text("v2")
    (stale / "model.py").write_text("v1")
    (current / "model.py").write_text("v2")
    transports: dict[str, _FakeDurableTransport] = {}

    def factory(device: Device):
        transport = transports.setdefault(device.name, _FakeDurableTransport(device))
        transport.load = 0.0 if device.name == "TEST" else 90.0  # rank the stale one first
        return transport

    monkeypatch.setattr(cli, "make_transport", factory)
    flags = ["--transfer", "none", "--verify", "model.py"]
    job = [sys.executable, "-c", "import pathlib; pathlib.Path('ran.txt').write_text('ran')"]

    code, refused = _launch(state, ["run", "TEST", *flags, "--", *job])
    assert code == EXIT_CONFLICT
    assert transports["TEST"].exec_calls == 0
    assert not (stale / "ran.txt").exists()
    assert not list((state / "hazards").glob("**/*.json"))
    conflicts = json.loads((state / "conflicts" / refused / "conflicts.json").read_text())
    assert [(c["path"], c["state"]) for c in conflicts["conflicts"]] == [
        ("model.py", "verify-mismatch")
    ]
    assert _summary(state, refused)["command_started"] is False

    # A copy the target cannot hash refuses the same way: equality is proved, not assumed.
    (stale / "model.py").unlink()
    code, refused = _launch(state, ["run", "TEST", *flags, "--", *job])
    assert code == EXIT_CONFLICT
    assert transports["TEST"].exec_calls == 0
    conflicts = json.loads((state / "conflicts" / refused / "conflicts.json").read_text())
    assert [c["state"] for c in conflicts["conflicts"]] == ["verify-unavailable"]

    capsys.readouterr()
    before = _run_ids(state)
    assert main(["run", "--auto", *flags, "--", *job]) == EXIT_OK
    assert transports["TEST"].exec_calls == 0
    assert transports["OTHER"].exec_calls == 1
    assert (current / "ran.txt").read_text() == "ran"
    assert not (durable_env["project"] / "ran.txt").exists()
    assert "reason=preflight_conflict" in capsys.readouterr().err
    (ran,) = [r for r in _run_ids(state) - before if _summary(state, r)["target"] == "OTHER"]
    assert _summary(state, ran)["verify"] == [
        {"path": "model.py", "sha256": hashlib.sha256(b"v2").hexdigest()}
    ]
    assert not list((state / "hazards").glob("**/*.json"))


def test_resume_finalizes_one_untransferred_run_and_clears_only_its_fence(
    durable_env, monkeypatch
):
    """Finalizing an untransferred job must pull nothing back: it has no launch
    manifests, so a pullback would claim every file on the target as its output. It
    must also leave a sibling job's fence standing, and resolve-unknown likewise clears
    only the run it names."""
    state = durable_env["state"]
    _set_transfer_mode(durable_env, "none")
    durable_env["remote_project"].mkdir()
    (durable_env["remote_project"] / "target-only.txt").write_text("stays on the target")
    transports = _install_concurrent(monkeypatch)
    pullbacks = _counted_pullbacks(monkeypatch)
    _, first = _launch(state, ["run", "--durable", "TEST", "--", *LONG_JOB])
    _, second = _launch(state, ["run", "--durable", "TEST", "--", *LONG_JOB])
    fake = transports["TEST"]
    fake.unreachable = False
    fake.finish(first)

    assert main(["resume", first]) == EXIT_OK
    assert pullbacks == []
    assert fake.pulls == 0
    assert not (durable_env["project"] / "target-only.txt").exists()
    assert _run_fences(state) == {second}
    summary = _summary(state, first)
    assert (summary["completion_state"], summary["terminal"], summary["exit_code"]) == (
        "complete", True, EXIT_OK
    )
    assert fake.cleanup_calls == 1

    assert main(["resolve-unknown", first, "--confirmed-ended"]) == EXIT_INTERNAL
    assert main(["resolve-unknown", second, "--confirmed-ended"]) == EXIT_OK
    assert _run_fences(state) == set()
    assert _summary(state, second)["unknown_resolution"]["action"] == "confirmed-ended"


def test_untransferred_run_respects_an_ordinary_unknown_fence(
    durable_env, monkeypatch, capsys
):
    """An untransferred job never starts beside an ordinary run whose completion is
    unknown: that run may be rewriting the checkout the job would read. A sibling
    untransferred job's own fence, by contrast, blocks nobody."""
    state = durable_env["state"]
    durable_env["remote_project"].mkdir()
    transports = _install_concurrent(monkeypatch)
    _set_transfer_mode(durable_env, "none")
    _, untransferred = _launch(state, ["run", "--durable", "TEST", "--", *LONG_JOB])
    _set_transfer_mode(durable_env, "safe")
    # The ordinary run reconciles, so it must use a target where no untransferred job
    # works; its project-wide fence still applies to every target.
    code, ordinary = _launch(state, ["run", "--durable", "OTHER", "--", *LONG_JOB])
    assert code == EXIT_INFRA and ordinary is not None
    assert len(_project_fences(state)) == 1
    capsys.readouterr()

    code, refused = _launch(
        state, ["run", "--durable", "--transfer", "none", "TEST", "--", *LONG_JOB]
    )
    assert (code, refused) == (EXIT_INTERNAL, None)
    assert transports["TEST"].launch_calls == 1
    assert transports["OTHER"].launch_calls == 1
    assert f"remrun resume {ordinary}" in capsys.readouterr().err
    assert _run_fences(state) == {untransferred}


def test_untransferred_resume_rebuilds_the_exact_saved_plan(durable_env, monkeypatch):
    """Resume finalizes a job as the mode and verified files it ran with. Rebuilding
    from today's [transfer] mode would refuse the job (or, worse, pull back every
    target file) after an owner changes the default. A local edit made while the job
    runs must not block its resume, and resume must not wait on the project lock it
    never held."""
    state = durable_env["state"]
    durable_env["remote_project"].mkdir()
    (durable_env["project"] / "model.py").write_text("v1")
    (durable_env["remote_project"] / "model.py").write_text("v1")
    transports = _install_concurrent(monkeypatch)
    pullbacks = _counted_pullbacks(monkeypatch)
    code, run_id = _launch(state, [
        "run", "--durable", "--transfer", "none", "--verify", "model.py", "TEST",
        "--", *LONG_JOB,
    ])
    assert code == EXIT_INFRA
    record = json.loads((state / "runs" / run_id / "durable.json").read_text())
    digest = hashlib.sha256(b"v1").hexdigest()
    assert (record["plan"]["transfer_mode"], record["plan"]["verify"]) == ("none", ["model.py"])
    assert record["verify"] == [{"path": "model.py", "sha256": digest}]

    (durable_env["project"] / "model.py").write_text("v2")  # local work continues
    holder = ProjectLock("proj1", "TEST", run_id="ordinary-run").acquire()  # a live holder
    try:
        transports["TEST"].unreachable = False
        transports["TEST"].finish(run_id)
        assert main(["resume", run_id]) == EXIT_OK
        assert holder.path.is_dir()
    finally:
        holder.release()
    assert pullbacks == []
    summary = _summary(state, run_id)
    assert summary["completion_state"] == "complete"
    assert summary["verify"] == [{"path": "model.py", "sha256": digest}]


def test_untransferred_observation_uses_real_project_and_label(durable_env, monkeypatch):
    """`fleet jobs` must show concurrent untransferred jobs as what they are: the real
    project, each under its own label, never a synthetic per-job project id."""
    state = durable_env["state"]
    devices = state.parent / "remrun" / "config" / "devices.toml"
    devices.write_text(devices.read_text(encoding="utf-8").replace(
        'address_candidates = ["test.invalid"]\n',
        'address_candidates = ["test.invalid"]\nobserve_jobs = true\n', 1,
    ), encoding="utf-8")
    _set_transfer_mode(durable_env, "none")
    durable_env["remote_project"].mkdir()
    transports = _install_concurrent(monkeypatch)
    for label in ("regen 2019", "regen 2020"):
        _launch(state, ["run", "--durable", "TEST", "--label", label, "--", *LONG_JOB])
    transports["TEST"].unreachable = False
    assert main(["run", "TEST", "--label", "smoke", "--", *LONG_JOB]) == EXIT_OK

    assert [(o.project, o.command_label) for o in transports["TEST"].observations] == [
        ("proj1", command_label(LONG_JOB, label))
        for label in ("regen 2019", "regen 2020", "smoke")
    ]


def test_successful_untransferred_run_records_learned_cost(durable_env, monkeypatch):
    """Placement and admission learn job costs from completed runs. An untransferred
    run pulls nothing back, so its successful command is the whole evidence, for
    ordinary and durable runs alike; a failed command still teaches nothing."""
    state = durable_env["state"]
    _set_transfer_mode(durable_env, "none")
    durable_env["remote_project"].mkdir()
    _install_fake(monkeypatch)
    write = "import pathlib; pathlib.Path('out.txt').write_text({!r})"
    ordinary = [sys.executable, "-c", write.format("ordinary")]
    durable = [sys.executable, "-c", write.format("durable")]
    failing = [sys.executable, "-c", "raise SystemExit(3)"]

    assert main(["run", "TEST", "--", *ordinary]) == EXIT_OK
    assert main(["run", "--durable", "TEST", "--", *durable]) == EXIT_OK
    assert main(["run", "TEST", "--", *failing]) == 3

    learned = load_profiles(state)["proj1"]
    assert learned[command_key(ordinary)]["TEST"]["exec_s"] > 0
    assert learned[command_key(durable)]["TEST"]["exec_s"] == 12.5  # the target's clock
    assert command_key(failing) not in learned
    assert (durable_env["remote_project"] / "out.txt").read_text() == "durable"
    assert not (durable_env["project"] / "out.txt").exists()


def test_prune_keeps_summaries_pinned_by_per_run_fences(durable_env, monkeypatch):
    """Retention must never delete the summary a per-run fence points at: resume and
    resolve-unknown record their outcome there. An unreadable fence stops pruning
    rather than being taken as absent; a resolved run is ordinary history again."""
    state = durable_env["state"]
    _set_transfer_mode(durable_env, "none")
    durable_env["remote_project"].mkdir()
    _install_concurrent(monkeypatch)
    _, run_id = _launch(state, ["run", "--durable", "TEST", "--", *LONG_JOB])

    assert main(["clean", "--keep", "0"]) == EXIT_OK
    assert (state / "runs" / run_id / "summary.json").is_file()
    assert _run_fences(state) == {run_id}

    (fence,) = (state / "hazards" / "project").glob("*/runs/*.json")
    good = fence.read_bytes()
    fence.write_text("{corrupt", encoding="utf-8")
    assert main(["clean", "--keep", "0"]) == EXIT_INTERNAL
    assert (state / "runs" / run_id / "summary.json").is_file()
    assert fence.read_text(encoding="utf-8") == "{corrupt"
    fence.write_bytes(good)

    assert main(["resolve-unknown", run_id, "--confirmed-ended"]) == EXIT_OK
    assert main(["clean", "--keep", "0"]) == EXIT_OK
    assert not (state / "runs" / run_id).exists()


def _helper_call(helper: Path, *args: str, input_bytes: bytes | None = None):
    return subprocess.run(
        [sys.executable, "-S", str(helper), *args],
        input=input_bytes,
        capture_output=True,
        check=False,
        timeout=10,
    )


def _target_spec(root: Path, run_id: str, token: str, *, output_bytes: int = 32):
    digest = hashlib.sha256(run_id.encode()).hexdigest()
    ready = root / "durable-runs" / run_id / "observer-ready.json"
    script = (
        "import json,pathlib,sys,time; "
        "p=pathlib.Path(sys.argv[1]); "
        "p.write_text(json.dumps({'schema':1,'job_id':sys.argv[2],"
        "'command_sha256':sys.argv[3]})); "
        "print('X'*int(sys.argv[4])); print('stderr-line',file=sys.stderr); "
        "time.sleep(.15); raise SystemExit(9)"
    )
    return {
        "schema": 1,
        "run_id": run_id,
        "resume_token": token,
        "controller": "controller-a",
        "project_id": "project-a",
        "target": "target-a",
        "command_sha256": digest,
        "argv": [sys.executable, "-c", script, str(ready), run_id, digest, str(output_bytes)],
        "ready_path": str(ready),
        "max_log_bytes": 96,
        "created_at": "2026-08-03T18:00:00Z",
    }


@pytest.mark.skipif(os.name != "posix", reason="POSIX observer ordering gate")
def test_observer_ready_record_exists_before_user_code(tmp_path: Path):
    observer = Path(cli.__file__).with_name("_job_observer.py")
    root = tmp_path / "observer-state"
    ready = root / "durable-runs" / "run-ready" / "observer-ready.json"
    observed = JobObservation.for_command(
        job_id="run-ready", project="project", target="target", phase="command",
        command=[sys.executable, "-c", "user"], source_controller="controller",
    )
    user = (
        "import pathlib,sys; "
        "p=pathlib.Path(sys.argv[1]); "
        "assert p.exists(); pathlib.Path(sys.argv[2]).write_text('started-after-ready')"
    )
    marker_path = tmp_path / "user-started.txt"
    proc = subprocess.run(
        [
            sys.executable, "-S", str(observer), "run", "--state-root", str(root),
            "--metadata-b64", observed.encoded(), "--ready-file", str(ready), "--",
            sys.executable, "-c", user, str(ready), str(marker_path),
        ],
        capture_output=True, check=False, timeout=10,
    )
    assert proc.returncode == 0, proc.stderr.decode()
    assert marker_path.read_text() == "started-after-ready"
    payload = json.loads(ready.read_text())
    assert payload["job_id"] == "run-ready"
    assert payload["command_sha256"] == observed.command_sha256


def test_target_supervisor_ack_order_no_duplicate_exact_result_and_bounded_cleanup(tmp_path: Path):
    helper = Path(cli.__file__).with_name("_durable_runner.py")
    root = tmp_path / "target-state"
    spec = _target_spec(root, "run-a", "secret-token", output_bytes=1000)
    raw = json.dumps(spec, separators=(",", ":")).encode()
    launch = _helper_call(helper, "launch", "--state-root", str(root), input_bytes=raw)
    assert launch.returncode == 0, launch.stderr.decode()
    launched = json.loads(launch.stdout)
    assert launched["acknowledged"] is True
    assert launched["state"] in {"running", "complete"}

    # Reissuing the exact launch adopts state and must not execute user code again.
    duplicate = _helper_call(helper, "launch", "--state-root", str(root), input_bytes=raw)
    assert duplicate.returncode == 0
    deadline = time.monotonic() + 5
    result = None
    while time.monotonic() < deadline:
        probe = _helper_call(
            helper, "status", "--state-root", str(root), "--run-id", "run-a",
            "--resume-token", "secret-token", "--include-logs",
        )
        if probe.returncode == 0:
            result = json.loads(probe.stdout)
            break
        time.sleep(0.05)
    assert result is not None
    status = result["status"]
    assert status["state"] == "complete"
    assert status["wrapper_exit_code"] == 9
    assert status["stdout_truncated"] is True
    stdout = base64.b64decode(result["stdout_b64"])
    stderr = base64.b64decode(result["stderr_b64"])
    assert len(stdout) <= 96
    # The helper writes this through Python's text stream, so the line terminator is
    # the target platform's (CRLF on Windows). Compare the semantic line: the runner's
    # contract is that it returns the child's bytes unaltered, not that it rewrites
    # them to LF. Asserting a literal \n here tests the platform, not the runner.
    assert stderr.decode().splitlines() == ["stderr-line"]

    wrong = _helper_call(
        helper, "status", "--state-root", str(root), "--run-id", "run-a",
        "--resume-token", "wrong-token",
    )
    assert wrong.returncode == 2
    cleanup = _helper_call(
        helper, "cleanup", "--state-root", str(root), "--run-id", "run-a",
        "--resume-token", "secret-token",
    )
    assert cleanup.returncode == 0
    assert not (root / "durable-runs" / "run-a").exists()


def test_status_cannot_replace_terminal_write_after_its_fresh_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "target-state"
    run_id = "run-terminal-race"
    token = "secret-token"
    rdir = root / "durable-runs" / run_id
    ready = rdir / "observer-ready.json"
    gate = rdir / "start-gate.json"
    started = rdir / "started.json"
    release = tmp_path / "release-command"
    ledger = tmp_path / "resource-ledger.json"
    operations = tmp_path / "resource-operations.log"
    resource_runner = tmp_path / "resource-runner.py"
    request_sha = hashlib.sha256(b"terminal-status-race").hexdigest()
    receipt = {
        "allocation_id": run_id,
        "operation_id": run_id,
        "request_sha256": request_sha,
        "fence": 1,
        "policy_generation": 0,
        "policy_digest": "0" * 64,
        "state": "RESERVED",
        "command_start_state": "NO",
    }
    ledger.write_text(json.dumps(receipt), encoding="utf-8")
    resource_runner.write_text(
        """from __future__ import annotations
import json
import sys
from pathlib import Path

request = json.loads(sys.stdin.buffer.read().decode("utf-8"))
ledger = Path(sys.argv[2]) / "resource-ledger.json"
operations = Path(sys.argv[2]) / "resource-operations.log"
receipt = json.loads(ledger.read_text(encoding="utf-8"))
operation = request["operation"]
with operations.open("a", encoding="utf-8") as handle:
    handle.write(operation + "\\n")
if operation == "claim":
    receipt.update(state="CLAIMED", command_start_state="NO")
elif operation == "start":
    receipt["command_start_state"] = request["values"]["state"]
elif operation == "exec_confirm":
    receipt["command_start_state"] = "YES"
elif operation == "finish":
    receipt.update(state="RELEASED", terminal_reason="explicit_finish")
ledger.write_text(json.dumps(receipt), encoding="utf-8")
sys.stdout.write(json.dumps({"ok": True, "receipt": receipt}))
""",
        encoding="utf-8",
    )
    command = [
        sys.executable,
        "-S",
        "-c",
        (
            "import sys,time\n"
            "from pathlib import Path\n"
            "path=Path(sys.argv[1])\n"
            "while not path.exists(): time.sleep(0.01)\n"
        ),
        str(release),
    ]
    observation = JobObservation.for_command(
        job_id=run_id,
        project="@fleet",
        source_controller="controller-a",
        target="TARGET",
        phase="fleet-worker",
        command=command,
    )
    spec = {
        "schema": 1,
        "run_id": run_id,
        "resume_token": token,
        "controller": "controller-a",
        "project_id": "@fleet",
        "target": "TARGET",
        "command_sha256": observation.command_sha256,
        "argv": [
            sys.executable,
            "-S",
            str(Path(cli.__file__).with_name("_job_observer.py")),
            "run",
            "--state-root", str(root),
            "--metadata-b64", observation.encoded(),
            "--ready-file", str(ready),
            "--start-gate-file", str(gate),
            "--started-file", str(started),
            "--",
            *command,
        ],
        "ready_path": str(ready),
        "max_log_bytes": 1024 * 1024,
        "created_at": "2026-08-26T00:00:00Z",
        "acceptance": {
            "schema": 1,
            "runner_path": str(resource_runner),
            "state_root": str(tmp_path),
            "operation_id": run_id,
            "request_sha256": request_sha,
            "reservation": {
                "allocation_id": run_id,
                "fence": 1,
                "policy_generation": 0,
                "policy_digest": "0" * 64,
            },
            "start_gate_path": str(gate),
            "started_path": str(started),
        },
    }
    launched = durable_runner._launch(
        root, json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()
    )
    assert launched["state"] == "running"
    assert launched["command_started"] is True

    def finish_after_supervisor_commits(
        _acceptance: dict[str, object], _token: str,
        operation: str, _values: dict[str, object],
    ) -> dict[str, object]:
        assert operation == "finish"
        release.write_text("go", encoding="utf-8")
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            terminal = json.loads((rdir / "status.json").read_text(encoding="utf-8"))
            if terminal.get("state") == "complete":
                return json.loads(ledger.read_text(encoding="utf-8"))
            time.sleep(0.01)
        pytest.fail("real supervisor did not commit its terminal status")

    # Force the real status path into its absent-supervisor recovery branch, then
    # arrange for the real detached supervisor to commit complete after the
    # branch's authenticated re-read but before its attempted failed write.
    monkeypatch.setattr(durable_runner, "_pid_alive", lambda _pid: False)
    monkeypatch.setattr(durable_runner, "_resource_transition", finish_after_supervisor_commits)

    observed = durable_runner._status(root, run_id, token, False)

    assert observed["state"] == "complete"
    assert observed["wrapper_exit_code"] == 0
    assert json.loads((rdir / "status.json").read_text(encoding="utf-8"))["state"] == "complete"
    assert operations.read_text(encoding="utf-8").splitlines()[-1] == "finish"


def test_target_corrupt_or_missing_state_fails_closed(tmp_path: Path):
    helper = Path(cli.__file__).with_name("_durable_runner.py")
    root = tmp_path / "target-state"
    missing = _helper_call(
        helper, "status", "--state-root", str(root), "--run-id", "missing",
        "--resume-token", "token",
    )
    assert missing.returncode == 2

    spec = _target_spec(root, "run-b", "token-b")
    launch = _helper_call(
        helper, "launch", "--state-root", str(root),
        input_bytes=json.dumps(spec, separators=(",", ":")).encode(),
    )
    assert launch.returncode == 0
    # Launch acknowledgement intentionally precedes detached completion. Wait
    # until the supervisor has made its final legitimate write before corrupting
    # the durable record; otherwise the test races that write and may observe a
    # newly valid status instead of the injected corruption.
    status_path = root / "durable-runs" / "run-b" / "status.json"
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            settled_status = json.loads(status_path.read_text(encoding="utf-8"))
            if settled_status.get("state") == "complete":
                break
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        time.sleep(0.05)
    else:
        pytest.fail("durable supervisor did not reach terminal state")
    status_path.write_text("{corrupt", encoding="utf-8")
    corrupt = _helper_call(
        helper, "status", "--state-root", str(root), "--run-id", "run-b",
        "--resume-token", "token-b",
    )
    assert corrupt.returncode == 2


def test_bounded_spool_preserves_control_tail(tmp_path: Path):
    path = tmp_path / "stderr.log"
    writer = _BoundedWriter(path, 96)
    writer.write(b"H" * 1000)
    writer.write(b"TAIL-CONTROL-RECORD")
    writer.close()
    data = path.read_bytes()
    assert len(data) <= 96
    assert data.startswith(b"H")
    assert TRUNCATION_MARKER in data
    assert data.endswith(b"TAIL-CONTROL-RECORD")


def test_boot_marker_is_stable_across_immediate_status_samples():
    samples = {_boot_marker() for _ in range(20)}
    assert len(samples) == 1
    if sys.platform == "darwin":
        assert next(iter(samples)).startswith("darwin:")


def test_windows_supervisor_breaks_away_from_the_source_ssh_job(monkeypatch):
    monkeypatch.setattr(durable_runner.os, "name", "nt")
    flags = durable_runner._detached_flags()["creationflags"]
    assert flags & getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0x01000000)


def test_resume_lock_adopts_only_the_exact_dead_durable_run(
    tmp_path: Path, monkeypatch
):
    original = ProjectLock(
        "project", "TARGET", state_root=tmp_path, run_id="run-a"
    ).acquire()
    # Resume runs only while the durable run's completion fence stands; that fence
    # keeps any other run from reclaiming the dead holder's lock.
    state_module.write_unknown_completion_hazard("project", "TARGET", "run-a", tmp_path)
    monkeypatch.setattr(state_module, "pid_liveness", lambda _pid: False)

    with pytest.raises(LockError):
        ProjectLock(
            "project", "TARGET", state_root=tmp_path,
            run_id="run-b", adopt_dead_run=True,
        ).acquire()

    adopted = ProjectLock(
        "project", "TARGET", state_root=tmp_path,
        run_id="run-a", adopt_dead_run=True,
    ).acquire()
    info = json.loads((adopted.path / "info.json").read_text(encoding="utf-8"))
    assert info["run_id"] == "run-a"
    adopted.release()
    # The original holder represents the killed process; never release it after adoption.
    del original


def test_durable_helper_integrity_check_is_cached_per_transport(
    tmp_path: Path, monkeypatch
):
    transport = SSHPosixTransport(_posix_device(tmp_path))
    calls: list[tuple[Path, str]] = []
    monkeypatch.setattr(
        transport,
        "_ensure_remote_helper_exact",
        lambda source, target: calls.append((source, target)),
    )
    assert transport._ensure_durable_runner() == transport._ensure_durable_runner()
    assert len(calls) == 1


def _posix_device(tmp_path: Path, *, guarded: bool = False) -> Device:
    raw_guard = None
    if guarded:
        raw_guard = {
            "schema": 3,
            "command_limit_fraction": 0.5,
            "host_reserve_fraction": 0.1,
        }
    return Device(
        name="POSIX", enabled=True, role="runner", kind="ssh-posix", os="posix",
        address_candidates=["host"], project_root="/projects", state_root=str(tmp_path),
        cache_root=str(tmp_path / "cache"), memory_guard=raw_guard,
    )


def test_posix_transport_forwards_guard_and_observer_before_user(tmp_path: Path, monkeypatch):
    transport = SSHPosixTransport(_posix_device(tmp_path, guarded=True))
    reservation = MemoryReservation(
        lease_id="lease", lease_token="lease-token", state_root=str(tmp_path),
        allowance_bytes=100, control_overhead_bytes=10, capacity_bytes=110,
        max_command_bytes=100, min_available_bytes=20, host_total_bytes=1000,
        safe_concurrency=1, expires_at=time.time() + 60,
        allowance_basis="unprofiled_available_backed",
    )
    admitted = MemoryAdmissionResult(
        "admitted", "ok", "renewed", {"schema": 1}, reservation
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(transport, "_ensure_job_observer", lambda: (str(tmp_path), "/observer.py"))
    monkeypatch.setattr(transport, "_ensure_durable_runner", lambda: (str(tmp_path), "/durable.py"))
    monkeypatch.setattr(transport, "_ensure_memory_guard_helper", lambda: "/guard.py")
    monkeypatch.setattr(transport, "renew_memory_guard", lambda value: admitted)

    def control(operation, **kwargs):  # noqa: ANN001
        captured.update(json.loads(kwargs["input_bytes"]))
        return {
            "schema": 1, "run_id": "run", "project_id": "project", "target": "POSIX",
            "controller": "controller", "command_sha256": observation.command_sha256,
            "state": "running", "acknowledged": True, "command_started": True,
        }

    monkeypatch.setattr(transport, "_durable_control", control)
    observation = JobObservation.for_command(
        job_id="run", project="project", target="POSIX", phase="command",
        command=["python", "job.py"], source_controller="controller",
    )
    _status, execution = transport.launch_durable(
        ["python", "job.py"], "/projects/project", run_id="run",
        resume_token="resume-token", observation=observation, controller="controller",
        project_id="project", max_log_bytes=1024, created_at="now",
        memory_reservation=reservation,
    )
    argv = captured["argv"]
    assert isinstance(argv, list)
    assert "/guard.py" in argv
    assert "/observer.py" in argv
    assert argv.index("/guard.py") < argv.index("/observer.py")
    assert "--ready-file" in argv
    assert execution["memory_reservation"]["lease_token"] == "lease-token"
    assert execution["memory_reservation"]["allowance_basis"] == (
        "unprofiled_available_backed"
    )


def test_windows_transport_builds_noninteractive_observed_durable_spec(tmp_path: Path, monkeypatch):
    device = Device(
        name="WIN", enabled=True, role="runner", kind="ssh-powershell", os="windows",
        address_candidates=["win"], project_root=r"C:\\projects", state_root=r"C:\\state",
        cache_root=r"C:\\cache", remote_python="python", shell="powershell",
    )
    transport = SSHPowerShellTransport(device)
    captured: dict[str, object] = {}
    monkeypatch.setattr(transport, "_ps_exe", lambda: "pwsh")
    monkeypatch.setattr(transport, "validate_command_context", lambda *a, **k: None)
    monkeypatch.setattr(transport, "_ensure_job_observer", lambda: (r"C:\\state", r"C:\\observer.py"))
    monkeypatch.setattr(transport, "_ensure_durable_runner", lambda: (r"C:\\state", r"C:\\durable.py"))
    monkeypatch.setattr(transport, "_ensure_remote_helper_exact", lambda source, target: None)

    def control(operation, **kwargs):  # noqa: ANN001
        captured.update(json.loads(kwargs["input_bytes"]))
        return {
            "schema": 1, "run_id": "run", "project_id": "project", "target": "WIN",
            "controller": "controller", "command_sha256": observation.command_sha256,
            "state": "running", "acknowledged": True, "command_started": True,
        }

    monkeypatch.setattr(transport, "_durable_control", control)
    observation = JobObservation.for_command(
        job_id="run", project="project", target="WIN", phase="command",
        command=["python", "job.py"], source_controller="controller",
    )
    transport.launch_durable(
        ["python", "job.py"], r"C:\\projects\\project", run_id="run",
        resume_token="resume", observation=observation, controller="controller",
        project_id="project", max_log_bytes=1024, created_at="now",
        telemetry=True,
    )
    argv = captured["argv"]
    assert isinstance(argv, list)
    assert r"C:\\observer.py" in argv
    assert r"C:\\state\helpers\remrun_win_telemetry_v1.py" in argv
    assert "--allow-observed-breakaway" in argv
    assert "--telemetry" in argv
    assert "--ready-file" in argv
    assert "-NonInteractive" in argv
    assert "-EncodedCommand" in argv


# --- remrun cancel -------------------------------------------------------------


def _detached_run(durable_env, monkeypatch):  # noqa: ANN001
    """A durable run that is still running on the target after its controller left."""
    holder = _install_fake(monkeypatch, "detached")
    monkeypatch.setattr(cli, "DURABLE_STATUS_RETRY_SECONDS", 0.0)
    assert main(["run", "--durable", "TEST", "--", sys.executable, "-c", "print(1)"]) == EXIT_INFRA
    fake = holder["transport"]
    fake.unreachable = False
    return fake, _only_run_id(durable_env["state"])


def _count_pullbacks(monkeypatch) -> list[int]:  # noqa: ANN001
    counter = [0]
    real = cli.postrun_pullback

    def counted(*args, **kwargs):  # noqa: ANN002, ANN003
        counter[0] += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(cli, "postrun_pullback", counted)
    return counter


_IDENTITY = ("job_id", "project", "source_controller", "target", "phase", "command_sha256")


def test_cancel_stops_the_exact_registered_job_then_finalizes_like_resume(
    durable_env, monkeypatch, capsys
):
    fake, run_id = _detached_run(durable_env, monkeypatch)
    pullbacks = _count_pullbacks(monkeypatch)
    rdir = durable_env["state"] / "runs" / run_id

    assert main(["cancel", run_id]) == 137

    (observation, _cwd), = fake.cancel_calls
    registered = fake.observation_seen
    assert registered is not None
    assert [getattr(observation, key) for key in _IDENTITY] == [
        getattr(registered, key) for key in _IDENTITY
    ]
    # Outputs come back as for any nonzero exit, and the fence is cleared only by
    # the ordinary finalization.
    assert pullbacks[0] == 1
    assert (durable_env["project"] / "partial.txt").read_text() == "partial"
    summary = json.loads((rdir / "summary.json").read_text())
    assert (summary["completion_state"], summary["terminal"], summary["exit_code"]) == (
        "complete", True, 137,
    )
    assert summary["cancel"]["owned_job_cancelled"] is True
    record = json.loads((rdir / "durable.json").read_text())
    assert record["cancel"]["owned_job_cancelled"] is True
    assert cli.read_unknown_completion_hazard("proj1") is None
    assert (fake.launch_calls, fake.cleanup_calls) == (1, 1)
    assert "cancel_requested" in capsys.readouterr().err

    # Repeating either verb neither signals again nor pulls outputs twice.
    assert main(["cancel", run_id]) == 137
    assert main(["resume", run_id]) == 137
    assert len(fake.cancel_calls) == 1
    assert pullbacks[0] == 1


def test_cancel_that_cannot_prove_ownership_leaves_completion_unknown(
    durable_env, monkeypatch, capsys
):
    fake, run_id = _detached_run(durable_env, monkeypatch)
    pullbacks = _count_pullbacks(monkeypatch)
    fake.cancel_mode = "refuse"
    capsys.readouterr()

    assert main(["cancel", run_id]) == EXIT_INFRA

    err = capsys.readouterr().err
    assert "completion_unknown" in err
    assert f"remrun resume {run_id}" in err
    assert fake.status is not None and fake.status["state"] == "running"
    summary = json.loads((durable_env["state"] / "runs" / run_id / "summary.json").read_text())
    assert summary["completion_state"] == "detached"
    assert summary["cancel"]["owned_job_cancelled"] is False
    assert "no registered job" in summary["cancel"]["detail"]
    hazard = cli.read_unknown_completion_hazard("proj1")
    assert hazard is not None and hazard["run_id"] == run_id
    assert (pullbacks[0], fake.cleanup_calls) == (0, 0)
    # The completion fence still blocks a new run in this project.
    assert main(["run", "TEST", "--", sys.executable, "-c", "print('next')"]) == EXIT_INTERNAL
    assert fake.exec_calls == 0


def test_cancel_never_signals_a_run_that_is_still_starting(durable_env, monkeypatch, capsys):
    fake, run_id = _detached_run(durable_env, monkeypatch)
    assert fake.status is not None
    fake.status.update(state="pending", acknowledged=False)
    capsys.readouterr()

    assert main(["cancel", run_id]) == EXIT_INFRA

    assert fake.cancel_calls == []
    assert "cancel_refused" in capsys.readouterr().err
    hazard = cli.read_unknown_completion_hazard("proj1")
    assert hazard is not None and hazard["run_id"] == run_id


def test_cancel_while_the_original_controller_waits_only_requests_the_stop(
    durable_env, monkeypatch, capsys
):
    """The waiting controller owns the summary and finalization; cancel must not race it."""
    fake, run_id = _detached_run(durable_env, monkeypatch)
    pullbacks = _count_pullbacks(monkeypatch)
    rdir = durable_env["state"] / "runs" / run_id
    summary_before = (rdir / "summary.json").read_bytes()
    # A live process holding this run's project lock is exactly a controller still
    # polling it (the original `run --durable`, or a `resume`).
    waiting = ProjectLock("proj1", "TEST", run_id=run_id).acquire()
    try:
        capsys.readouterr()
        assert main(["cancel", run_id]) == EXIT_OK
        assert len(fake.cancel_calls) == 1
        assert "cancel_handed_off" in capsys.readouterr().err
        assert (rdir / "summary.json").read_bytes() == summary_before
        assert (pullbacks[0], fake.cleanup_calls) == (0, 0)
        hazard = cli.read_unknown_completion_hazard("proj1")
        assert hazard is not None and hazard["run_id"] == run_id
        record = json.loads((rdir / "durable.json").read_text())
        assert record["cancel"]["owned_job_cancelled"] is True
    finally:
        waiting.release()
    # The waiting controller then finalizes the stopped command once.
    assert main(["resume", run_id]) == 137
    assert pullbacks[0] == 1
    assert (durable_env["project"] / "partial.txt").read_text() == "partial"


def _detached_untransferred_run(durable_env, monkeypatch):  # noqa: ANN001
    """An untransferred durable run still running on the target after its controller left."""
    _set_transfer_mode(durable_env, "none")
    durable_env["remote_project"].mkdir()
    return _detached_run(durable_env, monkeypatch)


def test_cancel_stops_an_untransferred_run_and_clears_only_its_own_fence(
    durable_env, monkeypatch
):
    """An untransferred run is fenced by its own record, not the project's, and moved
    no files: cancel must find that record, stop the job, finalize it without a
    pullback, and leave the project unfenced."""
    fake, run_id = _detached_untransferred_run(durable_env, monkeypatch)
    pullbacks = _count_pullbacks(monkeypatch)
    assert _run_fences(durable_env["state"]) == {run_id}

    assert main(["cancel", run_id]) == 137

    assert len(fake.cancel_calls) == 1
    assert pullbacks[0] == 0
    assert not (durable_env["project"] / "partial.txt").exists()
    assert _run_fences(durable_env["state"]) == set()
    assert not _project_fences(durable_env["state"])
    summary = _summary(durable_env["state"], run_id)
    assert (summary["completion_state"], summary["exit_code"]) == ("complete", 137)


def test_one_process_at_a_time_waits_on_an_untransferred_run(durable_env, monkeypatch, capsys):
    """With no project lock, the per-run lock is what stops two processes finalizing
    one untransferred run, and what tells cancel a controller is still waiting."""
    fake, run_id = _detached_untransferred_run(durable_env, monkeypatch)
    waiting = ProjectLock(f"proj1 untransferred run {run_id}", "TEST", run_id=run_id).acquire()
    try:
        assert main(["resume", run_id]) not in {EXIT_OK, 137}
        assert _summary(durable_env["state"], run_id)["completion_state"] == "detached"
        capsys.readouterr()
        assert main(["cancel", run_id]) == EXIT_OK
        assert "cancel_handed_off" in capsys.readouterr().err
        assert _run_fences(durable_env["state"]) == {run_id}
    finally:
        waiting.release()
    # The stop was requested; the next waiter finalizes it exactly once.
    assert main(["resume", run_id]) == 137
    assert _run_fences(durable_env["state"]) == set()


def test_cancel_of_a_run_that_already_finished_behaves_like_resume(durable_env, monkeypatch):
    fake, run_id = _detached_run(durable_env, monkeypatch)
    assert fake.status is not None
    fake.status = {
        **fake.status, "state": "complete", "wrapper_exit_code": 0,
        "acknowledged_at": 1000.0, "ended_at": 1002.0,
    }

    assert main(["cancel", run_id]) == EXIT_OK

    assert fake.cancel_calls == []
    summary = json.loads((durable_env["state"] / "runs" / run_id / "summary.json").read_text())
    assert (summary["completion_state"], summary["exit_code"]) == ("complete", 0)
    assert "cancel" not in summary


def test_cancel_is_restricted_to_the_creating_controller_and_project(
    durable_env, monkeypatch
):
    holder = _install_fake(monkeypatch, "detached")
    monkeypatch.setattr(cli, "DURABLE_STATUS_RETRY_SECONDS", 0.0)
    monkeypatch.setattr(cli, "controller_label", lambda: "controller-a")
    assert main(["run", "--durable", "TEST", "--", sys.executable, "-c", "print(1)"]) == EXIT_INFRA
    run_id = _only_run_id(durable_env["state"])
    fake = holder["transport"]
    fake.unreachable = False

    monkeypatch.setattr(cli, "controller_label", lambda: "controller-b")
    assert main(["cancel", run_id]) == EXIT_INTERNAL
    monkeypatch.setattr(cli, "controller_label", lambda: "controller-a")
    other = durable_env["project"].parent / "proj2"
    other.mkdir()
    monkeypatch.chdir(other)
    assert main(["cancel", run_id]) == EXIT_INTERNAL
    assert fake.cancel_calls == []


def test_confirmed_stop_without_a_recorded_exit_stays_unknown(
    durable_env, monkeypatch, capsys
):
    fake, run_id = _detached_run(durable_env, monkeypatch)
    fake.cancel_mode = "confirm_unsettled"
    monkeypatch.setattr(cli, "DURABLE_CANCEL_SETTLE_SECONDS", 0.0)
    capsys.readouterr()

    assert main(["cancel", run_id]) == EXIT_INFRA

    assert "completion_unknown" in capsys.readouterr().err
    hazard = cli.read_unknown_completion_hazard("proj1")
    assert hazard is not None and hazard["run_id"] == run_id
    summary = json.loads((durable_env["state"] / "runs" / run_id / "summary.json").read_text())
    assert summary["completion_state"] == "detached"
    assert summary["cancel"]["owned_job_cancelled"] is True


def test_posix_cancel_observed_requires_the_exact_confirmed_receipt(tmp_path, monkeypatch):
    from remrun.transport import ExecResult

    transport = SSHPosixTransport(_posix_device(tmp_path))
    monkeypatch.setattr(transport, "_ensure_job_observer", lambda: (str(tmp_path), "/observer.py"))
    observation = JobObservation.for_command(
        job_id="run", project="project", target="POSIX", phase="command",
        command=["python", "job.py"], source_controller="controller",
    )
    exact = {
        "schema": 1, "job_id": "run", "command_sha256": observation.command_sha256,
        "owned_job_cancelled": True,
    }
    replies = {
        "confirmed": ExecResult(0, json.dumps(exact), ""),
        "other job": ExecResult(0, json.dumps({**exact, "job_id": "other"}), ""),
        "refused": ExecResult(
            2, json.dumps({**exact, "owned_job_cancelled": False}),
            "remrun cancel refused: no registered job has this exact identity\n",
        ),
        "malformed": ExecResult(0, "not json", ""),
    }
    seen: list[tuple[list[str], str]] = []
    outcomes = {}
    for case, reply in replies.items():
        def control(command, *, cwd, timeout, reply=reply):  # noqa: ANN001
            del timeout
            seen.append((command, cwd))
            return reply

        monkeypatch.setattr(transport, "exec_control", control)
        outcomes[case] = transport.cancel_observed(observation, cwd="/projects/project")

    assert outcomes["confirmed"] == (True, "")
    assert outcomes["other job"][0] is False
    assert outcomes["refused"] == (
        False, "remrun cancel refused: no registered job has this exact identity",
    )
    assert outcomes["malformed"][0] is False
    assert seen[0] == (
        ["python3", "-S", "/observer.py", "cancel", "--state-root", str(tmp_path),
         "--metadata-b64", observation.encoded()],
        "/projects/project",
    )
