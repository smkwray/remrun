"""Focused regressions for opt-in durable ordinary runs."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

import remrun.cli as cli
import remrun._durable_runner as durable_runner
from remrun.cli import EXIT_GUARD, EXIT_INFRA, EXIT_INTERNAL, EXIT_OK, main
from remrun._durable_runner import TRUNCATION_MARKER, _BoundedWriter, _boot_marker
from remrun.job_observation import JobObservation
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
        self.reservation_seen: MemoryReservation | None = None
        self.load: float | None = None

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
            self.status = {
                **identity,
                "state": "running",
                "acknowledged": True,
                "command_started": True,
                "detached_after_ack": True,
            }
            return dict(self.status), {"platform": "POSIX", "telemetry": "none"}
        result = super().exec(
            command,
            cwd,
            env=env,
            path_prepend=path_prepend,
            telemetry=False,
        )
        self.stdout, self.stderr = result.stdout, result.stderr
        self.status = {
            **identity,
            "state": "complete",
            "acknowledged": True,
            "command_started": True,
            "wrapper_exit_code": result.exit_code,
        }
        return dict(self.status), {"platform": "POSIX", "telemetry": "none"}

    def durable_status(
        self, run_id: str, resume_token: str, *, include_logs: bool = False
    ) -> dict[str, object]:
        del run_id, resume_token
        if self.status is None:
            raise TransportError("no target durable state")
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

    def query_observed_jobs(self, *, sample_interval=0.05, timeout=30.0):  # noqa: ANN001
        del sample_interval, timeout
        assert self.status is not None
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


def test_acknowledged_request_loss_detaches_blocks_new_run_and_rejects_mismatch(
    durable_env, monkeypatch, capsys
):
    holder = _install_fake(monkeypatch, "detached")
    assert main(["run", "--durable", "TEST", "--", sys.executable, "-c", "print(1)"]) == EXIT_OK
    fake = holder["transport"]
    assert fake.launch_calls == 1
    run_id = _only_run_id(durable_env["state"])
    assert "durable_detached" in capsys.readouterr().err

    # The existing one-writer completion fence blocks any new mutating run.
    assert main(["run", "TEST", "--", sys.executable, "-c", "print('duplicate')"]) == EXIT_INTERNAL
    assert fake.launch_calls == 1
    assert fake.exec_calls == 0

    assert main(["resume", run_id, "--no-wait"]) == EXIT_OK
    assert fake.launch_calls == 1
    assert fake.status is not None
    fake.status["target"] = "OTHER"
    assert main(["resume", run_id]) == EXIT_INTERNAL


def test_resume_rejects_a_different_controller_identity(durable_env, monkeypatch):
    holder = _install_fake(monkeypatch, "detached")
    monkeypatch.setattr(cli, "controller_label", lambda: "controller-a")
    assert main(["run", "--durable", "TEST", "--", sys.executable, "-c", "print(1)"]) == EXIT_OK
    run_id = _only_run_id(durable_env["state"])
    monkeypatch.setattr(cli, "controller_label", lambda: "controller-b")
    assert main(["resume", run_id, "--no-wait"]) == EXIT_INTERNAL
    assert holder["transport"].launch_calls == 1


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
    assert stderr == b"stderr-line\n"

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
    monkeypatch.setattr(ProjectLock, "_pid_is_alive", staticmethod(lambda _pid: False))

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
