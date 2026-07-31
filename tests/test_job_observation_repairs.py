from __future__ import annotations

import hashlib
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from remrun import _job_observer as observer
from remrun.job_observation import JobObservation
from remrun.models import Device
from remrun.transport import SSHPosixTransport


def _metadata(command: list[str]) -> JobObservation:
    return JobObservation.for_command(
        job_id="repair-proof",
        project="project-a",
        source_controller="CTRL",
        target="TARGET",
        phase="command",
        command=command,
    )


def _wait_record(root: Path, *, predicate=None, timeout: float = 5.0) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        records, _ = observer._read_records(root)
        if records and (predicate is None or predicate(records[0])):
            return records[0]
        time.sleep(0.02)
    raise AssertionError("observer record did not appear")


def _wait_missing(pid: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.03)
    raise AssertionError(f"pid {pid} did not exit")


def _local_remote(_address, script, input_bytes=None, timeout=None, on_stdout=None):
    del on_stdout
    return subprocess.run(
        ["/bin/bash", "-c", script],
        input=input_bytes,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _transport(shell: str, state_root: Path) -> SSHPosixTransport:
    device = Device.from_mapping(
        "LOCAL",
        {
            "kind": "ssh-posix",
            "os": "linux",
            "address_candidates": ["local"],
            "project_root": str(state_root / "project"),
            "state_root": str(state_root),
            "cache_root": str(state_root / "cache"),
            "shell": shell,
            "remote_python": sys.executable,
            "login_shell": True,
        },
    )
    transport = SSHPosixTransport(device)
    transport._address = "local"
    transport._remote = _local_remote
    return transport


@pytest.mark.skipif(os.name != "posix", reason="POSIX shell/process-group proof")
def test_profile_startup_sentinel_executes_exactly_once(tmp_path, monkeypatch):
    sentinel = tmp_path / "profile-sentinel.log"
    profile = tmp_path / "profile"
    profile.write_text(f"printf 'profile\\n' >> {str(sentinel)!r}\n", encoding="utf-8")
    shell = tmp_path / "profile-shell"
    shell.write_text(
        "#!/bin/sh\n"
        f"if [ \"$1\" = '-lc' ]; then . {str(profile)!r}; fi\n"
        "exec /bin/bash \"$@\"\n",
        encoding="utf-8",
    )
    shell.chmod(0o755)
    transport = _transport(str(shell), tmp_path / "state")
    monkeypatch.setattr(
        transport,
        "_ensure_job_observer",
        lambda: (str(tmp_path / "state"), str(Path(observer.__file__))),
    )

    result = transport.exec_observed(
        [sys.executable, "-S", "-c", "print('ok')"],
        str(tmp_path),
        observation=_metadata([sys.executable, "-S", "-c", "print('ok')"]),
    )

    assert result.exit_code == 0, result.stderr
    assert result.stdout.strip() == "ok"
    assert sentinel.read_text(encoding="utf-8").splitlines() == ["profile"]


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group proof")
def test_normal_root_exit_keeps_surviving_descendant_visible(tmp_path):
    pid_file = tmp_path / "descendant.pid"
    child_code = (
        "import subprocess,sys; "
        "p=subprocess.Popen([sys.executable,'-S','-c','import time; time.sleep(30)'],"
        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); "
        f"open({str(pid_file)!r},'w').write(str(p.pid))"
    )
    command = [sys.executable, "-S", "-c", child_code]
    result = subprocess.run(
        [
            sys.executable,
            str(Path(observer.__file__)),
            "run",
            "--state-root",
            str(tmp_path / "state"),
            "--metadata-b64",
            _metadata(command).encoded(),
            "--",
            *command,
        ],
        capture_output=True,
        text=True,
        timeout=8,
        check=False,
    )
    descendant = int(pid_file.read_text())
    try:
        assert result.returncode == 0, result.stderr
        payload = observer._query(tmp_path / "state", 0.05)
        assert len(payload["jobs"]) == 1
        assert payload["jobs"][0]["state"] == "RUNNING"
        assert payload["jobs"][0]["processes"]["current_count"] >= 1
    finally:
        try:
            os.kill(descendant, signal.SIGKILL)
        except ProcessLookupError:
            pass


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group proof")
def test_wrapper_loss_then_root_exit_is_visible_but_not_false_running(tmp_path):
    state = tmp_path / "state"
    pid_file = tmp_path / "descendant.pid"
    child_code = (
        "import subprocess,sys,time; "
        "p=subprocess.Popen([sys.executable,'-S','-c','import time; time.sleep(30)'],"
        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); "
        f"open({str(pid_file)!r},'w').write(str(p.pid)); "
        "time.sleep(0.6)"
    )
    command = [sys.executable, "-S", "-c", child_code]
    wrapper = subprocess.Popen(
        [
            sys.executable,
            str(Path(observer.__file__)),
            "run",
            "--state-root",
            str(state),
            "--metadata-b64",
            _metadata(command).encoded(),
            "--",
            *command,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    descendant = None
    root_pid = None
    try:
        record = _wait_record(
            state, predicate=lambda item: int(item["root_pid"]) != wrapper.pid
        )
        root_pid = int(record["root_pid"])
        deadline = time.monotonic() + 5
        while not pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        descendant = int(pid_file.read_text())
        os.kill(wrapper.pid, signal.SIGKILL)
        wrapper.wait(timeout=5)
        _wait_missing(root_pid)

        payload = observer._query(state, 0.05)
        assert len(payload["jobs"]) == 1
        job = payload["jobs"][0]
        assert job["state"] == "UNKNOWN"
        assert job["memory"]["current_bytes"] is None
        assert job["cpu"]["current_pct_one_logical_cpu"] is None
    finally:
        if wrapper.poll() is None:
            wrapper.kill()
            wrapper.wait(timeout=5)
        for pid in (root_pid, descendant):
            if pid:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


@pytest.mark.skipif(os.name != "posix", reason="local SSH-POSIX command-semantics proof")
def test_observed_posix_preserves_command_semantics_and_telemetry(tmp_path):
    state = tmp_path / "state"
    cwd = tmp_path / "work dir"
    binary_dir = tmp_path / "custom bin"
    cwd.mkdir()
    binary_dir.mkdir()
    tool = binary_dir / "observer-probe"
    tool.write_text(
        "#!/bin/sh\n"
        "printf 'cwd=%s env=%s arg=%s\\n' \"$PWD\" \"$OBS_ENV\" \"$1\"\n"
        "printf 'stderr-line\\n' >&2\n"
        "exit 37\n",
        encoding="utf-8",
    )
    tool.chmod(0o755)
    transport = _transport("/bin/bash", state)
    command = ["observer-probe", "arg with spaces"]

    result = transport.exec_observed(
        command,
        str(cwd),
        observation=_metadata(command),
        env={"OBS_ENV": "value with spaces"},
        path_prepend=[str(binary_dir)],
        telemetry=True,
    )

    assert result.exit_code == 37
    assert result.stdout == (
        f"cwd={cwd} env=value with spaces arg=arg with spaces\n"
    )
    assert result.stderr == "stderr-line\n"
    assert result.telemetry is not None
    assert result.telemetry["wall_sec"] >= 0


@pytest.mark.skipif(os.name != "posix", reason="local SSH-POSIX helper staging proof")
def test_corrupt_existing_helper_is_replaced_before_user_code(tmp_path, monkeypatch):
    state = tmp_path / "state"
    transport = _transport("/bin/bash", state)
    source = Path(observer.__file__)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()[:16]
    helper = state / "helpers" / f"remrun_job_observer_v1_{digest}.py"
    helper.parent.mkdir(parents=True)
    helper.write_bytes(b"this is a corrupt truncated helper\n")
    marker = tmp_path / "user-ran.txt"
    pushes = []

    monkeypatch.setattr(transport, "remote_path_exists", lambda _path: True)

    def local_hash(path: str) -> str:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()

    def atomic_push(local_path: Path, remote_path: str) -> None:
        pushes.append(remote_path)
        destination = Path(remote_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = destination.with_suffix(destination.suffix + ".tmp")
        shutil.copyfile(local_path, temp)
        os.replace(temp, destination)

    monkeypatch.setattr(transport, "_job_helper_sha256", local_hash)
    monkeypatch.setattr(transport, "push_file", atomic_push)
    command = [
        sys.executable,
        "-S",
        "-c",
        f"from pathlib import Path; Path({str(marker)!r}).write_text('once')",
    ]
    result = transport.exec_observed(
        command,
        str(tmp_path),
        observation=_metadata(command),
    )

    assert result.exit_code == 0, result.stderr
    assert marker.read_text() == "once"
    assert helper.read_bytes() == source.read_bytes()
    assert pushes == [str(helper)]
