from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import remrun._durable_runner as durable_runner
import remrun.state as state_module
from remrun.state import LockError, ProjectLock


def test_project_lock_blocks_scoped_locks(tmp_path: Path):
    lock = ProjectLock("proj", "MACBOX", state_root=tmp_path).acquire()
    try:
        with pytest.raises(LockError):
            ProjectLock("proj", "WINBOX", state_root=tmp_path, scope="spec_a").acquire()
    finally:
        lock.release()


def test_scoped_locks_block_project_lock(tmp_path: Path):
    scoped = ProjectLock("proj", "MACBOX", state_root=tmp_path, scope="spec_a").acquire()
    try:
        with pytest.raises(LockError):
            ProjectLock("proj", "WINBOX", state_root=tmp_path).acquire()
    finally:
        scoped.release()


def test_different_scopes_serialize_until_attribution_is_scope_aware(tmp_path: Path):
    a = ProjectLock("proj", "MACBOX", state_root=tmp_path, scope="spec_a").acquire()
    try:
        with pytest.raises(LockError):
            ProjectLock("proj", "WINBOX", state_root=tmp_path, scope="spec_b").acquire()
    finally:
        a.release()


def test_same_scope_serializes_across_targets(tmp_path: Path):
    lock = ProjectLock("proj", "MACBOX", state_root=tmp_path, scope="spec_a").acquire()
    try:
        with pytest.raises(LockError):
            ProjectLock("proj", "WINBOX", state_root=tmp_path, scope="spec_a").acquire()
    finally:
        lock.release()


def test_abandoned_guard_error_names_manual_recovery_path(
    tmp_path: Path,
    monkeypatch,
):
    lock = ProjectLock("proj", "MACBOX", state_root=tmp_path)
    lock.guard.mkdir(parents=True)
    times = iter((0.0, 6.0))
    monkeypatch.setattr(state_module.time, "time", lambda: next(times))
    monkeypatch.setattr(state_module.time, "sleep", lambda _seconds: None)

    with pytest.raises(LockError) as exc:
        lock.acquire()

    assert str(lock.guard) in str(exc.value)


def _held_by(tmp_path: Path, *, pid: int, run_id: str | None = "run-old",
             scope: str | None = None) -> ProjectLock:
    owner = ProjectLock("proj", "MACBOX", state_root=tmp_path, scope=scope,
                        run_id=run_id).acquire()
    info_path = owner.path / "info.json"
    info = json.loads(info_path.read_text(encoding="utf-8"))
    info["pid"] = pid
    info_path.write_text(json.dumps(info), encoding="utf-8")
    return owner


def _liveness(monkeypatch, states: dict[int, bool | None]) -> None:
    monkeypatch.setattr(state_module, "pid_liveness", lambda pid: states[pid])


def test_dead_holder_is_reclaimed_and_reported(tmp_path: Path, monkeypatch):
    owner = _held_by(tmp_path, pid=4242, scope="spec_a")
    acquired_at = json.loads((owner.path / "info.json").read_text())["acquired_at"]
    _liveness(monkeypatch, {4242: False})
    events: list[tuple[str, dict]] = []

    contender = ProjectLock(
        "proj", "WINBOX", state_root=tmp_path, run_id="run-new",
        report=lambda name, **fields: events.append((name, fields)),
    ).acquire()
    try:
        assert not owner.path.exists()
        info = json.loads((contender.path / "info.json").read_text(encoding="utf-8"))
        assert (info["target"], info["run_id"]) == ("WINBOX", "run-new")
        assert info["pid"] == state_module.os.getpid()
        assert events == [("stale_lock_reclaimed", {
            "path": str(owner.path), "run_id": "run-old", "pid": 4242,
            "target": "MACBOX", "scope": "spec_a", "acquired_at": acquired_at,
        })]
    finally:
        contender.release()


@pytest.mark.parametrize(
    ("liveness", "reason", "waitable"),
    [
        (True, "holder pid 4242 is running", True),
        # A probe that cannot prove death (e.g. a failed Windows OpenProcess) must
        # not be read as "dead": pid reuse or a privileged holder looks the same.
        (None, "cannot prove holder pid 4242 has exited", False),
    ],
)
def test_holder_not_proven_dead_is_kept(
    tmp_path: Path, monkeypatch, liveness, reason, waitable
):
    owner = _held_by(tmp_path, pid=4242)
    _liveness(monkeypatch, {4242: liveness})

    with pytest.raises(LockError) as exc:
        ProjectLock("proj", "WINBOX", state_root=tmp_path).acquire()

    assert owner.path.is_dir()
    assert str(owner.path) in str(exc.value)
    assert "pid=4242" in str(exc.value)
    assert f"not reclaimed: {reason}" in str(exc.value)
    assert exc.value.waitable is waitable


def test_unknown_completion_hazard_blocks_reclaim_of_dead_holder(
    tmp_path: Path, monkeypatch
):
    # The controller died after fencing its remote command: the remote work may
    # still be running, so a dead local pid is not enough to take the project.
    owner = _held_by(tmp_path, pid=4242)
    state_module.write_unknown_completion_hazard(
        "proj", "MACBOX", "run-old", tmp_path
    )
    _liveness(monkeypatch, {4242: False})
    events: list[str] = []

    with pytest.raises(LockError) as exc:
        ProjectLock("proj", "WINBOX", state_root=tmp_path,
                    report=lambda name, **_f: events.append(name)).acquire()

    assert owner.path.is_dir()
    assert events == []
    message = str(exc.value)
    assert "holder pid 4242 is not running, but unknown-completion hazard" in message
    assert "remrun resolve-unknown run-old --confirmed-ended" in message
    assert exc.value.waitable is False


def test_unreadable_hazard_blocks_reclaim_of_dead_holder(tmp_path: Path, monkeypatch):
    owner = _held_by(tmp_path, pid=4242)
    hazard = state_module.unknown_completion_hazard_path("proj", tmp_path)
    hazard.parent.mkdir(parents=True)
    hazard.write_text("{torn", encoding="utf-8")
    _liveness(monkeypatch, {4242: False})

    with pytest.raises(LockError) as exc:
        ProjectLock("proj", "WINBOX", state_root=tmp_path).acquire()

    assert owner.path.is_dir()
    assert "hazard is unreadable" in str(exc.value)
    assert hazard.read_text(encoding="utf-8") == "{torn"


@pytest.mark.parametrize(
    ("metadata", "expected", "reason"),
    [
        (None, "metadata=missing", "lock metadata is missing"),
        ("{not-json", "metadata=invalid", "lock metadata is unreadable"),
        ("[4242]", "metadata=invalid", "lock metadata is unreadable"),
        ('{"pid": "4242"}', "pid=4242", "lock metadata has no integer pid"),
        ('{"pid": true}', "pid=True", "lock metadata has no integer pid"),
    ],
)
def test_unknown_lock_metadata_fails_closed_with_path_and_evidence(
    tmp_path: Path,
    monkeypatch,
    metadata: str | None,
    expected: str,
    reason: str,
):
    owner = ProjectLock("proj", "MACBOX", state_root=tmp_path)
    owner.path.mkdir(parents=True)
    if metadata is not None:
        (owner.path / "info.json").write_text(metadata, encoding="utf-8")
    # Every pid would read as dead; only the metadata itself may keep the lock.
    monkeypatch.setattr(state_module, "pid_liveness", lambda _pid: False)

    contender = ProjectLock("proj", "WINBOX", state_root=tmp_path)
    with pytest.raises(LockError) as exc:
        contender.acquire(wait_seconds=60)

    message = str(exc.value)
    assert owner.path.is_dir()
    assert str(owner.path) in message
    assert expected in message
    assert f"not reclaimed: {reason}" in message
    assert exc.value.waitable is False


def test_active_holder_evidence_remains_visible_and_blocks(tmp_path: Path):
    owner = ProjectLock("proj", "MACBOX", state_root=tmp_path).acquire()
    try:
        contender = ProjectLock("proj", "WINBOX", state_root=tmp_path)
        with pytest.raises(LockError) as exc:
            contender.acquire()

        message = str(exc.value)
        assert str(owner.path) in message
        assert "target=MACBOX" in message
        assert f"pid={state_module.os.getpid()}" in message
    finally:
        owner.release()


def test_wait_lock_acquires_once_live_holder_releases(tmp_path: Path, monkeypatch):
    owner = _held_by(tmp_path, pid=4242)
    _liveness(monkeypatch, {4242: True})
    events: list[str] = []
    releaser = threading.Timer(0.2, owner.release)
    releaser.start()
    try:
        contender = ProjectLock(
            "proj", "WINBOX", state_root=tmp_path,
            report=lambda name, **_f: events.append(name),
        ).acquire(wait_seconds=30)
    finally:
        releaser.join()
    contender.release()
    assert events == ["lock_wait"]


def test_wait_lock_gives_up_after_timeout(tmp_path: Path, monkeypatch):
    owner = _held_by(tmp_path, pid=4242)
    _liveness(monkeypatch, {4242: True})
    events: list[str] = []
    started = time.monotonic()

    with pytest.raises(LockError) as exc:
        ProjectLock(
            "proj", "WINBOX", state_root=tmp_path,
            report=lambda name, **_f: events.append(name),
        ).acquire(wait_seconds=1.2)

    elapsed = time.monotonic() - started
    assert 1.2 <= elapsed < 5
    assert events == ["lock_wait"]
    assert "gave up after waiting 1.2s" in str(exc.value)
    assert owner.path.is_dir()


def test_wait_lock_stops_when_holder_dies_leaving_its_hazard(tmp_path: Path, monkeypatch):
    owner = _held_by(tmp_path, pid=4242, run_id="run-live")
    state_module.write_unknown_completion_hazard("proj", "MACBOX", "run-live", tmp_path)
    states: dict[int, bool | None] = {4242: True}
    _liveness(monkeypatch, states)
    killer = threading.Timer(0.2, states.__setitem__, (4242, False))
    killer.start()
    started = time.monotonic()
    try:
        with pytest.raises(LockError) as exc:
            ProjectLock("proj", "WINBOX", state_root=tmp_path).acquire(wait_seconds=60)
    finally:
        killer.join()

    assert time.monotonic() - started < 5
    assert "is not running, but unknown-completion hazard for run run-live" in str(exc.value)
    assert "gave up after waiting" not in str(exc.value)
    assert owner.path.is_dir()


def test_wait_lock_never_waits_on_a_hazard_without_a_live_owner(
    tmp_path: Path, monkeypatch
):
    _held_by(tmp_path, pid=4242, run_id="run-live")
    state_module.write_unknown_completion_hazard("proj", "MACBOX", "run-gone", tmp_path)
    _liveness(monkeypatch, {4242: True})
    monkeypatch.setattr(
        state_module.time, "sleep",
        lambda _s: pytest.fail("waited on an unowned unknown-completion hazard"),
    )

    with pytest.raises(LockError) as exc:
        ProjectLock("proj", "WINBOX", state_root=tmp_path).acquire(wait_seconds=60)

    assert exc.value.waitable is False


def test_hazard_owned_by_live_holder_is_waitable(tmp_path: Path, monkeypatch):
    # A running run fences its own command; waiting for it to finish is safe because
    # the waiter re-checks the hazard after taking the lock.
    owner = _held_by(tmp_path, pid=4242, run_id="run-live")
    state_module.write_unknown_completion_hazard("proj", "MACBOX", "run-live", tmp_path)
    _liveness(monkeypatch, {4242: True})

    with pytest.raises(LockError) as exc:
        ProjectLock("proj", "WINBOX", state_root=tmp_path).acquire()

    assert exc.value.waitable is True
    assert ProjectLock("proj", "WINBOX", state_root=tmp_path).live_holder_run_ids() == {
        "run-live"
    }
    owner.release()


@pytest.mark.skipif(os.name == "nt", reason="simulates a Windows host without kernel32")
def test_windows_liveness_probe_never_sends_a_signal(tmp_path: Path, monkeypatch):
    # os.kill(pid, 0) is CTRL_C_EVENT on Windows. Without kernel32 (this host), the
    # probe must report "unknown", so the lock stays held, never fall back to a signal.
    owner = ProjectLock("proj", "MACBOX", state_root=tmp_path).acquire()
    contender = ProjectLock("proj", "WINBOX", state_root=tmp_path)

    def no_signal(*_args):
        raise AssertionError("os.kill used as a liveness probe on Windows")

    monkeypatch.setattr(durable_runner.os, "kill", no_signal)
    monkeypatch.setattr(durable_runner.os, "name", "nt")
    direct = durable_runner.pid_liveness(4242)
    try:
        contender.acquire()
    except LockError as exc:
        refused = str(exc)
    else:
        refused = ""
    monkeypatch.undo()

    assert direct is None
    assert "not reclaimed: cannot prove holder pid" in refused
    assert owner.path.is_dir()
    owner.release()


@pytest.mark.skipif(os.name != "nt", reason="exercises the real Windows process API")
def test_native_windows_liveness_uses_process_handles_never_signals(tmp_path: Path, monkeypatch):
    # The real kernel32 branch: a running process is alive, an exited child whose
    # handle is still open is provably dead, and no signal is ever sent.
    def no_signal(*_args):
        raise AssertionError("os.kill used as a liveness probe on Windows")

    monkeypatch.setattr(durable_runner.os, "kill", no_signal)
    assert durable_runner.pid_liveness(os.getpid()) is True
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    child.wait()
    assert durable_runner.pid_liveness(child.pid) is False  # Popen still holds its handle

    owner = ProjectLock("proj", "MACBOX", state_root=tmp_path).acquire()
    with pytest.raises(LockError, match="not reclaimed"):
        ProjectLock("proj", "WINBOX", state_root=tmp_path).acquire()
    assert owner.path.is_dir()
    owner.release()


@pytest.mark.parametrize("value", [None, "4242", True, 0, -1])
def test_liveness_of_invalid_pid_is_unknown(value):
    assert durable_runner.pid_liveness(value) is None


def test_liveness_of_this_process_is_running():
    assert durable_runner.pid_liveness(state_module.os.getpid()) is True


def test_a_lock_whose_metadata_cannot_be_written_is_removed_not_leaked(tmp_path: Path, monkeypatch):
    """A lock directory without metadata can never be proved stale and would block every
    later run, so a failed metadata write must remove the directory it just created."""
    real_write = state_module.write_json

    def failing_write(path, data):  # noqa: ANN001
        if path.name == "info.json":
            raise OSError("injected metadata write failure")
        return real_write(path, data)

    monkeypatch.setattr(state_module, "write_json", failing_write)
    lock = ProjectLock("proj", "MACBOX", state_root=tmp_path)
    with pytest.raises(OSError):
        lock.acquire()
    assert not lock.path.exists()
    monkeypatch.setattr(state_module, "write_json", real_write)
    ProjectLock("proj", "MACBOX", state_root=tmp_path).acquire().release()


def test_a_lock_whose_metadata_rename_fails_leaves_nothing_behind(tmp_path: Path, monkeypatch):
    """The metadata is staged as info.json.tmp and renamed into place; a failure at the
    rename leaves the staged file, which must not keep the lock directory alive."""
    real_replace = Path.replace

    def failing_replace(self, target):  # noqa: ANN001
        if self.name == "info.json.tmp":
            raise OSError("injected rename failure")
        return real_replace(self, target)

    monkeypatch.setattr(Path, "replace", failing_replace)
    lock = ProjectLock("proj", "MACBOX", state_root=tmp_path)
    with pytest.raises(OSError):
        lock.acquire()
    assert not lock.path.exists()
    monkeypatch.setattr(Path, "replace", real_replace)
    ProjectLock("proj", "MACBOX", state_root=tmp_path).acquire().release()
