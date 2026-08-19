from __future__ import annotations

import json
from pathlib import Path

import pytest

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


def test_dead_pid_lock_names_path_and_manual_removal_recovers(tmp_path: Path):
    owner = ProjectLock("proj", "MACBOX", state_root=tmp_path).acquire()
    info_path = owner.path / "info.json"
    info = json.loads(info_path.read_text(encoding="utf-8"))
    info["pid"] = 2_147_483_647
    info_path.write_text(json.dumps(info), encoding="utf-8")

    contender = ProjectLock("proj", "WINBOX", state_root=tmp_path)
    with pytest.raises(LockError) as exc:
        contender.acquire()

    message = str(exc.value)
    assert str(owner.path) in message
    assert "pid=2147483647" in message

    # The documented recovery is deliberately manual: after confirming the holder
    # PID is dead, remove the exact printed lock directory and retry.
    info_path.unlink()
    owner.path.rmdir()
    recovered = contender.acquire()
    recovered.release()


@pytest.mark.parametrize(
    ("metadata", "expected"),
    [
        (None, "metadata=missing"),
        ("{not-json", "metadata=invalid"),
    ],
)
def test_unknown_lock_metadata_fails_closed_with_path_and_evidence(
    tmp_path: Path,
    metadata: str | None,
    expected: str,
):
    owner = ProjectLock("proj", "MACBOX", state_root=tmp_path)
    owner.path.mkdir(parents=True)
    if metadata is not None:
        (owner.path / "info.json").write_text(metadata, encoding="utf-8")

    contender = ProjectLock("proj", "WINBOX", state_root=tmp_path)
    with pytest.raises(LockError) as exc:
        contender.acquire()

    message = str(exc.value)
    assert str(owner.path) in message
    assert expected in message


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
