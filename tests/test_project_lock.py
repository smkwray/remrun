from __future__ import annotations

from pathlib import Path

import pytest

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
