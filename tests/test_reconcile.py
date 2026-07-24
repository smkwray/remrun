"""End-to-end reconcile-engine tests against the LOCAL_SIM transport."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from remrun.manifest import build_manifest
from remrun.models import Device
from remrun.reconcile import postrun_pullback, preflight_reconcile
from remrun.transport import LocalSimTransport, TransportError


def make_transport(remote_base: Path) -> LocalSimTransport:
    device = Device.from_mapping("LOCAL_SIM", {"kind": "local-sim", "project_root": str(remote_base)})
    return LocalSimTransport(device)


def setup(tmp_path: Path):
    local = tmp_path / "local" / "proj"
    remote_base = tmp_path / "remote"
    local.mkdir(parents=True)
    remote_base.mkdir(parents=True)
    transport = make_transport(remote_base)
    remote_root = str(remote_base / "proj")
    backup = tmp_path / "state" / "backup"
    return local, remote_root, transport, backup


def reconcile(local, remote_root, transport, backup, prev_local=None, prev_remote=None):
    return preflight_reconcile(
        transport=transport,
        local_root=local,
        remote_root=remote_root,
        excludes=["node_modules/**"],
        hash_below_bytes=1_000_000,
        prev_local=prev_local,
        prev_remote=prev_remote,
        backup_root=backup,
    )


def manifest(root: Path):
    return build_manifest(root, ["node_modules/**"], hash_below_bytes=1_000_000)


def test_first_run_pushes_local_only(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "a.txt").write_text("hello")
    (local / "sub").mkdir()
    (local / "sub" / "b.txt").write_text("world")

    res = reconcile(local, remote_root, t, backup)
    assert not res.has_conflicts
    assert sorted(res.pushed) == ["a.txt", "sub/b.txt"]
    assert (Path(remote_root) / "a.txt").read_text() == "hello"
    assert (Path(remote_root) / "sub" / "b.txt").read_text() == "world"


def test_remote_only_pulled(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    Path(remote_root).mkdir(parents=True)
    (Path(remote_root) / "gen.txt").write_text("generated")

    res = reconcile(local, remote_root, t, backup)
    assert res.pulled == ["gen.txt"]
    assert (local / "gen.txt").read_text() == "generated"


def test_preflight_reports_planned_counts_and_bounded_progress(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    remote.mkdir(parents=True)
    for index in range(26):
        (remote / f"remote-{index:02}.txt").write_text("remote")
    for index in range(2):
        (local / f"local-{index:02}.txt").write_text("local")

    events: list[tuple[int, int, dict[str, int]]] = []
    result = preflight_reconcile(
        transport=t,
        local_root=local,
        remote_root=remote_root,
        excludes=["node_modules/**"],
        hash_below_bytes=1_000_000,
        prev_local=None,
        prev_remote=None,
        backup_root=backup,
        progress=lambda completed, total, counts: events.append(
            (completed, total, counts.copy())
        ),
    )

    assert not result.has_conflicts
    assert [completed for completed, _total, _counts in events] == [0, 25, 28]
    assert all(total == 28 for _completed, total, _counts in events)
    assert all(
        counts
        == {"pulls": 26, "pushes": 2, "deletes_local": 0, "deletes_remote": 0}
        for _completed, _total, counts in events
    )


def test_postrun_pullback_of_command_output(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "run.txt").write_text("input")
    pre = reconcile(local, remote_root, t, backup)

    # Simulate the remote command creating + modifying outputs.
    t.exec(["python", "-c",
            "open('out.txt','w').write('result'); open('run.txt','w').write('input2')"],
           cwd=remote_root)

    post = postrun_pullback(
        transport=t, local_root=local, remote_root=remote_root,
        excludes=["node_modules/**"], hash_below_bytes=1_000_000,
        pre_remote_manifest=pre.remote_manifest, pre_local_manifest=pre.local_manifest,
        backup_root=backup, conflict_remote_root=tmp_path / "state" / "remote",
    )
    assert sorted(post.pulled) == ["out.txt", "run.txt"]
    assert (local / "out.txt").read_text() == "result"
    assert (local / "run.txt").read_text() == "input2"
    assert not post.conflicts


def test_postrun_skips_pull_when_syncthing_already_delivered_identical(tmp_path: Path):
    # Idempotent external-writer: if Syncthing already delivered the identical output bytes to local
    # before pullback, remrun must NOT re-pull (the unnecessary overwrite that races Syncthing).
    local, remote_root, t, backup = setup(tmp_path)
    (local / "run.txt").write_text("input")
    pre = reconcile(local, remote_root, t, backup)

    (Path(remote_root) / "out.txt").write_text("RESULT-DATA")   # remote command produced out.txt
    (local / "out.txt").write_text("RESULT-DATA")               # ...Syncthing already delivered it

    post = postrun_pullback(
        transport=t, local_root=local, remote_root=remote_root,
        excludes=["node_modules/**"], hash_below_bytes=1_000_000,
        pre_remote_manifest=pre.remote_manifest, pre_local_manifest=pre.local_manifest,
        backup_root=backup, conflict_remote_root=tmp_path / "state" / "remote",
    )
    assert post.skipped_identical == ["out.txt"]
    assert "out.txt" not in post.pulled
    assert (local / "out.txt").read_text() == "RESULT-DATA"


def test_hash_file_matches_sha256_file(tmp_path: Path):
    from remrun.manifest import sha256_file
    base = tmp_path / "remote"
    base.mkdir()
    f = base / "big.bin"
    f.write_bytes(b"x" * 5000)
    t = make_transport(base)
    assert t.hash_file(str(f)) == sha256_file(f)


@pytest.mark.parametrize(
    "hash_result",
    [
        TransportError("injected hash failure"),
        NotImplementedError("hash unsupported"),
        ValueError("malformed hash response"),
        "not-a-sha256",
        "",
    ],
)
def test_postrun_unverifiable_hash_cannot_silently_skip_large_equal_metadata(
    tmp_path: Path,
    monkeypatch,
    hash_result,
):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "run.txt").write_text("input")
    pre = reconcile(local, remote_root, t, backup)

    size = 1_000_001  # Above this test's manifest hashing threshold.
    remote_output = Path(remote_root) / "out.bin"
    local_output = local / "out.bin"
    remote_output.write_bytes(b"R" * size)
    local_output.write_bytes(b"L" * size)
    same_ns = 1_700_000_000_000_000_000
    os.utime(remote_output, ns=(same_ns, same_ns))
    os.utime(local_output, ns=(same_ns, same_ns))

    def unverifiable_hash(_remote_path: str) -> str:
        if isinstance(hash_result, Exception):
            raise hash_result
        return hash_result

    monkeypatch.setattr(t, "hash_file", unverifiable_hash)
    conflict_remote = tmp_path / "state" / "remote"
    post = postrun_pullback(
        transport=t, local_root=local, remote_root=remote_root,
        excludes=["node_modules/**"], hash_below_bytes=1_000_000,
        pre_remote_manifest=pre.remote_manifest, pre_local_manifest=pre.local_manifest,
        backup_root=backup, conflict_remote_root=conflict_remote,
    )

    assert post.conflicts == ["out.bin"]
    assert not post.skipped_identical
    assert not post.pulled
    assert local_output.read_bytes() == b"L" * size
    assert (conflict_remote / "out.bin").read_bytes() == b"R" * size


def test_postrun_local_change_during_run_is_conflict(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "shared.txt").write_text("v0")
    pre = reconcile(local, remote_root, t, backup)

    # Remote modifies shared.txt...
    (Path(remote_root) / "shared.txt").write_text("remote-v1")
    # ...and local also changes it during the run.
    (local / "shared.txt").write_text("local-v1")

    conflict_remote = tmp_path / "state" / "remote"
    post = postrun_pullback(
        transport=t, local_root=local, remote_root=remote_root,
        excludes=["node_modules/**"], hash_below_bytes=1_000_000,
        pre_remote_manifest=pre.remote_manifest, pre_local_manifest=pre.local_manifest,
        backup_root=backup, conflict_remote_root=conflict_remote,
    )
    assert post.conflicts == ["shared.txt"]
    # Local edit preserved; remote version saved outside the project tree.
    assert (local / "shared.txt").read_text() == "local-v1"
    assert (conflict_remote / "shared.txt").read_text() == "remote-v1"


def test_known_local_deletion_propagates_to_remote(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "keep.txt").write_text("k")
    (local / "drop.txt").write_text("d")
    first = reconcile(local, remote_root, t, backup)
    # baseline = converged manifests from first run
    prev_local, prev_remote = first.local_manifest, first.remote_manifest

    # Locally delete drop.txt; next reconcile should mirror the deletion remotely.
    (local / "drop.txt").unlink()
    second = reconcile(local, remote_root, t, backup, prev_local, prev_remote)
    assert second.deleted_remote == ["drop.txt"]
    assert not (Path(remote_root) / "drop.txt").exists()
    assert (Path(remote_root) / "keep.txt").exists()


def test_known_remote_deletion_backs_up_then_deletes_local(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "x.txt").write_text("content")
    first = reconcile(local, remote_root, t, backup)
    prev_local, prev_remote = first.local_manifest, first.remote_manifest

    # Remote drops x.txt; local unchanged -> known remote deletion mirrored locally.
    (Path(remote_root) / "x.txt").unlink()
    second = reconcile(local, remote_root, t, backup, prev_local, prev_remote)
    assert second.deleted_local == ["x.txt"]
    assert not (local / "x.txt").exists()
    assert (backup / "x.txt").read_text() == "content"  # backed up before deleting


def test_vanished_remote_root_aborts_instead_of_deleting_local(tmp_path: Path):
    # A prior baseline exists; then the entire remote root disappears (wrong path,
    # unmounted, etc.). remrun must NOT treat that as "remote deleted everything"
    # and wipe the local tree — it must abort.
    local, remote_root, t, backup = setup(tmp_path)
    (local / "keep1.txt").write_text("one")
    (local / "keep2.txt").write_text("two")
    first = reconcile(local, remote_root, t, backup)
    prev_local, prev_remote = first.local_manifest, first.remote_manifest

    import shutil as _sh
    _sh.rmtree(remote_root)  # remote root vanishes entirely

    res = reconcile(local, remote_root, t, backup, prev_local, prev_remote)
    assert res.has_conflicts
    assert res.conflicts[0].state == "remote-vanished"
    # Local files are untouched.
    assert (local / "keep1.txt").read_text() == "one"
    assert (local / "keep2.txt").read_text() == "two"
    assert not res.deleted_local


def test_empty_but_present_remote_still_mirrors_last_deletion(tmp_path: Path):
    # Distinct from the vanished case: the remote root still EXISTS but its only
    # file was legitimately deleted. That must still mirror as a known deletion.
    local, remote_root, t, backup = setup(tmp_path)
    (local / "only.txt").write_text("x")
    first = reconcile(local, remote_root, t, backup)
    prev_local, prev_remote = first.local_manifest, first.remote_manifest

    (Path(remote_root) / "only.txt").unlink()  # root still present, now empty
    res = reconcile(local, remote_root, t, backup, prev_local, prev_remote)
    assert not res.has_conflicts
    assert res.deleted_local == ["only.txt"]
    assert not (local / "only.txt").exists()


def test_both_changed_aborts_without_mutation(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "c.txt").write_text("base")
    first = reconcile(local, remote_root, t, backup)
    prev_local, prev_remote = first.local_manifest, first.remote_manifest

    (local / "c.txt").write_text("local-edit")
    (Path(remote_root) / "c.txt").write_text("remote-edit")

    res = reconcile(local, remote_root, t, backup, prev_local, prev_remote)
    assert res.has_conflicts
    # Neither side mutated.
    assert (local / "c.txt").read_text() == "local-edit"
    assert (Path(remote_root) / "c.txt").read_text() == "remote-edit"
    assert not res.pushed and not res.pulled
