"""End-to-end reconcile-engine tests against the LOCAL_SIM transport."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from remrun.manifest import build_manifest
from remrun.models import Device
from remrun.reconcile import ReconcileResult, postrun_pullback, preflight_reconcile
from remrun.transfer_plan import ABORT_CONFLICT, ClassifiedPath
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


def test_fallback_preflight_refuses_local_pull_without_mutation(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    remote.mkdir(parents=True)
    (remote / "remote.txt").write_text("REMOTE")

    result = preflight_reconcile(
        transport=t,
        local_root=local,
        remote_root=remote_root,
        excludes=["node_modules/**"],
        hash_below_bytes=1_000_000,
        prev_local=None,
        prev_remote=None,
        backup_root=backup,
        is_fallback=True,
    )

    assert [conflict.path for conflict in result.conflicts] == ["remote.txt"]
    assert [conflict.state for conflict in result.conflicts] == ["fallback-local-mutation"]
    assert not (local / "remote.txt").exists()


def test_fallback_preflight_refuses_local_delete_without_mutation(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    local_path = local / "shared.txt"
    local_path.write_text("LOCAL")
    first = reconcile(local, remote_root, t, backup)
    (Path(remote_root) / "shared.txt").unlink()

    result = preflight_reconcile(
        transport=t,
        local_root=local,
        remote_root=remote_root,
        excludes=["node_modules/**"],
        hash_below_bytes=1_000_000,
        prev_local=first.local_manifest,
        prev_remote=first.remote_manifest,
        backup_root=backup,
        is_fallback=True,
    )

    assert [conflict.path for conflict in result.conflicts] == ["shared.txt"]
    assert [conflict.state for conflict in result.conflicts] == ["fallback-local-mutation"]
    assert local_path.read_text() == "LOCAL"


def test_fallback_local_mutation_is_retryable_only_when_all_conflicts_are_candidate_local():
    result = ReconcileResult(conflicts=[
        ClassifiedPath("shared.txt", "fallback-local-mutation", ABORT_CONFLICT, "candidate B"),
    ])
    assert result.conflicts_are_candidate_local

    result.conflicts.append(
        ClassifiedPath("<local root>", "local-vanished", ABORT_CONFLICT, "global")
    )
    assert not result.conflicts_are_candidate_local


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


def test_postrun_next_baseline_retains_unrelated_local_create(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "input.txt").write_text("input")
    pre = reconcile(local, remote_root, t, backup)

    (Path(remote_root) / "remote-output.txt").write_text("remote-created")
    (local / "local-work.txt").write_text("local-created-during-run")

    post = postrun_pullback(
        transport=t, local_root=local, remote_root=remote_root,
        excludes=["node_modules/**"], hash_below_bytes=1_000_000,
        pre_remote_manifest=pre.remote_manifest, pre_local_manifest=pre.local_manifest,
        backup_root=backup, conflict_remote_root=tmp_path / "state" / "remote",
    )

    assert post.next_baseline is not None
    assert "remote-output.txt" in post.next_baseline.local_manifest
    assert "remote-output.txt" in post.next_baseline.remote_manifest
    assert "local-work.txt" not in post.next_baseline.local_manifest
    assert "local-work.txt" not in post.next_baseline.remote_manifest

    next_preflight = reconcile(
        local, remote_root, t, backup,
        post.next_baseline.local_manifest, post.next_baseline.remote_manifest,
    )
    assert next_preflight.pushed == ["local-work.txt"]
    assert not next_preflight.has_conflicts


def test_postrun_next_baseline_retains_unrelated_local_modify(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "remote-output.txt").write_text("remote-v0")
    (local / "local-work.txt").write_text("local-v0")
    pre = reconcile(local, remote_root, t, backup)

    (Path(remote_root) / "remote-output.txt").write_text("remote-v1")
    (local / "local-work.txt").write_text("local-v1-during-run")

    post = postrun_pullback(
        transport=t, local_root=local, remote_root=remote_root,
        excludes=["node_modules/**"], hash_below_bytes=1_000_000,
        pre_remote_manifest=pre.remote_manifest, pre_local_manifest=pre.local_manifest,
        backup_root=backup, conflict_remote_root=tmp_path / "state" / "remote",
    )

    assert post.next_baseline is not None
    assert (
        post.next_baseline.local_manifest["remote-output.txt"].sha256
        != pre.local_manifest["remote-output.txt"].sha256
    )
    assert (
        post.next_baseline.remote_manifest["remote-output.txt"]
        == post.post_remote_manifest["remote-output.txt"]
    )
    assert post.next_baseline.local_manifest["local-work.txt"] == pre.local_manifest["local-work.txt"]
    assert post.next_baseline.remote_manifest["local-work.txt"] == pre.remote_manifest["local-work.txt"]

    next_preflight = reconcile(
        local, remote_root, t, backup,
        post.next_baseline.local_manifest, post.next_baseline.remote_manifest,
    )
    assert next_preflight.pushed == ["local-work.txt"]
    assert not next_preflight.has_conflicts


def test_postrun_next_baseline_retains_unrelated_local_delete(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "remote-output.txt").write_text("remote-v0")
    (local / "local-work.txt").write_text("local-v0")
    pre = reconcile(local, remote_root, t, backup)

    (Path(remote_root) / "remote-output.txt").unlink()
    (local / "local-work.txt").unlink()

    post = postrun_pullback(
        transport=t, local_root=local, remote_root=remote_root,
        excludes=["node_modules/**"], hash_below_bytes=1_000_000,
        pre_remote_manifest=pre.remote_manifest, pre_local_manifest=pre.local_manifest,
        backup_root=backup, conflict_remote_root=tmp_path / "state" / "remote",
    )

    assert post.next_baseline is not None
    assert "remote-output.txt" not in post.next_baseline.local_manifest
    assert "remote-output.txt" not in post.next_baseline.remote_manifest
    assert post.next_baseline.local_manifest["local-work.txt"] == pre.local_manifest["local-work.txt"]
    assert post.next_baseline.remote_manifest["local-work.txt"] == pre.remote_manifest["local-work.txt"]

    next_preflight = reconcile(
        local, remote_root, t, backup,
        post.next_baseline.local_manifest, post.next_baseline.remote_manifest,
    )
    assert next_preflight.deleted_remote == ["local-work.txt"]
    assert not next_preflight.has_conflicts


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
    assert post.next_baseline is not None
    assert "out.txt" in post.next_baseline.local_manifest
    assert "out.txt" in post.next_baseline.remote_manifest


def _converge_after_manifest(transport, write_side):
    """Make the transport's remote manifest call also perform ``write_side()`` once.

    That lands the converging write in the exact gap the guard exists for: after the plan
    is built from both manifests, before the transfer applies. Restores the real method
    immediately so the manifests rebuilt at the end of preflight are truthful.
    """
    real = transport.manifest

    def once(*args, **kwargs):
        result = real(*args, **kwargs)
        transport.manifest = real
        write_side()
        return result

    transport.manifest = once


def test_preflight_skips_pull_when_local_converged_between_plan_and_apply(tmp_path: Path):
    # Preflight counterpart of the postrun guard above. A pull is PLANNED from a manifest
    # snapshot, then Syncthing delivers those exact bytes locally before the transfer runs.
    # Rewriting the file would re-touch a path Syncthing is watching — the race remrun is
    # supposed to stay clear of. The transport fails loudly if the pull is attempted.
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    remote.mkdir(parents=True)
    (remote / "delivered.txt").write_text("SAME-BYTES")

    _converge_after_manifest(t, lambda: (local / "delivered.txt").write_text("SAME-BYTES"))

    def fail_pull(*args, **kwargs):
        raise AssertionError("preflight pulled a file that was already byte-identical")

    t.pull_file = fail_pull

    res = reconcile(local, remote_root, t, backup)
    assert res.skipped_identical == ["delivered.txt"]
    assert res.pulled == []
    assert (local / "delivered.txt").read_text() == "SAME-BYTES"


def test_preflight_skips_push_when_local_reverts_to_remote_bytes_before_apply(tmp_path: Path):
    # Same guard, push direction. A PUSH is planned because local changed; then Syncthing
    # delivers the remote's copy over the local file before the transfer runs, so local now
    # holds exactly what the remote already has and there is nothing to send.
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    (local / "shared.txt").write_text("ORIGINAL")
    first = reconcile(local, remote_root, t, backup)
    assert first.pushed == ["shared.txt"]

    (local / "shared.txt").write_text("LOCAL-EDIT")     # local changed -> PUSH planned
    # ...then the remote's copy lands back on top of the local edit, mid-reconcile.
    _converge_after_manifest(t, lambda: (local / "shared.txt").write_text("ORIGINAL"))

    def fail_push(*args, **kwargs):
        raise AssertionError("preflight pushed a file the remote already had")

    t.push_file = fail_push

    res = reconcile(local, remote_root, t, backup,
                    prev_local=first.local_manifest, prev_remote=first.remote_manifest)
    assert res.skipped_identical == ["shared.txt"]
    assert res.pushed == []
    assert (remote / "shared.txt").read_text() == "ORIGINAL"


def test_preflight_still_transfers_when_content_genuinely_differs(tmp_path: Path):
    # The guard must not swallow real work: differing bytes still move, in both directions.
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    remote.mkdir(parents=True)
    (remote / "pull-me.txt").write_text("REMOTE-ONLY")
    (local / "push-me.txt").write_text("LOCAL-ONLY")

    res = reconcile(local, remote_root, t, backup)
    assert res.pulled == ["pull-me.txt"]
    assert res.pushed == ["push-me.txt"]
    assert res.skipped_identical == []
    assert (local / "pull-me.txt").read_text() == "REMOTE-ONLY"
    assert (remote / "push-me.txt").read_text() == "LOCAL-ONLY"


def test_preflight_guard_fails_closed_when_remote_hash_is_unavailable(tmp_path: Path):
    # No remote hash (file above the hashing threshold) means no positive proof of equality,
    # so the transfer MUST still happen. Absence of evidence never becomes a skip.
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    remote.mkdir(parents=True)
    (remote / "big.bin").write_bytes(b"R" * 2048)

    res = preflight_reconcile(
        transport=t,
        local_root=local,
        remote_root=remote_root,
        excludes=["node_modules/**"],
        hash_below_bytes=1024,          # below the file size => manifests carry no sha256
        prev_local=None,
        prev_remote=None,
        backup_root=backup,
    )
    assert res.pulled == ["big.bin"]
    assert res.skipped_identical == []


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
        None,
        123,
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
    assert post.next_baseline is None


def test_postrun_pull_verification_failure_cannot_return_a_baseline(
    tmp_path: Path,
    monkeypatch,
):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "run.txt").write_text("input")
    pre = reconcile(local, remote_root, t, backup)
    (Path(remote_root) / "out.txt").write_text("expected-remote-output")

    def corrupt_pull(_remote_path: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_text("corrupt-transfer")

    monkeypatch.setattr(t, "pull_file", corrupt_pull)

    with pytest.raises(TransportError, match="pull verification failed for out.txt"):
        postrun_pullback(
            transport=t, local_root=local, remote_root=remote_root,
            excludes=["node_modules/**"], hash_below_bytes=1_000_000,
            pre_remote_manifest=pre.remote_manifest, pre_local_manifest=pre.local_manifest,
            backup_root=backup, conflict_remote_root=tmp_path / "state" / "remote",
        )


def test_postrun_scope_escape_withholds_next_baseline(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "run.txt").write_text("input")
    pre = reconcile(local, remote_root, t, backup)
    remote_escaped = Path(remote_root) / "escaped" / "out.txt"
    remote_escaped.parent.mkdir(parents=True)
    remote_escaped.write_text("outside declared write scope")

    post = postrun_pullback(
        transport=t, local_root=local, remote_root=remote_root,
        excludes=["node_modules/**"], hash_below_bytes=1_000_000,
        pre_remote_manifest=pre.remote_manifest, pre_local_manifest=pre.local_manifest,
        backup_root=backup, conflict_remote_root=tmp_path / "state" / "remote",
        write_scope_paths=["allowed/**"],
    )

    assert post.conflicts == ["escaped/out.txt"]
    assert post.next_baseline is None


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
    assert post.next_baseline is None
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


def _reconcile_unhashed(local, remote_root, transport, backup, prev_local, prev_remote):
    """Reconcile with hashing effectively disabled, so manifests carry size+mtime only.

    That is what a real run looks like for any file above `hash_small_files_below_mb`.
    """
    return preflight_reconcile(
        transport=transport, local_root=local, remote_root=remote_root,
        excludes=["node_modules/**"], hash_below_bytes=0,
        prev_local=prev_local, prev_remote=prev_remote, backup_root=backup,
    )


def test_identical_bytes_are_not_a_conflict_even_when_manifests_carry_no_hash(tmp_path: Path):
    # FIELD-REPORTED (2026-07-25, three lanes, one lost ~54 min): Syncthing delivers the same
    # edit to both devices. Each side therefore differs from its own baseline, and with no
    # hash in the manifest the equality test falls back to mtime — which differs — so the run
    # aborted `both-changed` with nothing actually to reconcile. Identical content must never
    # be a conflict, whatever the metadata says.
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    (local / "shared.txt").write_text("base")
    first = reconcile(local, remote_root, t, backup)

    # The same edit lands on both sides, with deliberately different mtimes.
    (local / "shared.txt").write_text("SAME-EDIT")
    (remote / "shared.txt").write_text("SAME-EDIT")
    os.utime(local / "shared.txt", ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000))
    os.utime(remote / "shared.txt", ns=(1_800_000_000_000_000_000, 1_800_000_000_000_000_000))

    res = _reconcile_unhashed(local, remote_root, t, backup,
                              first.local_manifest, first.remote_manifest)

    assert not res.has_conflicts
    assert res.converged_conflicts == ["shared.txt"]
    assert not res.pushed and not res.pulled
    assert (local / "shared.txt").read_text() == "SAME-EDIT"
    assert (remote / "shared.txt").read_text() == "SAME-EDIT"


def test_converged_conflict_does_not_stall_unrelated_work(tmp_path: Path):
    # The reported cost was not just the false alarm: one converged path blocked every other
    # lane in the same run. Once disproved, the rest of the tree must still reconcile.
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    (local / "shared.txt").write_text("base")
    first = reconcile(local, remote_root, t, backup)

    (local / "shared.txt").write_text("SAME-EDIT")
    (remote / "shared.txt").write_text("SAME-EDIT")
    os.utime(local / "shared.txt", ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000))
    os.utime(remote / "shared.txt", ns=(1_800_000_000_000_000_000, 1_800_000_000_000_000_000))
    (local / "unrelated.txt").write_text("NEW-LOCAL-WORK")

    res = _reconcile_unhashed(local, remote_root, t, backup,
                              first.local_manifest, first.remote_manifest)

    assert not res.has_conflicts
    assert res.converged_conflicts == ["shared.txt"]
    assert res.pushed == ["unrelated.txt"]
    assert (remote / "unrelated.txt").read_text() == "NEW-LOCAL-WORK"


def test_genuinely_divergent_bytes_still_conflict_without_hashes(tmp_path: Path):
    # The guard must not weaken the real protection: different bytes still abort, and
    # neither side is mutated.
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    (local / "shared.txt").write_text("base")
    first = reconcile(local, remote_root, t, backup)

    (local / "shared.txt").write_text("LOCAL-EDIT")
    (remote / "shared.txt").write_text("OTHER-EDIT")   # same length, different content

    res = _reconcile_unhashed(local, remote_root, t, backup,
                              first.local_manifest, first.remote_manifest)

    assert res.has_conflicts
    assert [c.path for c in res.conflicts] == ["shared.txt"]
    assert not res.converged_conflicts
    assert (local / "shared.txt").read_text() == "LOCAL-EDIT"
    assert (remote / "shared.txt").read_text() == "OTHER-EDIT"


def test_same_metadata_cannot_hide_divergent_unhashed_bytes(tmp_path: Path):
    """A large-file edit may preserve both size and mtime; metadata is not equality proof."""
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    local_path = local / "shared.bin"
    remote_path = remote / "shared.bin"
    local_path.write_bytes(b"BASE")
    first = reconcile(local, remote_root, t, backup)

    local_path.write_bytes(b"LEFT")
    remote_path.write_bytes(b"RGHT")
    same_mtime = 1_700_000_000_000_000_000
    os.utime(local_path, ns=(same_mtime, same_mtime))
    os.utime(remote_path, ns=(same_mtime, same_mtime))

    res = _reconcile_unhashed(
        local,
        remote_root,
        t,
        backup,
        first.local_manifest,
        first.remote_manifest,
    )

    assert res.has_conflicts
    assert [c.path for c in res.conflicts] == ["shared.bin"]
    assert not res.pushed and not res.pulled
    assert local_path.read_bytes() == b"LEFT"
    assert remote_path.read_bytes() == b"RGHT"


def test_same_metadata_identical_unhashed_bytes_converge_after_content_proof(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    remote.mkdir(parents=True)
    local_path = local / "shared.bin"
    remote_path = remote / "shared.bin"
    local_path.write_bytes(b"SAME")
    remote_path.write_bytes(b"SAME")
    same_mtime = 1_700_000_000_000_000_000
    os.utime(local_path, ns=(same_mtime, same_mtime))
    os.utime(remote_path, ns=(same_mtime, same_mtime))

    res = _reconcile_unhashed(local, remote_root, t, backup, None, None)

    assert not res.has_conflicts
    assert res.converged_conflicts == ["shared.bin"]
    assert not res.pushed and not res.pulled


def test_same_metadata_without_remote_hash_stays_unverified(tmp_path: Path, monkeypatch):
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    remote.mkdir(parents=True)
    (local / "shared.bin").write_bytes(b"SAME")
    (remote / "shared.bin").write_bytes(b"SAME")
    same_mtime = 1_700_000_000_000_000_000
    os.utime(local / "shared.bin", ns=(same_mtime, same_mtime))
    os.utime(remote / "shared.bin", ns=(same_mtime, same_mtime))

    def unavailable(_remote_path: str) -> str:
        raise TransportError("injected hash failure")

    monkeypatch.setattr(t, "hash_file", unavailable)
    res = _reconcile_unhashed(local, remote_root, t, backup, None, None)

    assert res.has_conflicts
    assert [item.state for item in res.conflicts] == ["both-present-unverified"]
    assert not res.converged_conflicts


@pytest.mark.parametrize(
    "hash_result",
    [
        TransportError("injected hash failure"),
        NotImplementedError("hash unsupported"),
        "not-a-sha256",
        "",
    ],
)
def test_unverifiable_remote_hash_keeps_the_conflict(tmp_path: Path, monkeypatch, hash_result):
    # Fail CLOSED: if the remote hash cannot be obtained or is malformed, equality is
    # UNPROVEN, so the conflict must stand. Absence of evidence is never convergence.
    local, remote_root, t, backup = setup(tmp_path)
    remote = Path(remote_root)
    (local / "shared.txt").write_text("base")
    first = reconcile(local, remote_root, t, backup)

    (local / "shared.txt").write_text("SAME-EDIT")
    (remote / "shared.txt").write_text("SAME-EDIT")

    def unverifiable(_remote_path: str) -> str:
        if isinstance(hash_result, Exception):
            raise hash_result
        return hash_result

    monkeypatch.setattr(t, "hash_file", unverifiable)

    res = _reconcile_unhashed(local, remote_root, t, backup,
                              first.local_manifest, first.remote_manifest)

    assert res.has_conflicts
    assert not res.converged_conflicts


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
