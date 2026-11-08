"""End-to-end reconcile-engine tests against the LOCAL_SIM transport."""
from __future__ import annotations

import os
import threading
from pathlib import Path

import pytest

import remrun.reconcile as reconcile_module
import remrun.state as state_module
from remrun.manifest import build_manifest
from remrun.models import Device
from remrun.reconcile import ReconcileResult, postrun_pullback, preflight_reconcile
from remrun.transfer_plan import ABORT_CONFLICT, ClassifiedPath
from remrun.transport import LocalSimTransport, TransportError


@pytest.fixture(autouse=True)
def _controller_state(tmp_path: Path, monkeypatch):
    """Keep the local-write lock table out of the real controller state."""
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(tmp_path / "controller-state"))


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


def test_preflight_refuses_local_edit_after_push(tmp_path: Path, monkeypatch):
    local, remote_root, transport, backup = setup(tmp_path)
    path = local / "input.txt"
    path.write_text("initial", encoding="utf-8")
    first = reconcile(local, remote_root, transport, backup)
    path.write_text("intended push", encoding="utf-8")
    original_push = transport.push_file

    def push_then_edit(source, destination):
        original_push(source, destination)
        source.write_text("concurrent local edit", encoding="utf-8")

    monkeypatch.setattr(transport, "push_file", push_then_edit)
    result = reconcile(
        local, remote_root, transport, backup, first.local_manifest, first.remote_manifest,
    )
    assert result.has_conflicts
    assert result.conflicts[0].path == "input.txt"
    assert path.read_text() == "concurrent local edit"
    assert (Path(remote_root) / "input.txt").read_text() == "intended push"


def test_preflight_does_not_rehash_untouched_large_historical_file(tmp_path, monkeypatch):
    local, remote_root, transport, backup = setup(tmp_path)
    (local / "large.bin").write_bytes(b"unchanged content")
    kwargs = dict(transport=transport, local_root=local, remote_root=remote_root,
                  excludes=[], hash_below_bytes=1, backup_root=backup)
    first = preflight_reconcile(**kwargs, prev_local=None, prev_remote=None)
    assert not first.has_conflicts
    monkeypatch.setattr(transport, "hash_file", lambda *_args: pytest.fail("untouched historical file"))
    result = preflight_reconcile(
        **kwargs, prev_local=first.local_manifest, prev_remote=first.remote_manifest,
    )
    assert not result.has_conflicts


def test_fresh_local_entry_fails_closed_on_unexplained_inspection_error(
    tmp_path: Path, monkeypatch
):
    import remrun.reconcile as reconcile_mod

    path = tmp_path / "output.bin"
    path.write_bytes(b"complete output")
    real_lstat = Path.lstat

    def denied_lstat(self: Path):
        if self == path:
            raise PermissionError(13, "scanner denied inspection", str(self))
        return real_lstat(self)

    monkeypatch.setattr(Path, "lstat", denied_lstat)
    with pytest.raises(TransportError, match=r"cannot inspect local file.*scanner denied"):
        reconcile_mod._fresh_local_entry(path, hash_if_size=path.stat().st_size)


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


def test_preflight_force_includes_declared_lock_input_over_global_exclude(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    (local / "uv.lock").write_text("pinned", encoding="utf-8")
    (local / "other.lock").write_text("not declared", encoding="utf-8")

    result = preflight_reconcile(
        transport=t,
        local_root=local,
        remote_root=remote_root,
        excludes=["*.lock"],
        hash_below_bytes=1_000_000,
        prev_local=None,
        prev_remote=None,
        backup_root=backup,
        include_paths=("uv.lock",),
    )

    assert result.pushed == ["uv.lock"]
    assert (Path(remote_root) / "uv.lock").read_text(encoding="utf-8") == "pinned"
    assert not (Path(remote_root) / "other.lock").exists()
    assert "uv.lock" in result.local_manifest
    assert "uv.lock" in result.remote_manifest


def test_bootstrap_input_target_edit_cannot_overwrite_controller(tmp_path: Path):
    local, remote_root, transport, backup = setup(tmp_path)
    (local / "uv.lock").write_text("controller", encoding="utf-8")
    first = preflight_reconcile(
        transport=transport,
        local_root=local,
        remote_root=remote_root,
        excludes=["*.lock"],
        hash_below_bytes=1_000_000,
        prev_local=None,
        prev_remote=None,
        backup_root=backup,
        include_paths=("uv.lock",),
    )
    (Path(remote_root) / "uv.lock").write_text("target-tampered", encoding="utf-8")

    result = preflight_reconcile(
        transport=transport,
        local_root=local,
        remote_root=remote_root,
        excludes=["*.lock"],
        hash_below_bytes=1_000_000,
        prev_local=first.local_manifest,
        prev_remote=first.remote_manifest,
        backup_root=backup,
        include_paths=("uv.lock",),
    )

    assert result.has_conflicts
    assert result.conflicts[0].state == "bootstrap-input-diverged"
    assert (local / "uv.lock").read_text(encoding="utf-8") == "controller"
    assert (Path(remote_root) / "uv.lock").read_text(encoding="utf-8") == "target-tampered"


def test_bootstrap_input_missing_on_target_is_repaired_from_controller(tmp_path: Path):
    local, remote_root, transport, backup = setup(tmp_path)
    (local / "uv.lock").write_text("controller", encoding="utf-8")
    first = preflight_reconcile(
        transport=transport,
        local_root=local,
        remote_root=remote_root,
        excludes=["*.lock"],
        hash_below_bytes=1_000_000,
        prev_local=None,
        prev_remote=None,
        backup_root=backup,
        include_paths=("uv.lock",),
    )
    (Path(remote_root) / "uv.lock").unlink()

    result = preflight_reconcile(
        transport=transport,
        local_root=local,
        remote_root=remote_root,
        excludes=["*.lock"],
        hash_below_bytes=1_000_000,
        prev_local=first.local_manifest,
        prev_remote=first.remote_manifest,
        backup_root=backup,
        include_paths=("uv.lock",),
    )

    assert not result.has_conflicts
    assert result.pushed == ["uv.lock"]
    assert (local / "uv.lock").read_text(encoding="utf-8") == "controller"
    assert (Path(remote_root) / "uv.lock").read_text(encoding="utf-8") == "controller"


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


# --- push guard -------------------------------------------------------------------


def test_push_guard_byte_limit_refuses_before_creating_the_remote_root(tmp_path: Path):
    # A few huge files are as costly as many small ones: the byte limit alone refuses,
    # and it does so before ensure_remote_dir or any transfer.
    local, remote_root, t, backup = setup(tmp_path)
    (local / "data").mkdir()
    (local / "data" / "big.bin").write_bytes(b"x" * 4000)
    (local / "a.txt").write_text("small")

    res = preflight_reconcile(
        transport=t, local_root=local, remote_root=remote_root, excludes=[],
        hash_below_bytes=1_000_000, prev_local=None, prev_remote=None,
        backup_root=backup, max_push_files=100, max_push_bytes=1000,
    )

    assert res.push_refused is not None and res.push_refused.refused
    assert (res.push_refused.files, res.push_refused.bytes) == (2, 4005)
    assert res.push_refused.top_by_bytes[0] == ("data", 4000)
    assert res.pushed == [] and res.pulled == [] and not res.has_conflicts
    assert not Path(remote_root).exists()


def test_push_guard_at_the_limit_still_pushes(tmp_path: Path):
    local, remote_root, t, backup = setup(tmp_path)
    for name in ("a.txt", "b.txt"):
        (local / name).write_text("v")

    res = preflight_reconcile(
        transport=t, local_root=local, remote_root=remote_root, excludes=[],
        hash_below_bytes=1_000_000, prev_local=None, prev_remote=None,
        backup_root=backup, max_push_files=2, max_push_bytes=2,
    )

    assert res.push_refused is None
    assert sorted(res.pushed) == ["a.txt", "b.txt"]


def test_push_guard_counts_only_the_push_not_the_whole_tree(tmp_path: Path):
    # After a first sync, a one-file edit must pass a limit the whole tree exceeds.
    local, remote_root, t, backup = setup(tmp_path)
    for i in range(5):
        (local / f"f{i}.txt").write_text("v1")
    first = reconcile(local, remote_root, t, backup)
    (local / "f0.txt").write_text("v2")

    res = preflight_reconcile(
        transport=t, local_root=local, remote_root=remote_root, excludes=[],
        hash_below_bytes=1_000_000, prev_local=first.local_manifest,
        prev_remote=first.remote_manifest, backup_root=backup, max_push_files=1,
    )

    assert res.push_refused is None and res.pushed == ["f0.txt"]


def test_measure_push_groups_by_first_two_directories():
    from remrun.manifest import FileEntry
    from remrun.reconcile import measure_push

    paths = ["root.txt", "a/x.txt", "a/b/c/d.txt", "a/b/e.txt", "z/y/q.txt"]
    manifest = {p: FileEntry(path=p, kind="file", size=1, mtime_ns=0) for p in paths}
    size = measure_push(paths, manifest, max_files=None, max_bytes=None)

    assert dict(size.top_by_files) == {".": 1, "a": 1, "a/b": 2, "z/y": 1}
    assert size.top_by_files[0] == ("a/b", 2)
    assert not size.refused


class _ScriptedRemote:
    """A target whose post-run manifest and file bytes are given, and whose pulls are
    recorded: the pullback's decisions are observed without any filesystem's own case
    rules deciding where a write lands."""

    def __init__(self, files: dict[str, bytes]):
        import hashlib

        from remrun.manifest import FileEntry

        self.files = files
        self.pulls: list[str] = []
        self.post = {
            rel: FileEntry(path=rel, kind="file", size=len(data), mtime_ns=1,
                           sha256=hashlib.sha256(data).hexdigest())
            for rel, data in files.items()
        }

    def manifest(self, remote_root, excludes, hash_below_bytes, include_paths=()):  # noqa: ANN001
        return dict(self.post)

    def remote_join(self, root: str, rel: str) -> str:
        return f"{root}/{rel}"

    def pull_file(self, remote_path: str, local_path: Path) -> None:
        rel = remote_path.removeprefix("tree/")
        self.pulls.append(rel)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(self.files[rel])


def _held_pullback(tmp_path: Path, remote: _ScriptedRemote, pre_remote, held):  # noqa: ANN001
    local = tmp_path / "local"
    return postrun_pullback(
        transport=remote, local_root=local, remote_root="tree", excludes=[],
        hash_below_bytes=1_000_000, pre_remote_manifest=pre_remote,
        pre_local_manifest=build_manifest(local, [], hash_below_bytes=1_000_000),
        backup_root=tmp_path / "backup", conflict_remote_root=tmp_path / "conflict",
        include_paths=["out/"], within=["out/"], held=held,
    )


@pytest.mark.parametrize(("held", "incoming"), [
    ("out/Sub", "out/sub/x.txt"),
    ("out/café", "out/café/x.txt"),
])
def test_held_link_holds_every_spelling_of_the_paths_below_it(tmp_path: Path, held, incoming):
    """A link the command left locally at ``out/Sub`` is ``out/sub`` too on a
    case-insensitive controller (APFS, NTFS), so pulling ``out/sub/x.txt`` would write
    through it, out of the project, and mirroring a deletion under it would delete
    there. Every spelling of a held path is held; other outputs still return."""
    local = tmp_path / "local"
    gone = incoming.rsplit("/", 1)[0] + "/old.txt"
    (local / gone).parent.mkdir(parents=True)
    (local / gone).write_text("local copy")
    remote = _ScriptedRemote({incoming: b"through the link?", "out/other.txt": b"fine"})
    pre_remote = build_manifest(local, [], hash_below_bytes=1_000_000)

    post = _held_pullback(tmp_path, remote, pre_remote, [held])

    assert remote.pulls == ["out/other.txt"] and post.pulled == ["out/other.txt"]
    assert post.deleted_local == [] and (local / gone).read_text() == "local copy"


def test_held_link_with_another_case_keeps_the_file_it_replaced(tmp_path: Path):
    """The command replaced ``out/keep.txt`` with a link named ``out/KEEP.txt``. The
    manifests skip links, so the file reads as deleted remotely; on a case-insensitive
    controller both names are one file, which must stay as it is, not be deleted."""
    local = tmp_path / "local"
    (local / "out").mkdir(parents=True)
    (local / "out" / "keep.txt").write_text("keep me")
    pre_remote = build_manifest(local, [], hash_below_bytes=1_000_000)

    post = _held_pullback(tmp_path, _ScriptedRemote({}), pre_remote, ["out/KEEP.txt"])

    assert post.deleted_local == [] and post.conflicts == []
    assert (local / "out" / "keep.txt").read_text() == "keep me"


# --- concurrent writers of one local tree ---------------------------------------------
#
# Ordinary runs hold the project lock and commit runs do not, so two controller
# processes can pull files into the same local tree at once. Each pullback checks a
# local file (unchanged since launch?) and then writes it; unless those steps are
# serialized, the second writer's check passes against bytes the first is about to
# replace, and one run's result silently overwrites the other's.


class _PausingTransport(LocalSimTransport):
    """Holds its pull into one local path until released: the window after a phase
    has checked the local file (and backed it up) and before it writes."""

    def __init__(self, device: Device, pause_at: Path):
        super().__init__(device)
        self.pause_at = pause_at
        self.paused = threading.Event()
        self.resume = threading.Event()

    def pull_file(self, remote_path: str, local_path: Path) -> None:
        if Path(local_path) == self.pause_at and not self.paused.is_set():
            self.paused.set()
            if not self.resume.wait(timeout=30):
                raise AssertionError("the paused pull was never released")
        super().pull_file(remote_path, local_path)


def _interleave(first, second, pausing: _PausingTransport) -> dict[str, object]:
    """Run ``first`` until it pauses in its write, let ``second`` run (to completion,
    unless something makes it wait), then release ``first``. Returns each outcome."""
    outcomes: dict[str, object] = {}

    def call(name, fn):  # noqa: ANN001
        try:
            outcomes[name] = fn()
        except BaseException as exc:  # noqa: BLE001 - reported by the assertions
            outcomes[name] = exc

    one = threading.Thread(target=call, args=("first", first), daemon=True)
    one.start()
    assert pausing.paused.wait(timeout=10), "the first phase never reached its write"
    two = threading.Thread(target=call, args=("second", second), daemon=True)
    two.start()
    two.join(timeout=2)  # unserialized, the second phase finishes inside this window
    pausing.resume.set()
    one.join(timeout=30)
    two.join(timeout=30)
    assert not one.is_alive() and not two.is_alive(), "a phase never finished"
    for name, outcome in outcomes.items():
        assert not isinstance(outcome, BaseException), f"{name} phase failed: {outcome!r}"
    return outcomes


def _sim_device(tmp_path: Path) -> Device:
    return Device.from_mapping(
        "LOCAL_SIM", {"kind": "local-sim", "project_root": str(tmp_path / "remote")}
    )


@pytest.mark.parametrize("first", ["commit", "ordinary"])
def test_concurrent_pullbacks_of_one_path_never_overwrite_each_other(
    tmp_path: Path, monkeypatch, first: str,
):
    """A commit run and an ordinary run both changed ``out/r.txt``. Whichever pullback
    writes it first wins; the other must see that write as a local change and save its
    own result as a conflict, never overwrite it (invariant 6)."""
    monkeypatch.setattr(reconcile_module, "LOCAL_WRITE_WAIT_SECONDS", 20.0, raising=False)
    local = tmp_path / "local" / "proj"
    (local / "out").mkdir(parents=True)
    target = local / "out" / "r.txt"
    target.write_text("as launched")
    pre_local = manifest(local)
    roots = {"commit": tmp_path / "remote" / "tree", "ordinary": tmp_path / "remote" / "proj"}
    produced = {"commit": "commit run result", "ordinary": "ordinary run's result"}
    pre_remote = {}
    for name, root in roots.items():
        (root / "out").mkdir(parents=True)
        (root / "out" / "r.txt").write_text("as launched")
        pre_remote[name] = manifest(root)
        (root / "out" / "r.txt").write_text(produced[name])  # the command's output

    def pullback(name: str, transport: LocalSimTransport):  # noqa: ANN202
        scoped = {"within": ["out/"], "include_paths": ["out/"]} if name == "commit" else {}
        return lambda: postrun_pullback(
            transport=transport, local_root=local, remote_root=str(roots[name]),
            excludes=["node_modules/**"], hash_below_bytes=1_000_000,
            pre_remote_manifest=pre_remote[name], pre_local_manifest=pre_local,
            backup_root=tmp_path / name / "backup",
            conflict_remote_root=tmp_path / name / "remote", **scoped,
        )

    second = "ordinary" if first == "commit" else "commit"
    pausing = _PausingTransport(_sim_device(tmp_path), target)
    outcomes = _interleave(
        pullback(first, pausing), pullback(second, make_transport(tmp_path / "remote")),
        pausing,
    )
    won, lost = outcomes["first"], outcomes["second"]

    # The phase that checked first writes first; the other sees its write.
    assert target.read_text() == produced[first]
    assert (won.pulled, won.conflicts) == (["out/r.txt"], [])
    assert (lost.pulled, lost.conflicts) == ([], ["out/r.txt"])
    assert (tmp_path / second / "remote" / "out" / "r.txt").read_text() == produced[second]
    assert lost.next_baseline is None


@pytest.mark.parametrize("first", ["preflight", "commit"])
def test_preflight_local_pull_and_a_commit_pullback_never_overwrite_each_other(
    tmp_path: Path, monkeypatch, first: str,
):
    """An ordinary run's preflight plans to pull a newer ``out/r.txt`` from its target
    while a commit run's pullback writes the same local file. If the commit result
    lands first, preflight must leave it and refuse to start; if preflight's pull lands
    first, the commit pullback must save its result as a conflict."""
    monkeypatch.setattr(reconcile_module, "LOCAL_WRITE_WAIT_SECONDS", 20.0, raising=False)
    local, remote_root, transport, backup = setup(tmp_path)
    (local / "out").mkdir()
    target = local / "out" / "r.txt"
    target.write_text("as launched")
    synced = reconcile(local, remote_root, transport, backup)
    assert not synced.has_conflicts
    (Path(remote_root) / "out" / "r.txt").write_text("newer on the target")
    tree = tmp_path / "remote" / "tree"
    (tree / "out").mkdir(parents=True)
    (tree / "out" / "r.txt").write_text("as launched")
    tree_pre = manifest(tree)
    (tree / "out" / "r.txt").write_text("commit run result")
    commit_conflicts = tmp_path / "commit" / "remote"

    def preflight(t: LocalSimTransport):  # noqa: ANN202
        return lambda: reconcile(
            local, remote_root, t, backup,
            prev_local=synced.local_manifest, prev_remote=synced.remote_manifest,
        )

    def commit_pullback(t: LocalSimTransport):  # noqa: ANN202
        return lambda: postrun_pullback(
            transport=t, local_root=local, remote_root=str(tree),
            excludes=["node_modules/**"], hash_below_bytes=1_000_000,
            pre_remote_manifest=tree_pre, pre_local_manifest=synced.local_manifest,
            backup_root=tmp_path / "commit" / "backup",
            conflict_remote_root=commit_conflicts,
            within=["out/"], include_paths=["out/"],
        )

    pausing = _PausingTransport(_sim_device(tmp_path), target)
    plain = make_transport(tmp_path / "remote")
    if first == "preflight":
        outcomes = _interleave(preflight(pausing), commit_pullback(plain), pausing)
        pre, post = outcomes["first"], outcomes["second"]
        assert target.read_text() == "newer on the target"
        assert (pre.pulled, pre.conflicts) == (["out/r.txt"], [])
        assert (post.pulled, post.conflicts) == ([], ["out/r.txt"])
        assert (commit_conflicts / "out" / "r.txt").read_text() == "commit run result"
    else:
        outcomes = _interleave(commit_pullback(pausing), preflight(plain), pausing)
        post, pre = outcomes["first"], outcomes["second"]
        assert target.read_text() == "commit run result"
        assert (post.pulled, post.conflicts) == (["out/r.txt"], [])
        # Preflight left the commit result in place and does not let the run start.
        assert pre.pulled == []
        assert [c.path for c in pre.conflicts] == ["out/r.txt"]
        assert (Path(remote_root) / "out" / "r.txt").read_text() == "newer on the target"


def _one_output(tmp_path: Path):  # noqa: ANN202
    """A run whose command changed ``out.txt``; returns the pullback to finish it."""
    local, remote_root, transport, backup = setup(tmp_path)
    (local / "out.txt").write_text("before")
    pre = reconcile(local, remote_root, transport, backup)
    (Path(remote_root) / "out.txt").write_text("after the run")
    events: list[tuple[str, dict]] = []

    def pullback(t=transport):  # noqa: ANN001, ANN202
        return postrun_pullback(
            transport=t, local_root=local, remote_root=remote_root,
            excludes=["node_modules/**"], hash_below_bytes=1_000_000,
            pre_remote_manifest=pre.remote_manifest, pre_local_manifest=pre.local_manifest,
            backup_root=backup, conflict_remote_root=tmp_path / "state" / "remote",
            report=lambda event, **fields: events.append((event, fields)),
        )
    return local, pullback, events


def test_local_write_lock_is_released_when_the_pullback_fails(tmp_path: Path):
    """A pullback that raises mid-write must not leave the tree locked: every later
    run of the project would otherwise wait on a holder that is still alive."""
    from remrun.state import LockError, local_write_lock

    local, pullback, _events = _one_output(tmp_path)
    held_during_write: list[bool] = []

    class FailingPull(LocalSimTransport):
        def pull_file(self, remote_path: str, local_path: Path) -> None:
            probe = local_write_lock(local)
            try:
                probe.acquire()
            except LockError:
                held_during_write.append(True)
            else:
                probe.release()
                held_during_write.append(False)
            raise TransportError("injected pull failure")

    with pytest.raises(TransportError, match="injected pull failure"):
        pullback(FailingPull(_sim_device(tmp_path)))
    assert held_during_write == [True]
    assert (local / "out.txt").read_text() == "before"
    local_write_lock(local).acquire().release()  # free again


def test_dead_local_write_holder_is_reclaimed(tmp_path: Path, monkeypatch):
    """A controller killed mid-pullback leaves its lock behind; once its process has
    provably exited, the next writer reclaims it and says so."""
    from remrun.state import local_write_lock

    local, pullback, events = _one_output(tmp_path)
    stale = local_write_lock(local)
    stale.acquire()
    info = state_module.read_json(stale.path / "info.json")
    state_module.write_json(stale.path / "info.json", {**info, "pid": 4242})
    real = state_module.pid_liveness
    monkeypatch.setattr(
        state_module, "pid_liveness", lambda pid: False if pid == 4242 else real(pid)
    )

    post = pullback()

    assert post.pulled == ["out.txt"] and (local / "out.txt").read_text() == "after the run"
    assert [(e, f["lock"], f["pid"]) for e, f in events if e == "stale_lock_reclaimed"] == [
        ("stale_lock_reclaimed", "local_writes", 4242)
    ]
    assert not stale.path.exists()


def test_live_local_write_holder_bounds_the_wait_and_writes_nothing(
    tmp_path: Path, monkeypatch,
):
    """Another live process still writing into the tree is waited on, but not forever:
    the pullback then fails as a transfer error before touching the file."""
    from remrun.state import local_write_lock

    monkeypatch.setattr(reconcile_module, "LOCAL_WRITE_WAIT_SECONDS", 0.3)
    local, pullback, events = _one_output(tmp_path)
    holder = local_write_lock(local).acquire()
    try:
        with pytest.raises(TransportError, match="still writing into"):
            pullback()
    finally:
        holder.release()
    assert (local / "out.txt").read_text() == "before"
    assert [(e, f["lock"]) for e, f in events] == [("lock_wait", "local_writes")]
