from remrun.manifest import FileEntry
from remrun.transfer_plan import (
    ABORT_CONFLICT,
    DELETE_LOCAL,
    DELETE_REMOTE,
    NONE,
    PULL,
    PUSH,
    compare_manifests,
    diff_remote_changes,
)


def e(path: str, mtime: int, size: int = 1, sha: str | None = None):
    return FileEntry(path=path, kind="file", size=size, mtime_ns=mtime, sha256=sha)


def only(plan):
    assert len(plan.paths) == 1, plan.paths
    return plan.paths[0]


# --- backward-compatible (no previous manifest) -------------------------------

def test_local_only_push():
    assert only(compare_manifests({"a": e("a", 1)}, {})).action == PUSH


def test_remote_only_pull():
    assert only(compare_manifests({}, {"a": e("a", 1)})).action == PULL


def test_no_prev_differ_is_conflict_not_mtime_guess():
    # With NO baseline, a both-present difference must NOT be resolved by comparing
    # the two devices' mtimes (clocks differ across the fleet). Unhashed + different
    # size -> we can't prove direction -> conflict (resolve once, baseline then drives).
    p = only(compare_manifests({"a": e("a", 3, size=1)}, {"a": e("a", 2, size=2)}))
    assert p.action == ABORT_CONFLICT


def test_no_prev_same_hash_is_none_regardless_of_mtime():
    # Identical content but very different mtimes -> same (no false transfer).
    p = only(compare_manifests({"a": e("a", 9, size=5, sha="H")},
                               {"a": e("a", 1, size=5, sha="H")}))
    assert p.action == NONE


def test_no_prev_diff_hash_is_conflict():
    p = only(compare_manifests({"a": e("a", 3, size=5, sha="L")},
                               {"a": e("a", 2, size=5, sha="R")}))
    assert p.action == ABORT_CONFLICT


def test_same_metadata_without_hash_is_unverified():
    p = only(compare_manifests({"a": e("a", 2)}, {"a": e("a", 2)}))
    assert p.state == "both-present-unverified"
    assert p.action == ABORT_CONFLICT


def test_equal_mtime_diff_meta_conflict():
    p = only(compare_manifests({"a": e("a", 2, size=1)}, {"a": e("a", 2, size=2)}))
    assert p.action == ABORT_CONFLICT


def test_no_prev_never_deletes():
    # local-only and remote-only must never be a delete without prev evidence.
    plan = compare_manifests({"a": e("a", 1)}, {"b": e("b", 1)})
    actions = {p.path: p.action for p in plan.paths}
    assert actions == {"a": PUSH, "b": PULL}


# --- previous-manifest-aware classification -----------------------------------

def test_both_changed_conflict():
    prev_l = {"a": e("a", 1)}
    prev_r = {"a": e("a", 1)}
    local = {"a": e("a", 5, sha="L")}
    remote = {"a": e("a", 6, sha="R")}
    p = only(compare_manifests(local, remote, prev_l, prev_r))
    assert p.state == "both-changed"
    assert p.action == ABORT_CONFLICT


def test_only_local_changed_push():
    prev = {"a": e("a", 1)}
    local = {"a": e("a", 5)}
    remote = {"a": e("a", 1)}
    p = only(compare_manifests(local, remote, prev, prev))
    assert p.state == "local-newer"
    assert p.action == PUSH


def test_only_remote_changed_pull():
    prev = {"a": e("a", 1)}
    local = {"a": e("a", 1)}
    remote = {"a": e("a", 5)}
    p = only(compare_manifests(local, remote, prev, prev))
    assert p.state == "remote-newer"
    assert p.action == PULL


def test_neither_changed_since_baseline_is_noop_even_if_cross_device_meta_differs():
    # Large unhashed file: local mtime 100, remote mtime 200 (different clocks/precision),
    # but NEITHER side changed vs its own baseline -> no-op, never an mtime guess/conflict.
    prev_l = {"a": e("a", 100, size=5)}
    prev_r = {"a": e("a", 200, size=5)}
    local = {"a": e("a", 100, size=5)}   # == prev_l (unchanged on local)
    remote = {"a": e("a", 200, size=5)}  # == prev_r (unchanged on remote)
    p = only(compare_manifests(local, remote, prev_l, prev_r))
    assert p.action == NONE


def test_known_local_deletion_deletes_remote():
    # Local had it last run, dropped it; remote unchanged -> delete remote.
    prev_l = {"a": e("a", 1)}
    prev_r = {"a": e("a", 1)}
    local: dict = {}
    remote = {"a": e("a", 1)}
    p = only(compare_manifests(local, remote, prev_l, prev_r))
    assert p.state == "local-deleted-known"
    assert p.action == DELETE_REMOTE


def test_known_remote_deletion_deletes_local():
    prev_l = {"a": e("a", 1)}
    prev_r = {"a": e("a", 1)}
    local = {"a": e("a", 1)}
    remote: dict = {}
    p = only(compare_manifests(local, remote, prev_l, prev_r))
    assert p.state == "remote-deleted-known"
    assert p.action == DELETE_LOCAL


def test_local_deletion_but_remote_modified_is_conflict():
    prev_l = {"a": e("a", 1)}
    prev_r = {"a": e("a", 1)}
    local: dict = {}
    remote = {"a": e("a", 9)}  # remote changed since prev
    p = only(compare_manifests(local, remote, prev_l, prev_r))
    assert p.state == "unknown-deletion"
    assert p.action == ABORT_CONFLICT


def test_remote_deletion_but_local_modified_is_conflict():
    prev_l = {"a": e("a", 1)}
    prev_r = {"a": e("a", 1)}
    local = {"a": e("a", 9)}  # local changed since prev
    remote: dict = {}
    p = only(compare_manifests(local, remote, prev_l, prev_r))
    assert p.state == "unknown-deletion"
    assert p.action == ABORT_CONFLICT


def test_new_on_both_sides_with_prev_still_push_pull():
    # Files that never existed before are genuinely new, not deletions.
    prev: dict = {}
    plan = compare_manifests({"a": e("a", 1)}, {"b": e("b", 1)}, prev, prev)
    actions = {p.path: p.action for p in plan.paths}
    assert actions == {"a": PUSH, "b": PULL}


# --- post-run remote diff -----------------------------------------------------

def test_diff_remote_changes():
    pre = {"keep": e("keep", 1), "mod": e("mod", 1), "gone": e("gone", 1)}
    post = {"keep": e("keep", 1), "mod": e("mod", 2), "new": e("new", 1)}
    changed, deleted = diff_remote_changes(pre, post)
    assert changed == ["mod", "new"]
    assert deleted == ["gone"]
