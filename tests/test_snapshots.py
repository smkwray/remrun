import uuid
from dataclasses import replace

from remrun.coordination import (
    ReplicaCursor,
    build_canonical_snapshot,
    canonical_policy_digest,
    deletion_tombstones,
    plan_replica_catchup,
)
from remrun.manifest import FileEntry


def entry(path: str, digest: str, *, mtime: int = 1, mode: int = 0o644) -> FileEntry:
    return FileEntry(path, "file", 3, mtime, digest, mode)


def snapshot(manifest, generation, policy, *, parent=None):
    return build_canonical_snapshot(
        manifest,
        generation=generation,
        parent_generation=parent,
        policy_sha256=policy,
    )


def cursor(base, *, state="ACTIVE"):
    acknowledged = state != "UNVERIFIED"
    return ReplicaCursor(
        format="remrun-replica-cursor-v1",
        project_key="1" * 64,
        replica_id=str(uuid.uuid4()),
        replica_kind="CONTROLLER",
        endpoint_id="DEVICE_B",
        root_fingerprint="root-device-b",
        credential_sha256="2" * 64,
        state=state,
        ack_generation=base.generation if acknowledged else None,
        ack_manifest_sha256=base.manifest_sha256 if acknowledged else None,
    )


def decision(plan, path):
    return next(item for item in plan.decisions if item.path == path)


def test_policy_digest_is_order_independent_but_policy_sensitive():
    first = canonical_policy_digest(["tmp/**", "data/**", "tmp/**"])
    second = canonical_policy_digest(["data/**", "tmp/**"])
    assert first == second
    assert first != canonical_policy_digest(["tmp/**"])


def test_snapshot_is_deterministic_and_requires_strong_identities():
    policy = canonical_policy_digest([])
    digest_a = "a" * 64
    digest_b = "b" * 64
    first = build_canonical_snapshot(
        {"b.txt": entry("b.txt", digest_b), "a.txt": entry("a.txt", digest_a)},
        generation=0, parent_generation=None, policy_sha256=policy,
    )
    second = build_canonical_snapshot(
        {"a.txt": entry("a.txt", digest_a, mtime=999), "b.txt": entry("b.txt", digest_b)},
        generation=0, parent_generation=None, policy_sha256=policy,
    )
    assert first.manifest_sha256 == second.manifest_sha256
    assert [item["path"] for item in first.entries] == ["a.txt", "b.txt"]


def test_snapshot_rejects_unhashed_or_invalid_generation_records():
    policy = canonical_policy_digest([])
    unhashed = FileEntry("a.txt", "file", 3, 1, None, 0o644)
    try:
        build_canonical_snapshot(
            {"a.txt": unhashed}, generation=0, parent_generation=None,
            policy_sha256=policy,
        )
    except ValueError as exc:
        assert "strong regular-file identity" in str(exc)
    else:
        raise AssertionError("unhashed snapshot entry was accepted")


def test_deletion_tombstone_preserves_prior_identity():
    prior = entry("gone.txt", "c" * 64, mode=0o755)
    events = deletion_tombstones(
        {"gone.txt": prior, "kept.txt": entry("kept.txt", "d" * 64)},
        {"kept.txt": entry("kept.txt", "d" * 64)},
        deleted_generation=3,
        deleted_by_txn_id="txn-3",
    )
    assert len(events) == 1
    assert events[0].path == "gone.txt"
    assert events[0].deleted_generation == 3
    assert events[0].prior_identity["sha256"] == "c" * 64
    assert events[0].prior_identity["mode"] == 0o755


def test_lagging_replica_predelete_bytes_are_deleted_not_resurrected():
    policy = canonical_policy_digest([])
    prior = entry("gone.txt", "a" * 64)
    base = snapshot({"gone.txt": prior}, 0, policy)
    head = snapshot({}, 1, policy, parent=0)
    tombstones = deletion_tombstones(
        {"gone.txt": prior}, {}, deleted_generation=1, deleted_by_txn_id="txn-1",
    )

    plan = plan_replica_catchup(
        cursor(base), base=base, head=head,
        current={"gone.txt": entry("gone.txt", "a" * 64, mtime=999)},
        tombstones=tombstones, expected_policy_sha256=policy,
    )

    assert plan.state == "READY"
    assert decision(plan, "gone.txt").action == "APPLY_HEAD"
    assert decision(plan, "gone.txt").reason == "STALE_PRE_DELETE"


def test_modified_stale_copy_is_delete_modify_conflict():
    policy = canonical_policy_digest([])
    prior = entry("gone.txt", "a" * 64)
    modified = entry("gone.txt", "b" * 64)
    base = snapshot({"gone.txt": prior}, 0, policy)
    head = snapshot({}, 1, policy, parent=0)
    tombstones = deletion_tombstones(
        {"gone.txt": prior}, {}, deleted_generation=1, deleted_by_txn_id="txn-1",
    )

    plan = plan_replica_catchup(
        cursor(base), base=base, head=head, current={"gone.txt": modified},
        tombstones=tombstones, expected_policy_sha256=policy,
    )

    assert plan.state == "CONFLICT"
    assert decision(plan, "gone.txt").reason == "DELETE_MODIFY"


def test_recreation_after_acknowledged_deletion_is_a_replica_edit():
    policy = canonical_policy_digest([])
    prior = entry("gone.txt", "a" * 64)
    recreated = entry("gone.txt", "b" * 64)
    base = snapshot({}, 1, policy, parent=0)
    tombstones = deletion_tombstones(
        {"gone.txt": prior}, {}, deleted_generation=1, deleted_by_txn_id="txn-1",
    )

    plan = plan_replica_catchup(
        cursor(base), base=base, head=base, current={"gone.txt": recreated},
        tombstones=tombstones, expected_policy_sha256=policy,
    )

    assert plan.state == "READY"
    assert decision(plan, "gone.txt").action == "ADOPT_REPLICA"
    assert decision(plan, "gone.txt").reason == "POST_DELETE_RECREATION"


def test_unverified_replica_is_blocked_before_path_decisions():
    policy = canonical_policy_digest([])
    base = snapshot({}, 0, policy)

    plan = plan_replica_catchup(
        cursor(base, state="UNVERIFIED"), base=base, head=base, current={},
        tombstones=(), expected_policy_sha256=policy,
    )

    assert plan.state == "BLOCKED"
    assert plan.reason == "UNVERIFIED_REPLICA"
    assert plan.decisions == ()


def test_offline_replica_catches_up_over_multiple_generations():
    policy = canonical_policy_digest([])
    old = entry("changed.txt", "a" * 64)
    new = entry("changed.txt", "b" * 64)
    added = entry("added.txt", "c" * 64)
    base = snapshot({"changed.txt": old}, 0, policy)
    head = snapshot({"changed.txt": new, "added.txt": added}, 4, policy, parent=3)

    plan = plan_replica_catchup(
        cursor(base), base=base, head=head, current={"changed.txt": old},
        tombstones=(), expected_policy_sha256=policy,
    )

    assert plan.state == "READY"
    assert decision(plan, "changed.txt").action == "APPLY_HEAD"
    assert decision(plan, "added.txt").action == "APPLY_HEAD"


def test_policy_mismatch_fails_closed():
    policy = canonical_policy_digest([])
    other_policy = canonical_policy_digest(["data/**"])
    base = snapshot({}, 0, policy)
    head = snapshot({}, 1, other_policy, parent=0)

    plan = plan_replica_catchup(
        cursor(base), base=base, head=head, current={},
        tombstones=(), expected_policy_sha256=policy,
    )

    assert plan.state == "BLOCKED"
    assert plan.reason == "POLICY_MISMATCH"


def test_tampered_snapshot_digest_fails_closed():
    policy = canonical_policy_digest([])
    base = snapshot({"a.txt": entry("a.txt", "a" * 64)}, 0, policy)
    tampered = replace(base, manifest_sha256="f" * 64)

    plan = plan_replica_catchup(
        cursor(tampered), base=tampered, head=tampered,
        current={"a.txt": entry("a.txt", "a" * 64)},
        tombstones=(), expected_policy_sha256=policy,
    )

    assert plan.state == "BLOCKED"
    assert plan.reason == "INVALID_IDENTITY"
