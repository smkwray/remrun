"""Controller-side identities and durable acquire intents for runner-v1 coordination.

This remains shadow-only groundwork. Legacy run/sync paths do not import it.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from .manifest import Manifest, canonical_identity, strong_manifest_digest


def project_key(cluster_id: str, normalized_project_id: str) -> str:
    value = b"remrun-project-v1\0" + cluster_id.encode() + b"\0" + normalized_project_id.encode()
    return hashlib.sha256(value).hexdigest()


def _sha256_hex(value: str, label: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{label} must be a lowercase SHA-256 hex digest")
    return value


def canonical_policy_digest(exclude_patterns: Iterable[str]) -> str:
    """Digest the Step-5 snapshot policy without changing the legacy planner."""
    policy = {
        "format": "remrun-snapshot-policy-v1",
        "identity": "strong-manifest-v2",
        "exclude": sorted({str(pattern).strip() for pattern in exclude_patterns
                           if str(pattern).strip()}),
        "symlinks": "excluded",
        "special_files": "excluded",
        "path_case": "preserved",
    }
    return hashlib.sha256(
        json.dumps(policy, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class CanonicalSnapshot:
    """Portable full snapshot record; storage/RPC wiring remains shadow-only."""

    format: str
    generation: int
    parent_generation: int | None
    policy_sha256: str
    manifest_sha256: str
    entries: tuple[dict, ...]

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class TombstoneEvent:
    path: str
    deleted_generation: int
    prior_identity: dict
    deleted_by_txn_id: str

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class ReplicaCursor:
    """Portable Step-5 acknowledgement record; persistence remains shadow-only."""

    format: str
    project_key: str
    replica_id: str
    replica_kind: str
    endpoint_id: str
    root_fingerprint: str
    credential_sha256: str
    state: str
    ack_generation: int | None = None
    ack_manifest_sha256: str | None = None
    pending_txn_id: str | None = None
    last_seen_at_ns: int | None = None

    def __post_init__(self) -> None:
        if self.format != "remrun-replica-cursor-v1":
            raise ValueError("unsupported replica cursor format")
        _sha256_hex(self.project_key, "project_key")
        _sha256_hex(self.credential_sha256, "credential_sha256")
        uuid.UUID(self.replica_id)
        if self.replica_kind not in {"CONTROLLER", "RUNNER"}:
            raise ValueError("replica_kind must be CONTROLLER or RUNNER")
        if self.state not in {"ACTIVE", "DIRTY", "UNVERIFIED", "RETIRED"}:
            raise ValueError("invalid replica state")
        if not self.endpoint_id or not self.root_fingerprint:
            raise ValueError("endpoint_id and root_fingerprint are required")
        if (self.ack_generation is None) != (self.ack_manifest_sha256 is None):
            raise ValueError("ack generation and manifest digest must be present together")
        if self.ack_generation is not None:
            if self.ack_generation < 0:
                raise ValueError("ack_generation must be nonnegative")
            _sha256_hex(self.ack_manifest_sha256 or "", "ack_manifest_sha256")
        if self.state == "ACTIVE" and self.ack_generation is None:
            raise ValueError("an active replica requires a verified acknowledgement")
        if self.state == "UNVERIFIED" and self.ack_generation is not None:
            raise ValueError("an unverified replica cannot claim an acknowledgement")
        if self.last_seen_at_ns is not None and self.last_seen_at_ns < 0:
            raise ValueError("last_seen_at_ns must be nonnegative")

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class CatchUpDecision:
    path: str
    action: str
    reason: str
    base_identity: dict | None
    head_identity: dict | None
    replica_identity: dict | None
    tombstone_generation: int | None = None

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class CatchUpPlan:
    """Pure Step-5 three-way result; this record never mutates a replica."""

    format: str
    state: str
    reason: str | None
    replica_id: str
    ack_generation: int | None
    head_generation: int
    decisions: tuple[CatchUpDecision, ...]

    @property
    def can_apply(self) -> bool:
        return self.state == "READY"

    def as_dict(self) -> dict:
        return asdict(self)


def build_canonical_snapshot(
    manifest: Manifest,
    *,
    generation: int,
    parent_generation: int | None,
    policy_sha256: str,
) -> CanonicalSnapshot:
    """Build a deterministic Step-5 record from a fully hashed manifest."""
    if generation < 0:
        raise ValueError("generation must be nonnegative")
    if generation == 0 and parent_generation is not None:
        raise ValueError("generation zero cannot have a parent")
    if generation > 0 and (parent_generation is None or parent_generation >= generation):
        raise ValueError("a later generation requires an earlier parent")
    _sha256_hex(policy_sha256, "policy_sha256")

    entries: list[dict] = []
    for path in sorted(manifest):
        entry = manifest[path]
        if entry.path != path:
            raise ValueError(f"snapshot key/path mismatch: {path}")
        if entry.kind != "file" or entry.mode is None or entry.sha256 is None:
            raise ValueError(f"snapshot entry is not a strong regular-file identity: {path}")
        _sha256_hex(entry.sha256, f"sha256 for {path}")
        entries.append({"path": path, **canonical_identity(entry)})
    return CanonicalSnapshot(
        format="remrun-canonical-snapshot-v1",
        generation=generation,
        parent_generation=parent_generation,
        policy_sha256=policy_sha256,
        manifest_sha256=strong_manifest_digest(manifest),
        entries=tuple(entries),
    )


def deletion_tombstones(
    parent: Manifest,
    current: Manifest,
    *,
    deleted_generation: int,
    deleted_by_txn_id: str,
) -> tuple[TombstoneEvent, ...]:
    """Describe parent paths absent from the next full snapshot; apply nothing."""
    if deleted_generation <= 0:
        raise ValueError("deleted_generation must be positive")
    if not deleted_by_txn_id:
        raise ValueError("deleted_by_txn_id is required")
    events = []
    for path in sorted(set(parent) - set(current)):
        entry = parent[path]
        if entry.sha256 is None or entry.mode is None:
            raise ValueError(f"prior identity is not strong enough to tombstone: {path}")
        events.append(TombstoneEvent(
            path=path,
            deleted_generation=deleted_generation,
            prior_identity=canonical_identity(entry),
            deleted_by_txn_id=deleted_by_txn_id,
        ))
    return tuple(events)


def _validate_identity(path: str, identity: dict, label: str) -> None:
    if (path.startswith("/") or "\\" in path or "\0" in path
            or any(part in {"", ".", ".."} for part in path.split("/"))):
        raise ValueError(f"{label} has a noncanonical path: {path}")
    if set(identity) != {"kind", "size", "mtime_ns", "sha256", "mode"}:
        raise ValueError(f"{label} has an invalid identity schema: {path}")
    if identity["kind"] != "file":
        raise ValueError(f"{label} is not a regular file: {path}")
    if type(identity["size"]) is not int or identity["size"] < 0:
        raise ValueError(f"{label} has an invalid size: {path}")
    if type(identity["mtime_ns"]) is not int or identity["mtime_ns"] < 0:
        raise ValueError(f"{label} has an invalid mtime_ns: {path}")
    if type(identity["mode"]) is not int or not 0 <= identity["mode"] <= 0o7777:
        raise ValueError(f"{label} has an invalid mode: {path}")
    _sha256_hex(str(identity["sha256"]), f"{label} sha256 for {path}")


def _content_identity(identity: dict) -> dict:
    """Match the strong-manifest certificate: content+mode, not sync-noisy mtime."""
    return {
        "kind": identity["kind"],
        "size": identity["size"],
        "sha256": identity["sha256"],
        "mode": identity["mode"],
    }


def _identity_manifest_digest(identities: dict[str, dict]) -> str:
    digest = hashlib.sha256()
    for path in sorted(identities):
        record = json.dumps(
            {"path": path, **_content_identity(identities[path])},
            sort_keys=True,
            separators=(",", ":"),
        )
        digest.update(record.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _snapshot_identities(snapshot: CanonicalSnapshot) -> dict[str, dict]:
    if snapshot.format != "remrun-canonical-snapshot-v1":
        raise ValueError("unsupported canonical snapshot format")
    if snapshot.generation < 0:
        raise ValueError("snapshot generation must be nonnegative")
    if snapshot.generation == 0 and snapshot.parent_generation is not None:
        raise ValueError("generation zero snapshot cannot have a parent")
    if snapshot.generation > 0 and (
        snapshot.parent_generation is None
        or snapshot.parent_generation >= snapshot.generation
    ):
        raise ValueError("later snapshot requires an earlier parent")
    _sha256_hex(snapshot.policy_sha256, "snapshot policy_sha256")
    _sha256_hex(snapshot.manifest_sha256, "snapshot manifest_sha256")
    raw_result: dict[str, dict] = {}
    for raw in snapshot.entries:
        item = dict(raw)
        path = item.pop("path", None)
        if not isinstance(path, str) or not path or path in raw_result:
            raise ValueError("snapshot entries require unique nonempty paths")
        _validate_identity(path, item, "snapshot identity")
        raw_result[path] = item
    if _identity_manifest_digest(raw_result) != snapshot.manifest_sha256:
        raise ValueError("snapshot entries do not match manifest_sha256")
    return {path: _content_identity(identity) for path, identity in raw_result.items()}


def _strong_manifest_identities(manifest: Manifest) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for path, entry in manifest.items():
        if entry.path != path:
            raise ValueError(f"replica key/path mismatch: {path}")
        if entry.kind != "file" or entry.mode is None or entry.sha256 is None:
            raise ValueError(f"replica entry is not a strong regular-file identity: {path}")
        identity = canonical_identity(entry)
        _validate_identity(path, identity, "replica identity")
        result[path] = _content_identity(identity)
    return result


def _blocked_catchup(
    cursor: ReplicaCursor,
    head: CanonicalSnapshot,
    reason: str,
) -> CatchUpPlan:
    return CatchUpPlan(
        format="remrun-catch-up-plan-v1",
        state="BLOCKED",
        reason=reason,
        replica_id=cursor.replica_id,
        ack_generation=cursor.ack_generation,
        head_generation=head.generation,
        decisions=(),
    )


def plan_replica_catchup(
    cursor: ReplicaCursor,
    *,
    base: CanonicalSnapshot,
    head: CanonicalSnapshot,
    current: Manifest,
    tombstones: Iterable[TombstoneEvent],
    expected_policy_sha256: str,
) -> CatchUpPlan:
    """Three-way one-replica catch-up with tombstone-aware deletion authority."""
    _sha256_hex(expected_policy_sha256, "expected_policy_sha256")
    if base.policy_sha256 != expected_policy_sha256 or head.policy_sha256 != expected_policy_sha256:
        return _blocked_catchup(cursor, head, "POLICY_MISMATCH")
    if cursor.state == "UNVERIFIED":
        return _blocked_catchup(cursor, head, "UNVERIFIED_REPLICA")
    if cursor.state == "RETIRED":
        return _blocked_catchup(cursor, head, "RETIRED_REPLICA")
    if cursor.ack_generation is None or cursor.ack_manifest_sha256 is None:
        return _blocked_catchup(cursor, head, "MISSING_ACKNOWLEDGEMENT")
    if (cursor.ack_generation != base.generation
            or cursor.ack_manifest_sha256 != base.manifest_sha256):
        return _blocked_catchup(cursor, head, "CURSOR_BASE_MISMATCH")
    if head.generation < base.generation:
        return _blocked_catchup(cursor, head, "HEAD_PRECEDES_CURSOR")

    try:
        base_identities = _snapshot_identities(base)
        head_identities = _snapshot_identities(head)
        replica_identities = _strong_manifest_identities(current)
    except ValueError:
        return _blocked_catchup(cursor, head, "INVALID_IDENTITY")

    events_by_path: dict[str, list[TombstoneEvent]] = {}
    seen_events: set[tuple[str, int]] = set()
    for event in tombstones:
        event_key = (event.path, event.deleted_generation)
        if event_key in seen_events or not event.deleted_by_txn_id:
            return _blocked_catchup(cursor, head, "INVALID_TOMBSTONE_HISTORY")
        seen_events.add(event_key)
        if event.deleted_generation <= 0 or event.deleted_generation > head.generation:
            return _blocked_catchup(cursor, head, "INVALID_TOMBSTONE_HISTORY")
        prior = event.prior_identity
        try:
            _validate_identity(event.path, prior, "tombstone prior identity")
        except ValueError:
            return _blocked_catchup(cursor, head, "INVALID_TOMBSTONE_HISTORY")
        events_by_path.setdefault(event.path, []).append(event)
    for events in events_by_path.values():
        events.sort(key=lambda item: item.deleted_generation)

    decisions: list[CatchUpDecision] = []
    paths = sorted(set(base_identities) | set(head_identities) | set(replica_identities))
    for path in paths:
        base_identity = base_identities.get(path)
        head_identity = head_identities.get(path)
        replica_identity = replica_identities.get(path)
        path_events = events_by_path.get(path, [])
        unseen_deletions = [
            event for event in path_events
            if cursor.ack_generation < event.deleted_generation <= head.generation
        ]

        # A deletion after this replica's cursor has authority over any surviving copy.
        # Exact historical bytes are stale; different bytes are a delete/modify conflict.
        if head_identity is None and replica_identity is not None and unseen_deletions:
            matching = [
                event for event in unseen_deletions
                if _content_identity(event.prior_identity) == replica_identity
            ]
            if matching:
                event = matching[-1]
                decisions.append(CatchUpDecision(
                    path, "APPLY_HEAD", "STALE_PRE_DELETE",
                    base_identity, head_identity, replica_identity,
                    event.deleted_generation,
                ))
            else:
                decisions.append(CatchUpDecision(
                    path, "CONFLICT", "DELETE_MODIFY",
                    base_identity, head_identity, replica_identity,
                    unseen_deletions[-1].deleted_generation,
                ))
            continue

        if replica_identity == base_identity and head_identity == base_identity:
            decisions.append(CatchUpDecision(
                path, "NOOP", "CLEAN",
                base_identity, head_identity, replica_identity,
            ))
        elif replica_identity == head_identity:
            decisions.append(CatchUpDecision(
                path, "NOOP", "CONVERGED_CHANGE",
                base_identity, head_identity, replica_identity,
            ))
        elif replica_identity == base_identity:
            if base_identity is not None and head_identity is None:
                decisions.append(CatchUpDecision(
                    path, "CONFLICT", "MISSING_TOMBSTONE",
                    base_identity, head_identity, replica_identity,
                ))
            else:
                decisions.append(CatchUpDecision(
                    path, "APPLY_HEAD", "CANONICAL_CHANGE",
                    base_identity, head_identity, replica_identity,
                ))
        elif head_identity == base_identity:
            observed_deletion = (
                head_identity is None
                and any(event.deleted_generation <= cursor.ack_generation
                        for event in path_events)
            )
            decisions.append(CatchUpDecision(
                path, "ADOPT_REPLICA",
                "POST_DELETE_RECREATION" if observed_deletion else "REPLICA_EDIT",
                base_identity, head_identity, replica_identity,
            ))
        else:
            delete_modify = (head_identity is None) != (replica_identity is None)
            decisions.append(CatchUpDecision(
                path, "CONFLICT",
                "DELETE_MODIFY" if delete_modify else "DIVERGENT_EDIT",
                base_identity, head_identity, replica_identity,
            ))

    state = "CONFLICT" if any(item.action == "CONFLICT" for item in decisions) else "READY"
    return CatchUpPlan(
        format="remrun-catch-up-plan-v1",
        state=state,
        reason=None,
        replica_id=cursor.replica_id,
        ack_generation=cursor.ack_generation,
        head_generation=head.generation,
        decisions=tuple(decisions),
    )


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw_temp = tempfile.mkstemp(prefix=".coord-", suffix=".tmp", dir=path.parent)
    temp = Path(raw_temp)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        _fsync_directory(path.parent)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise


def _create_json_exclusive(path: Path, value: dict) -> None:
    """Publish fully-written JSON only if no winner already exists."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw_temp = tempfile.mkstemp(prefix=".identity-", suffix=".tmp", dir=path.parent)
    temp = Path(raw_temp)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temp, path)
            _fsync_directory(path.parent)
        except FileExistsError:
            pass
    finally:
        temp.unlink(missing_ok=True)


def stable_identity(state_root: Path, name: str) -> str:
    path = state_root / "coord" / "v1" / f"{name}.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        identity = str(value["id"])
        uuid.UUID(identity)
        return identity
    except FileNotFoundError:
        identity = str(uuid.uuid4())
        _create_json_exclusive(path, {"format": f"remrun-{name}-v1", "id": identity})
        # Always reread the published winner. Two first-use creators must never
        # return different identities even if they generated different candidates.
        value = json.loads(path.read_text(encoding="utf-8"))
        winner = str(value["id"])
        uuid.UUID(winner)
        return winner


@dataclass(frozen=True)
class AcquireIntent:
    format: str
    acquire_id: str
    owner_token: str
    cluster_id: str
    project_key: str
    controller_id: str
    controller_replica_id: str
    owner_host: str
    owner_pid: int
    created_at_ns: int

    def as_dict(self) -> dict:
        return asdict(self)


def create_acquire_intent(state_root: Path, cluster_id: str, project_key_value: str,
                          controller_replica_id: str) -> tuple[AcquireIntent, Path]:
    controller_id = stable_identity(state_root, "controller")
    intent = AcquireIntent(
        format="remrun-acquire-intent-v1",
        acquire_id=str(uuid.uuid4()),
        owner_token=_b64url(os.urandom(32)),
        cluster_id=cluster_id,
        project_key=project_key_value,
        controller_id=controller_id,
        controller_replica_id=controller_replica_id,
        owner_host=socket.gethostname(),
        owner_pid=os.getpid(),
        created_at_ns=time.time_ns(),
    )
    path = state_root / "coord" / "v1" / "acquire-intents" / f"{intent.acquire_id}.json"
    _atomic_json(path, intent.as_dict())
    return intent, path


def load_acquire_intent(path: Path) -> AcquireIntent:
    value = json.loads(path.read_text(encoding="utf-8"))
    intent = AcquireIntent(**value)
    if intent.format != "remrun-acquire-intent-v1":
        raise ValueError("unsupported acquire intent format")
    return intent
