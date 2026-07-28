from __future__ import annotations

import shutil
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Callable

from .config import case_insensitive, casefold_collisions, current_os_key, device_os_key
from .manifest import FileEntry, Manifest, build_manifest, sha256_file, should_exclude
from .transfer_plan import (
    ABORT_CONFLICT,
    DELETE_LOCAL,
    DELETE_REMOTE,
    NONE,
    PULL,
    PUSH,
    ClassifiedPath,
    _changed_since,
    compare_manifests,
    diff_remote_changes,
    entries_same,
)
from .transport import BaseTransport, TransportError


# Positively enumerated candidate-local preflight failures. Any new/unknown state is
# non-retryable by default so a global controller-side problem cannot silently fail over.
_CANDIDATE_LOCAL_CONFLICT_STATES = frozenset({
    "both-present-differ",
    "both-present-unverified",
    "both-changed",
    "unknown-deletion",
    "casefold-collision",
    # This candidate's baseline would destroy a path disputed elsewhere. The refusal is
    # candidate-local: a later candidate may preserve/push the local bytes safely.
    "fallback-local-mutation",
    # A missing remote root is broken state on this candidate only; another candidate may
    # have a healthy checkout. The symmetric local-vanished state is intentionally absent.
    "remote-vanished",
})


@dataclass
class ReconcileResult:
    pulled: list[str] = field(default_factory=list)
    pushed: list[str] = field(default_factory=list)
    deleted_local: list[str] = field(default_factory=list)
    deleted_remote: list[str] = field(default_factory=list)
    conflicts: list[ClassifiedPath] = field(default_factory=list)
    # Planned transfers dropped at apply time because the two sides had already converged
    # (an external writer delivered the same bytes between manifesting and applying).
    # Reported so a skipped transfer stays visible instead of looking like it was never
    # planned.
    skipped_identical: list[str] = field(default_factory=list)
    # Paths initially disputed because content identity was absent, then proved
    # byte-identical by hashing. Not conflicts — recorded so the run can show it
    # examined them.
    converged_conflicts: list[str] = field(default_factory=list)
    # Converged manifests after reconcile == pre-run baselines.
    local_manifest: Manifest = field(default_factory=dict)
    remote_manifest: Manifest = field(default_factory=dict)

    @property
    def has_conflicts(self) -> bool:
        return bool(self.conflicts)

    @property
    def conflicts_are_candidate_local(self) -> bool:
        """Whether every conflict is safe to abandon and retry on another candidate."""
        return bool(self.conflicts) and all(
            conflict.state in _CANDIDATE_LOCAL_CONFLICT_STATES
            for conflict in self.conflicts
        )


@dataclass
class PullbackResult:
    pulled: list[str] = field(default_factory=list)
    deleted_local: list[str] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)
    skipped_identical: list[str] = field(default_factory=list)   # local already == remote (Syncthing delivered)
    post_remote_manifest: Manifest = field(default_factory=dict)
    local_manifest_after: Manifest = field(default_factory=dict)


def _fresh_local_entry(path: Path, *, hash_if_size: int | None) -> FileEntry | None:
    """Re-stat ONE local file RIGHT NOW (not from a pre-built manifest) so the pull decision uses
    live content — Syncthing may have delivered the same bytes since the loop's manifest was built.
    Hashes the file only when its size matches the remote candidate (a size mismatch already proves
    they differ, so hashing would be wasted)."""
    try:
        if path.is_symlink() or not path.is_file():
            return None
        st = path.stat()
    except OSError:
        return None
    digest = sha256_file(path) if (hash_if_size is not None and st.st_size == hash_if_size) else None
    return FileEntry(path=path.name, kind="file", size=st.st_size, mtime_ns=st.st_mtime_ns, sha256=digest)


def _already_converged(local_path: Path, remote_entry: FileEntry | None) -> bool:
    """Has the local file become byte-identical to the remote candidate since planning?

    Fails CLOSED: a missing remote entry, a remote entry with no hash, or an unreadable
    local file all return False, so the planned transfer still happens. A skip is only ever
    taken on positive proof of equal content, never on absence of evidence. Costs one stat,
    plus one local hash when the sizes match (a size mismatch already proves they differ).

    Scope: this re-reads the LOCAL side only. ``remote_entry`` is still the value from the
    manifest fetched during planning, so a change made on the REMOTE inside the same window
    is not visible here — catching that would need a per-file round-trip on every transfer.
    That is the right trade: the observed churn comes from an external writer delivering to
    the controller mid-reconcile, which this does catch, in both the pull and push
    directions (a push is skipped when local reverts to the bytes the remote already holds).
    """
    if remote_entry is None or remote_entry.kind != "file" or remote_entry.sha256 is None:
        return False
    local_now = _fresh_local_entry(local_path, hash_if_size=remote_entry.size)
    if local_now is None or local_now.sha256 is None:
        return False
    return local_now.sha256 == remote_entry.sha256


def _drop_identical_conflicts(
    conflicts: list[ClassifiedPath],
    *,
    transport: BaseTransport,
    local_root: Path,
    remote_root: str,
    local_manifest: Manifest,
    remote_manifest: Manifest,
) -> list[ClassifiedPath]:
    """Return the conflicts that survive a real content comparison.

    Re-examines both `both-changed` and `both-present-unverified`: each means both
    sides hold bytes that the manifests could not prove equal. Deletion conflicts
    and the vanished-root guard are about intent, not content, so they pass through.

    Fails CLOSED in every uncertain case — a missing manifest entry, a hash the
    transport cannot supply, a mismatched size, an unreadable local file, or any
    transport error all keep the conflict. A conflict is dropped only on two hashes
    that exist and match.
    """
    survivors: list[ClassifiedPath] = []
    for item in conflicts:
        if item.state not in {"both-changed", "both-present-unverified"}:
            survivors.append(item)
            continue
        le = local_manifest.get(item.path)
        re_ = remote_manifest.get(item.path)
        if le is None or re_ is None or le.kind != "file" or re_.kind != "file":
            survivors.append(item)
            continue
        if le.size != re_.size:      # different sizes already prove divergence
            survivors.append(item)
            continue
        try:
            local_hash = le.sha256 or sha256_file(local_root / item.path)
            remote_hash = re_.sha256 or transport.hash_file(
                transport.remote_join(remote_root, item.path)
            )
        except (TransportError, NotImplementedError, OSError, ValueError):
            survivors.append(item)
            continue
        if not local_hash or not remote_hash or len(str(remote_hash).strip()) != 64:
            survivors.append(item)
            continue
        if str(local_hash).strip().lower() != str(remote_hash).strip().lower():
            survivors.append(item)
    return survivors


def _remote_root_present(transport: BaseTransport, remote_root: str) -> bool:
    """Best-effort 'does the remote root exist?' for the vanished-root guard.

    Any uncertainty (backend without the probe, transient error) is treated as
    'not present' so the guard errs toward aborting rather than mass-deleting.
    """
    try:
        return bool(transport.remote_path_exists(remote_root))
    except (TransportError, NotImplementedError, OSError):
        return False


def _backup_local(local_root: Path, rel: str, backup_root: Path, max_bytes: int = 0) -> bool:
    """Snapshot a local file aside before it's overwritten/deleted. Returns whether a
    copy was made. Skips a file larger than ``max_bytes`` (when >0) to bound state
    growth — large files are regenerable/re-syncable, and snapshotting them on every
    run is the main growth risk; the size budget in prune_state is the hard backstop."""
    src = local_root / rel
    if not src.exists():
        return False
    if max_bytes and src.stat().st_size > max_bytes:
        return False
    dest = backup_root / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return True


def _matches_any_scope_path(rel: str, patterns: list[str] | tuple[str, ...]) -> bool:
    return any(should_exclude(rel, [pattern]) for pattern in patterns)


def preflight_reconcile(
    *,
    transport: BaseTransport,
    local_root: Path,
    remote_root: str,
    excludes: list[str],
    hash_below_bytes: int,
    prev_local: Manifest | None,
    prev_remote: Manifest | None,
    backup_root: Path,
    backup_below_bytes: int = 0,
    progress: Callable[[int, int, dict[str, int]], None] | None = None,
    is_fallback: bool = False,
) -> ReconcileResult:
    """Reconcile the active run surface before running the command (Mode 1/safe).

    Conflicts abort *before* any mutation. Otherwise pulls remote-newer/-only,
    pushes local-newer/-only, and applies only known-safe deletes (backing up
    local files before deleting them). A fallback candidate may push local bytes
    outward but may not pull or delete anything locally while candidate selection
    is still in progress. Returns converged manifests to serve as pre-run baselines.
    """
    result = ReconcileResult()

    local_manifest = build_manifest(local_root, excludes, hash_below_bytes=hash_below_bytes or None)
    remote_manifest = transport.manifest(remote_root, excludes, hash_below_bytes)

    # Vanished-root guard. If a side comes back *entirely empty* while its previous
    # baseline had files, that is either a legitimate "user deleted everything" or a
    # missing/unreachable root (remote project_root wrong/unmounted; local wrong cwd).
    # The two are indistinguishable by emptiness alone — only root existence tells
    # them apart — so we probe it *only* in this rare case (no extra round-trip on the
    # normal path). If the root is gone, refuse: otherwise compare_manifests would
    # classify every still-present file on the other side as a known deletion and
    # mirror it, gutting a real tree. (An existing-but-empty root is treated as a
    # genuine wipe and allowed, so a one-file project deleting its last file still
    # works.) Let the caller abort (exit 2).
    have_prev = prev_local is not None and prev_remote is not None
    if have_prev:
        vanished = None
        if prev_remote and not remote_manifest and not _remote_root_present(transport, remote_root):
            vanished = ("remote", "remote root missing/unreachable but the prior baseline "
                        "had files — refusing to mirror wholesale deletions")
        elif prev_local and not local_manifest and not local_root.exists():
            vanished = ("local", "local project root missing but the prior baseline had "
                        "files (wrong cwd?) — refusing to mirror wholesale deletions")
        if vanished:
            side, reason = vanished
            result.conflicts = [ClassifiedPath(f"<{side} root>", f"{side}-vanished",
                                               ABORT_CONFLICT, reason)]
            result.local_manifest = local_manifest
            result.remote_manifest = remote_manifest
            return result

    plan = compare_manifests(local_manifest, remote_manifest, prev_local, prev_remote)

    if plan.has_conflicts:
        # Before refusing, PROVE the disputed paths actually differ. compare_manifests can
        # only use what the manifests carry, and a file above `hash_small_files_below_mb`
        # carries no hash. Both a no-history equality decision and a both-changed
        # convergence decision therefore remain unverified until this narrow seam hashes
        # the disputed paths. Hashing only conflicts keeps the normal path metadata-fast.
        resolved = _drop_identical_conflicts(
            plan.conflicts(), transport=transport, local_root=local_root,
            remote_root=remote_root, local_manifest=local_manifest,
            remote_manifest=remote_manifest,
        )
        if resolved:
            # Genuinely divergent bytes: do not mutate either side; let the caller abort
            # (exit 2). Any conflict we could not disprove stays a conflict — fail closed.
            result.conflicts = resolved
            result.local_manifest = local_manifest
            result.remote_manifest = remote_manifest
            return result
        # Every conflict was disproved: both sides already hold identical content, so there
        # is nothing to transfer for them. Fall through and reconcile the rest of the tree.
        result.converged_conflicts.extend(item.path for item in plan.conflicts())
        plan = replace(plan, paths=[item for item in plan.paths
                                    if item.action != ABORT_CONFLICT])

    # PREFLIGHT candidate-shopping must not mutate the controller. Scope, precisely:
    # this covers preflight reconciliation on a FALLBACK attempt only. It is deliberately
    # NOT a claim that a fallback run never writes locally — postrun pullback still returns
    # the command's own outputs to their local paths (invariant 2, the whole point of the
    # tool), and the first attempt reconciles normally.
    #
    # The rule is blanket rather than per-path on purpose. Tracking WHICH paths an earlier
    # candidate disputed put safety on the wrong side of string comparison: a case-fold
    # collision is recorded as the synthetic path "Foo | foo", which never equals "Foo", so
    # the guard silently missed it — plus normalization and symlink spellings behind that.
    # Refusing every PULL/DELETE_LOCAL needs no path matching, so that whole bug class is
    # gone rather than patched. Measured cost: of 714 real runs, 89.9% mutated nothing
    # locally in preflight, so this almost never changes what a fallback would have done.
    #
    # PUSH stays allowed: it sends local bytes outward and cannot destroy them. A candidate
    # is refused before ensure_remote_dir or any transfer occurs.
    local_mutations = [
        item for item in plan.paths
        if is_fallback and item.action in (PULL, DELETE_LOCAL)
    ]
    if local_mutations:
        result.conflicts = [
            ClassifiedPath(
                item.path,
                "fallback-local-mutation",
                ABORT_CONFLICT,
                f"fallback plan would {item.action.lower()} the local tree",
            )
            for item in local_mutations
        ]
        result.local_manifest = local_manifest
        result.remote_manifest = remote_manifest
        return result

    # Case-fold collision guard (APFS/NTFS): two distinct paths that fold to the same name collapse
    # into one file on a case-insensitive TARGET (silent data loss). Abort before any mutation.
    collisions: dict[str, list[str]] = {}
    pulls = [p.path for p in plan.paths if p.action == PULL]
    pushes = [p.path for p in plan.paths if p.action == PUSH]
    if case_insensitive(current_os_key()):                     # local is the pull target
        collisions.update(casefold_collisions(list(local_manifest) + pulls))
    if case_insensitive(device_os_key(transport.device)):      # remote is the push target
        collisions.update(casefold_collisions(list(remote_manifest) + pushes))
    if collisions:
        result.conflicts = [
            ClassifiedPath(
                " | ".join(paths),
                "casefold-collision",
                ABORT_CONFLICT,
                "would collapse on a case-insensitive target: " + ", ".join(paths),
            )
            for paths in collisions.values()
        ]
        result.local_manifest = local_manifest
        result.remote_manifest = remote_manifest
        return result

    transport.ensure_remote_dir(remote_root)

    actions = [item for item in plan.paths if item.action != NONE]
    action_totals = {
        "pulls": sum(item.action == PULL for item in actions),
        "pushes": sum(item.action == PUSH for item in actions),
        "deletes_local": sum(item.action == DELETE_LOCAL for item in actions),
        "deletes_remote": sum(item.action == DELETE_REMOTE for item in actions),
    }
    total = len(actions)
    if progress:
        progress(0, total, action_totals)

    for completed, item in enumerate(actions, start=1):
        remote_path = transport.remote_join(remote_root, item.path)
        local_path = local_root / item.path
        if item.action == PULL:
            # Re-check the LOCAL file RIGHT BEFORE writing it. The plan above was built from
            # a manifest snapshot; an external writer (Syncthing) may have delivered the very
            # bytes we are about to pull in the meantime. Rewriting an already-converged file
            # is not just wasted work — it re-touches a path Syncthing is actively watching,
            # which is how remrun ends up racing the delivery it was trying to stay clear of.
            # This is the same guard postrun_pullback already applies; see below.
            if _already_converged(local_path, remote_manifest.get(item.path)):
                result.skipped_identical.append(item.path)
            else:
                # Snapshot the prior local version before overwriting it, so a wrong pull
                # is recoverable (no-op when the file is new or over the backup size cap).
                _backup_local(local_root, item.path, backup_root, backup_below_bytes)
                transport.pull_file(remote_path, local_path)
                result.pulled.append(item.path)
        elif item.action == PUSH:
            # The same guard in the other direction: if the remote already holds these exact
            # bytes there is nothing to send. Compares the freshly-stat'd local file against
            # the remote entry from the manifest already fetched above, so no extra
            # round-trip.
            if _already_converged(local_path, remote_manifest.get(item.path)):
                result.skipped_identical.append(item.path)
            else:
                transport.push_file(local_path, remote_path)
                result.pushed.append(item.path)
        elif item.action == DELETE_REMOTE:
            transport.delete_remote(remote_path)
            result.deleted_remote.append(item.path)
        elif item.action == DELETE_LOCAL:
            _backup_local(local_root, item.path, backup_root, backup_below_bytes)
            local_path.unlink(missing_ok=True)
            result.deleted_local.append(item.path)
        if progress and (completed == total or completed % 25 == 0):
            progress(completed, total, action_totals)

    # Rebuild converged manifests to use as pre-run baselines.
    result.local_manifest = build_manifest(
        local_root, excludes, hash_below_bytes=hash_below_bytes or None
    )
    result.remote_manifest = transport.manifest(remote_root, excludes, hash_below_bytes)
    return result


def postrun_pullback(
    *,
    transport: BaseTransport,
    local_root: Path,
    remote_root: str,
    excludes: list[str],
    hash_below_bytes: int,
    pre_remote_manifest: Manifest,
    pre_local_manifest: Manifest,
    backup_root: Path,
    conflict_remote_root: Path,
    backup_below_bytes: int = 0,
    write_scope_paths: list[str] | tuple[str, ...] | None = None,
) -> PullbackResult:
    """Pull command-caused remote changes back to local project paths.

    A path is only overwritten locally if the local copy did not independently
    change during the run; otherwise the remote version is saved outside the
    project tree and the path is flagged as a conflict. Command-caused remote
    deletions are mirrored locally (with backup) only when the local copy is
    unchanged.
    """
    result = PullbackResult()

    post_remote = transport.manifest(remote_root, excludes, hash_below_bytes)
    changed, deleted = diff_remote_changes(pre_remote_manifest, post_remote)
    cur_local = build_manifest(local_root, excludes, hash_below_bytes=hash_below_bytes or None)

    if write_scope_paths:
        escaped = [
            rel for rel in [*changed, *deleted]
            if not _matches_any_scope_path(rel, write_scope_paths)
        ]
        if escaped:
            for rel in escaped:
                if rel in post_remote:
                    dest = conflict_remote_root / rel
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    transport.pull_file(transport.remote_join(remote_root, rel), dest)
            result.conflicts.extend(escaped)
            result.post_remote_manifest = post_remote
            result.local_manifest_after = cur_local
            return result

    # Case-fold collision guard for the local pull target (APFS/NTFS): an incoming output whose name
    # folds onto an existing/other local path would collapse into one file — flag it, don't collapse.
    collide: set[str] = set()
    if case_insensitive(current_os_key()):
        groups = casefold_collisions(list(cur_local) + list(changed))
        collide = {p for ps in groups.values() for p in ps if p in set(changed)}

    for rel in changed:
        if rel in collide:
            result.conflicts.append(rel)   # would collapse on a case-insensitive local FS — skip
            continue
        remote_entry = post_remote.get(rel)
        remote_path = transport.remote_join(remote_root, rel)
        local_path = local_root / rel

        # Strong remote hash for the candidate: the manifest caps hashing at hash_below_bytes, so
        # a >64 MB output has sha256=None. Hash it now so the idempotent skip below is content-based
        # for large outputs too. If the hash cannot be obtained, preserve the remote candidate as a
        # conflict instead of silently degrading equality to size/mtime.
        if remote_entry is not None and remote_entry.sha256 is None:
            try:
                digest = transport.hash_file(remote_path)
                if (
                    len(digest) != 64
                    or any(char not in "0123456789abcdef" for char in digest.lower())
                ):
                    raise TransportError(f"hash {remote_path} returned an invalid SHA-256")
                remote_entry = replace(remote_entry, sha256=digest.lower())
            except (TransportError, NotImplementedError, ValueError):
                dest = conflict_remote_root / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                transport.pull_file(remote_path, dest)
                result.conflicts.append(rel)
                continue

        # Re-check the local file RIGHT BEFORE writing (not the stale cur_local): Syncthing may have
        # delivered the identical bytes during/after the run. If local already == the remote output,
        # SKIP the pull — that avoids an unnecessary overwrite that races Syncthing's own delivery
        # (the temp-orphan / sync-conflict source). Idempotent external-writer behavior.
        local_now = _fresh_local_entry(
            local_path, hash_if_size=(remote_entry.size if remote_entry is not None else None))
        if remote_entry is not None and entries_same(local_now, remote_entry):
            result.skipped_identical.append(rel)
            continue

        local_changed_during_run = _changed_since(pre_local_manifest.get(rel), local_now)
        if local_changed_during_run:
            # Local edited during the run and differs from the remote output -> preserve both.
            dest = conflict_remote_root / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            transport.pull_file(remote_path, dest)
            result.conflicts.append(rel)
            continue

        _backup_local(local_root, rel, backup_root, backup_below_bytes)  # rollback snapshot
        transport.pull_file(remote_path, local_path)
        # Verify the pull landed the expected bytes (cheap integrity guard when we know the hash).
        if remote_entry is not None and remote_entry.sha256 is not None:
            verify = _fresh_local_entry(local_path, hash_if_size=remote_entry.size)
            if verify is None or not entries_same(verify, remote_entry):
                raise TransportError(f"pull verification failed for {rel}")
        result.pulled.append(rel)

    for rel in deleted:
        local_path = local_root / rel
        if not local_path.exists():
            continue
        local_changed_during_run = _changed_since(
            pre_local_manifest.get(rel), cur_local.get(rel)
        )
        if local_changed_during_run:
            # Remote deleted it but local changed it during the run -> keep local.
            result.conflicts.append(rel)
            continue
        _backup_local(local_root, rel, backup_root, backup_below_bytes)
        local_path.unlink(missing_ok=True)
        result.deleted_local.append(rel)

    result.post_remote_manifest = post_remote
    result.local_manifest_after = build_manifest(
        local_root, excludes, hash_below_bytes=hash_below_bytes or None
    )
    return result


# Re-exported so callers can catch transfer failures distinctly.
__all__ = [
    "ReconcileResult",
    "PullbackResult",
    "preflight_reconcile",
    "postrun_pullback",
    "TransportError",
]
