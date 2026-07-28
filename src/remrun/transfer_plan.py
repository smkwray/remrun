from __future__ import annotations

from dataclasses import dataclass, field

from .manifest import FileEntry, Manifest

# Action vocabulary (what the reconcile engine should do for a path).
NONE = "none"
PUSH = "push"
PULL = "pull"
DELETE_REMOTE = "delete-remote"
DELETE_LOCAL = "delete-local"
ABORT_CONFLICT = "abort-conflict"

DESTRUCTIVE_ACTIONS = frozenset({DELETE_REMOTE, DELETE_LOCAL})


@dataclass(frozen=True)
class ClassifiedPath:
    path: str
    state: str
    action: str
    reason: str


@dataclass(frozen=True)
class ReconciliationPlan:
    paths: list[ClassifiedPath] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for item in self.paths:
            out[item.action] = out.get(item.action, 0) + 1
        return out

    @property
    def has_conflicts(self) -> bool:
        return any(item.action == ABORT_CONFLICT for item in self.paths)

    def conflicts(self) -> list[ClassifiedPath]:
        return [p for p in self.paths if p.action == ABORT_CONFLICT]

    def for_action(self, action: str) -> list[ClassifiedPath]:
        return [p for p in self.paths if p.action == action]


def entries_same(a: FileEntry | None, b: FileEntry | None) -> bool:
    if a is None or b is None:
        return a is b
    if a.kind != b.kind or a.size != b.size:
        return False
    if a.sha256 is not None and b.sha256 is not None:
        return a.sha256 == b.sha256
    return a.mtime_ns == b.mtime_ns


def _entries_content_same(a: FileEntry | None, b: FileEntry | None) -> bool:
    """Whether cross-side equality is proved by hashes, never inferred from metadata."""
    if a is None or b is None or a.kind != b.kind or a.size != b.size:
        return False
    return (
        a.sha256 is not None
        and b.sha256 is not None
        and a.sha256 == b.sha256
    )


def _changed_since(prev: FileEntry | None, cur: FileEntry | None) -> bool:
    """Did a side change relative to its previous manifest?

    A None->entry (created) or entry->None (deleted) counts as changed.
    """
    if prev is None and cur is None:
        return False
    if (prev is None) != (cur is None):
        return True
    return not entries_same(prev, cur)


def _heuristic_diff(path: str, le: FileEntry, re: FileEntry) -> ClassifiedPath:
    """Classify a both-present difference with NO previous-manifest evidence.

    Direction must NOT be inferred from a cross-device mtime comparison: clocks differ
    across devices (skew, time zones, and differing filesystem timestamp precision), so
    a larger mtime does not reliably mean "newer". With no sync history we therefore
    decide by *content* only — identical hashes are the same file; anything else (a real
    content difference, or a pair we can't verify without hashing) is a **conflict** for
    the user to resolve. The first resolution records a baseline, after which direction
    comes from per-side change detection (which only ever compares a device to its own
    past — see ``compare_manifests``) rather than from any clock.
    """
    if le.sha256 is not None and re.sha256 is not None:
        if le.sha256 == re.sha256:
            return ClassifiedPath(path, "same", NONE, "identical content (no baseline yet)")
        return ClassifiedPath(path, "both-present-differ", ABORT_CONFLICT,
                              "differs with no sync history — resolve manually")
    return ClassifiedPath(path, "both-present-unverified", ABORT_CONFLICT,
                          "present on both sides, unhashed, no history — resolve manually")


def compare_manifests(
    local: Manifest,
    remote: Manifest,
    prev_local: Manifest | None = None,
    prev_remote: Manifest | None = None,
) -> ReconciliationPlan:
    """Classify local/remote differences into a reconciliation plan.

    When both previous manifests are supplied, deletions are classified safely:
    a side that lost a file since its previous manifest is treated as a *known*
    deletion and may be mirrored (with backup) only if the other side did not
    independently change that path. Without previous manifests the engine never
    proposes a destructive action.
    """
    have_prev = prev_local is not None and prev_remote is not None
    all_paths = sorted(set(local) | set(remote))
    out: list[ClassifiedPath] = []

    for path in all_paths:
        le = local.get(path)
        re = remote.get(path)
        pl = prev_local.get(path) if prev_local else None
        pr = prev_remote.get(path) if prev_remote else None

        if le and re:
            if have_prev:
                # With a baseline, decide by INTRA-device change detection (each side vs
                # its OWN prior state) — NOT by a cross-device entries_same(le, re), which
                # for large unhashed files compares two devices' mtimes and can wrongly
                # declare a genuinely-changed file "same" when their clocks/precision
                # coincide. (The no-baseline branch below still uses the cross-side check.)
                local_changed = _changed_since(pl, le)
                remote_changed = _changed_since(pr, re)
                if local_changed and remote_changed:
                    # Both edited — unless they converged to identical content, in which
                    # case there's nothing to do.
                    if _entries_content_same(le, re):
                        out.append(ClassifiedPath(path, "same", NONE, "both changed to identical content"))
                    else:
                        out.append(ClassifiedPath(path, "both-changed", ABORT_CONFLICT,
                                                  "both sides changed since last run"))
                elif local_changed:
                    out.append(ClassifiedPath(path, "local-newer", PUSH, "local changed since last run"))
                elif remote_changed:
                    out.append(ClassifiedPath(path, "remote-newer", PULL, "remote changed since last run"))
                else:
                    # Neither side changed since the last converged baseline → nothing to
                    # do, even if le/re look different now (a cross-device metadata artifact,
                    # e.g. mtime precision across an NTFS/APFS boundary, not a real edit).
                    out.append(ClassifiedPath(path, "same", NONE, "neither side changed since last run"))
            else:
                out.append(_heuristic_diff(path, le, re))

        elif le and not re:
            # Present locally, absent remotely.
            if have_prev and pr is not None:
                # Remote had it last run and lost it -> known remote deletion.
                if _changed_since(pl, le):
                    out.append(ClassifiedPath(path, "unknown-deletion", ABORT_CONFLICT,
                                              "remote deleted a file modified locally"))
                else:
                    out.append(ClassifiedPath(path, "remote-deleted-known", DELETE_LOCAL,
                                              "remote deleted file; local unchanged (backup first)"))
            else:
                out.append(ClassifiedPath(path, "local-only", PUSH, "file exists only locally"))

        elif re and not le:
            # Present remotely, absent locally.
            if have_prev and pl is not None:
                # Local had it last run and lost it -> known local deletion.
                if _changed_since(pr, re):
                    out.append(ClassifiedPath(path, "unknown-deletion", ABORT_CONFLICT,
                                              "local deleted a file modified remotely"))
                else:
                    out.append(ClassifiedPath(path, "local-deleted-known", DELETE_REMOTE,
                                              "local deleted file; remote unchanged"))
            else:
                out.append(ClassifiedPath(path, "remote-only", PULL,
                                          "file exists only remotely and is included"))

    return ReconciliationPlan(out)


def diff_remote_changes(
    pre_remote: Manifest,
    post_remote: Manifest,
) -> tuple[list[str], list[str]]:
    """Compare pre/post remote manifests to find files the command touched.

    Returns (changed_or_created, deleted) as sorted path lists. "Changed" covers
    both new files and modified files; "deleted" covers files present before and
    gone after.
    """
    changed: list[str] = []
    deleted: list[str] = []
    for path in sorted(set(pre_remote) | set(post_remote)):
        before = pre_remote.get(path)
        after = post_remote.get(path)
        if after is not None and (before is None or not entries_same(before, after)):
            changed.append(path)
        elif before is not None and after is None:
            deleted.append(path)
    return changed, deleted
