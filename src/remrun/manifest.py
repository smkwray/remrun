from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class FileEntry:
    path: str
    kind: str
    size: int
    mtime_ns: int
    sha256: str | None = None
    # POSIX permission bits (design 11.1 "identity v2"). Populated by build_manifest but
    # DELIBERATELY excluded from comparable_tuple() below, so adding it does not change any
    # current comparison/transfer behavior — it is groundwork for the future transaction
    # engine, inert until that wires it in.
    mode: int | None = None

    def comparable_tuple(self) -> tuple[str, int, int, str | None]:
        return (self.kind, self.size, self.mtime_ns, self.sha256)


Manifest = dict[str, FileEntry]


class ManifestError(RuntimeError):
    """A local tree could not be scanned into a trustworthy snapshot.

    Raised instead of returning a partial manifest, so a transient scan/stat error
    can never be mistaken for files having been deleted.
    """


def _is_directory_link(path: Path) -> bool:
    """Return whether a directory entry can redirect traversal outside its parent tree.

    ``os.walk(..., followlinks=False)`` prunes POSIX symlinks, but Windows junctions are
    a separate reparse-point type and can still be descended. Python 3.12 added
    ``os.path.isjunction``; the lstat fallback preserves the same check on Python 3.11.
    """
    if path.is_symlink():
        return True
    isjunction = getattr(os.path, "isjunction", None)
    if isjunction is not None:
        return bool(isjunction(path))
    if os.name != "nt":
        return False
    item = os.lstat(path)
    mount_point_tag = getattr(stat, "IO_REPARSE_TAG_MOUNT_POINT", 0xA0000003)
    return getattr(item, "st_reparse_tag", None) == mount_point_tag


def should_exclude(rel_posix: str, patterns: Iterable[str]) -> bool:
    rel = rel_posix.strip("/")
    for pattern in patterns:
        pat = pattern.strip()
        if not pat:
            continue
        if pat.endswith("/**"):
            prefix = pat[:-3].strip("/")
            if rel == prefix or rel.startswith(prefix + "/"):
                return True
        if fnmatch.fnmatch(rel, pat) or fnmatch.fnmatch("/" + rel, pat):
            return True
    return False


def build_manifest(
    root: Path,
    exclude_patterns: Iterable[str],
    *,
    hash_below_bytes: int | None = None,
    always_hash: bool = False,
) -> Manifest:
    """Build a manifest for a local filesystem tree.

    This is intentionally local. Remote backends should produce the same schema.

    ``always_hash=True`` hashes every included file regardless of ``hash_below_bytes``
    (design 11.1: commit-gate manifests must hash every file; the size cap may stay only
    as a planning/cache optimization). Default False preserves current behavior.
    """
    root = root.resolve()
    manifest: Manifest = {}
    if not root.exists():
        return manifest

    def _abort(err: OSError) -> None:
        # Fail CLOSED: a directory we cannot enumerate must NOT silently vanish from the
        # manifest, or the planner would read the missing subtree as a deletion and could
        # delete the healthy copy on the other side.
        raise ManifestError(f"cannot scan a directory under {root}: {err}") from err

    for dirpath, dirnames, filenames in os.walk(root, onerror=_abort):
        current = Path(dirpath)
        rel_dir = current.relative_to(root).as_posix()
        if rel_dir == ".":
            rel_dir = ""

        # Prune excluded directories and any link-like directory before descent. A
        # junction can resolve outside ``root`` even though it is not a symlink.
        kept_dirs = []
        for dirname in dirnames:
            rel = f"{rel_dir}/{dirname}" if rel_dir else dirname
            if should_exclude(rel, exclude_patterns):
                continue
            try:
                if _is_directory_link(current / dirname):
                    continue
            except OSError as exc:
                raise ManifestError(f"cannot inspect directory {rel}: {exc}") from exc
            kept_dirs.append(dirname)
        dirnames[:] = kept_dirs

        for filename in filenames:
            path = current / filename
            rel = path.relative_to(root).as_posix()
            if should_exclude(rel, exclude_patterns):
                continue
            try:
                # A symlink would manifest content OUTSIDE the tree, and a later pull
                # through it could overwrite an external target — skip it. Directory
                # symlinks and junctions were already pruned above.
                if path.is_symlink():
                    continue
                st = path.stat()
            except FileNotFoundError:
                # Genuinely gone between listing and observation (e.g. a transient temp).
                continue
            except OSError as exc:
                # Exists but unreadable (permission/mount/IO): we can't prove it's absent,
                # so refuse rather than drop it and risk a phantom deletion downstream.
                raise ManifestError(f"cannot read {rel}: {exc}") from exc
            if not stat.S_ISREG(st.st_mode):
                continue
            digest = None
            if always_hash or (hash_below_bytes is not None and st.st_size <= hash_below_bytes):
                try:
                    digest = sha256_file(path)
                except FileNotFoundError as exc:
                    # Unlike a pre-stat disappearance, this path was already admitted
                    # into the snapshot. Dropping it now could manufacture a deletion;
                    # abort with a retryable explanation instead.
                    raise ManifestError(
                        f"file changed while hashing {rel}; retry the command"
                    ) from exc
                except OSError as exc:
                    raise ManifestError(f"cannot hash {rel}: {exc}") from exc
            manifest[rel] = FileEntry(
                path=rel,
                kind="file",
                size=st.st_size,
                mtime_ns=st.st_mtime_ns,
                sha256=digest,
                mode=stat.S_IMODE(st.st_mode) & 0o777,
            )
    return manifest


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_identity(entry: FileEntry) -> dict:
    """The identity-v2 record for one file (design 11.1): full content identity.

    Not yet used by the live path — a primitive for the future transaction/generation
    engine. Requires ``sha256`` to be populated (build with ``always_hash=True`` for a
    commit-gate manifest).
    """
    return {
        "kind": entry.kind,
        "size": entry.size,
        "mtime_ns": entry.mtime_ns,
        "sha256": entry.sha256,
        "mode": entry.mode,
    }


def strong_manifest_digest(manifest: Manifest) -> str:
    """Deterministic, order-independent content digest of a whole manifest (design 11.1).

    Covers path + CONTENT identity (kind, size, sha256, mode). It deliberately EXCLUDES
    mtime_ns, so two byte-for-byte-identical trees produce the same digest even if a
    background sync (Syncthing) bumped mtimes — this is a content-equality certificate,
    the basis for a future "both sides equal -> advance generation" decision. Callers that
    need a trustworthy digest must build the manifest with ``always_hash=True`` (a None
    sha256 is included verbatim and would make the digest untrustworthy).
    """
    h = hashlib.sha256()
    for path in sorted(manifest):
        e = manifest[path]
        record = json.dumps(
            {"path": path, "kind": e.kind, "size": e.size, "sha256": e.sha256, "mode": e.mode},
            sort_keys=True,
            separators=(",", ":"),
        )
        h.update(record.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()
