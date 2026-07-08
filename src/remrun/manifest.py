from __future__ import annotations

import fnmatch
import hashlib
import os
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

    def comparable_tuple(self) -> tuple[str, int, int, str | None]:
        return (self.kind, self.size, self.mtime_ns, self.sha256)


Manifest = dict[str, FileEntry]


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
) -> Manifest:
    """Build a manifest for a local filesystem tree.

    This is intentionally local. Remote backends should produce the same schema.
    """
    root = root.resolve()
    manifest: Manifest = {}
    if not root.exists():
        return manifest

    for dirpath, dirnames, filenames in os.walk(root):
        current = Path(dirpath)
        rel_dir = current.relative_to(root).as_posix()
        if rel_dir == ".":
            rel_dir = ""

        # Prune excluded directories early.
        kept_dirs = []
        for dirname in dirnames:
            rel = f"{rel_dir}/{dirname}" if rel_dir else dirname
            if not should_exclude(rel, exclude_patterns):
                kept_dirs.append(dirname)
        dirnames[:] = kept_dirs

        for filename in filenames:
            path = current / filename
            rel = path.relative_to(root).as_posix()
            if should_exclude(rel, exclude_patterns):
                continue
            # Skip symlinks: following them would manifest content OUTSIDE the tree, and a
            # later pull through an existing symlink could overwrite an external target.
            # Treat the tree as real files only. (os.walk already doesn't recurse symlinked
            # dirs by default.)
            if path.is_symlink():
                continue
            try:
                st = path.stat()
            except OSError:
                continue
            if not path.is_file():
                continue
            digest = None
            if hash_below_bytes is not None and st.st_size <= hash_below_bytes:
                digest = sha256_file(path)
            manifest[rel] = FileEntry(
                path=rel,
                kind="file",
                size=st.st_size,
                mtime_ns=st.st_mtime_ns,
                sha256=digest,
            )
    return manifest


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
