#!/usr/bin/env python3
"""remrun remote runner.

Self-contained: it must run under a remote ``python3`` with NO remrun install.
The local CLI pipes this file's source into ``python3 -`` and passes a single
base64-encoded JSON request as argv[1]. It emits a manifest in exactly the same
schema as the local ``remrun.state.manifest_to_json`` output, so the two sides
compare directly.

Request: {"op": "manifest"|"probe", "root": str, "exclude": [str], "hash_below_bytes": int}
"""
from __future__ import annotations

import base64
import fnmatch
import hashlib
import json
import os
import platform
import sys


def should_exclude(rel_posix: str, patterns) -> bool:
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


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_manifest(root: str, excludes, hash_below_bytes: int) -> dict:
    files: dict = {}
    if not os.path.isdir(root):
        return files
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root).replace(os.sep, "/")
        if rel_dir == ".":
            rel_dir = ""
        kept = []
        for d in dirnames:
            rel = f"{rel_dir}/{d}" if rel_dir else d
            if not should_exclude(rel, excludes):
                kept.append(d)
        dirnames[:] = kept
        for name in filenames:
            full = os.path.join(dirpath, name)
            rel = f"{rel_dir}/{name}" if rel_dir else name
            if should_exclude(rel, excludes):
                continue
            if os.path.islink(full):   # skip symlinks (don't track content outside the tree)
                continue
            try:
                st = os.stat(full)
            except OSError:
                continue
            if not os.path.isfile(full):
                continue
            digest = None
            if hash_below_bytes and st.st_size <= hash_below_bytes:
                try:
                    digest = sha256_file(full)
                except OSError:
                    digest = None
            files[rel] = {
                "kind": "file",
                "size": st.st_size,
                "mtime_ns": st.st_mtime_ns,
                "sha256": digest,
            }
    return files


def main(argv) -> int:
    if len(argv) < 2:
        sys.stderr.write("remrun-runner: missing request\n")
        return 2
    req = json.loads(base64.b64decode(argv[1]).decode("utf-8"))
    op = req.get("op")
    if op == "manifest":
        files = build_manifest(
            req["root"], req.get("exclude", []), int(req.get("hash_below_bytes", 0))
        )
        sys.stdout.write(json.dumps({"version": 1, "files": files}))
        return 0
    if op == "hash_file":
        # Stream-hash ONE file regardless of size (manifest caps hashing at hash_below_bytes;
        # `run` pullback uses this to strongly compare a >64 MB output candidate).
        sys.stdout.write(json.dumps({"sha256": sha256_file(req["path"])}))
        return 0
    if op == "probe":
        sys.stdout.write(json.dumps({
            "os": platform.system().lower(),
            "python": platform.python_version(),
            "machine": platform.machine(),
        }))
        return 0
    sys.stderr.write(f"remrun-runner: unknown op {op!r}\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
