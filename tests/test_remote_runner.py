"""The remote runner must produce a manifest identical to the local builder.

This runs the *actual* runner.py as a subprocess (it is self-contained), which
validates the remote manifest path without needing a live SSH host.
"""
from __future__ import annotations

import base64
import json
import subprocess
import sys
from pathlib import Path

from remrun.manifest import build_manifest
from remrun.state import manifest_to_json

RUNNER = Path(__file__).resolve().parents[1] / "src" / "remrun" / "remote" / "runner.py"


def run_runner(root: Path, exclude, hash_below):
    req = base64.b64encode(json.dumps(
        {"op": "manifest", "root": str(root), "exclude": list(exclude),
         "hash_below_bytes": hash_below}
    ).encode()).decode()
    proc = subprocess.run(
        [sys.executable, str(RUNNER), req], capture_output=True, text=True, check=True
    )
    return json.loads(proc.stdout)


def test_runner_manifest_matches_local_builder(tmp_path: Path):
    (tmp_path / "a.txt").write_text("alpha")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.bin").write_bytes(b"\x00\x01\x02beta")
    (tmp_path / "node_modules").mkdir()
    (tmp_path / "node_modules" / "junk.js").write_text("x")

    excludes = ["node_modules/**"]
    remote = run_runner(tmp_path, excludes, 1_000_000)
    local = manifest_to_json(build_manifest(tmp_path, excludes, hash_below_bytes=1_000_000))

    assert remote["files"].keys() == local["files"].keys()
    assert set(remote["files"]) == {"a.txt", "sub/b.bin"}
    # Same content -> same size and sha256 on both sides.
    for path, entry in remote["files"].items():
        assert entry["size"] == local["files"][path]["size"]
        assert entry["sha256"] == local["files"][path]["sha256"]
        assert entry["sha256"] is not None


def test_runner_probe():
    req = base64.b64encode(json.dumps({"op": "probe"}).encode()).decode()
    proc = subprocess.run(
        [sys.executable, str(RUNNER), req], capture_output=True, text=True, check=True
    )
    data = json.loads(proc.stdout)
    assert "os" in data and "python" in data


def test_runner_missing_root_is_empty(tmp_path: Path):
    out = run_runner(tmp_path / "does-not-exist", [], 0)
    assert out["files"] == {}
