"""The remote runner must produce a manifest identical to the local builder.

This runs the *actual* runner.py as a subprocess (it is self-contained), which
validates the remote manifest path without needing a live SSH host.
"""
from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from remrun.config import global_excludes, load_config
from remrun.manifest import build_manifest
from remrun.remote import runner as remote_runner
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


def test_runner_exclude_matcher_preserves_codex_project_policy():
    patterns = global_excludes(load_config(Path(__file__).resolve().parents[1]))

    for path in (
        ".codex/config.toml",
        ".codex/hooks.json",
        ".codex/rules/default.rules",
    ):
        assert not remote_runner.should_exclude(path, patterns)
    for path in (
        ".worktrees/agent/src/app.py",
        ".claude/worktrees/agent/src/app.py",
        ".delegate-worktrees/agent/src/app.py",
    ):
        assert remote_runner.should_exclude(path, patterns)


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


@pytest.mark.skipif(os.name == "nt", reason="POSIX directory-symlink coverage; Windows uses junction test")
def test_runner_manifest_prunes_directory_symlinks(tmp_path: Path):
    # Guard, not a regression test: runner symlink pruning predates this fix set.
    root = tmp_path / "project"
    external = tmp_path / "external"
    root.mkdir()
    external.mkdir()
    (external / "outside.txt").write_text("must stay outside")
    (root / "linked").symlink_to(external, target_is_directory=True)

    out = run_runner(root, [], 1024)

    assert "linked/outside.txt" not in out["files"]


def test_runner_manifest_prunes_directory_reported_as_junction(tmp_path: Path, monkeypatch):
    root = tmp_path / "project"
    junction = root / "junction"
    junction.mkdir(parents=True)
    (junction / "outside.txt").write_text("simulated external content")
    monkeypatch.setattr(
        remote_runner.os.path,
        "isjunction",
        lambda path: Path(path).name == "junction",
        raising=False,
    )

    files = remote_runner.build_manifest(str(root), [], 1024)

    assert "junction/outside.txt" not in files


@pytest.mark.skipif(os.name != "nt", reason="NTFS directory-junction coverage requires Windows")
def test_local_and_runner_manifests_prune_windows_junctions(tmp_path: Path):
    root = tmp_path / "project"
    external = tmp_path / "external"
    root.mkdir()
    external.mkdir()
    (external / "outside.txt").write_text("must stay outside")
    junction = root / "junction"
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(junction), str(external)],
        check=True,
        capture_output=True,
        text=True,
    )

    local = build_manifest(root, [], hash_below_bytes=1024)
    remote = run_runner(root, [], 1024)

    assert "junction/outside.txt" not in local
    assert "junction/outside.txt" not in remote["files"]
