import os
import stat
from pathlib import Path

import pytest

from remrun.config import global_excludes, load_config
from remrun.manifest import (
    FileEntry,
    ManifestError,
    build_manifest,
    canonical_identity,
    should_exclude,
    strong_manifest_digest,
)


def test_should_exclude_tree_pattern():
    assert should_exclude("scratch/a.txt", ["scratch/**"])
    assert should_exclude("scratch", ["scratch/**"])
    assert not should_exclude("src/scratch/a.txt", ["scratch/**"])


def test_build_manifest_excludes(tmp_path: Path):
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "scratch").mkdir()
    (tmp_path / "scratch" / "b.txt").write_text("b")
    manifest = build_manifest(tmp_path, ["scratch/**"], hash_below_bytes=1024)
    assert "a.txt" in manifest
    assert "scratch/b.txt" not in manifest


@pytest.mark.skipif(os.name == "nt", reason="POSIX directory-symlink coverage; Windows uses junction test")
def test_build_manifest_prunes_directory_symlinks(tmp_path: Path):
    # Guard, not a regression test: directory symlinks were already pruned before this fix set.
    root = tmp_path / "project"
    external = tmp_path / "external"
    root.mkdir()
    external.mkdir()
    (external / "outside.txt").write_text("must stay outside")
    (root / "linked").symlink_to(external, target_is_directory=True)

    manifest = build_manifest(root, [], hash_below_bytes=1024)

    assert "linked/outside.txt" not in manifest


def test_build_manifest_prunes_directory_reported_as_junction(tmp_path: Path, monkeypatch):
    root = tmp_path / "project"
    junction = root / "junction"
    junction.mkdir(parents=True)
    (junction / "outside.txt").write_text("simulated external content")
    monkeypatch.setattr(
        os.path, "isjunction", lambda path: Path(path).name == "junction", raising=False
    )

    manifest = build_manifest(root, [], hash_below_bytes=1024)

    assert "junction/outside.txt" not in manifest


def test_global_excludes_cover_nested_python_caches_without_hiding_source():
    config = load_config(Path(__file__).resolve().parents[1])
    patterns = global_excludes(config)

    generated = (
        "src/sampleproj/quality/__pycache__/module.cpython-313.pyc",
        "src/sampleproj/quality/module.pyc",
        "tests/unit/.pytest_cache/v/cache/nodeids",
        "src/sampleproj/.mypy_cache/3.13/module.meta.json",
        "src/sampleproj/.ruff_cache/0.12.1/cache-key",
    )
    assert all(should_exclude(path, patterns) for path in generated)
    assert not should_exclude("src/sampleproj/quality/module.py", patterns)


def test_global_excludes_cover_nested_node_modules_without_hiding_source():
    config = load_config(Path(__file__).resolve().parents[1])
    patterns = global_excludes(config)

    generated = (
        "node_modules/react/package.json",
        "web/node_modules/react/package.json",
        "apps/desktop/web/node_modules/typescript/lib/typescript.js",
    )
    assert all(should_exclude(path, patterns) for path in generated)
    assert not should_exclude("web/src/node_modules-report.ts", patterns)


def test_global_excludes_cover_remrun_private_scratch_namespace():
    # remrun's own atomic-write temp/backup/tar files must never be treated as
    # project data. Regression for audit F4: an orphaned Windows .remrun-backup-*.bak
    # left by an interrupted push was being swept into the surface and re-transferred.
    config = load_config(Path(__file__).resolve().parents[1])
    patterns = global_excludes(config)

    scratch = (
        ".remrun-tmp-result.rds-abc123.tmp",
        "results/.remrun-tmp-out.csv-def456.tmp",
        ".remrun-backup-result.rds-abc123.bak",
        "results/sub/.remrun-backup-out.parquet-ff00.bak",
        ".remrun-tar-xyz.tmp",
    )
    assert all(should_exclude(path, patterns) for path in scratch)
    # A real output that merely lives beside the scratch files must still transfer.
    assert not should_exclude("results/result.rds", patterns)


def test_global_excludes_cover_every_agent_nested_checkout_convention():
    # A nested worktree is a near-duplicate of the parent tree. Agents place them under
    # several explicit conventions; those directory names are safe to exclude without
    # hiding project-scoped configuration.
    config = load_config(Path(__file__).resolve().parents[1])
    patterns = global_excludes(config)

    nested = (
        ".worktrees/agent1/src/app.py",
        "sub/.worktrees/agent1/src/app.py",
        ".claude/worktrees/field-fixes/src/remrun/cli.py",
        "sub/.claude/worktrees/field-fixes/src/remrun/cli.py",
        ".delegate-worktrees/agent1/src/app.py",
    )
    assert all(should_exclude(path, patterns) for path in nested)
    # Only explicit worktree subtrees are excluded — agent configuration is ordinary
    # project surface, and `.codex/<name>/` cannot safely be distinguished from rules or
    # hooks by this glob matcher alone.
    preserved = (
        ".claude/settings.json",
        ".codex/config.toml",
        ".codex/hooks.json",
        ".codex/rules/default.rules",
        ".codex/reseal-bisect/data/big.parquet",
        "docs/claude-notes.md",
    )
    assert all(not should_exclude(path, patterns) for path in preserved)


def test_global_excludes_preserve_requested_build_outputs():
    config = load_config(Path(__file__).resolve().parents[1])
    patterns = global_excludes(config)

    assert not should_exclude("build/bin/app.exe", patterns)
    assert should_exclude("dist/assets/app.js", patterns)
    assert should_exclude("target/release/app", patterns)


def test_build_manifest_fails_closed_on_unreadable_file(tmp_path: Path, monkeypatch):
    # WHY: a file that exists but can't be stat'd (permission/mount/AV/network glitch)
    # must ABORT the manifest, not be silently dropped — a dropped file reads downstream
    # as a deletion and can wipe the healthy copy on the other side.
    (tmp_path / "a.txt").write_text("x")
    real_stat = Path.stat

    def flaky_stat(self, *a, **k):
        if self.name == "a.txt":
            raise PermissionError(13, "denied")
        return real_stat(self, *a, **k)

    monkeypatch.setattr(Path, "stat", flaky_stat)
    with pytest.raises(ManifestError):
        build_manifest(tmp_path, [], hash_below_bytes=1024)


def test_build_manifest_skips_file_that_vanished_mid_scan(tmp_path: Path, monkeypatch):
    # A file genuinely gone between listing and stat (FileNotFoundError) is fine to skip —
    # it really isn't there. Only *unprovable* absence aborts.
    (tmp_path / "a.txt").write_text("x")
    (tmp_path / "b.txt").write_text("y")
    real_stat = Path.stat

    def vanish(self, *a, **k):
        if self.name == "a.txt":
            raise FileNotFoundError(2, "gone")
        return real_stat(self, *a, **k)

    monkeypatch.setattr(Path, "stat", vanish)
    manifest = build_manifest(tmp_path, [], hash_below_bytes=1024)
    assert "a.txt" not in manifest
    assert "b.txt" in manifest


def test_build_manifest_fails_closed_when_file_vanishes_during_hash(
    tmp_path: Path, monkeypatch
):
    import remrun.manifest as manifest_mod

    path = tmp_path / "generated.tmp"
    path.write_text("transient", encoding="utf-8")
    real_sha256 = manifest_mod.sha256_file

    def disappear(candidate: Path) -> str:
        if Path(candidate) == path:
            path.unlink()
            raise FileNotFoundError(2, "gone", str(candidate))
        return real_sha256(candidate)

    monkeypatch.setattr(manifest_mod, "sha256_file", disappear)
    with pytest.raises(
        ManifestError, match=r"changed while hashing generated\.tmp.*retry"
    ):
        build_manifest(tmp_path, [], hash_below_bytes=1024)


# --- design Step 1: identity v2 + strong-manifest digest (inert groundwork) ----

def test_build_manifest_captures_mode_and_always_hash(tmp_path: Path):
    p = tmp_path / "s.sh"
    p.write_text("echo hi\n")
    os.chmod(p, 0o755)
    # Default: file above the cap (cap=0) is NOT hashed, but mode is captured.
    m = build_manifest(tmp_path, [], hash_below_bytes=0)
    # Windows chmod only controls the read-only bit; capture the platform's
    # actual mode rather than imposing POSIX semantics on this cross-platform test.
    expected_mode = stat.S_IMODE(p.stat().st_mode)
    assert m["s.sh"].mode == expected_mode
    assert m["s.sh"].sha256 is None
    # always_hash hashes it regardless of the cap (commit-gate behavior).
    m2 = build_manifest(tmp_path, [], hash_below_bytes=0, always_hash=True)
    assert m2["s.sh"].sha256 is not None
    assert m2["s.sh"].mode == expected_mode


def test_mode_is_not_in_comparable_tuple_so_live_equality_is_unchanged():
    # Groundwork must be inert: two entries differing ONLY by mode must still compare equal
    # via comparable_tuple() (which the live reconcile uses), so current behavior is unchanged.
    a = FileEntry("a.txt", "file", 3, 111, "aa", 0o644)
    b = FileEntry("a.txt", "file", 3, 111, "aa", 0o755)
    assert a.comparable_tuple() == b.comparable_tuple()


def test_strong_manifest_digest_deterministic_and_order_independent():
    a = FileEntry("a.txt", "file", 3, 111, "aa", 0o644)
    b = FileEntry("b.txt", "file", 5, 222, "bb", 0o600)
    assert strong_manifest_digest({"a.txt": a, "b.txt": b}) == \
        strong_manifest_digest({"b.txt": b, "a.txt": a})


def test_strong_manifest_digest_ignores_mtime_but_reflects_content_and_mode():
    d = strong_manifest_digest
    base = FileEntry("a.txt", "file", 3, 111, "aa", 0o644)
    same_content_diff_mtime = FileEntry("a.txt", "file", 3, 999, "aa", 0o644)
    diff_content = FileEntry("a.txt", "file", 3, 111, "cc", 0o644)
    diff_mode = FileEntry("a.txt", "file", 3, 111, "aa", 0o755)
    assert d({"a.txt": base}) == d({"a.txt": same_content_diff_mtime})  # mtime ignored
    assert d({"a.txt": base}) != d({"a.txt": diff_content})             # content matters
    assert d({"a.txt": base}) != d({"a.txt": diff_mode})               # mode matters


def test_strong_manifest_digest_preserves_case_distinct_paths():
    lower = FileEntry("readme.md", "file", 1, 1, "aa", 0o644)
    upper = FileEntry("README.md", "file", 1, 1, "aa", 0o644)
    assert strong_manifest_digest({"readme.md": lower}) != \
        strong_manifest_digest({"README.md": upper})


def test_canonical_identity_shape():
    e = FileEntry("a.txt", "file", 3, 111, "aa", 0o644)
    assert canonical_identity(e) == {
        "kind": "file", "size": 3, "mtime_ns": 111, "sha256": "aa", "mode": 0o644,
    }
