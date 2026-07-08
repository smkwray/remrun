from pathlib import Path

from remrun.manifest import build_manifest, should_exclude


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
