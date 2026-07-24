from pathlib import Path

import pytest

from remrun.config import RemrunConfig, resolve_excludes
from remrun.project import ProjectDetectionError, detect_project


def make_config(base: Path) -> RemrunConfig:
    return RemrunConfig(
        repo_root=base,
        defaults={"transfer": {"global_exclude": [".git/**", "node_modules/**"]}},
        devices={},
        project_roots={"default": str(base), "macos": str(base), "windows": str(base)},
    )


def test_simple_project_fallback(tmp_path: Path):
    base = tmp_path / "proj"
    (base / "paper1" / "analysis").mkdir(parents=True)
    cfg = make_config(base)
    ctx = detect_project(base / "paper1" / "analysis", cfg)
    assert ctx.project_id == "paper1"
    assert ctx.relative_cwd == "analysis"


def test_project_root_is_cwd(tmp_path: Path):
    base = tmp_path / "proj"
    (base / "paper1").mkdir(parents=True)
    cfg = make_config(base)
    ctx = detect_project(base / "paper1", cfg)
    assert ctx.project_id == "paper1"
    assert ctx.relative_cwd == "."


def test_nested_project_id_via_marker(tmp_path: Path):
    base = tmp_path / "proj"
    foo = base / "client" / "foo"
    (foo / "do").mkdir(parents=True)
    (foo / "analysis").mkdir()
    cfg = make_config(base)
    ctx = detect_project(foo / "analysis", cfg)
    # `client/foo` carries the marker (`do/`); `client` does not.
    assert ctx.project_id == "client/foo"
    assert ctx.relative_cwd == "analysis"


def test_git_worktree_is_refused(tmp_path: Path):
    # WHY: a linked worktree has a `.git` FILE (a gitdir pointer), not a repo dir. remrun
    # maps a project by its path, so running from a worktree would push to a different
    # remote location than the main checkout AND take a separate lock — two writers over
    # overlapping remote files with nothing serializing them. Refuse it.
    base = tmp_path / "proj"
    wt = base / "statera" / ".worktrees" / "agent1"
    wt.mkdir(parents=True)
    (wt / ".git").write_text("gitdir: /somewhere/.git/worktrees/agent1\n")
    cfg = make_config(base)
    with pytest.raises(ProjectDetectionError, match="worktree"):
        detect_project(wt, cfg)


def test_git_worktree_override_env_allows(tmp_path: Path, monkeypatch):
    # Escape hatch so an advanced user is never hard-blocked.
    base = tmp_path / "proj"
    wt = base / "statera" / ".worktrees" / "agent1"
    wt.mkdir(parents=True)
    (wt / ".git").write_text("gitdir: /somewhere/.git/worktrees/agent1\n")
    cfg = make_config(base)
    monkeypatch.setenv("REMRUN_ALLOW_WORKTREE", "1")
    ctx = detect_project(wt, cfg)  # must not raise
    assert ctx.project_id == "statera/.worktrees/agent1"


def test_marker_closest_to_cwd_wins(tmp_path: Path):
    base = tmp_path / "proj"
    foo = base / "client" / "foo"
    (base / "client" / "do").mkdir(parents=True)  # outer marker
    (foo / "do").mkdir(parents=True)  # inner marker, closer to cwd
    (foo / "sub").mkdir()
    cfg = make_config(base)
    ctx = detect_project(foo / "sub", cfg)
    assert ctx.project_id == "client/foo"
    assert ctx.relative_cwd == "sub"


def test_git_marker(tmp_path: Path):
    base = tmp_path / "proj"
    proj = base / "repo"
    (proj / ".git").mkdir(parents=True)
    (proj / "src").mkdir()
    cfg = make_config(base)
    ctx = detect_project(proj / "src", cfg)
    assert ctx.project_id == "repo"
    assert ctx.relative_cwd == "src"


def test_container_is_not_a_project(tmp_path: Path):
    base = tmp_path / "proj"
    base.mkdir(parents=True)
    cfg = make_config(base)
    with pytest.raises(ProjectDetectionError):
        detect_project(base, cfg)


def test_cwd_outside_base(tmp_path: Path):
    base = tmp_path / "proj"
    base.mkdir(parents=True)
    other = tmp_path / "elsewhere"
    other.mkdir()
    cfg = make_config(base)
    with pytest.raises(ProjectDetectionError):
        detect_project(other, cfg)


def test_resolve_excludes_merges_and_dedupes(tmp_path: Path):
    base = tmp_path / "proj"
    cfg = make_config(base)
    project_config = {"transfer": {"exclude": ["data/raw/**", "node_modules/**"]}}
    merged = resolve_excludes(cfg, project_config)
    assert merged == [".git/**", "node_modules/**", "data/raw/**"]
