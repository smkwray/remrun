import subprocess
from pathlib import Path

import pytest

from remrun.config import RemrunConfig, load_project_config, resolve_excludes
from remrun.project import (
    ProjectDetectionError, detect_project, find_project_config,
)


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
    wt = base / "sampleproj" / ".worktrees" / "agent1"
    wt.mkdir(parents=True)
    (wt / ".git").write_text("gitdir: /somewhere/.git/worktrees/agent1\n")
    cfg = make_config(base)
    with pytest.raises(ProjectDetectionError, match="worktree"):
        detect_project(wt, cfg)


def test_git_worktree_override_env_allows(tmp_path: Path, monkeypatch):
    # Escape hatch so an advanced user is never hard-blocked.
    base = tmp_path / "proj"
    wt = base / "sampleproj" / ".worktrees" / "agent1"
    wt.mkdir(parents=True)
    (wt / ".git").write_text("gitdir: /somewhere/.git/worktrees/agent1\n")
    cfg = make_config(base)
    monkeypatch.setenv("REMRUN_ALLOW_WORKTREE", "1")
    ctx = detect_project(wt, cfg)  # must not raise
    assert ctx.project_id == "sampleproj/.worktrees/agent1"


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



def test_linked_worktree_inherits_main_private_config_without_copy(
    tmp_path: Path, monkeypatch
):
    base = tmp_path / "proj"
    main = base / "sampleproj"
    main.mkdir(parents=True)
    subprocess.run(
        ["git", "init", "-b", "main"], cwd=str(main), check=True,
        capture_output=True, text=True,
    )
    subprocess.run(
        ["git", "config", "user.email", "remrun-test@example.invalid"],
        cwd=str(main), check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "remrun Test"],
        cwd=str(main), check=True,
    )
    (main / ".gitignore").write_text(
        "/do/remrun/remrun.toml\n/.worktrees/\n", encoding="utf-8"
    )
    (main / "README.md").write_text("tracked\n", encoding="utf-8")
    subprocess.run(
        ["git", "add", ".gitignore", "README.md"], cwd=str(main), check=True
    )
    subprocess.run(
        ["git", "commit", "-m", "base"], cwd=str(main), check=True,
        capture_output=True, text=True,
    )

    private_config = main / "do" / "remrun" / "remrun.toml"
    private_config.parent.mkdir(parents=True)
    private_config.write_text(
        '[transfer]\nexclude = ["private-cache/**"]\n'
        '[run]\nvenv = ".venv"\n',
        encoding="utf-8",
    )
    worktree = main / ".worktrees" / "agent"
    subprocess.run(
        ["git", "worktree", "add", "-b", "agent", str(worktree)],
        cwd=str(main), check=True, capture_output=True, text=True,
    )

    monkeypatch.setenv("REMRUN_ALLOW_WORKTREE", "1")
    ctx = detect_project(worktree, make_config(base))
    resolved = find_project_config(ctx.local_project_root)

    assert ctx.local_project_root == worktree.resolve()
    assert ctx.project_id == "sampleproj/.worktrees/agent"
    assert resolved == private_config
    assert load_project_config(resolved)["transfer"]["exclude"] == ["private-cache/**"]
    assert not (worktree / "do" / "remrun" / "remrun.toml").exists()
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "do/remrun/remrun.toml"],
        cwd=str(main), capture_output=True, text=True,
    )
    assert tracked.returncode != 0
    assert subprocess.run(
        ["git", "status", "--short", "--untracked-files=all"],
        cwd=str(main), capture_output=True, text=True, check=True,
    ).stdout == ""
