from __future__ import annotations

import os
from pathlib import Path

from .config import RemrunConfig, current_os_key, expand_path
from .models import ProjectContext

# Markers that identify a directory as a project root. Listed from strongest to
# weakest. A top-level `do/` directory is supported as a lightweight project
# marker alongside VCS roots.
PROJECT_MARKERS: tuple[str, ...] = (
    "do/remrun/remrun.toml",
    ".git",
    ".hg",
    ".svn",
    "do",
)


class ProjectDetectionError(RuntimeError):
    pass


def project_root_base(config: RemrunConfig) -> Path:
    """Resolve the configured project-root container for this platform."""
    os_key = current_os_key()
    root_spec = config.project_roots.get(os_key) or config.project_roots.get("default")
    if not root_spec:
        raise ProjectDetectionError("No project_roots entry configured for this platform")
    return expand_path(root_spec).resolve()


def _has_marker(directory: Path) -> bool:
    for marker in PROJECT_MARKERS:
        if (directory / marker).exists():
            return True
    return False


def find_project_root(cwd: Path, base: Path) -> Path:
    """Find the project root for ``cwd`` somewhere under ``base``.

    Strategy:

    1. Walk up from ``cwd`` toward ``base``. The closest strict descendant of
       ``base`` that carries a project marker (``do/``, ``.git``, ...) is the
       project root. This is what makes nested project IDs like ``client/foo``
       work: ``client/foo`` carries the marker, ``client`` does not.
    2. If nothing on the path carries a marker, fall back to the first-level
       child of ``base`` (the historical behavior), so marker-less projects keep
       working.
    """
    cwd = cwd.resolve()
    base = base.resolve()

    try:
        rel_to_base = cwd.relative_to(base)
    except ValueError as exc:
        raise ProjectDetectionError(
            f"cwd {cwd} is not under configured project root {base}"
        ) from exc

    if not rel_to_base.parts:
        raise ProjectDetectionError(f"cwd {cwd} is the project root container, not a project")

    # Candidate directories from cwd up to (and including) the first-level child
    # of base, ordered closest-to-cwd first.
    candidates: list[Path] = []
    current = cwd
    while current != base:
        candidates.append(current)
        current = current.parent

    for candidate in candidates:
        if _has_marker(candidate):
            return candidate

    # Fallback: first-level child of base.
    return base / rel_to_base.parts[0]


def detect_project(cwd: Path, config: RemrunConfig) -> ProjectContext:
    """Detect the project root and project ID from cwd.

    Supports nested project IDs (e.g. ``client/foo``). The project ID is the
    project root path relative to the configured base, in POSIX form. The
    relative cwd is the path inside the project, in POSIX form.
    """
    cwd = cwd.resolve()
    base = project_root_base(config)
    project_root = find_project_root(cwd, base)

    # A linked git worktree (or submodule) has a `.git` *file* (a `gitdir:` pointer),
    # not a repo directory. remrun maps a project by its path, so a worktree would push
    # to a DIFFERENT remote location than the main checkout and take a separate lock —
    # the two could then write overlapping remote files with nothing serializing them.
    # Refuse rather than risk that; the main checkout is the safe place to run.
    if (project_root / ".git").is_file() and os.environ.get("REMRUN_ALLOW_WORKTREE") != "1":
        raise ProjectDetectionError(
            f"{project_root} looks like a git worktree (its .git is a file, not a repo "
            "directory). remrun can't safely run from a linked worktree — run it from the "
            "project's main checkout instead. (Set REMRUN_ALLOW_WORKTREE=1 to override.)"
        )

    project_id = project_root.relative_to(base).as_posix()
    rel_cwd = cwd.relative_to(project_root)
    relative_cwd = rel_cwd.as_posix() if rel_cwd.parts else "."

    return ProjectContext(
        local_project_root=project_root,
        project_id=project_id,
        relative_cwd=relative_cwd,
        local_cwd=cwd,
    )


def find_project_config(project_root: Path) -> Path | None:
    candidate = project_root / "do" / "remrun" / "remrun.toml"
    return candidate if candidate.exists() else None
