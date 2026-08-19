from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        text=True,
        capture_output=True,
    )


def test_staged_mode_scans_index_bytes_not_sanitized_worktree(tmp_path: Path):
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    (repo / "src").mkdir()
    shutil.copy2(ROOT / "scripts" / "hygiene_check.sh", repo / "scripts")
    candidate = repo / "src" / "example.py"
    candidate.write_text('ROOT = "relative"\n', encoding="utf-8")

    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "smkwray")
    _git(
        repo,
        "config",
        "user.email",
        "45633267+smkwray@users.noreply.github.com",
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "initial")

    private_path = "/" + "Users/private/project"
    candidate.write_text(f'ROOT = "{private_path}"\n', encoding="utf-8")
    _git(repo, "add", "src/example.py")
    candidate.write_text('ROOT = "relative"\n', encoding="utf-8")

    result = subprocess.run(
        ["bash", "scripts/hygiene_check.sh", "--staged"],
        cwd=repo,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "absolute system paths" in result.stderr
    assert private_path in result.stderr
