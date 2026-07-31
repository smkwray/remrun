from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = ROOT / "bin" / "remrun"


def _write_executable(path: Path, contents: str) -> None:
    path.write_text(contents, encoding="utf-8")
    path.chmod(0o755)


def test_launcher_skips_incompatible_python3(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    _write_executable(fake_bin / "python3", "#!/bin/sh\nexit 1\n")
    _write_executable(
        fake_bin / "python3.14",
        f"#!/bin/sh\nexec {str(sys.executable)!r} \"$@\"\n",
    )
    env = os.environ.copy()
    env.pop("REMRUN_PYTHON", None)
    env["PATH"] = f"{fake_bin}:/usr/bin:/bin"

    result = subprocess.run(
        ["/bin/bash", str(LAUNCHER), "--help"],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "usage: remrun" in result.stdout


def test_launcher_rejects_incompatible_explicit_interpreter(tmp_path: Path) -> None:
    incompatible = tmp_path / "python"
    _write_executable(incompatible, "#!/bin/sh\nexit 1\n")
    env = os.environ.copy()
    env["REMRUN_PYTHON"] = str(incompatible)

    result = subprocess.run(
        ["/bin/bash", str(LAUNCHER), "--help"],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert "REMRUN_PYTHON must be an executable Python 3.11+" in result.stderr
