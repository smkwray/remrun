from __future__ import annotations

import pytest

from remrun.scopes import resolve_write_scope


def test_scope_paths_must_be_project_relative() -> None:
    cfg = {
        "parallel": {
            "scopes": {
                "bad_posix": {"paths": ["/results/spec_a/**"]},
                "bad_windows": {"paths": ["C:\\projects\\out\\**"]},
            }
        }
    }

    with pytest.raises(ValueError, match="project-relative"):
        resolve_write_scope(cfg, "bad_posix")
    with pytest.raises(ValueError, match="project-relative"):
        resolve_write_scope(cfg, "bad_windows")
