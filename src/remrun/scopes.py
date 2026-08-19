from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class WriteScope:
    name: str | None = None
    paths: tuple[str, ...] = ()

    @property
    def lock_name(self) -> str:
        return self.name or "project"

    @property
    def is_project_wide(self) -> bool:
        return self.name is None


def _normalize_scope_path(path: object) -> str:
    raw = str(path).strip()
    text = raw.replace("\\", "/")
    if not text:
        raise ValueError("write-scope paths must not be empty")
    if text.startswith("/") or ":" in text:
        raise ValueError(f"write-scope path must be project-relative: {path!r}")
    text = text.strip("/")
    parts = [p for p in text.split("/") if p]
    if any(p == ".." for p in parts):
        raise ValueError(f"write-scope path must not contain '..': {path!r}")
    return "/".join(parts)


def _scope_table(project_config: dict[str, Any]) -> dict[str, Any]:
    parallel = project_config.get("parallel", {}) if project_config else {}
    scopes = parallel.get("scopes", {}) if isinstance(parallel, dict) else {}
    return scopes if isinstance(scopes, dict) else {}


def configured_scope_names(project_config: dict[str, Any]) -> list[str]:
    return sorted(str(name) for name in _scope_table(project_config))


def resolve_write_scope(project_config: dict[str, Any], requested: str | None) -> WriteScope:
    if not requested:
        return WriteScope()

    name = requested.strip()
    if not name:
        raise ValueError("--scope must not be empty")
    scopes = _scope_table(project_config)
    if name not in scopes:
        known = ", ".join(configured_scope_names(project_config)) or "(none configured)"
        raise ValueError(f"unknown write scope {name!r}; configured scopes: {known}")

    data = scopes[name]
    if not isinstance(data, dict):
        raise ValueError(f"write scope {name!r} must be a table")
    raw_paths = data.get("paths", [])
    if not isinstance(raw_paths, list) or not raw_paths:
        raise ValueError(f"write scope {name!r} must declare a non-empty paths list")
    paths = tuple(_normalize_scope_path(p) for p in raw_paths)
    return WriteScope(name=name, paths=paths)
