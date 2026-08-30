from __future__ import annotations

import ntpath
import posixpath
import re
from dataclasses import dataclass, field
from typing import Any

from .models import Device, ProjectContext

# Env var names are interpolated *unquoted* into the remote shell / PowerShell
# (`export NAME=...`, `$env:NAME = ...`), so a crafted name in a project/device
# config would be a shell-injection vector. Restrict to the portable, safe form.
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_env_name(name: str) -> str:
    if not _ENV_NAME_RE.match(name):
        raise ValueError(
            f"invalid environment variable name {name!r}: only letters, digits and "
            "underscore are allowed (must not start with a digit)"
        )
    return name


@dataclass(frozen=True)
class RunEnv:
    """Resolved environment for a command on a specific target device.

    ``env`` are environment variables to export; ``path_prepend`` are directories
    to prepend to PATH (device-native, venv bin first); ``venv`` is the resolved
    virtualenv path if any. Remote paths may still contain ``~`` — the transport
    expands them against the remote home.
    """
    env: dict[str, str] = field(default_factory=dict)
    path_prepend: list[str] = field(default_factory=list)
    venv: str | None = None
    # The external root is retained so bootstrap can re-check confinement after
    # a target-local ``~`` has been expanded.
    venv_root: str | None = None
    managed: bool = False


def _native_join(base: str, *parts: str, windows: bool) -> str:
    sep = "\\" if windows else "/"
    base = base.rstrip("/\\")
    cleaned = [p.strip("/\\") for p in parts if p]
    return sep.join([base, *cleaned]) if cleaned else base


def _safe_venv_name(value: object) -> str:
    """Validate one environment directory component before target mutation."""
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("venv_name must be a non-empty path component")
    if value in {".", ".."} or "/" in value or "\\" in value:
        raise ValueError("venv_name must be one safe path component")
    return value


def _canonical_config_path(raw: object, *, windows: bool, label: str) -> str:
    """Canonicalize a target-native path without touching the target filesystem."""
    if not isinstance(raw, str) or not raw or "\x00" in raw:
        raise ValueError(f"{label} must be a non-empty path")
    if windows:
        if raw == "~" or raw.startswith(("~\\", "~/")):
            return ("~" + ntpath.normpath(raw[1:]).replace("/", "\\")).rstrip("\\") or "~"
        if not ntpath.isabs(raw):
            raise ValueError(f"{label} must be absolute or home-relative")
        return ntpath.normpath(raw)
    if "\\" in raw:
        raise ValueError(f"{label} must use native POSIX path syntax")
    if raw == "~" or raw.startswith("~/"):
        return ("~" + posixpath.normpath(raw[1:])).rstrip("/") or "~"
    if not posixpath.isabs(raw):
        raise ValueError(f"{label} must be absolute or home-relative")
    return posixpath.normpath(raw)


def _path_is_strictly_under(path: str, root: str, *, windows: bool) -> bool:
    path_fn = ntpath if windows else posixpath
    norm_path = path_fn.normcase(path_fn.normpath(path))
    norm_root = path_fn.normcase(path_fn.normpath(root)).rstrip("\\/")
    if norm_path == norm_root:
        return False
    separator = "\\" if windows else "/"
    return norm_path.startswith(norm_root + separator)


def _external_venv(
    *, device: Device, project: ProjectContext, run_cfg: dict[str, Any]
) -> tuple[str, str]:
    """Resolve and confine a bootstrap environment before target mutation."""
    root = _canonical_config_path(
        device.venv_root, windows=device.is_windows, label="device.venv_root"
    )
    explicit = run_cfg.get("venv", {})
    override = explicit.get(device.name) if isinstance(explicit, dict) else None
    if override is not None:
        venv = _canonical_config_path(
            override, windows=device.is_windows,
            label=f"[run.venv].{device.name}",
        )
    else:
        if run_cfg.get("venv_layout") != "external":
            raise ValueError(
                "[run.bootstrap] requires venv_layout = 'external' or a confined device override"
            )
        name = run_cfg.get("venv_name")
        if name is None:
            project_leaf = project.project_id.replace("\\", "/").rstrip("/").split("/")[-1]
            name = project_leaf
        venv = _native_join(
            root, _safe_venv_name(name), windows=device.is_windows
        )
    if not _path_is_strictly_under(venv, root, windows=device.is_windows):
        raise ValueError("bootstrap virtualenv must be strictly under device.venv_root")
    return root, venv


def resolve_run_env(
    *,
    device: Device,
    project: ProjectContext,
    project_config: dict[str, Any] | None,
) -> RunEnv:
    """Layer device + project configuration into a concrete run environment.

    Precedence (later wins for env vars): device ``env`` -> project ``[env]``.
    PATH order: project/device venv bin -> device ``path`` -> the remote's own
    PATH. A virtualenv is used when a project opts in via ``[run] use_venv``.
    Ordinary ``use_venv`` runs retain the project-local default. A
    ``[run.bootstrap]`` declaration is stricter: it must opt into ``use_venv``
    and resolve to an external environment under ``device.venv_root`` (or a
    per-device override confined beneath that root).
    """
    project_config = project_config or {}
    env: dict[str, str] = {}
    path_prepend: list[str] = []

    # 1. Device-level environment and PATH additions.
    for key, value in device.env.items():
        env[_check_env_name(str(key))] = str(value)
    path_prepend.extend(device.path)

    # 2. Project-level env vars (scalar values only; all devices).
    for key, value in project_config.get("env", {}).items():
        if not isinstance(value, dict):
            env[_check_env_name(str(key))] = str(value)

    # 3. Virtualenv resolution.
    run_cfg = project_config.get("run", {})
    if not isinstance(run_cfg, dict):
        run_cfg = {}
    venv: str | None = None
    venv_root: str | None = None
    managed = False
    explicit = run_cfg.get("venv", {})
    bootstrap_declared = "bootstrap" in run_cfg and run_cfg.get("bootstrap") is not None
    if bootstrap_declared:
        if run_cfg.get("use_venv") is not True:
            raise ValueError("[run.bootstrap] requires [run] use_venv = true")
        venv_root, venv = _external_venv(
            device=device, project=project, run_cfg=run_cfg
        )
        managed = True
    elif isinstance(explicit, dict) and device.name in explicit:
        venv = str(explicit[device.name])
    elif run_cfg.get("use_venv"):
        layout = str(run_cfg.get("venv_layout", "local")).lower()
        if layout == "external" and device.venv_root:
            name = str(run_cfg.get("venv_name") or project.project_id.split("/")[-1])
            venv_root = device.venv_root
            venv = _native_join(device.venv_root, name, windows=device.is_windows)
        else:
            # Default: project-local .venv beside the project on the target device.
            venv = _native_join(
                device.project_root, project.project_id, ".venv",
                windows=device.is_windows,
            )

    if venv:
        bindir = _native_join(
            venv, "Scripts" if device.is_windows else "bin", windows=device.is_windows
        )
        path_prepend.insert(0, bindir)  # venv takes priority on PATH
        env.setdefault("VIRTUAL_ENV", venv)

    return RunEnv(
        env=env,
        path_prepend=path_prepend,
        venv=venv,
        venv_root=venv_root,
        managed=managed,
    )
