from __future__ import annotations

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


def _native_join(base: str, *parts: str, windows: bool) -> str:
    sep = "\\" if windows else "/"
    base = base.rstrip("/\\")
    cleaned = [p.strip("/\\") for p in parts if p]
    return sep.join([base, *cleaned]) if cleaned else base


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
    By default it is **project-local** — ``<project dir>/.venv`` on the target
    device (simplest to manage; never synced, since ``.venv`` is excluded from
    transfer). Set ``[run] venv_layout = "external"`` to instead place it under
    the device's ``venv_root`` (``venv_root/<venv_name or project leaf>``), or
    name an explicit per-device path in ``[run.venv]`` (highest precedence).
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
    venv: str | None = None
    explicit = run_cfg.get("venv", {})
    if isinstance(explicit, dict) and device.name in explicit:
        venv = str(explicit[device.name])
    elif run_cfg.get("use_venv"):
        layout = str(run_cfg.get("venv_layout", "local")).lower()
        if layout == "external" and device.venv_root:
            name = str(run_cfg.get("venv_name") or project.project_id.split("/")[-1])
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

    return RunEnv(env=env, path_prepend=path_prepend, venv=venv)
