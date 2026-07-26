from __future__ import annotations

import os
import platform
import tomllib
import socket
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .models import Device


@dataclass(frozen=True)
class RemrunConfig:
    repo_root: Path
    defaults: dict[str, Any]
    devices: dict[str, Device]
    project_roots: dict[str, str]
    offload: dict[str, Any] = field(default_factory=dict)
    # Named, project-less sync trees for `remrun sync` (for example, generated
    # outputs that live outside a project checkout). Each tree maps an OS key ->
    # base path, e.g. {"shared_outputs": {"windows": "<windows-output-root>",
    # "macos": "<mac-output-root>"}}
    sync_roots: dict[str, dict[str, str]] = field(default_factory=dict)
    # Fleet task adapters from devices.toml [fleet.adapters.<task>.<device>] — the (task,device) ->
    # engine/worker-command/output-root mapping. Core ships NONE; the workflow is entirely user
    # config so nothing deployment-specific lives in the published code. {task: {device: spec}}.
    fleet_adapters: dict[str, dict[str, Any]] = field(default_factory=dict)
    # git-sync may intentionally cover a broader tree than command execution. For
    # example, normal projects live under ~/work/projects while remrun itself lives at
    # ~/work/remrun. Keeping this separate preserves the narrow run/transfer root.
    git_sync: dict[str, Any] = field(default_factory=dict)
    # Parsed now for the inert Step-3 runner rollout. Legacy remains the default;
    # later steps are responsible for enforcing runner-v1 coordination.
    coordination: dict[str, Any] = field(default_factory=dict)
    # Optional [scheduler] overrides from devices.toml. The `--auto` preference order is
    # deployment-specific (it names real devices), so it belongs in the private device
    # registry rather than the published defaults.toml. Merged OVER defaults.toml by
    # scheduler_config(); tuning knobs stay in defaults where they are device-agnostic.
    device_scheduler: dict[str, Any] = field(default_factory=dict)


def find_remrun_root(start: Path | None = None) -> Path:
    """Find the remrun repository root.

    The installed package may be imported from a venv, but for this seed the most
    useful behavior for agents is to find a nearby repo containing config/.
    """
    candidates: list[Path] = []
    if start:
        candidates.extend([start, *start.parents])
    here = Path(__file__).resolve()
    candidates.extend(here.parents)

    env_root = os.environ.get("REMRUN_ROOT")
    if env_root:
        candidates.insert(0, Path(env_root).expanduser())

    for candidate in candidates:
        config_dir = candidate / "config"
        if (config_dir / "devices.toml").exists() or (config_dir / "devices.example.toml").exists():
            return candidate

    # Fall back to the current working directory so error messages remain useful.
    return Path.cwd()


def load_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def load_config(remrun_root: Path | None = None) -> RemrunConfig:
    root = remrun_root or find_remrun_root(Path.cwd())
    defaults = load_toml(root / "config" / "defaults.toml")
    devices_doc = load_toml(root / "config" / "devices.toml")

    devices = {
        name: Device.from_mapping(name, data)
        for name, data in devices_doc.get("devices", {}).items()
    }
    project_roots = dict(devices_doc.get("project_roots", {}))
    offload = dict(devices_doc.get("offload", {}))
    sync_roots = {
        name: dict(paths)
        for name, paths in devices_doc.get("sync_roots", {}).items()
        if isinstance(paths, dict)
    }
    fleet_adapters = {
        task: {dev: dict(spec) for dev, spec in devmap.items() if isinstance(spec, dict)}
        for task, devmap in devices_doc.get("fleet", {}).get("adapters", {}).items()
        if isinstance(devmap, dict)
    }
    git_sync = dict(devices_doc.get("git_sync", {}) or {})
    coordination = dict(devices_doc.get("coordination", {}) or {})
    device_scheduler = dict(devices_doc.get("scheduler", {}) or {})
    return RemrunConfig(repo_root=root, defaults=defaults, devices=devices,
                        project_roots=project_roots, offload=offload, sync_roots=sync_roots,
                        fleet_adapters=fleet_adapters, git_sync=git_sync,
                        coordination=coordination, device_scheduler=device_scheduler)


# Conservative workstation defaults when a controller defines no explicit threshold
# (or its entry is a bare policy string). A weak controller overrides these downward.
_DEFAULT_OFFLOAD_THRESHOLD = {"ram_gb": 5.0, "wall_sec": 300, "note": ""}


def _offload_entry(config: RemrunConfig, hostname: str | None = None):
    """The raw `[offload]` entry (a policy string or a table) for a host.

    Falls back to the `default` entry when the host is not listed.
    """
    host = (hostname or socket.gethostname()).lower()
    table = config.offload or {}
    for key, value in table.items():
        if key.lower() == host and key.lower() != "default":
            return value
    return table.get("default")


def offload_policy(config: RemrunConfig, hostname: str | None = None,
                   project_config: dict[str, Any] | None = None) -> str:
    """Resolve the remrun offload policy ('auto'|'ask'|'never') for a host.

    A project's `[run] offload` wins; else the per-host `[offload]` entry in
    devices.toml (case-insensitive; either a bare policy string or a
    `{policy=...}` table); else `[offload].default`; else 'ask'.
    """
    proj = (project_config or {}).get("run", {}).get("offload")
    if proj:
        return str(proj)
    entry = _offload_entry(config, hostname)
    if isinstance(entry, dict):
        return str(entry.get("policy", "ask"))
    return str(entry) if entry else "ask"


def offload_threshold(config: RemrunConfig, hostname: str | None = None) -> dict[str, Any]:
    """Per-controller 'what counts as heavy/long' threshold for the host.

    Returns ``{ram_gb, wall_sec, note}`` from the host's `[offload]` table entry,
    falling back to the conservative workstation default when the entry is a bare
    policy string or unset. A weak controller (e.g. a 15 W laptop) sets these low
    so it offloads far more eagerly than a workstation.
    """
    entry = _offload_entry(config, hostname)
    if isinstance(entry, dict):
        return {
            "ram_gb": float(entry.get("ram_gb", _DEFAULT_OFFLOAD_THRESHOLD["ram_gb"])),
            "wall_sec": int(entry.get("wall_sec", _DEFAULT_OFFLOAD_THRESHOLD["wall_sec"])),
            "note": str(entry.get("note", "")),
        }
    return dict(_DEFAULT_OFFLOAD_THRESHOLD)


def current_os_key() -> str:
    system = platform.system().lower()
    if "windows" in system:
        return "windows"
    if "darwin" in system or "mac" in system:
        return "macos"
    return "default"


def device_os_key(device: Device) -> str:
    """Map a device's ``os`` to a ``[sync_roots]`` OS key (windows/macos/default)."""
    os_name = (device.os or "").lower()
    if os_name.startswith("win"):
        return "windows"
    if os_name.startswith("darwin") or "mac" in os_name:
        return "macos"
    return "default"


def case_insensitive(os_key: str) -> bool:
    """Windows (NTFS) and macOS (default APFS) are case-insensitive — two distinct paths that fold
    to the same name collapse into one file (silent data loss) on such a target."""
    return os_key in ("windows", "macos")


def casefold_collisions(paths) -> dict[str, list[str]]:
    """Group distinct paths that fold to the same case-insensitive (NFC+casefold) key."""
    groups: dict[str, list[str]] = {}
    for p in paths:
        key = unicodedata.normalize("NFC", p).casefold()
        groups.setdefault(key, []).append(p)
    return {k: sorted(set(v)) for k, v in groups.items() if len(set(v)) > 1}


def expand_path(path: str) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(path)))


def global_excludes(config: RemrunConfig) -> list[str]:
    """Default active-surface excludes from defaults.toml [transfer]."""
    transfer = config.defaults.get("transfer", {})
    return list(transfer.get("global_exclude", []))


def _enabled_device_names(config: RemrunConfig) -> list[str]:
    """Enabled, real (non-simulation) devices in config insertion order. A
    ``local-sim`` backend is a test/dry-run target, so it never seeds the derived
    scheduler order (auto-routing or the bench default)."""
    return [name for name, device in config.devices.items()
            if device.enabled and device.kind != "local-sim"]


def scheduler_config(config: RemrunConfig) -> dict[str, Any]:
    """Resolve the [scheduler] block, with generic fallbacks.

    Sources, later winning: defaults.toml `[scheduler]` (device-agnostic tuning knobs),
    then devices.toml `[scheduler]` (the `--auto` preference order, which names real
    devices and is therefore deployment-specific). Keeping the order in the private
    device registry is what lets the published core ship no private fleet name.

    When no order is configured in either, derive it from the enabled devices in
    devices.toml — but note that follows file insertion order, which is an accident of
    editing history and can put a laptop ahead of a dedicated compute box.
    """
    s = dict(config.defaults.get("scheduler", {}))
    s.update(config.device_scheduler)
    enabled = _enabled_device_names(config)
    if "primary" not in s:
        s["primary"] = enabled[0] if enabled else None
    if "fallback" not in s:
        s["fallback"] = enabled[1:]
    s.setdefault("prefer_reachable_primary", True)
    s.setdefault("load_balance", True)
    s.setdefault("busy_floor_pct", 40)
    s.setdefault("eff_core_weight", 1.0)
    s.setdefault("headroom_margin_cores", 4)
    s.setdefault("ram_headroom_pct", 80)
    s.setdefault("trivial_job_seconds", 30)
    return s


def hash_below_bytes(config: RemrunConfig) -> int:
    """Byte threshold under which files are hashed during manifesting."""
    transfer = config.defaults.get("transfer", {})
    mb = transfer.get("hash_small_files_below_mb", 0)
    try:
        return int(float(mb) * 1024 * 1024)
    except (TypeError, ValueError):
        return 0


def large_file_warn_bytes(config: RemrunConfig) -> int:
    transfer = config.defaults.get("transfer", {})
    mb = transfer.get("large_file_warn_above_mb", 0)
    try:
        return int(float(mb) * 1024 * 1024)
    except (TypeError, ValueError):
        return 0


def load_retention(config: RemrunConfig):
    """Build a RetentionPolicy from defaults.toml [logging]."""
    from .state import RetentionPolicy

    lg = config.defaults.get("logging", {})
    try:
        max_bytes = int(float(lg.get("max_full_log_mb", 100)) * 1024 * 1024)
    except (TypeError, ValueError):
        max_bytes = 100 * 1024 * 1024
    def _mb(key: str, default_mb: float) -> int:
        try:
            return int(float(lg.get(key, default_mb)) * 1024 * 1024)
        except (TypeError, ValueError):
            return int(default_mb * 1024 * 1024)

    return RetentionPolicy(
        full_log_days=int(lg.get("full_log_retention_days", 3)),
        failed_log_days=int(lg.get("failed_log_retention_days", 7)),
        summary_days=int(lg.get("summary_retention_days", 7)),
        max_log_bytes=max_bytes,
        backup_below_bytes=_mb("backup_below_mb", 50),
        backup_days=int(lg.get("backup_retention_days", 3)),
        max_backup_bytes=_mb("max_backup_mb", 1024),
    )


def load_project_config(path: Path | None) -> dict[str, Any]:
    """Load an optional ``do/remrun/remrun.toml`` project config."""
    if not path:
        return {}
    return load_toml(path)


def resolve_excludes(config: RemrunConfig, project_config: dict[str, Any]) -> list[str]:
    """Merge global excludes with project-specific excludes (order preserved,
    de-duplicated). Project config narrows the active surface; it never widens it
    by removing global excludes."""
    merged: list[str] = []
    seen: set[str] = set()
    for pattern in global_excludes(config):
        if pattern not in seen:
            merged.append(pattern)
            seen.add(pattern)
    project_transfer = project_config.get("transfer", {}) if project_config else {}
    for pattern in project_transfer.get("exclude", []):
        if pattern not in seen:
            merged.append(pattern)
            seen.add(pattern)
    return merged
