"""Controller-local bindings for content-verified shared-storage input routes."""
from __future__ import annotations

import json
import secrets
from pathlib import Path
from typing import Any, Mapping

from ..state import read_json, write_json
from ..transport import BaseTransport, TransportError

MARKER_NAME = ".remrun-storage-root-v1.json"
MAX_MARKER_BYTES = 4096


class StorageError(ValueError):
    pass


def registry_path(state_root: Path) -> Path:
    return state_root / "fleet" / "storage-roots-v1.json"


def load_registry(state_root: Path) -> dict[str, Any]:
    raw = read_json(registry_path(state_root)) or {"schema": 1, "roots": {}}
    if not isinstance(raw, dict) or set(raw) != {"schema", "roots"} \
            or raw["schema"] != 1 or not isinstance(raw["roots"], dict):
        raise StorageError("storage binding registry is malformed")
    return raw


def _marker(payload: Any) -> str:
    if not isinstance(payload, dict) or set(payload) != {"schema", "storage_id"} \
            or payload["schema"] != 1 or not isinstance(payload["storage_id"], str) \
            or len(payload["storage_id"]) != 32:
        raise StorageError("storage root marker is malformed")
    try:
        int(payload["storage_id"], 16)
    except ValueError as exc:
        raise StorageError("storage root marker identity is malformed") from exc
    return payload["storage_id"]


def enroll_local_root(state_root: Path, root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve(strict=True)
    if not root.is_dir():
        raise StorageError("storage root must be a directory")
    marker_path = root / MARKER_NAME
    if marker_path.exists():
        try:
            storage_id = _marker(json.loads(marker_path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as exc:
            raise StorageError(f"cannot read storage root marker: {exc}") from exc
    else:
        storage_id = secrets.token_hex(16)
        write_json(marker_path, {"schema": 1, "storage_id": storage_id})
    registry = load_registry(state_root)
    entry = registry["roots"].setdefault(storage_id, {"local_root": None, "devices": {}})
    entry["local_root"] = str(root)
    write_json(registry_path(state_root), registry)
    return {"schema": 1, "storage_id": storage_id, "local_root": str(root)}


def bind_device_root(
    state_root: Path, device_name: str, remote_root: str, transport: BaseTransport,
) -> dict[str, Any]:
    marker_path = transport.native_join(remote_root, MARKER_NAME)
    try:
        payload = json.loads(
            transport.read_small_file(marker_path, MAX_MARKER_BYTES).decode("utf-8", "strict")
        )
        storage_id = _marker(payload)
    except (TransportError, UnicodeError, json.JSONDecodeError) as exc:
        raise StorageError(f"cannot verify target storage marker: {exc}") from exc
    registry = load_registry(state_root)
    entry = registry["roots"].setdefault(storage_id, {"local_root": None, "devices": {}})
    entry["devices"][device_name] = remote_root
    write_json(registry_path(state_root), registry)
    return {
        "schema": 1, "storage_id": storage_id, "device": device_name,
        "remote_root": remote_root,
    }


def storage_ref_for_path(registry: Mapping[str, Any], source: Path) -> dict[str, Any] | None:
    resolved = source.expanduser().resolve(strict=True)
    matches: list[tuple[int, str, Path]] = []
    for storage_id, entry in registry.get("roots", {}).items():
        local = entry.get("local_root") if isinstance(entry, dict) else None
        if not local:
            continue
        root = Path(local)
        try:
            relative = resolved.relative_to(root)
        except ValueError:
            continue
        matches.append((len(root.parts), storage_id, relative))
    if not matches:
        return None
    _depth, storage_id, relative = max(matches)
    components = list(relative.parts)
    if not components or any(value in {"", ".", ".."} or "/" in value or "\\" in value
                             for value in components):
        raise StorageError("storage reference has unsafe relative components")
    return {"schema": 1, "storage_id": storage_id, "relative_components": components}


def device_candidate(
    registry: Mapping[str, Any], device_name: str, storage_ref: Mapping[str, Any],
    transport: BaseTransport,
) -> tuple[str, str] | None:
    storage_id = storage_ref.get("storage_id")
    entry = registry.get("roots", {}).get(storage_id)
    if not isinstance(entry, dict):
        return None
    root = entry.get("devices", {}).get(device_name)
    if not isinstance(root, str) or not root:
        return None
    components = storage_ref.get("relative_components")
    if not isinstance(components, list) or not components:
        return None
    return root, transport.native_join(root, *components)
