"""Generic projections over frozen configured-task and command records.

Workflow names and semantics are owned entirely by resolved configuration.
Core interprets only protocol fields such as argv, capabilities, pools, costs,
and completion policy.
"""
from __future__ import annotations

from .models import FleetTask, JobFeatures


def _frozen_adapter(task: FleetTask, device: str) -> dict | None:
    if task.resolved_spec is None:
        return None
    return (task.resolved_spec.get("adapters") or {}).get(device)


def candidate_devices(task: FleetTask) -> list[str]:
    if task.prepared and task.prepared["kind"] == "command":
        return [task.force_device] if task.force_device else []
    configured = (task.resolved_spec or {}).get("adapters", {})
    if task.engine:
        return [
            device
            for device, adapter in configured.items()
            if adapter["engine"] == task.engine
        ]
    return list(configured.keys())


def required_capabilities(task: FleetTask) -> frozenset[str]:
    return frozenset(task.requires)


def task_provided_capabilities(task: FleetTask, device: str) -> frozenset[str]:
    adapter = _frozen_adapter(task, device)
    return frozenset((adapter or {}).get("provides", ()))


def pool_for(task: FleetTask, device: str) -> str | None:
    """Configured exclusive resource pool, or None for no mutex."""
    adapter = _frozen_adapter(task, device)
    pool = (adapter or {}).get("pool")
    return str(pool) if pool else None


def resolve_output_root(task: FleetTask, device: str) -> str | None:
    if task.output_root:
        return task.output_root
    adapter = _frozen_adapter(task, device)
    return adapter.get("output_root") if adapter else None


def render_command(
    task: FleetTask,
    device: str,
    stage_dir: str,
    output_root: str | None,
    *,
    manifest_path: str | None = None,
) -> list[str]:
    """Render exact argv from an intrinsic command or frozen adapter.

    ``stage_dir`` is the populated input directory exposed as ``{stage}``.
    ``manifest_path`` is independent because the manifest lives beside that
    directory, not among worker inputs.
    """
    if task.prepared and task.prepared["kind"] == "command":
        return list(task.prepared["command"]["argv"])
    adapter = _frozen_adapter(task, device)
    if not adapter or not task.prepared:
        raise ValueError(f"no frozen adapter for task={task.task_name!r} device={device!r}")
    if task.engine and adapter["engine"] != task.engine:
        raise ValueError(
            f"adapter engine {adapter['engine']!r} does not match "
            f"requested engine {task.engine!r}"
        )
    values = {
        "{stage}": stage_dir,
        "{manifest}": manifest_path,
        "{output_root}": output_root,
    }
    rendered = []
    for token in adapter["argv"]:
        if token.startswith("{opt:") and token.endswith("}"):
            token = str(task.prepared["task"]["options"][token[5:-1]])
        elif token in values:
            value = values[token]
            if value is None:
                raise ValueError(f"adapter placeholder {token} has no prepared value")
            token = str(value)
        rendered.append(token)
    return rendered


def extract_features(task: FleetTask) -> JobFeatures:
    if not task.prepared:
        raise ValueError("fleet work must carry a prepared record")
    from .prepared import prepared_features
    return prepared_features(task.prepared)


def option_bucket(task: FleetTask) -> str:
    if not task.prepared:
        raise ValueError("fleet work must carry a prepared record")
    cost = task.prepared["cost"]
    return "|".join([cost.get("bucket_id") or "unbucketed",
                     f"q={cost.get('status')}"])


def engine_for(task: FleetTask, device: str) -> str:
    adapter = _frozen_adapter(task, device)
    if task.engine:
        if adapter is not None and adapter["engine"] != task.engine:
            raise ValueError(
                f"adapter engine {adapter['engine']!r} does not match "
                f"requested engine {task.engine!r}"
            )
        return task.engine
    return adapter["engine"] if adapter else "command"


def memory_kind_for(task: FleetTask, device: str) -> str:
    adapter = _frozen_adapter(task, device)
    return adapter.get("memory_kind", "none") if adapter else "none"
