from __future__ import annotations

from pathlib import Path

from .config import (
    RemrunConfig,
    hash_below_bytes,
    load_project_config,
    resolve_excludes,
    scheduler_config,
)
from .models import RunPlan
from .project import detect_project, find_project_config
from .resource_context import select_workload
from .scheduler import order_devices
from .scopes import resolve_write_scope


def make_run_plan(
    *,
    cwd: Path,
    config: RemrunConfig,
    target_name: str | None,
    command: list[str],
    scope_name: str | None = None,
    json_events: bool = False,
    requested_workload: str | None = None,
    allow_default_workload: bool = True,
) -> RunPlan:
    project = detect_project(cwd, config)
    project_config_path = find_project_config(project.local_project_root)
    project_config = load_project_config(project_config_path)

    workload = (
        select_workload(project_config, requested_workload)
        if requested_workload is not None or allow_default_workload
        else None
    )

    candidates = order_devices(
        config.devices, target_name, project_config=project_config, command=command,
        scheduler_cfg=scheduler_config(config),
    )
    transfer_mode = str(config.defaults.get("transfer", {}).get("mode", "safe"))
    write_scope = resolve_write_scope(project_config, scope_name)

    return RunPlan(
        target=candidates[0],
        candidates=candidates,
        project=project,
        command=command,
        transfer_mode=transfer_mode,
        project_config_path=project_config_path,
        excludes=resolve_excludes(config, project_config),
        hash_below_bytes=hash_below_bytes(config),
        project_config=project_config,
        json=json_events,
        write_scope=write_scope.name,
        write_scope_paths=list(write_scope.paths),
        workload=workload,
    )
