from __future__ import annotations

from pathlib import Path

from .bootstrap import parse_bootstrap
from .config import (
    RemrunConfig,
    global_excludes,
    hash_below_bytes,
    load_project_config,
    push_limits,
    resolve_excludes,
    scheduler_config,
)
from .manifest import path_identity, should_exclude
from .models import RunPlan
from .project import detect_project, find_project_config
from .resource_context import select_workload
from .scheduler import order_devices
from .scopes import resolve_write_scope

# The transfer modes `run` implements. `safe` reconciles the project tree before and
# after the command; `none` runs in the target's existing checkout and moves no files;
# `commit` runs a clean HEAD in a run-private tree and returns only declared outputs.
TRANSFER_MODES = ("safe", "none", "commit")


def _transfer_mode(config: RemrunConfig, requested: str | None) -> str:
    """The run's transfer mode: ``requested`` (the per-run flag) or ``[transfer] mode``."""
    if requested is not None:
        mode, source = requested, "--transfer"
    else:
        mode, source = config.defaults.get("transfer", {}).get("mode", "safe"), "[transfer] mode"
    if not isinstance(mode, str) or mode not in TRANSFER_MODES:
        raise ValueError(
            f"{source} must be one of {', '.join(TRANSFER_MODES)}; got {mode!r}"
        )
    return mode


def _project_path(raw: object, flag: str, *, directory_ok: bool = False) -> str:
    """One path flag value as a root-anchored, normalized, project-relative POSIX path.

    No globs, so two paths overlap exactly when one is a prefix of the other. With
    ``directory_ok``, a trailing ``/`` names a directory and everything under it.
    """
    text = str(raw).strip().replace("\\", "/")
    is_dir = directory_ok and text.endswith("/") and len(text) > 1
    body = text[:-1] if is_dir else text
    parts = body.split("/")
    if (
        not body
        or body.startswith("/")
        or any(part in {"", ".", ".."} for part in parts)
        or any(char in body for char in ":*?[]\x00")
    ):
        kind = "a file path, or a directory written dir/," if directory_ok else "a file path"
        raise ValueError(
            f"{flag} takes {kind} relative to the project root, without globs: {raw!r}"
        )
    return body + "/" if is_dir else body


def _verify_path(raw: object) -> str:
    """One ``--verify`` file as a root-anchored, project-relative POSIX path."""
    return _project_path(raw, "--verify")


def paths_overlap(first: str, second: str) -> bool:
    """Whether two normalized ``--output`` paths may name a common file: a prefix test on
    their ``path_identity``. The controller's filesystem may ignore case and Unicode
    normalization, so ``out/`` and ``OUT/`` overlap; on a case-sensitive one this
    over-matches, which only refuses a lease (fail closed)."""
    first, second = path_identity(first), path_identity(second)
    if first.rstrip("/") == second.rstrip("/"):
        return True
    return (first.endswith("/") and second.startswith(first)) or (
        second.endswith("/") and first.startswith(second)
    )


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
    transfer_mode: str | None = None,
    verify: list[str] | tuple[str, ...] = (),
    outputs: list[str] | tuple[str, ...] = (),
    inputs: list[str] | tuple[str, ...] = (),
) -> RunPlan:
    mode = _transfer_mode(config, transfer_mode)
    verify_paths = tuple(dict.fromkeys(_verify_path(path) for path in verify))
    output_paths = tuple(
        dict.fromkeys(_project_path(path, "--output", directory_ok=True) for path in outputs)
    )
    input_paths = tuple(dict.fromkeys(_project_path(path, "--input") for path in inputs))
    if verify_paths and mode != "none":
        raise ValueError("--verify applies only to --transfer none")
    if (output_paths or input_paths) and mode != "commit":
        raise ValueError("--output and --input apply only to --transfer commit")
    if mode != "safe" and scope_name:
        raise ValueError(
            f"--transfer {mode} cannot take --scope: --scope narrows a reconciling pullback"
        )
    tree_excludes = global_excludes(config)
    if mode == "commit":
        if not output_paths:
            raise ValueError("--transfer commit needs at least one --output PATH")
        # Everything under a declared output returns, but an output that is itself under
        # a global exclude names VCS, environment, sync or remrun-private paths
        # (`.git/`, `node_modules/`, `.remrun-*`); returning those is never intended.
        for path in output_paths:
            probe = path + "x" if path.endswith("/") else path
            if should_exclude(probe, tree_excludes):
                raise ValueError(
                    f"--output {path} is under a [transfer] global_exclude pattern; "
                    "remrun never returns such paths"
                )
    project = detect_project(cwd, config)
    project_config_path = find_project_config(project.local_project_root)
    project_config = load_project_config(project_config_path)
    bootstrap = parse_bootstrap(
        project_config, project_root=project.local_project_root
    )

    workload = (
        select_workload(project_config, requested_workload)
        if requested_workload is not None or allow_default_workload
        else None
    )

    candidates = order_devices(
        config.devices, target_name, project_config=project_config, command=command,
        scheduler_cfg=scheduler_config(config),
    )
    if mode != "safe" and workload is not None:
        raise ValueError(
            f"--transfer {mode} does not support workload envelopes/receipts "
            f"(workload {workload.name!r} is selected)"
        )
    write_scope = resolve_write_scope(project_config, scope_name)
    max_push_files, max_push_bytes = push_limits(config, project_config)

    return RunPlan(
        target=candidates[0],
        candidates=candidates,
        project=project,
        command=command,
        transfer_mode=mode,
        verify=verify_paths,
        outputs=output_paths,
        inputs=input_paths,
        tree_excludes=tree_excludes,
        project_config_path=project_config_path,
        excludes=resolve_excludes(config, project_config),
        hash_below_bytes=hash_below_bytes(config),
        max_push_files=max_push_files,
        max_push_bytes=max_push_bytes,
        project_config=project_config,
        json=json_events,
        write_scope=write_scope.name,
        write_scope_paths=list(write_scope.paths),
        workload=workload,
        bootstrap=bootstrap,
    )
