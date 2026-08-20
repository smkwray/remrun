from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import tempfile
import sys
import uuid
from contextlib import nullcontext
from dataclasses import dataclass, field, replace
from pathlib import Path

from .config import RemrunConfig, load_project_config
from .job_observation import (
    JobObservation,
    active_job_observation_enabled,
    observe_controller_operation,
)
from .output import Reporter
from .project import (
    ProjectDetectionError, detect_project, find_project_config, project_root_base,
)
from .state import default_state_root
from .transport import BaseTransport, TransportError, make_transport

EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_DIVERGED = 2
EXIT_TRANSFER = 3
EXIT_INFRA = 4
HOOK_BEGIN = "# >>> remrun git-sync hook >>>"
HOOK_END = "# <<< remrun git-sync hook <<<"
_BOUNDED_GIT_METADATA_LIMIT_MIB = 128
_REMOTE_MEMORY_LIMIT_KEY = "remote_memory_limit_mib"


class GitSyncError(RuntimeError):
    def __init__(self, message: str, exit_code: int = EXIT_INTERNAL) -> None:
        super().__init__(message)
        self.exit_code = exit_code


def _git_sync_observation_context(
    config: RemrunConfig, *, device_name: str, operation: str
):
    """Return an opt-in, fail-open controller-operation observation context."""
    if not active_job_observation_enabled():
        return nullcontext(False)
    try:
        effective = _git_sync_config(config)
        project, _project_config = _detect_git_project(
            effective, require_git=False, boundary_config=config
        )
        command = ["remrun", "git-sync", device_name, f"--{operation}"]
        observation = JobObservation.for_command(
            job_id=f"git-sync-{uuid.uuid4().hex[:12]}",
            project=project.project_id,
            target=device_name,
            phase="git-sync",
            command=command,
            declared_label=f"git-sync:{operation}",
        )
    except Exception:
        # The real Git-sync path owns project/config error reporting. Failure to
        # prepare optional observation must not replace or duplicate that error.
        return nullcontext(False)
    return observe_controller_operation(observation)


class _MemoryLimitedGitTransport:
    """Delegate one Git-sync path through the generic explicit-limit seam."""

    def __init__(self, transport: BaseTransport, memory_limit_mib: int) -> None:
        self._transport = transport
        self._memory_limit_mib = memory_limit_mib

    def __getattr__(self, name: str):
        return getattr(self._transport, name)

    def exec(self, command: list[str], cwd: str, **kwargs):
        if not command or command[0] != "git":
            raise TransportError(
                "Git-sync memory-limited transport accepts only direct git argv"
            )
        return self._transport.exec_with_memory_limit(
            command,
            cwd=cwd,
            memory_limit_mib=self._memory_limit_mib,
            **kwargs,
        )


@dataclass
class BranchAction:
    branch: str
    state: str
    old: str | None = None
    new: str | None = None
    detail: str = ""


@dataclass(frozen=True)
class WorktreeDirtySummary:
    """Small, agent-facing classification of a Git working tree."""

    tracked: int = 0
    content: int = 0
    mode_only: int = 0
    untracked: int = 0

    @property
    def dirty(self) -> bool:
        return self.tracked > 0

    def as_dict(self) -> dict[str, int]:
        return {
            "tracked": self.tracked,
            "content": self.content,
            "mode_only": self.mode_only,
            "untracked": self.untracked,
        }

    def describe(self) -> str:
        return (f"{self.tracked} tracked ({self.content} content, "
                f"{self.mode_only} mode-only), {self.untracked} untracked")


@dataclass
class GitSyncBootstrap:
    """Result of initializing a repo-less project from a peer's history.

    The working tree is left byte-for-byte untouched: history is fetched, the
    local branch is pointed at the selected peer branch (or peer HEAD when no
    branch is explicit) via update-ref/symbolic-ref, and
    only the index is refreshed (`git reset --mixed`).
    """
    device: str
    local_project: str
    remote_project: str
    branch: str
    head: str
    commits_fetched: int
    modified: int
    untracked: int
    hooks_path_set: bool = False

    def as_dict(self) -> dict:
        return {
            "device": self.device,
            "local_project": self.local_project,
            "remote_project": self.remote_project,
            "branch": self.branch,
            "head": self.head,
            "commits_fetched": self.commits_fetched,
            "modified": self.modified,
            "untracked": self.untracked,
            "hooks_path_set": self.hooks_path_set,
            "exit_code": EXIT_OK,
        }


@dataclass
class GitSyncResult:
    device: str
    direction: str
    local_project: str
    remote_project: str
    pulled: list[BranchAction] = field(default_factory=list)
    pushed: list[BranchAction] = field(default_factory=list)
    diverged: list[BranchAction] = field(default_factory=list)
    skipped: list[BranchAction] = field(default_factory=list)
    dry_run: bool = False
    bootstrap: GitSyncBootstrap | None = None

    @property
    def exit_code(self) -> int:
        return EXIT_DIVERGED if self.diverged else EXIT_OK

    def as_dict(self) -> dict:
        def pack(items: list[BranchAction]) -> list[dict]:
            return [a.__dict__ for a in items]

        return {
            "device": self.device,
            "direction": self.direction,
            "local_project": self.local_project,
            "remote_project": self.remote_project,
            "pulled": pack(self.pulled),
            "pushed": pack(self.pushed),
            "diverged": pack(self.diverged),
            "skipped": pack(self.skipped),
            "dry_run": self.dry_run,
            "bootstrap": self.bootstrap.as_dict() if self.bootstrap else None,
            "exit_code": self.exit_code,
        }


@dataclass
class GitSyncStatus:
    device: str
    local_project: str
    remote_project: str
    branches: list[BranchAction]
    local_history_present: bool
    local_dirty: bool
    remote_dirty: bool
    local_dirty_summary: WorktreeDirtySummary
    remote_dirty_summary: WorktreeDirtySummary
    hook_installed: bool
    hook_log: str | None
    line_endings_ok: bool

    @property
    def exit_code(self) -> int:
        return EXIT_DIVERGED if any(a.state == "diverged" for a in self.branches) else EXIT_OK

    def as_dict(self) -> dict:
        return {
            "device": self.device,
            "local_project": self.local_project,
            "remote_project": self.remote_project,
            "branches": [a.__dict__ for a in self.branches],
            "local_history_present": self.local_history_present,
            "local_dirty": self.local_dirty,
            "remote_dirty": self.remote_dirty,
            "local_dirty_summary": self.local_dirty_summary.as_dict(),
            "remote_dirty_summary": self.remote_dirty_summary.as_dict(),
            "hook_installed": self.hook_installed,
            "hook_log": self.hook_log,
            "line_endings_ok": self.line_endings_ok,
            "exit_code": self.exit_code,
        }


def run_git_sync(
    config: RemrunConfig,
    *,
    device_name: str,
    direction: str = "both",
    dry_run: bool = False,
    branch: str | None = None,
    bootstrap: bool = False,
    remote_memory_limit_mib: int | None = None,
    reporter: Reporter | None = None,
    as_json: bool = False,
) -> int:
    reporter = reporter or Reporter(json_events=as_json)
    operation = "bootstrap" if bootstrap else direction.lower()
    with _git_sync_observation_context(
        config, device_name=device_name, operation=operation
    ):
        try:
            result = run_git_sync_result(
                config,
                device_name=device_name,
                direction=direction,
                dry_run=dry_run,
                branch=branch,
                bootstrap=bootstrap,
                remote_memory_limit_mib=remote_memory_limit_mib,
                reporter=reporter,
            )
        except GitSyncError as exc:
            reporter.event("git_sync_error", message=str(exc), exit_code=exc.exit_code)
            return exc.exit_code
        except TransportError as exc:
            reporter.event("git_sync_error", message=str(exc), exit_code=EXIT_TRANSFER)
            return EXIT_TRANSFER

        if as_json:
            print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
        return result.exit_code


def run_git_sync_status(
    config: RemrunConfig,
    *,
    device_name: str,
    branch: str | None = None,
    remote_memory_limit_mib: int | None = None,
    reporter: Reporter | None = None,
    as_json: bool = False,
) -> int:
    reporter = reporter or Reporter(json_events=as_json)
    with _git_sync_observation_context(
        config, device_name=device_name, operation="status"
    ):
        try:
            status = git_sync_status_result(
                config, device_name=device_name, branch=branch,
                remote_memory_limit_mib=remote_memory_limit_mib, reporter=reporter)
        except GitSyncError as exc:
            reporter.event("git_sync_error", message=str(exc), exit_code=exc.exit_code)
            return exc.exit_code
        except TransportError as exc:
            reporter.event("git_sync_error", message=str(exc), exit_code=EXIT_TRANSFER)
            return EXIT_TRANSFER
        if as_json:
            print(json.dumps(status.as_dict(), indent=2, sort_keys=True))
        return status.exit_code


def install_git_sync_hook(
    config: RemrunConfig,
    *,
    device_name: str | None = None,
    reporter: Reporter | None = None,
) -> int:
    reporter = reporter or Reporter()
    try:
        boundary_config = config
        config = _git_sync_config(config)
        project, project_config = _detect_git_project(
            config, boundary_config=boundary_config
        )
        peers = _resolve_hook_peers(config, project_config, device_name)
        hook_path = _hook_path(project.local_project_root)
        backup_path = hook_path.with_name(hook_path.name + ".remrun-backup")
        hook_path.parent.mkdir(parents=True, exist_ok=True)
        existing = hook_path.read_text(encoding="utf-8") if hook_path.exists() else ""
        if hook_path.exists() and HOOK_BEGIN not in existing and not backup_path.exists():
            hook_path.replace(backup_path)
        elif hook_path.exists() and HOOK_BEGIN not in existing and backup_path.exists():
            raise GitSyncError(f"existing hook already present and backup exists: {hook_path}",
                               EXIT_INTERNAL)
        hook_path.write_text(_hook_script(config, project.local_project_root, peers),
                             encoding="utf-8", newline="\n")
        try:
            hook_path.chmod(hook_path.stat().st_mode | 0o111)
        except OSError:
            pass
        reporter.event("git_sync_hook_installed", path=str(hook_path), peers=peers,
                       backup=str(backup_path) if backup_path.exists() else None)
        _warn_line_endings(project.local_project_root, config, reporter)
        return EXIT_OK
    except GitSyncError as exc:
        reporter.event("git_sync_error", message=str(exc), exit_code=exc.exit_code)
        return exc.exit_code


def uninstall_git_sync_hook(
    config: RemrunConfig,
    *,
    reporter: Reporter | None = None,
) -> int:
    reporter = reporter or Reporter()
    try:
        boundary_config = config
        config = _git_sync_config(config)
        project, _project_config = _detect_git_project(
            config, boundary_config=boundary_config
        )
        hook_path = _hook_path(project.local_project_root)
        backup_path = hook_path.with_name(hook_path.name + ".remrun-backup")
        existing = hook_path.read_text(encoding="utf-8") if hook_path.exists() else ""
        if hook_path.exists() and HOOK_BEGIN not in existing:
            raise GitSyncError(f"post-commit hook is not managed by remrun: {hook_path}",
                               EXIT_INTERNAL)
        if hook_path.exists():
            hook_path.unlink()
        restored = False
        if backup_path.exists():
            backup_path.replace(hook_path)
            restored = True
        reporter.event("git_sync_hook_uninstalled", path=str(hook_path), restored_backup=restored)
        return EXIT_OK
    except GitSyncError as exc:
        reporter.event("git_sync_error", message=str(exc), exit_code=exc.exit_code)
        return exc.exit_code


def run_git_sync_result(
    config: RemrunConfig,
    *,
    device_name: str,
    direction: str = "both",
    dry_run: bool = False,
    branch: str | None = None,
    bootstrap: bool = False,
    remote_memory_limit_mib: int | None = None,
    reporter: Reporter | None = None,
) -> GitSyncResult:
    reporter = reporter or Reporter()
    boundary_config = config
    config = _git_sync_config(config)
    direction = direction.lower()
    if direction not in {"pull", "push", "both"}:
        raise GitSyncError(f"invalid git-sync direction: {direction}", EXIT_INTERNAL)
    if device_name not in config.devices:
        raise GitSyncError(f"unknown device: {device_name}", EXIT_INTERNAL)
    device = config.devices[device_name]
    if not device.enabled:
        raise GitSyncError(f"device disabled: {device_name}", EXIT_INFRA)

    project, project_config = _detect_git_project(
        config, require_git=False, boundary_config=boundary_config
    )
    memory_limit_mib = _remote_memory_limit_mib(
        config, project_config, remote_memory_limit_mib
    )
    local_root = project.local_project_root
    branch = _validated_branch(local_root, branch)

    local_is_repo = _is_git_repo(local_root)
    local_has_head = (
        local_is_repo
        and _local_git(local_root, ["rev-parse", "--verify", "--quiet", "HEAD"]).returncode == 0
    )
    if not local_has_head:
        # Repo-less project—or an empty/unborn `.git` left by an interrupted
        # bootstrap. Seed/recover it from the peer's authoritative history.
        if bootstrap or direction in {"pull", "both"}:
            return _bootstrap_from_peer(
                config, device_name, project, dry_run=dry_run, reporter=reporter,
                remote_memory_limit_mib=memory_limit_mib, branch=branch)
        raise GitSyncError(
            f"not a git repo: {local_root} (use --pull/--bootstrap to seed it from {device_name})",
            EXIT_INFRA)
    if bootstrap:
        raise GitSyncError(
            f"already a git repo: {local_root} (--bootstrap only applies to a repo-less project)",
            EXIT_INFRA)

    _ensure_local_git_repo(local_root)
    _warn_line_endings(local_root, config, reporter)
    transport = make_transport(device)
    probe = transport.probe()
    if not probe.reachable:
        raise GitSyncError(f"{device_name} unreachable: {probe.detail}", EXIT_INFRA)
    remote_root = transport.remote_project_path(project)
    metadata_transport = _bounded_metadata_transport(transport, memory_limit_mib)
    _ensure_remote_git_repo(metadata_transport, remote_root)

    local_ns = _ref_namespace(socket.gethostname() or "LOCAL")
    peer_ns = _ref_namespace(device_name)
    result = GitSyncResult(
        device=device_name,
        direction=direction,
        local_project=str(local_root),
        remote_project=remote_root,
        dry_run=dry_run,
    )
    reporter.event("git_sync", device=device_name, direction=direction,
                   local_project=str(local_root), remote_project=remote_root,
                   dry_run=dry_run)
    if dry_run:
        reporter.event("git_sync_dry_run", action="verified repos; no fetch or fast-forward")
        return result

    operation_transport = _repository_scaled_transport(
        transport, memory_limit_mib, operation=direction
    )

    project_git_sync = project_config.get("git_sync", {}) or {}
    advance_dirty = bool(project_git_sync.get(
        "advance_dirty_worktree",
        (config.git_sync or {}).get("advance_dirty_worktree", False),
    ))
    if direction in {"pull", "both"}:
        result.pulled.extend(_pull_from_peer(
            operation_transport, remote_root, local_root, peer_ns, branch, reporter,
            advance_dirty_worktree=advance_dirty))
    if direction in {"push", "both"}:
        peer_actions = _push_to_peer(
            operation_transport, remote_root, local_root, local_ns, branch, reporter,
            advance_dirty_worktree=advance_dirty)
        result.pushed.extend(peer_actions)

    result.diverged.extend([a for a in result.pulled + result.pushed if a.state == "diverged"])
    result.skipped.extend([a for a in result.pulled + result.pushed if a.state.startswith("skipped")])
    reporter.event("git_sync_summary", exit_code=result.exit_code,
                   fast_forwarded=len([a for a in result.pulled + result.pushed
                                        if a.state.startswith("fast_forwarded")]),
                   diverged=len(result.diverged),
                   skipped=len(result.skipped))
    return result


def git_sync_status_result(
    config: RemrunConfig,
    *,
    device_name: str,
    branch: str | None = None,
    remote_memory_limit_mib: int | None = None,
    reporter: Reporter | None = None,
) -> GitSyncStatus:
    reporter = reporter or Reporter()
    boundary_config = config
    config = _git_sync_config(config)
    if device_name not in config.devices:
        raise GitSyncError(f"unknown device: {device_name}", EXIT_INTERNAL)
    device = config.devices[device_name]
    if not device.enabled:
        raise GitSyncError(f"device disabled: {device_name}", EXIT_INFRA)

    project, project_config = _detect_git_project(
        config, require_git=False, boundary_config=boundary_config
    )
    memory_limit_mib = _remote_memory_limit_mib(
        config, project_config, remote_memory_limit_mib
    )
    local_root = project.local_project_root
    branch = _validated_branch(local_root, branch)
    local_history_present = (
        _is_git_repo(local_root)
        and _local_git(local_root, ["rev-parse", "--verify", "--quiet", "HEAD"]).returncode == 0
    )
    transport = make_transport(device)
    probe = transport.probe()
    if not probe.reachable:
        raise GitSyncError(f"{device_name} unreachable: {probe.detail}", EXIT_INFRA)
    remote_root = transport.remote_project_path(project)
    metadata_transport = _bounded_metadata_transport(transport, memory_limit_mib)
    _ensure_remote_git_repo(metadata_transport, remote_root)
    operation_transport = _repository_scaled_transport(
        transport, memory_limit_mib, operation="status"
    )

    peer_ns = _ref_namespace(device_name)
    source_missing = (
        branch is not None
        and not _remote_ref_exists(
            operation_transport, remote_root, f"refs/heads/{branch}"
        )
    )
    if source_missing:
        branches = _missing_local_branch_actions(
            local_root, peer_ns, branch, status=True,
            local_history_present=local_history_present,
        )
    else:
        with tempfile.TemporaryDirectory(prefix="remrun-gitsync-status-") as td:
            tmp_repo = Path(td) / "local.git"
            if local_history_present:
                _local_git_ok(Path.cwd(), ["clone", "--bare", str(local_root), str(tmp_repo)])
            else:
                _local_git_ok(Path.cwd(), ["init", "--bare", str(tmp_repo)])
            local_bundle = Path(td) / "peer.bundle"
            remote_tmp = operation_transport.remote_temp_dir("remrun-gitsync-status")
            remote_bundle = operation_transport.native_join(remote_tmp, "peer.bundle")
            try:
                _remote_git_ok(
                    operation_transport, remote_root,
                    ["bundle", "create", remote_bundle, *_bundle_selection(branch)],
                )
                operation_transport.pull_file(remote_bundle, local_bundle)
            finally:
                operation_transport.remove_remote_tree(remote_tmp)
            _local_git_ok(
                tmp_repo,
                ["fetch", "--tags", str(local_bundle), _fetch_refspec(peer_ns, branch)],
            )
            branches = (
                _status_branches(tmp_repo, peer_ns, branch)
                if local_history_present
                else _bootstrap_status_branches(tmp_repo, peer_ns, branch)
            )

    local_summary = (
        _dirty_summary_local(local_root)
        if local_history_present
        else WorktreeDirtySummary()
    )
    remote_summary = _dirty_summary_remote(operation_transport, remote_root)
    local_dirty = local_summary.dirty
    remote_dirty = remote_summary.dirty
    hook_installed = False
    if local_history_present:
        hook = _hook_path(local_root)
        hook_installed = hook.exists() and HOOK_BEGIN in hook.read_text(
            encoding="utf-8", errors="replace"
        )
    log_path = _hook_log_path(config, project.project_id)
    line_endings_ok = _line_endings_ok(local_root)
    for action in branches:
        reporter.event("git_sync_status", branch=action.branch, state=action.state,
                       old=action.old, new=action.new, detail=action.detail)
    reporter.event("git_sync_status_summary", device=device_name,
                   local_history_present=local_history_present, local_dirty=local_dirty,
                   remote_dirty=remote_dirty, hook_installed=hook_installed,
                   local_dirty_summary=local_summary.as_dict(),
                   remote_dirty_summary=remote_summary.as_dict(),
                   hook_log=str(log_path) if log_path.exists() else None,
                   line_endings_ok=line_endings_ok, exit_code=EXIT_DIVERGED
                   if any(a.state == "diverged" for a in branches) else EXIT_OK)
    if not line_endings_ok:
        _warn_line_endings(local_root, config, reporter)
    return GitSyncStatus(
        device=device_name,
        local_project=str(local_root),
        remote_project=remote_root,
        branches=branches,
        local_history_present=local_history_present,
        local_dirty=local_dirty,
        remote_dirty=remote_dirty,
        local_dirty_summary=local_summary,
        remote_dirty_summary=remote_summary,
        hook_installed=hook_installed,
        hook_log=str(log_path) if log_path.exists() else None,
        line_endings_ok=line_endings_ok,
    )


def _detect_git_project(
    config: RemrunConfig,
    *,
    require_git: bool = True,
    boundary_config: RemrunConfig | None = None,
):
    """Detect one Git-sync project without weakening ordinary project boundaries.

    A broader ``git_sync.project_roots`` changes the stable project ID and peer mapping,
    not the local project leaf. First detect against the ordinary boundary; if cwd lies
    outside it (the supported sibling-repository case), fall back to the broader mapping.
    """
    cwd = Path.cwd()
    project = None
    if boundary_config is not None and boundary_config is not config:
        try:
            bounded = detect_project(cwd, boundary_config)
        except ProjectDetectionError:
            bounded = None
        if bounded is not None:
            try:
                sync_base = project_root_base(config)
                project_id = bounded.local_project_root.relative_to(sync_base).as_posix()
            except (ProjectDetectionError, ValueError) as exc:
                raise GitSyncError(
                    f"ordinary project root {bounded.local_project_root} is not under "
                    f"the configured git-sync root",
                    EXIT_INFRA,
                ) from exc
            project = replace(bounded, project_id=project_id)
    if project is None:
        try:
            project = detect_project(cwd, config)
        except ProjectDetectionError as exc:
            raise GitSyncError(str(exc), EXIT_INFRA) from exc
    project_config = load_project_config(find_project_config(project.local_project_root))
    if require_git:
        _ensure_local_git_repo(project.local_project_root)
    return project, project_config


def _git_sync_config(config: RemrunConfig) -> RemrunConfig:
    """Apply the optional broader project mapping used only by git-sync.

    Command execution keeps using ``[project_roots]`` and each device's
    ``project_root``. A configured ``[git_sync.project_roots]`` replaces both sides
    for Git history exchange, so sibling repositories such as ``work/remrun`` and
    ordinary ``work/projects/foo`` share one stable relative project id without
    broadening remrun's arbitrary-file transfer surface.
    """
    roots = (config.git_sync or {}).get("project_roots", {})
    if not isinstance(roots, dict) or not roots:
        return config
    normalized = {str(key): str(value) for key, value in roots.items()}
    devices = dict(config.devices)
    for name, device in devices.items():
        root = _root_for_device_os(normalized, device.os)
        if root:
            devices[name] = replace(device, project_root=root)
    return replace(config, project_roots=normalized, devices=devices)


def _remote_memory_limit_mib(
    config: RemrunConfig,
    project_config: dict,
    override: object,
) -> int | None:
    """Resolve one explicit hard limit; never reinterpret it as an RSS sample."""
    if override is not None:
        raw = override
    else:
        project_git_sync = project_config.get("git_sync", {}) or {}
        if isinstance(project_git_sync, dict) and _REMOTE_MEMORY_LIMIT_KEY in project_git_sync:
            raw = project_git_sync[_REMOTE_MEMORY_LIMIT_KEY]
        else:
            raw = (config.git_sync or {}).get(_REMOTE_MEMORY_LIMIT_KEY)
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        raise GitSyncError(
            f"git-sync {_REMOTE_MEMORY_LIMIT_KEY} must be a positive integer MiB value",
            EXIT_INTERNAL,
        )
    return raw


def _bounded_metadata_transport(
    transport: BaseTransport,
    explicit_limit_mib: int | None,
) -> BaseTransport:
    if transport.memory_guard is None:
        return transport
    limit_mib = _BOUNDED_GIT_METADATA_LIMIT_MIB
    if explicit_limit_mib is not None:
        limit_mib = min(limit_mib, explicit_limit_mib)
    return _MemoryLimitedGitTransport(
        transport,
        limit_mib,
    )  # type: ignore[return-value]


def _repository_scaled_transport(
    transport: BaseTransport,
    explicit_limit_mib: int | None,
    *,
    operation: str,
) -> BaseTransport:
    if transport.memory_guard is None:
        return transport
    if explicit_limit_mib is None:
        raise GitSyncError(
            f"guarded target {transport.device.name!r} requires [git_sync] "
            f"{_REMOTE_MEMORY_LIMIT_KEY} or --remote-memory-limit-mib before "
            f"repository-scaling git-sync work ({operation}); the value is a hard "
            "per-remote-command process-tree limit, not a learned RSS measurement",
            EXIT_INFRA,
        )
    return _MemoryLimitedGitTransport(
        transport, explicit_limit_mib
    )  # type: ignore[return-value]


def _root_for_device_os(roots: dict[str, str], os_name: str) -> str | None:
    raw = os_name.lower()
    keys = [raw]
    if raw.startswith("win"):
        keys.append("windows")
    elif raw.startswith("mac") or raw.startswith("darwin"):
        keys.append("macos")
    elif raw.startswith("linux") or raw.startswith("posix"):
        keys.extend(["linux", "posix"])
    keys.append("default")
    for key in keys:
        value = roots.get(key)
        if value:
            return value
    return None


def _resolve_hook_peers(
    config: RemrunConfig,
    project_config: dict,
    device_name: str | None,
) -> list[str]:
    global_peers = (config.git_sync or {}).get("peers", [])
    raw_peers = (project_config.get("git_sync", {}) or {}).get("peers", global_peers)
    if isinstance(raw_peers, str):
        raw_peers = [raw_peers]
    peers: list[str] = []
    for item in [device_name, *list(raw_peers or [])]:
        if item and str(item) not in peers:
            peers.append(str(item))
    if not peers:
        raise GitSyncError("no git-sync peers configured; pass a device or set [git_sync].peers",
                           EXIT_INTERNAL)
    unknown = [peer for peer in peers if peer not in config.devices]
    if unknown:
        raise GitSyncError(f"unknown git-sync peer(s): {', '.join(unknown)}", EXIT_INTERNAL)
    return peers


def _hook_path(repo: Path) -> Path:
    git_dir = _local_git_ok(repo, ["rev-parse", "--git-path", "hooks/post-commit"]).stdout.strip()
    path = Path(git_dir)
    return path if path.is_absolute() else repo / path


def _hook_script(config: RemrunConfig, repo: Path, peers: list[str]) -> str:
    py = _shell_path(Path(sys.executable))
    remrun_root = _native_env_path(config.repo_root)
    src_root = _native_env_path(config.repo_root / "src")
    state_root = _native_env_path(default_state_root())
    log_path = _hook_log_path(config, _project_id_for_repo(config, repo))
    log_shell = _shell_path(log_path)
    log_dir_shell = _shell_path(log_path.parent)
    repo_root = _shell_path(repo)
    peer_lines = "\n".join(
        f"  {shlex.quote(py)} -m remrun.cli git-sync {shlex.quote(peer)} "
        f"--push --quiet >> {shlex.quote(log_shell)} 2>&1 || true"
        for peer in peers
    )
    return f"""#!/bin/sh
{HOOK_BEGIN}
# Generated by remrun. This hook starts background best-effort git-sync pushes.
(
  if [ -x "$0.remrun-backup" ]; then
    "$0.remrun-backup" "$@" >/dev/null 2>&1 || true
  fi
  cd {shlex.quote(repo_root)} || exit 0
  mkdir -p {shlex.quote(log_dir_shell)}
  printf '%s post-commit git-sync start\\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >> {shlex.quote(log_shell)}
  export REMRUN_ROOT={shlex.quote(remrun_root)}
  export REMRUN_STATE_ROOT={shlex.quote(state_root)}
  export PYTHONPATH={shlex.quote(src_root)}
{peer_lines}
  if command -v tail >/dev/null 2>&1; then
    tail -c 65536 {shlex.quote(log_shell)} > {shlex.quote(log_shell)}.tmp 2>/dev/null \\
      && mv {shlex.quote(log_shell)}.tmp {shlex.quote(log_shell)}
  fi
) >/dev/null 2>&1 &
exit 0
{HOOK_END}
"""


def _shell_path(path: Path) -> str:
    text = str(path.resolve()).replace("\\", "/")
    match = re.match(r"^([A-Za-z]):/(.*)$", text)
    if match:
        return f"/{match.group(1).lower()}/{match.group(2)}"
    return text


def _native_env_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/")


def _hook_log_path(config: RemrunConfig, project_id: str) -> Path:
    return default_state_root() / "logs" / "gitsync-hook" / f"{_ref_namespace(project_id)}.log"


def _project_id_for_repo(config: RemrunConfig, repo: Path) -> str:
    try:
        project = detect_project(repo, config)
        return project.project_id
    except ProjectDetectionError:
        return repo.name


def _line_endings_ok(repo: Path) -> bool:
    attrs = repo / ".gitattributes"
    text = attrs.read_text(encoding="utf-8", errors="replace") if attrs.exists() else ""
    return "eol=lf" in text


def _warn_line_endings(repo: Path, config: RemrunConfig, reporter: Reporter) -> None:
    oses = {d.os.lower() for d in config.devices.values() if d.enabled}
    cross_platform = any("win" in os_name for os_name in oses) and any(
        ("mac" in os_name or "darwin" in os_name or "posix" in os_name) for os_name in oses)
    if not cross_platform:
        return
    if not _line_endings_ok(repo):
        reporter.event("git_sync_warning", kind="line_endings",
                       message="cross-platform repo lacks `.gitattributes` with eol=lf; "
                               "Windows autocrlf can dirty tracked files on peers")


def _status_branches(repo: Path, peer_ns: str, branch: str | None) -> list[BranchAction]:
    actions: list[BranchAction] = []
    for name in _branches_local(repo, branch):
        local_ref = f"refs/heads/{name}"
        peer_ref = f"refs/remotes/{peer_ns}/{name}"
        if not _local_ref_exists(repo, peer_ref):
            actions.append(BranchAction(name, "missing_peer_ref", detail=peer_ref))
            continue
        action = _classify_local(repo, name, local_ref, peer_ref)
        state = {"behind": "would_fast_forward"}.get(action.state, action.state)
        actions.append(BranchAction(name, state, action.old, action.new, action.detail))
    return actions


def _bootstrap_status_branches(
    repo: Path,
    peer_ns: str,
    branch: str | None,
) -> list[BranchAction]:
    prefix = f"refs/remotes/{peer_ns}/"
    result = _local_git_ok(
        repo,
        ["for-each-ref", "--format=%(refname) %(objectname)", prefix],
    )
    actions: list[BranchAction] = []
    for line in result.stdout.splitlines():
        ref, head = line.split(" ", 1)
        name = ref.removeprefix(prefix)
        if branch and name != branch:
            continue
        actions.append(BranchAction(
            name,
            "bootstrap_available",
            new=head,
            detail="local project has no Git history; --pull would bootstrap from this peer",
        ))
    return actions


def _validated_branch(repo: Path, branch: str | None) -> str | None:
    """Validate an explicitly supplied branch before any transport or bundle work."""
    if branch is None:
        return None
    if not isinstance(branch, str) or not branch:
        raise GitSyncError(
            "invalid git-sync branch: expected a non-empty branch name", EXIT_INTERNAL
        )
    ref = f"refs/heads/{branch}"
    result = _local_git(repo, ["check-ref-format", ref])
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        suffix = f": {detail}" if detail else ""
        raise GitSyncError(f"invalid git-sync branch {branch!r}{suffix}", EXIT_INTERNAL)
    return branch


def _local_ref_exists(repo: Path, ref: str) -> bool:
    result = _local_git(repo, ["show-ref", "--verify", "--quiet", ref])
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False
    detail = (result.stderr or result.stdout).strip()
    raise GitSyncError(f"git show-ref {ref} failed: {detail}", EXIT_TRANSFER)


def _remote_ref_exists(transport: BaseTransport, remote_root: str, ref: str) -> bool:
    result = _remote_git(transport, remote_root, ["show-ref", "--verify", "--quiet", ref])
    if result.exit_code == 0:
        return True
    if result.exit_code == 1:
        return False
    detail = (result.stderr or result.stdout).strip()
    raise GitSyncError(f"remote git show-ref {ref} failed: {detail}", EXIT_TRANSFER)


def _local_has_tags(repo: Path) -> bool:
    result = _local_git_ok(
        repo, ["for-each-ref", "--count=1", "--format=%(refname)", "refs/tags"]
    )
    return bool(result.stdout.strip())


def _remote_has_tags(transport: BaseTransport, remote_root: str) -> bool:
    result = _remote_git_ok(
        transport, remote_root,
        ["for-each-ref", "--count=1", "--format=%(refname)", "refs/tags"],
    )
    return bool(result.stdout.strip())


def _missing_local_branch_actions(
    repo: Path, peer_ns: str, branch: str, *, status: bool,
    local_history_present: bool = True,
) -> list[BranchAction]:
    if not local_history_present or not _local_ref_exists(repo, f"refs/heads/{branch}"):
        return []
    state = "missing_peer_ref" if status else "skipped_missing_peer_ref"
    return [BranchAction(branch, state, detail=f"refs/remotes/{peer_ns}/{branch}")]


def _missing_remote_branch_actions(
    transport: BaseTransport, remote_root: str, local_ns: str, branch: str,
) -> list[BranchAction]:
    if not _remote_ref_exists(transport, remote_root, f"refs/heads/{branch}"):
        return []
    return [BranchAction(
        branch, "skipped_missing_peer_ref",
        detail=f"refs/remotes/{local_ns}/{branch}",
    )]


def _pull_tags_only(
    transport: BaseTransport, remote_root: str, local_root: Path,
) -> None:
    if not _remote_has_tags(transport, remote_root):
        return
    with tempfile.TemporaryDirectory(prefix="remrun-gitsync-tags-") as td:
        local_bundle = Path(td) / "peer-tags.bundle"
        remote_tmp = transport.remote_temp_dir("remrun-gitsync-tags")
        remote_bundle = transport.native_join(remote_tmp, "peer-tags.bundle")
        try:
            _remote_git_ok(
                transport, remote_root, ["bundle", "create", remote_bundle, "--tags"]
            )
            transport.pull_file(remote_bundle, local_bundle)
        finally:
            transport.remove_remote_tree(remote_tmp)
        _local_git_ok(
            local_root, ["fetch", str(local_bundle), "refs/tags/*:refs/tags/*"]
        )


def _push_tags_only(
    transport: BaseTransport, remote_root: str, local_root: Path,
) -> None:
    if not _local_has_tags(local_root):
        return
    with tempfile.TemporaryDirectory(prefix="remrun-gitsync-tags-") as td:
        local_bundle = Path(td) / "local-tags.bundle"
        _local_git_ok(local_root, ["bundle", "create", str(local_bundle), "--tags"])
        remote_tmp = transport.remote_temp_dir("remrun-gitsync-tags")
        remote_bundle = transport.native_join(remote_tmp, "local-tags.bundle")
        try:
            transport.push_file(local_bundle, remote_bundle)
            _remote_git_ok(
                transport, remote_root,
                ["fetch", remote_bundle, "refs/tags/*:refs/tags/*"],
            )
        finally:
            transport.remove_remote_tree(remote_tmp)


def _bundle_selection(branch: str | None) -> list[str]:
    """Select all tags plus the explicitly named branch when one was requested."""
    if branch:
        return ["--tags", f"refs/heads/{branch}"]
    return ["--branches", "--tags"]


def _fetch_refspec(namespace: str, branch: str | None) -> str:
    if branch:
        return f"+refs/heads/{branch}:refs/remotes/{namespace}/{branch}"
    return f"+refs/heads/*:refs/remotes/{namespace}/*"


def _pull_from_peer(
    transport: BaseTransport,
    remote_root: str,
    local_root: Path,
    peer_ns: str,
    branch: str | None,
    reporter: Reporter,
    *,
    advance_dirty_worktree: bool = False,
) -> list[BranchAction]:
    if branch is not None and not _remote_ref_exists(
        transport, remote_root, f"refs/heads/{branch}"
    ):
        _pull_tags_only(transport, remote_root, local_root)
        actions = _missing_local_branch_actions(
            local_root, peer_ns, branch, status=False
        )
        _emit_actions(reporter, "git_sync_pull", actions)
        return actions
    with tempfile.TemporaryDirectory(prefix="remrun-gitsync-") as td:
        local_bundle = Path(td) / "peer.bundle"
        remote_tmp = transport.remote_temp_dir("remrun-gitsync")
        remote_bundle = transport.native_join(remote_tmp, "peer.bundle")
        try:
            _remote_git_ok(
                transport, remote_root,
                ["bundle", "create", remote_bundle, *_bundle_selection(branch)],
            )
            transport.pull_file(remote_bundle, local_bundle)
        finally:
            transport.remove_remote_tree(remote_tmp)
        _local_git_ok(
            local_root,
            ["fetch", "--tags", str(local_bundle), _fetch_refspec(peer_ns, branch)],
        )
    actions = _fast_forward_local(
        local_root, peer_ns, branch,
        advance_dirty_worktree=advance_dirty_worktree)
    _emit_actions(reporter, "git_sync_pull", actions)
    return actions


def _push_to_peer(
    transport: BaseTransport,
    remote_root: str,
    local_root: Path,
    local_ns: str,
    branch: str | None,
    reporter: Reporter,
    *,
    advance_dirty_worktree: bool = False,
) -> list[BranchAction]:
    if branch is not None and not _local_ref_exists(local_root, f"refs/heads/{branch}"):
        _push_tags_only(transport, remote_root, local_root)
        actions = _missing_remote_branch_actions(
            transport, remote_root, local_ns, branch
        )
        _emit_actions(reporter, "git_sync_push", actions)
        return actions
    with tempfile.TemporaryDirectory(prefix="remrun-gitsync-") as td:
        local_bundle = Path(td) / "local.bundle"
        _local_git_ok(
            local_root,
            ["bundle", "create", str(local_bundle), *_bundle_selection(branch)],
        )
        remote_tmp = transport.remote_temp_dir("remrun-gitsync")
        remote_bundle = transport.native_join(remote_tmp, "local.bundle")
        try:
            transport.push_file(local_bundle, remote_bundle)
            _remote_git_ok(
                transport, remote_root,
                ["fetch", "--tags", remote_bundle, _fetch_refspec(local_ns, branch)],
            )
            actions = _fast_forward_remote(
                transport, remote_root, local_ns, branch,
                advance_dirty_worktree=advance_dirty_worktree)
        finally:
            transport.remove_remote_tree(remote_tmp)
    _emit_actions(reporter, "git_sync_push", actions)
    return actions


def _emit_actions(reporter: Reporter, event: str, actions: list[BranchAction]) -> None:
    for action in actions:
        reporter.event(event, branch=action.branch, state=action.state,
                       old=action.old, new=action.new, detail=action.detail)


def _is_git_repo(repo: Path) -> bool:
    # True only when `repo` itself is the top of a work tree, not merely inside
    # one (a repo-less project nested under an unrelated parent repo must still
    # read as repo-less).
    if not (repo / ".git").exists():
        return False
    res = _local_git(repo, ["rev-parse", "--is-inside-work-tree"])
    return res.returncode == 0 and res.stdout.strip() == "true"


def _ensure_local_git_repo(repo: Path) -> None:
    if not _is_git_repo(repo):
        raise GitSyncError(f"not a git repo: {repo}", EXIT_INFRA)


def _bootstrap_from_peer(
    config: RemrunConfig,
    device_name: str,
    project,
    *,
    dry_run: bool,
    reporter: Reporter,
    remote_memory_limit_mib: int | None,
    branch: str | None,
) -> GitSyncResult:
    """Initialize a repo-less project from a peer's authoritative history.

    Never touches the working tree: `git init`, fetch full history over the
    remrun transport, point the local branch at the peer's HEAD with
    update-ref/symbolic-ref, and `git reset --mixed` (index only). On any
    failure after `git init`, the half-created `.git` is removed.
    """
    local_root = project.local_project_root
    device = config.devices[device_name]

    transport = make_transport(device)
    probe = transport.probe()
    if not probe.reachable:
        raise GitSyncError(f"{device_name} unreachable: {probe.detail}", EXIT_INFRA)
    remote_root = transport.remote_project_path(project)
    metadata_transport = _bounded_metadata_transport(
        transport, remote_memory_limit_mib
    )
    _ensure_remote_git_repo(metadata_transport, remote_root)

    if branch is not None:
        branch_ref = f"refs/heads/{branch}"
        if not _remote_ref_exists(metadata_transport, remote_root, branch_ref):
            raise GitSyncError(
                f"peer {device_name} has no branch {branch!r}", EXIT_INFRA,
            )
        peer_branch = branch
        peer_head = _remote_git_ok(
            metadata_transport, remote_root, ["rev-parse", branch_ref]
        ).stdout.strip()
    else:
        # Without an explicit branch, peer HEAD is the bootstrap authority.
        if _remote_git(
            metadata_transport, remote_root,
            ["rev-parse", "--verify", "--quiet", "HEAD"],
        ).exit_code != 0:
            raise GitSyncError(
                f"peer {device_name} repo is empty (no commits to bootstrap from): {remote_root}",
                EXIT_INFRA,
            )
        peer_head = _remote_git_ok(
            metadata_transport, remote_root, ["rev-parse", "HEAD"]
        ).stdout.strip()
        peer_branch = _current_branch_remote(metadata_transport, remote_root) or "main"
    peer_ns = _ref_namespace(device_name)

    reporter.event(
        "git_sync_bootstrap_start",
        device=device_name,
        local_project=str(local_root),
        remote_project=remote_root,
        branch=peer_branch,
        head=peer_head,
        dry_run=dry_run,
        message="peer discovered; bootstrap has not completed yet",
    )
    if dry_run:
        reporter.event("git_sync_bootstrap_dry_run",
                       action=f"would init {local_root} and fetch {peer_branch}@{peer_head[:12]} "
                              "from peer; working tree untouched")
        return GitSyncResult(
            device=device_name, direction="pull", local_project=str(local_root),
            remote_project=remote_root, dry_run=True,
            bootstrap=GitSyncBootstrap(
                device=device_name, local_project=str(local_root), remote_project=remote_root,
                branch=peer_branch, head=peer_head, commits_fetched=0, modified=0, untracked=0))

    operation_transport = _repository_scaled_transport(
        transport, remote_memory_limit_mib, operation="bootstrap"
    )

    git_dir = local_root / ".git"
    created_git_dir = not git_dir.exists()
    _local_git_ok(local_root, ["init", "-q"])
    try:
        _local_git_ok(local_root, ["config", "core.autocrlf", "false"])
        if os.name == "nt":
            # Without longpaths, deep artifact paths >260 chars show up as
            # hundreds of phantom modifications on Windows.
            _local_git_ok(local_root, ["config", "core.longpaths", "true"])

        with tempfile.TemporaryDirectory(prefix="remrun-gitsync-boot-") as td:
            local_bundle = Path(td) / "peer.bundle"
            remote_tmp = operation_transport.remote_temp_dir("remrun-gitsync-boot")
            remote_bundle = operation_transport.native_join(remote_tmp, "peer.bundle")
            try:
                _remote_git_ok(
                    operation_transport, remote_root,
                    ["bundle", "create", remote_bundle, *_bundle_selection(branch)],
                )
                operation_transport.pull_file(remote_bundle, local_bundle)
            finally:
                operation_transport.remove_remote_tree(remote_tmp)
            if not local_bundle.is_file() or local_bundle.stat().st_size == 0:
                raise GitSyncError(
                    "peer bundle pull completed without a non-empty local bundle",
                    EXIT_TRANSFER,
                )
            _local_git_ok(local_root, ["bundle", "verify", str(local_bundle)])
            _local_git_ok(
                local_root,
                ["fetch", "--tags", str(local_bundle), _fetch_refspec(peer_ns, branch)],
            )

        # Do not trust the fetch exit code alone: require the discovered peer
        # commit and checked-out branch ref to have arrived before creating heads.
        _local_git_ok(local_root, ["cat-file", "-e", f"{peer_head}^{{commit}}"])
        peer_branch_ref = f"refs/remotes/{peer_ns}/{peer_branch}"
        fetched_peer_head = _local_git_ok(
            local_root, ["rev-parse", "--verify", peer_branch_ref]).stdout.strip()
        if fetched_peer_head != peer_head:
            raise GitSyncError(
                f"peer branch verification failed: {peer_branch_ref} is "
                f"{fetched_peer_head or '<missing>'}, expected {peer_head}",
                EXIT_TRANSFER,
            )

        # Recreate a local head for every fetched peer branch (a clone-like layout) so
        # `git branch`/`git log` work, then point HEAD at the peer's checked-out
        # branch without disturbing the working tree.
        for peer_ref in _local_git_ok(
                local_root,
                ["for-each-ref", "--format=%(refname:short)",
                 f"refs/remotes/{peer_ns}"]).stdout.splitlines():
            peer_ref = peer_ref.strip()
            if not peer_ref:
                continue
            name = peer_ref[len(peer_ns) + 1:] if peer_ref.startswith(peer_ns + "/") else peer_ref
            if name == "HEAD":
                continue
            sha = _local_git_ok(local_root, ["rev-parse", peer_ref]).stdout.strip()
            _local_git_ok(local_root, ["update-ref", f"refs/heads/{name}", sha])

        _local_git_ok(local_root, ["update-ref", f"refs/heads/{peer_branch}", peer_head])
        _local_git_ok(local_root, ["symbolic-ref", "HEAD", f"refs/heads/{peer_branch}"])
        # --mixed refreshes the index to match HEAD; the working tree is left as-is.
        _local_git_ok(local_root, ["reset", "--mixed", "-q"])
        installed_head = _local_git_ok(local_root, ["rev-parse", "HEAD"]).stdout.strip()
        installed_branch = _local_git_ok(
            local_root, ["symbolic-ref", "--short", "HEAD"]).stdout.strip()
        if installed_head != peer_head or installed_branch != peer_branch:
            raise GitSyncError(
                "bootstrap postcondition failed: "
                f"HEAD={installed_head or '<missing>'} branch={installed_branch or '<missing>'}; "
                f"expected {peer_head} on {peer_branch}",
                EXIT_TRANSFER,
            )

        hooks_path_set = False
        if (local_root / ".githooks").is_dir():
            _local_git_ok(local_root, ["config", "core.hooksPath", ".githooks"])
            hooks_path_set = True

        commits_fetched = int(_local_git_ok(
            local_root, ["rev-list", "--count", "--all"]).stdout.strip() or "0")
        if commits_fetched < 1:
            raise GitSyncError(
                "bootstrap postcondition failed: no local commits were installed",
                EXIT_TRANSFER,
            )
        modified, untracked = _worktree_counts(local_root)
    except BaseException:
        # Leave no half-initialized repo behind when this invocation created it,
        # including on Ctrl-C. Preserve a pre-existing empty repo so recovery is
        # retryable without deleting metadata that remrun did not create.
        if created_git_dir:
            shutil.rmtree(git_dir, ignore_errors=True)
        raise

    boot = GitSyncBootstrap(
        device=device_name, local_project=str(local_root), remote_project=remote_root,
        branch=peer_branch, head=peer_head, commits_fetched=commits_fetched,
        modified=modified, untracked=untracked, hooks_path_set=hooks_path_set)
    reporter.event("git_sync_bootstrap_done", device=device_name, branch=peer_branch,
                   head=peer_head, commits_fetched=commits_fetched, modified=modified,
                   untracked=untracked, hooks_path_set=hooks_path_set,
                   message=(f"created repo; {commits_fetched} commits fetched; HEAD set to "
                            f"{peer_head[:12]} ({peer_branch}); working tree untouched; "
                            f"{modified} modified / {untracked} untracked vs HEAD"))
    return GitSyncResult(
        device=device_name, direction="pull", local_project=str(local_root),
        remote_project=remote_root, bootstrap=boot)


def _worktree_counts(repo: Path) -> tuple[int, int]:
    res = _local_git_ok(repo, ["status", "--porcelain", "--untracked-files=all"])
    modified = untracked = 0
    for line in res.stdout.splitlines():
        if not line:
            continue
        if line.startswith("??"):
            untracked += 1
        else:
            modified += 1
    return modified, untracked


def _ensure_remote_git_repo(transport: BaseTransport, remote_root: str) -> None:
    """Require ``remote_root`` itself to be a healthy worktree root.

    ``--show-prefix`` is empty only at the repository top level. Pairing it with
    ``--show-toplevel`` gives a two-line Git-owned suffix while tolerating text emitted
    *before* the command by a login-shell profile. Failures retain exact probe evidence.
    """
    argv = ["git", "rev-parse", "--show-prefix", "--show-toplevel"]
    res = transport.exec(argv, cwd=remote_root)
    reason: str | None = None
    if res.exit_code != 0:
        reason = "git command failed"
    else:
        lines = res.stdout.splitlines()
        if len(lines) < 2 or not lines[-1]:
            reason = "git returned unexpected output"
        elif lines[-2]:
            reason = (
                "mapped cwd is inside a parent repository "
                f"(prefix={lines[-2]!r}, top_level={lines[-1]!r})"
            )
    if reason is None:
        return
    raise GitSyncError(
        "remote git repository probe failed: "
        f"reason={reason}; cwd={json.dumps(remote_root)}; "
        f"argv={json.dumps(argv)}; exit_code={res.exit_code}; "
        f"stdout_raw={json.dumps(res.stdout)}; stderr_raw={json.dumps(res.stderr)}",
        EXIT_INFRA,
    )


def _local_git(repo: Path, args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=str(repo), text=True, capture_output=True,
                          check=False)


def _local_git_ok(repo: Path, args: list[str]) -> subprocess.CompletedProcess:
    res = _local_git(repo, args)
    if res.returncode != 0:
        msg = (res.stderr or res.stdout).strip()
        raise GitSyncError(f"git {' '.join(args)} failed: {msg}", EXIT_TRANSFER)
    return res


def _remote_git(transport: BaseTransport, remote_root: str, args: list[str]):
    result = transport.exec(["git", *args], cwd=remote_root)
    guard = result.memory_guard
    if isinstance(guard, dict) and guard.get("status") != "ok":
        status = str(guard.get("status") or "unknown")
        reason = str(guard.get("reason") or "unknown")
        detail = str(guard.get("detail") or "")
        command_started = guard.get("command_started")
        started_text = (
            "true" if command_started is True
            else "false" if command_started is False
            else "unknown"
        )
        raise GitSyncError(
            f"remote git {' '.join(args)} memory guard {status}: {reason}: {detail}; "
            f"command_started={started_text}; command was not retried",
            EXIT_TRANSFER,
        )
    return result


def _remote_git_ok(transport: BaseTransport, remote_root: str, args: list[str]):
    res = _remote_git(transport, remote_root, args)
    if res.exit_code != 0:
        msg = (res.stderr or res.stdout).strip()
        raise GitSyncError(f"remote git {' '.join(args)} failed: {msg}", EXIT_TRANSFER)
    return res


def _branches_local(repo: Path, branch: str | None) -> list[str]:
    if branch is not None:
        if _local_ref_exists(repo, f"refs/heads/{branch}"):
            return [branch]
        return []
    res = _local_git_ok(repo, ["for-each-ref", "--format=%(refname:short)", "refs/heads"])
    return [b for b in res.stdout.splitlines() if b]


def _branches_remote(transport: BaseTransport, remote_root: str, branch: str | None) -> list[str]:
    if branch is not None:
        if _remote_ref_exists(transport, remote_root, f"refs/heads/{branch}"):
            return [branch]
        return []
    res = _remote_git_ok(transport, remote_root,
                         ["for-each-ref", "--format=%(refname:short)", "refs/heads"])
    return [b for b in res.stdout.splitlines() if b]


def _fast_forward_local(
    repo: Path,
    peer_ns: str,
    branch: str | None,
    *,
    advance_dirty_worktree: bool = False,
) -> list[BranchAction]:
    current = _current_branch_local(repo)
    dirty_summary = _dirty_summary_local(repo)
    dirty = dirty_summary.dirty
    actions: list[BranchAction] = []
    for name in _branches_local(repo, branch):
        local_ref = f"refs/heads/{name}"
        peer_ref = f"refs/remotes/{peer_ns}/{name}"
        if not _local_ref_exists(repo, peer_ref):
            actions.append(BranchAction(name, "skipped_missing_peer_ref", detail=peer_ref))
            continue
        action = _classify_local(repo, name, local_ref, peer_ref)
        if action.state != "behind":
            actions.append(action)
            continue
        if name == current:
            if dirty:
                if advance_dirty_worktree:
                    # Synced-worktree mode: move HEAD+index to the proved fast-forward
                    # target while preserving every worktree byte. Any edits or bytes
                    # not yet delivered by Syncthing remain visible as dirty.
                    _local_git_ok(repo, ["reset", "--mixed", "-q", peer_ref])
                    actions.append(BranchAction(
                        name, "fast_forwarded_worktree_preserved", action.old, action.new,
                        "history advanced; index refreshed; worktree bytes untouched; "
                        f"local worktree remains dirty: {dirty_summary.describe()}; "
                        "do not treat it as a clean checkout"))
                    continue
                actions.append(BranchAction(name, "skipped_dirty_worktree",
                                            action.old, action.new,
                                            "fetch completed; merge manually after cleaning tree"))
                continue
            _local_git_ok(repo, ["merge", "--ff-only", peer_ref])
        else:
            _local_git_ok(repo, ["update-ref", local_ref, action.new or "", action.old or ""])
        actions.append(BranchAction(name, "fast_forwarded", action.old, action.new))
    return actions


def _fast_forward_remote(
    transport: BaseTransport,
    remote_root: str,
    peer_ns: str,
    branch: str | None,
    *,
    advance_dirty_worktree: bool = False,
) -> list[BranchAction]:
    current = _current_branch_remote(transport, remote_root)
    dirty_summary = _dirty_summary_remote(transport, remote_root)
    dirty = dirty_summary.dirty
    actions: list[BranchAction] = []
    for name in _branches_remote(transport, remote_root, branch):
        local_ref = f"refs/heads/{name}"
        peer_ref = f"refs/remotes/{peer_ns}/{name}"
        if not _remote_ref_exists(transport, remote_root, peer_ref):
            actions.append(BranchAction(name, "skipped_missing_peer_ref", detail=peer_ref))
            continue
        action = _classify_remote(transport, remote_root, name, local_ref, peer_ref)
        if action.state != "behind":
            actions.append(action)
            continue
        if name == current:
            if dirty:
                if advance_dirty_worktree:
                    # Hub mode: move HEAD+index to the already-proved fast-forward target
                    # while preserving every worktree byte. This is intentionally `--mixed`,
                    # never checkout/reset-hard/clean. Syncthing-delivered bytes can converge
                    # later; any genuine edit stays visible as dirty rather than being lost.
                    _remote_git_ok(transport, remote_root,
                                   ["reset", "--mixed", "-q", peer_ref])
                    actions.append(BranchAction(
                        name, "fast_forwarded_worktree_preserved", action.old, action.new,
                        "history advanced; index refreshed; worktree bytes untouched; "
                        f"remote worktree remains dirty: {dirty_summary.describe()}; "
                        "do not treat it as a clean checkout"))
                    continue
                actions.append(BranchAction(name, "skipped_dirty_worktree",
                                            action.old, action.new,
                                            "fetch completed; merge manually after cleaning tree"))
                continue
            _remote_git_ok(transport, remote_root, ["merge", "--ff-only", peer_ref])
        else:
            _remote_git_ok(transport, remote_root,
                           ["update-ref", local_ref, action.new or "", action.old or ""])
        actions.append(BranchAction(name, "fast_forwarded", action.old, action.new))
    return actions


def _classify_local(repo: Path, branch: str, local_ref: str, peer_ref: str) -> BranchAction:
    old = _local_git_ok(repo, ["rev-parse", local_ref]).stdout.strip()
    new = _local_git_ok(repo, ["rev-parse", peer_ref]).stdout.strip()
    if old == new:
        return BranchAction(branch, "up_to_date", old, new)
    if _local_git(repo, ["merge-base", "--is-ancestor", local_ref, peer_ref]).returncode == 0:
        return BranchAction(branch, "behind", old, new)
    if _local_git(repo, ["merge-base", "--is-ancestor", peer_ref, local_ref]).returncode == 0:
        return BranchAction(branch, "ahead", old, new)
    return BranchAction(branch, "diverged", old, new, f"merge from {peer_ref} manually")


def _classify_remote(
    transport: BaseTransport,
    remote_root: str,
    branch: str,
    local_ref: str,
    peer_ref: str,
) -> BranchAction:
    old = _remote_git_ok(transport, remote_root, ["rev-parse", local_ref]).stdout.strip()
    new = _remote_git_ok(transport, remote_root, ["rev-parse", peer_ref]).stdout.strip()
    if old == new:
        return BranchAction(branch, "up_to_date", old, new)
    if _remote_git(transport, remote_root, ["merge-base", "--is-ancestor",
                                           local_ref, peer_ref]).exit_code == 0:
        return BranchAction(branch, "behind", old, new)
    if _remote_git(transport, remote_root, ["merge-base", "--is-ancestor",
                                           peer_ref, local_ref]).exit_code == 0:
        return BranchAction(branch, "ahead", old, new)
    return BranchAction(branch, "diverged", old, new, f"merge from {peer_ref} manually")


def _current_branch_local(repo: Path) -> str | None:
    res = _local_git(repo, ["symbolic-ref", "--quiet", "--short", "HEAD"])
    return res.stdout.strip() or None if res.returncode == 0 else None


def _current_branch_remote(transport: BaseTransport, remote_root: str) -> str | None:
    res = _remote_git(transport, remote_root, ["symbolic-ref", "--quiet", "--short", "HEAD"])
    return res.stdout.strip() or None if res.exit_code == 0 else None


def _dirty_local(repo: Path) -> bool:
    # Only tracked edits should stop an otherwise-safe fast-forward. Untracked
    # platform litter such as `.DS_Store` should not strand peer commits; Git
    # itself will still refuse the merge if an untracked path would be overwritten.
    return _local_git(repo, ["diff", "--quiet", "HEAD", "--"]).returncode != 0


def _dirty_remote(transport: BaseTransport, remote_root: str) -> bool:
    return _remote_git(transport, remote_root, ["diff", "--quiet", "HEAD", "--"]).exit_code != 0


def _parse_dirty_summary(status: str, numstat: str, diff_summary: str) -> WorktreeDirtySummary:
    tracked = untracked = 0
    for line in status.splitlines():
        if not line:
            continue
        if line.startswith("??"):
            untracked += 1
        else:
            tracked += 1

    # A pure mode change appears as 0/0 in --numstat and as a mode-change line in
    # --summary. Requiring both avoids misclassifying a newly added empty file.
    zero_line_diffs = 0
    for line in numstat.splitlines():
        fields = line.split("\t", 2)
        if len(fields) >= 2 and fields[0] == "0" and fields[1] == "0":
            zero_line_diffs += 1
    mode_changes = sum(1 for line in diff_summary.splitlines()
                       if line.strip().startswith("mode change "))
    mode_only = min(tracked, zero_line_diffs, mode_changes)
    return WorktreeDirtySummary(
        tracked=tracked,
        content=max(0, tracked - mode_only),
        mode_only=mode_only,
        untracked=untracked,
    )


def _dirty_summary_local(repo: Path) -> WorktreeDirtySummary:
    status = _local_git_ok(repo, ["status", "--porcelain", "--untracked-files=all"]).stdout
    numstat = _local_git_ok(repo, ["diff", "--numstat", "HEAD", "--"]).stdout
    summary = _local_git_ok(repo, ["diff", "--summary", "HEAD", "--"]).stdout
    return _parse_dirty_summary(status, numstat, summary)


def _dirty_summary_remote(
    transport: BaseTransport,
    remote_root: str,
) -> WorktreeDirtySummary:
    status = _remote_git_ok(
        transport, remote_root, ["status", "--porcelain", "--untracked-files=all"]
    ).stdout
    numstat = _remote_git_ok(
        transport, remote_root, ["diff", "--numstat", "HEAD", "--"]
    ).stdout
    summary = _remote_git_ok(
        transport, remote_root, ["diff", "--summary", "HEAD", "--"]
    ).stdout
    return _parse_dirty_summary(status, numstat, summary)


def _ref_namespace(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    cleaned = cleaned.strip(".-/")
    return cleaned or "LOCAL"
