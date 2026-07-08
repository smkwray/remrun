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
from dataclasses import dataclass, field
from pathlib import Path

from .config import RemrunConfig, load_project_config
from .output import Reporter
from .project import ProjectDetectionError, detect_project, find_project_config
from .state import default_state_root
from .transport import BaseTransport, TransportError, make_transport

EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_DIVERGED = 2
EXIT_TRANSFER = 3
EXIT_INFRA = 4
HOOK_BEGIN = "# >>> remrun git-sync hook >>>"
HOOK_END = "# <<< remrun git-sync hook <<<"


class GitSyncError(RuntimeError):
    def __init__(self, message: str, exit_code: int = EXIT_INTERNAL) -> None:
        super().__init__(message)
        self.exit_code = exit_code


@dataclass
class BranchAction:
    branch: str
    state: str
    old: str | None = None
    new: str | None = None
    detail: str = ""


@dataclass
class GitSyncBootstrap:
    """Result of initializing a repo-less project from a peer's history.

    The working tree is left byte-for-byte untouched: history is fetched, the
    local branch is pointed at the peer's HEAD via update-ref/symbolic-ref, and
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
    local_dirty: bool
    remote_dirty: bool
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
            "local_dirty": self.local_dirty,
            "remote_dirty": self.remote_dirty,
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
    reporter: Reporter | None = None,
    as_json: bool = False,
) -> int:
    reporter = reporter or Reporter(json_events=as_json)
    try:
        result = run_git_sync_result(
            config,
            device_name=device_name,
            direction=direction,
            dry_run=dry_run,
            branch=branch,
            bootstrap=bootstrap,
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
    reporter: Reporter | None = None,
    as_json: bool = False,
) -> int:
    reporter = reporter or Reporter(json_events=as_json)
    try:
        status = git_sync_status_result(
            config, device_name=device_name, branch=branch, reporter=reporter)
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
        project, project_config = _detect_git_project(config)
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
        project, _project_config = _detect_git_project(config)
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
    reporter: Reporter | None = None,
) -> GitSyncResult:
    reporter = reporter or Reporter()
    direction = direction.lower()
    if direction not in {"pull", "push", "both"}:
        raise GitSyncError(f"invalid git-sync direction: {direction}", EXIT_INTERNAL)
    if device_name not in config.devices:
        raise GitSyncError(f"unknown device: {device_name}", EXIT_INTERNAL)
    device = config.devices[device_name]
    if not device.enabled:
        raise GitSyncError(f"device disabled: {device_name}", EXIT_INFRA)

    project, _project_config = _detect_git_project(config, require_git=False)
    local_root = project.local_project_root

    if not _is_git_repo(local_root):
        # Repo-less project: the norm for a Syncthing-synced tree that arrives on
        # a fresh device with `.git` excluded. Bootstrap from the peer's history.
        if bootstrap or direction in {"pull", "both"}:
            return _bootstrap_from_peer(
                config, device_name, project, dry_run=dry_run, reporter=reporter)
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
    _ensure_remote_git_repo(transport, remote_root)

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

    if direction in {"pull", "both"}:
        result.pulled.extend(_pull_from_peer(
            transport, remote_root, local_root, peer_ns, branch, reporter))
    if direction in {"push", "both"}:
        peer_actions = _push_to_peer(
            transport, remote_root, local_root, local_ns, branch, reporter)
        result.pushed.extend(peer_actions)

    result.diverged.extend([a for a in result.pulled + result.pushed if a.state == "diverged"])
    result.skipped.extend([a for a in result.pulled + result.pushed if a.state.startswith("skipped")])
    reporter.event("git_sync_summary", exit_code=result.exit_code,
                   fast_forwarded=len([a for a in result.pulled + result.pushed
                                        if a.state == "fast_forwarded"]),
                   diverged=len(result.diverged),
                   skipped=len(result.skipped))
    return result


def git_sync_status_result(
    config: RemrunConfig,
    *,
    device_name: str,
    branch: str | None = None,
    reporter: Reporter | None = None,
) -> GitSyncStatus:
    reporter = reporter or Reporter()
    if device_name not in config.devices:
        raise GitSyncError(f"unknown device: {device_name}", EXIT_INTERNAL)
    device = config.devices[device_name]
    if not device.enabled:
        raise GitSyncError(f"device disabled: {device_name}", EXIT_INFRA)

    project, _project_config = _detect_git_project(config)
    local_root = project.local_project_root
    transport = make_transport(device)
    probe = transport.probe()
    if not probe.reachable:
        raise GitSyncError(f"{device_name} unreachable: {probe.detail}", EXIT_INFRA)
    remote_root = transport.remote_project_path(project)
    _ensure_remote_git_repo(transport, remote_root)

    peer_ns = _ref_namespace(device_name)
    with tempfile.TemporaryDirectory(prefix="remrun-gitsync-status-") as td:
        tmp_repo = Path(td) / "local.git"
        _local_git_ok(Path.cwd(), ["clone", "--bare", str(local_root), str(tmp_repo)])
        local_bundle = Path(td) / "peer.bundle"
        remote_tmp = transport.remote_temp_dir("remrun-gitsync-status")
        remote_bundle = transport.native_join(remote_tmp, "peer.bundle")
        try:
            _remote_git_ok(transport, remote_root, ["bundle", "create", remote_bundle,
                                                   "--branches", "--tags"])
            transport.pull_file(remote_bundle, local_bundle)
        finally:
            transport.remove_remote_tree(remote_tmp)
        _local_git_ok(tmp_repo, ["fetch", "--tags", str(local_bundle),
                                 f"+refs/heads/*:refs/remotes/{peer_ns}/*"])
        branches = _status_branches(tmp_repo, peer_ns, branch)

    local_dirty = _dirty_local(local_root)
    remote_dirty = _dirty_remote(transport, remote_root)
    hook = _hook_path(local_root)
    hook_installed = hook.exists() and HOOK_BEGIN in hook.read_text(encoding="utf-8",
                                                                    errors="replace")
    log_path = _hook_log_path(config, project.project_id)
    line_endings_ok = _line_endings_ok(local_root)
    for action in branches:
        reporter.event("git_sync_status", branch=action.branch, state=action.state,
                       old=action.old, new=action.new, detail=action.detail)
    reporter.event("git_sync_status_summary", device=device_name, local_dirty=local_dirty,
                   remote_dirty=remote_dirty, hook_installed=hook_installed,
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
        local_dirty=local_dirty,
        remote_dirty=remote_dirty,
        hook_installed=hook_installed,
        hook_log=str(log_path) if log_path.exists() else None,
        line_endings_ok=line_endings_ok,
    )


def _detect_git_project(config: RemrunConfig, *, require_git: bool = True):
    try:
        project = detect_project(Path.cwd(), config)
    except ProjectDetectionError as exc:
        raise GitSyncError(str(exc), EXIT_INFRA) from exc
    project_config = load_project_config(find_project_config(project.local_project_root))
    if require_git:
        _ensure_local_git_repo(project.local_project_root)
    return project, project_config


def _resolve_hook_peers(
    config: RemrunConfig,
    project_config: dict,
    device_name: str | None,
) -> list[str]:
    raw_peers = (project_config.get("git_sync", {}) or {}).get("peers", [])
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
        if _local_git(repo, ["show-ref", "--verify", "--quiet", peer_ref]).returncode != 0:
            actions.append(BranchAction(name, "missing_peer_ref", detail=peer_ref))
            continue
        action = _classify_local(repo, name, local_ref, peer_ref)
        state = {"behind": "would_fast_forward"}.get(action.state, action.state)
        actions.append(BranchAction(name, state, action.old, action.new, action.detail))
    return actions


def _pull_from_peer(
    transport: BaseTransport,
    remote_root: str,
    local_root: Path,
    peer_ns: str,
    branch: str | None,
    reporter: Reporter,
) -> list[BranchAction]:
    with tempfile.TemporaryDirectory(prefix="remrun-gitsync-") as td:
        local_bundle = Path(td) / "peer.bundle"
        remote_tmp = transport.remote_temp_dir("remrun-gitsync")
        remote_bundle = transport.native_join(remote_tmp, "peer.bundle")
        try:
            _remote_git_ok(transport, remote_root, ["bundle", "create", remote_bundle,
                                                   "--branches", "--tags"])
            transport.pull_file(remote_bundle, local_bundle)
        finally:
            transport.remove_remote_tree(remote_tmp)
        _local_git_ok(local_root, ["fetch", "--tags", str(local_bundle),
                                   f"+refs/heads/*:refs/remotes/{peer_ns}/*"])
    actions = _fast_forward_local(local_root, peer_ns, branch)
    _emit_actions(reporter, "git_sync_pull", actions)
    return actions


def _push_to_peer(
    transport: BaseTransport,
    remote_root: str,
    local_root: Path,
    local_ns: str,
    branch: str | None,
    reporter: Reporter,
) -> list[BranchAction]:
    with tempfile.TemporaryDirectory(prefix="remrun-gitsync-") as td:
        local_bundle = Path(td) / "local.bundle"
        _local_git_ok(local_root, ["bundle", "create", str(local_bundle), "--branches", "--tags"])
        remote_tmp = transport.remote_temp_dir("remrun-gitsync")
        remote_bundle = transport.native_join(remote_tmp, "local.bundle")
        try:
            transport.push_file(local_bundle, remote_bundle)
            _remote_git_ok(transport, remote_root, ["fetch", "--tags", remote_bundle,
                                                   f"+refs/heads/*:refs/remotes/{local_ns}/*"])
            actions = _fast_forward_remote(transport, remote_root, local_ns, branch)
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
    _ensure_remote_git_repo(transport, remote_root)

    # Peer must have at least one commit; an unborn/empty peer repo cannot seed us.
    if _remote_git(transport, remote_root, ["rev-parse", "--verify", "--quiet",
                                            "HEAD"]).exit_code != 0:
        raise GitSyncError(
            f"peer {device_name} repo is empty (no commits to bootstrap from): {remote_root}",
            EXIT_INFRA)

    peer_head = _remote_git_ok(transport, remote_root, ["rev-parse", "HEAD"]).stdout.strip()
    peer_branch = _current_branch_remote(transport, remote_root) or "main"
    peer_ns = _ref_namespace(device_name)

    reporter.event("git_sync_bootstrap", device=device_name, local_project=str(local_root),
                   remote_project=remote_root, branch=peer_branch, head=peer_head,
                   dry_run=dry_run)
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

    git_dir = local_root / ".git"
    _local_git_ok(local_root, ["init", "-q"])
    try:
        _local_git_ok(local_root, ["config", "core.autocrlf", "false"])
        if os.name == "nt":
            # Without longpaths, deep artifact paths >260 chars show up as
            # hundreds of phantom modifications on Windows.
            _local_git_ok(local_root, ["config", "core.longpaths", "true"])

        with tempfile.TemporaryDirectory(prefix="remrun-gitsync-boot-") as td:
            local_bundle = Path(td) / "peer.bundle"
            remote_tmp = transport.remote_temp_dir("remrun-gitsync-boot")
            remote_bundle = transport.native_join(remote_tmp, "peer.bundle")
            try:
                _remote_git_ok(transport, remote_root,
                               ["bundle", "create", remote_bundle, "--branches", "--tags"])
                transport.pull_file(remote_bundle, local_bundle)
            finally:
                transport.remove_remote_tree(remote_tmp)
            _local_git_ok(local_root, ["fetch", "--tags", str(local_bundle),
                                       f"+refs/heads/*:refs/remotes/{peer_ns}/*"])

        # Recreate a local head for every peer branch (a clone-like layout) so
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

        hooks_path_set = False
        if (local_root / ".githooks").is_dir():
            _local_git_ok(local_root, ["config", "core.hooksPath", ".githooks"])
            hooks_path_set = True

        commits_fetched = int(_local_git_ok(
            local_root, ["rev-list", "--count", "--all"]).stdout.strip() or "0")
        modified, untracked = _worktree_counts(local_root)
    except Exception:
        # Leave no half-initialized repo behind on failure.
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
    res = _remote_git(transport, remote_root, ["rev-parse", "--is-inside-work-tree"])
    if res.exit_code != 0 or res.stdout.strip() != "true":
        raise GitSyncError(f"remote path is not a git repo: {remote_root}", EXIT_INFRA)


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
    return transport.exec(["git", *args], cwd=remote_root)


def _remote_git_ok(transport: BaseTransport, remote_root: str, args: list[str]):
    res = _remote_git(transport, remote_root, args)
    if res.exit_code != 0:
        msg = (res.stderr or res.stdout).strip()
        raise GitSyncError(f"remote git {' '.join(args)} failed: {msg}", EXIT_TRANSFER)
    return res


def _branches_local(repo: Path, branch: str | None) -> list[str]:
    if branch:
        if _local_git(repo, ["show-ref", "--verify", "--quiet", f"refs/heads/{branch}"]).returncode == 0:
            return [branch]
        return []
    res = _local_git_ok(repo, ["for-each-ref", "--format=%(refname:short)", "refs/heads"])
    return [b for b in res.stdout.splitlines() if b]


def _branches_remote(transport: BaseTransport, remote_root: str, branch: str | None) -> list[str]:
    if branch:
        if _remote_git(transport, remote_root, ["show-ref", "--verify", "--quiet",
                                               f"refs/heads/{branch}"]).exit_code == 0:
            return [branch]
        return []
    res = _remote_git_ok(transport, remote_root,
                         ["for-each-ref", "--format=%(refname:short)", "refs/heads"])
    return [b for b in res.stdout.splitlines() if b]


def _fast_forward_local(repo: Path, peer_ns: str, branch: str | None) -> list[BranchAction]:
    current = _current_branch_local(repo)
    dirty = _dirty_local(repo)
    actions: list[BranchAction] = []
    for name in _branches_local(repo, branch):
        local_ref = f"refs/heads/{name}"
        peer_ref = f"refs/remotes/{peer_ns}/{name}"
        if _local_git(repo, ["show-ref", "--verify", "--quiet", peer_ref]).returncode != 0:
            actions.append(BranchAction(name, "skipped_missing_peer_ref", detail=peer_ref))
            continue
        action = _classify_local(repo, name, local_ref, peer_ref)
        if action.state != "behind":
            actions.append(action)
            continue
        if name == current:
            if dirty:
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
) -> list[BranchAction]:
    current = _current_branch_remote(transport, remote_root)
    dirty = _dirty_remote(transport, remote_root)
    actions: list[BranchAction] = []
    for name in _branches_remote(transport, remote_root, branch):
        local_ref = f"refs/heads/{name}"
        peer_ref = f"refs/remotes/{peer_ns}/{name}"
        if _remote_git(transport, remote_root, ["show-ref", "--verify", "--quiet",
                                               peer_ref]).exit_code != 0:
            actions.append(BranchAction(name, "skipped_missing_peer_ref", detail=peer_ref))
            continue
        action = _classify_remote(transport, remote_root, name, local_ref, peer_ref)
        if action.state != "behind":
            actions.append(action)
            continue
        if name == current:
            if dirty:
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


def _ref_namespace(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    cleaned = cleaned.strip(".-/")
    return cleaned or "LOCAL"
