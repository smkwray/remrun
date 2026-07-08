"""``remrun sync`` — project-less folder sync (content-hash + baseline, never clocks).

A standalone fetch/converge primitive for folders that live *outside* the project tree —
for example, generated fleet output trees configured in ``[sync_roots]``. The defining
case is **local may be older than remote** (the remote device is the producer; local is
behind it).

How direction is decided (and why it's safe under unreliable clocks):

  * **Same vs different** is decided by **content hash** (size, then sha256 — sync hashes
    everything). Never by mtime.
  * **Which way to copy** (`--both`) is decided by a **baseline** — the last-synced manifest
    for *each side*. We compare each side to *its own* previous state (same device, same
    clock) to see *which side changed*: only remote changed → pull; only local changed →
    push; both changed → conflict (saved aside). This never compares two devices' clocks,
    so skew / time-zone / precision differences can't mislead it. First sync (no baseline
    yet) falls back to the tree ``authority`` (default ``remote`` wins). `--pull`/`--push`
    are explicit one-way (remote/local authoritative), additive.
  * **Rollback net:** before overwriting or deleting any local file, the prior version is
    snapshotted under the state root (``conflicts/<id>/backup``), so a wrong sync is
    recoverable. Conflicting remote versions are saved under ``conflicts/<id>/remote``.

Path mapping uses the ``[sync_roots]`` config table (a project-less parallel to
``[project_roots]``), plus a ``--remote`` escape hatch.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from .config import (
    RemrunConfig, case_insensitive, casefold_collisions, current_os_key, device_os_key,
    expand_path, global_excludes, load_retention,
)
from .manifest import FileEntry, Manifest, build_manifest
from .models import Device
from .output import Reporter
from .state import (
    conflict_dir, default_state_root, new_run_id, prune_state, read_baseline, write_baseline,
)
from .transfer_plan import (
    ABORT_CONFLICT, DELETE_LOCAL, DELETE_REMOTE, NONE, PULL, PUSH,
    ClassifiedPath, compare_manifests,
)
from .transport import TransportError, make_transport

# Exit codes mirror cli.py.
EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_CONFLICT = 2
EXIT_TRANSFER = 3
EXIT_INFRA = 4

DIRECTIONS = ("both", "pull", "push")
AUTHORITIES = ("remote", "local", "conflict")

# sync compares by content, so it hashes everything (clocks are not trustworthy for a
# "same" decision). Overrides the size-capped default hashing used elsewhere. Streaming archive
# transfer now reduces per-file SSH overhead during apply; incremental manifests remain future work
# for very large trees.
_HASH_ALL = 1 << 62


class SyncError(RuntimeError):
    """A usage/mapping error (unknown tree, bad subpath, no path for the OS pair…)."""


@dataclass(frozen=True)
class SyncMapping:
    local_root: Path
    remote_base: str          # unexpanded spec (may start with ~); transport expands it
    remote_sub: str           # POSIX-relative subpath under both bases ("" for tree root)
    tree: str                 # the [sync_roots] tree name, or "(explicit)" for --remote
    authority: str = "remote"  # first-sync/one-way winner of a difference (remote|local|conflict)


@dataclass
class SyncResult:
    device: str = ""
    local_root: str = ""
    remote_root: str = ""
    direction: str = "both"
    authority: str = "remote"
    pulled: list[str] = field(default_factory=list)
    pushed: list[str] = field(default_factory=list)
    deleted_local: list[str] = field(default_factory=list)
    deleted_remote: list[str] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    backup_dir: str | None = None
    conflict_dir: str | None = None
    used_baseline: bool = False
    remote_files: int = 0      # count of files in the remote tree (verify output landed)
    remote_paths: list[str] = field(default_factory=list)   # their relpaths (per-batch new-file check)
    dry_run: bool = False
    exit_code: int = EXIT_OK




def _base_for(tree: dict[str, str], os_key: str) -> str | None:
    return tree.get(os_key) or tree.get("default")


def _authority_of(tree: dict[str, str]) -> str:
    a = str(tree.get("authority", "remote")).lower()
    return a if a in AUTHORITIES else "remote"


def _safe_subpath(sub: str) -> str:
    """Normalize + validate a tree subpath; reject traversal/absolute/drive parts."""
    sub = str(sub).replace("\\", "/").strip("/")
    if not sub:
        return ""
    pp = PurePosixPath(sub)
    if pp.is_absolute() or pp.drive:
        raise SyncError(f"subpath must be relative, got {sub!r}")
    for part in pp.parts:
        if part in ("..", ".") or ":" in part:
            raise SyncError(f"unsafe subpath component {part!r} in {sub!r}")
    return pp.as_posix()


def resolve_sync_paths(config: RemrunConfig, arg: str, device: Device,
                       remote_override: str | None = None) -> SyncMapping:
    """Resolve a sync ``arg`` (+ optional ``--remote``) to a local/remote mapping."""
    sync_roots = config.sync_roots or {}
    local_os = current_os_key()
    remote_os = device_os_key(device)

    if remote_override:  # explicit remote path (escape hatch; no config needed)
        return SyncMapping(local_root=Path(arg).expanduser().resolve(),
                           remote_base=remote_override, remote_sub="", tree="(explicit)")

    arg_norm = str(arg).replace("\\", "/").strip("/")
    head = arg_norm.split("/", 1)[0] if arg_norm else ""
    if head in sync_roots:  # named tree: "outputs" or "outputs/subdir/..."
        sub = _safe_subpath(arg_norm[len(head):])
        tree = sync_roots[head]
        local_base = _base_for(tree, local_os)
        remote_base = _base_for(tree, remote_os)
        if not local_base or not remote_base:
            raise SyncError(
                f"sync tree '{head}' has no path for {local_os} (local) and/or "
                f"{remote_os} (device {device.name}); check [sync_roots.{head}] in devices.toml")
        local_root = expand_path(local_base)
        if sub:
            local_root = local_root / Path(sub)
        return SyncMapping(local_root=local_root.resolve(), remote_base=remote_base,
                           remote_sub=sub, tree=head, authority=_authority_of(tree))

    candidate = Path(arg).expanduser()  # a path already under a known tree's local base
    if candidate.is_absolute() or candidate.exists():
        resolved = candidate.resolve()
        for name, tree in sync_roots.items():
            local_base = _base_for(tree, local_os)
            remote_base = _base_for(tree, remote_os)
            if not local_base or not remote_base:
                continue
            try:
                sub = resolved.relative_to(expand_path(local_base).resolve()).as_posix()
            except ValueError:
                continue
            return SyncMapping(local_root=resolved, remote_base=remote_base,
                               remote_sub=("" if sub == "." else sub), tree=name,
                               authority=_authority_of(tree))

    known = ", ".join(sorted(sync_roots)) or "(none configured)"
    raise SyncError(f"don't know how to map '{arg}'. Use a configured [sync_roots] tree "
                    f"({known}) like 'outputs/audio', or pass --remote <remote-path>.")


def remote_spec_to_tree(config: RemrunConfig, device: Device,
                        remote_spec: str) -> tuple[str, str] | None:
    """Reverse of ``resolve_sync_paths``: map an (unexpanded) remote output root back
    to a ``[sync_roots]`` tree name + subpath, by matching the device's OS base for
    each tree (longest base wins). Returns ``(tree, sub)`` or ``None`` when no tree
    contains the spec.

    Pure (no SSH): the fleet dispatcher uses it to decide which tree to ``sync --pull`` after
    a batch writes its output. Both the spec and the tree bases are the *unexpanded* config
    forms (``~``/drive letters), so they share a literal prefix and we never have to expand a
    remote home just to classify. Separators are unified; comparison casefolds on a
    case-insensitive OS (Windows/macOS) but the returned subpath keeps its original case."""
    sync_roots = config.sync_roots or {}
    os_key = device_os_key(device)
    ci = case_insensitive(os_key)
    spec_sep = str(remote_spec).replace("\\", "/").rstrip("/")
    spec_cmp = spec_sep.casefold() if ci else spec_sep
    best: tuple[str, str] | None = None
    best_len = -1
    for name, tree in sync_roots.items():
        base = _base_for(tree, os_key)
        if not base:
            continue
        base_sep = str(base).replace("\\", "/").rstrip("/")
        base_cmp = base_sep.casefold() if ci else base_sep
        if spec_cmp == base_cmp:
            sub = ""
        elif spec_cmp.startswith(base_cmp + "/"):
            sub = spec_sep[len(base_sep) + 1:]   # slice the case-preserved form
        else:
            continue
        if len(base_cmp) > best_len:
            best, best_len = (name, sub), len(base_cmp)
    return best


def _baseline_key(mapping: SyncMapping) -> str:
    """Stable per-mapping id for the last-synced baseline (under read/write_baseline)."""
    sig = f"{mapping.local_root}|{mapping.remote_base}|{mapping.remote_sub}".encode()
    # 16 hex chars: this key selects a tree's delete evidence, so keep the collision domain
    # comfortably large.
    return f"sync-{mapping.tree}-{hashlib.sha1(sig).hexdigest()[:16]}"


def _same_content(le: FileEntry, re: FileEntry) -> bool:
    """SAME only when size matches and both content hashes match. Never trusts mtime."""
    if le.size != re.size:
        return False
    if le.sha256 is not None and re.sha256 is not None:
        return le.sha256 == re.sha256
    return False


def classify_sync(local: Manifest, remote: Manifest, authority: str) -> list[ClassifiedPath]:
    """First-sync / one-way classification by content + authority (no baseline, no deletes)."""
    out: list[ClassifiedPath] = []
    for path in sorted(set(local) | set(remote)):
        le, re = local.get(path), remote.get(path)
        if le and re:
            if _same_content(le, re):
                out.append(ClassifiedPath(path, "same", NONE, "identical content"))
            elif authority == "local":
                out.append(ClassifiedPath(path, "differs", PUSH, "differs; local authoritative"))
            elif authority == "conflict":
                out.append(ClassifiedPath(path, "differs", ABORT_CONFLICT,
                                          "differs; no authority (clocks not comparable)"))
            else:
                out.append(ClassifiedPath(path, "differs", PULL, "differs; remote authoritative"))
        elif re and not le:
            out.append(ClassifiedPath(path, "remote-only", PULL, "only on remote"))
        elif le and not re:
            out.append(ClassifiedPath(path, "local-only", PUSH, "only on local"))
    return out


def _filter_actions(paths: list[ClassifiedPath],
                    direction: str) -> tuple[list[ClassifiedPath], list[ClassifiedPath]]:
    """Split into (to-apply, skipped). ``--pull`` keeps local-affecting actions
    (PULL/DELETE_LOCAL), ``--push`` keeps remote-affecting (PUSH/DELETE_REMOTE); ``--both``
    keeps all. Conflicts are always kept (save-aside); NONE is dropped."""
    pull_side = {PULL, DELETE_LOCAL}
    push_side = {PUSH, DELETE_REMOTE}
    apply: list[ClassifiedPath] = []
    skipped: list[ClassifiedPath] = []
    for p in paths:
        if p.action == NONE:
            continue
        if p.action == ABORT_CONFLICT:
            apply.append(p)
        elif direction == "pull" and p.action in push_side:
            skipped.append(p)
        elif direction == "push" and p.action in pull_side:
            skipped.append(p)
        else:
            apply.append(p)
    return apply, skipped


def run_sync_result(config: RemrunConfig, *, arg: str, device_name: str,
                    remote_override: str | None = None, direction: str = "both",
                    dry_run: bool = False, extra_excludes: list[str] | None = None,
                    reporter: Reporter | None = None, as_json: bool = False,
                    state_root: Path | None = None) -> SyncResult:
    """The sync engine, returning the structured ``SyncResult`` (``.exit_code`` is set on
    EVERY return path). ``run_sync`` is the thin int-returning wrapper the CLI uses; the fleet
    dispatcher calls this directly so it can inspect ``remote_files``/``pulled`` and verify a
    batch's output actually landed where expected (Phase 2b deterministic fetch)."""
    reporter = reporter or Reporter(json_events=as_json)
    state_root = state_root or default_state_root()
    policy = load_retention(config)
    result = SyncResult(device=device_name, direction=direction, dry_run=dry_run)
    if direction not in DIRECTIONS:
        reporter.event("error", message=f"invalid direction {direction!r}; expected {DIRECTIONS}")
        result.exit_code = EXIT_INTERNAL
        return result

    device = config.devices.get(device_name)
    if device is None:
        reporter.event("error", message=f"unknown device {device_name!r}")
        result.exit_code = EXIT_INTERNAL
        return result
    try:
        mapping = resolve_sync_paths(config, arg, device, remote_override)
    except SyncError as exc:
        reporter.event("error", message=str(exc))
        result.exit_code = EXIT_INTERNAL
        return result

    authority = {"pull": "remote", "push": "local"}.get(direction, mapping.authority)
    result.authority = authority

    transport = make_transport(device)
    probe = transport.probe()
    if not probe.reachable:
        reporter.event("unreachable", device=device_name, detail=probe.detail)
        result.exit_code = EXIT_INFRA
        return result

    remote_root = transport.remote_join(transport.expand_remote(mapping.remote_base),
                                        mapping.remote_sub)
    result.local_root = str(mapping.local_root)
    result.remote_root = remote_root

    if direction != "push":  # a wrong/absent remote root otherwise looks like an empty tree
        try:
            if not transport.remote_path_exists(remote_root):
                reporter.event("error", message=f"remote root does not exist: {remote_root} "
                               "(check [sync_roots]/--remote, or use --push to create it)")
                result.exit_code = EXIT_INFRA
                return result
        except (TransportError, NotImplementedError):
            pass

    excludes = list(global_excludes(config)) + list(extra_excludes or [])
    reporter.event("sync", tree=mapping.tree, device=device_name, direction=direction,
                   authority=authority, local=str(mapping.local_root), remote=remote_root)

    # Hash everything on both sides: content, not mtime, is the source of truth.
    local_m = build_manifest(mapping.local_root, excludes, hash_below_bytes=_HASH_ALL)
    try:
        remote_m = transport.manifest(remote_root, excludes, _HASH_ALL)
    except TransportError as exc:
        reporter.event("transfer_error", phase="manifest", message=str(exc))
        result.exit_code = EXIT_TRANSFER
        return result
    result.remote_files = len(remote_m)
    result.remote_paths = list(remote_m)

    # --both with a baseline → clock-safe change detection (and known-deletion mirroring,
    # with backup). First --both / --pull / --push → authority-based, additive (no deletes).
    base_key = _baseline_key(mapping)
    prev_l = prev_r = None
    if direction == "both":
        prev_l, prev_r = read_baseline(device_name, base_key, state_root)
    if prev_l is not None and prev_r is not None:
        # Vanished-local guard (mirrors reconcile.preflight_reconcile). If the baseline had
        # local files but the local tree is now EMPTY and its root is GONE (wrong cwd /
        # unmounted), refuse: otherwise compare_manifests classifies every remote file as a
        # known local deletion and DELETE_REMOTEs the whole remote tree. (An existing-but-
        # empty local root is treated as a genuine wipe and allowed.)
        if prev_l and not local_m and not mapping.local_root.exists():
            reporter.event("error", message=f"local root missing ({mapping.local_root}) but the "
                           "baseline had files — refusing to mirror wholesale deletions to remote")
            result.exit_code = EXIT_INFRA
            return result
        plan = compare_manifests(local_m, remote_m, prev_l, prev_r)
        classified = plan.paths
        result.used_baseline = True
    else:
        classified = classify_sync(local_m, remote_m, authority)

    to_apply, dir_skipped = _filter_actions(classified, direction)
    result.skipped = [p.path for p in dir_skipped]

    # Case-fold collision guard: if a TARGET filesystem is case-insensitive, two distinct
    # paths that fold to the same name would collapse into one file (last writer wins, silent
    # data loss). Check what would actually land on each insensitive target (its existing
    # files + the incoming writes) and abort before mutating anything.
    collisions: dict[str, list[str]] = {}
    if case_insensitive(current_os_key()):     # local is the target of pulls
        collisions.update(casefold_collisions(
            list(local_m) + [p.path for p in to_apply if p.action == PULL]))
    if case_insensitive(device_os_key(device)):  # remote is the target of pushes
        collisions.update(casefold_collisions(
            list(remote_m) + [p.path for p in to_apply if p.action == PUSH]))
    if collisions:
        for _key, paths in sorted(collisions.items()):
            reporter.event("error", message="case-fold collision (would collapse on a "
                           "case-insensitive target): " + ", ".join(paths))
        result.exit_code = EXIT_CONFLICT
        return result

    if dry_run:
        reporter.event("plan", used_baseline=result.used_baseline,
                       pull=sum(1 for p in to_apply if p.action == PULL),
                       push=sum(1 for p in to_apply if p.action == PUSH),
                       delete_local=sum(1 for p in to_apply if p.action == DELETE_LOCAL),
                       delete_remote=sum(1 for p in to_apply if p.action == DELETE_REMOTE),
                       conflict=sum(1 for p in to_apply if p.action == ABORT_CONFLICT),
                       skipped=len(dir_skipped))
        for p in to_apply:
            reporter.event("would", action=p.action, path=p.path, why=p.reason)
        if as_json:
            _print_json(result, planned=to_apply)
        result.exit_code = EXIT_OK
        return result

    # Lazily create a run dir for backups/conflicts (so a clean run writes nothing extra).
    needs_dir = any(p.action in (PULL, DELETE_LOCAL, ABORT_CONFLICT) for p in to_apply)
    run_id = new_run_id(device_name, base_key) if needs_dir else ""
    backup_root = conflict_dir(run_id, state_root) / "backup" if run_id else None
    conflict_root = conflict_dir(run_id, state_root) / "remote" if run_id else None
    if run_id:
        result.backup_dir = str(conflict_dir(run_id, state_root))

    # Apply, then ALWAYS prune (even if a transfer error aborts mid-apply, where backups
    # may already have been written) so repeated failures can't grow the state unbounded.
    try:
        try:
            pulls: list[ClassifiedPath] = []
            pushes: list[ClassifiedPath] = []
            for item in to_apply:
                remote_path = transport.remote_join(remote_root, item.path)
                local_path = mapping.local_root / Path(item.path)
                if item.action == PULL:
                    _snapshot_local(local_path, item.path, backup_root, policy.backup_below_bytes)
                    pulls.append(item)
                elif item.action == PUSH:
                    pushes.append(item)
                elif item.action == DELETE_LOCAL:
                    _snapshot_local(local_path, item.path, backup_root, policy.backup_below_bytes)
                    local_path.unlink(missing_ok=True)
                    result.deleted_local.append(item.path)
                elif item.action == DELETE_REMOTE:
                    transport.delete_remote(remote_path)
                    result.deleted_remote.append(item.path)
                elif item.action == ABORT_CONFLICT:
                    # Save the remote version aside ONLY if it still exists. A "remote deleted
                    # the file, local modified it" conflict has no remote file to pull — record
                    # the conflict and leave both sides untouched (don't crash on a missing path).
                    if conflict_root is not None and remote_m.get(item.path) is not None:
                        transport.pull_file(remote_path, conflict_root / Path(item.path))
                    result.conflicts.append(item.path)
            if pulls:
                transport.pull_files(remote_root, mapping.local_root, [p.path for p in pulls])
                result.pulled.extend(p.path for p in pulls)
            if pushes:
                transport.ensure_remote_dir(remote_root)
                transport.push_files(mapping.local_root, remote_root, [p.path for p in pushes])
                result.pushed.extend(p.path for p in pushes)
        except TransportError as exc:
            reporter.event("transfer_error", phase="apply", message=str(exc))
            result.exit_code = EXIT_TRANSFER
            return result

        # Advance the baseline (the new converged state) for --both, unless conflicts remain —
        # then leave the old baseline so the divergence is re-detected next run. Computed from
        # the applied actions (transfers preserve content+mtime), so no re-hashing.
        if direction == "both" and not result.conflicts:
            local_after, remote_after = _converged(local_m, remote_m, to_apply)
            write_baseline(device_name, base_key, local_after, remote_after, state_root)

        result.exit_code = EXIT_CONFLICT if result.conflicts else EXIT_OK
        for rel in result.conflicts:
            reporter.event("conflict", path=rel, saved=str((conflict_root or Path()) / rel))
        reporter.event("synced", pulled=len(result.pulled), pushed=len(result.pushed),
                       deleted_local=len(result.deleted_local),
                       deleted_remote=len(result.deleted_remote),
                       conflicts=len(result.conflicts), skipped=len(result.skipped),
                       backup_dir=result.backup_dir, exit_code=result.exit_code)
        if as_json:
            _print_json(result)
        return result
    finally:
        try:
            prune_state(policy, state_root=state_root)
        except Exception:  # noqa: BLE001
            pass


def run_sync(config: RemrunConfig, *, arg: str, device_name: str,
             remote_override: str | None = None, direction: str = "both",
             dry_run: bool = False, extra_excludes: list[str] | None = None,
             reporter: Reporter | None = None, as_json: bool = False,
             state_root: Path | None = None) -> int:
    """CLI entry point: run a sync and return just the exit code. The full structured
    outcome is available via ``run_sync_result`` (used by the fleet dispatcher)."""
    return run_sync_result(
        config, arg=arg, device_name=device_name, remote_override=remote_override,
        direction=direction, dry_run=dry_run, extra_excludes=extra_excludes,
        reporter=reporter, as_json=as_json, state_root=state_root).exit_code


def _snapshot_local(local_path: Path, rel: str, backup_root: Path | None,
                    max_bytes: int = 0) -> None:
    """Copy an existing local file aside before it's overwritten/deleted (rollback net).
    No-op when the file is new, backups are disabled, or the file exceeds ``max_bytes``
    (large files are regenerable/re-syncable; snapshotting them is the main growth risk —
    the size budget in prune_state is the hard backstop)."""
    if backup_root is None or not local_path.exists():
        return
    if max_bytes and local_path.stat().st_size > max_bytes:
        return
    dest = backup_root / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(local_path, dest)


def _converged(local_m: Manifest, remote_m: Manifest,
               applied: list[ClassifiedPath]) -> tuple[Manifest, Manifest]:
    """The post-sync manifests for both sides, computed from the applied actions."""
    local_after = dict(local_m)
    remote_after = dict(remote_m)
    for p in applied:
        if p.action == PULL:
            local_after[p.path] = remote_m[p.path]
        elif p.action == PUSH:
            remote_after[p.path] = local_m[p.path]
        elif p.action == DELETE_LOCAL:
            local_after.pop(p.path, None)
        elif p.action == DELETE_REMOTE:
            remote_after.pop(p.path, None)
    return local_after, remote_after


def _print_json(result: SyncResult, planned: list[ClassifiedPath] | None = None) -> None:
    payload = {
        "device": result.device, "local_root": result.local_root,
        "remote_root": result.remote_root, "direction": result.direction,
        "authority": result.authority, "used_baseline": result.used_baseline,
        "dry_run": result.dry_run, "pulled": result.pulled, "pushed": result.pushed,
        "deleted_local": result.deleted_local, "deleted_remote": result.deleted_remote,
        "conflicts": result.conflicts, "skipped": result.skipped,
        "backup_dir": result.backup_dir, "exit_code": result.exit_code,
    }
    if planned is not None:
        payload["planned"] = [{"action": p.action, "path": p.path, "reason": p.reason}
                              for p in planned]
    print(json.dumps(payload, indent=2, sort_keys=True))
