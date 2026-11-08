"""``--transfer commit``: run a clean HEAD in a run-private tree on the target.

The source comes from git, never from the target's synced checkout. A remrun-owned
bare cache under the target's state root receives a bundle (incremental when the cache
already holds an ancestor), and each run clones it into ``<state_root>/work/<id>/tree``.
Only the declared ``--output`` paths return, through the ordinary pullback; the tree is
removed after a clean, verified pullback and retained otherwise.
"""
from __future__ import annotations

import hashlib
import os
import re
import secrets
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Callable

from ._durable_runner import pid_liveness
from .config import RemrunConfig, work_min_free_fraction
from .gitsync import (
    GitSyncError,
    _bounded_metadata_transport,
    _remote_git_ok,
    _remote_memory_limit_mib,
    _repository_scaled_transport,
)
from .manifest import (
    ManifestError,
    build_manifest,
    nonregular_paths,
    path_within,
    sha256_file,
    should_exclude,
)
from .models import RunPlan
from .planner import paths_overlap
from .reconcile import PullbackResult, postrun_pullback
from .state import (
    RetentionPolicy,
    _safe_component,
    conflict_dir,
    default_state_root,
    per_run_completion_fences,
    read_json,
    read_manifest,
    read_unknown_completion_hazard,
    utc_now_iso,
    write_json,
    write_manifest,
)
from .transport import BaseTransport, TransportError

# Commit-cache refs, one per seeded run; the newest few stay as incremental bases.
_CACHE_REFS = "refs/remrun/runs/"
_CACHE_REFS_KEPT = 16
# Lists in summaries and events stay bounded; the count is always exact.
_LISTED_PATHS = 200
# The work ids work_paths allocates; nothing else is ever removed as a work tree.
_WORK_ID = re.compile(r"[0-9a-f]{10}")


class CommitSourceError(ValueError):
    """The controller's checkout cannot be a commit run's source."""

    def __init__(self, message: str, paths: list[str] | None = None) -> None:
        super().__init__(message)
        self.paths = paths or []


class CandidateRefusal(Exception):
    """This target cannot host the run; nothing was written to it."""

    def __init__(self, state: str, reason: str) -> None:
        super().__init__(reason)
        self.state = state
        self.reason = reason


@dataclass(frozen=True)
class CommitSource:
    commit: str
    tree_bytes: int
    inputs: tuple[dict[str, Any], ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {"commit": self.commit, "inputs": [dict(item) for item in self.inputs]}


@dataclass
class CommitWork:
    """Where one commit run lives on its target, in the target's native path form."""

    work_id: str
    path: str
    tree: str
    cache: str
    root: str
    # Set once the target has been written for this run, and once its command may have
    # started; a seeded, never-dispatched tree is removed by the caller.
    seeded: bool = False
    dispatched: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"id": self.work_id, "path": self.path, "tree": self.tree}


# --- controller side: the source ------------------------------------------------

def _git(root: Path, *args: str) -> bytes:
    proc = subprocess.run(
        ["git", "--no-optional-locks", "-C", str(root), *args],
        capture_output=True, check=False,
    )
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", "replace").strip() or f"exit {proc.returncode}"
        raise CommitSourceError(f"git {' '.join(args)} failed in {root}: {detail}")
    return proc.stdout


def _nul_fields(data: bytes) -> list[str]:
    return [os.fsdecode(item) for item in data.split(b"\0") if item]


def _status_paths(raw: bytes) -> list[str]:
    """Paths named by ``git status --porcelain=v1 -z`` (both sides of a rename)."""
    paths: list[str] = []
    fields = raw.split(b"\0")
    index = 0
    while index < len(fields):
        entry = fields[index]
        index += 1
        if len(entry) < 4:
            continue
        paths.append(os.fsdecode(entry[3:]))
        if entry[:1] in {b"R", b"C"} or entry[1:2] in {b"R", b"C"}:
            if index < len(fields) and fields[index]:
                paths.append(os.fsdecode(fields[index]))
            index += 1
    return paths


def commit_source(plan: RunPlan) -> CommitSource:
    """Prove the local checkout is a clean HEAD, apart from declared outputs and inputs.

    The project root must be its repository's top level, so project paths and
    repository paths agree. Tracked changes outside ``--output``/``--input`` refuse
    the run: a commit run is the commit, plus exactly the files it names.
    """
    root = plan.project.local_project_root
    top = os.fsdecode(_git(root, "rev-parse", "--show-toplevel").strip())
    if Path(top).resolve() != root.resolve():
        raise CommitSourceError(
            f"--transfer commit needs the project root {root} to be its git repository's "
            f"top level (git reports {top})"
        )
    commit = _git(root, "rev-parse", "--verify", "--quiet", "HEAD^{commit}").decode().strip()
    if len(commit) not in {40, 64}:
        raise CommitSourceError("--transfer commit needs a commit at HEAD")
    declared = (*plan.outputs, *plan.inputs)
    dirty = sorted({
        path for path in _status_paths(
            _git(root, "status", "--porcelain=v1", "-z", "--untracked-files=no")
        ) if not path_within(path, declared)
    })
    if dirty:
        raise CommitSourceError(
            f"{len(dirty)} tracked file(s) differ from HEAD outside --output/--input; "
            "commit them, or name them with --input",
            dirty,
        )
    tree_bytes = 0
    links: list[str] = []
    for line in _nul_fields(_git(root, "ls-tree", "-r", "-l", "-z", "--full-tree", commit)):
        meta, _tab, path = line.partition("\t")
        parts = meta.split()
        if len(parts) == 4 and parts[1] == "commit":
            raise CommitSourceError(
                f"--transfer commit does not check out submodules ({path} is one)"
            )
        if parts[:1] == ["120000"]:
            links.append(path)
        if len(parts) == 4 and parts[3].isdigit():
            tree_bytes += int(parts[3])
    # remrun returns only regular files. A symlink or special file at, above or under an
    # output would be skipped by the manifests (or, locally, written through), so one
    # already in HEAD or in the working tree refuses the run before any target.
    for rel in plan.outputs:
        link = _head_link(rel, links)
        if link is not None:
            raise CommitSourceError(
                f"--output {rel}: HEAD has a symlink at {link}; remrun returns only "
                "regular files"
            )
    try:
        odd = nonregular_paths(root, plan.outputs)
    except ManifestError as exc:
        raise CommitSourceError(f"--output: {exc}") from exc
    if odd:
        raise CommitSourceError(
            f"--output: {odd[0]} is a symlink or special file in this project; remrun "
            "returns only regular files"
        )
    inputs = []
    for rel in plan.inputs:
        # The overlay is written into the checked-out commit, where a symlink of HEAD's
        # at or above it would carry the write out of the private tree; locally, one
        # would read bytes from outside the project. Both are refused.
        link = _head_link(rel, links)
        if link is not None:
            raise CommitSourceError(
                f"--input {rel}: HEAD has a symlink at {link}, so the overlay could be "
                "written outside the run's private tree"
            )
        local = root.joinpath(*rel.split("/"))
        try:
            odd = nonregular_paths(root, [rel])
        except ManifestError as exc:
            raise CommitSourceError(f"--input {rel}: {exc}") from exc
        if odd or not local.is_file() or not local.resolve().is_relative_to(root.resolve()):
            raise CommitSourceError(
                f"--input {rel}: no such regular file in this project"
                + (f" ({odd[0]} is a symlink or special file)" if odd else "")
            )
        inputs.append({"path": rel, "bytes": local.stat().st_size, "sha256": sha256_file(local)})
    return CommitSource(commit=commit, tree_bytes=tree_bytes, inputs=tuple(inputs))


def _head_link(rel: str, links: list[str]) -> str | None:
    """A symlink of HEAD at, above or under project path ``rel`` (by ``path_identity``,
    since the target's filesystem may ignore case)."""
    return next((link for link in links if paths_overlap(link + "/", rel)), None)


# --- target layout ---------------------------------------------------------------

def _target_path(device_windows: bool, raw: str) -> PurePosixPath | PureWindowsPath:
    return PureWindowsPath(raw) if device_windows else PurePosixPath(raw)


def _inside(child: str, parent: str, *, windows: bool) -> bool:
    a, b = _target_path(windows, child), _target_path(windows, parent)
    fold = (lambda parts: [p.lower() for p in parts]) if windows else list
    return fold(a.parts[: len(b.parts)]) == fold(b.parts)


def _state_root(transport: BaseTransport) -> str:
    """The target's state root as an absolute native path; ``ValueError`` otherwise.

    A ``~`` root needs the target's home, which a transport learns by probing; one that
    has not probed yet (resume, cleanup) probes once, read-only.
    """
    device = transport.device
    state_root = transport.expand_remote(device.state_root)
    if state_root.startswith("~"):
        transport.probe()
        state_root = transport.expand_remote(device.state_root)
    if not state_root or state_root.startswith("~") or not _target_path(
        transport.target_is_windows, state_root
    ).is_absolute():
        raise ValueError(f"{device.name}: state_root did not resolve to an absolute path")
    return state_root


def work_paths(transport: BaseTransport, plan: RunPlan) -> CommitWork:
    """Allocate this attempt's private tree under the target's state root.

    The short id keeps Windows paths short. A state root inside the target's project
    root would put remrun's work tree in the synced tree, so it is refused.
    """
    windows = transport.target_is_windows
    state_root = _state_root(transport)
    project_root = transport.expand_remote(plan.target.project_root)
    if project_root and _inside(state_root, project_root, windows=windows):
        raise ValueError(
            f"{plan.target.name}: --transfer commit keeps its work outside project trees, "
            f"but state_root {state_root} is inside project_root {project_root}"
        )
    root = transport.native_join(state_root, "work")
    work_id = secrets.token_hex(5)
    path = transport.native_join(root, work_id)
    project_hash = hashlib.sha256(plan.project.project_id.encode("utf-8")).hexdigest()[:16]
    return CommitWork(
        work_id=work_id, path=path, tree=transport.native_join(path, "tree"),
        cache=transport.native_join(root, "git", f"{project_hash}.git"), root=root,
    )


# --- controller-local registry of this controller's work trees -------------------

def _registry_dir(target: str, state_root: Path | None = None) -> Path:
    return (state_root or default_state_root()) / "commit-work" / _safe_component(target)


def _registry_path(target: str, work_id: str) -> Path:
    return _registry_dir(target) / f"{work_id}.json"


def record_work(
    plan: RunPlan, run_id: str, work: CommitWork, state: str, reason: str | None = None
) -> None:
    write_json(_registry_path(plan.target.name, work.work_id), {
        "version": 1, "work_id": work.work_id, "run_id": run_id,
        "project_id": plan.project.project_id, "target": plan.target.name,
        "path": work.path, "created_at": work.extra.get("created_at") or utc_now_iso(),
        "updated_at": utc_now_iso(), "state": state, "reason": reason,
        # The controller seeding the tree: until its fence is published at dispatch,
        # this process being alive is what keeps the tree from being pruned.
        "pid": os.getpid(),
    })


def remove_work(
    transport: BaseTransport, target: str, work_id: object, path: object,
    report: Callable[..., None], *, tree: object = None,
) -> str:
    """Remove one work tree and its registry record: ``removed``, ``remove_failed`` or
    ``remove_refused``.

    Only ``<state_root>/work/<work_id>`` (with its ``tree``), for an id remrun
    allocates, is ever removed: the path comes from a record or summary, and a wrong
    one would be deleted recursively. Any other is refused and reported, and its
    record is left untouched, so ``prune_work_trees`` reaches it again on later commit
    launches on this target and refuses it again. A summary's ``remove_refused`` is final:
    ``resume`` does not retry it. A failed removal, or a state root that cannot be
    resolved right now, keeps the record (state ``remove_failed``) so the next commit
    launch on this target, or ``resume``, retries it.
    """
    if not isinstance(work_id, str) or not _WORK_ID.fullmatch(work_id):
        report("work_remove_refused", work_id=work_id, path=path,
               reason="not a work id remrun allocates")
        return "remove_refused"
    record_path = _registry_path(target, work_id)
    try:
        expected = transport.native_join(_state_root(transport), "work", work_id)
        if path != expected or tree not in {None, transport.native_join(expected, "tree")}:
            report("work_remove_refused", work_id=work_id, path=path, tree=tree,
                   reason=f"not this target's work directory {expected} and its tree")
            return "remove_refused"
        transport.remove_remote_tree(expected)
    except (ValueError, TransportError, NotImplementedError, OSError) as exc:
        record = read_json(record_path) or {"version": 1, "work_id": work_id, "path": path}
        record.update(state="remove_failed", reason=f"removal failed: {exc}",
                      updated_at=utc_now_iso())
        write_json(record_path, record)
        return "remove_failed"
    record_path.unlink(missing_ok=True)
    return "removed"


def prune_work_trees(
    transport: BaseTransport,
    plan: RunPlan,
    policy: RetentionPolicy,
    report: Callable[..., None],
    *,
    only_run: str | None = None,
    now: datetime | None = None,
) -> None:
    """Remove this controller's leftover trees on ``plan.target``.

    A failed removal is retried at once; a retained tree, or one orphaned by a crash
    before its run finished, only after ``[logging] failed_log_retention_days``. A
    tree whose run still holds a completion fence may be in use and is never touched,
    nor is an ``active`` one whose controller may still be seeding it. A record whose
    removal ``remove_work`` refuses is kept, so every later launch reports it again.
    """
    directory = _registry_dir(plan.target.name)
    if not directory.is_dir():
        return
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=policy.failed_log_days)
    for path in sorted(directory.glob("*.json")):
        record = read_json(path)
        if not isinstance(record, dict) or not all(
            isinstance(record.get(key), str) and record[key]
            for key in ("path", "project_id", "run_id")
        ) or record.get("work_id") != path.stem:
            report("work_record_unreadable", path=str(path))
            continue
        if only_run is not None and record["run_id"] != only_run:
            continue
        # Any project's run on this target may own the record; its fence is checked.
        if read_unknown_completion_hazard(record["project_id"], run_id=record["run_id"]):
            continue
        # A tree is recorded `active` before seeding but fenced only at dispatch, so in
        # between only its controller protects it: while that pid runs, or cannot be
        # proven gone, the tree may be mid-seed. Taking the fence before seeding instead
        # would hold the output lease through every pre-dispatch refusal, and a crash
        # there would leave it for a manual resolve-unknown; a dead pid needs no cleanup.
        if record.get("state") == "active" and pid_liveness(record.get("pid")) is not False:
            continue
        if record.get("state") != "remove_failed":
            try:
                created = datetime.strptime(
                    str(record.get("created_at")), "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                report("work_record_unreadable", path=str(path))
                continue
            if created > cutoff:
                continue
        outcome = remove_work(
            transport, plan.target.name, record["work_id"], record["path"], report,
        )
        report("work_pruned", work_id=record.get("work_id"), run_id=record.get("run_id"),
               path=record["path"], previous_state=record.get("state"), outcome=outcome)


# --- output leases --------------------------------------------------------------

def lease_conflicts(project_id: str, outputs: tuple[str, ...]) -> list[dict[str, str]]:
    """Other live commit runs of the project whose leased outputs overlap ours."""
    conflicts: list[dict[str, str]] = []
    for fence in per_run_completion_fences(project_id):
        for mine in outputs:
            theirs = next(
                (other for other in fence.get("outputs", []) if paths_overlap(mine, other)),
                None,
            )
            if theirs is not None:
                conflicts.append({
                    "path": mine,
                    "state": "output-leased",
                    "reason": f"commit run {fence['run_id']} on {fence['target']} holds "
                              f"--output {theirs}; wait for it or choose other outputs",
                })
    return conflicts


# --- seeding ----------------------------------------------------------------------

def _local_bundle(root: Path, commit: str, bases: list[str], dest: Path) -> tuple[bool, int]:
    """Bundle HEAD minus everything the cache holds.

    Returns (whether a bundle was written, how many cache commits it builds on); no
    bundle means the cache already holds the commit.
    """
    known: list[str] = []
    if bases:
        proc = subprocess.run(
            ["git", "-C", str(root), "cat-file", "--batch-check"],
            input=("\n".join(bases) + "\n").encode(), capture_output=True, check=False,
        )
        known = [
            line.split()[0] for line in proc.stdout.decode().splitlines()
            if len(line.split()) == 3 and line.split()[1] == "commit"
        ] if proc.returncode == 0 else []
    proc = subprocess.run(
        ["git", "-C", str(root), "bundle", "create", str(dest), "HEAD",
         *(f"^{sha}" for sha in known)],
        capture_output=True, check=False,
    )
    if proc.returncode != 0:
        if b"empty bundle" in proc.stderr:
            return False, len(known)
        raise CommitSourceError(
            "git bundle create failed: " + proc.stderr.decode("utf-8", "replace").strip()
        )
    heads = _git(root, "bundle", "list-heads", str(dest)).decode().split()
    if heads[:1] != [commit]:
        raise CommitSourceError("HEAD moved while this run was starting; run it again")
    return True, len(known)


def seed_commit_tree(
    *,
    transport: BaseTransport,
    plan: RunPlan,
    config: RemrunConfig,
    policy: RetentionPolicy,
    run_id: str,
    rdir: Path,
    work: CommitWork,
    source: CommitSource,
    remote_cwd: str,
    report: Callable[..., None],
) -> None:
    """Build the run's private tree; raise ``CandidateRefusal`` before any target write.

    Order: prune old trees, measure the disk, size the bundle, refuse below the floor;
    only then write: cache, bundle fetch, clone, checkout, ``--input`` overlay, and the
    launch manifests the pullback compares against.
    """
    prune_work_trees(transport, plan, policy, report)
    try:
        limit = _remote_memory_limit_mib(config, plan.project_config, None)
        git = _repository_scaled_transport(transport, limit, operation="commit seeding")
        meta = _bounded_metadata_transport(transport, limit)
    except GitSyncError as exc:
        raise CandidateRefusal("git-memory-limit-missing", str(exc)) from exc
    try:
        probe = transport.work_probe(work.cache)
    except (TransportError, NotImplementedError, OSError) as exc:
        raise CandidateRefusal(
            "disk-unmeasured", f"cannot measure free space under {work.root}: {exc}"
        ) from exc
    if not probe.get("git"):
        raise CandidateRefusal(
            "git-unavailable", f"git is not on {plan.target.name}'s PATH; "
            "--transfer commit seeds its tree with git"
        )
    bases: list[str] = []
    if probe["exists"]:  # the cache exists; its refs are the incremental bases
        listing = _remote_git_ok(meta, work.cache, [
            "for-each-ref", "--sort=-committerdate", "--format=%(objectname) %(refname)",
            _CACHE_REFS,
        ]).stdout.split("\n")
        refs = [line.split() for line in listing if len(line.split()) == 2]
        bases = list(dict.fromkeys(sha for sha, _ref in refs))
        work.extra["stale_refs"] = [ref for _sha, ref in refs[_CACHE_REFS_KEPT:]]
    local_root = plan.project.local_project_root
    with tempfile.TemporaryDirectory(prefix="remrun-commit-") as scratch:
        bundle = Path(scratch) / "seed.bundle"
        has_bundle, prerequisites = _local_bundle(local_root, source.commit, bases, bundle)
        bundle_bytes = bundle.stat().st_size if has_bundle else 0
        input_bytes = sum(int(item["bytes"]) for item in source.inputs)
        needed = source.tree_bytes + input_bytes + bundle_bytes
        floor = work_min_free_fraction(config)
        free, total = int(probe["free_bytes"]), int(probe["total_bytes"])
        report("work_disk", free_bytes=free, total_bytes=total, needed_bytes=needed,
               min_free_fraction=floor, bundle="incremental" if prerequisites and has_bundle
               else "full" if has_bundle else "cached")
        if free - needed < floor * total:
            raise CandidateRefusal(
                "disk-floor",
                f"{plan.target.name} would keep {max(free - needed, 0)} of {total} bytes free "
                f"after this run's {needed} bytes, below [transfer] "
                f"work_min_free_fraction = {floor}",
            )
        work.extra["created_at"] = utc_now_iso()
        record_work(plan, run_id, work, "active")
        work.seeded = True
        transport.ensure_remote_dir(work.path)
        if not probe["exists"]:
            _remote_git_ok(git, work.path, ["init", "--bare", "-q", work.cache])
        if has_bundle:
            remote_bundle = transport.native_join(work.path, "seed.bundle")
            transport.push_file(bundle, remote_bundle)
            _remote_git_ok(git, work.cache, [
                "-c", "gc.auto=0", "fetch", "-q", remote_bundle,
                f"+HEAD:{_CACHE_REFS}{work.work_id}",
            ])
            transport.delete_remote(remote_bundle)
    for ref in work.extra.get("stale_refs", []):
        _remote_git_ok(meta, work.cache, ["update-ref", "-d", ref])
    # A local clone hardlinks the cache's objects, so no alternates outlive the cache.
    _remote_git_ok(git, work.path, [
        "clone", "--no-checkout", "-q", "--config", "core.longpaths=true",
        "--config", "core.autocrlf=false", work.cache, work.tree,
    ])
    _remote_git_ok(git, work.tree, [
        "-c", "core.longpaths=true", "checkout", "-q", "--detach", source.commit,
    ])
    for item in source.inputs:
        local = local_root.joinpath(*item["path"].split("/"))
        with local.open("rb") as handle:
            receipt = transport.materialize_input(
                handle, transport.remote_join(work.tree, item["path"]),
                expected_bytes=int(item["bytes"]), expected_sha256=item["sha256"],
            )
        if receipt.get("sha256") != "sha256:" + item["sha256"]:
            raise TransportError(f"--input {item['path']} arrived with a different SHA-256")
    transport.ensure_remote_dir(remote_cwd)
    write_manifest(rdir / "pre_remote_manifest.json", transport.manifest(
        work.tree, plan.tree_excludes, plan.hash_below_bytes, include_paths=plan.outputs,
    ))
    # Outputs as they stand locally at launch: a later local edit becomes a conflict.
    write_manifest(rdir / "pre_local_manifest.json", build_manifest(
        local_root, plan.tree_excludes, hash_below_bytes=plan.hash_below_bytes or None,
        include_paths=plan.outputs, within=plan.outputs,
    ))


# --- finalization -----------------------------------------------------------------

@dataclass
class CommitFinal:
    post: PullbackResult
    scratch: list[str]
    unexpected: list[str]
    work: dict[str, Any]
    # Symlinks or special files at or under an output, on either side: held, not pulled.
    unsupported: list[str] = field(default_factory=list)

    @property
    def unresolved(self) -> bool:
        """Something the pullback could not return; the run exits 2."""
        return bool(self.unexpected or self.unsupported)

    @property
    def undelivered(self) -> list[str]:
        """Output paths not cleanly delivered into the project; they keep the lease.

        Unsupported entries, plus every pullback conflict: a case-fold collision
        (``out/A.txt`` beside ``out/a.txt`` on a case-insensitive controller) saves
        neither copy, so only the retained tree still holds them.
        """
        return sorted({*self.unsupported, *self.post.conflicts})


def finalize_commit_outputs(
    *,
    transport: BaseTransport,
    plan: RunPlan,
    run_id: str,
    rdir: Path,
    work: dict[str, Any],
    policy: RetentionPolicy,
    command_clean: bool,
    command_note: str,
    report: Callable[..., None],
) -> CommitFinal:
    """Pull back the declared outputs and judge every other write.

    A write outside the outputs that the project's transfer excludes cover is scratch,
    counted and discarded; any other is unexpected: reported, never pulled, and it
    keeps the tree. A symlink or special file at or under an output, in the tree or
    locally, is unsupported: manifests cannot see it, so its path is held (neither
    pulled nor mirrored as a deletion) and it keeps the tree. Only a clean command, a
    conflict-free verified pullback and neither kind of write leave the work
    ``pending_removal`` (see ``remove_commit_work``); otherwise it is ``retained`` with
    the reason. ``TransportError`` from the pullback propagates with the tree retained.
    """
    pre_paths = (rdir / "pre_remote_manifest.json", rdir / "pre_local_manifest.json")
    if not all(path.is_file() for path in pre_paths):
        raise TransportError(
            "the commit run's launch manifests are missing; its outputs cannot be attributed"
        )
    registry = {"work_id": work["id"], "path": work["path"]}
    try:
        # A vanished tree would read as every file deleted; never attribute that.
        if not transport.remote_path_exists(work["tree"]):
            raise TransportError(f"the run's private tree {work['tree']} is gone")
        try:
            local_odd = nonregular_paths(plan.project.local_project_root, plan.outputs)
        except ManifestError as exc:
            raise TransportError(str(exc)) from exc
        unsupported = sorted({
            *transport.nonregular_paths(work["tree"], plan.outputs), *local_odd,
        })
        post = postrun_pullback(
            transport=transport,
            local_root=plan.project.local_project_root,
            remote_root=work["tree"],
            excludes=plan.tree_excludes,
            hash_below_bytes=plan.hash_below_bytes,
            pre_remote_manifest=read_manifest(pre_paths[0]),
            pre_local_manifest=read_manifest(pre_paths[1]),
            backup_root=conflict_dir(run_id) / "backup",
            conflict_remote_root=conflict_dir(run_id) / "remote",
            backup_below_bytes=policy.backup_below_bytes,
            # Everything under a declared output returns, global excludes included
            # (`results/x.tmp` under `--output results/`): the caller named the whole
            # directory, and the tree holding anything left out is deleted after a
            # clean run. remrun writes nothing of its own under the outputs in the
            # private tree, so no scratch name needs keeping out there.
            include_paths=plan.outputs,
            within=plan.outputs,
            held=unsupported,
        )
    except TransportError as exc:
        _retain(plan, run_id, registry, f"pullback failed: {exc}", report)
        raise
    write_manifest(rdir / "post_remote_manifest.json", post.post_remote_manifest)
    scratch = [path for path in post.outside if should_exclude(path, plan.excludes)]
    unexpected = [path for path in post.outside if not should_exclude(path, plan.excludes)]
    report("work_pulled", pulled=len(post.pulled), deleted_local=len(post.deleted_local),
           conflicts=len(post.conflicts))
    if scratch:
        report("scratch_writes", count=len(scratch), note="excluded paths; discarded")
    if unexpected:
        report("unexpected_writes", count=len(unexpected), paths=unexpected[:_LISTED_PATHS],
               note="written outside --output; left in the retained private tree")
    if unsupported:
        report("unsupported_outputs", count=len(unsupported),
               paths=unsupported[:_LISTED_PATHS],
               note="symlink or special file at or under --output, in the private tree or "
                    "locally: not returned, its local counterpart left as it is; the "
                    "private tree is retained")
    reasons = [command_note] if not command_clean else []
    if post.conflicts:
        reasons.append(f"{len(post.conflicts)} output conflict(s)")
    if unexpected:
        reasons.append(f"{len(unexpected)} unexpected write(s)")
    if unsupported:
        reasons.append(f"{len(unsupported)} unsupported output(s) (symlink or special file)")
    reason = "; ".join(reasons) or None
    state = (
        _retain(plan, run_id, registry, reason, report) if reason else "pending_removal"
    )
    return CommitFinal(post=post, scratch=scratch, unexpected=unexpected, work={
        **work, "state": state, "reason": reason,
        "scratch_writes": len(scratch), "unexpected_writes": len(unexpected),
        "unexpected_paths": unexpected[:_LISTED_PATHS],
        "unsupported_paths": unsupported[:_LISTED_PATHS],
    }, unsupported=unsupported)


def remove_commit_work(
    transport: BaseTransport, target: str, work: dict[str, Any],
    report: Callable[..., None],
) -> dict[str, Any]:
    """Remove a finalized clean tree (or retry a failed removal); else change nothing."""
    if work.get("state") not in {"pending_removal", "remove_failed"}:
        return work
    state = remove_work(
        transport, target, work.get("id"), work.get("path"), report, tree=work.get("tree"),
    )
    if state == "remove_failed":
        report("work_remove_failed", path=work["path"],
               note="retried by `remrun resume` and the next commit run on this target")
    elif state == "removed":
        report("work_removed", path=work["path"])
    return {**work, "state": state}


def _retain(
    plan: RunPlan, run_id: str, registry: dict[str, str], reason: str,
    report: Callable[..., None],
) -> str:
    record_path = _registry_path(plan.target.name, registry["work_id"])
    record = read_json(record_path) or {"version": 1, **registry, "run_id": run_id,
                                        "created_at": utc_now_iso()}
    record.update(state="retained", reason=reason, updated_at=utc_now_iso())
    write_json(record_path, record)
    report("work_retained", path=registry["path"], reason=reason)
    return "retained"


def discard_unstarted_work(
    transport: BaseTransport, plan: RunPlan, work: CommitWork, report: Callable[..., None],
) -> str:
    """Remove a tree whose command never started (a pre-dispatch refusal or failure)."""
    outcome = remove_work(
        transport, plan.target.name, work.work_id, work.path, report, tree=work.tree,
    )
    if outcome != "remove_refused":
        report("work_removed" if outcome == "removed" else "work_remove_failed",
               path=work.path, note="the command never started")
    return outcome

