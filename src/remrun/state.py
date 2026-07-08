from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .manifest import FileEntry, Manifest


def default_state_root() -> Path:
    override = os.environ.get("REMRUN_STATE_ROOT")
    if override:
        return Path(override).expanduser()
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
        return Path(base) / "remrun"
    return Path.home() / ".local" / "state" / "remrun"


def new_run_id(target: str, project_id: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_project = _safe_component(project_id)
    # Short random suffix so rapid successive runs in the same second stay unique.
    return f"{ts}-{target}-{safe_project}-{secrets.token_hex(3)}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_plus_seconds(now_iso: str, seconds: int) -> str:
    """``now_iso`` (utc_now_iso form) + ``seconds``, in the same form — so the two are
    lexicographically comparable as SQLite TEXT (used for lease deadlines)."""
    try:
        dt = datetime.strptime(now_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        dt = datetime.now(timezone.utc)
    return (dt + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_component(value: str) -> str:
    return value.replace("/", "-").replace("\\", "-")


def parse_run_timestamp(run_id: str) -> datetime | None:
    """Parse the leading ``YYYYMMDDTHHMMSSZ`` stamp from a run id."""
    try:
        return datetime.strptime(run_id[:16], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def cap_text(text: str, max_bytes: int) -> str:
    """Cap a log to ``max_bytes`` by keeping the head and tail with a marker."""
    if max_bytes <= 0:
        return text
    data = text.encode("utf-8", "replace")
    if len(data) <= max_bytes:
        return text
    half = max(1, max_bytes // 2)
    head = data[:half].decode("utf-8", "replace")
    tail = data[-half:].decode("utf-8", "replace")
    omitted = len(data) - 2 * half
    return f"{head}\n...[remrun truncated {omitted} bytes of log output]...\n{tail}"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


# --- run + conflict directories ----------------------------------------------

def run_dir(run_id: str, state_root: Path | None = None) -> Path:
    return (state_root or default_state_root()) / "runs" / run_id


def conflict_dir(run_id: str, state_root: Path | None = None) -> Path:
    return (state_root or default_state_root()) / "conflicts" / run_id


# --- manifest (de)serialization ----------------------------------------------

def manifest_to_json(manifest: Manifest) -> dict[str, Any]:
    return {
        "version": 1,
        "files": {
            path: {
                "kind": entry.kind,
                "size": entry.size,
                "mtime_ns": entry.mtime_ns,
                "sha256": entry.sha256,
            }
            for path, entry in sorted(manifest.items())
        },
    }


def manifest_from_json(data: dict[str, Any] | None) -> Manifest:
    if not data:
        return {}
    out: Manifest = {}
    for path, raw in data.get("files", {}).items():
        out[path] = FileEntry(
            path=path,
            kind=raw.get("kind", "file"),
            size=int(raw.get("size", 0)),
            mtime_ns=int(raw.get("mtime_ns", 0)),
            sha256=raw.get("sha256"),
        )
    return out


def write_manifest(path: Path, manifest: Manifest) -> None:
    write_json(path, manifest_to_json(manifest))


def read_manifest(path: Path) -> Manifest:
    return manifest_from_json(read_json(path))


# --- baselines (previous manifests, per target+project) ----------------------

def baseline_dir(target: str, project_id: str, state_root: Path | None = None) -> Path:
    root = state_root or default_state_root()
    return root / "manifests" / _safe_component(target) / _safe_component(project_id)


def read_baseline(
    target: str, project_id: str, state_root: Path | None = None
) -> tuple[Manifest | None, Manifest | None]:
    """Return (prev_local, prev_remote) baseline manifests, or (None, None) when no
    prior run has been recorded for this target+project. The pair is stored in ONE
    file (``baseline.json``) so the two sides can never be a mismatched generation; a
    legacy split ``local.json``/``remote.json`` is no longer read (treated as absent →
    re-learned, which is safe since baselines are regenerable)."""
    d = baseline_dir(target, project_id, state_root)
    data = read_json(d / "baseline.json")
    if not data:
        return None, None
    return manifest_from_json(data.get("local")), manifest_from_json(data.get("remote"))


def write_baseline(
    target: str,
    project_id: str,
    local_manifest: Manifest,
    remote_manifest: Manifest,
    state_root: Path | None = None,
) -> None:
    """Write the (local, remote) baseline as ONE atomically-replaced file so a crash or
    a concurrent writer can never leave a new local manifest beside a stale remote one
    (which the 'neither changed since baseline' branch would then wrongly bless)."""
    d = baseline_dir(target, project_id, state_root)
    write_json(d / "baseline.json", {
        "version": 1,
        "local": manifest_to_json(local_manifest),
        "remote": manifest_to_json(remote_manifest),
    })


# --- project lock (one in-place writer per project per target) ---------------

class LockError(RuntimeError):
    pass


class ProjectLock:
    """Filesystem lock keyed by project and optional write scope.

    Uses an atomic directory create. The lock lives in remrun state, never in the
    project tree. An unscoped run takes the whole-project lock and conflicts with
    every scoped run. Scoped runs also serialize with each other: scopes narrow
    post-run pullback validation, but baseline attribution is still project-wide.
    """

    def __init__(self, project_id: str, target: str, state_root: Path | None = None,
                 scope: str | None = None) -> None:
        root = state_root or default_state_root()
        project_key = hashlib.sha256(project_id.encode()).hexdigest()[:16]
        self.root = root / "locks" / "project" / project_key
        scope_name = scope or "project"
        scope_key = hashlib.sha256(scope_name.encode()).hexdigest()[:16]
        self.path = (
            self.root / "whole.lock" if scope is None
            else self.root / "scopes" / f"{scope_key}.lock"
        )
        self.guard = self.root / ".guard"
        self.project_id = project_id
        self.target = target
        self.scope = scope

    def _acquire_guard(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        deadline = time.time() + 5.0
        while True:
            try:
                self.guard.mkdir()
                return
            except FileExistsError:
                if time.time() >= deadline:
                    raise LockError(f"project {self.project_id} lock table is busy")
                time.sleep(0.05)

    def _release_guard(self) -> None:
        try:
            self.guard.rmdir()
        except OSError:
            pass

    def _lock_info(self, path: Path) -> dict[str, object]:
        return read_json(path / "info.json") or {}

    def _describe_holder(self, path: Path) -> str:
        info = self._lock_info(path)
        scope = info.get("scope") or "project"
        return (
            f"scope={scope} target={info.get('target')} "
            f"pid={info.get('pid')} since {info.get('acquired_at')}"
        )

    def _conflicting_scope_locks(self) -> list[Path]:
        scopes_dir = self.root / "scopes"
        if not scopes_dir.exists():
            return []
        return sorted(p for p in scopes_dir.glob("*.lock") if p.is_dir())

    def acquire(self) -> "ProjectLock":
        self._acquire_guard()
        try:
            whole = self.root / "whole.lock"
            if self.scope is None:
                conflicts = []
                if whole.exists():
                    conflicts.append(whole)
                conflicts.extend(self._conflicting_scope_locks())
                if conflicts:
                    raise LockError(
                        f"project {self.project_id} already locked "
                        f"({self._describe_holder(conflicts[0])})"
                    )
            else:
                conflicts = []
                if whole.exists():
                    conflicts.append(whole)
                conflicts.extend(self._conflicting_scope_locks())
                if conflicts:
                    raise LockError(
                        f"project {self.project_id} already locked "
                        f"({self._describe_holder(conflicts[0])})"
                    )

            self.path.parent.mkdir(parents=True, exist_ok=True)
            try:
                self.path.mkdir()
            except FileExistsError as exc:
                raise LockError(
                    f"project {self.project_id} already locked "
                    f"({self._describe_holder(self.path)})"
                ) from exc
            write_json(
                self.path / "info.json",
                {"project_id": self.project_id, "target": self.target,
                 "scope": self.scope or "project",
                 "pid": os.getpid(), "acquired_at": utc_now_iso()},
            )
        finally:
            self._release_guard()
        return self

    def release(self) -> None:
        info = self.path / "info.json"
        try:
            info.unlink(missing_ok=True)
            self.path.rmdir()
        except OSError:
            pass

    def __enter__(self) -> "ProjectLock":
        return self.acquire()

    def __exit__(self, *exc: object) -> None:
        self.release()


# --- retention ---------------------------------------------------------------

@dataclass(frozen=True)
class RetentionPolicy:
    """How long run state is kept (see config/defaults.toml [logging]). Lean by design —
    remrun state is regenerable and safe to delete. Ordering invariant:
    full_log_days <= failed_log_days <= summary_days (the run dir can't be deleted before
    its own heavy artifacts)."""
    full_log_days: int = 3        # heavy artifacts (logs + manifests) for ok runs
    failed_log_days: int = 7      # heavy artifacts for failed/aborted runs (debug headroom)
    summary_days: int = 7         # the lightweight summary.json (run dir) lifetime
    max_log_bytes: int = 100 * 1024 * 1024
    # Conflict/backup area (rollback snapshots — full file copies, so the main growth
    # risk). Short-lived recovery aids, capped three ways:
    backup_below_bytes: int = 50 * 1024 * 1024   # don't snapshot files larger than this (0=unlimited)
    backup_days: int = 3                           # delete conflict/backup dirs older than this
    max_backup_bytes: int = 1024 * 1024 * 1024     # hard size budget for conflicts/ (0=unlimited)


@dataclass
class PruneReport:
    runs_deleted: int = 0
    runs_trimmed: int = 0
    bytes_freed: int = 0
    details: list[str] = field(default_factory=list)


_HEAVY_NAMES = {"stdout.log", "stderr.log"}


def _is_heavy(name: str) -> bool:
    return name in _HEAVY_NAMES or name.endswith("manifest.json")


def _dir_size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def run_is_failed(summary: dict[str, Any] | None) -> bool:
    if not summary:
        return False
    if summary.get("error"):
        return True
    ec = summary.get("exit_code")
    return ec is not None and ec != 0


def prune_state(
    policy: RetentionPolicy,
    *,
    now: datetime | None = None,
    state_root: Path | None = None,
    dry_run: bool = False,
    older_than_days: int | None = None,
    keep: int | None = None,
) -> PruneReport:
    """Prune the run journal under the state root.

    Modes (first that applies wins per run):
    - ``keep=N``: protect the newest N runs, delete the rest entirely.
    - ``older_than_days=D``: delete whole run dirs older than D days.
    - otherwise the tiered ``policy``: after the heavy-retention window strip
      logs+manifests but keep summary.json; after ``summary_days`` delete the dir.
    """
    now = now or datetime.now(timezone.utc)
    root = state_root or default_state_root()
    report = PruneReport()
    runs_root = root / "runs"

    run_dirs = sorted([d for d in runs_root.iterdir() if d.is_dir()],
                      key=lambda d: d.name, reverse=True) if runs_root.exists() else []
    protected = {d.name for d in run_dirs[:keep]} if keep is not None else set()

    for d in run_dirs:
        ts = parse_run_timestamp(d.name)
        age_days = (now - ts).days if ts else None
        failed = run_is_failed(read_json(d / "summary.json"))

        delete_whole = False
        trim_heavy = False
        if keep is not None:
            delete_whole = d.name not in protected
        elif older_than_days is not None:
            delete_whole = age_days is not None and age_days > older_than_days
        else:
            heavy_days = policy.failed_log_days if failed else policy.full_log_days
            if age_days is not None and age_days > policy.summary_days:
                delete_whole = True
            elif age_days is not None and age_days > heavy_days:
                trim_heavy = True

        if delete_whole:
            report.bytes_freed += _dir_size(d)
            report.runs_deleted += 1
            report.details.append(f"delete runs/{d.name}")
            if not dry_run:
                shutil.rmtree(d, ignore_errors=True)
        elif trim_heavy:
            freed = 0
            for f in d.iterdir():
                if f.is_file() and _is_heavy(f.name):
                    freed += f.stat().st_size
                    if not dry_run:
                        f.unlink(missing_ok=True)
            if freed:
                report.bytes_freed += freed
                report.runs_trimmed += 1
                report.details.append(f"trim runs/{d.name}")

    # Conflict/backup area (rollback snapshots — full file copies). Bounded two ways:
    # (1) age: short-lived recovery aids, deleted after backup_days (NOT summary_days);
    # (2) size: a hard byte budget — if the area still exceeds max_backup_bytes after the
    #     age pass, delete oldest dirs until under it. This guarantees the rollback net
    #     can't make the state grow without bound (e.g. backing up large media).
    conflicts_root = root / "conflicts"
    if conflicts_root.exists() and keep is None:
        cutoff = older_than_days if older_than_days is not None else policy.backup_days
        pruned: set[Path] = set()  # track so the budget pass doesn't double-count (dry-run)
        for d in sorted(conflicts_root.iterdir()):
            if not d.is_dir():
                continue
            ts = parse_run_timestamp(d.name)
            age_days = (now - ts).days if ts else None
            if age_days is not None and age_days > cutoff:
                report.bytes_freed += _dir_size(d)
                report.runs_deleted += 1
                report.details.append(f"delete conflicts/{d.name}")
                pruned.add(d)
                if not dry_run:
                    shutil.rmtree(d, ignore_errors=True)

        budget = policy.max_backup_bytes
        if budget and older_than_days is None:
            remaining = [d for d in sorted(conflicts_root.iterdir())
                         if d.is_dir() and d not in pruned]
            sizes = {d: _dir_size(d) for d in remaining}
            total = sum(sizes.values())
            # Delete oldest-first (run-id names sort chronologically) until under budget.
            for d in remaining:
                if total <= budget:
                    break
                total -= sizes[d]
                report.bytes_freed += sizes[d]
                report.runs_deleted += 1
                report.details.append(f"delete conflicts/{d.name} (over backup budget)")
                if not dry_run:
                    shutil.rmtree(d, ignore_errors=True)

    return report
