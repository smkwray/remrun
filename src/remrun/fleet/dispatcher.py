"""The fleet dispatcher — drains the durable queue and runs it.

`fleet submit` enqueues jobs; nothing ran them until this. The dispatcher is the loop that
ties queue → placement → ``executor.run_batch`` together while honoring each
adapter's configured resource lease and coalescing a compatible burst into ONE
worker invocation (one cold model load — Invariant 0's only amortization).

v1 (this module): **pre-launch coalescing** only — group the jobs already waiting (plus a
short debounce window for late arrivals), place ONE compatible group, claim it all-or-
nothing (acquiring the configured resource lease when needed), run it,
complete/fail. No resident/warm model and no worker drain-loop. Output delivery is
verify-only for mapped shared-output trees; the user's sync tool or explicit sync
command delivers the files locally.
"""
from __future__ import annotations

import hashlib
import itertools
import json
import time
import uuid
import dataclasses
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .. import sync
from ..config import RemrunConfig, global_excludes, load_config
from ..output import Reporter
from ..job_observation import JobObservation, active_job_observation_enabled
from ..state import default_state_root, utc_now_iso
from . import adapters, executor, placement, probes, profiles
from .config import fleet_config, idle_grace_s as configured_idle_grace_s, load_costs, safety_fraction
from .models import DrainResultV1, FleetTask
from .queue import BatchHeartbeat, FleetQueue
from .prepared import RAW_COMMAND_SPEC_ID, as_fleet_task, prepared_memory_limit_mib
from .task_contract import resolve_tasks
from ..transport import make_transport


def _row_to_task(row: dict[str, Any], q: FleetQueue | None = None) -> FleetTask:
    if q is None:
        raise ValueError("prepared queue row requires its queue integrity reader")
    record = q.prepared_record(row["job_id"])
    spec = q.prepared_spec(row["spec_id"])
    if record is None or spec is None:
        raise ValueError(f"prepared job {row['job_id']} is missing durable semantics")
    task = as_fleet_task(record, spec)
    # The queue may clear an opted-in forced target after that target refuses
    # the work. This is placement state, not a reinterpretation of the frozen
    # submission (the original target remains inside prepared identity).
    if row.get("force_device") != task.force_device:
        task = dataclasses.replace(task, force_device=row.get("force_device"))
    if row.get("exclude_devices"):
        options = dict(task.options)
        options[EXCLUDE_DEVICES_OPT] = str(row["exclude_devices"])
        task = dataclasses.replace(task, options=options)
    return task


def _compat_key(task: FleetTask) -> tuple:
    """Jobs that can share ONE worker invocation. `run_batch` renders the command +
    output root from the FIRST job, so the key must pin everything that determines them:
    task type, forced-device constraint, effective engine, output root, the option bucket
    (which includes the classified variant), and — for `cmd` — the exact argv (each cmd
    job has its own command, so only identical-argv cmd jobs may batch). Call with the
    Resolved capability requirements are part of compatibility: jobs that need
    different target features must never be coalesced into one worker invocation."""
    if task.prepared["kind"] == "command":
        # One submission is one process invocation, even for byte-identical argv.
        return ("__command__", task.options.get("_queue_job_id") or
                task.prepared["prepared_id"] + ":" + uuid.uuid4().hex)
    definition = task.resolved_spec["definition"]
    if definition["execution"]["batching"] == "never":
        return (task.prepared["spec_id"], "never",
                task.options.get("_queue_job_id") or task.prepared["prepared_id"])
    routing = task.prepared["routing"]
    return (task.prepared["spec_id"], tuple(sorted(task.prepared["task"]["options"].items())),
            tuple(task.requires), task.engine or "",
            routing["force_device"], routing["allow_fallback"],
            task.output_root or "", task.prepared["cost"]["bucket_id"],
            prepared_memory_limit_mib(task.prepared),
            json.dumps(definition["completion"], sort_keys=True),
            str(task.options.get(EXCLUDE_DEVICES_OPT) or ""))


def _lease_until(now: str, seconds: int) -> str:
    """``now`` (utc_now_iso) + ``seconds``, in the same ISO form (string-comparable)."""
    try:
        dt = datetime.strptime(now, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        dt = datetime.now(timezone.utc)
    return (dt + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


# Phase 3d failure-backoff tuning (seconds).
_BACKOFF_BASE_S = 30.0       # transport/SSH base cooldown; doubles per attempt, capped
_BACKOFF_CAP_S = 600.0
_OOM_COOLDOWN_S = 180.0      # OOM cools the (device, engine) pair 3 min, retry elsewhere meanwhile
# A device that asked for time gets it before it is used again. Engine-scoped
# like the OOM precedent, and deliberately longer: unlike an OOM, exhausted
# scratch usually needs a person or a GC pass. It DOES hold back other jobs of
# the same engine on that box — correctly, since they share the scratch root.
_RETRY_LATER_COOLDOWN_S = 600.0
# Queue-derived placement state, carried on the in-memory task only (never in the
# submitted payload, never in the cost bucket — option_bucket uses a key allowlist).
EXCLUDE_DEVICES_OPT = "_exclude_devices"
_MAX_IDLE_GRACE_S = 240.0    # Invariant 0: never linger close to the 5-minute unload boundary
_TIMELINE_EXHAUSTIVE_GROUPS = 6


def _classify_failure(error: str, stderr: str = "", exit_code: int | None = None,
                      task: FleetTask | None = None) -> dict[str, Any]:
    """Heuristic classification of a batch failure (Phase 3d). Returns
    ``{kind, scope, cooldown_s}`` where kind ∈ transport|oom|capability|other and scope ∈
    ``device`` | ``device_engine`` | ``none``.

    **Phase-aware to avoid false cooldowns from arbitrary ``cmd`` output (audit F10):**
    *transport* is recognized ONLY from the CONTROLLER's own error string (``run_batch``'s
    ``stage failed`` / ``exec failed``, an SSH exit 255, or an explicit ``ssh:``/``unreachable``)
    — never from a worker's stderr, so a raw command that prints "connection timed out" can't
    cool the whole box. Prepared work honors unstructured OOM text only when its frozen generic
    completion policy selects ``patterns-v1``. Everything else is ``other``."""
    err = (error or "").lower()
    if (exit_code == 255 or "exec failed" in err or "stage failed" in err
            or "unreachable" in err or "ssh:" in err):
        return {"kind": "transport", "scope": "device", "cooldown_s": _BACKOFF_BASE_S}
    if "not installed" in err or "no adapter" in err:
        return {"kind": "capability", "scope": "none", "cooldown_s": 0.0}
    memory_patterns = bool(
        task is not None and task.prepared["kind"] == "task"
        and task.resolved_spec["definition"]["completion"]["unstructured_memory"]
        == "patterns-v1"
    )
    if memory_patterns:
        blob = err + "\n" + (stderr or "").lower()
        if any(s in blob for s in ("out of memory", "cuda error: out of memory",
                                   "cublas_status_alloc", "memoryerror", "oom-kill",
                                   "torch.cuda.outofmemory")):
            return {"kind": "oom", "scope": "device_engine", "cooldown_s": _OOM_COOLDOWN_S}
    return {"kind": "other", "scope": "none", "cooldown_s": 0.0}


def _bounded_idle_grace_s(config: RemrunConfig, override: float | None = None) -> float:
    """Idle linger for ``dispatch --drain``: config-driven, non-negative, capped below 5 min."""
    raw = configured_idle_grace_s(config) if override is None else float(override)
    return min(_MAX_IDLE_GRACE_S, max(0.0, raw))


def _backoff_until(now: str, base_s: float, attempts: int, *, salt: str = "") -> str:
    """Exponential backoff deadline: base · 2^(attempts-1), capped, plus 0–10% jitter so
    several controllers don't re-probe a flapping device in lockstep. Jitter is a STABLE hash
    (blake2b) over salt+now+attempts — Python's builtin ``hash`` is per-process randomized, so
    it wouldn't agree across dispatcher processes (audit F12)."""
    n = max(1, int(attempts))
    secs = min(_BACKOFF_CAP_S, base_s * (2 ** (n - 1)))
    h = int(hashlib.blake2b(f"{salt}|{now}|{n}".encode(), digest_size=4).hexdigest(), 16)
    secs += 0.1 * secs * ((h % 100) / 100.0)
    return _lease_until(now, int(secs))


def _cooled_lookup(cooled: list[dict[str, Any]]) -> dict[tuple[str, str], str]:
    return {(r["device"], r["engine"]): (r.get("reason") or "") for r in cooled}


def _is_cooled(lookup: dict[tuple[str, str], str], device: str, engine: str) -> str | None:
    """The cooldown reason if a device-wide or (device, engine) cooldown is active, else None."""
    return lookup.get((device, "")) or lookup.get((device, engine))


def _apply_cooldown(q: FleetQueue, device: str, engine: str, cls: dict[str, Any],
                    attempts: int, now: str, reporter: Reporter) -> None:
    """Record a failure-driven cooldown (Phase 3d). ``device`` scope cools the whole box
    (transport/SSH backs off with exponential growth); ``device_engine`` cools just that
    engine on that device (OOM — retry elsewhere meanwhile). ``none`` is a no-op."""
    scope = cls.get("scope", "none")
    if scope == "none" or cls.get("cooldown_s", 0.0) <= 0:
        return
    eng = engine if scope == "device_engine" else ""
    if cls["kind"] == "transport":
        until = _backoff_until(now, float(cls["cooldown_s"]), attempts, salt=f"{device}|{engine}")
    else:
        until = _lease_until(now, int(cls["cooldown_s"]))
    q.set_cooldown(device, until, engine=eng, kind=cls["kind"], reason=cls["kind"], now=now)
    reporter.event("dispatch_cooldown", device=device, engine=eng or "*",
                   kind=cls["kind"], until=until)


def _cool_if_asked(q: FleetQueue, device: str, engine: str, batch_id: str,
                   item_results: list[dict[str, Any]], reporter: Reporter) -> None:
    """``retry_later`` means the TARGET asked for time (its scratch, not this job).

    Requeueing without cooling sends the job straight back to the same exhausted
    device on the next tick and burns an attempt to learn nothing.
    """
    if not any(executor.item_disposition(r) == "retry_later"
               for r in item_results if not r.get("ok")):
        return
    _apply_cooldown(q, device, engine,
                    {"kind": "retry_later", "scope": "device_engine",
                     "cooldown_s": _RETRY_LATER_COOLDOWN_S},
                    q.batch_attempts(batch_id), utc_now_iso(), reporter)


def _learn_oom_memory(config: RemrunConfig, state_root: Path, device: str, engine: str,
                      task: FleetTask, reporter: Reporter) -> None:
    """Persist a conservative memory-estimate bump after a model OOM."""
    try:
        fcfg = fleet_config(config)
        common = {
            "memory_kind": adapters.memory_kind_for(task, device),
            "factor": float(fcfg.get("oom_memory_raise_factor", 1.25)),
            "min_delta_mb": float(fcfg.get("oom_memory_raise_min_delta_mb", 512.0)),
            "now": utc_now_iso(), "base_profiles": load_costs(config, state_root),
        }
        field, value = profiles.raise_prepared_memory_estimate(
            state_root, task, device, **common)
    except Exception as exc:  # noqa: BLE001 - report cache failure without changing run evidence
        reporter.event(
            "dispatch_oom_learning_failed", device=device, engine=engine,
            task_name=task.task_name, error=f"{type(exc).__name__}: {exc}",
        )
        return
    reporter.event("dispatch_oom_learned", device=device, engine=engine,
                   task_name=task.task_name, field=field, value_mb=value)


def _allows_fallback(task: FleetTask) -> bool:
    """Whether a forced job may retry through normal auto placement after this device fails."""
    return bool(task.force_device and task.prepared["routing"]["allow_fallback"])


def _profile_observation(tasks: list[FleetTask], device: str, result: dict[str, Any],
                         result_record: str | None) -> dict[str, Any]:
    return profiles.profile_observation(tasks, device, result, result_record)


def _has_health_patterns(dev) -> bool:  # noqa: ANN001
    cancel = getattr(dev, "cancel", {}) or {}
    for key in ("process_patterns", "wsl_process_patterns"):
        raw = cancel.get(key, [])
        if isinstance(raw, str) and raw:
            return True
        if isinstance(raw, list) and any(str(x) for x in raw):
            return True
    return False


def _excluded_devices(task: FleetTask) -> set[str]:
    """Devices that already reported they cannot serve THIS job."""
    return {d for d in str(task.options.get(EXCLUDE_DEVICES_OPT) or "").split(",") if d}


def _known_candidates(config: RemrunConfig, task: FleetTask) -> list[str]:
    """Devices this task could consider BEFORE its own exclusions are applied.

    Kept separate so a caller can tell "every candidate refused this job" from
    "there are no candidates at all", which look identical downstream and are
    opposite facts.
    """
    if task.force_device:
        return [task.force_device]
    return adapters.candidate_devices(task)


def _probe_adapter_specs(task: FleetTask, device_name: str) -> list[dict[str, Any]]:
    """Return the configured adapter to probe; intrinsic commands have none."""
    by_device = (task.resolved_spec or {}).get("adapters") or {}
    adapter = by_device.get(device_name) if isinstance(by_device, dict) else None
    return [adapter] if isinstance(adapter, dict) else []


def _candidate_names(config: RemrunConfig, task: FleetTask) -> list[str]:
    """Devices this task could consider before live probes/cooldowns.

    A device that answered with a terminal-here failure is removed permanently
    for this job. Unlike a cooldown, that answer is structural and does not heal
    on a timer, so re-offering the job would spin it on a device that cannot run
    it at all.
    """
    excluded = _excluded_devices(task)
    return [n for n in _known_candidates(config, task) if n not in excluded]


def _health_audit(config: RemrunConfig, q: FleetQueue, device_name: str, engine: str,
                  reporter: Reporter) -> bool:
    """Invariant-0 post-batch audit: configured worker processes should be gone.

    The core has no built-in worker/model names. It only uses each device's configured
    ``cancel.process_patterns`` / ``cancel.wsl_process_patterns``. If a process still matches while
    this batch's resource lease is held, kill via the configured cancel action and cool the
    device+engine briefly so placement does not immediately reuse a suspect runner.
    """
    fcfg = fleet_config(config)
    if not bool(fcfg.get("health_audit", True)):
        return False
    dev = config.devices.get(device_name)
    if dev is None:
        return False
    if not _has_health_patterns(dev):
        return False
    try:
        transport = make_transport(dev)
        if not transport.workers_running():
            reporter.event("dispatch_health_ok", device=device_name, engine=engine)
            return False
        killed = transport.kill_workers()
        now = utc_now_iso()
        q.set_cooldown(device_name, _lease_until(now, int(float(fcfg.get("health_cooldown_s", 300.0)))),
                       engine=engine, kind="health", reason="worker_leak", now=now)
        reporter.event("dispatch_health_leak", device=device_name, engine=engine,
                       killed=killed)
        return True
    except Exception as exc:  # noqa: BLE001 - health audit must not mask the batch result
        reporter.event("dispatch_health_error", device=device_name, engine=engine,
                       error=str(exc))
        return False


def _mapped_remote_root(config: RemrunConfig, device_name: str, head: FleetTask):
    """(device, tree-arg) for the batch's output root, or (device|None, None) when no
    ``[sync_roots]`` tree maps it. Single source of the reverse-map both helpers below use."""
    device = config.devices.get(device_name)
    if device is None:
        return None, None
    spec = adapters.resolve_output_root(head, device_name)
    if not spec:
        return device, None
    mapped = sync.remote_spec_to_tree(config, device, spec)
    if mapped is None:
        return device, None
    tree, sub = mapped
    return device, tree + ("/" + sub if sub else "")


def _remote_output_mtimes(config: RemrunConfig, device_name: str, head: FleetTask) -> dict[str, int]:
    """``{relpath: mtime_ns}`` for files under the batch's mapped remote output root — best-effort,
    empty on any failure / no mapped tree. Snapshotted BEFORE and AFTER the run so verification can
    require the worker to have ADDED a new file OR rewritten one (newer mtime) — not merely leave a
    non-empty root (a stale file must not mask a worker that wrote elsewhere; and a re-run that
    overwrites a same-named file must still count). mtimes are the REMOTE's own (same clock for
    pre/post), so no cross-device skew. Paths only (hash_below=0, no hashing)."""
    device, arg = _mapped_remote_root(config, device_name, head)
    if device is None or arg is None:
        return {}
    try:
        transport = make_transport(device)
        # SSH backends resolve a leading ~ from the home captured during probe(), so probe FIRST;
        # otherwise expand_remote leaves the literal config path and the manifest finds nothing.
        if not transport.probe().reachable:
            return {}
        mapping = sync.resolve_sync_paths(config, arg, device)
        remote_root = transport.remote_join(transport.expand_remote(mapping.remote_base),
                                             mapping.remote_sub)
        if not transport.remote_path_exists(remote_root):
            return {}
        manifest = transport.manifest(remote_root, list(global_excludes(config)), 0)
        return {p: int(e.mtime_ns or 0) for p, e in manifest.items()}
    except Exception:  # noqa: BLE001 - a baseline snapshot must never block a run
        return {}


def _verify_batch_output(config: RemrunConfig, device_name: str, head: FleetTask,
                         reporter: Reporter,
                         pre_output: dict[str, int] | None = None) -> dict[str, Any]:
    """VERIFY (not fetch): confirm the worker produced NEW output under the mapped
    ``[sync_roots]`` tree on the runner, then let the user's sync tool deliver
    files to the controller and other devices.

    remrun deliberately does NOT copy mapped shared-output trees itself. If it
    also writes them while a sync tool is transferring, it can race the tool's
    temp-file rename and create conflicts. Verifying by remote manifest still
    catches a worker writing to the wrong place (0 new files → final).

    Returns ``{"status": ok|skip|final, "detail": ...}``:
      * ``ok``   — new output appeared under the mapped tree (or it's a cmd job); complete.
      * ``skip`` — no ``[sync_roots]`` tree maps this output root (cmd custom root / local-sim).
      * ``final`` — the configured output contract proves a non-retryable mismatch.
    """
    pre_output = pre_output or {}
    if head.prepared["kind"] == "command":
        return {"status": "skip", "detail": "raw commands have no output contract"}
    contract = head.resolved_spec["definition"]["output"]
    if contract["verification"] == "none":
        return {"status": "skip", "detail": "frozen output contract requires no tree check"}
    device, arg = _mapped_remote_root(config, device_name, head)
    if device is None:
        return {"status": "final", "detail": f"unknown device {device_name!r}"}
    if arg is None:
        status = "final" if contract["missing_mapping"] == "final" else "skip"
        return {"status": status,
                "detail": "no [sync_roots] tree maps the frozen output root"}
    post = _remote_output_mtimes(config, device_name, head)
    produced = [path for path, mtime in post.items()
                if mtime > pre_output.get(path, -1)]
    if not produced and contract["no_change"] == "final":
        return {"status": "final",
                "detail": f"no new/updated output under {arg}"}
    return {"status": "ok", "new_files": len(produced),
            "detail": f"verified {len(produced)} new/updated file(s) under {arg}"}


def _apply_item_output_verdict(
        item_results: list[dict[str, Any]], verify: dict[str, Any],
) -> list[dict[str, Any]]:
    """Apply a controller output failure only to rows the worker called successful.

    A worker-side review verdict is durable evidence, especially when the worker
    reports that it may already have published output.  Batch-level verification
    must never erase that row by failing the whole batch.  Successful rows still
    take the task's configured final output-verification verdict, with their
    original worker evidence nested in the stored failure record.
    """
    if verify.get("status") != "final":
        return item_results
    detail = str(verify.get("detail") or "mapped output verification failed")
    resolved: list[dict[str, Any]] = []
    for row in item_results:
        if row.get("outcome") != "succeeded" and not row.get("ok"):
            resolved.append(row)
            continue
        replacement = dict(row)
        replacement.update({
            "ok": False,
            "outcome": "failed",
            "disposition": "final",
            "publication": "none",
            "outputs": [],
            "companion": None,
            "message": f"output verify: {detail}",
            "error": f"output verify: {detail}",
            "failure_code": "output_verification_failed",
            "resource": "none",
            "details": {
                "output_verify": verify,
                "worker_result": row,
            },
        })
        resolved.append(replacement)
    return resolved


def _run_claimed_batch(config: RemrunConfig, state_root: Path, claim: dict[str, Any],
                       lease_seconds: int, reporter: Reporter) -> dict[str, Any]:
    """Execute ONE already-claimed batch end-to-end: heartbeat-wrapped ``run_batch`` → 2b
    output VERIFY (sync tool delivers) → complete / fail (+ cooldown). Runs in its OWN
    ``FleetQueue`` connection so
    batches on different devices can run concurrently (a sqlite3 connection is not thread-
    shareable). Never raises — a worker death must not crash the pool; it fails the batch
    (releasing the lease) instead. Returns ``{ran, ok, failed}`` counters."""
    device, batch_id = claim["device"], claim["batch_id"]
    owner_token = claim["owner_token"]
    btasks, engine = claim["btasks"], claim["engine"]
    job_ids = claim.get("job_ids") or []
    head = btasks[0]
    db_path = state_root / "fleet" / "fleet.db"
    q = FleetQueue(db_path)
    out = {"ran": 0, "ok": 0, "failed": 0, "review": 0}
    batch_state = "leased"
    attempt_record: str | None = None
    latest_result: dict[str, Any] = {}

    def ownership_lost(stage: str) -> dict[str, Any]:
        reporter.event(
            "dispatch_ownership_lost", batch=batch_id, device=device, stage=stage,
        )
        out["ownership_lost"] = 1
        return out

    def fail_owned(error: str, **kwargs: Any) -> bool:
        kwargs.setdefault("result_record", attempt_record)
        kwargs.setdefault(
            "observation", _profile_observation(btasks, device, latest_result, attempt_record),
        )
        return q.fail_batch(
            batch_id, error, expected_state=batch_state,
            owner_token=owner_token, **kwargs,
        )

    def complete_items_owned(succeeded: dict[str, str | None],
                             failed: dict[str, str], **kwargs: Any) -> bool:
        kwargs.setdefault("result_record", attempt_record)
        kwargs.setdefault(
            "observation", _profile_observation(btasks, device, latest_result, attempt_record),
        )
        return q.complete_batch_items(
            batch_id, succeeded, failed, expected_state=batch_state,
            owner_token=owner_token, **kwargs,
        )

    try:
        if head.prepared["kind"] == "command":
            current_spec_id = RAW_COMMAND_SPEC_ID
        else:
            try:
                live_config = load_config(config.repo_root)
                current_spec_id = (resolve_tasks(live_config).get(head.task_name) or {}).get(
                    "spec_id")
            except Exception:  # noqa: BLE001 - unreadable live config is revocation
                current_spec_id = None
        if current_spec_id != head.prepared["spec_id"]:
            reason = ("definition_missing" if current_spec_id is None
                      else "definition_changed")
            if not q.revoke_prelaunch_batch(
                batch_id, owner_token=owner_token, reason=reason,
            ):
                return ownership_lost("definition_drift")
            out["failed"] = 1
            reporter.event("dispatch_definition_drift", batch=batch_id,
                           device=device, reason=reason)
            return out
        if not q.set_batch_state(
            batch_id, "staging", expected_state=batch_state,
            owner_token=owner_token,
        ):
            return ownership_lost("stage")
        batch_state = "staging"
        # run_batch (+ verify) is the slow part and holds NO DB txn. A heartbeat thread (its own
        # connection) keeps the lease fresh for the whole window (Phase 2c); a stage/render bug
        # must fail the batch (releasing its lease), not leak it.
        verify: dict[str, Any] = {"status": "skip", "detail": "run_batch did not succeed"}
        pre_output = _remote_output_mtimes(config, device, head)   # baseline the root BEFORE the run (2b)
        def frozen_launch_gate() -> bool:
            nonlocal batch_state
            if not head.prepared or head.prepared["kind"] == "command":
                current = head.prepared["spec_id"]
            else:
                try:
                    current = (resolve_tasks(load_config(config.repo_root))
                               .get(head.task_name) or {}).get("spec_id")
                except Exception:  # noqa: BLE001 - unreadable current config revokes launch
                    current = None
            if current != head.prepared["spec_id"]:
                reason = "definition_missing" if current is None else "definition_changed"
                revoked = q.revoke_prelaunch_batch(
                    batch_id, owner_token=owner_token, reason=reason,
                )
                if revoked:
                    reporter.event("dispatch_definition_drift", batch=batch_id,
                                   device=device, reason=reason)
                return False
            if not heartbeat.transition(q, "running"):
                return False
            batch_state = "running"
            reporter.event(
                "dispatch_run", batch=batch_id, device=device, jobs=len(btasks), engine=engine,
            )
            return True
        try:
            with BatchHeartbeat(
                db_path, batch_id, owner_token, batch_state, lease_seconds,
            ) as heartbeat:
                if heartbeat.ownership_lost.is_set():
                    return ownership_lost("pre_run")
                res = executor.run_batch(
                    device, btasks, config, state_root=state_root, job_ids=job_ids,
                    observation_id=batch_id, prelaunch_gate=frozen_launch_gate,
                )
            latest_result = res
            attempt_record = executor.durable_attempt_record(head, res)
            if heartbeat.ownership_lost.is_set():
                return ownership_lost("prelaunch_gate")
            if res.get("definition_drift"):
                if attempt_record is not None:
                    q.record_revoked_prelaunch_result(
                        batch_id, owner_token=owner_token,
                        result_record=attempt_record,
                    )
                out["failed"] = 1
                return out
            if res.get("ok"):
                if not q.set_batch_state(
                    batch_id, "fetching", expected_state=batch_state,
                    owner_token=owner_token,
                ):
                    out["ran"] = 1
                    return ownership_lost("fetch")
                batch_state = "fetching"
                with BatchHeartbeat(
                    db_path, batch_id, owner_token, batch_state, lease_seconds,
                ) as heartbeat:
                    if heartbeat.ownership_lost.is_set():
                        return ownership_lost("pre_verify")
                    verify = _verify_batch_output(config, device, head, reporter, pre_output)
                if heartbeat.ownership_lost.is_set():
                    out["ran"] = 1
                    return ownership_lost("verify")
        except Exception as exc:  # noqa: BLE001
            attempt_record = executor.durable_attempt_record(head, {})
            err = f"run_batch raised: {type(exc).__name__}: {exc}"
            cls = _classify_failure(err, task=head)
            if cls["kind"] == "oom":
                _learn_oom_memory(config, state_root, device, engine, head, reporter)
            _apply_cooldown(q, device, engine, cls, q.batch_attempts(batch_id),
                            utc_now_iso(), reporter)
            _health_audit(config, q, device, engine, reporter)
            fallback = _allows_fallback(head)
            if batch_state in {"running", "fetching"}:
                unknown = q.mark_completion_unknown(
                    batch_id, err, expected_state=batch_state, owner_token=owner_token,
                    result_record=attempt_record,
                    observation=_profile_observation(
                        btasks, device, latest_result, attempt_record,
                    ),
                )
                if not unknown:
                    out["ran"] = 1
                    return ownership_lost("run_exception")
                out["ran"] = out["review"] = 1
                reporter.event(
                    "dispatch_completion_unknown", batch=batch_id, device=device, error=err,
                )
                return out
            if not fail_owned(err, clear_force_device=fallback):
                out["ran"] = 1
                return ownership_lost("run_exception")
            out["ran"] = out["failed"] = 1
            if fallback:
                reporter.event("dispatch_fallback", batch=batch_id, from_device=device)
            reporter.event("dispatch_failed", batch=batch_id, device=device, error=str(exc),
                           fallback=fallback)
            return out
        out["ran"] = 1
        if (res.get("completion_state") == "unknown"
                or ("command_started" in res and res.get("command_started") is None)):
            error = res.get("error") or "completion unknown after launch authorization"
            _health_audit(config, q, device, engine, reporter)
            if not q.mark_completion_unknown(
                batch_id, error, expected_state=batch_state, owner_token=owner_token,
                result_record=attempt_record,
                observation=_profile_observation(btasks, device, latest_result, attempt_record),
            ):
                return ownership_lost("completion_unknown")
            out["review"] = 1
            reporter.event(
                "dispatch_completion_unknown", batch=batch_id, device=device, error=error,
            )
            return out
        if not res.get("ok") and res.get("no_retry"):
            error = res.get("error") or "worker completion evidence is incomplete"
            _health_audit(config, q, device, engine, reporter)
            if q.batch_replay_policy(batch_id) == "at-most-once-v1":
                if not q.mark_completion_unknown(
                    batch_id, error, expected_state=batch_state, owner_token=owner_token,
                    result_record=attempt_record,
                    observation=_profile_observation(
                        btasks, device, latest_result, attempt_record,
                    ),
                ):
                    return ownership_lost("final_failure")
                out["review"] = 1
                reporter.event(
                    "dispatch_completion_unknown", batch=batch_id, device=device, error=error,
                )
            else:
                if not fail_owned(error):
                    return ownership_lost("final_failure")
                out["failed"] = 1
                reporter.event("dispatch_failed", batch=batch_id, device=device,
                               error=error, retry_suppressed=False)
            return out
        item_results = res.get("item_results") or []
        if item_results and job_ids:
            succeeded, failed = executor.item_result_maps(item_results)
            if succeeded and not res.get("ok"):
                verify = _verify_batch_output(config, device, head, reporter, pre_output)
            item_results = _apply_item_output_verdict(item_results, verify)
            succeeded, failed = executor.item_result_maps(item_results)
            _health_audit(config, q, device, engine, reporter)
            if any((not row.get("ok") and row.get("resource") == "memory")
                   for row in item_results):
                _learn_oom_memory(config, state_root, device, engine, head, reporter)
                _apply_cooldown(
                    q, device, engine,
                    {"kind": "oom", "scope": "device_engine",
                     "cooldown_s": _OOM_COOLDOWN_S},
                    q.batch_attempts(batch_id), utc_now_iso(), reporter,
                )
            _cool_if_asked(q, device, engine, batch_id, item_results, reporter)
            fallback = _allows_fallback(head) and bool(failed)
            worker_records = executor.item_records(item_results)
            terminal_records = {
                job_id: executor.durable_attempt_record(head, res, record)
                for job_id, record in worker_records.items()
            }
            if not complete_items_owned(
                succeeded, failed,
                dispositions=executor.item_dispositions(item_results),
                results={job_id: record for job_id, record in terminal_records.items()
                         if record is not None},
                clear_force_device=fallback,
            ):
                return ownership_lost("item_completion")
            ok_items = len(succeeded)
            failed_items = max(0, len(job_ids) - ok_items)
            dispositions = executor.item_dispositions(item_results)
            review_items = sum(1 for job_id in failed if dispositions.get(job_id) == "review")
            out["ok"] = 1 if ok_items else 0
            out["review"] = 1 if review_items else 0
            out["failed"] = 1 if failed_items > review_items else 0
            if fallback:
                reporter.event("dispatch_fallback", batch=batch_id, from_device=device)
            reporter.event(
                "dispatch_done" if res.get("ok") else "dispatch_partial",
                batch=batch_id, device=device, elapsed_s=res.get("elapsed_s"),
                exit_code=res.get("exit_code"), verify=verify["status"],
                detail=verify.get("detail"), items_ok=ok_items,
                items_failed=failed_items, fallback=fallback,
            )
            return out
        if not res.get("ok"):
            cls = _classify_failure(res.get("error") or "", res.get("stderr_tail") or "",
                                    res.get("exit_code"), task=head)
            if cls["kind"] == "oom":
                _learn_oom_memory(config, state_root, device, engine, head, reporter)
            _apply_cooldown(q, device, engine, cls, q.batch_attempts(batch_id),
                            utc_now_iso(), reporter)
            # A FORCED device that's missing the capability can't be fixed by retrying it
            # A forced target missing a capability cannot heal by retrying that target.
            fallback = _allows_fallback(head)
            final = cls["kind"] == "capability" and bool(head.force_device) and not fallback
            _health_audit(config, q, device, engine, reporter)
            if not fail_owned(
                res.get("error") or f"exit {res.get('exit_code')}",
                **({"max_attempts": 1} if final else {}),
                clear_force_device=fallback,
            ):
                return ownership_lost("failure")
            out["failed"] = 1
            if fallback:
                reporter.event("dispatch_fallback", batch=batch_id, from_device=device)
            reporter.event("dispatch_failed", batch=batch_id, device=device,
                           error=res.get("error"), exit_code=res.get("exit_code"),
                           kind=cls["kind"], final=final, fallback=fallback)
        elif verify["status"] in ("ok", "skip"):
            _health_audit(config, q, device, engine, reporter)
            if not q.complete_batch(
                batch_id, expected_state=batch_state, owner_token=owner_token,
                result_record=attempt_record,
                observation=_profile_observation(btasks, device, latest_result, attempt_record),
            ):
                return ownership_lost("completion")
            out["ok"] = 1
            reporter.event("dispatch_done", batch=batch_id, device=device,
                           elapsed_s=res.get("elapsed_s"), verify=verify["status"],
                           detail=verify.get("detail"))
        else:   # "final" — worker produced no new output under the mapped tree; re-run won't fix it
            _health_audit(config, q, device, engine, reporter)
            if not fail_owned(f"output verify: {verify['detail']}", max_attempts=1):
                return ownership_lost("verify_failure")
            out["failed"] = 1
            reporter.event("dispatch_failed", batch=batch_id, device=device,
                           error=f"verify: {verify['detail']}")
        return out
    except Exception as exc:  # noqa: BLE001 - last-resort guard so one worker can't crash the pool
        if attempt_record is None:
            attempt_record = executor.durable_attempt_record(head, {})
        error = f"worker crashed: {type(exc).__name__}: {exc}"
        if batch_state in {"running", "fetching"}:
            try:
                held = q.mark_completion_unknown(
                    batch_id, error, expected_state=batch_state, owner_token=owner_token,
                    result_record=attempt_record,
                    observation=_profile_observation(
                        btasks, device, latest_result, attempt_record,
                    ),
                )
            except Exception:  # noqa: BLE001 - ownership result remains fail closed
                held = False
            if not held:
                out["ran"] = 1
                return ownership_lost("worker_exception")
            out["ran"] = out["review"] = 1
            reporter.event(
                "dispatch_completion_unknown", batch=batch_id, device=device, error=error,
            )
            return out
        try:
            failed_owned = fail_owned(error)
        except Exception:  # noqa: BLE001
            failed_owned = False
        if not failed_owned:
            out["ran"] = 1
            return ownership_lost("worker_exception")
        out["ran"] = out["failed"] = 1
        reporter.event("dispatch_failed", batch=batch_id, device=device, error=f"worker: {exc}")
        return out
    finally:
        q.close()


def _run_device_reclaim(dev, reporter: Reporter) -> bool:  # noqa: ANN001
    """Run a device's configured host-RAM reclaim command (best-effort; never raises).

    Probes first so the SSH backend can resolve a working address and expand a leading ``~`` in the
    command to the remote home. Returns True if the command was dispatched (the caller re-probes to
    measure the effect rather than trusting an exit code — tools like EmptyStandbyList may exit
    non-zero yet still free memory)."""
    cmd = (getattr(dev, "reclaim", {}) or {}).get("command")
    if not cmd:
        return False
    tokens = [str(t) for t in (cmd if isinstance(cmd, list) else [cmd])]
    try:
        transport = make_transport(dev)
        if not transport.probe().reachable:
            return False
        tokens = [transport.expand_remote(t) if t.startswith("~") else t for t in tokens]
        observed_exec = getattr(transport, "exec_observed", None)
        if not active_job_observation_enabled() or observed_exec is None:
            transport.exec(tokens, cwd=("C:\\" if dev.is_windows else "/"),
                           telemetry=False, timeout=30)
        else:
            observation = JobObservation.for_command(
                job_id=f"reclaim-{uuid.uuid4().hex[:12]}",
                project="@fleet",
                target=dev.name,
                phase="reclaim",
                command=tokens,
                declared_label="host-ram-reclaim",
            )
            observed_exec(
                tokens, cwd=("C:\\" if dev.is_windows else "/"), observation=observation,
                telemetry=False, timeout=30,
            )
        return True
    except Exception as exc:  # noqa: BLE001 - reclaim is best-effort, must not break dispatch
        reporter.event("dispatch_reclaim_error", device=dev.name, error=str(exc))
        return False


def _reclaim_marginal_devices(config: RemrunConfig, groups: list[dict[str, Any]],
                              snap_cache: dict[str, Any], lease_used: dict[str, dict[str, int]],
                              active_batches: dict[str, int], fcfg: dict, profs: dict,
                              sf: float, reporter: Reporter) -> None:
    """Before placement, free host RAM on an IDLE reclaim-capable device that a queued job would
    otherwise not fit in host RAM, then RE-PROBE so the placement that follows sees the freed RAM.

    Fires ONLY for a device that (1) configures ``[devices.*.reclaim] command``, (2) holds no
    resource lease (idle — so a running model's working set is never trimmed mid-run), and (3) is a
    candidate for some queued job whose predicted host RSS exceeds ``available * safety_fraction``.
    A no-op when no device configures reclaim, so this is opt-in and cannot change behavior for a
    fleet that doesn't ask for it. Updates ``snap_cache`` in place with the post-reclaim snapshot."""
    for name, dev in config.devices.items():
        if dev.kind == "local-sim" or not (getattr(dev, "reclaim", {}) or {}).get("command"):
            continue
        if lease_used.get(name):            # a pool lease is held -> device busy, don't disturb it
            continue
        need = 0.0                          # largest predicted host RSS among queued candidate jobs
        for g in groups:
            head = g["tasks"][0]
            if name not in _candidate_names(config, head):
                continue
            rss, _vram = placement.predicted_resources(head, name, profs)
            need = max(need, rss)
        if need <= 0.0:
            continue
        if name not in snap_cache:
            snap_cache[name] = probes.build_snapshot(
                dev, None, fcfg,
                active_jobs=active_batches.get(name, 0),
                pool_used=lease_used.get(name, {}),
                adapter_specs=[head.resolved_spec["adapters"][name]]
                if head.resolved_spec and name in head.resolved_spec["adapters"] else [])
        snap = snap_cache[name]
        free = snap.ram_free_mb
        if not snap.reachable or free is None or need <= free * sf:
            continue                        # unreachable / unknown / already fits -> no reclaim
        if _run_device_reclaim(dev, reporter):
            snap_cache[name] = probes.build_snapshot(
                dev, None, fcfg,
                active_jobs=active_batches.get(name, 0),
                pool_used=lease_used.get(name, {}),
                adapter_specs=[head.resolved_spec["adapters"][name]]
                if head.resolved_spec and name in head.resolved_spec["adapters"] else [])
            after = snap_cache[name].ram_free_mb
            reporter.event("dispatch_reclaim", device=name, need_mb=round(need),
                           before_mb=round(free), after_mb=round(after or 0.0),
                           fits=bool(after is not None and need <= after * sf))


def drain_once(config: RemrunConfig, *, state_root: Path | None = None,
               debounce_s: float = 0.0, lease_seconds: int = 300,
               reporter: Reporter | None = None, max_parallel: int | None = None,
               sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    """Run ONE dispatcher tick: place + claim every runnable group (lease/cooldown-aware), then
    execute the claimed batches CONCURRENTLY (one per device — Phase 3a). Returns a summary dict
    (never raises for normal outcomes)."""
    reporter = reporter or Reporter(json_events=False)
    state_root = state_root or default_state_root()
    fcfg = fleet_config(config)
    q = FleetQueue(state_root / "fleet" / "fleet.db")
    summary: dict[str, Any] = {"recovered": 0, "placed": 0, "ran": 0, "ok": 0,
                               "failed": 0, "review": 0, "skipped": {}, "cooled": []}
    try:
        now0 = utc_now_iso()
        summary["recovered"] = q.recover_stale(now0)
        q.prune_cooldowns(now0)

        queued = q.list("queued")
        if not queued:
            return summary
        if debounce_s > 0:                 # let a burst fill, then re-read so late arrivals join
            sleep(debounce_s)
            queued = q.list("queued")
            if not queued:
                return summary

        # Snapshot cooldowns + held leases ONCE (current after any debounce). Placement skips a
        # cooled (device,engine) and — via pool_used — a device whose configured resource
        # pool is already leased by another dispatcher or ad-hoc run.
        now = utc_now_iso()
        cooled_lookup = _cooled_lookup(q.active_cooldowns(now))
        summary["cooled"] = [f"{d}/{e or '*'}" for (d, e) in cooled_lookup]
        lease_used = q.lease_usage(now)
        active_batches = q.active_batches_by_device(now)
        # Per-device backlog (remaining compute of in-flight batches) so placement won't pile a
        # new job onto a busy-but-faster device when an idle one would finish sooner. Grows as we
        # claim batches THIS tick, so later groups see earlier groups' load on a device.
        live_backlog = q.active_backlog(now)

        # Classify FIRST (so the compat key reflects the cost regime), then build compatible
        # groups. Before claiming, simulate several group orders against a shared per-device
        # finish-time timeline. This avoids queue-order pathologies like a flexible group taking
        # the only device that a later forced/constrained group can use.
        classified = {}
        valid_queued = []
        for row in queued:
            try:
                task = _row_to_task(row, q)
            except Exception as exc:  # noqa: BLE001 - corrupt frozen bytes are durable review
                q.review_queued(
                    row["job_id"],
                    f"prepared_integrity: {type(exc).__name__}: {exc}",
                )
                reporter.event("dispatch_prepared_review", job_id=row["job_id"],
                               error=str(exc))
                continue
            valid_queued.append(row)
            options = dict(task.options)
            options["_queue_job_id"] = row["job_id"]
            classified[row["job_id"]] = dataclasses.replace(task, options=options)
        queued = valid_queued
        grouped_rows: dict[tuple, list[dict]] = {}
        for row in queued:
            grouped_rows.setdefault(_compat_key(classified[row["job_id"]]), []).append(row)
        groups: list[dict[str, Any]] = []
        all_tasks: list[FleetTask] = []
        all_job_ids: list[str] = []
        for key, rows in grouped_rows.items():
            start = len(all_tasks)
            tasks = [classified[r["job_id"]] for r in rows]
            feats = [adapters.extract_features(t) for t in tasks]
            gids = list(range(start, start + len(tasks)))
            groups.append({"key": key, "indices": gids, "tasks": tasks,
                           "features": feats, "job_ids": [r["job_id"] for r in rows]})
            all_tasks.extend(tasks)
            all_job_ids.extend(r["job_id"] for r in rows)
        # A job can end up excluded from every device that could ever run it.
        # Leaving it queued makes a zombie no tick can place; the exclusions ARE
        # the evidence that waiting changes nothing. Guarded on a non-empty
        # exclusion set so an empty/unloaded device roster never finalizes
        # anything, and fenced on 'queued' so a row another dispatcher just
        # claimed is left alone.
        for g in list(groups):
            head = g["tasks"][0]
            known = _known_candidates(config, head)
            # "No candidates" is NOT proof that every candidate refused: an empty
            # or not-yet-loaded device roster produces the same empty list, and
            # finalizing on that would terminally fail live work because the
            # controller could not see its own fleet. Require a known roster, and
            # that the exclusions account for ALL of it.
            if known and not [n for n in known if n not in _excluded_devices(head)]:
                for jid in g["job_ids"]:
                    q.finalize_queued(jid, "every candidate device reported it "
                                           "cannot serve this job")
                groups.remove(g)

        # --- place + claim phase (main thread; fast — planning + an atomic claim each) -------
        snap_cache: dict[str, Any] = {}
        claimed: list[dict[str, Any]] = []
        profs = load_costs(config, state_root)
        # Opt-in: free host RAM on an idle reclaim-capable device that a queued job would otherwise
        # not fit (e.g. a high-RAM model job vs a cache-heavy device), then the placement below
        # sees the freed RAM via snap_cache. No-op unless a device configures [devices.*.reclaim].
        _reclaim_marginal_devices(config, groups, snap_cache, lease_used, active_batches,
                                  fcfg, profs,
                                  safety_fraction(config), reporter)
        group_count = len(groups)
        if group_count <= _TIMELINE_EXHAUSTIVE_GROUPS:
            orders = itertools.permutations(range(group_count))
        else:
            def _constraint_key(i: int) -> tuple[int, float, int]:
                g = groups[i]
                head = g["tasks"][0]
                cand = _candidate_names(config, head)
                usable = 0
                best = float("inf")
                for name in cand:
                    eng = adapters.engine_for(head, name)
                    if _is_cooled(cooled_lookup, name, eng):
                        continue
                    if name not in snap_cache:
                        dev = config.devices.get(name)
                        if dev is None:
                            continue
                        snap_cache[name] = probes.build_snapshot(
                            dev, None, fcfg,
                            active_jobs=active_batches.get(name, 0),
                            pool_used=lease_used.get(name, {}),
                            adapter_specs=_probe_adapter_specs(head, name))
                    snap = snap_cache[name]
                    ok, _why = placement.fits(
                        head, name, snap, profs, safety_fraction(config), fcfg,
                        allow_unknown_capability=bool(head.force_device),
                    )
                    if ok:
                        usable += 1
                        estimate = placement.estimate_finish(
                            list(range(len(g["tasks"]))), name, g["tasks"], g["features"],
                            profs, fcfg)
                        if estimate is not None:
                            best = min(best, estimate)
                return (usable or 999, best, i)
            orders = [tuple(sorted(range(group_count), key=_constraint_key))]

        def _simulate(order: tuple[int, ...]) -> dict[str, Any]:
            used_devices: set[str] = set()
            timeline = dict(live_backlog)
            planned: list[dict[str, Any]] = []
            skipped: dict[str, str] = {}
            for gi in order:
                if max_parallel and len(planned) >= max_parallel:
                    break
                g = groups[gi]
                head = g["tasks"][0]
                snaps = {}
                for name in _candidate_names(config, head):
                    eng = adapters.engine_for(head, name)
                    why = _is_cooled(cooled_lookup, name, eng)
                    if why:
                        skipped.setdefault(name, f"cooldown: {why}")
                        continue
                    if name in used_devices:
                        skipped.setdefault(name, "device already planned this tick")
                        continue
                    if name not in snap_cache:
                        dev = config.devices.get(name)
                        if dev is None:
                            continue
                        snap_cache[name] = probes.build_snapshot(
                            dev, None, fcfg,
                            active_jobs=active_batches.get(name, 0),
                            pool_used=lease_used.get(name, {}),
                            adapter_specs=_probe_adapter_specs(head, name))
                    snaps[name] = snap_cache[name]
                if not snaps:
                    continue
                result = placement.plan_jobs(g["tasks"], g["features"], snaps, profs, fcfg,
                                             safety_fraction(config), device_backlog=timeline)
                if not result.batches:
                    skipped.update(result.skipped)
                    continue
                for batch in result.batches:
                    if max_parallel and len(planned) >= max_parallel:
                        break
                    if batch.device in used_devices:
                        continue
                    local_indices = batch.job_indices
                    global_indices = [g["indices"][i] for i in local_indices]
                    planned.append({"group": g, "batch": batch, "global_indices": global_indices})
                    used_devices.add(batch.device)
                    current = timeline.get(batch.device, 0.0)
                    if current is None or batch.estimated_finish_s is None:
                        timeline[batch.device] = None
                    else:
                        timeline[batch.device] = float(current) + batch.estimated_finish_s
            placed_jobs = sum(len(p["global_indices"]) for p in planned)
            makespan = (None if any(timeline[d] is None for d in used_devices)
                        else max((float(timeline[d]) for d in used_devices), default=0.0))
            return {"planned": planned, "skipped": skipped,
                    "placed_jobs": placed_jobs, "placed_batches": len(planned),
                    "makespan": makespan}

        best_plan: dict[str, Any] | None = None
        for order in orders:
            plan = _simulate(tuple(order))
            primary = (plan["placed_jobs"], plan["placed_batches"])
            best_primary = ((best_plan or {}).get("placed_jobs", -1),
                            (best_plan or {}).get("placed_batches", -1))
            if (best_plan is None or primary > best_primary
                    or (primary == best_primary and plan["makespan"] is not None
                        and best_plan["makespan"] is not None
                        and plan["makespan"] < best_plan["makespan"])):
                best_plan = plan
        planned_batches = (best_plan or {"planned": [], "skipped": {}})["planned"]
        summary["skipped"].update((best_plan or {"skipped": {}})["skipped"])

        for planned in planned_batches:
            if max_parallel and len(claimed) >= max_parallel:
                break       # don't claim more than will start heartbeating now (audit F4)
            batch = planned["batch"]
            global_indices = planned["global_indices"]
            btasks = [all_tasks[i] for i in global_indices]
            bjob_ids = [all_job_ids[i] for i in global_indices]
            head = btasks[0]
            batch_id = uuid.uuid4().hex[:12]
            engine = adapters.engine_for(head, batch.device)
            cnow = utc_now_iso()
            pool = adapters.pool_for(head, batch.device)
            def live_spec_ids() -> dict[str, str | None]:
                if all(task.prepared["kind"] == "command" for task in btasks):
                    return {jid: RAW_COMMAND_SPEC_ID for jid in bjob_ids}
                live_specs = resolve_tasks(load_config(config.repo_root))
                return {
                    jid: (RAW_COMMAND_SPEC_ID if task.prepared["kind"] == "command" else
                          (live_specs.get(task.task_name) or {}).get("spec_id"))
                    for jid, task in zip(bjob_ids, btasks)
                }
            owner_token = q.claim_many(
                bjob_ids, batch.device, batch_id=batch_id,
                lease_until=_lease_until(cnow, lease_seconds),
                pool=pool, task_name=head.task_name, engine=engine,
                bucket=adapters.option_bucket(head),
                estimated_finish_s=batch.estimated_finish_s, now=cnow,
                current_spec_ids=live_spec_ids,
            )
            if owner_token is None:
                reporter.event("dispatch_claim_lost", device=batch.device, jobs=len(bjob_ids))
                continue
            summary["placed"] += 1
            claimed.append({"batch_id": batch_id, "device": batch.device,
                            "owner_token": owner_token,
                            "btasks": btasks, "engine": engine,
                            "job_ids": bjob_ids,
                            "spec_id": head.prepared["spec_id"],
                            "task_name": head.task_name})

        if queued and not claimed and groups:
            reporter.event("dispatch_unplaceable", skipped=dict(summary["skipped"]))

        # --- execute phase (concurrent; one worker per claimed batch, each its own conn) -----
        if claimed:
            workers = max(1, max_parallel or len(claimed))
            with ThreadPoolExecutor(max_workers=workers) as ex:
                outs = list(ex.map(
                    lambda c: _run_claimed_batch(config, state_root, c, lease_seconds, reporter),
                    claimed))
            for o in outs:
                summary["ran"] += o["ran"]
                summary["ok"] += o["ok"]
                summary["failed"] += o["failed"]
                summary["review"] += o.get("review", 0)

        q.prune_final()
        return summary
    finally:
        q.close()


def run(config: RemrunConfig, *, state_root: Path | None = None, poll_s: float = 2.0,
        debounce_s: float = 5.0, lease_seconds: int = 300, max_ticks: int | None = None,
        until_empty: bool = False, reporter: Reporter | None = None,
        idle_grace_s: float | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic) -> DrainResultV1:
    """Continuous dispatcher: drain ticks until interrupted (or ``max_ticks`` for tests).
    Sleeps ``poll_s`` only when a tick found no work, so a busy queue drains promptly.

    ``until_empty`` (``dispatch --drain``) makes it a self-terminating supervised drain: keep going
    while any job is queued or in flight; once the queue is empty, stay alive for a bounded idle
    grace so a near-follow-up trigger can join the same lifecycle, then exit. This is still not a
    resident warm pool: no model process is kept alive by remrun, and the grace is capped below
    five minutes."""
    reporter = reporter or Reporter(json_events=False)
    sr = state_root or default_state_root()
    grace_s = 0.0
    idle_deadline: float | None = None
    ticks = 0
    totals = {"ran": 0, "ok": 0, "failed": 0, "review": 0}
    last_skipped: dict[str, str] = {}
    last_queued = 0
    last_active = 0

    def queue_counts() -> tuple[int, int]:
        queue = FleetQueue(sr / "fleet" / "fleet.db")
        try:
            return (
                int(queue.counts().get("queued", 0)),
                sum(queue.active_by_device().values()),
            )
        finally:
            queue.close()

    def best_effort_counts() -> tuple[int, int]:
        try:
            return queue_counts()
        except Exception:  # noqa: BLE001 - final result must survive a broken queue reader
            return last_queued, last_active

    try:
        grace_s = _bounded_idle_grace_s(config, idle_grace_s) if until_empty else 0.0
        reporter.event("dispatch_loop_start", poll_s=poll_s, debounce_s=debounce_s,
                       drain=until_empty, idle_grace_s=grace_s)
        while max_ticks is None or ticks < max_ticks:
            summary = drain_once(config, state_root=sr, debounce_s=debounce_s,
                                 lease_seconds=lease_seconds, reporter=reporter, sleep=sleep)
            ticks += 1
            for key in totals:
                totals[key] += int(summary.get(key, 0) or 0)
            last_skipped = dict(summary.get("skipped") or {})
            if until_empty:
                queued, active = queue_counts()
                last_queued, last_active = queued, active
                if queued + active == 0:        # nothing queued and nothing in flight -> done
                    if grace_s <= 0:
                        reporter.event("dispatch_drain_idle", waited_s=0.0)
                        return DrainResultV1(
                            status="drained", queued=0, active=0, skipped={}, error=None,
                            **totals,
                        )
                    now = monotonic()
                    if idle_deadline is None:
                        idle_deadline = now + grace_s
                        reporter.event("dispatch_drain_idle_wait", idle_grace_s=grace_s)
                    remaining = idle_deadline - now
                    if remaining <= 0:
                        reporter.event("dispatch_drain_idle", waited_s=grace_s)
                        return DrainResultV1(
                            status="drained", queued=0, active=0, skipped={}, error=None,
                            **totals,
                        )
                    sleep(min(max(poll_s, 0.001), remaining))
                    continue
                idle_deadline = None
                if active == 0 and summary["placed"] == 0 and summary["ran"] == 0:
                    reporter.event("dispatch_drain_stuck", queued=queued)
                    return DrainResultV1(
                        status="stuck_unplaceable", queued=queued, active=0,
                        skipped=last_skipped, error=None, **totals,
                    )
            if summary["ran"] == 0:
                sleep(poll_s)
    except KeyboardInterrupt:
        queued, active = best_effort_counts()
        return DrainResultV1(
            status="cancelled", queued=queued, active=active, skipped=last_skipped,
            error={"kind": "interrupted", "message": "dispatcher interrupted"}, **totals,
        )
    except Exception as exc:  # noqa: BLE001 - stable drain error document
        queued, active = best_effort_counts()
        return DrainResultV1(
            status="infrastructure_error", queued=queued, active=active,
            skipped=last_skipped,
            error={"kind": type(exc).__name__, "message": str(exc)}, **totals,
        )
    try:
        queued, active = queue_counts()
    except Exception as exc:  # noqa: BLE001 - stable drain error document
        return DrainResultV1(
            status="infrastructure_error", queued=last_queued, active=last_active,
            skipped=last_skipped,
            error={"kind": type(exc).__name__, "message": str(exc)}, **totals,
        )
    return DrainResultV1(
        status="drained" if queued + active == 0 else "stuck_unplaceable",
        queued=queued, active=active, skipped=last_skipped, error=None, **totals,
    )
