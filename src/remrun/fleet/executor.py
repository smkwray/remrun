"""Run fleet jobs: stage inputs -> run the device worker once -> record cost/memory ->
clean up. Project-less (no reconcile). ``run_batch`` runs a pre-grouped, already-placed
BURST through ONE worker invocation (one cold model load — Invariant 0's only
amortization); ``run_once`` places a single task and delegates to it. The real model run
lives in the device worker, which must honor Invariant 0 (load -> drain -> unload).

Cost-coefficient learning (separating fixed model-load from per-unit compute) needs
multiple batch sizes / a regression and is seeded from the configured/shared benchmark
profiles; here we record the directly-observed peak RSS/VRAM into the profile (which
drives the fit check) and report timing.
"""
from __future__ import annotations

import dataclasses
import json
import re
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

from ..config import RemrunConfig
from ..job_observation import JobObservation, active_job_observation_enabled
from ..state import default_state_root, iso_plus_seconds, utc_now_iso
from ..transport import GuardFinalizationError, TransportError, make_transport
from . import adapters, placement, probes, profiles
from .config import fleet_config, load_costs, safety_fraction
from .models import FleetTask
from .queue import FleetQueue

BATCH_MANIFEST_NAME = "remrun_batch.json"
BATCH_METRICS_NAME = "batch_metrics.json"
DONE_JSON_NAME = "done.json"
RESERVED_OUTPUTS_KEY = "_reserved_outputs"


def _guard_outcome_fields(memory_guard: dict[str, Any]) -> dict[str, Any]:
    """Normalize one classified guard outcome for fleet callers and queue policy."""
    status = str(memory_guard.get("status") or "unknown")
    command_started = memory_guard.get("command_started")
    if status == "ok":
        return {}
    prestart_refusal = status == "refused" and command_started is False
    phase = "memory_admission" if prestart_refusal else "memory_guard"
    if prestart_refusal:
        boundary = "before command start"
    elif command_started is True:
        boundary = "after command start"
    else:
        boundary = "with unknown command-start state"
    reason = str(memory_guard.get("reason") or "unspecified")
    detail = str(memory_guard.get("detail") or "")
    label = "memory admission" if phase == "memory_admission" else "memory guard"
    error = f"{label} {status} {boundary}: {reason}"
    if detail:
        error += f": {detail}"
    fields: dict[str, Any] = {"phase": phase, "error": error}
    # A positive or unknown start state can include user-code/output mutation.
    # Only a conclusive pre-start refusal is safe for ordinary retry/fallback.
    if not prestart_refusal:
        fields["no_retry"] = True
    return fields


def _admission_guard_payload(transport: Any, admission: Any) -> dict[str, Any]:
    """Represent controller-side target admission refusal in the guard result schema."""
    guard = transport.memory_guard
    return {
        "schema": 1,
        "status": "refused",
        "reason": admission.reason,
        "detail": admission.detail,
        "command_started": False,
        "command_exit_code": None,
        "helper_exit_code": 125,
        "max_command_bytes": None,
        "min_available_bytes": None,
        "command_limit_fraction": getattr(guard, "command_limit_fraction", None),
        "host_reserve_fraction": getattr(guard, "host_reserve_fraction", None),
        "peak_command_bytes": None,
        "min_host_available_bytes": None,
        "sample_count": 0,
        "platform": "controller",
        "memory_admission": admission.payload,
    }


def _choose_device(task: FleetTask, features, config: RemrunConfig, fcfg: dict,
                   profs: dict) -> tuple[str | None, dict[str, str]]:
    candidates = ([task.force_device] if task.force_device
                  else (adapters.supported_devices(task.task_type)
                        or list(config.devices.keys())))
    snapshots = {}
    for name in candidates:
        dev = config.devices.get(name)
        if dev is None:
            continue
        snapshots[name] = probes.build_snapshot(dev, make_transport(dev), fcfg)
    result = placement.plan_jobs([task], [features], snapshots, profs, fcfg,
                                 safety_fraction(config))
    if not result.batches:
        return None, result.skipped
    return result.batches[0].device, result.skipped


def run_once(task: FleetTask, config: RemrunConfig, *, state_root: Path | None = None,
             cleanup: bool = True, use_lease: bool = False, lease_seconds: int = 300) -> dict[str, Any]:
    """Place ONE task, then run it. A thin wrapper over ``run_batch`` (a one-job batch);
    the only extra step is choosing the device.

    ``use_lease`` (set by ``remrun fleet run``) makes the ad-hoc run acquire the same
    configured resource lease the dispatcher uses, so a manual run cannot race a
    dispatcher batch onto an exclusive resource. If the lease is held it returns
    ``lease_busy`` immediately rather than oversubscribing. The library default is
    ``use_lease=False`` (direct run — LOCAL_SIM tests, and callers that manage
    their own concurrency).
    """
    return run_group(
        [task], config, placement_task=task, state_root=state_root,
        cleanup=cleanup, use_lease=use_lease, lease_seconds=lease_seconds,
    )


def run_group(tasks: list[FleetTask], config: RemrunConfig, *,
              placement_task: FleetTask | None = None, state_root: Path | None = None,
              cleanup: bool = True, use_lease: bool = False,
              lease_seconds: int = 300) -> dict[str, Any]:
    """Place one compatible synchronous group and run it in one worker invocation.

    ``fleet run`` uses this for folder OCR/TTS: placement sees the original aggregate
    task, while execution receives one task per input so the manifest and completion
    evidence remain per-file.  The group still holds one resource lease and pays one
    model-load cost.
    """
    if not tasks:
        return {"ok": False, "error": "empty task group"}
    state_root = state_root or default_state_root()
    adapters.configure(config)
    fcfg = fleet_config(config)
    profs = load_costs(config, state_root)   # shared measured costs + local EWMA refinements
    placement_task = placement_task or tasks[0]
    classified = adapters.with_variant(placement_task, fcfg)  # classify regime pre-placement
    features = adapters.extract_features(classified)
    device_name, skipped = _choose_device(classified, features, config, fcfg, profs)
    if device_name is None:
        return {"ok": False, "error": "no eligible device", "skipped": skipped}
    if not use_lease:
        return _ad_hoc_result(
            run_batch(device_name, tasks, config, state_root=state_root, cleanup=cleanup)
        )
    return _run_group_leased(device_name, tasks, config, state_root=state_root,
                             cleanup=cleanup, lease_seconds=lease_seconds)


def _run_one_leased(device_name: str, task: FleetTask, config: RemrunConfig, *,
                    state_root: Path, cleanup: bool, lease_seconds: int) -> dict[str, Any]:
    """Run one ad-hoc job under the configured resource lease, if any.

    Fails fast with ``lease_busy`` if the resource is already leased; an ad-hoc
    failure is finalized (max_attempts=1), never left as a durable queued retry.
    """
    return _run_group_leased(
        device_name, [task], config, state_root=state_root,
        cleanup=cleanup, lease_seconds=lease_seconds,
    )


def _run_group_leased(device_name: str, tasks: list[FleetTask], config: RemrunConfig, *,
                      state_root: Path, cleanup: bool,
                      lease_seconds: int) -> dict[str, Any]:
    """Run one compatible ad-hoc group under one target resource lease."""
    task = tasks[0]
    fcfg = fleet_config(config)
    classified = adapters.with_variant(task, fcfg)
    pool = adapters.pool_for(classified, device_name)
    if not pool:
        return _ad_hoc_result(
            run_batch(device_name, tasks, config, state_root=state_root, cleanup=cleanup)
        )
    q = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        now = utc_now_iso()
        lease_until = iso_plus_seconds(now, lease_seconds)
        batch_id = uuid.uuid4().hex[:12]
        job_ids = [
            q.enqueue(dataclasses.replace(item, idempotency_key=""),
                      job_id=f"adhoc-{uuid.uuid4().hex[:12]}", now=now)
            for item in tasks
        ]
        if not q.claim_many(job_ids, device_name, batch_id=batch_id, lease_until=lease_until,
                            pool=pool, task_type=task.task_type,
                            engine=adapters.engine_for(classified, device_name),
                            bucket=adapters.option_bucket(classified), now=now):
            for job_id in job_ids:
                q.set_state(job_id, "failed_final",
                            error=f"{device_name} {pool} lease busy", now=utc_now_iso())
            return {"ok": False, "device": device_name, "lease_busy": True,
                    "error": f"{device_name} resource is busy (lease held); "
                             "use `fleet submit` to queue"}
        q.set_batch_state(batch_id, "running")
        try:
            res = run_batch(device_name, tasks, config, state_root=state_root,
                            cleanup=cleanup, job_ids=job_ids, observation_id=batch_id)
        except BaseException as exc:  # noqa: BLE001 - never leave the lease held on an error
            q.fail_batch(batch_id, f"run raised: {type(exc).__name__}: {exc}", max_attempts=1)
            raise
        if res.get("ok") and res.get("item_results"):
            succeeded, failed = item_result_maps(res["item_results"])
            q.complete_batch_items(batch_id, succeeded, failed, max_attempts=1)
        elif res.get("ok"):
            q.complete_batch(batch_id)
        else:
            q.fail_batch(batch_id, res.get("error") or f"exit {res.get('exit_code')}",
                         max_attempts=1)
        return _ad_hoc_result(res)
    finally:
        q.close()


def run_batch(device_name: str, tasks: list[FleetTask], config: RemrunConfig, *,
              state_root: Path | None = None, cleanup: bool = True,
              job_ids: list[str] | None = None,
              observation_id: str | None = None) -> dict[str, Any]:
    """Run a batch of COMPATIBLE jobs on an ALREADY-CHOSEN device with ONE worker
    invocation — so the cold model load is paid once for the whole burst (Invariant 0's
    only amortization). The caller (the dispatcher) is responsible for grouping
    compatible jobs (same task_type/engine/variant/output-root) and for placement;
    ``run_batch`` never re-places. All jobs' inputs are staged into a single input dir
    with collision-free names; the worker processes that dir once. ``run_batch`` also writes
    ``remrun_batch.json`` in the stage root and points workers to it via ``REMRUN_BATCH_MANIFEST``;
    compatible workers may emit ``batch_metrics.json`` or ``done.json`` (same stage root, stage
    input dir, or output root) to report per-item results. Workers that emit nothing preserve the
    legacy all-or-nothing behavior for commands and single-item model runs; multi-item OCR/TTS
    requires exact per-file evidence because an unattributed success cannot be retried safely.
    """
    state_root = state_root or default_state_root()
    fcfg = fleet_config(config)
    device = config.devices.get(device_name)
    if device is None:
        return {"ok": False, "error": f"unknown device {device_name!r}"}
    if not tasks:
        return {"ok": False, "device": device_name, "error": "empty batch"}
    tasks = [adapters.with_variant(t, fcfg) for t in tasks]
    head = tasks[0]                                       # the batch's compatibility key
    transport = make_transport(device)

    configured_output_root = adapters.resolve_output_root(head, device_name)
    output_error = _output_root_error(configured_output_root, device)
    if output_error:
        return {"ok": False, "device": device_name, "phase": "output_root",
                "error": output_error}

    try:
        stage = transport.remote_temp_dir("fleet")
    except TransportError as exc:
        return {"ok": False, "device": device_name, "error": f"stage failed: {exc}"}
    stage_in = transport.native_join(stage, "in")
    transport.ensure_remote_dir(stage_in)

    # --- stage every job's inputs into ONE dir, with collision-free basenames so two
    #     jobs that share an input name don't clobber each other's output --------------
    used: set[str] = set()
    staged = 0
    item_manifest: list[dict[str, Any]] = []
    try:
        for idx, t in enumerate(tasks):
            staged_names: list[str] = []
            staged_outputs: list[dict[str, str]] = []
            reservations = _reservation_list(t)
            if t.text is not None:
                # Name the staged text file from the text itself, so generated
                # outputs are content-derived instead of a generic "clip" stem.
                # File inputs keep their own names (staged below).
                stem = _reservation_stem(reservations, 0) if t.task_type in ("ocr", "tts") else None
                name = _unique_name((stem or _text_slug(t.text)) + ".txt", used)
                with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False,
                                                 encoding="utf-8") as tf:
                    tf.write(t.text)
                    local_clip = Path(tf.name)
                try:
                    transport.push_file(local_clip, transport.native_join(stage_in, name))
                    staged += 1
                    staged_names.append(name)
                    staged_outputs.append({"staged": name, "stem": Path(name).stem})
                finally:
                    local_clip.unlink(missing_ok=True)
            else:
                for input_idx, p in enumerate(t.inputs):
                    lp = Path(p)
                    if lp.is_file():
                        stem = _reservation_stem(reservations, input_idx) \
                            if t.task_type in ("ocr", "tts") else None
                        staged_name = f"{stem}{lp.suffix}" if stem else lp.name
                        name = _unique_name(staged_name, used)
                        transport.push_file(lp, transport.native_join(stage_in, name))
                        staged += 1
                        staged_names.append(name)
                        staged_outputs.append({"staged": name, "source": str(lp),
                                               "stem": Path(name).stem})
            item_manifest.append({
                "index": idx,
                "job_id": job_ids[idx] if job_ids and idx < len(job_ids) else None,
                "task_type": t.task_type,
                "staged": staged_names,
                "reserved_outputs": staged_outputs,
                "reserved_output_stem": staged_outputs[0]["stem"] if staged_outputs else None,
                "units": adapters.extract_features(t).units(t.task_type),
            })
    except TransportError as exc:
        if cleanup:
            _safe_delete(transport, stage)
        return {"ok": False, "device": device_name, "staged": staged,
                "error": f"stage failed: {exc}"}

    # Adapter output roots and worker paths may use ``~``. exec/ensure_remote_dir
    # shlex-quote their arguments, so the remote shell never tilde-expands them —
    # expand ``~`` here via the remote home.
    output_root = transport.expand_remote(configured_output_root or stage)
    transport.ensure_remote_dir(output_root)
    manifest_path = transport.native_join(stage, BATCH_MANIFEST_NAME)
    metrics_path = transport.native_join(stage, BATCH_METRICS_NAME)
    done_path = transport.native_join(stage, DONE_JSON_NAME)
    try:
        _push_json(transport, manifest_path, {
            "version": 1,
            "task_type": head.task_type,
            "device": device_name,
            "engine": adapters.engine_for(head, device_name),
            "stage_in": stage_in,
            "output_root": output_root,
            "items": item_manifest,
        })
    except TransportError as exc:
        if cleanup:
            _safe_delete(transport, stage)
        return {"ok": False, "device": device_name, "staged": staged,
                "error": f"stage failed: {exc}"}
    command = adapters.render_command(head, device_name, stage_in, output_root)
    command = [transport.expand_remote(x) if x.startswith("~") else x for x in command]

    exec_env = {
        "REMRUN_BATCH_MANIFEST": manifest_path,
        "REMRUN_BATCH_METRICS": metrics_path,
        "REMRUN_DONE_JSON": done_path,
        "REMRUN_STAGE_IN": stage_in,
        "REMRUN_OUTPUT_ROOT": output_root,
    }
    # Size the memory guard to what this batch actually needs. Left to itself the
    # transport reserves the device's configured MAXIMUM as an unprofiled ceiling,
    # because only the ordinary `remrun run` path passes a prediction. That ceiling
    # is a fraction of total RAM, not a measured need, so a worker with a ~5 GB peak
    # claims ~20 GB on a 64 GiB box and is refused whenever the device cannot also
    # preserve its host reserve. The fleet already knows this batch's peak RSS
    # (seeded in fleet_costs.toml, refined by the local EWMA store), and a batch is
    # ONE worker invocation paying ONE cold model load (Invariant 0), so the head
    # task's profile is the whole batch's figure. A missing/zero profile stays None
    # and keeps the conservative unprofiled ceiling.
    reservation = None
    if getattr(transport, "memory_guard", None) is not None:
        predicted_rss_mb = placement.predicted_resources(
            head, device_name, load_costs(config, state_root))[0]
        admission = transport.reserve_memory_guard(
            predicted_rss_mb=predicted_rss_mb or None)
        if not admission.admitted:
            if cleanup:
                _safe_delete(transport, stage)
            memory_guard = _admission_guard_payload(transport, admission)
            return {
                "ok": False,
                "device": device_name,
                "staged": staged,
                "memory_guard": memory_guard,
                **_guard_outcome_fields(memory_guard),
            }
        reservation = admission.reservation

    t0 = time.monotonic()
    try:
        observed_exec = getattr(transport, "exec_observed", None)
        if not active_job_observation_enabled() or observed_exec is None:
            res = transport.exec(command, cwd=stage, telemetry=True, env=exec_env,
                                 memory_reservation=reservation)
        else:
            observed_id = observation_id
            if not observed_id:
                observed_id = (job_ids[0] if job_ids and len(job_ids) == 1
                               else f"fleet-{uuid.uuid4().hex[:12]}")
            observation = JobObservation.for_command(
                job_id=observed_id,
                project="@fleet",
                target=device_name,
                phase="fleet-worker",
                command=command,
                declared_label=f"{head.task_type}:{adapters.engine_for(head, device_name)}",
                member_count=len(tasks),
            )
            res = observed_exec(
                command, cwd=stage, telemetry=True, env=exec_env, observation=observation,
                memory_reservation=reservation
            )
    except GuardFinalizationError as exc:
        prestart = exc.command_started is False
        # A true/unknown start may still be using staged inputs or writing an
        # output_root that is the stage itself. Preserve that evidence instead of
        # deleting under a possibly live or partially completed workload.
        cleanup_deferred = bool(cleanup and not prestart)
        if cleanup and prestart:
            _safe_delete(transport, stage)
        result = {
            "ok": False,
            "device": device_name,
            "staged": staged,
            "phase": ("memory_admission" if prestart else "memory_guard"),
            "completion_state": ("not_started" if prestart else "unknown"),
            "command_started": exc.command_started,
            "error": f"exec failed: {exc}",
        }
        if exc.memory_guard is not None:
            result["memory_guard"] = exc.memory_guard
        if cleanup_deferred:
            result["cleanup_deferred"] = True
            result["stage_dir"] = stage
        if not prestart:
            result["no_retry"] = True
        return result
    except TransportError as exc:
        if cleanup:
            _safe_delete(transport, stage)
        return {"ok": False, "device": device_name, "staged": staged,
                "error": f"exec failed: {exc}"}
    elapsed = round(time.monotonic() - t0, 3)
    batch_metrics = _read_worker_metrics(transport, stage, stage_in, output_root)
    item_results = _normalize_item_results(batch_metrics, job_ids or [])
    evidence_error = (
        _per_item_evidence_error(head, item_manifest, item_results)
        if res.exit_code == 0 else None
    )
    completion_evidence = None
    if len(tasks) > 1 and head.task_type in ("ocr", "tts"):
        completion_evidence = "missing" if evidence_error else "complete"

    # Record observed memory into the profile (drives future fit checks) — ONLY on a
    # SUCCESSFUL run (a failed worker often dies before loading the model, so its tiny
    # peak RSS/VRAM would corrupt the EWMA and make the memory-fit gate under-count).
    tel = res.telemetry or {}
    if res.exit_code == 0 and not evidence_error:
        try:
            # Total units the worker actually processed this batch (pages for OCR, kchar for TTS)
            # so the profile can regress fixed-load vs per-unit time from `elapsed` (= worker wall
            # time: one cold load + per-unit compute). None on any failure -> no time observation.
            try:
                units = sum(adapters.extract_features(t).units(head.task_type) for t in tasks)
            except Exception:  # noqa: BLE001
                units = None
            profiles.update_profile(
                state_root, head.task_type, adapters.engine_for(head, device_name),
                device_name, adapters.option_bucket(head),
                peak_rss_mb=tel.get("peak_rss_mb"), peak_vram_mb=tel.get("peak_vram_mb"),
                observed_elapsed_s=elapsed, observed_units=units, now=utc_now_iso())
        except Exception:  # noqa: BLE001 - telemetry must not break a run
            pass

    if cleanup:
        _safe_delete(transport, stage)

    worker_ok = res.exit_code == 0
    result = {
        "ok": worker_ok and not evidence_error, "device": device_name,
        "engine": adapters.engine_for(head, device_name),
        "exit_code": res.exit_code, "elapsed_s": elapsed, "staged": staged, "jobs": len(tasks),
        "output_root": output_root, "telemetry": res.telemetry,
        "batch_metrics": batch_metrics, "item_results": item_results,
        "stdout_tail": (res.stdout or "")[-500:], "stderr_tail": (res.stderr or "")[-500:],
    }
    if res.memory_guard is not None:
        result["memory_guard"] = res.memory_guard
        result.update(_guard_outcome_fields(res.memory_guard))
    if completion_evidence is not None:
        result["completion_evidence"] = completion_evidence
    if evidence_error:
        result["error"] = evidence_error
        # User code exited successfully, but its per-file attribution is insufficient.
        # Retrying could duplicate outputs, so callers must finalize this attempt.
        result["no_retry"] = True
    return result


def _ad_hoc_result(result: dict[str, Any]) -> dict[str, Any]:
    """For synchronous runs, any explicit per-item failure fails the command."""
    item_results = result.get("item_results") or []
    if result.get("ok") and any(not item.get("ok") for item in item_results):
        result = dict(result)
        result["ok"] = False
        result["error"] = "one or more batch items failed"
    return result


def item_result_maps(item_results: list[dict[str, Any]]) -> tuple[dict[str, str | None],
                                                                  dict[str, str]]:
    """Convert normalized worker items into queue completion maps."""
    succeeded: dict[str, str | None] = {}
    failed: dict[str, str] = {}
    for item in item_results:
        jid = item.get("job_id")
        if not jid:
            continue
        if item.get("ok"):
            succeeded[jid] = json.dumps(item, sort_keys=True)
        else:
            failed[jid] = item.get("error") or "worker reported item failure"
    return succeeded, failed


def _output_root_error(output_root: str | None, device) -> str | None:  # noqa: ANN001
    """Reject a controller-expanded absolute path that cannot name a target path."""
    if not output_root or output_root.startswith("~"):
        return None
    windows_absolute = bool(re.match(r"^[A-Za-z]:[\\/]", output_root)) \
        or output_root.startswith("\\\\")
    posix_absolute = output_root.startswith("/")
    if device.is_windows and posix_absolute:
        return (f"output root {output_root!r} is a POSIX path but {device.name} is a "
                "Windows target; use a target-native path")
    if not device.is_windows and windows_absolute:
        return (f"output root {output_root!r} is a Windows path but {device.name} is a "
                "POSIX target; pass a target-native path (quote '~' on the controller)")
    return None


def _per_item_evidence_error(head: FleetTask, manifest: list[dict[str, Any]],
                             results: list[dict[str, Any]]) -> str | None:
    """Require exact staged-file attribution for multi-item OCR/TTS success."""
    if len(manifest) <= 1 or head.task_type not in ("ocr", "tts"):
        return None
    by_job = {row.get("job_id"): row for row in results if row.get("job_id")}
    by_index = {row.get("index"): row for row in results
                if isinstance(row.get("index"), int)}
    if len(results) != len(manifest):
        return (f"worker succeeded without complete per-file completion evidence "
                f"({len(results)}/{len(manifest)} items); outputs were not retried")
    for expected in manifest:
        row = by_job.get(expected.get("job_id")) if expected.get("job_id") else None
        row = row or by_index.get(expected["index"])
        if row is None:
            return ("worker succeeded without complete per-file completion evidence; "
                    f"manifest item {expected['index']} is missing")
        if not row.get("ok"):
            continue
        expected_stem = expected.get("reserved_output_stem")
        outputs = row.get("outputs")
        if not expected_stem or not isinstance(outputs, list) or not any(
            _portable_stem(value) == expected_stem for value in outputs
        ):
            return ("worker per-file completion evidence does not match staged item "
                    f"{expected['index']} ({expected_stem or 'unknown stem'})")
    return None


def _portable_stem(value: object) -> str:
    """Filename stem for target paths, independent of controller path syntax."""
    name = re.split(r"[\\/]", str(value))[-1]
    return name.rsplit(".", 1)[0]


def _text_slug(text: str, maxlen: int = 30) -> str:
    """A filesystem-safe stem from inline text, for naming an inline-text output:
    collapse whitespace, take the first ``maxlen`` chars, keep letters/digits/hyphens (so
    'downward-sloping' stays hyphenated, not merged), spaces -> underscores, trim stray
    separators. Falls back to ``clip`` if empty."""
    s = re.sub(r"\s+", " ", (text or "").strip())[:maxlen]
    s = re.sub(r"[^0-9A-Za-z -]+", "", s).strip().replace(" ", "_").strip("_-")
    return s or "clip"


def _unique_name(name: str, used: set[str]) -> str:
    """A staged basename not already in ``used`` (suffix ``--N`` before the extension on
    collision), so batched jobs with the same input name can't overwrite each other."""
    if name not in used:
        used.add(name)
        return name
    stem, dot, ext = name.rpartition(".")
    base, suffix = (stem, "." + ext) if dot else (name, "")
    i = 1
    while f"{base}--{i}{suffix}" in used:
        i += 1
    chosen = f"{base}--{i}{suffix}"
    used.add(chosen)
    return chosen


def _reservation_list(task: FleetTask) -> list[dict[str, str]]:
    raw = task.options.get(RESERVED_OUTPUTS_KEY, [])
    if not isinstance(raw, list):
        return []
    return [r for r in raw if isinstance(r, dict) and r.get("stem")]


def _reservation_stem(reservations: list[dict[str, str]], idx: int) -> str | None:
    if 0 <= idx < len(reservations):
        stem = str(reservations[idx].get("stem") or "")
        return stem or None
    return None


def _push_json(transport, remote_path: str, payload: dict[str, Any]) -> None:  # noqa: ANN001
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tf:
        json.dump(payload, tf, sort_keys=True)
        local = Path(tf.name)
    try:
        transport.push_file(local, remote_path)
    finally:
        local.unlink(missing_ok=True)


def _pull_json(transport, remote_path: str) -> dict[str, Any] | None:  # noqa: ANN001
    try:
        if not transport.remote_path_exists(remote_path):
            return None
        with tempfile.NamedTemporaryFile("w+b", suffix=".json", delete=False) as tf:
            local = Path(tf.name)
        try:
            transport.pull_file(remote_path, local)
            data = json.loads(local.read_text(encoding="utf-8"))
        finally:
            local.unlink(missing_ok=True)
        return data if isinstance(data, dict) else None
    except (OSError, ValueError, TransportError):
        return None


def _read_worker_metrics(transport, stage: str, stage_in: str, output_root: str) -> dict[str, Any] | None:  # noqa: ANN001
    """Read optional worker metrics from the stage root, input dir, or output root.

    New workers should prefer ``REMRUN_BATCH_METRICS`` (stage root). Stage input and output-root
    fallbacks make the contract forgiving for shell scripts that only know their input/output args.
    """
    for root in (stage, stage_in, output_root):
        for name in (BATCH_METRICS_NAME, DONE_JSON_NAME):
            data = _pull_json(transport, transport.native_join(root, name))
            if data is not None:
                return data
    return None


def _normalize_item_results(metrics: dict[str, Any] | None,
                            job_ids: list[str]) -> list[dict[str, Any]]:
    """Normalize worker ``items``/``jobs``/``results`` entries into dispatcher-friendly rows."""
    if not metrics:
        return []
    raw_items = metrics.get("items") or metrics.get("jobs") or metrics.get("results") or []
    if not isinstance(raw_items, list):
        return []
    out: list[dict[str, Any]] = []
    known = set(job_ids)
    for i, item in enumerate(raw_items):
        if not isinstance(item, dict):
            continue
        jid = item.get("job_id")
        if not jid:
            idx = item.get("index", item.get("input_index", i))
            if isinstance(idx, int) and 0 <= idx < len(job_ids):
                jid = job_ids[idx]
        if job_ids and jid not in known:
            continue
        status = str(item.get("status", "")).lower()
        ok = item.get("ok")
        if isinstance(ok, bool):
            item_ok = ok
        elif status in ("ok", "done", "success", "succeeded", "skipped"):
            item_ok = True
        elif status in ("fail", "failed", "error"):
            item_ok = False
        else:
            continue
        out.append({
            "job_id": jid,
            "index": item.get("index", i),
            "ok": item_ok,
            "error": str(item.get("error") or item.get("message") or ""),
            "units": item.get("units"),
            "elapsed_s": item.get("elapsed_s"),
            "outputs": item.get("outputs") or item.get("output_paths") or [],
            "metrics": item,
        })
    return out


def _safe_delete(transport, remote_dir: str) -> None:
    # The stage is a directory; use the recursive tree remove (delete_remote is
    # file-only). Best-effort — cleanup must never fail a run.
    try:
        transport.remove_remote_tree(remote_dir)
    except (TransportError, NotImplementedError, Exception):  # noqa: BLE001
        pass
