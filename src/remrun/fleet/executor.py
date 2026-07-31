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
from ..transport import TransportError, make_transport
from . import adapters, placement, probes, profiles
from .config import fleet_config, load_costs, safety_fraction
from .models import FleetTask
from .queue import FleetQueue

BATCH_MANIFEST_NAME = "remrun_batch.json"
BATCH_METRICS_NAME = "batch_metrics.json"
DONE_JSON_NAME = "done.json"
RESERVED_OUTPUTS_KEY = "_reserved_outputs"


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
    state_root = state_root or default_state_root()
    adapters.configure(config)
    fcfg = fleet_config(config)
    profs = load_costs(config, state_root)   # shared measured costs + local EWMA refinements
    classified = adapters.with_variant(task, fcfg)        # classify regime pre-placement
    features = adapters.extract_features(classified)
    device_name, skipped = _choose_device(classified, features, config, fcfg, profs)
    if device_name is None:
        return {"ok": False, "error": "no eligible device", "skipped": skipped}
    if not use_lease:
        return run_batch(device_name, [task], config, state_root=state_root, cleanup=cleanup)
    return _run_one_leased(device_name, task, config, state_root=state_root,
                           cleanup=cleanup, lease_seconds=lease_seconds)


def _run_one_leased(device_name: str, task: FleetTask, config: RemrunConfig, *,
                    state_root: Path, cleanup: bool, lease_seconds: int) -> dict[str, Any]:
    """Run one ad-hoc job under the configured resource lease, if any.

    Fails fast with ``lease_busy`` if the resource is already leased; an ad-hoc
    failure is finalized (max_attempts=1), never left as a durable queued retry.
    """
    fcfg = fleet_config(config)
    classified = adapters.with_variant(task, fcfg)
    pool = adapters.pool_for(classified, device_name)
    if not pool:
        return run_batch(device_name, [task], config, state_root=state_root, cleanup=cleanup)
    q = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        now = utc_now_iso()
        lease_until = iso_plus_seconds(now, lease_seconds)
        batch_id = uuid.uuid4().hex[:12]
        adhoc = dataclasses.replace(task, idempotency_key="")   # never dedupe an ad-hoc run
        job_id = q.enqueue(adhoc, job_id=f"adhoc-{uuid.uuid4().hex[:12]}", now=now)
        if not q.claim_many([job_id], device_name, batch_id=batch_id, lease_until=lease_until,
                            pool=pool, task_type=task.task_type,
                            engine=adapters.engine_for(classified, device_name),
                            bucket=adapters.option_bucket(classified), now=now):
            q.set_state(job_id, "failed_final",
                        error=f"{device_name} {pool} lease busy", now=utc_now_iso())
            return {"ok": False, "device": device_name, "lease_busy": True,
                    "error": f"{device_name} resource is busy (lease held); "
                             "use `fleet submit` to queue"}
        q.set_batch_state(batch_id, "running")
        try:
            res = run_batch(device_name, [task], config, state_root=state_root,
                            cleanup=cleanup, job_ids=[job_id], observation_id=batch_id)
        except BaseException as exc:  # noqa: BLE001 - never leave the lease held on an error
            q.fail_batch(batch_id, f"run raised: {type(exc).__name__}: {exc}", max_attempts=1)
            raise
        if res.get("ok"):
            q.complete_batch(batch_id)
        else:
            q.fail_batch(batch_id, res.get("error") or f"exit {res.get('exit_code')}", max_attempts=1)
        return res
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
    legacy all-or-nothing behavior.
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
    output_root = transport.expand_remote(adapters.resolve_output_root(head, device_name) or stage)
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
    t0 = time.monotonic()
    try:
        observed_exec = getattr(transport, "exec_observed", None)
        if not active_job_observation_enabled() or observed_exec is None:
            res = transport.exec(command, cwd=stage, telemetry=True, env=exec_env)
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
                command, cwd=stage, telemetry=True, env=exec_env, observation=observation
            )
    except TransportError as exc:
        if cleanup:
            _safe_delete(transport, stage)
        return {"ok": False, "device": device_name, "staged": staged,
                "error": f"exec failed: {exc}"}
    elapsed = round(time.monotonic() - t0, 3)
    batch_metrics = _read_worker_metrics(transport, stage, stage_in, output_root)
    item_results = _normalize_item_results(batch_metrics, job_ids or [])

    # Record observed memory into the profile (drives future fit checks) — ONLY on a
    # SUCCESSFUL run (a failed worker often dies before loading the model, so its tiny
    # peak RSS/VRAM would corrupt the EWMA and make the memory-fit gate under-count).
    tel = res.telemetry or {}
    if res.exit_code == 0:
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

    return {
        "ok": res.exit_code == 0, "device": device_name,
        "engine": adapters.engine_for(head, device_name),
        "exit_code": res.exit_code, "elapsed_s": elapsed, "staged": staged, "jobs": len(tasks),
        "output_root": output_root, "telemetry": res.telemetry,
        "batch_metrics": batch_metrics, "item_results": item_results,
        "stdout_tail": (res.stdout or "")[-500:], "stderr_tail": (res.stderr or "")[-500:],
    }


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
