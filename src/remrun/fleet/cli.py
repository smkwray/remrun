"""`remrun fleet <subcommand>` — plan / submit / status / run.

Delegated to from remrun's main CLI. Output goes to stderr as `remrun: fleet …`
events (or JSON with --json), mirroring remrun's Reporter style.
"""
from __future__ import annotations

import argparse
import json
import math
import platform
import signal
import subprocess
import sys
import threading
from pathlib import Path

from ..config import load_config
from ..output import Reporter, emit_json_document
from ..state import default_state_root
from ..target_resources import (
    TargetReservation, TargetResourceClient, TargetResourceError, canonical_json,
    policy_digest,
)
from . import adapters, dispatcher, executor, placement, probes
from .config import fleet_config, load_costs, safety_fraction
from .models import FleetTask
from .queue import FleetQueue, TaskSpecDriftRefusal
from .prepared import (
    RAW_COMMAND_SPEC, RAW_COMMAND_SPEC_ID, as_fleet_task, parse_option_assignments,
    pin_prepared_job, prepare_raw_command, prepare_task_jobs,
)
from .request_stdin import RequestStdinError, read_request_document
from .storage import StorageError, bind_device_root, enroll_local_root, load_registry
from .task_contract import resolve_tasks, task_routes_document
from ..transport import (
    TransportError, _posix_cancel_script, _powershell_cancel_script, make_transport,
)

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_STUCK = 2
EXIT_DRAIN_STOPPED = 3
EXIT_INFRA = 4
EXIT_CANCELLED = 130

# No console-window flash on Windows when invoked from a GUI trigger; 0 elsewhere.
_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def _positive_seconds(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number of seconds") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("must be finite and greater than zero")
    return seconds


def _configured_payload(
    args, definition: dict, *, task_name: str, spec_id: str,
) -> tuple[str | None, list[str]]:  # noqa: ANN001
    if getattr(args, "stdin_request", False):
        if (getattr(args, "text", None) is not None
                or getattr(args, "input", None)
                or getattr(args, "clipboard", False)):
            raise ValueError(
                "--stdin-request cannot be combined with --text, --input, or --clipboard"
            )
        try:
            request = read_request_document()
        except RequestStdinError as exc:
            raise ValueError(str(exc)) from exc
        if request["task"] != task_name:
            raise ValueError("request task does not match the configured task argument")
        if request["spec_id"] != spec_id:
            raise ValueError("request spec_id does not match the resolved task definition")
        return request["text"], []

    text = getattr(args, "text", None)
    inputs = list(getattr(args, "input", None) or [])
    if getattr(args, "clipboard", False):
        raw = _read_clipboard().strip()
        if not raw:
            raise ValueError("clipboard is empty")
        candidates = [line.strip() for line in raw.splitlines() if line.strip()]
        if candidates and all(Path(value).expanduser().exists() for value in candidates):
            inputs = candidates + inputs
        elif definition["input"]["mode"] in {"text", "text-or-files"}:
            if text is not None:
                raise ValueError("clipboard text and --text may not both supply the payload")
            text = raw
        else:
            raise ValueError("clipboard does not contain usable configured input files")
    return text, inputs


def _prepare_configured(args, config):  # noqa: ANN001
    specs = resolve_tasks(config)
    spec = specs.get(args.task_name)
    if spec is None:
        available = ", ".join(sorted(specs)) or "none"
        raise ValueError(f"unknown configured task {args.task_name!r}; available: {available}")
    text, inputs = _configured_payload(
        args, spec["definition"], task_name=spec["task_name"], spec_id=spec["spec_id"],
    )
    options = parse_option_assignments(spec["definition"], getattr(args, "opt", None))
    records = prepare_task_jobs(
        spec, repo_root=config.repo_root, text=text, inputs=inputs, options=options,
        caller_requirements=getattr(args, "require", None) or (),
        force_device=getattr(args, "device", None),
        allow_fallback=getattr(args, "allow_fallback", False),
        engine=getattr(args, "engine", None),
        output_root=getattr(args, "output_root", None),
        memory_limit_mib=getattr(args, "memory_limit_mib", None),
        storage_registry=_optional_storage_registry(default_state_root()),
        return_root=getattr(args, "return_root", None),
    )
    # Preparation may be slow. Re-resolve immediately before the caller opens
    # its queue transaction so authority/config changes insert zero rows.
    current = resolve_tasks(load_config(config.repo_root)).get(args.task_name)
    if current is None or current["spec_id"] != spec["spec_id"]:
        raise ValueError("task definition changed during preparation; no job was enqueued")
    return spec, records, [as_fleet_task(record, spec) for record in records]


def _optional_storage_registry(state_root: Path) -> dict:
    """Use shared routing only when its controller-local registry is readable."""
    try:
        return load_registry(state_root)
    except StorageError as exc:
        print(
            f"remrun: fleet storage registry unavailable; using stream route: {exc}",
            file=sys.stderr,
        )
        return {"schema": 1, "roots": {}}


def _read_clipboard() -> str:
    """The OS clipboard as text, best-effort (empty on any failure)."""
    if sys.platform == "darwin":
        cmds = [["pbpaste"]]
    elif sys.platform.startswith("win"):
        cmds = [["powershell", "-NoProfile", "-Command", "Get-Clipboard -Raw"]]
    else:
        cmds = [["wl-paste"], ["xclip", "-selection", "clipboard", "-o"]]
    for c in cmds:
        try:
            r = subprocess.run(c, capture_output=True, text=True, timeout=10,
                               creationflags=_NO_WINDOW)
            if r.returncode == 0 and r.stdout:
                return r.stdout
        except (OSError, subprocess.SubprocessError):
            continue
    return ""


def cmd_storage(args, reporter: Reporter) -> int:  # noqa: ANN001
    state_root = default_state_root()
    if args.storage_action == "enroll":
        result = enroll_local_root(state_root, Path(args.root))
    elif args.storage_action == "bind":
        config = load_config()
        device = config.devices.get(args.device)
        if device is None:
            raise ValueError(f"unknown device {args.device!r}")
        result = bind_device_root(
            state_root, args.device, args.root, make_transport(device),
        )
    else:
        result = load_registry(state_root)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        reporter.event("storage_" + args.storage_action, **result)
    return EXIT_OK


def cmd_task_interfaces(args, reporter: Reporter) -> int:  # noqa: ANN001
    """Expose resolved task/device routes without probing any device."""
    config = load_config()
    document = task_routes_document(resolve_tasks(config), config.devices)
    if args.json:
        emit_json_document(document)
    else:
        for route in document["routes"]:
            reporter.event("task_route", **route)
    return EXIT_OK


def _candidate_devices(task: FleetTask, config) -> list[str]:
    if task.force_device:
        return [task.force_device]
    return adapters.candidate_devices(task)


def _snapshots(task: FleetTask, config, fcfg, *, active_batches: dict[str, int] | None = None) -> dict:
    snaps = {}
    active_batches = active_batches or {}
    configured_adapters = (task.resolved_spec or {}).get("adapters") or {}
    for name in _candidate_devices(task, config):
        dev = config.devices.get(name)
        if dev is not None:
            snaps[name] = probes.build_snapshot(
                dev, None, fcfg,
                active_jobs=active_batches.get(name, 0),
                adapter_specs=[configured_adapters[name]]
                if name in configured_adapters else [],
            )
    return snaps


def _pool_lease_count(q: FleetQueue, task: FleetTask, device: str) -> int:
    pool = adapters.pool_for(task, device)
    if not pool:
        return 0
    return q.lease_usage().get(device, {}).get(pool, 0)


def _route_preview(task: FleetTask, config, q: FleetQueue, state_root) -> dict:
    """Best-effort routing prediction for a trigger UI: which device this task
    will LIKELY land on, and whether that device is busy. Probes devices live.
    NB: a hint, not a commitment — the dispatcher does the authoritative placement at drain time,
    and queue/lease state can change before the job is claimed."""
    fcfg = fleet_config(config)
    features = adapters.extract_features(task)
    snaps = _snapshots(
        task, config, fcfg, active_batches=q.active_batches_by_device(),
    )
    profs = load_costs(config, state_root)
    result = placement.plan_jobs([task], [features], snaps, profs, fcfg,
                                 safety_fraction(config), device_backlog=q.active_backlog())
    if not result.batches:
        # Couldn't place RIGHT NOW. If the task is FORCED to a device that's merely BUSY (another
        # job is running there, so its RAM/VRAM is temporarily taken), it's not a dead-end: the job
        # is queued and will run when that device frees. Report queued-behind-busy, not the
        # alarming "no device (insufficient RAM)" for a forced job that is just
        # waiting behind that device's current work.
        forced = task.force_device
        if forced and (q.active_by_device().get(forced, 0)
                       or _pool_lease_count(q, task, forced)):
            return {
                "device": forced, "device_busy": True,
                "active_on_device": q.active_by_device().get(forced, 0),
                "note": "queued behind a running job on this device",
                "skipped": result.skipped,
            }
        return {"device": None, "device_busy": False,
                "note": result.note or "no eligible device", "skipped": result.skipped}
    b = result.batches[0]
    active = q.active_by_device().get(b.device, 0)
    busy = active > 0 or _pool_lease_count(q, task, b.device) > 0
    return {"device": b.device, "engine": adapters.engine_for(task, b.device),
            "variant": task.options.get("_variant"), "device_busy": busy,
            "active_on_device": active, "estimated_finish_s": b.estimated_finish_s,
            "selection_basis": b.selection_basis,
            "estimate_reason": b.estimate_reason,
            "placement_explanation": b.explanation}


def _route_preview_multi(tasks: list[FleetTask], config, q: FleetQueue, state_root) -> dict:
    """Routing prediction for a MULTI-JOB submit (a folder / many files): how the jobs are
    expected to SPREAD across devices this drain. Same caveat as ``_route_preview`` — a hint, not
    a commitment; the dispatcher does the authoritative placement (and re-batching) at drain time."""
    fcfg = fleet_config(config)
    feats = [adapters.extract_features(t) for t in tasks]
    snaps = _snapshots(
        tasks[0], config, fcfg, active_batches=q.active_batches_by_device(),
    )
    profs = load_costs(config, state_root)
    result = placement.plan_jobs(tasks, feats, snaps, profs, fcfg,
                                 safety_fraction(config), device_backlog=q.active_backlog())
    by_device: dict[str, int] = {}
    for b in result.batches:
        by_device[b.device] = by_device.get(b.device, 0) + len(b.job_indices)
    placed = sum(by_device.values())
    return {"by_device": by_device, "placed": placed, "total": len(tasks),
            "unplaced": len(tasks) - placed, "skipped": result.skipped,
            "makespan_s": result.makespan_s}


def _route_line_multi(task_name: str, preview: dict, queued_total: int) -> str:
    """One concise, ASCII, prefix-free line summarizing a multi-job spread for a trigger HUD."""
    label = task_name
    total = preview.get("total", 0)
    by = preview.get("by_device") or {}
    if not by:
        skipped = preview.get("skipped") or {}
        why = "; ".join(f"{d}: {r}" for d, r in sorted(skipped.items())) or "no eligible device"
        return f"{label} x{total}: no device ({why})"
    spread = " ".join(f"{d}:{n}" for d, n in sorted(by.items(), key=lambda kv: (-kv[1], kv[0])))
    tail = f", +{preview['unplaced']} queued" if preview.get("unplaced") else ""
    return f"{label} x{total} -> {spread} (#{queued_total} queued){tail}"


def cmd_plan(args, reporter: Reporter) -> int:
    if getattr(args, "priority", 0) and not getattr(args, "save", False):
        raise ValueError("--priority on fleet plan requires --save")
    config = load_config()
    spec, records, tasks = _prepare_configured(args, config)
    fcfg = fleet_config(config)
    state_root = default_state_root()
    q = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        active = q.active_batches_by_device()
        backlog = q.active_backlog()
    finally:
        q.close()
    candidates = {name for task in tasks for name in adapters.candidate_devices(task)}
    snaps = {}
    for name in candidates:
        dev = config.devices.get(name)
        if dev is not None:
            snaps[name] = probes.build_snapshot(
                dev, None, fcfg, active_jobs=active.get(name, 0),
                adapter_specs=[task.resolved_spec["adapters"][name]
                               for task in tasks
                               if task.resolved_spec and name in task.resolved_spec["adapters"]])
    features = [adapters.extract_features(task) for task in tasks]
    result = placement.plan_jobs(tasks, features, snaps, load_costs(config, state_root),
                                 fcfg, safety_fraction(config), device_backlog=backlog)
    saved_plan = None
    if getattr(args, "save", False):
        assignments: list[str | None] = [None] * len(records)
        for batch in result.batches:
            for index in batch.job_indices:
                if index < 0 or index >= len(assignments) or assignments[index] is not None:
                    raise ValueError("planner returned duplicate or out-of-range job assignment")
                assignments[index] = batch.device
        if any(device is None for device in assignments):
            raise ValueError("cannot save a plan unless every prepared job has a selected device")
        pinned = [
            pin_prepared_job(record, str(device), spec)
            for record, device in zip(records, assignments)
        ]
        q = FleetQueue(state_root / "fleet" / "fleet.db")
        try:
            saved_plan = q.save_submission_plan(
                spec=spec,
                prepared_records=pinned,
                priority=getattr(args, "priority", 0),
                current_spec_id=lambda: (
                    (resolve_tasks(load_config(config.repo_root)).get(spec["task_name"]) or {})
                    .get("spec_id")
                ),
            )
        finally:
            q.close()
        records = pinned
    payload = {
        "task": spec["task_name"], "spec_id": spec["spec_id"],
        "prepared_ids": [record["prepared_id"] for record in records],
        "cost": [record["cost"] for record in records],
        "batches": [{"device": batch.device, "jobs": batch.job_indices,
                     "estimated_finish_s": batch.estimated_finish_s,
                     "reason": batch.reason,
                     "selection_basis": batch.selection_basis,
                     "estimate_reason": batch.estimate_reason,
                     "placement_explanation": batch.explanation}
                    for batch in result.batches],
        "makespan_s": result.makespan_s,
        "skipped": result.skipped, "note": result.note,
    }
    if saved_plan is not None:
        payload.update({
            "plan_id": saved_plan["plan_id"],
            "plan_digest": saved_plan["plan_digest"],
            "priority": saved_plan["priority"],
        })
    if records and "limits" in records[0]:
        payload["limits"] = records[0]["limits"]
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        event = {"task": spec["task_name"], "spec_id": spec["spec_id"],
                 "jobs": len(records)}
        if records and "limits" in records[0]:
            event["memory_limit_mib"] = records[0]["limits"]["process_tree_rss_mib"]
        reporter.event("fleet_task", **event)
        for batch in result.batches:
            fields = {
                "device": batch.device,
                "jobs": len(batch.job_indices),
                "estimated_finish_s": batch.estimated_finish_s,
                "reason": batch.reason,
                "selection_basis": batch.selection_basis,
                "estimate_reason": batch.estimate_reason,
            }
            if batch.estimated_finish_s is None \
                    and batch.estimate_reason == "uncalibrated":
                fields["message"] = "Uncalibrated placement; no duration estimate."
            reporter.event("placement", **fields)
        for device, reason in sorted(result.skipped.items()):
            reporter.event("skipped", device=device, reason=reason)
        if saved_plan is not None:
            reporter.event(
                "plan_saved", plan_id=saved_plan["plan_id"],
                plan_digest=saved_plan["plan_digest"], priority=saved_plan["priority"],
            )
    return EXIT_OK if result.batches else EXIT_ERROR


def _route_line(task_name: str, route: dict, will_run: bool, queued_total: int) -> str:
    """One concise, ASCII, prefix-free line for a trigger tooltip/HUD."""
    label = task_name
    dev = route.get("device")
    if not dev:
        # Surface WHY there's no device (the per-device skip reasons), not just
        # "no eligible device", so forced jobs explain why that device cannot
        # fit right now rather than returning a dead-end.
        skipped = route.get("skipped") or {}
        why = "; ".join(f"{d}: {r}" for d, r in sorted(skipped.items())) \
            or route.get("note") or "no eligible device"
        return f"{label}: no device ({why})"
    if will_run:
        return f"{label} -> {dev} - runs now"
    if route.get("device_busy"):
        return f"{label} -> {dev} - queued (#{queued_total}), resource busy"
    return f"{label} -> {dev} - queued (#{queued_total})"


def _emit_submission_refusal(
    refusal: dict, args, reporter: Reporter,  # noqa: ANN001
) -> int:
    """Emit the closed durable refusal without exposing frozen task bytes."""
    if getattr(args, "json", False):
        emit_json_document(refusal)
    else:
        reporter.event("submission_refused", reason=refusal["reason"])
    return EXIT_ERROR


def cmd_submit(args, reporter: Reporter) -> int:
    if getattr(args, "preview_route", False) and not getattr(args, "json", False):
        raise ValueError("--preview-route requires --json")
    plan_id = getattr(args, "plan_id", None)
    if plan_id:
        incompatible = {
            "task_name": getattr(args, "task_name", None),
            "require": getattr(args, "require", None) or [],
            "text": getattr(args, "text", None),
            "input": getattr(args, "input", None) or [],
            "stdin_request": getattr(args, "stdin_request", False),
            "clipboard": getattr(args, "clipboard", False),
            "device": getattr(args, "device", None),
            "engine": getattr(args, "engine", None),
            "opt": getattr(args, "opt", None) or [],
            "output_root": getattr(args, "output_root", None),
            "return_root": getattr(args, "return_root", None),
            "memory_limit_mib": getattr(args, "memory_limit_mib", None),
            "route_line": getattr(args, "route_line", False),
            "preview_route": getattr(args, "preview_route", False),
            "allow_fallback": getattr(args, "allow_fallback", False),
            "priority": bool(getattr(args, "priority", 0)),
        }
        used = sorted(name for name, value in incompatible.items() if value)
        if used:
            raise ValueError(
                "--plan consumes its frozen task and placement; incompatible arguments: "
                + ", ".join(used)
            )
        state_root = default_state_root()
        q = FleetQueue(state_root / "fleet" / "fleet.db")
        refusal = None
        try:
            plan = q.get_submission_plan(plan_id)
            if plan is None:
                raise ValueError(f"unknown saved plan {plan_id!r}")
            task_name = plan["spec"]["task_name"]

            def current_spec_id() -> str | None:
                config = load_config()
                current = resolve_tasks(load_config(config.repo_root)).get(task_name)
                return current.get("spec_id") if current else None

            try:
                receipt = q.enqueue_saved_plan(
                    plan_id,
                    request_id=getattr(args, "request_id", None),
                    current_spec_id=current_spec_id,
                )
            except TaskSpecDriftRefusal as exc:
                refusal = exc.document
            if refusal is None:
                queued_total = q.counts().get("queued", 0)
        finally:
            q.close()
        if refusal is not None:
            return _emit_submission_refusal(refusal, args, reporter)
        payload = {**receipt, "task": task_name, "queued_total": queued_total,
                   "route_preview": False}
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True, default=str))
        else:
            reporter.event(
                "submission_accepted", submission_id=receipt["submission_id"],
                plan_id=plan_id, jobs=len(receipt["job_ids"]),
                replayed=receipt["replayed"],
            )
        return EXIT_OK
    if not getattr(args, "task_name", None):
        raise ValueError("submit requires a configured task name or --plan PLAN_ID")
    config = load_config()
    spec, records, tasks = _prepare_configured(args, config)
    state_root = default_state_root()
    route_line = getattr(args, "route_line", False)
    # Enqueue is a durable local database operation. A JSON representation of
    # that result must not silently turn it into a live fleet scan. Route
    # previews remain available when a caller explicitly asks for one.
    need_route = bool(getattr(args, "preview_route", False) or route_line)
    q = FleetQueue(state_root / "fleet" / "fleet.db")
    route: dict = {}
    preview_multi = None
    will_run = False
    try:
        if need_route:
            if len(tasks) == 1:
                route = _route_preview(tasks[0], config, q, state_root)
            else:
                preview_multi = _route_preview_multi(tasks, config, q, state_root)
        def current_spec_id() -> str | None:
            current = resolve_tasks(load_config(config.repo_root)).get(args.task_name)
            return current.get("spec_id") if current else None
        receipt = q.enqueue_submission(
            records, spec=spec, priority=getattr(args, "priority", 0),
            request_id=getattr(args, "request_id", None),
            current_spec_id=current_spec_id,
        )
        jids = receipt["job_ids"]
        queued_total = q.counts().get("queued", 0) if (args.json or need_route) else 0
        if need_route and len(tasks) == 1:
            will_run = (bool(route.get("device")) and not route.get("device_busy")
                        and queued_total <= 1)
    finally:
        q.close()
    if route_line:
        print(_route_line(spec["task_name"], route, will_run, queued_total)
              if len(tasks) == 1 else
              _route_line_multi(spec["task_name"], preview_multi or {}, queued_total))
    elif args.json:
        payload = {**receipt, "task": spec["task_name"],
                   "spec_id": spec["spec_id"], "queued_total": queued_total,
                   "route_preview": need_route}
        if records and "limits" in records[0]:
            payload["limits"] = records[0]["limits"]
        if need_route:
            payload.update(route if len(tasks) == 1 else (preview_multi or {}))
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        for jid, record in zip(jids, records):
            event = {
                "job_id": jid, "task": spec["task_name"],
                "submission_id": receipt["submission_id"],
                "prepared_id": record["prepared_id"],
                "device": record["routing"]["force_device"] or "auto",
            }
            if "limits" in record:
                event["memory_limit_mib"] = record["limits"]["process_tree_rss_mib"]
            reporter.event("enqueued", **event)
    return EXIT_OK


def cmd_command(args, reporter: Reporter) -> int:
    """Plan, enqueue, or synchronously run the intrinsic raw-command primitive."""
    config = load_config()
    argv = list(getattr(args, "argv", None) or [])
    if argv and argv[0] == "--":
        argv = argv[1:]
    record = prepare_raw_command(
        argv, device=args.device, inputs=getattr(args, "input", None) or [],
        memory_limit_mib=getattr(args, "memory_limit_mib", None),
    )
    spec = {**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID}
    task = as_fleet_task(record, spec)
    state_root = default_state_root()
    if args.command_action == "submit":
        queue = FleetQueue(state_root / "fleet" / "fleet.db")
        try:
            receipt = queue.enqueue_submission(
                [record], spec=None, priority=args.priority,
                request_id=getattr(args, "request_id", None),
            )
        finally:
            queue.close()
        job_id = receipt["job_ids"][0]
        payload = {**receipt, "job_id": job_id, "prepared_id": record["prepared_id"],
                   "device": args.device, "state": "queued"}
        if "limits" in record:
            payload["limits"] = record["limits"]
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            reporter.event("fleet_command_queued", **payload)
        return EXIT_OK
    if args.command_action == "run":
        result = executor.run_once(
            task, config, state_root=state_root,
            use_lease=not getattr(args, "no_lease", False),
        )
        if args.json:
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
        else:
            event = {
                "device": args.device, "ok": bool(result.get("ok")),
                "exit_code": result.get("exit_code"), "error": result.get("error"),
            }
            if "limits" in record:
                event["memory_limit_mib"] = record["limits"]["process_tree_rss_mib"]
            reporter.event("fleet_command_done", **event)
        return EXIT_OK if result.get("ok") else EXIT_ERROR
    fcfg = fleet_config(config)
    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        active = queue.active_batches_by_device()
        backlog = queue.active_backlog()
    finally:
        queue.close()
    snaps = _snapshots(task, config, fcfg, active_batches=active)
    result = placement.plan_jobs(
        [task], [adapters.extract_features(task)], snaps, load_costs(config, state_root),
        fcfg, safety_fraction(config), device_backlog=backlog,
    )
    payload = {
        "prepared_id": record["prepared_id"], "device": args.device,
        "estimate": {"status": "unestimated", "estimated_seconds": None},
        "batches": [{"device": batch.device, "jobs": batch.job_indices,
                     "estimated_finish_s": batch.estimated_finish_s,
                     "reason": batch.reason,
                     "selection_basis": batch.selection_basis,
                     "estimate_reason": batch.estimate_reason,
                     "placement_explanation": batch.explanation}
                    for batch in result.batches],
        "skipped": result.skipped,
    }
    if "limits" in record:
        payload["limits"] = record["limits"]
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        event = {
            "device": args.device, "placeable": bool(result.batches),
            "estimate": "unestimated", "skipped": result.skipped,
        }
        if "limits" in record:
            event["memory_limit_mib"] = record["limits"]["process_tree_rss_mib"]
        reporter.event("fleet_command_plan", **event)
    return EXIT_OK if result.batches else EXIT_ERROR


def _target_operation_status(
    queue: FleetQueue, job: dict, *, refresh: bool, config=None,
) -> tuple[dict | None, bool]:
    stored = queue.target_operation_for_job(job["job_id"], include_token=refresh)
    if stored is None:
        return None, True
    token = stored.pop("resume_token", None)
    result = dict(stored)
    if not refresh:
        return result, True
    try:
        if config is None or stored["device"] not in config.devices:
            raise TransportError("accepted target is absent from current device configuration")
        status = make_transport(config.devices[stored["device"]]).durable_status(
            stored["operation_id"], token
        )
        if status.get("operation_id") != stored["operation_id"] \
                or status.get("request_sha256") != stored["request_sha256"]:
            raise TransportError("target status identity does not match the queue receipt")
        result["query_status"] = "ok"
        result["target_status"] = status
        return result, True
    except (OSError, TransportError, ValueError) as exc:
        try:
            attempt = json.loads(job.get("last_result") or "null")
            finalized = attempt.get("target_operation") if isinstance(attempt, dict) else None
        except (TypeError, json.JSONDecodeError):
            finalized = None
        if isinstance(finalized, dict) \
                and finalized.get("operation_id") == stored["operation_id"] \
                and finalized.get("request_sha256") == stored["request_sha256"] \
                and finalized.get("state") == "complete":
            result["query_status"] = "finalized_from_queue_receipt"
            result["target_status"] = finalized
            return result, True
        result["query_status"] = "unknown"
        result["query_error"] = str(exc)
        return result, False


def cmd_target_policy(args, reporter: Reporter) -> int:
    """Inspect or explicitly install one target-local capacity-one resource policy."""
    config = load_config()
    device_name = str(args.device)
    if device_name not in config.devices:
        raise ValueError(f"unknown device {device_name!r}")
    client = TargetResourceClient.connect(config, device_name, install=True)
    current = client.policy_get()
    if args.target_policy_action == "show":
        payload = {"schema": 1, "device": device_name, **current}
    else:
        keys = sorted(set(args.resource or []))
        if not keys:
            raise ValueError("target-policy install requires at least one --resource")
        installed = current.get("policy")
        if current.get("status") == "installed" and isinstance(installed, dict):
            generation = installed.get("generation")
            digest = installed.get("digest")
            document = installed.get("document")
            if isinstance(generation, bool) or not isinstance(generation, int) \
                    or not isinstance(digest, str) or not isinstance(document, dict):
                raise ValueError("target returned a malformed installed policy")
            expected_generation: int | None = generation
            expected_digest: str | None = digest
            desired_generation = generation + 1
        elif current.get("status") == "absent" and installed is None:
            expected_generation = None
            expected_digest = None
            desired_generation = 1
            document = None
        else:
            raise ValueError("target returned an invalid policy status")
        desired = {
            "schema": "remrun.target-resource-policy",
            "version": 1,
            "generation": desired_generation,
            "resources": [{"key": key, "capacity": 1} for key in keys],
        }
        if document is not None:
            current_keys = sorted(
                str(item.get("key")) for item in document.get("resources", [])
                if isinstance(item, dict)
            )
            if current_keys == keys:
                payload = {
                    "schema": 1,
                    "device": device_name,
                    "status": "installed",
                    "generation": expected_generation,
                    "digest": expected_digest,
                    "idempotent": True,
                    "resources": keys,
                }
                if args.json:
                    print(json.dumps(payload, indent=2, sort_keys=True))
                else:
                    reporter.event("target_policy", **payload)
                return EXIT_OK
        desired_digest = policy_digest(desired)
        rpc_id = (
            f"target-policy-{device_name}-{desired_generation}-{desired_digest[:20]}"
        )
        receipt = client.policy_install(
            desired,
            expected_generation=expected_generation,
            expected_digest=expected_digest,
            rpc_id=rpc_id,
        )
        payload = {
            "schema": 1,
            "device": device_name,
            **receipt,
            "resources": keys,
        }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        printable = dict(payload)
        if isinstance(printable.get("policy"), dict):
            printable["policy"] = canonical_json(printable["policy"]).decode("utf-8")
        reporter.event("target_policy", **printable)
    return EXIT_OK


def cmd_status(args, reporter: Reporter) -> int:
    q = FleetQueue(default_state_root() / "fleet" / "fleet.db")
    try:
        submission_id = getattr(args, "submission_id", None)
        request_id = getattr(args, "request_id", None)
        exact_job_ids = list(getattr(args, "job_id", None) or [])
        refresh_target = bool(getattr(args, "refresh_target", False))
        config = load_config() if refresh_target else None
        if submission_id is not None or request_id is not None:
            receipt = q.get_submission(
                submission_id=submission_id, request_id=request_id,
            )
            if receipt is None:
                refusal = q.get_submission_refusal(request_id=request_id) \
                    if request_id is not None else None
                if refusal is not None:
                    if args.json:
                        emit_json_document(refusal)
                    else:
                        reporter.event(
                            "submission_refused", reason=refusal["reason"],
                        )
                    return EXIT_OK
                raise ValueError("submission identity was not found")
            jobs = q.jobs_for_submission(receipt["submission_id"])
            target_ok = True
            for job in jobs:
                operation, ok = _target_operation_status(
                    q, job, refresh=refresh_target, config=config,
                )
                if operation is not None:
                    job["target_operation"] = operation
                target_ok = target_ok and ok
            payload = {"schema": 1, "submission": receipt, "jobs": jobs}
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                reporter.event(
                    "submission", submission_id=receipt["submission_id"],
                    request_id=receipt["request_id"], jobs=len(jobs),
                )
                for job in jobs:
                    reporter.event(
                        "job", job_id=job["job_id"],
                        task=job.get("task_name") or "-",
                        state=job.get("state") or "pruned",
                        device=job.get("assigned_device") or "-",
                    )
            return EXIT_OK if target_ok else EXIT_ERROR
        if exact_job_ids:
            jobs = []
            target_ok = True
            for job_id in exact_job_ids:
                row = q.get(job_id)
                if row is None:
                    raise ValueError(f"job {job_id!r} was not found")
                operation, ok = _target_operation_status(
                    q, row, refresh=refresh_target, config=config,
                )
                if operation is not None:
                    row["target_operation"] = operation
                target_ok = target_ok and ok
                jobs.append(row)
            if args.json:
                print(json.dumps({"schema": 1, "jobs": jobs}, indent=2, sort_keys=True))
            else:
                for job in jobs:
                    reporter.event(
                        "job", job_id=job["job_id"], task=job["task_name"],
                        state=job["state"], device=job.get("assigned_device") or "-",
                    )
            return EXIT_OK if target_ok else EXIT_ERROR
        if refresh_target:
            raise ValueError("--refresh-target requires an exact job or submission identity")
        counts = q.counts()
        recent = q.list()[-getattr(args, "limit", 20):]
        active = q.active_by_device()
    finally:
        q.close()
    if args.json:
        print(json.dumps({"counts": counts, "active_by_device": active,
                          "recent": recent}, indent=2, sort_keys=True))
        return EXIT_OK
    reporter.event("queue_counts", **counts)
    reporter.event("active_by_device", **active)
    for j in recent:
        fields = {
            "job_id": j["job_id"],
            "task": j["task_name"],
            "state": j["state"],
            "device": j.get("assigned_device") or "-",
        }
        if j.get("last_error"):
            fields["error"] = j["last_error"]
        reporter.event("job", **fields)
    return EXIT_OK


def cmd_run(args, reporter: Reporter) -> int:
    config = load_config()
    _spec, _records, tasks = _prepare_configured(args, config)
    result = executor.run_group(
        tasks, config, placement_task=tasks[0],
        use_lease=not getattr(args, "no_lease", False),
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    else:
        reporter.event("fleet_run", **{key: value for key, value in result.items()
                                       if key not in ("stdout_tail", "stderr_tail",
                                                      "telemetry", "memory_guard")})
    return EXIT_OK if result.get("ok") else EXIT_ERROR


def cmd_dispatch(args, reporter: Reporter) -> int:
    config = load_config()
    scope_job_ids = list(getattr(args, "job_id", None) or [])
    submission_id = getattr(args, "submission_id", None)
    request_id = getattr(args, "request_id", None)
    if submission_id is not None or request_id is not None:
        queue = FleetQueue(default_state_root() / "fleet" / "fleet.db")
        try:
            receipt = queue.get_submission(
                submission_id=submission_id, request_id=request_id,
            )
            if receipt is None:
                raise ValueError("scoped dispatch submission identity was not found")
            submission_id = receipt["submission_id"]
            scope_job_ids = receipt["job_ids"]
        finally:
            queue.close()
    if scope_job_ids:
        queue = FleetQueue(default_state_root() / "fleet" / "fleet.db")
        try:
            missing = [job_id for job_id in scope_job_ids if queue.get(job_id) is None]
        finally:
            queue.close()
        if missing:
            raise ValueError("scoped dispatch job was not found: " + ", ".join(missing))
        reporter.event(
            "dispatch_scope", submission_id=submission_id,
            request_id=request_id, job_ids=scope_job_ids,
        )
    scope = scope_job_ids or None
    if args.once:
        summary = dispatcher.drain_once(
            config, debounce_s=args.debounce, reporter=reporter, job_ids=scope,
        )
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
        else:
            reporter.event("dispatch_tick", **{k: v for k, v in summary.items() if k != "skipped"})
        return EXIT_OK
    until_empty = bool(getattr(args, "drain", False) or scope is not None)
    stop_event = dispatcher.DrainStopEvent() if until_empty else None
    previous_handlers: dict[signal.Signals, object] = {}

    def request_stop(signum, _frame) -> None:  # noqa: ANN001
        assert stop_event is not None
        if not stop_event.is_set():
            stop_event.set()
            return
        previous = previous_handlers[signal.Signals(signum)]
        signal.signal(signum, previous)
        signal.raise_signal(signum)

    if stop_event is not None and threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, request_stop)
    try:
        result = dispatcher.run(
            config, poll_s=args.poll, debounce_s=args.debounce,
            until_empty=until_empty, reporter=reporter, job_ids=scope,
            stop_event=stop_event,
            stop_timeout_s=float(getattr(
                args, "stop_timeout", dispatcher.DEFAULT_STOP_TIMEOUT_S,
            )),
        )
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    emit_json_document(result.to_dict())
    if not args.json:
        reporter.event("dispatch_final", **result.to_dict())
    return result.exit_code


def cmd_clear(args, reporter: Reporter) -> int:
    """Unstick the fleet: drop all queued + in-flight jobs, release every
    resource lease, and clear all cooldowns. ``--all`` also wipes done/failed
    history. Prints a one-line summary so a trigger/HUD can show what cleared."""
    q = FleetQueue(default_state_root() / "fleet" / "fleet.db")
    try:
        res = q.clear(include_final=getattr(args, "all", False))
    finally:
        q.close()
    if args.json:
        print(json.dumps(res, sort_keys=True))
    else:
        scope = "all jobs + history" if getattr(args, "all", False) else "queued + in-flight jobs"
        print(f"cleared {scope}: {res['jobs']} job(s), "
              f"{res['leases']} lease(s), {res['cooldowns']} cooldown(s)")
    return EXIT_OK


def _kill_local_workers(device=None) -> bool:  # noqa: ANN001
    """Run this controller's configured cancel actions directly.

    Used when the controller is also a runner and ssh-to-self is unavailable.
    Empty cancel config is a harmless no-op.
    """
    cancel = getattr(device, "cancel", {}) or {}
    nowin = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        if platform.system() == "Windows":
            script = _powershell_cancel_script(cancel)
            result = subprocess.run(
                ["powershell", "-NoProfile", "-Command", script],
                capture_output=True, timeout=25, creationflags=nowin,
            )
        else:
            script = _posix_cancel_script(cancel)
            result = subprocess.run(
                ["sh", "-lc", script], capture_output=True, timeout=25,
            )
        return result.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _cancel_scope(args, queue: FleetQueue) -> list[str] | None:  # noqa: ANN001
    """Resolve the same exact job/submission/request vocabulary used by dispatch."""
    job_ids = list(getattr(args, "job_id", None) or [])
    submission_id = getattr(args, "submission_id", None)
    request_id = getattr(args, "request_id", None)
    if submission_id is None and request_id is None and not job_ids:
        return None
    if submission_id is not None or request_id is not None:
        receipt = queue.get_submission(
            submission_id=submission_id, request_id=request_id,
        )
        if receipt is None:
            raise ValueError("targeted cancel submission identity was not found")
        job_ids = list(receipt["job_ids"])
    job_ids = list(dict.fromkeys(job_ids))
    missing = [job_id for job_id in job_ids if queue.get(job_id) is None]
    if missing:
        raise ValueError("targeted cancel job was not found: " + ", ".join(missing))
    return job_ids


def _already_finished(state: str) -> bool:
    return state in {"done", "failed_final", "needs_review"}


def _stop_target_operation(config, operation: dict) -> tuple[str, str | None, str | None]:  # noqa: ANN001
    """Stop one exact target-owned tree, returning disposition, error, cleanup state."""
    device_name = operation["device"]
    if device_name not in config.devices:
        return "could_not_stop", f"target device {device_name!r} is not configured", None
    operation_id = operation["operation_id"]
    request_sha = operation["request_sha256"]
    token = operation["resume_token"]
    try:
        client = TargetResourceClient.connect(config, device_name, install=False)
        observed = client.status_identity(operation_id, token)
        receipt = observed.get("receipt")
        if not isinstance(receipt, dict) \
                or receipt.get("operation_id") != operation_id \
                or receipt.get("request_sha256") != request_sha:
            return "could_not_stop", "target cancellation identity did not match", None
        state = str(receipt.get("state") or "")
        if state in {"RESERVED", "CLAIMED", "QUARANTINED"}:
            stopped = client.cancel(TargetReservation(receipt, token))
            stopped_receipt = stopped.get("receipt")
            if not isinstance(stopped_receipt, dict) \
                    or stopped_receipt.get("operation_id") != operation_id \
                    or stopped_receipt.get("request_sha256") != request_sha:
                return "could_not_stop", "target cancellation receipt did not match", None
            if stopped.get("status") == "could_not_stop":
                return (
                    "could_not_stop",
                    str(stopped_receipt.get("terminal_reason") or
                        "target process-tree termination could not be verified"),
                    None,
                )
            receipt = stopped_receipt
            state = str(receipt.get("state") or "")
    except (OSError, TargetResourceError, TransportError, ValueError) as exc:
        return "could_not_stop", str(exc), None
    if state == "RELEASED":
        return "already_finished", None, state
    if state not in {"CANCELLED", "EXPIRED", "REBOOTED"}:
        return "could_not_stop", f"target process tree remained {state or 'unknown'}", None
    return "stopped", None, state


def _target_operation_for_cancel(
    queue: FleetQueue, batch_id: str,
) -> tuple[dict | None, str | None]:
    try:
        operation = queue.target_operation(batch_id, include_token=True)
    except Exception as exc:  # noqa: BLE001 - malformed durable identity must remain untouched
        return None, str(exc)
    if operation is None:
        return None, "job has no exact target operation"
    return operation, None


def _cancel_active_batch(
    config, queue: FleetQueue, batch_id: str, job_ids: list[str],  # noqa: ANN001
) -> tuple[str, str | None]:
    """Stop one exact active worker invocation and retain all its evidence."""
    batch = queue.get_batch(batch_id)
    if batch is None or batch.get("state") not in {
        "leased", "staging", "running", "fetching", "cancelling",
    }:
        return "already_finished", None
    active_members = {
        row["job_id"] for row in queue.jobs_for_batch(batch_id)
        if row["state"] in {"leased", "staging", "running", "fetching"}
    }
    if active_members != set(job_ids):
        return "could_not_stop", "active batch also contains untargeted jobs"
    operation, error = _target_operation_for_cancel(queue, batch_id)
    if operation is None:
        return "could_not_stop", error
    # Commit the intent and revoke the execution owner's CAS before the first
    # target-side mutation. Recovery can then distinguish cancellation from
    # ordinary stale execution even if this controller disappears mid-call.
    if not queue.begin_active_cancellation(batch_id, job_ids):
        return "could_not_stop", "queue ownership changed before cancellation"
    disposition, error, cleanup_state = _stop_target_operation(config, operation)
    if disposition == "already_finished":
        reason = "target completed before cancellation; completion retained for review"
        if not queue.mark_active_cancellation_unknown(
            batch_id, job_ids, reason=reason,
        ):
            return "could_not_stop", "queue ownership changed during cancellation"
        return "already_finished", reason
    if disposition != "stopped":
        return disposition, error
    assert cleanup_state is not None
    if not queue.cancel_active_batch(batch_id, job_ids, cleanup_state=cleanup_state):
        rows = [queue.get(job_id) for job_id in job_ids]
        if all(row is not None and row["state"] == "cancelled" for row in rows):
            return "stopped", None
        if all(row is not None and _already_finished(row["state"]) for row in rows):
            return "already_finished", None
        return "could_not_stop", "queue ownership changed during cancellation"
    return "stopped", None


def _cancel_fenced_batch(
    config, queue: FleetQueue, batch_id: str, job_ids: list[str],  # noqa: ANN001
) -> tuple[str, str | None]:
    """Stop exact completion-unknown work without weakening its replay fence."""
    fenced_members = {
        row["job_id"] for row in queue.jobs_for_batch(batch_id)
        if row["state"] == "completion_unknown"
    }
    if fenced_members != set(job_ids):
        return "could_not_stop", "fenced batch also contains untargeted jobs"
    operation, error = _target_operation_for_cancel(queue, batch_id)
    if operation is None:
        return "could_not_stop", error
    disposition, error, cleanup_state = _stop_target_operation(config, operation)
    if disposition != "stopped":
        return disposition, error
    assert cleanup_state is not None
    if not queue.record_fenced_cancellation(batch_id, cleanup_state=cleanup_state):
        return "could_not_stop", "fenced queue state changed during cancellation"
    return "stopped", None


def _cmd_cancel_targeted(args, config, queue: FleetQueue, job_ids: list[str]) -> int:  # noqa: ANN001
    results: dict[str, dict[str, str]] = {}
    active: dict[str, list[str]] = {}
    fenced: dict[str, list[str]] = {}
    for job_id in job_ids:
        row = queue.get(job_id)
        assert row is not None
        state = str(row["state"])
        if state == "queued":
            if queue.cancel_queued(job_id):
                results[job_id] = {"job_id": job_id, "disposition": "stopped"}
                continue
            row = queue.get(job_id)
            assert row is not None
            state = str(row["state"])
        if state == "cancelled":
            results[job_id] = {"job_id": job_id, "disposition": "stopped"}
        elif state == "completion_unknown" and row["batch_id"]:
            fenced.setdefault(str(row["batch_id"]), []).append(job_id)
        elif _already_finished(state):
            results[job_id] = {"job_id": job_id, "disposition": "already_finished"}
        elif state in {"leased", "staging", "running", "fetching"} and row["batch_id"]:
            active.setdefault(str(row["batch_id"]), []).append(job_id)
        else:
            results[job_id] = {
                "job_id": job_id,
                "disposition": "could_not_stop",
                "error": f"job state {state!r} is not cancellable",
            }
    for batch_id, selected in active.items():
        disposition, error = _cancel_active_batch(config, queue, batch_id, selected)
        for job_id in selected:
            result = {"job_id": job_id, "disposition": disposition}
            if error is not None:
                result["error"] = error
            results[job_id] = result
    for batch_id, selected in fenced.items():
        disposition, error = _cancel_fenced_batch(config, queue, batch_id, selected)
        for job_id in selected:
            result = {"job_id": job_id, "disposition": disposition}
            if error is not None:
                result["error"] = error
            results[job_id] = result
    payload = {"schema": 1, "jobs": [results[job_id] for job_id in job_ids]}
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for result in payload["jobs"]:
            suffix = f": {result['error']}" if result.get("error") else ""
            print(f"{result['job_id']}: {result['disposition']}{suffix}")
    return (
        EXIT_ERROR
        if any(result["disposition"] == "could_not_stop" for result in payload["jobs"])
        else EXIT_OK
    )


def cmd_cancel(args, reporter: Reporter) -> int:
    """Cancel an exact job scope, or retain the legacy whole-fleet sweep.

    Scoped cancellation preserves queue/history rows and addresses only exact
    target-owned process trees. With no scope, the existing global clear plus
    configured device-worker sweep remains available for Stop all fleet work.
    """
    config = load_config()
    q = FleetQueue(default_state_root() / "fleet" / "fleet.db")
    try:
        scope = _cancel_scope(args, q)
        if scope is not None:
            return _cmd_cancel_targeted(args, config, q, scope)
        res = q.clear(include_final=getattr(args, "all", False))
    finally:
        q.close()
    sysname = platform.system()

    def _is_local_os(dev) -> bool:  # noqa: ANN001
        return ((dev.kind == "ssh-powershell" and sysname == "Windows")
                or (dev.kind == "ssh-posix" and sysname in ("Darwin", "Linux")))

    stopped: dict[str, bool] = {}
    for name, dev in config.devices.items():
        if dev.kind == "local-sim":
            continue
        try:
            ok = make_transport(dev).kill_workers()
        except Exception:   # noqa: BLE001 - cancel is best-effort; never fail the sweep
            ok = False
        if not ok and _is_local_os(dev):
            ok = _kill_local_workers(dev)
        stopped[name] = ok
    ok = [d for d, success in stopped.items() if success]
    if args.json:
        print(json.dumps({**res, "stopped_workers": stopped}, sort_keys=True))
    else:
        print(f"cancelled: cleared {res['jobs']} job(s), {res['leases']} lease(s); "
              f"stopped workers on {', '.join(ok) if ok else '(none reachable)'}")
    return EXIT_OK if all(stopped.values()) else EXIT_ERROR


def cmd_release(args, reporter: Reporter) -> int:
    """Acknowledge or execute cleanup for one exact fenced target operation."""
    operation_id = str(args.operation_id)
    request_sha256 = str(args.request_sha256)
    q = FleetQueue(default_state_root() / "fleet" / "fleet.db")
    try:
        if args.release_action == "acknowledge-effects":
            operation = q.target_release_operation(operation_id, request_sha256)
            if operation is None:
                disposition = "identity_mismatch"
                error = "exact target operation identity was not found"
            elif q.acknowledge_target_release_effects(operation_id, request_sha256):
                disposition = "effects_acknowledged"
                error = None
            else:
                disposition = "not_releasable"
                error = "operation is not an unfinalized fenced cancellation"
        else:
            from . import dispatcher

            disposition, error = dispatcher.release_target_operation(
                load_config(),
                q,
                operation_id,
                request_sha256,
                reporter=reporter,
            )
    finally:
        q.close()

    result = {
        "operation_id": operation_id,
        "request_sha256": request_sha256,
        "disposition": disposition,
    }
    if error is not None:
        result["error"] = error
    payload = {"schema": 1, "operations": [result]}
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        suffix = f": {error}" if error is not None else ""
        print(f"{operation_id}: {disposition}{suffix}")
    return (
        EXIT_OK
        if disposition in {"effects_acknowledged", "released", "already_released"}
        else EXIT_ERROR
    )


def cmd_resources(args, reporter: Reporter) -> int:
    """Live hardware for the fleet: CPU / RAM / GPU / disk per device.

    Distinct from `fleet status`, which reports the job QUEUE. This probes the
    devices themselves and is safe to run at any time: it never mutates state,
    never stages files, and never touches the queue.
    """
    from . import local_resources, resources
    from .resources_render import IncrementalTable, render_table, to_dict

    config = load_config()
    usage_display = (
        getattr(config, "defaults", {})
        .get("fleet", {})
        .get("resources", {})
        .get("usage_display", "percent")
    )
    if usage_display not in {"percent", "amounts"}:
        reporter.event(
            "invalid_config",
            detail=("fleet.resources.usage_display must be "
                    f"'percent' or 'amounts', not {usage_display!r}"),
        )
        return EXIT_ERROR
    wanted = {d.upper() for d in (getattr(args, "device", None) or [])}

    # Visibility is not placement. `enabled = false` keeps a device out of run
    # scheduling; it does not mean "don't tell me how much RAM it has". A box
    # you never dispatch to (a file server, a paused laptop) still belongs in
    # the picture when you are deciding where work should go.
    targets = []
    for name, dev in config.devices.items():
        if dev.kind == "local-sim":
            continue                      # a simulation target has no hardware to report
        if wanted and name.upper() not in wanted:
            continue
        if getattr(args, "enabled_only", False) and not dev.enabled:
            continue
        targets.append(dev)

    unknown = wanted - {d.name.upper() for d in targets}
    if unknown:
        reporter.event("unknown_devices", names=",".join(sorted(unknown)))

    # A TTY gets an append-only table immediately, with one row per completed
    # probe. Redirected output and --json remain buffered and deterministic.
    # --no-progress preserves the prior one-shot table for callers that prefer it.
    quiet = getattr(args, "json", False) or getattr(args, "no_progress", False)
    stream = not quiet and sys.stdout.isatty()
    include_local = (
        not getattr(args, "no_local", False)
        and not wanted
    )
    incremental = None
    local = None
    if stream:
        labels = [device.name for device in targets]
        if include_local:
            local_label = (platform.node().split(".")[0] or "LOCAL").upper()
            labels.append(local_label)
        incremental = IncrementalTable(labels, usage_display=usage_display)
        print(incremental.header(), flush=True)
        if include_local:
            local = local_resources.local_view()
            print(incremental.row(local), flush=True)

    # The old compact progress lines remain useful when stdout is not a TTY but
    # stderr is (for example, when the final table is redirected to a file).
    show = not quiet and not stream and sys.stderr.isatty()
    pending: set[str] = set()
    # Width of the last status line written, so it can be erased exactly. A `\r`
    # alone only moves the cursor: without overwriting, a shorter line leaves the
    # tail of the longer one behind, which renders as garbage like
    # "ok DEV1ing DEV1, DEV2".
    last_width = 0

    def is_local_duplicate(view) -> bool:  # noqa: ANN001
        if local is None or view is None:
            return False
        local_hostname = (local.hostname or "").split(".")[0].casefold()
        return (
            view.name.casefold() == local.name.casefold()
            or (
                local_hostname
                and (view.hostname or "").split(".")[0].casefold() == local_hostname
            )
        )

    def on_event(kind: str, name: str, view) -> None:  # noqa: ANN001
        nonlocal last_width
        if (
            incremental is not None
            and kind == "done"
            and view is not None
            and not is_local_duplicate(view)
        ):
            print(incremental.row(view), flush=True)
        if not show:
            return
        if last_width:
            print("\r" + " " * last_width + "\r", end="", file=sys.stderr, flush=True)
            last_width = 0
        if kind == "start":
            pending.add(name)
        else:
            pending.discard(name)
            mark = "ok" if (view is not None and view.reachable) else "--"
            print(f"  {mark} {name}", file=sys.stderr, flush=True)
        if pending:
            line = f"  .. probing {', '.join(sorted(pending))}"
            last_width = len(line)
            print(line, end="\r", file=sys.stderr, flush=True)

    if show:
        print(f"probing {len(targets)} device(s)...", file=sys.stderr, flush=True)
    views = resources.probe_fleet(targets, timeout=getattr(args, "timeout", 45.0),
                                  on_event=on_event)
    if show and last_width:
        print("\r" + " " * last_width + "\r", end="", file=sys.stderr, flush=True)
    if include_local:
        if local is None:
            local = local_resources.local_view()
        # The controller is frequently ALSO a configured device (a laptop is both the
        # box you sit at and a run target for the rest of the mesh). Probing it over SSH
        # from itself usually fails — it has no authorized key for its own controller —
        # so leaving both rows in prints the device twice: once measured locally, once
        # as a spurious "ssh auth refused". Drop the remote row and keep the local
        # measurement, which is strictly better information about the same machine.
        local_hostname = (local.hostname or "").split(".")[0].casefold()
        views = [v for v in views
                 if not (v.name.casefold() == local.name.casefold()
                         or (local_hostname
                             and (v.hostname or "").split(".")[0].casefold() == local_hostname))]
        views.insert(0, local)

    if getattr(args, "json", False):
        print(json.dumps({"devices": [to_dict(v) for v in views]},
                         indent=2, sort_keys=True))
    elif incremental is not None:
        footer = incremental.footer(views)
        if footer:
            print(footer)
    else:
        print(render_table(views, usage_display=usage_display))

    # Exit nonzero only if EVERY remote device failed: a single offline laptop
    # is normal and must not make the command look broken to a caller.
    remote = [v for v in views if not v.is_local]
    if remote and not any(v.reachable for v in remote):
        return EXIT_ERROR
    return EXIT_OK


def cmd_jobs(args, reporter: Reporter) -> int:
    """Cross-controller view of target-local active remrun jobs.

    This is distinct from ``fleet status``: it queries each target's bounded
    active-job registry and never treats an unreachable or incompatible target
    as an empty target.
    """
    from . import jobs
    from .jobs_render import IncrementalTable, render_table

    config = load_config()
    wanted = {d.upper() for d in (getattr(args, "device", None) or [])}
    targets = []
    for name, dev in config.devices.items():
        if dev.kind == "local-sim":
            continue
        if wanted and name.upper() not in wanted:
            continue
        if getattr(args, "enabled_only", False) and not dev.enabled:
            continue
        targets.append(dev)

    selected = {d.name.upper() for d in targets}
    unknown = wanted - selected
    if unknown:
        reporter.event("unknown_devices", names=",".join(sorted(unknown)))
    if not targets:
        reporter.event("no_devices")
        return EXIT_ERROR

    quiet = getattr(args, "json", False) or getattr(args, "no_progress", False)
    stream = not quiet and sys.stdout.isatty()
    incremental = IncrementalTable([d.name for d in targets]) if stream else None
    if incremental is not None:
        print(incremental.header(), flush=True)

    pending: set[str] = set()
    show = not quiet and not stream and sys.stderr.isatty()
    last_width = 0

    def on_event(kind: str, name: str, view) -> None:  # noqa: ANN001
        nonlocal last_width
        if incremental is not None and kind == "done" and view is not None:
            for row in incremental.rows(view):
                print(row, flush=True)
        if not show:
            return
        if last_width:
            print("\r" + " " * last_width + "\r", end="", file=sys.stderr, flush=True)
            last_width = 0
        if kind == "start":
            pending.add(name)
        else:
            pending.discard(name)
            mark = "ok" if view is not None and view.status in {"ok", "partial"} else "--"
            print(f"  {mark} {name}", file=sys.stderr, flush=True)
        if pending:
            line = f"  .. querying {', '.join(sorted(pending))}"
            last_width = len(line)
            print(line, end="\r", file=sys.stderr, flush=True)

    if show:
        print(f"querying {len(targets)} device(s)...", file=sys.stderr, flush=True)
    controller = (platform.node().split(".", 1)[0] or "").casefold()
    local_names = {
        device.name.casefold()
        for device in targets
        if controller and device.name.casefold() == controller
    }
    views = jobs.probe_fleet(
        targets,
        sample_interval=getattr(args, "sample_interval", 0.2),
        timeout=getattr(args, "timeout", 45.0),
        local_names=local_names,
        on_event=on_event,
    )
    if show and last_width:
        print("\r" + " " * last_width + "\r", end="", file=sys.stderr, flush=True)

    if getattr(args, "json", False):
        print(json.dumps({
            "schema": 1,
            "jobs": jobs.flatten_jobs(views),
            "targets": [jobs.to_dict(v) for v in views],
        }, indent=2, sort_keys=True))
    elif incremental is None:
        print(render_table(views))

    # One offline target is normal. The command fails only when no target
    # supplied a supported observation document, avoiding a plausible empty fleet.
    if not any(view.status in {"ok", "partial"} for view in views):
        return EXIT_ERROR
    return EXIT_OK

def cmd_mesh(args, reporter: Reporter) -> int:
    """Directed SSH reachability: which devices can log into which.

    Read-only. Every cell is measured, never inferred from config, because SSH
    trust is asymmetric and config cannot tell you whose key is actually
    installed where.
    """
    import socket

    from . import mesh
    from .mesh_render import render_matrix, to_dict

    config = load_config()
    wanted = {d.upper() for d in (getattr(args, "device", None) or [])}
    devices = [d for name, d in config.devices.items()
               if d.kind != "local-sim" and (not wanted or name.upper() in wanted)]
    if not devices:
        reporter.event("no_devices")
        return EXIT_ERROR

    try:
        controller = socket.gethostname().split(".")[0].upper()
    except OSError:
        controller = "LOCAL"

    matrix = mesh.build_matrix(devices, controller,
                               hops=not getattr(args, "no_hops", False),
                               connect_timeout=int(getattr(args, "connect_timeout", 8)))
    if getattr(args, "json", False):
        print(json.dumps(to_dict(matrix), indent=2, sort_keys=True))
    else:
        print(render_matrix(matrix, controller))
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="remrun fleet")
    sub = p.add_subparsers(dest="fleet_command", required=True)

    def add_common(sp, *, task_optional: bool = False):
        sp.add_argument(
            "task_name", nargs="?" if task_optional else None,
            help="configured task name",
        )
        sp.add_argument("--require", action="append", default=[], metavar="TOKEN",
                        help="opaque capability token an eligible adapter must "
                             "list in its `provides` (repeatable)")
        sp.add_argument("--text", help="inline text payload")
        sp.add_argument(
            "--stdin-request", action="store_true",
            help="read the closed versioned inline payload document from stdin",
        )
        sp.add_argument("--input", action="append", help="input file or folder (repeatable)")
        sp.add_argument("--clipboard", action="store_true",
                        help="use the OS clipboard as the payload: a folder path -> its eligible "
                             "files, file path(s) -> those files, else the text")
        sp.add_argument("--device", help="force a configured device")
        sp.add_argument("--engine", help="force an engine")
        sp.add_argument("--opt", action="append", help="task option key=value (repeatable)")
        sp.add_argument("--output-root", dest="output_root", help="override output folder")
        sp.add_argument(
            "--return-root", dest="return_root",
            help="opt in to verified output return under this controller-local folder",
        )
        sp.add_argument(
            "--memory-limit-mib", type=int,
            help="explicit hard sampled process-tree RSS ceiling in whole MiB; "
                 "the selected target still admits it against its own reserve and policy",
        )
        sp.add_argument("--json", action="store_true")
        sp.add_argument("--route-line", dest="route_line", action="store_true",
                        help="print one concise routing line for a trigger tooltip/HUD, then exit")

    pp = sub.add_parser("plan", help="show placement decision; runs nothing")
    add_common(pp)
    pp.add_argument("--priority", type=int, default=0,
                    help="freeze this queue priority into a saved plan")
    pp.add_argument(
        "--save", action="store_true",
        help="persist this exact fully placed plan and return a token for submit --plan",
    )
    ps = sub.add_parser("submit", help="enqueue a job")
    add_common(ps, task_optional=True)
    ps.add_argument("--priority", type=int, default=0)
    ps.add_argument("--request-id", help="caller-owned stable submission retry/correlation token")
    ps.add_argument(
        "--plan", dest="plan_id",
        help="consume an exact saved plan instead of preparing task arguments again",
    )
    ps.add_argument(
        "--preview-route", action="store_true",
        help="requires --json; probe live placement before enqueue and include the non-binding preview",
    )
    ps.add_argument("--allow-fallback", action="store_true",
                    help="when used with --device, retry later attempts with automatic placement "
                         "if the forced device fails")
    pr = sub.add_parser("run", help="run one job now (place -> stage -> exec)")
    add_common(pr)
    pr.add_argument("--no-lease", action="store_true",
                    help="skip the configured resource lease (dev/test; may race the dispatcher)")
    pcm = sub.add_parser("command", help="intrinsic arbitrary-command queue (never batched)")
    command_sub = pcm.add_subparsers(dest="command_action", required=True)
    for action, help_text in (
            ("plan", "show whether the explicitly selected device is available"),
            ("submit", "enqueue one exact command invocation"),
            ("run", "run one exact command invocation now")):
        command_parser = command_sub.add_parser(action, help=help_text)
        command_parser.add_argument("--device", required=True,
                                    help="explicit configured execution device")
        command_parser.add_argument("--input", action="append",
                                    help="input file or folder to stage (repeatable)")
        command_parser.add_argument("--json", action="store_true")
        command_parser.add_argument(
            "--memory-limit-mib", type=int,
            help="explicit hard sampled process-tree RSS ceiling in whole MiB",
        )
        if action == "submit":
            command_parser.add_argument("--priority", type=int, default=0)
            command_parser.add_argument(
                "--request-id", help="caller-owned stable submission retry/correlation token",
            )
        if action == "run":
            command_parser.add_argument("--no-lease", action="store_true",
                                        help="skip the resource lease (dev/test only)")
        command_parser.add_argument("argv", nargs=argparse.REMAINDER,
                                    help="exact argv after --")
    pst = sub.add_parser("status", help="show the fleet queue")
    pst.add_argument("--limit", type=int, default=20)
    status_scope = pst.add_mutually_exclusive_group()
    status_scope.add_argument("--job", dest="job_id", action="append",
                              help="look up this exact job ID (repeatable)")
    status_scope.add_argument("--submission", dest="submission_id",
                              help="look up one exact submission and all its jobs")
    status_scope.add_argument("--request-id",
                              help="look up the submission accepted for this caller request")
    pst.add_argument("--json", action="store_true")
    pst.add_argument(
        "--refresh-target", action="store_true",
        help="for exact jobs/submissions, query the authenticated detached target operation",
    )
    pd = sub.add_parser("dispatch", help="drain the queue: place + run batched jobs "
                                         "(loops until Ctrl-C; --once for one tick; --drain "
                                         "to run empty or detach gracefully on SIGINT/SIGTERM)")
    pd.add_argument("--once", action="store_true", help="run a single tick and exit")
    pd.add_argument("--drain", action="store_true",
                    help="loop until the queue is empty (no queued/in-flight jobs), then exit "
                         "— the fire-and-forget mode the interactive triggers use")
    pd.add_argument("--poll", type=float, default=2.0, help="idle poll interval (s)")
    pd.add_argument("--debounce", type=float, default=5.0,
                    help="seconds to coalesce a burst before launching one worker")
    pd.add_argument(
        "--stop-timeout", type=_positive_seconds,
        default=dispatcher.DEFAULT_STOP_TIMEOUT_S,
        help="maximum seconds to return after SIGINT/SIGTERM during --drain (default 35)",
    )
    dispatch_scope = pd.add_mutually_exclusive_group()
    dispatch_scope.add_argument("--job", dest="job_id", action="append",
                                help="dispatch only this exact queued job (repeatable)")
    dispatch_scope.add_argument("--submission", dest="submission_id",
                                help="dispatch only the immutable jobs in this submission")
    dispatch_scope.add_argument("--request-id",
                                help="dispatch only the submission accepted for this caller request")
    pd.add_argument("--json", action="store_true")
    pc = sub.add_parser("clear", help="unstick the fleet: drop all queued + in-flight jobs, "
                                      "release leases, clear cooldowns")
    pc.add_argument("--all", action="store_true",
                    help="also wipe done/failed job history (default keeps it for `status`)")
    pc.add_argument("--json", action="store_true")
    px = sub.add_parser(
        "cancel",
        help="cancel exact jobs, or with no scope clear the queue and stop every device",
    )
    cancel_scope = px.add_mutually_exclusive_group()
    cancel_scope.add_argument(
        "--job", dest="job_id", action="append",
        help="cancel this exact job (repeatable)",
    )
    cancel_scope.add_argument(
        "--submission", dest="submission_id",
        help="cancel the immutable jobs in this submission",
    )
    cancel_scope.add_argument(
        "--request-id",
        help="cancel the submission accepted for this caller request",
    )
    cancel_scope.add_argument(
        "--all", action="store_true", help="global sweep: also wipe done/failed job history",
    )
    px.add_argument("--json", action="store_true")
    prelease = sub.add_parser(
        "release",
        help="explicitly authorize or execute cleanup for one exact fenced operation",
    )
    release_actions = prelease.add_subparsers(dest="release_action", required=True)
    for action, help_text in (
        (
            "acknowledge-effects",
            "durably acknowledge that this exact operation may have external effects",
        ),
        ("execute", "clean up this exact owner-authorized operation"),
    ):
        release_parser = release_actions.add_parser(action, help=help_text)
        release_parser.add_argument(
            "--operation", dest="operation_id", required=True,
            help="exact target operation ID",
        )
        release_parser.add_argument(
            "--request-sha256", required=True,
            help="exact target request digest",
        )
        release_parser.add_argument("--json", action="store_true")
    prs = sub.add_parser("resources", help="live CPU / RAM / GPU / disk for every configured "
                                           "device (hardware, not the job queue)")
    prs.add_argument("--device", action="append",
                     help="limit to this device (repeatable; default is all enabled)")
    prs.add_argument("--no-local", action="store_true",
                     help="omit this controller's own row")
    prs.add_argument("--enabled-only", dest="enabled_only", action="store_true",
                     help="only devices with enabled = true (default shows all, since a "
                          "device you never dispatch to still has hardware worth seeing)")
    prs.add_argument("--timeout", type=float, default=45.0,
                     help="per-device probe timeout in seconds (default 45; a heavily "
                          "loaded box can take tens of seconds to answer)")
    prs.add_argument("--no-progress", dest="no_progress", action="store_true",
                     help="suppress the per-device progress lines on stderr")
    prs.add_argument("--json", action="store_true")
    pj = sub.add_parser("jobs", help="active remrun jobs observed on configured targets "
                                     "(cross-controller; not the local queue)")
    pj.add_argument("--device", action="append",
                    help="limit to this target (repeatable; default is all configured targets)")
    pj.add_argument("--enabled-only", dest="enabled_only", action="store_true",
                    help="only targets with enabled = true")
    pj.add_argument("--sample-interval", dest="sample_interval", type=float, default=0.2,
                    help="bounded CPU sampling interval per target in seconds (default 0.2)")
    pj.add_argument("--timeout", type=float, default=45.0,
                    help="per-target query timeout in seconds (default 45)")
    pj.add_argument("--no-progress", dest="no_progress", action="store_true",
                    help="suppress progressive completion output")
    pj.add_argument("--json", action="store_true")
    ptp = sub.add_parser(
        "target-policy",
        help="inspect or explicitly install one target-local resource authority",
    )
    target_policy_sub = ptp.add_subparsers(dest="target_policy_action", required=True)
    ptps = target_policy_sub.add_parser("show", help="show one target's installed policy")
    ptps.add_argument("--device", required=True)
    ptps.add_argument("--json", action="store_true")
    ptpi = target_policy_sub.add_parser(
        "install", help="install the exact capacity-one resource key set",
    )
    ptpi.add_argument("--device", required=True)
    ptpi.add_argument(
        "--resource", action="append", required=True,
        help="opaque target resource key such as pool/gpu (repeatable)",
    )
    ptpi.add_argument("--json", action="store_true")
    pm = sub.add_parser("mesh", help="who can ssh into whom: directed reachability matrix "
                                     "across the fleet (read-only, measured not inferred)")
    pm.add_argument("--device", action="append",
                    help="limit to this device (repeatable)")
    pm.add_argument("--no-hops", dest="no_hops", action="store_true",
                    help="only test edges FROM this controller (fast; leaves other rows blank)")
    pm.add_argument("--connect-timeout", dest="connect_timeout", type=int, default=8,
                    help="ssh ConnectTimeout in seconds (default 8)")
    pm.add_argument("--json", action="store_true")
    pstorage = sub.add_parser(
        "storage", help="enroll and bind shared roots used only as verified input optimizations",
    )
    storage_sub = pstorage.add_subparsers(dest="storage_action", required=True)
    pse = storage_sub.add_parser("enroll", help="create/read a marker and enroll this local root")
    pse.add_argument("root")
    pse.add_argument("--json", action="store_true")
    psb = storage_sub.add_parser("bind", help="bind a target-visible root by reading its marker")
    psb.add_argument("--device", required=True)
    psb.add_argument("root")
    psb.add_argument("--json", action="store_true")
    psl = storage_sub.add_parser("list", help="show this controller's verified bindings")
    psl.add_argument("--json", action="store_true")
    pti = sub.add_parser(
        "task-interfaces",
        help="show resolved task/device routes (read-only; no device probe)",
    )
    pti.add_argument("--json", action="store_true")
    return p


def main(argv: list[str]) -> int:
    args = build_parser().parse_args(argv)
    lifecycle_json = bool(
        args.fleet_command == "dispatch" and getattr(args, "json", False)
    )
    reporter = Reporter(
        json_events=lifecycle_json,
        event_schema="remrun.fleet.lifecycle" if lifecycle_json else None,
        event_version=1 if lifecycle_json else None,
    )
    try:
        if args.fleet_command == "plan":
            return cmd_plan(args, reporter)
        if args.fleet_command == "submit":
            return cmd_submit(args, reporter)
        if args.fleet_command == "status":
            return cmd_status(args, reporter)
        if args.fleet_command == "run":
            return cmd_run(args, reporter)
        if args.fleet_command == "command":
            return cmd_command(args, reporter)
        if args.fleet_command == "dispatch":
            return cmd_dispatch(args, reporter)
        if args.fleet_command == "clear":
            return cmd_clear(args, reporter)
        if args.fleet_command == "cancel":
            return cmd_cancel(args, reporter)
        if args.fleet_command == "release":
            return cmd_release(args, reporter)
        if args.fleet_command == "resources":
            return cmd_resources(args, reporter)
        if args.fleet_command == "jobs":
            return cmd_jobs(args, reporter)
        if args.fleet_command == "target-policy":
            return cmd_target_policy(args, reporter)
        if args.fleet_command == "mesh":
            return cmd_mesh(args, reporter)
        if args.fleet_command == "storage":
            return cmd_storage(args, reporter)
        if args.fleet_command == "task-interfaces":
            return cmd_task_interfaces(args, reporter)
    except Exception as exc:  # noqa: BLE001 - keep agent-visible error concise
        reporter.event("error", type=type(exc).__name__, message=str(exc))
        return EXIT_ERROR
    return EXIT_ERROR
