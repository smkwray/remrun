"""`remrun fleet <subcommand>` — plan / submit / status / run.

Delegated to from remrun's main CLI. Output goes to stderr as `remrun: fleet …`
events (or JSON with --json), mirroring remrun's Reporter style.
"""
from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from pathlib import Path

from ..config import load_config
from ..output import Reporter
from ..state import default_state_root
from . import adapters, executor, placement, probes
from .config import fleet_config, load_costs, safety_fraction
from .models import FleetTask
from .queue import FleetQueue
from .prepared import (
    RAW_COMMAND_SPEC, RAW_COMMAND_SPEC_ID, as_fleet_task, parse_option_assignments,
    prepare_raw_command, prepare_task_jobs,
)
from .task_contract import resolve_tasks
from ..transport import make_transport, _posix_cancel_script, _powershell_cancel_script

EXIT_OK = 0
EXIT_ERROR = 1

# No console-window flash on Windows when invoked from a GUI trigger; 0 elsewhere.
_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def _configured_payload(args, definition: dict) -> tuple[str | None, list[str]]:  # noqa: ANN001
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
    text, inputs = _configured_payload(args, spec["definition"])
    options = parse_option_assignments(spec["definition"], getattr(args, "opt", None))
    records = prepare_task_jobs(
        spec, repo_root=config.repo_root, text=text, inputs=inputs, options=options,
        caller_requirements=getattr(args, "require", None) or (),
        force_device=getattr(args, "device", None),
        allow_fallback=getattr(args, "allow_fallback", False),
        engine=getattr(args, "engine", None),
        output_root=getattr(args, "output_root", None),
        memory_limit_mib=getattr(args, "memory_limit_mib", None),
    )
    # Preparation may be slow. Re-resolve immediately before the caller opens
    # its queue transaction so authority/config changes insert zero rows.
    current = resolve_tasks(load_config(config.repo_root)).get(args.task_name)
    if current is None or current["spec_id"] != spec["spec_id"]:
        raise ValueError("task definition changed during preparation; no job was enqueued")
    return spec, records, [as_fleet_task(record, spec) for record in records]


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


def _candidate_devices(task: FleetTask, config) -> list[str]:
    if task.force_device:
        return [task.force_device]
    return adapters.candidate_devices(task)


def _snapshots(task: FleetTask, config, fcfg, *, active_batches: dict[str, int] | None = None) -> dict:
    snaps = {}
    active_batches = active_batches or {}
    for name in _candidate_devices(task, config):
        dev = config.devices.get(name)
        if dev is not None:
            snaps[name] = probes.build_snapshot(
                dev, None, fcfg,
                active_jobs=active_batches.get(name, 0),
                adapter_specs=[(task.resolved_spec or {})["adapters"][name]]
                if task.resolved_spec and name in task.resolved_spec["adapters"] else [],
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
            "active_on_device": active, "estimated_finish_s": b.estimated_finish_s}


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
    payload = {
        "task": spec["task_name"], "spec_id": spec["spec_id"],
        "prepared_ids": [record["prepared_id"] for record in records],
        "cost": [record["cost"] for record in records],
        "batches": [{"device": batch.device, "jobs": batch.job_indices,
                     "estimated_finish_s": batch.estimated_finish_s,
                     "reason": batch.reason} for batch in result.batches],
        "skipped": result.skipped, "note": result.note,
    }
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
            reporter.event("placement", device=batch.device,
                           jobs=len(batch.job_indices),
                           estimated_finish_s=batch.estimated_finish_s,
                           reason=batch.reason)
        for device, reason in sorted(result.skipped.items()):
            reporter.event("skipped", device=device, reason=reason)
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


def cmd_submit(args, reporter: Reporter) -> int:
    if getattr(args, "preview_route", False) and not getattr(args, "json", False):
        raise ValueError("--preview-route requires --json")
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
        jids = q.enqueue_prepared_many(
            records, spec=spec, priority=getattr(args, "priority", 0),
            current_spec_id=current_spec_id,
        )
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
        payload = {"job_ids": jids, "task": spec["task_name"],
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
            job_id = queue.enqueue_prepared(record, spec=None, priority=args.priority)
        finally:
            queue.close()
        payload = {"job_id": job_id, "prepared_id": record["prepared_id"],
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
                     "reason": batch.reason} for batch in result.batches],
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


def cmd_status(args, reporter: Reporter) -> int:
    q = FleetQueue(default_state_root() / "fleet" / "fleet.db")
    try:
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
    from . import dispatcher
    config = load_config()
    if args.once:
        summary = dispatcher.drain_once(config, debounce_s=args.debounce, reporter=reporter)
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
        else:
            reporter.event("dispatch_tick", **{k: v for k, v in summary.items() if k != "skipped"})
        return EXIT_OK
    dispatcher.run(config, poll_s=args.poll, debounce_s=args.debounce,
                   until_empty=getattr(args, "drain", False), reporter=reporter)
    return EXIT_OK


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
            subprocess.run(["powershell", "-NoProfile", "-Command", script],
                           capture_output=True, timeout=25, creationflags=nowin)
        else:
            script = _posix_cancel_script(cancel)
            subprocess.run(["sh", "-lc", script], capture_output=True, timeout=25)
        return True
    except (OSError, subprocess.SubprocessError):
        return False


def cmd_cancel(args, reporter: Reporter) -> int:
    """Cancel the fleet best-effort: clear queue state and run each device's
    configured cancel actions. Heavier than `clear`, which only clears the DB
    queue and leaves already-running external workers alone. If a remote kill
    fails on a device matching the controller OS, also try a direct local kill so
    a controller-runner can stop its own jobs without ssh-to-self.
    """
    config = load_config()
    q = FleetQueue(default_state_root() / "fleet" / "fleet.db")
    try:
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
    return EXIT_OK


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

    def add_common(sp):
        sp.add_argument("task_name", help="configured task name")
        sp.add_argument("--require", action="append", default=[], metavar="TOKEN",
                        help="opaque capability token an eligible adapter must "
                             "list in its `provides` (repeatable)")
        sp.add_argument("--text", help="inline text payload")
        sp.add_argument("--input", action="append", help="input file or folder (repeatable)")
        sp.add_argument("--clipboard", action="store_true",
                        help="use the OS clipboard as the payload: a folder path -> its eligible "
                             "files, file path(s) -> those files, else the text")
        sp.add_argument("--device", help="force a configured device")
        sp.add_argument("--engine", help="force an engine")
        sp.add_argument("--opt", action="append", help="task option key=value (repeatable)")
        sp.add_argument("--output-root", dest="output_root", help="override output folder")
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
    ps = sub.add_parser("submit", help="enqueue a job")
    add_common(ps)
    ps.add_argument("--priority", type=int, default=0)
    ps.add_argument(
        "--preview-route", action="store_true",
        help="probe live placement before enqueue and include the non-binding preview in JSON",
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
        if action == "run":
            command_parser.add_argument("--no-lease", action="store_true",
                                        help="skip the resource lease (dev/test only)")
        command_parser.add_argument("argv", nargs=argparse.REMAINDER,
                                    help="exact argv after --")
    pst = sub.add_parser("status", help="show the fleet queue")
    pst.add_argument("--limit", type=int, default=20)
    pst.add_argument("--json", action="store_true")
    pd = sub.add_parser("dispatch", help="drain the queue: place + run batched jobs "
                                         "(loops until Ctrl-C; --once for one tick; --drain "
                                         "to run the queue empty then exit)")
    pd.add_argument("--once", action="store_true", help="run a single tick and exit")
    pd.add_argument("--drain", action="store_true",
                    help="loop until the queue is empty (no queued/in-flight jobs), then exit "
                         "— the fire-and-forget mode the interactive triggers use")
    pd.add_argument("--poll", type=float, default=2.0, help="idle poll interval (s)")
    pd.add_argument("--debounce", type=float, default=5.0,
                    help="seconds to coalesce a burst before launching one worker")
    pd.add_argument("--json", action="store_true")
    pc = sub.add_parser("clear", help="unstick the fleet: drop all queued + in-flight jobs, "
                                      "release leases, clear cooldowns")
    pc.add_argument("--all", action="store_true",
                    help="also wipe done/failed job history (default keeps it for `status`)")
    pc.add_argument("--json", action="store_true")
    px = sub.add_parser("cancel", help="cancel everything: clear the queue AND stop configured in-flight "
                                       "workers on every device, releasing configured resource locks")
    px.add_argument("--all", action="store_true", help="also wipe done/failed job history")
    px.add_argument("--json", action="store_true")
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
    pm = sub.add_parser("mesh", help="who can ssh into whom: directed reachability matrix "
                                     "across the fleet (read-only, measured not inferred)")
    pm.add_argument("--device", action="append",
                    help="limit to this device (repeatable)")
    pm.add_argument("--no-hops", dest="no_hops", action="store_true",
                    help="only test edges FROM this controller (fast; leaves other rows blank)")
    pm.add_argument("--connect-timeout", dest="connect_timeout", type=int, default=8,
                    help="ssh ConnectTimeout in seconds (default 8)")
    pm.add_argument("--json", action="store_true")
    return p


def main(argv: list[str]) -> int:
    args = build_parser().parse_args(argv)
    reporter = Reporter(json_events=False)
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
        if args.fleet_command == "resources":
            return cmd_resources(args, reporter)
        if args.fleet_command == "jobs":
            return cmd_jobs(args, reporter)
        if args.fleet_command == "mesh":
            return cmd_mesh(args, reporter)
    except Exception as exc:  # noqa: BLE001 - keep agent-visible error concise
        reporter.event("error", type=type(exc).__name__, message=str(exc))
        return EXIT_ERROR
    return EXIT_ERROR
