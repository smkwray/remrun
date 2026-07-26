"""`remrun fleet <subcommand>` — plan / submit / status / run.

Delegated to from remrun's main CLI. Output goes to stderr as `remrun: fleet …`
events (or JSON with --json), mirroring remrun's Reporter style.
"""
from __future__ import annotations

import argparse
import dataclasses
import hashlib
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
from ..transport import make_transport, _posix_cancel_script, _powershell_cancel_script

EXIT_OK = 0
EXIT_ERROR = 1

# No console-window flash on Windows when invoked from a GUI trigger; 0 elsewhere.
_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def _idempotency_key(task_type: str, text, inputs, options) -> str:
    blob = json.dumps({"t": task_type, "x": text, "i": sorted(inputs), "o": options},
                      sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


TEXT_EXTS = {".txt", ".md", ".markdown"}    # generic text payload extensions


def _expand_inputs(task_type: str, raw: list[str]) -> list[str]:
    """Expand folders to eligible files (OCR extensions for ocr, text extensions for tts, all
    files for cmd). Explicit file paths are taken as-is (no extension filter)."""
    out: list[str] = []
    for r in raw:
        p = Path(r).expanduser()
        if p.is_dir():
            for f in sorted(p.iterdir()):
                if not f.is_file():
                    continue
                if task_type == "ocr" and f.suffix.lower() not in adapters.OCR_EXTS:
                    continue
                if task_type == "tts" and f.suffix.lower() not in TEXT_EXTS:
                    continue
                out.append(str(f))
        elif p.exists():
            out.append(str(p))
    return out


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


def _resolve_clipboard(task_type: str, clip: str) -> tuple[str | None, list[str]]:
    """Clipboard text -> ``(text, input_paths)`` for a fleet task:
      * one line that is an existing FOLDER -> that folder (expanded to eligible files later);
      * one or more LINES that are all existing FILES -> those files;
      * a single line of several existing FILE paths joined by whitespace (e.g. a
        ``/a.md /b.md`` selection copied onto one line) -> those files;
      * otherwise the literal clipboard text — valid as text-to-speech (tts) input, but NOT OCR
        (ocr needs files), so ocr returns ``(None, [])`` (caller reports "no OCR input").
    Surrounding quotes are stripped (Windows 'Copy as path' quotes them). The whitespace-split
    fallback only fires when EVERY token is an existing file, so ordinary prose can never be
    mistaken for paths; a lone path that itself contains spaces is still matched as one file by
    the line checks above, so only genuinely multi-path lines reach the split."""
    raw = (clip or "").strip()
    if not raw:
        return None, []

    def _clean(s: str) -> str:
        return s.strip().strip('"').strip("'")

    lines = [_clean(ln) for ln in raw.splitlines() if ln.strip()]
    if len(lines) == 1 and Path(lines[0]).expanduser().is_dir():
        return None, [lines[0]]
    if lines and all(Path(ln).expanduser().is_file() for ln in lines):
        return None, lines
    tokens = [_clean(t) for t in raw.split()]
    if len(tokens) > 1 and all(Path(t).expanduser().is_file() for t in tokens):
        return None, tokens
    return (raw, []) if task_type == "tts" else (None, [])


def _build_task(args: argparse.Namespace) -> FleetTask:
    options = {}
    for kv in (getattr(args, "opt", None) or []):
        if "=" in kv:
            k, v = kv.split("=", 1)
            options[k.strip()] = v.strip()
    if getattr(args, "argv", None):
        options["argv"] = args.argv
    device = getattr(args, "device", None)
    if getattr(args, "allow_fallback", False) and device:
        options["_allow_fallback"] = True
        options["_preferred_device"] = device
    text = getattr(args, "text", None)
    raw_inputs = list(getattr(args, "input", None) or [])
    if getattr(args, "clipboard", False):
        # Resolve the OS clipboard into either input paths (folder/files) or text.
        ctext, cinputs = _resolve_clipboard(args.task_type, _read_clipboard())
        if cinputs:
            raw_inputs = cinputs + raw_inputs
        elif ctext is not None:
            text = ctext
    inputs = _expand_inputs(args.task_type, raw_inputs)
    return FleetTask(
        task_type=args.task_type, text=text, inputs=inputs, options=options,
        force_device=device, engine=getattr(args, "engine", None),
        output_root=getattr(args, "output_root", None),
        idempotency_key=_idempotency_key(args.task_type, text, inputs, options),
    )


def _split_tasks(task: FleetTask) -> list[FleetTask]:
    """Split a multi-input tts/ocr task into ONE JOB PER input file.

    A single job with many inputs can only ever run on one device (the executor stages them into
    one worker invocation), so a folder or multi-file selection would never spread across the
    fleet — it would pile the whole folder onto the single fastest device. Emitting one job per
    file lets the dispatcher place them across devices (and it still re-batches the files that land
    on the same device into one cold model load — Invariant 0 is preserved). Text jobs, cmd jobs,
    and single-input jobs pass through unchanged. Each split job gets its own idempotency key so a
    re-submitted folder de-dupes per file rather than as an all-or-nothing whole."""
    if task.task_type not in ("tts", "ocr") or len(task.inputs) <= 1:
        return [task]
    return [dataclasses.replace(
        task, inputs=[p],
        idempotency_key=_idempotency_key(task.task_type, task.text, [p], task.options))
        for p in task.inputs]


def _candidate_devices(task: FleetTask, config) -> list[str]:
    if task.force_device:
        return [task.force_device]
    return adapters.supported_devices(task.task_type) or list(config.devices.keys())


def _snapshots(task: FleetTask, config, fcfg) -> dict:
    snaps = {}
    for name in _candidate_devices(task, config):
        dev = config.devices.get(name)
        if dev is not None:
            snaps[name] = probes.build_snapshot(dev, make_transport(dev), fcfg)
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
    adapters.configure(config)   # load [fleet.adapters] from devices.toml (core ships none)
    classified = adapters.with_variant(task, fcfg)
    features = adapters.extract_features(classified)
    snaps = _snapshots(classified, config, fcfg)
    profs = load_costs(config, state_root)
    result = placement.plan_jobs([classified], [features], snaps, profs, fcfg,
                                 safety_fraction(config), device_backlog=q.active_backlog())
    if not result.batches:
        # Couldn't place RIGHT NOW. If the task is FORCED to a device that's merely BUSY (another
        # job is running there, so its RAM/VRAM is temporarily taken), it's not a dead-end: the job
        # is queued and will run when that device frees. Report queued-behind-busy, not the
        # alarming "no device (insufficient RAM)" for a forced job that is just
        # waiting behind that device's current work.
        forced = task.force_device
        if forced and (q.active_by_device().get(forced, 0)
                       or _pool_lease_count(q, classified, forced)):
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
    busy = active > 0 or _pool_lease_count(q, classified, b.device) > 0
    return {"device": b.device, "engine": adapters.engine_for(classified, b.device),
            "variant": classified.options.get("_variant"), "device_busy": busy,
            "active_on_device": active, "estimated_finish_s": b.estimated_finish_s}


def _route_preview_multi(tasks: list[FleetTask], config, q: FleetQueue, state_root) -> dict:
    """Routing prediction for a MULTI-JOB submit (a folder / many files): how the jobs are
    expected to SPREAD across devices this drain. Same caveat as ``_route_preview`` — a hint, not
    a commitment; the dispatcher does the authoritative placement (and re-batching) at drain time."""
    fcfg = fleet_config(config)
    adapters.configure(config)
    classified = [adapters.with_variant(t, fcfg) for t in tasks]
    feats = [adapters.extract_features(t) for t in classified]
    snaps = _snapshots(classified[0], config, fcfg)
    profs = load_costs(config, state_root)
    result = placement.plan_jobs(classified, feats, snaps, profs, fcfg,
                                 safety_fraction(config), device_backlog=q.active_backlog())
    by_device: dict[str, int] = {}
    for b in result.batches:
        by_device[b.device] = by_device.get(b.device, 0) + len(b.job_indices)
    placed = sum(by_device.values())
    return {"by_device": by_device, "placed": placed, "total": len(tasks),
            "unplaced": len(tasks) - placed, "skipped": result.skipped,
            "makespan_s": result.makespan_s}


def _route_line_multi(task_type: str, preview: dict, queued_total: int) -> str:
    """One concise, ASCII, prefix-free line summarizing a multi-job spread for a trigger HUD."""
    label = {"tts": "TTS", "ocr": "OCR", "cmd": "cmd"}.get(task_type, task_type)
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
    fcfg = fleet_config(config)
    adapters.configure(config)   # load [fleet.adapters] from devices.toml (core ships none)
    task = adapters.with_variant(_build_task(args), fcfg)   # classify regime pre-placement
    features = adapters.extract_features(task)
    snaps = _snapshots(task, config, fcfg)
    profs = load_costs(config, default_state_root())
    result = placement.plan_jobs([task], [features], snaps, profs, fcfg, safety_fraction(config))

    if args.json:
        print(json.dumps({
            "task_type": task.task_type,
            "features": features.__dict__,
            "batches": [{"device": b.device, "jobs": b.job_indices,
                         "estimated_finish_s": b.estimated_finish_s, "reason": b.reason}
                        for b in result.batches],
            "skipped": result.skipped, "makespan_s": result.makespan_s, "note": result.note,
        }, indent=2, sort_keys=True))
        return EXIT_OK
    reporter.event("fleet_task", task_type=task.task_type, files=features.file_count,
                   pages=features.pages, chars=features.text_chars,
                   pages_approx=features.pages_approx,
                   variant=task.options.get("_variant"))
    for name, snap in sorted(snaps.items()):
        reporter.event("candidate", device=name, reachable=snap.reachable,
                       cpu_busy_pct=snap.cpu_busy_pct, ram_free_mb=snap.ram_free_mb,
                       vram_free_mb=snap.vram_free_mb,
                       engines=sorted(snap.engines_available))
    for b in result.batches:
        reporter.event("placement", device=b.device, jobs=len(b.job_indices),
                       estimated_finish_s=b.estimated_finish_s, reason=b.reason)
    for dev, why in sorted(result.skipped.items()):
        reporter.event("skipped", device=dev, reason=why)
    if not result.batches:
        reporter.event("unplaceable", note=result.note)
        return EXIT_ERROR
    return EXIT_OK


def _route_line(task_type: str, route: dict, will_run: bool, queued_total: int) -> str:
    """One concise, ASCII, prefix-free line for a trigger tooltip/HUD."""
    label = {"tts": "TTS", "ocr": "OCR", "cmd": "cmd"}.get(task_type, task_type)
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
    base = _build_task(args)
    if getattr(args, "clipboard", False) and base.task_type in ("tts", "ocr") \
            and not base.text and not base.inputs:
        kind = "files/folder" if base.task_type == "ocr" else "text or files/folder"
        reporter.event("error", message=f"clipboard had no usable {kind} for {base.task_type}")
        return EXIT_ERROR
    # A folder / multi-file submit becomes one job PER file so the dispatcher can spread them
    # across devices (a single multi-input job is pinned to one device). Single-file / text / cmd
    # submits stay one job.
    tasks = _split_tasks(base)
    state_root = default_state_root()
    # --json / --route-line carry a routing + queue preview for the trigger UI (probes devices
    # live, so they need the config); a plain submit stays fast (enqueue only, no probe).
    route_line = getattr(args, "route_line", False)
    need_route = args.json or route_line
    config = load_config() if need_route else None
    q = FleetQueue(state_root / "fleet" / "fleet.db")
    route, will_run, preview_multi = {}, False, None
    try:
        jids = [q.enqueue(t, priority=getattr(args, "priority", 0)) for t in tasks]
        queued_total = q.counts().get("queued", 0) if need_route else 0
        if need_route:
            if len(tasks) == 1:
                route = _route_preview(base, config, q, state_root)
                # runs on the next tick only if a device was chosen, it's free, and nothing else is
                # queued ahead of this job (queued_total counts this one).
                will_run = (bool(route.get("device")) and not route.get("device_busy")
                            and queued_total <= 1)
            else:
                preview_multi = _route_preview_multi(tasks, config, q, state_root)
    finally:
        q.close()
    if route_line:
        if len(tasks) == 1:
            print(_route_line(base.task_type, route, will_run, queued_total))
        else:
            print(_route_line_multi(base.task_type, preview_multi or {}, queued_total))
    elif args.json:
        if len(tasks) == 1:
            payload = {"job_id": jids[0], "task_type": base.task_type,
                       "queued_total": queued_total, "will_run": will_run, **route}
        else:
            payload = {"job_ids": jids, "task_type": base.task_type, "jobs": len(jids),
                       "queued_total": queued_total, **(preview_multi or {})}
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        for jid, t in zip(jids, tasks):
            reporter.event("enqueued", job_id=jid, task_type=t.task_type,
                           device=t.force_device or "auto")
    return EXIT_OK


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
        reporter.event("job", job_id=j["job_id"], task_type=j["task_type"],
                       state=j["state"], device=j.get("assigned_device") or "-")
    return EXIT_OK


def cmd_run(args, reporter: Reporter) -> int:
    config = load_config()
    adapters.configure(config)   # load [fleet.adapters] from devices.toml (core ships none)
    task = _build_task(args)
    # Acquire the same configured resource lease the dispatcher uses, so an
    # ad-hoc run cannot race a dispatcher batch onto an exclusive resource
    # (--no-lease opts out for dev/test).
    result = executor.run_once(task, config, use_lease=not getattr(args, "no_lease", False))
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    else:
        reporter.event("fleet_run", **{k: v for k, v in result.items()
                                       if k not in ("stdout_tail", "stderr_tail", "telemetry")})
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
    """Live hardware for the fleet: CPU / RAM / GPU per device.

    Distinct from `fleet status`, which reports the job QUEUE. This probes the
    devices themselves and is safe to run at any time: it never mutates state,
    never stages files, and never touches the queue.
    """
    from . import local_resources, resources
    from .resources_render import render_table, to_dict

    config = load_config()
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

    # Progress to stderr: a fleet probe takes seconds per device, and a blank
    # terminal is indistinguishable from a hang. Suppressed for --json (which
    # must stay machine-parseable) and when stderr is not a TTY.
    quiet = getattr(args, "json", False) or getattr(args, "no_progress", False)
    show = not quiet and sys.stderr.isatty()
    pending: set[str] = set()
    # Width of the last status line written, so it can be erased exactly. A `\r`
    # alone only moves the cursor: without overwriting, a shorter line leaves the
    # tail of the longer one behind, which renders as garbage like
    # "ok DEV1ing DEV1, DEV2".
    last_width = 0

    def on_event(kind: str, name: str, view) -> None:  # noqa: ANN001
        nonlocal last_width
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
    if not getattr(args, "no_local", False) and not wanted:
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
    else:
        print(render_table(views))

    # Exit nonzero only if EVERY remote device failed: a single offline laptop
    # is normal and must not make the command look broken to a caller.
    remote = [v for v in views if not v.is_local]
    if remote and not any(v.reachable for v in remote):
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
        sp.add_argument("task_type", choices=("tts", "ocr", "cmd"))
        sp.add_argument("--text", help="inline text payload (e.g. TTS clipboard)")
        sp.add_argument("--input", action="append", help="input file or folder (repeatable)")
        sp.add_argument("--clipboard", action="store_true",
                        help="use the OS clipboard as the payload: a folder path -> its eligible "
                             "files, file path(s) -> those files, else the text")
        sp.add_argument("--device", help="force a configured device")
        sp.add_argument("--engine", help="force an engine")
        sp.add_argument("--opt", action="append", help="task option key=value (repeatable)")
        sp.add_argument("--output-root", dest="output_root", help="override output folder")
        sp.add_argument("--json", action="store_true")
        sp.add_argument("--route-line", dest="route_line", action="store_true",
                        help="print one concise routing line for a trigger tooltip/HUD, then exit")

    pp = sub.add_parser("plan", help="show placement decision; runs nothing")
    add_common(pp)
    ps = sub.add_parser("submit", help="enqueue a job")
    add_common(ps)
    ps.add_argument("--priority", type=int, default=0)
    ps.add_argument("--allow-fallback", action="store_true",
                    help="when used with --device, retry later attempts with automatic placement "
                         "if the forced device fails")
    pr = sub.add_parser("run", help="run one job now (place -> stage -> exec)")
    add_common(pr)
    pr.add_argument("--argv", nargs=argparse.REMAINDER, help="for task_type=cmd: the command")
    pr.add_argument("--no-lease", action="store_true",
                    help="skip the configured resource lease (dev/test; may race the dispatcher)")
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
    prs = sub.add_parser("resources", help="live CPU / RAM / GPU for every configured device "
                                           "(hardware, not the job queue)")
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
        if args.fleet_command == "dispatch":
            return cmd_dispatch(args, reporter)
        if args.fleet_command == "clear":
            return cmd_clear(args, reporter)
        if args.fleet_command == "cancel":
            return cmd_cancel(args, reporter)
        if args.fleet_command == "resources":
            return cmd_resources(args, reporter)
        if args.fleet_command == "mesh":
            return cmd_mesh(args, reporter)
    except Exception as exc:  # noqa: BLE001 - keep agent-visible error concise
        reporter.event("error", type=type(exc).__name__, message=str(exc))
        return EXIT_ERROR
    return EXIT_ERROR
