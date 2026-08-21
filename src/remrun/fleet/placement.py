"""Cost model + placement/makespan. Pure functions over snapshots + profiles.

Invariant 0: model load is ALWAYS the cold ``fixed_load_s``, charged once per batch
(one model lifetime). The only amortization is batching jobs that share that one
load. Memory is per-loaded-model, not per-job (jobs in a batch run sequentially in
one model), so batch size does not multiply RAM/VRAM.
"""
from __future__ import annotations

import hashlib

from .adapters import (
    candidate_devices, engine_for, memory_kind_for, option_bucket,
    required_capabilities, task_provided_capabilities,
)
from .models import (
    DeviceSnapshot, FleetTask, JobFeatures, PlacedBatch, PlacementResult,
)
from .profiles import (
    duration_observation_count, estimate_unavailable_reason, prepared_profile,
    prepared_profile_family_id, prepared_resource_profile,
)


def _profile(task: FleetTask, device: str, profiles: dict):  # noqa: ANN001
    return prepared_profile(profiles, task, device)


def transfer_seconds(total_bytes: int, file_count: int, fleet_cfg: dict) -> float:
    if total_bytes <= 0 and file_count <= 0:
        return 0.0
    mbps = float(fleet_cfg.get("transfer_mbps", 200.0))
    bytes_per_s = max(1.0, mbps * 125000.0)              # Mbps -> bytes/s (1e6/8)
    return (float(fleet_cfg.get("ssh_setup_s", 0.6))
            + total_bytes / bytes_per_s
            + file_count * float(fleet_cfg.get("per_file_overhead_s", 0.05)))


def estimate_finish(indices: list[int], device: str, tasks: list[FleetTask],
                    features: list[JobFeatures], profiles: dict, fleet_cfg: dict,
                    input_local: bool = False) -> float | None:
    """Estimated wall time to process ``indices`` as one batch on ``device``.

    One cold model load + per-job variable compute + (optional) input transfer.
    Output transfer is 0: outputs land in the configured output folder and any
    external sync tool can converge later (correctness-now fetches are the executor's
    concern, not the estimate's).
    """
    if not indices:
        return 0.0
    t0 = tasks[indices[0]]
    prof = _profile(t0, device, profiles)
    if prof is None:
        return None
    prepared_units = sum(features[i].units() for i in indices)
    if (prof.min_units is not None and prof.max_units is not None
            and not prof.min_units <= prepared_units <= prof.max_units):
        return None
    fixed = float(prof.fixed_load_s or 0.0)
    var_rate = float(prof.var_per_unit_s or 0.0)
    variable = sum(var_rate * features[i].units() for i in indices)
    compute = (fixed + variable) * (1.0 + _confidence_penalty(prof, fleet_cfg))
    tin = 0.0
    if not input_local:
        tin = transfer_seconds(sum(features[i].input_bytes for i in indices),
                               sum(features[i].file_count for i in indices), fleet_cfg)
    return round(compute + tin, 3)


def _confidence_penalty(profile, fleet_cfg: dict) -> float:  # noqa: ANN001
    """Fractional uncertainty pad for low-confidence time estimates.

    Local observations carry ``n>0`` and the pad decays as observations accumulate. Priors and
    shared seed costs carry ``n=0`` and receive the full configured pad, so placement prefers a
    similarly fast learned profile without letting confidence swamp a large true speed gap.
    """
    cap = max(0.0, float((fleet_cfg or {}).get("confidence_penalty_frac", 0.0)))
    if cap <= 0.0:
        return 0.0
    n = max(0, int(getattr(profile, "duration_n", 0) or 0))
    if n <= 0:
        return cap
    return cap / (n ** 0.5)


def _exploration_slot(task: FleetTask) -> int:
    """Stable one-in-four slot for bounded mixed-state calibration."""
    family = prepared_profile_family_id(task)
    prepared_id = str(task.prepared.get("prepared_id") or "")
    digest = hashlib.sha256(f"{family}\0{prepared_id}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % 4


def predicted_resources(task: FleetTask, device: str, profiles: dict) -> tuple[float, float]:
    prof = prepared_resource_profile(profiles, task, device)
    if prof is None:
        return 0.0, 0.0
    return float(prof.peak_rss_mb or 0.0), float(prof.peak_vram_mb or 0.0)


def fits(task: FleetTask, device: str, snap: DeviceSnapshot, profiles: dict,
         safety_fraction: float, fleet_cfg: dict | None = None, *,
         allow_unknown_capability: bool = False) -> tuple[bool, str]:
    """Whether ``device`` can host ``task`` right now.

    Resource measurements may degrade to unknown, but adapter qualification is
    fail-closed for automatic placement. An explicit-device run may proceed to
    the worker's immediate preflight when ``allow_unknown_capability`` is true.
    """
    fleet_cfg = fleet_cfg or {}
    if not snap.reachable:
        return False, "unreachable"
    if snap.active_jobs >= snap.max_jobs:
        return False, "at max_jobs"
    a = ((task.resolved_spec.get("adapters") or {}).get(device)
         if task.resolved_spec else None)
    is_command = bool(task.prepared and task.prepared["kind"] == "command")
    if not is_command and a is None:
        return False, "no adapter for this device"
    try:
        required = required_capabilities(task)
        provided = task_provided_capabilities(task, device) if required else frozenset()
    except ValueError as exc:
        return False, str(exc)
    missing = sorted(required - provided)
    if missing:
        return False, f"missing required capabilities: {', '.join(missing)}"
    if a is not None:                       # adapter tasks: pool + capability gates
        pool = a.get("pool")
        if pool is not None:
            free = snap.pool_free.get(pool)
            if free is not None and free <= 0:
                return False, f"no {pool} slot free"
        try:
            eng = engine_for(task, device)
        except ValueError as exc:
            return False, str(exc)
        status = snap.engine_status.get(eng, "unknown")
        if status == "absent":
            return False, f"engine {eng} not installed"
        if status != "present" and not allow_unknown_capability:
            return False, f"engine {eng} qualification unknown"

    # Memory fit: GPU model jobs are gated on VRAM on a discrete-GPU device (host
    # RAM only needs headroom for the process), or on unified memory on a Mac. CPU
    # jobs are gated on system RAM, VRAM irrelevant. Unknown probe values never veto.
    rss, vram = predicted_resources(task, device, profiles)
    sf = safety_fraction
    discrete_gpu = snap.vram_total_mb is not None and snap.vram_total_mb > 0
    if memory_kind_for(task, device) == "gpu":
        if discrete_gpu:
            # Two-part GPU memory gate. A configured pool makes a model job
            # exclusive among fleet jobs, but it cannot see external GPU/desktop use.
            #  (a) can-ever-fit: the model peak plus an engine-specific reserve must
            #      fit the card's full capacity; set reserves in [fleet.vram_reserve_mb].
            #  (b) can-launch-now: only for engines configured as non-elastic. For
            #      those, the model must fit LIVE free VRAM now. The default set is
            #      empty, so this check only fires when users opt in.
            eng = engine_for(task, device)
            reserves = fleet_cfg.get("vram_reserve_mb", {}) or {}
            reserve = float(reserves.get(eng, reserves.get("default", 0.0)))
            cap = snap.vram_total_mb
            if cap is not None and vram + reserve > cap:
                need = vram + reserve
                return False, f"model needs ~{need:.0f}MB VRAM > the {cap:.0f}MB card"
            nonelastic = set(fleet_cfg.get("gpu_nonelastic_engines", []) or [])
            if eng in nonelastic and snap.vram_free_mb is not None:
                frag = float(fleet_cfg.get("vram_frag_reserve_mb", 512.0))
                if vram + frag > snap.vram_free_mb:
                    return False, (f"insufficient free VRAM now (~{vram:.0f}+{frag:.0f}MB > "
                                   f"{snap.vram_free_mb:.0f}MB free)")
            if snap.ram_free_mb is not None and rss > snap.ram_free_mb * sf:
                return False, f"insufficient host RAM (~{rss:.0f}MB > {sf:.0%} of free)"
        else:                                # unified memory: model footprint hits RAM
            need = max(vram, rss)
            if snap.ram_free_mb is not None and need > snap.ram_free_mb * sf:
                return False, f"insufficient unified memory (~{need:.0f}MB > {sf:.0%} of free)"
    else:                                    # CPU job
        if snap.ram_free_mb is not None and rss > snap.ram_free_mb * sf:
            return False, f"insufficient RAM (~{rss:.0f}MB > {sf:.0%} of free)"
    return True, "ok"


def _candidate_devices(task: FleetTask, snapshots: dict[str, DeviceSnapshot]) -> list[str]:
    """Devices named by the frozen task record and present in snapshots."""
    return [device for device in candidate_devices(task) if device in snapshots]


def _fitting_devices(task: FleetTask, snapshots: dict[str, DeviceSnapshot],
                     profiles: dict, sf: float,
                     fleet_cfg: dict | None = None) -> tuple[list[str], dict[str, str]]:
    fitting, skipped = [], {}
    for dev in _candidate_devices(task, snapshots):
        ok, why = fits(task, dev, snapshots[dev], profiles, sf, fleet_cfg)
        (fitting.append(dev) if ok else skipped.__setitem__(dev, why))
    return fitting, skipped


def _greedy_split(indices: list[int], devices: list[str], tasks, features,
                  profiles, fleet_cfg, input_local_map,
                  device_backlog: dict[str, float] | None = None) -> dict[str, list[int]]:
    """List-scheduling: assign each job (largest first) to the device that yields
    the smallest resulting makespan. Fixed load is charged once per device batch
    (estimate_finish handles that); a device's pre-existing backlog raises its makespan."""
    bl = device_backlog or {}
    assign: dict[str, list[int]] = {d: [] for d in devices}
    order = sorted(indices, key=lambda i: features[i].units(), reverse=True)
    for i in order:
        best_dev, best_makespan = None, float("inf")
        for d in devices:
            trial = assign[d] + [i]
            own = estimate_finish(
                trial, d, tasks, features, profiles, fleet_cfg,
                input_local_map.get((i, d), False),
            )
            if own is None or bl.get(d) is None:
                continue
            f = float(bl.get(d, 0.0)) + own
            alternatives = []
            valid = True
            for other in devices:
                if other == d:
                    continue
                other_own = estimate_finish(
                    assign[other], other, tasks, features, profiles, fleet_cfg, False,
                )
                if other_own is None or bl.get(other) is None:
                    valid = False
                    break
                alternatives.append(float(bl.get(other, 0.0)) + other_own)
            if not valid:
                continue
            makespan = max([f] + alternatives)
            if makespan < best_makespan:
                best_dev, best_makespan = d, makespan
        if best_dev is None:
            raise ValueError("split requires known estimates and backlog for every device")
        assign[best_dev].append(i)
    return {d: ix for d, ix in assign.items() if ix}


def assign_group(indices: list[int], tasks: list[FleetTask], features: list[JobFeatures],
                 snapshots: dict[str, DeviceSnapshot], profiles: dict, fleet_cfg: dict,
                 sf: float, device_backlog: dict[str, float] | None = None
                 ) -> tuple[list[PlacedBatch], dict[str, str]]:
    """Place one group of jobs that share a task name and option bucket and so can batch onto a
    single device. Device SELECTION minimizes the backlog-adjusted finish (a device's in-flight
    work delays a new job there, so an idle device can win over a faster-but-busy one), but each
    returned batch stores its OWN compute estimate so the backlog isn't double-counted next tick.
    Returns batches + per-device skip reasons."""
    t0 = tasks[indices[0]]
    skipped: dict[str, str] = {}
    bl = device_backlog or {}

    def _own(ix, d):
        return estimate_finish(ix, d, tasks, features, profiles, fleet_cfg)

    def _eff(ix, d):                 # backlog-adjusted finish — for DEVICE SELECTION only
        own = _own(ix, d)
        backlog = bl.get(d, 0.0)
        if own is None or backlog is None:
            return None
        return float(backlog) + own

    def _batch(d: str, *, reason: str, basis: str) -> PlacedBatch:
        estimate = _own(indices, d)
        estimate_reason = None
        if bl.get(d, 0.0) is None:
            estimate = None
            estimate_reason = "backlog_unknown"
        elif estimate is None:
            estimate_reason = estimate_unavailable_reason(
                profiles, t0, d, sum(features[index].units() for index in indices),
            )
        return PlacedBatch(
            d,
            indices,
            estimate,
            reason,
            basis,
            estimate_reason,
        )

    forced = {t.force_device for t in (tasks[i] for i in indices) if t.force_device}
    if forced:
        if len(forced) > 1:
            return [], {"*": f"jobs in one group forced to different devices: {sorted(forced)}"}
        dev = next(iter(forced))
        ok, why = (fits(t0, dev, snapshots[dev], profiles, sf, fleet_cfg,
                        allow_unknown_capability=True)
                   if snapshots.get(dev) else (False, "no snapshot"))
        if not ok:
            return [], {dev: f"forced but unavailable: {why}"}
        return [_batch(dev, reason="forced", basis="forced")], skipped

    fitting, skipped = _fitting_devices(t0, snapshots, profiles, sf, fleet_cfg)
    if not fitting:
        return [], skipped

    prepared_units = sum(features[index].units() for index in indices)
    estimates = {device: _own(indices, device) for device in fitting}
    estimable = [device for device in fitting if estimates[device] is not None
                 and bl.get(device, 0.0) is not None]
    unestimated = [device for device in fitting if device not in estimable]
    unavailable_reasons = {
        device: (
            "backlog_unknown" if bl.get(device, 0.0) is None
            else estimate_unavailable_reason(profiles, t0, device, prepared_units)
        )
        for device in unestimated
    }

    if len(fitting) == 1:
        device = fitting[0]
        return [_batch(device, reason="batched", basis="sole_qualified")], skipped

    if not estimable:
        device = min(
            fitting,
            key=lambda name: (
                duration_observation_count(profiles, t0, name),
                snapshots[name].active_jobs,
                name,
            ),
        )
        return [_batch(device, reason="batched", basis="cold_start")], skipped

    # Give every otherwise-qualified duration identity four prompt observations.
    # Unknown backlog is not a calibration gap and therefore never steals work
    # from an estimable peer.
    calibration = [
        device for device in unestimated
        if unavailable_reasons[device] != "backlog_unknown"
        if duration_observation_count(profiles, t0, device) < 4
    ]
    if calibration:
        device = min(
            calibration,
            key=lambda name: (
                duration_observation_count(profiles, t0, name),
                snapshots[name].active_jobs,
                name,
            ),
        )
        batch = _batch(device, reason="batched", basis="exploration")
        return [batch], skipped

    # After the prompt budget, only stable slot zero explores an unready model.
    # Invalid models park at eight accepted observations. A ready model whose
    # current work is outside its range remains eligible so its range can grow.
    if _exploration_slot(t0) == 0:
        bounded = [
            device for device in unestimated
            if unavailable_reasons[device] == "out_of_range"
            or (
                unavailable_reasons[device] in {"uncalibrated", "model_unfit"}
                and duration_observation_count(profiles, t0, device) < 8
            )
        ]
        if bounded:
            device = min(
                bounded,
                key=lambda name: (
                    duration_observation_count(profiles, t0, name),
                    snapshots[name].active_jobs,
                    name,
                ),
            )
            return [_batch(device, reason="batched", basis="exploration")], skipped

    # Option A: all on one device (one model load). Pick the smallest backlog-adjusted finish.
    best_d = min(estimable, key=lambda d: _eff(indices, d))
    same = _batch(best_d, reason="batched", basis="estimated")
    same_eff = _eff(indices, best_d)
    if len(indices) < 2 or len(estimable) < 2 or unestimated:
        return [same], skipped

    # Option B: split across devices (a model load each) -> backlog-adjusted makespan = max.
    split_assign = _greedy_split(indices, estimable, tasks, features, profiles, fleet_cfg, {}, bl)
    split_batches = [PlacedBatch(
        d, ix, _own(ix, d), "split", "estimated", None,
    ) for d, ix in split_assign.items()]
    split_makespan = max(_eff(ix, d) for d, ix in split_assign.items())

    hysteresis = _split_hysteresis(indices, t0, features, profiles, fitting, fleet_cfg,
                                   best_finish=min(same_eff, split_makespan))
    if split_makespan + hysteresis < same_eff:
        return split_batches, skipped
    return [same], skipped


def _split_hysteresis(indices: list[int], t0: FleetTask, features: list[JobFeatures],
                      profiles: dict, fitting: list[str], fleet_cfg: dict, *,
                      best_finish: float) -> float:
    """Adaptive tie-break margin a split must beat the single-batch finish by, to stop
    flip-flopping near a crossover (Phase 3c). ``max`` of: a small floor, a fraction of the
    better finish, and the configured unit uncertainty. Replaces the fixed
    ``hysteresis_s=1.0`` (far too small versus a substantial cold load)."""
    min_h = float(fleet_cfg.get("min_hysteresis_s", fleet_cfg.get("hysteresis_s", 5.0)))
    frac = float(fleet_cfg.get("hysteresis_finish_frac", 0.05))
    margin = max(min_h, frac * max(best_finish, 0.0))
    uncertain_units = sum(
        float(features[index].prepared_units or 0.0)
        * float(features[index].relative_uncertainty or 0.0)
        for index in indices
    )
    if uncertain_units > 0:
        var_rate = max((float((_profile(t0, device, profiles).var_per_unit_s
                               if _profile(t0, device, profiles) else 0.0) or 0.0)
                        for device in fitting), default=0.0)
        margin = max(margin, var_rate * uncertain_units)
    return margin


def plan_jobs(tasks: list[FleetTask], features: list[JobFeatures],
              snapshots: dict[str, DeviceSnapshot], profiles: dict,
              fleet_cfg: dict, safety_fraction: float = 0.90,
              device_backlog: dict[str, float] | None = None) -> PlacementResult:
    """Group jobs by configured name and option bucket. Makespan is
    the max finish across all placed batches (groups run concurrently across devices
    where they can). ``device_backlog`` (per-device seconds of in-flight work) makes placement
    backlog-aware: a busy device's finish estimate is raised so a new job routes to an idle
    device when that finishes sooner (rather than always picking the cheapest-per-job device)."""
    groups: dict[tuple[str, str, tuple[str, ...]], list[int]] = {}
    for i, t in enumerate(tasks):
        groups.setdefault((t.task_name, option_bucket(t), t.requires), []).append(i)

    batches: list[PlacedBatch] = []
    skipped: dict[str, str] = {}
    for _key, idx in groups.items():
        b, sk = assign_group(idx, tasks, features, snapshots, profiles, fleet_cfg,
                             safety_fraction, device_backlog)
        batches.extend(b)
        for d, why in sk.items():
            skipped.setdefault(d, why)
    makespan = (None if any(batch.estimated_finish_s is None for batch in batches)
                else max((float(batch.estimated_finish_s) for batch in batches), default=0.0))
    note = "" if batches else "no eligible device for any job"
    return PlacementResult(batches=batches, skipped=skipped, makespan_s=makespan, note=note)
