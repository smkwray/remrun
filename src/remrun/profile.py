"""Per-(project, command, device) resource + timing profiles.

Records a rolling EWMA, from the telemetry remrun already captures, of each job's
typical peak RAM, CPU%, **remote exec time**, **full remrun round-trip time**, and
the **overhead** (trip − exec) — keyed by the device it ran on. A special ``LOCAL``
device row holds a locally-measured baseline (``remrun bench``).

Two consumers, both advisory (a missing profile just means default behavior):
  * scheduler placement inside ``--auto`` (RAM-headroom, trivial-skip) via ``predict_job``;
  * the offload decision (local vs. remote) via ``recommend_offload``.

One small bounded JSON file; never synced (local regenerable state).
"""
from __future__ import annotations

import json
import os
import re
import socket
from pathlib import Path
from typing import Any

PROFILE_FILE = "profiles.json"
DEFAULT_ALPHA = 0.4          # EWMA weight on the newest observation
DEFAULT_MAX_ENTRIES = 64     # cap total (project, command, device) rows; evict oldest
LOCAL_DEVICE = "LOCAL"       # pseudo-device key for the locally-measured baseline

_SCRIPT_EXTS = (".py", ".r", ".jl", ".sh", ".do", ".rb", ".pl", ".js", ".ts")


def command_key(command: list[str]) -> str:
    """Coarse signature that groups repeated invocations of the same job.

    Interpreter basename + first script-like arg basename, with paths and
    varying args/configs stripped: ["python", "-B", "do/run.py", "cfg.yaml"]
    -> "python:run.py". Falls back to the first non-flag arg, else the leading
    token. Intentionally fuzzy — same script over different data shares a key.
    """
    tokens = [t for t in command if t]
    if not tokens:
        return "?"
    lead = Path(tokens[0]).name
    script = None
    for t in tokens[1:]:
        if t.startswith("-"):
            continue
        if Path(t).name.lower().endswith(_SCRIPT_EXTS):
            script = Path(t).name
            break
    if script is None:
        for t in tokens[1:]:
            if not t.startswith("-"):
                script = Path(t).name
                break
    return f"{lead}:{script}" if script else lead


def _path(state_root: Path) -> Path:
    return Path(state_root) / PROFILE_FILE


def load_profiles(state_root: Path) -> dict[str, Any]:
    p = _path(state_root)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError, ValueError):
        return {}


def _ewma(old, new, alpha: float):
    # Keep millisecond precision in the store; round only at display time. (Rounding
    # to 0.1 s here used to collapse sub-second overhead to 0.0.)
    if new is None:
        return old
    if old is None:
        return round(float(new), 3)
    return round(alpha * float(new) + (1.0 - alpha) * float(old), 3)


def _devmap(profiles: dict, project_id: str, key: str) -> dict[str, dict]:
    """The ``{device: entry}`` map for (project, command-key).

    Tolerates legacy flat entries (pre per-device schema): those have scalar
    values, so they filter out to ``{}`` and are simply re-learned. profiles.json
    is local regenerable state, so no migration is needed.
    """
    node = (profiles.get(project_id) or {}).get(key)
    if not isinstance(node, dict):
        return {}
    return {d: e for d, e in node.items() if isinstance(e, dict)}


def device_profile(profiles: dict, project_id: str, key: str, device: str) -> dict | None:
    """The rolling estimate for one (project, command-key, device), or None."""
    return _devmap(profiles, project_id, key).get(device)


def predict_job(profiles: dict, project_id: str, key: str) -> dict | None:
    """Device-agnostic estimate for scheduler pre-selection.

    ``rss_mb`` = max observed peak RSS (a job property, roughly device-independent;
    take the max for safe RAM-headroom placement). ``dur_s`` = min remote ``exec_s``
    (best-case "is this quick?" for trivial-skip; ``LOCAL`` excluded). Returns None
    when nothing is known.
    """
    devs = _devmap(profiles, project_id, key)
    if not devs:
        return None
    rss_vals = [e["rss_mb"] for e in devs.values() if e.get("rss_mb") is not None]
    remote = {d: e for d, e in devs.items() if d != LOCAL_DEVICE}
    exec_src = remote or devs
    exec_vals = [e["exec_s"] for e in exec_src.values() if e.get("exec_s") is not None]
    rss = max(rss_vals) if rss_vals else None
    dur = min(exec_vals) if exec_vals else None
    if rss is None and dur is None:
        return None
    return {"rss_mb": rss, "dur_s": dur}


def recommend_offload(profiles: dict, project_id: str, key: str, *,
                      devices: list[str] | None = None, bias: float = 1.0) -> dict:
    """Recommend local vs. remote for a job, from measured local + trip times.

    Compares the ``LOCAL`` baseline ``exec_s`` against the best candidate device's
    full ``trip_s`` (push+exec+pullback). ``bias`` tilts toward remote for
    responsiveness: with bias 1.25, remote is chosen even when its trip is up to
    25% slower than local. ``devices`` optionally restricts the candidate set.

    Returns ``{recommend: 'remote'|'local'|'unknown', ...}``. 'unknown' means no
    bench data yet — the caller should fall back to the `[offload]` policy.
    """
    devs = _devmap(profiles, project_id, key)
    local = devs.get(LOCAL_DEVICE) or {}
    local_s = local.get("exec_s")
    remote = {d: e for d, e in devs.items()
              if d != LOCAL_DEVICE and e.get("trip_s") is not None
              and (devices is None or d in devices)}
    if not remote or local_s is None:
        return {"recommend": "unknown", "local_s": local_s,
                "have_remote": bool(remote), "have_local": local_s is not None}
    best_dev = min(remote, key=lambda d: remote[d]["trip_s"])
    best_trip = float(remote[best_dev]["trip_s"])
    local_s = float(local_s)
    rec = "remote" if best_trip <= local_s * bias else "local"
    return {"recommend": rec, "local_s": round(local_s, 3), "best_device": best_dev,
            "best_trip_s": round(best_trip, 3), "bias": bias}


# --- portable per-project job costs (travels WITH the project) ----------------
# The RESOURCE cost of a job (peak RAM / CPU% / remote exec time) is a property of the
# job x the device it ran on, NOT of the controller that dispatched it. So those
# portable fields live with the project — in do/remrun/job_costs.json, which syncs like
# the rest of do/ — and any controller reads them as the base for --auto placement. The
# controller-specific fields (trip_s/overhead_s and the LOCAL offload baseline) stay in
# the local state profiles.json (they depend on this box's network/CPU). Raw telemetry/logs
# stay local; the distilled portable cost model travels.
JOB_COSTS_FILE = "job_costs.json"   # legacy single-writer file; now read-only (merged for migration)
_PORTABLE_FIELDS = ("rss_mb", "cpu_pct", "exec_s")


def _controller_id() -> str:
    """Per-controller tag for the job_costs filename so multiple controllers writing the SAME
    synced project don't fight over one file (the Syncthing shared-writer conflict the consult
    flagged). Sanitized hostname — stable and distinct per box."""
    host = socket.gethostname() or "controller"
    return re.sub(r"[^A-Za-z0-9]+", "-", host).strip("-").lower() or "controller"


def _job_costs_dir(project_root: Path) -> Path:
    return Path(project_root) / "do" / "remrun"


def job_costs_path(project_root: Path) -> Path:
    """THIS controller's job-costs file (the write target). Per-controller (``job_costs.<id>.json``)
    so two controllers never read-modify-write one shared file; readers MERGE all of them."""
    return _job_costs_dir(project_root) / f"job_costs.{_controller_id()}.json"


def load_job_costs(project_root: Path) -> dict[str, Any]:
    """Read + MERGE every do/remrun job-costs file — this controller's ``job_costs.<id>.json``, the
    other controllers', and the legacy single ``job_costs.json`` — into
    {command_key: {device: {portable fields}}}. Per-controller files remove the shared-writer
    conflict; merging on read keeps the portable cost knowledge project-wide (newest ``updated``
    wins for a duplicated (command, device) row)."""
    d = _job_costs_dir(project_root)
    if not d.is_dir():
        return {}
    paths: list[Path] = []
    legacy = d / JOB_COSTS_FILE
    if legacy.exists():
        paths.append(legacy)
    paths += sorted(d.glob("job_costs.*.json"))   # per-controller files (excludes the legacy single)
    merged: dict[str, dict[str, Any]] = {}
    for p in paths:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, ValueError):
            continue
        costs = data.get("costs") if isinstance(data, dict) else None
        if not isinstance(costs, dict):
            continue
        for k, v in costs.items():
            if not isinstance(v, dict):
                continue
            row = merged.setdefault(k, {})
            for dev, e in v.items():
                # Defensive: a LOCAL (offload-baseline) row is controller-specific and must never
                # travel between controllers — strip it on read (also guards a conflicted file).
                if dev == LOCAL_DEVICE or not isinstance(e, dict):
                    continue
                cur = row.get(dev)
                if cur is None or str(e.get("updated", "")) >= str(cur.get("updated", "")):
                    row[dev] = e
    return merged


def update_job_costs(project_root: Path, key: str, device: str, *, rss_mb=None,
                     cpu_pct=None, exec_s=None, now: str = "", alpha: float = DEFAULT_ALPHA,
                     max_entries: int = DEFAULT_MAX_ENTRIES) -> None:
    """Fold one run's PORTABLE costs (rss/cpu/exec) for (command, device) into the
    project's job_costs.json (EWMA, atomic). Skips the LOCAL baseline (controller-
    specific). Best-effort; never raises (a read-only project just isn't recorded)."""
    if device == LOCAL_DEVICE:
        return
    p = job_costs_path(project_root)
    try:
        existing = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    except (json.JSONDecodeError, OSError, ValueError):
        existing = {}
    costs = existing.get("costs") if isinstance(existing, dict) else None
    if not isinstance(costs, dict):
        costs = {}
    devmap = costs.get(key)
    if not isinstance(devmap, dict):
        devmap = {}
        costs[key] = devmap
    prev = devmap.get(device) if isinstance(devmap.get(device), dict) else {}
    devmap[device] = {
        "n": int(prev.get("n", 0)) + 1,
        "rss_mb": _ewma(prev.get("rss_mb"), rss_mb, alpha),
        "cpu_pct": _ewma(prev.get("cpu_pct"), cpu_pct, alpha),
        "exec_s": _ewma(prev.get("exec_s"), exec_s, alpha),
        "updated": now,
    }
    flat = [(k, d, e.get("updated", "")) for k, ds in costs.items() if isinstance(ds, dict)
            for d, e in ds.items() if isinstance(e, dict)]
    if len(flat) > max_entries:
        flat.sort(key=lambda x: x[2])
        for k, d, _ in flat[: len(flat) - max_entries]:
            ds = costs.get(k)
            if isinstance(ds, dict):
                ds.pop(d, None)
                if not ds:
                    costs.pop(k, None)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + f".tmp{os.getpid()}")
        tmp.write_text(json.dumps({"version": 1, "costs": costs}, indent=2, sort_keys=True),
                       encoding="utf-8")
        os.replace(tmp, p)
    except OSError:
        pass


def merge_job_costs(profiles: dict, project_id: str, job_costs: dict) -> dict:
    """Overlay the portable project job-costs UNDER the local profiles for ``project_id``:
    a fresh controller (no local row yet) still gets rss/cpu/exec for placement, while a
    controller that has run the job keeps its own refined values. Controller-specific
    fields (trip_s/overhead_s/LOCAL) come only from the local store. Returns a new dict;
    does not mutate ``profiles``."""
    if not job_costs:
        return profiles
    merged = dict(profiles)
    proj = {k: ({d: dict(e) for d, e in v.items() if isinstance(e, dict)}
                if isinstance(v, dict) else v)
            for k, v in (merged.get(project_id) or {}).items()}
    for key, devmap in job_costs.items():
        if not isinstance(devmap, dict):
            continue
        node = proj.get(key)
        if not isinstance(node, dict):
            node = {}
            proj[key] = node
        for device, pentry in devmap.items():
            if not isinstance(pentry, dict):
                continue
            out = dict(node.get(device) if isinstance(node.get(device), dict) else {})
            for f in _PORTABLE_FIELDS:
                if out.get(f) is None and pentry.get(f) is not None:
                    out[f] = pentry[f]
            out.setdefault("n", int(pentry.get("n", 0)))
            node[device] = out
    merged[project_id] = proj
    return merged


def update_profile(state_root: Path, project_id: str, key: str, device: str, *,
                   peak_rss_mb=None, avg_cpu_pct=None, exec_s=None, trip_s=None,
                   now: str = "", alpha: float = DEFAULT_ALPHA,
                   max_entries: int = DEFAULT_MAX_ENTRIES) -> None:
    """Fold one run's observed cost for (project, command-key, device) into the
    store (EWMA, bounded). ``overhead_s`` is derived as ``trip_s - exec_s`` when
    both are present. Best-effort; never raises."""
    profiles = load_profiles(state_root)
    proj = profiles.setdefault(project_id, {})
    node = proj.get(key)
    # Fresh, or a legacy flat entry (scalar values) → restart as a per-device map.
    if not isinstance(node, dict) or (node and not any(isinstance(v, dict) for v in node.values())):
        node = {}
    proj[key] = node
    prev = node.get(device) or {}
    overhead = None
    if trip_s is not None and exec_s is not None:
        overhead = max(0.0, round(float(trip_s) - float(exec_s), 3))
    node[device] = {
        "n": int(prev.get("n", 0)) + 1,
        "rss_mb": _ewma(prev.get("rss_mb"), peak_rss_mb, alpha),
        "cpu_pct": _ewma(prev.get("cpu_pct"), avg_cpu_pct, alpha),
        "exec_s": _ewma(prev.get("exec_s"), exec_s, alpha),
        "trip_s": _ewma(prev.get("trip_s"), trip_s, alpha),
        "overhead_s": _ewma(prev.get("overhead_s"), overhead, alpha),
        "updated": now,
    }
    # Bound total device-rows; evict oldest by 'updated', pruning empty parents.
    flat = [(pid, k, d, e.get("updated", ""))
            for pid, ks in profiles.items() if isinstance(ks, dict)
            for k, ds in ks.items() if isinstance(ds, dict)
            for d, e in ds.items() if isinstance(e, dict)]
    if len(flat) > max_entries:
        flat.sort(key=lambda x: x[3])
        for pid, k, d, _ in flat[: len(flat) - max_entries]:
            ds = profiles.get(pid, {}).get(k)
            if isinstance(ds, dict):
                ds.pop(d, None)
                if not ds:
                    profiles[pid].pop(k, None)
            if not profiles.get(pid):
                profiles.pop(pid, None)
    # Atomic replace so a concurrent reader never sees a torn file (which
    # load_profiles would treat as empty and the next write would then clobber).
    # NOTE: this prevents corruption, not lost updates — two concurrent
    # read-modify-write runs can still drop one another's row. profiles.json is
    # local regenerable state, so the residual race is acceptable for now.
    try:
        p = _path(state_root)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + f".tmp{os.getpid()}")
        tmp.write_text(json.dumps(profiles, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, p)
    except OSError:
        pass
