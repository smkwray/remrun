"""Execute frozen fleet work without configured-workflow vocabulary."""
from __future__ import annotations

import json
import re
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from ..config import RemrunConfig, load_config
from ..job_observation import JobObservation, active_job_observation_enabled
from ..state import default_state_root, iso_plus_seconds, utc_now_iso
from ..transport import GuardFinalizationError, TransportError, make_transport
from . import adapters, placement, probes, profiles
from .config import fleet_config, load_costs, safety_fraction
from .models import FleetTask
from .prepared import (
    SourceChangedError, prepared_memory_limit_mib, snapshot_prepared_input,
)
from .queue import BatchHeartbeat, FleetQueue
from .result_protocol import ResultProtocolError, validate_result_envelope
from .task_contract import resolve_tasks

BATCH_MANIFEST_NAME = "remrun_batch.json"
BATCH_METRICS_NAME = "batch_metrics.json"
DONE_JSON_NAME = "done.json"
MAX_RESULT_EVIDENCE_BYTES = 4 * 1024 * 1024


def _guard_outcome_fields(memory_guard: dict[str, Any]) -> dict[str, Any]:
    status = str(memory_guard.get("status") or "unknown")
    command_started = memory_guard.get("command_started")
    if status == "ok":
        return {}
    prestart = status == "refused" and command_started is False
    phase = "memory_admission" if prestart else "memory_guard"
    boundary = ("before command start" if prestart else
                "after command start" if command_started is True else
                "with unknown command-start state")
    reason = str(memory_guard.get("reason") or "unspecified")
    detail = str(memory_guard.get("detail") or "")
    label = "memory admission" if prestart else "memory guard"
    error = f"{label} {status} {boundary}: {reason}"
    if detail:
        error += f": {detail}"
    fields: dict[str, Any] = {"phase": phase, "error": error}
    if not prestart:
        fields["no_retry"] = True
    return fields


def _admission_guard_payload(transport: Any, admission: Any) -> dict[str, Any]:
    guard = transport.memory_guard
    return {
        "schema": 1, "status": "refused", "reason": admission.reason,
        "detail": admission.detail, "command_started": False,
        "command_exit_code": None, "helper_exit_code": 125,
        "max_command_bytes": None, "min_available_bytes": None,
        "command_limit_fraction": getattr(guard, "command_limit_fraction", None),
        "host_reserve_fraction": getattr(guard, "host_reserve_fraction", None),
        "peak_command_bytes": None, "min_host_available_bytes": None,
        "sample_count": 0, "platform": "controller",
        "memory_admission": admission.payload,
    }

def _first_value(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _admission_receipt(payload: Any) -> dict[str, Any] | None:
    """Sanitize target admission evidence; never retain tokens, IDs, or state paths."""
    if not isinstance(payload, dict):
        return None
    policy = payload.get("policy") if isinstance(payload.get("policy"), dict) else {}
    capacity = payload.get("capacity") if isinstance(payload.get("capacity"), dict) else {}
    lease = payload.get("lease") if isinstance(payload.get("lease"), dict) else {}
    return {
        "status": payload.get("status"),
        "reason": payload.get("reason"),
        "active_leases": payload.get("active_leases"),
        "lease_released": payload.get("lease_released"),
        "allowance_basis": _first_value(
            lease.get("allowance_basis"), capacity.get("allowance_basis"),
        ),
        "allocation_rule": _first_value(
            lease.get("allocation_rule"), capacity.get("allocation_rule"),
        ),
        "enforced_command_limit_bytes": _first_value(
            lease.get("enforced_command_limit_bytes"), capacity.get("allowance_bytes"),
        ),
        "control_overhead_bytes": _first_value(
            lease.get("control_overhead_bytes"), capacity.get("control_overhead_bytes"),
        ),
        "capacity_bytes": _first_value(
            lease.get("capacity_bytes"), capacity.get("capacity_bytes"),
        ),
        "policy_command_ceiling_bytes": _first_value(
            lease.get("policy_command_ceiling_bytes"), policy.get("max_command_bytes"),
            capacity.get("policy_command_ceiling_bytes"),
        ),
        "host_reserve_bytes": _first_value(
            lease.get("host_reserve_bytes"), policy.get("min_available_bytes"),
        ),
        "host_total_bytes": _first_value(
            lease.get("host_total_bytes"), policy.get("host_total_bytes"),
        ),
        "safe_concurrency": _first_value(
            lease.get("safe_concurrency"), policy.get("safe_concurrency"),
        ),
    }


def _guard_receipt(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    fields = (
        "status", "reason", "command_started", "command_exit_code",
        "helper_exit_code", "max_command_bytes", "min_available_bytes",
        "host_total_bytes", "initial_host_available_bytes", "min_host_available_bytes",
        "peak_command_bytes", "trigger_value_bytes", "memory_metric", "sample_count",
        "sample_interval_ms", "cleanup_complete", "process_tree_drained",
        "forced_descendant_cleanup", "platform",
    )
    return {name: payload.get(name) for name in fields}


def _memory_limit_receipt(record: dict[str, Any], *, admission: Any = None,
                          guard: Any = None, release: Any = None) -> dict[str, Any]:
    limit = prepared_memory_limit_mib(record)
    if limit is None:
        raise ValueError("memory-limit receipt requires an explicit prepared limit")
    receipt: dict[str, Any] = {
        "schema": 1,
        "resource": "host-process-tree-rss",
        "metric": "sampled-process-tree-rss-v1",
        "requested_mib": limit,
        "requested_bytes": limit * 1024 * 1024,
        "provenance": "submit-explicit",
    }
    admitted = _admission_receipt(admission)
    outcome = _guard_receipt(guard)
    if admitted is not None:
        receipt["admission"] = admitted
    if outcome is not None:
        receipt["outcome"] = outcome
    released = _admission_receipt(release)
    if released is not None:
        receipt["release"] = released
    return receipt


def durable_attempt_record(task: FleetTask, result: dict[str, Any],
                           worker_record: str | None = None) -> str | None:
    """Return one token-free completed-attempt record for durable queue status."""
    if prepared_memory_limit_mib(task.prepared) is None:
        return worker_record
    receipt = result.get("memory_limit")
    if not isinstance(receipt, dict):
        receipt = _memory_limit_receipt(task.prepared, guard=result.get("memory_guard"))
    record: dict[str, Any] = {
        "schema": 1,
        "kind": "fleet-attempt-receipt",
        "memory_limit": receipt,
    }
    if worker_record is not None:
        try:
            record["worker_result"] = json.loads(worker_record)
        except (TypeError, json.JSONDecodeError):
            # Worker prose is not a closed receipt schema and can contain
            # input text or target-local paths. Keep only validated JSON.
            pass
    return json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)


def _choose_device(task: FleetTask, features: Any, config: RemrunConfig, fcfg: dict,
                   costs: dict, *, active_batches: dict[str, int] | None = None
                   ) -> tuple[str | None, dict[str, str]]:
    candidates = [task.force_device] if task.force_device else adapters.candidate_devices(task)
    snapshots = {}
    active_batches = active_batches or {}
    for name in candidates:
        device = config.devices.get(name)
        if device is None:
            continue
        adapter = ((task.resolved_spec or {}).get("adapters") or {}).get(name)
        snapshots[name] = probes.build_snapshot(
            device, None, fcfg,
            active_jobs=active_batches.get(name, 0),
            adapter_specs=[adapter] if adapter else [],
        )
    result = placement.plan_jobs(
        [task], [features], snapshots, costs, fcfg, safety_fraction(config),
    )
    if not result.batches:
        return None, result.skipped
    return result.batches[0].device, result.skipped


def _group_contract_error(tasks: list[FleetTask],
                          placement_task: FleetTask | None = None) -> str | None:
    if not tasks:
        return "empty task group"
    if any(task.prepared is None or task.resolved_spec is None for task in tasks):
        return "fleet work must carry frozen prepared semantics"
    head = tasks[0]
    if head.prepared["kind"] == "command":
        return None if len(tasks) == 1 else "raw command submissions are never batchable"
    definition = head.resolved_spec["definition"]
    if definition["execution"]["batching"] == "never" and len(tasks) != 1:
        return "this configured task is not batchable"

    def compatible(task: FleetTask) -> tuple[Any, ...]:
        record = task.prepared
        return (
            record["spec_id"], record["task"]["name"],
            json.dumps(record["task"]["options"], sort_keys=True, separators=(",", ":")),
            tuple(record["routing"]["requirements"]), record["routing"]["engine"],
            record["routing"]["force_device"], record["routing"]["allow_fallback"],
            record["output"]["root_override"], record["cost"]["bucket_id"],
            prepared_memory_limit_mib(record),
            json.dumps(task.resolved_spec["definition"]["completion"],
                       sort_keys=True, separators=(",", ":")),
        )

    key = compatible(head)
    for index, task in enumerate(tasks[1:], start=1):
        if compatible(task) != key:
            return f"prepared task-group member {index} is not compatible with the head"
    if placement_task is not None and compatible(placement_task) != key:
        return "incompatible placement task: frozen semantics differ from the executable group"
    return None


def run_once(task: FleetTask, config: RemrunConfig, *, state_root: Path | None = None,
             cleanup: bool = True, use_lease: bool = False,
             lease_seconds: int = 300) -> dict[str, Any]:
    return run_group(
        [task], config, placement_task=task, state_root=state_root,
        cleanup=cleanup, use_lease=use_lease, lease_seconds=lease_seconds,
    )


def run_group(tasks: list[FleetTask], config: RemrunConfig, *,
              placement_task: FleetTask | None = None, state_root: Path | None = None,
              cleanup: bool = True, use_lease: bool = False,
              lease_seconds: int = 300) -> dict[str, Any]:
    if not tasks:
        return {"ok": False, "error": "empty task group"}
    placement_task = placement_task or tasks[0]
    error = _group_contract_error(tasks, placement_task)
    if error:
        return {"ok": False, "error": error}
    state_root = state_root or default_state_root()
    fcfg = fleet_config(config)
    costs = load_costs(config, state_root)
    active: dict[str, int] = {}
    if use_lease:
        queue = FleetQueue(state_root / "fleet" / "fleet.db")
        try:
            active = queue.active_batches_by_device()
        finally:
            queue.close()
    device_name, skipped = _choose_device(
        placement_task, adapters.extract_features(placement_task), config, fcfg, costs,
        active_batches=active,
    )
    if device_name is None:
        return {"ok": False, "error": "no eligible device", "skipped": skipped}
    if use_lease:
        return _run_group_leased(
            device_name, tasks, config, state_root=state_root,
            cleanup=cleanup, lease_seconds=lease_seconds,
        )

    def live_launch_gate() -> bool:
        head = tasks[0]
        if head.prepared["kind"] == "command":
            return True
        try:
            current = (resolve_tasks(load_config(config.repo_root)).get(head.task_name) or {}).get(
                "spec_id")
        except Exception:  # noqa: BLE001 - unreadable config revokes launch
            current = None
        return current == head.prepared["spec_id"]

    return _ad_hoc_result(run_batch(
        device_name, tasks, config, state_root=state_root, cleanup=cleanup,
        prelaunch_gate=live_launch_gate,
    ))


def _run_one_leased(device_name: str, task: FleetTask, config: RemrunConfig, *,
                    state_root: Path, cleanup: bool, lease_seconds: int) -> dict[str, Any]:
    return _run_group_leased(
        device_name, [task], config, state_root=state_root,
        cleanup=cleanup, lease_seconds=lease_seconds,
    )


def _run_group_leased(device_name: str, tasks: list[FleetTask], config: RemrunConfig, *,
                      state_root: Path, cleanup: bool,
                      lease_seconds: int) -> dict[str, Any]:
    task = tasks[0]
    pool = adapters.pool_for(task, device_name)
    db_path = state_root / "fleet" / "fleet.db"
    queue = FleetQueue(db_path)
    try:
        now = utc_now_iso()
        batch_id = uuid.uuid4().hex[:12]
        if task.prepared["kind"] == "command":
            current_spec = task.prepared["spec_id"]
            current_gate = None
        else:
            def current_gate() -> str | None:
                try:
                    return (resolve_tasks(load_config(config.repo_root))
                            .get(task.task_name) or {}).get("spec_id")
                except Exception:  # noqa: BLE001
                    return None
            current_spec = current_gate()
        kwargs = {"current_spec_id": current_gate} if current_gate is not None else {}
        idempotency_keys = ([""] * len(tasks)
                            if task.prepared["kind"] == "command" else None)
        requested_job_ids = [f"adhoc-{uuid.uuid4().hex[:12]}" for _ in tasks]
        job_ids = queue.enqueue_prepared_many(
            [item.prepared for item in tasks],
            spec=None if task.prepared["kind"] == "command" else task.resolved_spec,
            idempotency_keys=idempotency_keys,
            job_ids=requested_job_ids, now=now,
            **kwargs,
        )
        # One configured submission may contain the same prepared identity more than
        # once (for example, an explicit file that is also reached through a supplied
        # directory). The queue correctly converges those records to one job_id; keep
        # the executable batch equally unique so claim_many sees one row per id. Raw
        # commands retain distinct job_ids and therefore remain deliberately repeatable.
        unique_jobs: dict[str, tuple[FleetTask, str]] = {}
        for job_id, item, requested in zip(
                job_ids, tasks, requested_job_ids, strict=True):
            unique_jobs.setdefault(job_id, (item, requested))
        job_ids = list(unique_jobs)
        tasks = [item for item, _requested in unique_jobs.values()]
        requested_job_ids = [requested for _item, requested in unique_jobs.values()]
        newly_enqueued = {
            job_id for job_id, requested in zip(job_ids, requested_job_ids, strict=True)
            if job_id == requested
        }

        def current_spec_ids() -> dict[str, str | None]:
            live = current_spec if current_gate is None else current_gate()
            return {job_id: live for job_id in job_ids}

        owner_token = queue.claim_many(
            job_ids, device_name, batch_id=batch_id,
            lease_until=iso_plus_seconds(now, lease_seconds), pool=pool,
            task_name=task.task_name, engine=adapters.engine_for(task, device_name),
            bucket=adapters.option_bucket(task), now=now,
            current_spec_ids=current_spec_ids,
        )
        if owner_token is None:
            # An active idempotent submission can resolve to a row that predates this
            # synchronous call. A busy resource must not terminalize that owner's row.
            for job_id in newly_enqueued:
                queue.finalize_queued(
                    job_id, f"{device_name} {pool or 'capacity'} lease busy",
                    now=utc_now_iso(),
                )
            return {"ok": False, "device": device_name, "lease_busy": True,
                    "error": f"{device_name} resource is busy; use `fleet submit` to queue"}

        batch_state = "leased"
        if not queue.set_batch_state(
            batch_id, "staging", expected_state=batch_state, owner_token=owner_token,
        ):
            return {"ok": False, "device": device_name, "ownership_lost": True,
                    "error": "lost batch ownership before staging"}
        batch_state = "staging"
        heartbeat: BatchHeartbeat | None = None
        attempt_record: str | None = None
        result: dict[str, Any] = {}

        def launch_gate() -> bool:
            nonlocal batch_state
            if task.prepared["kind"] != "command":
                current = current_gate()
                if current != task.prepared["spec_id"]:
                    queue.revoke_prelaunch_batch(
                        batch_id, owner_token=owner_token,
                        reason="definition_missing" if current is None else "definition_changed",
                    )
                    return False
            if heartbeat is None or not heartbeat.transition(queue, "running"):
                return False
            batch_state = "running"
            return True

        try:
            with BatchHeartbeat(
                db_path, batch_id, owner_token, batch_state, lease_seconds,
            ) as heartbeat:
                if heartbeat.ownership_lost.is_set():
                    return {"ok": False, "device": device_name,
                            "ownership_lost": True,
                            "error": "lost batch ownership before remote launch"}
                result = run_batch(
                    device_name, tasks, config, state_root=state_root,
                    cleanup=cleanup, job_ids=job_ids, observation_id=batch_id,
                    prelaunch_gate=launch_gate,
                )
            attempt_record = durable_attempt_record(task, result)
        except BaseException as exc:  # noqa: BLE001
            if heartbeat is not None and heartbeat.ownership_lost.is_set():
                return {"ok": False, "device": device_name,
                        "ownership_lost": True,
                        "error": "lost batch ownership during remote run"}
            attempt_record = durable_attempt_record(task, result)
            error = f"run raised: {type(exc).__name__}: {exc}"
            observation = profiles.profile_observation(
                tasks, device_name, result, attempt_record,
            )
            if batch_state in {"running", "fetching"}:
                transitioned = queue.mark_completion_unknown(
                    batch_id, error, expected_state=batch_state, owner_token=owner_token,
                    result_record=attempt_record, observation=observation,
                )
            else:
                transitioned = queue.fail_batch(
                    batch_id, error, expected_state=batch_state, owner_token=owner_token,
                    max_attempts=1, result_record=attempt_record, observation=observation,
                )
            if not transitioned:
                return {"ok": False, "device": device_name,
                        "ownership_lost": True,
                        "error": "lost batch ownership during remote failure"}
            raise
        if heartbeat.ownership_lost.is_set():
            return {**_ad_hoc_result(result), "ok": False, "ownership_lost": True,
                    "error": "lost batch ownership during remote run"}
        if result.get("definition_drift"):
            if attempt_record is not None:
                queue.record_revoked_prelaunch_result(
                    batch_id, owner_token=owner_token,
                    result_record=attempt_record,
                )
            return result

        observation = profiles.profile_observation(
            tasks, device_name, result, attempt_record,
        )
        if (result.get("completion_state") == "unknown"
                or ("command_started" in result and result.get("command_started") is None)):
            transitioned = queue.mark_completion_unknown(
                batch_id, result.get("error") or "completion unknown after launch authorization",
                expected_state=batch_state, owner_token=owner_token,
                result_record=attempt_record, observation=observation,
            )
        elif not result.get("ok") and result.get("no_retry") \
                and queue.batch_replay_policy(batch_id) == "at-most-once-v1":
            transitioned = queue.mark_completion_unknown(
                batch_id, result.get("error") or "worker completion evidence is incomplete",
                expected_state=batch_state, owner_token=owner_token,
                result_record=attempt_record, observation=observation,
            )
        else:
            if result.get("ok"):
                if not queue.set_batch_state(
                    batch_id, "fetching", expected_state=batch_state, owner_token=owner_token,
                ):
                    return {**_ad_hoc_result(result), "ok": False,
                            "ownership_lost": True,
                            "error": "lost batch ownership before recording completion"}
                batch_state = "fetching"
            if result.get("item_results") and not result.get("no_retry"):
                succeeded, failed = item_result_maps(result["item_results"])
                worker_records = item_records(result["item_results"])
                terminal_records = {
                    job_id: durable_attempt_record(task, result, record)
                    for job_id, record in worker_records.items()
                }
                transitioned = queue.complete_batch_items(
                    batch_id, succeeded, failed, expected_state=batch_state,
                    owner_token=owner_token, max_attempts=1,
                    dispositions=item_dispositions(result["item_results"]),
                    results={job_id: record for job_id, record in terminal_records.items()
                             if record is not None},
                    result_record=attempt_record, observation=observation,
                )
            elif result.get("ok"):
                transitioned = queue.complete_batch(
                    batch_id, expected_state=batch_state, owner_token=owner_token,
                    result_record=attempt_record, observation=observation,
                )
            else:
                transitioned = queue.fail_batch(
                    batch_id, result.get("error") or f"exit {result.get('exit_code')}",
                    expected_state=batch_state, owner_token=owner_token, max_attempts=1,
                    result_record=attempt_record, observation=observation,
                )
        if not transitioned:
            return {**_ad_hoc_result(result), "ok": False, "ownership_lost": True,
                    "error": "lost batch ownership before recording completion"}
        return _ad_hoc_result(result)
    finally:
        queue.close()


def run_batch(device_name: str, tasks: list[FleetTask], config: RemrunConfig, *,
              state_root: Path | None = None, cleanup: bool = True,
              job_ids: list[str] | None = None,
              observation_id: str | None = None,
              prelaunch_gate: Callable[[], bool] | None = None) -> dict[str, Any]:
    """Run one already-placed compatible prepared batch."""
    if device_name not in config.devices:
        return {"ok": False, "error": f"unknown device {device_name!r}"}
    if not tasks:
        return {"ok": False, "device": device_name, "error": "empty batch"}
    error = _group_contract_error(tasks)
    if error:
        return {"ok": False, "device": device_name, "error": error}
    head = tasks[0]
    if prelaunch_gate is None:
        if head.prepared["kind"] == "command":
            def command_gate() -> bool:
                return True
            prelaunch_gate = command_gate
        else:
            def default_gate() -> bool:
                try:
                    current = (resolve_tasks(load_config(config.repo_root))
                               .get(head.task_name) or {}).get("spec_id")
                except Exception:  # noqa: BLE001
                    current = None
                return current == head.prepared["spec_id"]
            prelaunch_gate = default_gate
    result = _run_prepared_batch(
        device_name, tasks, config, state_root=state_root or default_state_root(),
        cleanup=cleanup, job_ids=job_ids, observation_id=observation_id,
        prelaunch_gate=prelaunch_gate,
    )
    admission = result.pop("_memory_admission", None)
    if prepared_memory_limit_mib(head.prepared) is not None:
        result["memory_limit"] = _memory_limit_receipt(
            head.prepared, admission=admission, guard=result.get("memory_guard"),
            release=result.get("memory_reservation_release"),
        )
    return result


def _run_prepared_batch(device_name: str, tasks: list[FleetTask], config: RemrunConfig, *,
                        state_root: Path, cleanup: bool, job_ids: list[str] | None,
                        observation_id: str | None,
                        prelaunch_gate: Callable[[], bool] | None) -> dict[str, Any]:
    device = config.devices[device_name]
    head = tasks[0]
    record = head.prepared
    spec = head.resolved_spec
    is_command = record["kind"] == "command"
    verified: dict[tuple[int, int], Path] = {}
    changed: dict[int, str] = {}
    for task_index, task in enumerate(tasks):
        for item in task.prepared["payload"]["items"]:
            try:
                verified[(task_index, item["index"])] = snapshot_prepared_input(item)
            except SourceChangedError as exc:
                changed[task_index] = str(exc)
                break
    if changed:
        _discard_snapshots(verified)
        rows = []
        for index, _task in enumerate(tasks):
            job_id = job_ids[index] if job_ids and index < len(job_ids) else f"adhoc-{index}"
            detail = changed.get(index)
            rows.append({
                "job_id": job_id, "ok": False,
                "outcome": "review" if detail else "failed",
                "disposition": "review" if detail else "retry",
                "message": detail or "batch staging stopped because a sibling source changed",
            })
        return {"ok": False, "device": device_name, "staged": 0,
                "error": "source_changed", "item_results": rows}

    adapter = None if is_command else spec["adapters"].get(device_name)
    if not is_command and adapter is None:
        _discard_snapshots(verified)
        return {"ok": False, "device": device_name,
                "error": "frozen spec has no adapter for the selected device"}
    configured_root = adapters.resolve_output_root(head, device_name)
    output_error = _output_root_error(configured_root, device)
    if output_error:
        _discard_snapshots(verified)
        return {"ok": False, "device": device_name, "phase": "output_root",
                "error": output_error}
    transport = None
    stage = None
    try:
        transport = make_transport(device)
        stage = transport.remote_temp_dir("fleet")
        stage_in = transport.native_join(stage, "in")
        transport.ensure_remote_dir(stage_in)
    except TransportError as exc:
        _discard_snapshots(verified)
        if cleanup and transport is not None and stage is not None:
            _safe_delete(transport, stage)
        return {"ok": False, "device": device_name, "error": f"stage failed: {exc}"}

    batch_id = observation_id or f"batch-{uuid.uuid4().hex[:12]}"
    used: set[str] = set()
    manifest_items: list[dict[str, Any]] = []
    expected: list[dict[str, Any]] = []
    staged = 0
    try:
        for index, task in enumerate(tasks):
            prepared = task.prepared
            staged_names: list[str] = []
            payload = prepared["payload"]
            if payload["mode"] == "text":
                name = _unique_name(f"item-{index:04d}.txt", used)
                with tempfile.NamedTemporaryFile(
                    "w", suffix=".txt", delete=False, encoding="utf-8",
                ) as stream:
                    stream.write(payload["text"])
                    local_path = Path(stream.name)
                try:
                    transport.push_file(local_path, transport.native_join(stage_in, name))
                finally:
                    local_path.unlink(missing_ok=True)
                staged_names.append(name)
                staged += 1
            for item in payload["items"]:
                snapshot = verified.pop((index, item["index"]))
                name = _unique_name(Path(item["source_path"]).name, used)
                try:
                    transport.push_file(snapshot, transport.native_join(stage_in, name))
                finally:
                    snapshot.unlink(missing_ok=True)
                staged_names.append(name)
                staged += 1
            job_id = job_ids[index] if job_ids and index < len(job_ids) else f"adhoc-{index}"
            item_costs = {
                int(row["index"]): float(row["value"])
                for row in prepared["cost"].get("item_values", [])
            }
            manifest_items.append({
                "index": index, "job_id": job_id,
                "prepared_id": prepared["prepared_id"], "work_id": prepared["work_id"],
                "payload": payload, "staged": staged_names,
                "reservations": prepared["output"]["reservations"],
                "cost": prepared["cost"],
            })
            expected.append({
                "job_id": job_id, "prepared_id": prepared["prepared_id"],
                "index": index, "cost_unit": prepared["cost"]["unit"],
                "cost_status": prepared["cost"]["status"],
                "measure_id": prepared["cost"].get("measure_id"),
                "prepared_value": (
                    item_costs.get(0) if len(prepared["payload"]["items"]) == 1
                    else prepared["cost"]["value"]
                ),
                "verify_relative_tolerance": (
                    spec["definition"]["cost"].get("verify_relative_tolerance", 0.0)
                    if not is_command else 0.0
                ),
                "reservations": prepared["output"]["reservations"],
            })
        output_root = transport.expand_remote(configured_root or stage)
        transport.ensure_remote_dir(output_root)
        manifest_path = transport.native_join(stage, BATCH_MANIFEST_NAME)
        metrics_path = transport.native_join(stage, BATCH_METRICS_NAME)
        done_path = transport.native_join(stage, DONE_JSON_NAME)
        _push_json(transport, manifest_path, {
            "schema": 2, "batch_id": batch_id, "kind": record["kind"],
            "spec_id": record["spec_id"],
            "adapter_id": adapter["adapter_id"] if adapter else None,
            "device": device_name, "stage": stage, "stage_in": stage_in,
            "output_root": output_root, "items": manifest_items,
        })
    except (OSError, TransportError, ValueError) as exc:
        _discard_snapshots(verified)
        if cleanup:
            _safe_delete(transport, stage)
        return {"ok": False, "device": device_name, "staged": staged,
                "error": f"stage failed: {exc}"}

    try:
        command = adapters.render_command(
            head,
            device_name,
            stage_in,
            output_root,
            manifest_path=manifest_path,
        )
    except ValueError as exc:
        if cleanup:
            _safe_delete(transport, stage)
        return {
            "ok": False,
            "device": device_name,
            "staged": staged,
            "error": f"render failed: {exc}",
        }
    if not is_command:
        command = [transport.expand_remote(value) if value.startswith("~") else value
                   for value in command]
    env = {
        "REMRUN_BATCH_MANIFEST": manifest_path,
        "REMRUN_DONE_JSON": done_path, "REMRUN_STAGE": stage,
        "REMRUN_STAGE_IN": stage_in, "REMRUN_OUTPUT_ROOT": output_root,
    }
    if (not is_command and
            spec["definition"]["completion"]["protocol"] == "item-result-v2"):
        env["REMRUN_BATCH_METRICS"] = metrics_path
    reservation = None
    admission_payload = None
    explicit_limit_mib = prepared_memory_limit_mib(record)
    if explicit_limit_mib is not None or getattr(transport, "memory_guard", None) is not None:
        predicted = None
        if explicit_limit_mib is None:
            predicted = placement.predicted_resources(
                head, device_name, load_costs(config, state_root),
            )[0] or None
        admission = transport.reserve_memory_guard(
            predicted_rss_mb=predicted, explicit_limit_mib=explicit_limit_mib,
        )
        admission_payload = admission.payload
        if not admission.admitted:
            if cleanup:
                _safe_delete(transport, stage)
            memory_guard = _admission_guard_payload(transport, admission)
            return {"ok": False, "device": device_name, "staged": staged,
                    "memory_guard": memory_guard, "_memory_admission": admission_payload,
                    **_guard_outcome_fields(memory_guard)}
        reservation = admission.reservation

    if prelaunch_gate is not None and not prelaunch_gate():
        release_receipt = None
        if reservation is not None:
            try:
                release = transport.release_memory_guard(reservation, reserved_only=True)
                release_receipt = _admission_receipt(release.payload)
            except Exception as exc:  # noqa: BLE001 - expiry remains the final backstop
                release_receipt = {
                    "status": "release_failed",
                    "reason": type(exc).__name__,
                }
            reservation = None
        if cleanup:
            _safe_delete(transport, stage)
        response = {"ok": False, "device": device_name, "staged": staged,
                    "definition_drift": True, "_memory_admission": admission_payload,
                    "error": "task definition changed before process launch"}
        if release_receipt is not None:
            response["memory_reservation_release"] = release_receipt
        return response
    started = time.monotonic()
    try:
        observed_exec = getattr(transport, "exec_observed", None)
        if not active_job_observation_enabled() or observed_exec is None:
            result = transport.exec(
                command, cwd=stage, telemetry=True, env=env,
                memory_reservation=reservation,
            )
        else:
            observation = JobObservation.for_command(
                job_id=(observation_id or
                        (job_ids[0] if job_ids and len(job_ids) == 1
                         else f"fleet-{uuid.uuid4().hex[:12]}")),
                project="@fleet", target=device_name, phase="fleet-worker",
                command=command,
                declared_label=("raw-command" if is_command else
                                f"{record['task']['name']}:{adapter['engine']}"),
                member_count=len(tasks),
            )
            result = observed_exec(
                command, cwd=stage, telemetry=True, env=env,
                observation=observation, memory_reservation=reservation,
            )
    except GuardFinalizationError as exc:
        prestart = exc.command_started is False
        if cleanup and prestart:
            _safe_delete(transport, stage)
        memory_guard = exc.memory_guard or {}
        response = {"ok": False, "device": device_name, "staged": staged,
                    "completion_state": "not_started" if prestart else "unknown",
                    "command_started": exc.command_started, "memory_guard": memory_guard,
                    "_memory_admission": admission_payload,
                    **_guard_outcome_fields(memory_guard)}
        if cleanup and not prestart:
            response.update({"cleanup_deferred": True, "stage_dir": stage})
        return response
    except TransportError as exc:
        return {"ok": False, "device": device_name, "staged": staged,
                "error": f"exec failed: {exc}", "completion_state": "unknown",
                "command_started": None, "cleanup_deferred": bool(cleanup),
                "stage_dir": stage}

    elapsed = round(time.monotonic() - started, 3)
    rows: list[dict[str, Any]] = []
    evidence_error: str | None = None
    if not is_command and spec["definition"]["completion"]["protocol"] == "item-result-v2":
        envelope = _read_worker_metrics(transport, stage, stage_in, output_root)
        try:
            rows = validate_result_envelope(
                envelope, batch_id=batch_id, spec_id=record["spec_id"],
                adapter_id=adapter["adapter_id"], expected_items=expected,
                completion=spec["definition"]["completion"],
            )
            rows = [{**row, "ok": row["outcome"] == "succeeded",
                     "error": row["message"]} for row in rows]
        except ResultProtocolError as exc:
            evidence_error = str(exc)
    if cleanup:
        _safe_delete(transport, stage)
    response = {
        "ok": result.exit_code == 0 and evidence_error is None,
        "device": device_name, "engine": adapter["engine"] if adapter else "raw-command",
        "exit_code": result.exit_code, "elapsed_s": elapsed, "staged": staged,
        "jobs": len(tasks), "output_root": output_root, "telemetry": result.telemetry,
        "item_results": rows, "stdout_tail": (result.stdout or "")[-500:],
        "stderr_tail": (result.stderr or "")[-500:],
        "_memory_admission": admission_payload,
    }
    if evidence_error:
        response.update({"error": evidence_error, "completion_evidence": "missing",
                         "no_retry": True})
    elif rows:
        response["completion_evidence"] = "complete"
    if result.memory_guard is not None:
        response["memory_guard"] = result.memory_guard
        response.update(_guard_outcome_fields(result.memory_guard))
    return response


def _ad_hoc_result(result: dict[str, Any]) -> dict[str, Any]:
    """Return a synchronous result while retaining structured item evidence."""
    return result


def item_disposition(item: dict[str, Any]) -> str:
    if item.get("outcome") == "review":
        return "review"
    return str(item.get("disposition") or "final").replace("-", "_")


def item_result_maps(item_results: list[dict[str, Any]]) -> tuple[dict[str, str | None],
                                                                  dict[str, str]]:
    succeeded: dict[str, str | None] = {}
    failed: dict[str, str] = {}
    for item in item_results:
        job_id = item.get("job_id")
        if not job_id:
            continue
        if item.get("outcome") == "succeeded" or item.get("ok"):
            succeeded[job_id] = json.dumps(item, sort_keys=True)
        else:
            failed[job_id] = (item.get("message") or item.get("error") or
                              "worker reported item failure")
    return succeeded, failed


def item_records(item_results: list[dict[str, Any]]) -> dict[str, str]:
    return {str(item["job_id"]): json.dumps(item, sort_keys=True, default=str)
            for item in item_results
            if item.get("outcome") != "succeeded" and not item.get("ok")
            and item.get("job_id")}


def item_dispositions(item_results: list[dict[str, Any]]) -> dict[str, str]:
    return {str(item["job_id"]): item_disposition(item) for item in item_results
            if item.get("outcome") != "succeeded" and not item.get("ok")
            and item.get("job_id")}


def _output_root_error(output_root: str | None, device: Any) -> str | None:
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
                "POSIX target; pass a target-native path")
    return None


def _unique_name(name: str, used: set[str]) -> str:
    candidate = name
    stem, suffix = Path(name).stem, Path(name).suffix
    counter = 1
    while candidate.casefold() in used:
        candidate = f"{stem}-{counter}{suffix}"
        counter += 1
    used.add(candidate.casefold())
    return candidate


def _push_json(transport: Any, remote_path: str, payload: dict[str, Any]) -> None:
    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False, encoding="utf-8",
    ) as stream:
        json.dump(payload, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        local_path = Path(stream.name)
    try:
        transport.push_file(local_path, remote_path)
    finally:
        local_path.unlink(missing_ok=True)


def _pull_json(transport: Any, remote_path: str) -> dict[str, Any] | None:
    if not transport.remote_path_exists(remote_path):
        return None
    with tempfile.TemporaryDirectory(prefix="remrun-result-") as directory:
        local = Path(directory) / "result.json"
        transport.pull_file(remote_path, local)
        if local.stat().st_size > MAX_RESULT_EVIDENCE_BYTES:
            raise ResultProtocolError("result envelope exceeds size limit")
        try:
            value = json.loads(local.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ResultProtocolError(f"result envelope is unreadable: {exc}") from exc
    if not isinstance(value, dict):
        raise ResultProtocolError("result envelope must be an object")
    return value


def _read_worker_metrics(transport: Any, stage: str, stage_in: str,
                         output_root: str) -> dict[str, Any] | None:
    for root in (stage, stage_in, output_root):
        for name in (BATCH_METRICS_NAME, DONE_JSON_NAME):
            path = transport.native_join(root, name)
            try:
                value = _pull_json(transport, path)
            except ResultProtocolError:
                raise
            except (OSError, TransportError, ValueError) as exc:
                raise ResultProtocolError(f"result envelope is unreadable: {exc}") from exc
            if value is not None:
                return value
    return None


def _discard_snapshots(snapshots: dict[tuple[int, int], Path]) -> None:
    for snapshot in snapshots.values():
        snapshot.unlink(missing_ok=True)
    snapshots.clear()


def _safe_delete(transport: Any, remote_dir: str) -> None:
    try:
        transport.remove_remote_tree(remote_dir)
    except (OSError, TransportError, NotImplementedError):
        pass
