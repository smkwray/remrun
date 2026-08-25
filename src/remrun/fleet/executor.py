"""Execute frozen fleet work without configured-workflow vocabulary."""
from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
import time
import uuid
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Callable

from ..config import RemrunConfig, load_config, load_retention
from ..job_observation import JobObservation, active_job_observation_enabled, controller_label
from ..state import default_state_root, iso_plus_seconds, utc_now_iso
from ..target_resources import (
    EMPTY_POLICY_DIGEST,
    TargetReservation,
    TargetResourceClient,
    TargetResourceError,
    canonical_json,
)
from ..transport import (
    GuardFinalizationError,
    TransportError,
    finalize_durable_result,
    make_transport,
)
from . import adapters, placement, probes, profiles, storage
from .config import fleet_config, load_costs, safety_fraction
from .models import FleetTask
from .prepared import (
    SourceChangedError, materialize_prepared_input, prepared_memory_limit_mib,
)
from .queue import (
    FINALIZATION_FENCED,
    FINALIZATION_FINALIZED,
    FINALIZATION_PENDING,
    FINALIZATION_REPLAYABLE,
    BatchHeartbeat,
    FleetQueue,
)
from .result_protocol import ResultProtocolError, validate_result_envelope
from .task_contract import resolve_tasks

BATCH_MANIFEST_NAME = "remrun_batch.json"
BATCH_METRICS_NAME = "batch_metrics.json"
DONE_JSON_NAME = "done.json"
MAX_RESULT_EVIDENCE_BYTES = 4 * 1024 * 1024
TARGET_OPERATION_POLL_SECONDS = 1.0
TARGET_RESERVATION_RENEW_SECONDS = 10.0


class _TargetReservationHeartbeat:
    """Keep one prelaunch target reservation alive while inputs are staged."""

    def __init__(
        self,
        client: TargetResourceClient,
        reservation: TargetReservation,
        *,
        policy_generation: int,
        policy_digest: str,
        operation_id: str,
        interval_s: float = TARGET_RESERVATION_RENEW_SECONDS,
    ) -> None:
        self.client = client
        self.reservation = reservation
        self.policy_generation = policy_generation
        self.policy_digest = policy_digest
        self.operation_id = operation_id
        self.interval_s = interval_s
        self.failed = threading.Event()
        self.error: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._counter = 0

    def __enter__(self) -> "_TargetReservationHeartbeat":
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_s + 1.0))

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            self._counter += 1
            try:
                renewed = self.client.renew(
                    self.reservation,
                    expected_policy_generation=self.policy_generation,
                    expected_policy_digest=self.policy_digest,
                    rpc_id=f"fleet-renew-{self.operation_id}-{self._counter}",
                )
                receipt = renewed.get("receipt")
                if not isinstance(receipt, dict) or receipt.get("state") != "RESERVED":
                    raise TargetResourceError("target reservation renewal was not reserved")
            except (OSError, TargetResourceError, TransportError, ValueError) as exc:
                self.error = str(exc)
                self.failed.set()
                return

    def require_live(self) -> None:
        if self.failed.is_set():
            raise TargetResourceError(
                f"target reservation renewal failed during staging: {self.error or 'unknown'}"
            )


def _target_acceptance_supported(device: Any) -> bool:
    return device.kind in {"ssh-posix", "ssh-powershell"}


def _target_state_root(transport: Any, device: Any) -> str:
    """Resolve the configured target state root before constructing operation paths."""
    probe = transport.probe()
    if not probe.reachable:
        raise TransportError(f"{device.name} unreachable: {probe.detail}")
    root = transport.expand_remote(device.state_root)
    path = PureWindowsPath(root) if device.os == "windows" else PurePosixPath(root)
    if root.startswith("~") or not path.is_absolute():
        raise TransportError("target state root did not resolve to an absolute path")
    return root


def _target_operation_identity(batch_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]", "-", batch_id)[:120]
    return f"fleet-{safe or hashlib.sha256(batch_id.encode()).hexdigest()[:24]}"


def _target_operation_digest(
    *,
    operation_id: str,
    device_name: str,
    stage: str,
    command: list[str],
    env: dict[str, str],
    prepared_ids: list[str],
    job_ids: list[str],
    resource_keys: list[str],
) -> str:
    return hashlib.sha256(canonical_json({
        "schema": 1,
        "operation_id": operation_id,
        "device": device_name,
        "stage": stage,
        "command": command,
        "env": env,
        "prepared_ids": prepared_ids,
        "job_ids": job_ids,
        "resource_keys": resource_keys,
    })).hexdigest()


def _wait_target_operation(
    transport: Any, operation_id: str, token: str
) -> dict[str, object]:
    while True:
        status = transport.durable_status(operation_id, token)
        state = status.get("state")
        if state == "complete":
            return transport.durable_status(operation_id, token, include_logs=True)
        if state == "failed":
            return status
        if state not in {"launching", "pending", "running"}:
            raise TransportError(f"target operation returned invalid state {state!r}")
        time.sleep(TARGET_OPERATION_POLL_SECONDS)


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
    target_operation = result.get("target_operation")
    has_target_operation = isinstance(target_operation, dict)
    if prepared_memory_limit_mib(task.prepared) is None and not has_target_operation:
        return worker_record
    record: dict[str, Any] = {
        "schema": 1,
        "kind": "fleet-attempt-receipt",
    }
    if prepared_memory_limit_mib(task.prepared) is not None:
        receipt = result.get("memory_limit")
        if not isinstance(receipt, dict):
            receipt = _memory_limit_receipt(task.prepared, guard=result.get("memory_guard"))
        record["memory_limit"] = receipt
    if has_target_operation:
        record["target_operation"] = target_operation
    if worker_record is not None:
        try:
            record["worker_result"] = json.loads(worker_record)
        except (TypeError, json.JSONDecodeError):
            # Worker prose is not a closed receipt schema and can contain
            # input text or target-local paths. Keep only validated JSON.
            pass
    return json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)


def target_finalization_disposition(result: dict[str, Any]) -> str | None:
    """Translate one worker result into the queue's durable target disposition."""
    target = result.get("target_operation")
    if target is None and not result.get("cleanup_deferred"):
        return None
    if (
        result.get("target_acceptance_unknown")
        or result.get("completion_state") == "unknown"
        or ("command_started" in result and result.get("command_started") is None)
    ):
        return FINALIZATION_FENCED
    if result.get("cleanup_deferred"):
        if result.get("completion_state") == "not_started" \
                and result.get("command_started") is False:
            return FINALIZATION_REPLAYABLE
        return FINALIZATION_PENDING
    return FINALIZATION_FINALIZED


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
            target_protocol_version=(
                1 if _target_acceptance_supported(config.devices[device_name]) else None
            ),
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
        definition_drift_reason: str | None = None

        def launch_gate() -> bool:
            nonlocal batch_state, definition_drift_reason
            if task.prepared["kind"] != "command":
                current = current_gate()
                if current != task.prepared["spec_id"]:
                    definition_drift_reason = (
                        "definition_missing" if current is None else "definition_changed"
                    )
                    return False
            if heartbeat is None or not heartbeat.transition(queue, "running"):
                return False
            batch_state = "running"
            return True

        def output_return_gate() -> bool:
            nonlocal batch_state
            if heartbeat is None or heartbeat.ownership_lost.is_set():
                return False
            if batch_state != "fetching":
                if not heartbeat.transition(queue, "fetching"):
                    return False
                batch_state = "fetching"
            return True

        def record_target_reservation(receipt: dict[str, Any], resume_token: str) -> bool:
            if heartbeat is None or heartbeat.ownership_lost.is_set():
                return False
            recorded = queue.record_target_reservation(
                batch_id,
                operation_id=str(receipt.get("operation_id") or ""),
                request_sha256=str(receipt.get("request_sha256") or ""),
                resume_token=resume_token,
                expected_state=batch_state,
                owner_token=owner_token,
            )
            if not recorded:
                heartbeat.ownership_lost.set()
            return recorded

        def record_target_acceptance(receipt: dict[str, Any]) -> bool:
            if heartbeat is None or heartbeat.ownership_lost.is_set():
                return False
            recorded = queue.record_target_acceptance(
                batch_id,
                operation_id=str(receipt.get("operation_id") or ""),
                request_sha256=str(receipt.get("request_sha256") or ""),
                expected_state=batch_state,
                owner_token=owner_token,
            )
            if not recorded:
                heartbeat.ownership_lost.set()
            return recorded

        def authorize_target_cleanup(receipt: dict[str, Any]) -> bool:
            if heartbeat is None or heartbeat.ownership_lost.is_set():
                return False
            recorded = queue.authorize_target_cleanup(
                batch_id,
                operation_id=str(receipt.get("operation_id") or ""),
                request_sha256=str(receipt.get("request_sha256") or ""),
                cleanup_state=str(receipt.get("cleanup_state") or ""),
                expected_state=batch_state,
                owner_token=owner_token,
            )
            if not recorded:
                heartbeat.ownership_lost.set()
            return recorded

        def record_target_finalization(receipt: dict[str, Any]) -> bool:
            if heartbeat is None or heartbeat.ownership_lost.is_set():
                return False
            recorded = queue.record_target_finalization(
                batch_id,
                operation_id=str(receipt.get("operation_id") or ""),
                request_sha256=str(receipt.get("request_sha256") or ""),
                cleanup_state=str(receipt.get("cleanup_state") or ""),
                stage_cleaned=receipt.get("stage_cleaned") is True,
                durable_cleaned=receipt.get("durable_cleaned") is True,
                expected_state=batch_state,
                owner_token=owner_token,
            )
            if not recorded:
                heartbeat.ownership_lost.set()
            return recorded

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
                    before_output_return=output_return_gate,
                    on_target_reservation=record_target_reservation,
                    on_target_acceptance=record_target_acceptance,
                    before_target_cleanup=authorize_target_cleanup,
                    on_target_finalization=record_target_finalization,
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
                    finalization_disposition=FINALIZATION_FENCED,
                )
            else:
                transitioned = queue.fail_batch(
                    batch_id, error, expected_state=batch_state, owner_token=owner_token,
                    max_attempts=1, result_record=attempt_record, observation=observation,
                    finalization_disposition=target_finalization_disposition(result),
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
            if not queue.revoke_prelaunch_batch(
                batch_id,
                owner_token=owner_token,
                reason=definition_drift_reason or "definition_changed",
                finalization_disposition=target_finalization_disposition(result),
            ):
                return {**_ad_hoc_result(result), "ok": False, "ownership_lost": True,
                        "error": "lost batch ownership while recording definition drift"}
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
                finalization_disposition=FINALIZATION_FENCED,
            )
        elif not result.get("ok") and result.get("no_retry") \
                and queue.batch_replay_policy(batch_id) == "at-most-once-v1":
            transitioned = queue.mark_completion_unknown(
                batch_id, result.get("error") or "worker completion evidence is incomplete",
                expected_state=batch_state, owner_token=owner_token,
                result_record=attempt_record, observation=observation,
                finalization_disposition=FINALIZATION_FENCED,
            )
        else:
            if result.get("ok") and batch_state != "fetching":
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
                    finalization_disposition=target_finalization_disposition(result),
                )
            elif result.get("ok"):
                transitioned = queue.complete_batch(
                    batch_id, expected_state=batch_state, owner_token=owner_token,
                    result_record=attempt_record, observation=observation,
                    finalization_disposition=target_finalization_disposition(result),
                )
            else:
                transitioned = queue.fail_batch(
                    batch_id, result.get("error") or f"exit {result.get('exit_code')}",
                    expected_state=batch_state, owner_token=owner_token, max_attempts=1,
                    result_record=attempt_record, observation=observation,
                    finalization_disposition=target_finalization_disposition(result),
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
              prelaunch_gate: Callable[[], bool] | None = None,
              before_output_return: Callable[[], bool] | None = None,
              on_target_reservation: Callable[[dict[str, Any], str], bool] | None = None,
              on_target_acceptance: Callable[[dict[str, Any]], bool] | None = None,
              before_target_cleanup: Callable[[dict[str, Any]], bool] | None = None,
              on_target_finalization: Callable[[dict[str, Any]], bool] | None = None,
              ) -> dict[str, Any]:
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
    try:
        result = _run_prepared_batch(
            device_name, tasks, config, state_root=state_root or default_state_root(),
            cleanup=cleanup, job_ids=job_ids, observation_id=observation_id,
            prelaunch_gate=prelaunch_gate, before_output_return=before_output_return,
            on_target_reservation=on_target_reservation,
            on_target_acceptance=on_target_acceptance,
            before_target_cleanup=before_target_cleanup,
            on_target_finalization=on_target_finalization,
        )
    except _OutputReturnOwnershipLost:
        return {
            "ok": False, "device": device_name, "ownership_lost": True,
            "completion_state": "unknown", "command_started": True,
            "error": "lost batch ownership during output return",
        }
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
                        prelaunch_gate: Callable[[], bool] | None,
                        before_output_return: Callable[[], bool] | None,
                        on_target_reservation: Callable[[dict[str, Any], str], bool] | None,
                        on_target_acceptance: Callable[[dict[str, Any]], bool] | None,
                        before_target_cleanup: Callable[[dict[str, Any]], bool] | None,
                        on_target_finalization: Callable[[dict[str, Any]], bool] | None,
                        ) -> dict[str, Any]:
    device = config.devices[device_name]
    head = tasks[0]
    record = head.prepared
    spec = head.resolved_spec
    is_command = record["kind"] == "command"
    adapter = None if is_command else spec["adapters"].get(device_name)
    if not is_command and adapter is None:
        return {"ok": False, "device": device_name,
                "error": "frozen spec has no adapter for the selected device"}
    configured_root = adapters.resolve_output_root(head, device_name)
    output_error = _output_root_error(configured_root, device)
    if output_error:
        return {"ok": False, "device": device_name, "phase": "output_root",
                "error": output_error}
    uses_storage_ref = any(
        item.get("storage_ref") is not None
        for task in tasks for item in task.prepared["payload"]["items"]
    )
    if uses_storage_ref:
        try:
            storage_registry = storage.load_registry(state_root)
        except storage.StorageError as exc:
            return {
                "ok": False, "device": device_name, "phase": "storage_registry",
                "error": f"storage registry invalid: {exc}",
            }
    else:
        storage_registry = {"schema": 1, "roots": {}}
    batch_id = observation_id or f"batch-{uuid.uuid4().hex[:12]}"
    operation_id = _target_operation_identity(batch_id)
    transport = None
    stage = None
    target_resource_client: TargetResourceClient | None = None
    target_reservation: TargetReservation | None = None
    reservation_receipt: dict[str, Any] | None = None
    target_heartbeat: _TargetReservationHeartbeat | None = None
    reserved_target: TargetReservation | None = None
    request_sha = ""
    generation = 0
    policy_sha = EMPTY_POLICY_DIGEST

    def stop_target_heartbeat() -> None:
        nonlocal target_heartbeat
        if target_heartbeat is not None:
            target_heartbeat.__exit__(None, None, None)
            target_heartbeat = None

    def cleanup_terminal_target(
        cleanup_state: str,
        durable: tuple[str, str],
    ) -> dict[str, Any]:
        """Authorize, delete, then prove cleanup for one terminal target operation."""
        authorization = {
            "schema": 1,
            "operation_id": operation_id,
            "request_sha256": request_sha,
            "cleanup_state": cleanup_state,
        }
        if before_target_cleanup is None or not before_target_cleanup(authorization):
            deferred = {
                "cleanup_deferred": True,
                "stage_dir": stage,
                "target_cleanup_deferred": (
                    "controller queue rejected target cleanup authorization"
                ),
            }
            # The finalization surface remains the post-deletion proof. Sending
            # an explicitly incomplete receipt lets an attached controller
            # reject and durably report that no proof was recorded.
            if on_target_finalization is not None and not on_target_finalization({
                **authorization,
                "stage_cleaned": False,
                "durable_cleaned": False,
            }):
                deferred["target_finalization_deferred"] = (
                    "controller queue rejected target finalization proof"
                )
            return deferred
        try:
            transport.remove_remote_tree(stage)
        except (OSError, TransportError, NotImplementedError) as exc:
            return {
                "cleanup_deferred": True,
                "stage_dir": stage,
                "target_cleanup_deferred": f"target stage deletion failed: {exc}",
            }
        try:
            transport.durable_cleanup(*durable)
        except TransportError as exc:
            return {
                "cleanup_deferred": True,
                "target_cleanup_deferred": f"durable evidence cleanup failed: {exc}",
            }
        if on_target_finalization is not None and not on_target_finalization({
            **authorization,
            "stage_cleaned": True,
            "durable_cleaned": True,
        }):
            return {
                "cleanup_deferred": True,
                "target_finalization_deferred": (
                    "controller queue rejected target finalization proof"
                ),
            }
        return {}

    def finalize_prelaunch_target(
        known_cleanup_state: str | None = None,
    ) -> dict[str, Any]:
        """Cancel and clean an operation that never crossed the launch gate."""
        stop_target_heartbeat()
        if target_reservation is None or target_resource_client is None:
            return {}
        extra: dict[str, Any] = {}
        cleanup_state = known_cleanup_state
        if cleanup_state is None:
            try:
                cancelled = target_resource_client.cancel(
                    target_reservation, rpc_id=f"fleet-cancel-prelaunch-{operation_id}"
                )
                receipt = cancelled.get("receipt")
                cleanup_state = receipt.get("state") if isinstance(receipt, dict) else None
            except (OSError, TargetResourceError, TransportError, ValueError) as exc:
                extra.update(
                    cleanup_deferred=True,
                    stage_dir=stage,
                    target_cleanup_deferred=f"target reservation cancellation failed: {exc}",
                )
                return extra
        if cleanup_state not in {"RELEASED", "CANCELLED", "EXPIRED", "REBOOTED"}:
            extra.update(
                cleanup_deferred=True,
                stage_dir=stage,
                target_cleanup_deferred="target reservation cleanup is not terminal",
            )
            return extra
        if not cleanup:
            return extra
        extra.update(cleanup_terminal_target(
            cleanup_state, (operation_id, target_reservation.token),
        ))
        return extra
    try:
        transport = make_transport(device)
        if _target_acceptance_supported(device):
            stage = transport.native_join(
                _target_state_root(transport, device),
                "fleet-operations",
                operation_id,
            )
        else:
            # Legacy/non-SSH execution has no durable target reservation and
            # retains the existing transport-owned temporary stage allocation.
            stage = transport.remote_temp_dir("fleet")
        stage_in = transport.native_join(stage, "in")
        output_root = transport.expand_remote(configured_root) if configured_root else stage
        manifest_path = transport.native_join(stage, BATCH_MANIFEST_NAME)
        metrics_path = transport.native_join(stage, BATCH_METRICS_NAME)
        done_path = transport.native_join(stage, DONE_JSON_NAME)
        command = adapters.render_command(
            head,
            device_name,
            stage_in,
            output_root,
            manifest_path=manifest_path,
        )
        if not is_command:
            command = [
                transport.expand_remote(value) if value.startswith("~") else value
                for value in command
            ]
        env = {
            "REMRUN_BATCH_MANIFEST": manifest_path,
            "REMRUN_DONE_JSON": done_path,
            "REMRUN_STAGE": stage,
            "REMRUN_STAGE_IN": stage_in,
            "REMRUN_OUTPUT_ROOT": output_root,
        }
        if (
            not is_command
            and spec["definition"]["completion"]["protocol"] == "item-result-v2"
        ):
            env["REMRUN_BATCH_METRICS"] = metrics_path
    except (TransportError, ValueError) as exc:
        return {"ok": False, "device": device_name, "error": f"stage plan failed: {exc}"}

    if _target_acceptance_supported(device):
        try:
            pool = adapters.pool_for(head, device_name)
            resource_keys = [f"pool/{pool}"] if pool else []
            target_resource_client = TargetResourceClient.connect(
                config, device_name, install=True
            )
            if resource_keys:
                policy = target_resource_client.policy_get()
                current = policy.get("policy")
                if policy.get("status") != "installed" or not isinstance(current, dict):
                    raise TargetResourceError(
                        f"target resource policy is not installed on {device_name}"
                    )
                generation = current.get("generation")
                policy_sha = current.get("digest")
                if isinstance(generation, bool) or not isinstance(generation, int) \
                        or not isinstance(policy_sha, str):
                    raise TargetResourceError("target resource policy identity is malformed")
            else:
                generation = 0
                policy_sha = EMPTY_POLICY_DIGEST
            stable_job_ids = list(
                job_ids or [f"adhoc-{index}" for index in range(len(tasks))]
            )
            request_sha = _target_operation_digest(
                operation_id=operation_id,
                device_name=device_name,
                stage=stage,
                command=command,
                env=env,
                prepared_ids=[task.prepared["prepared_id"] for task in tasks],
                job_ids=stable_job_ids,
                resource_keys=resource_keys,
            )
            accepted_response = target_resource_client.reserve(
                allocation_id=operation_id,
                operation_id=operation_id,
                request_sha256=request_sha,
                resource_keys=resource_keys,
                expected_policy_generation=generation,
                expected_policy_digest=policy_sha,
                rpc_id=f"fleet-reserve-{operation_id}",
            )
            if not isinstance(accepted_response, TargetReservation):
                status = str(accepted_response.get("status") or "refused")
                busy = accepted_response.get("busy_keys")
                detail = f": {', '.join(busy)}" if isinstance(busy, list) and busy else ""
                raise TargetResourceError(f"target resource reservation {status}{detail}")
            reserved_target = target_reservation = accepted_response
            reservation_receipt = {
                "schema": 1,
                "operation_id": operation_id,
                "request_sha256": request_sha,
                "device": device_name,
                "accepted": False,
            }
            if on_target_reservation is not None and not on_target_reservation(
                reservation_receipt, reserved_target.token
            ):
                try:
                    target_resource_client.cancel(
                        reserved_target, rpc_id=f"fleet-cancel-owner-loss-{operation_id}"
                    )
                except TargetResourceError:
                    pass
                return {
                    "ok": False,
                    "device": device_name,
                    "staged": 0,
                    "error": "lost batch ownership before target staging",
                    "completion_state": "not_started",
                    "command_started": False,
                    "ownership_lost": True,
                    "target_operation": reservation_receipt,
                }
            target_heartbeat = _TargetReservationHeartbeat(
                target_resource_client,
                reserved_target,
                policy_generation=generation,
                policy_digest=policy_sha,
                operation_id=operation_id,
            )
            target_heartbeat.__enter__()
        except TargetResourceError as exc:
            return {
                "ok": False,
                "device": device_name,
                "staged": 0,
                "error": f"target acceptance failed before staging: {exc}",
                "completion_state": "not_started",
                "command_started": False,
            }
    used: set[str] = set()
    manifest_items: list[dict[str, Any]] = []
    expected: list[dict[str, Any]] = []
    staged = 0
    try:
        transport.ensure_remote_dir(stage)
        transport.ensure_remote_dir(stage_in)
        transport.ensure_remote_dir(output_root)
        if target_heartbeat is not None:
            target_heartbeat.require_live()
        for index, task in enumerate(tasks):
            prepared = task.prepared
            staged_names: list[str] = []
            verified_inputs: list[dict[str, Any]] = []
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
                if target_heartbeat is not None:
                    target_heartbeat.require_live()
            for item in payload["items"]:
                name = _unique_name(Path(item["source_path"]).name, used)
                remote_input = transport.native_join(stage_in, name)
                shared = None
                if item.get("storage_ref") is not None:
                    shared = storage.device_candidate(
                        storage_registry, device_name, item["storage_ref"], transport,
                    )
                if shared is not None:
                    root, candidate = shared
                    try:
                        receipt = transport.materialize_shared_input(
                            root, candidate, remote_input,
                            storage_id=item["storage_ref"]["storage_id"],
                            expected_bytes=item["identity"]["bytes"],
                            expected_sha256=item["identity"]["sha256"].removeprefix("sha256:"),
                        )
                    except (OSError, TransportError):
                        receipt = materialize_prepared_input(transport, item, remote_input)
                else:
                    receipt = materialize_prepared_input(transport, item, remote_input)
                verified_inputs.append({"index": item["index"], **receipt})
                staged_names.append(name)
                staged += 1
                if target_heartbeat is not None:
                    target_heartbeat.require_live()
            job_id = job_ids[index] if job_ids and index < len(job_ids) else f"adhoc-{index}"
            item_costs = {
                int(row["index"]): float(row["value"])
                for row in prepared["cost"].get("item_values", [])
            }
            manifest_items.append({
                "index": index, "job_id": job_id,
                "prepared_id": prepared["prepared_id"], "work_id": prepared["work_id"],
                "payload": payload, "staged": staged_names,
                "verified_inputs": verified_inputs,
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
        _push_json(transport, manifest_path, {
            "schema": 2, "batch_id": batch_id, "kind": record["kind"],
            "spec_id": record["spec_id"],
            "adapter_id": adapter["adapter_id"] if adapter else None,
            "device": device_name, "stage": stage, "stage_in": stage_in,
            "output_root": output_root, "items": manifest_items,
        })
        if target_heartbeat is not None:
            target_heartbeat.require_live()
    except SourceChangedError as exc:
        cleanup_result = finalize_prelaunch_target()
        if cleanup and target_reservation is None:
            _safe_delete(transport, stage)
        rows = []
        for index, _task in enumerate(tasks):
            job_id = job_ids[index] if job_ids and index < len(job_ids) else f"adhoc-{index}"
            rows.append({
                "job_id": job_id, "ok": False,
                "outcome": "review" if index == len(manifest_items) else "failed",
                "disposition": "review" if index == len(manifest_items) else "retry",
                "message": str(exc) if index == len(manifest_items)
                else "batch staging stopped because a sibling source changed",
            })
        return {"ok": False, "device": device_name, "staged": staged,
                "error": "source_changed", "item_results": rows, **cleanup_result}
    except (OSError, TargetResourceError, TransportError, ValueError) as exc:
        cleanup_result = finalize_prelaunch_target()
        if cleanup and target_reservation is None:
            _safe_delete(transport, stage)
        return {"ok": False, "device": device_name, "staged": staged,
                "error": f"stage failed: {exc}", **cleanup_result}
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
            cleanup_result = finalize_prelaunch_target()
            if cleanup and target_reservation is None:
                _safe_delete(transport, stage)
            memory_guard = _admission_guard_payload(transport, admission)
            return {"ok": False, "device": device_name, "staged": staged,
                    "memory_guard": memory_guard, "_memory_admission": admission_payload,
                    **_guard_outcome_fields(memory_guard), **cleanup_result}
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
        cleanup_result = finalize_prelaunch_target()
        if cleanup and target_reservation is None:
            _safe_delete(transport, stage)
        response = {"ok": False, "device": device_name, "staged": staged,
                    "definition_drift": True, "_memory_admission": admission_payload,
                    "error": "task definition changed before process launch"}
        if release_receipt is not None:
            response["memory_reservation_release"] = release_receipt
        response.update(cleanup_result)
        return response
    if target_heartbeat is not None:
        try:
            target_heartbeat.require_live()
        except TargetResourceError as exc:
            cleanup_result = finalize_prelaunch_target()
            return {
                "ok": False,
                "device": device_name,
                "staged": staged,
                "error": str(exc),
                "completion_state": "not_started",
                "command_started": False,
                **cleanup_result,
            }
        stop_target_heartbeat()
    started = time.monotonic()
    target_operation: dict[str, Any] | None = None
    durable_context: tuple[str, str] | None = None

    def operation_observation(job_id: str) -> JobObservation:
        return JobObservation.for_command(
            job_id=job_id,
            project="@fleet", target=device_name, phase="fleet-worker",
            command=command,
            declared_label=("raw-command" if is_command else
                            f"{record['task']['name']}:{adapter['engine']}"),
            member_count=len(tasks),
        )

    try:
        if _target_acceptance_supported(device):
            if reserved_target is None or target_resource_client is None:
                raise TargetResourceError("target reservation was not established before staging")
            client = target_resource_client
            observation = operation_observation(operation_id)
            launch_status, execution = transport.launch_durable(
                command,
                stage,
                run_id=operation_id,
                resume_token=reserved_target.token,
                observation=observation,
                controller=controller_label(),
                project_id="@fleet",
                max_log_bytes=load_retention(config).max_log_bytes,
                created_at=utc_now_iso(),
                env=env,
                telemetry=True,
                memory_reservation=reservation,
                target_acceptance={
                    "runner_path": client.info.installed_path,
                    "state_root": client.state_root,
                    "operation_id": operation_id,
                    "request_sha256": request_sha,
                    "reservation": {
                        "allocation_id": reserved_target.allocation_id,
                        "fence": reserved_target.fence,
                        "policy_generation": generation,
                        "policy_digest": policy_sha,
                    },
                },
            )
            target_receipt = launch_status.get("target_acceptance")
            if launch_status.get("acknowledged") is not True \
                    or not isinstance(target_receipt, dict):
                raise TransportError("target did not durably acknowledge the operation")
            target_operation = {
                "schema": 1,
                "operation_id": operation_id,
                "request_sha256": request_sha,
                "accepted": True,
                "command_started": launch_status.get("command_started"),
                "state": launch_status.get("state"),
            }
            if on_target_acceptance is not None \
                    and not on_target_acceptance(target_operation):
                return {
                    "ok": False,
                    "device": device_name,
                    "staged": staged,
                    "error": "lost batch ownership after target acceptance",
                    "completion_state": "unknown",
                    "command_started": launch_status.get("command_started"),
                    "ownership_lost": True,
                    "cleanup_deferred": True,
                    "stage_dir": stage,
                    "target_operation": target_operation,
                    "_memory_admission": admission_payload,
                }
            terminal = _wait_target_operation(transport, operation_id, reserved_target.token)
            terminal_status = terminal.get("status", terminal)
            if not isinstance(terminal_status, dict):
                raise TransportError("target operation returned malformed terminal status")
            target_operation.update(
                command_started=terminal_status.get("command_started"),
                state=terminal_status.get("state"),
                cleanup=terminal_status.get("target_cleanup"),
            )
            durable_context = (operation_id, reserved_target.token)
            if terminal_status.get("state") != "complete":
                prestart = terminal_status.get("command_started") is False
                cleanup_state = (
                    terminal_status["target_cleanup"].get("state")
                    if isinstance(terminal_status.get("target_cleanup"), dict) else None
                )
                cleanup_result: dict[str, Any] = {}
                if cleanup and prestart and cleanup_state in {
                    "RELEASED", "CANCELLED", "EXPIRED", "REBOOTED",
                }:
                    cleanup_result.update(
                        cleanup_terminal_target(cleanup_state, durable_context)
                    )
                else:
                    cleanup_result.update(cleanup_deferred=bool(cleanup), stage_dir=stage)
                return {
                    "ok": False,
                    "device": device_name,
                    "staged": staged,
                    "error": str(
                        terminal_status.get("error") or "target supervisor failed"
                    ),
                    "completion_state": "not_started" if prestart else "unknown",
                    "command_started": terminal_status.get("command_started"),
                    "target_operation": target_operation,
                    "_memory_admission": admission_payload,
                    **cleanup_result,
                }
            result = finalize_durable_result(terminal, execution)
        else:
            observed_exec = getattr(transport, "exec_observed", None)
            if not active_job_observation_enabled() or observed_exec is None:
                result = transport.exec(
                    command, cwd=stage, telemetry=True, env=env,
                    memory_reservation=reservation,
                )
            else:
                observation = operation_observation(batch_id)
                result = observed_exec(
                    command, cwd=stage, telemetry=True, env=env,
                    observation=observation, memory_reservation=reservation,
                )
    except TargetResourceError as exc:
        if reservation is not None:
            try:
                transport.release_memory_guard(reservation, reserved_only=True)
            except Exception:
                pass
        cleanup_result = finalize_prelaunch_target()
        if cleanup and target_reservation is None:
            _safe_delete(transport, stage)
        return {
            "ok": False,
            "device": device_name,
            "staged": staged,
            "error": f"target acceptance failed before launch: {exc}",
            "completion_state": "not_started",
            "command_started": False,
            "_memory_admission": admission_payload,
            **cleanup_result,
        }
    except GuardFinalizationError as exc:
        prestart = exc.command_started is False
        cleanup_result: dict[str, Any] = {}
        target_cleanup_state = None
        if target_operation is not None and isinstance(target_operation.get("cleanup"), dict):
            target_cleanup_state = target_operation["cleanup"].get("state")
        if cleanup and prestart and durable_context is not None \
                and target_cleanup_state in {
                    "RELEASED", "CANCELLED", "EXPIRED", "REBOOTED",
                }:
            cleanup_result.update(
                cleanup_terminal_target(target_cleanup_state, durable_context)
            )
        elif cleanup and prestart and target_operation is None:
            _safe_delete(transport, stage)
        elif cleanup and prestart:
            cleanup_result.update(cleanup_deferred=True, stage_dir=stage)
        memory_guard = exc.memory_guard or {}
        response = {"ok": False, "device": device_name, "staged": staged,
                    "completion_state": "not_started" if prestart else "unknown",
                    "command_started": exc.command_started, "memory_guard": memory_guard,
                    "_memory_admission": admission_payload,
                    **_guard_outcome_fields(memory_guard)}
        if cleanup and not prestart:
            response.update({"cleanup_deferred": True, "stage_dir": stage})
        response.update(cleanup_result)
        return response
    except TransportError as exc:
        accepted_evidence = target_operation is not None
        acceptance_unknown = False
        ownership_lost = False
        reservation_cleanup = None
        prestart_cleanup_state = None
        if not accepted_evidence and target_resource_client is not None \
                and target_reservation is not None:
            # A lost launch response is reconciled against the durable accepted
            # record first.  The resource ledger can prove that replay is unsafe,
            # but a CLAIMED row alone is not the protocol's positive acceptance
            # authority.
            try:
                durable = transport.durable_status(operation_id, target_reservation.token)
            except (OSError, TransportError, ValueError):
                durable = None
            target_receipt = (
                durable.get("target_acceptance") if isinstance(durable, dict) else None
            )
            if (
                isinstance(durable, dict)
                and durable.get("operation_id") == operation_id
                and durable.get("request_sha256") == request_sha
                and durable.get("acknowledged") is True
                and isinstance(target_receipt, dict)
                and target_receipt.get("operation_id") == operation_id
                and target_receipt.get("request_sha256") == request_sha
            ):
                target_operation = {
                    **(reservation_receipt or {}),
                    "schema": 1,
                    "operation_id": operation_id,
                    "request_sha256": request_sha,
                    "accepted": True,
                    "command_started": durable.get("command_started"),
                    "state": durable.get("state"),
                    "cleanup": durable.get("target_cleanup"),
                }
                accepted_evidence = True
                if on_target_acceptance is not None \
                        and not on_target_acceptance(target_operation):
                    ownership_lost = True
        if not accepted_evidence and target_resource_client is not None \
                and target_reservation is not None:
            try:
                observed = target_resource_client.status(
                    target_reservation,
                    rpc_id=f"fleet-status-launch-failure-{operation_id}",
                )
                resource_receipt = observed.get("receipt")
                if not isinstance(resource_receipt, dict):
                    raise TargetResourceError("target status omitted its resource receipt")
                resource_state = str(resource_receipt.get("state") or "UNKNOWN")
                if resource_state == "RESERVED":
                    reservation_cleanup = "reserved"
                elif resource_state in {"CANCELLED", "EXPIRED", "REBOOTED"}:
                    reservation_cleanup = resource_state.lower()
                    prestart_cleanup_state = resource_state
                elif resource_state in {"CLAIMED", "QUARANTINED", "RELEASED"}:
                    start_state = resource_receipt.get("command_start_state")
                    command_started = (
                        True if start_state == "YES" else
                        False if start_state == "NO" else None
                    )
                    target_operation = {
                        **(reservation_receipt or {}),
                        "schema": 1,
                        "operation_id": operation_id,
                        "request_sha256": request_sha,
                        "accepted": False,
                        "command_started": command_started,
                        "state": "unknown",
                        "cleanup": resource_receipt,
                    }
                    acceptance_unknown = True
                    reservation_cleanup = f"unknown:{resource_state}"
                else:
                    acceptance_unknown = True
                    reservation_cleanup = f"unknown:{resource_state}"
            except TargetResourceError as status_exc:
                acceptance_unknown = True
                reservation_cleanup = f"unknown:{status_exc}"
        memory_release = None
        safe_prestart = not accepted_evidence and not acceptance_unknown
        if safe_prestart and reservation is not None:
            try:
                released = transport.release_memory_guard(
                    reservation, reserved_only=True,
                )
                memory_release = _admission_receipt(released.payload)
            except Exception as release_exc:  # noqa: BLE001 - expiry remains the backstop
                memory_release = {
                    "status": "release_failed",
                    "reason": type(release_exc).__name__,
                }
            reservation = None
        cleanup_result = (
            finalize_prelaunch_target(prestart_cleanup_state)
            if safe_prestart else {}
        )
        if cleanup and safe_prestart and target_reservation is None:
            _safe_delete(transport, stage)
        operation_receipt = target_operation or reservation_receipt
        return {"ok": False, "device": device_name, "staged": staged,
                "error": f"exec failed: {exc}",
                "completion_state": (
                    "unknown" if accepted_evidence or acceptance_unknown else "not_started"
                ),
                "command_started": (
                    target_operation.get("command_started")
                    if target_operation is not None else
                    None if acceptance_unknown else False
                ),
                "cleanup_deferred": bool(cleanup and (accepted_evidence or acceptance_unknown)),
                **({"stage_dir": stage} if accepted_evidence or acceptance_unknown else {}),
                **({"target_operation": operation_receipt}
                   if operation_receipt is not None else {}),
                **({"target_acceptance_unknown": True} if acceptance_unknown else {}),
                **({"ownership_lost": True} if ownership_lost else {}),
                **({"target_reservation_cleanup": reservation_cleanup}
                   if reservation_cleanup is not None else {}),
                **({"memory_reservation_release": memory_release}
                   if memory_release is not None else {}),
                **cleanup_result,
                "_memory_admission": admission_payload}

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
            rows, output_return_started = _return_outputs(
                tasks, rows, output_root, transport, job_ids,
                before_output_return=before_output_return,
            )
            if (output_return_started and before_output_return is not None
                    and not before_output_return()):
                raise _OutputReturnOwnershipLost
        except ResultProtocolError as exc:
            evidence_error = str(exc)
    stage_cleanup_error = None
    target_cleanup_result: dict[str, Any] = {}
    target_cleanup_state = None
    if target_operation is not None and isinstance(target_operation.get("cleanup"), dict):
        target_cleanup_state = target_operation["cleanup"].get("state")
    target_cleanup_terminal = target_cleanup_state in {
        "RELEASED", "CANCELLED", "EXPIRED", "REBOOTED",
    }
    retain_target_evidence = target_operation is not None and not target_cleanup_terminal
    if cleanup and target_cleanup_terminal and durable_context is not None:
        target_cleanup_result.update(
            cleanup_terminal_target(target_cleanup_state, durable_context)
        )
    elif cleanup and target_operation is None:
        try:
            transport.remove_remote_tree(stage)
        except (OSError, TransportError, NotImplementedError) as exc:
            stage_cleanup_error = str(exc)
    delivery_complete = not any(
        row.get("failure_code") == "output_return_failed" for row in rows
    )
    response = {
        "ok": result.exit_code == 0 and evidence_error is None and delivery_complete,
        "device": device_name, "engine": adapter["engine"] if adapter else "raw-command",
        "exit_code": result.exit_code, "elapsed_s": elapsed, "staged": staged,
        "jobs": len(tasks), "output_root": output_root, "telemetry": result.telemetry,
        "item_results": rows, "stdout_tail": (result.stdout or "")[-500:],
        "stderr_tail": (result.stderr or "")[-500:],
        "_memory_admission": admission_payload,
    }
    if target_operation is not None:
        response["target_operation"] = target_operation
    response.update(target_cleanup_result)
    if stage_cleanup_error is not None:
        response.update(
            cleanup_deferred=True,
            stage_dir=stage,
            target_cleanup_deferred="target stage deletion failed",
        )
    if retain_target_evidence:
        response.update(
            cleanup_deferred=True,
            stage_dir=stage,
            target_cleanup_deferred="target process cleanup is not terminal",
        )
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


class _OutputReturnOwnershipLost(RuntimeError):
    pass


def _return_outputs(
    tasks: list[FleetTask], rows: list[dict[str, Any]], output_root: str,
    transport: Any, job_ids: list[str] | None, *,
    before_output_return: Callable[[], bool] | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """Opt-in verified delivery; a failure preserves remote publication for review."""
    task_by_job = {
        (job_ids[index] if job_ids and index < len(job_ids) else f"adhoc-{index}"): task
        for index, task in enumerate(tasks)
    }
    delivered: list[dict[str, Any]] = []
    output_return_started = False
    for original in rows:
        row = dict(original)
        task = task_by_job.get(str(row.get("job_id")))
        return_root = (task.prepared["output"].get("return_root")
                       if task is not None and task.prepared is not None else None)
        if not return_root or row.get("publication") not in {"produced", "reused"}:
            delivered.append(row)
            continue
        rel_paths = list(row.get("outputs") or [])
        if row.get("companion") is not None:
            rel_paths.append(row["companion"])
        receipts = []
        try:
            for rel in rel_paths:
                if before_output_return is not None and not before_output_return():
                    raise _OutputReturnOwnershipLost
                output_return_started = True
                components = rel.split("/")
                remote_path = transport.native_join(output_root, *components)
                local_path = _confined_return_path(Path(return_root), components)
                receipts.append({"path": rel, **transport.fetch_output(remote_path, local_path)})
        except (OSError, TransportError) as exc:
            row.update({
                "ok": False, "outcome": "review", "disposition": "none",
                "retry_after_s": None, "failure_code": "output_return_failed",
                "message": f"output return failed: {exc}",
                "error": f"output return failed: {exc}",
            })
            details = dict(row.get("details") or {})
            details["output_return"] = {"status": "failed", "receipts": receipts}
            row["details"] = details
        else:
            row["delivery"] = {
                "schema": "output-delivery-v1", "status": "complete",
                "root": return_root, "receipts": receipts,
            }
        delivered.append(row)
    return delivered, output_return_started


def _confined_return_path(root: Path, components: list[str]) -> Path:
    """Resolve a prepared safe-relative output without traversing mutable links/mounts."""
    root = root.resolve(strict=False)
    cursor = root
    is_junction = getattr(os.path, "isjunction", lambda _path: False)
    for component in components[:-1]:
        cursor = cursor / component
        if cursor.is_symlink() or is_junction(cursor) or (cursor.exists() and os.path.ismount(cursor)):
            raise TransportError("output return path crosses a link or mount boundary")
        if cursor.exists() and not cursor.is_dir():
            raise TransportError("output return parent is not a directory")
        if cursor.exists():
            try:
                cursor.resolve(strict=True).relative_to(root)
            except ValueError as exc:
                raise TransportError("output return path escapes its prepared root") from exc
    return root.joinpath(*components)


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


def _safe_delete(transport: Any, remote_dir: str) -> None:
    try:
        transport.remove_remote_tree(remote_dir)
    except (OSError, TransportError, NotImplementedError):
        pass
