from __future__ import annotations

import json
import shutil
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun.config import RemrunConfig
from remrun.fleet import cli, dispatcher, executor, placement, queue as queue_mod
from remrun.fleet.models import DeviceSnapshot
from remrun.fleet.prepared import (
    RAW_COMMAND_SPEC,
    RAW_COMMAND_SPEC_ID,
    PreparationError,
    as_fleet_task,
    prepare_raw_command,
    prepare_task_job,
    prepared_features,
    prepared_memory_limit_mib,
    validate_prepared_job,
)
from remrun.fleet.queue import FleetQueue
from remrun.fleet.task_contract import resolve_task_spec
from remrun.memory_guard import MemoryAdmissionResult, MemoryReservation
from remrun.models import Device
from remrun.transport import ExecResult

MIB = 1024 * 1024


def _config(tmp_path: Path) -> RemrunConfig:
    device = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim", "os": "posix",
        "address_candidates": ["localhost"],
        "project_root": str(tmp_path / "remote"),
        "cache_root": str(tmp_path / "cache"),
        "state_root": str(tmp_path / "device-state"),
        "max_jobs": 2,
    })
    return RemrunConfig(
        repo_root=tmp_path, defaults={}, devices={"LOCAL_SIM": device},
        project_roots={}, offload={},
    )


def _raw_task(limit_mib: int | None = None):  # noqa: ANN202
    spec = {**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID}
    record = prepare_raw_command(
        ["python", "-c", "raise SystemExit(0)"],
        device="LOCAL_SIM", memory_limit_mib=limit_mib,
    )
    return record, as_fleet_task(record, spec)


def _compatible_definition(tmp_path: Path) -> dict:
    return {
        "input": {"mode": "text", "split": "never"},
        "prepare": {"mode": "none"},
        "routing": {"requirements": [], "requirements_by_option": {}},
        "execution": {"batching": "compatible", "replay": "at-most-once-v1"},
        "cost": {"measure": "none", "bucket_options": []},
        "output": {"reservation": "none", "allow_root_override": False,
                   "verification": "none"},
        "completion": {"protocol": "item-result-v2", "evidence": "always",
                       "companion": "forbidden", "allowed_publication": ["none"],
                       "unstructured_memory": "ignore"},
        "options": {},
        "adapters": {"LOCAL_SIM": {
            "engine": "generic", "argv": ["python", "-c", "raise SystemExit(0)"],
            # LOCAL_SIM deliberately models a POSIX target even when the
            # controller test runs on Windows, so keep its target path native.
            "output_root": f"/tmp/{tmp_path.name}-output", "pool": False,
            "memory_kind": "cpu", "capability_paths": [], "provides": [],
        }},
    }


class GuardedTransport:
    def __init__(self, root: Path, *, terminate: bool = False) -> None:
        self.root = root
        self.memory_guard = SimpleNamespace(
            command_limit_fraction=0.5, host_reserve_fraction=None,
        )
        self.reserve_calls: list[dict] = []
        self.release_calls: list[tuple[MemoryReservation, bool]] = []
        self.exec_calls = 0
        self.terminate = terminate

    @staticmethod
    def native_join(*parts: str) -> str:
        return str(Path(*parts))

    def remote_temp_dir(self, _prefix: str) -> str:
        stage = self.root / "stage"
        stage.mkdir(parents=True, exist_ok=True)
        return str(stage)

    @staticmethod
    def ensure_remote_dir(path: str) -> None:
        Path(path).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def push_file(source: Path, target: str) -> None:
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)

    @staticmethod
    def expand_remote(path: str) -> str:
        return path

    @staticmethod
    def remove_remote_tree(path: str) -> None:
        shutil.rmtree(path, ignore_errors=True)

    def reserve_memory_guard(self, *, predicted_rss_mb=None,
                             explicit_limit_mib=None) -> MemoryAdmissionResult:
        self.reserve_calls.append({
            "predicted_rss_mb": predicted_rss_mb,
            "explicit_limit_mib": explicit_limit_mib,
        })
        assert explicit_limit_mib is not None
        allowance = explicit_limit_mib * MIB
        overhead = 64 * MIB
        ceiling = 12 * 1024 * MIB
        reserve = 4 * 1024 * MIB
        total = 32 * 1024 * MIB
        reservation = MemoryReservation(
            lease_id="private-lease-id", lease_token="private-lease-token",
            state_root="/private/target/ledger", allowance_bytes=allowance,
            control_overhead_bytes=overhead, capacity_bytes=allowance + overhead,
            max_command_bytes=ceiling, min_available_bytes=reserve,
            host_total_bytes=total, safe_concurrency=2, expires_at=4102444800.0,
            allowance_basis="explicit_command_limit",
        )
        payload = {
            "schema": 2, "status": "admitted", "reason": "reserved",
            "detail": "/private/admission/detail",
            "active_leases": 1,
            "policy": {"max_command_bytes": ceiling, "min_available_bytes": reserve,
                       "host_total_bytes": total, "safe_concurrency": 2},
            "lease": {
                **reservation.as_dict(include_token=True),
                "enforced_command_limit_bytes": allowance,
                "allowance_basis": "explicit_command_limit",
                "host_reserve_bytes": reserve,
            },
        }
        return MemoryAdmissionResult("admitted", "reserved", "", payload, reservation)

    def release_memory_guard(self, reservation: MemoryReservation, *,
                             reserved_only: bool = True) -> MemoryAdmissionResult:
        self.release_calls.append((reservation, reserved_only))
        payload = {
            "schema": 2, "status": "released", "reason": "released",
            "detail": "/private/release/detail",
            "active_leases": 0, "lease_released": True,
            "lease_token": "must-never-persist",
            "state_root": "/must/never/persist",
        }
        return MemoryAdmissionResult("released", "released", "", payload, None)

    def exec(self, _command, **kwargs) -> ExecResult:  # noqa: ANN001
        self.exec_calls += 1
        reservation = kwargs["memory_reservation"]
        peak = reservation.allowance_bytes + MIB if self.terminate else 6 * 1024 * MIB
        guard = {
            "schema": 1,
            "status": "terminated" if self.terminate else "ok",
            "reason": "command_memory_limit" if self.terminate else "completed",
            "detail": "/private/guard/detail",
            "command_started": True,
            "command_exit_code": 125 if self.terminate else 0,
            "helper_exit_code": 125 if self.terminate else 0,
            "max_command_bytes": reservation.allowance_bytes,
            "min_available_bytes": reservation.min_available_bytes,
            "host_total_bytes": reservation.host_total_bytes,
            "initial_host_available_bytes": 20 * 1024 * MIB,
            "min_host_available_bytes": 18 * 1024 * MIB,
            "peak_command_bytes": peak,
            "trigger_value_bytes": peak if self.terminate else None,
            "memory_metric": "rss_sum_sampled",
            "sample_count": 9,
            "sample_interval_ms": 200,
            "cleanup_complete": True,
            "process_tree_drained": True,
            "forced_descendant_cleanup": False,
            "platform": "Darwin",
            "lease_token": "must-never-persist",
        }
        return ExecResult(
            exit_code=125 if self.terminate else 0,
            stdout="", stderr="", telemetry={"peak_rss_mb": peak / MIB},
            memory_guard=guard, memory_reservation=reservation,
        )


def test_explicit_limit_uses_v2_but_unlimited_jobs_remain_exact_v1() -> None:
    legacy = prepare_raw_command(["echo", "ok"], device="LOCAL_SIM")
    explicit = prepare_raw_command(
        ["echo", "ok"], device="LOCAL_SIM", memory_limit_mib=8192,
    )

    validate_prepared_job(legacy)
    validate_prepared_job(explicit)
    assert legacy["schema"] == 1 and "limits" not in legacy
    assert explicit["schema"] == 2
    assert explicit["limits"] == {
        "process_tree_rss_mib": 8192, "provenance": "submit-explicit",
    }
    assert explicit["work_id"] == legacy["work_id"]
    assert explicit["prepared_id"] != legacy["prepared_id"]
    assert explicit["cost"] == legacy["cost"]
    assert explicit["cost"]["status"] == "unestimated"
    assert prepared_memory_limit_mib(legacy) is None
    assert prepared_memory_limit_mib(explicit) == 8192


@pytest.mark.parametrize("value", [True, 0, -1, 1.5, 2**63])
def test_explicit_limit_validation_is_strict(value) -> None:  # noqa: ANN001
    with pytest.raises(PreparationError, match="positive whole-MiB"):
        prepare_raw_command(
            ["echo", "ok"], device="LOCAL_SIM", memory_limit_mib=value,
        )


def test_v2_limits_are_closed_and_unsupported_schemas_fail_closed() -> None:
    explicit = prepare_raw_command(
        ["echo", "ok"], device="LOCAL_SIM", memory_limit_mib=8192,
    )
    unknown = deepcopy(explicit)
    unknown["limits"]["extra"] = True
    with pytest.raises(PreparationError, match="unknown or missing fields"):
        prepared_memory_limit_mib(unknown)
    with pytest.raises(PreparationError, match="unsupported prepared job schema"):
        prepared_memory_limit_mib({"schema": 5})


def test_explicit_v2_round_trips_through_durable_queue(
        tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    record = prepare_raw_command(
        ["echo", "ok"], device="LOCAL_SIM", memory_limit_mib=8192,
    )
    db_path = tmp_path / "fleet.db"
    queue = FleetQueue(db_path)
    try:
        job_id = queue.enqueue_prepared(record, spec=None, job_id="explicit-v2")
    finally:
        queue.close()

    queue = FleetQueue(db_path)
    try:
        loaded = queue.prepared_record(job_id)
        row = queue.get(job_id)
    finally:
        queue.close()
    assert loaded == record
    assert row is not None and row["prepared_id"] == record["prepared_id"]
    assert json.loads(row["prepared_json"])["limits"] == record["limits"]


def test_hard_limit_preserves_unestimated_cold_start_without_inventing_eta(
        tmp_path: Path) -> None:
    definition = _compatible_definition(tmp_path)
    adapter = next(iter(definition["adapters"].values()))
    definition["adapters"] = {
        name: {**deepcopy(adapter), "output_root": str(tmp_path / name.lower())}
        for name in ("A", "B")
    }
    spec = resolve_task_spec(
        "generic", definition, devices={"A", "B"}, repo_root=tmp_path,
    )
    unforced = prepare_task_job(
        spec, repo_root=tmp_path, text="same", memory_limit_mib=8192,
    )
    snapshots = {
        name: DeviceSnapshot(
            name=name, reachable=True, max_jobs=2, pool_free={},
            engine_status={"generic": "present"}, ram_free_mb=32000.0,
        )
        for name in ("A", "B")
    }
    fcfg = {
        "transfer_mbps": 200.0, "ssh_setup_s": 0.0,
        "per_file_overhead_s": 0.0, "min_hysteresis_s": 1.0, "pools": {},
    }
    automatic = placement.plan_jobs(
        [as_fleet_task(unforced, spec)], [prepared_features(unforced)],
        snapshots, {}, fcfg,
    )
    assert automatic.skipped == {}
    assert automatic.makespan_s is None
    assert [batch.device for batch in automatic.batches] == ["A"]
    assert automatic.batches[0].selection_basis == "cold_start"
    assert automatic.batches[0].estimated_finish_s is None
    assert automatic.batches[0].estimate_reason == "uncalibrated"

    forced = prepare_task_job(
        spec, repo_root=tmp_path, text="same", force_device="A",
        memory_limit_mib=8192,
    )
    manual = placement.plan_jobs(
        [as_fleet_task(forced, spec)], [prepared_features(forced)],
        snapshots, {}, fcfg,
    )
    assert [batch.device for batch in manual.batches] == ["A"]
    assert manual.batches[0].reason == "forced"


def test_limit_is_bound_to_prepared_identity_and_batch_compatibility(tmp_path: Path) -> None:
    spec = resolve_task_spec(
        "generic", _compatible_definition(tmp_path),
        devices={"LOCAL_SIM"}, repo_root=tmp_path,
    )
    first = prepare_task_job(
        spec, repo_root=tmp_path, text="same", force_device="LOCAL_SIM",
        memory_limit_mib=6144,
    )
    second = prepare_task_job(
        spec, repo_root=tmp_path, text="same", force_device="LOCAL_SIM",
        memory_limit_mib=8192,
    )
    first_task = as_fleet_task(first, spec)
    second_task = as_fleet_task(second, spec)

    assert first["work_id"] == second["work_id"]
    assert first["prepared_id"] != second["prepared_id"]
    assert executor._group_contract_error([first_task, second_task]) is not None
    assert dispatcher._compat_key(first_task) != dispatcher._compat_key(second_task)


def test_executor_forwards_exact_limit_without_turning_it_into_prediction(
        tmp_path: Path, monkeypatch) -> None:
    record, task = _raw_task(8192)
    transport = GuardedTransport(tmp_path)
    monkeypatch.setattr(executor, "make_transport", lambda _device: transport)

    result = executor.run_batch(
        "LOCAL_SIM", [task], _config(tmp_path), state_root=tmp_path / "state",
        prelaunch_gate=lambda: True,
    )

    assert result["ok"] is True
    assert transport.reserve_calls == [{
        "predicted_rss_mb": None, "explicit_limit_mib": 8192,
    }]
    assert result["memory_limit"]["requested_bytes"] == 8192 * MIB
    assert result["memory_limit"]["provenance"] == "submit-explicit"
    assert result["memory_limit"]["admission"]["allowance_basis"] == (
        "explicit_command_limit"
    )
    assert result["memory_limit"]["admission"]["enforced_command_limit_bytes"] == 8192 * MIB
    assert result["memory_limit"]["outcome"]["memory_metric"] == "rss_sum_sampled"
    durable = executor.durable_attempt_record(task, result)
    assert durable is not None
    assert "private-lease" not in durable and "/private/" not in durable
    assert "must-never-persist" not in durable
    assert prepared_memory_limit_mib(record) == 8192


def test_explicit_limit_fails_closed_on_target_without_guard(tmp_path: Path) -> None:
    _record, task = _raw_task(8192)
    result = executor.run_batch(
        "LOCAL_SIM", [task], _config(tmp_path), state_root=tmp_path / "state",
        prelaunch_gate=lambda: True,
    )

    assert result["ok"] is False
    assert result["phase"] == "memory_admission"
    assert result["memory_limit"]["admission"]["reason"] == "guard_not_configured"
    assert result["memory_limit"]["requested_mib"] == 8192
    assert "no_retry" not in result


def test_definition_drift_releases_reserved_only_ledger_entry(
        tmp_path: Path, monkeypatch) -> None:
    _record, task = _raw_task(8192)
    transport = GuardedTransport(tmp_path)
    monkeypatch.setattr(executor, "make_transport", lambda _device: transport)

    result = executor.run_batch(
        "LOCAL_SIM", [task], _config(tmp_path), state_root=tmp_path / "state",
        prelaunch_gate=lambda: False,
    )

    assert result["definition_drift"] is True
    assert transport.exec_calls == 0
    assert len(transport.release_calls) == 1
    assert transport.release_calls[0][1] is True
    assert result["memory_limit"]["release"]["status"] == "released"
    assert result["memory_limit"]["release"]["lease_released"] is True
    assert "must-never-persist" not in json.dumps(result["memory_limit"], sort_keys=True)


def test_leased_definition_drift_persists_sanitized_release_receipt(
        tmp_path: Path, monkeypatch) -> None:
    """The fenced queue keeps proof that target admission was released."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    spec = resolve_task_spec(
        "generic", _compatible_definition(tmp_path),
        devices={"LOCAL_SIM"}, repo_root=tmp_path,
    )
    record = prepare_task_job(
        spec, repo_root=tmp_path, text="same", force_device="LOCAL_SIM",
        memory_limit_mib=8192,
    )
    task = as_fleet_task(record, spec)
    transport = GuardedTransport(tmp_path / "target")
    monkeypatch.setattr(executor, "make_transport", lambda _device: transport)
    monkeypatch.setattr(executor, "load_config", lambda _root: object())
    monkeypatch.setattr(
        executor, "resolve_tasks",
        lambda _config: {} if transport.reserve_calls else {"generic": spec},
    )
    state_root = tmp_path / "state"

    result = executor._run_group_leased(
        "LOCAL_SIM", [task], _config(tmp_path), state_root=state_root,
        cleanup=True, lease_seconds=60,
    )

    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        row = dict(queue.db.execute(
            "SELECT state,attempts,last_result FROM jobs ORDER BY created_at DESC LIMIT 1"
        ).fetchone())
    finally:
        queue.close()
    assert result.get("definition_drift") is True, result
    assert row["state"] == "needs_review"
    assert row["attempts"] == 1
    receipt = json.loads(row["last_result"])
    assert receipt["kind"] == "fleet-attempt-receipt"
    assert receipt["memory_limit"]["requested_mib"] == 8192
    release = receipt["memory_limit"]["release"]
    assert release["status"] == "released"
    assert release["reason"] == "released"
    assert release["lease_released"] is True
    assert "private-lease" not in row["last_result"]
    assert "must-never-persist" not in row["last_result"]
    assert "/private/" not in row["last_result"]


@pytest.mark.parametrize(
    ("argv", "expected_action"),
    [
        (["plan", "generic", "--memory-limit-mib", "8192"], None),
        (["submit", "generic", "--memory-limit-mib", "8192"], None),
        (["run", "generic", "--memory-limit-mib", "8192"], None),
        (["command", "plan", "--device", "LOCAL_SIM", "--memory-limit-mib", "8192",
          "--", "echo", "ok"], "plan"),
        (["command", "submit", "--device", "LOCAL_SIM", "--memory-limit-mib", "8192",
          "--", "echo", "ok"], "submit"),
        (["command", "run", "--device", "LOCAL_SIM", "--memory-limit-mib", "8192",
          "--", "echo", "ok"], "run"),
    ],
)
def test_all_fleet_submission_surfaces_accept_explicit_limit(argv, expected_action) -> None:  # noqa: ANN001
    parsed = cli.build_parser().parse_args(argv)
    assert parsed.memory_limit_mib == 8192
    if expected_action is not None:
        assert parsed.command_action == expected_action


@pytest.mark.parametrize("terminate", [False, True])
def test_leased_queue_persists_sanitized_explicit_limit_receipt(
        tmp_path: Path, monkeypatch, terminate: bool) -> None:
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    _record, task = _raw_task(8192)
    transport = GuardedTransport(tmp_path / "target", terminate=terminate)
    monkeypatch.setattr(executor, "make_transport", lambda _device: transport)
    state_root = tmp_path / "state"

    result = executor._run_group_leased(
        "LOCAL_SIM", [task], _config(tmp_path), state_root=state_root,
        cleanup=True, lease_seconds=60,
    )

    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        row = dict(queue.db.execute(
            "SELECT state,attempts,last_result FROM jobs ORDER BY created_at DESC LIMIT 1"
        ).fetchone())
    finally:
        queue.close()
    assert row["state"] == ("completion_unknown" if terminate else "done")
    assert row["attempts"] == 1
    receipt = json.loads(row["last_result"])
    assert receipt["kind"] == "fleet-attempt-receipt"
    assert receipt["memory_limit"]["requested_mib"] == 8192
    assert receipt["memory_limit"]["admission"]["allowance_basis"] == (
        "explicit_command_limit"
    )
    assert "private-lease" not in row["last_result"]
    assert "must-never-persist" not in row["last_result"]
    assert "/private/" not in row["last_result"]
    if terminate:
        assert result["no_retry"] is True
        assert receipt["memory_limit"]["outcome"]["reason"] == "command_memory_limit"
        assert receipt["memory_limit"]["outcome"]["command_started"] is True


def test_durable_dispatcher_path_persists_poststart_limit_receipt_without_retry(
        tmp_path: Path, monkeypatch) -> None:
    """Exercise queue -> dispatcher -> executor, not only the ad-hoc leased runner."""
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    record, _task = _raw_task(8192)
    state_root = tmp_path / "state"
    db_path = state_root / "fleet" / "fleet.db"
    queue = FleetQueue(db_path)
    try:
        job_id = queue.enqueue_prepared(record, spec=None, job_id="dispatch-explicit-v2")
    finally:
        queue.close()

    transport = GuardedTransport(tmp_path / "target", terminate=True)
    monkeypatch.setattr(executor, "make_transport", lambda _device: transport)
    result = dispatcher.drain_once(
        _config(tmp_path), state_root=state_root, max_parallel=1,
    )

    assert result["placed"] == 1 and result["ran"] == 1 and result["review"] == 1
    queue = FleetQueue(db_path)
    try:
        row = queue.get(job_id)
    finally:
        queue.close()
    assert row is not None
    assert row["state"] == "completion_unknown"
    assert row["attempts"] == 1
    receipt = json.loads(row["last_result"])
    assert receipt["kind"] == "fleet-attempt-receipt"
    assert receipt["memory_limit"]["requested_mib"] == 8192
    assert receipt["memory_limit"]["outcome"]["command_started"] is True
    assert receipt["memory_limit"]["outcome"]["reason"] == "command_memory_limit"
    assert "private-lease" not in row["last_result"]
    assert "must-never-persist" not in row["last_result"]
