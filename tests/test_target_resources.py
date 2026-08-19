from __future__ import annotations

import builtins
import errno
import hashlib
import json
import os
import sqlite3
import struct
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from remrun.config import RemrunConfig
from remrun.models import Device
from remrun.remote import runner as remote_runner
from remrun.runner_client import RunnerClientError, ensure_versioned_runner, runner_rpc
from remrun.target_resources import TargetReservation, TargetResourceClient
from remrun.transport import make_transport


def _config(tmp_path: Path) -> RemrunConfig:
    device = Device.from_mapping(
        "LOCAL_SIM",
        {
            "kind": "local-sim",
            "os": "posix",
            "project_root": str(tmp_path / "remote-projects"),
            "state_root": str(tmp_path / "remote-state"),
            "cache_root": str(tmp_path / "remote-cache"),
        },
    )
    return RemrunConfig(
        repo_root=tmp_path / "tool",
        defaults={},
        devices={"LOCAL_SIM": device},
        project_roots={"default": str(tmp_path / "projects")},
    )


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _policy(*keys: str, generation: int = 1) -> tuple[dict, str]:
    document = {
        "schema": "remrun.target-resource-policy",
        "version": 1,
        "generation": generation,
        "resources": [{"key": key, "capacity": 1} for key in sorted(keys)],
    }
    return document, hashlib.sha256(_canonical(document)).hexdigest()


def _installed(tmp_path: Path, *keys: str):
    cfg = _config(tmp_path)
    info = ensure_versioned_runner(cfg, "LOCAL_SIM", install=True)
    transport = make_transport(cfg.devices["LOCAL_SIM"])
    document, digest = _policy(*keys)
    response = runner_rpc(
        transport,
        info.installed_path,
        cfg.devices["LOCAL_SIM"].state_root,
        "target_resource_policy_install",
        {
            "expected_generation": None,
            "expected_digest": None,
            "policy_document": document,
            "supplied_digest": digest,
        },
    )
    assert response["status"] == "installed"
    return cfg, info, transport, digest


def _reserve(
    cfg: RemrunConfig,
    info,
    transport,
    digest: str,
    *,
    allocation_id: str,
    keys: list[str],
    rpc_id: str | None = None,
) -> dict:
    return runner_rpc(
        transport,
        info.installed_path,
        cfg.devices["LOCAL_SIM"].state_root,
        "target_resource_reserve",
        {
            "allocation_id": allocation_id,
            "operation_id": f"operation-{allocation_id}",
            "request_sha256": hashlib.sha256(allocation_id.encode()).hexdigest(),
            "resource_keys": keys,
            "expected_policy_generation": 1,
            "expected_policy_digest": digest,
        },
        rpc_id=rpc_id,
    )


def test_participant_store_migrates_v2_to_resource_schema_v3(tmp_path: Path):
    runner_root = tmp_path / "state" / "runner" / "v1"
    runner_root.mkdir(parents=True)
    db = runner_root / "runner.sqlite3"
    device_id = str(uuid.uuid4())
    with sqlite3.connect(db) as conn:
        for statement in remote_runner.SCHEMA[: remote_runner.RUNNER_V2_SCHEMA_COUNT]:
            conn.execute(statement)
        conn.execute(
            "INSERT INTO runner_meta "
            "(singleton,schema_version,device_id,created_at_ns) VALUES (1,2,?,1)",
            (device_id,),
        )
        conn.execute("PRAGMA user_version=2")

    conn, _root, meta = remote_runner.open_participant_store(str(tmp_path / "state"))
    try:
        assert meta["schema_version"] == 3
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert {
            "target_resource_policy",
            "target_resource_fence",
            "target_resource_allocations",
            "target_resource_holds",
        } <= tables
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    finally:
        conn.close()


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda value: {**value, "generation": 2}, "generation 1"),
        (
            lambda value: {
                **value,
                "resources": [{"key": "pool/gpu", "capacity": 2}],
            },
            "capacity",
        ),
        (
            lambda value: {
                **value,
                "resources": [
                    {"key": "pool/gpu", "capacity": 1},
                    {"key": "pool/gpu", "capacity": 1},
                ],
            },
            "duplicate",
        ),
    ],
)
def test_policy_first_install_is_strict(tmp_path: Path, mutate, match: str):
    cfg = _config(tmp_path)
    info = ensure_versioned_runner(cfg, "LOCAL_SIM", install=True)
    transport = make_transport(cfg.devices["LOCAL_SIM"])
    document, _digest = _policy("pool/gpu")
    document = mutate(document)
    digest = hashlib.sha256(_canonical(document)).hexdigest()

    with pytest.raises(RunnerClientError, match=match):
        runner_rpc(
            transport,
            info.installed_path,
            cfg.devices["LOCAL_SIM"].state_root,
            "target_resource_policy_install",
            {
                "expected_generation": None,
                "expected_digest": None,
                "policy_document": document,
                "supplied_digest": digest,
            },
        )


def test_policy_absence_and_digest_mismatch_fail_closed(tmp_path: Path):
    cfg = _config(tmp_path)
    info = ensure_versioned_runner(cfg, "LOCAL_SIM", install=True)
    transport = make_transport(cfg.devices["LOCAL_SIM"])
    missing_digest = "0" * 64
    with pytest.raises(RunnerClientError, match="policy is not installed"):
        _reserve(
            cfg,
            info,
            transport,
            missing_digest,
            allocation_id="missing-policy",
            keys=["pool/gpu"],
        )

    cfg, info, transport, digest = _installed(tmp_path, "pool/gpu")
    with pytest.raises(RunnerClientError, match="policy.*mismatch"):
        _reserve(
            cfg,
            info,
            transport,
            "f" * 64,
            allocation_id="wrong-policy",
            keys=["pool/gpu"],
        )
    assert digest != "f" * 64


def test_two_connections_race_one_capacity_one_key(tmp_path: Path):
    cfg, info, _transport, digest = _installed(tmp_path, "pool/gpu")

    def contender(index: int) -> dict:
        return _reserve(
            cfg,
            info,
            make_transport(cfg.devices["LOCAL_SIM"]),
            digest,
            allocation_id=f"allocation-{index}",
            keys=["pool/gpu"],
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(contender, range(2)))

    assert sorted(result["status"] for result in results) == ["reserved", "resource_busy"]
    winner = next(result for result in results if result["status"] == "reserved")
    assert winner["token"]
    assert winner["receipt"]["state"] == "RESERVED"
    db = Path(info.probe["runner_root"]) / "runner.sqlite3"
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM target_resource_holds").fetchone()[0] == 1
        assert conn.execute(
            "SELECT count(*) FROM target_resource_allocations"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT count(*) FROM target_resource_allocations WHERE token_sha256=?",
            (hashlib.sha256(winner["token"].encode()).hexdigest(),),
        ).fetchone()[0] == 1


def test_multi_key_reservation_is_all_or_none(tmp_path: Path):
    cfg, info, transport, digest = _installed(tmp_path, "pool/gpu", "tcp/8188")
    first = _reserve(
        cfg,
        info,
        transport,
        digest,
        allocation_id="gpu-owner",
        keys=["pool/gpu"],
    )
    assert first["status"] == "reserved"

    blocked = _reserve(
        cfg,
        info,
        transport,
        digest,
        allocation_id="both-owner",
        keys=["pool/gpu", "tcp/8188"],
    )
    assert blocked["status"] == "resource_busy"
    assert blocked["busy_keys"] == ["pool/gpu"]
    db = Path(info.probe["runner_root"]) / "runner.sqlite3"
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT count(*) FROM target_resource_holds WHERE resource_key='tcp/8188'"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM target_resource_allocations WHERE allocation_id='both-owner'"
        ).fetchone()[0] == 0


def test_exact_rpc_replay_returns_original_token_and_changed_bytes_fail(tmp_path: Path):
    cfg, info, transport, digest = _installed(tmp_path, "pool/gpu")
    rpc_id = "reservation-replay"
    first = _reserve(
        cfg,
        info,
        transport,
        digest,
        allocation_id="replayed",
        keys=["pool/gpu"],
        rpc_id=rpc_id,
    )
    replay = _reserve(
        cfg,
        info,
        transport,
        digest,
        allocation_id="replayed",
        keys=["pool/gpu"],
        rpc_id=rpc_id,
    )
    assert replay == first
    with pytest.raises(RunnerClientError, match="rpc_id reused"):
        _reserve(
            cfg,
            info,
            transport,
            digest,
            allocation_id="changed",
            keys=["pool/gpu"],
            rpc_id=rpc_id,
        )


def test_typed_client_keeps_token_out_of_status_receipts(tmp_path: Path):
    cfg = _config(tmp_path)
    client = TargetResourceClient.connect(cfg, "LOCAL_SIM")
    document, digest = _policy("pool/gpu")
    installed = client.policy_install(
        document, expected_generation=None, expected_digest=None
    )
    assert installed["digest"] == digest
    reservation = client.reserve(
        allocation_id="typed-client",
        operation_id="typed-operation",
        request_sha256=hashlib.sha256(b"typed").hexdigest(),
        resource_keys=["pool/gpu"],
        expected_policy_generation=1,
        expected_policy_digest=digest,
    )
    assert isinstance(reservation, TargetReservation)
    status = client.status(reservation)
    assert status["receipt"]["state"] == "RESERVED"
    assert "token" not in status and "token" not in status["receipt"]


def test_reservation_fault_rolls_back_all_rows(tmp_path: Path, monkeypatch):
    cfg, info, transport, digest = _installed(tmp_path, "pool/gpu", "tcp/8188")
    monkeypatch.setenv(
        "REMRUN_TEST_ONLY_FAULT_POINT", "after_resource_allocation_insert"
    )
    with pytest.raises(RunnerClientError, match="injected test fault"):
        _reserve(
            cfg,
            info,
            transport,
            digest,
            allocation_id="faulted",
            keys=["pool/gpu", "tcp/8188"],
        )
    db = Path(info.probe["runner_root"]) / "runner.sqlite3"
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT count(*) FROM target_resource_allocations"
        ).fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM target_resource_holds").fetchone()[0] == 0


def test_policy_update_refuses_an_active_hold(tmp_path: Path):
    cfg = _config(tmp_path)
    client = TargetResourceClient.connect(cfg, "LOCAL_SIM")
    first, first_digest = _policy("pool/gpu")
    client.policy_install(first, expected_generation=None, expected_digest=None)
    reservation = client.reserve(
        allocation_id="held-policy",
        operation_id="held-policy-operation",
        request_sha256=hashlib.sha256(b"held-policy").hexdigest(),
        resource_keys=["pool/gpu"],
        expected_policy_generation=1,
        expected_policy_digest=first_digest,
    )
    assert isinstance(reservation, TargetReservation)
    second, _second_digest = _policy("pool/gpu", "tcp/8188", generation=2)
    with pytest.raises(Exception, match="holds are active"):
        client.policy_install(
            second, expected_generation=1, expected_digest=first_digest
        )


def test_expiry_is_monotonic_and_cannot_be_renewed(tmp_path: Path, monkeypatch):
    conn, _root, _meta = remote_runner.open_participant_store(str(tmp_path / "state"))
    try:
        policy, digest = _policy("pool/gpu")
        conn.execute("BEGIN IMMEDIATE")
        remote_runner._resource_policy_install(
            conn,
            {
                "expected_generation": None,
                "expected_digest": None,
                "policy_document": policy,
                "supplied_digest": digest,
            },
        )
        reserved = remote_runner._resource_reserve(
            conn,
            {
                "allocation_id": "monotonic-expiry",
                "operation_id": "monotonic-operation",
                "request_sha256": hashlib.sha256(b"monotonic").hexdigest(),
                "resource_keys": ["pool/gpu"],
                "expected_policy_generation": 1,
                "expected_policy_digest": digest,
            },
            "boot-a",
            1_000,
        )
        conn.execute("COMMIT")
        monkeypatch.setattr(remote_runner.time, "time_ns", lambda: -10**12)
        conn.execute("BEGIN IMMEDIATE")
        renewed = remote_runner._resource_renew(
            conn,
            {
                "allocation_id": "monotonic-expiry",
                "fence": reserved["receipt"]["fence"],
                "token": reserved["token"],
                "expected_policy_generation": 1,
                "expected_policy_digest": digest,
            },
            "boot-a",
            1_000 + remote_runner.RESOURCE_RESERVATION_NS,
        )
        conn.execute("COMMIT")
        assert renewed["status"] == "expired"
        assert renewed["receipt"]["state"] == "EXPIRED"
        assert conn.execute("SELECT count(*) FROM target_resource_holds").fetchone()[0] == 0
    finally:
        conn.close()


def test_strict_monotonic_clock_is_boot_relative_and_increasing():
    first = remote_runner._strict_monotonic_ns()
    second = remote_runner._strict_monotonic_ns()
    assert first > 0
    assert second >= first


@pytest.mark.parametrize("field,value", [("fence", 999), ("token", "wrong")])
def test_stale_fence_or_token_cannot_cancel(tmp_path: Path, field: str, value):
    cfg, info, transport, digest = _installed(tmp_path, "pool/gpu")
    reserved = _reserve(
        cfg,
        info,
        transport,
        digest,
        allocation_id="stale-auth",
        keys=["pool/gpu"],
    )
    body = {
        "allocation_id": "stale-auth",
        "fence": reserved["receipt"]["fence"],
        "token": reserved["token"],
    }
    body[field] = value
    with pytest.raises(RunnerClientError, match=f"resource {field} mismatch"):
        runner_rpc(
            transport,
            info.installed_path,
            cfg.devices["LOCAL_SIM"].state_root,
            "target_resource_cancel",
            body,
        )
    db = Path(info.probe["runner_root"]) / "runner.sqlite3"
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT state FROM target_resource_allocations WHERE allocation_id='stale-auth'"
        ).fetchone()[0] == "RESERVED"
        assert conn.execute("SELECT count(*) FROM target_resource_holds").fetchone()[0] == 1


def _client_reservation(tmp_path: Path, allocation_id: str = "owner-run"):
    cfg = _config(tmp_path)
    client = TargetResourceClient.connect(cfg, "LOCAL_SIM")
    document, digest = _policy("pool/gpu")
    client.policy_install(document, expected_generation=None, expected_digest=None)
    reservation = client.reserve(
        allocation_id=allocation_id,
        operation_id=f"operation-{allocation_id}",
        request_sha256=hashlib.sha256(allocation_id.encode()).hexdigest(),
        resource_keys=["pool/gpu"],
        expected_policy_generation=1,
        expected_policy_digest=digest,
    )
    assert isinstance(reservation, TargetReservation)
    return client, reservation, digest


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner gate")
def test_posix_owner_claims_before_exec_and_target_releases(tmp_path: Path):
    client, reservation, _digest = _client_reservation(tmp_path)
    sentinel = tmp_path / "sentinel.txt"
    result = client.owner_run(
        reservation,
        [sys.executable, "-c", f"from pathlib import Path; Path({str(sentinel)!r}).write_text('ran')"],
    )

    assert result["exit_code"] == 0
    assert result["receipt"]["state"] == "RELEASED"
    assert result["receipt"]["command_start_state"] == "YES"
    assert result["claim_receipt"]["state"] == "CLAIMED"
    assert result["claim_receipt"]["command_start_state"] == "NO"
    assert result["receipt"]["owner"]["root_pid"] > 0
    assert result["receipt"]["owner"]["root_start_id"]
    assert result["receipt"]["owner"]["user_pid"] > 0
    assert result["receipt"]["owner"]["user_start_id"]
    assert sentinel.read_text() == "ran"
    status = client.status(reservation)
    assert status["receipt"]["state"] == "RELEASED"


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner gate")
def test_claim_receipt_precedes_gate_and_detached_owner_survives_source_loss(
    tmp_path: Path,
):
    client, reservation, _digest = _client_reservation(tmp_path, "source-loss")
    marker = tmp_path / "source-loss.txt"
    handle = client.owner_start(
        reservation,
        [
            sys.executable,
            "-c",
            "from pathlib import Path; import sys,time; "
            "Path(sys.argv[1]).write_text('started'); time.sleep(1.0)",
            str(marker),
        ],
    )
    assert handle.claim_receipt["state"] == "CLAIMED"
    assert handle.claim_receipt["command_start_state"] == "NO"

    handle.process.kill()
    handle.process.wait(timeout=10)
    deadline = time.monotonic() + 10
    observed = None
    while time.monotonic() < deadline:
        observed = client.status(reservation)["receipt"]
        if observed["state"] == "RELEASED":
            break
        time.sleep(0.1)
    assert marker.read_text() == "started"
    assert observed is not None
    assert observed["state"] == "RELEASED"
    assert observed["command_start_state"] == "YES"


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner gate")
def test_owner_timeout_is_bounded_but_detached_target_owner_finishes(tmp_path: Path):
    client, reservation, _digest = _client_reservation(tmp_path, "owner-timeout")
    marker = tmp_path / "owner-timeout.txt"
    with pytest.raises(subprocess.TimeoutExpired):
        client.owner_run(
            reservation,
            [
                sys.executable,
                "-c",
                "from pathlib import Path; import sys,time; time.sleep(0.5); "
                "Path(sys.argv[1]).write_text('finished')",
                str(marker),
            ],
            timeout=0.2,
        )
    deadline = time.monotonic() + 10
    observed = None
    while time.monotonic() < deadline:
        observed = client.status(reservation)["receipt"]
        if observed["state"] == "RELEASED":
            break
        time.sleep(0.1)
    assert marker.read_text() == "finished"
    assert observed is not None and observed["state"] == "RELEASED"


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner gate")
def test_two_clients_open_exactly_one_target_gate(tmp_path: Path):
    cfg = _config(tmp_path)
    first = TargetResourceClient.connect(cfg, "LOCAL_SIM")
    second = TargetResourceClient.connect(cfg, "LOCAL_SIM")
    document, digest = _policy("pool/gpu")
    first.policy_install(document, expected_generation=None, expected_digest=None)
    marker = tmp_path / "markers"

    def contender(client: TargetResourceClient, index: int):
        reservation = client.reserve(
            allocation_id=f"gate-race-{index}",
            operation_id=f"gate-race-operation-{index}",
            request_sha256=hashlib.sha256(f"gate-race-{index}".encode()).hexdigest(),
            resource_keys=["pool/gpu"],
            expected_policy_generation=1,
            expected_policy_digest=digest,
        )
        if not isinstance(reservation, TargetReservation):
            return reservation["status"]
        result = client.owner_run(
            reservation,
            [
                sys.executable,
                "-c",
                "from pathlib import Path; import sys, time; "
                "p=Path(sys.argv[1]); p.mkdir(exist_ok=True); "
                "(p / sys.argv[2]).write_text('opened'); time.sleep(0.3)",
                str(marker),
                str(index),
            ],
        )
        return result["receipt"]["state"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda args: contender(*args), [(first, 1), (second, 2)]))

    assert sorted(outcomes) == ["RELEASED", "resource_busy"]
    assert len(list(marker.iterdir())) == 1


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner gate")
def test_fault_during_gate_release_is_maybe_and_quarantined(tmp_path: Path, monkeypatch):
    client, reservation, _digest = _client_reservation(tmp_path)
    sentinel = tmp_path / "must-not-run.txt"
    monkeypatch.setenv("REMRUN_TEST_ONLY_FAULT_POINT", "during_resource_gate_release")
    result = client.owner_run(
        reservation,
        [sys.executable, "-c", f"from pathlib import Path; Path({str(sentinel)!r}).write_text('bad')"],
    )

    assert not sentinel.exists()
    assert result["receipt"]["state"] == "QUARANTINED"
    assert result["receipt"]["command_start_state"] == "MAYBE"
    status = client.status(reservation)
    assert status["receipt"]["state"] == "RELEASED"


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner gate")
def test_exec_failure_is_proved_not_started_and_releases(tmp_path: Path):
    client, reservation, _digest = _client_reservation(tmp_path)
    result = client.owner_run(reservation, [str(tmp_path / "missing-executable")])

    assert result["exit_code"] == 127
    assert result["receipt"]["state"] == "RELEASED"
    assert result["receipt"]["command_start_state"] == "NO"
    assert result["exec_confirmed"] is False


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner gate")
def test_child_death_after_gate_is_not_exec_confirmation(tmp_path: Path, monkeypatch):
    client, reservation, _digest = _client_reservation(tmp_path, "exec-proof-eof")
    state_root = str(Path(client.info.probe["runner_root"]).parents[1])
    request = {
        "reservation": {
            "allocation_id": reservation.allocation_id,
            "fence": reservation.fence,
            "token": reservation.token,
            "policy_generation": reservation.receipt["policy_generation"],
            "policy_digest": reservation.receipt["policy_digest"],
        },
        "argv": [sys.executable, "-c", "raise SystemExit(0)"],
        "cwd": None,
        "env": {},
    }
    monkeypatch.setenv(
        "REMRUN_TEST_ONLY_FAULT_POINT", "after_posix_gate_before_exec"
    )

    result = remote_runner._run_posix_resource_owner(state_root, request)

    assert result["exit_code"] == 126
    assert result["exec_confirmed"] is False
    assert result["receipt"]["command_start_state"] != "YES"


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner gate")
def test_unproved_cleanup_quarantines_and_keeps_the_hold(tmp_path: Path, monkeypatch):
    client, reservation, digest = _client_reservation(tmp_path)
    monkeypatch.setenv("REMRUN_TEST_ONLY_FAULT_POINT", "resource_cleanup_unknown")
    result = client.owner_run(reservation, [sys.executable, "-c", "raise SystemExit(0)"])
    assert result["receipt"]["state"] == "QUARANTINED"

    blocked = client.reserve(
        allocation_id="after-quarantine",
        operation_id="operation-after-quarantine",
        request_sha256=hashlib.sha256(b"after-quarantine").hexdigest(),
        resource_keys=["pool/gpu"],
        expected_policy_generation=1,
        expected_policy_digest=digest,
    )
    assert isinstance(blocked, dict)
    assert blocked["status"] == "resource_busy"

    monkeypatch.delenv("REMRUN_TEST_ONLY_FAULT_POINT")
    status = client.status(reservation)
    assert status["receipt"]["state"] == "RELEASED"
    replacement = client.reserve(
        allocation_id="after-positive-reconciliation",
        operation_id="operation-after-positive-reconciliation",
        request_sha256=hashlib.sha256(b"after-positive-reconciliation").hexdigest(),
        resource_keys=["pool/gpu"],
        expected_policy_generation=1,
        expected_policy_digest=digest,
    )
    assert isinstance(replacement, TargetReservation)


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner gate")
def test_cancelled_stale_owner_cannot_open_newer_allocation(tmp_path: Path):
    client, old, digest = _client_reservation(tmp_path, "old-owner")
    client.cancel(old)
    newer = client.reserve(
        allocation_id="new-owner",
        operation_id="operation-new-owner",
        request_sha256=hashlib.sha256(b"new-owner").hexdigest(),
        resource_keys=["pool/gpu"],
        expected_policy_generation=1,
        expected_policy_digest=digest,
    )
    assert isinstance(newer, TargetReservation)
    with pytest.raises(Exception, match="reserved allocation"):
        client.owner_run(old, [sys.executable, "-c", "raise SystemExit(0)"])
    result = client.owner_run(newer, [sys.executable, "-c", "raise SystemExit(0)"])
    assert result["receipt"]["state"] == "RELEASED"


@pytest.mark.parametrize(
    ("operation", "values"),
    [
        ("start", {"state": "MAYBE"}),
        ("release", {"reason": "stale_owner_probe"}),
        ("quarantine", {"reason": "stale_owner_probe"}),
    ],
)
def test_stale_internal_owner_mutation_cannot_touch_newer_allocation(
    tmp_path: Path, operation: str, values: dict[str, str]
):
    client, old, digest = _client_reservation(tmp_path, f"stale-{operation}")
    client.cancel(old)
    newer = client.reserve(
        allocation_id=f"newer-{operation}",
        operation_id=f"operation-newer-{operation}",
        request_sha256=hashlib.sha256(f"newer-{operation}".encode()).hexdigest(),
        resource_keys=["pool/gpu"],
        expected_policy_generation=1,
        expected_policy_digest=digest,
    )
    assert isinstance(newer, TargetReservation)
    state_root = str(Path(client.info.probe["runner_root"]).parents[1])
    body = {
        "allocation_id": old.allocation_id,
        "fence": old.fence,
        "token": old.token,
        "policy_generation": 1,
        "policy_digest": digest,
    }

    with pytest.raises(remote_runner.RunnerError):
        remote_runner._resource_owner_mutation(
            state_root, operation, body, **values
        )

    assert client.status(newer)["receipt"]["state"] == "RESERVED"
    client.cancel(newer)


def test_fence_increases_after_terminal_release_and_reallocation(tmp_path: Path):
    client, first, digest = _client_reservation(tmp_path, "first-fence")
    client.cancel(first)
    second = client.reserve(
        allocation_id="second-fence",
        operation_id="operation-second-fence",
        request_sha256=hashlib.sha256(b"second-fence").hexdigest(),
        resource_keys=["pool/gpu"],
        expected_policy_generation=1,
        expected_policy_digest=digest,
    )
    assert isinstance(second, TargetReservation)
    assert second.fence > first.fence


def test_allocation_id_reuse_with_changed_identity_is_a_conflict(tmp_path: Path):
    cfg, info, transport, digest = _installed(tmp_path, "pool/gpu")
    first = _reserve(
        cfg, info, transport, digest, allocation_id="immutable", keys=["pool/gpu"]
    )
    assert first["status"] == "reserved"
    with pytest.raises(RunnerClientError, match="different immutable fields"):
        runner_rpc(
            transport,
            info.installed_path,
            cfg.devices["LOCAL_SIM"].state_root,
            "target_resource_reserve",
            {
                "allocation_id": "immutable",
                "operation_id": "changed-operation",
                "request_sha256": hashlib.sha256(b"immutable").hexdigest(),
                "resource_keys": ["pool/gpu"],
                "expected_policy_generation": 1,
                "expected_policy_digest": digest,
            },
        )


@pytest.mark.parametrize("state", ["RESERVED", "CLAIMED", "QUARANTINED"])
def test_verified_boot_change_terminalizes_active_allocation_and_frees_hold(
    tmp_path: Path, state: str
):
    client, reservation, _digest = _client_reservation(tmp_path, f"reboot-{state.lower()}")
    db = Path(client.info.probe["runner_root"]) / "runner.sqlite3"
    with sqlite3.connect(db) as conn:
        if state != "RESERVED":
            conn.execute(
                "UPDATE target_resource_allocations SET state=?,claim_boot_id=?,"
                "owner_kind='posix_pgid_v1',owner_key='999999',owner_pid=999999,"
                "owner_start_id='test',root_pid=999999,root_start_id='test' "
                "WHERE allocation_id=?",
                (state, reservation.receipt["target_boot_id"], reservation.allocation_id),
            )
            conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        remote_runner._reconcile_resources(
            conn, "different-verified-boot", reservation.receipt["reservation_expires_mono_ns"] - 1
        )
        conn.execute("COMMIT")
        row = conn.execute(
            "SELECT state,terminal_reason FROM target_resource_allocations "
            "WHERE allocation_id=?",
            (reservation.allocation_id,),
        ).fetchone()
        assert row == ("REBOOTED", "target_rebooted")
        assert conn.execute("SELECT count(*) FROM target_resource_holds").fetchone()[0] == 0


def test_missing_strict_boot_identity_refuses_owner_claim(tmp_path: Path, monkeypatch):
    _client, reservation, _digest = _client_reservation(tmp_path, "missing-boot")
    monkeypatch.setattr(
        remote_runner,
        "_strict_boot_id",
        lambda: (_ for _ in ()).throw(remote_runner.RunnerError("strict boot unavailable")),
    )
    owner = {
        "kind": "posix_pgid_v1",
        "key": "999999",
        "pid": 999999,
        "start_id": "test",
        "root_pid": 999999,
        "root_start_id": "test",
    }
    with pytest.raises(remote_runner.RunnerError, match="strict boot unavailable"):
        remote_runner._resource_owner_mutation(
            str(Path(_client.info.probe["runner_root"]).parents[1]),
            "claim",
            {
                "allocation_id": reservation.allocation_id,
                "fence": reservation.fence,
                "token": reservation.token,
                "policy_generation": 1,
                "policy_digest": reservation.receipt["policy_digest"],
            },
            owner=owner,
        )


@pytest.mark.parametrize("error_number", [errno.EACCES, errno.EPERM, errno.EIO])
def test_linux_proc_inspection_error_is_unknown(monkeypatch, error_number: int):
    monkeypatch.setattr(remote_runner.platform, "system", lambda: "Linux")
    monkeypatch.setattr(remote_runner.os, "listdir", lambda _path: ["4242"])
    real_open = builtins.open

    def unreadable_stat(path, *args, **kwargs):
        if str(path) == "/proc/4242/stat":
            raise OSError(error_number, "injected unreadable proc stat")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", unreadable_stat)
    assert remote_runner._posix_group_members(777) is None


@pytest.mark.parametrize("error_number", [errno.ENOENT, errno.ESRCH])
def test_linux_proc_disappearance_race_is_ignored(monkeypatch, error_number: int):
    monkeypatch.setattr(remote_runner.platform, "system", lambda: "Linux")
    monkeypatch.setattr(remote_runner.os, "listdir", lambda _path: ["4242"])

    def vanished_stat(_path, *args, **kwargs):
        raise OSError(error_number, "injected vanished proc stat")

    monkeypatch.setattr(builtins, "open", vanished_stat)
    assert remote_runner._posix_group_members(777) == []


@pytest.mark.parametrize(
    "stat_text",
    [
        "",
        "4242 missing-parentheses",
        "9999 (worker) S 1 777 " + "0 " * 16 + "12345",
        "4242 (worker) S 1 777",
        "4242 (worker) S 1 not-a-group " + "0 " * 16 + "12345",
        "4242 (worker) S 1 777 " + "0 " * 16 + "not-a-start-time",
    ],
)
def test_linux_proc_malformed_stat_is_unknown(monkeypatch, stat_text: str):
    monkeypatch.setattr(remote_runner.platform, "system", lambda: "Linux")
    monkeypatch.setattr(remote_runner.os, "listdir", lambda _path: ["4242"])
    real_open = builtins.open

    def malformed_stat(path, *args, **kwargs):
        if str(path) == "/proc/4242/stat":
            from io import StringIO

            return StringIO(stat_text)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", malformed_stat)
    assert remote_runner._posix_group_members(777) is None


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"\x00\x00",
        struct.pack("!I", 1) + b"x",
        struct.pack("!I", 4097),
        struct.pack("!I", 10) + b"{}",
        struct.pack("!I", 2) + b"\xff\xff",
        struct.pack("!I", 2) + b"xx",
        struct.pack("!I", 2) + b"[]",
    ],
)
def test_posix_exec_record_rejects_eof_truncation_and_malformed_data(raw: bytes):
    read_fd, write_fd = os.pipe()
    try:
        if raw:
            os.write(write_fd, raw)
        os.close(write_fd)
        write_fd = -1
        assert remote_runner._read_posix_exec_record(read_fd, timeout=0.1) is None
    finally:
        os.close(read_fd)
        if write_fd >= 0:
            os.close(write_fd)


def test_posix_exec_record_accepts_one_bounded_object():
    payload = b'{"kind":"EXEC_CONFIRMED","user_pid":42,"user_start_id":"exact"}'
    read_fd, write_fd = os.pipe()
    try:
        os.write(write_fd, struct.pack("!I", len(payload)) + payload)
        os.close(write_fd)
        write_fd = -1
        assert remote_runner._read_posix_exec_record(read_fd, timeout=0.1) == {
            "kind": "EXEC_CONFIRMED",
            "user_pid": 42,
            "user_start_id": "exact",
        }
    finally:
        os.close(read_fd)
        if write_fd >= 0:
            os.close(write_fd)


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner gate")
def test_fault_before_claim_never_opens_gate(tmp_path: Path, monkeypatch):
    client, reservation, _digest = _client_reservation(tmp_path, "before-claim")
    sentinel = tmp_path / "before-claim.txt"
    monkeypatch.setenv("REMRUN_TEST_ONLY_FAULT_POINT", "before_resource_claim")
    with pytest.raises(Exception, match="before_resource_claim"):
        client.owner_run(
            reservation,
            [sys.executable, "-c", f"open({str(sentinel)!r}, 'w').write('bad')"],
        )
    assert not sentinel.exists()
    monkeypatch.delenv("REMRUN_TEST_ONLY_FAULT_POINT")
    assert client.status(reservation)["receipt"]["state"] == "RESERVED"


@pytest.mark.skipif(os.name != "posix", reason="POSIX owner gate")
def test_fault_after_claim_never_opens_gate_and_is_quarantined(
    tmp_path: Path, monkeypatch
):
    client, reservation, _digest = _client_reservation(tmp_path, "after-claim")
    sentinel = tmp_path / "after-claim.txt"
    monkeypatch.setenv("REMRUN_TEST_ONLY_FAULT_POINT", "after_resource_claim")
    with pytest.raises(Exception, match="after_resource_claim"):
        client.owner_run(
            reservation,
            [sys.executable, "-c", f"open({str(sentinel)!r}, 'w').write('bad')"],
        )
    assert not sentinel.exists()
    db = Path(client.info.probe["runner_root"]) / "runner.sqlite3"
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT state,command_start_state FROM target_resource_allocations "
            "WHERE allocation_id=?",
            (reservation.allocation_id,),
        ).fetchone() == ("QUARANTINED", "NO")
