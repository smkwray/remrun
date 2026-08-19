from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sqlite3
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from remrun.config import RemrunConfig
from remrun.coordination import (
    create_acquire_intent,
    load_acquire_intent,
    project_key,
    stable_identity,
)
from remrun.models import Device
from remrun.remote import runner as remote_runner
from remrun.runner_client import (
    RunnerClientError,
    enroll_target_key,
    ensure_versioned_runner,
    runner_rpc,
)
from remrun.transport import make_transport


def _config(tmp_path: Path) -> RemrunConfig:
    device = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim",
        "os": "posix",
        "project_root": str(tmp_path / "remote-projects"),
        "state_root": str(tmp_path / "remote-state"),
        "cache_root": str(tmp_path / "remote-cache"),
    })
    return RemrunConfig(
        repo_root=tmp_path / "tool",
        defaults={},
        devices={"LOCAL_SIM": device},
        project_roots={"default": str(tmp_path / "projects")},
    )


def _authority(tmp_path: Path):
    cfg = _config(tmp_path)
    info = ensure_versioned_runner(cfg, "LOCAL_SIM", install=True)
    transport = make_transport(cfg.devices["LOCAL_SIM"])
    state_root = cfg.devices["LOCAL_SIM"].state_root
    cluster = str(uuid.uuid4())
    project = project_key(cluster, "example/project")
    policy = "a" * 64
    runner_rpc(transport, info.installed_path, state_root, "authority_init", {
        "cluster_id": cluster, "lease_seconds": 30,
    })
    runner_rpc(transport, info.installed_path, state_root, "authority_project_register", {
        "cluster_id": cluster, "project_key": project,
        "project_id": "example/project", "policy_sha256": policy,
    })
    return cfg, info, cluster, project, policy


def _acquire(cfg, info, cluster, project, policy, controller_root: Path):
    replica = stable_identity(controller_root, "replica")
    intent, path = create_acquire_intent(controller_root, cluster, project, replica)
    assert load_acquire_intent(path) == intent
    body = {**intent.as_dict(), "policy_sha256": policy}
    transport = make_transport(cfg.devices["LOCAL_SIM"])
    response = runner_rpc(
        transport, info.installed_path, cfg.devices["LOCAL_SIM"].state_root,
        "authority_acquire", body,
    )
    return response, body


def test_first_use_identity_race_returns_one_winner(tmp_path: Path):
    state_root = tmp_path / "controller-state"
    with ThreadPoolExecutor(max_workers=8) as pool:
        identities = list(pool.map(
            lambda _index: stable_identity(state_root, "controller"), range(24)
        ))
    assert len(set(identities)) == 1


def test_two_controller_race_has_exactly_one_lease(tmp_path: Path):
    cfg, info, cluster, project, policy = _authority(tmp_path)

    def acquire(name: str):
        return _acquire(cfg, info, cluster, project, policy, tmp_path / name)[0]

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(acquire, ("controller-a", "controller-b")))

    assert sorted(response["status"] for response in responses) == ["ACQUIRED", "BUSY"]
    winner = next(response for response in responses if response["status"] == "ACQUIRED")
    assert winner["lease"]["fence"] == 1


def test_dropped_acquire_response_retries_idempotently(tmp_path: Path):
    cfg, info, cluster, project, policy = _authority(tmp_path)
    first, body = _acquire(
        cfg, info, cluster, project, policy, tmp_path / "controller-a"
    )
    transport = make_transport(cfg.devices["LOCAL_SIM"])

    retried = runner_rpc(
        transport, info.installed_path, cfg.devices["LOCAL_SIM"].state_root,
        "authority_acquire", body, rpc_id="retry-after-drop",
    )

    assert retried["status"] == "ACQUIRED"
    assert retried["idempotent"] is True
    assert retried["lease"]["lease_id"] == first["lease"]["lease_id"]

    changed = dict(body)
    changed["controller_id"] = str(uuid.uuid4())
    with pytest.raises(RunnerClientError, match="different request"):
        runner_rpc(
            transport, info.installed_path, cfg.devices["LOCAL_SIM"].state_root,
            "authority_acquire", changed, rpc_id="changed-retry",
        )


def test_authority_commit_and_response_replay_are_atomic(tmp_path: Path, monkeypatch):
    cfg, info, cluster, project, policy = _authority(tmp_path)
    controller_root = tmp_path / "controller-a"
    replica = stable_identity(controller_root, "replica")
    intent, _path = create_acquire_intent(controller_root, cluster, project, replica)
    body = {**intent.as_dict(), "policy_sha256": policy}
    transport = make_transport(cfg.devices["LOCAL_SIM"])
    state_root = cfg.devices["LOCAL_SIM"].state_root
    authority_db = Path(state_root) / "coord" / "v1" / "authority.sqlite3"
    participant_db = Path(state_root) / "runner" / "v1" / "runner.sqlite3"

    def crash_then_retry(operation: str, request: dict, rpc_id: str):
        monkeypatch.setenv("REMRUN_TEST_ONLY_FAULT_POINT", "after_authority_commit")
        with pytest.raises(RunnerClientError, match="injected test fault"):
            runner_rpc(
                transport, info.installed_path, state_root,
                operation, request, rpc_id=rpc_id,
            )
        monkeypatch.delenv("REMRUN_TEST_ONLY_FAULT_POINT")
        with sqlite3.connect(participant_db) as conn:
            assert conn.execute(
                "SELECT count(*) FROM rpc_requests WHERE rpc_id=?", (rpc_id,)
            ).fetchone()[0] == 0
        return runner_rpc(
            transport, info.installed_path, state_root,
            operation, request, rpc_id=rpc_id,
        )

    target = info.probe["device_id"]
    prepared = crash_then_retry(
        "authority_target_key_create",
        {"cluster_id": cluster, "target_device_id": target},
        "crash-key-create",
    )
    assert prepared["status"] == "PENDING"
    enrolled = enroll_target_key(cfg, "LOCAL_SIM", "LOCAL_SIM", cluster)
    assert enrolled["finalized"]["status"] == "ENROLLED"
    rpc_id = "crash-after-authority-commit"
    replayed = crash_then_retry("authority_acquire", body, rpc_id)
    assert replayed["status"] == "ACQUIRED"
    assert replayed["idempotent"] is False

    with sqlite3.connect(authority_db) as conn:
        assert conn.execute("SELECT count(*) FROM leases").fetchone()[0] == 1
        assert conn.execute(
            "SELECT count(*) FROM authority_rpc_requests WHERE rpc_id=?", (rpc_id,)
        ).fetchone()[0] == 1
    credentials = {
        "cluster_id": cluster, "project_key": project,
        "lease_id": replayed["lease"]["lease_id"],
        "fence": replayed["lease"]["fence"], "owner_token": body["owner_token"],
    }
    heartbeat = crash_then_retry(
        "authority_heartbeat", credentials, "crash-heartbeat"
    )
    assert heartbeat["status"] == "OWNED"
    released = crash_then_retry("authority_release", credentials, "crash-release")
    assert released["status"] == "RELEASED"

    next_intent, _path = create_acquire_intent(
        controller_root, cluster, project, replica
    )
    next_body = {**next_intent.as_dict(), "policy_sha256": policy}
    next_lease = runner_rpc(
        transport, info.installed_path, state_root,
        "authority_acquire", next_body, rpc_id="next-acquire",
    )
    grant = crash_then_retry("authority_grant_issue", {
        "cluster_id": cluster, "project_key": project,
        "lease_id": next_lease["lease"]["lease_id"],
        "fence": next_lease["lease"]["fence"],
        "owner_token": next_body["owner_token"], "grant_request_id": "crash-grant",
        "target_device_id": target, "grant_operation": "txn_apply",
        "operation_id": "crash-op", "request_sha256": "9" * 64,
    }, "crash-grant-issue")
    assert grant["status"] == "ISSUED"


def test_expired_owner_two_reclaimers_and_stale_credentials(tmp_path: Path):
    cfg, info, cluster, project, policy = _authority(tmp_path)
    original, original_body = _acquire(
        cfg, info, cluster, project, policy, tmp_path / "controller-a"
    )
    db = Path(cfg.devices["LOCAL_SIM"].state_root) / "coord" / "v1" / "authority.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE leases SET lease_until_ns=0 WHERE lease_id=?",
            (original["lease"]["lease_id"],),
        )

    def reclaim(name: str):
        return _acquire(cfg, info, cluster, project, policy, tmp_path / name)[0]

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(reclaim, ("controller-b", "controller-c")))

    assert sorted(response["status"] for response in responses) == ["ACQUIRED", "BUSY"]
    successor = next(response for response in responses if response["status"] == "ACQUIRED")
    assert successor["lease"]["fence"] == 2
    assert successor["lease"]["recovery_of_lease_id"] == original["lease"]["lease_id"]

    credential = {
        "cluster_id": cluster,
        "project_key": project,
        "lease_id": original["lease"]["lease_id"],
        "fence": original["lease"]["fence"],
        "owner_token": original_body["owner_token"],
    }
    transport = make_transport(cfg.devices["LOCAL_SIM"])
    heartbeat = runner_rpc(
        transport, info.installed_path, cfg.devices["LOCAL_SIM"].state_root,
        "authority_heartbeat", credential,
    )
    release = runner_rpc(
        transport, info.installed_path, cfg.devices["LOCAL_SIM"].state_root,
        "authority_release", credential,
    )
    assert heartbeat["status"] == "LOST"
    assert release["status"] == "LOST"

    retried = runner_rpc(
        transport, info.installed_path, cfg.devices["LOCAL_SIM"].state_root,
        "authority_acquire", original_body, rpc_id="old-owner-retry",
    )
    assert retried["status"] == "SUPERSEDED"


def test_stale_grant_and_successor_barrier_have_only_safe_outcomes(tmp_path: Path):
    cfg, info, cluster, project, policy = _authority(tmp_path)
    first, first_body = _acquire(
        cfg, info, cluster, project, policy, tmp_path / "controller-a"
    )
    state_root = cfg.devices["LOCAL_SIM"].state_root
    transport = make_transport(cfg.devices["LOCAL_SIM"])
    target = info.probe["device_id"]
    enrolled = enroll_target_key(cfg, "LOCAL_SIM", "LOCAL_SIM", cluster)
    assert enrolled["finalized"]["status"] == "ENROLLED"

    def issue(lease, owner_body, request_id: str, operation: str, operation_id: str):
        return runner_rpc(
            make_transport(cfg.devices["LOCAL_SIM"]), info.installed_path, state_root,
            "authority_grant_issue", {
                "cluster_id": cluster, "project_key": project,
                "lease_id": lease["lease_id"], "fence": lease["fence"],
                "owner_token": owner_body["owner_token"],
                "grant_request_id": request_id, "target_device_id": target,
                "grant_operation": operation, "operation_id": operation_id,
                "request_sha256": "b" * 64,
            },
        )["capability"]

    stale_capability = issue(
        first["lease"], first_body, "stale-request", "txn_apply", "txn-old"
    )

    collision = {
        "cluster_id": cluster, "project_key": project,
        "lease_id": first["lease"]["lease_id"], "fence": first["lease"]["fence"],
        "owner_token": first_body["owner_token"], "grant_request_id": "stale-request",
        "target_device_id": target, "grant_operation": "fence_barrier",
        "operation_id": "different", "request_sha256": "b" * 64,
    }
    with pytest.raises(RunnerClientError, match="different request"):
        runner_rpc(
            transport, info.installed_path, state_root,
            "authority_grant_issue", collision, rpc_id="grant-collision",
        )
    db = Path(state_root) / "coord" / "v1" / "authority.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE leases SET lease_until_ns=0 WHERE lease_id=?",
            (first["lease"]["lease_id"],),
        )
    successor, successor_body = _acquire(
        cfg, info, cluster, project, policy, tmp_path / "controller-b"
    )
    assert successor["lease"]["phase"] == "RECOVERY"
    blocked = runner_rpc(
        transport, info.installed_path, state_root, "authority_grant_issue", {
            "cluster_id": cluster, "project_key": project,
            "lease_id": successor["lease"]["lease_id"],
            "fence": successor["lease"]["fence"],
            "owner_token": successor_body["owner_token"],
            "grant_request_id": "blocked-normal", "target_device_id": target,
            "grant_operation": "txn_apply", "operation_id": "new-txn",
            "request_sha256": "c" * 64,
        },
    )
    assert blocked["status"] == "RECOVERY_REQUIRED"
    release = runner_rpc(
        transport, info.installed_path, state_root, "authority_release", {
            "cluster_id": cluster, "project_key": project,
            "lease_id": successor["lease"]["lease_id"],
            "fence": successor["lease"]["fence"],
            "owner_token": successor_body["owner_token"],
        },
    )
    assert release["status"] == "RECOVERY_REQUIRED"
    barrier_capability = issue(
        successor["lease"], successor_body, "barrier-request",
        "fence_barrier", "barrier-2",
    )

    def accept(item):
        name, capability = item
        response = runner_rpc(
            make_transport(cfg.devices["LOCAL_SIM"]), info.installed_path, state_root,
            "participant_grant_accept", {"capability": capability}, rpc_id=f"accept-{name}",
        )
        return name, response

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = dict(pool.map(accept, (
            ("stale", stale_capability), ("barrier", barrier_capability),
        )))

    assert outcomes["barrier"]["status"] == "BARRIER"
    if outcomes["stale"]["status"] == "FENCED":
        assert outcomes["barrier"]["result"]["lower_fence_operations"] == []
    else:
        assert outcomes["stale"]["status"] == "ACCEPTED"
        recovered = outcomes["barrier"]["result"]["lower_fence_operations"]
        assert [row["grant_id"] for row in recovered] == [
            stale_capability["body"]["grant_id"]
        ]


def test_recovery_imports_signed_absence_then_barrier_fences_old_grant(tmp_path: Path):
    cfg, info, cluster, project, policy = _authority(tmp_path)
    first, first_body = _acquire(
        cfg, info, cluster, project, policy, tmp_path / "controller-a"
    )
    state_root = cfg.devices["LOCAL_SIM"].state_root
    transport = make_transport(cfg.devices["LOCAL_SIM"])
    target = info.probe["device_id"]
    enroll_target_key(cfg, "LOCAL_SIM", "LOCAL_SIM", cluster)

    def authority(operation: str, body: dict, rpc_id: str | None = None):
        return runner_rpc(
            transport, info.installed_path, state_root, operation, body, rpc_id=rpc_id,
        )

    def credentials(acquired, owner_body):
        return {
            "cluster_id": cluster, "project_key": project,
            "lease_id": acquired["lease"]["lease_id"],
            "fence": acquired["lease"]["fence"],
            "owner_token": owner_body["owner_token"],
        }

    stale = authority("authority_grant_issue", {
        **credentials(first, first_body), "grant_request_id": "old-grant",
        "target_device_id": target, "grant_operation": "txn_apply",
        "operation_id": "txn-old", "request_sha256": "1" * 64,
    })["capability"]
    stale_grant_id = stale["body"]["grant_id"]
    db = Path(state_root) / "coord" / "v1" / "authority.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE leases SET lease_until_ns=0 WHERE lease_id=?",
            (first["lease"]["lease_id"],),
        )
    successor, successor_body = _acquire(
        cfg, info, cluster, project, policy, tmp_path / "controller-b"
    )
    successor_credentials = credentials(successor, successor_body)
    assert successor["lease"]["phase"] == "RECOVERY"
    del stale

    worklist = authority("authority_recovery_worklist", successor_credentials)
    assert worklist["status"] == "RECOVERY"
    assert [row["grant_id"] for row in worklist["grants"]] == [stale_grant_id]
    stale_from_authority = worklist["grants"][0]["capability"]

    # An unreachable target produces no receipt. The authority must stay blocked.
    blocked = authority("authority_recovery_complete", successor_credentials)
    assert blocked["status"] == "RECOVERY_REQUIRED"
    assert blocked["operations"][0]["state"] == "ISSUED"

    absent = authority(
        "participant_grant_status", {"capability": stale_from_authority},
        rpc_id="status-old-absent",
    )
    assert absent["status"] == "ABSENT"
    imported = authority("authority_grant_import", {
        **successor_credentials, "receipt": absent["receipt"],
    })
    assert imported["status"] == "UNKNOWN"
    assert authority(
        "authority_recovery_complete", successor_credentials,
        rpc_id="complete-after-absence",
    )["status"] == "RECOVERY_REQUIRED"
    unrelated_recovery = authority("authority_grant_issue", {
        **successor_credentials, "grant_request_id": "unrelated-recovery",
        "target_device_id": target, "grant_operation": "txn_recover",
        "operation_id": "unrelated-txn", "request_sha256": "6" * 64,
    })
    assert unrelated_recovery["status"] == "RECOVERY_REQUIRED"

    barrier = authority("authority_grant_issue", {
        **successor_credentials, "grant_request_id": "barrier-grant",
        "target_device_id": target, "grant_operation": "fence_barrier",
        "operation_id": "barrier-2", "request_sha256": "2" * 64,
    })["capability"]
    assert authority(
        "participant_grant_accept", {"capability": barrier}, rpc_id="accept-barrier"
    )["status"] == "BARRIER"
    barrier_status = authority(
        "participant_grant_status", {"capability": barrier}, rpc_id="status-barrier"
    )
    assert barrier_status["status"] == "TERMINAL"
    assert barrier_status["receipt"]["body"]["result"] == {
        "lower_fence_operations": []
    }

    tampered = json.loads(json.dumps(barrier_status["receipt"]))
    tampered["body"]["result"]["lower_fence_operations"].append({"grant_id": "fake"})
    with pytest.raises(RunnerClientError, match="signature mismatch"):
        authority("authority_grant_import", {
            **successor_credentials, "receipt": tampered,
        }, rpc_id="import-tampered")

    forged = json.loads(json.dumps(barrier_status["receipt"]))
    forged["body"]["lease_id"] = str(uuid.uuid4())
    target_key = next((Path(info.probe["runner_root"]) / "keys").glob("*.key")).read_bytes()
    forged["sig"] = base64.urlsafe_b64encode(hmac.new(
        target_key, remote_runner.canonical_json(forged["body"]), hashlib.sha256,
    ).digest()).rstrip(b"=").decode("ascii")
    with pytest.raises(RunnerClientError, match="does not match the authority grant"):
        authority("authority_grant_import", {
            **successor_credentials, "receipt": forged,
        }, rpc_id="import-forged-fields")

    barrier_import = authority("authority_grant_import", {
        **successor_credentials, "receipt": barrier_status["receipt"],
    }, rpc_id="import-barrier")
    assert barrier_import["status"] == "TERMINAL"
    repeated = authority("authority_grant_import", {
        **successor_credentials, "receipt": barrier_status["receipt"],
    }, rpc_id="import-barrier-again")
    assert repeated["status"] == "TERMINAL"
    assert repeated["idempotent"] is True
    with sqlite3.connect(db) as conn:
        old_state = conn.execute(
            "SELECT state FROM grants WHERE grant_id=?", (stale_grant_id,)
        ).fetchone()[0]
    assert old_state == "FENCED"

    assert authority(
        "authority_release", successor_credentials, rpc_id="release-before-complete"
    )["status"] == "RECOVERY_REQUIRED"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE leases SET lease_until_ns=0 WHERE lease_id=?",
            (successor["lease"]["lease_id"],),
        )
    third, third_body = _acquire(
        cfg, info, cluster, project, policy, tmp_path / "controller-c"
    )
    assert third["lease"]["phase"] == "RECOVERY"
    third_credentials = credentials(third, third_body)
    still_blocked = authority("authority_grant_issue", {
        **third_credentials, "grant_request_id": "still-blocked",
        "target_device_id": target, "grant_operation": "txn_apply",
        "operation_id": "still-blocked-op", "request_sha256": "7" * 64,
    })
    assert still_blocked["status"] == "RECOVERY_REQUIRED"
    completed = authority(
        "authority_recovery_complete", third_credentials, rpc_id="complete-recovery"
    )
    assert completed["status"] == "NORMAL"
    assert completed["restored_project_state"] == "BOOTSTRAP"
    normal = authority("authority_grant_issue", {
        **third_credentials, "grant_request_id": "new-normal",
        "target_device_id": target, "grant_operation": "txn_apply",
        "operation_id": "txn-new", "request_sha256": "3" * 64,
    })
    assert normal["status"] == "ISSUED"
    assert authority(
        "authority_recovery_worklist", third_credentials,
        rpc_id="worklist-after-completion",
    )["status"] == "NOT_RECOVERING"
    assert authority(
        "authority_recovery_worklist", successor_credentials,
        rpc_id="worklist-stale-owner",
    )["status"] == "LOST"


def test_barrier_preserves_a_lower_grant_proven_accepted(tmp_path: Path):
    cfg, info, cluster, project, policy = _authority(tmp_path)
    first, first_body = _acquire(
        cfg, info, cluster, project, policy, tmp_path / "controller-a"
    )
    state_root = cfg.devices["LOCAL_SIM"].state_root
    transport = make_transport(cfg.devices["LOCAL_SIM"])
    target = info.probe["device_id"]
    enroll_target_key(cfg, "LOCAL_SIM", "LOCAL_SIM", cluster)

    def rpc(operation: str, body: dict, rpc_id: str):
        return runner_rpc(
            transport, info.installed_path, state_root, operation, body, rpc_id=rpc_id,
        )

    old_credentials = {
        "cluster_id": cluster, "project_key": project,
        "lease_id": first["lease"]["lease_id"], "fence": first["lease"]["fence"],
        "owner_token": first_body["owner_token"],
    }
    old = rpc("authority_grant_issue", {
        **old_credentials, "grant_request_id": "accepted-old",
        "target_device_id": target, "grant_operation": "txn_apply",
        "operation_id": "accepted-txn", "request_sha256": "4" * 64,
    }, "issue-accepted-old")["capability"]
    assert rpc(
        "participant_grant_accept", {"capability": old}, "accept-old"
    )["status"] == "ACCEPTED"
    db = Path(state_root) / "coord" / "v1" / "authority.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE leases SET lease_until_ns=0 WHERE lease_id=?",
            (first["lease"]["lease_id"],),
        )
    successor, successor_body = _acquire(
        cfg, info, cluster, project, policy, tmp_path / "controller-b"
    )
    recovery = {
        "cluster_id": cluster, "project_key": project,
        "lease_id": successor["lease"]["lease_id"],
        "fence": successor["lease"]["fence"],
        "owner_token": successor_body["owner_token"],
    }
    old_status = rpc(
        "participant_grant_status", {"capability": old}, "status-accepted-old"
    )
    assert old_status["status"] == "ACCEPTED"
    assert rpc("authority_grant_import", {
        **recovery, "receipt": old_status["receipt"],
    }, "import-accepted-old")["status"] == "ACCEPTED"

    barrier = rpc("authority_grant_issue", {
        **recovery, "grant_request_id": "accepted-barrier",
        "target_device_id": target, "grant_operation": "fence_barrier",
        "operation_id": "barrier-accepted", "request_sha256": "5" * 64,
    }, "issue-accepted-barrier")["capability"]
    rpc("participant_grant_accept", {"capability": barrier}, "accept-accepted-barrier")
    barrier_status = rpc(
        "participant_grant_status", {"capability": barrier}, "status-accepted-barrier"
    )
    assert [item["grant_id"] for item in
            barrier_status["receipt"]["body"]["result"]["lower_fence_operations"]] == [
        old["body"]["grant_id"]
    ]
    target_key = next((Path(info.probe["runner_root"]) / "keys").glob("*.key"))
    target_secret = target_key.read_bytes()
    altered = json.loads(json.dumps(barrier_status["receipt"]))
    altered["body"]["result"]["lower_fence_operations"][0]["request_sha256"] = "0" * 64
    altered["sig"] = base64.urlsafe_b64encode(hmac.new(
        target_secret, remote_runner.canonical_json(altered["body"]), hashlib.sha256,
    ).digest()).rstrip(b"=").decode("ascii")
    with pytest.raises(RunnerClientError, match="observation is inconsistent"):
        rpc("authority_grant_import", {
            **recovery, "receipt": altered,
        }, "import-altered-lower-field")

    omitted = json.loads(json.dumps(barrier_status["receipt"]))
    omitted["body"]["result"]["lower_fence_operations"] = []
    omitted["sig"] = base64.urlsafe_b64encode(hmac.new(
        target_secret, remote_runner.canonical_json(omitted["body"]), hashlib.sha256,
    ).digest()).rstrip(b"=").decode("ascii")
    with pytest.raises(RunnerClientError, match="omitted a grant already known accepted"):
        rpc("authority_grant_import", {
            **recovery, "receipt": omitted,
        }, "import-omitted-lower-grant")

    rpc("authority_grant_import", {
        **recovery, "receipt": barrier_status["receipt"],
    }, "import-accepted-barrier")
    blocked = rpc("authority_recovery_complete", recovery, "complete-still-accepted")
    assert blocked["status"] == "RECOVERY_REQUIRED"
    assert blocked["operations"][0]["state"] == "ACCEPTED"


def test_barrier_reports_terminal_lower_grant_and_authority_preserves_it(tmp_path: Path):
    cfg, info, cluster, project, policy = _authority(tmp_path)
    first, first_body = _acquire(
        cfg, info, cluster, project, policy, tmp_path / "controller-a"
    )
    state_root = cfg.devices["LOCAL_SIM"].state_root
    transport = make_transport(cfg.devices["LOCAL_SIM"])
    target = info.probe["device_id"]
    enroll_target_key(cfg, "LOCAL_SIM", "LOCAL_SIM", cluster)

    def rpc(operation: str, body: dict, rpc_id: str):
        return runner_rpc(
            transport, info.installed_path, state_root, operation, body, rpc_id=rpc_id,
        )

    first_credentials = {
        "cluster_id": cluster, "project_key": project,
        "lease_id": first["lease"]["lease_id"], "fence": first["lease"]["fence"],
        "owner_token": first_body["owner_token"],
    }
    old = rpc("authority_grant_issue", {
        **first_credentials, "grant_request_id": "terminal-old",
        "target_device_id": target, "grant_operation": "txn_apply",
        "operation_id": "terminal-op", "request_sha256": "8" * 64,
    }, "issue-terminal-old")["capability"]
    assert rpc(
        "participant_grant_accept", {"capability": old}, "accept-terminal-old"
    )["status"] == "ACCEPTED"
    terminal_result = {"generation": 9, "manifest_sha256": "d" * 64}
    participant_db = Path(info.probe["runner_root"]) / "runner.sqlite3"
    with sqlite3.connect(participant_db) as conn:
        conn.execute(
            "UPDATE accepted_grants SET state='TERMINAL',result_json=? WHERE grant_id=?",
            (remote_runner.canonical_json(terminal_result), old["body"]["grant_id"]),
        )
    authority_db = Path(state_root) / "coord" / "v1" / "authority.sqlite3"
    with sqlite3.connect(authority_db) as conn:
        conn.execute(
            "UPDATE leases SET lease_until_ns=0 WHERE lease_id=?",
            (first["lease"]["lease_id"],),
        )
    successor, successor_body = _acquire(
        cfg, info, cluster, project, policy, tmp_path / "controller-b"
    )
    recovery = {
        "cluster_id": cluster, "project_key": project,
        "lease_id": successor["lease"]["lease_id"],
        "fence": successor["lease"]["fence"],
        "owner_token": successor_body["owner_token"],
    }
    barrier = rpc("authority_grant_issue", {
        **recovery, "grant_request_id": "terminal-barrier",
        "target_device_id": target, "grant_operation": "fence_barrier",
        "operation_id": "terminal-barrier-op", "request_sha256": "9" * 64,
    }, "issue-terminal-barrier")["capability"]
    accepted = rpc(
        "participant_grant_accept", {"capability": barrier}, "accept-terminal-barrier"
    )
    lower = accepted["result"]["lower_fence_operations"]
    assert lower == [{
        "grant_id": old["body"]["grant_id"], "operation": "txn_apply",
        "operation_id": "terminal-op", "fence": first["lease"]["fence"],
        "request_sha256": "8" * 64, "state": "TERMINAL",
        "result": terminal_result,
    }]
    status = rpc(
        "participant_grant_status", {"capability": barrier}, "status-terminal-barrier"
    )
    imported = rpc("authority_grant_import", {
        **recovery, "receipt": status["receipt"],
    }, "import-terminal-barrier")
    assert imported["status"] == "TERMINAL"
    with sqlite3.connect(authority_db) as conn:
        row = conn.execute(
            "SELECT state,result_json FROM grants WHERE grant_id=?",
            (old["body"]["grant_id"],),
        ).fetchone()
    assert row[0] == "TERMINAL"
    assert json.loads(bytes(row[1]).decode("utf-8")) == terminal_result
    completed = rpc(
        "authority_recovery_complete", recovery, "complete-terminal-recovery"
    )
    assert completed["status"] == "NORMAL"
    new_grant = rpc("authority_grant_issue", {
        **recovery, "grant_request_id": "terminal-followup",
        "target_device_id": target, "grant_operation": "txn_apply",
        "operation_id": "terminal-followup-op", "request_sha256": "a" * 64,
    }, "issue-terminal-followup")["capability"]
    assert rpc(
        "participant_grant_accept", {"capability": new_grant},
        "accept-terminal-followup",
    )["status"] == "ACCEPTED"


def test_old_key_cannot_claim_a_higher_authority_epoch(tmp_path: Path):
    cfg, info, cluster, project, policy = _authority(tmp_path)
    acquired, owner_body = _acquire(
        cfg, info, cluster, project, policy, tmp_path / "controller"
    )
    state_root = cfg.devices["LOCAL_SIM"].state_root
    transport = make_transport(cfg.devices["LOCAL_SIM"])
    target = info.probe["device_id"]
    enroll_target_key(cfg, "LOCAL_SIM", "LOCAL_SIM", cluster)
    capability = runner_rpc(
        transport, info.installed_path, state_root, "authority_grant_issue", {
            "cluster_id": cluster, "project_key": project,
            "lease_id": acquired["lease"]["lease_id"],
            "fence": acquired["lease"]["fence"], "owner_token": owner_body["owner_token"],
            "grant_request_id": "epoch-grant", "target_device_id": target,
            "grant_operation": "txn_apply", "operation_id": "epoch-op",
            "request_sha256": "e" * 64,
        },
    )["capability"]
    old_key = next((Path(info.probe["runner_root"]) / "keys").glob("*.key")).read_bytes()
    forged = json.loads(json.dumps(capability))
    forged["body"]["authority_epoch"] = 999
    forged["sig"] = base64.urlsafe_b64encode(hmac.new(
        old_key, remote_runner.canonical_json(forged["body"]), hashlib.sha256,
    ).digest()).rstrip(b"=").decode("ascii")

    with pytest.raises(RunnerClientError, match="key is not enrolled"):
        runner_rpc(
            transport, info.installed_path, state_root,
            "participant_grant_accept", {"capability": forged},
        )


def test_authority_rejects_old_nonzero_schema(tmp_path: Path):
    cfg, info, cluster, _project, _policy = _authority(tmp_path)
    state_root = cfg.devices["LOCAL_SIM"].state_root
    db = Path(state_root) / "coord" / "v1" / "authority.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute("PRAGMA user_version=1")

    with pytest.raises(RunnerClientError, match="reset the disposable shadow authority state"):
        runner_rpc(
            make_transport(cfg.devices["LOCAL_SIM"]), info.installed_path, state_root,
            "authority_probe", {"cluster_id": cluster},
        )


def test_authority_rejects_complete_prior_v4_metadata(tmp_path: Path):
    cfg, info, cluster, _project, _policy = _authority(tmp_path)
    state_root = cfg.devices["LOCAL_SIM"].state_root
    db = Path(state_root) / "coord" / "v1" / "authority.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE authority_meta SET schema_version=4 WHERE singleton=1")
        conn.execute("PRAGMA user_version=4")

    with pytest.raises(RunnerClientError, match="reset the disposable shadow authority state"):
        runner_rpc(
            make_transport(cfg.devices["LOCAL_SIM"]), info.installed_path, state_root,
            "authority_probe", {"cluster_id": cluster},
        )


def test_authority_v5_persists_step5_records_transactionally(tmp_path: Path):
    cfg, _info, _cluster, project, policy = _authority(tmp_path)
    db = Path(cfg.devices["LOCAL_SIM"].state_root) / "coord" / "v1" / "authority.sqlite3"
    manifest_sha = "b" * 64
    replica_id = str(uuid.uuid4())
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 5
        tables = {
            row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert {"snapshots", "tombstone_events", "replicas"} <= tables

        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO snapshots "
            "(project_key,generation,parent_generation,policy_sha256,manifest_sha256,"
            "manifest_zlib,committed_by_txn_id,committed_at_ns) VALUES (?,?,?,?,?,?,?,?)",
            (project, 0, None, policy, manifest_sha, b"snapshot", "bootstrap-txn", 1),
        )
        conn.execute(
            "INSERT INTO replicas "
            "(project_key,replica_id,replica_kind,endpoint_id,root_fingerprint,"
            "credential_sha256,state,ack_generation,ack_manifest_sha256,last_seen_at_ns) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (project, replica_id, "CONTROLLER", "DEVICE_B", "root", "c" * 64,
             "ACTIVE", 0, manifest_sha, 1),
        )
        conn.execute("ROLLBACK")
        assert conn.execute(
            "SELECT count(*) FROM snapshots WHERE project_key=?", (project,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM replicas WHERE project_key=?", (project,)
        ).fetchone()[0] == 0


def test_authority_rejects_partial_current_schema(tmp_path: Path):
    cfg, info, cluster, _project, _policy = _authority(tmp_path)
    state_root = cfg.devices["LOCAL_SIM"].state_root
    db = Path(state_root) / "coord" / "v1" / "authority.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute("DROP TABLE authority_rpc_requests")

    with pytest.raises(RunnerClientError, match="complete expected definition"):
        runner_rpc(
            make_transport(cfg.devices["LOCAL_SIM"]), info.installed_path, state_root,
            "authority_probe", {"cluster_id": cluster},
        )
