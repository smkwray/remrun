#!/usr/bin/env python3
"""Disposable exact-source Step-4 cross-device proof gate.

The producer creates remote temp state roots, installs the current content-addressed
runner on both devices, exercises relay/recovery invariants, prints one JSON record,
and removes both temp roots. It never enables coordinated execution.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import time
import uuid
from dataclasses import replace
from pathlib import Path

from remrun.config import RemrunConfig, load_config
from remrun.coordination import create_acquire_intent, project_key, stable_identity
from remrun.runner_client import (
    enroll_target_key,
    ensure_versioned_runner,
    runner_rpc,
    runner_source,
)
from remrun.transport import make_transport


_RPC_TRANSPORTS: dict[tuple[int, str], object] = {}


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _rpc(config, info, device: str, operation: str, body: dict) -> dict:
    key = (id(config), device)
    transport = _RPC_TRANSPORTS.get(key)
    if transport is None:
        transport = make_transport(config.devices[device])
        _RPC_TRANSPORTS[key] = transport
    return runner_rpc(
        transport, info.installed_path, config.devices[device].state_root,
        operation, body,
    )


def _acquire(config, info, coordinator: str, cluster: str, project: str,
             policy: str, controller_root: Path) -> tuple[dict, dict]:
    replica = stable_identity(controller_root, "replica")
    intent, _path = create_acquire_intent(controller_root, cluster, project, replica)
    body = {**intent.as_dict(), "policy_sha256": policy}
    return _rpc(config, info, coordinator, "authority_acquire", body), body


def _credentials(cluster: str, project: str, acquired: dict, owner: dict) -> dict:
    lease = acquired["lease"]
    return {
        "cluster_id": cluster, "project_key": project,
        "lease_id": lease["lease_id"], "fence": lease["fence"],
        "owner_token": owner["owner_token"],
    }


def _heartbeat(config, info, coordinator: str, credentials: dict) -> None:
    response = _rpc(config, info, coordinator, "authority_heartbeat", credentials)
    _check(response["status"] == "OWNED", f"recovery lease lost: {response}")


def _issue(config, info, coordinator: str, credentials: dict, target_id: str,
           request_id: str, operation: str, operation_id: str) -> dict:
    response = _rpc(config, info, coordinator, "authority_grant_issue", {
        **credentials, "grant_request_id": request_id,
        "target_device_id": target_id, "grant_operation": operation,
        "operation_id": operation_id,
        "request_sha256": hashlib.sha256(operation_id.encode()).hexdigest(),
    })
    return response


def run_gate(coordinator_name: str, target_name: str, lease_seconds: int) -> dict:
    base = load_config()
    coordinator_transport = make_transport(base.devices[coordinator_name])
    target_transport = make_transport(base.devices[target_name])
    coordinator_temp = coordinator_transport.remote_temp_dir("remrun-step4-gate")
    target_temp = target_transport.remote_temp_dir("remrun-step4-gate")
    devices = dict(base.devices)
    devices[coordinator_name] = replace(
        devices[coordinator_name], state_root=coordinator_temp
    )
    devices[target_name] = replace(devices[target_name], state_root=target_temp)
    config = RemrunConfig(
        repo_root=base.repo_root, defaults=base.defaults, devices=devices,
        project_roots=base.project_roots, sync_roots=base.sync_roots,
        fleet_adapters=base.fleet_adapters, git_sync=base.git_sync,
        coordination=base.coordination,
    )
    _RPC_TRANSPORTS[(id(config), coordinator_name)] = coordinator_transport
    _RPC_TRANSPORTS[(id(config), target_name)] = target_transport
    try:
        coordinator = ensure_versioned_runner(config, coordinator_name, install=True)
        target = ensure_versioned_runner(config, target_name, install=True)
        source_sha = hashlib.sha256(runner_source()).hexdigest()
        _check(coordinator.source_sha256 == source_sha, "coordinator source mismatch")
        _check(target.source_sha256 == source_sha, "target source mismatch")
        cluster = str(uuid.uuid4())
        initialized = _rpc(config, coordinator, coordinator_name, "authority_init", {
            "cluster_id": cluster, "lease_seconds": lease_seconds,
        })
        enrollment = enroll_target_key(
            config, coordinator_name, target_name, cluster
        )
        coordinator_enrollment = enroll_target_key(
            config, coordinator_name, coordinator_name, cluster
        )
        target_id = target.probe["device_id"]
        coordinator_id = coordinator.probe["device_id"]
        _check(enrollment["finalized"]["status"] == "ENROLLED", "relay did not finalize")
        _check(
            coordinator_enrollment["finalized"]["status"] == "ENROLLED",
            "coordinator target enrollment did not finalize",
        )

        policy = hashlib.sha256(b"step4-live-policy").hexdigest()
        project_names = (
            "barrier-first", "accepted-first", "terminal-before-barrier",
            "unreachable-target",
        )
        projects = {name: project_key(cluster, f"live/{name}") for name in project_names}
        first_grant_ids: dict[str, str] = {}
        with tempfile.TemporaryDirectory(prefix="remrun-step4-controller-") as raw_controller:
            controller_root = Path(raw_controller)
            for index, name in enumerate(project_names):
                key = projects[name]
                _rpc(config, coordinator, coordinator_name, "authority_project_register", {
                    "cluster_id": cluster, "project_key": key,
                    "project_id": f"live/{name}", "policy_sha256": policy,
                })
                acquired, owner = _acquire(
                    config, coordinator, coordinator_name, cluster, key, policy,
                    controller_root / f"first-{name}",
                )
                initial_operation = "fence_barrier" \
                    if name == "terminal-before-barrier" else "txn_apply"
                grant = _issue(
                    config, coordinator, coordinator_name,
                    _credentials(cluster, key, acquired, owner), target_id,
                    f"stale-{index}", initial_operation, f"stale-op-{index}",
                )["capability"]
                first_grant_ids[name] = grant["body"]["grant_id"]
                if name != "barrier-first":
                    accepted = _rpc(
                        config, target, target_name, "participant_grant_accept",
                        {"capability": grant},
                    )
                    expected = "BARRIER" if name == "terminal-before-barrier" else "ACCEPTED"
                    _check(accepted["status"] == expected, "old grant was not accepted")
                del grant

            time.sleep(lease_seconds + 0.5)
            outcomes: dict[str, dict] = {}

            name = "barrier-first"
            key = projects[name]
            successor, owner = _acquire(
                config, coordinator, coordinator_name, cluster, key, policy,
                controller_root / "successor-barrier-first",
            )
            credentials = _credentials(cluster, key, successor, owner)
            _check(successor["lease"]["phase"] == "RECOVERY", "successor was not recovery")
            worklist = _rpc(
                config, coordinator, coordinator_name,
                "authority_recovery_worklist", credentials,
            )
            stale_capability = next(
                row["capability"] for row in worklist["grants"]
                if row["grant_id"] == first_grant_ids[name]
            )
            blocked = _rpc(
                config, coordinator, coordinator_name,
                "authority_recovery_complete", credentials,
            )
            _check(
                blocked["status"] == "RECOVERY_REQUIRED",
                f"missing target did not block: {blocked}",
            )
            _heartbeat(config, coordinator, coordinator_name, credentials)
            normal = _issue(
                config, coordinator, coordinator_name, credentials, target_id,
                "blocked-normal", "txn_apply", "blocked-normal-op",
            )
            _check(
                normal["status"] == "RECOVERY_REQUIRED",
                f"normal grant escaped recovery: {normal}",
            )
            unrelated = _issue(
                config, coordinator, coordinator_name, credentials, coordinator_id,
                "blocked-unrelated", "txn_recover", "unrelated-target-op",
            )
            _check(
                unrelated["status"] == "RECOVERY_REQUIRED",
                f"unrelated cross-target recovery escaped: {unrelated}",
            )
            _heartbeat(config, coordinator, coordinator_name, credentials)
            barrier = _issue(
                config, coordinator, coordinator_name, credentials, target_id,
                "barrier-first-grant", "fence_barrier", "barrier-first-op",
            )["capability"]
            barrier_accept = _rpc(
                config, target, target_name, "participant_grant_accept",
                {"capability": barrier},
            )
            stale_accept = _rpc(
                config, target, target_name, "participant_grant_accept",
                {"capability": stale_capability},
            )
            _check(barrier_accept["status"] == "BARRIER", "barrier was not accepted")
            _check(stale_accept["status"] == "FENCED", "stale grant escaped barrier")
            barrier_status = _rpc(
                config, target, target_name, "participant_grant_status",
                {"capability": barrier},
            )
            _heartbeat(config, coordinator, coordinator_name, credentials)
            imported = _rpc(config, coordinator, coordinator_name, "authority_grant_import", {
                **credentials, "receipt": barrier_status["receipt"],
            })
            _heartbeat(config, coordinator, coordinator_name, credentials)
            completed = _rpc(
                config, coordinator, coordinator_name,
                "authority_recovery_complete", credentials,
            )
            _check(imported["status"] == "TERMINAL", "barrier import was not terminal")
            _check(completed["status"] == "NORMAL", "safe recovery did not complete")
            outcomes[name] = {
                "successor_phase": successor["lease"]["phase"],
                "pre_receipt": blocked["status"], "normal_issue": normal["status"],
                "barrier": barrier_accept["status"], "stale": stale_accept["status"],
                "import": imported["status"], "complete": completed["status"],
                "fresh_worklist": worklist["status"],
                "unrelated_cross_target": unrelated["status"],
            }

            name = "accepted-first"
            key = projects[name]
            successor, owner = _acquire(
                config, coordinator, coordinator_name, cluster, key, policy,
                controller_root / "successor-accepted-first",
            )
            credentials = _credentials(cluster, key, successor, owner)
            worklist = _rpc(
                config, coordinator, coordinator_name,
                "authority_recovery_worklist", credentials,
            )
            old_capability = next(
                row["capability"] for row in worklist["grants"]
                if row["grant_id"] == first_grant_ids[name]
            )
            old_status = _rpc(
                config, target, target_name, "participant_grant_status",
                {"capability": old_capability},
            )
            _heartbeat(config, coordinator, coordinator_name, credentials)
            old_import = _rpc(config, coordinator, coordinator_name, "authority_grant_import", {
                **credentials, "receipt": old_status["receipt"],
            })
            _heartbeat(config, coordinator, coordinator_name, credentials)
            barrier = _issue(
                config, coordinator, coordinator_name, credentials, target_id,
                "accepted-first-barrier", "fence_barrier", "accepted-first-barrier-op",
            )["capability"]
            barrier_accept = _rpc(
                config, target, target_name, "participant_grant_accept",
                {"capability": barrier},
            )
            barrier_status = _rpc(
                config, target, target_name, "participant_grant_status",
                {"capability": barrier},
            )
            _heartbeat(config, coordinator, coordinator_name, credentials)
            barrier_import = _rpc(
                config, coordinator, coordinator_name, "authority_grant_import",
                {**credentials, "receipt": barrier_status["receipt"]},
            )
            _heartbeat(config, coordinator, coordinator_name, credentials)
            blocked = _rpc(
                config, coordinator, coordinator_name,
                "authority_recovery_complete", credentials,
            )
            _check(old_import["status"] == "ACCEPTED", "accepted grant was not imported")
            _check(barrier_accept["status"] == "BARRIER", "accepted-order barrier failed")
            _check(barrier_import["status"] == "TERMINAL", "barrier import failed")
            _check(blocked["status"] == "RECOVERY_REQUIRED", "accepted work was discarded")
            outcomes[name] = {
                "old_import": old_import["status"], "barrier": barrier_accept["status"],
                "barrier_import": barrier_import["status"], "complete": blocked["status"],
                "fresh_worklist": worklist["status"],
                "reported_lower": len(
                    barrier_status["receipt"]["body"]["result"]["lower_fence_operations"]
                ),
            }

            name = "terminal-before-barrier"
            key = projects[name]
            successor, owner = _acquire(
                config, coordinator, coordinator_name, cluster, key, policy,
                controller_root / "successor-terminal-before-barrier",
            )
            credentials = _credentials(cluster, key, successor, owner)
            worklist = _rpc(
                config, coordinator, coordinator_name,
                "authority_recovery_worklist", credentials,
            )
            old_capability = next(
                row["capability"] for row in worklist["grants"]
                if row["grant_id"] == first_grant_ids[name]
            )
            del old_capability
            _heartbeat(config, coordinator, coordinator_name, credentials)
            barrier = _issue(
                config, coordinator, coordinator_name, credentials, target_id,
                "terminal-successor-barrier", "fence_barrier",
                "terminal-successor-barrier-op",
            )["capability"]
            barrier_accept = _rpc(
                config, target, target_name, "participant_grant_accept",
                {"capability": barrier},
            )
            lower = barrier_accept["result"]["lower_fence_operations"]
            _check(len(lower) == 1, f"terminal lower grant missing: {lower}")
            _check(lower[0]["state"] == "TERMINAL", f"terminal state changed: {lower}")
            _check(
                lower[0]["grant_id"] == first_grant_ids[name],
                f"wrong terminal lower grant: {lower}",
            )
            barrier_status = _rpc(
                config, target, target_name, "participant_grant_status",
                {"capability": barrier},
            )
            _heartbeat(config, coordinator, coordinator_name, credentials)
            imported = _rpc(
                config, coordinator, coordinator_name, "authority_grant_import",
                {**credentials, "receipt": barrier_status["receipt"]},
            )
            _heartbeat(config, coordinator, coordinator_name, credentials)
            completed = _rpc(
                config, coordinator, coordinator_name,
                "authority_recovery_complete", credentials,
            )
            _check(imported["status"] == "TERMINAL", "terminal barrier import failed")
            _check(completed["status"] == "NORMAL", "terminal recovery did not complete")
            outcomes[name] = {
                "fresh_worklist": worklist["status"],
                "barrier": barrier_accept["status"],
                "reported_lower_state": lower[0]["state"],
                "reported_lower_grant": lower[0]["grant_id"],
                "import": imported["status"], "complete": completed["status"],
            }

            name = "unreachable-target"
            key = projects[name]
            successor, owner = _acquire(
                config, coordinator, coordinator_name, cluster, key, policy,
                controller_root / "successor-unreachable",
            )
            credentials = _credentials(cluster, key, successor, owner)
            worklist = _rpc(
                config, coordinator, coordinator_name,
                "authority_recovery_worklist", credentials,
            )
            old_capability = next(
                row["capability"] for row in worklist["grants"]
                if row["grant_id"] == first_grant_ids[name]
            )
            offline = replace(
                config.devices[target_name], address_candidates=["remrun-unreachable.invalid"],
                tailscale_ip="",
            )
            offline_transport = make_transport(offline)
            unreachable_error = ""
            try:
                runner_rpc(
                    offline_transport, target.installed_path, target_temp,
                    "participant_grant_status", {"capability": old_capability},
                )
            except Exception as exc:  # This is the fault injected by the gate.
                unreachable_error = f"{type(exc).__name__}: {exc}"
            _check(bool(unreachable_error), "unreachable target unexpectedly answered")
            _heartbeat(config, coordinator, coordinator_name, credentials)
            blocked = _rpc(
                config, coordinator, coordinator_name,
                "authority_recovery_complete", credentials,
            )
            released = _rpc(
                config, coordinator, coordinator_name, "authority_release", credentials,
            )
            _check(blocked["status"] == "RECOVERY_REQUIRED", "unreachable recovery completed")
            _check(released["status"] == "RECOVERY_REQUIRED", "unreachable lease released")
            outcomes[name] = {
                "transport_error": unreachable_error,
                "complete": blocked["status"], "release": released["status"],
                "fresh_worklist": worklist["status"],
            }

        return {
            "ok": True, "producer": "scripts/step4_live_gate.py",
            "coordinator": coordinator_name, "target": target_name,
            "cluster_id": cluster, "source_sha256": source_sha,
            "coordinator_runner_sha256": coordinator.source_sha256,
            "target_runner_sha256": target.source_sha256,
            "coordinator_device_id": coordinator.probe["device_id"],
            "target_device_id": target.probe["device_id"],
            "coordinator_schema": coordinator.probe["schema_version"],
            "target_schema": target.probe["schema_version"],
            "authority_schema": initialized["schema_version"],
            "enrollment": {
                "epoch": enrollment["finalized"]["authority_epoch"],
                "status": enrollment["finalized"]["status"],
                "key_sha256": enrollment["finalized"]["key_sha256"],
            },
            "coordinator_target_enrollment": {
                "epoch": coordinator_enrollment["finalized"]["authority_epoch"],
                "status": coordinator_enrollment["finalized"]["status"],
                "key_sha256": coordinator_enrollment["finalized"]["key_sha256"],
            },
            "outcomes": outcomes,
        }
    finally:
        try:
            target_transport.remove_remote_tree(target_temp)
            coordinator_transport.remove_remote_tree(coordinator_temp)
            if target_transport.remote_path_exists(target_temp) \
                    or coordinator_transport.remote_path_exists(coordinator_temp):
                raise RuntimeError("live-gate temporary state cleanup did not complete")
        finally:
            _RPC_TRANSPORTS.pop((id(config), coordinator_name), None)
            _RPC_TRANSPORTS.pop((id(config), target_name), None)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--coordinator", default="MACBOX")
    parser.add_argument("--target", default="WINBOX")
    parser.add_argument("--lease-seconds", type=int, default=10)
    args = parser.parse_args()
    result = run_gate(args.coordinator, args.target, args.lease_seconds)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
