#!/usr/bin/env python3
"""Native RWO 5 conformance gate for one configured target.

The gate installs the exact runner bytes from ``--repo`` into an isolated target
state root. It does not alter fleet execution, public capabilities, or the normal
target state root. Run it once for a POSIX target and once for a Windows target;
compare the emitted ``receipt_shape`` values for parity.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path


class GateFailure(RuntimeError):
    pass


def _load(repo: Path, config_root: Path):  # noqa: ANN202
    sys.path.insert(0, str(repo / "src"))
    from remrun.config import load_config
    from remrun.fleet.queue import FleetQueue
    from remrun.frame import encode_frame
    from remrun.target_resources import (
        TargetReservation,
        TargetResourceClient,
        TargetResourceError,
        _read_stream_frame,
        canonical_json,
        policy_digest,
    )

    return (
        load_config(config_root),
        FleetQueue,
        encode_frame,
        TargetReservation,
        TargetResourceClient,
        TargetResourceError,
        _read_stream_frame,
        canonical_json,
        policy_digest,
    )


def _reservation_request(reservation, argv: list[str], canonical_json, encode_frame) -> bytes:
    request = {
        "schema": "remrun.target-resource-owner-request",
        "version": 1,
        "reservation": {
            "allocation_id": reservation.allocation_id,
            "fence": reservation.fence,
            "token": reservation.token,
            "policy_generation": reservation.receipt["policy_generation"],
            "policy_digest": reservation.receipt["policy_digest"],
        },
        "argv": argv,
        "cwd": None,
        "env": {},
    }
    payload = canonical_json(request)
    return encode_frame(
        {
            "v": 2,
            "kind": "target-resource-owner-request",
            "request_sha256": hashlib.sha256(payload).hexdigest(),
        },
        payload,
    )


def _faulted_remote_command(command: list[str], fault: str, kind: str) -> list[str]:
    result = list(command)
    if kind == "ssh-posix":
        result[-1] = f"env REMRUN_TEST_ONLY_FAULT_POINT={fault} {result[-1]}"
        return result
    if kind == "ssh-powershell":
        match = re.search(r"^(.*-EncodedCommand )([A-Za-z0-9+/=]+)$", result[-1])
        if match is None:
            raise GateFailure("could not identify encoded PowerShell runner command")
        script = base64.b64decode(match.group(2)).decode("utf-16le")
        script = f"$env:REMRUN_TEST_ONLY_FAULT_POINT='{fault}'\n" + script
        encoded = base64.b64encode(script.encode("utf-16le")).decode("ascii")
        result[-1] = match.group(1) + encoded
        return result
    raise GateFailure(f"native gate does not support transport kind {kind!r}")


def _spawn_owner(client, reservation, argv, canonical_json, encode_frame, *, fault=None):
    command = client.transport.runner_stream_argv(
        client.info.installed_path, client.state_root, "resource-owner-run"
    )
    if fault:
        command = _faulted_remote_command(
            command, fault, client.config.devices[client.device_name].kind
        )
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=os.name == "posix",
    )
    assert process.stdin is not None
    process.stdin.write(
        _reservation_request(reservation, argv, canonical_json, encode_frame)
    )
    process.stdin.close()
    process.stdin = None
    return process


def _owner_result(process, read_stream_frame) -> dict:
    assert process.stdout is not None
    _claim_header, claim_payload = read_stream_frame(process.stdout)
    claim = json.loads(claim_payload)
    if not claim.get("ok") or "claim_receipt" not in claim:
        raise GateFailure(str(claim.get("error", "owner claim failed")))
    _header, payload = read_stream_frame(process.stdout)
    result = json.loads(payload)
    process.wait(timeout=90)
    if process.returncode != 0:
        detail = ""
        if process.stderr is not None:
            detail = process.stderr.read().decode("utf-8", "replace").strip()
        raise GateFailure(f"owner transport exited {process.returncode}: {detail}")
    if not result.get("ok"):
        raise GateFailure(str(result.get("error", "owner failed")))
    return result


def _terminate_source(process) -> None:
    if process.poll() is not None:
        return
    process.kill()
    process.wait(timeout=10)


def _wait_status(client, reservation, predicate, timeout: float):  # noqa: ANN001, ANN202
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = client.status(reservation)["receipt"]
        if predicate(last):
            return last
        time.sleep(0.2)
    raise GateFailure(f"timed out waiting for target receipt: {last}")


def _wait_path(transport, path: str, timeout: float = 15) -> None:  # noqa: ANN001
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if transport.remote_path_exists(path):
            return
        time.sleep(0.15)
    raise GateFailure(f"target sentinel did not appear: {path}")


def _stale_owner_mutation_probe(
    client, reservation, operation: str, values: dict, evidence_root: str
) -> dict:
    """Exercise an installed runner's private owner mutation without exposing an RPC."""
    request = {
        "state_root": client.state_root,
        "operation": operation,
        "body": {
            "allocation_id": reservation.allocation_id,
            "fence": reservation.fence,
            "token": reservation.token,
            "policy_generation": reservation.receipt["policy_generation"],
            "policy_digest": reservation.receipt["policy_digest"],
        },
        "values": values,
    }
    remote_request = client.transport.native_join(
        evidence_root, f"stale-{operation}-{uuid.uuid4().hex}.json"
    )
    local_request = None
    try:
        with tempfile.NamedTemporaryFile(prefix="remrun-stale-owner-", delete=False) as stream:
            stream.write(
                json.dumps(request, sort_keys=True, separators=(",", ":")).encode("utf-8")
            )
            local_request = Path(stream.name)
        client.transport.push_file(local_request, remote_request)
    finally:
        if local_request is not None:
            local_request.unlink(missing_ok=True)
    source = (
        "import json,os,runpy,sys; "
        "request_path=sys.argv[2]; "
        "request=json.loads(open(request_path,encoding='utf-8').read()); "
        "os.unlink(request_path); ns=runpy.run_path(sys.argv[1]); "
        "mutation=ns['_resource_owner_mutation']; error=ns['RunnerError']; "
        "\ntry:\n mutation(request['state_root'],request['operation'],"
        "request['body'],**request['values'])\n"
        "except error as exc:\n print(json.dumps({'rejected':True,'error':str(exc)})); "
        "raise SystemExit(0)\n"
        "print(json.dumps({'rejected':False})); raise SystemExit(17)"
    )
    memory_guard = client.transport.memory_guard
    client.transport.memory_guard = None
    try:
        result = client.transport.exec(
            [client.config.devices[client.device_name].remote_python, "-c", source,
             client.info.installed_path, remote_request],
            cwd=client.info.probe["runner_root"],
            timeout=30,
        )
    finally:
        client.transport.memory_guard = memory_guard
    if result.exit_code != 0:
        raise GateFailure(
            f"stale internal {operation} mutation was not rejected: "
            f"exit={result.exit_code} stdout={result.stdout!r} stderr={result.stderr!r}"
        )
    try:
        evidence = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise GateFailure(f"stale internal {operation} probe output was invalid") from exc
    if evidence.get("rejected") is not True:
        raise GateFailure(f"stale internal {operation} mutation succeeded: {evidence}")
    return evidence


def _shape(value: object) -> object:
    if isinstance(value, dict):
        return {key: _shape(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [_shape(value[0])] if value else []
    if value is None:
        return "null"
    return type(value).__name__


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--config-root", type=Path, required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--controller-root-a", type=Path, required=True)
    parser.add_argument("--controller-root-b", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    (
        config,
        FleetQueue,
        encode_frame,
        TargetReservation,
        TargetResourceClient,
        TargetResourceError,
        read_stream_frame,
        canonical_json,
        policy_digest,
    ) = _load(args.repo.resolve(), args.config_root.resolve())
    if args.target not in config.devices:
        raise GateFailure(f"target is not configured: {args.target}")
    device = replace(config.devices[args.target], state_root=args.state_root)
    controller_roots = [args.controller_root_a.resolve(), args.controller_root_b.resolve()]
    if controller_roots[0] == controller_roots[1]:
        raise GateFailure("controller roots must be distinct")
    queue_paths = []
    for root in controller_roots:
        root.mkdir(parents=True, exist_ok=True)
        queue_path = root / "fleet.sqlite3"
        queue = FleetQueue(queue_path)
        queue.close()
        queue_paths.append(queue_path)
    if len({path.stat().st_ino for path in queue_paths}) != 2:
        raise GateFailure("controller contexts did not create separate queue databases")
    configs = [
        replace(config, repo_root=root, devices={args.target: device})
        for root in controller_roots
    ]
    clients = [TargetResourceClient.connect(item, args.target) for item in configs]
    client = clients[0]
    run_id = uuid.uuid4().hex[:12]
    policy = {
        "schema": "remrun.target-resource-policy",
        "version": 1,
        "generation": 1,
        "resources": [
            {"key": "pool/gpu", "capacity": 1},
            {"key": "tcp/48188", "capacity": 1},
        ],
    }
    digest = policy_digest(policy)
    client.policy_install(policy, expected_generation=None, expected_digest=None)
    evidence_root = client.transport.native_join(
        client.info.probe["runner_root"], "native-evidence", run_id
    )
    client.transport.ensure_remote_dir(evidence_root)
    python = device.remote_python

    def reserve(owner: str, keys: list[str]):
        return client.reserve(
            allocation_id=f"{run_id}-{owner}",
            operation_id=f"{run_id}-{owner}",
            request_sha256=hashlib.sha256(f"{run_id}-{owner}".encode()).hexdigest(),
            resource_keys=keys,
            expected_policy_generation=1,
            expected_policy_digest=digest,
        )

    barrier = threading.Barrier(2)

    def contender(index: int):
        contender_client = clients[index]
        barrier.wait()
        held = contender_client.reserve(
            allocation_id=f"{run_id}-race-{index}",
            operation_id=f"{run_id}-race-{index}",
            request_sha256=hashlib.sha256(f"{run_id}-race-{index}".encode()).hexdigest(),
            resource_keys=["pool/gpu"],
            expected_policy_generation=1,
            expected_policy_digest=digest,
        )
        if not isinstance(held, TargetReservation):
            return held["status"], None, None
        marker = client.transport.native_join(evidence_root, f"race-{index}.txt")
        result = contender_client.owner_run(
            held,
            [
                python,
                "-c",
                "from pathlib import Path; import sys,time; "
                "p=Path(sys.argv[1]); p.parent.mkdir(parents=True,exist_ok=True); "
                "p.write_text('opened'); time.sleep(0.5)",
                marker,
            ],
        )
        if result["exit_code"] != 0:
            raise GateFailure(f"race sentinel command failed: {result}")
        return result["receipt"]["state"], marker, held

    with ThreadPoolExecutor(max_workers=2) as pool:
        race = list(pool.map(contender, range(2)))
    if sorted(item[0] for item in race) != ["RELEASED", "resource_busy"]:
        raise GateFailure(f"two-controller race was not exclusive: {race}")
    markers = [item[1] for item in race if item[1]]
    if len(markers) != 1 or not client.transport.remote_path_exists(markers[0]):
        raise GateFailure(
            f"two-controller race did not create exactly one sentinel: {race}"
        )
    first_reservation = next(item[2] for item in race if item[2] is not None)

    both = reserve("both", ["pool/gpu", "tcp/48188"])
    if not isinstance(both, TargetReservation):
        raise GateFailure(f"multi-key reservation failed: {both}")
    for key in ("pool/gpu", "tcp/48188"):
        blocked = reserve("blocked-" + key.replace("/", "-"), [key])
        if not isinstance(blocked, dict) or blocked.get("status") != "resource_busy":
            raise GateFailure(f"multi-key hold was partial for {key}: {blocked}")
    client.cancel(both)

    preclaim = reserve("preclaim-death", ["pool/gpu"])
    assert isinstance(preclaim, TargetReservation)
    preclaim_marker = client.transport.native_join(evidence_root, "preclaim.txt")
    preclaim_process = _spawn_owner(
        client,
        preclaim,
        [python, "-c", "from pathlib import Path; import sys; Path(sys.argv[1]).write_text('bad')", preclaim_marker],
        canonical_json,
        encode_frame,
        fault="pause_before_resource_claim",
    )
    time.sleep(1.5)
    if client.status(preclaim)["receipt"]["state"] != "RESERVED":
        raise GateFailure("pre-claim source-death probe crossed the claim boundary")
    _terminate_source(preclaim_process)
    expired = _wait_status(
        client, preclaim, lambda row: row["state"] == "EXPIRED", timeout=40
    )
    if client.transport.remote_path_exists(preclaim_marker):
        raise GateFailure("pre-claim source death opened the user-code gate")
    replacement = reserve("after-preclaim", ["pool/gpu"])
    if not isinstance(replacement, TargetReservation):
        raise GateFailure(f"expired pre-claim hold was not reusable: {replacement}")
    client.cancel(replacement)

    postclaim = reserve("postclaim-death", ["pool/gpu"])
    assert isinstance(postclaim, TargetReservation)
    postclaim_marker = client.transport.native_join(evidence_root, "postclaim.txt")
    postclaim_handle = client.owner_start(
        postclaim,
        [
            python,
            "-c",
            "from pathlib import Path; import subprocess,sys; "
            "p=Path(sys.argv[1]); p.parent.mkdir(parents=True,exist_ok=True); "
            "subprocess.Popen([sys.executable,'-c','import time;time.sleep(12)']); "
            "p.write_text('started')",
            postclaim_marker,
        ],
    )
    postclaim_claim = postclaim_handle.claim_receipt
    if postclaim_claim["command_start_state"] != "NO":
        raise GateFailure(f"claim receipt crossed the start boundary: {postclaim_claim}")
    _terminate_source(postclaim_handle.process)
    _wait_path(client.transport, postclaim_marker)
    live = _wait_status(
        client,
        postclaim,
        lambda row: row["state"] == "CLAIMED" and row["command_start_state"] == "YES",
        timeout=15,
    )
    blocked = reserve("during-postclaim", ["pool/gpu"])
    if not isinstance(blocked, dict) or blocked.get("status") != "resource_busy":
        raise GateFailure(f"post-claim source death lost its target hold: {blocked}")
    completed = _wait_status(
        client, postclaim, lambda row: row["state"] == "RELEASED", timeout=25
    )

    newer = reserve("stale-newer", ["pool/gpu"])
    assert isinstance(newer, TargetReservation)
    stale_marker = client.transport.native_join(evidence_root, "stale.txt")
    try:
        client.owner_run(
            first_reservation,
            [python, "-c", "from pathlib import Path; import sys; Path(sys.argv[1]).write_text('bad')", stale_marker],
        )
        raise GateFailure("stale owner opened a newer allocation's gate")
    except TargetResourceError:
        pass
    for operation in (
        lambda: client.renew(
            first_reservation,
            expected_policy_generation=1,
            expected_policy_digest=digest,
        ),
        lambda: client.cancel(first_reservation),
    ):
        try:
            stale_response = operation()
        except TargetResourceError:
            continue
        if stale_response["receipt"]["state"] != "RELEASED":
            raise GateFailure(f"stale reservation changed state: {stale_response}")
    if client.status(newer)["receipt"]["state"] != "RESERVED":
        raise GateFailure("stale reservation mutated the newer allocation")
    if client.transport.remote_path_exists(stale_marker):
        raise GateFailure("stale reservation created a sentinel")
    stale_internal = {
        operation: _stale_owner_mutation_probe(
            client,
            first_reservation,
            operation,
            {"state": "MAYBE"}
            if operation == "start"
            else {"reason": "stale_owner_native_probe"},
            evidence_root,
        )
        for operation in ("start", "release", "quarantine")
    }
    if client.status(newer)["receipt"]["state"] != "RESERVED":
        raise GateFailure("stale internal owner mutation changed the newer allocation")
    client.cancel(newer)

    uncertain = reserve("cleanup-unknown", ["pool/gpu"])
    assert isinstance(uncertain, TargetReservation)
    uncertain_process = _spawn_owner(
        client,
        uncertain,
        [python, "-c", "raise SystemExit(0)"],
        canonical_json,
        encode_frame,
        fault="resource_cleanup_unknown",
    )
    uncertain_result = _owner_result(uncertain_process, read_stream_frame)
    if uncertain_result["receipt"]["state"] != "QUARANTINED":
        raise GateFailure(f"cleanup uncertainty was not quarantined: {uncertain_result}")
    if client.policy_get().get("holds_active") is not True:
        raise GateFailure("quarantined cleanup did not retain its hold")
    reconciled = client.status(uncertain)["receipt"]
    if reconciled["state"] != "RELEASED":
        raise GateFailure(f"positive target reconciliation did not release: {reconciled}")

    probe = client.info.probe
    if probe.get("journal_mode") != "delete" or not probe.get("filesystem", {}).get("local"):
        raise GateFailure(f"target store is not verified local rollback-journal: {probe}")
    evidence = {
        "status": "PASS",
        "target": args.target,
        "run_id": run_id,
        "runner_source_sha256": client.info.source_sha256,
        "installed_path": client.info.installed_path,
        "controller_contexts": [
            {
                "root": str(root),
                "queue_db": str(queue_path),
                "queue_exists": queue_path.is_file(),
                "queue_inode": queue_path.stat().st_ino,
            }
            for root, queue_path in zip(controller_roots, queue_paths)
        ],
        "store": {
            "filesystem": probe["filesystem"],
            "journal_mode": probe["journal_mode"],
            "sqlite_version": probe["sqlite_version"],
            "schema_version": probe["schema_version"],
        },
        "race_outcomes": sorted(item[0] for item in race),
        "preclaim_terminal": expired,
        "postclaim_live": live,
        "postclaim_claim_receipt": postclaim_claim,
        "postclaim_terminal": completed,
        "stale_internal_owner_mutations": stale_internal,
        "cleanup_uncertain": uncertain_result["receipt"],
        "cleanup_reconciled": reconciled,
        "receipt_shape": _shape(completed),
    }
    print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
