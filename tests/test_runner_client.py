from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sqlite3
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from remrun.config import RemrunConfig
from remrun.models import Device
from remrun.remote import runner as remote_runner
from remrun.runner_client import (
    RunnerClientError,
    enroll_target_key,
    ensure_versioned_runner,
    runner_rpc,
)
from remrun.transport import make_transport


def config(tmp_path: Path) -> RemrunConfig:
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


def relay_config(tmp_path: Path) -> RemrunConfig:
    devices = {}
    for name in ("COORD", "TARGET"):
        devices[name] = Device.from_mapping(name, {
            "kind": "local-sim", "os": "posix",
            "project_root": str(tmp_path / name / "projects"),
            "state_root": str(tmp_path / name / "state"),
            "cache_root": str(tmp_path / name / "cache"),
        })
    return RemrunConfig(
        repo_root=tmp_path / "tool", defaults={}, devices=devices,
        project_roots={"default": str(tmp_path / "projects")},
    )


def test_install_initializes_versioned_runner_and_participant_store(tmp_path: Path):
    cfg = config(tmp_path)

    first = ensure_versioned_runner(cfg, "LOCAL_SIM", install=True)
    second = ensure_versioned_runner(cfg, "LOCAL_SIM", install=True)

    installed = Path(first.installed_path)
    assert installed.name == f"remrun-runner-{first.source_sha256}.py"
    assert installed.is_file()
    assert first.reused is False
    assert second.reused is True
    assert second.probe["device_id"] == first.probe["device_id"]
    assert second.probe["schema_version"] == 3
    assert second.probe["filesystem"]["local"] is True
    assert second.probe["journal_mode"] == "delete"
    runner_root = Path(second.probe["runner_root"])
    metadata = json.loads((runner_root / "runner.json").read_text(encoding="utf-8"))
    assert metadata["runner_source_sha256"] == first.source_sha256
    with sqlite3.connect(runner_root / "runner.sqlite3") as conn:
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"runner_meta", "project_fences", "accepted_grants", "executions",
                "participant_transactions", "mutations", "logical_modes",
                "rpc_requests", "enrolled_authorities"} <= tables
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"


def test_probe_refuses_missing_versioned_runner(tmp_path: Path):
    with pytest.raises(RunnerClientError, match="missing"):
        ensure_versioned_runner(config(tmp_path), "LOCAL_SIM", install=False)


def test_install_replaces_corrupt_content_at_pinned_path(tmp_path: Path):
    cfg = config(tmp_path)
    first = ensure_versioned_runner(cfg, "LOCAL_SIM", install=True)
    Path(first.installed_path).write_bytes(b"corrupt")

    repaired = ensure_versioned_runner(cfg, "LOCAL_SIM", install=True)

    assert repaired.reused is False
    assert repaired.probe["runner_source_sha256"] == repaired.source_sha256


def test_concurrent_rpc_db_writers_are_serialized(tmp_path: Path):
    cfg = config(tmp_path)
    info = ensure_versioned_runner(cfg, "LOCAL_SIM", install=True)
    state_root = cfg.devices["LOCAL_SIM"].state_root

    def touch(index: int):
        transport = make_transport(cfg.devices["LOCAL_SIM"])
        return runner_rpc(
            transport, info.installed_path, state_root, "participant_touch",
            {"index": index}, rpc_id=f"touch-{index}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(touch, range(24)))

    assert sorted(row["echo"]["index"] for row in responses) == list(range(24))
    with sqlite3.connect(Path(info.probe["runner_root"]) / "runner.sqlite3") as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM rpc_requests WHERE operation='participant_touch'"
        ).fetchone()[0] == 24


def test_hot_rollback_journal_reopens_cleanly(tmp_path: Path):
    cfg = config(tmp_path)
    info = ensure_versioned_runner(cfg, "LOCAL_SIM", install=True)
    db = Path(info.probe["runner_root"]) / "runner.sqlite3"
    code = """
import os,sqlite3,sys,time
conn=sqlite3.connect(sys.argv[1],isolation_level=None)
conn.execute('PRAGMA journal_mode=DELETE')
conn.execute('PRAGMA synchronous=EXTRA')
conn.execute('BEGIN IMMEDIATE')
conn.execute("INSERT INTO rpc_requests VALUES ('crash','x','x',x'7b7d',?)",(time.time_ns(),))
os._exit(0)
"""
    subprocess.run([sys.executable, "-c", code, str(db)], check=True)

    reopened = ensure_versioned_runner(cfg, "LOCAL_SIM", install=False)

    assert reopened.probe["device_id"] == info.probe["device_id"]
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM rpc_requests WHERE rpc_id='crash'"
        ).fetchone()[0] == 0


def test_participant_store_refuses_nonlocal_filesystem(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(remote_runner, "filesystem_probe", lambda _path: {
        "local": False, "kind": "nfs", "path": "/network/state",
    })
    with pytest.raises(remote_runner.RunnerError, match="local filesystem"):
        remote_runner.open_participant_store(str(tmp_path / "state"))


def test_participant_store_verifies_rollback_journal_mode(tmp_path: Path):
    class MemorySQLite:
        sqlite_version = sqlite3.sqlite_version
        OperationalError = sqlite3.OperationalError

        @staticmethod
        def connect(*_args, **_kwargs):
            return sqlite3.connect(":memory:", isolation_level=None)

    with pytest.raises(remote_runner.RunnerError, match="rollback journal mode"):
        remote_runner.open_participant_store(
            str(tmp_path / "state"), sqlite_module=MemorySQLite
        )


def test_participant_store_reports_missing_sqlite(tmp_path: Path):
    with pytest.raises(remote_runner.RunnerError, match="no sqlite3"):
        remote_runner.open_participant_store(str(tmp_path / "state"), sqlite_module=False)


def test_participant_store_refuses_newer_schema(tmp_path: Path):
    db_dir = tmp_path / "state" / "runner" / "v1"
    db_dir.mkdir(parents=True)
    db = db_dir / "runner.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute("PRAGMA user_version=99")
    with pytest.raises(remote_runner.RunnerError, match="incompatible with supported"):
        remote_runner.open_participant_store(str(tmp_path / "state"))


def test_participant_store_migrates_v1_identity_to_v2(tmp_path: Path):
    db_dir = tmp_path / "state" / "runner" / "v1"
    db_dir.mkdir(parents=True)
    db = db_dir / "runner.sqlite3"
    device_id = str(uuid.uuid4())
    with sqlite3.connect(db) as conn:
        for statement in remote_runner.SCHEMA[: remote_runner.RUNNER_V1_SCHEMA_COUNT]:
            conn.execute(statement)
        conn.execute(
            "INSERT INTO runner_meta "
            "(singleton,schema_version,device_id,created_at_ns) VALUES (1,1,?,1)",
            (device_id,),
        )
        conn.execute(
            "INSERT INTO project_fences VALUES (?,?,?,?)",
            ("cluster-a", "project-a", 3, 17),
        )
        conn.execute("PRAGMA user_version=1")

    conn, _root, meta = remote_runner.open_participant_store(str(tmp_path / "state"))
    try:
        assert meta["device_id"] == device_id
        assert meta["schema_version"] == 3
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
        assert conn.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' "
            "AND name='enrolled_authorities'"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT authority_epoch,max_fence FROM project_fences "
            "WHERE cluster_id='cluster-a' AND project_key='project-a'"
        ).fetchone() == (3, 17)
    finally:
        conn.close()


def test_participant_store_rejects_partial_v1_without_relabeling(tmp_path: Path):
    db_dir = tmp_path / "state" / "runner" / "v1"
    db_dir.mkdir(parents=True)
    db = db_dir / "runner.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute(remote_runner.SCHEMA[0])
        conn.execute(
            "INSERT INTO runner_meta "
            "(singleton,schema_version,device_id,created_at_ns) VALUES (1,1,?,1)",
            (str(uuid.uuid4()),),
        )
        conn.execute("PRAGMA user_version=1")

    with pytest.raises(remote_runner.RunnerError, match="complete expected definition"):
        remote_runner.open_participant_store(str(tmp_path / "state"))
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
        assert conn.execute(
            "SELECT schema_version FROM runner_meta WHERE singleton=1"
        ).fetchone()[0] == 1


@pytest.mark.parametrize("damaged_key", [b"", b"short"])
def test_direct_key_relay_repairs_interruption_and_retires_old_epoch(
        tmp_path: Path, damaged_key: bytes):
    cfg = relay_config(tmp_path)
    coordinator = ensure_versioned_runner(cfg, "COORD", install=True)
    target = ensure_versioned_runner(cfg, "TARGET", install=True)
    coord_transport = make_transport(cfg.devices["COORD"])
    target_transport = make_transport(cfg.devices["TARGET"])
    cluster = str(uuid.uuid4())
    coord_root = cfg.devices["COORD"].state_root
    target_root = cfg.devices["TARGET"].state_root
    runner_rpc(
        coord_transport, coordinator.installed_path, coord_root,
        "authority_init", {"cluster_id": cluster},
    )

    first = enroll_target_key(cfg, "COORD", "TARGET", cluster)
    assert first["prepared"]["status"] == "PENDING"
    assert first["imported"]["status"] == "ENROLLED"
    assert first["finalized"]["status"] == "ENROLLED"
    target_keys = Path(target.probe["runner_root"]) / "keys"
    old_key_path = next(target_keys.glob("*.e1.*.key"))
    old_secret = old_key_path.read_bytes()

    rotated = runner_rpc(
        coord_transport, coordinator.installed_path, coord_root,
        "authority_epoch_rotate",
        {"cluster_id": cluster, "expected_authority_epoch": 1},
    )
    assert rotated["status"] == "ROTATED"
    prepared = runner_rpc(
        coord_transport, coordinator.installed_path, coord_root,
        "authority_target_key_create", {
            "cluster_id": cluster, "target_device_id": target.probe["device_id"],
        },
    )
    assert prepared["authority_epoch"] == 2

    export_argv = coord_transport.runner_stream_argv(
        coordinator.installed_path, coord_root, "key-export", [
            cluster, target.probe["device_id"], "2", prepared["key_id"],
        ]
    )
    exported = subprocess.run(export_argv, capture_output=True, check=True).stdout
    import_argv = target_transport.runner_stream_argv(
        target.installed_path, target_root, "key-import"
    )
    fault_env = {**os.environ,
                 "REMRUN_TEST_ONLY_FAULT_POINT": "after_enrollment_key_create"}
    interrupted = subprocess.run(
        import_argv, input=exported, capture_output=True, check=False, env=fault_env,
    )
    assert interrupted.returncode != 0
    assert len(list(target_keys.glob("*.e2.*.key"))) == 1
    target_db = Path(target.probe["runner_root"]) / "runner.sqlite3"
    with sqlite3.connect(target_db) as conn:
        assert conn.execute(
            "SELECT count(*) FROM enrolled_authorities WHERE authority_epoch=2"
        ).fetchone()[0] == 0
    epoch_two_key = next(target_keys.glob("*.e2.*.key"))
    epoch_two_key.write_bytes(damaged_key)

    repaired = enroll_target_key(cfg, "COORD", "TARGET", cluster)
    assert repaired["imported"]["idempotent"] is False
    assert repaired["finalized"]["status"] == "ENROLLED"
    assert len(list(target_keys.glob("*.e2.*.key"))) == 1
    assert epoch_two_key.stat().st_size == 32
    with sqlite3.connect(target_db) as conn:
        rows = conn.execute(
            "SELECT authority_epoch,state FROM enrolled_authorities ORDER BY authority_epoch"
        ).fetchall()
    assert rows == [(1, "RETIRED"), (2, "ENROLLED")]

    stale_body = {
        "v": 1, "cluster_id": cluster, "authority_epoch": 1,
        "grant_id": str(uuid.uuid4()), "project_key": "a" * 64,
        "lease_id": str(uuid.uuid4()), "fence": 1,
        "target_device_id": target.probe["device_id"], "operation": "txn_apply",
        "operation_id": "stale-epoch", "request_sha256": "b" * 64,
    }
    stale = {
        "body": stale_body, "sig_alg": "hmac-sha256",
        "key_id": first["prepared"]["key_id"],
        "sig": base64.urlsafe_b64encode(hmac.new(
            old_secret, remote_runner.canonical_json(stale_body), hashlib.sha256,
        ).digest()).rstrip(b"=").decode("ascii"),
    }
    with pytest.raises(RunnerClientError, match="not enrolled for this authority epoch"):
        runner_rpc(
            target_transport, target.installed_path, target_root,
            "participant_grant_accept", {"capability": stale},
        )


def test_authority_reset_with_same_cluster_and_epoch_fails_closed_at_target(tmp_path: Path):
    cfg = relay_config(tmp_path)
    coordinator = ensure_versioned_runner(cfg, "COORD", install=True)
    target = ensure_versioned_runner(cfg, "TARGET", install=True)
    cluster = str(uuid.uuid4())
    runner_rpc(
        make_transport(cfg.devices["COORD"]), coordinator.installed_path,
        cfg.devices["COORD"].state_root, "authority_init", {"cluster_id": cluster},
    )
    assert enroll_target_key(cfg, "COORD", "TARGET", cluster)["finalized"][
        "status"
    ] == "ENROLLED"

    reset_coordinator = Device.from_mapping("COORD", {
        "kind": "local-sim", "os": "posix",
        "project_root": str(tmp_path / "COORD-RESET" / "projects"),
        "state_root": str(tmp_path / "COORD-RESET" / "state"),
        "cache_root": str(tmp_path / "COORD-RESET" / "cache"),
    })
    reset_cfg = RemrunConfig(
        repo_root=cfg.repo_root, defaults=cfg.defaults,
        devices={"COORD": reset_coordinator, "TARGET": cfg.devices["TARGET"]},
        project_roots=cfg.project_roots,
    )
    reset_info = ensure_versioned_runner(reset_cfg, "COORD", install=True)
    runner_rpc(
        make_transport(reset_coordinator), reset_info.installed_path,
        reset_coordinator.state_root, "authority_init", {"cluster_id": cluster},
    )
    with pytest.raises(
            RunnerClientError, match="authority epoch already has a different enrolled key"):
        enroll_target_key(reset_cfg, "COORD", "TARGET", cluster)

    target_db = Path(target.probe["runner_root"]) / "runner.sqlite3"
    with sqlite3.connect(target_db) as conn:
        rows = conn.execute(
            "SELECT authority_epoch,state FROM enrolled_authorities WHERE cluster_id=?",
            (cluster,),
        ).fetchall()
    assert rows == [(1, "ENROLLED")]
