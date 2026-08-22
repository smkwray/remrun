from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from remrun.config import RemrunConfig
from remrun.fleet import executor
from remrun.fleet.cli import build_parser
from remrun.fleet.prepared import as_fleet_task, prepare_task_job, validate_prepared_job
from remrun.fleet.storage import (
    MARKER_NAME, bind_device_root, enroll_local_root, load_registry,
    storage_ref_for_path,
)
from remrun.fleet.task_contract import resolve_task_spec
from remrun.models import Device
from remrun.transport import LocalSimTransport, _MATERIALIZE_SHARED_INPUT_PROG, make_transport


def _config(tmp_path: Path) -> RemrunConfig:
    device = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim", "os": "posix", "address_candidates": ["localhost"],
        "project_root": str(tmp_path / "remote"),
        "state_root": str(tmp_path / "target-state"),
        "cache_root": str(tmp_path / "target-cache"), "max_jobs": 1,
    })
    return RemrunConfig(
        repo_root=tmp_path, defaults={}, devices={"LOCAL_SIM": device},
        project_roots={}, offload={},
    )


def _spec(tmp_path: Path, worker: Path) -> dict:
    return resolve_task_spec("novel", {
        "input": {"mode": "files", "extensions": [".bin"], "split": "per-item",
                  "file_identity": "sha256"},
        "prepare": {"mode": "none"},
        "routing": {"requirements": [], "requirements_by_option": {}},
        "execution": {"batching": "never", "replay": "at-most-once-v1"},
        "cost": {"measure": "item-count", "unit": "items", "divisor": 1,
                 "bucket_options": []},
        "output": {"reservation": "none", "allow_root_override": False,
                   "verification": "none"},
        "completion": {"protocol": "exit-code-v1", "evidence": "never",
                       "companion": "forbidden", "allowed_publication": ["none"],
                       "unstructured_memory": "ignore"},
        "options": {},
        "adapters": {"LOCAL_SIM": {
            "engine": "generic", "argv": ["python", str(worker)], "pool": False,
            "memory_kind": "cpu", "capability_paths": [], "provides": [],
        }},
    }, devices={"LOCAL_SIM"}, repo_root=tmp_path)


def test_enroll_bind_and_prepare_storage_ref(tmp_path: Path) -> None:
    state = tmp_path / "state"
    root = tmp_path / "shared"
    root.mkdir()
    source = root / "folder" / "input.bin"
    source.parent.mkdir()
    source.write_bytes(b"same bytes")

    enrolled = enroll_local_root(state, root)
    transport = make_transport(_config(tmp_path).devices["LOCAL_SIM"])
    bound = bind_device_root(state, "LOCAL_SIM", str(root), transport)
    registry = load_registry(state)
    ref = storage_ref_for_path(registry, source)

    assert json.loads((root / MARKER_NAME).read_text())["storage_id"] == enrolled["storage_id"]
    assert bound["storage_id"] == enrolled["storage_id"]
    assert ref == {
        "schema": 1, "storage_id": enrolled["storage_id"],
        "relative_components": ["folder", "input.bin"],
    }

    worker = tmp_path / "worker.py"
    worker.write_text("raise SystemExit(0)", encoding="utf-8")
    record = prepare_task_job(
        _spec(tmp_path, worker), repo_root=tmp_path, inputs=[str(source)],
        storage_registry=registry,
    )
    validate_prepared_job(record)
    assert record["schema"] == 5
    assert record["payload"]["items"][0]["storage_ref"] == ref


def test_empty_registry_preserves_legacy_prepared_identity(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"same bytes")
    worker = tmp_path / "worker.py"
    worker.write_text("raise SystemExit(0)", encoding="utf-8")
    spec = _spec(tmp_path, worker)

    legacy = prepare_task_job(spec, repo_root=tmp_path, inputs=[str(source)])
    empty = prepare_task_job(
        spec, repo_root=tmp_path, inputs=[str(source)],
        storage_registry={"schema": 1, "roots": {}},
    )

    assert empty == legacy
    assert empty["schema"] == 3


def test_storage_cli_is_explicit_and_output_return_is_opt_in() -> None:
    parser = build_parser()
    enroll = parser.parse_args(["storage", "enroll", "/shared", "--json"])
    bind = parser.parse_args(["storage", "bind", "--device", "BOX", "/shared"])
    run = parser.parse_args(["run", "novel", "--return-root", "/returned"])

    assert enroll.storage_action == "enroll" and enroll.json is True
    assert bind.storage_action == "bind" and bind.device == "BOX"
    assert run.return_root == "/returned"


def test_shared_view_executes_and_wrong_candidate_falls_back_to_stream(
    tmp_path: Path, monkeypatch,
) -> None:
    state = tmp_path / "state"
    local_root = tmp_path / "controller-view"
    target_root = tmp_path / "target-view"
    local_root.mkdir()
    target_root.mkdir()
    source = local_root / "input.bin"
    source.write_bytes(b"frozen source")
    enrolled = enroll_local_root(state, local_root)
    (target_root / MARKER_NAME).write_text(
        json.dumps({"schema": 1, "storage_id": enrolled["storage_id"]}), encoding="utf-8",
    )
    (target_root / "input.bin").write_bytes(b"frozen source")
    config = _config(tmp_path)
    bind_device_root(state, "LOCAL_SIM", str(target_root), make_transport(config.devices["LOCAL_SIM"]))
    worker = tmp_path / "worker.py"
    worker.write_text("raise SystemExit(0)", encoding="utf-8")
    spec = _spec(tmp_path, worker)
    record = prepare_task_job(
        spec, repo_root=tmp_path, inputs=[str(source)],
        storage_registry=load_registry(state),
    )

    calls = {"shared": 0, "stream": 0}
    real_shared = LocalSimTransport.materialize_shared_input
    real_stream = LocalSimTransport.materialize_input

    def shared(self, *args, **kwargs):  # noqa: ANN001
        calls["shared"] += 1
        return real_shared(self, *args, **kwargs)

    def stream(self, *args, **kwargs):  # noqa: ANN001
        calls["stream"] += 1
        return real_stream(self, *args, **kwargs)

    monkeypatch.setattr(LocalSimTransport, "materialize_shared_input", shared)
    monkeypatch.setattr(LocalSimTransport, "materialize_input", stream)
    shared_result = executor.run_batch(
        "LOCAL_SIM", [as_fleet_task(record, spec)], config, state_root=state,
        prelaunch_gate=lambda: True,
    )
    assert shared_result["ok"] is True, shared_result
    assert calls == {"shared": 1, "stream": 0}

    (target_root / "input.bin").write_bytes(b"wrong source!")
    fallback_result = executor.run_batch(
        "LOCAL_SIM", [as_fleet_task(record, spec)], config, state_root=state,
        prelaunch_gate=lambda: True,
    )
    assert fallback_result["ok"] is True, fallback_result
    assert calls == {"shared": 2, "stream": 1}


def test_corrupt_registry_fails_before_shared_job_launch(tmp_path: Path) -> None:
    state = tmp_path / "state"
    root = tmp_path / "shared"
    root.mkdir()
    source = root / "input.bin"
    source.write_bytes(b"frozen source")
    enroll_local_root(state, root)
    worker_marker = tmp_path / "worker-launched"
    worker = tmp_path / "worker.py"
    worker.write_text(
        f"import pathlib; pathlib.Path({str(worker_marker)!r}).write_text('yes')",
        encoding="utf-8",
    )
    spec = _spec(tmp_path, worker)
    record = prepare_task_job(
        spec, repo_root=tmp_path, inputs=[str(source)],
        storage_registry=load_registry(state),
    )
    (state / "fleet" / "storage-roots-v1.json").write_text("{", encoding="utf-8")

    result = executor.run_batch(
        "LOCAL_SIM", [as_fleet_task(record, spec)], _config(tmp_path), state_root=state,
        prelaunch_gate=lambda: True,
    )

    assert result["ok"] is False
    assert result["phase"] == "storage_registry"
    assert "registry is unreadable" in result["error"]
    assert not worker_marker.exists()


@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink court; Windows helper uses junction check")
def test_target_shared_helper_rejects_escaping_symlink(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    storage_id = "1" * 32
    (root / MARKER_NAME).write_text(
        json.dumps({"schema": 1, "storage_id": storage_id}), encoding="utf-8",
    )
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside")
    linked = root / "linked.bin"
    linked.symlink_to(outside)
    destination = tmp_path / "private" / "input.bin"
    digest = hashlib.sha256(outside.read_bytes()).hexdigest()

    result = subprocess.run(
        [sys.executable, "-S", "-c", _MATERIALIZE_SHARED_INPUT_PROG,
         str(root), str(linked), str(destination), storage_id,
         str(outside.stat().st_size), digest],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode != 0
    assert not destination.exists()
