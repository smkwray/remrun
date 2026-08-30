from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun.fleet import cli, dispatcher, executor, placement, probes
from remrun.fleet.models import DeviceSnapshot
from remrun.fleet.prepared import as_fleet_task, prepare_task_job
from remrun.fleet.queue import FleetQueue
from remrun.models import Device
from remrun.transport import ProbeResult
from remrun.fleet.task_contract import (
    TaskContractError,
    resolve_tasks,
    resolved_route_eligibility,
    task_routes_document,
    validate_task_routes_document,
)


def _definition(mode: str) -> dict:
    file_input = mode in {"files", "text-or-files"}
    input_spec = {"mode": mode, "split": "never"}
    if file_input:
        input_spec.update({"extensions": [".unit"], "file_identity": "metadata"})
    return {
        "input": input_spec,
        "prepare": {"mode": "none"},
        "routing": {"requirements": [], "requirements_by_option": {}},
        "execution": {"batching": "never", "replay": "at-most-once-v1"},
        "cost": {"measure": "none", "bucket_options": []},
        "output": {"reservation": "none", "allow_root_override": False, "verification": "none"},
        "completion": {
            "protocol": "exit-code-v1", "evidence": "never", "companion": "forbidden",
            "allowed_publication": ["none"], "unstructured_memory": "ignore",
        },
        "options": {},
        "adapters": {
            "BOX": {
                "engine": "unit-engine", "argv": ["/workers/unit"], "pool": False,
                "memory_kind": "cpu", "capability_paths": [], "provides": [],
            },
        },
    }


def _config(tmp_path: Path, modes: dict[str, str]):
    return SimpleNamespace(
        repo_root=tmp_path,
        devices={"BOX": SimpleNamespace(enabled=True)},
        fleet_tasks={name: _definition(mode) for name, mode in modes.items()},
    )


def test_route_projection_court_1_is_closed_sorted_and_supports_all_modes(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, {
        "textual": "text", "nothing": "none", "file-or-text": "text-or-files",
        "filewise": "files",
    })
    specs = resolve_tasks(config)
    document = task_routes_document(specs, config.devices)
    expected_routes = [
        {
            "task": name,
            "spec_id": specs[name]["spec_id"],
            "device": "BOX",
            "input_mode": specs[name]["definition"]["input"]["mode"],
            "accepted_option_ids": [],
            "eligibility": {"status": "eligible", "reason": "configured"},
        }
        for name in sorted(specs)
    ]

    assert document == {
        "schema": "remrun.fleet.task-routes",
        "version": 1,
        "routes": expected_routes,
    }


def test_route_projection_opaque_task_name_and_input_change_are_content_addressed(
    tmp_path: Path,
) -> None:
    first = _config(tmp_path, {"new.workflow": "text"})
    first_document = task_routes_document(resolve_tasks(first), first.devices)
    second = _config(tmp_path, {"new.workflow": "none"})
    second_document = task_routes_document(resolve_tasks(second), second.devices)

    first_id = first_document["routes"][0]["spec_id"]
    second_id = second_document["routes"][0]["spec_id"]
    assert first_id != second_id
    assert first_document["routes"][0]["input_mode"] == "text"
    assert second_document["routes"][0]["input_mode"] == "none"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda raw: raw["input"].pop("mode"),
        lambda raw: raw["input"].__setitem__("mode", "unknown"),
    ],
)
def test_malformed_input_fails_without_a_partial_projection(
    tmp_path: Path, monkeypatch, capsys, mutate,
) -> None:
    raw = _definition("text")
    mutate(raw)
    config = SimpleNamespace(repo_root=tmp_path, devices={"BOX": SimpleNamespace(enabled=True)},
                             fleet_tasks={"broken": raw})
    monkeypatch.setattr(cli, "load_config", lambda: config)

    assert cli.main(["task-interfaces", "--json"]) == cli.EXIT_ERROR
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "error" in captured.err


def test_route_projection_court_6_does_not_probe_devices_or_run_workloads(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    config = _config(tmp_path, {"generic.task": "text"})
    expected = task_routes_document(resolve_tasks(config), config.devices)
    monkeypatch.setattr(cli, "load_config", lambda: config)
    monkeypatch.setattr(cli.probes, "build_snapshot",
                        lambda *_args, **_kwargs: pytest.fail("device probe attempted"))

    assert cli.main(["task-interfaces", "--json"]) == cli.EXIT_OK
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.count("\n") == 1
    assert json.loads(captured.out) == expected


def test_route_projection_human_projection_uses_reporter_and_same_document(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    config = _config(tmp_path, {"generic.task": "text"})
    expected = task_routes_document(resolve_tasks(config), config.devices)["routes"][0]
    monkeypatch.setattr(cli, "load_config", lambda: config)

    assert cli.main(["task-interfaces"]) == cli.EXIT_OK
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "remrun: task_route "
        f"task={expected['task']} spec_id={expected['spec_id']} "
        f"device={expected['device']} input_mode={expected['input_mode']} "
        f"accepted_option_ids={expected['accepted_option_ids']} "
        f"eligibility={json.dumps(expected['eligibility'])}\n"
    )


@pytest.mark.parametrize(
    "document",
    [
        {"schema": "remrun.fleet.task-interfaces", "version": 1,
         "tasks": [{"task": "same", "spec_id": "sha256:" + "0" * 64,
                    "input_mode": "text"},
                   {"task": "same", "spec_id": "sha256:" + "1" * 64,
                    "input_mode": "files"}]},
        {"schema": "remrun.fleet.task-interfaces", "version": 1,
         "tasks": [{"task": "task", "spec_id": "sha256:" + "0" * 64,
                    "input_mode": "text", "extra": True}]},
    ],
)
def test_malformed_projection_is_rejected(document) -> None:  # noqa: ANN001
    with pytest.raises(TaskContractError):
        validate_task_routes_document(document)


def test_projection_rejects_cross_device_contract_drift() -> None:
    document = {
        "schema": "remrun.fleet.task-routes",
        "version": 1,
        "routes": [
            {
                "task": "same", "spec_id": "sha256:" + "0" * 64,
                "device": "A", "input_mode": "text",
                "accepted_option_ids": ["voice"],
                "eligibility": {"status": "eligible", "reason": "configured"},
            },
            {
                "task": "same", "spec_id": "sha256:" + "1" * 64,
                "device": "B", "input_mode": "text",
                "accepted_option_ids": ["voice"],
                "eligibility": {"status": "eligible", "reason": "configured"},
            },
        ],
    }
    with pytest.raises(TaskContractError, match="inconsistent route contracts"):
        validate_task_routes_document(document)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda row: row.__setitem__("extra", True),
        lambda row: row["accepted_option_ids"].__setitem__(slice(None), ["voice", "speed"]),
        lambda row: row.__setitem__(
            "eligibility", {"status": "eligible", "reason": "disabled"},
        ),
    ],
)
def test_new_route_schema_rejects_unknown_unsorted_and_incoherent_fields(mutate) -> None:  # noqa: ANN001
    row = {
        "task": "same", "spec_id": "sha256:" + "0" * 64,
        "device": "A", "input_mode": "text",
        "accepted_option_ids": ["speed", "voice"],
        "eligibility": {"status": "eligible", "reason": "configured"},
    }
    mutate(row)
    document = {
        "schema": "remrun.fleet.task-routes", "version": 1, "routes": [row],
    }
    with pytest.raises(TaskContractError):
        validate_task_routes_document(document)


def test_new_route_schema_rejects_duplicate_task_device_pair() -> None:
    row = {
        "task": "same", "spec_id": "sha256:" + "0" * 64,
        "device": "A", "input_mode": "text", "accepted_option_ids": [],
        "eligibility": {"status": "eligible", "reason": "configured"},
    }
    document = {
        "schema": "remrun.fleet.task-routes", "version": 1,
        "routes": [row, deepcopy(row)],
    }
    with pytest.raises(TaskContractError, match="duplicate task/device route"):
        validate_task_routes_document(document)


def test_route_projection_court_1_covers_static_eligibility_and_options(tmp_path: Path) -> None:
    raw = _definition("text")
    raw["options"] = {
        "voice": {"type": "string", "required": False, "default": "neutral"},
        "speed": {"type": "number", "required": False, "default": 1.0},
    }
    adapter = raw["adapters"].pop("BOX")
    raw["adapters"] = {"OPTIONED": adapter, "DISABLED": deepcopy(adapter),
                        "MALFORMED": deepcopy(adapter)}
    config = SimpleNamespace(
        repo_root=tmp_path,
        devices={
            "MALFORMED": SimpleNamespace(enabled="yes"),
            "MISSING": SimpleNamespace(enabled=True),
            "DISABLED": SimpleNamespace(enabled=False),
            "OPTIONED": SimpleNamespace(enabled=True),
        },
        fleet_tasks={"optioned.task": raw},
    )
    spec = resolve_tasks(config)["optioned.task"]
    document = task_routes_document({"optioned.task": spec}, config.devices)
    assert [(row["device"], row["eligibility"]) for row in document["routes"]] == [
        ("DISABLED", {"status": "ineligible", "reason": "disabled"}),
        ("MALFORMED", {"status": "ineligible", "reason": "malformed"}),
        ("MISSING", {"status": "ineligible", "reason": "missing_adapter"}),
        ("OPTIONED", {"status": "eligible", "reason": "configured"}),
    ]
    assert all(row["accepted_option_ids"] == ["speed", "voice"]
               for row in document["routes"])
    validate_task_routes_document(document)

    reversed_config = SimpleNamespace(
        repo_root=tmp_path, devices=dict(reversed(list(config.devices.items()))),
        fleet_tasks={"optioned.task": deepcopy(raw)},
    )
    reversed_config.fleet_tasks["optioned.task"]["adapters"] = {
        key: value for key, value in reversed(
            reversed_config.fleet_tasks["optioned.task"]["adapters"].items()
        )
    }
    assert task_routes_document(resolve_tasks(reversed_config), reversed_config.devices) == document


def test_route_projection_court_2_fails_closed_on_stale_resolved_identity(tmp_path: Path) -> None:
    config = _config(tmp_path, {"generic.task": "text"})
    spec = resolve_tasks(config)["generic.task"]
    stale = deepcopy(spec)
    stale["spec_id"] = "sha256:" + "0" * 64
    with pytest.raises(TaskContractError, match="spec_id"):
        task_routes_document({"generic.task": stale}, config.devices)


def test_route_projection_court_3_disabled_route_is_ineligible_to_planner_even_with_reachable_snapshot(
    tmp_path: Path,
) -> None:
    raw = _definition("text")
    raw["adapters"] = {"PAUSED": raw["adapters"]["BOX"]}
    config = SimpleNamespace(
        repo_root=tmp_path,
        devices={"PAUSED": SimpleNamespace(enabled=False, allow_explicit_run=True)},
        fleet_tasks={"generic.task": raw},
    )
    spec = resolve_tasks(config)["generic.task"]
    task = as_fleet_task(
        prepare_task_job(spec, repo_root=tmp_path, text="hello"), spec,
    )
    snapshot = DeviceSnapshot(
        name="PAUSED", reachable=True, enabled=False,
        engine_status={"unit-engine": "present"},
    )
    assert placement.fits(task, "PAUSED", snapshot, {}, 0.9) == (
        False, "disabled",
    )


def test_missing_enabled_authority_fails_closed() -> None:
    assert resolved_route_eligibility({}, "BOX", requires_adapter=False) == (
        "ineligible", "malformed",
    )
    snapshot = DeviceSnapshot(name="BOX", reachable=True)
    assert snapshot.enabled is False


def test_route_projection_court_3_disabled_device_remains_read_only_probeable(
    tmp_path: Path, monkeypatch,
) -> None:
    calls: list[str] = []

    class Transport:
        def probe(self):  # noqa: ANN201
            calls.append("probe")
            return ProbeResult(True, "PAUSED", "reachable", "posix")

    monkeypatch.setattr(probes, "probe_target_resources", lambda *_args, **_kwargs: None)
    device = Device.from_mapping("PAUSED", {
        "enabled": False, "kind": "ssh-posix", "os": "posix",
        "allow_explicit_run": True,
        "address_candidates": ["paused.example"],
        "project_root": str(tmp_path), "state_root": str(tmp_path / "state"),
        "cache_root": str(tmp_path / "cache"),
    })
    snapshot = probes.build_snapshot(device, Transport(), {}, probe_capability=False)
    assert calls == ["probe"]
    assert snapshot.enabled is False
    assert snapshot.reachable is True


def test_route_projection_court_3_disabled_route_is_rejected_by_preview_and_dispatch(
    tmp_path: Path, monkeypatch,
) -> None:
    raw = _definition("text")
    raw["adapters"] = {"PAUSED": raw["adapters"]["BOX"]}
    device = Device.from_mapping("PAUSED", {
        "enabled": False, "kind": "local-sim", "os": "posix",
        "allow_explicit_run": True,
        "address_candidates": ["localhost"],
        "project_root": str(tmp_path), "state_root": str(tmp_path / "state"),
        "cache_root": str(tmp_path / "cache"),
    })
    config = SimpleNamespace(
        repo_root=tmp_path,
        defaults={},
        devices={"PAUSED": device},
        fleet_tasks={"generic.task": raw},
    )
    spec = resolve_tasks(config)["generic.task"]
    task = as_fleet_task(
        prepare_task_job(spec, repo_root=tmp_path, text="hello"), spec,
    )
    snapshot = DeviceSnapshot(
        name="PAUSED", reachable=True, enabled=False,
        engine_status={"unit-engine": "present"},
    )
    monkeypatch.setattr(cli.probes, "build_snapshot", lambda *_args, **_kwargs: snapshot)
    monkeypatch.setattr(dispatcher.probes, "build_snapshot", lambda *_args, **_kwargs: snapshot)
    state_root = tmp_path / "state"
    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        preview = cli._route_preview(task, config, queue, state_root)
        assert preview["device"] is None
        assert preview["skipped"] == {"PAUSED": "disabled"}
        queue.enqueue_prepared(
            task.prepared, spec=spec, current_spec_id=lambda: spec["spec_id"],
        )
    finally:
        queue.close()
    summary = dispatcher.drain_once(config, state_root=state_root, max_parallel=1)
    assert summary["placed"] == 0
    assert summary["ran"] == 0


def test_route_projection_court_3_disabled_route_is_rejected_by_direct_execution(
    tmp_path: Path,
) -> None:
    from remrun.fleet.prepared import RAW_COMMAND_SPEC, RAW_COMMAND_SPEC_ID, prepare_raw_command

    config = SimpleNamespace(
        repo_root=tmp_path,
        devices={"PAUSED": SimpleNamespace(enabled=False, allow_explicit_run=True)},
    )
    task = as_fleet_task(
        prepare_raw_command(["echo", "must-not-run"], device="PAUSED"),
        {**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID},
    )
    result = executor.run_batch("PAUSED", [task], config, state_root=tmp_path / "state")
    assert result == {
        "ok": False, "device": "PAUSED", "error": "route is ineligible: disabled",
    }


def test_route_projection_court_3_explicit_only_device_never_runs_fleet_reclaim(
    tmp_path: Path, monkeypatch,
) -> None:
    device = Device.from_mapping("PAUSED", {
        "enabled": False,
        "allow_explicit_run": True,
        "kind": "ssh-posix",
        "os": "posix",
        "project_root": str(tmp_path),
        "state_root": str(tmp_path / "state"),
        "cache_root": str(tmp_path / "cache"),
        "reclaim": {"command": ["true"]},
    })
    config = SimpleNamespace(devices={"PAUSED": device})
    monkeypatch.setattr(
        dispatcher,
        "_run_device_reclaim",
        lambda *_args, **_kwargs: pytest.fail("disabled device reclaim ran"),
    )

    dispatcher._reclaim_marginal_devices(
        config,
        groups=[],
        snap_cache={},
        lease_used={},
        active_batches={},
        fcfg={},
        profs={},
        sf=0.9,
        reporter=SimpleNamespace(event=lambda *_args, **_kwargs: None),
    )


def test_route_projection_court_4_accepts_arbitrary_configured_adapter_without_branches(
    tmp_path: Path,
) -> None:
    raw = _definition("text")
    raw["adapters"] = {"NOVEL-DEVICE": raw["adapters"].pop("BOX")}
    config = SimpleNamespace(
        repo_root=tmp_path,
        devices={"NOVEL-DEVICE": SimpleNamespace(enabled=True)},
        fleet_tasks={"novel.workflow": raw},
    )
    document = task_routes_document(resolve_tasks(config), config.devices)
    assert document["routes"] == [{
        "task": "novel.workflow",
        "spec_id": document["routes"][0]["spec_id"],
        "device": "NOVEL-DEVICE",
        "input_mode": "text",
        "accepted_option_ids": [],
        "eligibility": {"status": "eligible", "reason": "configured"},
    }]


def test_route_projection_court_5_keeps_fixed_and_optioned_task_contracts_distinct(
    tmp_path: Path,
) -> None:
    optioned = _definition("text")
    optioned["options"] = {
        "voice": {"type": "string", "required": False, "default": "neutral"},
        "speed": {"type": "number", "required": False, "default": 1.0},
    }
    optioned["adapters"] = {"OPTIONED": optioned["adapters"].pop("BOX")}
    fixed = _definition("text")
    fixed["adapters"] = {"FIXED": fixed["adapters"].pop("BOX")}
    config = SimpleNamespace(
        repo_root=tmp_path,
        devices={"FIXED": SimpleNamespace(enabled=True),
                 "OPTIONED": SimpleNamespace(enabled=True)},
        fleet_tasks={"optioned.task": optioned, "fixed.task": fixed},
    )
    routes = task_routes_document(resolve_tasks(config), config.devices)["routes"]
    by_key = {(route["task"], route["device"]): route for route in routes}
    assert by_key[("optioned.task", "OPTIONED")]["accepted_option_ids"] == ["speed", "voice"]
    assert by_key[("fixed.task", "FIXED")]["accepted_option_ids"] == []
