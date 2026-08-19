from __future__ import annotations

import copy
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun.fleet.task_contract import (
    TaskContractError,
    canonical_json,
    resolve_task_spec,
    resolve_tasks,
    validate_task_definition,
)


def _task() -> dict:
    return {
        "input": {
            "mode": "files",
            "extensions": [".zot", ".bin"],
            "split": "per-item",
            "file_identity": "sha256",
        },
        "prepare": {"mode": "none"},
        "routing": {"requirements": ["zot.base"], "requirements_by_option": {
            "flavor": {"plain": [], "spicy": ["zot.spice"]},
        }},
        "execution": {"batching": "compatible"},
        "cost": {
            "measure": "input-bytes",
            "unit": "mib",
            "divisor": 1048576,
            "bucket_options": ["flavor"],
        },
        "output": {
            "reservation": "content-work-stem-v1",
            "allow_root_override": False,
            "verification": "mapped-tree-change-v1",
            "missing_mapping": "final",
            "no_change": "final",
        },
        "completion": {
            "protocol": "item-result-v2",
            "evidence": "always",
            "companion": "forbidden",
            "allowed_publication": ["produced"],
            "unstructured_memory": "ignore",
        },
        "options": {
            "flavor": {"type": "string", "required": False, "default": "plain",
                       "values": ["plain", "spicy"]},
        },
        "adapters": {
            "BOX": {
                "engine": "zot-engine",
                "argv": ["/workers/zot", "{manifest}", "{output_root}", "{opt:flavor}"],
                "output_root": "/outputs/zot",
                "pool": "gpu",
                "memory_kind": "gpu",
                "capability_paths": ["/workers/zot"],
                "provides": ["zot.v1"],
            },
        },
    }


def test_novel_task_definition_is_config_only_and_content_addressed(tmp_path: Path) -> None:
    raw = _task()
    config = SimpleNamespace(
        repo_root=tmp_path,
        devices={"BOX": object()},
        fleet_tasks={"zotomatic": raw},
    )
    first = resolve_tasks(config)["zotomatic"]
    second = resolve_tasks(config)["zotomatic"]

    assert first == second
    assert first["task_name"] == "zotomatic"
    assert first["definition"]["input"]["extensions"] == [".bin", ".zot"]
    assert first["spec_id"].startswith("sha256:")
    assert first["adapters"]["BOX"]["adapter_id"].startswith("sha256:")


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda task: task.__setitem__("mystery", True), "unknown field"),
        (lambda task: task["input"].pop("mode"), "missing required"),
        (lambda task: task["input"].__setitem__("extensions", ["PNG"]), "lowercase suffix"),
        (lambda task: task["completion"].__setitem__("protocol", "maybe"), "must be one of"),
        (lambda task: task["cost"].__setitem__("divisor", float("nan")), "finite"),
        (lambda task: task["adapters"]["BOX"]["argv"].append("x={stage}"),
         "whole token"),
        (lambda task: task["options"].__setitem__(
            "argv", {"type": "string", "required": True}), "reserved option"),
    ],
)
def test_closed_schema_rejects_malformed_definitions(mutate, message: str) -> None:  # noqa: ANN001
    raw = _task()
    mutate(raw)
    with pytest.raises(TaskContractError, match=message):
        validate_task_definition("zotomatic", raw, {"BOX"})


def test_noncanonical_task_name_is_rejected() -> None:
    with pytest.raises(TaskContractError, match="invalid task name"):
        validate_task_definition("Zotomatic", _task(), {"BOX"})


def test_external_preparer_definition_is_rejected_before_resolution(tmp_path: Path) -> None:
    raw = _task()
    raw["prepare"] = {
        "mode": "external-v1",
        "process_model": "single-v1",
        "argv": ["python", "prepare.py"],
        "authority_files": ["prepare.py"],
        "timeout_s": 20,
        "max_stdout_bytes": 4096,
        "produces": ["variant"],
    }
    with pytest.raises(TaskContractError, match="prepare.*unknown field|prepare.mode"):
        resolve_task_spec("zotomatic", raw, devices={"BOX"}, repo_root=tmp_path)


def test_none_preparer_forbids_process_model() -> None:
    raw = _task()
    raw["prepare"]["process_model"] = "single-v1"
    with pytest.raises(TaskContractError, match="prepare contains unknown field.*process_model"):
        validate_task_definition("zotomatic", raw, {"BOX"})


def test_routing_requirements_by_option_are_closed_and_content_addressed(
        tmp_path: Path) -> None:
    plain = resolve_task_spec("zotomatic", _task(), devices={"BOX"}, repo_root=tmp_path)
    changed = _task()
    changed["routing"]["requirements_by_option"]["flavor"]["spicy"] = ["zot.hot"]
    spicy = resolve_task_spec("zotomatic", changed, devices={"BOX"}, repo_root=tmp_path)

    assert plain["definition"]["routing"] == {
        "requirements": ["zot.base"],
        "requirements_by_option": {"flavor": {"plain": [], "spicy": ["zot.spice"]}},
    }
    assert plain["spec_id"] != spicy["spec_id"]


def test_routing_option_mapping_must_cover_every_declared_value() -> None:
    raw = _task()
    raw["routing"]["requirements_by_option"]["flavor"].pop("spicy")
    with pytest.raises(TaskContractError, match="not exhaustive.*missing spicy"):
        validate_task_definition("zotomatic", raw, {"BOX"})


def test_canonical_json_rejects_nonfinite_and_nul() -> None:
    with pytest.raises(TaskContractError, match="finite"):
        canonical_json({"x": float("inf")})
    with pytest.raises(TaskContractError, match="NUL"):
        canonical_json({"x": "bad\x00value"})


def test_validation_does_not_mutate_caller_definition() -> None:
    raw = _task()
    original = copy.deepcopy(raw)
    validate_task_definition("zotomatic", raw, {"BOX"})
    assert raw == original


def test_mixed_payload_cannot_turn_an_unmeasured_modality_into_zero() -> None:
    raw = _task()
    raw["input"]["mode"] = "text-or-files"
    raw["cost"] = {
        "measure": "input-bytes", "unit": "bytes", "divisor": 1,
        "bucket_options": [],
    }
    with pytest.raises(TaskContractError, match="text-or-files requires cost.measure=none"):
        validate_task_definition("zotomatic", raw, {"BOX"})


@pytest.mark.parametrize("mode", ["text", "none"])
def test_item_count_rejects_non_file_modalities(mode: str) -> None:
    raw = _task()
    raw["input"] = {"mode": mode, "split": "never"}
    raw["cost"] = {
        "measure": "item-count", "unit": "items", "divisor": 1,
        "bucket_options": [],
    }
    raw["output"] = {
        "reservation": "none", "allow_root_override": False, "verification": "none",
    }
    raw["execution"] = {"batching": "never"}
    raw["completion"] = {
        "protocol": "exit-code-v1", "evidence": "never", "companion": "forbidden",
        "allowed_publication": ["none"], "unstructured_memory": "ignore",
    }
    with pytest.raises(TaskContractError, match="item-count requires file-capable input"):
        validate_task_definition("zotomatic", raw, {"BOX"})
