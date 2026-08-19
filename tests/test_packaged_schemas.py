from __future__ import annotations

import json
import tomllib
from importlib import resources
from pathlib import Path

from remrun.resource_context import select_workload
from remrun.resource_envelope import DeviceResourcePolicy, parse_device_resource_policy
from remrun.protocol import build_capabilities_document, build_error_document

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_NAMES = (
    "run-context.v1.schema.json",
    "workload-receipt.v1.schema.json",
    "capabilities.v1.schema.json",
    "error.v1.schema.json",
    "target-resource-policy.v1.schema.json",
    "target-resource-receipt.v1.schema.json",
)


def _toml(path: Path) -> dict:
    with path.open("rb") as stream:
        return tomllib.load(stream)


def _packaged_schema(name: str) -> dict:
    path = resources.files("remrun").joinpath("schemas", name)
    return json.loads(path.read_text(encoding="utf-8"))


def test_versioned_schemas_are_importable_package_data() -> None:
    run_context = _packaged_schema(SCHEMA_NAMES[0])
    receipt = _packaged_schema(SCHEMA_NAMES[1])

    assert run_context["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert run_context["properties"]["schema"]["const"] == "remrun.run-context"
    assert run_context["properties"]["version"]["const"] == 1
    assert receipt["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert receipt["properties"]["schema"]["const"] == "remrun.workload-receipt"
    assert receipt["properties"]["version"]["const"] == 1


def test_protocol_schemas_pin_and_accept_the_golden_documents() -> None:
    capabilities = _packaged_schema("capabilities.v1.schema.json")
    error = _packaged_schema("error.v1.schema.json")
    capability_document = build_capabilities_document()
    error_document = build_error_document("test failure")

    for schema, document in (
        (capabilities, capability_document),
        (error, error_document),
    ):
        assert set(schema["required"]) <= set(document)
        for name, rule in schema["properties"].items():
            if "const" in rule:
                assert document[name] == rule["const"]

    assert capabilities["properties"]["schema"]["const"] == "remrun.capabilities"
    assert capabilities["properties"]["version"]["const"] == 1
    assert capabilities["properties"]["protocol"]["properties"]["major"]["type"] == "integer"
    assert capabilities["properties"]["protocol"]["properties"]["minor"]["type"] == "integer"
    assert error["properties"]["schema"]["const"] == "remrun.error"
    assert error["properties"]["version"]["const"] == 1


def test_schema_contract_pins_receipt_values_and_unified_no_vram_offer() -> None:
    run_context = _packaged_schema(SCHEMA_NAMES[0])
    receipt = _packaged_schema(SCHEMA_NAMES[1])

    assert receipt["properties"]["status"]["enum"] == [
        "applied",
        "fallback",
        "no_op",
        "blocked",
    ]
    assert receipt["properties"]["evaluation"]["enum"] == [
        "baseline",
        "trial",
        "accepted",
        "fallback",
    ]
    non_discrete_branch = run_context["$defs"]["resourceEnvelope"]["allOf"][0]["else"]
    assert non_discrete_branch["properties"]["offered"]["properties"]["gpu"]["maxItems"] == 0
    no_vram = run_context["$defs"]["gpuStaticDeviceNoVram"]["properties"]
    assert "vram_total_bytes" not in no_vram


def test_setuptools_declares_schema_package_data() -> None:
    project = _toml(ROOT / "pyproject.toml")

    assert project["tool"]["setuptools"]["package-data"]["remrun"] == ["schemas/*.json"]


def test_target_resource_schemas_are_packaged_but_not_advertised() -> None:
    policy = _packaged_schema("target-resource-policy.v1.schema.json")
    receipt = _packaged_schema("target-resource-receipt.v1.schema.json")
    capabilities = build_capabilities_document()

    assert policy["properties"]["schema"]["const"] == "remrun.target-resource-policy"
    assert policy["properties"]["resources"]["items"]["properties"]["capacity"]["const"] == 1
    assert receipt["properties"]["schema"]["const"] == "remrun.target-resource-receipt"
    assert capabilities["features"]["target_fenced_admission"] == "unavailable"
    assert all(
        item["schema"] not in {"remrun.target-resource-policy", "remrun.target-resource-receipt"}
        for group in capabilities["documents"].values()
        for item in group
    )


def test_setuptools_reads_the_package_version_from_one_source() -> None:
    project = _toml(ROOT / "pyproject.toml")

    assert project["project"]["dynamic"] == ["version"]
    assert "version" not in project["project"]
    assert project["tool"]["setuptools"]["dynamic"]["version"] == {
        "attr": "remrun.__version__"
    }


def test_public_project_example_is_schema_1_and_explicitly_selected() -> None:
    project_config = _toml(ROOT / "examples/project/do/remrun/remrun.toml")

    assert project_config["resources"]["schema"] == 1
    assert "default_workload" not in project_config["resources"]
    workload = select_workload(project_config, "example.analysis")
    assert workload is not None
    assert workload.adapter_id == "example.resource-policy"
    assert workload.require_receipt


def test_example_device_policies_are_explicit_and_valid() -> None:
    devices = _toml(ROOT / "config/devices.example.toml")["devices"]

    mac_policy = parse_device_resource_policy(devices["macbox"]["resource_policy"])
    win_policy = parse_device_resource_policy(devices["winbox"]["resource_policy"])
    assert isinstance(mac_policy, DeviceResourcePolicy)
    assert isinstance(win_policy, DeviceResourcePolicy)
    assert mac_policy.mode == "interactive"
    assert win_policy.mode == "unattended"
    assert not mac_policy.allow_static_fallback
    assert not win_policy.allow_static_fallback


def test_inert_legacy_resource_examples_are_removed() -> None:
    public_surfaces = (
        ROOT / "docs/PROJECT_CONFIG.md",
        ROOT / "docs/CONFIGURATION.md",
        ROOT / "examples/project/do/remrun/remrun.toml",
    )

    for path in public_surfaces:
        text = path.read_text(encoding="utf-8")
        assert "[resources.default]" not in text
        assert "[resources.heavy]" not in text
        assert "[resources.workloads." in text
