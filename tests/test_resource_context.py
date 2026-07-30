from __future__ import annotations

import json
from pathlib import Path

import pytest

from remrun.resource_context import (
    MAX_RESOURCE_DOCUMENT_BYTES,
    ResourceContextError,
    WorkloadSpec,
    build_resource_envelope,
    build_run_context,
    envelope_meets_required_minimum,
    select_workload,
    validate_workload_receipt,
    write_bounded_json,
)
from remrun.resource_envelope import Metric, parse_device_resource_policy
from remrun.resource_probe import GPUResourceSnapshot, ResourceSnapshot
from remrun.models import Device


def valid_config() -> dict:
    return {
        "resources": {
            "schema": 1,
            "workloads": {
                "demo.build": {
                    "protocol": 1,
                    "adapter_id": "demo.policy",
                    "adapter_version": 2,
                    "work_unit": "target",
                    "require_envelope": False,
                    "require_receipt": True,
                }
            },
        }
    }


def test_no_selected_or_default_workload_is_exactly_inert():
    assert select_workload({}, None) is None
    assert select_workload({"resources": {"default": {"cores": 4}}}, None) is None
    assert select_workload({"resources": "malformed-but-unselected"}, None) is None


def test_explicit_and_default_workload_resolve():
    cfg = valid_config()
    explicit = select_workload(cfg, "demo.build")
    assert explicit == WorkloadSpec(
        name="demo.build",
        adapter_id="demo.policy",
        adapter_version=2,
        work_unit="target",
        require_receipt=True,
    )
    cfg["resources"]["default_workload"] = "demo.build"
    assert select_workload(cfg, None) == explicit


def test_selected_legacy_resource_examples_get_migration_error():
    with pytest.raises(ResourceContextError, match="inert legacy examples"):
        select_workload({"resources": {"default": {"cores": 4}}}, "demo")


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda cfg: cfg["resources"].update(schema=2), "schema must be 1"),
        (lambda cfg: cfg["resources"].update(workloads=[]), "workloads must be a table"),
        (lambda cfg: None, "unknown workload"),
        (
            lambda cfg: cfg["resources"]["workloads"]["demo.build"].update(protocol=2),
            "protocol must be 1",
        ),
        (
            lambda cfg: cfg["resources"]["workloads"]["demo.build"].update(adapter_version=True),
            "positive integer",
        ),
        (
            lambda cfg: cfg["resources"]["workloads"]["demo.build"].update(require_receipt="yes"),
            "must be boolean",
        ),
        (
            lambda cfg: cfg["resources"]["workloads"]["demo.build"].update(workers=4),
            "unknown keys",
        ),
    ],
)
def test_selected_workload_schema_fails_closed(mutate, match):
    cfg = valid_config()
    mutate(cfg)
    requested = "missing" if match == "unknown workload" else "demo.build"
    with pytest.raises(ResourceContextError, match=match):
        select_workload(cfg, requested)


def test_context_has_one_versioned_receipt_pointer(tmp_path: Path):
    workload = select_workload(valid_config(), "demo.build")
    assert workload is not None
    context = build_run_context(
        run_id="run-1",
        created_at="2026-07-28T23:14:55Z",
        workload=workload,
        receipt_path="/state/runs/run-1/workload-receipt.v1.json",
        resources={"schema": "remrun.resource-envelope", "version": 1},
    )
    assert context["schema"] == "remrun.run-context"
    assert context["version"] == 1
    assert context["workload"]["receipt"]["path"].startswith("/state/")
    written = write_bounded_json(tmp_path / "context.json", context)
    assert 0 < written < MAX_RESOURCE_DOCUMENT_BYTES
    assert json.loads((tmp_path / "context.json").read_text()) == context


def test_context_size_limit_fails_before_writing(tmp_path: Path):
    path = tmp_path / "too-large.json"
    with pytest.raises(ResourceContextError, match="limit"):
        write_bounded_json(path, {"x": "a" * MAX_RESOURCE_DOCUMENT_BYTES})
    assert not path.exists()


def measured(value):
    return Metric(value, "measured", "test", "exact")


def unavailable():
    return Metric(None, "unavailable", "test", "unknown")


def snapshot(*, unified=False, unavailable_live=False):
    missing = unavailable()
    return ResourceSnapshot(
        status="partial" if unavailable_live else "ok",
        platform="darwin" if unified else "linux",
        machine="arm64" if unified else "x86_64",
        logical_cores=measured(20),
        effective_cores=measured(20),
        cpu_busy_pct=missing if unavailable_live else measured(10),
        cpu_sample_interval_ms=500,
        ram_total_bytes=measured(64 * 1024**3),
        ram_available_bytes=missing if unavailable_live else measured(48 * 1024**3),
        gpu_kind="unified" if unified else "discrete",
        gpus=(
            GPUResourceSnapshot(
                id="unified" if unified else "0",
                name="Apple GPU" if unified else "GPU",
                util_pct=measured(5),
                vram_free_bytes=missing if unified else measured(10 * 1024**3),
                vram_total_bytes=missing if unified else measured(12 * 1024**3),
            ),
        ),
    )


def policy(**overrides):
    raw = {
        "schema": 1,
        "mode": "unattended",
        "probe_timeout_sec": 5,
        "cpu_reserve_cores": 1,
        "cpu_max_fraction": 1.0,
        "ram_reserve_mib": 4096,
        "ram_max_fraction": 0.9,
        "gpu_busy_ceiling_pct": 100,
        "vram_reserve_mib": 1024,
        "vram_max_fraction": 0.95,
        "allow_static_fallback": False,
    }
    raw.update(overrides)
    return parse_device_resource_policy(raw)


def device(**overrides):
    raw = {"perf_cores": 8, "eff_cores": 4, "ram_gb": 64, "vram_gb": 12}
    raw.update(overrides)
    return Device.from_mapping("RUNNER", raw)


def test_resource_envelope_applies_policy_and_preserves_multiple_kinds():
    resources = build_resource_envelope(
        snapshot=snapshot(),
        policy=policy(),
        device=device(),
        captured_at="2026-07-28T23:14:55Z",
    )
    assert resources["status"] == "ok"
    assert resources["offered"]["cpu"] == {"cores": 17, "status": "usable"}
    assert resources["offered"]["ram"]["bytes"] == 44 * 1024**3
    assert resources["offered"]["gpu"][0]["vram_bytes"] == 9 * 1024**3
    assert envelope_meets_required_minimum(resources)


def test_unified_envelope_has_no_vram_fields_or_offer():
    resources = build_resource_envelope(
        snapshot=snapshot(unified=True),
        policy=policy(),
        device=device(vram_gb=99),
        captured_at="now",
    )
    static_gpu = resources["static"]["gpu"]["devices"][0]
    live_gpu = resources["live"]["gpu"][0]
    assert resources["static"]["gpu"]["kind"] == "unified"
    assert "vram_total_bytes" not in static_gpu
    assert "vram_free_bytes" not in live_gpu
    assert resources["offered"]["gpu"] == []


def test_missing_policy_and_failed_live_probe_offer_nothing():
    resources = build_resource_envelope(
        snapshot=snapshot(unavailable_live=True),
        policy=parse_device_resource_policy(None),
        device=device(),
        captured_at="now",
    )
    assert resources["status"] == "policy_missing"
    assert resources["offered"]["cpu"]["cores"] is None
    assert not envelope_meets_required_minimum(resources)


def test_static_fallback_is_only_used_when_explicitly_allowed():
    without = build_resource_envelope(
        snapshot=snapshot(unavailable_live=True),
        policy=policy(allow_static_fallback=False),
        device=device(),
        captured_at="now",
    )
    assert without["offered"]["cpu"]["status"] == "unavailable"

    with_fallback = build_resource_envelope(
        snapshot=snapshot(unavailable_live=True),
        policy=policy(allow_static_fallback=True),
        device=device(),
        captured_at="now",
    )
    assert with_fallback["live"]["cpu"]["status"] == "configured"
    assert with_fallback["live"]["ram"]["status"] == "configured"
    assert with_fallback["offered"]["cpu"]["status"] == "usable"
    assert envelope_meets_required_minimum(with_fallback)


def receipt(workload: WorkloadSpec) -> dict:
    return {
        "schema": "remrun.workload-receipt",
        "version": 1,
        "run_id": "run-1",
        "workload": workload.name,
        "adapter_id": workload.adapter_id,
        "adapter_version": workload.adapter_version,
        "status": "applied",
        "evaluation": "accepted",
        "setting": {"outer_workers": 2},
        "constraints": {"concurrent_process_cap": 4},
        "work": {"unit": workload.work_unit, "count": 12},
        "setting_fingerprint": "sha256:abc",
        "written_at": "2026-07-28T23:18:21Z",
    }


def test_receipt_validation_is_identity_bound_and_preserves_bytes(tmp_path: Path):
    workload = select_workload(valid_config(), "demo.build")
    assert workload is not None
    path = tmp_path / "receipt.json"
    assert validate_workload_receipt(path, run_id="run-1", workload=workload).status == "missing"

    payload = receipt(workload)
    path.write_text(json.dumps(payload), encoding="utf-8")
    result = validate_workload_receipt(path, run_id="run-1", workload=workload)
    assert result.valid and result.data == payload

    before = path.read_bytes()
    payload["adapter_id"] = "other"
    path.write_text(json.dumps(payload), encoding="utf-8")
    malformed = validate_workload_receipt(path, run_id="run-1", workload=workload)
    assert malformed.status == "malformed"
    assert path.read_bytes() != before
    mismatched = path.read_bytes()
    validate_workload_receipt(path, run_id="run-1", workload=workload)
    assert path.read_bytes() == mismatched


@pytest.mark.parametrize(
    "change",
    [
        lambda data: data.update(status="unknown"),
        lambda data: data.update(evaluation="experiment"),
        lambda data: data.update(extra="typo"),
        lambda data: data.update(setting=[]),
        lambda data: data["work"].update(unit="other"),
        lambda data: data["work"].update(count=-1),
        lambda data: data["work"].update(count=float("nan")),
        lambda data: data.update(setting_fingerprint=""),
        lambda data: data.update(setting_fingerprint="md5:abc"),
    ],
)
def test_receipt_schema_rejects_malformed_fields(tmp_path: Path, change):
    workload = select_workload(valid_config(), "demo.build")
    assert workload is not None
    data = receipt(workload)
    change(data)
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    assert validate_workload_receipt(path, run_id="run-1", workload=workload).status == "malformed"
