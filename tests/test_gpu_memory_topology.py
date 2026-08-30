from __future__ import annotations

import json

import pytest

from remrun.fleet.models import FleetTask
from remrun.fleet.placement import fits
from remrun.fleet.probes import build_snapshot
from remrun.fleet.profiles import prepared_profile_key
from remrun.fleet.resources import ResourceView, _parse_posix, apply_gpu_memory_topology
from remrun.fleet.resources_render import to_dict
from remrun.gpu_topology import GpuTopologyError, resolve_gpu_memory_topology
from remrun.models import Device
from remrun.resource_context import build_resource_envelope
from remrun.resource_envelope import parse_device_resource_policy
from remrun.resource_probe import parse_resource_probe, probe_target_resources
from remrun.transport import ExecResult, ProbeResult


def _device(**overrides: object) -> Device:
    raw: dict[str, object] = {
        "os": "linux",
        "kind": "ssh-posix",
        "gpu_memory_topology": "auto",
    }
    raw.update(overrides)
    return Device.from_mapping("opaque", raw)


def _payload(
    *, gpu: dict[str, object], available_bytes: int = 100 * 1024**3,
) -> dict[str, object]:
    return {
        "platform": "linux",
        "machine": "aarch64",
        "cpu": {
            "logical_cores": 20,
            "effective_cores": 20,
            "busy_pct": 5.0,
            "sample_interval_ms": 500,
        },
        "ram": {
            "total_bytes": 127_600_592 * 1024,
            "available_bytes": available_bytes,
        },
        "gpu": gpu,
    }


def _policy():
    return parse_device_resource_policy(
        {
            "schema": 1,
            "mode": "unattended",
            "probe_timeout_sec": 5,
            "cpu_reserve_cores": 1,
            "cpu_max_fraction": 1.0,
            "ram_reserve_mib": 1024,
            "ram_max_fraction": 0.95,
            "gpu_busy_ceiling_pct": 100,
            "vram_reserve_mib": 512,
            "vram_max_fraction": 0.95,
            "allow_static_fallback": False,
        }
    )


def test_linux_nvidia_na_with_explicit_unified_preserves_raw_and_hides_vram():
    snapshot = parse_resource_probe(
        json.dumps(
            _payload(
                gpu={
                    "kind": "unknown",
                    "status": "measured",
                    "devices": [
                        {
                            "id": "gpu-0",
                            "name": "generic accelerator",
                            "util_pct": 12.0,
                            "vram_free_bytes": None,
                            "vram_total_bytes": None,
                        }
                    ],
                }
            )
        ),
        gpu_memory_topology="unified",
    )

    assert snapshot.status == "ok"
    assert snapshot.gpu_memory_topology == "unified"
    assert snapshot.gpu_kind == "unified"
    assert snapshot.gpus[0].vram_total_bytes.status == "not_applicable"
    assert snapshot.gpus[0].vram_total_bytes.value is None
    assert snapshot.raw_gpu_kind == "unknown"
    assert snapshot.raw_gpu_status == "measured"
    assert snapshot.raw_gpus[0].vram_total_bytes.status == "unavailable"
    assert snapshot.raw_gpus[0].vram_total_bytes.value is None

    envelope = build_resource_envelope(
        snapshot=snapshot,
        policy=_policy(),
        device=_device(gpu_memory_topology="unified"),
        captured_at="now",
    )
    assert envelope["static"]["gpu"]["kind"] == "unified"
    assert envelope["static"]["gpu"]["devices"] == [
        {"id": "gpu-0", "name": "generic accelerator"}
    ]
    assert "vram_free_bytes" not in envelope["live"]["gpu"][0]
    assert envelope["offered"]["gpu"] == []


def test_probe_entrypoint_applies_device_topology_declaration():
    class Transport:
        def exec(self, _command, **_kwargs):  # noqa: ANN001
            return ExecResult(
                0,
                json.dumps(
                    _payload(
                        gpu={
                            "kind": "unknown",
                            "status": "measured",
                            "devices": [
                                {
                                    "id": "gpu-0",
                                    "name": "generic accelerator",
                                    "util_pct": 12.0,
                                    "vram_free_bytes": None,
                                    "vram_total_bytes": None,
                                }
                            ],
                        }
                    )
                ),
                "",
            )

    snapshot = probe_target_resources(
        Transport(), _device(gpu_memory_topology="unified"), timeout_sec=1
    )
    assert snapshot.gpu_memory_topology == "unified"
    assert snapshot.gpus[0].vram_total_bytes.status == "not_applicable"


def test_numeric_nvidia_explicit_unified_flows_to_snapshot_and_placement():
    payload = _payload(
        available_bytes=10_000 * 1024**2,
        gpu={
            "kind": "discrete",
            "status": "measured",
            "devices": [
                {
                    "id": "gpu-0",
                    "name": "generic accelerator",
                    "util_pct": 12.0,
                    "vram_free_bytes": 10 * 1024**3,
                    "vram_total_bytes": 12 * 1024**3,
                }
            ],
        },
    )

    class Transport:
        def probe(self):  # noqa: ANN201
            return ProbeResult(True, "target", "reachable", "linux")

        def exec(self, _command, **_kwargs):  # noqa: ANN001
            return ExecResult(0, json.dumps(payload), "")

    device = _device(gpu_memory_topology="unified")
    snapshot = build_snapshot(device, Transport(), {}, probe_capability=False)
    assert snapshot.gpu_memory_topology == "unified"
    assert snapshot.vram_free_mb is None
    assert snapshot.vram_total_mb is None

    raw = parse_resource_probe(json.dumps(payload), gpu_memory_topology="unified")
    assert raw.raw_gpus[0].vram_total_bytes.value == 12 * 1024**3
    automatic = parse_resource_probe(json.dumps(payload))
    assert automatic.gpu_memory_topology == "discrete"
    assert automatic.gpu_kind == "discrete"

    task = FleetTask(
        task_name="opaque",
        prepared={
            "spec_id": "spec-1",
            "prepared_id": "prepared-1",
            "kind": "task",
            "cost": {
                "unit": "unit", "measure_id": "measure", "bucket_id": "bucket",
                "status": "exact",
            },
        },
        resolved_spec={
            "adapters": {
                "opaque": {"engine": "engine", "memory_kind": "gpu", "provides": []}
            }
        },
    )
    profiles = {
        prepared_profile_key(task, "opaque"): {
            "peak_rss_mb": 4000.0,
            "peak_vram_mb": 9000.0,
            "fixed_load_s": 1.0,
            "var_per_unit_s": 1.0,
            "n": 1,
        }
    }
    assert fits(
        task, "opaque", snapshot, profiles, 0.9, {}, allow_unknown_capability=True
    ) == (True, "ok")


@pytest.mark.parametrize(
    ("declared", "observed"),
    [
        ("unified", "discrete"),
        ("discrete", "unified"),
        ("unified", "none"),
        ("discrete", "none"),
    ],
)
def test_contradictory_topology_is_rejected(declared: str, observed: str):
    with pytest.raises(GpuTopologyError, match="declares GPU memory topology"):
        resolve_gpu_memory_topology(declared, observed, observed_authoritative=True)


def test_config_only_opaque_device_accepts_topology_and_rejects_vram_contradiction():
    device = _device(gpu_memory_topology="unified")
    assert device.gpu_memory_topology == "unified"

    with pytest.raises(ValueError, match="cannot declare separate vram_gb"):
        _device(gpu_memory_topology="unified", vram_gb=8)

    with pytest.raises(GpuTopologyError, match="must be one of"):
        _device(gpu_memory_topology="shared")


def test_authoritative_unified_observation_rejects_discrete_declaration_with_detail():
    snapshot = parse_resource_probe(
        json.dumps(
            _payload(
                gpu={
                    "kind": "unified",
                    "status": "measured",
                    "devices": [
                        {
                            "id": "shared",
                            "name": "integrated accelerator",
                            "util_pct": 2.0,
                            "vram_free_bytes": None,
                            "vram_total_bytes": None,
                        }
                    ],
                }
            )
        ),
        gpu_memory_topology="discrete",
    )
    assert snapshot.status == "partial"
    assert snapshot.gpu_memory_topology == "unknown"
    assert "declares GPU memory topology" in snapshot.detail
    assert snapshot.gpus[0].vram_total_bytes.value is None
    assert snapshot.gpus[0].vram_total_bytes.status == "unavailable"


def test_fleet_nvidia_na_retains_raw_row_and_normalizes_unified():
    view = ResourceView(name="opaque", reachable=True, os="linux")
    _parse_posix(
        "MEMINFO:MemTotal:       127600592 kB\n"
        "MEMINFO:MemAvailable:   100000000 kB\n"
        "NVIDIA:generic accelerator, 12, N/A, N/A\n",
        view,
    )
    apply_gpu_memory_topology(view, _device(gpu_memory_topology="unified"))

    rendered = to_dict(view)
    assert view.gpu_memory_topology == "unified"
    assert view.gpu_unified is True
    assert view.vram_total_mb is None
    assert rendered["gpu_raw"] == {
        "kind": "unknown",
        "status": "measured",
        "detail": "",
        "name": "generic accelerator",
        "util_pct": "12",
        "vram_free_mb": "N/A",
        "vram_total_mb": "N/A",
    }


def test_fleet_shared_gpu_row_resolves_topology_before_rendering():
    view = ResourceView(name="opaque", reachable=True, os="macos")
    _parse_posix("AGX:12\n", view)
    assert view.gpu_unified is True
    assert view.gpu_memory_topology == "unified"


def test_discrete_and_apple_topology_normalization_remains_unchanged():
    discrete = parse_resource_probe(
        json.dumps(
            _payload(
                gpu={
                    "kind": "discrete",
                    "status": "measured",
                    "devices": [
                        {
                            "id": "gpu-0",
                            "name": "generic discrete",
                            "util_pct": 2.0,
                            "vram_free_bytes": 10 * 1024**3,
                            "vram_total_bytes": 12 * 1024**3,
                        }
                    ],
                }
            )
        )
    )
    assert discrete.gpu_kind == "discrete"
    assert discrete.gpu_memory_topology == "discrete"
    assert discrete.gpus[0].vram_total_bytes.value == 12 * 1024**3

    apple = parse_resource_probe(
        json.dumps(
            _payload(
                gpu={
                    "kind": "unified",
                    "status": "measured",
                    "devices": [
                        {
                            "id": "shared",
                            "name": "integrated accelerator",
                            "util_pct": 2.0,
                            "vram_free_bytes": 10 * 1024**3,
                            "vram_total_bytes": 12 * 1024**3,
                        }
                    ],
                }
            )
        )
    )
    assert apple.gpu_kind == "unified"
    assert apple.gpu_memory_topology == "unified"
    assert apple.gpus[0].vram_total_bytes.status == "not_applicable"
