from __future__ import annotations

import json
import subprocess

from remrun.models import Device
from remrun.resource_probe import _PROBE_PROGRAM, parse_resource_probe, probe_target_resources
from remrun.transport import ExecResult, TransportError


def payload(**overrides):
    data = {
        "platform": "linux",
        "machine": "x86_64",
        "cpu": {
            "logical_cores": 20,
            "effective_cores": 18,
            "busy_pct": 7.1,
            "sample_interval_ms": 500,
        },
        "ram": {
            "total_bytes": 64 * 1024**3,
            "available_bytes": 48 * 1024**3,
        },
        "gpu": {
            "kind": "discrete",
            "devices": [
                {
                    "id": "0",
                    "name": "GPU A",
                    "util_pct": 3.0,
                    "vram_free_bytes": 10 * 1024**3,
                    "vram_total_bytes": 12 * 1024**3,
                },
                {
                    "id": "1",
                    "name": "GPU B",
                    "util_pct": 50.0,
                    "vram_free_bytes": 4 * 1024**3,
                    "vram_total_bytes": 8 * 1024**3,
                },
            ],
        },
    }
    data.update(overrides)
    return data


def test_parse_complete_snapshot_preserves_gpu_ids_and_live_metrics():
    snapshot = parse_resource_probe(json.dumps(payload()))
    assert snapshot.status == "ok"
    assert snapshot.effective_cores.value == 18
    assert snapshot.cpu_busy_pct.value == 7.1
    assert snapshot.ram_available_bytes.value == 48 * 1024**3
    assert snapshot.gpu_kind == "discrete"
    assert [gpu.id for gpu in snapshot.gpus] == ["0", "1"]
    assert snapshot.gpus[0].vram_free_bytes.value == 10 * 1024**3


def test_unified_memory_has_no_vram_values():
    data = payload(
        platform="darwin",
        machine="arm64",
        gpu={
            "kind": "unified",
            "devices": [{
                "id": "unified",
                "name": "Apple GPU",
                "util_pct": 12,
                "vram_free_bytes": 99,
                "vram_total_bytes": 100,
            }],
        },
    )
    snapshot = parse_resource_probe(json.dumps(data))
    gpu = snapshot.gpus[0]
    assert snapshot.gpu_kind == "unified"
    assert gpu.vram_free_bytes.value is None
    assert gpu.vram_free_bytes.status == "not_applicable"
    assert gpu.vram_total_bytes.value is None


def test_missing_values_stay_unknown_not_zero():
    data = payload()
    data["cpu"]["busy_pct"] = None
    data["ram"]["available_bytes"] = None
    snapshot = parse_resource_probe(json.dumps(data))
    assert snapshot.status == "partial"
    assert snapshot.cpu_busy_pct.value is None
    assert snapshot.cpu_busy_pct.status == "unavailable"
    assert snapshot.ram_available_bytes.value is None


def test_impossible_ranges_are_malformed():
    data = payload()
    data["cpu"]["busy_pct"] = 101
    data["ram"]["available_bytes"] = data["ram"]["total_bytes"] + 1
    data["gpu"]["devices"][0]["vram_free_bytes"] = (
        data["gpu"]["devices"][0]["vram_total_bytes"] + 1
    )
    snapshot = parse_resource_probe(json.dumps(data))
    assert snapshot.cpu_busy_pct.status == "malformed"
    assert snapshot.ram_available_bytes.status == "malformed"
    assert snapshot.gpus[0].vram_free_bytes.status == "malformed"


def test_non_finite_values_are_malformed_not_exceptions():
    data = payload()
    data["cpu"]["busy_pct"] = float("nan")
    data["ram"]["available_bytes"] = float("inf")
    data["gpu"]["devices"][0]["util_pct"] = float("-inf")

    snapshot = parse_resource_probe(json.dumps(data))

    assert snapshot.status == "partial"
    assert snapshot.cpu_busy_pct.status == "malformed"
    assert snapshot.ram_available_bytes.status == "malformed"
    assert snapshot.gpus[0].util_pct.status == "malformed"


def test_malformed_document_returns_explicit_snapshot():
    snapshot = parse_resource_probe("{broken")
    assert snapshot.status == "malformed"
    assert snapshot.effective_cores.value is None
    assert snapshot.gpu_kind == "unknown"


def test_partial_gpu_rows_preserve_valid_devices_and_detail():
    data = payload()
    data["gpu"]["status"] = "partial"
    data["gpu"]["detail"] = "one counter row was malformed"
    data["gpu"]["devices"].append({"id": "broken"})

    snapshot = parse_resource_probe(json.dumps(data))

    assert snapshot.status == "partial"
    assert [gpu.id for gpu in snapshot.gpus] == ["0", "1"]
    assert "malformed" in snapshot.detail
    assert "without identity" in snapshot.detail


def test_windows_effective_capacity_provenance_is_explicit():
    data = payload(platform="windows")
    data["cpu"]["effective_source"] = "process-affinity"
    data["cpu"]["effective_confidence"] = "exact"

    snapshot = parse_resource_probe(json.dumps(data))

    assert snapshot.effective_cores.source == "process-affinity"
    assert snapshot.effective_cores.confidence == "exact"
    assert "GetProcessAffinityMask" in _PROBE_PROGRAM


def test_darwin_resource_probe_uses_bounded_iostat_not_process_walking_top():
    darwin = _PROBE_PROGRAM.split("def darwin_cpu_sample():", 1)[1].split(
        "def windows_cpu_sample():", 1
    )[0]
    assert '["iostat", "-c", "2", "-w", "1"]' in darwin
    assert "top" not in darwin
    assert 'sample_interval_ms = 1000 if system == "darwin" else 500' in _PROBE_PROGRAM


class FakeTransport:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls = []

    def exec(self, command, **kwargs):
        self.calls.append((command, kwargs))
        if self.error:
            raise self.error
        return self.result


def device():
    return Device.from_mapping(
        "RUNNER",
        {"os": "linux", "remote_python": "python3"},
    )


def test_probe_uses_configured_timeout_and_remote_python():
    transport = FakeTransport(ExecResult(0, json.dumps(payload()), ""))
    snapshot = probe_target_resources(transport, device(), timeout_sec=4.5)
    assert snapshot.status == "ok"
    command, kwargs = transport.calls[0]
    assert command[:2] == ["python3", "-c"]
    assert kwargs["timeout"] == 4.5
    assert kwargs["telemetry"] is False


def test_probe_timeout_is_explicit():
    transport = FakeTransport(error=TransportError("command timed out after 5s"))
    snapshot = probe_target_resources(transport, device(), timeout_sec=5)
    assert snapshot.status == "timeout"
    assert snapshot.ram_available_bytes.status == "timeout"


def test_probe_native_timeout_is_explicit():
    transport = FakeTransport(
        error=subprocess.TimeoutExpired(["python", "-c", "probe"], timeout=5)
    )

    snapshot = probe_target_resources(transport, device(), timeout_sec=5)

    assert snapshot.status == "timeout"
    assert snapshot.ram_available_bytes.status == "timeout"


def test_probe_unexpected_failure_is_contained():
    transport = FakeTransport(error=RuntimeError("unexpected transport defect"))

    snapshot = probe_target_resources(transport, device(), timeout_sec=5)

    assert snapshot.status == "unavailable"
    assert snapshot.ram_available_bytes.status == "unavailable"
    assert "RuntimeError" in snapshot.detail


def test_probe_nonzero_is_unavailable():
    transport = FakeTransport(ExecResult(1, "", "python unavailable"))
    snapshot = probe_target_resources(transport, device(), timeout_sec=5)
    assert snapshot.status == "unavailable"
    assert "python unavailable" in snapshot.detail
