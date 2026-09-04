"""Focused tests for Windows fleet resource framing and honest probe_status."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from remrun.fleet.resources import (
    ResourceFrameError,
    ResourceView,
    _extract_fleet_resources_frame,
    _parse_windows,
    probe_device,
)
from remrun.fleet.resources_render import _status_cell, to_dict
from remrun.models import Device
from remrun.transport import ControlSourceError, ExecResult, TransportError


def _framed(body: str, status: str = "ok", issues: list[str] | None = None) -> str:
    meta = json.dumps({"schema": 1, "status": status, "issues": issues or []})
    return (
        "__REMRUN_FLEET_RESOURCES_V1_BEGIN__\n"
        f"{body.rstrip()}\n"
        f"PROBE_META_JSON={meta}\n"
        "__REMRUN_FLEET_RESOURCES_V1_END__\n"
    )


def test_extract_frame_accepts_valid_and_rejects_duplicates():
    body, meta = _extract_fleet_resources_frame(
        _framed("HOST=BOX\nNCPU=8\n", status="partial", issues=["cpu_idle_failed"])
    )
    assert "HOST=BOX" in body
    assert "PROBE_META_JSON" not in body
    assert meta["status"] == "partial"
    assert meta["issues"] == ["cpu_idle_failed"]

    with pytest.raises(ResourceFrameError):
        _extract_fleet_resources_frame("HOST=BOX\n")
    with pytest.raises(ResourceFrameError):
        _extract_fleet_resources_frame(
            _framed("HOST=BOX\n") + _framed("HOST=OTHER\n")
        )


def test_windows_parser_still_reads_inner_keys():
    view = ResourceView(name="w", reachable=True, os="windows")
    _parse_windows(
        "HOST=BOX\nNCPU=4\nMEMTOTAL_KB=1048576\nRAM_AVAIL_MB=100\n"
        "CPU_IDLE=40\nCPU_QUEUE=1\n",
        view,
    )
    assert view.hostname == "BOX"
    assert view.cpu_count == 4
    assert view.cpu_busy_pct == 60.0
    assert view.ram_total_mb == 1024.0
    assert view.ram_free_mb == 100.0


def _windows_device(**over) -> Device:
    data = {
        "kind": "ssh-powershell",
        "os": "windows",
        "address_candidates": ["winbox"],
        "project_root": "C:\\proj",
        "state_root": "D:\\remrun\\state",
        "remote_python": "python",
        "shell": "pwsh",
        "ram_gb": 64,
        "vram_gb": 8,
    }
    data.update(over)
    return Device.from_mapping("WINBOX", data)


class FakeWindowsTransport:
    def __init__(self, *, probe_ok=True, source_result=None, source_error=None):
        self.probe_ok = probe_ok
        self.source_result = source_result
        self.source_error = source_error
        self.device = _windows_device()

    def probe(self):
        if self.probe_ok:
            return SimpleNamespace(reachable=True, address="winbox", detail="ssh ok")
        return SimpleNamespace(reachable=False, address=None, detail="permission denied")

    def exec_control_source(self, source, cwd, *, timeout=None, max_source_bytes=None):
        del source, cwd, timeout, max_source_bytes
        if self.source_error is not None:
            raise self.source_error
        return self.source_result


def test_probe_device_windows_healthy_frame():
    body = (
        "HOST=WIN\nNCPU=8\nMEMTOTAL_KB=16777216\nRAM_AVAIL_MB=2048\n"
        "CPU_IDLE=50\n"
        'DISK_JSON={"mount":"C:","total_bytes":"1000","available_bytes":"400",'
        '"semantics":"allocated-used","source":"Win32_LogicalDisk"}\n'
    )
    transport = FakeWindowsTransport(
        source_result=ExecResult(0, _framed(body), "")
    )
    view = probe_device(_windows_device(), transport=transport, retries=0)
    assert view.reachable is True
    assert view.probe_status == "healthy"
    assert view.hostname == "WIN"
    assert view.cpu_count == 8
    assert view.resource_exit_code == 0
    assert to_dict(view)["probe_status"] == "healthy"


def test_probe_device_delivery_failure_is_not_blank_healthy():
    transport = FakeWindowsTransport(
        source_error=ControlSourceError(
            "remote command exceeded the host command-line boundary",
            phase="delivery",
            started=False,
            exit_code=1,
        )
    )
    view = probe_device(_windows_device(), transport=transport, retries=0)
    assert view.reachable is True
    assert view.probe_status == "command_boundary_failed"
    assert view.hostname == ""
    assert view.ram_total_mb is None  # config fallback must not mask delivery failure
    assert _status_cell(view) == "command boundary failed"


def test_probe_device_protocol_error_rejects_telemetry():
    transport = FakeWindowsTransport(
        source_result=ExecResult(0, "HOST=WIN\nNCPU=8\n", "")
    )
    view = probe_device(_windows_device(), transport=transport, retries=0)
    assert view.reachable is True
    assert view.probe_status == "protocol_error"
    assert view.hostname == ""


def test_probe_device_timeout_transport_maps_resource_timeout():
    transport = FakeWindowsTransport(
        source_error=TransportError("ssh timed out after 45.0s")
    )
    view = probe_device(_windows_device(), transport=transport, retries=0)
    assert view.reachable is True
    assert view.probe_status == "resource_timeout"


def test_config_fallback_only_after_valid_frame():
    body = (
        "HOST=WIN\nNCPU=2\nMEMTOTAL_KB=\nRAM_AVAIL_MB=100\nCPU_IDLE=10\n"
        'DISK_JSON={"mount":"C:","total_bytes":"1000","available_bytes":"400",'
        '"semantics":"allocated-used","source":"Win32_LogicalDisk"}\n'
    )
    transport = FakeWindowsTransport(
        source_result=ExecResult(
            0, _framed(body, status="partial", issues=["memtotal_missing"]), ""
        )
    )
    view = probe_device(_windows_device(ram_gb=32), transport=transport, retries=0)
    assert view.probe_status == "partial"
    assert view.ram_total_mb == 32 * 1024.0
    assert "ram_total from config" in view.notes


def test_ok_frame_missing_core_fields_is_protocol_error():
    body = "HOST=WIN\nNCPU=8\nCPU_IDLE=50\n"  # missing RAM + disk
    transport = FakeWindowsTransport(
        source_result=ExecResult(0, _framed(body, status="ok"), "")
    )
    view = probe_device(_windows_device(ram_gb=32), transport=transport, retries=0)
    assert view.probe_status == "protocol_error"
    assert view.ram_total_mb is None  # config fallback must not fill a protocol error
    assert "ram_total from config" not in view.notes


def test_local_malformed_windows_frame_is_protocol_error(monkeypatch):
    from remrun.fleet import local_resources as local_mod

    monkeypatch.setattr(local_mod.os, "name", "nt")
    monkeypatch.setattr(local_mod.os, "cpu_count", lambda: 16)
    monkeypatch.setattr(local_mod.shutil, "which", lambda *_a, **_k: "pwsh")
    monkeypatch.setattr(local_mod, "_run", lambda *_a, **_k: "HOST=FROMFRAME\nNCPU=4\n")
    view = local_mod.local_view("CONTROLLER")
    assert view.probe_status == "protocol_error"
    assert view.hostname != "FROMFRAME"
    assert view.cpu_count == 16
