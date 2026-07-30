from __future__ import annotations

from pathlib import Path

import pytest

from remrun.config import load_config
from remrun.memory_guard import MemoryGuardConfigError, parse_memory_guard
from remrun.models import Device
from remrun.transport import LocalSimTransport


def test_valid_guard_is_strict_relative_policy():
    guard = parse_memory_guard(
        {
            "schema": 2,
            "command_limit_fraction": 0.3125,
            "host_reserve_fraction": 0.3125,
        },
        ram_gb=64,
        device_name="RUNNER",
        max_jobs=2,
        device_kind="ssh-posix",
        device_os="macos",
    )

    assert guard is not None
    assert guard.command_limit_fraction == 0.3125
    assert guard.host_reserve_fraction == 0.3125
    assert guard.max_jobs == 2
    assert guard.as_dict() == {
        "schema": 2,
        "command_limit_fraction": 0.3125,
        "host_reserve_fraction": 0.3125,
    }


@pytest.mark.parametrize(
    ("raw", "max_jobs", "kind", "os_name", "match"),
    [
        ({"schema": 2, "command_limit_fraction": 0.25}, 2, "ssh-posix", "macos", "missing key"),
        (
            {
                "schema": 2,
                "command_limit_fraction": 0.25,
                "host_reserve_fraction": 0.25,
                "disable": True,
            },
            2,
            "ssh-posix",
            "macos",
            "unknown key",
        ),
        (
            {
                "schema": 1,
                "command_limit_fraction": 0.25,
                "host_reserve_fraction": 0.25,
            },
            2,
            "ssh-posix",
            "macos",
            "schema must be 2",
        ),
        (
            {
                "schema": 2,
                "command_limit_fraction": True,
                "host_reserve_fraction": 0.25,
            },
            2,
            "ssh-posix",
            "macos",
            "finite fraction",
        ),
        (
            {
                "schema": 2,
                "command_limit_fraction": 0.75,
                "host_reserve_fraction": 0.50,
            },
            2,
            "ssh-posix",
            "macos",
            "exceeds RAM",
        ),
        (
            {
                "schema": 2,
                "command_limit_fraction": 0.25,
                "host_reserve_fraction": 0.25,
            },
            0,
            "ssh-posix",
            "macos",
            "max_jobs must be a positive integer",
        ),
        (
            {
                "schema": 2,
                "command_limit_fraction": 0.25,
                "host_reserve_fraction": 0.25,
            },
            2,
            "ssh-powershell",
            "windows",
            "not proved on Windows",
        ),
    ],
)
def test_invalid_guard_is_rejected(raw, max_jobs, kind, os_name, match):
    with pytest.raises(MemoryGuardConfigError, match=match):
        parse_memory_guard(
            raw,
            ram_gb=64,
            device_name="RUNNER",
            max_jobs=max_jobs,
            device_kind=kind,
            device_os=os_name,
        )


def test_load_config_rejects_old_absolute_schema(tmp_path: Path):
    root = tmp_path / "remrun"
    (root / "config").mkdir(parents=True)
    (root / "config" / "defaults.toml").write_text("", encoding="utf-8")
    (root / "config" / "devices.toml").write_text(
        "[devices.RUNNER]\n"
        'kind = "ssh-posix"\n'
        'os = "macos"\n'
        "max_jobs = 2\n"
        "[devices.RUNNER.memory_guard]\n"
        "schema = 1\n"
        "max_command_mib = 20480\n"
        "min_available_mib = 20480\n",
        encoding="utf-8",
    )

    with pytest.raises(MemoryGuardConfigError, match="missing key|unknown key"):
        load_config(root)


def test_directly_constructed_transport_cannot_bypass_guard_validation(tmp_path: Path):
    device = Device(
        name="RUNNER",
        enabled=True,
        role="runner",
        kind="local-sim",
        os="posix",
        address_candidates=[],
        project_root=str(tmp_path),
        state_root=str(tmp_path / "state"),
        cache_root=str(tmp_path / "cache"),
        max_jobs=2,
        ram_gb=64,
        memory_guard={"schema": 2, "command_limit_fraction": 0.25},
    )

    with pytest.raises(MemoryGuardConfigError, match="missing key"):
        LocalSimTransport(device)
