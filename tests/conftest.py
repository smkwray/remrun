"""Shared test fixtures for configuration-owned fleet tasks."""


import os
from pathlib import Path

from remrun.models import Device


def native_target_device(root: Path, name: str = "TARGET") -> Device:
    """A target device whose declared OS matches the paths built from `root`.

    Target roots in these tests are built from pytest's native `tmp_path`, while
    the device's OS family is declared by hand. `_target_state_root` validates
    absoluteness with `PureWindowsPath` or `PurePosixPath` according to
    `device.os`, so a hand-written `posix` declaration cannot validate a
    `C:\\...` root and every recovery assertion fails on Windows before the
    behaviour under test runs.

    That mismatch was introduced independently three times. Construct the device
    here so there is no fourth. The production check is correct and must not be
    relaxed to accept a controller-native path regardless of declared target.
    """
    windows = os.name == "nt"
    return Device.from_mapping(
        name,
        {
            "kind": "ssh-powershell" if windows else "ssh-posix",
            "os": "windows" if windows else "posix",
            "project_root": str(root / "projects"),
            "state_root": str(root / "target-state"),
            "cache_root": str(root / "cache"),
        },
    )
