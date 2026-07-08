"""Shared test fixtures.

The published core ships NO fleet adapters (the (task,device)->engine/worker mapping is user config
in devices.toml [fleet.adapters]). Tests provide a representative table here so the fleet code under
test has engines/commands to resolve — exactly as a real deployment's devices.toml would supply.
"""
from __future__ import annotations

import pytest

from remrun.fleet import adapters

# Representative adapters for tests only (mirror the schema; not shipped in core).
_TEST_ADAPTERS: dict[tuple[str, str], dict] = {
    ("tts", "WINBOX"): {
        "engine": "tts-remote", "output_root": r"C:\outputs\tts", "output_in_cmd": False,
        "capability": [r"C:\workers\tts-worker.ps1"],
        "cmd": ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass",
                "-File", r"C:\workers\tts-worker.ps1", "-InputDir", "{stage}"],
        "pool": "gpu", "memory_kind": "gpu"},
    ("tts", "MACBOX"): {
        "engine": "tts-local", "output_root": "~/outputs/tts", "output_in_cmd": False,
        "capability": ["~/workers/tts-worker.sh"],
        "cmd": ["~/workers/tts-worker.sh", "{stage}"],
        "pool": "gpu", "memory_kind": "gpu"},
    ("ocr", "WINBOX"): {
        "engine": "ocr-remote", "output_root": r"C:\outputs\ocr", "output_in_cmd": True,
        "capability": [r"C:\workers\ocr-worker.ps1"],
        "cmd": ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass",
                "-File", r"C:\workers\ocr-worker.ps1", "-InputDir", "{stage}",
                "-OutputRoot", "{output_root}"],
        "pool": "gpu", "memory_kind": "gpu"},
    ("ocr", "MACBOX"): {
        "engine": "ocr-local", "output_root": "~/outputs/ocr", "output_in_cmd": True,
        "capability": ["~/workers/ocr-worker.sh"],
        "cmd": ["~/workers/ocr-worker.sh", "{stage}", "{output_root}"],
        "pool": "gpu", "memory_kind": "gpu"},
}


@pytest.fixture(autouse=True)
def _fleet_adapters():
    """Populate the adapter table for each test, then clear it (the core default is empty)."""
    adapters.set_adapters(_TEST_ADAPTERS)
    yield
    adapters.set_adapters({})
