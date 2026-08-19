from __future__ import annotations

import base64
import importlib.util
from pathlib import Path
from types import SimpleNamespace

from remrun.frame import decode_frame, encode_frame
from remrun.target_resources import canonical_json


GATE_PATH = (
    Path(__file__).resolve().parents[1]
    / "native-gates"
    / "target_resources_native_gate.py"
)
SPEC = importlib.util.spec_from_file_location("target_resources_native_gate", GATE_PATH)
assert SPEC is not None and SPEC.loader is not None
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def test_native_owner_request_keeps_token_in_framed_stdin() -> None:
    reservation = SimpleNamespace(
        allocation_id="allocation",
        fence=7,
        token="private-token",
        receipt={"policy_generation": 1, "policy_digest": "a" * 64},
    )
    frame = GATE._reservation_request(
        reservation, ["python", "-c", "print('ok')"], canonical_json, encode_frame
    )
    _header, payload = decode_frame(frame)

    assert b"private-token" in payload
    source = GATE_PATH.read_text(encoding="utf-8")
    assert "reservation.token" not in source.split("def _spawn_owner", 1)[1].split(
        "def _owner_result", 1
    )[0]


def test_native_gate_injects_fault_only_into_remote_runner_environment() -> None:
    posix = GATE._faulted_remote_command(
        ["ssh", "host", "python runner.py resource-owner-run state"],
        "pause_before_resource_claim",
        "ssh-posix",
    )
    assert "REMRUN_TEST_ONLY_FAULT_POINT=pause_before_resource_claim" in posix[-1]

    script = "$ErrorActionPreference='Stop'\n& 'python' 'runner.py'\n"
    encoded = base64.b64encode(script.encode("utf-16le")).decode("ascii")
    windows = GATE._faulted_remote_command(
        ["ssh", "host", "pwsh -EncodedCommand " + encoded],
        "resource_cleanup_unknown",
        "ssh-powershell",
    )
    rewritten = base64.b64decode(windows[-1].rsplit(" ", 1)[1]).decode("utf-16le")
    assert rewritten.startswith(
        "$env:REMRUN_TEST_ONLY_FAULT_POINT='resource_cleanup_unknown'\n"
    )
    assert script in rewritten


def test_native_gate_is_isolated_and_does_not_reboot_or_activate_fleet() -> None:
    source = GATE_PATH.read_text(encoding="utf-8").lower()
    assert "--state-root" in source
    assert "--controller-root-a" in source
    assert "--controller-root-b" in source
    parsed = GATE._parser().parse_args(
        [
            "--repo", "/repo",
            "--config-root", "/config",
            "--target", "DEVICE",
            "--state-root", "/target-state",
            "--controller-root-a", "/controller-a",
            "--controller-root-b", "/controller-b",
        ]
    )
    assert parsed.controller_root_a != parsed.controller_root_b
    assert "fleetexecutor" not in source
    assert "shutdown.exe" not in source
    assert "restart-computer" not in source
