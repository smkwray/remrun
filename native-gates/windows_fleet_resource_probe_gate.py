#!/usr/bin/env python3
"""Native gate for Windows fleet resource control-source delivery.

Run from the candidate repository with the private device registry present::

    python native-gates/windows_fleet_resource_probe_gate.py WINDOWS_TARGET

This gate proves the fixed launcher, framed snapshot, honest probe_status, and a
degraded CIM path on one reachable ssh-powershell target without mutating
OpenSSH DefaultShell. Activation on additional hosts remains a separate court.
"""
from __future__ import annotations

import argparse
import base64
import json
import sys
from pathlib import Path


class GateFailure(RuntimeError):
    """One bounded native assertion failed."""


def _load(repo: Path):  # noqa: ANN202
    source = repo / "src"
    if not source.is_dir():
        raise GateFailure(f"repository has no src directory: {source}")
    sys.path.insert(0, str(source))
    from remrun.config import load_config
    from remrun.fleet.resources import _WINDOWS_SCRIPT, probe_device
    from remrun.fleet.resources_render import to_dict
    from remrun.transport import (
        _PS_CONTROL_SOURCE_LAUNCHER_LIMIT,
        _PS_CONTROL_SOURCE_LAUNCHER_LEN,
        _ps_control_source_bootstrap,
        _ps_control_source_frame,
        _ps_control_source_remote_command,
        make_transport,
    )

    return (
        load_config,
        probe_device,
        to_dict,
        make_transport,
        _WINDOWS_SCRIPT,
        _PS_CONTROL_SOURCE_LAUNCHER_LIMIT,
        _PS_CONTROL_SOURCE_LAUNCHER_LEN,
        _ps_control_source_frame,
        _ps_control_source_remote_command,
        _ps_control_source_bootstrap,
    )


def _device(config, requested: str):  # noqa: ANN001, ANN202
    direct = config.devices.get(requested)
    if direct is not None:
        return direct
    matches = [
        device
        for name, device in config.devices.items()
        if name.casefold() == requested.casefold()
    ]
    if len(matches) == 1:
        return matches[0]
    raise GateFailure(f"target {requested!r} is not uniquely configured")


def _decoded_bootstrap(remote: str) -> str:
    if "-EncodedCommand " not in remote:
        raise GateFailure("remote command missing EncodedCommand")
    encoded = remote.split("-EncodedCommand ", 1)[1].strip()
    return base64.b64decode(encoded.encode("ascii")).decode("utf-16-le")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "target",
        help="configured ssh-powershell device name (required; no deployment default)",
    )
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    args = parser.parse_args()
    repo = args.repo.resolve()
    (
        load_config,
        probe_device,
        to_dict,
        make_transport,
        windows_script,
        launcher_limit,
        launcher_len,
        frame_builder,
        remote_command,
        bootstrap_builder,
    ) = _load(repo)
    config = load_config(repo)
    device = _device(config, args.target)
    if device.kind != "ssh-powershell" or (device.shell or "").lower() != "pwsh":
        raise GateFailure(f"{device.name} is not an ssh-powershell pwsh target")

    if launcher_len > launcher_limit:
        raise GateFailure(
            f"control-source launcher length {launcher_len} exceeds {launcher_limit}"
        )
    nonce = "0123456789abcdef0123456789abcdef"
    remote = remote_command("pwsh", nonce)
    if len(remote) != launcher_len:
        raise GateFailure("control-source launcher length drifted from module constant")
    decoded = _decoded_bootstrap(remote)
    expected = bootstrap_builder(nonce)
    if decoded != expected:
        raise GateFailure("decoded launcher bootstrap does not match source-independent template")
    frame = frame_builder(windows_script)
    if b"RRPS1 " not in frame:
        raise GateFailure("control-source frame missing RRPS1 header")

    transport = make_transport(device)
    probe = transport.probe()
    if not probe.reachable:
        raise GateFailure(f"{device.name} unreachable: {probe.detail}")

    default_shell = ""
    try:
        result = transport.exec_control(
            [
                "reg",
                "query",
                r"HKLM\SOFTWARE\OpenSSH",
                "/v",
                "DefaultShell",
            ],
            cwd="C:\\",
            timeout=20,
        )
        if result.exit_code == 0:
            default_shell = (result.stdout or "").strip()
    except Exception as exc:  # noqa: BLE001
        default_shell = f"unreadable: {exc}"

    view = probe_device(device, transport=transport, timeout=45.0, retries=0)
    payload = to_dict(view)

    # Degraded path: shadow Get-CimInstance so core collection cannot stay healthy.
    degraded_source = (
        "function Get-CimInstance { throw 'remrun-gate-forced-cim-failure' }\n"
        + windows_script
    )
    degraded = transport.exec_control_source(degraded_source, cwd="C:\\", timeout=45.0)
    from remrun.fleet.resources import ResourceFrameError, _extract_fleet_resources_frame

    try:
        _body, degraded_meta = _extract_fleet_resources_frame(degraded.stdout or "")
    except ResourceFrameError as exc:
        raise GateFailure(f"degraded frame invalid: {exc}") from exc
    if degraded_meta.get("status") == "ok":
        raise GateFailure(
            f"degraded CIM probe still reported ok: {degraded_meta!r}"
        )

    print(
        json.dumps(
            {
                "target": device.name,
                "reachable": view.reachable,
                "probe_status": view.probe_status,
                "resource_exit_code": view.resource_exit_code,
                "launcher_chars": len(remote),
                "source_bytes": len(windows_script.encode("utf-8")),
                "frame_bytes": len(frame),
                "default_shell_reg": default_shell,
                "hostname": view.hostname,
                "cpu_count": view.cpu_count,
                "degraded_meta_status": degraded_meta.get("status"),
                "degraded_issues": degraded_meta.get("issues", [])[:8],
                "resources": payload,
            },
            indent=2,
            sort_keys=True,
        )
    )
    if not view.reachable:
        raise GateFailure(f"{device.name} auth/network failed after probe(): {view.detail}")
    if view.probe_status != "healthy":
        raise GateFailure(
            f"{device.name} probe_status={view.probe_status!r} detail={view.detail!r}"
        )
    if not view.hostname or not view.cpu_count or view.ram_total_mb is None:
        raise GateFailure(f"{device.name} healthy frame missing core fields")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except GateFailure as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
