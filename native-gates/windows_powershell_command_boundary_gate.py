#!/usr/bin/env python3
"""Concise native gate for the ssh-powershell command-type boundary.

Run from the candidate repository with the private device registry present::

    python native-gates/windows_powershell_command_boundary_gate.py WINBOX

The gate does not revisit the already-proved batch surface. It proves exact native
and positional-data .ps1 argv, then proves a named-parameter cmdlet is rejected
both by the direct transport and by ordinary CLI preflight without an UNKNOWN fence.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path


class GateFailure(RuntimeError):
    """One bounded native assertion failed."""


def _load(repo: Path):  # noqa: ANN202
    source = repo / "src"
    if not source.is_dir():
        raise GateFailure(f"repository has no src directory: {source}")
    sys.path.insert(0, str(source))
    from remrun.cli import EXIT_INFRA
    from remrun.config import load_config
    from remrun.transport import CommandNotStartedError, make_transport

    return EXIT_INFRA, load_config, CommandNotStartedError, make_transport


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


def _events(text: str) -> list[dict]:
    rows: list[dict] = []
    for line in text.splitlines():
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _assert_exact(result, expected: list[str], label: str) -> None:  # noqa: ANN001
    if result.exit_code != 0:
        raise GateFailure(f"{label} failed with exit {result.exit_code}: {result.stderr}")
    try:
        actual = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise GateFailure(f"{label} returned malformed JSON: {result.stdout!r}") from exc
    if actual != expected:
        raise GateFailure(f"{label} argv mismatch: expected={expected!r} actual={actual!r}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", nargs="?", default="WINBOX")
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument(
        "--project",
        type=Path,
        help="configured local project used only for the CLI preflight refusal",
    )
    args = parser.parse_args()
    repo = args.repo.resolve()
    project = (args.project or repo).resolve()
    if not project.is_dir():
        raise GateFailure(f"CLI test project is not a directory: {project}")
    EXIT_INFRA, load_config, CommandNotStartedError, make_transport = _load(repo)
    config = load_config(repo)
    device = _device(config, args.target)
    if device.kind != "ssh-powershell" or (device.shell or "").lower() != "pwsh":
        raise GateFailure(f"{device.name} is not an ssh-powershell pwsh target")

    transport = make_transport(device)
    probe = transport.probe()
    if not probe.reachable:
        raise GateFailure(f"{device.name} is unreachable: {probe.detail}")

    remote_root = transport.remote_temp_dir("remrun-powershell-boundary")
    marker = transport.native_join(remote_root, f"must-remain-{uuid.uuid4().hex}.txt")
    adversarial = [
        "",
        "space value",
        'quote"inside',
        "quote'inside",
        "trailing\\",
        "&",
        "|",
        "<",
        ">",
        "%PATH%",
        "^",
        "(group)!",
        "-LiteralPath",
    ]
    try:
        py = device.remote_python or "python"
        native_source = "import json,sys;print(json.dumps(sys.argv[1:]))"
        _assert_exact(
            transport.exec([py, "-c", native_source, *adversarial], cwd=remote_root),
            adversarial,
            "native application",
        )

        ps1_source = (
            "[Console]::Out.Write((ConvertTo-Json -Compress "
            "-InputObject ([string[]]$args)))\n"
        )
        with tempfile.TemporaryDirectory(prefix="remrun-pwsh-boundary-") as temp_dir:
            local_ps1 = Path(temp_dir) / "argv_probe.ps1"
            local_ps1.write_text(ps1_source, encoding="utf-8")
            remote_ps1 = transport.native_join(remote_root, "argv_probe.ps1")
            transport.push_file(local_ps1, remote_ps1)
        _assert_exact(
            transport.exec([remote_ps1, *adversarial], cwd=remote_root),
            adversarial,
            ".ps1 positional-data",
        )

        create_marker = (
            "import pathlib,sys;pathlib.Path(sys.argv[1]).write_text('must-remain')"
        )
        created = transport.exec([py, "-c", create_marker, marker], cwd=remote_root)
        if created.exit_code != 0:
            raise GateFailure(f"marker setup failed: {created.stderr}")

        cmdlet = ["Remove-Item", "-LiteralPath", marker, "-Force"]
        try:
            transport.exec(cmdlet, cwd=remote_root)
        except CommandNotStartedError as exc:
            if "direct PowerShell" not in str(exc):
                raise GateFailure(f"wrong direct refusal: {exc}") from exc
        else:
            raise GateFailure("direct cmdlet was not rejected")
        if transport.read_small_file(marker, 64) != b"must-remain":
            raise GateFailure("direct cmdlet refusal did not preserve the marker")

        cli_env = os.environ.copy()
        cli_env["PYTHONPATH"] = str(repo / "src")
        cli_env["REMRUN_ROOT"] = str(repo)
        with tempfile.TemporaryDirectory(prefix="remrun-pwsh-state-") as state_dir:
            cli_env["REMRUN_STATE_ROOT"] = state_dir
            proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "remrun.cli",
                    "run",
                    "--json",
                    device.name,
                    "--",
                    *cmdlet,
                ],
                cwd=project,
                env=cli_env,
                text=True,
                capture_output=True,
                check=False,
            )
        rows = _events(proc.stdout + "\n" + proc.stderr)
        names = [row.get("event") for row in rows]
        if proc.returncode != EXIT_INFRA:
            raise GateFailure(
                f"CLI refusal exit {proc.returncode}, expected {EXIT_INFRA}: {proc.stderr}"
            )
        rejected = [row for row in rows if row.get("event") == "command_rejected"]
        if not rejected or rejected[-1].get("phase") != "preflight":
            raise GateFailure(f"CLI emitted no preflight command_rejected event: {rows!r}")
        forbidden = {"command_dispatch", "command_started", "completion_unknown"}
        if forbidden.intersection(names):
            raise GateFailure(f"CLI emitted a false start/UNKNOWN event: {names!r}")
        if transport.read_small_file(marker, 64) != b"must-remain":
            raise GateFailure("CLI cmdlet refusal did not preserve the marker")
    finally:
        transport.remove_remote_tree(remote_root)

    print(
        f"{device.name} PowerShell boundary gate passed: "
        "native+.ps1 exact, direct cmdlet no-start, CLI no UNKNOWN"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except GateFailure as exc:
        print(f"native gate failed: {exc}", file=sys.stderr)
        raise SystemExit(2)
