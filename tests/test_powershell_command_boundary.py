"""Focused contract tests for the ssh-powershell command-type boundary."""
from __future__ import annotations

import base64
import re
import subprocess

import pytest

from remrun.job_observation import JobObservation
from remrun.models import Device
from remrun.transport import (
    CommandNotStartedError,
    SSHPowerShellTransport,
    finalize_durable_result,
)


def _device() -> Device:
    return Device.from_mapping(
        "WINBOX",
        {
            "kind": "ssh-powershell",
            "os": "windows",
            "address_candidates": ["winbox"],
            "project_root": r"C:\projects",
            "state_root": r"C:\state",
            "cache_root": r"C:\cache",
            "remote_python": "python",
            "shell": "pwsh",
        },
    )


def _decoded(remote_command: str) -> str:
    encoded = remote_command.split("-EncodedCommand ", 1)[1].split(";", 1)[0].strip()
    return base64.b64decode(encoded).decode("utf-16-le")


def _boundary_marker(script: str, code: str) -> str:
    match = re.search(r"__REMRUN_COMMAND_BOUNDARY_[0-9a-f]{32}__", script)
    assert match is not None, script
    return match.group(0) + code + ";"


def _completed(returncode: int, *, stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(
        args=["ssh"], returncode=returncode, stdout=b"", stderr=stderr.encode()
    )


def test_context_validation_rejects_direct_cmdlet_before_reconciliation(monkeypatch):
    transport = SSHPowerShellTransport(_device())
    transport._address = "winbox"
    seen: list[str] = []

    def run(argv, input_bytes=None, timeout=None, on_stdout=None):  # noqa: ANN001
        del input_bytes, timeout, on_stdout
        script = _decoded(argv[-1])
        seen.append(script)
        marker = _boundary_marker(script, "P")
        return _completed(126, stderr=marker)

    monkeypatch.setattr(transport, "_run", run)

    with pytest.raises(CommandNotStartedError, match="direct PowerShell"):
        transport.validate_command_context(
            ["Remove-Item", "-LiteralPath", r"C:\scratch", "-Recurse", "-Force"],
            env={"PATHEXT": ".COM;.EXE;.BAT;.CMD"},
            path_prepend=[r"C:\tools"],
        )

    assert len(seen) == 1
    script = seen[0]
    lookup = "$ExecutionContext.InvokeCommand.GetCommand('Remove-Item'"
    assert lookup in script
    assert "$PSModuleAutoLoadingPreference='None'" in script
    assert script.index("$env:PATHEXT =") < script.index("$env:PATH =") < script.index(lookup)
    assert "& 'Remove-Item'" not in script


def test_runtime_guard_rejects_direct_cmdlet_before_invocation(monkeypatch):
    transport = SSHPowerShellTransport(_device())
    transport._address = "winbox"
    seen: list[str] = []

    def run(argv, input_bytes=None, timeout=None, on_stdout=None):  # noqa: ANN001
        del input_bytes, timeout, on_stdout
        script = _decoded(argv[-1])
        seen.append(script)
        marker = _boundary_marker(script, "P")
        return _completed(126, stderr=marker)

    monkeypatch.setattr(transport, "_run", run)

    with pytest.raises(CommandNotStartedError, match="direct PowerShell"):
        transport.exec(
            ["Remove-Item", "-LiteralPath", r"C:\scratch", "-Recurse", "-Force"],
            cwd=r"C:\projects\demo",
        )

    script = seen[0]
    assert script.index("GetCommand('Remove-Item'") < script.index("& 'Remove-Item'")


def test_runtime_guard_classifies_missing_command_as_not_started(monkeypatch):
    transport = SSHPowerShellTransport(_device())
    transport._address = "winbox"

    def run(argv, input_bytes=None, timeout=None, on_stdout=None):  # noqa: ANN001
        del input_bytes, timeout, on_stdout
        script = _decoded(argv[-1])
        marker = _boundary_marker(script, "N")
        return _completed(127, stderr=marker)

    monkeypatch.setattr(transport, "_run", run)

    with pytest.raises(CommandNotStartedError, match="not found at the dispatch boundary"):
        transport.exec(["missing-tool"], cwd=r"C:\projects\demo")



def test_durable_language_marker_remains_conclusive_not_started():
    marker = "__REMRUN_COMMAND_BOUNDARY_" + ("a" * 32) + "__P;"
    payload = {
        "status": {"state": "complete", "wrapper_exit_code": 126},
        "stdout_b64": "",
        "stderr_b64": base64.b64encode(marker.encode()).decode(),
    }

    with pytest.raises(CommandNotStartedError, match="direct PowerShell"):
        finalize_durable_result(
            payload,
            {"powershell_language_marker": marker, "platform": "Windows"},
        )

def test_durable_launch_revalidates_context_before_staging(monkeypatch):
    transport = SSHPowerShellTransport(_device())
    touched: list[str] = []

    def reject(command, *, env=None, path_prepend=None):  # noqa: ANN001
        assert command[0] == "Remove-Item"
        assert env == {"X": "1"}
        assert path_prepend == [r"C:\tools"]
        raise CommandNotStartedError("direct PowerShell command rejected")

    monkeypatch.setattr(transport, "validate_command_context", reject, raising=False)
    monkeypatch.setattr(
        transport,
        "_ensure_job_observer",
        lambda: touched.append("observer") or (_ for _ in ()).throw(
            AssertionError("observer staging must not run")
        ),
    )
    observation = JobObservation.for_command(
        job_id="run",
        project="demo",
        target="WINBOX",
        phase="command",
        command=["Remove-Item", "-LiteralPath", r"C:\scratch"],
        source_controller="controller",
    )

    with pytest.raises(CommandNotStartedError, match="direct PowerShell"):
        transport.launch_durable(
            ["Remove-Item", "-LiteralPath", r"C:\scratch"],
            r"C:\projects\demo",
            run_id="run",
            resume_token="resume",
            observation=observation,
            controller="controller",
            project_id="demo",
            max_log_bytes=1024,
            created_at="now",
            env={"X": "1"},
            path_prepend=[r"C:\tools"],
        )

    assert touched == []
