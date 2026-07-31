"""Unit tests for SSHPowerShellTransport script construction (ssh mocked).

The backend is not live-tested in this environment; these tests pin the exact
PowerShell scripts (decoded back from -EncodedCommand) and transfer framing so a
later live validation on a Windows runner has a precise contract to check.
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
import re
import subprocess
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun.job_observation import JobObservation
from remrun.memory_guard import MemoryGuardConfigError
from remrun.models import Device
from remrun.transport import (
    SSHPowerShellTransport,
    TelemetryRequest,
    TransportError,
    _extract_windows_exit_marker,
    _ps_command_argv,
    _ps_encode,
    _ps_squote,
)


def device(**over) -> Device:
    data = {
        "kind": "ssh-powershell",
        "os": "windows",
        "address_candidates": ["winbox", "WINBOX.local"],
        "project_root": "C:\\Users\\you\\projects",
        "state_root": "D:\\remrun\\state",
        "cache_root": "D:\\remrun\\cache",
        "remote_python": "python",
        "shell": "pwsh",
    }
    data.update(over)
    return Device.from_mapping("WINBOX", data)


def cp(returncode=0, stdout=b"", stderr=b"") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=["ssh"], returncode=returncode,
                                       stdout=stdout, stderr=stderr)


class Recorder:
    def __init__(self, responder):
        self.calls = []
        self.responder = responder

    def __call__(self, argv, input_bytes=None, timeout=None, on_stdout=None):
        self.calls.append({"argv": argv, "input": input_bytes, "timeout": timeout,
                           "on_stdout": on_stdout})
        result = self.responder(argv, input_bytes)
        # Mirror the real transport: when the caller asked for live output, feed it
        # the same bytes the buffered path would have returned at exit.
        if on_stdout is not None and getattr(result, "stdout", None):
            on_stdout(result.stdout.decode("utf-8", "replace"))
        return result

    @property
    def commands(self):
        return [c["argv"][-1] for c in self.calls]


def decoded(cmd: str) -> str:
    """Recover the PowerShell script from a `... -EncodedCommand <b64>` command."""
    b64 = cmd.split("-EncodedCommand ", 1)[1].split(";", 1)[0].strip()
    return base64.b64decode(b64).decode("utf-16-le")


def nested_job_script(outer_script: str) -> tuple[str, str]:
    """Recover the target shell and its encoded command from the Job invocation."""
    match = re.search(
        r"'--' '(powershell|pwsh)' '-NoProfile' '-NonInteractive' "
        r"'-EncodedCommand' '([A-Za-z0-9+/=]+)'",
        outer_script,
    )
    assert match is not None, outer_script
    return match.group(1), base64.b64decode(match.group(2)).decode("utf-16-le")


# --- helpers ------------------------------------------------------------------

def test_ps_squote_escapes_quotes():
    assert _ps_squote("a'b") == "'a''b'"
    assert _ps_squote("C:\\x") == "'C:\\x'"


def test_ps_encode_roundtrip():
    assert base64.b64decode(_ps_encode("Write-Output 1")).decode("utf-16-le") == "Write-Output 1"


def test_extract_windows_exit_marker_requires_terminated_record():
    marker = "__REMRUN_EXIT_test__"
    for stderr in (
        f"user {marker}",
        f"user {marker}7",
        f"user {marker}-;",
        f"user {marker}not-a-number;",
    ):
        assert _extract_windows_exit_marker(stderr, marker) == (stderr, None)


def test_extract_windows_exit_marker_preserves_user_stderr_and_trailing_digits():
    marker = "__REMRUN_EXIT_test__"
    user_spoof = f"before {marker}91; still-user "
    stderr = user_spoof + marker + "7;123 after"

    cleaned, exit_code = _extract_windows_exit_marker(stderr, marker)

    assert exit_code == 7
    assert cleaned == user_spoof + "123 after"


def test_extract_windows_exit_marker_ignores_other_private_nonce():
    stderr = "user __REMRUN_EXIT_other__91; output"
    assert _extract_windows_exit_marker(stderr, "__REMRUN_EXIT_expected__") == (
        stderr,
        None,
    )


def test_ps_command_argv_preserves_tokens_inside_one_encoded_shell_command():
    command = [
        "C:\\Tools\\build.exe",
        "",
        "space value",
        "quote'value",
        "literal&pipe|redirect<out>",
        "100%",
        "caret^x",
        "(group)!",
    ]

    argv = _ps_command_argv(
        "pwsh",
        command,
        inherited_path_var="__REMRUN_INHERITED_PATH_test",
    )
    script = base64.b64decode(argv[-1]).decode("utf-16-le")

    assert argv[:-1] == ["pwsh", "-NoProfile", "-NonInteractive", "-EncodedCommand"]
    assert "& " + " ".join(_ps_squote(token) for token in command) in script
    assert "$remrunCommandExitCode = $LASTEXITCODE" in script
    assert "if ($remrunCommandSucceeded) { exit 0 }" in script
    assert "$env:PATH = $env:__REMRUN_INHERITED_PATH_test" in script
    assert "$env:__REMRUN_INHERITED_PATH_test = $null" in script


# --- probe / --auto failover (Windows path; not live-tested here) -------------

def test_probe_rejects_windows_powershell_before_connecting(monkeypatch):
    t = SSHPowerShellTransport(device(shell="powershell"))
    rec = Recorder(lambda argv, inp: cp(0))
    monkeypatch.setattr(t, "_run", rec)

    probe = t.probe()

    assert not probe.reachable
    assert probe.address is None
    assert probe.remote_os == "windows"
    assert "cannot preserve arbitrary native and batch argv" in probe.detail
    assert "shell='pwsh'" in probe.detail
    assert rec.calls == []


def test_probe_fails_over_when_first_candidate_blackholes(monkeypatch):
    # WHY: a blackholed first address (probe times out -> TransportError) must not
    # abort --auto failover; probe() must try the next candidate and land on it.
    t = SSHPowerShellTransport(device())  # candidates: winbox, then WINBOX.local

    def responder(argv, _inp):
        if "WINBOX.local" not in " ".join(argv):
            raise TransportError("ssh timed out after 30s")  # first candidate blackholes
        return cp(0, b"remrun-ok\nWin32NT\nC:\\Users\\you\n7.6.2\n")

    monkeypatch.setattr(t, "_run", Recorder(responder))
    probe = t.probe()
    assert probe.reachable
    assert probe.address == "WINBOX.local"
    assert probe.remote_os == "windows"


def test_probe_rejects_pwsh_older_than_native_argv_fix(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh", address_candidates=["winbox"]))
    rec = Recorder(
        lambda argv, inp: cp(0, b"remrun-ok\nWin32NT\nC:\\Users\\you\n7.2.24\n")
    )
    monkeypatch.setattr(t, "_run", rec)

    probe = t.probe()

    assert not probe.reachable
    assert probe.address is None
    assert "requires pwsh 7.3+" in probe.detail
    assert len(rec.calls) == 1


def test_probe_all_candidates_timeout_returns_unreachable_not_raise(monkeypatch):
    t = SSHPowerShellTransport(device())

    def boom(argv, _inp):
        raise TransportError("ssh timed out after 30s")

    monkeypatch.setattr(t, "_run", Recorder(boom))
    probe = t.probe()  # must not raise
    assert not probe.reachable
    assert probe.address is None
    assert "timed out" in probe.detail


def test_delete_remote_verifies_absence_and_fails_loud(monkeypatch):
    # WHY: the old delete used -ErrorAction SilentlyContinue and only treated ssh-255 as
    # failure, so a locked / ACL-denied remote file was reported deleted while it still
    # existed (and the run continued against a stale file).
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    seen = {}

    def responder(argv, _inp):
        seen["cmd"] = argv[-1]
        return cp(0)

    monkeypatch.setattr(t, "_run", Recorder(responder))
    t.delete_remote("C:\\proj\\out.rds")
    script = decoded(seen["cmd"])
    assert "SilentlyContinue" not in script   # no longer swallows the error
    assert "Test-Path" in script              # verifies the path is actually gone

    # A non-zero result (still present / access denied) must raise, not silently pass.
    monkeypatch.setattr(t, "_run", Recorder(lambda a, i: cp(1, stderr=b"still present")))
    with pytest.raises(TransportError):
        t.delete_remote("C:\\proj\\out.rds")


def test_read_small_file_caps_remotely_and_returns_binary(monkeypatch):
    payload = b"\x00receipt\xff"
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    rec = Recorder(lambda argv, inp: cp(0, base64.b64encode(payload)))
    monkeypatch.setattr(t, "_run", rec)

    assert t.read_small_file("D:\\state\\run receipt.json", 64) == payload
    script = decoded(rec.commands[-1])
    assert "[IO.File]::Exists($p)" in script
    assert "$info.Length -gt $limit" in script
    assert "$memory.Length + $read" in script
    assert "[Convert]::ToBase64String($bytes)" in script


@pytest.mark.parametrize(
    ("returncode", "marker", "message"),
    [
        (44, "__REMRUN_SMALL_FILE_MISSING__", "missing"),
        (45, "__REMRUN_SMALL_FILE_OVERSIZE__", "exceeds 64 byte limit"),
    ],
)
def test_read_small_file_reports_missing_and_oversize(
    monkeypatch, returncode, marker, message
):
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    monkeypatch.setattr(
        t, "_run", Recorder(lambda argv, inp: cp(returncode, stderr=marker.encode()))
    )

    with pytest.raises(TransportError, match=message):
        t.read_small_file("D:\\state\\receipt.json", 64)


def test_read_small_file_rejects_corrupt_transfer(monkeypatch):
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    monkeypatch.setattr(t, "_run", Recorder(lambda argv, inp: cp(0, b"not base64!")))

    with pytest.raises(TransportError, match="invalid base64"):
        t.read_small_file("D:\\state\\receipt.json", 64)


# --- path mapping -------------------------------------------------------------

def test_remote_project_path_and_join():
    t = SSHPowerShellTransport(device())

    class P:
        project_id = "client/foo"
        relative_cwd = "analysis"

    rp = t.remote_project_path(P())
    assert rp == "C:\\Users\\you\\projects\\client\\foo"
    assert t.remote_join(rp, "analysis/run.py") == "C:\\Users\\you\\projects\\client\\foo\\analysis\\run.py"
    assert t.remote_join(rp, ".") == rp


# --- diagnostics --------------------------------------------------------------

def test_probe_parses_ok_and_home(monkeypatch):
    t = SSHPowerShellTransport(device())
    monkeypatch.setattr(t, "_run",
                        Recorder(lambda a, i: cp(0, b"remrun-ok\r\nWin32NT\r\nC:\\Users\\alice\r\n7.6.2\r\n")))
    probe = t.probe()
    assert probe.reachable
    assert probe.address == "winbox"
    assert probe.remote_os == "windows"
    assert t._remote_home == "C:\\Users\\alice"


def test_probe_unreachable(monkeypatch):
    t = SSHPowerShellTransport(device())
    monkeypatch.setattr(t, "_run", Recorder(lambda a, i: cp(255, stderr=b"no route")))
    assert not t.probe().reachable


# --- execution ----------------------------------------------------------------

def test_exec_builds_pwsh_script(monkeypatch):
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    rec = Recorder(lambda a, i: cp(0, b"out", b""))
    monkeypatch.setattr(t, "_run", rec)
    t.exec(
        ["python", "do/compute.py"],
        cwd="C:\\Users\\you\\projects\\remrun-test",
        env={"VIRTUAL_ENV": "C:\\Users\\alice\\venvs\\remrun-test"},
        path_prepend=["C:\\Users\\alice\\venvs\\remrun-test\\Scripts"],
    )
    cmd = rec.commands[-1]
    assert cmd.startswith("pwsh -NoProfile -NonInteractive -EncodedCommand ")
    script = decoded(cmd)
    assert "$env:VIRTUAL_ENV = 'C:\\Users\\alice\\venvs\\remrun-test'" in script
    assert "$env:PATH = 'C:\\Users\\alice\\venvs\\remrun-test\\Scripts;' + $env:PATH" in script
    assert "Set-Location -LiteralPath 'C:\\Users\\you\\projects\\remrun-test'" in script
    assert "& 'python' 'do/compute.py'" in script
    assert "[Console]::Error.Write('__REMRUN_EXIT_" in script
    assert "[string]$remrunCommandExitCode + ';'" in script
    assert "exit $remrunCommandExitCode" in script


def test_exec_shell_override_uses_pwsh(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.exec(["whoami"], cwd="C:\\x")
    assert rec.commands[-1].startswith("pwsh -NoProfile -NonInteractive -EncodedCommand ")


@pytest.mark.parametrize(
    "command",
    [
        ["C:\\Tools\\native_probe.cmd", "value&literal"],
        ["C:/Tools/NATIVE_PROBE.CMD", "%PATH%"],
        ["relative\\native_probe.bat", "!"],
    ],
)
@pytest.mark.parametrize("detailed", [False, True], ids=["direct", "detailed"])
def test_exec_rejects_explicit_batch_before_connect_staging_or_run(
    command, detailed, monkeypatch
):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    touched: list[str] = []

    def touched_address():
        touched.append("address")
        raise AssertionError("address resolution must not run")

    monkeypatch.setattr(t, "_address_or_resolve", touched_address)
    monkeypatch.setattr(t, "push_file", lambda *_args, **_kwargs: touched.append("stage"))
    monkeypatch.setattr(t, "_run", lambda *_args, **_kwargs: touched.append("run"))

    kwargs = {"telemetry_request": TelemetryRequest()} if detailed else {}
    with pytest.raises(TransportError, match=r"top-level \.cmd/\.bat"):
        t.exec(command, cwd="C:\\project", **kwargs)

    assert touched == []


def test_exec_guards_path_and_pathext_batch_resolution_before_invocation(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)

    t.exec(["native_probe", "value&literal"], cwd="C:\\project")

    script = decoded(rec.commands[-1])
    lookup = (
        "$ExecutionContext.InvokeCommand.GetCommand('native_probe', "
        "[System.Management.Automation.CommandTypes]::All)"
    )
    assert lookup in script
    assert "$remrunBatch.ResolvedCommand" in script
    assert "$remrunBatch.Path -match '\\.(cmd|bat)$'" in script
    assert "__REMRUN_BATCH_UNSUPPORTED_" in script
    cleanup = "Remove-Variable -Name remrunBatch"
    assert cleanup in script
    assert script.index(lookup) < script.index(cleanup) < script.index("& 'native_probe'")


@pytest.mark.parametrize("detailed", [False, True], ids=["direct", "detailed"])
def test_exec_remote_batch_guard_raises_before_returning_corruption(
    detailed, monkeypatch
):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    t._remote_home = "C:\\Users\\you"
    monkeypatch.setattr(
        "remrun.transport.uuid.uuid4",
        lambda: SimpleNamespace(hex="d" * 32),
    )
    monkeypatch.setattr(t, "push_file", lambda *_args, **_kwargs: None)
    marker = f"__REMRUN_BATCH_UNSUPPORTED_{'d' * 32}__;"
    rec = Recorder(lambda argv, inp: cp(1, b"corrupt", (marker + "noise").encode()))
    monkeypatch.setattr(t, "_run", rec)

    kwargs = {"telemetry_request": TelemetryRequest()} if detailed else {}
    with pytest.raises(TransportError, match=r"top-level \.cmd/\.bat"):
        t.exec(["native_probe", "value&literal"], cwd="C:\\project", **kwargs)

    assert len(rec.calls) == 1


def test_exec_detailed_telemetry_uses_staged_job_wrapper_and_preserves_argv(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    t._remote_home = "C:\\Users\\you"
    staged = {}
    monkeypatch.setattr(
        t,
        "push_file",
        lambda local, remote: staged.update(local=local, remote=remote),
    )
    payload = {
        "schema": 1,
        "memory": {"metric": "job_memory_peak"},
        "peak_rss_mb": 1.0,
        "avg_cpu_pct": 2.0,
    }
    stderr = ("\n__REMRUN_TELEMETRY__ " + json.dumps(payload) + "\n").encode()
    rec = Recorder(lambda argv, inp: cp(9, b"out", stderr))
    monkeypatch.setattr(t, "_run", rec)

    result = t.exec(
        ["python", "worker.py", "--jobs", "8"],
        cwd="C:\\project",
        telemetry_request=TelemetryRequest(),
    )

    assert result.exit_code == 9
    assert result.telemetry == payload
    assert staged["local"].name == "_win_telemetry.py"
    assert "\\AppData\\Local\\Temp\\" in staged["remote"]
    outer_script = decoded(rec.commands[-1])
    shell, job_script = nested_job_script(outer_script)
    assert shell == "pwsh"
    assert "'--detailed' '--' 'pwsh' '-NoProfile'" in outer_script
    assert "& 'python' 'worker.py' '--jobs' '8'" in job_script
    assert "exit $remrunCommandExitCode" in job_script
    match = re.search(
        r"\$env:(__REMRUN_INHERITED_PATH_[0-9a-f]{32}) = \$env:PATH",
        outer_script,
    )
    assert match is not None
    assert f"$env:PATH = $env:{match.group(1)}" in job_script
    assert f"$env:{match.group(1)} = $null" in job_script


def test_exec_detailed_supported_ps1_adversarial_argv_fits_remote_limit(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    remote_root = "C:\\" + r"Users\runner\AppData\Local\Temp\remrun-batch-boundary"
    t._remote_home = remote_root + r"\pwsh"
    staged = []
    monkeypatch.setattr(t, "push_file", lambda *args, **kwargs: staged.append(args))
    payload = {"schema": 1, "status": "ok"}
    stderr = ("\n__REMRUN_TELEMETRY__ " + json.dumps(payload) + "\n").encode()
    rec = Recorder(lambda argv, inp: cp(11, stderr=stderr))
    monkeypatch.setattr(t, "_run", rec)
    command = [
        remote_root + r"\native_probe.ps1",
        "",
        "with spaces",
        'quote"inside',
        "trailing\\",
        "&",
        "|",
        "<",
        ">",
        "%",
        "%PATH%",
        "^",
        "(",
        ")",
        "!",
    ]

    result = t.exec(
        command,
        cwd=remote_root,
        telemetry_request=TelemetryRequest(),
    )

    assert result.exit_code == 11
    assert result.telemetry == payload
    assert staged
    assert len(rec.commands[-1]) <= 8_191
    assert "_win_telemetry.py" in decoded(rec.commands[-1])


@pytest.mark.parametrize(
    "command",
    [
        ["Get-ChildItem", "-LiteralPath", "C:\\Program Files"],
        ["C:\\project\\adapter.ps1", "", "space value", "quote'value"],
        ["C:\\Python\\python.exe", "-c", "raise SystemExit(7)"],
    ],
    ids=["cmdlet", "ps1", "exe"],
)
def test_exec_detailed_job_preserves_powershell_command_semantics(
    command, monkeypatch
):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    t._remote_home = "C:\\Users\\you"
    monkeypatch.setattr(t, "push_file", lambda *_args, **_kwargs: None)
    rec = Recorder(lambda argv, inp: cp(0))
    monkeypatch.setattr(t, "_run", rec)

    result = t.exec(
        command,
        cwd="C:\\project",
        telemetry_request=TelemetryRequest(),
    )

    assert result.exit_code == 0
    outer_script = decoded(rec.commands[-1])
    _, job_script = nested_job_script(outer_script)
    assert "& " + " ".join(_ps_squote(token) for token in command) in job_script
    # User metacharacters live only in the encoded nested script. They never
    # enter the Job helper's direct argv grammar; the target PowerShell owns the
    # supported command's established invocation semantics.
    assert "& " + " ".join(_ps_squote(token) for token in command) not in outer_script


def test_exec_observed_telemetry_grants_breakaway_only_to_observer_wrapper(
    monkeypatch,
):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    t._remote_home = r"C:\home\runner"
    monkeypatch.setattr(
        t,
        "_ensure_job_observer",
        lambda: (r"D:\remrun\state", r"D:\remrun\state\observer.py"),
    )
    staged = []

    def capture_stage(local, remote):
        staged.append((local.name, remote, local.read_bytes()))

    monkeypatch.setattr(t, "push_file", capture_stage)
    deleted = []
    monkeypatch.setattr(t, "delete_remote", lambda remote: deleted.append(remote))
    telemetry_payload = {"peak_rss_mb": 4.0, "avg_cpu_pct": 1.0}
    telemetry_stderr = (
        "\n__REMRUN_TELEMETRY__ " + json.dumps(telemetry_payload) + "\n"
    ).encode()
    rec = Recorder(lambda argv, inp: cp(37, stderr=telemetry_stderr))
    monkeypatch.setattr(t, "_run", rec)
    command = ["Write-Output", "observed"]
    observation = JobObservation.for_command(
        job_id="native-1",
        project="@native-gate",
        source_controller="test-controller",
        target="WINBOX",
        phase="telemetry-on",
        command=command,
    )

    result = t.exec_observed(
        command,
        cwd=r"C:\project",
        observation=observation,
        telemetry=True,
    )

    assert result.exit_code == 37
    assert result.telemetry == telemetry_payload
    outer_script = decoded(rec.commands[-1])
    assert "'--allow-observed-breakaway' '--argv-json-file'" in outer_script
    assert len(rec.commands[-1]) <= 8_191
    assert {name for name, _remote, _data in staged} == {
        "_win_telemetry.py",
        "argv.json",
    }
    request_entry = next(item for item in staged if item[0] == "argv.json")
    request_remote = request_entry[1]
    assert request_remote in outer_script
    assert deleted == [request_remote]
    observed_argv = json.loads(request_entry[2].decode("utf-8"))
    assert observed_argv[:3] == [
        "python",
        "-S",
        r"D:\remrun\state\observer.py",
    ]
    child_index = observed_argv.index("--") + 1
    assert observed_argv[child_index:child_index + 4] == [
        "pwsh",
        "-NoProfile",
        "-NonInteractive",
        "-EncodedCommand",
    ]
    user_script = base64.b64decode(observed_argv[child_index + 4]).decode(
        "utf-16-le"
    )
    assert "& 'Write-Output' 'observed'" in user_script


def test_exec_observed_telemetry_staging_failure_falls_back_before_user_start(
    monkeypatch,
):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    t._remote_home = r"C:\home\runner"
    monkeypatch.setattr(
        t,
        "_ensure_job_observer",
        lambda: (r"D:\remrun\state", r"D:\remrun\state\observer.py"),
    )
    staged = []

    def fail_request_stage(local, remote):
        staged.append((local.name, remote))
        if local.name == "argv.json":
            raise OSError("request stage failed")

    monkeypatch.setattr(t, "push_file", fail_request_stage)
    deleted = []
    monkeypatch.setattr(t, "delete_remote", lambda remote: deleted.append(remote))
    rec = Recorder(lambda argv, inp: cp(19))
    monkeypatch.setattr(t, "_run", rec)
    command = ["Write-Output", "once"]
    observation = JobObservation.for_command(
        job_id="native-fallback",
        project="@native-gate",
        source_controller="test-controller",
        target="WINBOX",
        phase="telemetry-stage-fallback",
        command=command,
    )

    result = t.exec_observed(
        command,
        cwd=r"C:\project",
        observation=observation,
        telemetry=True,
    )

    assert result.exit_code == 19
    assert result.telemetry is None
    assert len(rec.calls) == 1
    script = decoded(rec.commands[-1])
    assert "_win_telemetry.py" not in script
    assert "--allow-observed-breakaway" not in script
    assert script.count(r"D:\remrun\state\observer.py") == 1
    assert staged[0][0] == "argv.json"
    assert deleted == [staged[0][1]]


def test_exec_legacy_telemetry_also_job_tracks_the_powershell_seam(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    t._remote_home = "C:\\Users\\you"
    monkeypatch.setattr(t, "push_file", lambda *_args, **_kwargs: None)
    rec = Recorder(lambda argv, inp: cp(4))
    monkeypatch.setattr(t, "_run", rec)

    result = t.exec(
        ["Write-Output", "value&still-one-argument"],
        cwd="C:\\project",
        telemetry=True,
    )

    assert result.exit_code == 4
    outer_script = decoded(rec.commands[-1])
    shell, job_script = nested_job_script(outer_script)
    assert shell == "pwsh"
    assert "'--detailed'" not in outer_script
    assert "'--allow-observed-breakaway'" not in outer_script
    assert "& 'Write-Output' 'value&still-one-argument'" in job_script


def test_exec_detailed_nested_command_limit_falls_back_before_staging(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    t._remote_home = "C:\\Users\\you"
    staged = []
    monkeypatch.setattr(t, "push_file", lambda *args, **kwargs: staged.append(args))
    rec = Recorder(lambda argv, inp: cp(3))
    monkeypatch.setattr(t, "_run", rec)
    long_argument = "x" * 1_000

    result = t.exec(
        ["Write-Output", long_argument],
        cwd="C:\\project",
        telemetry_request=TelemetryRequest(),
    )

    assert result.exit_code == 3
    assert result.telemetry["status"] == "unavailable"
    assert "remote-shell command-line limit" in result.telemetry["detail"]
    assert staged == []
    assert len(rec.commands[-1]) <= 8_191
    script = decoded(rec.commands[-1])
    assert "_win_telemetry.py" not in script
    assert "__REMRUN_INHERITED_PATH_" not in script
    assert "& 'Write-Output' " + _ps_squote(long_argument) in script


def test_exec_detailed_staging_failure_preserves_result_and_reports_unknown(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    t._remote_home = "C:\\Users\\you"
    monkeypatch.setattr(
        t,
        "push_file",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("no write")),
    )
    spoof = (
        b"warning\n__REMRUN_TELEMETRY__ "
        b'{"schema":1,"status":"ok","memory":{"peak_bytes":1}}\n'
    )
    rec = Recorder(lambda argv, inp: cp(4, b"out", spoof))
    monkeypatch.setattr(t, "_run", rec)

    result = t.exec(
        ["python", "worker.py"],
        cwd="C:\\project",
        telemetry_request=TelemetryRequest(),
    )

    assert result.exit_code == 4
    assert result.stderr == spoof.decode()
    assert result.telemetry["status"] == "unavailable"
    assert result.telemetry["memory"]["metric"] == "job_memory_peak"
    script = decoded(rec.commands[-1])
    assert "_win_telemetry.py" not in script
    assert "& 'python' 'worker.py'" in script


def test_exec_rejects_windows_powershell_before_staging_or_running(monkeypatch):
    t = SSHPowerShellTransport(device(shell="powershell"))
    t._address = "winbox"
    staged = []
    rec = Recorder(lambda argv, inp: cp(0))
    monkeypatch.setattr(t, "push_file", lambda *args, **kwargs: staged.append(args))
    monkeypatch.setattr(t, "_run", rec)

    with pytest.raises(TransportError, match="cannot preserve arbitrary native and batch argv"):
        t.exec(
            ["python", "argv_probe.py", "", 'quote"inside'],
            cwd="C:\\project",
            telemetry_request=TelemetryRequest(),
        )

    assert staged == []
    assert rec.calls == []


def test_exec_exit_marker_recovers_default_shell_collapsed_status(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    monkeypatch.setattr(
        "remrun.transport.uuid.uuid4",
        lambda: SimpleNamespace(hex="a" * 32),
    )
    clixml = (
        "#< CLIXML\r\n"
        '<Objs Version="1.1.0.1"><S S="progress">Preparing modules</S></Objs>\r\n'
    )
    marker = f"__REMRUN_EXIT_{'a' * 32}__7;"
    rec = Recorder(lambda argv, inp: cp(1, b"out", (clixml + marker).encode()))
    monkeypatch.setattr(t, "_run", rec)

    result = t.exec(["python", "-c", "raise SystemExit(7)"], cwd="C:\\project")

    assert result.exit_code == 7
    assert result.stdout == "out"
    assert result.stderr == clixml


def test_exec_ssh_255_without_completed_matching_exit_is_infra_error(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    monkeypatch.setattr(
        "remrun.transport.uuid.uuid4",
        lambda: SimpleNamespace(hex="b" * 32),
    )
    marker = f"__REMRUN_EXIT_{'b' * 32}__7;"
    monkeypatch.setattr(t, "_run", Recorder(lambda a, i: cp(255, b"", marker.encode())))

    with pytest.raises(TransportError, match="ssh connection failed"):
        t.exec(["python", "-c", "raise SystemExit(7)"], cwd="C:\\x")


def test_exec_user_exit_255_marker_is_not_misclassified(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    monkeypatch.setattr(
        "remrun.transport.uuid.uuid4",
        lambda: SimpleNamespace(hex="c" * 32),
    )
    marker = f"__REMRUN_EXIT_{'c' * 32}__255;"
    monkeypatch.setattr(t, "_run", Recorder(lambda a, i: cp(255, b"", marker.encode())))

    result = t.exec(["python", "-c", "raise SystemExit(255)"], cwd="C:\\x")

    assert result.exit_code == 255
    assert result.stderr == ""


def test_exec_ssh_255_is_infra_error(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    monkeypatch.setattr(t, "_run", Recorder(lambda a, i: cp(255, b"", b"broken")))
    with pytest.raises(TransportError, match="ssh connection failed"):
        t.exec(["whoami"], cwd="C:\\x")


def test_workers_running_uses_native_and_wsl_patterns(monkeypatch):
    t = SSHPowerShellTransport(device(cancel={
        "process_patterns": ["tts-worker"],
        "wsl_process_patterns": ["vllm-worker"],
    }))
    t._address = "winbox"
    rec = Recorder(lambda a, i: cp(0, b"__REMRUN_WORKERS__1\r\n"))
    monkeypatch.setattr(t, "_run", rec)
    assert t.workers_running() is True
    script = decoded(rec.commands[-1])
    assert "Win32_Process" in script
    assert "tts-worker" in script
    assert "wsl.exe -- bash -lc" in script
    assert "ps ax -o pid= -o command=" in script
    assert "grep -v __REMRUN_WORKERS__" in script
    assert "vllm-worker" in script
    assert "__REMRUN_WORKERS__" in script


def test_workers_running_no_patterns_is_false(monkeypatch):
    t = SSHPowerShellTransport(device(cancel={}))
    t._address = "winbox"
    rec = Recorder(lambda a, i: cp(0, b"__REMRUN_WORKERS__1\r\n"))
    monkeypatch.setattr(t, "_run", rec)
    assert t.workers_running() is False
    assert rec.calls == []


# --- filesystem ---------------------------------------------------------------

def test_push_streams_base64_and_sets_mtime(monkeypatch, tmp_path: Path):
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    f = tmp_path / "f.bin"
    f.write_bytes(b"\x00\x01hello")
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.push_file(f, "C:\\proj\\sub\\f.bin")
    assert rec.calls[-1]["input"] == base64.b64encode(b"\x00\x01hello")
    script = decoded(rec.commands[-1])
    assert "$p = 'C:\\proj\\sub\\f.bin'" in script
    assert "FromBase64String" in script
    assert "SetLastWriteTimeUtc" in script
    assert "[IO.File]::Replace($tmp,$p,$backup,$true)" in script
    assert "$null" not in script
    assert ".remrun-backup-" in script
    assert "Remove-Item -LiteralPath $backup" in script


def test_push_files_streams_tar_archive(monkeypatch, tmp_path: Path):
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    local = tmp_path / "local"
    (local / "sub").mkdir(parents=True)
    (local / "a.txt").write_text("A")
    (local / "sub" / "b.txt").write_text("B")
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.push_files(local, "C:\\out", ["a.txt", "sub/b.txt"])
    cmd = rec.commands[-1]
    script = decoded(cmd)
    assert "& 'python' '-c'" in script and "tarfile" in script
    with tarfile.open(fileobj=io.BytesIO(rec.calls[-1]["input"]), mode="r:*") as tf:
        assert sorted(tf.getnames()) == ["a.txt", "sub/b.txt"]


def test_pull_decodes_base64_and_sets_mtime(monkeypatch, tmp_path: Path):
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    data = b"\x00\x01result-bytes"
    ns = 1700000000000000000
    stdout = (f"{ns}\r\n".encode() + base64.b64encode(data) + b"\r\n")
    monkeypatch.setattr(t, "_run", Recorder(lambda a, i: cp(0, stdout)))
    out = tmp_path / "local" / "f.bin"
    t.pull_file("C:\\proj\\f.bin", out)
    assert out.read_bytes() == data
    assert abs(out.stat().st_mtime_ns - ns) < 1_000_000


def test_pull_empty_file(monkeypatch, tmp_path: Path):
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    # base64 of empty bytes is empty -> only the mtime line comes back.
    stdout = b"1700000000000000000\r\n\r\n"
    monkeypatch.setattr(t, "_run", Recorder(lambda a, i: cp(0, stdout)))
    out = tmp_path / "e.bin"
    t.pull_file("C:\\proj\\e.bin", out)
    assert out.read_bytes() == b""


def test_delete_and_mkdir(monkeypatch):
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.delete_remote("C:\\proj\\old.txt")
    t.ensure_remote_dir("C:\\proj\\sub")
    del_script = decoded(rec.commands[-2])
    assert "$p = 'C:\\proj\\old.txt'" in del_script
    assert "Remove-Item -LiteralPath $p -Force" in del_script
    assert "Test-Path -LiteralPath $p" in del_script      # verifies absence
    assert "SilentlyContinue" not in del_script           # no longer swallows errors
    assert "New-Item -ItemType Directory -Force -Path 'C:\\proj\\sub'" in decoded(rec.commands[-1])


def test_manifest_pipes_runner_and_parses(monkeypatch):
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    payload = json.dumps({"version": 1, "files": {
        "a.txt": {"kind": "file", "size": 1, "mtime_ns": 5, "sha256": "deadbeef"}
    }}).encode()
    rec = Recorder(lambda a, i: cp(0, payload))
    monkeypatch.setattr(t, "_run", rec)
    m = t.manifest("C:\\proj", ["scratch/**"], hash_below_bytes=64)
    assert m["a.txt"].sha256 == "deadbeef"
    # Manifest runs python directly (not -EncodedCommand), runner piped via stdin.
    cmd = rec.commands[-1]
    assert cmd.startswith("python - ")
    assert rec.calls[-1]["input"].startswith(b"#!/usr/bin/env python3")


def test_versioned_runner_install_and_rpc_use_framed_stdin(monkeypatch):
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    rec = Recorder(lambda a, i: cp(0, b"response-frame"))
    monkeypatch.setattr(t, "_run", rec)
    source = b"print('runner')\n"
    digest = hashlib.sha256(source).hexdigest()

    t.install_versioned_runner(source, "D:\\remrun\\state\\runner.py", digest)
    install_script = decoded(rec.commands[-1])
    assert rec.calls[-1]["input"].startswith(b"RRFRAME2 ")
    assert "D:\\remrun\\state\\runner.py" in install_script
    assert digest in install_script

    assert t.runner_rpc(
        "D:\\remrun\\state\\runner.py", "D:\\remrun\\state", b"request-frame"
    ) == b"response-frame"
    rpc_script = decoded(rec.commands[-1])
    assert rec.calls[-1]["input"] == b"request-frame"
    assert "'rpc'" in rpc_script and "D:\\remrun\\state" in rpc_script


def test_memory_guard_schema_two_is_explicitly_unsupported_on_windows():
    with pytest.raises(
        MemoryGuardConfigError, match="memory_guard schema 2 is not proved on Windows"
    ):
        SSHPowerShellTransport(
            device(
                ram_gb=64,
                memory_guard={
                    "schema": 2,
                    "command_limit_fraction": 0.25,
                    "host_reserve_fraction": 0.25,
                },
            )
        )
