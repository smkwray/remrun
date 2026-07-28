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
import subprocess
import tarfile
from pathlib import Path

import pytest

from remrun.models import Device
from remrun.transport import SSHPowerShellTransport, TransportError, _ps_encode, _ps_squote


def device(**over) -> Device:
    data = {
        "kind": "ssh-powershell",
        "os": "windows",
        "address_candidates": ["winbox", "WINBOX.local"],
        "project_root": "C:\\Users\\you\\projects",
        "state_root": "D:\\remrun\\state",
        "cache_root": "D:\\remrun\\cache",
        "remote_python": "python",
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
    b64 = cmd.split("-EncodedCommand ", 1)[1].strip()
    return base64.b64decode(b64).decode("utf-16-le")


# --- helpers ------------------------------------------------------------------

def test_ps_squote_escapes_quotes():
    assert _ps_squote("a'b") == "'a''b'"
    assert _ps_squote("C:\\x") == "'C:\\x'"


def test_ps_encode_roundtrip():
    assert base64.b64decode(_ps_encode("Write-Output 1")).decode("utf-16-le") == "Write-Output 1"


# --- probe / --auto failover (Windows path; not live-tested here) -------------

def test_probe_fails_over_when_first_candidate_blackholes(monkeypatch):
    # WHY: a blackholed first address (probe times out -> TransportError) must not
    # abort --auto failover; probe() must try the next candidate and land on it.
    t = SSHPowerShellTransport(device())  # candidates: winbox, then WINBOX.local

    def responder(argv, _inp):
        if "WINBOX.local" not in " ".join(argv):
            raise TransportError("ssh timed out after 30s")  # first candidate blackholes
        return cp(0, b"remrun-ok\nWin32NT\nC:\\Users\\you\n")

    monkeypatch.setattr(t, "_run", Recorder(responder))
    probe = t.probe()
    assert probe.reachable
    assert probe.address == "WINBOX.local"
    assert probe.remote_os == "windows"


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
                        Recorder(lambda a, i: cp(0, b"remrun-ok\r\nWin32NT\r\nC:\\Users\\alice\r\n")))
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

def test_exec_builds_powershell_script(monkeypatch):
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
    assert cmd.startswith("powershell -NoProfile -NonInteractive -EncodedCommand ")
    script = decoded(cmd)
    assert "$env:VIRTUAL_ENV = 'C:\\Users\\alice\\venvs\\remrun-test'" in script
    assert "$env:PATH = 'C:\\Users\\alice\\venvs\\remrun-test\\Scripts;' + $env:PATH" in script
    assert "Set-Location -LiteralPath 'C:\\Users\\you\\projects\\remrun-test'" in script
    assert "& 'python' 'do/compute.py'" in script
    assert "exit $LASTEXITCODE" in script


def test_exec_shell_override_uses_pwsh(monkeypatch):
    t = SSHPowerShellTransport(device(shell="pwsh"))
    t._address = "winbox"
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.exec(["whoami"], cwd="C:\\x")
    assert rec.commands[-1].startswith("pwsh -NoProfile -NonInteractive -EncodedCommand ")


def test_exec_ssh_255_is_infra_error(monkeypatch):
    import pytest
    t = SSHPowerShellTransport(device())
    t._address = "winbox"
    monkeypatch.setattr(t, "_run", Recorder(lambda a, i: cp(255, b"", b"broken")))
    with pytest.raises(Exception):
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
