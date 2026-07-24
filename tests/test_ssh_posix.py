"""Unit tests for SSHPosixTransport command construction (ssh mocked)."""
from __future__ import annotations

import io
import hashlib
import json
import subprocess
import tarfile
from pathlib import Path

import pytest

from remrun.models import Device
from remrun.transport import SSHPosixTransport, TransportError, _extract_telemetry


def device(**over) -> Device:
    data = {
        "kind": "ssh-posix",
        "os": "macos",
        "address_candidates": ["macbox.local", "macbox"],
        "project_root": "~/workspace/proj",
        "state_root": "~/.local/state/remrun",
        "cache_root": "~/.cache/remrun",
        "cancel": {
            "process_patterns": ["tts-worker", "ocr-worker"],
            "lock_paths": ["/tmp/remrun-test-worker.lock"],
        },
    }
    data.update(over)
    return Device.from_mapping("MACBOX", data)


def cp(returncode=0, stdout=b"", stderr=b"") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=["ssh"], returncode=returncode,
                                       stdout=stdout, stderr=stderr)


class Recorder:
    def __init__(self, responder):
        self.calls = []
        self.responder = responder

    def __call__(self, argv, input_bytes=None, timeout=None):
        self.calls.append({"argv": argv, "input": input_bytes, "timeout": timeout})
        return self.responder(argv, input_bytes)

    @property
    def scripts(self):
        return [c["argv"][-1] for c in self.calls]


def test_ssh_opts_expand_controller_home():
    t = SSHPosixTransport(device(ssh_opts=["-o", "IdentitiesOnly=yes", "-i",
                                                  "~/.ssh/id_mesh"]))
    argv = t._ssh_base("macbox")
    assert str(Path("~/.ssh/id_mesh").expanduser()) in argv
    assert "~/.ssh/id_mesh" not in argv


def test_kill_workers_pkills_workers_and_releases_lock(monkeypatch):
    # `fleet cancel` -> run this device's configured best-effort kill/lock cleanup.
    t = SSHPosixTransport(device())
    rec = Recorder(lambda argv, inp: cp(0, b"remrun-ok\nDarwin\n"))
    monkeypatch.setattr(t, "_run", rec)
    assert t.kill_workers() is True
    script = rec.scripts[-1]
    assert "pkill -f" in script
    assert "tts-worker" in script and "ocr-worker" in script
    assert "rm -rf -- /tmp/remrun-test-worker.lock" in script


def test_workers_running_uses_configured_patterns(monkeypatch):
    t = SSHPosixTransport(device())
    t._address = "h"
    rec = Recorder(lambda argv, inp: cp(0, b"__REMRUN_WORKERS__1\n"))
    monkeypatch.setattr(t, "_run", rec)
    assert t.workers_running() is True
    script = rec.scripts[-1]
    assert "ps ax -o pid= -o command=" in script
    assert "grep -v __REMRUN_WORKERS__" in script
    assert "tts-worker" in script and "ocr-worker" in script
    assert "__REMRUN_WORKERS__" in script


def test_workers_running_no_patterns_is_false(monkeypatch):
    t = SSHPosixTransport(device(cancel={}))
    rec = Recorder(lambda argv, inp: cp(0, b"__REMRUN_WORKERS__1\n"))
    monkeypatch.setattr(t, "_run", rec)
    assert t.workers_running() is False
    assert rec.calls == []


def test_push_file_handles_spaces_and_commas_in_path(tmp_path, monkeypatch):
    # Regression: a filename with spaces/commas must not
    # break the EXIT-cleanup trap. The old `trap 'rm -f {shlex.quote(tmp)}' EXIT` nested single
    # quotes -> `zsh: trap: undefined signal`, the push failed, and the job fell back off the device.
    t = SSHPosixTransport(device())
    rec = Recorder(lambda argv, inp: cp(0, b"remrun-ok\nDarwin\n"))
    monkeypatch.setattr(t, "_run", rec)
    local = tmp_path / "src.pdf"
    local.write_bytes(b"data")
    t.push_file(local, "/tmp/fleet-x/in/input file, with spaces - sample.pdf")
    script = rec.scripts[-1]
    assert 't=' in script and 'input file' in script       # temp path bound to a shell var
    assert 'trap \'rm -f "$t"\' EXIT' in script            # trap references "$t", not nested quotes
    assert 'cat > "$t"' in script
    assert "trap 'rm -f '" not in script                   # the broken nested-quote form is gone


def test_probe_resolves_first_reachable(monkeypatch):
    t = SSHPosixTransport(device())

    def responder(argv, _inp):
        # First candidate (macbox.local) answers ok.
        if "macbox.local" in " ".join(argv):
            return cp(0, b"remrun-ok\nDarwin\n")
        return cp(255, stderr=b"timeout")

    monkeypatch.setattr(t, "_run", Recorder(responder))
    probe = t.probe()
    assert probe.reachable
    assert probe.address == "macbox.local"
    assert probe.remote_os == "darwin"


def test_probe_unreachable(monkeypatch):
    t = SSHPosixTransport(device())
    monkeypatch.setattr(t, "_run", Recorder(lambda a, i: cp(255, stderr=b"no route")))
    probe = t.probe()
    assert not probe.reachable
    assert probe.address is None
    assert "no route" in probe.detail


def test_run_maps_ssh_timeout_to_transport_error(monkeypatch):
    # A hung ssh (subprocess timeout) must become a typed TransportError, not a raw
    # TimeoutExpired that escapes probe()/sample_load() and crashes --auto failover.
    t = SSHPosixTransport(device())

    def boom(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="ssh", timeout=8)

    monkeypatch.setattr(subprocess, "run", boom)
    with pytest.raises(TransportError):
        t._run(["ssh", "host", "echo hi"], timeout=8)


def test_probe_fails_over_when_first_candidate_blackholes(monkeypatch):
    # WHY: on a flaky network the first address can blackhole (probe times out).
    # That MUST NOT abort failover — probe must try the next candidate and land on
    # it. Before the fix the timeout propagated out of probe() and killed the run.
    t = SSHPosixTransport(device())  # candidates: macbox.local, then macbox

    def responder(argv, _inp):
        if "macbox.local" in " ".join(argv):
            raise TransportError("ssh timed out after 20s")  # what _run now raises
        return cp(0, b"remrun-ok\nDarwin\n")

    monkeypatch.setattr(t, "_run", Recorder(responder))
    probe = t.probe()
    assert probe.reachable
    assert probe.address == "macbox"  # fell over to the second candidate


def test_probe_all_candidates_timeout_returns_unreachable_not_raise(monkeypatch):
    # WHY: if every candidate blackholes, probe() must report unreachable so the
    # scheduler can fail over to the next DEVICE — not raise and abort the run.
    t = SSHPosixTransport(device())

    def boom(argv, _inp):
        raise TransportError("ssh timed out after 20s")

    monkeypatch.setattr(t, "_run", Recorder(boom))
    probe = t.probe()  # must not raise
    assert not probe.reachable
    assert probe.address is None
    assert "timed out" in probe.detail


def test_sample_load_returns_none_on_timeout(monkeypatch):
    # WHY: load_balance is on by default, so --auto samples each candidate's load.
    # A hung load probe must degrade to unknown (None), which pick_by_load tolerates,
    # rather than aborting placement.
    t = SSHPosixTransport(device())
    t._address = "macbox.local"  # skip reachability resolve

    def boom(argv, _inp):
        raise TransportError("ssh timed out after 20s")

    monkeypatch.setattr(t, "_run", Recorder(boom))
    assert t.sample_load() is None


def test_exec_builds_cd_and_command(monkeypatch):
    t = SSHPosixTransport(device(user="user", login_shell=False))
    t._address = "macbox.local"
    rec = Recorder(lambda a, i: cp(0, b"hi\n", b""))
    monkeypatch.setattr(t, "_run", rec)
    res = t.exec(["Rscript", "do/tmp/test.R"], cwd="/srv/user/workspace/proj/p/analysis")
    assert res.exit_code == 0
    assert res.stdout == "hi\n"
    script = rec.scripts[-1]
    assert script == "cd /srv/user/workspace/proj/p/analysis && Rscript do/tmp/test.R"
    # user@host target present.
    assert "user@macbox.local" in rec.calls[-1]["argv"]


def test_exec_login_shell_wraps_in_bash_lc(monkeypatch):
    t = SSHPosixTransport(device())  # login_shell defaults to True
    t._address = "h"
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.exec(["Rscript", "a.R"], cwd="/x")
    assert rec.scripts[-1] == "bash -lc 'cd /x && Rscript a.R'"


def test_extract_telemetry():
    stderr = ('real warning line\n__REMRUN_TELEMETRY__ '
              '{"peak_rss_mb": 12.3, "avg_cpu_pct": 88.0}\n')
    cleaned, telem = _extract_telemetry(stderr)
    assert telem["peak_rss_mb"] == 12.3
    assert telem["avg_cpu_pct"] == 88.0
    assert cleaned == "real warning line"
    assert "__REMRUN_TELEMETRY__" not in cleaned


def test_extract_telemetry_absent():
    cleaned, telem = _extract_telemetry("just normal stderr\n")
    assert telem is None
    assert cleaned == "just normal stderr\n"


def test_exec_telemetry_wraps_sampler_and_parses(monkeypatch):
    t = SSHPosixTransport(device())
    t._address = "h"

    def responder(argv, _inp):
        stderr = (b'warn\n__REMRUN_TELEMETRY__ '
                  b'{"peak_rss_mb": 42.0, "avg_cpu_pct": 150.0, "cpu_sec": 1.0, "wall_sec": 0.7}\n')
        return cp(0, b"hello\n", stderr)

    rec = Recorder(responder)
    monkeypatch.setattr(t, "_run", rec)
    res = t.exec(["Rscript", "a.R"], cwd="/p", telemetry=True)
    assert res.exit_code == 0
    assert res.stdout == "hello\n"
    assert res.stderr == "warn"  # sentinel stripped from logged stderr
    assert res.telemetry["peak_rss_mb"] == 42.0
    assert res.telemetry["avg_cpu_pct"] == 150.0
    # Command wrapped in the rusage sampler, inner shell invocation preserved.
    script = rec.scripts[-1]
    assert script.startswith("python3 -c ")
    assert " -- bash -lc " in script
    assert "RUSAGE_CHILDREN" in script


def test_exec_ssh_255_is_infra_error(monkeypatch):
    t = SSHPosixTransport(device())
    t._address = "macbox.local"
    monkeypatch.setattr(t, "_run", Recorder(lambda a, i: cp(255, b"", b"broken pipe")))
    with pytest.raises(TransportError):
        t.exec(["echo", "x"], cwd="/x")


def test_exec_quotes_paths_with_spaces(monkeypatch):
    t = SSHPosixTransport(device(login_shell=False))
    t._address = "h"
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.exec(["ls"], cwd="/a b/c")
    assert rec.scripts[-1] == "cd '/a b/c' && ls"


def test_manifest_pipes_runner_and_parses(monkeypatch):
    t = SSHPosixTransport(device())
    t._address = "h"
    payload = json.dumps({"version": 1, "files": {
        "a.txt": {"kind": "file", "size": 3, "mtime_ns": 9, "sha256": "deadbeef"}
    }}).encode()
    rec = Recorder(lambda a, i: cp(0, payload))
    monkeypatch.setattr(t, "_run", rec)
    m = t.manifest("/root/p", ["scratch/**"], hash_below_bytes=64)
    assert "a.txt" in m
    assert m["a.txt"].sha256 == "deadbeef"
    # Runner source was piped via stdin.
    assert rec.calls[-1]["input"].startswith(b"#!/usr/bin/env python3")
    assert " - " in rec.scripts[-1]  # python3 - <b64>


def test_versioned_runner_install_and_rpc_use_framed_stdin(monkeypatch):
    t = SSHPosixTransport(device())
    t._address = "h"
    rec = Recorder(lambda a, i: cp(0, b"response-frame"))
    monkeypatch.setattr(t, "_run", rec)
    source = b"print('runner')\n"
    digest = hashlib.sha256(source).hexdigest()

    t.install_versioned_runner(source, "/state/runner.py", digest)
    assert rec.calls[-1]["input"].startswith(b"RRFRAME2 ")
    assert "/state/runner.py" in rec.scripts[-1]
    assert digest in rec.scripts[-1]

    assert t.runner_rpc("/state/runner.py", "/state", b"request-frame") == b"response-frame"
    assert rec.calls[-1]["input"] == b"request-frame"
    assert rec.scripts[-1] == "python3 /state/runner.py rpc /state"


def test_push_builds_mkdir_cat_and_mtime(monkeypatch, tmp_path: Path):
    t = SSHPosixTransport(device())
    t._address = "h"
    f = tmp_path / "f.txt"
    f.write_bytes(b"payload")
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.push_file(f, "/root/p/sub/f.txt")
    script = rec.scripts[-1]
    assert "mkdir -p /root/p/sub" in script
    assert "t=/root/p/sub/f.txt.remrun-tmp" in script   # temp path bound to a shell var
    assert 'cat > "$t"' in script
    assert "os.utime" in script
    assert rec.calls[-1]["input"] == b"payload"


def test_push_files_streams_tar_archive(monkeypatch, tmp_path: Path):
    t = SSHPosixTransport(device())
    t._address = "h"
    local = tmp_path / "local"
    (local / "sub").mkdir(parents=True)
    (local / "a.txt").write_text("A")
    (local / "sub" / "b.txt").write_text("B")
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.push_files(local, "/root/out", ["a.txt", "sub/b.txt"])
    script = rec.scripts[-1]
    assert "tarfile" in script and "os.replace" in script
    archive = tmp_path / "pushed.tar"
    archive.write_bytes(rec.calls[-1]["input"])
    with tarfile.open(archive, "r:*") as tf:
        assert sorted(tf.getnames()) == ["a.txt", "sub/b.txt"]


def test_pull_files_extracts_tar_archive(monkeypatch, tmp_path: Path):
    t = SSHPosixTransport(device())
    t._address = "h"
    archive = tmp_path / "remote.tar"
    with tarfile.open(archive, "w") as tf:
        for name, data in {"a.txt": b"A", "sub/b.txt": b"B"}.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    monkeypatch.setattr(t, "_run", Recorder(lambda a, i: cp(0, archive.read_bytes())))
    out = tmp_path / "out"
    t.pull_files("/root/out", out, ["a.txt", "sub/b.txt"])
    assert (out / "a.txt").read_text() == "A"
    assert (out / "sub" / "b.txt").read_text() == "B"


def test_pull_writes_bytes_and_sets_mtime(monkeypatch, tmp_path: Path):
    t = SSHPosixTransport(device())
    t._address = "h"

    def responder(argv, _inp):
        script = argv[-1]
        if script.startswith("cat "):
            return cp(0, b"remote-bytes")
        if "st_mtime_ns" in script:
            return cp(0, b"1700000000000000000\n")
        return cp(0)

    monkeypatch.setattr(t, "_run", Recorder(responder))
    out = tmp_path / "local" / "f.txt"
    t.pull_file("/root/p/f.txt", out)
    assert out.read_bytes() == b"remote-bytes"
    assert abs(out.stat().st_mtime_ns - 1700000000000000000) < 1_000_000


def test_delete_and_mkdir(monkeypatch):
    t = SSHPosixTransport(device())
    t._address = "h"
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.delete_remote("/root/p/old.txt")
    t.ensure_remote_dir("/root/p/sub")
    assert rec.scripts[-2] == "rm -f /root/p/old.txt"
    assert rec.scripts[-1] == "mkdir -p /root/p/sub"


def test_probe_captures_home_and_expands_tilde(monkeypatch):
    t = SSHPosixTransport(device(project_root="~/workspace/proj"))
    monkeypatch.setattr(t, "_run",
                        Recorder(lambda a, i: cp(0, b"remrun-ok\nDarwin\n/srv/user")))
    assert t.probe().reachable

    class P:
        project_id = "paper1"
        relative_cwd = "."

    # ~ expanded to the captured remote home so shlex-quoting stays correct.
    assert t.remote_project_path(P()) == "/srv/user/workspace/proj/paper1"


def test_remote_project_path_and_join():
    t = SSHPosixTransport(device(project_root="/srv/user/workspace/proj"))

    class P:
        project_id = "client/foo"
        relative_cwd = "analysis"

    assert t.remote_project_path(P()) == "/srv/user/workspace/proj/client/foo"
    rp = t.remote_project_path(P())
    assert t.remote_join(rp, "analysis/run.R") == \
        "/srv/user/workspace/proj/client/foo/analysis/run.R"
    assert t.remote_join(rp, ".") == "/srv/user/workspace/proj/client/foo"
