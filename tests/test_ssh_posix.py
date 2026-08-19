"""Unit tests for SSHPosixTransport command construction (ssh mocked)."""
from __future__ import annotations

import io
import hashlib
import json
import os
import signal
import subprocess
import sys
import tarfile
import time
from pathlib import Path

import pytest

from remrun.memory_guard import MemoryAdmissionResult, MemoryReservation
from remrun.models import Device
from remrun.transport import (
    LocalSimTransport,
    SSHPosixTransport,
    TelemetryRequest,
    TransportError,
    _extract_telemetry,
    _guarded_helper_args,
)


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


def test_read_small_file_caps_remotely_and_returns_binary(monkeypatch):
    payload = b"\x00receipt\xff"
    t = SSHPosixTransport(device())
    t._address = "h"
    rec = Recorder(lambda argv, inp: cp(0, payload))
    monkeypatch.setattr(t, "_run", rec)

    assert t.read_small_file("/state/run receipt.json", 64) == payload
    script = rec.scripts[-1]
    assert "__REMRUN_SMALL_FILE_MISSING__" in script
    assert "__REMRUN_SMALL_FILE_OVERSIZE__" in script
    assert "read(limit+1)" in script
    assert "'/state/run receipt.json'" in script


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
    t = SSHPosixTransport(device())
    t._address = "h"
    monkeypatch.setattr(
        t, "_run", Recorder(lambda argv, inp: cp(returncode, stderr=marker.encode()))
    )

    with pytest.raises(TransportError, match=message):
        t.read_small_file("/state/receipt.json", 64)


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


def test_sample_load_uses_interval_cpu_and_never_load_average(monkeypatch):
    t = SSHPosixTransport(device())
    t._address = "macbox.local"
    recorder = Recorder(lambda argv, inp: cp(0, b"20.0\n"))
    monkeypatch.setattr(t, "_run", recorder)

    assert t.sample_load() == 20.0
    script = recorder.scripts[-1]
    assert "iostat" in script
    assert "/proc/stat" in script
    assert "getloadavg" not in script
    assert "top -l" not in script


def test_sample_load_keeps_unavailable_utilization_unknown(monkeypatch):
    t = SSHPosixTransport(device())
    t._address = "macbox.local"
    monkeypatch.setattr(t, "_run", Recorder(lambda argv, inp: cp(0, b"")))

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


@pytest.mark.parametrize(
    "payload",
    ["[]", "1", '"text"', "null", '{"schema":1,"value":NaN}'],
)
def test_extract_telemetry_rejects_non_object_json_and_preserves_stderr(payload):
    stderr = f"warning\n__REMRUN_TELEMETRY__ {payload}\n"

    cleaned, telem = _extract_telemetry(stderr)

    assert telem is None
    assert cleaned == stderr


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


def test_exec_detailed_telemetry_stages_helper_outside_project_and_preserves_command(monkeypatch):
    t = SSHPosixTransport(device())
    t._address = "h"
    remote_home = "/" + "Users/test"
    t._remote_home = remote_home
    staged = {}
    monkeypatch.setattr(
        t,
        "push_file",
        lambda local, remote: staged.update(local=local, remote=remote),
    )
    payload = {
        "schema": 1,
        "memory": {"metric": "rss_sum_sampled", "peak_bytes": 10},
        "peak_rss_mb": 0.0,
        "avg_cpu_pct": 1.0,
    }
    rec = Recorder(
        lambda argv, inp: cp(
            7,
            b"out\n",
            ("\n__REMRUN_TELEMETRY__ " + json.dumps(payload) + "\n").encode(),
        )
    )
    monkeypatch.setattr(t, "_run", rec)

    result = t.exec(
        ["python3", "worker.py", "--jobs", "8"],
        cwd="/project/run",
        telemetry_request=TelemetryRequest(),
    )

    assert result.exit_code == 7
    assert result.telemetry == payload
    assert staged["local"].name == "_posix_telemetry.py"
    assert staged["remote"] == (
        f"{remote_home}/.local/state/remrun/helpers/remrun_posix_telemetry_v1.py"
    )
    assert not staged["remote"].startswith("/project/")
    script = rec.scripts[-1]
    assert "--detailed -- bash -lc" in script
    assert "python3 worker.py --jobs 8" in script


def test_exec_detailed_staging_failure_runs_plain_command_with_explicit_unknown(monkeypatch):
    t = SSHPosixTransport(device(login_shell=False))
    t._address = "h"
    monkeypatch.setattr(
        t,
        "push_file",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("no write")),
    )
    spoof = (
        b"warning\n__REMRUN_TELEMETRY__ "
        b'{"schema":1,"status":"ok","memory":{"peak_bytes":1}}\n'
    )
    rec = Recorder(lambda argv, inp: cp(3, b"out", spoof))
    monkeypatch.setattr(t, "_run", rec)

    result = t.exec(
        ["python3", "worker.py"],
        cwd="/project",
        telemetry_request=TelemetryRequest(),
    )

    assert result.exit_code == 3
    assert rec.scripts[-1] == "cd /project && python3 worker.py"
    assert result.stderr == spoof.decode()
    assert result.telemetry["status"] == "unavailable"
    assert result.telemetry["memory"]["coverage"] == "sampler_failed"
    assert result.telemetry["peak_rss_mb"] is None


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
    os.chmod(f, 0o755)
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.push_file(f, "/root/p/sub/f.txt")
    script = rec.scripts[-1]
    assert "mkdir -p /root/p/sub" in script
    assert "t=/root/p/sub/f.txt.remrun-tmp" in script   # temp path bound to a shell var
    assert 'cat > "$t"' in script
    assert "os.utime" in script
    assert "os.fchmod" in script
    assert script.index("os.open") < script.index("os.fchmod")
    assert script.endswith(" 493 && trap - EXIT")
    assert rec.calls[-1]["input"] == b"payload"


def test_push_files_streams_tar_archive(monkeypatch, tmp_path: Path):
    t = SSHPosixTransport(device())
    t._address = "h"
    local = tmp_path / "local"
    (local / "sub").mkdir(parents=True)
    (local / "a.txt").write_text("A")
    (local / "sub" / "b.txt").write_text("B")
    os.chmod(local / "sub" / "b.txt", 0o755)
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.push_files(local, "/root/out", ["a.txt", "sub/b.txt"])
    script = rec.scripts[-1]
    assert "tarfile" in script and "os.replace" in script and "os.fchmod" in script
    archive = tmp_path / "pushed.tar"
    archive.write_bytes(rec.calls[-1]["input"])
    with tarfile.open(archive, "r:*") as tf:
        assert sorted(tf.getnames()) == ["a.txt", "sub/b.txt"]
        assert tf.getmember("sub/b.txt").mode == 0o755


def test_pull_files_extracts_tar_archive(monkeypatch, tmp_path: Path):
    t = SSHPosixTransport(device())
    t._address = "h"
    archive = tmp_path / "remote.tar"
    with tarfile.open(archive, "w") as tf:
        for name, data in {"a.txt": b"A", "sub/b.txt": b"B"}.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o755 if name == "sub/b.txt" else 0o644
            tf.addfile(info, io.BytesIO(data))
    monkeypatch.setattr(t, "_run", Recorder(lambda a, i: cp(0, archive.read_bytes())))
    out = tmp_path / "out"
    t.pull_files("/root/out", out, ["a.txt", "sub/b.txt"])
    assert (out / "a.txt").read_text() == "A"
    assert (out / "sub" / "b.txt").read_text() == "B"
    if os.name == "posix":
        assert (out / "sub" / "b.txt").stat().st_mode & 0o777 == 0o755


def test_pull_writes_bytes_and_sets_mtime_and_mode(monkeypatch, tmp_path: Path):
    t = SSHPosixTransport(device())
    t._address = "h"

    def responder(argv, _inp):
        script = argv[-1]
        if script.startswith("cat "):
            return cp(0, b"remote-bytes")
        if "st_mtime_ns" in script:
            return cp(0, b"1700000000000000000 493\n")
        return cp(0)

    monkeypatch.setattr(t, "_run", Recorder(responder))
    out = tmp_path / "local" / "f.txt"
    t.pull_file("/root/p/f.txt", out)
    assert out.read_bytes() == b"remote-bytes"
    assert abs(out.stat().st_mtime_ns - 1700000000000000000) < 1_000_000
    if os.name == "posix":
        assert out.stat().st_mode & 0o777 == 0o755


def test_delete_and_mkdir(monkeypatch):
    t = SSHPosixTransport(device())
    t._address = "h"
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)
    t.delete_remote("/root/p/old.txt")
    t.ensure_remote_dir("/root/p/sub")
    assert rec.scripts[-2] == "rm -f /root/p/old.txt"
    assert rec.scripts[-1] == "mkdir -p /root/p/sub"


def test_delete_expands_remote_home_before_quoting(monkeypatch):
    t = SSHPosixTransport(device())
    t._address = "h"
    t._remote_home = "/srv/user"
    rec = Recorder(lambda a, i: cp(0))
    monkeypatch.setattr(t, "_run", rec)

    t.delete_remote("~/workspace/proj/old.txt")

    assert rec.scripts == ["rm -f /srv/user/workspace/proj/old.txt"]


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


def test_streaming_exec_keeps_telemetry_sentinel_off_stdout(monkeypatch):
    """Streaming must not merge stderr into stdout.

    The rusage sampler round-trips a `__REMRUN_TELEMETRY__` sentinel through stderr. A
    naive streaming implementation (stderr=STDOUT, one pipe) would interleave that
    sentinel into the command's real output and break both the telemetry parse and the
    pulled-back stdout. Keeping the streams separate is the contract this pins.
    """
    t = SSHPosixTransport(device())
    t._address = "h"

    def responder(argv, _inp):
        stderr = (b'warn\n__REMRUN_TELEMETRY__ '
                  b'{"peak_rss_mb": 42.0, "avg_cpu_pct": 150.0, "cpu_sec": 1.0, "wall_sec": 0.7}\n')
        return cp(0, b"real-output\n", stderr)

    chunks: list[str] = []
    monkeypatch.setattr(t, "_run", Recorder(responder))
    res = t.exec(["Rscript", "a.R"], cwd="/p", telemetry=True, on_stdout=chunks.append)

    # Telemetry still parsed off stderr, and stripped from the logged stderr.
    assert res.telemetry["peak_rss_mb"] == 42.0
    assert res.stderr == "warn"
    # The live stream carries the command's stdout and NOTHING from stderr.
    streamed = "".join(chunks)
    assert "real-output" in streamed
    assert "__REMRUN_TELEMETRY__" not in streamed
    assert "warn" not in streamed
    # The buffered return value is unchanged by streaming.
    assert res.stdout == "real-output\n"


def test_stream_process_returns_both_streams_separately():
    """The shared streamer drains stderr on its own thread.

    Without a dedicated reader, a command that fills the stderr pipe buffer while we
    block on stdout would deadlock. This runs a real process that writes to both.
    """
    from remrun.transport import _stream_process

    chunks: list[str] = []
    proc = subprocess.Popen(
        [sys.executable, "-c",
         "import sys\n"
         "sys.stdout.write('O' * 200000 + '\\n'); sys.stdout.flush()\n"
         "sys.stderr.write('E' * 200000 + '\\n'); sys.stderr.flush()\n"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    code, out, err = _stream_process(proc, chunks.append, timeout=60)
    assert code == 0
    assert out.strip() == "O" * 200000
    assert err.strip() == "E" * 200000
    assert "".join(chunks) == out          # every stdout byte reached the sink
    assert "E" not in "".join(chunks)      # and no stderr leaked into it


def test_stream_process_timeout_covers_silent_child():
    """The timeout clock starts when streaming starts, not after stdout reaches EOF."""
    from remrun.transport import _stream_process

    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(5)"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        _stream_process(proc, lambda _chunk: None, timeout=0.2)
    elapsed = time.monotonic() - started
    assert 0.1 <= elapsed < 1.5
    assert proc.poll() is not None


def test_stream_process_timeout_covers_continuous_output():
    """A child that keeps stdout open and busy cannot postpone the deadline."""
    from remrun.transport import _stream_process

    proc = subprocess.Popen(
        [sys.executable, "-c",
         "import sys,time\n"
         "end=time.monotonic()+5\n"
         "while time.monotonic()<end:\n"
         " sys.stdout.write('still-running\\n'); sys.stdout.flush(); time.sleep(0.01)\n"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    chunks: list[str] = []
    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        _stream_process(proc, chunks.append, timeout=0.2)
    elapsed = time.monotonic() - started
    assert 0.1 <= elapsed < 1.5
    assert chunks
    assert proc.poll() is not None


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group orphan check")
def test_stream_process_timeout_kills_pipe_inheriting_grandchild_near_deadline(tmp_path: Path):
    grandchild_pid = tmp_path / "grandchild.pid"
    script = (
        "import pathlib,subprocess,sys,time\n"
        "child=subprocess.Popen([sys.executable,'-c',"
        "\"import sys,time; sys.stderr.write('GRANDCHILD\\\\n'); "
        "sys.stderr.flush(); time.sleep(10)\"])\n"
        f"pathlib.Path({str(grandchild_pid)!r}).write_text(str(child.pid))\n"
        "sys.stderr.write('PARENT\\n'); sys.stderr.flush(); time.sleep(10)\n"
    )
    transport = LocalSimTransport(
        Device.from_mapping("LOCAL_SIM", {"kind": "local-sim", "os": "posix"})
    )

    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired) as raised:
        transport.exec(
            [sys.executable, "-c", script],
            cwd=str(tmp_path),
            timeout=0.3,
            on_stdout=lambda _chunk: None,
        )
    elapsed = time.monotonic() - started

    # Process creation can be delayed substantially on a loaded host; the
    # timeout itself still begins when the transport enters its stream loop.
    assert 0.2 <= elapsed < 5.0
    assert "PARENT" in (raised.value.stderr or "")
    pid = int(grandchild_pid.read_text(encoding="utf-8"))
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.01)
    else:
        pytest.fail(f"grandchild process {pid} survived timeout")


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group orphan check")
def test_stream_process_timeout_kills_descendant_after_leader_exits(tmp_path: Path):
    """A reaped group leader must not spare descendants that still own its pipes."""
    from remrun.transport import _stream_process

    descendant_pid = tmp_path / "descendant.pid"
    script = (
        "import pathlib,subprocess,sys\n"
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(10)'])\n"
        f"pathlib.Path({str(descendant_pid)!r}).write_text(str(child.pid))\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    pid: int | None = None
    deadline = time.monotonic() + 2
    while not descendant_pid.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert descendant_pid.exists()
    assert proc.wait(timeout=2) == 0
    started = time.monotonic()
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            _stream_process(
                proc,
                lambda _chunk: None,
                timeout=0.3,
                process_group=True,
            )
        elapsed = time.monotonic() - started

        assert 0.2 <= elapsed < 0.9
        pid = int(descendant_pid.read_text(encoding="utf-8"))
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.01)
        else:
            pytest.fail(f"descendant process {pid} survived timeout")
    finally:
        if pid is None and descendant_pid.exists():
            pid = int(descendant_pid.read_text(encoding="utf-8"))
        if pid is not None:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


@pytest.mark.skipif(os.name != "nt", reason="Windows inherited-pipe timeout coverage")
def test_stream_process_windows_leader_exit_with_inherited_pipe_is_bounded(tmp_path: Path):
    """Windows cannot recover an exited taskkill root, but timeout return must stay bounded."""
    from remrun.transport import _stream_process

    descendant_pid = tmp_path / "descendant.pid"
    script = (
        "import pathlib,subprocess,sys\n"
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(10)'])\n"
        f"pathlib.Path({str(descendant_pid)!r}).write_text(str(child.pid))\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    )
    started = time.monotonic()
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            _stream_process(
                proc,
                lambda _chunk: None,
                timeout=0.3,
                process_group=True,
            )
        assert 0.2 <= time.monotonic() - started < 0.9
        assert proc.poll() == 0
    finally:
        if descendant_pid.exists():
            subprocess.run(
                ["taskkill", "/PID", descendant_pid.read_text(encoding="utf-8"), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=2,
            )


def test_stream_process_decodes_split_multibyte_stderr():
    """Arbitrary stderr read boundaries must not corrupt a UTF-8 code point."""
    from remrun.transport import _stream_process

    proc = subprocess.Popen(
        [sys.executable, "-c",
         "import sys\n"
         "sys.stderr.buffer.write(b'A' * 65535 + '€'.encode('utf-8'))\n"
         "sys.stderr.buffer.flush()\n"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    code, out, err = _stream_process(proc, lambda _chunk: None, timeout=5)
    assert code == 0
    assert out == ""
    assert err == "A" * 65535 + "€"
    assert "�" not in err


def test_protected_posix_staging_failure_is_a_known_refusal_not_plain_fallback(
    monkeypatch
):
    t = SSHPosixTransport(
        device(
            ram_gb=64,
            memory_guard={
                "schema": 3,
                "command_limit_fraction": 0.25,
                "host_reserve_fraction": 0.25,
            },
        )
    )
    t._address = "macbox"
    remote_home = "/" + "Users/test"
    t._remote_home = remote_home
    reservation = MemoryReservation(
        lease_id="a" * 32,
        lease_token="b" * 32,
        state_root=f"{remote_home}/.local/state/remrun",
        allowance_bytes=8 * 1024**3,
        control_overhead_bytes=256 * 1024**2,
        capacity_bytes=8 * 1024**3 + 256 * 1024**2,
        max_command_bytes=16 * 1024**3,
        min_available_bytes=16 * 1024**3,
        host_total_bytes=64 * 1024**3,
        safe_concurrency=3,
        expires_at=time.time() + 60,
    )
    monkeypatch.setattr(
        t,
        "push_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")),
    )
    monkeypatch.setattr(
        t,
        "release_memory_guard",
        lambda *_args, **_kwargs: MemoryAdmissionResult.refused(
            "release_deferred", "test"
        ),
    )
    monkeypatch.setattr(
        t,
        "_remote",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unguarded user command must not be attempted")
        ),
    )

    result = t.exec(
        ["python3", "-c", "print('unsafe')"],
        cwd="/tmp",
        telemetry=False,
        memory_reservation=reservation,
    )

    assert result.exit_code == 125
    assert result.memory_guard["status"] == "refused"
    assert result.memory_guard["reason"] == "helper_unavailable"
    assert result.memory_guard["command_started"] is False


def _ssh_admitted_payload(request: dict[str, object]) -> dict[str, object]:
    allowance = 2 * 1024**3
    control = 256 * 1024**2
    return {
        "schema": 2,
        "status": "admitted",
        "reason": "reserved" if request["op"] == "reserve" else "renewed",
        "detail": "test",
        "active_leases": 1,
        "stale_reaped": 0,
        "lease": {
            "lease_id": request["lease_id"],
            "lease_token": request["lease_token"],
            "state_root": request["state_root"],
            "allowance_bytes": request.get("allowance_bytes", allowance),
            "control_overhead_bytes": request.get("control_overhead_bytes", control),
            "capacity_bytes": request.get("capacity_bytes", allowance + control),
            "max_command_bytes": 16 * 1024**3,
            "min_available_bytes": 16 * 1024**3,
            "host_total_bytes": 64 * 1024**3,
            "safe_concurrency": 3,
            "expires_at": time.time() + 60,
        },
    }


def test_ssh_posix_reserve_renew_and_guard_args_preserve_lease_capacity(
    monkeypatch
):
    t = SSHPosixTransport(
        device(
            max_jobs=3,
            memory_guard={
                "schema": 3,
                "command_limit_fraction": 0.25,
                "host_reserve_fraction": 0.25,
            },
        )
    )
    t._remote_home = "/" + "Users/test"
    requests: list[dict[str, object]] = []

    def invoke(request):
        requests.append(dict(request))
        return _ssh_admitted_payload(request)

    monkeypatch.setattr(t, "_invoke_memory_admission", invoke)
    reserved = t.reserve_memory_guard(predicted_rss_mb=128)
    assert reserved.admitted
    reservation = reserved.reservation
    assert reservation is not None
    renewed = t.renew_memory_guard(reservation)
    assert renewed.admitted
    assert requests[0]["op"] == "reserve"
    assert requests[0]["predicted_rss_bytes"] == 128 * 1024**2
    assert requests[1]["op"] == "renew"
    assert requests[1]["control_overhead_bytes"] == reservation.control_overhead_bytes
    assert requests[1]["capacity_bytes"] == reservation.capacity_bytes

    args = _guarded_helper_args(
        "/state/helper.py", t.memory_guard, reservation, "c" * 32,
        detailed=False, telemetry=False,
    )
    encoded = args[args.index("--guard-lease-b64") + 1]
    import base64
    decoded = json.loads(base64.urlsafe_b64decode(encoded).decode("utf-8"))
    assert decoded["lease_id"] == reservation.lease_id
    assert decoded["allowance_bytes"] == reservation.allowance_bytes
    assert decoded["control_overhead_bytes"] == reservation.control_overhead_bytes
    assert decoded["capacity_bytes"] == reservation.capacity_bytes


def test_ssh_posix_fair_share_helper_uses_new_filename_and_existing_ledger():
    from remrun import _posix_telemetry as telemetry

    t = SSHPosixTransport(
        device(
            memory_guard={
                "schema": 3,
                "command_limit_fraction": 0.25,
                "host_reserve_fraction": 0.25,
            },
        )
    )
    t._remote_home = "/" + "Users/test"

    assert t._memory_guard_helper_path().endswith(
        "/helpers/remrun_posix_memory_guard_v3.py"
    )
    ledger, lock = telemetry._ledger_paths("/" + "Users/test/.local/state/remrun")
    assert ledger.as_posix().endswith("/memory-guard/v2/ledger.json")
    assert lock.as_posix().endswith("/memory-guard/v2/ledger.lock")


def test_ssh_posix_accepts_only_downward_unprofiled_renewal(monkeypatch):
    t = SSHPosixTransport(
        device(
            max_jobs=3,
            memory_guard={
                "schema": 3,
                "command_limit_fraction": 0.25,
                "host_reserve_fraction": 0.25,
            },
        )
    )
    t._remote_home = "/srv/remrun-test-home"

    def invoke(request):
        payload = _ssh_admitted_payload(request)
        lease = payload["lease"]
        lease["allowance_basis"] = "unprofiled_available_backed"
        if request["op"] == "renew":
            lease["allowance_bytes"] = 1024**3
            lease["capacity_bytes"] = 1024**3 + 256 * 1024**2
        return payload

    monkeypatch.setattr(t, "_invoke_memory_admission", invoke)
    reserved = t.reserve_memory_guard()
    assert reserved.admitted
    original = reserved.reservation
    assert original is not None

    renewed = t.renew_memory_guard(original)

    assert renewed.admitted
    resized = renewed.reservation
    assert resized is not None
    assert resized.allowance_bytes == 1024**3
    assert resized.allowance_basis == "unprofiled_available_backed"
    args = _guarded_helper_args(
        "/state/helper.py", t.memory_guard, resized, "c" * 32,
        detailed=False, telemetry=False,
    )
    assert args[args.index("--guard-max-bytes") + 1] == str(1024**3)

    learned = MemoryReservation(
        **{
            **original.__dict__,
            "allowance_basis": "learned_profile_plus_25_percent",
        }
    )
    rejected = t.renew_memory_guard(learned)
    assert not rejected.admitted
    assert rejected.reason == "admission_mismatch"


def test_ssh_posix_reservation_refusal_is_structured(monkeypatch):
    t = SSHPosixTransport(
        device(
            memory_guard={
                "schema": 3,
                "command_limit_fraction": 0.25,
                "host_reserve_fraction": 0.25,
            },
        )
    )
    t._remote_home = "/" + "Users/test"
    monkeypatch.setattr(
        t,
        "_invoke_memory_admission",
        lambda request: {
            "schema": 2,
            "status": "refused",
            "reason": "insufficient_live_memory",
            "detail": "test refusal",
            "active_leases": 1,
            "stale_reaped": 0,
        },
    )

    result = t.reserve_memory_guard()

    assert result.status == "refused"
    assert result.reason == "insufficient_live_memory"
    assert result.reservation is None
