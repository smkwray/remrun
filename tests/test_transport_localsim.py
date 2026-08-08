from pathlib import Path
import os
import sys

import pytest

from remrun.models import Device, ProjectContext
from remrun.transport import LocalSimTransport, TelemetryRequest, TransportError, make_transport


def make_device(root: Path) -> Device:
    return Device.from_mapping(
        "LOCAL_SIM",
        {
            "kind": "local-sim",
            "os": "posix",
            "project_root": str(root),
            "state_root": str(root / "state"),
            "cache_root": str(root / "cache"),
        },
    )


def test_make_transport_localsim(tmp_path: Path):
    t = make_transport(make_device(tmp_path))
    assert isinstance(t, LocalSimTransport)
    assert t.probe().reachable


def test_remote_project_path_and_join(tmp_path: Path):
    t = LocalSimTransport(make_device(tmp_path / "projects"))
    ctx = ProjectContext(
        local_project_root=tmp_path / "x",
        project_id="client/foo",
        relative_cwd="analysis",
        local_cwd=tmp_path / "x",
    )
    rp = t.remote_project_path(ctx)
    assert rp.endswith(str(Path("projects") / "client" / "foo"))
    joined = t.remote_join(rp, "analysis/run.R")
    assert joined.endswith(str(Path("run.R")))
    assert t.remote_join(rp, ".") == str(Path(rp))


def test_push_pull_delete_roundtrip(tmp_path: Path):
    src = tmp_path / "src.txt"
    src.write_bytes(b"hello world")
    mtime_ns = src.stat().st_mtime_ns

    t = LocalSimTransport(make_device(tmp_path / "projects"))
    remote = str(tmp_path / "projects" / "sub" / "dest.txt")
    t.push_file(src, remote)
    assert Path(remote).read_bytes() == b"hello world"
    # copy2 preserves mtime to the second (filesystems vary on ns granularity).
    assert abs(Path(remote).stat().st_mtime_ns - mtime_ns) < 2_000_000_000

    back = tmp_path / "back.txt"
    t.pull_file(remote, back)
    assert back.read_bytes() == b"hello world"

    t.delete_remote(remote)
    assert not Path(remote).exists()
    t.delete_remote(remote)  # idempotent


def test_read_small_file_is_binary_and_strictly_capped(tmp_path: Path):
    path = tmp_path / "receipt.bin"
    path.write_bytes(b"\x00payload\xff")
    transport = LocalSimTransport(make_device(tmp_path / "projects"))

    assert transport.read_small_file(str(path), 9) == b"\x00payload\xff"
    with pytest.raises(TransportError, match="exceeds 8 byte limit"):
        transport.read_small_file(str(path), 8)
    with pytest.raises(TransportError, match="missing"):
        transport.read_small_file(str(tmp_path / "missing.bin"), 64)
    with pytest.raises(ValueError, match="non-negative integer"):
        transport.read_small_file(str(path), -1)


def test_manifest_matches_schema(tmp_path: Path):
    proj = tmp_path / "projects" / "p"
    (proj / "do").mkdir(parents=True)
    (proj / "a.txt").write_text("a")
    (proj / "node_modules").mkdir()
    (proj / "node_modules" / "junk.js").write_text("x")
    t = LocalSimTransport(make_device(tmp_path / "projects"))
    m = t.manifest(str(proj), ["node_modules/**"], hash_below_bytes=1024)
    assert "a.txt" in m
    assert "node_modules/junk.js" not in m
    assert m["a.txt"].sha256 is not None


def test_push_is_atomic_no_partial_on_failure(tmp_path: Path):
    # An interrupted/failed write must leave the prior destination contents intact and
    # leave no stray temp file beside it.
    from remrun.transport import _atomic_write_local
    dest = tmp_path / "out" / "f.bin"
    dest.parent.mkdir(parents=True)
    dest.write_text("OLD GOOD CONTENT")

    def boom(tmp: Path):
        tmp.write_text("half-written")
        raise RuntimeError("stream interrupted")

    try:
        _atomic_write_local(dest, boom)
    except RuntimeError:
        pass
    assert dest.read_text() == "OLD GOOD CONTENT"                 # untouched
    assert not list(dest.parent.glob(".remrun-tmp-*"))            # no leaked temp


def test_atomic_write_retries_bounded_windows_replace_lock(tmp_path: Path, monkeypatch):
    import remrun.transport as transport_mod

    dest = tmp_path / "out" / "result.exe"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"old")
    real_replace = transport_mod.os.replace
    calls = 0
    sleeps: list[float] = []

    def flaky_replace(src, target):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise PermissionError(13, "sharing violation")
        return real_replace(src, target)

    monkeypatch.setattr(transport_mod.os, "replace", flaky_replace)
    monkeypatch.setattr(
        transport_mod,
        "_is_retryable_windows_replace_error",
        lambda _exc: True,
        raising=False,
    )
    monkeypatch.setattr(transport_mod.time, "sleep", sleeps.append)

    transport_mod._atomic_write_local(dest, lambda tmp: tmp.write_bytes(b"new"))

    assert dest.read_bytes() == b"new"
    assert calls == 3
    assert sleeps == list(transport_mod._LOCAL_REPLACE_RETRY_DELAYS[:2])
    assert not list(dest.parent.glob(".remrun-tmp-*"))


def test_atomic_write_retries_locked_temp_during_fsync(tmp_path: Path, monkeypatch):
    import remrun.transport as transport_mod

    dest = tmp_path / "out" / "result.exe"
    real_open = Path.open
    attempts = 0
    sleeps: list[float] = []

    def flaky_open(self, mode="r", *args, **kwargs):
        nonlocal attempts
        if self.name.startswith(".remrun-tmp-") and mode == "r+b":
            attempts += 1
            if attempts < 3:
                raise PermissionError(13, "scanner locked temp")
        return real_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", flaky_open)
    monkeypatch.setattr(
        transport_mod,
        "_is_retryable_windows_replace_error",
        lambda _exc: True,
    )
    monkeypatch.setattr(transport_mod.time, "sleep", sleeps.append)

    transport_mod._atomic_write_local(dest, lambda tmp: tmp.write_bytes(b"new"))

    assert dest.read_bytes() == b"new"
    assert attempts == 3
    assert sleeps == list(transport_mod._LOCAL_REPLACE_RETRY_DELAYS[:2])
    assert not list(dest.parent.glob(".remrun-tmp-*"))


def test_atomic_write_exhausts_lock_retries_without_replacing_prior_bytes(
    tmp_path: Path, monkeypatch
):
    import remrun.transport as transport_mod

    dest = tmp_path / "out" / "result.exe"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"old complete result")
    calls = 0

    def locked_replace(_src, _target):
        nonlocal calls
        calls += 1
        raise PermissionError(13, "sharing violation")

    monkeypatch.setattr(transport_mod.os, "replace", locked_replace)
    monkeypatch.setattr(
        transport_mod,
        "_is_retryable_windows_replace_error",
        lambda _exc: True,
        raising=False,
    )
    monkeypatch.setattr(transport_mod.time, "sleep", lambda _delay: None)

    with pytest.raises(PermissionError, match="sharing violation"):
        transport_mod._atomic_write_local(dest, lambda tmp: tmp.write_bytes(b"new"))

    assert calls == len(transport_mod._LOCAL_REPLACE_RETRY_DELAYS) + 1
    assert dest.read_bytes() == b"old complete result"
    assert not list(dest.parent.glob(".remrun-tmp-*"))


def test_atomic_write_cleanup_lock_does_not_mask_install_failure(
    tmp_path: Path, monkeypatch
):
    import remrun.transport as transport_mod

    dest = tmp_path / "out" / "result.exe"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"old complete result")
    real_unlink = Path.unlink
    cleanup_attempts = 0

    def fail_replace(_src, _target):
        raise PermissionError(13, "install lock")

    def locked_cleanup(self: Path, *args, **kwargs):
        nonlocal cleanup_attempts
        if self.name.startswith(".remrun-tmp-"):
            cleanup_attempts += 1
            if cleanup_attempts < 3:
                raise PermissionError(13, "cleanup lock")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(transport_mod.os, "replace", fail_replace)
    monkeypatch.setattr(
        transport_mod,
        "_is_retryable_windows_replace_error",
        lambda exc: "cleanup lock" in str(exc),
        raising=False,
    )
    monkeypatch.setattr(transport_mod.time, "sleep", lambda _delay: None)
    monkeypatch.setattr(Path, "unlink", locked_cleanup)

    with pytest.raises(PermissionError, match="install lock"):
        transport_mod._atomic_write_local(dest, lambda tmp: tmp.write_bytes(b"new"))

    assert cleanup_attempts == 3
    assert dest.read_bytes() == b"old complete result"
    assert not list(dest.parent.glob(".remrun-tmp-*"))


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode ordering")
def test_atomic_write_applies_read_only_mode_after_writable_fsync(
    tmp_path: Path, monkeypatch
):
    import remrun.transport as transport_mod

    dest = tmp_path / "out" / "read-only-tool"
    real_open = Path.open
    saw_finalization_open = False

    def assert_writable_before_fsync(self: Path, mode="r", *args, **kwargs):
        nonlocal saw_finalization_open
        if self.name.startswith(".remrun-tmp-") and mode == "r+b":
            saw_finalization_open = True
            assert self.stat().st_mode & 0o200
        return real_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", assert_writable_before_fsync)
    transport_mod._atomic_write_local(
        dest, lambda tmp: tmp.write_bytes(b"tool"), mode=0o444
    )

    assert saw_finalization_open is True
    assert dest.read_bytes() == b"tool"
    assert dest.stat().st_mode & 0o777 == 0o444


def test_localsim_push_pull_preserves_content_and_mtime(tmp_path: Path):
    import os
    src = tmp_path / "src.bin"
    src.write_bytes(b"payload-1234")
    os.utime(src, ns=(111_000_000_000, 111_000_000_000))
    t = LocalSimTransport(make_device(tmp_path / "projects"))
    remote = str(tmp_path / "projects" / "a" / "dest.bin")
    t.push_file(src, remote)
    assert Path(remote).read_bytes() == b"payload-1234"
    back = tmp_path / "back.bin"
    t.pull_file(remote, back)
    assert back.read_bytes() == b"payload-1234"
    assert abs(back.stat().st_mtime_ns - 111_000_000_000) < 2_000_000_000


def test_exec_runs_in_remote_cwd(tmp_path: Path):
    proj = tmp_path / "projects" / "p"
    proj.mkdir(parents=True)
    t = LocalSimTransport(make_device(tmp_path / "projects"))
    res = t.exec(["python", "-c", "import os;print(os.getcwd())"], cwd=str(proj))
    assert res.exit_code == 0
    assert str(proj.resolve()) in res.stdout


def test_exec_accepts_detailed_telemetry_request_without_changing_command_result(tmp_path: Path):
    proj = tmp_path / "projects" / "p"
    proj.mkdir(parents=True)
    transport = LocalSimTransport(make_device(tmp_path / "projects"))

    result = transport.exec(
        [sys.executable, "-c", "print('unchanged'); raise SystemExit(6)"],
        cwd=str(proj),
        telemetry_request=TelemetryRequest(),
    )

    assert result.exit_code == 6
    assert result.stdout == "unchanged\n"
    assert result.telemetry["schema"] == 1
    assert result.telemetry["memory"]["metric"] == "rss_sum_sampled"


def test_detailed_helper_missing_cannot_trust_user_stderr_sentinel(
    tmp_path: Path, monkeypatch
):
    proj = tmp_path / "projects" / "p"
    proj.mkdir(parents=True)
    transport = LocalSimTransport(make_device(tmp_path / "projects"))
    real_is_file = Path.is_file

    def hide_telemetry_helper(path: Path) -> bool:
        if path.name in {"_posix_telemetry.py", "_win_telemetry.py"}:
            return False
        return real_is_file(path)

    monkeypatch.setattr(Path, "is_file", hide_telemetry_helper)
    spoof = (
        'warning\n__REMRUN_TELEMETRY__ '
        '{"schema":1,"status":"ok","memory":{"peak_bytes":1}}\n'
    )
    program = f"import sys\nsys.stderr.write({spoof!r})\n"

    result = transport.exec(
        [sys.executable, "-c", program],
        cwd=str(proj),
        telemetry_request=TelemetryRequest(),
    )

    assert result.exit_code == 0
    assert result.telemetry["status"] == "unavailable"
    assert result.telemetry["memory"]["peak_bytes"] is None
    assert result.stderr == spoof


def test_required_memory_guard_helper_missing_refuses_without_user_code(
    tmp_path: Path, monkeypatch
):
    remote = tmp_path / "remote"
    remote.mkdir()
    marker = remote / "must-not-run.txt"
    guarded = Device.from_mapping(
        "GUARDED",
        {
            "kind": "local-sim",
            "os": "posix",
            "project_root": str(remote),
            "state_root": str(tmp_path / "state"),
            "cache_root": str(tmp_path / "cache"),
            "ram_gb": 8,
            "memory_guard": {
                "schema": 3,
                "command_limit_fraction": 0.05,
                "host_reserve_fraction": 0.05,
            },
        },
    )
    transport = LocalSimTransport(guarded)
    admission = transport.reserve_memory_guard(predicted_rss_mb=1)
    assert admission.admitted
    reservation = admission.reservation
    assert reservation is not None
    real_is_file = Path.is_file

    def hide_guard_helper(path: Path) -> bool:
        if path.name in {"_posix_telemetry.py", "_win_telemetry.py"}:
            return False
        return real_is_file(path)

    monkeypatch.setattr(Path, "is_file", hide_guard_helper)
    result = transport.exec(
        [sys.executable, "-c", f"open({str(marker)!r},'w').write('unsafe')"],
        cwd=str(remote),
        telemetry=False,
        memory_reservation=reservation,
    )

    assert result.exit_code == 125
    assert result.memory_guard["status"] == "refused"
    assert result.memory_guard["reason"] == "helper_unavailable"
    assert result.memory_guard["command_started"] is False
    assert not marker.exists()


def test_guarded_localsim_rejects_empty_state_root_without_writing_cwd(
    tmp_path: Path, monkeypatch
):
    project = tmp_path / "project"
    project.mkdir()
    monkeypatch.chdir(project)
    guarded = Device.from_mapping(
        "EMPTY_STATE",
        {
            "kind": "local-sim",
            "os": "posix",
            "project_root": str(tmp_path / "remote"),
            "state_root": "",
            "cache_root": str(tmp_path / "cache"),
            "memory_guard": {
                "schema": 3,
                "command_limit_fraction": 0.05,
                "host_reserve_fraction": 0.05,
            },
        },
    )
    transport = LocalSimTransport(guarded)

    result = transport.reserve_memory_guard(predicted_rss_mb=1)

    assert result.status == "refused"
    assert result.reason == "admission_unavailable"
    assert "state root is empty" in result.detail
    assert not (project / "memory-guard" / "v2").exists()
