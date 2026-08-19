from __future__ import annotations

import base64
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from remrun import _job_observer as observer
from remrun import job_observation as observation_module
from remrun.job_observation import JobObservation, command_label


def _metadata(command: list[str] | None = None) -> JobObservation:
    command = command or [sys.executable, "-S", "-c", "import time; time.sleep(10)"]
    return JobObservation.for_command(
        job_id="run-1",
        project="project-a",
        source_controller="CTRL",
        target="TARGET",
        phase="command",
        command=command,
    )


def _wait_for_record(
    root: Path, timeout: float = 5.0, predicate=None
) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        records, _ = observer._read_records(root)
        if records and (predicate is None or predicate(records[0])):
            return records[0]
        time.sleep(0.02)
    raise AssertionError("observer record did not appear")


def test_metadata_contains_only_safe_label_and_digest():
    command = ["/opt/secret/bin/tool", "--token", "top-secret", "/private/project/file"]
    item = _metadata(command)
    payload = item.payload()
    encoded = base64.urlsafe_b64decode(item.encoded()).decode("utf-8")

    assert payload["command_label"] == "tool"
    assert len(payload["command_sha256"]) == 64
    assert "top-secret" not in encoded
    assert "/private/project" not in encoded
    assert "--token" not in encoded


def test_command_label_handles_windows_paths_and_sanitizes_declared_label():
    assert command_label([r"C:\Tools\worker.exe"]) == "worker.exe"
    assert command_label(["ignored"], "worker label / private") == "worker_label_private"


def test_controller_operation_is_opt_in_reports_no_process_metrics_and_cleans_up(
    tmp_path, monkeypatch
):
    """A controller-side operation is live, but never pretends to be target CPU/RAM."""
    state_root = tmp_path / "state"
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(state_root))
    monkeypatch.setenv("REMRUN_FLEET_JOBS_OBSERVE", "1")
    item = JobObservation.for_command(
        job_id="git-sync-push-1",
        project="demo",
        source_controller="CTRL",
        target="PEER",
        phase="git-sync",
        command=["remrun", "git-sync", "PEER", "--push"],
        declared_label="git-sync:push",
    )

    with observation_module.observe_controller_operation(item):
        payload = observer._query(state_root, 0.05)
        assert payload["status"] == "ok"
        assert len(payload["jobs"]) == 1
        job = payload["jobs"][0]
        assert job["state"] == "RUNNING"
        assert job["project"] == "demo"
        assert job["source_controller"] == "CTRL"
        assert job["target"] == "PEER"
        assert job["phase"] == "git-sync"
        assert job["command"]["label"] == "git-sync:push"
        assert job["cpu"]["current_pct_one_logical_cpu"] is None
        assert job["threads"]["current_os_threads"] is None
        assert job["memory"]["current_bytes"] is None

    assert observer._query(state_root, 0.05)["jobs"] == []


def test_controller_operation_is_dormant_without_explicit_observation_switch(
    tmp_path, monkeypatch
):
    state_root = tmp_path / "state"
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(state_root))
    monkeypatch.delenv("REMRUN_FLEET_JOBS_OBSERVE", raising=False)
    item = JobObservation.for_command(
        job_id="git-sync-pull-1",
        project="demo",
        source_controller="CTRL",
        target="PEER",
        phase="git-sync",
        command=["remrun", "git-sync", "PEER", "--pull"],
        declared_label="git-sync:pull",
    )

    with observation_module.observe_controller_operation(item):
        assert not observer._db_path(state_root).exists()


def test_controller_operations_from_different_projects_remain_separate_rows(
    tmp_path, monkeypatch
):
    state_root = tmp_path / "state"
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(state_root))
    monkeypatch.setenv("REMRUN_FLEET_JOBS_OBSERVE", "true")
    alpha = JobObservation.for_command(
        job_id="git-sync-alpha",
        project="alpha",
        source_controller="CTRL",
        target="A",
        phase="git-sync",
        command=["remrun", "git-sync", "A", "--push"],
        declared_label="git-sync:push",
    )
    zeta = JobObservation.for_command(
        job_id="git-sync-zeta",
        project="zeta",
        source_controller="CTRL",
        target="Z",
        phase="git-sync",
        command=["remrun", "git-sync", "Z", "--pull"],
        declared_label="git-sync:pull",
    )

    with observation_module.observe_controller_operation(alpha):
        with observation_module.observe_controller_operation(zeta):
            payload = observer._query(state_root, 0.05)

    assert [(job["project"], job["target"]) for job in payload["jobs"]] == [
        ("alpha", "A"),
        ("zeta", "Z"),
    ]


def test_live_linux_job_reports_current_tree_metrics_and_cleans_up(tmp_path):
    command = [sys.executable, "-S", "-c", "import time; time.sleep(1.5)"]
    proc = subprocess.Popen(
        [
            sys.executable,
            str(Path(observer.__file__)),
            "run",
            "--state-root",
            str(tmp_path),
            "--metadata-b64",
            _metadata(command).encoded(),
            "--",
            *command,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_record(tmp_path)
        payload = observer._query(tmp_path, 0.05)
        assert payload["status"] in {"ok", "partial"}
        assert len(payload["jobs"]) == 1
        job = payload["jobs"][0]
        assert job["state"] == "RUNNING"
        assert job["threads"]["current_os_threads"] >= 1
        assert job["memory"]["current_bytes"] > 0
        assert job["memory"]["peak_bytes"] is None
        assert job["cpu"]["current_pct_one_logical_cpu"] is not None
        assert job["cpu"]["normalized_host_pct"] is not None
        assert job["command"]["label"] == Path(sys.executable).name
        assert proc.wait(timeout=5) == 0
        assert observer._query(tmp_path, 0.05)["jobs"] == []
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


@pytest.mark.skipif(os.name != "posix", reason="POSIX signal lifecycle")
def test_wrapper_death_retains_live_child_identity(tmp_path):
    command = [sys.executable, "-S", "-c", "import time; time.sleep(30)"]
    wrapper = subprocess.Popen(
        [
            sys.executable,
            str(Path(observer.__file__)),
            "run",
            "--state-root",
            str(tmp_path),
            "--metadata-b64",
            _metadata(command).encoded(),
            "--",
            *command,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    child_pid = None
    try:
        record = _wait_for_record(
            tmp_path, predicate=lambda item: int(item["root_pid"]) != wrapper.pid
        )
        child_pid = int(record["root_pid"])
        os.kill(wrapper.pid, signal.SIGKILL)
        wrapper.wait(timeout=5)
        os.kill(child_pid, 0)
        payload = observer._query(tmp_path, 0.05)
        assert [job["job_id"] for job in payload["jobs"]] == ["run-1"]
    finally:
        if wrapper.poll() is None:
            wrapper.kill()
            wrapper.wait(timeout=5)
        if child_pid:
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_stale_pid_identity_is_hidden_and_query_does_not_mutate_registry(tmp_path):
    current = observer._processes()[os.getpid()]
    fake = observer.ProcessRow(
        pid=current.pid,
        ppid=current.ppid,
        identity=current.identity + "-reused",
        start_order=current.start_order,
        cpu_sec=current.cpu_sec,
        rss_bytes=current.rss_bytes,
        threads=current.threads,
    )
    observer._register(tmp_path, _metadata().payload(), fake)
    db = observer._db_path(tmp_path)
    before = (db.stat().st_mtime_ns, hashlib.sha256(db.read_bytes()).hexdigest())

    payload = observer._query(tmp_path, 0.05)
    after = (db.stat().st_mtime_ns, hashlib.sha256(db.read_bytes()).hexdigest())

    assert payload["jobs"] == []
    assert payload["registry"]["stale_hidden"] == 1
    assert payload["registry"]["query_mutated_registry"] is False
    assert after == before
    assert len(observer._read_records(tmp_path)[0]) == 1


def test_registry_hard_bound_is_transactional(tmp_path):
    current = observer._processes()[os.getpid()]
    conn = observer._writer(tmp_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        values = _metadata().payload()
        for index in range(observer.MAX_ACTIVE_JOBS):
            conn.execute(
                "INSERT INTO active_jobs("
                "token,schema,job_id,project,source_controller,target,phase,command_label,"
                "command_sha256,member_count,root_pid,root_identity,started_at_ns"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f"{index:032x}", 1, f"job-{index}", values["project"],
                    values["source_controller"], values["target"], values["phase"],
                    values["command_label"], values["command_sha256"], 1,
                    current.pid, current.identity, time.time_ns() + index,
                ),
            )
        conn.execute("COMMIT")
    finally:
        conn.close()

    with pytest.raises(observer.RegistryFull):
        observer._register(tmp_path, _metadata().payload(), current)
    assert len(observer._read_records(tmp_path)[0]) == observer.MAX_ACTIVE_JOBS


def test_nested_registered_roots_are_not_double_counted():
    rows = {
        10: observer.ProcessRow(10, 1, "a", 10, 1.0, 100, 1),
        20: observer.ProcessRow(20, 10, "b", 20, 2.0, 200, 2),
        30: observer.ProcessRow(30, 20, "c", 30, 3.0, 300, 3),
    }
    records = [
        {"root_pid": 10, "root_identity": "a", "token": "outer"},
        {"root_pid": 20, "root_identity": "b", "token": "inner"},
    ]
    assigned = observer._assign(rows, records)
    assert [r.pid for r in assigned["outer"]] == [10]
    assert [r.pid for r in assigned["inner"]] == [20, 30]


def test_reused_parent_pid_fails_temporal_ancestry():
    rows = {
        10: observer.ProcessRow(10, 1, "root", 100, 0.0, 1, 1),
        20: observer.ProcessRow(20, 10, "child", 50, 0.0, 1, 1),
    }
    assert observer._nearest_root(rows[20], rows, {10: ("root", "token")}) is None


def test_cpu_delta_is_one_logical_cpu_percent_and_churn_is_partial():
    record = {
        "job_id": "j", "project": "p", "source_controller": "c", "target": "t",
        "phase": "command", "started_at_ns": time.time_ns() - 1_000_000_000,
        "root_pid": 1, "root_identity": "one", "member_count": 1,
        "command_label": "worker", "command_sha256": "a" * 64,
    }
    first = [observer.ProcessRow(1, 0, "one", 1, 2.0, 100, 1)]
    second = [observer.ProcessRow(1, 0, "one", 1, 3.0, 110, 1)]
    payload = observer._job_payload(record, first, second, 0.5, 4)
    assert payload["cpu"]["current_pct_one_logical_cpu"] == pytest.approx(200.0)
    assert payload["cpu"]["normalized_host_pct"] == pytest.approx(50.0)

    churn = observer._job_payload(
        record,
        first,
        second + [observer.ProcessRow(2, 1, "two", 2, 0.0, 50, 1)],
        0.5,
        4,
    )
    assert churn["cpu"]["current_pct_one_logical_cpu"] is None
    assert churn["observation_status"] == "partial"
    assert churn["threads"]["current_os_threads"] == 2


def test_query_uses_two_shared_snapshots_not_one_per_job(tmp_path, monkeypatch):
    now = time.time_ns()
    base = observer.ProcessRow(os.getpid(), os.getppid(), "root", 1, 1.0, 100, 1)
    conn = observer._writer(tmp_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        for index in range(3):
            conn.execute(
                "INSERT INTO active_jobs("
                "token,schema,job_id,project,source_controller,target,phase,command_label,"
                "command_sha256,member_count,root_pid,root_identity,started_at_ns"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (f"t{index}", 1, f"j{index}", "p", "c", "target", "command", "x",
                 "a" * 64, 1, os.getpid(), "root", now + index),
            )
        conn.execute("COMMIT")
    finally:
        conn.close()
    calls = 0

    def snapshots():
        nonlocal calls
        calls += 1
        return {os.getpid(): observer.ProcessRow(
            base.pid, base.ppid, base.identity, base.start_order,
            base.cpu_sec + calls * 0.1, base.rss_bytes, base.threads,
        )}

    monkeypatch.setattr(observer, "_processes", snapshots)
    monkeypatch.setattr(observer.time, "sleep", lambda _: None)
    payload = observer._query(tmp_path, 0.05)
    assert calls == 2
    assert len(payload["jobs"]) == 3



def test_read_only_query_accepts_physical_legacy_schema_without_migration(tmp_path):
    db = observer._db_path(tmp_path)
    db.parent.mkdir(parents=True)
    conn = sqlite3.connect(db)
    try:
        conn.execute(
            "CREATE TABLE registry_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) "
            "WITHOUT ROWID"
        )
        conn.execute("INSERT INTO registry_meta(key,value) VALUES('schema','1')")
        conn.execute(
            "CREATE TABLE active_jobs ("
            "token TEXT PRIMARY KEY, schema INTEGER NOT NULL, job_id TEXT NOT NULL, "
            "project TEXT NOT NULL, source_controller TEXT NOT NULL, target TEXT NOT NULL, "
            "phase TEXT NOT NULL, command_label TEXT NOT NULL, command_sha256 TEXT NOT NULL, "
            "member_count INTEGER NOT NULL, root_pid INTEGER NOT NULL, "
            "root_identity TEXT NOT NULL, started_at_ns INTEGER NOT NULL"
            ") WITHOUT ROWID"
        )
        conn.execute(
            "INSERT INTO active_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "legacy-token", 1, "legacy-job", "p", "c", "t", "command", "x",
                "a" * 64, 1, os.getpid(), "legacy-identity", time.time_ns(),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    before = db.read_bytes()
    records, errors = observer._read_records(tmp_path)

    assert errors == []
    assert len(records) == 1
    assert records[0]["owner_kind"] == "legacy_child"
    assert records[0]["owner_key"] is None
    assert db.read_bytes() == before


def test_read_only_query_waits_for_concurrent_schema_initialization(
    tmp_path, monkeypatch
):
    db = observer._db_path(tmp_path)
    db.parent.mkdir(parents=True)
    sqlite3.connect(db).close()
    sleeps = []

    def finish_initialization(delay):
        sleeps.append(delay)
        conn = observer._writer(tmp_path)
        conn.close()

    monkeypatch.setattr(observer.time, "sleep", finish_initialization)
    records, errors = observer._read_records(tmp_path)

    assert sleeps == [observer._SCHEMA_READY_DELAY_SECONDS]
    assert records == []
    assert errors == []


def test_corrupt_registry_returns_explicit_unknown_without_overwrite(tmp_path):
    db = observer._db_path(tmp_path)
    db.parent.mkdir(parents=True)
    db.write_bytes(b"not-a-sqlite-database")
    before = db.read_bytes()
    result = subprocess.run(
        [
            sys.executable,
            str(Path(observer.__file__)),
            "query",
            "--state-root",
            str(tmp_path),
            "--sample-interval",
            "0.05",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    assert result.returncode == 2
    assert payload["status"] == "unknown"
    assert payload["jobs"] == []
    assert payload["errors"][0]["kind"] == "query_failed"
    assert db.read_bytes() == before


def test_launch_reclamation_is_fail_safe_when_process_snapshot_fails(tmp_path, monkeypatch):
    current = observer._processes()[os.getpid()]
    observer._register(tmp_path, _metadata().payload(), current)
    monkeypatch.setattr(observer, "_processes", lambda: (_ for _ in ()).throw(OSError("denied")))
    conn = observer._writer(tmp_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        assert observer._cleanup_stale_locked(conn) == 0
        conn.execute("COMMIT")
    finally:
        conn.close()
    assert len(observer._read_records(tmp_path)[0]) == 1


def test_posix_launch_registers_owner_before_user_process_spawn(tmp_path, monkeypatch):
    owner = observer.ProcessRow(100, 1, "owner", 1, 0.0, 10, 1, 100)
    child = observer.ProcessRow(101, 100, "child", 2, 0.0, 10, 1, 100)
    events = []

    class FakeProcess:
        pid = 101

        def wait(self):
            events.append("wait")
            return 0

        def poll(self):
            return None

    monkeypatch.setattr(observer, "_posix_owner_row", lambda: owner)
    monkeypatch.setattr(
        observer,
        "_register",
        lambda *args, **kwargs: events.append("register") or "token",
    )
    monkeypatch.setattr(
        observer.subprocess,
        "Popen",
        lambda _command: events.append("spawn") or FakeProcess(),
    )
    monkeypatch.setattr(observer, "_child_row", lambda _proc: child)
    monkeypatch.setattr(
        observer,
        "_update_record_processes",
        lambda *args, **kwargs: events.append("update"),
    )
    monkeypatch.setattr(observer, "_processes", lambda: {})
    monkeypatch.setattr(observer, "_unregister", lambda *_args: events.append("unregister"))

    assert observer._run_posix_command(tmp_path, _metadata().payload(), ["tool"]) == 0
    assert events.index("register") < events.index("spawn")
    assert events[-1] == "unregister"


def test_windows_job_launch_assigns_keeper_registers_then_resumes_user(tmp_path, monkeypatch):
    from types import SimpleNamespace

    user_row = observer.ProcessRow(200, 100, "windows:200:1", 1, 0.0, None, None)
    keeper_row = observer.ProcessRow(300, 100, "windows:300:1", 1, 0.0, None, None)
    user = SimpleNamespace(hProcess="user-process", hThread="user-thread", dwProcessId=200)
    keeper = SimpleNamespace(hProcess="keeper-process", hThread="keeper-thread", dwProcessId=300)
    events = []
    token = "fixed-ready-cleanup-token"
    monkeypatch.setattr(observer.uuid, "uuid4", lambda: SimpleNamespace(hex=token))
    # A non-file at the best-effort ready path makes unlink raise.  That cleanup
    # occurs after user resume and therefore must never trigger fallback/retry.
    observer._win_keeper_ready_path(tmp_path, token).mkdir(parents=True)
    monkeypatch.setattr(observer, "_win_create_named_job", lambda name: events.append("job") or "job")
    monkeypatch.setattr(
        observer,
        "_win_create_suspended",
        lambda command: events.append("user-suspended") or user,
    )
    monkeypatch.setattr(
        observer,
        "_win_assign_process",
        lambda job, proc: events.append("assigned"),
    )
    monkeypatch.setattr(
        observer,
        "_win_create_keeper_suspended",
        lambda root, token, name: events.append("keeper-suspended") or keeper,
    )
    monkeypatch.setattr(
        observer,
        "_win_process_row",
        lambda proc: keeper_row if proc is keeper else user_row,
    )
    monkeypatch.setattr(
        observer,
        "_win_resume",
        lambda proc: events.append("keeper-resumed" if proc is keeper else "user-resumed"),
    )
    monkeypatch.setattr(
        observer,
        "_wait_for_keeper_ready",
        lambda root, token, name, proc: events.append("keeper-ready"),
    )

    def register(*args, **kwargs):
        events.append("registered")
        assert kwargs["owner_kind"] == "windows_job_v2"
        assert kwargs["owner_process"] == keeper_row
        return kwargs["token"]

    monkeypatch.setattr(observer, "_register", register)
    monkeypatch.setattr(observer, "_win_wait_exit", lambda proc: events.append("waited") or 0)
    monkeypatch.setattr(observer, "_win_job_pids", lambda job: {201})
    monkeypatch.setattr(observer, "_win_close", lambda handle: None)
    monkeypatch.setattr(
        observer,
        "_unregister",
        lambda *_args: (_ for _ in ()).throw(AssertionError("surviving job must remain")),
    )

    assert observer._run_windows_command(tmp_path, _metadata().payload(), ["pwsh"]) == 0
    assert events.index("assigned") < events.index("keeper-resumed")
    assert events.index("keeper-ready") < events.index("registered") < events.index("user-resumed")


def test_windows_job_name_uses_cross_session_global_namespace():
    assert observer._win_job_name("abc123") == r"Global\remrun-job-observer-v1-abc123"


def test_windows_breakaway_limit_uses_required_extended_information(monkeypatch):
    seen = {}

    class FakeKernel32:
        @staticmethod
        def CreateJobObjectW(_security, _name):
            return 123

        @staticmethod
        def SetInformationJobObject(_handle, info_class, pointer, size):
            seen["class"] = info_class
            seen["size"] = size
            limits = observer.ctypes.cast(
                pointer,
                observer.ctypes.POINTER(observer._WinJobExtendedLimitInformation),
            ).contents
            seen["flags"] = limits.BasicLimitInformation.LimitFlags
            return 1

    monkeypatch.setattr(observer, "_win_kernel32", lambda: FakeKernel32())
    monkeypatch.setattr(observer.ctypes, "set_last_error", lambda _value: None, raising=False)
    monkeypatch.setattr(observer.ctypes, "get_last_error", lambda: 0, raising=False)

    assert observer._win_create_named_job(r"Local\remrun-test") == 123
    assert seen == {
        "class": observer._WIN_JOB_EXTENDED_LIMIT_INFORMATION,
        "size": observer.ctypes.sizeof(observer._WinJobExtendedLimitInformation),
        "flags": observer._WIN_JOB_OBJECT_LIMIT_BREAKAWAY_OK,
    }


def test_windows_prestart_assignment_failure_falls_back_once(tmp_path, monkeypatch):
    from types import SimpleNamespace

    process = SimpleNamespace(hProcess="process", hThread="thread", dwProcessId=200)
    events = []

    class FakePopen:
        def __init__(self, command):
            events.append(("fallback", tuple(command)))

        def wait(self):
            return 7

    monkeypatch.setattr(observer, "_win_create_named_job", lambda name: "job")
    monkeypatch.setattr(observer, "_win_create_suspended", lambda command: process)
    monkeypatch.setattr(
        observer,
        "_win_assign_process",
        lambda job, proc: (_ for _ in ()).throw(OSError("nested job unavailable")),
    )
    monkeypatch.setattr(observer, "_win_discard_suspended", lambda proc: events.append("discarded"))
    monkeypatch.setattr(observer, "_win_close", lambda handle: None)
    monkeypatch.setattr(observer.subprocess, "Popen", FakePopen)

    assert observer._run_windows_command(tmp_path, _metadata().payload(), ["pwsh", "-c", "x"]) == 7
    assert events == ["discarded", ("fallback", ("pwsh", "-c", "x"))]


def test_posix_group_without_exact_generation_witness_is_unknown_not_running():
    record = {
        "owner_key": "77",
        "owner_pid": 50,
        "owner_identity": "old-owner",
        "witness_pid": 51,
        "witness_identity": "old-witness",
    }
    snapshot = {
        51: observer.ProcessRow(51, 1, "reused-witness", 20, 0.0, 1, 1, 77),
        60: observer.ProcessRow(60, 1, "new-member", 21, 0.0, 1, 1, 77),
    }

    state, rows, detail = observer._posix_owned_sample(record, snapshot)

    assert state == "unknown"
    assert rows == []
    assert "generation witness" in detail


def test_windows_job_sample_uses_exact_kernel_membership(monkeypatch):
    record = {"owner_key": r"Global\remrun-job-observer-v1-token"}
    snapshot = {
        10: observer.ProcessRow(10, 1, "windows:10:1", 1, 1.0, 100, 1),
    }
    monkeypatch.setattr(observer, "_windows_job_pids_by_name", lambda _name: {10, 11})

    state, rows, detail = observer._windows_owned_sample(record, snapshot)

    assert state == "live" and detail == ""
    assert [row.pid for row in rows] == [10, 11]
    assert rows[1].identity is None


@pytest.mark.skipif(os.name != "posix", reason="POSIX escape-boundary proof")
def test_deliberate_setsid_descendant_is_outside_observer_coverage(tmp_path):
    pid_file = tmp_path / "escaped.pid"
    code = (
        "import subprocess,sys; "
        "p=subprocess.Popen([sys.executable,'-S','-c','import time; time.sleep(30)'],"
        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,"
        "start_new_session=True); "
        f"open({str(pid_file)!r},'w').write(str(p.pid))"
    )
    command = [sys.executable, "-S", "-c", code]
    result = subprocess.run(
        [
            sys.executable,
            str(Path(observer.__file__)),
            "run",
            "--state-root",
            str(tmp_path / "state"),
            "--metadata-b64",
            _metadata(command).encoded(),
            "--",
            *command,
        ],
        capture_output=True,
        text=True,
        timeout=8,
        check=False,
    )
    escaped = int(pid_file.read_text())
    try:
        assert result.returncode == 0
        assert observer._query(tmp_path / "state", 0.05)["jobs"] == []
    finally:
        try:
            os.kill(escaped, signal.SIGKILL)
        except ProcessLookupError:
            pass


def test_windows_missing_job_name_is_unknown_not_stale(monkeypatch):
    record = {"owner_key": r"Global\remrun-job-observer-v1-token"}
    monkeypatch.setattr(observer, "_windows_job_pids_by_name", lambda _name: None)

    state, rows, detail = observer._windows_owned_sample(record, {})

    assert state == "unknown"
    assert rows == []
    assert "completion is unproved" in detail


def test_windows_v2_refuses_named_job_without_exact_keeper_generation(monkeypatch):
    record = {
        "owner_kind": "windows_job_v2",
        "owner_key": r"Global\remrun-job-observer-v1-token",
        "owner_pid": 300,
        "owner_identity": "windows:300:7",
    }

    def unexpected_open(_name):
        raise AssertionError("the name must not be trusted without its exact keeper")

    monkeypatch.setattr(observer, "_windows_job_pids_by_name", unexpected_open)
    state, rows, detail = observer._windows_owned_sample(record, {})

    assert state == "unknown"
    assert rows == []
    assert "handle-keeper generation" in detail


def test_windows_v2_uses_named_job_when_exact_keeper_generation_matches(monkeypatch):
    keeper = observer.ProcessRow(300, 1, "windows:300:7", 7, 0.0, 4096, 1)
    member = observer.ProcessRow(200, 1, "windows:200:5", 5, 0.0, 8192, 2)
    record = {
        "owner_kind": "windows_job_v2",
        "owner_key": r"Global\remrun-job-observer-v1-token",
        "owner_pid": keeper.pid,
        "owner_identity": keeper.identity,
    }
    monkeypatch.setattr(observer, "_windows_job_pids_by_name", lambda _name: {member.pid})

    state, rows, detail = observer._windows_owned_sample(
        record, {keeper.pid: keeper, member.pid: member}
    )

    assert state == "live"
    assert rows == [member]
    assert detail == ""


def test_launch_cleanup_preserves_windows_row_when_name_is_missing(tmp_path, monkeypatch):
    current = observer.ProcessRow(
        100, 1, "windows:100:1", 1, 0.0, None, None
    )
    observer._register(
        tmp_path,
        _metadata().payload(),
        current,
        token="keeper-missing",
        owner_kind="windows_job_v2",
        owner_key=r"Global\remrun-job-observer-v1-keeper-missing",
        owner_process=current,
        witness=current,
    )
    monkeypatch.setattr(observer, "_processes", lambda: {})
    monkeypatch.setattr(observer.os, "name", "nt")
    monkeypatch.setattr(observer, "_windows_job_pids_by_name", lambda _name: None)

    conn = observer._writer(tmp_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        assert observer._cleanup_stale_locked(conn) == 0
        conn.execute("COMMIT")
    finally:
        conn.close()

    records, errors = observer._read_records(tmp_path)
    assert errors == []
    assert [record["token"] for record in records] == ["keeper-missing"]


def test_legacy_launch_cleanup_cannot_delete_owned_v2_row_in_same_registry(tmp_path):
    current = observer._processes()[os.getpid()]
    token = "same-physical-token"
    observer._register(tmp_path, _metadata().payload(), current, token=token)

    conn = observer._writer(tmp_path)
    try:
        values = _metadata().payload()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO active_jobs("
            "token,schema,job_id,project,source_controller,target,phase,command_label,"
            "command_sha256,member_count,root_pid,root_identity,started_at_ns"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                token,
                1,
                "legacy-job",
                values["project"],
                values["source_controller"],
                values["target"],
                values["phase"],
                values["command_label"],
                values["command_sha256"],
                1,
                current.pid,
                current.identity,
                time.time_ns(),
            ),
        )
        conn.execute("COMMIT")

        # This is the old helper's reachable cleanup surface: it knows and deletes
        # only active_jobs.  Use the same token to prove table isolation rather
        # than relying on UUID uniqueness.
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM active_jobs WHERE token=?", (token,))
        conn.execute("COMMIT")
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('active_jobs','owned_jobs_v2')"
            )
        }
        assert tables == {"active_jobs", "owned_jobs_v2"}
        assert conn.execute("SELECT COUNT(*) FROM active_jobs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM owned_jobs_v2").fetchone()[0] == 1
    finally:
        conn.close()

    records, errors = observer._read_records(tmp_path)
    assert errors == []
    assert len(records) == 1
    assert records[0]["token"] == token
    assert records[0]["registry_table"] == observer.OWNED_TABLE


def test_windows_keeper_creation_is_detached_breakaway_and_inherits_no_handles(
    tmp_path, monkeypatch
):
    seen = {}

    class FakeKernel32:
        @staticmethod
        def CreateProcessW(
            application,
            command_line,
            _proc_security,
            _thread_security,
            inherit_handles,
            flags,
            _environment,
            _cwd,
            startup_pointer,
            _process_pointer,
        ):
            seen["application"] = application
            seen["command_line"] = command_line.value
            seen["inherit_handles"] = inherit_handles
            seen["flags"] = flags
            startup = observer.ctypes.cast(
                startup_pointer, observer.ctypes.POINTER(observer._WinStartupInfo)
            ).contents
            seen["startup_flags"] = startup.dwFlags
            return 1

    monkeypatch.setattr(observer, "_win_kernel32", lambda: FakeKernel32())
    process = observer._win_create_keeper_suspended(
        tmp_path, "abc123", observer._win_job_name("abc123")
    )

    assert isinstance(process, observer._WinProcessInformation)
    assert seen["application"] == sys.executable
    assert "hold-windows-job" in seen["command_line"]
    assert seen["inherit_handles"] is False
    assert seen["flags"] & observer._WIN_CREATE_SUSPENDED
    assert seen["flags"] & observer._WIN_DETACHED_PROCESS
    assert seen["flags"] & observer._WIN_CREATE_BREAKAWAY_FROM_JOB
    assert seen["startup_flags"] & observer._WIN_STARTF_USESTDHANDLES == 0


def test_windows_observed_root_breaks_from_ssh_job_before_named_job_assignment(
    monkeypatch,
):
    seen = {}

    class FakeKernel32:
        @staticmethod
        def GetStdHandle(_kind):
            return 1

        @staticmethod
        def SetHandleInformation(_handle, _mask, _flags):
            return 1

        @staticmethod
        def CreateProcessW(
            _application,
            _command_line,
            _proc_security,
            _thread_security,
            _inherit_handles,
            flags,
            _environment,
            _cwd,
            _startup_pointer,
            _process_pointer,
        ):
            seen["flags"] = flags
            return 1

    monkeypatch.setattr(observer, "_win_kernel32", lambda: FakeKernel32())
    monkeypatch.setattr(observer.shutil, "which", lambda _command: r"C:\\Python\\python.exe")

    process = observer._win_create_suspended(["python", "-c", "pass"])

    assert isinstance(process, observer._WinProcessInformation)
    assert seen["flags"] & observer._WIN_CREATE_SUSPENDED
    assert seen["flags"] & observer._WIN_CREATE_BREAKAWAY_FROM_JOB
    assert seen["flags"] & observer._WIN_DETACHED_PROCESS == 0


def test_windows_keeper_ready_failure_falls_back_once_before_user_resume(
    tmp_path, monkeypatch
):
    from types import SimpleNamespace

    user = SimpleNamespace(hProcess="user-process", hThread="user-thread", dwProcessId=200)
    keeper = SimpleNamespace(
        hProcess="keeper-process", hThread="keeper-thread", dwProcessId=300
    )
    user_row = observer.ProcessRow(200, 1, "windows:200:1", 1, 0.0, None, None)
    keeper_row = observer.ProcessRow(300, 1, "windows:300:1", 1, 0.0, None, None)
    events = []

    class FakePopen:
        def __init__(self, command):
            events.append(("fallback", tuple(command)))

        def wait(self):
            return 7

    monkeypatch.setattr(observer, "_win_create_named_job", lambda _name: "job")
    monkeypatch.setattr(observer, "_win_create_suspended", lambda _command: user)
    monkeypatch.setattr(observer, "_win_assign_process", lambda _job, _proc: None)
    monkeypatch.setattr(observer, "_win_create_keeper_suspended", lambda *_args: keeper)
    monkeypatch.setattr(
        observer,
        "_win_process_row",
        lambda proc: keeper_row if proc is keeper else user_row,
    )

    def resume(proc):
        events.append("keeper-resumed" if proc is keeper else "user-resumed")

    monkeypatch.setattr(observer, "_win_resume", resume)
    monkeypatch.setattr(
        observer,
        "_wait_for_keeper_ready",
        lambda *_args: (_ for _ in ()).throw(observer.RegistryError("keeper unavailable")),
    )
    monkeypatch.setattr(
        observer, "_win_discard_suspended", lambda proc: events.append("user-discarded")
    )
    monkeypatch.setattr(
        observer, "_win_terminate_process", lambda proc: events.append("keeper-terminated")
    )
    monkeypatch.setattr(observer, "_win_close", lambda _handle: None)
    monkeypatch.setattr(observer.subprocess, "Popen", FakePopen)

    assert observer._run_windows_command(
        tmp_path, _metadata().payload(), ["pwsh", "-c", "x"]
    ) == 7
    assert events.count(("fallback", ("pwsh", "-c", "x"))) == 1
    assert "keeper-resumed" in events
    assert "user-resumed" not in events
    assert "user-discarded" in events
    assert "keeper-terminated" in events


def test_windows_handle_keeper_unregisters_only_after_job_is_empty(tmp_path, monkeypatch):
    events = []
    samples = iter(({101}, set()))
    monkeypatch.setattr(observer.os, "name", "nt")
    monkeypatch.setattr(observer, "_win_open_job", lambda _name: "job-handle")
    monkeypatch.setattr(
        observer, "_win_job_pids", lambda _job: events.append("sample") or next(samples)
    )
    monkeypatch.setattr(
        observer, "_write_keeper_ready", lambda *_args: events.append("ready")
    )
    monkeypatch.setattr(
        observer, "_unregister", lambda *_args: events.append("unregister") or True
    )
    monkeypatch.setattr(observer.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(observer, "_win_close", lambda _handle: events.append("closed"))

    assert observer._run_windows_handle_keeper(
        tmp_path, "keeper-token", observer._win_job_name("keeper-token")
    ) == 0
    assert events == ["ready", "sample", "sample", "unregister", "closed"]
