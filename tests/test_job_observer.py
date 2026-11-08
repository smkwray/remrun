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
    item = JobObservation.for_command(
        job_id="git-sync-push-1",
        project="demo",
        source_controller="CTRL",
        target="PEER",
        phase="git-sync",
        command=["remrun", "git-sync", "PEER", "--push"],
        declared_label="git-sync:push",
    )

    with observation_module.observe_controller_operation(item, state_root):
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


def test_controller_operations_from_different_projects_remain_separate_rows(
    tmp_path, monkeypatch
):
    state_root = tmp_path / "state"
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

    with observation_module.observe_controller_operation(alpha, state_root):
        with observation_module.observe_controller_operation(zeta, state_root):
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


_BOOT_NS = 1_700_000_000 * 1_000_000_000


def _boot_case(tmp_path, monkeypatch, kind: str, boot_ns: int | None):
    """Seed one owned record from before the boot and one from just after it.

    Both rows sample as UNKNOWN on their own: a Windows record whose handle keeper
    is gone, or a POSIX group with members but no exact generation witness. That
    is the phantom a restart leaves behind, so only the boot proof can hide it.
    """
    if kind == "windows_job_v2":
        monkeypatch.setattr(observer.os, "name", "nt")
        monkeypatch.setattr(observer, "_platform_name", lambda: "windows")
        snapshot: dict[int, observer.ProcessRow] = {}
        owner = (300, "windows:300:7", r"Global\remrun-job-observer-v1-{token}")
    else:
        monkeypatch.setattr(observer.os, "name", "posix")
        monkeypatch.setattr(observer, "_platform_name", lambda: "linux")
        snapshot = {501: observer.ProcessRow(501, 1, "linux:501:9", 9, 0.0, 4096, 1, pgid=500)}
        owner = (500, "linux:500:8", "500")

        def no_such_process(pid, _signal):
            raise ProcessLookupError(pid)

        monkeypatch.setattr(observer.os, "kill", no_such_process)
    values = _metadata().payload()
    conn = observer._writer(tmp_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        for token, started in (
            ("before-boot", _BOOT_NS - observer._BOOT_STALE_MARGIN_NS - 1_000_000_000),
            ("after-boot", _BOOT_NS + 1_000_000_000),
        ):
            conn.execute(
                "INSERT INTO owned_jobs_v2("
                "token,schema,job_id,project,source_controller,target,phase,command_label,"
                "command_sha256,member_count,root_pid,root_identity,started_at_ns,"
                "owner_kind,owner_key,owner_pid,owner_identity,witness_pid,witness_identity"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (token, 2, token, values["project"], values["source_controller"],
                 values["target"], values["phase"], values["command_label"],
                 values["command_sha256"], 1, owner[0], owner[1], started, kind,
                 owner[2].format(token=token), owner[0], owner[1], owner[0], owner[1]),
            )
        conn.execute("COMMIT")
    finally:
        conn.close()
    sampled: list[str] = []
    real_owned_sample = observer._owned_sample

    def owned_sample(record, rows):
        sampled.append(str(record["token"]))
        return real_owned_sample(record, rows)

    monkeypatch.setattr(observer, "_owned_sample", owned_sample)
    monkeypatch.setattr(observer, "_processes", lambda: snapshot)
    monkeypatch.setattr(observer, "_boot_time_ns", lambda: boot_ns)
    db = observer._db_path(tmp_path)
    before = (db.stat().st_mtime_ns, hashlib.sha256(db.read_bytes()).hexdigest())
    payload = observer._query(tmp_path, 0.05)
    after = (db.stat().st_mtime_ns, hashlib.sha256(db.read_bytes()).hexdigest())
    # Queries are read-only: the boot proof hides rows, it never deletes them.
    assert after == before
    assert payload["registry"]["query_mutated_registry"] is False
    assert len(observer._read_records(tmp_path)[0]) == 2
    return payload, sampled


@pytest.mark.parametrize("kind", ["windows_job_v2", "posix_pgid"])
def test_record_from_before_current_boot_is_stale_without_ownership_sampling(
    tmp_path, monkeypatch, kind
):
    payload, sampled = _boot_case(tmp_path, monkeypatch, kind, _BOOT_NS)

    # No process outlives a restart, so the pre-boot row is stale outright; an
    # inconclusive ownership sample must not resurrect it as an UNKNOWN phantom.
    assert payload["registry"]["stale_hidden"] == 1
    assert payload["registry"]["boot_time_unix_ns"] == _BOOT_NS
    assert "before-boot" not in sampled
    # A job started after the boot keeps today's ownership verdict.
    assert [(job["job_id"], job["state"]) for job in payload["jobs"]] == [
        ("after-boot", "UNKNOWN")
    ]


@pytest.mark.parametrize("kind", ["windows_job_v2", "posix_pgid"])
def test_unreadable_boot_time_hides_nothing(tmp_path, monkeypatch, kind):
    payload, sampled = _boot_case(tmp_path, monkeypatch, kind, None)

    assert payload["registry"]["stale_hidden"] == 0
    assert payload["registry"]["boot_time_unix_ns"] is None
    assert sorted(sampled) == ["after-boot", "before-boot"]
    assert sorted((job["job_id"], job["state"]) for job in payload["jobs"]) == [
        ("after-boot", "UNKNOWN"),
        ("before-boot", "UNKNOWN"),
    ]


@pytest.mark.parametrize(
    ("stdout", "expected"),
    [
        ("{ sec = 1700000000, usec = 250000 } Tue Nov 14 22:13:20 2023\n",
         1_700_000_000_250_000_000),
        ("unexpected\n", None),
    ],
)
def test_darwin_boot_time_reads_kern_boottime_or_fails_closed(monkeypatch, stdout, expected):
    monkeypatch.setattr(observer.sys, "platform", "darwin")
    monkeypatch.setattr(
        observer.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, stdout, ""),
    )
    assert observer._boot_time_ns() == expected


def test_boot_time_fails_closed_without_a_sleep_counting_source(monkeypatch):
    # time.monotonic() stops during sleep, so it must never stand in for uptime.
    monkeypatch.setattr(observer.sys, "platform", "sunos5")
    monkeypatch.setattr(observer.os, "name", "posix")
    assert observer._boot_time_ns() is None


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
    monkeypatch.setattr(observer.sys, "_base_executable", "C:\\Python314\\python.exe")
    process = observer._win_create_keeper_suspended(
        tmp_path, "abc123", observer._win_job_name("abc123")
    )

    assert isinstance(process, observer._WinProcessInformation)
    assert seen["application"] == "C:\\Python314\\python.exe"
    assert seen["command_line"].startswith("C:\\Python314\\python.exe")
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


@pytest.mark.parametrize(
    ("keeper_matches", "lingering"), [(True, False), (False, False), (True, True)]
)
def test_cancel_windows_job_requires_exact_keeper_and_only_terminates_owned_handle(
    tmp_path, monkeypatch, keeper_matches, lingering,
):
    from types import SimpleNamespace

    metadata = _metadata().payload()
    keeper = observer.ProcessRow(300, 1, "windows:300:7", 7, 0.0, 4096, 1)
    record = {**metadata, "owner_kind": "windows_job_v2", "owner_key": "unique-job",
              "owner_pid": keeper.pid, "owner_identity": keeper.identity}
    monkeypatch.setattr(observer, "_read_records", lambda _root: ([record], []))
    monkeypatch.setattr(observer, "_processes", lambda: {300: keeper} if keeper_matches else {})
    members = {200, 201}
    monkeypatch.setattr(observer, "_windows_job_pids_by_name", lambda _name: set(members))
    opened, terminated, closed = [], [], []

    def open_job(name, *, terminate=False):
        opened.append((name, terminate))
        return "owned-handle"

    def terminate(handle, code):
        terminated.append((handle, code))
        if not lingering:
            members.clear()
        return True

    monkeypatch.setattr(observer, "_win_open_job", open_job)
    monkeypatch.setattr(observer, "_win_kernel32", lambda: SimpleNamespace(TerminateJobObject=terminate))
    monkeypatch.setattr(observer, "_win_job_pids", lambda handle: set(members))
    monkeypatch.setattr(observer, "_win_close", closed.append)
    monkeypatch.setattr(observer, "_CANCEL_SETTLE_SECONDS", 0.0)
    receipt = observer._cancel_windows_job(tmp_path, metadata)
    assert receipt["owned_job_cancelled"] is (keeper_matches and not lingering)
    # A refusal says whether the Job was terminated: only one that signalled nothing
    # proves the job's completion was not caused by this cancel.
    assert receipt.get("signals_sent", "absent") == (
        True if lingering else False if not keeper_matches else "absent"
    )
    assert opened == [("unique-job", True)]
    assert terminated == ([("owned-handle", 130)] if keeper_matches else [])
    assert closed == ["owned-handle"]


def test_cancel_windows_missing_or_mismatched_record_does_not_open_job(tmp_path, monkeypatch):
    metadata = _metadata().payload()
    monkeypatch.setattr(observer, "_read_records", lambda _root: ([{**metadata, "job_id": "other"}], []))
    monkeypatch.setattr(observer, "_win_open_job", lambda *_a, **_kw: pytest.fail("unproved owner"))
    assert observer._cancel_windows_job(tmp_path, metadata)["owned_job_cancelled"] is False


@pytest.mark.parametrize("failed", [False, True])
def test_observer_completion_marker_requires_normal_command_return(tmp_path, monkeypatch, capsys, failed):
    def run(*_args):
        if failed:
            raise RuntimeError("wait failed after launch")
        return 7

    monkeypatch.setattr(observer, "_run_command", run)
    token = "a" * 32
    argv = ["run", "--state-root", str(tmp_path), "--metadata-b64", _metadata().encoded(),
            "--exit-token", token, "--", "example"]
    if failed:
        with pytest.raises(RuntimeError, match="wait failed"):
            observer.main(argv)
        assert "__REMRUN_OBSERVED_EXIT_" not in capsys.readouterr().err
    else:
        assert observer.main(argv) == 7
        assert capsys.readouterr().err == f"__REMRUN_OBSERVED_EXIT_{token}__7;"


def test_windows_postpublication_resume_failure_never_relaunches(tmp_path, monkeypatch):
    from types import SimpleNamespace

    user = SimpleNamespace(hProcess="user", hThread="user-thread", dwProcessId=200)
    keeper = SimpleNamespace(hProcess="keeper", hThread="keeper-thread", dwProcessId=300)
    events = []
    monkeypatch.setattr(observer, "_win_create_named_job", lambda _name: "job")
    monkeypatch.setattr(observer, "_win_create_suspended", lambda _command: user)
    monkeypatch.setattr(observer, "_win_assign_process", lambda *_a: None)
    monkeypatch.setattr(observer, "_win_create_keeper_suspended", lambda *_a: keeper)
    monkeypatch.setattr(observer, "_win_process_row", lambda proc: observer.ProcessRow(
        proc.dwProcessId, 100, f"windows:{proc.dwProcessId}:1", 1, 0.0, None, None,
    ))
    monkeypatch.setattr(observer, "_wait_for_keeper_ready", lambda *_a: None)
    monkeypatch.setattr(observer, "_register", lambda *_a, **_kw: events.append("published"))
    monkeypatch.setattr(observer, "_unregister", lambda *_a: events.append("unregistered"))
    monkeypatch.setattr(observer, "_win_discard_suspended", lambda proc: events.append("discarded"))
    monkeypatch.setattr(observer, "_win_terminate_process", lambda proc: None)
    monkeypatch.setattr(observer, "_win_close", lambda handle: None)

    def resume(proc):
        if proc is user:
            raise OSError("owned job was cancelled before ResumeThread")

    def fallback(*_a, **_kw):
        pytest.fail("a published command must never be relaunched unobserved")

    monkeypatch.setattr(observer, "_win_resume", resume)
    monkeypatch.setattr(observer.subprocess, "Popen", fallback)
    metadata = JobObservation.for_command(
        job_id="run-1", project="example-project", source_controller="CTRL",
        target="WINBOX", phase="command", command=["example.exe"],
    ).payload()
    with pytest.raises(OSError, match="cancelled before ResumeThread"):
        observer._run_windows_command(tmp_path, metadata, ["example.exe"])
    assert events == ["published", "unregistered", "discarded"]


# --- exact POSIX cancel ------------------------------------------------------
#
# The incident this guards against: an operator stopped a job by signalling the
# group of its top-level remote helper, which was the SSH server's session group,
# and ended unrelated sessions of the same user. Cancel must reach exactly the
# registered job's own group and nothing else in the shared session.

_JOB_CODE = (
    "import pathlib,subprocess,sys,time; "
    "g=subprocess.Popen([sys.executable,'-S','-c','import time; time.sleep(60)']); "
    "pathlib.Path(sys.argv[1]).write_text(f'{g.pid}'); time.sleep(60)"
)


def _job_metadata(job_id: str, command: list[str], **changes: str) -> JobObservation:
    from dataclasses import replace

    item = JobObservation.for_command(
        job_id=job_id, project="example-project", source_controller="CTRL",
        target="POSIXBOX", phase="command", command=command,
    )
    return replace(item, **changes) if changes else item


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_gone(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.02)
    return False


def _start_session(tmp_path: Path, jobs: dict[str, list[str]]):
    """Start one session (like one SSH login) holding an observer per job.

    The session leader reaps every child as the durable supervisor does, and its
    own process group stands in for the server's session group.
    """
    root = tmp_path / "state"
    leader = (
        "import os,subprocess,sys,json\n"
        "for argv in json.loads(sys.argv[1]): subprocess.Popen(argv)\n"
        "while True:\n"
        "    try: os.wait()\n"
        "    except ChildProcessError: break\n"
    )
    argvs = []
    for job_id, command in jobs.items():
        argvs.append([
            sys.executable, "-S", str(Path(observer.__file__)), "run",
            "--state-root", str(root),
            "--metadata-b64", _job_metadata(job_id, command).encoded(), "--", *command,
        ])
    session = subprocess.Popen(
        [sys.executable, "-S", "-c", leader, json.dumps(argvs)],
        start_new_session=True,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        records, _ = observer._read_records(root)
        rows = {str(row["job_id"]): row for row in records}
        if set(rows) == set(jobs) and all(
            int(row["root_pid"]) != int(row["owner_pid"]) for row in rows.values()
        ):
            return session, root, rows
        time.sleep(0.02)
    session.kill()
    raise AssertionError("observed jobs did not register their command roots")


def _stop_session(session, rows) -> None:  # noqa: ANN001
    for row in rows.values():
        try:
            os.killpg(int(row["owner_key"]), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    try:
        os.killpg(session.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    session.wait(timeout=5)


def _cancel_cli(root: Path, item: JobObservation) -> tuple[int, dict, str]:
    result = subprocess.run(
        [sys.executable, "-S", str(Path(observer.__file__)), "cancel",
         "--state-root", str(root), "--metadata-b64", item.encoded()],
        capture_output=True, text=True, timeout=20, check=False,
    )
    return result.returncode, json.loads(result.stdout), result.stderr


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group cancel")
def test_posix_cancel_stops_only_the_exact_job_in_a_shared_session(tmp_path):
    marks = {name: tmp_path / f"{name}.grandchild" for name in ("run-a", "run-b")}
    commands = {
        name: [sys.executable, "-S", "-c", _JOB_CODE, str(mark)] for name, mark in marks.items()
    }
    session, root, rows = _start_session(tmp_path, commands)
    try:
        deadline = time.monotonic() + 10
        while not all(mark.exists() for mark in marks.values()):
            assert time.monotonic() < deadline, "job grandchildren did not start"
            time.sleep(0.02)
        grandchild = {name: int(mark.read_text()) for name, mark in marks.items()}
        # Both jobs share the session; each observer leads its own group.
        assert os.getsid(int(rows["run-a"]["owner_pid"])) == session.pid
        assert os.getsid(int(rows["run-b"]["owner_pid"])) == session.pid
        assert int(rows["run-a"]["owner_key"]) != session.pid

        code, receipt, _stderr = _cancel_cli(root, _job_metadata("run-a", commands["run-a"]))

        assert code == 0
        assert receipt["owned_job_cancelled"] is True
        for pid in (rows["run-a"]["owner_pid"], rows["run-a"]["root_pid"], grandchild["run-a"]):
            assert _wait_gone(int(pid))
        # The other job and the session's own group (other sessions in the
        # incident) are untouched.
        assert session.poll() is None
        for pid in (rows["run-b"]["owner_pid"], rows["run-b"]["root_pid"], grandchild["run-b"]):
            assert _alive(int(pid))
        snapshot = observer._processes()
        assert snapshot[int(rows["run-b"]["root_pid"])].identity == rows["run-b"]["root_identity"]
    finally:
        _stop_session(session, rows)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group cancel")
def test_posix_cancel_refuses_any_identity_but_the_exact_registered_one(tmp_path):
    command = [sys.executable, "-S", "-c", "import time; time.sleep(60)"]
    session, root, rows = _start_session(tmp_path, {"run-a": command})
    row = rows["run-a"]
    try:
        exact = _job_metadata("run-a", command)
        wrong = {
            "no observer row": _job_metadata("run-unregistered", command),
            "other project": _job_metadata("run-a", command, project="other-project"),
            "other target": _job_metadata("run-a", command, target="OTHERBOX"),
            "other controller": _job_metadata("run-a", command, source_controller="OTHER"),
            "other phase": _job_metadata("run-a", command, phase="fleet-worker"),
            "other command": _job_metadata("run-a", [*command, "x"]),
        }
        for case, item in wrong.items():
            code, receipt, stderr = _cancel_cli(root, item)
            assert (code, receipt["owned_job_cancelled"]) == (2, False), case
            assert "cancel refused" in stderr, case
            assert _alive(int(row["root_pid"])), case
        # The same live job is cancellable by its exact identity, so the refusals
        # above were about identity and nothing else.
        code, receipt, _stderr = _cancel_cli(root, exact)
        assert (code, receipt["owned_job_cancelled"]) == (0, True)
        assert _wait_gone(int(row["root_pid"]))
    finally:
        _stop_session(session, rows)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group cancel")
def test_posix_cancel_refuses_when_the_root_left_its_registered_group(tmp_path):
    """A root that moved to another group no longer proves the group is the job's."""
    command = [
        sys.executable, "-S", "-c", "import os,time; os.setpgid(0,0); time.sleep(60)",
    ]
    session, root, rows = _start_session(tmp_path, {"run-a": command})
    row = rows["run-a"]
    try:
        deadline = time.monotonic() + 5
        while observer._processes()[int(row["root_pid"])].pgid != int(row["root_pid"]):
            assert time.monotonic() < deadline, "root did not leave the observer's group"
            time.sleep(0.02)
        code, receipt, stderr = _cancel_cli(root, _job_metadata("run-a", command))
        assert (code, receipt["owned_job_cancelled"]) == (2, False)
        assert "not live inside its registered group" in stderr
        assert _alive(int(row["owner_pid"]))
        assert _alive(int(row["root_pid"]))
    finally:
        try:
            os.killpg(int(row["root_pid"]), signal.SIGKILL)
        except ProcessLookupError:
            pass
        _stop_session(session, rows)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group cancel")
def test_posix_cancel_refuses_a_reused_pid_and_never_signals(tmp_path, monkeypatch):
    """A live pid in the registered group whose start identity differs is a reused pid."""
    stranger = subprocess.Popen(
        [sys.executable, "-S", "-c", "import time; time.sleep(60)"], start_new_session=True,
    )
    try:
        live = None
        deadline = time.monotonic() + 5
        while live is None or live.identity is None:
            assert time.monotonic() < deadline
            live = observer._processes().get(stranger.pid)
        metadata = _metadata().payload()
        record = {
            **metadata, "owner_kind": "posix_pgid", "owner_key": str(stranger.pid),
            "root_pid": stranger.pid, "root_identity": live.identity + "-earlier-generation",
        }
        monkeypatch.setattr(observer, "_read_records", lambda _root: ([record], []))

        def killpg(pgid, sig):  # noqa: ANN001
            if sig != 0:
                pytest.fail("an unproved group must never be signalled")
            return os.killpg(pgid, sig)

        monkeypatch.setattr(observer.os, "killpg", killpg)
        receipt = observer._cancel_posix_job(tmp_path, metadata)
        assert (receipt["owned_job_cancelled"], receipt["signals_sent"]) == (False, False)
        assert stranger.poll() is None
    finally:
        stranger.kill()
        stranger.wait(timeout=5)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group cancel")
def test_posix_cancel_asks_first_and_kills_only_what_outlives_the_grace(tmp_path, monkeypatch):
    """A stop sends SIGTERM first so a job can flush or checkpoint; a job that ignores
    it is still stopped, with SIGKILL, once the grace period has passed."""
    ignoring = tmp_path / "ignoring-sigterm"
    command = [sys.executable, "-S", "-c",
               "import pathlib, signal, sys, time; "
               "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
               "pathlib.Path(sys.argv[1]).write_text('1'); time.sleep(60)", str(ignoring)]
    session, root, rows = _start_session(tmp_path, {"run-a": command})
    sent: list[int] = []
    real_killpg = os.killpg

    def recording_killpg(pgid, sig):  # noqa: ANN001
        if sig != 0:
            sent.append(sig)
        return real_killpg(pgid, sig)

    monkeypatch.setattr(observer.os, "killpg", recording_killpg)
    monkeypatch.setattr(observer, "_CANCEL_TERM_GRACE_SECONDS", 0.5)
    try:
        # Registration precedes the job's own setup; a SIGTERM sent before it ignores
        # the signal would end it at once and prove nothing about SIGKILL.
        deadline = time.monotonic() + 10
        while not ignoring.exists():
            assert time.monotonic() < deadline, "job did not start ignoring SIGTERM"
            time.sleep(0.02)
        started = time.monotonic()
        receipt = observer._cancel_posix_job(root, _job_metadata("run-a", command).payload())
        assert receipt["owned_job_cancelled"] is True
        # The confirmed receipt is unchanged: no refusal-only fields.
        assert set(receipt) == {"schema", "job_id", "command_sha256", "owned_job_cancelled"}
        assert sent == [signal.SIGTERM, signal.SIGKILL]
        assert time.monotonic() - started >= 0.5
        assert _wait_gone(int(rows["run-a"]["root_pid"]))
    finally:
        _stop_session(session, rows)


@pytest.mark.parametrize("after_grace", ["reused", "unseen", "same", "late child"])
def test_posix_cancel_kills_only_a_group_it_can_still_prove(tmp_path, monkeypatch, after_grace):
    """During the grace period the job's group can empty and its number be taken by an
    unrelated group. SIGKILL goes out only while a member seen at proof time, by pid
    and start identity, is still in the group; otherwise the stranger is left alone
    and the refusal says a signal was already sent."""
    pgid = os.getpgrp() + 7919  # never this process's group; every signal is injected
    leader = observer.ProcessRow(pgid, 1, "start:leader", 1, 0.0, 0, 1, pgid)
    job = observer.ProcessRow(pgid + 1, pgid, "start:job", 2, 0.0, 0, 1, pgid)
    metadata = _metadata().payload()
    record = {
        **metadata, "owner_kind": "posix_pgid", "owner_key": str(pgid),
        "root_pid": job.pid, "root_identity": job.identity,
    }
    monkeypatch.setattr(observer, "_read_records", lambda _root: ([record], []))
    stranger = observer.ProcessRow(pgid + 2, 1, "start:stranger", 9, 0.0, 0, 1, pgid)
    reused_pid = observer.ProcessRow(job.pid, 1, "start:later", 9, 0.0, 0, 1, pgid)
    late_child = observer.ProcessRow(pgid + 3, job.pid, "start:child", 3, 0.0, 0, 1, pgid)
    members = {
        "reused": [stranger, reused_pid],
        # The group still answers kill(0) but shows no member to identify.
        "unseen": [],
        "same": [job],
        "late child": [job, late_child],
    }[after_grace]
    snapshots = iter([
        {leader.pid: leader, job.pid: job},
        {row.pid: row for row in members},
    ])
    monkeypatch.setattr(observer, "_processes", lambda: next(snapshots))
    sent: list[int] = []
    group = {"exists": True}

    def killpg(target, sig):  # noqa: ANN001
        assert target == pgid
        if sig == 0:
            if not group["exists"]:
                raise ProcessLookupError
            return
        sent.append(sig)
        if sig == signal.SIGKILL:
            group["exists"] = False

    monkeypatch.setattr(observer.os, "killpg", killpg)
    monkeypatch.setattr(observer, "_CANCEL_TERM_GRACE_SECONDS", 0.0)
    monkeypatch.setattr(observer, "_CANCEL_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(observer.time, "sleep", lambda _seconds: None)

    receipt = observer._cancel_posix_job(tmp_path, metadata)

    if after_grace in {"reused", "unseen"}:
        assert sent == [signal.SIGTERM]
        assert (receipt["owned_job_cancelled"], receipt["signals_sent"]) == (False, True)
    else:
        assert sent == [signal.SIGTERM, signal.SIGKILL]
        assert receipt["owned_job_cancelled"] is True


@pytest.mark.skipif(os.name != "posix", reason="POSIX durable supervisor")
def test_durable_supervisor_records_a_cancelled_observed_job_as_complete(tmp_path):
    """The stop reaches the supervisor as an ordinary wrapper exit, so resume can finalize."""
    from remrun import _durable_runner as durable_runner

    root = tmp_path / "target-state"
    run_id = "run-cancel"
    command = [sys.executable, "-S", "-c", "import time; time.sleep(60)"]
    item = _job_metadata(run_id, command)
    ready = root / "durable-runs" / run_id / "observer-ready.json"
    spec = {
        "schema": 1, "run_id": run_id, "resume_token": "resume-token",
        "controller": "CTRL", "project_id": "example-project", "target": "POSIXBOX",
        "command_sha256": item.command_sha256,
        "argv": [
            sys.executable, "-S", str(Path(observer.__file__)), "run",
            "--state-root", str(root), "--metadata-b64", item.encoded(),
            "--ready-file", str(ready), "--", *command,
        ],
        "ready_path": str(ready), "max_log_bytes": 4096,
        "created_at": "2026-11-08T00:00:00Z",
    }
    launched = durable_runner._launch(root, json.dumps(spec).encode())
    assert launched["state"] == "running"
    status = launched
    try:
        _wait_for_record(root, predicate=lambda row: int(row["root_pid"]) != int(row["owner_pid"]))
        code, receipt, _stderr = _cancel_cli(root, item)
        assert (code, receipt["owned_job_cancelled"]) == (0, True)
        deadline = time.monotonic() + 10
        while status["state"] != "complete":
            assert time.monotonic() < deadline, status
            time.sleep(0.05)
            status = durable_runner._status(root, run_id, "resume-token", False)
        # The job ignored nothing, so the first, polite signal ended it.
        assert status["wrapper_exit_code"] == 128 + signal.SIGTERM
    finally:
        records, _ = observer._read_records(root)
        for row in records:
            try:
                os.killpg(int(row["owner_key"]), signal.SIGKILL)
            except (ProcessLookupError, PermissionError, ValueError):
                pass
