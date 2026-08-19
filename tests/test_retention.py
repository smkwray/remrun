from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from remrun.state import (
    RetentionPolicy,
    cap_text,
    parse_run_timestamp,
    prune_state,
    run_is_failed,
)

NOW = datetime(2026, 6, 28, 12, 0, 0, tzinfo=timezone.utc)
POLICY = RetentionPolicy(full_log_days=30, failed_log_days=90, summary_days=365,
                         max_log_bytes=1000)


def make_run(runs_root: Path, age_days: int, *, exit_code: int = 0, files: bool = True) -> Path:
    dt = NOW - timedelta(days=age_days)
    rid = dt.strftime("%Y%m%dT%H%M%SZ") + "-MACBOX-proj"
    d = runs_root / rid
    d.mkdir(parents=True)
    (d / "summary.json").write_text(json.dumps({"run_id": rid, "exit_code": exit_code}))
    if files:
        (d / "stdout.log").write_text("x" * 100)
        (d / "stderr.log").write_text("")
        (d / "pre_local_manifest.json").write_text("{}")
        (d / "post_remote_manifest.json").write_text("{}")
    return d


# --- helpers ------------------------------------------------------------------

def test_parse_run_timestamp():
    dt = parse_run_timestamp("20260628T120000Z-MACBOX-proj")
    assert dt == datetime(2026, 6, 28, 12, 0, 0, tzinfo=timezone.utc)
    assert parse_run_timestamp("not-a-run") is None


def test_cap_text_passthrough_and_truncate():
    assert cap_text("short", 1000) == "short"
    big = "y" * 5000
    capped = cap_text(big, 1000)
    assert "truncated" in capped
    assert len(capped.encode()) <= 1000


def test_run_is_failed():
    assert not run_is_failed(None)
    assert not run_is_failed({"exit_code": 0})
    assert run_is_failed({"exit_code": 2})
    assert run_is_failed({"error": "boom"})


def test_prune_exempts_active_run_conflict_from_budget(tmp_path: Path):
    # WHY (audit B7): the in-flight run's just-saved recovery copy must survive the size
    # budget, or the summary prints a saved= path that no longer exists. Two equal-size
    # conflict dirs over a tiny budget: the exempt (current) one must survive, the other not.
    conflicts = tmp_path / "conflicts"
    old_id = (NOW - timedelta(days=1)).strftime("%Y%m%dT%H%M%SZ") + "-MACBOX-proj"
    cur_id = NOW.strftime("%Y%m%dT%H%M%SZ") + "-MACBOX-proj"
    for rid in (old_id, cur_id):
        d = conflicts / rid / "backup"
        d.mkdir(parents=True)
        (d / "big.bin").write_bytes(b"x" * 500)
    policy = RetentionPolicy(full_log_days=30, failed_log_days=90, summary_days=365,
                             max_log_bytes=1000, backup_days=3, max_backup_bytes=100)
    prune_state(policy, now=NOW, state_root=tmp_path, exempt_run_id=cur_id)
    assert (conflicts / cur_id).exists()       # exempt: survives even over budget
    assert not (conflicts / old_id).exists()   # non-exempt over-budget copy is pruned


def test_prune_exempts_all_attempted_run_conflicts_from_budget(tmp_path: Path):
    conflicts = tmp_path / "conflicts"
    first_id = (NOW - timedelta(minutes=2)).strftime("%Y%m%dT%H%M%SZ") + "-SIM_A-proj"
    second_id = (NOW - timedelta(minutes=1)).strftime("%Y%m%dT%H%M%SZ") + "-SIM_B-proj"
    old_id = (NOW - timedelta(days=1)).strftime("%Y%m%dT%H%M%SZ") + "-OLD-proj"
    for rid in (old_id, first_id, second_id):
        d = conflicts / rid
        d.mkdir(parents=True)
        (d / "conflicts.json").write_bytes(b"x" * 500)
    policy = RetentionPolicy(backup_days=3, max_backup_bytes=100)

    prune_state(
        policy,
        now=NOW,
        state_root=tmp_path,
        exempt_run_ids={first_id, second_id},
    )

    assert (conflicts / first_id).exists()
    assert (conflicts / second_id).exists()
    assert not (conflicts / old_id).exists()


# --- tiered policy ------------------------------------------------------------

def test_policy_keeps_recent(tmp_path: Path):
    runs = tmp_path / "runs"
    make_run(runs, age_days=5)
    prune_state(POLICY, now=NOW, state_root=tmp_path)
    assert (runs / (((NOW - timedelta(days=5)).strftime("%Y%m%dT%H%M%SZ")) + "-MACBOX-proj")).exists()


def test_policy_trims_heavy_after_full_window(tmp_path: Path):
    runs = tmp_path / "runs"
    d = make_run(runs, age_days=45, exit_code=0)
    rep = prune_state(POLICY, now=NOW, state_root=tmp_path)
    assert d.exists()
    assert (d / "summary.json").exists()
    assert not (d / "stdout.log").exists()
    assert not (d / "pre_local_manifest.json").exists()
    assert rep.runs_trimmed == 1


def test_policy_deletes_after_summary_window(tmp_path: Path):
    runs = tmp_path / "runs"
    d = make_run(runs, age_days=400)
    rep = prune_state(POLICY, now=NOW, state_root=tmp_path)
    assert not d.exists()
    assert rep.runs_deleted == 1


def test_failed_runs_kept_longer(tmp_path: Path):
    runs = tmp_path / "runs"
    d = make_run(runs, age_days=45, exit_code=2)  # within 90d failed window
    prune_state(POLICY, now=NOW, state_root=tmp_path)
    assert (d / "stdout.log").exists()  # not trimmed yet


def test_failed_run_trimmed_after_failed_window(tmp_path: Path):
    runs = tmp_path / "runs"
    d = make_run(runs, age_days=100, exit_code=2)
    prune_state(POLICY, now=NOW, state_root=tmp_path)
    assert (d / "summary.json").exists()
    assert not (d / "stdout.log").exists()


# --- explicit modes -----------------------------------------------------------

def test_keep_n(tmp_path: Path):
    runs = tmp_path / "runs"
    for age in (1, 2, 3, 4, 5):
        make_run(runs, age_days=age)
    rep = prune_state(POLICY, now=NOW, state_root=tmp_path, keep=2)
    remaining = sorted(d.name for d in runs.iterdir())
    assert len(remaining) == 2
    assert rep.runs_deleted == 3


def test_older_than(tmp_path: Path):
    runs = tmp_path / "runs"
    make_run(runs, age_days=10)
    make_run(runs, age_days=40)
    rep = prune_state(POLICY, now=NOW, state_root=tmp_path, older_than_days=20)
    assert rep.runs_deleted == 1
    assert len(list(runs.iterdir())) == 1


def test_dry_run_changes_nothing(tmp_path: Path):
    runs = tmp_path / "runs"
    d = make_run(runs, age_days=400)
    rep = prune_state(POLICY, now=NOW, state_root=tmp_path, dry_run=True)
    assert d.exists()  # not actually deleted
    assert rep.runs_deleted == 1  # but reported


def _make_conflict(tmp_path: Path, age_days: int, size_bytes: int) -> Path:
    dt = NOW - timedelta(days=age_days)
    cdir = tmp_path / "conflicts" / (dt.strftime("%Y%m%dT%H%M%SZ") + "-MACBOX-sync")
    (cdir / "backup").mkdir(parents=True)
    (cdir / "backup" / "f.bin").write_bytes(b"x" * size_bytes)
    return cdir


def test_backups_pruned_by_backup_days(tmp_path: Path):
    pol = RetentionPolicy(backup_days=3, max_backup_bytes=0)
    old = _make_conflict(tmp_path, age_days=5, size_bytes=10)   # > 3 days -> deleted
    keep = _make_conflict(tmp_path, age_days=1, size_bytes=10)  # within 3 days -> kept
    prune_state(pol, now=NOW, state_root=tmp_path)
    assert not old.exists() and keep.exists()


def test_backup_size_budget_prunes_oldest_first(tmp_path: Path):
    # Budget 150 B, two 100 B dirs both within the age window -> drop the oldest.
    pol = RetentionPolicy(backup_days=999, max_backup_bytes=150)
    old = _make_conflict(tmp_path, age_days=2, size_bytes=100)
    new = _make_conflict(tmp_path, age_days=1, size_bytes=100)
    prune_state(pol, now=NOW, state_root=tmp_path)
    assert not old.exists() and new.exists()


def test_backup_skips_large_files(tmp_path: Path):
    from remrun.reconcile import _backup_local
    local = tmp_path / "local"
    local.mkdir()
    (local / "big.bin").write_bytes(b"x" * 200)
    (local / "small.txt").write_text("hi")
    backup = tmp_path / "backup"
    assert _backup_local(local, "small.txt", backup, max_bytes=100) is True
    assert _backup_local(local, "big.bin", backup, max_bytes=100) is False
    assert (backup / "small.txt").exists()
    assert not (backup / "big.bin").exists()
