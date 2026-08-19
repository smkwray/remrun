from __future__ import annotations

import json
import os
import socket
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

import remrun.gitsync as gitsync_module
from remrun import _job_observer as observer
from remrun.config import RemrunConfig, current_os_key
from remrun.gitsync import (
    EXIT_DIVERGED,
    EXIT_INFRA,
    EXIT_OK,
    GitSyncError,
    _ensure_remote_git_repo,
    _parse_dirty_summary,
    _ref_namespace,
    git_sync_status_result,
    install_git_sync_hook,
    run_git_sync,
    run_git_sync_result,
    run_git_sync_status,
    uninstall_git_sync_hook,
)
from remrun.models import Device
from remrun.output import Reporter
from remrun.transport import ExecResult, LocalSimTransport


def posix(p: Path) -> str:
    return str(p).replace("\\", "/")


def git(repo: Path, *args: str) -> str:
    res = subprocess.run(["git", *args], cwd=str(repo), text=True, capture_output=True,
                         check=False)
    assert res.returncode == 0, res.stderr or res.stdout
    return res.stdout.strip()


def commit(repo: Path, name: str, text: str) -> str:
    (repo / name).write_text(text, encoding="utf-8")
    git(repo, "add", name)
    git(repo, "commit", "-m", f"add {name}")
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture()
def repos(tmp_path: Path, monkeypatch):
    local_base = tmp_path / "local" / "proj"
    remote_base = tmp_path / "remote"
    cache = tmp_path / "cache"
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(tmp_path / "state"))
    local = local_base / "demo"
    remote = remote_base / "demo"
    local.mkdir(parents=True)
    remote_base.mkdir(parents=True)

    subprocess.run(["git", "init", "-b", "main"], cwd=str(local), check=True,
                   capture_output=True, text=True)
    for repo in [local]:
        git(repo, "config", "user.email", "remrun-test@example.invalid")
        git(repo, "config", "user.name", "remrun Test")
    commit(local, "base.txt", "base")
    subprocess.run(["git", "clone", str(local), str(remote)], check=True,
                   capture_output=True, text=True)
    git(remote, "config", "user.email", "remrun-test@example.invalid")
    git(remote, "config", "user.name", "remrun Test")

    device = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim",
        "os": "posix",
        "project_root": posix(remote_base),
        "cache_root": posix(cache),
    })
    cfg = RemrunConfig(
        repo_root=tmp_path / "remrun",
        defaults={},
        devices={"LOCAL_SIM": device},
        project_roots={"default": posix(local_base), "windows": posix(local_base),
                       "macos": posix(local_base)},
    )
    monkeypatch.chdir(local)
    return cfg, local, remote


def sync(cfg: RemrunConfig, **kwargs) -> int:
    return run_git_sync(cfg, device_name="LOCAL_SIM", reporter=Reporter(), **kwargs)


def test_git_sync_registers_opt_in_controller_operation_for_its_full_lifecycle(
    repos, monkeypatch
):
    cfg, _local, _remote = repos
    monkeypatch.setenv("REMRUN_FLEET_JOBS_OBSERVE", "1")
    state_root = Path(os.environ["REMRUN_STATE_ROOT"])
    seen = []

    def fake_result(_config, **_kwargs):
        payload = observer._query(state_root, 0.05)
        seen.extend(payload["jobs"])
        return type("Result", (), {"exit_code": EXIT_OK})()

    monkeypatch.setattr(gitsync_module, "run_git_sync_result", fake_result)

    assert run_git_sync(cfg, device_name="LOCAL_SIM", direction="push", reporter=Reporter()) == EXIT_OK
    assert len(seen) == 1
    job = seen[0]
    assert job["state"] == "RUNNING"
    assert job["project"] == "demo"
    assert job["target"] == "LOCAL_SIM"
    assert job["phase"] == "git-sync"
    assert job["command"]["label"] == "git-sync:push"
    assert job["cpu"]["current_pct_one_logical_cpu"] is None
    assert observer._query(state_root, 0.05)["jobs"] == []


def test_git_sync_can_use_broader_roots_for_a_sibling_repository(tmp_path, monkeypatch):
    local_base = tmp_path / "local"
    remote_base = tmp_path / "remote"
    local = local_base / "remrun"
    remote = remote_base / "remrun"
    local.mkdir(parents=True)
    subprocess.run(["git", "init", "-b", "main"], cwd=str(local), check=True,
                   capture_output=True, text=True)
    git(local, "config", "user.email", "remrun-test@example.invalid")
    git(local, "config", "user.name", "remrun Test")
    commit(local, "base.txt", "base")
    subprocess.run(["git", "clone", str(local), str(remote)], check=True,
                   capture_output=True, text=True)

    remote_os = "remote-test"
    device = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim",
        "os": remote_os,
        "project_root": posix(remote_base / "proj"),
        "cache_root": posix(tmp_path / "cache"),
    })
    cfg = RemrunConfig(
        repo_root=tmp_path / "tool",
        defaults={},
        devices={"LOCAL_SIM": device},
        project_roots={"default": posix(local_base / "proj")},
        git_sync={
            "project_roots": {
                current_os_key(): posix(local_base),
                remote_os: posix(remote_base),
                "default": posix(local_base),
            },
        },
    )
    monkeypatch.chdir(local)

    result = run_git_sync_result(
        cfg, device_name="LOCAL_SIM", direction="pull", dry_run=True, reporter=Reporter())

    assert result.local_project == str(local)
    assert Path(result.remote_project) == remote


@pytest.fixture()
def bootstrap_repos(tmp_path: Path, monkeypatch):
    """A peer repo with authoritative history + a repo-less local working tree.

    Mirrors the production case: a Syncthing-delivered project arrives on a new
    device with a full working tree and no `.git`, while the peer holds history.
    """
    local_base = tmp_path / "local" / "proj"
    remote_base = tmp_path / "remote"
    cache = tmp_path / "cache"
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(tmp_path / "state"))
    local = local_base / "demo"
    remote = remote_base / "demo"
    local.mkdir(parents=True)
    remote.mkdir(parents=True)

    # Authoritative peer repo with real history (two commits, a nested path).
    subprocess.run(["git", "init", "-b", "main"], cwd=str(remote), check=True,
                   capture_output=True, text=True)
    git(remote, "config", "user.email", "remrun-test@example.invalid")
    git(remote, "config", "user.name", "remrun Test")
    commit(remote, "base.txt", "base contents")
    (remote / "pkg").mkdir()
    peer_head = commit(remote, "pkg/mod.txt", "module contents")

    # Repo-less local working tree: copy the peer's committed files (Syncthing),
    # then introduce a deliberate tracked modification and an untracked file.
    (local / "base.txt").write_text("base contents", encoding="utf-8")
    (local / "pkg").mkdir()
    (local / "pkg" / "mod.txt").write_text("module contents", encoding="utf-8")
    (local / "base.txt").write_text("LOCAL UNCOMMITTED EDIT", encoding="utf-8")  # modified vs HEAD
    (local / "scratch.txt").write_text("local-only work", encoding="utf-8")     # untracked

    device = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim",
        "os": "posix",
        "project_root": posix(remote_base),
        "cache_root": posix(cache),
    })
    cfg = RemrunConfig(
        repo_root=tmp_path / "remrun",
        defaults={},
        devices={"LOCAL_SIM": device},
        project_roots={"default": posix(local_base), "windows": posix(local_base),
                       "macos": posix(local_base)},
    )
    monkeypatch.chdir(local)
    return cfg, local, remote, peer_head


def test_bootstrap_seeds_repoless_project_and_leaves_worktree_untouched(bootstrap_repos):
    cfg, local, remote, peer_head = bootstrap_repos
    assert not (local / ".git").exists()

    rc = run_git_sync(cfg, device_name="LOCAL_SIM", direction="pull", reporter=Reporter())
    assert rc == EXIT_OK

    # Repo created, HEAD points at the peer's history, full history arrived.
    assert (local / ".git").exists()
    assert git(local, "rev-parse", "HEAD") == peer_head
    assert git(local, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert git(local, "rev-list", "--count", "HEAD") == "2"

    # ABSOLUTE RULE: the working tree is byte-for-byte untouched.
    assert (local / "base.txt").read_text(encoding="utf-8") == "LOCAL UNCOMMITTED EDIT"
    assert (local / "scratch.txt").read_text(encoding="utf-8") == "local-only work"
    assert (local / "pkg" / "mod.txt").read_text(encoding="utf-8") == "module contents"

    # base.txt reads as modified vs HEAD; scratch.txt as untracked; nothing reset.
    porcelain = git(local, "status", "--porcelain", "--untracked-files=all").splitlines()
    assert any(line.endswith("base.txt") and not line.startswith("??") for line in porcelain)
    assert any(line.startswith("??") and line.endswith("scratch.txt") for line in porcelain)

    # Windows-safety + cross-platform config applied.
    assert git(local, "config", "core.autocrlf") == "false"


def test_bootstrap_result_reports_counts(bootstrap_repos):
    cfg, local, remote, peer_head = bootstrap_repos
    result = run_git_sync_result(cfg, device_name="LOCAL_SIM", direction="pull",
                                 reporter=Reporter())
    boot = result.bootstrap
    assert boot is not None
    assert boot.head == peer_head
    assert boot.branch == "main"
    assert boot.commits_fetched == 2
    assert boot.modified == 1
    assert boot.untracked == 1
    assert result.as_dict()["bootstrap"]["head"] == peer_head


def test_bootstrap_sets_hooks_path_when_githooks_present(bootstrap_repos):
    cfg, local, remote, _peer_head = bootstrap_repos
    (local / ".githooks").mkdir()

    result = run_git_sync_result(cfg, device_name="LOCAL_SIM", direction="pull",
                                 reporter=Reporter())
    assert result.bootstrap is not None
    assert result.bootstrap.hooks_path_set is True
    assert git(local, "config", "core.hooksPath") == ".githooks"


def test_bootstrap_dry_run_creates_no_repo(bootstrap_repos):
    cfg, local, remote, peer_head = bootstrap_repos
    result = run_git_sync_result(cfg, device_name="LOCAL_SIM", direction="pull",
                                 dry_run=True, reporter=Reporter())
    assert not (local / ".git").exists()
    assert result.bootstrap is not None
    assert result.bootstrap.head == peer_head


def test_bootstrap_push_only_refuses_repoless(bootstrap_repos):
    cfg, local, remote, _peer_head = bootstrap_repos
    rc = run_git_sync(cfg, device_name="LOCAL_SIM", direction="push", reporter=Reporter())
    assert rc == EXIT_INFRA
    assert not (local / ".git").exists()


def test_bootstrap_flag_rejects_existing_repo(repos):
    cfg, local, _remote = repos
    rc = run_git_sync(cfg, device_name="LOCAL_SIM", direction="pull", bootstrap=True,
                      reporter=Reporter())
    assert rc == EXIT_INFRA


def test_bootstrap_empty_peer_reports_cleanly(tmp_path, monkeypatch):
    local_base = tmp_path / "local" / "proj"
    remote_base = tmp_path / "remote"
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(tmp_path / "state"))
    local = local_base / "demo"
    remote = remote_base / "demo"
    local.mkdir(parents=True)
    remote.mkdir(parents=True)
    (local / "work.txt").write_text("local work", encoding="utf-8")
    # Peer is an unborn repo: initialized, no commits.
    subprocess.run(["git", "init", "-b", "main"], cwd=str(remote), check=True,
                   capture_output=True, text=True)

    device = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim", "os": "posix",
        "project_root": posix(remote_base), "cache_root": posix(tmp_path / "cache"),
    })
    cfg = RemrunConfig(
        repo_root=tmp_path / "remrun", defaults={}, devices={"LOCAL_SIM": device},
        project_roots={"default": posix(local_base), "windows": posix(local_base),
                       "macos": posix(local_base)},
    )
    monkeypatch.chdir(local)

    rc = run_git_sync(cfg, device_name="LOCAL_SIM", direction="pull", reporter=Reporter())
    assert rc == EXIT_INFRA
    assert not (local / ".git").exists()  # no half-initialized repo left behind


def test_branch_scoped_bootstrap_fetches_only_named_branch(bootstrap_repos):
    cfg, local, remote, _peer_head = bootstrap_repos
    git(remote, "checkout", "-b", "side")
    side_head = commit(remote, "side.txt", "side")
    git(remote, "checkout", "main")

    result = run_git_sync_result(
        cfg, device_name="LOCAL_SIM", direction="pull", branch="side",
        reporter=Reporter(),
    )

    assert result.bootstrap is not None
    assert result.bootstrap.branch == "side"
    assert result.bootstrap.head == side_head
    assert git(local, "rev-parse", "--abbrev-ref", "HEAD") == "side"
    assert subprocess.run(
        ["git", "show-ref", "--verify", "--quiet", "refs/heads/main"],
        cwd=str(local), capture_output=True, text=True,
    ).returncode != 0


def test_bootstrap_recovers_existing_unborn_repo(bootstrap_repos):
    cfg, local, _remote, peer_head = bootstrap_repos
    subprocess.run(["git", "init", "-b", "master"], cwd=str(local), check=True,
                   capture_output=True, text=True)

    rc = run_git_sync(cfg, device_name="LOCAL_SIM", direction="pull", reporter=Reporter())

    assert rc == EXIT_OK
    assert git(local, "rev-parse", "HEAD") == peer_head
    assert git(local, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert git(local, "rev-list", "--count", "HEAD") == "2"
    assert (local / "base.txt").read_text(encoding="utf-8") == "LOCAL UNCOMMITTED EDIT"


def test_bootstrap_missing_bundle_cannot_report_success(bootstrap_repos, monkeypatch):
    cfg, local, _remote, _peer_head = bootstrap_repos

    def omit_bundle(_self, _remote_path, _local_path):
        return None

    monkeypatch.setattr(LocalSimTransport, "pull_file", omit_bundle)

    rc = run_git_sync(cfg, device_name="LOCAL_SIM", direction="pull", reporter=Reporter())

    assert rc != EXIT_OK
    assert not (local / ".git").exists()


def test_interrupted_bootstrap_removes_git_dir_it_created(bootstrap_repos, monkeypatch):
    cfg, local, _remote, _peer_head = bootstrap_repos

    def interrupt_pull(_self, _remote_path, _local_path):
        raise KeyboardInterrupt

    monkeypatch.setattr(LocalSimTransport, "pull_file", interrupt_pull)

    with pytest.raises(KeyboardInterrupt):
        gitsync_module.run_git_sync_result(
            cfg, device_name="LOCAL_SIM", direction="pull", reporter=Reporter())

    assert not (local / ".git").exists()


def test_pull_fast_forwards_current_branch(repos):
    cfg, local, remote = repos
    remote_head = commit(remote, "remote.txt", "from remote")

    assert sync(cfg, direction="pull") == EXIT_OK

    assert git(local, "rev-parse", "HEAD") == remote_head
    assert (local / "remote.txt").read_text(encoding="utf-8") == "from remote"
    assert git(local, "rev-parse", "refs/remotes/LOCAL_SIM/main") == remote_head


def test_push_fast_forwards_remote_current_branch(repos):
    cfg, local, remote = repos
    local_head = commit(local, "local.txt", "from local")

    assert sync(cfg, direction="push") == EXIT_OK

    assert git(remote, "rev-parse", "HEAD") == local_head
    assert (remote / "local.txt").read_text(encoding="utf-8") == "from local"


def test_both_pulls_then_pushes_when_peer_is_ahead(repos):
    cfg, local, remote = repos
    remote_head = commit(remote, "peer.txt", "peer ahead")

    assert sync(cfg, direction="both") == EXIT_OK

    assert git(local, "rev-parse", "HEAD") == remote_head
    assert git(remote, "rev-parse", "HEAD") == remote_head


def test_divergence_exits_two_and_clobbers_nothing(repos):
    cfg, local, remote = repos
    local_head = commit(local, "local.txt", "local")
    remote_head = commit(remote, "remote.txt", "remote")

    assert sync(cfg, direction="pull") == EXIT_DIVERGED

    assert git(local, "rev-parse", "HEAD") == local_head
    assert git(remote, "rev-parse", "HEAD") == remote_head
    assert git(local, "rev-parse", "refs/remotes/LOCAL_SIM/main") == remote_head
    assert not (local / "remote.txt").exists()


def test_dirty_current_branch_fetches_but_does_not_advance(repos):
    cfg, local, remote = repos
    old_local = git(local, "rev-parse", "HEAD")
    remote_head = commit(remote, "remote.txt", "remote")
    (local / "base.txt").write_text("dirty local edit", encoding="utf-8")

    assert sync(cfg, direction="pull") == EXIT_OK

    assert git(local, "rev-parse", "HEAD") == old_local
    assert git(local, "rev-parse", "refs/remotes/LOCAL_SIM/main") == remote_head
    assert not (local / "remote.txt").exists()


def test_synced_controller_advances_dirty_pull_without_touching_worktree(repos):
    cfg, local, remote = repos
    cfg = replace(cfg, git_sync={"advance_dirty_worktree": True})
    remote_head = commit(remote, "remote.txt", "remote")
    (local / "base.txt").write_text("dirty local edit", encoding="utf-8")
    before = {path.relative_to(local).as_posix(): path.read_bytes()
              for path in local.rglob("*") if path.is_file() and ".git" not in path.parts}

    result = run_git_sync_result(
        cfg, device_name="LOCAL_SIM", direction="pull", reporter=Reporter())
    assert result.exit_code == EXIT_OK

    after = {path.relative_to(local).as_posix(): path.read_bytes()
             for path in local.rglob("*") if path.is_file() and ".git" not in path.parts}
    assert git(local, "rev-parse", "HEAD") == remote_head
    assert after == before
    assert (local / "base.txt").read_text(encoding="utf-8") == "dirty local edit"
    assert not (local / "remote.txt").exists()
    assert "local worktree remains dirty" in result.pulled[0].detail
    assert "do not treat it as a clean checkout" in result.pulled[0].detail


def test_untracked_litter_does_not_block_fast_forward(repos):
    cfg, local, remote = repos
    local_head = commit(local, "local.txt", "from local")
    (remote / ".DS_Store").write_text("platform litter", encoding="utf-8")

    assert sync(cfg, direction="push") == EXIT_OK

    assert git(remote, "rev-parse", "HEAD") == local_head
    assert (remote / ".DS_Store").read_text(encoding="utf-8") == "platform litter"


def test_push_dirty_remote_is_skipped_by_default(repos):
    cfg, local, remote = repos
    old_remote = git(remote, "rev-parse", "HEAD")
    local_head = commit(local, "local.txt", "from local")
    (remote / "base.txt").write_text("remote uncommitted edit", encoding="utf-8")

    assert sync(cfg, direction="push") == EXIT_OK

    assert git(remote, "rev-parse", "HEAD") == old_remote
    assert git(remote, "cat-file", "-t", local_head) == "commit"  # fetch completed
    assert (remote / "base.txt").read_text(encoding="utf-8") == "remote uncommitted edit"


def test_history_hub_advances_dirty_remote_without_touching_worktree(repos):
    cfg, local, remote = repos
    cfg = replace(cfg, git_sync={"advance_dirty_worktree": True})
    local_head = commit(local, "local.txt", "from local")
    (remote / "base.txt").write_text("remote uncommitted edit", encoding="utf-8")
    before = {path.relative_to(remote).as_posix(): path.read_bytes()
              for path in remote.rglob("*") if path.is_file() and ".git" not in path.parts}

    result = run_git_sync_result(
        cfg, device_name="LOCAL_SIM", direction="push", reporter=Reporter())
    assert result.exit_code == EXIT_OK

    after = {path.relative_to(remote).as_posix(): path.read_bytes()
             for path in remote.rglob("*") if path.is_file() and ".git" not in path.parts}
    assert git(remote, "rev-parse", "HEAD") == local_head
    assert after == before  # reset --mixed changed HEAD/index only
    assert (remote / "base.txt").read_text(encoding="utf-8") == "remote uncommitted edit"
    assert not (remote / "local.txt").exists()  # Syncthing may deliver it later
    assert "remote worktree remains dirty" in result.pushed[0].detail
    assert "do not treat it as a clean checkout" in result.pushed[0].detail


def test_dry_run_does_not_fetch_or_fast_forward(repos):
    cfg, local, remote = repos
    old_local = git(local, "rev-parse", "HEAD")
    remote_head = commit(remote, "remote.txt", "remote")

    assert sync(cfg, direction="pull", dry_run=True) == EXIT_OK

    assert git(local, "rev-parse", "HEAD") == old_local
    assert git(remote, "rev-parse", "HEAD") == remote_head
    assert subprocess.run(["git", "rev-parse", "--verify", "refs/remotes/LOCAL_SIM/main"],
                          cwd=str(local), capture_output=True, text=True).returncode != 0


def test_status_reports_peer_state_without_mutating_refs(repos):
    cfg, local, remote = repos
    remote_head = commit(remote, "remote.txt", "remote")

    status = git_sync_status_result(cfg, device_name="LOCAL_SIM", reporter=Reporter())

    assert status.exit_code == EXIT_OK
    assert status.branches[0].branch == "main"
    assert status.branches[0].state == "would_fast_forward"
    assert status.branches[0].new == remote_head
    assert status.local_history_present is True
    assert status.local_dirty_summary.as_dict() == {
        "tracked": 0, "content": 0, "mode_only": 0, "untracked": 0,
    }
    assert subprocess.run(["git", "rev-parse", "--verify", "refs/remotes/LOCAL_SIM/main"],
                          cwd=str(local), capture_output=True, text=True).returncode != 0


def test_status_reports_repoless_peer_without_creating_git_metadata(bootstrap_repos):
    cfg, local, _remote, peer_head = bootstrap_repos
    assert not (local / ".git").exists()

    status = git_sync_status_result(cfg, device_name="LOCAL_SIM", reporter=Reporter())

    assert status.exit_code == EXIT_OK
    assert status.local_history_present is False
    assert status.local_dirty is False
    assert status.hook_installed is False
    assert [(a.branch, a.state, a.new) for a in status.branches] == [
        ("main", "bootstrap_available", peer_head),
    ]
    assert not (local / ".git").exists()


def test_dirty_summary_distinguishes_content_mode_and_untracked():
    summary = _parse_dirty_summary(
        " M content.py\n M mode.py\n?? scratch.txt\n",
        "2\t1\tcontent.py\n0\t0\tmode.py\n",
        " mode change 100644 => 100755 mode.py\n",
    )
    assert summary.as_dict() == {
        "tracked": 2, "content": 1, "mode_only": 1, "untracked": 1,
    }


def test_status_reports_divergence_exit_two(repos):
    cfg, _local, remote = repos
    commit(Path.cwd(), "local.txt", "local")
    commit(remote, "remote.txt", "remote")

    assert run_git_sync_status(cfg, device_name="LOCAL_SIM", reporter=Reporter()) == EXIT_DIVERGED


def test_branch_option_limits_fast_forward(repos):
    cfg, local, remote = repos
    git(local, "checkout", "-b", "side")
    commit(local, "side.txt", "side base")
    git(local, "checkout", "main")
    subprocess.run(["git", "fetch", str(local), "side:side"], cwd=str(remote), check=True,
                   capture_output=True, text=True)
    git(remote, "checkout", "side")
    side_head = commit(remote, "side-remote.txt", "side remote")
    git(remote, "checkout", "main")
    main_head = commit(remote, "main-remote.txt", "main remote")

    assert sync(cfg, direction="pull", branch="side") == EXIT_OK

    assert git(local, "rev-parse", "side") == side_head
    assert git(local, "rev-parse", "main") != main_head
    assert subprocess.run(
        ["git", "rev-parse", "--verify", "refs/remotes/LOCAL_SIM/main"],
        cwd=str(local), capture_output=True, text=True,
    ).returncode != 0


def test_branch_option_limits_push_bundle_and_remote_fetch(repos):
    cfg, local, remote = repos
    git(local, "checkout", "-b", "side")
    commit(local, "side-base.txt", "side base")
    git(local, "checkout", "main")
    subprocess.run(
        ["git", "fetch", str(local), "side:side"], cwd=str(remote), check=True,
        capture_output=True, text=True,
    )
    git(local, "checkout", "side")
    side_head = commit(local, "side.txt", "side")
    git(local, "checkout", "main")
    main_head = commit(local, "main.txt", "main")

    assert sync(cfg, direction="push", branch="side") == EXIT_OK

    assert git(remote, "rev-parse", "side") == side_head
    assert git(remote, "rev-parse", "main") != main_head
    local_ns = _ref_namespace(socket.gethostname() or "LOCAL")
    assert subprocess.run(
        ["git", "rev-parse", "--verify", f"refs/remotes/{local_ns}/main"],
        cwd=str(remote), capture_output=True, text=True,
    ).returncode != 0


def test_status_branch_scope_uses_single_branch_bundle_and_fetch(repos, monkeypatch):
    cfg, _local, _remote = repos
    remote_calls = []
    local_calls = []
    real_remote = gitsync_module._remote_git_ok
    real_local = gitsync_module._local_git_ok

    def record_remote(transport, cwd, args):
        remote_calls.append(list(args))
        return real_remote(transport, cwd, args)

    def record_local(cwd, args):
        local_calls.append(list(args))
        return real_local(cwd, args)

    monkeypatch.setattr(gitsync_module, "_remote_git_ok", record_remote)
    monkeypatch.setattr(gitsync_module, "_local_git_ok", record_local)

    status = git_sync_status_result(
        cfg, device_name="LOCAL_SIM", branch="main", reporter=Reporter(),
    )

    assert status.exit_code == EXIT_OK
    bundle = next(args for args in remote_calls if args[:2] == ["bundle", "create"])
    assert "refs/heads/main" in bundle
    assert "--branches" not in bundle
    fetch = next(args for args in local_calls if args and args[0] == "fetch")
    assert "+refs/heads/main:refs/remotes/LOCAL_SIM/main" in fetch
    assert not any("refs/heads/*" in arg for arg in fetch)


def test_install_hook_uses_project_config_peers(repos):
    cfg, local, _remote = repos
    cfgdir = local / "do" / "remrun"
    cfgdir.mkdir(parents=True)
    (cfgdir / "remrun.toml").write_text('[git_sync]\npeers = ["LOCAL_SIM"]\n',
                                        encoding="utf-8")

    assert install_git_sync_hook(cfg, reporter=Reporter()) == EXIT_OK

    hook = local / ".git" / "hooks" / "post-commit"
    text = hook.read_text(encoding="utf-8")
    assert "remrun git-sync hook" in text
    assert "git-sync LOCAL_SIM --push --quiet" in text
    assert "gitsync-hook/demo.log" in text.replace("\\", "/")
    assert "tail -c 65536" in text


def test_install_hook_uses_global_git_sync_peers(repos):
    cfg, local, _remote = repos
    cfg = replace(cfg, git_sync={"peers": ["LOCAL_SIM"]})

    assert install_git_sync_hook(cfg, reporter=Reporter()) == EXIT_OK

    hook = local / ".git" / "hooks" / "post-commit"
    assert "git-sync LOCAL_SIM --push --quiet" in hook.read_text(encoding="utf-8")


def test_install_hook_backs_up_and_uninstall_restores_existing_hook(repos):
    cfg, local, _remote = repos
    hook = local / ".git" / "hooks" / "post-commit"
    hook.write_text("#!/bin/sh\necho prior\n", encoding="utf-8")

    assert install_git_sync_hook(cfg, device_name="LOCAL_SIM", reporter=Reporter()) == EXIT_OK

    backup = local / ".git" / "hooks" / "post-commit.remrun-backup"
    assert backup.read_text(encoding="utf-8") == "#!/bin/sh\necho prior\n"
    assert "git-sync LOCAL_SIM --push --quiet" in hook.read_text(encoding="utf-8")

    assert uninstall_git_sync_hook(cfg, reporter=Reporter()) == EXIT_OK

    assert hook.read_text(encoding="utf-8") == "#!/bin/sh\necho prior\n"
    assert not backup.exists()


def test_uninstall_hook_removes_managed_hook_without_backup(repos):
    cfg, local, _remote = repos

    assert install_git_sync_hook(cfg, device_name="LOCAL_SIM", reporter=Reporter()) == EXIT_OK
    assert uninstall_git_sync_hook(cfg, reporter=Reporter()) == EXIT_OK

    assert not (local / ".git" / "hooks" / "post-commit").exists()


@pytest.mark.parametrize("local_os_key", ["macos", "windows"])
def test_repoless_broader_roots_preserve_leaf_for_status_and_pull(
    tmp_path, monkeypatch, local_os_key
):
    local_sync_root = tmp_path / "local"
    local_run_root = local_sync_root / "proj"
    local = local_run_root / "hearken"
    remote_sync_root = tmp_path / "remote"
    remote_run_root = remote_sync_root / "proj"
    remote = remote_run_root / "hearken"
    local.mkdir(parents=True)
    remote.mkdir(parents=True)
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(tmp_path / "state"))
    monkeypatch.setattr("remrun.project.current_os_key", lambda: local_os_key)

    subprocess.run(
        ["git", "init", "-b", "main"], cwd=str(remote), check=True,
        capture_output=True, text=True,
    )
    git(remote, "config", "user.email", "remrun-test@example.invalid")
    git(remote, "config", "user.name", "remrun Test")
    peer_head = commit(remote, "base.txt", "peer bytes")

    (local / "base.txt").write_text("local edited bytes", encoding="utf-8")
    (local / "scratch.txt").write_text("local-only", encoding="utf-8")
    before = {
        path.relative_to(local).as_posix(): path.read_bytes()
        for path in local.rglob("*") if path.is_file()
    }

    remote_os = "peer-test"
    device = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim",
        "os": remote_os,
        "project_root": posix(remote_run_root),
        "cache_root": posix(tmp_path / "cache"),
    })
    cfg = RemrunConfig(
        repo_root=tmp_path / "tool",
        defaults={},
        devices={"LOCAL_SIM": device},
        project_roots={
            local_os_key: posix(local_run_root),
            "default": posix(local_run_root),
        },
        git_sync={
            "project_roots": {
                local_os_key: posix(local_sync_root),
                remote_os: posix(remote_sync_root),
                "default": posix(local_sync_root),
            },
        },
    )
    monkeypatch.chdir(local)

    status = git_sync_status_result(cfg, device_name="LOCAL_SIM", reporter=Reporter())

    assert status.local_project == str(local)
    assert Path(status.remote_project) == remote
    assert status.local_history_present is False
    assert not (local / ".git").exists()
    assert {
        path.relative_to(local).as_posix(): path.read_bytes()
        for path in local.rglob("*") if path.is_file()
    } == before

    result = run_git_sync_result(
        cfg, device_name="LOCAL_SIM", direction="pull", reporter=Reporter()
    )

    assert result.local_project == str(local)
    assert Path(result.remote_project) == remote
    assert result.bootstrap is not None
    assert result.bootstrap.head == peer_head
    after = {
        path.relative_to(local).as_posix(): path.read_bytes()
        for path in local.rglob("*")
        if path.is_file() and ".git" not in path.parts
    }
    assert after == before


class _RemoteRepoProbeTransport:
    def __init__(self, *, old_result: ExecResult, new_result: ExecResult) -> None:
        self.old_result = old_result
        self.new_result = new_result
        self.calls: list[tuple[list[str], str]] = []

    def exec(self, command, cwd, **_kwargs):
        self.calls.append((list(command), cwd))
        if command == ["git", "rev-parse", "--is-inside-work-tree"]:
            return self.old_result
        return self.new_result


def test_remote_repo_probe_rejects_real_mapped_child_of_parent_repo(tmp_path):
    parent = tmp_path / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    subprocess.run(
        ["git", "init", "-b", "main"], cwd=str(parent), check=True,
        capture_output=True, text=True,
    )
    device = Device.from_mapping("LOCAL_SIM", {
        "kind": "local-sim",
        "os": "posix",
        "project_root": posix(tmp_path),
        "cache_root": posix(tmp_path / "cache"),
    })

    with pytest.raises(GitSyncError, match="inside a parent repository"):
        _ensure_remote_git_repo(LocalSimTransport(device), str(child))


def test_remote_repo_probe_accepts_login_shell_chatter_before_git_output():
    remote_root = "/srv/projects/example"
    transport = _RemoteRepoProbeTransport(
        old_result=ExecResult(0, "profile banner\ntrue\n", ""),
        new_result=ExecResult(
            0,
            "profile banner\n\n/srv/projects/example\n",
            "",
        ),
    )

    _ensure_remote_git_repo(transport, remote_root)

    assert transport.calls == [(
        ["git", "rev-parse", "--show-prefix", "--show-toplevel"],
        remote_root,
    )]


@pytest.mark.parametrize(
    ("result", "reason"),
    [
        (ExecResult(127, "", "git: command not found\n"), "git command failed"),
        (ExecResult(0, "profile output only\n", ""), "git returned unexpected output"),
    ],
)
def test_remote_repo_probe_diagnostics_classify_execution_and_output_failures(
    result, reason
):
    remote_root = "/srv/projects/example"
    transport = _RemoteRepoProbeTransport(old_result=result, new_result=result)

    with pytest.raises(GitSyncError) as exc_info:
        _ensure_remote_git_repo(transport, remote_root)

    message = str(exc_info.value)
    assert f"reason={reason}" in message
    assert f"stdout_raw={json.dumps(result.stdout)}" in message
    assert f"stderr_raw={json.dumps(result.stderr)}" in message


def test_remote_repo_probe_rejects_parent_repo_and_retains_raw_diagnostics():
    remote_root = "/srv/projects/example"
    argv = ["git", "rev-parse", "--show-prefix", "--show-toplevel"]
    stdout = "example/\n/srv/projects\n"
    stderr = "remote warning\n"
    transport = _RemoteRepoProbeTransport(
        old_result=ExecResult(0, "true\n", ""),
        new_result=ExecResult(0, stdout, stderr),
    )

    with pytest.raises(GitSyncError) as exc_info:
        _ensure_remote_git_repo(transport, remote_root)

    message = str(exc_info.value)
    assert "mapped cwd is inside a parent repository" in message
    assert f"cwd={json.dumps(remote_root)}" in message
    assert f"argv={json.dumps(argv)}" in message
    assert "exit_code=0" in message
    assert f"stdout_raw={json.dumps(stdout)}" in message
    assert f"stderr_raw={json.dumps(stderr)}" in message
