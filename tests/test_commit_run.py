"""`--transfer commit`: a clean HEAD runs in a private tree; only declared outputs return.

Every run goes through ``main([...])``. Non-durable runs use the LocalSim target, whose
"remote" paths are local directories; durable runs fake only the SSH transport. The
projects are real git repositories.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import remrun.cli as cli
from remrun.cli import EXIT_CONFLICT, EXIT_INFRA, EXIT_INTERNAL, EXIT_OK, main
from remrun.models import Device
from remrun.profile import command_key, load_profiles
from remrun.state import write_unknown_completion_hazard
from remrun.transport import (
    ExecResult,
    LocalSimTransport,
    SSHPosixTransport,
    SSHPowerShellTransport,
    TransportError,
)
from test_durable_run import _ConcurrentDurableTransport


def _posix(path: Path) -> str:
    return str(path).replace("\\", "/")


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
        cwd=repo, check=True, capture_output=True, text=True, timeout=60,
    ).stdout.strip()


@pytest.fixture()
def commit_env(tmp_path: Path, monkeypatch):
    remrun_root = tmp_path / "remrun"
    (remrun_root / "config").mkdir(parents=True)
    local_base = tmp_path / "local"
    remote_base = tmp_path / "remote"
    target_state = tmp_path / "target-state"
    state = tmp_path / "state"
    for path in (local_base, remote_base):
        path.mkdir(parents=True)
    (remrun_root / "config" / "defaults.toml").write_text(
        '[transfer]\nmode = "safe"\n'
        'global_exclude = ["node_modules/**", ".git/**", "__pycache__/**"]\n'
        'hash_small_files_below_mb = 8\n'
        "work_min_free_fraction = 0.0\n"
        "[telemetry]\nenabled = false\n",
        encoding="utf-8",
    )
    common = (
        f'project_root = "{_posix(remote_base)}"\n'
        f'state_root = "{_posix(target_state)}"\n'
        f'cache_root = "{_posix(tmp_path / "cache")}"\n'
    )
    (remrun_root / "config" / "devices.toml").write_text(
        "[project_roots]\n"
        f'default = "{_posix(local_base)}"\nmacos = "{_posix(local_base)}"\n'
        f'windows = "{_posix(local_base)}"\n\n'
        '[devices.LOCAL_SIM]\nenabled = true\nrole = "simulation"\nkind = "local-sim"\n'
        'os = "posix"\n' + common
        + '[devices.TEST]\nenabled = true\nrole = "runner"\nkind = "ssh-posix"\n'
        'os = "posix"\naddress_candidates = ["test.invalid"]\n' + common,
        encoding="utf-8",
    )
    monkeypatch.setenv("REMRUN_ROOT", str(remrun_root))
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(state))
    project = local_base / "proj1"
    (project / "src").mkdir(parents=True)
    (project / "out").mkdir()
    (project / "src" / "a.py").write_text("A = 1\n", encoding="utf-8")
    (project / "out" / "keep.txt").write_text("tracked output\n", encoding="utf-8")
    _git(project, "init", "-q")
    _git(project, "add", ".")
    _git(project, "commit", "-qm", "base")
    monkeypatch.chdir(project)
    return {
        "project": project, "remote_project": remote_base / "proj1", "state": state,
        "target_state": target_state, "defaults": remrun_root / "config" / "defaults.toml",
    }


def _py(code: str) -> list[str]:
    return [sys.executable, "-c", code]


def _write(path: str, text: str) -> str:
    return f"import pathlib;p=pathlib.Path({path!r});p.parent.mkdir(parents=True,exist_ok=True);p.write_text({text!r})"


def _runs(state: Path) -> set[str]:
    runs = state / "runs"
    return {p.name for p in runs.iterdir() if p.is_dir()} if runs.exists() else set()


def _summary(state: Path, run_id: str) -> dict:
    return json.loads((state / "runs" / run_id / "summary.json").read_text(encoding="utf-8"))


def _launch(state: Path, argv: list[str]) -> tuple[int, str | None]:
    before = _runs(state)
    code = main(argv)
    new = _runs(state) - before
    assert len(new) <= 1
    return code, next(iter(new), None)


def _work_dirs(env: dict) -> set[str]:
    work = env["target_state"] / "work"
    return {p.name for p in work.iterdir() if p.name != "git"} if work.exists() else set()


def _registry(env: dict) -> dict[str, dict]:
    root = env["state"] / "commit-work"
    return {
        p.stem: json.loads(p.read_text(encoding="utf-8")) for p in root.glob("*/*.json")
    } if root.exists() else {}


def _lease_fences(env: dict) -> dict[str, list[str]]:
    return {
        data["run_id"]: data.get("outputs")
        for data in (
            json.loads(p.read_text(encoding="utf-8"))
            for p in (env["state"] / "hazards" / "project").glob("*/runs/*.json")
        )
    }


def _events(capsys) -> list[dict]:
    lines = capsys.readouterr().err.splitlines()
    return [json.loads(line) for line in lines if line.startswith("{")]


# --- the run itself ---------------------------------------------------------------


def test_clean_commit_run_returns_only_outputs_and_removes_its_tree(commit_env, capsys):
    """The command sees the commit, writes only its declared output, and that output is
    the only thing that reaches the project. The target's synced checkout is never
    created, no project lock or baseline appears, and a clean pullback removes the tree.
    """
    env = commit_env
    head = _git(env["project"], "rev-parse", "HEAD")
    code = (
        "import pathlib; a=pathlib.Path('src/a.py').read_text(); "
        "pathlib.Path('out/result.txt').write_text('saw '+a)"
    )
    status, run_id = _launch(env["state"], [
        "run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--", *_py(code),
    ])
    assert status == EXIT_OK
    assert (env["project"] / "out" / "result.txt").read_text() == "saw A = 1\n"
    assert not env["remote_project"].exists()  # the synced checkout was never touched
    summary = _summary(env["state"], run_id)
    assert summary["source"] == {"commit": head, "inputs": []}
    assert summary["work"]["state"] == "removed"
    assert summary["files_pulled_post"] == 1
    assert _work_dirs(env) == set() and _registry(env) == {}
    assert not (env["state"] / "manifests").exists()  # no baseline
    assert not list((env["state"] / "locks").glob("project/*/whole.lock"))
    assert _lease_fences(env) == {}
    learned = load_profiles(env["state"])["proj1"][command_key(_py(code))]["LOCAL_SIM"]
    assert learned["exec_s"] > 0


def test_a_fenced_tree_of_another_project_is_never_pruned(commit_env):
    """The registry is per target, shared by every project this controller runs there.
    A tree whose run (of any project) still holds its fence may be in use."""
    env = commit_env
    status, run_id = _launch(env["state"], [
        "run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
        *_py("raise SystemExit(3)"),
    ])
    assert status == 3
    (record_path,) = (env["state"] / "commit-work").glob("*/*.json")
    record = json.loads(record_path.read_text())
    record.update(created_at="2000-01-01T00:00:00Z", project_id="other", run_id="r-other")
    record_path.write_text(json.dumps(record))
    write_unknown_completion_hazard("other", "LOCAL_SIM", "r-other", per_run=True,
                                    outputs=["x/"])
    assert main(["run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
                 *_py("print(1)")]) == EXIT_OK
    assert _work_dirs(env) == {record["work_id"]}


def test_no_work_path_falls_inside_a_project_tree(commit_env, monkeypatch):
    """Work trees, bundles and the cache live under the target's state root. A state
    root inside the target's project root would put them in the synced tree, so that
    configuration is refused before anything is written."""
    env = commit_env
    paths: list[str] = []
    real_seed = cli.seed_commit_tree

    def record(**kwargs):  # noqa: ANN003
        paths.extend([kwargs["work"].path, kwargs["work"].tree, kwargs["work"].cache])
        return real_seed(**kwargs)

    monkeypatch.setattr(cli, "seed_commit_tree", record)
    assert main(["run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/x.txt", "--",
                 *_py(_write("out/x.txt", "x"))]) == EXIT_OK
    assert paths
    for path in paths:
        assert Path(path).is_relative_to(env["target_state"] / "work")
        assert not Path(path).is_relative_to(env["project"])
        assert not Path(path).is_relative_to(env["remote_project"].parent)

    devices = env["defaults"].parent / "devices.toml"
    inside = env["remote_project"].parent / "state-inside"
    devices.write_text(
        devices.read_text().replace(_posix(env["target_state"]), _posix(inside)),
        encoding="utf-8",
    )
    assert main(["run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
                 *_py("print(1)")]) == EXIT_INTERNAL
    assert not inside.exists()


def test_dirty_tracked_file_outside_outputs_is_refused_before_any_target(commit_env):
    """A commit run is the commit plus exactly the files it names. A tracked edit
    anywhere else would silently not run, so it refuses before choosing a target;
    an edited tracked output is fine, because the pullback owns it."""
    env = commit_env
    marker = env["project"].parent / "ran.txt"
    (env["project"] / "src" / "a.py").write_text("A = 2\n", encoding="utf-8")
    argv = ["run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
            *_py(_write(str(marker), "ran"))]
    status, run_id = _launch(env["state"], argv)
    assert (status, run_id) == (EXIT_CONFLICT, None)
    assert not marker.exists() and not env["target_state"].exists()

    _git(env["project"], "checkout", "--", "src/a.py")
    (env["project"] / "out" / "keep.txt").write_text("edited output\n", encoding="utf-8")
    assert main(argv) == EXIT_OK
    assert marker.read_text() == "ran"


def test_input_overlay_is_copied_hashed_and_recorded(commit_env):
    """An untracked input reaches the command only when named, and the summary records
    the exact bytes (SHA-256) that ran, so the result can be traced to its inputs."""
    env = commit_env
    data = env["project"] / "data" / "in.csv"
    data.parent.mkdir()
    data.write_bytes(b"1,2,3\n")  # exact bytes on every OS (no newline translation)
    copy = "import pathlib; pathlib.Path('out/copy.csv').write_text(pathlib.Path('data/in.csv').read_text())"
    status, run_id = _launch(env["state"], [
        "run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/copy.csv",
        "--input", "data/in.csv", "--", *_py(copy),
    ])
    assert status == EXIT_OK
    assert (env["project"] / "out" / "copy.csv").read_text() == "1,2,3\n"
    assert _summary(env["state"], run_id)["source"]["inputs"] == [{
        "path": "data/in.csv", "bytes": 6,
        "sha256": hashlib.sha256(b"1,2,3\n").hexdigest(),
    }]
    # Without --input the untracked file is not part of the run.
    status, _ = _launch(env["state"], [
        "run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/copy.csv", "--",
        *_py(copy),
    ])
    assert status == 1


def test_unexpected_write_keeps_the_tree_exits_2_and_still_returns_outputs(
    commit_env, capsys
):
    """A write outside the outputs that no exclude covers may be work the caller meant
    to keep. It is never pulled into the project and never deleted: the tree stays for
    inspection and the run exits 2, while the declared output still comes back."""
    env = commit_env
    code = _write("out/r.txt", "result") + ";" + _write("src/new.py", "surprise")
    status, run_id = _launch(env["state"], [
        "run", "LOCAL_SIM", "--transfer", "commit",
        "--output", "out/", "--", *_py(code),
    ])
    assert status == EXIT_CONFLICT
    assert (env["project"] / "out" / "r.txt").read_text() == "result"
    assert not (env["project"] / "src" / "new.py").exists()
    work = _summary(env["state"], run_id)["work"]
    assert work["state"] == "retained" and work["unexpected_writes"] == 1
    assert work["unexpected_paths"] == ["src/new.py"]
    assert (Path(work["tree"]) / "src" / "new.py").read_text() == "surprise"
    (record,) = _registry(env).values()
    assert record["state"] == "retained" and "unexpected" in record["reason"]
    assert _lease_fences(env) == {}  # the lease ends with the pullback


def test_scratch_under_project_excludes_is_discarded(commit_env):
    env = commit_env
    cfg = env["project"] / "do" / "remrun"
    cfg.mkdir(parents=True)
    (cfg / "remrun.toml").write_text('[transfer]\nexclude = [".cache/**", "do/**"]\n')
    _git(env["project"], "add", ".")
    _git(env["project"], "commit", "-qm", "config")
    code = _write("out/r.txt", "result") + ";" + _write(".cache/tmp.bin", "scratch")
    status, run_id = _launch(env["state"], [
        "run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--", *_py(code),
    ])
    assert status == EXIT_OK
    work = _summary(env["state"], run_id)["work"]
    assert (work["state"], work["scratch_writes"], work["unexpected_writes"]) == (
        "removed", 1, 0
    )
    assert not (env["project"] / ".cache").exists()


def test_output_under_a_project_exclude_still_returns(commit_env):
    """Projects exclude their output trees from ordinary transfer; a declared output
    must come back anyway."""
    env = commit_env
    cfg = env["project"] / "do" / "remrun"
    cfg.mkdir(parents=True)
    (cfg / "remrun.toml").write_text('[transfer]\nexclude = ["results/**"]\n')
    _git(env["project"], "add", ".")
    _git(env["project"], "commit", "-qm", "config")
    assert main(["run", "LOCAL_SIM", "--transfer", "commit", "--output", "results/", "--",
                 *_py(_write("results/r.txt", "r"))]) == EXIT_OK
    assert (env["project"] / "results" / "r.txt").read_text() == "r"


def test_local_edit_during_the_run_is_saved_as_a_conflict(commit_env):
    """Invariant 6: an output edited locally while the job ran is never overwritten.
    The command's copy is saved outside the project and the run exits 2."""
    env = commit_env
    local_copy = env["project"] / "out" / "keep.txt"
    code = (
        _write("out/keep.txt", "remote result")
        + f";pathlib.Path({str(local_copy)!r}).write_text('local edit')"
    )
    status, run_id = _launch(env["state"], [
        "run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--", *_py(code),
    ])
    assert status == EXIT_CONFLICT
    assert local_copy.read_text() == "local edit"
    saved = env["state"] / "conflicts" / run_id / "remote" / "out" / "keep.txt"
    assert saved.read_text() == "remote result"
    assert _summary(env["state"], run_id)["work"]["state"] == "retained"


def test_tree_is_removed_only_after_a_clean_pullback(commit_env, monkeypatch):
    """A failed command keeps its tree; so does a pullback that cannot finish. Only a
    clean, verified pullback removes it, and a failed removal is reported and kept on
    record for a later retry."""
    env = commit_env
    status, run_id = _launch(env["state"], [
        "run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
        *_py(_write("out/r.txt", "partial") + ";raise SystemExit(3)"),
    ])
    assert status == 3
    assert (env["project"] / "out" / "r.txt").read_text() == "partial"
    assert _summary(env["state"], run_id)["work"]["state"] == "retained"
    assert len(_work_dirs(env)) == 1

    def broken_pull(self, remote_path, local_path):  # noqa: ANN001
        raise TransportError("injected pull failure")

    real_pull, real_remove = LocalSimTransport.pull_file, LocalSimTransport.remove_remote_tree
    monkeypatch.setattr(LocalSimTransport, "pull_file", broken_pull)
    status, run_id = _launch(env["state"], [
        "run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
        *_py(_write("out/r2.txt", "lost?")),
    ])
    assert status == 3  # EXIT_TRANSFER
    assert _summary(env["state"], run_id)["work"]["state"] == "retained"
    assert len(_work_dirs(env)) == 2
    monkeypatch.setattr(LocalSimTransport, "pull_file", real_pull)

    def broken_remove(self, remote_path):  # noqa: ANN001
        raise TransportError("injected removal failure")

    monkeypatch.setattr(LocalSimTransport, "remove_remote_tree", broken_remove)
    status, run_id = _launch(env["state"], [
        "run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
        *_py(_write("out/r3.txt", "ok")),
    ])
    assert status == EXIT_OK
    work = _summary(env["state"], run_id)["work"]
    assert work["state"] == "remove_failed"
    assert _registry(env)[work["id"]]["state"] == "remove_failed"
    monkeypatch.setattr(LocalSimTransport, "remove_remote_tree", real_remove)
    # The next commit launch on this target retries the failed removal at once and
    # leaves the two retained trees (younger than the failed-log retention) alone.
    assert main(["run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
                 *_py("print(1)")]) == EXIT_OK
    assert work["id"] not in _work_dirs(env)
    assert len(_work_dirs(env)) == 2


def test_retained_trees_are_pruned_after_the_failed_log_retention(commit_env):
    env = commit_env
    status, run_id = _launch(env["state"], [
        "run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
        *_py("raise SystemExit(3)"),
    ])
    assert status == 3
    (record_path,) = (env["state"] / "commit-work").glob("*/*.json")
    record = json.loads(record_path.read_text())
    record["created_at"] = "2000-01-01T00:00:00Z"
    record_path.write_text(json.dumps(record))
    assert main(["run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
                 *_py("print(1)")]) == EXIT_OK
    assert _work_dirs(env) == set() and _registry(env) == {}


# --- seeding --------------------------------------------------------------------------


def test_second_run_sends_an_incremental_bundle(commit_env, capsys, monkeypatch):
    """The cache keeps what earlier runs sent, so a later commit costs only its new
    objects, and a commit the cache already holds sends nothing."""
    env = commit_env
    pushed: list[int] = []
    real_push = LocalSimTransport.push_file

    def counted(self, local_path, remote_path):  # noqa: ANN001
        if str(remote_path).endswith("seed.bundle"):
            pushed.append(Path(local_path).stat().st_size)
        return real_push(self, local_path, remote_path)

    monkeypatch.setattr(LocalSimTransport, "push_file", counted)
    argv = ["run", "LOCAL_SIM", "--json", "--transfer", "commit", "--output", "out/",
            "--", *_py("print(1)")]

    def bundle_kind() -> str:
        (event,) = [e for e in _events(capsys) if e.get("event") == "work_disk"]
        return event["bundle"]

    # Incompressible, so the first (full) bundle is large and the next is not.
    (env["project"] / "src" / "big.txt").write_text(
        base64.b64encode(os.urandom(150_000)).decode(), encoding="utf-8"
    )
    _git(env["project"], "add", ".")
    _git(env["project"], "commit", "-qm", "big")
    assert main(argv) == EXIT_OK
    assert bundle_kind() == "full"
    (env["project"] / "src" / "b.py").write_text("B = 2\n", encoding="utf-8")
    _git(env["project"], "add", ".")
    _git(env["project"], "commit", "-qm", "second")
    assert main(argv) == EXIT_OK
    assert bundle_kind() == "incremental"
    assert len(pushed) == 2 and pushed[1] < pushed[0] / 10
    assert main(argv) == EXIT_OK
    assert bundle_kind() == "cached"
    assert len(pushed) == 2


def test_disk_floor_refuses_before_any_target_write(commit_env, capsys, monkeypatch):
    """Below the configured free-space floor nothing is written to the target: no work
    directory, no cache, no record. A target whose free space cannot be measured is
    refused the same way, never assumed to have room."""
    env = commit_env
    marker = env["project"].parent / "ran.txt"
    argv = ["run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
            *_py(_write(str(marker), "ran"))]
    env["defaults"].write_text(env["defaults"].read_text().replace(
        "work_min_free_fraction = 0.0", "work_min_free_fraction = 0.9999999"
    ))
    assert main(argv) == EXIT_CONFLICT
    assert "disk-floor" in capsys.readouterr().err
    assert not marker.exists()
    assert not (env["target_state"] / "work").exists() and _registry(env) == {}

    env["defaults"].write_text(env["defaults"].read_text().replace(
        "work_min_free_fraction = 0.9999999", "work_min_free_fraction = 0.0"
    ))

    def unmeasured(self, remote_path):  # noqa: ANN001
        raise TransportError("probe failed")

    monkeypatch.setattr(LocalSimTransport, "work_probe", unmeasured)
    assert main(argv) == EXIT_CONFLICT
    assert "disk-unmeasured" in capsys.readouterr().err
    assert not marker.exists() and not (env["target_state"] / "work").exists()

    monkeypatch.setattr(LocalSimTransport, "work_probe", lambda self, path: {
        "path": path, "exists": False, "free_bytes": 10**12, "total_bytes": 10**12,
        "git": None,
    })
    assert main(argv) == EXIT_CONFLICT
    assert "git-unavailable" in capsys.readouterr().err
    assert not marker.exists() and not (env["target_state"] / "work").exists()


def test_floor_is_a_required_config_value(commit_env):
    env = commit_env
    env["defaults"].write_text(
        env["defaults"].read_text().replace("work_min_free_fraction = 0.0\n", "")
    )
    assert main(["run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
                 *_py("print(1)")]) == EXIT_INTERNAL
    assert not (env["target_state"] / "work").exists()


# --- leases ------------------------------------------------------------------------------


def _detached_commit_transport(monkeypatch):
    """A durable target whose launched commands have already written their files and
    keep running until the test finishes them."""

    class _Target(_ConcurrentDurableTransport):
        def launch_durable(self, command, cwd, *, run_id, observation, **kwargs):  # noqa: ANN001, ANN003
            LocalSimTransport.exec(self, command, cwd)
            return super().launch_durable(
                command, cwd, run_id=run_id, observation=observation, **kwargs
            )

        def cancel_observed(self, observation, *, cwd, timeout=30.0):  # noqa: ANN001
            outcome = super().cancel_observed(observation, cwd=cwd, timeout=timeout)
            if outcome[0]:  # the supervisor records the stopped command's exit
                self.runs[str(self.status["run_id"])].update(self.status)
            return outcome

    transports: dict[str, _Target] = {}
    monkeypatch.setattr(
        cli, "make_transport",
        lambda device: transports.setdefault(device.name, _Target(device)),
    )
    monkeypatch.setattr(cli, "DURABLE_STATUS_RETRY_SECONDS", 0.0)
    monkeypatch.setattr(cli, "DURABLE_CANCEL_SETTLE_SECONDS", 5.0)
    return transports


def test_overlapping_outputs_are_refused_and_disjoint_runs_share_a_project(
    commit_env, monkeypatch
):
    """Two long jobs of one project may run at once when their outputs are disjoint.
    An overlap would make two jobs race to return one path, and it holds on every
    target, so it refuses before any target is touched."""
    env = commit_env
    transports = _detached_commit_transport(monkeypatch)
    first_code, first = _launch(env["state"], [
        "run", "--durable", "TEST", "--transfer", "commit", "--output", "out/", "--",
        *_py(_write("out/a.txt", "a")),
    ])
    assert first_code == EXIT_INFRA  # detached while the job runs
    assert _lease_fences(env) == {first: ["out/"]}
    seeded = _work_dirs(env)

    overlap_code, overlap = _launch(env["state"], [
        "run", "--durable", "TEST", "--transfer", "commit", "--output", "out/sub/b.txt",
        "--", *_py("print(1)"),
    ])
    assert (overlap_code, overlap) == (EXIT_CONFLICT, None)
    assert _work_dirs(env) == seeded and transports["TEST"].launch_calls == 1

    second_code, second = _launch(env["state"], [
        "run", "--durable", "TEST", "--transfer", "commit", "--output", "other/", "--",
        *_py(_write("other/b.txt", "b")),
    ])
    assert second_code == EXIT_INFRA
    assert _lease_fences(env) == {first: ["out/"], second: ["other/"]}
    assert transports["TEST"].launch_calls == 2
    assert not list((env["state"] / "locks").glob("project/*/whole.lock"))


def test_lease_taken_at_dispatch_refuses_a_run_that_raced_past_the_early_check(
    commit_env, monkeypatch, capsys
):
    """The early check is advisory; the lease is taken under the lock table's guard at
    dispatch. A run that lost that race never starts and its seeded tree is removed."""
    env = commit_env
    real_seed = cli.seed_commit_tree

    def seed_then_lose_race(**kwargs):  # noqa: ANN003
        real_seed(**kwargs)
        write_unknown_completion_hazard(
            "proj1", "LOCAL_SIM", "20260101T000000Z-LOCAL_SIM-proj1-aaaaaa",
            per_run=True, outputs=["out/x/"],
        )

    monkeypatch.setattr(cli, "seed_commit_tree", seed_then_lose_race)
    marker = env["project"].parent / "ran.txt"
    status, run_id = _launch(env["state"], [
        "run", "LOCAL_SIM", "--transfer", "commit", "--output", "out/", "--",
        *_py(_write(str(marker), "ran")),
    ])
    assert status == EXIT_CONFLICT
    assert not marker.exists()
    assert _work_dirs(env) == set() and _registry(env) == {}
    assert "output-leased" in capsys.readouterr().err


def test_an_ordinary_run_is_not_blocked_by_a_commit_lease(commit_env):
    """A commit run works in a private tree, not the target's checkout, so a reconciling
    run on the same target may proceed; same-path collisions on the controller are
    settled by the pullback conflict rule (exercised above)."""
    env = commit_env
    write_unknown_completion_hazard(
        "proj1", "LOCAL_SIM", "20260101T000000Z-LOCAL_SIM-proj1-aaaaaa",
        per_run=True, outputs=["out/"],
    )
    assert main(["run", "LOCAL_SIM", "--", *_py(_write("other.txt", "x"))]) == EXIT_OK
    assert (env["project"] / "other.txt").read_text() == "x"


# --- durable runs ------------------------------------------------------------------------


def test_durable_commit_run_resumes_finalizes_once_and_learns_its_cost(
    commit_env, monkeypatch
):
    env = commit_env
    transports = _detached_commit_transport(monkeypatch)
    command = _py(_write("out/r.txt", "durable"))
    status, run_id = _launch(env["state"], [
        "run", "--durable", "TEST", "--label", "nightly", "--transfer", "commit",
        "--output", "out/", "--", *command,
    ])
    assert status == EXIT_INFRA
    fake = transports["TEST"]
    assert fake.observations[-1].project == "proj1"
    assert not (env["project"] / "out" / "r.txt").exists()
    fake.unreachable = False
    fake.finish(run_id)

    assert main(["resume", run_id]) == EXIT_OK
    assert (env["project"] / "out" / "r.txt").read_text() == "durable"
    summary = _summary(env["state"], run_id)
    assert (summary["completion_state"], summary["work"]["state"]) == ("complete", "removed")
    assert summary["plan"]["outputs"] == ["out/"] and summary["plan"]["label"] == "nightly"
    assert _lease_fences(env) == {} and _work_dirs(env) == set()
    assert load_profiles(env["state"])["proj1"][command_key(command)]["TEST"]["exec_s"] == 12.5

    (env["project"] / "out" / "r.txt").write_text("edited after", encoding="utf-8")
    assert main(["resume", run_id]) == EXIT_OK  # already finalized: nothing pulled again
    assert (env["project"] / "out" / "r.txt").read_text() == "edited after"


def test_durable_resume_retries_a_failed_tree_removal(commit_env, monkeypatch):
    env = commit_env
    transports = _detached_commit_transport(monkeypatch)
    _, run_id = _launch(env["state"], [
        "run", "--durable", "TEST", "--transfer", "commit", "--output", "out/", "--",
        *_py(_write("out/r.txt", "durable")),
    ])
    fake = transports["TEST"]
    fake.unreachable = False
    fake.finish(run_id)
    real_remove = LocalSimTransport.remove_remote_tree

    def broken(self, remote_path):  # noqa: ANN001
        raise TransportError("injected removal failure")

    monkeypatch.setattr(LocalSimTransport, "remove_remote_tree", broken)
    assert main(["resume", run_id]) == EXIT_OK
    assert _summary(env["state"], run_id)["work"]["state"] == "remove_failed"
    assert len(_work_dirs(env)) == 1
    monkeypatch.setattr(LocalSimTransport, "remove_remote_tree", real_remove)
    assert main(["resume", run_id]) == EXIT_OK
    assert _summary(env["state"], run_id)["work"]["state"] == "removed"
    assert _work_dirs(env) == set() and _registry(env) == {}


def test_cancel_of_a_durable_commit_run_returns_outputs_and_keeps_its_tree(
    commit_env, monkeypatch
):
    """Cancel stops the exact registered job, then finalizes like resume: outputs come
    back as for any nonzero exit, the tree is retained, and the lease is released."""
    env = commit_env
    transports = _detached_commit_transport(monkeypatch)
    _, run_id = _launch(env["state"], [
        "run", "--durable", "TEST", "--transfer", "commit", "--output", "out/", "--",
        *_py(_write("out/r.txt", "half")),
    ])
    transports["TEST"].unreachable = False
    assert main(["cancel", run_id]) == 137
    assert len(transports["TEST"].cancel_calls) == 1
    assert (env["project"] / "out" / "r.txt").read_text() == "half"
    work = _summary(env["state"], run_id)["work"]
    assert work["state"] == "retained" and "137" in work["reason"]
    assert _lease_fences(env) == {}
    assert (Path(work["tree"]) / "partial.txt").exists()  # the fake's cancel output


def test_resolve_unknown_releases_a_commit_lease(commit_env, monkeypatch, capsys):
    env = commit_env
    _detached_commit_transport(monkeypatch)
    _, run_id = _launch(env["state"], [
        "run", "--durable", "TEST", "--transfer", "commit", "--output", "out/", "--",
        *_py("print(1)"),
    ])
    capsys.readouterr()
    assert main(["resolve-unknown", run_id, "--confirmed-ended"]) == EXIT_OK
    assert "output lease is released" in capsys.readouterr().err
    assert _lease_fences(env) == {}


# --- plan validation --------------------------------------------------------------------


@pytest.mark.parametrize("argv", [
    ["--transfer", "commit", "--"],                                   # no output
    ["--transfer", "commit", "--output", "out/*.csv", "--"],          # a glob
    ["--transfer", "commit", "--output", "../x", "--"],               # escapes
    ["--transfer", "commit", "--output", "__pycache__/", "--"],       # globally excluded
    ["--transfer", "commit", "--output", "out/", "--no-pullback", "--"],
    ["--transfer", "none", "--output", "out/", "--"],
    ["--transfer", "commit", "--output", "out/", "--verify", "src/a.py", "--"],
])
def test_malformed_commit_requests_are_refused(commit_env, argv):
    env = commit_env
    assert main(["run", "LOCAL_SIM", *argv, *_py("print(1)")]) == EXIT_INTERNAL
    assert not (env["target_state"] / "work").exists()


# --- target-side commands on both transports -------------------------------------------


def _ps_device() -> Device:
    return Device.from_mapping("WINBOX", {
        "kind": "ssh-powershell", "os": "windows", "address_candidates": ["winbox"],
        "project_root": "C:\\Users\\you\\projects", "state_root": "D:\\remrun\\state",
        "cache_root": "D:\\remrun\\cache", "remote_python": "python", "shell": "pwsh",
    })


def _recorder(stdout: bytes):
    calls: list[dict] = []

    def run(argv, input_bytes=None, timeout=None, on_stdout=None):  # noqa: ANN001
        calls.append({"argv": argv, "input": input_bytes})
        return subprocess.CompletedProcess(argv, 0, stdout, b"")

    return calls, run


@pytest.mark.parametrize("transport_cls,device,python", [
    (SSHPowerShellTransport, _ps_device(), "python"),
    (SSHPosixTransport, Device.from_mapping("POSIX", {
        "kind": "ssh-posix", "os": "posix", "address_candidates": ["h"],
        "project_root": "/p", "state_root": "/s", "remote_python": "python3",
    }), "python3"),
])
def test_work_probe_pipes_the_runner_and_fails_closed(transport_cls, device, python, monkeypatch):
    t = transport_cls(device)
    t._address = "h"
    answer = {"path": "x", "exists": False, "free_bytes": 5, "total_bytes": 10, "git": "/g"}
    calls, run = _recorder(json.dumps(answer).encode())
    monkeypatch.setattr(t, "_run", run)
    assert t.work_probe("D:\\remrun\\state\\work\\git\\p.git") == answer
    command = calls[-1]["argv"][-1]
    assert command.startswith(f"{python} - ")
    request = json.loads(base64.b64decode(command.split(" - ", 1)[1]))
    assert request == {"op": "work_probe", "path": "D:\\remrun\\state\\work\\git\\p.git"}
    assert calls[-1]["input"].startswith(b"#!/usr/bin/env python3")
    for bad in ({**answer, "free_bytes": None}, {**answer, "total_bytes": 0}, {"git": "/g"}):
        _calls, run = _recorder(json.dumps(bad).encode())
        monkeypatch.setattr(t, "_run", run)
        with pytest.raises(TransportError):
            t.work_probe("/s/work/git/p.git")


def test_windows_seeding_uses_native_paths_and_long_path_git(commit_env, monkeypatch):
    """Every target-side step on a Windows target: the private tree sits under the
    state root in backslash form, git clones and checks out with core.longpaths, and
    PowerShell runs each git argv as literal tokens in that directory."""
    env = commit_env
    t = SSHPowerShellTransport(_ps_device())
    t._address = "winbox"
    from remrun.config import load_config, load_retention
    from remrun.planner import make_run_plan

    config = load_config()
    plan = make_run_plan(
        cwd=env["project"], config=config, target_name="LOCAL_SIM", command=["python", "x"],
        transfer_mode="commit", outputs=["out/"],
    )
    from dataclasses import replace

    plan = replace(plan, target=t.device)
    from remrun.isolated import commit_source, seed_commit_tree, work_paths

    work = work_paths(t, plan)
    assert work.tree == f"D:\\remrun\\state\\work\\{work.work_id}\\tree"
    assert work.cache.startswith("D:\\remrun\\state\\work\\git\\") and work.cache.endswith(".git")
    execs: list[tuple[list[str], str]] = []

    def fake_exec(command, cwd, **kwargs):  # noqa: ANN001, ANN003
        execs.append((list(command), cwd))
        return ExecResult(0, "", "", None)

    monkeypatch.setattr(t, "work_probe", lambda path: {
        "path": path, "exists": False, "free_bytes": 10**12, "total_bytes": 10**12,
        "git": "C:\\Program Files\\Git\\cmd\\git.exe",
    })
    monkeypatch.setattr(t, "exec", fake_exec)
    for name in ("ensure_remote_dir", "push_file", "delete_remote"):
        monkeypatch.setattr(t, name, lambda *a, **k: None)
    monkeypatch.setattr(t, "manifest", lambda *a, **k: {})
    rdir = env["state"] / "runs" / "r"
    rdir.mkdir(parents=True)
    seed_commit_tree(
        transport=t, plan=plan, config=config, policy=load_retention(config), run_id="r",
        rdir=rdir, work=work, source=commit_source(plan),
        remote_cwd=t.remote_join(work.tree, "."), report=lambda *a, **k: None,
    )
    by_verb = {argv[1] if argv[1] != "-c" else argv[3]: (argv, cwd) for argv, cwd in execs}
    clone, clone_cwd = by_verb["clone"]
    assert clone_cwd == work.path
    assert clone[-2:] == [work.cache, work.tree]
    assert "core.longpaths=true" in clone and "core.autocrlf=false" in clone
    checkout, checkout_cwd = by_verb["checkout"]
    assert checkout_cwd == work.tree and checkout[:3] == ["git", "-c", "core.longpaths=true"]

    # The real PowerShell construction of that clone: literal tokens, native cwd.
    t = SSHPowerShellTransport(_ps_device())
    t._address = "winbox"
    calls, run = _recorder(b"")
    monkeypatch.setattr(t, "_run", run)
    t.exec(clone, cwd=clone_cwd)
    encoded = calls[-1]["argv"][-1].split("-EncodedCommand ", 1)[1].split(";", 1)[0].strip()
    script = base64.b64decode(encoded).decode("utf-16-le")
    assert f"Set-Location -LiteralPath '{work.path}'" in script
    assert "'clone' '--no-checkout' '-q' '--config' 'core.longpaths=true'" in script
    assert f"'{work.cache}' '{work.tree}'" in script
