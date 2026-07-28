"""Integration tests driving remrun.cli.main end-to-end via LOCAL_SIM."""
from __future__ import annotations

import json
import shutil
import threading
import time
from pathlib import Path

import pytest

from remrun.cli import (
    EXIT_CONFLICT, EXIT_INFRA, EXIT_INTERNAL, EXIT_OK, _best_remote_verdict, main,
)
from remrun.profile import LOCAL_DEVICE, command_key, device_profile, load_profiles
from remrun.transport import ExecResult, LocalSimTransport, TransportError


def posix(p: Path) -> str:
    return str(p).replace("\\", "/")


def set_log_cap(env: dict, max_bytes: int) -> None:
    defaults = env["remrun_root"] / "config" / "defaults.toml"
    defaults.write_text(
        defaults.read_text(encoding="utf-8")
        + f'\n[logging]\nmax_full_log_mb = {max_bytes / (1024 * 1024)!r}\n',
        encoding="utf-8",
    )


@pytest.fixture()
def env(tmp_path: Path, monkeypatch):
    remrun_root = tmp_path / "remrun"
    (remrun_root / "config").mkdir(parents=True)
    local_base = tmp_path / "local" / "proj"
    remote_base = tmp_path / "remote"
    state_root = tmp_path / "state"
    local_base.mkdir(parents=True)
    remote_base.mkdir(parents=True)

    (remrun_root / "config" / "defaults.toml").write_text(
        '[transfer]\n'
        'mode = "safe"\n'
        'global_exclude = ["node_modules/**", ".git/**"]\n'
        'hash_small_files_below_mb = 8\n'
    )
    (remrun_root / "config" / "devices.toml").write_text(
        '[project_roots]\n'
        f'default = "{posix(local_base)}"\n'
        f'macos = "{posix(local_base)}"\n'
        f'windows = "{posix(local_base)}"\n'
        '\n'
        '[devices.LOCAL_SIM]\n'
        'enabled = true\n'
        'role = "simulation"\n'
        'kind = "local-sim"\n'
        'os = "posix"\n'
        f'project_root = "{posix(remote_base)}"\n'
        f'state_root = "{posix(state_root)}"\n'
        f'cache_root = "{posix(tmp_path / "cache")}"\n'
    )

    monkeypatch.setenv("REMRUN_ROOT", str(remrun_root))
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(state_root))

    proj = local_base / "proj1"
    proj.mkdir()
    monkeypatch.chdir(proj)
    return {
        "proj": proj,
        "remote_proj": remote_base / "proj1",
        "state": state_root,
        "remrun_root": remrun_root,
    }


def test_run_happy_path_pulls_output(env, capsys):
    (env["proj"] / "input.txt").write_text("in")
    code = main(["run", "LOCAL_SIM", "--",
                 "python", "-c", "open('result.txt','w').write('ok')"])
    assert code == EXIT_OK
    # Output created remotely is pulled back to the local project path.
    assert (env["proj"] / "result.txt").read_text() == "ok"
    assert (env["remote_proj"] / "input.txt").read_text() == "in"
    assert not (env["proj"] / "do").exists()
    err = capsys.readouterr().err
    assert "preflight_progress completed=0 total=1 pulls=0 pushes=1" in err
    assert "preflight_progress completed=1 total=1 pulls=0 pushes=1" in err


def test_run_passes_through_nonzero_exit(env):
    code = main(["run", "LOCAL_SIM", "--", "python", "-c", "import sys; sys.exit(7)"])
    assert code == 7


def test_exec_transport_failure_records_unknown_completion_guidance(
    env, monkeypatch, capsys
):
    def disconnect_after_start(*_args, **_kwargs):
        raise TransportError("injected connection reset")

    monkeypatch.setattr(LocalSimTransport, "exec", disconnect_after_start)
    code = main(["run", "LOCAL_SIM", "--", "python", "-c", "print('maybe ran')"])

    assert code == EXIT_INFRA
    err = capsys.readouterr().err
    assert "completion_unknown" in err
    assert "do not retry" in err
    summaries = list((env["state"] / "runs").glob("*/summary.json"))
    assert len(summaries) == 1
    summary = json.loads(summaries[0].read_text(encoding="utf-8"))
    assert summary["phase"] == "exec"
    assert summary["completion_state"] == "unknown"
    assert "read-only process/artifact probe" in summary["guidance"]
    assert not list((env["state"] / "locks").glob("**/*.lock"))


def test_auto_resolves_target(env):
    # With only LOCAL_SIM configured, --auto resolves to it.
    code = main(["run", "--auto", "--", "python", "-c", "open('auto.txt','w').write('ok')"])
    assert code == EXIT_OK
    assert (env["proj"] / "auto.txt").read_text() == "ok"


def test_dry_run_does_not_execute(env):
    code = main(["run", "LOCAL_SIM", "--dry-run", "--",
                 "python", "-c", "open('nope.txt','w').write('x')"])
    assert code == EXIT_OK
    assert not (env["proj"] / "nope.txt").exists()
    assert not (env["remote_proj"]).exists() or not (env["remote_proj"] / "nope.txt").exists()


def test_scoped_run_pulls_declared_output(env):
    cfgdir = env["proj"] / "do" / "remrun"
    cfgdir.mkdir(parents=True)
    (cfgdir / "remrun.toml").write_text(
        '[parallel.scopes.spec_a]\npaths = ["results/spec_a/**"]\n',
        encoding="utf-8",
    )
    code = main(["run", "LOCAL_SIM", "--scope", "spec_a", "--",
                 "python", "-c",
                 "import pathlib; p=pathlib.Path('results/spec_a/out.txt'); "
                 "p.parent.mkdir(parents=True, exist_ok=True); p.write_text('ok')"])
    assert code == EXIT_OK
    assert (env["proj"] / "results" / "spec_a" / "out.txt").read_text() == "ok"


def test_scoped_run_rejects_output_outside_declared_paths(env):
    cfgdir = env["proj"] / "do" / "remrun"
    cfgdir.mkdir(parents=True)
    (cfgdir / "remrun.toml").write_text(
        '[parallel.scopes.spec_a]\npaths = ["results/spec_a/**"]\n',
        encoding="utf-8",
    )
    code = main(["run", "LOCAL_SIM", "--scope", "spec_a", "--",
                 "python", "-c",
                 "import pathlib; p=pathlib.Path('results/spec_b/out.txt'); "
                 "p.parent.mkdir(parents=True, exist_ok=True); p.write_text('escaped')"])
    assert code == EXIT_CONFLICT
    assert not (env["proj"] / "results" / "spec_b" / "out.txt").exists()
    saved = list((env["state"] / "conflicts").glob("*/remote/results/spec_b/out.txt"))
    assert saved and saved[0].read_text() == "escaped"


def test_unknown_scope_is_rejected_before_execution(env):
    code = main(["run", "LOCAL_SIM", "--scope", "missing", "--",
                 "python", "-c", "open('nope.txt','w').write('x')"])
    assert code == EXIT_INTERNAL
    assert not (env["proj"] / "nope.txt").exists()


def test_conflict_aborts_with_exit_2(env):
    # First run establishes a baseline with shared.txt on both sides.
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "LOCAL_SIM", "--", "python", "-c", "print('first')"]) == EXIT_OK

    # Diverge both sides, then a run must abort before executing the command.
    (env["proj"] / "shared.txt").write_text("local-edit")
    (env["remote_proj"] / "shared.txt").write_text("remote-edit")
    code = main(["run", "LOCAL_SIM", "--",
                 "python", "-c", "open('should_not_exist.txt','w').write('x')"])
    assert code == EXIT_CONFLICT
    assert not (env["remote_proj"] / "should_not_exist.txt").exists()
    # Conflict metadata recorded outside the project tree.
    conflicts = list((env["state"] / "conflicts").glob("*/conflicts.json"))
    assert conflicts


def test_status_and_logs(env, capsys):
    main(["run", "LOCAL_SIM", "--", "python", "-c", "print('hello-logs')"])
    capsys.readouterr()
    assert main(["status"]) == EXIT_OK
    assert main(["logs", "last"]) == EXIT_OK
    out = capsys.readouterr().out
    assert "hello-logs" in out


def test_status_json_includes_fleet_state(env, capsys):
    assert main(["status", "--json"]) == EXIT_OK
    payload = capsys.readouterr().out
    assert '"fleet_state"' in payload
    assert '"runs"' in payload


def test_status_device_filter_applies_before_limit(env, capsys):
    runs = env["state"] / "runs"
    for run_id, target in (
        ("20260724T030000Z-WINBOX-demo-3", "WINBOX"),
        ("20260724T020000Z-MACBOX-demo-2", "MACBOX"),
        ("20260724T010000Z-WINBOX-demo-1", "WINBOX"),
    ):
        run = runs / run_id
        run.mkdir(parents=True)
        (run / "summary.json").write_text(
            json.dumps({"run_id": run_id, "target": target, "exit_code": 0}),
            encoding="utf-8",
        )

    assert main(["status", "WINBOX", "--limit", "2", "--json"]) == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert [run["run_id"] for run in payload["runs"]] == [
        "20260724T030000Z-WINBOX-demo-3",
        "20260724T010000Z-WINBOX-demo-1",
    ]
    assert main(["status", "WINBOX", "--limit", "0", "--json"]) == EXIT_OK
    assert json.loads(capsys.readouterr().out)["runs"] == []


def test_bench_records_local_and_remote_rows(env, capsys):
    cmd = ["python", "-c", "print('bench')"]
    code = main(["bench", "LOCAL_SIM", "--", *cmd])
    assert code == EXIT_OK
    key = command_key(cmd)
    profs = load_profiles(env["state"])
    # Both a LOCAL baseline row and the per-target trip row land in the profile.
    assert device_profile(profs, "proj1", key, LOCAL_DEVICE) is not None
    sim = device_profile(profs, "proj1", key, "LOCAL_SIM")
    assert sim is not None and sim["trip_s"] is not None
    assert "bench_verdict" in capsys.readouterr().err


def test_bench_no_local_skips_baseline_and_recommends_remote(env, capsys):
    cmd = ["python", "-c", "print('nl')"]
    code = main(["bench", "LOCAL_SIM", "--no-local", "--", *cmd])
    assert code == EXIT_OK
    key = command_key(cmd)
    profs = load_profiles(env["state"])
    # No local leg ran → no LOCAL baseline row, but the target trip is recorded.
    assert device_profile(profs, "proj1", key, LOCAL_DEVICE) is None
    assert device_profile(profs, "proj1", key, "LOCAL_SIM") is not None
    err = capsys.readouterr().err
    assert "bench_local_skipped" in err
    assert "recommend=remote" in err and "basis=no-local" in err


def test_plan_offload_policy_fallback_without_profile(env, capsys):
    # No bench data yet → plan still emits actionable offload guidance, falling
    # back to the host's static policy (the empty [offload] table → "ask").
    code = main(["plan", "LOCAL_SIM", "--", "python", "-c", "print('p')"])
    assert code == EXIT_OK
    err = capsys.readouterr().err
    assert "offload_policy" in err and "basis=no-measurement" in err


def test_plan_is_probe_free_by_default(env, capsys, monkeypatch):
    # Probing costs a round-trip per device, so `plan` must not do it unless asked.
    from remrun import transport as transport_mod

    def boom(self):
        raise AssertionError("plan sampled load without --probe")

    monkeypatch.setattr(transport_mod.LocalSimTransport, "sample_load", boom, raising=False)
    code = main(["plan", "LOCAL_SIM", "--json", "--", "python", "-c", "print('p')"])
    assert code == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert "candidates_probed" not in payload


def test_plan_probe_reports_live_load_and_spare_capacity(env, capsys, monkeypatch):
    # --probe exposes the same `spare` figure pick_by_load ranks on, so an orchestrator
    # sees the scheduler's own number rather than re-deriving it from a raw percentage.
    from remrun import transport as transport_mod

    monkeypatch.setattr(transport_mod.LocalSimTransport, "sample_load",
                        lambda self: 25.0, raising=False)
    code = main(["plan", "LOCAL_SIM", "--probe", "--json", "--", "python", "-c", "print('p')"])
    assert code == EXIT_OK
    probed = json.loads(capsys.readouterr().out)["candidates_probed"]
    entry = next(e for e in probed if e["name"] == "LOCAL_SIM")
    assert entry["reachable"] is True
    assert entry["cpu_busy_pct"] == 25.0
    # Nothing was asked about git, so the key is absent rather than a misleading default.
    assert "git" not in entry


def test_plan_probe_reports_unknown_load_as_null_not_zero(env, capsys, monkeypatch):
    # A backend that cannot measure must yield null. Zero would read as "totally idle"
    # and attract every routing decision to the device we know least about.
    from remrun import transport as transport_mod

    monkeypatch.setattr(transport_mod.LocalSimTransport, "sample_load",
                        lambda self: None, raising=False)
    code = main(["plan", "LOCAL_SIM", "--probe", "--json", "--", "python", "-c", "print('p')"])
    assert code == EXIT_OK
    entry = json.loads(capsys.readouterr().out)["candidates_probed"][0]
    assert entry["cpu_busy_pct"] is None
    assert entry["spare_perf_core_equiv"] is None


def test_plan_check_git_reports_unknown_for_a_non_git_checkout(env, capsys, monkeypatch):
    # The live-observed case: a Syncthing-delivered working tree with no .git at all.
    # It must report `unknown`, never `same` — remrun excludes .git/**, so EXCLUDED
    # paths on such a device are unreconciled and may be stale.
    from remrun import transport as transport_mod

    monkeypatch.setattr(transport_mod.LocalSimTransport, "sample_load",
                        lambda self: 10.0, raising=False)
    code = main(["plan", "LOCAL_SIM", "--check-git", "--json", "--",
                 "python", "-c", "print('p')"])
    assert code == EXIT_OK
    entry = json.loads(capsys.readouterr().out)["candidates_probed"][0]
    assert entry["git"]["status"] == "unknown"
    assert entry["reachable"] is True          # unknown git != unreachable device


def test_doctor_reports_project_scopes_and_fleet_state(env, capsys):
    cfgdir = env["proj"] / "do" / "remrun"
    cfgdir.mkdir(parents=True)
    (cfgdir / "remrun.toml").write_text(
        '[parallel.scopes.spec_a]\npaths = ["results/spec_a/**"]\n'
        '[git_sync]\npeers = ["LOCAL_SIM"]\n',
        encoding="utf-8",
    )
    code = main(["doctor"])
    assert code == EXIT_OK
    err = capsys.readouterr().err
    assert "syncthing" in err
    assert "fleet_state" in err
    assert "project_write_scopes" in err and "spec_a" in err
    assert "git_sync_hook" in err and "LOCAL_SIM" in err


def test_postrun_conflict_is_terminal_and_preserves_baseline(env):
    # Establish a baseline with conflict.txt identical on both sides.
    (env["proj"] / "conflict.txt").write_text("orig")
    assert main(["run", "LOCAL_SIM", "--", "python", "-c", "print('A')"]) == EXIT_OK

    # A run whose command rewrites the remote copy while the local copy also
    # changes (simulated via absolute-path write) is an unresolved post-run conflict.
    lp = posix(env["proj"])
    code = main(["run", "LOCAL_SIM", "--", "python", "-c",
                 f"open('conflict.txt','w').write('REMOTE'); "
                 f"open(r'{lp}/conflict.txt','w').write('LOCAL')"])
    # Command exited 0, but remrun could not converge -> reported as a conflict.
    assert code == EXIT_CONFLICT
    assert (env["proj"] / "conflict.txt").read_text() == "LOCAL"      # local not clobbered
    saved = list((env["state"] / "conflicts").glob("*/remote/conflict.txt"))
    assert saved and saved[0].read_text() == "REMOTE"                  # remote copy saved aside

    # Baseline was NOT advanced: the next plain run sees both sides diverged from the
    # preserved baseline and aborts in preflight (proves the baseline wasn't poisoned).
    assert main(["run", "LOCAL_SIM", "--", "python", "-c",
                 "open('should_not_exist.txt','w').write('x')"]) == EXIT_CONFLICT
    assert not (env["remote_proj"] / "should_not_exist.txt").exists()


def test_best_remote_verdict_ignores_excluded_devices():
    profs = {"p": {"k": {"MACBOX": {"trip_s": 5.0}, "WINBOX": {"trip_s": 9.0}}}}
    # Only WINBOX completed this bench; MACBOX's row is stale and must be ignored.
    rec = _best_remote_verdict(profs, "p", "k", ["WINBOX"])
    assert rec["recommend"] == "remote" and rec["best_device"] == "WINBOX"
    # No target completed -> unknown, never a stale recommendation.
    assert _best_remote_verdict(profs, "p", "k", [])["recommend"] == "unknown"


def test_bench_returns_infra_when_no_remote_leg_completes(env, monkeypatch, capsys):
    import remrun.cli as climod
    # Simulate every remote leg failing the round-trip.
    monkeypatch.setattr(climod, "cmd_run", lambda a, r: climod.EXIT_INFRA)
    code = main(["bench", "LOCAL_SIM", "--no-local", "--", "python", "-c", "print(1)"])
    assert code == EXIT_INFRA
    err = capsys.readouterr().err
    assert "bench_legs_failed" in err and "recommend=unknown" in err


def test_invalid_env_var_name_is_rejected(env):
    # A project config carrying a shell-injecting env var name must be refused.
    cfgdir = env["proj"] / "do" / "remrun"
    cfgdir.mkdir(parents=True)
    (cfgdir / "remrun.toml").write_text('[env]\n"BAD; rm -rf x" = "1"\n')
    code = main(["run", "LOCAL_SIM", "--", "python", "-c", "print(1)"])
    assert code == EXIT_INTERNAL   # rejected, not executed


def test_clean_keep_prunes_old_runs(env):
    for i in range(3):
        main(["run", "LOCAL_SIM", "--", "python", "-c", f"print({i})"])
    runs_root = env["state"] / "runs"
    assert len(list(runs_root.iterdir())) == 3

    # Dry-run keeps everything.
    assert main(["clean", "--keep", "1", "--dry-run"]) == EXIT_OK
    assert len(list(runs_root.iterdir())) == 3

    # Real clean keeps only the newest run.
    assert main(["clean", "--keep", "1"]) == EXIT_OK
    assert len(list(runs_root.iterdir())) == 1


@pytest.fixture()
def two_device_env(tmp_path: Path, monkeypatch):
    """Two reachable sim devices, SIM_A preferred, each with its own remote tree.

    Mirrors the field topology behind the 2026-07-27 reports: --auto ranks one device
    first, that device's tree has a conflict, and a second reachable device is clean.
    """
    remrun_root = tmp_path / "remrun"
    (remrun_root / "config").mkdir(parents=True)
    local_base = tmp_path / "local" / "proj"
    remote_a = tmp_path / "remote_a"
    remote_b = tmp_path / "remote_b"
    state_root = tmp_path / "state"
    for d in (local_base, remote_a, remote_b):
        d.mkdir(parents=True)

    (remrun_root / "config" / "defaults.toml").write_text(
        '[transfer]\n'
        'mode = "safe"\n'
        'global_exclude = ["node_modules/**", ".git/**"]\n'
        'hash_small_files_below_mb = 8\n'
        '\n'
        '[scheduler]\n'
        'primary = "SIM_A"\n'
        'fallback = ["SIM_B"]\n'
        'load_balance = false\n'
        '\n'
        '[logging]\n'
        'backup_below_mb = 1\n'
    )
    devices = '[project_roots]\n' + "".join(
        f'{k} = "{posix(local_base)}"\n' for k in ("default", "macos", "windows")
    )
    for name, root in (("SIM_A", remote_a), ("SIM_B", remote_b)):
        devices += (
            f'\n[devices.{name}]\n'
            'enabled = true\n'
            'role = "simulation"\n'
            'kind = "local-sim"\n'
            'os = "posix"\n'
            f'project_root = "{posix(root)}"\n'
            f'state_root = "{posix(state_root)}"\n'
            f'cache_root = "{posix(tmp_path / ("cache_" + name))}"\n'
        )
    (remrun_root / "config" / "devices.toml").write_text(devices)

    monkeypatch.setenv("REMRUN_ROOT", str(remrun_root))
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(state_root))

    proj = local_base / "proj1"
    proj.mkdir()
    monkeypatch.chdir(proj)
    return {
        "proj": proj,
        "remote_a": remote_a / "proj1",
        "remote_b": remote_b / "proj1",
        "state": state_root,
        "remrun_root": remrun_root,
    }


@pytest.fixture()
def three_device_env(two_device_env):
    env = two_device_env
    root = env["remrun_root"].parent
    remote_c = root / "remote_c"
    remote_c.mkdir()
    defaults = env["remrun_root"] / "config" / "defaults.toml"
    defaults.write_text(
        defaults.read_text(encoding="utf-8").replace(
            'fallback = ["SIM_B"]', 'fallback = ["SIM_B", "SIM_C"]'
        ),
        encoding="utf-8",
    )
    devices = env["remrun_root"] / "config" / "devices.toml"
    with devices.open("a", encoding="utf-8") as handle:
        handle.write(
            '\n[devices.SIM_C]\n'
            'enabled = true\n'
            'role = "simulation"\n'
            'kind = "local-sim"\n'
            'os = "posix"\n'
            f'project_root = "{posix(remote_c)}"\n'
            f'state_root = "{posix(env["state"])}"\n'
            f'cache_root = "{posix(root / "cache_SIM_C")}"\n'
        )
    return {**env, "remote_c": remote_c / "proj1"}


def test_auto_fails_over_to_next_candidate_on_preflight_conflict(two_device_env, capsys):
    """A conflict on the first-ranked candidate must not abandon the run.

    Reported three times from separate projects: --auto stopped on the first candidate's
    `both-changed` paths while a reachable, conflict-free device sat unused. The conflict
    is a property of ONE candidate's tree and is raised before any mutation, so the next
    ranked candidate must be tried.
    """
    env = two_device_env
    # Baseline shared.txt on the local tree and on BOTH remotes.
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    assert main(["run", "SIM_B", "--", "python", "-c", "print('seed-b')"]) == EXIT_OK
    capsys.readouterr()

    # Diverge ONLY SIM_A: local and SIM_A both changed since their shared baseline.
    # SIM_B still matches its own baseline, so it can reconcile cleanly.
    (env["proj"] / "shared.txt").write_text("local-edit")
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")

    code = main(["run", "--auto", "--",
                 "python", "-c", "open('ran.txt','w').write('ok')"])

    # The run completed on the fallback rather than aborting on the preferred device.
    assert code == EXIT_OK
    assert (env["remote_b"] / "ran.txt").exists()
    assert not (env["remote_a"] / "ran.txt").exists()
    assert (env["proj"] / "ran.txt").read_text() == "ok"

    err = capsys.readouterr().err
    assert "candidate_skipped name=SIM_A reason=preflight_conflict" in err
    assert "target name=SIM_B" in err

    # The skipped candidate's conflict evidence is retained, not discarded.
    receipts = [json.loads(p.read_text())
                for p in (env["state"] / "conflicts").glob("*/conflicts.json")]
    assert [r for r in receipts if r["target"] == "SIM_A"
            and any(c["path"] == "shared.txt" for c in r["conflicts"])]
    # SIM_A's diverged bytes were left alone — failover must not "fix" the skipped device.
    assert (env["remote_a"] / "shared.txt").read_text() == "remote-a-edit"


def test_auto_casefold_collision_then_fallback_pull_leaves_local_unchanged(
    two_device_env, monkeypatch, capsys
):
    env = two_device_env
    local_path = env["proj"] / "Foo"
    local_path.write_text("LOCAL")
    assert main(["run", "SIM_B", "--", "python", "-c", "print('seed-b')"]) == EXIT_OK
    (env["remote_b"] / "Foo").write_text("REMOTE-B")
    env["remote_a"].mkdir(parents=True)
    (env["remote_a"] / "foo").write_text("REMOTE-A")
    monkeypatch.setattr("remrun.reconcile.current_os_key", lambda: "macos")
    capsys.readouterr()

    code = main(["run", "--auto", "--", "python", "-c", "print('must-not-run')"])

    assert code == EXIT_CONFLICT
    assert local_path.read_text() == "LOCAL"
    receipts = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (env["state"] / "conflicts").glob("*/conflicts.json")
    ]
    first = next(receipt for receipt in receipts if receipt["target"] == "SIM_A")
    assert any(
        conflict["path"] == "Foo | foo" and conflict["state"] == "casefold-collision"
        for conflict in first["conflicts"]
    )
    fallback = next(receipt for receipt in receipts if receipt["target"] == "SIM_B")
    assert any(
        conflict["path"] == "Foo" and conflict["state"] == "fallback-local-mutation"
        for conflict in fallback["conflicts"]
    )


def test_auto_retries_after_fallback_local_mutation_and_reaches_safe_third_candidate(
    three_device_env, capsys
):
    env = three_device_env
    shared = env["proj"] / "shared.txt"
    shared.write_text("v0")
    for name in ("SIM_A", "SIM_B", "SIM_C"):
        assert main(["run", name, "--", "python", "-c", f"print('seed-{name}')"]) == EXIT_OK

    shared.write_text("local-disputed")
    assert main(["run", "SIM_B", "--", "python", "-c", "print('baseline-b')"]) == EXIT_OK
    (env["remote_b"] / "shared.txt").write_text("remote-b-new")
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")
    capsys.readouterr()

    code = main([
        "run", "--auto", "--", "python", "-c", "open('ran.txt','w').write('safe')"
    ])

    assert code == EXIT_OK
    assert shared.read_text() == "local-disputed"
    assert (env["remote_b"] / "shared.txt").read_text() == "remote-b-new"
    assert not (env["remote_b"] / "ran.txt").exists()
    assert (env["remote_c"] / "shared.txt").read_text() == "local-disputed"
    assert (env["remote_c"] / "ran.txt").read_text() == "safe"
    err = capsys.readouterr().err
    assert "candidate_skipped name=SIM_B reason=preflight_conflict" in err
    assert "target name=SIM_C" in err
    receipts = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (env["state"] / "conflicts").glob("*/conflicts.json")
    ]
    fallback = next(receipt for receipt in receipts if receipt["target"] == "SIM_B")
    assert any(
        conflict["path"] == "shared.txt"
        and conflict["state"] == "fallback-local-mutation"
        for conflict in fallback["conflicts"]
    )


def test_auto_skips_fallback_that_would_pull_any_path(two_device_env, capsys):
    """A fallback may not pull even a path unrelated to the first candidate's conflict."""
    env = two_device_env
    shared = env["proj"] / "shared.txt"
    unrelated = env["proj"] / "unrelated.txt"
    shared.write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    assert main(["run", "SIM_B", "--", "python", "-c", "print('seed-b')"]) == EXIT_OK

    shared.write_text("local-disputed")
    unrelated.write_text("local-baseline")
    assert main(["run", "SIM_B", "--", "python", "-c", "print('baseline-b')"]) == EXIT_OK
    (env["remote_b"] / "unrelated.txt").write_text("remote-b-new")
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")
    capsys.readouterr()

    code = main(["run", "--auto", "--",
                 "python", "-c", "open('ran.txt','w').write('unsafe')"])

    assert code == EXIT_CONFLICT
    assert shared.read_text() == "local-disputed"
    assert unrelated.read_text() == "local-baseline"
    assert (env["remote_b"] / "unrelated.txt").read_text() == "remote-b-new"
    assert not (env["remote_b"] / "ran.txt").exists()
    err = capsys.readouterr().err
    assert "candidate_skipped name=SIM_A reason=preflight_conflict" in err
    receipts = [json.loads(p.read_text())
                for p in (env["state"] / "conflicts").glob("*/conflicts.json")]
    fallback = next(r for r in receipts if r["target"] == "SIM_B")
    assert any(c["path"] == "unrelated.txt" and c["state"] == "fallback-local-mutation"
               for c in fallback["conflicts"])


def test_auto_skips_fallback_that_would_delete_local_earlier_conflict_above_backup_cap(
    two_device_env, capsys
):
    """Failover must preserve a disputed local file even when no backup would be retained."""
    env = two_device_env
    disputed = "L" * (2 * 1024 * 1024)
    path = env["proj"] / "shared.txt"
    path.write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    assert main(["run", "SIM_B", "--", "python", "-c", "print('seed-b')"]) == EXIT_OK

    # SIM_B records the large local edit, then sees a clean remote-side deletion.
    path.write_text(disputed)
    assert main(["run", "SIM_B", "--", "python", "-c", "print('baseline-b')"]) == EXIT_OK
    (env["remote_b"] / "shared.txt").unlink()

    # SIM_A still has the old baseline and independently changed bytes: a real conflict.
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")
    capsys.readouterr()
    code = main(["run", "--auto", "--",
                 "python", "-c", "open('ran.txt','w').write('unsafe')"])

    assert code == EXIT_CONFLICT
    assert path.exists()
    assert path.read_text() == disputed
    assert not list((env["state"] / "conflicts").glob("*/backup/shared.txt"))
    assert not (env["remote_b"] / "ran.txt").exists()
    receipts = [json.loads(p.read_text())
                for p in (env["state"] / "conflicts").glob("*/conflicts.json")]
    fallback = next(r for r in receipts if r["target"] == "SIM_B")
    assert any(c["path"] == "shared.txt" and c["state"] == "fallback-local-mutation"
               for c in fallback["conflicts"])


def test_auto_returns_conflict_when_every_candidate_conflicts(two_device_env, capsys):
    """Failover must not mask a genuine all-candidates-conflicted state as success."""
    env = two_device_env
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    assert main(["run", "SIM_B", "--", "python", "-c", "print('seed-b')"]) == EXIT_OK
    capsys.readouterr()

    (env["proj"] / "shared.txt").write_text("local-edit")
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")
    (env["remote_b"] / "shared.txt").write_text("remote-b-edit")

    code = main(["run", "--auto", "--",
                 "python", "-c", "open('ran.txt','w').write('ok')"])
    assert code == EXIT_CONFLICT
    assert not (env["remote_a"] / "ran.txt").exists()
    assert not (env["remote_b"] / "ran.txt").exists()


def test_explicit_target_never_fails_over_on_conflict(two_device_env, capsys):
    """An explicitly named device is a user instruction, not a placement hint."""
    env = two_device_env
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    capsys.readouterr()

    (env["proj"] / "shared.txt").write_text("local-edit")
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")

    code = main(["run", "SIM_A", "--", "python", "-c", "open('ran.txt','w').write('ok')"])
    assert code == EXIT_CONFLICT
    # SIM_B was never touched: naming a device must not silently redirect the work.
    assert not (env["remote_b"] / "ran.txt").exists()
    assert "SIM_B" not in capsys.readouterr().err


def test_auto_local_vanished_aborts_without_trying_another_candidate(
    two_device_env, monkeypatch, capsys
):
    env = two_device_env
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    env["remote_b"].mkdir(parents=True)
    (env["remote_b"] / "from-b.txt").write_text("must not be pulled")
    capsys.readouterr()

    from remrun import cli as cli_mod

    real_resolve = cli_mod._resolve_targets
    vanished = env["proj"].with_name("proj1-vanished")

    def resolve_then_vanish(*args, **kwargs):
        selection = real_resolve(*args, **kwargs)
        # Windows will not rename a directory while it is the process cwd. Move
        # outside first, then create the same live product condition: the project
        # resolved at command start has vanished before preflight.
        monkeypatch.chdir(env["proj"].parent)
        env["proj"].rename(vanished)
        return selection

    monkeypatch.setattr(cli_mod, "_resolve_targets", resolve_then_vanish)
    try:
        code = main([
            "run", "--auto", "--", "python", "-c", "open('ran.txt','w').write('wrong')"
        ])
        err = capsys.readouterr().err
        assert code == EXIT_CONFLICT
        assert not env["proj"].exists()
        assert not (env["remote_b"] / "ran.txt").exists()
        assert "target name=SIM_B" not in err
    finally:
        if env["proj"].exists():
            shutil.rmtree(env["proj"])
        if vanished.exists():
            vanished.rename(env["proj"])


def test_auto_remote_vanished_still_fails_over(two_device_env, capsys):
    # Guard, not a regression test: remote-vanished failover already worked before this fix set.
    env = two_device_env
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    shutil.rmtree(env["remote_a"])
    capsys.readouterr()

    code = main([
        "run", "--auto", "--", "python", "-c", "open('ran.txt','w').write('ok')"
    ])

    assert code == EXIT_OK
    assert (env["remote_b"] / "ran.txt").read_text() == "ok"
    err = capsys.readouterr().err
    assert "candidate_skipped name=SIM_A reason=preflight_conflict" in err
    assert "target name=SIM_B" in err


def test_skipped_candidate_conflict_receipt_survives_successful_fallback_retention(
    two_device_env, capsys
):
    env = two_device_env
    (env["proj"] / "shared.txt").write_text("v0")
    assert main(["run", "SIM_A", "--", "python", "-c", "print('seed-a')"]) == EXIT_OK
    assert main(["run", "SIM_B", "--", "python", "-c", "print('seed-b')"]) == EXIT_OK
    defaults = env["remrun_root"] / "config" / "defaults.toml"
    defaults.write_text(
        defaults.read_text(encoding="utf-8")
        + "\nbackup_retention_days = 999\nmax_backup_mb = 0.000001\n",
        encoding="utf-8",
    )
    (env["proj"] / "shared.txt").write_text("local-edit")
    (env["remote_a"] / "shared.txt").write_text("remote-a-edit")
    capsys.readouterr()

    code = main(["run", "--auto", "--", "python", "-c", "print('fallback')"])

    assert code == EXIT_OK
    receipts = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (env["state"] / "conflicts").glob("*/conflicts.json")
    ]
    assert any(receipt["target"] == "SIM_A" for receipt in receipts)


def test_stdout_reaches_the_log_before_the_command_exits(env):
    """A long run must not be indistinguishable from a hang.

    Before streaming, `stdout.log` was written only after transport.exec returned, so a
    multi-hour run left a zero-byte log and an agent watching it could not tell live from
    dead (two independent field reports). Here a watcher thread
    releases the remote command only once it has seen the marker IN THE LOG — so the
    command can only exit if the bytes were really flushed mid-run, and the buffered
    implementation deadlocks until its own timeout instead of passing.
    """
    seen: dict[str, object] = {}
    state_root = env["state"]

    def watch() -> None:
        deadline = time.time() + 30
        while time.time() < deadline:
            for log in (state_root / "runs").glob("*/stdout.log"):
                if "LIVE-MARKER" in log.read_text(errors="replace"):
                    seen["mid_run"] = True
                    (env["proj"] / "observed.flag").write_text("1")
                    return
            time.sleep(0.05)

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    code = main(["run", "LOCAL_SIM", "--", "python", "-c",
                 "import time,os\n"
                 "print('LIVE-MARKER', flush=True)\n"
                 "deadline = time.time() + 30\n"
                 f"while not os.path.exists({str(env['proj'] / 'observed.flag')!r}):\n"
                 "    if time.time() > deadline: raise SystemExit('log never observed')\n"
                 "    time.sleep(0.05)\n"])
    watcher.join(timeout=5)

    assert code == EXIT_OK
    # The marker was in the log while the remote command was still running.
    assert seen.get("mid_run") is True


def test_streaming_stdout_log_is_capped_before_transport_returns(env, monkeypatch):
    max_bytes = 256
    set_log_cap(env, max_bytes)
    observed: dict[str, object] = {}
    payload = "x" * 10_000

    def verbose_exec(self, command, cwd, **kwargs):
        on_stdout = kwargs["on_stdout"]
        on_stdout(payload)
        log = next((env["state"] / "runs").glob("*/stdout.log"))
        data = log.read_bytes()
        observed["size"] = len(data)
        observed["truncated"] = b"remrun truncated" in data
        return ExecResult(0, payload, "")

    monkeypatch.setattr(LocalSimTransport, "exec", verbose_exec)

    code = main(["run", "LOCAL_SIM", "--", "python", "-c", "print('unused')"])

    assert code == EXIT_OK
    assert 0 < int(observed["size"]) <= max_bytes
    assert observed["truncated"] is True


def test_streaming_stdout_log_stays_capped_after_transport_error(env, monkeypatch):
    max_bytes = 256
    set_log_cap(env, max_bytes)

    def failing_exec(self, command, cwd, **kwargs):
        kwargs["on_stdout"]("x" * 10_000)
        raise TransportError("injected disconnect after output")

    monkeypatch.setattr(LocalSimTransport, "exec", failing_exec)

    code = main(["run", "LOCAL_SIM", "--", "python", "-c", "print('unused')"])

    assert code == EXIT_INFRA
    log = next((env["state"] / "runs").glob("*/stdout.log"))
    assert log.stat().st_size <= max_bytes
    assert "remrun truncated" in log.read_text(encoding="utf-8", errors="replace")


def test_unwritable_stdout_log_does_not_abort_run(env, monkeypatch):
    real_open = Path.open

    def deny_stdout_log(self, mode="r", *args, **kwargs):
        if self.name == "stdout.log" and any(flag in mode for flag in "wax+"):
            raise PermissionError("injected unwritable stdout log")
        return real_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", deny_stdout_log)

    code = main([
        "run", "LOCAL_SIM", "--", "python", "-c", "open('ran.txt','w').write('ok')"
    ])

    assert code == EXIT_OK
    assert (env["proj"] / "ran.txt").read_text() == "ok"
