"""Integration tests driving remrun.cli.main end-to-end via LOCAL_SIM."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from remrun.cli import (
    EXIT_CONFLICT, EXIT_INFRA, EXIT_INTERNAL, EXIT_OK, _best_remote_verdict, main,
)
from remrun.profile import LOCAL_DEVICE, command_key, device_profile, load_profiles
from remrun.transport import LocalSimTransport, TransportError


def posix(p: Path) -> str:
    return str(p).replace("\\", "/")


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
    return {"proj": proj, "remote_proj": remote_base / "proj1", "state": state_root}


def test_run_happy_path_pulls_output(env, capsys):
    (env["proj"] / "input.txt").write_text("in")
    code = main(["run", "LOCAL_SIM", "--",
                 "python", "-c", "open('result.txt','w').write('ok')"])
    assert code == EXIT_OK
    # Output created remotely is pulled back to the local project path.
    assert (env["proj"] / "result.txt").read_text() == "ok"
    assert (env["remote_proj"] / "input.txt").read_text() == "in"
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
