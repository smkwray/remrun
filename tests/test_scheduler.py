"""Tests for --auto device selection: ordering and CPU/perf-aware load balancing."""
import pytest

from remrun.models import Device
from remrun.scheduler import SchedulingError, order_devices, pick_by_load


def _dev(name, **kw):
    data = {"enabled": True, "kind": "ssh-posix", "os": "macos", **kw}
    return Device.from_mapping(name, data)


def _devices():
    return {
        "MACBOX": _dev("MACBOX", perf_cores=16, eff_cores=4),
        "WINBOX": _dev("WINBOX", os="windows", kind="ssh-powershell", perf_cores=8, eff_cores=12),
        "LOCAL_SIM": _dev("LOCAL_SIM", kind="local-sim", os="posix"),
    }


SCHED = {
    "primary": "MACBOX",
    "fallback": ["WINBOX"],
    "load_balance": True,
    "busy_floor_pct": 40,
    "headroom_margin_cores": 4,
    "eff_core_weight": 0.5,
}


def test_explicit_target_resolves_to_single():
    assert [d.name for d in order_devices(_devices(), "WINBOX")] == ["WINBOX"]


def test_explicit_unknown_raises():
    with pytest.raises(SchedulingError):
        order_devices(_devices(), "NOPE")


def test_auto_uses_scheduler_order_and_excludes_local_sim():
    names = [d.name for d in order_devices(_devices(), "auto", scheduler_cfg=SCHED)]
    assert names == ["MACBOX", "WINBOX"]  # LOCAL_SIM is not in primary/fallback


def test_devices_toml_scheduler_order_overrides_defaults(tmp_path):
    # The --auto preference order names real devices, so it belongs in the private
    # devices.toml, not the published defaults.toml. Prove devices.toml wins, and that
    # device-agnostic tuning knobs from defaults.toml still survive the merge.
    from remrun.config import load_config, scheduler_config

    cfgdir = tmp_path / "config"
    cfgdir.mkdir()
    (cfgdir / "defaults.toml").write_text(
        '[scheduler]\nprimary = "FROM_DEFAULTS"\nbusy_floor_pct = 55\n', encoding="utf-8")
    (cfgdir / "devices.toml").write_text(
        '[scheduler]\nprimary = "FROM_DEVICES"\nfallback = ["SECOND"]\n'
        '[devices.FROM_DEVICES]\nkind = "ssh-posix"\nproject_root = "/x"\n'
        '[devices.SECOND]\nkind = "ssh-posix"\nproject_root = "/x"\n', encoding="utf-8")

    s = scheduler_config(load_config(tmp_path))
    assert s["primary"] == "FROM_DEVICES"       # devices.toml wins the order
    assert s["fallback"] == ["SECOND"]
    assert s["busy_floor_pct"] == 55            # defaults.toml keeps the tuning knobs


def test_auto_drops_controller_with_a_different_configured_root(monkeypatch, tmp_path):
    monkeypatch.setattr("socket.gethostname", lambda: "MACBOX.local")
    devices = _devices()
    devices["MACBOX"] = _dev(
        "MACBOX", project_root=str(tmp_path / "different-projects"),
        perf_cores=16, eff_cores=4,
    )

    names = [d.name for d in order_devices(devices, "auto", scheduler_cfg=SCHED)]

    assert names == ["WINBOX"]


def test_auto_drops_controller_even_when_configured_root_looks_matching(monkeypatch, tmp_path):
    # order_devices has neither the detected local project root nor the project ID needed
    # to resolve device.project_root for this project. A matching-looking base therefore
    # cannot prove that loopback SSH would target the same directory.
    monkeypatch.setattr("socket.gethostname", lambda: "MACBOX")
    devices = _devices()
    devices["MACBOX"] = _dev(
        "MACBOX", project_root=str(tmp_path), perf_cores=16, eff_cores=4,
    )

    names = [d.name for d in order_devices(devices, "auto", scheduler_cfg=SCHED)]

    assert names == ["WINBOX"]


def test_auto_matches_and_drops_self_by_address_alias(monkeypatch):
    # A box is often reachable by an alias whose case differs from the configured name.
    devices = _devices()
    devices["MACBOX"] = _dev("MACBOX", perf_cores=16, eff_cores=4,
                             address_candidates=["macbox.local", "macbox"])
    monkeypatch.setattr("socket.gethostname", lambda: "macbox")
    names = [d.name for d in order_devices(devices, "auto", scheduler_cfg=SCHED)]
    assert names == ["WINBOX"]


def test_auto_raises_when_the_controller_is_the_only_candidate(monkeypatch):
    monkeypatch.setattr("socket.gethostname", lambda: "MACBOX")
    only = {"MACBOX": _dev("MACBOX", perf_cores=16, eff_cores=4)}

    with pytest.raises(SchedulingError, match="No safe auto-routing candidates"):
        order_devices(only, "auto", scheduler_cfg={"primary": "MACBOX", "fallback": []})


@pytest.mark.parametrize(
    "loopback",
    [
        "localhost",
        "localhost.localdomain",
        "127.0.0.1",
        "127.42.0.9",
        "::1",
        "0.0.0.0",
        "::",
        "[::]",
    ],
)
def test_auto_excludes_loopback_alias_but_explicit_target_still_allows_it(
    monkeypatch, loopback
):
    monkeypatch.setattr("socket.gethostname", lambda: "actual-host.example")
    devices = {
        "CONTROLLER": _dev("CONTROLLER", address_candidates=[loopback]),
        "REMOTE": _dev("REMOTE", address_candidates=["remote.example"]),
    }
    scheduler = {"primary": "CONTROLLER", "fallback": ["REMOTE"]}

    assert [d.name for d in order_devices(devices, "auto", scheduler_cfg=scheduler)] == [
        "REMOTE"
    ]
    assert [d.name for d in order_devices(devices, "CONTROLLER", scheduler_cfg=scheduler)] == [
        "CONTROLLER"
    ]


@pytest.mark.parametrize(
    "remote_ipv6",
    ["2001:db8::42", "[2001:db8::42]", "fe80::42%en0"],
)
def test_auto_keeps_remote_ipv6_candidates(monkeypatch, remote_ipv6):
    monkeypatch.setattr("socket.gethostname", lambda: "actual-host.example")
    devices = {
        "REMOTE": _dev("REMOTE", address_candidates=[remote_ipv6]),
    }

    assert [d.name for d in order_devices(devices, "auto")] == ["REMOTE"]


def test_explicit_target_may_still_be_the_controller_itself_with_any_root(monkeypatch, tmp_path):
    # Guard, not a regression test: explicit self-targeting was already deliberately allowed.
    # Naming your own box is deliberate, so explicit routing is not subject to the auto gate.
    monkeypatch.setattr("socket.gethostname", lambda: "MACBOX")
    devices = _devices()
    devices["MACBOX"] = _dev("MACBOX", project_root=str(tmp_path / "different-projects"))

    names = [d.name for d in order_devices(devices, "MACBOX", scheduler_cfg=SCHED)]

    assert names == ["MACBOX"]


def test_auto_placement_command_rule_wins():
    pc = {"placement": {"primary": "MACBOX", "fallback": ["WINBOX"],
                        "rules": [{"match_command": r"\.do$", "prefer": "WINBOX"}]}}
    names = [d.name for d in order_devices(
        _devices(), "auto", project_config=pc, command=["stata", "run.do"], scheduler_cfg=SCHED)]
    assert names[0] == "WINBOX"


def test_pick_keeps_primary_when_idle():
    devs = _devices()
    d, reason = pick_by_load([(devs["MACBOX"], 10.0), (devs["WINBOX"], 5.0)], SCHED)
    assert (d.name, reason) == ("MACBOX", "auto")  # below busy_floor -> no reallocation


def test_pick_reallocates_when_primary_busy_and_alt_much_freer():
    devs = _devices()
    # MACBOX 90% -> spare 18*0.1=1.8 ; WINBOX 5% -> spare 14*0.95=13.3 ; diff >> 4
    d, reason = pick_by_load([(devs["MACBOX"], 90.0), (devs["WINBOX"], 5.0)], SCHED)
    assert (d.name, reason) == ("WINBOX", "auto-loadbalance")


def test_pick_keeps_primary_when_margin_not_met():
    devs = _devices()
    # MACBOX 70% -> spare 18*0.3=5.4 ; WINBOX 50% -> spare 14*0.5=7.0 ; diff 1.6 < 4
    d, reason = pick_by_load([(devs["MACBOX"], 70.0), (devs["WINBOX"], 50.0)], SCHED)
    assert (d.name, reason) == ("MACBOX", "auto")


def test_pick_load_balance_off_keeps_primary():
    devs = _devices()
    d, reason = pick_by_load([(devs["MACBOX"], 95.0), (devs["WINBOX"], 1.0)],
                             {**SCHED, "load_balance": False})
    assert (d.name, reason) == ("MACBOX", "auto")


def test_pick_unknown_load_keeps_primary():
    devs = _devices()
    d, reason = pick_by_load([(devs["MACBOX"], None), (devs["WINBOX"], 5.0)], SCHED)
    assert (d.name, reason) == ("MACBOX", "auto")  # primary load unknown -> don't move
