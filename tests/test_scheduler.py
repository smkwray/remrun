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
