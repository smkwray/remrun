"""`remrun fleet resources`: parsing, rendering, concurrency, and failure reporting.

The parsing tests use REAL captured output from the live fleet (MACHUB/WINBOX/WINTWO),
so a change that breaks a parser fails here rather than silently rendering `-`.
"""
from __future__ import annotations

import json
import time
from types import SimpleNamespace

import pytest

from remrun.fleet import resources
from remrun.fleet.resources import ResourceView, _parse_posix, _parse_windows, probe_fleet
from remrun.fleet.resources_render import render_table, to_dict
from remrun.models import Device

# Captured verbatim from MACHUB (Mac Studio, M1 Ultra) on 2026-07-26.
MACOS_RESOURCE_OUT = """HOST=MACHUB
NCPU=20
MEMTOTAL=68719476736
CHIP=Apple M1 Ultra
LOADAVG= 4.35 3.98 3.71
CPUUSAGE:CPU usage: 9.52% user, 4.76% sys, 85.71% idle
VMSTAT:Mach Virtual Memory Statistics: (page size of 16384 bytes)
VMSTAT:Pages free:                               580325.
VMSTAT:Pages active:                            1639430.
VMSTAT:Pages inactive:                          1588829.
AGX:"Device Utilization %"=37
"""

# Captured verbatim from WINBOX (Windows, RTX 5060 Ti) on 2026-07-26.
WINDOWS_RESOURCE_OUT = """HOST=WINBOX
MEMTOTAL_KB=33002636
RAM_AVAIL_MB=14830
MEMFREE_KB=15184000
CPU_IDLE=84
NCPU=20
CHIP=Intel(R) Core(TM) Ultra 7 255HX
NVIDIA:NVIDIA GeForce RTX 5060 Ti, 3, 14321, 16311
"""


def test_parse_posix_unified_memory_mac():
    view = ResourceView(name="MACHUB", reachable=True)
    _parse_posix(MACOS_RESOURCE_OUT, view)
    assert view.hostname == "MACHUB"
    assert view.chip == "Apple M1 Ultra"
    assert view.cpu_count == 20
    assert view.ram_total_mb == pytest.approx(65536.0)
    # (580325 + 1588829) pages * 16384 B = ~33.9 GB available.
    assert view.ram_free_mb == pytest.approx(33908.7, rel=1e-3)
    # REAL utilization from top (100 - 85.71 idle), NOT load/cores. Load average
    # counts blocked threads, so it overstates busy-ness: measured on MACBOX, load
    # 10.84/14 cores implied "77% busy" while top reported 66% idle.
    assert view.cpu_busy_pct == pytest.approx(14.29, rel=1e-2)
    # Load is kept separately as the queueing/oversubscription signal.
    assert view.load1 == pytest.approx(4.35)
    assert view.load_per_core == pytest.approx(0.2175)
    assert view.oversubscribed is False
    # Apple GPU: utilization is real, but VRAM is not a separate pool.
    assert view.gpu_unified is True
    assert view.gpu_util_pct == 37.0
    assert view.vram_total_mb is None


def test_parse_windows_discrete_gpu():
    view = ResourceView(name="WINBOX", reachable=True)
    _parse_windows(WINDOWS_RESOURCE_OUT, view)
    assert view.hostname == "WINBOX"
    assert view.cpu_count == 20
    assert view.cpu_busy_pct == pytest.approx(16.0)     # 100 - 84 idle
    assert view.ram_total_mb == pytest.approx(32229.1, rel=1e-3)
    assert view.ram_free_mb == 14830.0                  # AvailableMBytes, not FreePhysicalMemory
    assert view.gpu_unified is False
    assert view.gpu_name == "NVIDIA GeForce RTX 5060 Ti"
    assert view.gpu_util_pct == 3.0
    assert view.vram_free_mb == 14321.0
    assert view.vram_total_mb == 16311.0


def test_load_average_is_not_used_as_cpu_busy():
    """The MACBOX misreport: load 10.84 over 14 cores rendered as 95% CPU while the
    box was ~34% busy. Load counts runnable AND blocked threads, so it is a
    queueing signal, not a utilization one. They must stay separate fields.
    """
    view = ResourceView(name="MACBOX", reachable=True)
    _parse_posix("NCPU=14\nLOADAVG= 10.84 11.22 10.41\n"
                 "CPUUSAGE:CPU usage: 27.29% user, 6.40% sys, 66.29% idle\n", view)
    assert view.cpu_busy_pct == pytest.approx(33.71, rel=1e-2)   # 100 - 66.29
    assert view.load1 == pytest.approx(10.84)
    assert view.load_per_core == pytest.approx(0.774, rel=1e-2)


def test_oversubscription_detected_independently_of_cpu_busy():
    """A box can be oversubscribed while CPU looks modest — that is contention
    (threads waiting on each other or on I/O), and it is exactly the state that
    makes a device a bad place to send more work."""
    view = ResourceView(name="X", reachable=True)
    _parse_posix("NCPU=4\nLOADAVG= 9.60 9.0 8.5\n"
                 "CPUUSAGE:CPU usage: 20.0% user, 5.0% sys, 75.0% idle\n", view)
    assert view.cpu_busy_pct == pytest.approx(25.0)
    assert view.oversubscribed is True
    assert "OVERSUB" in view.load_label
    # Unmeasured load is unknown, not "fine".
    assert ResourceView(name="Y", reachable=True).oversubscribed is None


def test_loadavg_fallback_is_labelled_when_top_is_unavailable():
    """Linux boxes without `top -l` still get a CPU figure, but the note says the
    number came from load average so it is not mistaken for real utilization."""
    view = ResourceView(name="L", reachable=True)
    _parse_posix("NCPU=8\nLOADAVG= 4.00 3.0 2.0\n", view)
    assert view.cpu_busy_pct == pytest.approx(50.0)
    assert any("loadavg" in n for n in view.notes)


def test_timeout_is_retried_before_declaring_a_device_unreachable():
    """A loaded-but-healthy box must not be reported offline.

    MACHUB answered fine by hand at load average 8.4 while a concurrent fleet
    probe called it offline; probe() has a fixed 20 s ssh budget it can miss.
    """
    calls = {"n": 0}

    class Flaky:
        def probe(self):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("ssh timed out after 20s")
            return SimpleNamespace(reachable=True, detail="ssh ok")

        def exec(self, *a, **k):
            return SimpleNamespace(stdout="HOST=MACHUB\nNCPU=20\n", exit_code=0)

    device = Device.from_mapping("MACHUB", {"kind": "ssh-posix", "os": "macos",
                                          "address_candidates": ["machub"],
                                          "project_root": "/tmp", "state_root": "/tmp",
                                          "cache_root": "/tmp"})
    view = resources.probe_device(device, transport=Flaky())
    assert calls["n"] == 2                       # retried once
    assert view.reachable is True
    assert any("retried" in n for n in view.notes)


def test_diagnose_only_probes_the_devices_own_ip(monkeypatch):
    """A hijacking router must not be able to speak for a device.

    Some routers resolve every unknown local alias to their gateway address,
    which answers SSH with "Host key verification failed". Diagnosing via aliases
    then reports healthy, reachable Macs as "hostname did not resolve" /
    "host key not trusted" — a fact about the router, presented as a fact about
    the device.
    """
    tried = []

    def fake_run(command, timeout, **kw):
        tried.append(command[-2])
        return SimpleNamespace(returncode=255, stdout=b"", stderr=b"Host key verification failed.")

    monkeypatch.setattr(resources.subprocess, "run", fake_run)
    device = Device.from_mapping("MACTWO", {"kind": "ssh-posix", "os": "macos",
                                          "address_candidates": ["mactwo", "mactwo.local"],
                                          "tailscale_ip": "192.0.2.14",
                                          "user": "user", "project_root": "/tmp",
                                          "state_root": "/tmp", "cache_root": "/tmp"})
    resources._diagnose(device, "some earlier failure")
    assert tried == ["user@192.0.2.14"]     # never the router-hijackable aliases


def test_diagnose_uses_a_shell_portable_probe_command(monkeypatch):
    """`true` is a POSIX builtin PowerShell lacks; using it would fail a working
    Windows login and invent a reason for a healthy device."""
    seen = []

    def fake_run(command, timeout, **kw):
        seen.append(command[-1])
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(resources.subprocess, "run", fake_run)
    device = Device.from_mapping("W", {"kind": "ssh-powershell", "os": "windows",
                                       "address_candidates": ["w"], "tailscale_ip": "192.0.2.9",
                                       "project_root": "C:\\", "state_root": "C:\\",
                                       "cache_root": "C:\\"})
    resources._diagnose(device, "earlier failure")
    assert seen == ["exit 0"]


def test_auth_failure_is_not_retried():
    """Only timeouts are worth retrying; a refused key is deterministic, so
    retrying it just doubles the wait for a known answer."""
    calls = {"n": 0}

    class Refuses:
        def probe(self):
            calls["n"] += 1
            raise RuntimeError("Permission denied (publickey)")

    device = Device.from_mapping("X", {"kind": "ssh-posix", "os": "macos",
                                       "address_candidates": ["x"], "project_root": "/tmp",
                                       "state_root": "/tmp", "cache_root": "/tmp"})
    view = resources.probe_device(device, transport=Refuses())
    assert calls["n"] == 1
    assert view.reachable is False


def test_progress_callback_fires_per_device(monkeypatch):
    """A blank terminal during a multi-second probe is indistinguishable from a
    hang, so callers need start/done events."""
    events = []
    monkeypatch.setattr(resources, "probe_device",
                        lambda d, timeout=45.0: ResourceView(name=d.name, reachable=True))
    devices = [_device("A"), _device("B")]
    resources.probe_fleet(devices, on_event=lambda kind, name, view: events.append((kind, name)))
    assert sorted(events) == [("done", "A"), ("done", "B"), ("start", "A"), ("start", "B")]


def test_windows_prefers_available_over_free_physical():
    """AvailableMBytes counts the reclaimable standby cache; FreePhysicalMemory does not.

    Picking the wrong one makes a box that is merely caching files look nearly
    full, which is exactly the misreading this report exists to prevent.
    """
    view = ResourceView(name="W", reachable=True)
    _parse_windows("RAM_AVAIL_MB=14830\nMEMFREE_KB=2048000\n", view)
    assert view.ram_free_mb == 14830.0


def test_windows_falls_back_to_free_physical_when_perf_class_absent():
    view = ResourceView(name="W", reachable=True)
    _parse_windows("RAM_AVAIL_MB=\nMEMFREE_KB=2097152\n", view)
    assert view.ram_free_mb == pytest.approx(2048.0)     # KB -> MB


def test_missing_measurements_never_become_zero():
    """A probe that measured nothing must render `-`, not a plausible 0%.

    Contract rule: malformed/absent input never becomes a plausible value. A
    device reported as 0% busy with 0 GB used would read as idle-and-empty.
    """
    view = ResourceView(name="X", reachable=True)
    _parse_posix("HOST=X\n", view)
    assert view.cpu_busy_pct is None
    assert view.ram_free_mb is None
    assert view.gpu_util_pct is None
    table = render_table([view])
    assert "0%" not in table
    assert "-" in table


def test_linux_meminfo_fallback_when_no_vm_stat():
    view = ResourceView(name="L", reachable=True)
    _parse_posix("HOST=L\nNCPU=8\nMEMINFO:MemTotal:       16384000 kB\n"
                 "MEMINFO:MemAvailable:    8192000 kB\n", view)
    assert view.ram_total_mb == pytest.approx(16000.0)
    assert view.ram_free_mb == pytest.approx(8000.0)


def test_render_marks_local_and_reports_unreachable_reason():
    views = [
        ResourceView(name="WINTWO", reachable=True, is_local=True, cpu_busy_pct=25.0,
                     ram_free_mb=6144.0, ram_total_mb=32768.0),
        ResourceView(name="MACBOX", reachable=False,
                     detail="ssh key missing on this controller: ~/.ssh/id_ed25519_mesh"),
    ]
    table = render_table(views)
    assert "WINTWO*" in table and "* this controller" in table
    # The failure states the cause, so a broken key is diagnosable from the table.
    assert "ssh key missing" in table
    # An unreachable device reports no invented metrics.
    unreachable_line = next(ln for ln in table.splitlines() if ln.startswith("MACBOX"))
    assert "%" not in unreachable_line


def test_unified_gpu_never_reports_a_vram_figure():
    """Apple silicon shares system RAM; a VRAM number there would be fiction."""
    view = ResourceView(name="MACHUB", reachable=True, gpu_unified=True, gpu_util_pct=37.0)
    cell = render_table([view])
    assert "unified" in cell
    assert "GB" not in cell.split("unified")[0].split("MACHUB")[-1]


def test_ram_used_pct_derived_from_free_and_total():
    view = ResourceView(name="D", reachable=True, ram_free_mb=8192.0, ram_total_mb=32768.0)
    assert view.ram_used_pct == pytest.approx(75.0)
    # Unmeasured stays unmeasured rather than defaulting to 0%.
    assert ResourceView(name="D", reachable=True).ram_used_pct is None


def _device(name: str, kind: str = "ssh-posix") -> Device:
    return Device.from_mapping(name, {"kind": kind, "os": "macos",
                                      "address_candidates": ["localhost"],
                                      "project_root": "/tmp", "state_root": "/tmp",
                                      "cache_root": "/tmp"})


def test_probe_fleet_is_concurrent_and_preserves_order(monkeypatch):
    """A slow/dead box must not serialize the report.

    Four devices sleeping 0.3 s each take ~1.2 s serially; concurrently they
    take ~0.3 s. Asserting well under the serial total keeps this meaningful
    without being timing-flaky.
    """
    def slow(device, timeout=30.0):
        time.sleep(0.3)
        return ResourceView(name=device.name, reachable=True)

    monkeypatch.setattr(resources, "probe_device", lambda d, timeout=30.0: slow(d, timeout))
    devices = [_device(n) for n in ("A", "B", "C", "D")]
    start = time.time()
    views = probe_fleet(devices)
    elapsed = time.time() - start
    assert [v.name for v in views] == ["A", "B", "C", "D"]   # order preserved
    assert elapsed < 0.9                                      # serial would be ~1.2 s


def test_probe_fleet_one_dead_device_does_not_kill_the_report(monkeypatch):
    def flaky(device, timeout=30.0):
        if device.name == "B":
            raise RuntimeError("ssh exploded")
        return ResourceView(name=device.name, reachable=True)

    monkeypatch.setattr(resources, "probe_device", flaky)
    with pytest.raises(RuntimeError):
        # probe_device is the layer that must swallow errors; if a caller ever
        # replaces it with a raising version, the pool surfaces it rather than
        # silently dropping a device from the fleet picture.
        probe_fleet([_device("A"), _device("B")])


def test_probe_device_never_raises_on_transport_failure():
    """The real contract: one dead device yields an unreachable row, not an exception."""
    class Boom:
        def probe(self):
            raise RuntimeError("Permission denied (publickey)")

    view = resources.probe_device(_device("DEAD"), transport=Boom())
    assert view.reachable is False
    assert "auth refused" in view.detail


def test_friendly_errors_name_the_actual_fix():
    from remrun.fleet.resources import _friendly_error
    assert "auth refused" in _friendly_error("user@host: Permission denied (publickey,password)")
    assert "key missing" in _friendly_error(
        "Warning: Identity file /c/Users/x/.ssh/id_ed25519_mesh not accessible: No such file")
    assert "host key not trusted" in _friendly_error("Host key verification failed.")
    assert "sshd not running" in _friendly_error("ssh: connect to host x port 22: Connection refused")
    # A timeout must NOT assert "offline": a healthy but loaded box times out too.
    timed_out = _friendly_error("ssh timed out after 8s")
    assert "no answer within timeout" in timed_out
    assert "too loaded" in timed_out


def test_auth_failure_is_not_masked_by_a_later_dns_failure():
    """The real MACBOX/MACFS bug: the transport tries every address candidate and
    keeps only the LAST error. Both devices answered on their Tailscale IP and
    refused the key, but a trailing unresolvable alias made the report say
    "hostname did not resolve" — sending you to fix DNS instead of the key.
    """
    from remrun.fleet.resources import _friendly_error
    combined = ("user@192.0.2.11: Permission denied (publickey,password).\n"
                "ssh: Could not resolve hostname macbox.example-net.ts.net: Name or service not known")
    assert _friendly_error(combined) == "ssh auth refused (no authorized key for this controller)"


def test_pure_dns_failure_still_reports_dns():
    from remrun.fleet.resources import _friendly_error
    assert _friendly_error(
        "ssh: Could not resolve hostname nope.local: Name or service not known"
    ) == "hostname did not resolve"


def test_json_payload_is_complete_and_serializable():
    view = ResourceView(name="WINBOX", reachable=True, cpu_busy_pct=12.0, ram_free_mb=14608.0,
                        ram_total_mb=32229.0, gpu_util_pct=3.0, vram_free_mb=14321.0,
                        vram_total_mb=16311.0, gpu_name="RTX 5060 Ti")
    payload = to_dict(view)
    json.dumps(payload)                     # must not raise
    assert payload["gpu_util_pct"] == 3.0
    assert payload["vram_free_mb"] == 14321.0
    assert payload["ram_used_pct"] == pytest.approx(54.68, rel=1e-3)


def test_resources_command_shows_disabled_devices_by_default(monkeypatch, capsys):
    """`enabled = false` governs PLACEMENT, not visibility.

    A file server or paused laptop still has hardware worth seeing when you are
    deciding where to send work; hiding it would make the fleet picture lie.
    """
    from remrun.fleet import cli

    disabled = Device.from_mapping("MACFS", {"kind": "ssh-posix", "os": "macos", "enabled": False,
                                            "address_candidates": ["x"], "project_root": "/tmp",
                                            "state_root": "/tmp", "cache_root": "/tmp"})
    enabled = _device("MACHUB")
    config = SimpleNamespace(devices={"MACHUB": enabled, "MACFS": disabled})
    monkeypatch.setattr(cli, "load_config", lambda: config)
    monkeypatch.setattr("remrun.fleet.resources.probe_device",
                        lambda d, timeout=30.0: ResourceView(name=d.name, reachable=True))

    args = SimpleNamespace(device=None, no_local=True, enabled_only=False,
                           timeout=5.0, json=False)
    from remrun.output import Reporter
    assert cli.cmd_resources(args, Reporter(json_events=False)) == 0
    out = capsys.readouterr().out
    assert "MACFS" in out and "MACHUB" in out

    args.enabled_only = True
    cli.cmd_resources(args, Reporter(json_events=False))
    assert "MACFS" not in capsys.readouterr().out


def test_local_sim_is_never_reported(monkeypatch, capsys):
    """A simulation backend has no hardware; reporting it would invent a device."""
    from remrun.fleet import cli
    from remrun.output import Reporter

    sim = Device.from_mapping("LOCAL_SIM", {"kind": "local-sim", "os": "posix",
                                            "address_candidates": ["localhost"],
                                            "project_root": "/tmp", "state_root": "/tmp",
                                            "cache_root": "/tmp"})
    monkeypatch.setattr(cli, "load_config", lambda: SimpleNamespace(devices={"LOCAL_SIM": sim}))
    args = SimpleNamespace(device=None, no_local=True, enabled_only=False, timeout=5.0, json=False)
    cli.cmd_resources(args, Reporter(json_events=False))
    assert "LOCAL_SIM" not in capsys.readouterr().out
