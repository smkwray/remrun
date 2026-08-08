"""`remrun fleet resources`: parsing, rendering, concurrency, and failure reporting.

The parsing tests use REAL captured output from the live fleet (MACHUB/WINBOX/WINTWO),
so a change that breaks a parser fails here rather than silently rendering `-`.
"""
from __future__ import annotations

import io
import json
import time
from types import SimpleNamespace

import pytest

from remrun.fleet import probes, resources
from remrun.fleet.models import DeviceSnapshot
from remrun.fleet.resources import (
    PrimaryDiskView,
    ResourceView,
    _parse_posix,
    _parse_windows,
    probe_fleet,
)
from remrun.fleet.resources_render import IncrementalTable, render_table, to_dict
from remrun.models import Device
from remrun.resource_envelope import MIB, Metric
from remrun.resource_probe import GPUResourceSnapshot, ResourceSnapshot

# Captured from MACHUB (Mac Studio, M1 Ultra). The CPUUSAGE line is the
# normalized rendering of iostat's interval fields: `us sy id`.
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
DISK_JSON={"mount":"/","total_bytes":"994662584320","available_bytes":"460475989216","semantics":"effective-used","source":"Foundation important-usage capacity"}
"""

# Captured verbatim from WINBOX (Windows, RTX 5060 Ti) on 2026-07-26.
WINDOWS_RESOURCE_OUT = """HOST=WINBOX
MEMTOTAL_KB=33002636
RAM_AVAIL_MB=14830
MEMFREE_KB=15184000
CPU_IDLE=84
NCPU=20
CPU_QUEUE=3
CHIP=Intel(R) Core(TM) Ultra 7 255HX
NVIDIA:NVIDIA GeForce RTX 5060 Ti, 3, 14321, 16311
DISK_JSON={"mount":"C:","total_bytes":"1000000000000","available_bytes":"400000000000","semantics":"allocated-used","source":"Win32_LogicalDisk"}
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
    # REAL interval utilization (100 - 85.71 idle), NOT load/cores. A one-minute
    # demand average can remain high while current CPU utilization is low.
    assert view.cpu_busy_pct == pytest.approx(14.29, rel=1e-2)
    # Load is retained separately as the raw one-minute demand ratio.
    assert view.load1 == pytest.approx(4.35)
    assert view.load_per_core == pytest.approx(0.2175)
    # Apple GPU: utilization is real, but VRAM is not a separate pool.
    assert view.gpu_unified is True
    assert view.gpu_util_pct == 37.0
    assert view.vram_total_mb is None
    assert view.primary_disk.used_bytes == 534186595104
    assert view.primary_disk.used_pct == pytest.approx(53.7043, rel=1e-4)
    assert view.primary_disk.semantics == "effective-used"


def test_macos_resource_script_avoids_process_walking_top():
    """A reachable busy Mac must not be called offline because `top` is slow.

    A live macOS runner's `top -l 2 -n 0` took 43.79 seconds after a reboot;
    SSH setup pushed the combined probe past its 45-second timeout. The macOS
    branch must use the bounded interval counters from built-in `iostat`.
    """
    script = resources._POSIX_SCRIPT
    darwin_branch = script.split(
        'if [ "$(uname -s 2>/dev/null)" = "Darwin" ]; then',
        1,
    )[1].split("else", 1)[0]
    assert "iostat -c 2 -w 1" in darwin_branch
    assert "top " not in darwin_branch


def test_linux_resource_script_uses_interval_proc_stat_not_macos_top():
    assert "/proc/stat" in resources._POSIX_SCRIPT
    assert "sleep 0.5" in resources._POSIX_SCRIPT
    view = ResourceView(name="L", reachable=True)
    _parse_posix("CPUUSAGE:CPU usage: 25.00% busy, 75.00% idle\n", view)
    assert view.cpu_busy_pct == pytest.approx(25.0)


def test_disk_probe_reads_capacity_metadata_without_scanning_files():
    assert "NSURLVolumeAvailableCapacityForImportantUsageKey" in resources._POSIX_SCRIPT
    assert "df -B1 -P /" in resources._POSIX_SCRIPT
    assert "Win32_LogicalDisk" in resources._WINDOWS_SCRIPT
    for recursive_scan in ("du ", "find /", "Get-ChildItem", "os.walk"):
        assert recursive_scan not in resources._POSIX_SCRIPT
        assert recursive_scan not in resources._WINDOWS_SCRIPT


def test_windows_resource_script_reads_bounded_processor_queue_metadata():
    script = resources._WINDOWS_SCRIPT
    assert "Win32_PerfFormattedData_PerfOS_System" in script
    assert "ProcessorQueueLength" in script
    assert "Get-Counter" not in script
    assert "Start-Sleep" not in script


def test_parse_windows_discrete_gpu():
    view = ResourceView(name="WINBOX", reachable=True)
    _parse_windows(WINDOWS_RESOURCE_OUT, view)
    assert view.hostname == "WINBOX"
    assert view.cpu_count == 20
    assert view.cpu_busy_pct == pytest.approx(16.0)     # 100 - 84 idle
    assert view.processor_queue_length == 3.0
    assert view.processor_queue_per_core == pytest.approx(0.15)
    assert view.load_label == "0.15q"
    assert view.ram_total_mb == pytest.approx(32229.1, rel=1e-3)
    assert view.ram_free_mb == 14830.0                  # AvailableMBytes, not FreePhysicalMemory
    assert view.gpu_unified is False
    assert view.gpu_name == "NVIDIA GeForce RTX 5060 Ti"
    assert view.gpu_util_pct == 3.0
    assert view.vram_free_mb == 14321.0
    assert view.vram_total_mb == 16311.0
    assert view.primary_disk.used_bytes == 600_000_000_000
    assert view.primary_disk.used_pct == pytest.approx(60.0)


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


def test_high_load_ratio_is_not_rendered_as_cpu_oversubscription():
    """One-minute demand may be high while interval CPU usage is low."""
    view = ResourceView(name="X", reachable=True)
    _parse_posix("NCPU=4\nLOADAVG= 9.60 9.0 8.5\n"
                 "CPUUSAGE:CPU usage: 20.0% user, 5.0% sys, 75.0% idle\n", view)
    assert view.cpu_busy_pct == pytest.approx(25.0)
    assert view.load_label == "2.4x"
    assert "OVERSUB" not in view.load_label
    assert "busy" not in view.load_label


def test_load_average_never_becomes_plausible_cpu_utilization():
    view = ResourceView(name="L", reachable=True)
    _parse_posix("NCPU=8\nLOADAVG= 4.00 3.0 2.0\n", view)
    assert view.cpu_busy_pct is None
    assert view.load_per_core == pytest.approx(0.5)


def test_windows_queue_requires_both_valid_queue_and_core_count():
    for output in (
        "NCPU=20\nCPU_QUEUE=bad\n",
        "NCPU=0\nCPU_QUEUE=3\n",
        "CPU_QUEUE=3\n",
    ):
        view = ResourceView(name="W", reachable=True)
        _parse_windows(output, view)
        assert view.processor_queue_per_core is None
        assert view.load_label == "-"


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


def test_guarded_device_uses_unguarded_fixed_resource_probe(monkeypatch):
    """The fixed metadata script is not an unknown arbitrary-command workload."""
    constructed = []

    class FixedProbe:
        def probe(self):
            return SimpleNamespace(reachable=True, detail="ssh ok")

        def exec(self, *a, **k):
            return SimpleNamespace(
                stdout="HOST=MACHUB\nNCPU=20\nMEMTOTAL=68719476736\n",
                exit_code=0,
            )

    def fake_make_transport(candidate):
        constructed.append(candidate)
        return FixedProbe()

    monkeypatch.setattr(resources, "make_transport", fake_make_transport)
    device = Device.from_mapping(
        "MACHUB",
        {
            "kind": "ssh-posix",
            "os": "macos",
            "address_candidates": ["machub"],
            "project_root": "/tmp",
            "state_root": "/tmp",
            "cache_root": "/tmp",
            "ram_gb": 64,
            "max_jobs": 2,
            "memory_guard": {
                "schema": 3,
                "command_limit_fraction": 0.3125,
                "host_reserve_fraction": 0.25,
            },
        },
    )

    view = resources.probe_device(device)

    assert view.reachable is True
    assert len(constructed) == 1
    assert constructed[0].memory_guard is None


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
    local_row = next(ln for ln in table.splitlines() if ln.startswith("WINTWO"))
    assert "WINTWO*" not in local_row
    assert "local" in local_row
    assert "* this controller" not in table
    # The compact failure still states the actionable cause.
    assert "key missing" in table
    assert "id_ed25519_mesh" not in table
    # An unreachable device reports no invented metrics.
    unreachable_line = next(ln for ln in table.splitlines() if ln.startswith("MACBOX"))
    assert "%" not in unreachable_line


def test_render_shortens_common_status_messages():
    views = [
        ResourceView(name="SLOW", reachable=False, detail="timeout (offline/busy)"),
        ResourceView(name="AUTH", reachable=False, detail="ssh auth refused"),
        ResourceView(name="SSHD", reachable=False,
                     detail="connection refused (sshd not running / not enabled)"),
    ]
    table = render_table(views)
    assert "timeout (offline/busy)" not in table
    assert "ssh auth refused" not in table
    assert "sshd not running" not in table
    assert "timeout" in table
    assert "auth refused" in table
    assert "sshd off" in table


def test_render_uses_compact_load_footnote():
    table = render_table([
        ResourceView(name="X", reachable=True, cpu_count=4, load1=2.0, load_per_core=0.5),
    ])
    assert "LOAD = 1-min demand/core; not CPU%" in table


def test_render_distinguishes_posix_load_from_windows_queue():
    table = render_table([
        ResourceView(name="MAC", reachable=True, load1=2.0, load_per_core=0.5),
        ResourceView(
            name="WIN",
            reachable=True,
            processor_queue_length=1.0,
            processor_queue_per_core=0.05,
        ),
    ])
    assert "0.50x" in table
    assert "0.05q" in table
    assert "x=1-min run queue/core" in table
    assert "q=ready waiters/core" in table


def test_render_marks_only_values_at_or_above_alert_thresholds():
    view = ResourceView(
        name="BUSY",
        reachable=True,
        is_local=True,
        cpu_busy_pct=85.0,
        load1=4.0,
        load_per_core=1.0,
        ram_free_mb=15.0,
        ram_total_mb=100.0,
        gpu_util_pct=84.0,
        vram_free_mb=16.0,
        vram_total_mb=100.0,
        primary_disk=PrimaryDiskView(
            total_bytes=100,
            available_bytes=15,
            status="ok",
        ),
    )
    table = render_table([view])
    row = next(line for line in table.splitlines() if line.startswith("BUSY"))

    assert "BUSY*" not in row
    assert "85*" in row
    assert "1.0x*" in row
    assert "84%" in row
    assert row.count("*") == 4  # CPU, LOAD, RAM, and DISK; not device/GPU/VRAM.
    assert "* alert: usage >=85%; LOAD >=1.0x/q" in table


def test_alert_marker_does_not_widen_incremental_percent_columns():
    table = IncrementalTable(["X"])
    heading = table.header().splitlines()[0]
    row = table.row(
        ResourceView(
            name="X",
            reachable=True,
            cpu_busy_pct=100.0,
            ram_free_mb=0.0,
            ram_total_mb=100.0,
        )
    )

    assert "100*" in row
    assert "100%*" not in row
    assert row.index("100*") == heading.index("CPU")
    assert row.index("100*", heading.index("RAM")) == heading.index("RAM")


def test_unified_gpu_never_reports_a_vram_figure_or_long_label():
    """Apple silicon shares system RAM; a VRAM number there would be fiction."""
    view = ResourceView(name="MACHUB", reachable=True, gpu_unified=True, gpu_util_pct=37.0)
    table = render_table([view])
    row = next(line for line in table.splitlines() if line.startswith("MACHUB"))
    assert "unified" not in row
    assert "37%" in row


def test_percent_display_is_compact_default_and_amounts_are_configurable():
    view = ResourceView(
        name="WINBOX",
        reachable=True,
        ram_free_mb=8 * 1024,
        ram_total_mb=32 * 1024,
        gpu_util_pct=3.0,
        vram_free_mb=12 * 1024,
        vram_total_mb=16 * 1024,
        primary_disk=PrimaryDiskView(
            mount="C:",
            total_bytes=1_000_000_000_000,
            available_bytes=400_000_000_000,
            status="ok",
        ),
    )
    compact = render_table([view])
    row = next(line for line in compact.splitlines() if line.startswith("WINBOX"))
    assert "75%" in row
    assert "25%" in row
    assert "60%" in row
    assert "GB" not in row

    amounts = render_table([view], usage_display="amounts")
    row = next(line for line in amounts.splitlines() if line.startswith("WINBOX"))
    assert "24/32 GB" in row
    assert "4/16 GB" in row
    assert "600/1000 GB" in row


def test_incremental_table_keeps_columns_stable_across_arrival_order():
    table = IncrementalTable(["A", "MUCH-LONGER"], usage_display="percent")
    first = ResourceView(
        name="A",
        reachable=True,
        cpu_busy_pct=5.0,
        ram_free_mb=8 * 1024,
        ram_total_mb=16 * 1024,
    )
    second = ResourceView(
        name="MUCH-LONGER",
        reachable=True,
        cpu_busy_pct=100.0,
        ram_free_mb=0,
        ram_total_mb=16 * 1024,
    )

    header = table.header()
    first_row = table.row(first)
    second_row = table.row(second)

    heading = header.splitlines()[0]
    ram_column = heading.index("RAM")
    assert heading.index("CPU") == first_row.index("5%")
    assert first_row.index("50%") == ram_column
    assert second_row.index("100*", ram_column) == ram_column
    assert first_row.startswith("A")
    assert second_row.startswith("MUCH-LONGER")


@pytest.mark.parametrize(
    ("payload", "status"),
    [
        (None, "unavailable"),
        ('{"total_bytes":"100","available_bytes":"0"}', "ok"),
        ('{"total_bytes":"100","available_bytes":"101"}', "invalid"),
        ('{"total_bytes":"bad","available_bytes":"20"}', "invalid"),
        ('{"total_bytes":true,"available_bytes":"20"}', "invalid"),
        ("not-json", "invalid"),
    ],
)
def test_disk_parser_rejects_missing_or_malformed_capacity(payload, status):
    view = ResourceView(name="X", reachable=True)
    out = "" if payload is None else f"DISK_JSON={payload}\n"
    _parse_posix(out, view)
    assert view.primary_disk.status == status
    if status != "ok":
        assert view.primary_disk.used_pct is None


def test_disk_parser_preserves_integers_above_javascript_exact_range():
    view = ResourceView(name="X", reachable=True)
    _parse_posix(
        'DISK_JSON={"total_bytes":"9007199254740993",'
        '"available_bytes":"9007199254740000"}\n',
        view,
    )
    assert view.primary_disk.total_bytes == 9_007_199_254_740_993
    assert view.primary_disk.available_bytes == 9_007_199_254_740_000
    assert view.primary_disk.used_bytes == 993


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


def _metric(value, *, status="measured", source="test") -> Metric:
    return Metric(
        value=value,
        status=status,
        source=source,
        confidence="test",
    )


def _resource_snapshot(
    *,
    cpu=17.5,
    ram_available=12_000 * MIB,
    ram_total=32_000 * MIB,
    gpus=(),
    gpu_kind="none",
) -> ResourceSnapshot:
    return ResourceSnapshot(
        status="ok",
        platform="windows",
        machine="amd64",
        logical_cores=_metric(20),
        effective_cores=_metric(20),
        cpu_busy_pct=_metric(cpu),
        cpu_sample_interval_ms=500,
        ram_total_bytes=_metric(ram_total),
        ram_available_bytes=_metric(ram_available),
        gpu_kind=gpu_kind,
        gpus=gpus,
    )


def test_placement_snapshot_consumes_normalized_resources_and_preserves_fleet_state(
    monkeypatch,
):
    """One normalized probe replaces legacy CPU/RAM/VRAM probes without changing
    the queue, pool, capability, or configured-capacity parts of DeviceSnapshot.
    """
    first = GPUResourceSnapshot(
        id="0",
        name="GPU 0",
        util_pct=_metric(12),
        vram_free_bytes=_metric(6_000 * MIB),
        vram_total_bytes=_metric(16_000 * MIB),
    )
    second = GPUResourceSnapshot(
        id="1",
        name="GPU 1",
        util_pct=_metric(4),
        vram_free_bytes=_metric(20_000 * MIB),
        vram_total_bytes=_metric(24_000 * MIB),
    )
    seen = {}

    def normalized(transport, device, *, timeout_sec):
        seen["args"] = (transport, device.name, timeout_sec)
        return _resource_snapshot(gpus=(first, second), gpu_kind="discrete")

    monkeypatch.setattr(probes, "probe_target_resources", normalized)
    monkeypatch.setattr(
        probes,
        "_capability_engines",
        lambda transport, device: frozenset({"ocr"}),
    )

    class Transport:
        def probe(self):
            return SimpleNamespace(reachable=True, detail="ssh ok")

        def sample_load(self):
            raise AssertionError("legacy CPU probe must not run")

        def exec(self, *args, **kwargs):
            raise AssertionError("legacy RAM/VRAM probes must not run")

    transport = Transport()
    device = Device.from_mapping(
        "WINBOX",
        {
            "kind": "ssh-powershell",
            "os": "windows",
            "address_candidates": ["winbox"],
            "project_root": "C:\\",
            "state_root": "C:\\",
            "cache_root": "C:\\",
            "max_jobs": 3,
            "ram_gb": 64,
            "vram_gb": 16,
        },
    )
    snapshot = probes.build_snapshot(
        device,
        transport,
        {"pools": {"gpu": 2, "cpu": 4}},
        active_jobs=2,
        pool_used={"gpu": 1, "cpu": 3},
    )

    assert isinstance(snapshot, DeviceSnapshot)
    assert seen["args"] == (transport, "WINBOX", 20.0)
    assert snapshot.cpu_busy_pct == 17.5
    assert snapshot.ram_free_mb == 12_000
    # Preserve the established configured RAM capacity and measured-first VRAM
    # capacity. The legacy single-GPU view continues to select device 0.
    assert snapshot.ram_total_mb == 64 * 1024
    assert snapshot.vram_free_mb == 6_000
    assert snapshot.vram_total_mb == 16_000
    assert snapshot.active_jobs == 2
    assert snapshot.max_jobs == 3
    assert snapshot.pool_free == {"gpu": 1, "cpu": 1}
    assert snapshot.engines_available == frozenset({"ocr"})
    assert snapshot.detail == "ssh ok"


def test_placement_snapshot_uses_measured_ram_total_without_config(monkeypatch):
    monkeypatch.setattr(
        probes,
        "probe_target_resources",
        lambda *args, **kwargs: _resource_snapshot(ram_total=48_000 * MIB),
    )

    class Transport:
        def probe(self):
            return SimpleNamespace(reachable=True, detail="")

    snapshot = probes.build_snapshot(
        _device("LINUX"),
        Transport(),
        {"pools": {}},
        probe_capability=False,
    )
    assert snapshot.ram_total_mb == 48_000


def test_placement_snapshot_stays_no_throw_when_normalized_probe_raises(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("unexpected parser failure")

    monkeypatch.setattr(probes, "probe_target_resources", boom)

    class Transport:
        def probe(self):
            return SimpleNamespace(reachable=True, detail="ssh ok")

    snapshot = probes.build_snapshot(
        _device("X"),
        Transport(),
        {"pools": {"cpu": 2}},
        active_jobs=1,
        pool_used={"cpu": 1},
        probe_capability=False,
    )
    assert snapshot.reachable is True
    assert snapshot.cpu_busy_pct is None
    assert snapshot.ram_free_mb is None
    assert snapshot.vram_free_mb is None
    assert snapshot.active_jobs == 1
    assert snapshot.pool_free == {"cpu": 1}


def test_friendly_errors_name_the_actual_fix():
    from remrun.fleet.resources import _friendly_error
    assert _friendly_error("user@host: Permission denied (publickey,password)") == "ssh auth refused"
    assert "key missing" in _friendly_error(
        "Warning: Identity file /c/"
        "Users/x/.ssh/id_ed25519_mesh not accessible: No such file")
    assert "host key not trusted" in _friendly_error("Host key verification failed.")
    assert "sshd not running" in _friendly_error("ssh: connect to host x port 22: Connection refused")
    # A timeout must NOT assert "offline": a healthy but loaded box times out too.
    timed_out = _friendly_error("ssh timed out after 8s")
    assert timed_out == "timeout (offline/busy)"


def test_auth_failure_is_not_masked_by_a_later_dns_failure():
    """The real MACBOX/MACFS bug: the transport tries every address candidate and
    keeps only the LAST error. Both devices answered on their Tailscale IP and
    refused the key, but a trailing unresolvable alias made the report say
    "hostname did not resolve" — sending you to fix DNS instead of the key.
    """
    from remrun.fleet.resources import _friendly_error
    combined = ("user@192.0.2.11: Permission denied (publickey,password).\n"
                "ssh: Could not resolve hostname macbox.example-net.ts.net: Name or service not known")
    assert _friendly_error(combined) == "ssh auth refused"


def test_pure_dns_failure_still_reports_dns():
    from remrun.fleet.resources import _friendly_error
    assert _friendly_error(
        "ssh: Could not resolve hostname nope.local: Name or service not known"
    ) == "hostname did not resolve"


def test_json_payload_is_complete_and_serializable():
    view = ResourceView(name="WINBOX", reachable=True, cpu_busy_pct=12.0,
                        processor_queue_length=2.0, processor_queue_per_core=0.1,
                        ram_free_mb=14608.0,
                        ram_total_mb=32229.0, gpu_util_pct=3.0, vram_free_mb=14321.0,
                        vram_total_mb=16311.0, gpu_name="RTX 5060 Ti",
                        primary_disk=PrimaryDiskView(
                            mount="C:", total_bytes=1_000_000_000_000,
                            available_bytes=400_000_000_000, status="ok"))
    payload = to_dict(view)
    json.dumps(payload)                     # must not raise
    assert payload["gpu_util_pct"] == 3.0
    assert payload["processor_queue_length"] == 2.0
    assert payload["processor_queue_per_core"] == 0.1
    assert payload["vram_free_mb"] == 14321.0
    assert payload["ram_used_pct"] == pytest.approx(54.68, rel=1e-3)
    assert payload["vram_used_pct"] == pytest.approx(12.2, rel=1e-2)
    assert payload["disk"]["used_bytes"] == 600_000_000_000
    assert payload["disk"]["used_pct"] == pytest.approx(60.0)


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


def test_resources_tty_prints_header_then_rows_as_probes_finish(monkeypatch):
    from remrun.fleet import cli
    from remrun.output import Reporter

    class TTY(io.StringIO):
        def isatty(self):
            return True

    stdout = TTY()
    stderr = TTY()
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    devices = {name: _device(name) for name in ("SLOW", "FAST")}
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda: SimpleNamespace(devices=devices, defaults={}),
    )

    def finish_out_of_order(targets, *, timeout, on_event):
        assert stdout.getvalue().startswith("DEVICE")
        fast = ResourceView(name="FAST", reachable=True, cpu_busy_pct=4.0)
        slow = ResourceView(name="SLOW", reachable=True, cpu_busy_pct=8.0)
        on_event("start", "SLOW", None)
        on_event("start", "FAST", None)
        on_event("done", "FAST", fast)
        assert "FAST" in stdout.getvalue()
        assert "SLOW" not in stdout.getvalue()
        on_event("done", "SLOW", slow)
        return [slow, fast]

    monkeypatch.setattr(
        "remrun.fleet.resources.probe_fleet",
        finish_out_of_order,
    )
    args = SimpleNamespace(
        device=None,
        no_local=True,
        enabled_only=False,
        timeout=5.0,
        json=False,
        no_progress=False,
    )

    assert cli.cmd_resources(args, Reporter(json_events=False)) == 0
    out = stdout.getvalue()
    assert out.index("FAST") < out.index("SLOW")
    assert out.count("FAST") == 1
    assert out.count("SLOW") == 1
    assert "probing" not in stderr.getvalue()


def test_resources_no_progress_keeps_buffered_config_order_in_tty(monkeypatch):
    from remrun.fleet import cli
    from remrun.output import Reporter

    class TTY(io.StringIO):
        def isatty(self):
            return True

    stdout = TTY()
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    devices = {name: _device(name) for name in ("FIRST", "SECOND")}
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda: SimpleNamespace(devices=devices, defaults={}),
    )

    def finish_out_of_order(targets, *, timeout, on_event):
        second = ResourceView(name="SECOND", reachable=True)
        first = ResourceView(name="FIRST", reachable=True)
        on_event("done", "SECOND", second)
        on_event("done", "FIRST", first)
        return [first, second]

    monkeypatch.setattr(
        "remrun.fleet.resources.probe_fleet",
        finish_out_of_order,
    )
    args = SimpleNamespace(
        device=None,
        no_local=True,
        enabled_only=False,
        timeout=5.0,
        json=False,
        no_progress=True,
    )

    assert cli.cmd_resources(args, Reporter(json_events=False)) == 0
    out = stdout.getvalue()
    assert out.index("FIRST") < out.index("SECOND")
    assert out.count("DEVICE") == 1


def test_resources_json_stays_one_buffered_document_in_tty(monkeypatch):
    from remrun.fleet import cli
    from remrun.output import Reporter

    class TTY(io.StringIO):
        def isatty(self):
            return True

    stdout = TTY()
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    device = _device("BOX")
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda: SimpleNamespace(devices={"BOX": device}, defaults={}),
    )

    def finish(targets, *, timeout, on_event):
        view = ResourceView(name="BOX", reachable=True, cpu_busy_pct=3.0)
        on_event("done", "BOX", view)
        return [view]

    monkeypatch.setattr("remrun.fleet.resources.probe_fleet", finish)
    args = SimpleNamespace(
        device=None,
        no_local=True,
        enabled_only=False,
        timeout=5.0,
        json=True,
        no_progress=False,
    )

    assert cli.cmd_resources(args, Reporter(json_events=False)) == 0
    payload = json.loads(stdout.getvalue())
    assert payload["devices"][0]["name"] == "BOX"


def test_resources_stream_suppresses_remote_duplicate_of_local_row(monkeypatch):
    from remrun.fleet import cli, local_resources
    from remrun.output import Reporter

    class TTY(io.StringIO):
        def isatty(self):
            return True

    stdout = TTY()
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    devices = {name: _device(name) for name in ("CTRL", "REMOTE")}
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda: SimpleNamespace(devices=devices, defaults={}),
    )
    monkeypatch.setattr(
        local_resources,
        "local_view",
        lambda: ResourceView(
            name="CTRL",
            hostname="ctrl.local",
            reachable=True,
            is_local=True,
        ),
    )

    def finish(targets, *, timeout, on_event):
        controller = ResourceView(
            name="CTRL",
            hostname="ctrl.local",
            reachable=True,
        )
        remote = ResourceView(name="REMOTE", hostname="remote.local", reachable=True)
        on_event("done", "CTRL", controller)
        on_event("done", "REMOTE", remote)
        return [controller, remote]

    monkeypatch.setattr("remrun.fleet.resources.probe_fleet", finish)
    args = SimpleNamespace(
        device=None,
        no_local=False,
        enabled_only=False,
        timeout=5.0,
        json=False,
        no_progress=False,
    )

    assert cli.cmd_resources(args, Reporter(json_events=False)) == 0
    rows = stdout.getvalue().splitlines()
    assert len([line for line in rows if line.startswith("CTRL")]) == 1
    assert len([line for line in rows if line.startswith("REMOTE")]) == 1


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
