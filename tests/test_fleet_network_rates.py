from __future__ import annotations

import subprocess
import threading
import time

from remrun.fleet import resources as resources_module
from remrun.fleet.resources import (
    _POSIX_SCRIPT,
    _WINDOWS_SCRIPT,
    ResourceView,
    _parse_network,
    _parse_posix,
    _parse_windows,
    probe_fleet,
)
from remrun.fleet.resources_render import render_table, to_dict
from remrun.models import Device


def test_posix_probe_calculates_rounded_rate_on_live_script_path(tmp_path) -> None:
    """Run the embedded probe with controlled route/counter command outputs."""
    counter_state = tmp_path / "counter-state"
    # This test exercises the network sample, not live controller hardware.
    # Define the route/counter commands in the same shell as the production
    # section, avoiding startup latency from newly created temporary binaries.
    command_stubs = r'''
counter_state=${COUNTER_STATE:?}
uname() { printf '%s\n' Darwin; }
route() { printf '%s\n' 'interface: fake0'; }
netstat() {
  if [ -f "$counter_state" ]; then
    printf '%s\n' 2 > "$counter_state"
    rx=13562500; tx=3000000
  else
    printf '%s\n' 1 > "$counter_state"
    rx=1000000; tx=2000000
  fi
  printf '%s\n' 'Name Mtu Network Address Ipkts Ierrs Ibytes Opkts Oerrs Obytes Coll'
  printf 'fake0 1500 <Link#9> aa 1 0 %s 2 0 %s 0\n' "$rx" "$tx"
}
'''
    env = {
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
        "COUNTER_STATE": str(counter_state),
    }
    # The network section is the production script path under test.  Extract
    # precisely that section so a slow or wedged host probe (for example,
    # macOS osascript disk metadata) cannot make this focused test flaky.
    network_start = "# Passive network usage estimate on the interface selected by the default"
    network_end = "vm_stat 2>/dev/null | sed 's/^/VMSTAT:/'"
    start = _POSIX_SCRIPT.find(network_start)
    end = _POSIX_SCRIPT.find(network_end, start + len(network_start))
    assert start >= 0
    assert end >= 0
    network_script = _POSIX_SCRIPT[start:end]
    assert network_script.startswith(network_start)
    assert network_end not in network_script
    result = subprocess.run(
        ["/bin/bash", "-c", f"{command_stubs}{network_script}exit 0\n"],
        capture_output=True,
        text=True,
        env=env,
        timeout=5,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    output = result.stdout
    assert "NETWORK_INTERFACE=fake0" in output
    assert "NETWORK_STATUS=measured" in output
    # RX delta 12,562,500 bytes/s = 100.5 Mbps -> 101; TX delta 1,000,000
    # bytes/s = 8 Mbps. This exercises the production awk calculation.
    assert "NETWORK_DOWNLOAD_MBPS=101" in output, output
    assert "NETWORK_UPLOAD_MBPS=8" in output, output


def test_posix_network_parse_exposes_interface_and_up_down_rates() -> None:
    view = ResourceView(name="linux", reachable=True, os="linux")
    _parse_posix(
        "NETWORK_INTERFACE=eth0\n"
        "NETWORK_STATUS=measured\n"
        "NETWORK_UPLOAD_MBPS=13\n"
        "NETWORK_DOWNLOAD_MBPS=42\n",
        view,
    )

    assert view.network_interface == "eth0"
    assert view.network_status == "measured"
    assert view.network_upload_mbps == 13
    assert view.network_download_mbps == 42
    assert to_dict(view)["network"] == {
        "interface": "eth0",
        "status": "measured",
        "detail": "",
        "upload_mbps": 13,
        "download_mbps": 42,
    }
    assert "13/42" in render_table([view])


def test_windows_network_parse_uses_same_wire_shape() -> None:
    view = ResourceView(name="windows", reachable=True, os="windows")
    _parse_windows(
        "NETWORK_INTERFACE=Wi-Fi\n"
        "NETWORK_STATUS=measured\n"
        "NETWORK_UPLOAD_MBPS=2\n"
        "NETWORK_DOWNLOAD_MBPS=7\n",
        view,
    )
    assert view.network_interface == "Wi-Fi"
    assert view.network_upload_mbps == 2
    assert view.network_download_mbps == 7


def test_network_reset_and_missing_counters_never_become_zero() -> None:
    reset = ResourceView(name="reset", reachable=True, os="linux")
    _parse_network(
        {
            "NETWORK_INTERFACE": "eth0",
            "NETWORK_STATUS": "counter_reset",
            "NETWORK_UPLOAD_MBPS": "0",
            "NETWORK_DOWNLOAD_MBPS": "0",
        },
        reset,
    )
    assert reset.network_status == "counter_reset"
    assert reset.network_upload_mbps is None
    assert reset.network_download_mbps is None

    missing = ResourceView(name="missing", reachable=True, os="linux")
    _parse_network(
        {
            "NETWORK_INTERFACE": "wlan0",
            "NETWORK_STATUS": "unavailable",
        },
        missing,
    )
    assert missing.network_status == "unavailable"
    assert missing.network_upload_mbps is None
    assert missing.network_download_mbps is None


def test_network_measured_without_both_rates_is_malformed() -> None:
    view = ResourceView(name="bad", reachable=True, os="linux")
    _parse_network(
        {
            "NETWORK_INTERFACE": "eth0",
            "NETWORK_STATUS": "measured",
            "NETWORK_UPLOAD_MBPS": "5",
        },
        view,
    )
    assert view.network_status == "malformed"
    assert view.network_upload_mbps is None
    assert view.network_download_mbps is None


def test_probe_scripts_select_default_route_and_use_passive_counters() -> None:
    assert "ip route show default" in _POSIX_SCRIPT
    assert "/proc/net/route" in _POSIX_SCRIPT
    assert "/sys/class/net/$network_interface/statistics/rx_bytes" in _POSIX_SCRIPT
    assert "route -n get default" in _POSIX_SCRIPT
    assert "Get-NetIPConfiguration" in _WINDOWS_SCRIPT
    assert "Get-NetAdapterStatistics" in _WINDOWS_SCRIPT
    assert "Start-Sleep -Milliseconds 1000" in _WINDOWS_SCRIPT
    assert "$rx2 -lt $rx1 -or $tx2 -lt $tx1" in _WINDOWS_SCRIPT
    assert "[MidpointRounding]::AwayFromZero" in _WINDOWS_SCRIPT
    assert "NETWORK_STATUS=counter_reset" in _WINDOWS_SCRIPT


def test_fleet_probe_workers_remain_parallel_during_per_device_sampling(monkeypatch) -> None:
    active = 0
    maximum = 0
    lock = threading.Lock()

    def fake_probe(device, **_kwargs):  # noqa: ANN001
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return ResourceView(name=device.name, reachable=True, os="linux")

    monkeypatch.setattr(resources_module, "probe_device", fake_probe)
    devices = [
        Device.from_mapping(name, {"os": "linux", "kind": "ssh-posix"})
        for name in ("one", "two")
    ]
    views = probe_fleet(devices, max_workers=2)
    assert [view.name for view in views] == ["one", "two"]
    assert maximum == 2
