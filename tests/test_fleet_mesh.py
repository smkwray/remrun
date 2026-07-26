"""`remrun fleet mesh`: edge classification, matrix shape, and asymmetry reporting.

The command-construction tests encode two real bugs found against the live mesh:
a POSIX-only probe command that a Windows hop could not run, and a shlex-quoted
inner command that the remote shell read as one filename. Both turned working
edges into a false "offline".
"""
from __future__ import annotations

import pytest

from remrun.fleet import mesh
from remrun.fleet.mesh import (
    _PROBE_COMMAND,
    AUTH,
    OFFLINE,
    OK,
    SELF,
    Edge,
    _classify,
    _target_spec,
    build_matrix,
)
from remrun.fleet.mesh_render import _asymmetries, _unreachable_by_anyone, render_matrix
from remrun.models import Device


def _device(name: str, **extra) -> Device:
    data = {"kind": "ssh-posix", "os": "macos", "address_candidates": [name.lower()],
            "project_root": "/tmp", "state_root": "/tmp", "cache_root": "/tmp"}
    data.update(extra)
    return Device.from_mapping(name, data)


def test_probe_command_runs_on_powershell_and_posix():
    """`true` is a POSIX builtin PowerShell lacks.

    A Windows hop ran it, the shell errored, and the edge was reported offline
    even though the SSH login itself had succeeded. `exit 0` is valid in sh,
    PowerShell and cmd alike.
    """
    assert _PROBE_COMMAND == "exit 0"
    assert "true" not in _PROBE_COMMAND


def test_hop_command_is_passed_as_tokens_not_one_quoted_word(monkeypatch):
    """shlex.quote'ing the inner ssh line made the remote shell treat the whole
    command as a single filename: `command not found: ssh -o BatchMode=yes ...`.
    """
    captured = {}

    def fake_run(command, timeout):
        captured["command"] = command
        return 0, ""

    monkeypatch.setattr(mesh, "_run", fake_run)
    hop = _device("MACHUB", tailscale_ip="192.0.2.12", user="user")
    target = _device("WINBOX", tailscale_ip="192.0.2.15", user="user", kind="ssh-powershell",
                     os="windows")
    mesh.probe_edge_via(hop, target)

    command = captured["command"]
    # The inner ssh must appear as its own argv token, never wrapped in quotes.
    assert "ssh" in command[command.index("user@192.0.2.12") + 1:]
    assert not any(token.startswith("'") for token in command)
    assert command[-1] == _PROBE_COMMAND


def test_explicit_ip_is_tried_before_the_bare_alias(monkeypatch):
    """The IP always names the real device; a bare alias is at the mercy of DNS.

    Measured on this network: `mactwo`, `macfs`, and `mactwo.local` ALL resolve to
    192.168.42.1 — the router, which answers every unknown name with itself.
    Probing the alias first sent the check at the router and reported healthy
    devices as "hostname did not resolve" / "host key not trusted".
    """
    tried = []

    def fake_run(command, timeout):
        spec = command[-2]
        tried.append(spec)
        return (0, "")

    monkeypatch.setattr(mesh, "_run", fake_run)
    target = _device("MACTWO", tailscale_ip="192.0.2.14", user="user")
    assert mesh.probe_edge_direct(target).status == OK
    assert tried[0] == "user@192.0.2.14"      # IP first, never the alias


def test_alias_used_as_fallback_when_ip_is_refused(monkeypatch):
    """The alias can still win: the caller's ssh config may bind a specific
    IdentityFile to that Host with `IdentitiesOnly yes`, so the alias offers a
    key the raw IP never presents. Measured on MACHUB: `ssh macbox` succeeded while
    `ssh user@192.0.2.11` was refused.
    """
    tried = []

    def fake_run(command, timeout):
        spec = command[-2]
        tried.append(spec)
        return (0, "") if spec == "macbox" else (255, "Permission denied (publickey)")

    monkeypatch.setattr(mesh, "_run", fake_run)
    target = _device("MACBOX", tailscale_ip="192.0.2.11", user="user")
    assert mesh.probe_edge_direct(target).status == OK
    assert tried == ["user@192.0.2.11", "macbox"]


def test_edge_fails_only_when_every_spelling_fails(monkeypatch):
    monkeypatch.setattr(mesh, "_run",
                        lambda c, t: (255, "user@x: Permission denied (publickey)"))
    target = _device("MACFS", tailscale_ip="192.0.2.13", user="user")
    edge = mesh.probe_edge_direct(target)
    assert edge.status == AUTH


def test_target_spec_prefers_tailnet_ip_and_attaches_user():
    """A hop does not share this controller's ~/.ssh/config, so a bare alias
    there may map to a different user or nothing at all."""
    device = _device("MACHUB", tailscale_ip="192.0.2.12", user="user")
    assert _target_spec(device) == "user@192.0.2.12"
    # No tailnet IP configured: fall back to the first candidate, still with user.
    assert _target_spec(_device("BOX", user="me")) == "me@box"
    # No user configured: bare address (ssh then uses the local user).
    assert _target_spec(_device("BOX")) == "box"


@pytest.mark.parametrize("stderr,expected", [
    ("user@1.2.3.4: Permission denied (publickey,password).", AUTH),
    ("ssh: connect to host 1.2.3.4 port 22: Connection refused", "refused"),
    ("ssh: connect to host 1.2.3.4 port 22: Operation timed out", OFFLINE),
    ("ssh: Could not resolve hostname nope: Name or service not known", "dns"),
    ("Host key verification failed.", "hostkey"),
])
def test_classify_maps_ssh_errors_to_states(stderr, expected):
    status, _ = _classify(stderr)
    assert status == expected


def test_matrix_leaves_unreachable_hop_rows_unknown_not_failed(monkeypatch):
    """Not knowing must never render as knowing it is broken.

    A device the controller cannot reach cannot be asked about ITS outbound
    edges; marking those cells as failures would invent evidence.
    """
    devices = [_device("MACHUB"), _device("MACBOX")]

    def fake_direct(target, connect_timeout=8, timeout=25.0):
        return Edge("", target.name, OK if target.name == "MACHUB" else AUTH)

    def fake_via(hop, target, connect_timeout=8, timeout=40.0):
        return Edge(hop.name, target.name, OK)

    monkeypatch.setattr(mesh, "probe_edge_direct", fake_direct)
    monkeypatch.setattr(mesh, "probe_edge_via", fake_via)
    matrix = build_matrix(devices, "WINTWO")

    # MACBOX could not be reached, so it was never used as a hop: its row is absent.
    assert "MACBOX" not in matrix["edges"] or not matrix["edges"].get("MACBOX")
    assert matrix["edges"]["MACHUB"]["MACBOX"].status == OK
    table = render_matrix(matrix, "WINTWO")
    assert "not tested" in table


def test_no_hops_flag_only_measures_controller_row(monkeypatch):
    monkeypatch.setattr(mesh, "probe_edge_direct",
                        lambda t, connect_timeout=8, timeout=25.0: Edge("", t.name, OK))
    called = []
    monkeypatch.setattr(mesh, "probe_edge_via",
                        lambda h, t, **k: called.append((h.name, t.name)))
    matrix = build_matrix([_device("A"), _device("B")], "CTRL", hops=False)
    assert called == []
    assert set(matrix["edges"]["CTRL"]) >= {"A", "B"}


def test_asymmetry_is_reported_with_direction():
    """The real WINTWO/MACHUB case: outbound works, inbound times out."""
    matrix = {
        "rows": ["WINTWO", "MACHUB"],
        "edges": {
            "WINTWO": {"MACHUB": Edge("WINTWO", "MACHUB", OK)},
            "MACHUB": {"WINTWO": Edge("MACHUB", "WINTWO", OFFLINE, "timed out (device offline)")},
        },
    }
    lines = _asymmetries(matrix)
    assert len(lines) == 1
    assert "WINTWO -> MACHUB works, but MACHUB -> WINTWO does not" in lines[0]


def test_asymmetry_needs_both_directions_measured():
    """An untested reverse edge is not evidence of asymmetry."""
    matrix = {"rows": ["A", "B"], "edges": {"A": {"B": Edge("A", "B", OK)}, "B": {}}}
    assert _asymmetries(matrix) == []


def test_unreachable_by_anyone_flags_only_auth_failures():
    """A box that ANSWERS and refuses the key is a trust problem needing console
    access. A box that is merely powered off is not."""
    matrix = {
        "rows": ["A", "MACBOX", "OFF"],
        "edges": {"A": {"MACBOX": Edge("A", "MACBOX", AUTH), "OFF": Edge("A", "OFF", OFFLINE)}},
    }
    assert _unreachable_by_anyone(matrix) == ["MACBOX"]


def test_render_marks_self_and_controller():
    matrix = {"rows": ["WINTWO", "MACHUB"],
              "edges": {"WINTWO": {"WINTWO": Edge("WINTWO", "WINTWO", SELF),
                                 "MACHUB": Edge("WINTWO", "MACHUB", OK)}}}
    table = render_matrix(matrix, "WINTWO")
    assert "WINTWO*" in table and "* this controller (WINTWO)" in table
    assert "Rows = FROM" in table
