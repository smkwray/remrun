"""`remrun fleet mesh`: edge classification, matrix shape, and asymmetry reporting.

The command-construction tests encode two real bugs found against the live mesh:
a POSIX-only probe command that a Windows hop could not run, and a shlex-quoted
inner command that the remote shell read as one filename. Both turned working
edges into a false "offline".
"""
from __future__ import annotations

import subprocess

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
    build_matrix,
)
from remrun.fleet.mesh_render import _asymmetries, _unreachable_by_anyone, render_matrix
from remrun.models import Device
from remrun.transport import ProbeResult, SSHPosixTransport, SSHPowerShellTransport


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

    hop = _device("HOPBOX", tailscale_ip="192.0.2.12", user="user")
    target = _device("WINBOX", tailscale_ip="192.0.2.15", user="user", kind="ssh-powershell",
                     os="windows")
    class FakeTransport:
        def probe(self):
            return ProbeResult(True, "192.0.2.12", "ssh ok", "windows")

    monkeypatch.setattr(mesh, "_run", fake_run)
    monkeypatch.setattr(mesh, "make_transport", lambda _device: FakeTransport())
    mesh.probe_edge_via(hop, target)

    command = captured["command"]
    # The inner ssh must appear as its own argv token, never wrapped in quotes.
    assert "ssh" in command[command.index("user@192.0.2.12") + 1:]
    assert not any(token.startswith("'") for token in command)
    assert command[-1] == _PROBE_COMMAND


def test_explicit_ip_is_tried_before_the_bare_alias(monkeypatch):
    """The IP always names the real device; a bare alias is at the mercy of DNS.

    Some routers resolve every unknown local alias to their own gateway address.
    Probing the alias first then checks the router and reports healthy devices as
    "hostname did not resolve" / "host key not trusted".
    """
    tried = []

    def fake_run(argv, timeout=None, **_kwargs):
        spec = argv[-2]
        tried.append(spec)
        return subprocess.CompletedProcess(
            argv, 0, b"remrun-ok\nLinux\n/srv/user\n", b""
        )

    target = _device("POSIXBOX2", tailscale_ip="192.0.2.14", user="user")
    runner = SSHPosixTransport(target)
    monkeypatch.setattr(runner, "_run", fake_run)
    monkeypatch.setattr(mesh, "make_transport", lambda _device: runner)
    assert mesh.probe_edge_direct(target).status == OK
    assert tried[0] == "user@192.0.2.14"      # IP first, never the alias


def test_alias_used_as_fallback_when_ip_is_refused(monkeypatch):
    """The alias can still win: the caller's ssh config may bind a specific
    IdentityFile to that Host with `IdentitiesOnly yes`, so the alias offers a
    key the raw IP never presents. For example, `ssh macbox` can succeed while
    `ssh user@192.0.2.11` is refused.
    """
    tried = []

    def fake_run(argv, timeout=None, **_kwargs):
        spec = argv[-2]
        tried.append(spec)
        if spec == "user@macbox":
            return subprocess.CompletedProcess(
                argv, 0, b"remrun-ok\nLinux\n/srv/user\n", b""
            )
        return subprocess.CompletedProcess(
            argv, 255, b"", b"Permission denied (publickey)"
        )

    target = _device("MACBOX", tailscale_ip="192.0.2.11", user="user")
    runner = SSHPosixTransport(target)
    monkeypatch.setattr(runner, "_run", fake_run)
    monkeypatch.setattr(mesh, "make_transport", lambda _device: runner)
    assert mesh.probe_edge_direct(target).status == OK
    assert tried == ["user@192.0.2.11", "user@macbox"]


@pytest.mark.parametrize("identity_file", ["~/.ssh/id_ed25519", "~/.ssh/id_ed25519_mesh"])
def test_mesh_probe_matches_runner_identity_and_case_sensitive_alias(
    monkeypatch, identity_file,
):
    """Mesh must use the runner's exact alias/options, including a non-default key.

    The live runner tries configured addresses in order and passes its configured
    SSH options to every attempt. A lowercased device name is not an equivalent
    alias: it can select a different Host block and therefore a different
    IdentityFile. This models the field failure without opening a network socket.
    """
    target = _device(
        "MIXBOX",
        address_candidates=["192.0.2.15", "MiXbOx"],
        user="runner",
        ssh_opts=["-o", "IdentitiesOnly=yes", "-o", f"IdentityFile={identity_file}"],
    )
    runner = SSHPosixTransport(target)
    expected_alias_argv = runner._ssh_base("MiXbOx", connect_timeout=8)
    runner_argvs = {
        tuple(runner._ssh_base(address, connect_timeout=8))
        for address in target.all_addresses()
    }
    calls = []

    def fake_run(argv, timeout=None, **_kwargs):
        calls.append(argv)
        if tuple(argv[:-1]) in runner_argvs:
            code = 0 if argv[:-1] == expected_alias_argv else 255
            return subprocess.CompletedProcess(
                argv,
                code,
                b"remrun-ok\nLinux\n/srv/user\n" if code == 0 else b"",
                b"" if code == 0 else b"Permission denied (publickey)",
            )
        # The old mesh path uses a different two-value subprocess seam.
        return 255, "Permission denied (publickey)"

    monkeypatch.setattr(mesh, "_run", fake_run)
    monkeypatch.setattr(runner, "_run", fake_run)
    # The repaired mesh path delegates to the same transport instance used by
    # remrun. `raising=False` keeps this test a genuine red test on the old tree,
    # where mesh has no transport-resolution seam yet.
    monkeypatch.setattr(mesh, "make_transport", lambda _device: runner, raising=False)

    edge = mesh.probe_edge_direct(target)

    assert edge.status == OK
    probe_argv = calls[-1][:-1]
    runner_argv = runner._ssh_base("MiXbOx", connect_timeout=8)
    assert probe_argv == runner_argv
    assert probe_argv[-1] == "runner@MiXbOx"
    assert f"IdentityFile={identity_file}" in probe_argv


def test_edge_fails_only_when_every_spelling_fails(monkeypatch):
    target = _device("POSIXFS", tailscale_ip="192.0.2.13", user="user")
    runner = SSHPosixTransport(target)
    monkeypatch.setattr(
        runner, "_run",
        lambda argv, timeout=None, **_kwargs: subprocess.CompletedProcess(
            argv, 255, b"", b"user@x: Permission denied (publickey)"
        ),
    )
    monkeypatch.setattr(mesh, "make_transport", lambda _device: runner)
    edge = mesh.probe_edge_direct(target)
    assert edge.status == AUTH


@pytest.mark.parametrize(
    ("transport_cls", "kind", "os_name", "shell"),
    [
        (SSHPosixTransport, "ssh-posix", "macos", "bash"),
        (SSHPowerShellTransport, "ssh-powershell", "windows", "pwsh"),
    ],
)
def test_auth_failure_survives_trailing_dns_attempt_for_mesh_users(
    monkeypatch, transport_cls, kind, os_name, shell,
):
    """A late DNS miss must not hide an earlier key-installation signal.

    The mesh glyph and physical-access advisory are what operators act on:
    ``!`` says the host answered and needs a key, while ``?``/``.`` sends work
    away as though the capacity were unavailable. Both SSH transports must
    preserve the actionable result across all configured address spellings.
    """
    target = _device(
        "FARBOX",
        kind=kind,
        os=os_name,
        shell=shell,
        user="runner",
        address_candidates=["203.0.113.20", "FARBOX.local"],
    )
    runner = transport_cls(target)

    def fake_run(argv, timeout=None, **_kwargs):
        del timeout
        address = argv[-2]
        if address.endswith("203.0.113.20"):
            return subprocess.CompletedProcess(
                argv, 255, b"", b"Permission denied (publickey)"
            )
        return subprocess.CompletedProcess(
            argv, 255, b"", b"ssh: Could not resolve hostname FARBOX.local: Name or service not known"
        )

    monkeypatch.setattr(runner, "_run", fake_run)
    monkeypatch.setattr(mesh, "make_transport", lambda _device: runner)

    matrix = build_matrix([target], "NEARBOX", hops=False)
    edge = matrix["edges"]["NEARBOX"]["FARBOX"]
    table = render_matrix(matrix, "NEARBOX")

    assert edge.status == AUTH
    assert "FARBOX" in table and "!" in table
    assert "No node can ssh into these (a key must be installed with physical/console access):" in table


def test_hop_probe_preserves_auth_failure_before_trailing_dns(monkeypatch):
    """A hop must retain the actionable failure from its inner SSH attempts."""
    hop = _device("HOPBOX", user="runner")
    target = _device(
        "FARBOX",
        user="runner",
        address_candidates=["203.0.113.20", "FARBOX.local"],
    )

    class FakeHopTransport:
        def probe(self):
            return ProbeResult(True, "hopbox", "ssh ok", "macos")

    def fake_run(argv, timeout):
        del timeout
        if argv[-2].endswith("203.0.113.20"):
            return 255, "Permission denied (publickey)"
        return 255, "ssh: Could not resolve hostname FARBOX.local: Name or service not known"

    monkeypatch.setattr(mesh, "make_transport", lambda _device: FakeHopTransport())
    monkeypatch.setattr(mesh, "_run", fake_run)

    edge = mesh.probe_edge_via(hop, target)

    assert edge.status == AUTH


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
    devices = [_device("HOPBOX"), _device("MACBOX")]

    def fake_direct(target, connect_timeout=8, timeout=25.0):
        return Edge("", target.name, OK if target.name == "HOPBOX" else AUTH)

    def fake_via(hop, target, connect_timeout=8, timeout=40.0):
        return Edge(hop.name, target.name, OK)

    monkeypatch.setattr(mesh, "probe_edge_direct", fake_direct)
    monkeypatch.setattr(mesh, "probe_edge_via", fake_via)
    matrix = build_matrix(devices, "CTRLBOX")

    # MACBOX could not be reached, so it was never used as a hop: its row is absent.
    assert "MACBOX" not in matrix["edges"] or not matrix["edges"].get("MACBOX")
    assert matrix["edges"]["HOPBOX"]["MACBOX"].status == OK
    table = render_matrix(matrix, "CTRLBOX")
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
    """A representative asymmetric case: outbound works, inbound times out."""
    matrix = {
        "rows": ["CTRLBOX", "HOPBOX"],
        "edges": {
            "CTRLBOX": {"HOPBOX": Edge("CTRLBOX", "HOPBOX", OK)},
            "HOPBOX": {"CTRLBOX": Edge("HOPBOX", "CTRLBOX", OFFLINE, "timed out (device offline)")},
        },
    }
    lines = _asymmetries(matrix)
    assert len(lines) == 1
    assert "CTRLBOX -> HOPBOX works, but HOPBOX -> CTRLBOX does not" in lines[0]


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
    matrix = {"rows": ["CTRLBOX", "HOPBOX"],
              "edges": {"CTRLBOX": {"CTRLBOX": Edge("CTRLBOX", "CTRLBOX", SELF),
                                 "HOPBOX": Edge("CTRLBOX", "HOPBOX", OK)}}}
    table = render_matrix(matrix, "CTRLBOX")
    assert "CTRLBOX*" in table and "* this controller (CTRLBOX)" in table
    assert "Rows = FROM" in table
