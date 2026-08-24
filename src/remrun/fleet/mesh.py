"""Who can SSH into whom: the fleet's directed reachability matrix.

`fleet resources` answers "how loaded is each box"; this answers the question
underneath it — "which boxes can talk to each other at all". SSH trust is
directed and frequently asymmetric (A holds B's key, B does not hold A's), so a
single "online/offline" flag per device hides the failure that actually bites:
a device everyone can see but nobody can log into.

Edges are measured, never inferred:
  * rows the controller can reach are tested directly;
  * other rows are tested by hopping through a device the controller CAN reach,
    which is the only way to observe an edge that does not start here.
A device that cannot be used as a hop yields `unknown` cells, never `fail` —
not knowing is different from knowing it is broken.
"""
from __future__ import annotations

import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from ..models import Device
from ..transport import _ssh_base_argv, make_transport
from .resources import _NO_WINDOW_FLAG, _friendly_error

# Cell states, most to least severe. `unknown` means untested, not failed.
OK = "ok"
AUTH = "auth"
REFUSED = "refused"
OFFLINE = "offline"
DNS = "dns"
HOSTKEY = "hostkey"
UNKNOWN = "unknown"
SELF = "self"

# The order is the user-facing diagnostic priority: retain the first state in
# this sequence when several address spellings fail.
_STATUS_PRIORITY = {
    status: priority
    for priority, status in enumerate(
        (OK, AUTH, REFUSED, OFFLINE, DNS, HOSTKEY, UNKNOWN, SELF)
    )
}

# One-character glyphs for the matrix; the legend spells them out.
GLYPH = {OK: "Y", AUTH: "!", REFUSED: "x", OFFLINE: ".", DNS: "?",
         HOSTKEY: "k", UNKNOWN: " ", SELF: "-"}

_STATUS_FROM_MESSAGE = (
    ("auth refused", AUTH),
    ("key missing", AUTH),
    ("host key not trusted", HOSTKEY),
    ("sshd not running", REFUSED),
    ("no answer within timeout", OFFLINE),
    ("offline", OFFLINE),
    ("no route", OFFLINE),
    ("did not resolve", DNS),
)


@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    status: str
    detail: str = ""


def _classify(stderr: str) -> tuple[str, str]:
    message = _friendly_error(stderr)
    low = message.lower()
    for needle, status in _STATUS_FROM_MESSAGE:
        if needle in low:
            return status, message
    return OFFLINE, message


def _prefer_failure(
    current: tuple[str, str] | None, detail: str,
) -> tuple[str, str]:
    """Keep the most informative classified failure, preserving raw detail."""
    status, _ = _classify(detail)
    candidate = (status, detail)
    if current is None or _STATUS_PRIORITY[status] < _STATUS_PRIORITY[current[0]]:
        return candidate
    return current


def _login_attempts(target: Device) -> list[str]:
    """Use the same exact address spellings and order as the live transport."""
    return [
        f"{target.user}@{address}" if target.user else address
        for address in target.all_addresses()
    ]


# The remote command must succeed on BOTH a POSIX shell and PowerShell, since a
# Windows sshd runs it through pwsh/cmd. `true` is a POSIX builtin that
# PowerShell does not have, so it fails there with a shell error even though the
# SSH login itself worked — which is exactly the false "offline" this avoids.
# `exit 0` is valid in sh, PowerShell, and cmd alike.
_PROBE_COMMAND = "exit 0"


def _run(command: list[str], timeout: float) -> tuple[int, str]:
    try:
        proc = subprocess.run(command, capture_output=True, timeout=timeout,
                              check=False, creationflags=_NO_WINDOW_FLAG)
    except subprocess.TimeoutExpired:
        return 255, "ssh timed out"
    except OSError as exc:
        return 255, str(exc)
    text = (proc.stderr or b"").decode("utf-8", "replace").strip()
    if proc.returncode == 0:
        return 0, (proc.stdout or b"").decode("utf-8", "replace").strip()
    return proc.returncode, text or (proc.stdout or b"").decode("utf-8", "replace").strip()


def probe_edge_direct(target: Device, connect_timeout: int = 8,
                      timeout: float = 25.0) -> Edge:
    """Controller -> target, measured from this machine.

    Delegate to the transport's own preflight. This is the same address, user,
    identity-option, and SSH-config resolution path used by remrun itself.
    """
    del connect_timeout, timeout
    try:
        result = make_transport(target).probe()
    except Exception as exc:  # noqa: BLE001 - mesh must render a measured failure
        status, detail = _classify(str(exc))
        return Edge("", target.name, status, detail)
    if result.reachable:
        return Edge("", target.name, OK)
    status, detail = _classify(result.detail)
    return Edge("", target.name, status, detail)


def probe_edge_via(hop: Device, target: Device, connect_timeout: int = 8,
                   timeout: float = 40.0) -> Edge:
    """hop -> target, measured by running ssh ON the hop.

    The inner command is quoted for the hop's own shell. Windows hops run
    OpenSSH from PowerShell/cmd, where POSIX quoting would be wrong, so the
    inner line is kept free of characters that need escaping either way.
    """
    # ssh already concatenates its trailing args into ONE command string for the
    # remote shell, so the inner line is passed as plain tokens. Wrapping it in
    # shlex.quote() instead makes the whole line a single word, and the remote
    # shell reports "command not found: ssh -o BatchMode=yes ..." — a false
    # 'offline' for an edge that works.
    # Resolve the outer hop through the same runner preflight used by remrun,
    # then build the inner command from the shared SSH argv seam. In particular,
    # preserve each configured candidate's case and IdentityFile options.
    try:
        hop_probe = make_transport(hop).probe()
    except Exception as exc:  # noqa: BLE001 - mesh must render a measured failure
        status, detail = _classify(str(exc))
        return Edge(hop.name, target.name, status, detail)
    if not hop_probe.reachable or not hop_probe.address:
        status, detail = _classify(hop_probe.detail)
        return Edge(hop.name, target.name, status, detail)

    best_failure = None
    outer_prefix = _ssh_base_argv(
        hop, hop_probe.address, connect_timeout=connect_timeout,
    )
    for spec in _login_attempts(target):
        address = spec.split("@", 1)[-1]
        inner = [
            *_ssh_base_argv(
                target, address, connect_timeout=connect_timeout,
                expand_user_paths=False,
            ),
            _PROBE_COMMAND,
        ]
        outer = [*outer_prefix, *inner]
        code, text = _run(outer, timeout)
        if code == 0:
            return Edge(hop.name, target.name, OK)
        best_failure = _prefer_failure(best_failure, text)
    status, detail = _classify(best_failure[1] if best_failure else "")
    return Edge(hop.name, target.name, status, detail)


def build_matrix(devices: list[Device], controller: str, *, hops: bool = True,
                 max_workers: int = 8, connect_timeout: int = 8) -> dict:
    """Directed reachability for every ordered pair.

    Returns {"rows": [...], "edges": {source: {target: Edge}}}. The controller
    row is always measured; other rows only when `hops` is on and that device is
    itself reachable — an unreachable hop cannot report on anything.
    """
    names = [d.name for d in devices]
    by_name = {d.name: d for d in devices}
    edges: dict[str, dict[str, Edge]] = {controller: {}}

    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(devices) or 1))) as pool:
        direct = list(pool.map(
            lambda d: probe_edge_direct(d, connect_timeout=connect_timeout), devices))
    for edge, device in zip(direct, devices):
        edges[controller][device.name] = Edge(controller, device.name,
                                              edge.status, edge.detail)

    if hops:
        # Only a device we can reach can be asked about its own outbound edges.
        usable = [d for d in devices
                  if edges[controller].get(d.name) is not None
                  and edges[controller][d.name].status == OK]
        pairs = [(hop, target) for hop in usable for target in devices
                 if hop.name != target.name]
        if pairs:
            with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(pairs)))) as pool:
                results = list(pool.map(
                    lambda pair: probe_edge_via(pair[0], pair[1],
                                                connect_timeout=connect_timeout),
                    pairs))
            for edge in results:
                edges.setdefault(edge.source, {})[edge.target] = edge
        for hop in usable:
            edges.setdefault(hop.name, {})[hop.name] = Edge(hop.name, hop.name, SELF)

    edges[controller][controller] = Edge(controller, controller, SELF)
    rows = [controller] + [n for n in names if n != controller]
    return {"rows": rows, "columns": rows, "edges": edges, "devices": by_name}
