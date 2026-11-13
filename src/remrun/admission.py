"""Controller-local admission ledger: which request may start heavy work on a device next.

Several agents share one fleet. Without a queue, one project can keep refilling a device
while another project's larger job waits forever. This ledger queues requests per device
and grants one only when both the ledger and a fresh probe say it fits.

One SQLite file under the controller state root holds every request. Write transactions
are serialized (``BEGIN IMMEDIATE``) and never span a probe: callers gather
``DeviceFacts`` first, then one transaction records the request and promotes the queue.

Waiting requests on a device are ordered by:

1. class: protected (already bypassed once), then device-bound, then critical, then normal;
2. the root project's last grant on this device, least recently served first;
3. enqueue sequence.

Promotion grants the head while it fits and stops at the first request that does not.
Nothing behind a blocked head is granted, so new small jobs cannot starve it.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import RemrunConfig, admission_settings
from .job_observation import local_device
from .models import Device
from .remote.runner import RESOURCE_KEY_RE, filesystem_probe
from .resource_envelope import (
    MIB,
    Metric,
    MissingResourcePolicy,
    ResourceEnvelopeError,
    ResourcePolicy,
    offer_cpu,
    offer_ram,
    parse_device_resource_policy,
)
from .resource_probe import ResourceSnapshot, probe_target_resources, unavailable_snapshot
from .state import default_state_root
from .transport import BaseTransport, TransportError, make_transport

SCHEMA_VERSION = 1
REQUEST_SCHEMA = "remrun.admission-request"
# A waiter replays its acquire this often, or three times per wait TTL if that is shorter.
WAIT_POLL_SECONDS = 10.0
# A coordinator answers one `remrun admit` call, including its own probe, within this.
REMOTE_TIMEOUT_SECONDS = 120.0
# Set on calls forwarded to the coordinator, which must answer them from its own ledger.
FORWARDED_ENV = "REMRUN_ADMISSION_FORWARDED"
_CLASSES = ("protected", "device_bound", "critical", "normal")
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_MAX_TEXT = 200

_SCHEMA = (
    """CREATE TABLE requests (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT NOT NULL UNIQUE,
        digest TEXT NOT NULL,
        device TEXT NOT NULL,
        root_project TEXT NOT NULL,
        logical_thread TEXT NOT NULL,
        owner_thread TEXT NOT NULL,
        label TEXT NOT NULL,
        cpu_cores INTEGER NOT NULL,
        ram_mib INTEGER NOT NULL,
        keys_json TEXT NOT NULL,
        device_bound INTEGER NOT NULL,
        critical INTEGER NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('waiting','granted','released','cancelled')),
        bypassed INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL,
        heartbeat_at REAL NOT NULL,
        granted_at REAL,
        ended_at REAL,
        end_reason TEXT
    )""",
    "CREATE INDEX requests_device_state ON requests(device, state)",
    # The root project's most recent grant on each device, as a ledger-wide sequence.
    """CREATE TABLE service (
        device TEXT NOT NULL,
        root_project TEXT NOT NULL,
        last_grant_seq INTEGER NOT NULL,
        PRIMARY KEY (device, root_project)
    )""",
)


class AdmissionError(ValueError):
    """An invalid request, an unknown id, or a transition the request's state forbids."""


class AdmissionConflict(AdmissionError):
    """A request id was reused with different request content."""


class AdmissionUnavailable(AdmissionError):
    """The coordinator that owns the ledger could not be reached or did not answer."""


class AdmissionTimeout(AdmissionError):
    """No grant within the wait; the request was withdrawn."""

    def __init__(self, document: dict[str, Any]) -> None:
        super().__init__(f"admission request {document['request_id']} was not granted in time")
        self.document = document


@dataclass(frozen=True)
class DeviceFacts:
    """One device's capacity, observed before (never inside) a ledger transaction.

    ``budget_*`` is what the device's resource_policy offers at zero load: the ceiling for
    the sum of every grant on the device. ``offer_*`` is the same policy applied to a fresh
    probe. ``None`` is unknown, and unknown capacity is never treated as free.
    """

    device: str
    observed_at: float
    budget_cpu_cores: int | None
    budget_ram_mib: int | None
    offer_cpu_cores: int | None
    offer_ram_mib: int | None
    detail: str = ""

    @property
    def known(self) -> bool:
        return None not in (
            self.budget_cpu_cores, self.budget_ram_mib, self.offer_cpu_cores, self.offer_ram_mib,
        )

    def document(self) -> dict[str, Any]:
        return {
            "device": self.device,
            "known": self.known,
            "observed_at": _iso(self.observed_at),
            "budget": {"cpu_cores": self.budget_cpu_cores, "ram_mib": self.budget_ram_mib},
            "offer": {"cpu_cores": self.offer_cpu_cores, "ram_mib": self.offer_ram_mib},
            "detail": self.detail,
        }


def facts_from_snapshot(
    device: str, snapshot: ResourceSnapshot, policy: ResourcePolicy, *, observed_at: float,
) -> DeviceFacts:
    """Apply the device policy to one probe, exactly as a workload's launch envelope does.

    The budget is the offer the same functions give at zero load, so the ledger and the
    live check read the reserve and the cap the same way. Only measured values count:
    the static fallback a policy may allow for envelopes is never used here.
    """
    idle = Metric(0, "measured", "admission-zero-load", "hypothetical")
    try:
        offers = (
            offer_cpu(snapshot.effective_cores, idle, policy),
            offer_ram(snapshot.ram_total_bytes, snapshot.ram_total_bytes, policy),
            offer_cpu(snapshot.effective_cores, snapshot.cpu_busy_pct, policy),
            offer_ram(snapshot.ram_total_bytes, snapshot.ram_available_bytes, policy),
        )
    except ResourceEnvelopeError as exc:
        detail = f"probe out of range: {exc}"
        return DeviceFacts(device, observed_at, None, None, None, None, detail)
    reasons = sorted({offer.reason for offer in offers if offer.status == "unavailable"})
    if reasons:
        detail = "; ".join(reasons) + (f" ({snapshot.detail})" if snapshot.detail else "")
        return DeviceFacts(device, observed_at, None, None, None, None, detail)
    budget_cpu, budget_ram, live_cpu, live_ram = (offer.value for offer in offers)
    return DeviceFacts(
        device, observed_at, budget_cpu, budget_ram // MIB, live_cpu, live_ram // MIB,
    )


def probe_device_facts(device: Device, *, clock: Callable[[], float] = time.time) -> DeviceFacts:
    """Probe one device now. A malformed resource_policy raises; a failed probe is unknown."""
    policy = parse_device_resource_policy(device.resource_policy)
    observed_at = clock()
    if isinstance(policy, MissingResourcePolicy):
        snapshot = unavailable_snapshot(
            "unavailable", "device resource_policy is missing; live probe was not attempted",
        )
    else:
        # A fixed, bounded probe program rather than caller argv: like `fleet resources`,
        # keep it outside the arbitrary-command memory guard handshake.
        transport = make_transport(replace(device, memory_guard=None))
        snapshot = probe_target_resources(transport, device, timeout_sec=policy.probe_timeout_sec)
    return facts_from_snapshot(device.name, snapshot, policy, observed_at=observed_at)


@dataclass(frozen=True)
class AdmissionRequest:
    """What one caller wants on one device. Everything but ``request_id`` is its digest."""

    request_id: str
    device: str
    root_project: str
    owner_thread: str
    logical_thread: str = ""
    label: str = ""
    cpu_cores: int = 0
    ram_mib: int = 0
    keys: tuple[str, ...] = ()
    device_bound: bool = False
    critical: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.request_id, str) or not _REQUEST_ID_RE.fullmatch(self.request_id):
            raise AdmissionError(
                "request id must be 1-128 letters, digits, '.', '_', ':' or '-', "
                "starting with a letter or digit"
            )
        for name in ("device", "root_project", "owner_thread"):
            _text(getattr(self, name), name, required=True)
        _text(self.logical_thread, "logical_thread", required=False)
        _text(self.label, "label", required=False)
        if not self.logical_thread:
            object.__setattr__(self, "logical_thread", self.owner_thread)
        for name in ("cpu_cores", "ram_mib"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise AdmissionError(f"{name} must be a non-negative integer")
        keys = tuple(self.keys)
        for key in keys:
            if not isinstance(key, str) or not RESOURCE_KEY_RE.fullmatch(key):
                raise AdmissionError(f"resource key is invalid: {key!r}")
        if len(set(keys)) != len(keys):
            raise AdmissionError("duplicate resource key")
        object.__setattr__(self, "keys", tuple(sorted(keys)))
        for name in ("device_bound", "critical"):
            if not isinstance(getattr(self, name), bool):
                raise AdmissionError(f"{name} must be a boolean")

    @property
    def digest(self) -> str:
        content = {
            "device": self.device, "root_project": self.root_project,
            "logical_thread": self.logical_thread, "owner_thread": self.owner_thread,
            "label": self.label, "cpu_cores": self.cpu_cores, "ram_mib": self.ram_mib,
            "keys": list(self.keys), "device_bound": self.device_bound,
            "critical": self.critical,
        }
        packed = json.dumps(content, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(packed).hexdigest()


class AdmissionLedger:
    """The controller's admission ledger. One instance owns one SQLite connection."""

    def __init__(
        self,
        state_root: Path,
        *,
        settle_seconds: float,
        wait_ttl_seconds: float,
        clock: Callable[[], float] = time.time,
    ) -> None:
        # Every symlink on the way, the database file's own included, is resolved before
        # the filesystem check, and SQLite opens exactly the path that was checked.
        self.path = Path(os.path.realpath(
            Path(state_root) / "admission" / f"ledger-v{SCHEMA_VERSION}.sqlite3"
        ))
        self.settle_seconds = settle_seconds
        self.wait_ttl_seconds = wait_ttl_seconds
        self._clock = clock
        self._db = _connect(self.path)

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> AdmissionLedger:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def acquire(self, request: AdmissionRequest, facts: DeviceFacts) -> dict[str, Any]:
        """Record the request, or replay it exactly, then promote its device's queue.

        A replay of a waiting request refreshes its heartbeat and keeps its place.
        """
        if facts.device != request.device:
            raise AdmissionError(
                f"facts are for {facts.device!r}, request is for {request.device!r}"
            )
        with self._immediate() as now:
            row = self._find(request.request_id)
            if row is None:
                self._insert(request, now)
            elif row["digest"] != request.digest:
                raise _conflict(request)
            elif row["state"] == "waiting":
                self._db.execute(
                    "UPDATE requests SET heartbeat_at=? WHERE seq=?", (now, row["seq"])
                )
            promotion = self._promote(request.device, facts, now)
            document = self._document(self._get(request.request_id))
        return {**document, "replayed": row is not None, "promotion": promotion}

    def withdraw(self, request: AdmissionRequest) -> dict[str, Any]:
        """End the caller's own request, whatever became of its acquire: cancel it if
        waiting, release it if granted (its work never started), and if no acquire has
        landed yet, record it as cancelled so a late one replays as cancelled. Another
        request under the same id is a conflict and is never touched.
        """
        with self._immediate() as now:
            row = self._find(request.request_id)
            if row is None:
                self._insert(request, now, ended="withdrawn")
            elif row["digest"] != request.digest:
                raise _conflict(request)
            elif row["state"] in {"waiting", "granted"}:
                self._db.execute(
                    "UPDATE requests SET state=?, ended_at=?, end_reason='withdrawn' "
                    "WHERE seq=?",
                    ("cancelled" if row["state"] == "waiting" else "released", now, row["seq"]),
                )
            return self._document(self._get(request.request_id))

    def promote(self, device: str, facts: DeviceFacts) -> dict[str, Any]:
        """Grant whatever now fits at the head of one device's queue."""
        with self._immediate() as now:
            return self._promote(device, facts, now)

    def release(self, request_id: str) -> dict[str, Any]:
        """End a grant. Releasing an already released request returns it unchanged."""
        return self._end(request_id, "granted", "released")

    def cancel(self, request_id: str) -> dict[str, Any]:
        """Withdraw a waiting request. Cancelling it again returns it unchanged."""
        return self._end(request_id, "waiting", "cancelled")

    def status(self, request_id: str) -> dict[str, Any]:
        with self._immediate():
            return self._document(self._get(request_id))

    def list_requests(self, device: str | None = None) -> list[dict[str, Any]]:
        """Active requests: per device, grants in grant order, then the queue in order."""
        with self._immediate():
            names = [row["device"] for row in self._db.execute(
                "SELECT DISTINCT device FROM requests WHERE state IN ('waiting','granted') "
                "ORDER BY device"
            )]
            documents = []
            for name in [name for name in names if device is None or name == device]:
                order = self._order(name)
                documents += [self._document(row, order) for row in self._granted(name)]
                documents += [self._document(row, order) for row in order]
            return documents

    @contextmanager
    def _immediate(self) -> Iterator[float]:
        """One serialized write transaction, yielding its clock reading."""
        self._db.execute("BEGIN IMMEDIATE")
        try:
            now = self._clock()
            # A waiting request holds nothing, so dropping a silent one frees no resource
            # and loses only intent. A grant never expires: only release ends it.
            self._db.execute(
                "UPDATE requests SET state='cancelled', end_reason='heartbeat_expired', "
                "ended_at=? WHERE state='waiting' AND heartbeat_at < ?",
                (now, now - self.wait_ttl_seconds),
            )
            yield now
            self._db.execute("COMMIT")
        except BaseException:
            try:
                self._db.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    def _end(self, request_id: str, from_state: str, to_state: str) -> dict[str, Any]:
        with self._immediate() as now:
            row = self._get(request_id)
            if row["state"] == from_state:
                self._db.execute(
                    "UPDATE requests SET state=?, ended_at=?, end_reason=? WHERE seq=?",
                    (to_state, now, to_state, row["seq"]),
                )
            elif row["state"] != to_state:
                raise AdmissionError(
                    f"request {request_id!r} is {row['state']}; only a {from_state} request "
                    f"can be {to_state}"
                )
            return self._document(self._get(request_id))

    def _insert(self, request: AdmissionRequest, now: float, *, ended: str | None = None) -> None:
        self._db.execute(
            "INSERT INTO requests (request_id,digest,device,root_project,logical_thread,"
            "owner_thread,label,cpu_cores,ram_mib,keys_json,device_bound,critical,state,"
            "created_at,heartbeat_at,ended_at,end_reason) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                request.request_id, request.digest, request.device, request.root_project,
                request.logical_thread, request.owner_thread, request.label,
                request.cpu_cores, request.ram_mib, json.dumps(list(request.keys)),
                int(request.device_bound), int(request.critical),
                "cancelled" if ended else "waiting", now, now, now if ended else None, ended,
            ),
        )

    def _find(self, request_id: str) -> sqlite3.Row | None:
        return self._db.execute(
            "SELECT * FROM requests WHERE request_id=?", (request_id,)
        ).fetchone()

    def _get(self, request_id: str) -> sqlite3.Row:
        row = self._find(request_id)
        if row is None:
            raise AdmissionError(f"unknown admission request {request_id!r}")
        return row

    def _granted(self, device: str) -> list[sqlite3.Row]:
        return self._db.execute(
            "SELECT * FROM requests WHERE device=? AND state='granted' ORDER BY granted_at, seq",
            (device,),
        ).fetchall()

    def _service(self, device: str) -> dict[str, int]:
        return {
            row["root_project"]: row["last_grant_seq"]
            for row in self._db.execute(
                "SELECT root_project, last_grant_seq FROM service WHERE device=?", (device,)
            )
        }

    def _order(self, device: str) -> list[sqlite3.Row]:
        service = self._service(device)
        rows = self._db.execute(
            "SELECT * FROM requests WHERE device=? AND state='waiting'", (device,)
        ).fetchall()
        return sorted(rows, key=lambda row: (_rank(row), *_age(row, service)))

    def _promote(self, device: str, facts: DeviceFacts, now: float) -> dict[str, Any]:
        if facts.device != device:
            raise AdmissionError(f"facts are for {facts.device!r}, not {device!r}")
        if facts.known:
            # A request larger than the whole budget could never be granted, and at the
            # head of the queue it would block the device forever.
            self._db.execute(
                "UPDATE requests SET state='cancelled', end_reason='exceeds_device_budget', "
                "ended_at=? WHERE device=? AND state='waiting' AND (cpu_cores>? OR ram_mib>?)",
                (now, device, facts.budget_cpu_cores, facts.budget_ram_mib),
            )
        granted: list[str] = []
        blocked = None
        while order := self._order(device):
            head = order[0]
            if facts.known:
                reason, detail = self._misfit(head, self._granted(device), facts)
            else:
                reason, detail = "facts_unknown", facts.detail
            if reason:
                blocked = {"request_id": head["request_id"], "reason": reason, "detail": detail}
                break
            self._grant(head, order[1:], device, now)
            granted.append(head["request_id"])
        return {"facts": facts.document(), "granted": granted, "blocked": blocked}

    def _misfit(
        self, head: sqlite3.Row, granted: list[sqlite3.Row], facts: DeviceFacts,
    ) -> tuple[str | None, str]:
        held = {key for row in granted for key in json.loads(row["keys_json"])}
        clash = sorted(held.intersection(json.loads(head["keys_json"])))
        if clash:
            return "key_held", ", ".join(clash)
        # Telemetry may not show a recent grant's load yet, so the live offer covers only
        # what remains after every grant made since shortly before the probe.
        pending = [row for row in granted
                   if row["granted_at"] > facts.observed_at - self.settle_seconds]
        checks = (
            ("ledger_cpu", "cores", granted, facts.budget_cpu_cores, "cpu_cores"),
            ("ledger_ram", "MiB", granted, facts.budget_ram_mib, "ram_mib"),
            ("live_cpu", "cores", pending, facts.offer_cpu_cores, "cpu_cores"),
            ("live_ram", "MiB", pending, facts.offer_ram_mib, "ram_mib"),
        )
        for reason, unit, holders, limit, column in checks:
            used = sum(row[column] for row in holders)
            # A dimension the request does not use cannot block it.
            if head[column] and used + head[column] > limit:
                what = "granted" if reason.startswith("ledger") else "pending"
                return reason, f"{used} {what} + {head[column]} requested > {limit} {unit}"
        return None, ""

    def _grant(
        self, head: sqlite3.Row, behind: list[sqlite3.Row], device: str, now: float,
    ) -> None:
        service = self._service(device)
        head_age = _age(head, service)
        for row in behind:
            # Bypassed only when class alone put the head first: an older or less recently
            # served waiter would otherwise have been ahead of it.
            if _rank(row) > _rank(head) and _age(row, service) < head_age:
                self._db.execute(
                    "UPDATE requests SET bypassed=bypassed+1 WHERE seq=?", (row["seq"],)
                )
        self._db.execute(
            "UPDATE requests SET state='granted', granted_at=? WHERE seq=?", (now, head["seq"])
        )
        self._db.execute(
            "INSERT INTO service (device, root_project, last_grant_seq) "
            "SELECT ?, ?, COALESCE(MAX(last_grant_seq), 0) + 1 FROM service "
            "WHERE true ON CONFLICT (device, root_project) "
            "DO UPDATE SET last_grant_seq=excluded.last_grant_seq",
            (device, head["root_project"]),
        )

    def _document(
        self, row: sqlite3.Row, order: list[sqlite3.Row] | None = None,
    ) -> dict[str, Any]:
        position, ahead = None, []
        if row["state"] == "waiting":
            ids = [item["request_id"] for item in (order or self._order(row["device"]))]
            index = ids.index(row["request_id"])
            position, ahead = index + 1, ids[:index]
        return {
            "schema": REQUEST_SCHEMA,
            "version": 1,
            "request_id": row["request_id"],
            "state": row["state"],
            "device": row["device"],
            "root_project": row["root_project"],
            "logical_thread": row["logical_thread"],
            "owner_thread": row["owner_thread"],
            "label": row["label"],
            "cpu_cores": row["cpu_cores"],
            "ram_mib": row["ram_mib"],
            "keys": json.loads(row["keys_json"]),
            "device_bound": bool(row["device_bound"]),
            "critical": bool(row["critical"]),
            "class": _CLASSES[_rank(row)],
            "bypassed": row["bypassed"],
            "enqueued_seq": row["seq"],
            "created_at": _iso(row["created_at"]),
            "heartbeat_at": _iso(row["heartbeat_at"]),
            "granted_at": _iso(row["granted_at"]),
            "ended_at": _iso(row["ended_at"]),
            "end_reason": row["end_reason"],
            "position": position,
            "ahead": ahead,
        }


class AdmissionClient:
    """This deployment's one admission authority, as seen from this controller.

    With no coordinator, or when this controller is the coordinator, calls use the local
    ledger and probe the device here. Otherwise every call runs ``remrun admit ... --json``
    on the coordinator over its transport. An unreachable coordinator raises
    ``AdmissionUnavailable``; nothing ever falls back to a local ledger.
    """

    def __init__(self, config: RemrunConfig, coordinator: str | None) -> None:
        self.config = config
        self.coordinator = coordinator
        self._settings = admission_settings(config)
        self.poll_seconds = min(WAIT_POLL_SECONDS, self._settings["wait_ttl_seconds"] / 3)
        here = local_device(config)
        here_name = here.name if here is not None else None
        expected = os.environ.get(FORWARDED_ENV)
        if expected is not None and not (expected and expected == coordinator == here_name):
            # A forwarded call is answered only by the coordinator its sender meant: any
            # other ledger would be a second authority, and forwarding again could loop.
            raise AdmissionError(
                f"refusing an admission call forwarded to {expected or 'an unnamed coordinator'}"
                f": this controller is {here_name or 'not a configured device'} and its "
                f"coordinator is {coordinator or 'unset'}"
            )
        self._remote: Device | None = None
        self._transport: BaseTransport | None = None
        if coordinator is not None and here_name != coordinator:
            if coordinator not in config.devices:
                raise AdmissionError(f"admission coordinator {coordinator!r} is not configured")
            self._remote = config.devices[coordinator]

    @classmethod
    def for_config(cls, config: RemrunConfig) -> AdmissionClient:
        return cls(config, admission_settings(config)["coordinator"])

    @property
    def authority(self) -> str:
        """Where the ledger is: the coordinator's name, or this controller's own."""
        return self.coordinator or "this controller"

    def acquire(self, request: AdmissionRequest) -> dict[str, Any]:
        if self._remote is not None:
            return self._call("acquire", _request_argv(request), request.request_id)
        device = self.config.devices.get(request.device)
        if device is None:
            raise AdmissionError(f"unknown device {request.device!r}")
        facts = probe_device_facts(device)
        with self._ledger() as ledger:
            return ledger.acquire(request, facts)

    def withdraw(self, request: AdmissionRequest) -> dict[str, Any]:
        """End this caller's own request, even one whose acquire has not landed yet."""
        if self._remote is not None:
            return self._call("withdraw", _request_argv(request), request.request_id)
        with self._ledger() as ledger:
            return ledger.withdraw(request)

    def status(self, request_id: str) -> dict[str, Any]:
        return self._by_id("status", request_id)

    def release(self, request_id: str) -> dict[str, Any]:
        """End a grant. The queue is promoted by the next waiter's poll, not here."""
        return self._by_id("release", request_id)

    def cancel(self, request_id: str) -> dict[str, Any]:
        return self._by_id("cancel", request_id)

    def list_requests(self, device: str | None = None) -> list[dict[str, Any]]:
        if self._remote is not None:
            document = self._call("list", [f"--device={device}"] if device else [])
            return list(document["requests"])
        with self._ledger() as ledger:
            return ledger.list_requests(device)

    def _by_id(self, action: str, request_id: str) -> dict[str, Any]:
        if self._remote is not None:
            return self._call(action, [request_id], request_id)
        with self._ledger() as ledger:
            return getattr(ledger, action)(request_id)

    def _ledger(self) -> AdmissionLedger:
        return AdmissionLedger(
            default_state_root(),
            settle_seconds=self._settings["settle_seconds"],
            wait_ttl_seconds=self._settings["wait_ttl_seconds"],
        )

    def _call(self, action: str, argv: list[str], request_id: str | None = None) -> dict[str, Any]:
        device = self._remote
        assert device is not None
        if self._transport is None:
            transport = make_transport(replace(device, memory_guard=None))
            try:
                probe = transport.probe()
            except Exception as exc:  # noqa: BLE001 - any failure leaves no authority
                raise AdmissionUnavailable(
                    f"admission coordinator {device.name} is unreachable: {exc}"
                ) from exc
            if not probe.reachable:
                raise AdmissionUnavailable(
                    f"admission coordinator {device.name} is unreachable: {probe.detail}"
                )
            self._transport = transport
        try:
            result = self._transport.exec_control(
                ["remrun", "admit", action, *argv, "--json"], "~",
                env={FORWARDED_ENV: device.name}, timeout=REMOTE_TIMEOUT_SECONDS,
                path_prepend=list(device.path) or None,
            )
        except (TransportError, OSError) as exc:
            raise AdmissionUnavailable(
                f"admission coordinator {device.name} did not answer: {exc}"
            ) from exc
        # `admit` prints its document on 0, an acquire's on 75 (waiting) and on 1 (ended);
        # any other 1 is a refusal it explained on stderr.
        document = _last_json_object(result.stdout)
        printed = result.exit_code == 0 or (action == "acquire" and result.exit_code in (1, 75))
        if printed and document is not None:
            if not _valid_response(action, document, request_id):
                raise AdmissionUnavailable(
                    f"admission coordinator {device.name} answered `{action}` with a "
                    "malformed or mismatched document"
                )
            return document
        error = _last_json_object(result.stderr) or {}
        detail = str(error.get("message") or result.stderr.strip()[-300:]
                     or f"exit {result.exit_code}")
        if result.exit_code == 1 and error.get("message"):
            raise (AdmissionConflict if error.get("type") == "AdmissionConflict"
                   else AdmissionError)(f"admission coordinator {device.name}: {detail}")
        raise AdmissionUnavailable(
            f"admission coordinator {device.name} could not run `remrun admit` "
            f"(exit {result.exit_code}): {detail}"
        )


# What each `remrun admit` action may answer with: anything else is not that action's result.
_ANSWER_STATES = {
    "acquire": {"waiting", "granted", "released", "cancelled"},
    "status": {"waiting", "granted", "released", "cancelled"},
    "release": {"released"},
    "cancel": {"cancelled"},
    "withdraw": {"released", "cancelled"},
}


def _valid_response(action: str, document: dict[str, Any], request_id: str | None) -> bool:
    if action == "list":
        requests = document.get("requests")
        return (document.get("schema") == "remrun.admission-list"
                and document.get("version") == 1 and isinstance(requests, list)
                and all(isinstance(item, dict) and item.get("schema") == REQUEST_SCHEMA
                        for item in requests))
    return (document.get("schema") == REQUEST_SCHEMA and document.get("version") == 1
            and document.get("request_id") == request_id
            and document.get("state") in _ANSWER_STATES[action])


def _request_argv(request: AdmissionRequest) -> list[str]:
    return [
        f"--device={request.device}", f"--project={request.root_project}",
        f"--thread={request.owner_thread}", f"--logical-thread={request.logical_thread}",
        f"--label={request.label}", f"--request-id={request.request_id}",
        f"--cpu={request.cpu_cores}", f"--ram-gib={request.ram_mib / 1024!r}",
        *[f"--key={key}" for key in request.keys],
        *(["--device-bound"] if request.device_bound else []),
        *(["--critical"] if request.critical else []),
    ]


def admit_first(
    client: AdmissionClient,
    requests: list[AdmissionRequest],
    *,
    wait_seconds: float,
    report: Callable[[dict[str, Any]], None],
    unresolved: Callable[[list[str]], None],
    keep_waiting: bool = False,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Acquire each request in order and keep the first granted at once; if none is,
    keep the first still queued on a device whose capacity is known (else the first
    queued) and replay it, refreshing its heartbeat, until granted or ``wait_seconds``
    pass. ``report`` sees the kept request each time its place in the queue changes.

    Each request is recorded before its acquire is sent, so one whose reply was lost is
    still withdrawn by its exact identity, and a late acquire then lands cancelled. A
    request id that names someone else's request is never withdrawn. Every other request
    made here is withdrawn, as is every one on an error or interrupt; ``unresolved`` is
    told the ids whose withdrawal could not be confirmed.

    Returns the kept request's last document: granted, or ended. At the deadline it is
    returned still waiting when ``keep_waiting``; otherwise it is withdrawn and
    ``AdmissionTimeout`` is raised.
    """
    if not requests:
        raise AdmissionError("no admission request to make")
    made: list[AdmissionRequest] = []
    try:
        documents: list[dict[str, Any]] = []
        for request in requests:
            made.append(request)
            try:
                documents.append(client.acquire(request))
            except AdmissionConflict:
                made.pop()  # that id is someone else's request
                raise
            if documents[-1]["state"] == "granted":
                break
        queued = [doc for doc in documents if doc["state"] == "waiting"]
        known = [doc for doc in queued if doc.get("promotion", {}).get("facts", {}).get("known")]
        document = next(
            (doc for doc in documents if doc["state"] == "granted"),
            (known or queued or documents)[0],
        )
        kept = next(item for item in made if item.request_id == document["request_id"])
        _withdraw_all(client, [item for item in made if item is not kept], unresolved)
        made = [kept]
        deadline = clock() + wait_seconds
        seen = None
        while document["state"] == "waiting":
            remaining = deadline - clock()
            if remaining <= 0:
                if keep_waiting:
                    break
                made = []
                _withdraw_all(client, [kept], unresolved)
                raise AdmissionTimeout(document)
            place = (document["position"], document["ahead"],
                     (document.get("promotion", {}).get("blocked") or {}).get("reason"))
            if place != seen:
                report(document)
                seen = place
            _sleep(min(client.poll_seconds, remaining))
            document = client.acquire(kept)
        return document
    except BaseException:
        _withdraw_all(client, made, unresolved, quiet=True)
        raise


def _withdraw_all(
    client: AdmissionClient,
    requests: list[AdmissionRequest],
    unresolved: Callable[[list[str]], None],
    *,
    quiet: bool = False,
) -> None:
    """Withdraw each request; report the ones that could not be, then raise unless quiet."""
    failed: list[tuple[str, Exception]] = []
    for request in requests:
        try:
            client.withdraw(request)
        except Exception as exc:  # noqa: BLE001 - every request still gets its attempt
            failed.append((request.request_id, exc))
    if failed:
        unresolved([request_id for request_id, _exc in failed])
        if not quiet:
            raise failed[0][1]


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


def _last_json_object(text: str) -> dict[str, Any] | None:
    for line in reversed((text or "").splitlines()):
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict):
            return value
    return None


def _conflict(request: AdmissionRequest) -> AdmissionConflict:
    return AdmissionConflict(
        f"request id {request.request_id!r} already names a different request"
    )


def _rank(row: sqlite3.Row) -> int:
    if row["bypassed"] >= 1:
        return 0
    if row["device_bound"]:
        return 1
    return 2 if row["critical"] else 3


def _age(row: sqlite3.Row, service: dict[str, int]) -> tuple[int, int]:
    """Order within a class: a never-served root project (0) first, then the oldest request."""
    return service.get(row["root_project"], 0), row["seq"]


def _text(value: object, name: str, *, required: bool) -> None:
    if not isinstance(value, str) or (required and not value.strip()):
        raise AdmissionError(f"{name} must be a non-empty string" if required
                             else f"{name} must be a string")
    if len(value) > _MAX_TEXT or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise AdmissionError(f"{name} must be at most {_MAX_TEXT} printable characters")


def _iso(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _connect(path: Path) -> sqlite3.Connection:
    probe = filesystem_probe(str(path))
    if not probe["local"]:
        raise AdmissionError(
            f"admission ledger must be on a local filesystem; got {probe['kind']}: {probe['path']}"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("PRAGMA journal_mode = DELETE").fetchone()
        if not row or str(row[0]).lower() != "delete":
            raise AdmissionError("admission ledger requires SQLite rollback journal mode")
        conn.execute("PRAGMA synchronous = EXTRA")
        conn.execute("PRAGMA busy_timeout = 10000")
        conn.execute("BEGIN IMMEDIATE")
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version == 0:
            for statement in _SCHEMA:
                conn.execute(statement)
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        elif version != SCHEMA_VERSION:
            raise AdmissionError(
                f"admission ledger schema {version} is not the supported {SCHEMA_VERSION}"
            )
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        conn.close()
        raise
    return conn
