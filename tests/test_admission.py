from __future__ import annotations

import json
import multiprocessing
import time
from dataclasses import replace
from pathlib import Path

import pytest

import remrun.admission as admission
import remrun.cli as cli
from remrun.admission import (
    AdmissionConflict,
    AdmissionError,
    AdmissionLedger,
    AdmissionRequest,
    DeviceFacts,
    admit_first,
    facts_from_snapshot,
)
from remrun.config import RemrunConfig, admission_settings, load_toml
from remrun.models import Device
from remrun.resource_context import build_resource_envelope
from remrun.resource_envelope import (
    Metric,
    MissingResourcePolicy,
    parse_device_resource_policy,
)
from remrun.resource_probe import ResourceSnapshot, unavailable_snapshot

GIB = 1024**3
SETTLE = 60.0
TTL = 300.0
POLICY = {
    "schema": 1,
    "mode": "unattended",
    "probe_timeout_sec": 5,
    "cpu_reserve_cores": 4,
    "cpu_max_fraction": 0.5,
    "ram_reserve_mib": 8192,
    "ram_max_fraction": 0.7,
    "gpu_busy_ceiling_pct": 100,
    "vram_reserve_mib": 0,
    "vram_max_fraction": 1.0,
    # Allowed, so the tests prove admission never takes the static fallback anyway.
    "allow_static_fallback": True,
}


class Clock:
    def __init__(self, now: float = 1_800_000_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def ledger(tmp_path: Path, clock: Clock):
    with AdmissionLedger(
        tmp_path / "state", settle_seconds=SETTLE, wait_ttl_seconds=TTL, clock=clock,
    ) as opened:
        yield opened


def facts(clock: Clock, *, cpu: int = 16, offer_cpu: int | None = None,
          device: str = "macbox") -> DeviceFacts:
    """A probe taken now: budget ``cpu`` cores and 64 GiB, live offer ``offer_cpu``."""
    offer = cpu if offer_cpu is None else offer_cpu
    return DeviceFacts(device, clock(), cpu, 65536, offer, 65536)


def request(request_id: str, project: str, *, cpu: int = 0, keys: tuple[str, ...] = (),
            device: str = "macbox", **flags) -> AdmissionRequest:
    return AdmissionRequest(
        request_id=request_id, device=device, root_project=project,
        owner_thread=f"{project}-thread", cpu_cores=cpu, keys=keys, **flags,
    )


def snapshot(*, cores: int = 20, busy: float = 0.0, total_gib: int = 64,
             available_gib: int = 64) -> ResourceSnapshot:
    def measured(value):
        return Metric(value, "measured", "resource-probe", "exact")

    return ResourceSnapshot(
        status="ok", platform="darwin", machine="arm64",
        logical_cores=measured(cores), effective_cores=measured(cores),
        cpu_busy_pct=measured(busy), cpu_sample_interval_ms=500,
        ram_total_bytes=measured(total_gib * GIB),
        ram_available_bytes=measured(available_gib * GIB), gpu_kind="none",
    )


def _race_worker(state_root: str, device_bound: bool, barrier, results) -> None:
    # Hold each grant open between the fit check's read and the grant's write.
    grant = AdmissionLedger._grant

    def slow_grant(self, *args, **kwargs):
        time.sleep(1.0)
        return grant(self, *args, **kwargs)

    AdmissionLedger._grant = slow_grant
    with AdmissionLedger(Path(state_root), settle_seconds=SETTLE, wait_ttl_seconds=TTL) as ledger:
        room_for_one = DeviceFacts("macbox", time.time(), 8, 16384, 8, 16384)
        barrier.wait(timeout=30)
        if device_bound:
            time.sleep(0.2)  # arrive while the other acquirer is inside its grant
        document = ledger.acquire(
            AdmissionRequest(request_id=f"job-{int(device_bound)}", device="macbox",
                             root_project=f"project-{int(device_bound)}",
                             owner_thread="thread", cpu_cores=6, device_bound=device_bound),
            room_for_one,
        )
    results.put(document["state"])


def test_two_processes_racing_for_room_for_one_get_exactly_one_grant(tmp_path: Path) -> None:
    # Agents acquire from separate processes against one ledger. The later, device-bound
    # request heads its own view of the queue; unless each promotion reads and writes in
    # one serialized transaction, it too reads an empty 8-core device, and both requests
    # are granted 6 cores.
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    results = context.Queue()
    processes = [
        context.Process(target=_race_worker,
                        args=(str(tmp_path / "state"), device_bound, barrier, results))
        for device_bound in (False, True)
    ]
    for process in processes:
        process.start()
    states = sorted(results.get(timeout=60) for _ in processes)
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0
    assert states == ["granted", "waiting"]


def test_exact_replay_returns_current_state_and_keeps_its_place(ledger, clock) -> None:
    # A caller whose acquire timed out retries with the same id. The retry must neither
    # create a second request nor send the first to the back of the queue.
    ledger.acquire(request("hold", "alpha", cpu=16), facts(clock))
    first = ledger.acquire(request("job", "beta", cpu=4), facts(clock))
    clock.now += 100
    replay = ledger.acquire(request("job", "beta", cpu=4), facts(clock))

    assert (first["state"], first["replayed"]) == ("waiting", False)
    assert (replay["state"], replay["replayed"]) == ("waiting", True)
    assert replay["enqueued_seq"] == first["enqueued_seq"]
    assert replay["heartbeat_at"] > first["heartbeat_at"]
    assert [item["request_id"] for item in ledger.list_requests()] == ["hold", "job"]

    ledger.release("hold")
    granted = ledger.acquire(request("job", "beta", cpu=4), facts(clock))
    clock.now += 100
    again = ledger.acquire(request("job", "beta", cpu=4), facts(clock))
    assert granted["state"] == again["state"] == "granted"
    assert again["granted_at"] == granted["granted_at"]


def test_reused_request_id_with_different_content_is_refused(ledger, clock) -> None:
    # Replaying a changed request under an old id would hand out a grant the caller
    # never asked for under that id.
    ledger.acquire(request("job", "alpha", cpu=2), facts(clock))
    with pytest.raises(AdmissionConflict):
        ledger.acquire(request("job", "alpha", cpu=8), facts(clock))
    assert ledger.status("job")["cpu_cores"] == 2
    assert len(ledger.list_requests()) == 1


def test_refilling_project_cannot_pass_a_larger_waiting_request(ledger, clock) -> None:
    # Project alpha holds most of a 16-core budget and keeps asking for one more core.
    # Each ask fits. If the queue backfilled past beta's 8-core request at its head,
    # alpha would refill the device forever and beta would never start.
    assert ledger.acquire(request("alpha-big", "alpha", cpu=12), facts(clock))["state"] == "granted"
    clock.now += 10
    small = ledger.acquire(request("alpha-small-0", "alpha", cpu=1), facts(clock))
    assert small["state"] == "granted"  # the same small ask fits while nobody waits
    clock.now += 10
    assert ledger.acquire(request("beta", "beta", cpu=8), facts(clock))["state"] == "waiting"

    for index in (1, 2, 3):
        clock.now += 10
        document = ledger.acquire(request(f"alpha-small-{index}", "alpha", cpu=1), facts(clock))
        assert document["state"] == "waiting"
        assert document["ahead"][0] == "beta"
        assert document["promotion"]["blocked"]["request_id"] == "beta"

    clock.now += 10
    ledger.release("alpha-big")
    promoted = ledger.promote("macbox", facts(clock))
    assert promoted["granted"] == ["beta", "alpha-small-1", "alpha-small-2", "alpha-small-3"]


def test_device_bound_request_jumps_an_older_waiter_only_once(ledger, clock) -> None:
    # Work that needs this device's unique hardware goes before an older CPU-only
    # waiter. The waiter is then protected, so a stream of such requests, from the same
    # project or a new one, cannot keep passing it.
    ledger.acquire(request("alpha-hold", "alpha", cpu=10), facts(clock))
    assert ledger.acquire(request("gamma-cpu", "gamma", cpu=8), facts(clock))["state"] == "waiting"
    first_gpu = ledger.acquire(
        request("beta-gpu-1", "beta", cpu=2, keys=("pool/gpu",), device_bound=True), facts(clock)
    )
    assert first_gpu["state"] == "granted"
    protected = ledger.status("gamma-cpu")
    assert (protected["bypassed"], protected["class"]) == (1, "protected")

    ledger.release("beta-gpu-1")
    for request_id, project in (("beta-gpu-2", "beta"), ("delta-gpu", "delta")):
        document = ledger.acquire(
            request(request_id, project, cpu=2, keys=("pool/gpu",), device_bound=True),
            facts(clock),
        )
        # 10 + 2 cores and a free key: it fits, yet waits behind the protected request.
        assert document["state"] == "waiting"
        assert document["ahead"] == ["gamma-cpu"]
    assert ledger.status("gamma-cpu")["bypassed"] == 1

    ledger.release("alpha-hold")
    assert ledger.promote("macbox", facts(clock))["granted"][0] == "gamma-cpu"


def test_less_recently_served_root_project_goes_first(ledger, clock) -> None:
    # Within a class the queue rotates between root projects, so a project that has just
    # had a turn waits for one that has not, even if its request is older.
    ledger.acquire(request("alpha-1", "alpha", cpu=16), facts(clock))
    ledger.acquire(request("alpha-2", "alpha", cpu=16), facts(clock))
    ledger.acquire(request("beta-1", "beta", cpu=16), facts(clock))
    assert ledger.status("alpha-2")["ahead"] == ["beta-1"]

    ledger.release("alpha-1")
    assert ledger.promote("macbox", facts(clock))["granted"] == ["beta-1"]
    rotated = ledger.status("alpha-2")
    assert (rotated["state"], rotated["position"]) == ("waiting", 1)
    assert rotated["bypassed"] == 0  # a fairness turn is not a class bypass


def test_unknown_or_partial_probe_grants_nothing(ledger, clock) -> None:
    # A failed or partial probe is not an idle device: granting on it could pile work
    # onto a host that is already full.
    policy = parse_device_resource_policy(POLICY)
    unknown = Metric(None, "timeout", "resource-probe", "unknown")
    probes = {
        "unreachable": (unavailable_snapshot("unavailable", "ssh: connect timed out"), policy),
        "busy-unknown": (replace(snapshot(), cpu_busy_pct=unknown), policy),
        "ram-unknown": (replace(snapshot(), ram_available_bytes=unknown), policy),
        "no-policy": (snapshot(), MissingResourcePolicy()),
    }
    for request_id, (probe, device_policy) in probes.items():
        observed = facts_from_snapshot("macbox", probe, device_policy, observed_at=clock())
        document = ledger.acquire(request(request_id, request_id, cpu=1), observed)
        assert not observed.known
        assert document["state"] == "waiting"
        assert document["promotion"]["blocked"]["reason"] == "facts_unknown"

    measured = facts_from_snapshot("macbox", snapshot(), policy, observed_at=clock())
    assert ledger.promote("macbox", measured)["granted"] == list(probes)


def test_budget_and_offer_read_the_policy_as_launch_envelopes_do(clock) -> None:
    # 20 cores, at most half of them, 4 kept back for the host: all grants together may
    # hold 10 cores, and with the host half busy 6 more are free now. The live offer is
    # exactly what a workload's launch envelope offers for the same probe.
    policy = parse_device_resource_policy(POLICY)
    probe = snapshot(cores=20, busy=50.0, total_gib=64, available_gib=32)
    observed = facts_from_snapshot("macbox", probe, policy, observed_at=clock())
    assert (observed.budget_cpu_cores, observed.offer_cpu_cores) == (10, 6)
    # RAM: min(64 GiB - 8 GiB reserve, 70% of 64 GiB) and min(32 GiB - 8 GiB, 70% of 64 GiB).
    assert (observed.budget_ram_mib, observed.offer_ram_mib) == (45875, 24 * 1024)

    device = Device.from_mapping("macbox", {"resource_policy": POLICY})
    envelope = build_resource_envelope(
        snapshot=probe, policy=policy, device=device, captured_at="2026-01-01T00:00:00Z",
    )
    assert envelope["offered"]["cpu"]["cores"] == observed.offer_cpu_cores
    assert envelope["offered"]["ram"]["bytes"] // (1024 * 1024) == observed.offer_ram_mib


def test_recent_grant_counts_against_live_offer_until_it_settles(ledger, clock) -> None:
    # A probe just after a grant still shows the cores free. Unless the pending grant is
    # subtracted, a second job lands on the cores the first is about to use.
    assert ledger.acquire(request("first", "alpha", cpu=6), facts(clock, offer_cpu=8))["state"] \
        == "granted"
    clock.now += SETTLE - 1
    second = ledger.acquire(request("second", "beta", cpu=4), facts(clock, offer_cpu=8))
    assert second["state"] == "waiting"
    assert second["promotion"]["blocked"]["reason"] == "live_cpu"

    clock.now += 2  # a probe now would show the first job's load, so trust it
    assert ledger.promote("macbox", facts(clock, offer_cpu=8))["granted"] == ["second"]


def test_a_resource_the_request_does_not_use_cannot_block_it(ledger, clock) -> None:
    # A GPU job that declares no CPU must not wait on CPU contention it adds nothing to.
    full = facts(clock, cpu=8, offer_cpu=8)
    assert ledger.acquire(request("cpu-job", "alpha", cpu=8), full)["state"] == "granted"
    no_cpu_left = facts(clock, cpu=8, offer_cpu=0)
    gpu = ledger.acquire(request("gpu-job", "beta", keys=("pool/gpu",)), no_cpu_left)
    assert gpu["state"] == "granted"


def test_silent_waiter_expires_but_a_grant_never_does(ledger, clock) -> None:
    # An abandoned waiter would otherwise hold its place, possibly the head, forever. A
    # grant may be running work, and only its release can say that work ended.
    ledger.acquire(request("held", "alpha", cpu=16), facts(clock))
    ledger.acquire(request("kept", "beta", cpu=4), facts(clock))
    ledger.acquire(request("silent", "gamma", cpu=4), facts(clock))
    clock.now += TTL - 10
    ledger.acquire(request("kept", "beta", cpu=4), facts(clock))
    clock.now += 20

    expired = ledger.status("silent")
    assert (expired["state"], expired["end_reason"]) == ("cancelled", "heartbeat_expired")
    assert ledger.status("kept")["state"] == "waiting"
    clock.now += 100 * TTL
    assert ledger.status("held")["state"] == "granted"


def test_one_key_is_never_granted_twice_on_a_device(ledger, clock) -> None:
    # Capacity-one keys stand for things like a GPU pool: two holders would share it.
    unknown = DeviceFacts("macbox", clock(), None, None, None, None, "probe failed")
    ledger.acquire(request("gpu-a", "alpha", cpu=1, keys=("pool/gpu",)), unknown)
    ledger.acquire(request("gpu-b", "beta", cpu=1, keys=("pool/gpu",)), unknown)

    promoted = ledger.promote("macbox", facts(clock))
    assert promoted["granted"] == ["gpu-a"]
    assert promoted["blocked"] == {"request_id": "gpu-b", "reason": "key_held", "detail": "pool/gpu"}
    elsewhere = ledger.acquire(
        request("gpu-c", "gamma", cpu=1, keys=("pool/gpu",), device="winbox"),
        facts(clock, device="winbox"),
    )
    assert elsewhere["state"] == "granted"  # keys are per device

    ledger.release("gpu-a")
    assert ledger.promote("macbox", facts(clock))["granted"] == ["gpu-b"]


def test_request_larger_than_the_device_budget_is_cancelled(ledger, clock) -> None:
    # It could never be granted, so at the head of the queue it would block the device.
    too_big = ledger.acquire(request("too-big", "alpha", cpu=17), facts(clock))
    assert (too_big["state"], too_big["end_reason"]) == ("cancelled", "exceeds_device_budget")
    assert ledger.acquire(request("fits", "beta", cpu=16), facts(clock))["state"] == "granted"


def test_release_ends_only_grants_and_cancel_only_waiters(ledger, clock) -> None:
    # Cancelling a grant would drop running work from the ledger without a release.
    ledger.acquire(request("held", "alpha", cpu=16), facts(clock))
    ledger.acquire(request("queued", "beta", cpu=1), facts(clock))
    with pytest.raises(AdmissionError):
        ledger.cancel("held")
    with pytest.raises(AdmissionError):
        ledger.release("queued")
    assert ledger.release("held")["state"] == ledger.release("held")["state"] == "released"
    assert ledger.cancel("queued")["state"] == ledger.cancel("queued")["state"] == "cancelled"


class LedgerClient:
    """The ledger behind the admission client interface, probing nothing."""

    poll_seconds = TTL / 3

    def __init__(self, ledger: AdmissionLedger, clock: Clock, **facts_kwargs) -> None:
        self.ledger, self.clock, self.facts_kwargs = ledger, clock, facts_kwargs

    def acquire(self, item: AdmissionRequest) -> dict:
        return self.ledger.acquire(
            item, facts(self.clock, device=item.device, **self.facts_kwargs)
        )

    def status(self, request_id: str) -> dict:
        return self.ledger.status(request_id)

    def release(self, request_id: str) -> dict:
        return self.ledger.release(request_id)

    def cancel(self, request_id: str) -> dict:
        return self.ledger.cancel(request_id)


def test_wait_keeps_the_request_alive_until_granted(ledger, clock, monkeypatch) -> None:
    # The wait outlasts the heartbeat TTL; only polling keeps the request queued.
    ledger.acquire(request("held", "alpha", cpu=16), facts(clock))
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.now += seconds
        if len(sleeps) == 4:
            ledger.release("held")

    monkeypatch.setattr(admission, "_sleep", sleep)
    reports: list[dict] = []
    document = admit_first(
        LedgerClient(ledger, clock), [request("next", "beta", cpu=4)],
        wait_seconds=3600, report=reports.append, clock=clock,
    )
    assert document["state"] == "granted"
    assert sum(sleeps) > TTL and max(sleeps) <= TTL / 3
    assert len(reports) == 1  # one status line while the place in the queue stays the same


def test_auto_takes_the_first_candidate_that_grants_and_withdraws_the_rest(
    ledger, clock, monkeypatch,
) -> None:
    # Candidate order is kept: the run lands on the first device with room now, and a
    # request it made on a fuller device is cancelled rather than left to hold a place.
    ledger.acquire(request("full", "alpha", cpu=16), facts(clock))
    client = LedgerClient(ledger, clock)
    candidates = [request("run-0", "beta", cpu=4), request("run-1", "beta", cpu=4,
                                                           device="winbox")]
    document = admit_first(client, candidates, wait_seconds=0, report=lambda _doc: None,
                           clock=clock)
    assert (document["request_id"], document["state"]) == ("run-1", "granted")
    assert ledger.status("run-0")["state"] == "cancelled"

    # When no candidate has room, the run queues on the first and gives the rest back.
    ledger.acquire(request("full-too", "alpha", cpu=12, device="winbox"),
                   facts(clock, device="winbox"))
    monkeypatch.setattr(admission, "_sleep", lambda seconds: setattr(
        clock, "now", clock.now + seconds))
    queued = admit_first(client, [request("run-2", "beta", cpu=4),
                                  request("run-3", "beta", cpu=4, device="winbox")],
                         wait_seconds=1, report=lambda _doc: None, clock=clock)
    assert (queued["request_id"], queued["state"]) == ("run-2", "waiting")
    assert ledger.status("run-3")["state"] == "cancelled"


def test_admission_settings_are_required_and_shipped(tmp_path: Path) -> None:
    def settings(defaults: dict, devices: dict | None = None) -> dict:
        return admission_settings(RemrunConfig(tmp_path, defaults, devices or {}, {},
                                               device_admission={}))

    shipped = load_toml(Path(__file__).resolve().parents[1] / "config" / "defaults.toml")
    assert settings(shipped)["coordinator"] is None  # the published default is one ledger each
    valid = {"settle_seconds": 60, "wait_ttl_seconds": 300, "run_wait_seconds": 0,
             "coordinator": ""}
    for broken in ({}, {"admission": {"settle_seconds": 60}},
                   {"admission": {**valid, "wait_ttl_seconds": 0}},
                   {"admission": {**valid, "settle_seconds": "60"}},
                   {"admission": {**valid, "coordinator": "nobox"}}):
        with pytest.raises(ValueError):
            settings(broken)


def test_admit_cli_end_to_end(tmp_path: Path, monkeypatch, capsys) -> None:
    root = tmp_path / "remrun"
    (root / "config").mkdir(parents=True)
    (root / "config" / "defaults.toml").write_text(
        "[admission]\nsettle_seconds = 60\nwait_ttl_seconds = 300\nrun_wait_seconds = 0\n"
        'coordinator = ""\n', encoding="utf-8"
    )
    policy = {**POLICY, "cpu_reserve_cores": 0, "cpu_max_fraction": 1.0,
              "ram_reserve_mib": 0, "ram_max_fraction": 1.0, "allow_static_fallback": False}
    (root / "config" / "devices.toml").write_text(
        '[devices.macbox]\nkind = "ssh-posix"\nos = "darwin"\n\n'
        "[devices.macbox.resource_policy]\n"
        + "".join(f"{key} = {json.dumps(value)}\n" for key, value in policy.items()),
        encoding="utf-8",
    )
    monkeypatch.setenv("REMRUN_ROOT", str(root))
    monkeypatch.setenv("REMRUN_STATE_ROOT", str(tmp_path / "state"))
    probes: list[tuple[str, float]] = []
    probe = {"snapshot": snapshot(cores=8, total_gib=16, available_gib=16)}

    def fake_probe(_transport, device, *, timeout_sec):
        probes.append((device.name, timeout_sec))
        return probe["snapshot"]

    monkeypatch.setattr(admission, "probe_target_resources", fake_probe)

    def admit(*argv: str):
        code = cli.main(["admit", *argv])
        out = capsys.readouterr().out
        return code, json.loads(out) if out.strip() else None

    def acquire(request_id: str, project: str, *extra: str):
        return admit("acquire", "--device", "macbox", "--project", project, "--thread",
                     f"{project}-thread", "--request-id", request_id, "--json", *extra)

    code, first = acquire("a1", "alpha", "--cpu", "6")
    assert (code, first["state"]) == (cli.EXIT_OK, "granted")
    code, second = acquire("b1", "beta", "--cpu", "6", "--ram-gib", "0.5")
    assert (code, second["state"], second["ram_mib"]) == (cli.EXIT_ADMISSION_WAITING, "waiting", 512)
    assert (second["position"], second["ahead"]) == (1, [])
    assert second["promotion"]["blocked"]["reason"] == "ledger_cpu"

    code, listing = admit("list", "--device", "macbox", "--json")
    assert [item["request_id"] for item in listing["requests"]] == ["a1", "b1"]
    assert admit("cancel", "a1", "--json") == (cli.EXIT_INTERNAL, None)
    code, released = admit("release", "a1", "--json")
    assert (code, released["state"]) == (cli.EXIT_OK, "released")

    code, replay = acquire("b1", "beta", "--cpu", "6", "--ram-gib", "0.5", "--wait", "5")
    assert (code, replay["state"], replay["replayed"]) == (cli.EXIT_OK, "granted", True)
    assert admit("status", "b1", "--json")[1]["state"] == "granted"

    probe["snapshot"] = unavailable_snapshot("unavailable", "ssh: connection refused")
    calls = len(probes)
    code, blind = acquire("c1", "gamma", "--cpu", "1", "--wait", "0.2")
    assert (code, blind["state"]) == (cli.EXIT_ADMISSION_WAITING, "waiting")
    assert blind["promotion"]["blocked"]["reason"] == "facts_unknown"
    assert len(probes) - calls >= 2  # the wait re-probed before giving up
    assert set(probes) == {("macbox", 5.0)}

    assert acquire("d1", "delta", "--device", "winbox")[0] == cli.EXIT_INTERNAL
