"""`remrun run` taking its turn from shared admission before it launches."""
from __future__ import annotations

import io
import json
import os
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import pytest

import remrun.admission as admission
import remrun.cli as cli
from remrun.admission import (
    AdmissionClient, AdmissionError, AdmissionLedger, AdmissionRequest, AdmissionUnavailable,
    DeviceFacts,
)
from remrun.config import load_config
from remrun.cli import EXIT_ADMISSION_WAITING, EXIT_CANCELLED, EXIT_INFRA, EXIT_OK, main
from remrun.resource_envelope import Metric
from remrun.resource_probe import ResourceSnapshot
from remrun.transport import ExecResult, ProbeResult, TransportError
from test_durable_run import (  # noqa: F401 - durable_env is a fixture
    _FakeDurableTransport,
    _install_fake,
    _only_run_id,
    durable_env,
)

GIB = 1024**3
POLICY = (
    'schema = 1\nmode = "unattended"\nprobe_timeout_sec = 5\ncpu_reserve_cores = 0\n'
    'cpu_max_fraction = 1.0\nram_reserve_mib = 0\nram_max_fraction = 1.0\n'
    'gpu_busy_ceiling_pct = 100\nvram_reserve_mib = 0\nvram_max_fraction = 1.0\n'
    "allow_static_fallback = false\n"
)
WRITE_RAN = "import pathlib; pathlib.Path('ran.txt').write_text('ran')"


def _config_path(env: dict, name: str) -> Path:
    return env["state"].parent / "remrun" / "config" / name


def _append(path: Path, text: str) -> None:
    path.write_text(path.read_text(encoding="utf-8") + text, encoding="utf-8")


@pytest.fixture()
def admitted(durable_env, monkeypatch):  # noqa: F811
    """Both devices have an 8-core, 16 GiB policy budget and probe idle."""
    _append(_config_path(durable_env, "defaults.toml"),
            "[admission]\nsettle_seconds = 60\nwait_ttl_seconds = 300\n"
            'run_wait_seconds = 0\ncoordinator = ""\n')
    _append(_config_path(durable_env, "devices.toml"),
            f"\n[devices.TEST.resource_policy]\n{POLICY}"
            f"\n[devices.OTHER.resource_policy]\n{POLICY}")

    def measured(value):
        return Metric(value, "measured", "resource-probe", "exact")

    idle = ResourceSnapshot(
        status="ok", platform="linux", machine="x86_64", logical_cores=measured(8),
        effective_cores=measured(8), cpu_busy_pct=measured(0.0), cpu_sample_interval_ms=500,
        ram_total_bytes=measured(16 * GIB), ram_available_bytes=measured(16 * GIB),
        gpu_kind="none",
    )
    monkeypatch.setattr(admission, "probe_target_resources", lambda *_a, **_k: idle)
    ledger = AdmissionLedger(durable_env["state"], settle_seconds=60, wait_ttl_seconds=300)
    yield {**durable_env, "ledger": ledger}
    ledger.close()


def _hold(ledger: AdmissionLedger, device: str = "TEST", cpu: int = 8) -> None:
    """Another project's grant fills the device."""
    held = ledger.acquire(
        AdmissionRequest(request_id=f"holder-{device}", device=device,
                         root_project="other-project", owner_thread="other-thread",
                         cpu_cores=cpu),
        DeviceFacts(device, time.time(), 8, 16384, 8, 16384),
    )
    assert held["state"] == "granted"


def _run_admission(env: dict, run_id: str) -> dict:
    summary = json.loads((env["state"] / "runs" / run_id / "summary.json").read_text())
    return summary["plan"]["admission"]


def test_undeclared_run_never_touches_admission(durable_env, monkeypatch):  # noqa: F811
    # Invariant 1: a run that declares no resource behaves exactly as before. defaults
    # here has no [admission] section, and devices.toml's is malformed: validating
    # either would fail the run.
    def forbidden(*_args, **_kwargs):
        raise AssertionError("an undeclared run used admission")

    devices = _config_path(durable_env, "devices.toml")
    devices.write_text("admission = 5\n" + devices.read_text(encoding="utf-8"), encoding="utf-8")

    monkeypatch.setattr(admission, "AdmissionClient", forbidden)
    monkeypatch.setattr(admission, "AdmissionLedger", forbidden)
    _install_fake(monkeypatch)
    assert main(["run", "TEST", "--", sys.executable, "-c", WRITE_RAN]) == EXIT_OK
    run_id = _only_run_id(durable_env["state"])
    summary = json.loads((durable_env["state"] / "runs" / run_id / "summary.json").read_text())
    assert "admission" not in summary["plan"]
    assert not (durable_env["state"] / "admission").exists()


def test_admission_flags_without_a_declaration_are_refused(durable_env, monkeypatch):  # noqa: F811
    # A wait or thread with nothing declared would otherwise be silently ignored.
    holder = _install_fake(monkeypatch)
    code = main(["run", "TEST", "--admission-wait", "5", "--", sys.executable, "-c", "pass"])
    assert code == cli.EXIT_INTERNAL
    assert "transport" not in holder


def test_declared_run_waits_for_its_turn_then_launches_and_releases(
    admitted, monkeypatch, capsys,
):
    # The device is full, so the run must not start user code until it is granted, and
    # once its command has finished the grant must go back to the queue.
    _hold(admitted["ledger"])
    holder = _install_fake(monkeypatch)
    polls: list[float] = []

    def poll(seconds: float) -> None:
        polls.append(seconds)
        assert not (admitted["remote_project"] / "ran.txt").exists()
        admitted["ledger"].release("holder-TEST")

    monkeypatch.setattr(admission, "_sleep", poll)
    code = main(["run", "TEST", "--cpu", "4", "--thread", "t1", "--admission-wait", "60",
                 "--", sys.executable, "-c", WRITE_RAN])

    assert code == EXIT_OK and len(polls) == 1
    assert (admitted["project"] / "ran.txt").read_text() == "ran"
    err = capsys.readouterr().err
    assert "admission_waiting" in err and "head_blocked=ledger_cpu" in err
    assert "admission_released" in err
    request_id = _run_admission(admitted, _only_run_id(admitted["state"]))["request_id"]
    released = admitted["ledger"].status(request_id)
    assert (released["state"], released["owner_thread"], released["root_project"]) == (
        "released", "t1", "proj1",
    )
    assert holder["transport"].exec_calls >= 1


def test_failed_command_releases_its_grant(admitted, monkeypatch):
    # A nonzero exit is still a known end: holding the grant would starve the queue.
    _install_fake(monkeypatch)
    code = main(["run", "TEST", "--cpu", "4", "--", sys.executable, "-c", "raise SystemExit(3)"])
    assert code == 3
    request_id = _run_admission(admitted, _only_run_id(admitted["state"]))["request_id"]
    assert admitted["ledger"].status(request_id)["state"] == "released"


def test_interrupt_before_launch_releases_its_grant(admitted, monkeypatch):
    # Nothing was launched, so holding the grant would only starve the queue.
    _install_fake(monkeypatch)

    def interrupted(*_args, **_kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "preflight_reconcile", interrupted)
    assert main(["run", "TEST", "--cpu", "4", "--", sys.executable, "-c", WRITE_RAN]) \
        == EXIT_CANCELLED
    request_id = _run_admission(admitted, _only_run_id(admitted["state"]))["request_id"]
    assert admitted["ledger"].status(request_id)["state"] == "released"


class _LostMidCommand(_FakeDurableTransport):
    """The connection drops while the user's command runs."""

    def exec(self, command, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        if any("ran.txt" in str(token) for token in command):
            raise TransportError("connection lost mid-command")
        return super().exec(command, *args, **kwargs)


def test_lost_completion_keeps_the_grant_until_the_end_is_confirmed(
    admitted, monkeypatch, capsys,
):
    # The command may still be running: releasing would hand its cores to someone else.
    monkeypatch.setattr(cli, "make_transport", _LostMidCommand)
    assert main(["run", "TEST", "--cpu", "4", "--", sys.executable, "-c", WRITE_RAN]) \
        == EXIT_INFRA
    run_id = _only_run_id(admitted["state"])
    request_id = _run_admission(admitted, run_id)["request_id"]
    assert admitted["ledger"].status(request_id)["state"] == "granted"
    assert "admission_held" in capsys.readouterr().err

    # Only the operator's confirmation that the command ended gives the grant back.
    assert main(["resolve-unknown", run_id, "--confirmed-ended"]) == EXIT_OK
    assert admitted["ledger"].status(request_id)["state"] == "released"


def test_detached_durable_run_keeps_its_grant_until_resume_finalizes_it(
    admitted, monkeypatch,
):
    # Exit 4 after acknowledgement means the job is still running on the target.
    holder = _install_fake(monkeypatch, "detached")
    monkeypatch.setattr(cli, "DURABLE_STATUS_RETRY_SECONDS", 0.0)
    assert main(["run", "--durable", "TEST", "--cpu", "4", "--",
                 sys.executable, "-c", "print(1)"]) == EXIT_INFRA
    run_id = _only_run_id(admitted["state"])
    request_id = _run_admission(admitted, run_id)["request_id"]
    assert admitted["ledger"].status(request_id)["state"] == "granted"

    fake = holder["transport"]
    fake.unreachable = False
    assert main(["resume", run_id, "--no-wait"]) == EXIT_OK  # still running: still held
    assert admitted["ledger"].status(request_id)["state"] == "granted"

    assert fake.status is not None
    fake.status = {**fake.status, "state": "complete", "wrapper_exit_code": 0,
                   "acknowledged_at": 1000.0, "ended_at": 1001.0}
    assert main(["resume", run_id]) == EXIT_OK
    assert admitted["ledger"].status(request_id)["state"] == "released"


def test_timeout_cancels_the_waiting_request_without_running(admitted, monkeypatch, capsys):
    # A run that gives up must not leave a request that later wins a turn nobody uses.
    # The wait comes from [admission] run_wait_seconds (0 here), not a code default.
    _hold(admitted["ledger"])
    holder = _install_fake(monkeypatch)
    code = main(["run", "TEST", "--cpu", "4", "--", sys.executable, "-c", WRITE_RAN])
    assert code == EXIT_ADMISSION_WAITING == 75
    assert "admission_timeout" in capsys.readouterr().err
    assert holder["transport"].exec_calls == 0
    assert not (admitted["state"] / "runs").exists()
    states = [item["state"] for item in admitted["ledger"].list_requests("TEST")]
    assert states == ["granted"]  # only the holder; the run's request was cancelled


def test_auto_runs_on_the_first_candidate_that_grants(admitted, monkeypatch, capsys):
    # TEST ranks first but is full; the run lands on OTHER and gives TEST's place back.
    from test_durable_run import _ranked_fakes

    _hold(admitted["ledger"], "TEST")
    transports = _ranked_fakes(monkeypatch)
    assert main(["run", "--auto", "--cpu", "4", "--", sys.executable, "-c", WRITE_RAN]) \
        == EXIT_OK
    assert transports["OTHER"].exec_calls >= 1 and transports["TEST"].exec_calls == 0
    test_queue = admitted["ledger"].list_requests("TEST")
    assert [item["request_id"] for item in test_queue] == ["holder-TEST"]
    run_id = _only_run_id(admitted["state"])
    request_id = _run_admission(admitted, run_id)["request_id"]
    assert admitted["ledger"].status(request_id)["device"] == "OTHER"


class _Receiver:
    """The coordinator's end of the transport. Each forwarded `remrun admit` call runs in
    this process through the real receiving `cli.main`, with the receiver's own config,
    state root and machine identity."""

    def __init__(self, env: dict, who: dict, *, coordinator: str, identity: str | None,
                 reachable: bool = True) -> None:
        self.root = env["state"].parent / "receiver"
        (self.root / "config").mkdir(parents=True)
        for name in ("defaults.toml", "devices.toml"):
            text = _config_path(env, name).read_text(encoding="utf-8")
            if name == "devices.toml":
                text += f'\n[admission]\ncoordinator = "{coordinator}"\n'
            (self.root / "config" / name).write_text(text, encoding="utf-8")
        self.state = self.root / "state"
        self.who, self.identity, self.reachable = who, identity, reachable
        self.calls: list[tuple[list[str], dict[str, str]]] = []

    def probe(self) -> ProbeResult:
        if not self.reachable:
            return ProbeResult(False, None, "ssh: connect to host timed out")
        return ProbeResult(True, "fake", remote_os="posix")

    def exec_control(self, command, cwd, *, env=None, timeout=None, path_prepend=None):  # noqa: ANN001
        del cwd, timeout, path_prepend
        assert command[:2] == ["remrun", "admit"]
        self.calls.append((list(command), dict(env or {})))
        overrides = {"REMRUN_ROOT": str(self.root), "REMRUN_STATE_ROOT": str(self.state),
                     **(env or {})}
        saved = {key: os.environ.get(key) for key in overrides}
        sender, self.who["name"] = self.who["name"], self.identity
        os.environ.update(overrides)
        out, err = io.StringIO(), io.StringIO()
        try:
            with redirect_stdout(out), redirect_stderr(err):
                code = cli.main(command[1:])
        finally:
            self.who["name"] = sender
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        return ExecResult(code, out.getvalue(), err.getvalue())

    def ledger(self) -> AdmissionLedger:
        return AdmissionLedger(self.state, settle_seconds=60, wait_ttl_seconds=300)


def _coordinated(env: dict, monkeypatch, *, coordinator: str = "OTHER",  # noqa: ANN001
                 identity: str | None = "OTHER", reachable: bool = True) -> _Receiver:
    """This controller (no configured identity) names OTHER as its coordinator."""
    who: dict[str, str | None] = {"name": None}
    receiver = _Receiver(env, who, coordinator=coordinator, identity=identity,
                         reachable=reachable)
    _append(_config_path(env, "devices.toml"), '\n[admission]\ncoordinator = "OTHER"\n')
    monkeypatch.setattr(admission, "local_device",
                        lambda config: config.devices.get(who["name"] or ""))
    monkeypatch.setattr(admission, "make_transport", lambda _device: receiver)
    return receiver


def test_another_coordinator_owns_every_admission_call(admitted, monkeypatch):
    # One authority: this controller asks the coordinator over its transport and never
    # opens a ledger of its own, or two controllers could grant the same cores.
    receiver = _coordinated(admitted, monkeypatch)
    _install_fake(monkeypatch)
    assert main(["run", "TEST", "--cpu", "4", "--key", "pool/gpu", "--",
                 sys.executable, "-c", WRITE_RAN]) == EXIT_OK

    assert [command[:3] for command, _env in receiver.calls] == [
        ["remrun", "admit", "acquire"], ["remrun", "admit", "release"]]
    acquire, env = receiver.calls[0]
    assert {"--device=TEST", "--cpu=4", "--key=pool/gpu", "--json"} <= set(acquire)
    assert env == {admission.FORWARDED_ENV: "OTHER"}  # the coordinator the sender meant
    run_id = _only_run_id(admitted["state"])
    request_id = _run_admission(admitted, run_id)["request_id"]
    with receiver.ledger() as ledger:
        assert ledger.status(request_id)["state"] == "released"
    assert admitted["ledger"].list_requests() == []  # nothing in this controller's ledger
    assert _run_admission(admitted, run_id)["coordinator"] == "OTHER"


def test_unreachable_coordinator_fails_closed(admitted, monkeypatch, capsys):
    # Falling back to a local ledger would grant cores the coordinator may have given away.
    receiver = _coordinated(admitted, monkeypatch, reachable=False)
    holder = _install_fake(monkeypatch)
    assert main(["run", "TEST", "--cpu", "4", "--", sys.executable, "-c", WRITE_RAN]) \
        == EXIT_INFRA
    assert "admission_unavailable" in capsys.readouterr().err
    assert holder["transport"].exec_calls == 0
    assert admitted["ledger"].list_requests() == []
    assert not (receiver.state / "admission").exists()


@pytest.mark.parametrize(
    ("coordinator", "identity", "accepted"),
    [
        ("", "OTHER", False),       # the receiver has no coordinator: it is nobody's ledger
        ("OTHER", "TEST", False),   # the route reached the wrong machine
        ("TEST", "TEST", False),    # a machine that coordinates a different deployment
        ("OTHER", "OTHER", True),   # the coordinator the sender meant
    ],
    ids=["unset", "wrong-host", "different-self-coordinator", "intended"],
)
def test_forwarded_call_reaches_only_the_intended_coordinator(
    admitted, monkeypatch, coordinator, identity, accepted,
):
    # A forwarded call answered by any other ledger makes two authorities over one fleet.
    receiver = _coordinated(admitted, monkeypatch, coordinator=coordinator, identity=identity)
    client = AdmissionClient(load_config(), "OTHER")
    if accepted:
        assert client.list_requests() == []
        assert (receiver.state / "admission").exists()
        return
    with pytest.raises(AdmissionError, match="refusing an admission call forwarded to OTHER"):
        client.list_requests()
    assert not (receiver.state / "admission").exists()  # refused before any ledger opened


@pytest.mark.parametrize("durable", [False, True], ids=["ordinary", "durable"])
@pytest.mark.parametrize("recovery", ["resume", "cancel"])
def test_recovery_during_preflight_keeps_live_owners_grant(
    admitted, monkeypatch, recovery, durable,
):
    # A recovery command run against a run still in preflight fails, as it must; it must
    # not release that run's grant, or the run launches without its reservation.
    state = admitted["state"]

    def grant(run_id: str) -> str:
        return admitted["ledger"].status(_run_admission(admitted, run_id)["request_id"])["state"]

    seen: dict[str, object] = {"dispatch": []}

    class Dispatch(_FakeDurableTransport):
        def exec(self, command, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            if any("ran.txt" in str(token) for token in command):
                seen["dispatch"].append(grant(_only_run_id(state)))
            return super().exec(command, *args, **kwargs)

        def launch_durable(self, command, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            seen["dispatch"].append(grant(_only_run_id(state)))
            return super().launch_durable(command, *args, **kwargs)

    transports: dict[str, Dispatch] = {}
    monkeypatch.setattr(cli, "make_transport",
                        lambda device: transports.setdefault(device.name, Dispatch(device)))
    real_preflight = cli.preflight_reconcile

    def preflight(*args, **kwargs):  # noqa: ANN002, ANN003
        if "recovery" not in seen:
            run_id = _only_run_id(state)
            seen["recovery"] = main([recovery, run_id])
            seen["after_recovery"] = grant(run_id)
        return real_preflight(*args, **kwargs)

    monkeypatch.setattr(cli, "preflight_reconcile", preflight)
    argv = ["run", *(["--durable"] if durable else []), "TEST", "--cpu", "4", "--",
            sys.executable, "-c", WRITE_RAN]
    assert main(argv) == EXIT_OK
    assert seen["recovery"] != EXIT_OK
    assert seen["after_recovery"] == "granted"
    assert seen["dispatch"] == ["granted"]
    assert grant(_only_run_id(state)) == "released"


@pytest.mark.parametrize("cleanup_fails", [False, True], ids=["withdrawn", "unresolved"])
def test_lost_acquire_reply_never_launches_and_names_the_request(
    admitted, monkeypatch, capsys, cleanup_fails,
):
    # The grant committed but its reply was lost. The run must not launch, and the grant
    # must be withdrawn by the id it sent, or be named so someone can release it.
    acquire = AdmissionClient.acquire

    def lost(self, request):  # noqa: ANN001
        acquire(self, request)
        raise AdmissionUnavailable("the reply was lost")

    monkeypatch.setattr(AdmissionClient, "acquire", lost)
    if cleanup_fails:
        def unreachable(self, request):  # noqa: ANN001
            raise AdmissionUnavailable("still unreachable")

        monkeypatch.setattr(AdmissionClient, "withdraw", unreachable)
    holder = _install_fake(monkeypatch)
    assert main(["run", "TEST", "--cpu", "4", "--", sys.executable, "-c", WRITE_RAN]) \
        == EXIT_INFRA
    assert holder["transport"].exec_calls == 0
    assert not (admitted["state"] / "runs").exists()
    (request_id, state, end_reason), = admitted["ledger"]._db.execute(
        "SELECT request_id, state, end_reason FROM requests").fetchall()
    err = capsys.readouterr().err
    if cleanup_fails:
        assert state == "granted"
        assert f'request_ids=["{request_id}"]' in err and "admission_unresolved" in err
    else:
        assert (state, end_reason) == ("released", "withdrawn")
        assert "admission_unresolved" not in err


class _Answers:
    """A coordinator transport that answers every call with one fixed document."""

    def __init__(self, document: dict) -> None:
        self.document = document

    def probe(self) -> ProbeResult:
        return ProbeResult(True, "fake", remote_os="posix")

    def exec_control(self, command, cwd, **_kwargs):  # noqa: ANN001, ANN003
        return ExecResult(0, json.dumps(self.document) + "\n", "")


@pytest.mark.parametrize("document", [
    {"schema": "remrun.admission-request", "version": 1, "request_id": "someone-else",
     "state": "released"},
    {"schema": "remrun.admission-request", "version": 1, "request_id": "mine",
     "state": "granted"},
    {"request_id": "mine", "state": "released"},
], ids=["other-request", "not-released", "no-schema"])
def test_forwarded_reply_must_answer_the_call_that_was_made(admitted, monkeypatch, document):
    # A reply about another request, or one that did not do what was asked, is not proof
    # that this grant was released.
    _coordinated(admitted, monkeypatch)
    monkeypatch.setattr(admission, "make_transport", lambda _device: _Answers(document))
    with pytest.raises(AdmissionUnavailable, match="malformed or mismatched"):
        AdmissionClient(load_config(), "OTHER").release("mine")


def test_forwarded_list_without_its_requests_is_not_an_empty_ledger(admitted, monkeypatch):
    _coordinated(admitted, monkeypatch)
    monkeypatch.setattr(admission, "make_transport", lambda _device: _Answers(
        {"schema": "remrun.admission-list", "version": 1}))
    with pytest.raises(AdmissionUnavailable):
        AdmissionClient(load_config(), "OTHER").list_requests()
