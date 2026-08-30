from __future__ import annotations

import json
import os
import signal
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from remrun.fleet import cli, dispatcher
from remrun.fleet.models import DrainResultV1
from remrun.fleet.prepared import prepare_raw_command
from remrun.fleet.queue import FleetQueue
from remrun.output import Reporter


def _active_and_queued_jobs(state_root: Path) -> tuple[str, str, str]:
    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        running_id = queue.enqueue_prepared(
            prepare_raw_command(["python", "-c", "pass"], device="A"),
            spec=None,
            idempotency_key="rr4-running",
        )
        queued_id = queue.enqueue_prepared(
            prepare_raw_command(["python", "-c", "pass"], device="A"),
            spec=None,
            idempotency_key="rr4-queued",
        )
        batch_id = "rr4-batch"
        owner = queue.claim_many(
            [running_id], "A", batch_id=batch_id,
            lease_until="2099-01-01T00:00:00Z", pool=None,
        )
        assert owner is not None
        assert queue.set_batch_state(
            batch_id, "staging", expected_state="leased", owner_token=owner,
        )
        assert queue.set_batch_state(
            batch_id, "running", expected_state="staging", owner_token=owner,
        )
        return running_id, queued_id, batch_id
    finally:
        queue.close()


def test_stop_mid_tick_returns_within_bound_without_mutating_queue_evidence(
    tmp_path: Path, monkeypatch,
) -> None:
    state_root = tmp_path / "state"
    running_id, queued_id, batch_id = _active_and_queued_jobs(state_root)
    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        before_running = queue.get(running_id)
        before_queued = queue.get(queued_id)
        before_batch = queue.get_batch(batch_id)
    finally:
        queue.close()
    tick_entered = threading.Event()
    release_tick = threading.Event()
    stop = threading.Event()

    def blocked_tick(*_args, **_kwargs):  # noqa: ANN001
        tick_entered.set()
        release_tick.wait(2.0)
        return {
            "recovered": 0, "placed": 0, "ran": 0, "ok": 0,
            "failed": 0, "review": 0, "skipped": {}, "cooled": [],
        }

    monkeypatch.setattr(dispatcher, "drain_once", blocked_tick)

    def request_stop() -> None:
        assert tick_entered.wait(1.0)
        stop.set()

    requester = threading.Thread(target=request_stop)
    requester.start()
    started = time.monotonic()
    result = dispatcher.run(
        SimpleNamespace(defaults={}), state_root=state_root, until_empty=True,
        stop_event=stop, stop_timeout_s=0.1,
    )
    elapsed = time.monotonic() - started
    requester.join(timeout=1.0)
    try:
        assert elapsed < 0.5
        assert result.status == "stopped"
        assert result.exit_code == cli.EXIT_DRAIN_STOPPED
        assert result.stop == {
            "reason": "signal",
            "return_bound_s": 0.1,
            "bound_exceeded": False,
        }
        assert result.left_running == [{
            "batch_id": batch_id,
            "device": "A",
            "job_ids": [running_id],
            "state": "running",
        }]

        queue = FleetQueue(state_root / "fleet" / "fleet.db")
        try:
            assert queue.get(running_id) == before_running
            assert queue.get(queued_id) == before_queued
            assert queue.get_batch(batch_id) == before_batch
        finally:
            queue.close()
    finally:
        release_tick.set()


def test_stopped_result_names_started_completed_and_left_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_root = tmp_path / "state"
    live_job, _queued_job, live_batch = _active_and_queued_jobs(state_root)
    queue = FleetQueue(state_root / "fleet" / "fleet.db")
    try:
        done_job = queue.enqueue_prepared(
            prepare_raw_command(["python", "-c", "pass"], device="A"),
            spec=None,
            idempotency_key="rr4-done",
        )
        done_batch = "rr4-done-batch"
        done_owner = queue.claim_many(
            [done_job], "A", batch_id=done_batch,
            lease_until="2099-01-01T00:00:00Z", pool=None,
        )
        assert done_owner is not None
        assert queue.complete_batch(
            done_batch, expected_state="leased", owner_token=done_owner,
        )
    finally:
        queue.close()

    stop = dispatcher.DrainStopEvent()

    def stopped_tick(*_args, **kwargs):  # noqa: ANN001
        ledger = kwargs["_ledger"]
        ledger.record_started({"batch_id": done_batch, "job_ids": [done_job]})
        ledger.record_started({"batch_id": live_batch, "job_ids": [live_job]})
        stop.set()
        return {
            "recovered": 0, "placed": 0, "ran": 2, "ok": 1,
            "failed": 0, "review": 0, "skipped": {}, "cooled": [],
        }

    monkeypatch.setattr(dispatcher, "drain_once", stopped_tick)
    result = dispatcher.run(
        SimpleNamespace(defaults={}), state_root=state_root, until_empty=True,
        stop_event=stop, stop_timeout_s=2.0,
    )
    document = result.to_dict()

    assert document["schema"] == 1
    assert document["started"] == [
        {"batch_id": done_batch, "job_ids": [done_job]},
        {"batch_id": live_batch, "job_ids": [live_job]},
    ]
    assert document["completed"] == [{
        "batch_id": done_batch,
        "device": "A",
        "jobs": [{"job_id": done_job, "state": "done"}],
    }]
    assert document["left_running"] == [{
        "batch_id": live_batch,
        "device": "A",
        "job_ids": [live_job],
        "state": "running",
    }]
    assert result.exit_code == cli.EXIT_DRAIN_STOPPED


def test_final_snapshot_exceeding_stop_bound_is_explicit(
    tmp_path: Path, monkeypatch,
) -> None:
    release = threading.Event()

    class BlockedQueue:
        def __init__(self, *_args, **_kwargs) -> None:
            release.wait(1.0)

    monkeypatch.setattr(dispatcher, "FleetQueue", BlockedQueue)
    stop = dispatcher.DrainStopEvent()
    stop.set()
    started = time.monotonic()
    try:
        result = dispatcher.run(
            SimpleNamespace(defaults={}), state_root=tmp_path,
            until_empty=True, stop_event=stop, stop_timeout_s=0.05,
        )
    finally:
        release.set()

    assert time.monotonic() - started < 0.25
    assert result.status == "stopped"
    assert result.stop == {
        "reason": "signal",
        "return_bound_s": 0.05,
        "bound_exceeded": True,
    }


def test_natural_drain_always_emits_exactly_one_stdout_document(
    monkeypatch, capsys,
) -> None:
    result = DrainResultV1(
        status="drained", ran=1, ok=1, failed=0, review=0,
        queued=0, active=0,
        started=[{"batch_id": "b1", "job_ids": ["j1"]}],
        completed=[{
            "batch_id": "b1", "device": "A",
            "jobs": [{"job_id": "j1", "state": "done"}],
        }],
    )
    monkeypatch.setattr(cli, "load_config", lambda: object())
    monkeypatch.setattr(dispatcher, "run", lambda *_args, **_kwargs: result)
    args = cli.build_parser().parse_args(["dispatch", "--drain"])

    assert cli.cmd_dispatch(args, Reporter()) == cli.EXIT_OK
    captured = capsys.readouterr()
    assert captured.out.count("\n") == 1
    assert json.loads(captured.out) == result.to_dict()


@pytest.mark.skipif(
    os.name == "nt",
    reason=(
        "os.kill(getpid(), SIGINT) does not deliver a catchable signal on Windows -- it "
        "calls TerminateProcess and takes the test runner with it. The graceful stop it "
        "covers is reachable there through a real Ctrl-C, which a test cannot raise "
        "in-process; SIGTERM is registrable on Windows but never delivered."
    ),
)
def test_signal_stop_emits_one_document_naming_left_running(
    monkeypatch, capsys,
) -> None:
    result = DrainResultV1(
        status="stopped", ran=1, ok=0, failed=0, review=0,
        queued=1, active=1,
        started=[{"batch_id": "b-live", "job_ids": ["j-live"]}],
        left_running=[{
            "batch_id": "b-live", "device": "A",
            "job_ids": ["j-live"], "state": "running",
        }],
        stop={"reason": "signal", "return_bound_s": 0.2, "bound_exceeded": False},
    )
    monkeypatch.setattr(cli, "load_config", lambda: object())

    def fake_run(*_args, stop_event, **_kwargs):  # noqa: ANN001
        os.kill(os.getpid(), signal.SIGINT)
        assert stop_event.wait(0.2)
        return result

    monkeypatch.setattr(dispatcher, "run", fake_run)
    args = cli.build_parser().parse_args([
        "dispatch", "--drain", "--stop-timeout", "0.2",
    ])

    assert cli.cmd_dispatch(args, Reporter()) == cli.EXIT_DRAIN_STOPPED
    captured = capsys.readouterr()
    assert captured.out.count("\n") == 1
    document = json.loads(captured.out)
    assert document["status"] == "stopped"
    assert document["left_running"][0]["job_ids"] == ["j-live"]


def test_second_signal_restores_previous_handler_and_reraises(
    monkeypatch, capsys,
) -> None:
    installed: dict[signal.Signals, object] = {}
    previous = {
        signal.SIGINT: signal.default_int_handler,
        signal.SIGTERM: signal.SIG_DFL,
    }
    raised: list[signal.Signals] = []

    monkeypatch.setattr(cli, "load_config", lambda: object())
    monkeypatch.setattr(cli.signal, "getsignal", lambda signum: previous[signum])

    def install(signum, handler):  # noqa: ANN001
        installed[signum] = handler

    def reraise(signum):  # noqa: ANN001
        assert installed[signum] is previous[signum]
        raised.append(signum)
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.signal, "signal", install)
    monkeypatch.setattr(cli.signal, "raise_signal", reraise)

    def fake_run(*_args, stop_event, **_kwargs):  # noqa: ANN001
        handler = installed[signal.SIGINT]
        assert callable(handler)
        handler(signal.SIGINT, None)
        assert stop_event.is_set()
        assert raised == []
        handler(signal.SIGINT, None)
        raise AssertionError("second signal did not escalate")

    monkeypatch.setattr(dispatcher, "run", fake_run)
    args = cli.build_parser().parse_args(["dispatch", "--drain"])

    with pytest.raises(KeyboardInterrupt):
        cli.cmd_dispatch(args, Reporter())

    assert raised == [signal.SIGINT]
    assert installed[signal.SIGINT] is signal.default_int_handler
    assert installed[signal.SIGTERM] is signal.SIG_DFL
    assert capsys.readouterr().out == ""


def test_forced_interrupt_is_not_reported_as_cancellation(
    tmp_path: Path, monkeypatch,
) -> None:
    stop = dispatcher.DrainStopEvent()

    def interrupted_tick(*_args, **_kwargs):  # noqa: ANN001
        stop.set()
        raise KeyboardInterrupt

    monkeypatch.setattr(dispatcher, "drain_once", interrupted_tick)

    with pytest.raises(KeyboardInterrupt):
        dispatcher.run(
            SimpleNamespace(defaults={}), state_root=tmp_path,
            until_empty=True, stop_event=stop,
        )


def test_dispatch_accepts_positive_stop_timeout() -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["dispatch", "--drain", "--stop-timeout", "1.25"])
    assert args.stop_timeout == 1.25


def test_dispatch_default_stop_timeout_covers_queue_busy_timeout() -> None:
    args = cli.build_parser().parse_args(["dispatch", "--drain"])
    assert args.stop_timeout == dispatcher.DEFAULT_STOP_TIMEOUT_S == 35.0
