from __future__ import annotations

import json

import pytest

import remrun.cli as cli


def test_capabilities_is_a_real_command_not_a_target_alias(monkeypatch, capsys) -> None:
    called = False

    def fail_run(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("capabilities was rewritten as a run target")

    monkeypatch.setattr(cli, "cmd_run", fail_run)

    assert "capabilities" in cli.KNOWN_COMMANDS
    assert cli.main(["capabilities", "--json"]) == cli.EXIT_OK
    assert not called
    assert json.loads(capsys.readouterr().out)["schema"] == "remrun.capabilities"


def test_capabilities_json_is_one_deterministic_stdout_document(capsys) -> None:
    outputs = []
    for _ in range(2):
        assert cli.main(["capabilities", "--json"]) == cli.EXIT_OK
        captured = capsys.readouterr()
        assert captured.err == ""
        assert captured.out.endswith("\n")
        assert captured.out.count("\n") == 1
        outputs.append(captured.out)

    assert outputs[0] == outputs[1]
    assert outputs[0] == (
        json.dumps(json.loads(outputs[0]), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    )


def test_capabilities_does_not_touch_config_queue_transport_or_devices(
    monkeypatch, capsys,
) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("capabilities touched runtime state")

    for name in ("load_config", "make_transport", "probe_target_resources"):
        monkeypatch.setattr(cli, name, forbidden)

    assert cli.main(["capabilities", "--json"]) == cli.EXIT_OK
    assert capsys.readouterr().err == ""


def test_human_capabilities_is_derived_from_the_same_document(monkeypatch, capsys) -> None:
    document = {
        "schema": "remrun.capabilities",
        "version": 1,
        "protocol": {"major": 1, "minor": 0},
        "package_version": "test-package",
        "documents": {"requests": [], "receipts": [], "errors": []},
        "features": {"capabilities": "stable", "sentinel": "unavailable"},
        "coordination": {
            "scope": "controller_local_queue",
            "accepted_work_recovery": "origin_controller_only",
            "unaccepted_queue_recovery": "origin_controller_only",
            "ambiguous_acceptance_retry_scope": "none",
            "global_ordering": False,
            "global_idempotency": False,
            "cross_target_exactly_once": False,
        },
    }
    monkeypatch.setattr(cli, "build_capabilities_document", lambda: document)

    assert cli.main(["capabilities"]) == cli.EXIT_OK
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "test-package" in captured.out
    assert "sentinel: unavailable" in captured.out


def test_capabilities_internal_error_is_one_protocol_error(monkeypatch, capsys) -> None:
    def fail():
        raise RuntimeError("deliberate failure")

    monkeypatch.setattr(cli, "build_capabilities_document", fail)

    assert cli.main(["capabilities", "--json"]) == cli.EXIT_INTERNAL
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.count("\n") == 1
    assert json.loads(captured.err) == {
        "schema": "remrun.error",
        "version": 1,
        "code": "internal_error",
        "message": "deliberate failure",
        "retryable": False,
    }


def test_capabilities_argparse_errors_remain_usage_errors(capsys) -> None:
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["capabilities", "--unknown"])

    captured = capsys.readouterr()
    assert excinfo.value.code == 2
    assert captured.out == ""
    assert "unrecognized arguments: --unknown" in captured.err
    assert "remrun.error" not in captured.err
