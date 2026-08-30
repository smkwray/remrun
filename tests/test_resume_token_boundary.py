"""Issued resume tokens survive the controller-to-runner argv boundary."""
from __future__ import annotations

import ast
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from remrun import _durable_runner as durable_runner
from remrun import _job_observer as job_observer
from remrun.config import RemrunConfig
from remrun.fleet import cli, dispatcher, executor, queue as queue_mod
from remrun.fleet.prepared import (
    RAW_COMMAND_SPEC,
    RAW_COMMAND_SPEC_ID,
    as_fleet_task,
    prepare_raw_command,
)
from remrun.models import Device
from remrun.output import Reporter
from remrun.target_resources import EMPTY_POLICY_DIGEST, TargetReservation
from remrun.transport import (
    LocalSimTransport,
    SSHPowerShellTransport,
    SSHPosixTransport,
    TransportError,
)

ISSUED_TOKEN = "-issued-resume-token"
NOW = "2026-08-26T00:00:00Z"
FUTURE = "2099-01-01T00:00:00Z"


def _device(root: Path) -> Device:
    return Device.from_mapping(
        "TARGET",
        {
            "kind": "ssh-posix",
            "os": "posix",
            "project_root": str(root / "projects"),
            "state_root": str(root / "target-state"),
            "cache_root": str(root / "cache"),
            "remote_python": sys.executable,
            "shell": "/bin/sh",
        },
    )


def _config(root: Path) -> RemrunConfig:
    device = _device(root)
    return RemrunConfig(
        repo_root=root,
        defaults={"fleet": {"pools": {}}},
        devices={device.name: device},
        project_roots={},
    )


def _runner_operation(script: str) -> str | None:
    try:
        argv = shlex.split(script)
    except ValueError:
        return None
    for index, token in enumerate(argv[:-1]):
        if Path(token).name == "_durable_runner.py":
            return argv[index + 1]
    return None


class _ShellBoundaryTransport(LocalSimTransport):
    """Local staging plus the production POSIX durable-control shell boundary."""

    _durable_control = SSHPosixTransport._durable_control
    launch_durable = SSHPosixTransport.launch_durable
    durable_status = SSHPosixTransport.durable_status
    durable_cleanup = SSHPosixTransport.durable_cleanup

    def __init__(
        self,
        device: Device,
        *,
        lose_launch_response: bool = False,
        reject_status: bool = False,
    ) -> None:
        super().__init__(device)
        self.scripts: list[str] = []
        self.stderr: list[str] = []
        self.lose_launch_response = lose_launch_response
        self.reject_status = reject_status
        self.launches = 0

    def _expand_remote(self, path: str) -> str:
        return path

    def _address_or_resolve(self) -> str:
        return "local-shell"

    def _ensure_job_observer(self) -> tuple[str, str]:
        return (
            str(Path(self.device.state_root)),
            str(Path(job_observer.__file__).resolve()),
        )

    def _ensure_durable_runner(self) -> tuple[str, str]:
        return (
            str(Path(self.device.state_root)),
            str(Path(durable_runner.__file__).resolve()),
        )

    def _remote(
        self,
        _address: str,
        script: str,
        input_bytes: bytes | None = None,
        timeout: float | None = None,
        on_stdout=None,  # noqa: ANN001
    ) -> subprocess.CompletedProcess[bytes]:
        del on_stdout
        self.scripts.append(script)
        operation = _runner_operation(script)
        if operation == "status" and self.reject_status:
            error = (
                "usage: remrun-durable-runner status [-h] --state-root STATE_ROOT "
                "--run-id RUN_ID --resume-token RESUME_TOKEN [--include-logs]\n"
                "remrun-durable-runner status: error: argument --resume-token: "
                "expected one argument\n"
            ).encode()
            self.stderr.append(error.decode())
            return subprocess.CompletedProcess(script, 2, b"", error)
        proc = subprocess.run(
            ["/bin/sh", "-c", script],
            input=input_bytes,
            capture_output=True,
            check=False,
            timeout=timeout,
        )
        if proc.stderr:
            self.stderr.append(proc.stderr.decode("utf-8", "replace"))
        if operation == "launch":
            self.launches += 1
            if self.lose_launch_response:
                self.lose_launch_response = False
                return subprocess.CompletedProcess(
                    script, 255, b"", b"simulated connection loss after target ack\n"
                )
        return proc

    def scripts_for(self, operation: str) -> list[str]:
        return [script for script in self.scripts if _runner_operation(script) == operation]


class _TargetClient:
    def __init__(self, root: Path, token: str = ISSUED_TOKEN) -> None:
        self.root = root
        self.token = token
        self.state_root = str(root)
        self.runner = root.parent / "target-resource-runner.py"
        self.info = SimpleNamespace(installed_path=str(self.runner))
        self.runner.write_text(
            """from __future__ import annotations
import json
import sys
from pathlib import Path

request = json.loads(sys.stdin.buffer.read().decode("utf-8"))
ledger_path = Path(sys.argv[2]) / "resume-token-ledger.json"
receipt = json.loads(ledger_path.read_text(encoding="utf-8"))
operation = request["operation"]
if operation == "claim":
    receipt.update(state="CLAIMED", command_start_state="NO")
elif operation == "start":
    receipt["command_start_state"] = request["values"]["state"]
elif operation == "exec_confirm":
    receipt["command_start_state"] = "YES"
elif operation == "finish":
    receipt["state"] = "RELEASED"
elif operation == "quarantine":
    receipt["state"] = "QUARANTINED"
ledger_path.write_text(json.dumps(receipt), encoding="utf-8")
sys.stdout.write(json.dumps({"ok": True, "receipt": receipt}))
""",
            encoding="utf-8",
        )

    @property
    def ledger(self) -> Path:
        return self.root / "resume-token-ledger.json"

    def reserve(self, **kwargs: Any) -> TargetReservation:
        receipt = {
            "schema": 1,
            "allocation_id": kwargs["allocation_id"],
            "operation_id": kwargs["operation_id"],
            "request_sha256": kwargs["request_sha256"],
            "fence": 1,
            "policy_generation": 0,
            "policy_digest": EMPTY_POLICY_DIGEST,
            "resource_keys": [],
            "state": "RESERVED",
            "command_start_state": "NO",
        }
        self.root.mkdir(parents=True, exist_ok=True)
        self.ledger.write_text(json.dumps(receipt), encoding="utf-8")
        return TargetReservation(receipt, self.token)

    def renew(self, *_args: Any, **_kwargs: Any) -> dict[str, object]:
        return {"receipt": {"state": "RESERVED"}}

    def status(self, *_args: Any, **_kwargs: Any) -> dict[str, object]:
        return {"status": "found", "receipt": json.loads(self.ledger.read_text())}

    def status_identity(self, *_args: Any, **_kwargs: Any) -> dict[str, object]:
        return self.status()

    def cancel(self, *_args: Any, **_kwargs: Any) -> dict[str, object]:
        receipt = json.loads(self.ledger.read_text())
        receipt["state"] = "CANCELLED"
        self.ledger.write_text(json.dumps(receipt), encoding="utf-8")
        return {"status": "cancelled", "receipt": receipt}


def _dispatch_live_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    lose_launch_response: bool = False,
    reject_status: bool = False,
) -> tuple[_ShellBoundaryTransport, Path, str, str, dict[str, int]]:
    monkeypatch.setattr(queue_mod, "_wal_reset_safe", lambda _version: True)
    monkeypatch.setattr(executor, "TARGET_OPERATION_POLL_SECONDS", 0.01)
    config = _config(tmp_path)
    state_root = tmp_path / "controller-state"
    db_path = state_root / "fleet" / "fleet.db"
    prepared = prepare_raw_command(
        [sys.executable, "-c", "import time; time.sleep(0.15); print('ok')"],
        device="TARGET",
    )
    task = as_fleet_task(prepared, {**RAW_COMMAND_SPEC, "spec_id": RAW_COMMAND_SPEC_ID})
    job_id = "job-resume-token"
    batch_id = "batch-resume-token"
    queue = queue_mod.FleetQueue(db_path)
    queue.enqueue_prepared(prepared, spec=None, job_id=job_id, now=NOW)
    owner = queue.claim_many(
        [job_id],
        "TARGET",
        batch_id=batch_id,
        lease_until=FUTURE,
        pool=None,
        task_name="command",
        engine="raw",
        bucket="",
        now=NOW,
        target_protocol_version=1,
        current_spec_ids={job_id: RAW_COMMAND_SPEC_ID},
    )
    queue.close()
    assert isinstance(owner, str)

    transport = _ShellBoundaryTransport(
        config.devices["TARGET"],
        lose_launch_response=lose_launch_response,
        reject_status=reject_status,
    )
    client = _TargetClient(tmp_path / "target-state")
    monkeypatch.setattr(executor, "make_transport", lambda _device: transport)
    monkeypatch.setattr(
        executor.TargetResourceClient,
        "connect",
        lambda *_args, **_kwargs: client,
    )
    monkeypatch.setattr(dispatcher, "_remote_output_mtimes", lambda *_a, **_k: {})
    reporter = Reporter(
        json_events=True,
        event_schema="remrun.fleet.lifecycle",
        event_version=1,
    )
    result = dispatcher._run_claimed_batch(
        config,
        state_root,
        {
            "batch_id": batch_id,
            "device": "TARGET",
            "owner_token": owner,
            "btasks": [task],
            "engine": "raw",
            "job_ids": [job_id],
        },
        60,
        reporter,
    )
    return transport, db_path, job_id, batch_id, result


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell boundary")
def test_positive_ack_binds_issued_token_before_dispatch_target_acknowledged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    transport, db_path, job_id, batch_id, result = _dispatch_live_batch(
        tmp_path, monkeypatch
    )
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    queue = queue_mod.FleetQueue(db_path)
    try:
        operation = queue.target_operation(batch_id, include_token=True)
        job = queue.get(job_id)
    finally:
        queue.close()
    assert operation is not None and operation["resume_token"] == ISSUED_TOKEN
    assert any(event["event"] == "dispatch_target_acknowledged" for event in events)
    assert job is not None and job["state"] == "done", "".join(transport.stderr)
    assert result == {"ran": 1, "ok": 1, "failed": 0, "review": 0}


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell boundary")
def test_immediate_and_later_status_bind_the_same_token_on_runner_argv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport, _db_path, _job_id, _batch_id, _result = _dispatch_live_batch(
        tmp_path, monkeypatch
    )
    status_scripts = transport.scripts_for("status")
    assert len(status_scripts) >= 2, "".join(transport.stderr)
    assert all(f"--resume-token={ISSUED_TOKEN}" in script for script in status_scripts), (
        "\n".join(status_scripts) + "\n" + "".join(transport.stderr)
    )


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell boundary")
def test_controller_reentry_after_ack_reconciles_one_terminal_result_without_relaunch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport, db_path, job_id, _batch_id, result = _dispatch_live_batch(
        tmp_path, monkeypatch, lose_launch_response=True
    )
    queue = queue_mod.FleetQueue(db_path)
    try:
        job = queue.get(job_id)
    finally:
        queue.close()
    assert transport.launches == 1
    assert job is not None and job["state"] == "done", "".join(transport.stderr)
    assert result["ok"] == 1 and result["review"] == 0


def test_missing_empty_or_malformed_token_fails_closed_without_incomplete_status_argv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    invalid = [None, "", 7, "x" * (durable_runner.MAX_TOKEN_CHARS + 1)]
    posix_device = _device(tmp_path)
    posix = SSHPosixTransport(posix_device)
    posix._address = "local"  # noqa: SLF001 - isolate the final wire seam
    posix._durable_runner_cache = (  # noqa: SLF001
        str(tmp_path / "target-state"), str(Path(durable_runner.__file__).resolve())
    )
    posix_calls: list[str] = []

    def posix_wire(_address, script, **_kwargs):  # noqa: ANN001, ANN202
        posix_calls.append(script)
        return subprocess.CompletedProcess(script, 2, b"", b"unexpected wire call")

    monkeypatch.setattr(posix, "_remote", posix_wire)
    for token in invalid:
        with pytest.raises(TransportError, match="resume token is invalid"):
            posix._durable_control(  # type: ignore[arg-type]  # noqa: SLF001
                "status", run_id="fleet-invalid", resume_token=token
            )
    assert posix_calls == []

    powershell_device = Device.from_mapping(
        "WINDOWS",
        {
            "kind": "ssh-powershell",
            "os": "windows",
            "project_root": r"C:\\projects",
            "state_root": r"C:\\state",
            "cache_root": r"C:\\cache",
            "remote_python": "python",
            "shell": "pwsh",
        },
    )
    powershell = SSHPowerShellTransport(powershell_device)
    powershell._address = "local"  # noqa: SLF001
    powershell._durable_runner_cache = (  # noqa: SLF001
        r"C:\\state", r"C:\\state\\helpers\\remrun_durable_runner_v1.py"
    )
    powershell_calls: list[str] = []

    def powershell_wire(_address, script, **_kwargs):  # noqa: ANN001, ANN202
        powershell_calls.append(script)
        return subprocess.CompletedProcess(script, 2, b"", b"unexpected wire call")

    monkeypatch.setattr(powershell, "_ps_remote", powershell_wire)
    for token in invalid:
        with pytest.raises(TransportError, match="resume token is invalid"):
            powershell._durable_control(  # type: ignore[arg-type]  # noqa: SLF001
                "status", run_id="fleet-invalid", resume_token=token
            )
    assert powershell_calls == []

    def powershell_status(_address, script, **_kwargs):  # noqa: ANN001, ANN202
        powershell_calls.append(script)
        return subprocess.CompletedProcess(script, 0, b'{"state":"running"}', b"")

    monkeypatch.setattr(powershell, "_ps_remote", powershell_status)
    assert powershell.durable_status("fleet-valid", ISSUED_TOKEN) == {"state": "running"}
    assert len(powershell_calls) == 1
    assert "'--resume-token=-issued-resume-token'" in powershell_calls[0]
    powershell_calls.clear()

    class StoredQueue:
        def target_operation_for_job(self, _job_id, *, include_token):  # noqa: ANN001
            assert include_token is True
            return {
                "device": "TARGET",
                "operation_id": "fleet-invalid",
                "request_sha256": "a" * 64,
                "resume_token": None,
            }

    monkeypatch.setattr(cli, "make_transport", lambda _device: posix)
    result, complete = cli._target_operation_status(  # noqa: SLF001
        StoredQueue(), {"job_id": "job-invalid"}, refresh=True, config=_config(tmp_path)
    )
    assert complete is False
    assert result is not None and result["query_status"] == "unknown"
    assert posix_calls == []


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell boundary")
def test_at_most_once_fence_unchanged_when_post_ack_status_cannot_bind_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport, db_path, job_id, batch_id, result = _dispatch_live_batch(
        tmp_path, monkeypatch, reject_status=True
    )
    queue = queue_mod.FleetQueue(db_path)
    try:
        job = queue.get(job_id)
        batch = queue.get_batch(batch_id)
    finally:
        queue.close()
    assert job is not None and job["state"] == "completion_unknown"
    assert batch is not None
    assert batch["target_finalization_disposition"] == "completion_unknown_or_started"
    assert result == {"ran": 1, "ok": 0, "failed": 0, "review": 1}
    assert transport.launches == 1


def test_production_source_has_no_consumer_task_or_adapter_branch() -> None:
    """Production source must carry no consumer, task or device vocabulary.

    The forbidden names are read from the deployment's own private denylist rather
    than written here. Spelling them in a public test file would put the very
    deployment names this project keeps out of `main` into `main`, and
    `scripts/public_release_check.py` correctly refuses that. The denylist is
    gitignored, so a public checkout skips: there is nothing private to leak there,
    and this guard exists to protect a boundary only this deployment has.
    """
    repo = Path(__file__).resolve().parents[1]
    denylist = repo / "config" / "private_release_patterns.txt"
    if not denylist.exists():
        pytest.skip("private denylist absent; nothing deployment-specific to check against")
    patterns = [
        re.compile(line.strip(), re.IGNORECASE)
        for line in denylist.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert patterns, "denylist present but empty"
    root = repo / "src" / "remrun"
    matches: dict[str, list[str]] = {}
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        hits = [p.pattern for p in patterns if p.search(text)]
        if hits:
            matches[str(path.relative_to(root))] = hits
    assert matches == {}


def test_saved_record_without_a_credential_never_becomes_the_string_none() -> None:
    """A record that cannot produce its token must fence, not invent ``"None"``.

    ``str(record["resume_token"])`` renders a missing or null credential as the
    four-character string ``"None"``.  That value is a non-empty ``str``, so it
    satisfies the transport's own validity check and travels to the target as a
    real credential.  The target then rejects it, and the controller reports an
    authentication failure — the credential was *refused* — when the truth is that
    this controller no longer holds one.  Those two conditions need opposite
    responses, so collapsing them is what makes the fence unresolvable by hand.
    """
    from remrun import cli as remrun_cli

    assert remrun_cli._durable_record_token(  # noqa: SLF001
        {"resume_token": ISSUED_TOKEN}
    ) == ISSUED_TOKEN

    for record in ({}, {"resume_token": None}, {"resume_token": ""},
                   {"resume_token": 7}, {"resume_token": ["t"]}):
        with pytest.raises(TransportError, match="no usable resume token"):
            remrun_cli._durable_record_token(record)  # noqa: SLF001

    # The defect was a repeated pattern, not one site: nine call sites each
    # rendered the credential with str() before handing it to the transport.
    source = Path(remrun_cli.__file__).read_text(encoding="utf-8")
    offenders = [
        ast.get_source_segment(source, node)
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "str"
        and node.args
        and "resume_token" in (ast.get_source_segment(source, node.args[0]) or "")
    ]
    assert offenders == [], f"credential rendered with str(): {offenders}"


def test_durable_control_binds_run_id_as_one_argv_item(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Identifier binding must not rely on a generator elsewhere avoiding '-'.

    Today ``new_run_id`` starts with a timestamp and fleet operation ids start with
    the literal ``fleet-``, so neither can open with a dash.  That is a property of
    two functions in other modules, not of this seam.  Splitting ``--run-id`` and
    its value across two argv items makes this boundary silently depend on both of
    them never changing, which is exactly the coupling that lost a resume token.
    """
    dashed = "-2026-08-26T00:00:00Z-TARGET-project-abc123"

    device = _device(tmp_path)
    posix = SSHPosixTransport(device)
    posix._address = "local"  # noqa: SLF001
    posix._durable_runner_cache = (  # noqa: SLF001
        str(tmp_path / "target-state"), str(Path(durable_runner.__file__).resolve())
    )
    scripts: list[str] = []

    def wire(_address, script, **_kwargs):  # noqa: ANN001, ANN202
        scripts.append(script)
        return subprocess.CompletedProcess(script, 0, b'{"state":"running"}', b"")

    monkeypatch.setattr(posix, "_remote", wire)
    assert posix.durable_status(dashed, ISSUED_TOKEN) == {"state": "running"}

    argv = shlex.split(scripts[0])
    assert f"--run-id={dashed}" in argv
    assert "--run-id" not in argv, "flag and value must not be separate argv items"

    # The real parser is the arbiter: it must recover both dashed values intact.
    parsed = durable_runner._parser().parse_args(  # noqa: SLF001
        argv[argv.index("status"):]
    )
    assert parsed.run_id == dashed
    assert parsed.resume_token == ISSUED_TOKEN
