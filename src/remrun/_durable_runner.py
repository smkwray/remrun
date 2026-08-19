"""Target-local durable ordinary-run supervisor.

This helper is staged under the configured target state root.  It is deliberately
small: it owns only one-run launch/result spooling and delegates process ownership
to remrun's existing job observer and memory-guard wrappers supplied in ``argv``.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

SCHEMA = 1
MAX_SPEC_BYTES = 1024 * 1024
MAX_STATUS_BYTES = 128 * 1024
MAX_TOKEN_CHARS = 256
MAX_ARGV_ITEMS = 100_000
MAX_ARG_CHARS = 1024 * 1024
MAX_LOG_BYTES = 1024 * 1024 * 1024
MAX_RUN_DIRS = 256
POLL_SECONDS = 0.05
TRUNCATION_MARKER = b"\n...[remrun durable log truncated]...\n"


class DurableError(RuntimeError):
    pass


def _safe_component(value: object, field: str, limit: int = 160) -> str:
    text = str(value or "")
    if not text or len(text) > limit or text in {".", ".."}:
        raise DurableError(f"{field} is invalid")
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-:@+"
    if any(ch not in allowed for ch in text):
        raise DurableError(f"{field} contains unsafe characters")
    return text


def _state_root(raw: str) -> Path:
    root = Path(raw).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    return root.resolve()


def _runs_root(root: Path) -> Path:
    return root / "durable-runs"


def _run_dir(root: Path, run_id: str) -> Path:
    return _runs_root(root) / _safe_component(run_id, "run_id")


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except (OSError, ValueError):
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _atomic_bytes(path: Path, data: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp-{os.getpid()}-{time.time_ns()}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(str(tmp), flags, mode)
    try:
        with os.fdopen(fd, "wb", closefd=False) as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(fd)
    os.replace(tmp, path)
    try:
        os.chmod(path, mode)
    except OSError:
        pass
    _fsync_dir(path.parent)


def _atomic_json(path: Path, payload: dict[str, Any], mode: int = 0o600) -> None:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(raw) > MAX_STATUS_BYTES and path.name == "status.json":
        raise DurableError("status document exceeds bound")
    _atomic_bytes(path, raw, mode)


def _read_json(path: Path, max_bytes: int) -> dict[str, Any]:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise DurableError(f"missing state file: {path.name}") from exc
    if size < 2 or size > max_bytes:
        raise DurableError(f"state file has invalid size: {path.name}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DurableError(f"state file is unreadable: {path.name}") from exc
    if not isinstance(value, dict):
        raise DurableError(f"state file is not an object: {path.name}")
    return value


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                return h.hexdigest()
            h.update(chunk)


def _token_hash(token: str) -> str:
    if not isinstance(token, str) or not token or len(token) > MAX_TOKEN_CHARS:
        raise DurableError("resume token is invalid")
    return _sha256_bytes(token.encode("utf-8"))


def _boot_marker() -> str:
    linux = Path("/proc/sys/kernel/random/boot_id")
    try:
        value = linux.read_text(encoding="ascii").strip()
        if value:
            return "linux:" + value
    except OSError:
        pass
    if sys.platform == "darwin":
        try:
            result = subprocess.run(
                ["/usr/sbin/sysctl", "-n", "kern.boottime"],
                capture_output=True,
                check=True,
                text=True,
                timeout=2.0,
            )
            seconds = result.stdout.split("sec =", 1)[1].split(",", 1)[0].strip()
            if seconds.isdigit():
                return "darwin:" + seconds
        except (IndexError, OSError, subprocess.SubprocessError):
            pass
    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.GetTickCount64.restype = ctypes.c_ulonglong
            uptime = float(kernel32.GetTickCount64()) / 1000.0
            # Quantization removes sub-second sampling jitter. PID liveness is
            # checked separately, so this remains a second reboot witness.
            return "windows:" + str(int(round((time.time() - uptime) / 10.0) * 10))
        except Exception:
            pass
    # Portable last resort. Quantize to avoid false reboot reports from the two
    # clocks being sampled on opposite sides of a one-second boundary.
    epoch = time.time() - time.monotonic()
    return "epoch:" + str(int(round(epoch / 10.0) * 10))


def _pid_alive(pid: object) -> bool:
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            handle = kernel32.OpenProcess(0x1000, False, wintypes.DWORD(pid))
            if not handle:
                return False
            kernel32.CloseHandle(handle)
            return True
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _validate_spec(spec: dict[str, Any], root: Path) -> dict[str, Any]:
    if spec.get("schema") != SCHEMA or isinstance(spec.get("schema"), bool):
        raise DurableError("spec schema must be 1")
    run_id = _safe_component(spec.get("run_id"), "run_id")
    for field, limit in (("project_id", 256), ("target", 64), ("controller", 64)):
        value = spec.get(field)
        if not isinstance(value, str) or not value or len(value) > limit:
            raise DurableError(f"{field} is invalid")
    digest = spec.get("command_sha256")
    if not isinstance(digest, str) or len(digest) != 64:
        raise DurableError("command_sha256 is invalid")
    try:
        int(digest, 16)
    except ValueError as exc:
        raise DurableError("command_sha256 is invalid") from exc
    argv = spec.get("argv")
    if not isinstance(argv, list) or not argv or len(argv) > MAX_ARGV_ITEMS:
        raise DurableError("argv is invalid")
    total = 0
    for token in argv:
        if not isinstance(token, str) or "\x00" in token or len(token) > MAX_ARG_CHARS:
            raise DurableError("argv contains an invalid token")
        total += len(token)
    if total > MAX_SPEC_BYTES:
        raise DurableError("argv exceeds durable spec bound")
    max_log_bytes = spec.get("max_log_bytes")
    if (
        isinstance(max_log_bytes, bool)
        or not isinstance(max_log_bytes, int)
        or max_log_bytes < 0
        or max_log_bytes > MAX_LOG_BYTES
    ):
        raise DurableError("max_log_bytes is invalid")
    expected_ready = _run_dir(root, run_id) / "observer-ready.json"
    if spec.get("ready_path") != str(expected_ready):
        raise DurableError("ready_path does not match the run identity")
    token_sha = spec.get("token_sha256")
    if not isinstance(token_sha, str) or len(token_sha) != 64:
        raise DurableError("token_sha256 is invalid")
    return spec


def _load_authenticated(root: Path, run_id: str, token: str) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    rdir = _run_dir(root, run_id)
    spec = _validate_spec(_read_json(rdir / "spec.json", MAX_SPEC_BYTES), root)
    auth = _read_json(rdir / "auth.json", 4096)
    if auth.get("schema") != SCHEMA or auth.get("run_id") != run_id:
        raise DurableError("authentication state mismatch")
    expected = auth.get("token_sha256")
    actual = _token_hash(token)
    if not isinstance(expected, str) or not hmac.compare_digest(expected, actual):
        raise DurableError("resume token mismatch")
    if spec.get("token_sha256") != expected:
        raise DurableError("spec authentication mismatch")
    status = _read_json(rdir / "status.json", MAX_STATUS_BYTES)
    _validate_status(status, spec)
    return rdir, spec, status


def _validate_status(status: dict[str, Any], spec: dict[str, Any]) -> None:
    if status.get("schema") != SCHEMA:
        raise DurableError("status schema mismatch")
    for field in ("run_id", "project_id", "target", "controller", "command_sha256"):
        if status.get(field) != spec.get(field):
            raise DurableError(f"status {field} mismatch")
    if status.get("state") not in {"launching", "pending", "running", "complete", "failed"}:
        raise DurableError("status state is invalid")
    if not isinstance(status.get("acknowledged"), bool):
        raise DurableError("status acknowledgement is invalid")


class _BoundedWriter:
    def __init__(self, path: Path, max_bytes: int) -> None:
        self.path = path
        self.max_bytes = max_bytes
        self.handle = path.open("w+b")
        self.written = 0
        self.truncated = False
        self.tail_limit = min(256 * 1024, max(0, max_bytes // 2))
        self.marker = TRUNCATION_MARKER[: max(0, max_bytes - self.tail_limit)]
        self.head_limit = max(0, max_bytes - self.tail_limit - len(self.marker))
        self.tail = bytearray()
        self.lock = threading.Lock()

    def _start_truncation(self, overflow: bytes) -> None:
        self.handle.flush()
        if self.tail_limit:
            tail_start = max(0, self.written - self.tail_limit)
            self.handle.seek(tail_start)
            self.tail.extend(self.handle.read(self.written - tail_start))
        self.handle.seek(self.head_limit)
        self.handle.truncate()
        self.written = self.head_limit
        self.truncated = True
        self._retain_tail(overflow)

    def _retain_tail(self, data: bytes) -> None:
        if not data or self.tail_limit <= 0:
            return
        self.tail.extend(data)
        if len(self.tail) > self.tail_limit:
            del self.tail[: len(self.tail) - self.tail_limit]

    def write(self, data: bytes) -> None:
        if not data:
            return
        with self.lock:
            if self.truncated:
                self._retain_tail(data)
                return
            remaining = self.max_bytes - self.written
            if len(data) <= remaining:
                self.handle.write(data)
                self.written += len(data)
                return
            if remaining > 0:
                self.handle.write(data[:remaining])
                self.written += remaining
            self._start_truncation(data[max(0, remaining):])

    def close(self) -> None:
        with self.lock:
            if self.truncated:
                self.handle.seek(self.head_limit)
                self.handle.write(self.marker)
                self.handle.write(self.tail)
                self.handle.truncate()
                self.written = self.handle.tell()
            self.handle.flush()
            os.fsync(self.handle.fileno())
            self.handle.close()


def _pump(pipe, writer: _BoundedWriter) -> None:  # noqa: ANN001
    try:
        while True:
            data = pipe.read(64 * 1024)
            if not data:
                return
            writer.write(data)
    finally:
        try:
            pipe.close()
        except OSError:
            pass


def _base_status(spec: dict[str, Any], state: str) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "run_id": spec["run_id"],
        "project_id": spec["project_id"],
        "target": spec["target"],
        "controller": spec["controller"],
        "command_sha256": spec["command_sha256"],
        "state": state,
        "acknowledged": False,
        "command_started": None,
        "wrapper_exit_code": None,
        "created_at": spec["created_at"],
        "updated_at": time.time(),
        "boot_marker": _boot_marker(),
    }


def _ready_valid(path: Path, spec: dict[str, Any]) -> bool:
    try:
        ready = _read_json(path, 64 * 1024)
    except DurableError:
        return False
    return (
        ready.get("schema") == 1
        and ready.get("job_id") == spec["run_id"]
        and ready.get("command_sha256") == spec["command_sha256"]
    )


def _supervise(root: Path, run_id: str) -> int:
    rdir = _run_dir(root, run_id)
    try:
        spec = _validate_spec(_read_json(rdir / "spec.json", MAX_SPEC_BYTES), root)
        auth = _read_json(rdir / "auth.json", 4096)
        if auth.get("token_sha256") != spec.get("token_sha256"):
            raise DurableError("authentication state mismatch")
        status = _base_status(spec, "pending")
        status["supervisor_pid"] = os.getpid()
        _atomic_json(rdir / "status.json", status)

        stdout_writer = _BoundedWriter(rdir / "stdout.log", int(spec["max_log_bytes"]))
        stderr_writer = _BoundedWriter(rdir / "stderr.log", int(spec["max_log_bytes"]))
        try:
            proc = subprocess.Popen(
                list(spec["argv"]),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
            )
            assert proc.stdout is not None and proc.stderr is not None
            out_thread = threading.Thread(target=_pump, args=(proc.stdout, stdout_writer), daemon=True)
            err_thread = threading.Thread(target=_pump, args=(proc.stderr, stderr_writer), daemon=True)
            out_thread.start()
            err_thread.start()
            status["wrapper_pid"] = proc.pid
            status["updated_at"] = time.time()
            _atomic_json(rdir / "status.json", status)

            ready_path = Path(str(spec["ready_path"]))
            acknowledged = False
            while proc.poll() is None:
                if ready_path.exists():
                    if not _ready_valid(ready_path, spec):
                        try:
                            proc.terminate()
                        except OSError:
                            pass
                        raise DurableError("observer readiness record is corrupt or mismatched")
                    acknowledged = True
                    status.update(
                        state="running",
                        acknowledged=True,
                        command_started=True,
                        acknowledged_at=time.time(),
                        updated_at=time.time(),
                    )
                    _atomic_json(rdir / "status.json", status)
                    break
                time.sleep(POLL_SECONDS)

            code = proc.wait()
            out_thread.join()
            err_thread.join()
            stdout_writer.close()
            stderr_writer.close()
            if not acknowledged and ready_path.exists():
                acknowledged = _ready_valid(ready_path, spec)
            status.update(
                state="complete",
                acknowledged=acknowledged,
                command_started=acknowledged,
                wrapper_exit_code=(128 - code) if code < 0 else code,
                ended_at=time.time(),
                updated_at=time.time(),
                stdout_bytes=(rdir / "stdout.log").stat().st_size,
                stderr_bytes=(rdir / "stderr.log").stat().st_size,
                stdout_sha256=_sha256_file(rdir / "stdout.log"),
                stderr_sha256=_sha256_file(rdir / "stderr.log"),
                stdout_truncated=stdout_writer.truncated,
                stderr_truncated=stderr_writer.truncated,
            )
            _atomic_json(rdir / "status.json", status)
            return 0
        finally:
            for writer in (locals().get("stdout_writer"), locals().get("stderr_writer")):
                if writer is not None:
                    try:
                        if not writer.handle.closed:
                            writer.close()
                    except (OSError, ValueError):
                        pass
    except BaseException as exc:
        try:
            spec = locals().get("spec")
            if isinstance(spec, dict):
                status = locals().get("status")
                if not isinstance(status, dict):
                    status = _base_status(spec, "failed")
                status.update(
                    state="failed",
                    error=f"{type(exc).__name__}: {exc}"[:1000],
                    ended_at=time.time(),
                    updated_at=time.time(),
                )
                _atomic_json(rdir / "status.json", status)
        except Exception:
            pass
        return 1


def _detached_flags() -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "close_fds": True,
    }
    if os.name == "nt":
        kwargs["creationflags"] = (
            getattr(subprocess, "DETACHED_PROCESS", 0x00000008)
            | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)
            | getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
            | getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0x01000000)
        )
    else:
        kwargs["start_new_session"] = True
    return kwargs


def _launch(root: Path, raw_spec: bytes) -> dict[str, Any]:
    if len(raw_spec) < 2 or len(raw_spec) > MAX_SPEC_BYTES:
        raise DurableError("launch spec has invalid size")
    try:
        supplied = json.loads(raw_spec.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise DurableError("launch spec is not valid JSON") from exc
    if not isinstance(supplied, dict):
        raise DurableError("launch spec is not an object")
    token = supplied.pop("resume_token", None)
    token_sha = _token_hash(token)
    supplied["token_sha256"] = token_sha
    spec = _validate_spec(supplied, root)
    run_id = str(spec["run_id"])
    rdir = _run_dir(root, run_id)
    # Every terminal directory still present is unresolved controller evidence:
    # successful finalization removes it with the authenticated cleanup call.
    # Never age-prune that sole result merely because its controller was offline.
    runs = _runs_root(root)
    if runs.exists() and not rdir.exists():
        run_dirs = sum(1 for child in runs.iterdir() if child.is_dir())
        if run_dirs >= MAX_RUN_DIRS:
            raise DurableError(
                "durable state root reached its bounded run-directory limit; "
                "resolve or clean terminal state before launching another run"
            )
    if rdir.exists():
        _rdir, existing_spec, status = _load_authenticated(root, run_id, token)
        if existing_spec != spec:
            raise DurableError("existing durable run spec does not match")
        return status

    runs = _runs_root(root)
    runs.mkdir(parents=True, exist_ok=True)
    rdir.mkdir(mode=0o700)
    try:
        _atomic_json(rdir / "spec.json", spec)
        _atomic_json(
            rdir / "auth.json",
            {"schema": SCHEMA, "run_id": run_id, "token_sha256": token_sha},
        )
        for name in ("stdout.log", "stderr.log"):
            _atomic_bytes(rdir / name, b"")
        status = _base_status(spec, "launching")
        _atomic_json(rdir / "status.json", status)
        _fsync_dir(rdir)
        proc = subprocess.Popen(
            [
                sys.executable,
                "-S",
                str(Path(__file__).resolve()),
                "supervise",
                "--state-root",
                str(root),
                "--run-id",
                run_id,
            ],
            **_detached_flags(),
        )
        # The detached supervisor owns status.json once spawned.  Rewriting the
        # pre-spawn ``launching`` record here can clobber a faster pending/running
        # acknowledgement and would make acknowledgement ordering ambiguous.
    except BaseException:
        shutil.rmtree(rdir, ignore_errors=True)
        raise

    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        status = _read_json(rdir / "status.json", MAX_STATUS_BYTES)
        _validate_status(status, spec)
        if status.get("acknowledged") or status.get("state") in {"complete", "failed"}:
            return status
        if not _pid_alive(status.get("supervisor_pid", proc.pid)):
            raise DurableError("durable supervisor exited before acknowledgement")
        time.sleep(POLL_SECONDS)
    raise DurableError("durable launch acknowledgement timed out")


def _status(root: Path, run_id: str, token: str, include_logs: bool) -> dict[str, Any]:
    rdir, spec, status = _load_authenticated(root, run_id, token)
    if status.get("state") in {"launching", "pending", "running"}:
        if status.get("boot_marker") != _boot_marker() or not _pid_alive(status.get("supervisor_pid")):
            status = dict(status)
            status["state"] = "failed"
            status["error"] = "durable supervisor is absent or target rebooted; restart is forbidden"
            status["ambiguous"] = True
    if include_logs:
        if status.get("state") != "complete":
            raise DurableError("logs are available only for a complete durable run")
        output: dict[str, Any] = {"status": status}
        for name in ("stdout", "stderr"):
            path = rdir / f"{name}.log"
            expected_size = status.get(f"{name}_bytes")
            if path.stat().st_size != expected_size:
                raise DurableError(f"{name} spool size mismatch")
            if _sha256_file(path) != status.get(f"{name}_sha256"):
                raise DurableError(f"{name} spool digest mismatch")
            output[f"{name}_b64"] = base64.b64encode(path.read_bytes()).decode("ascii")
        return output
    return status


def _cleanup(root: Path, run_id: str, token: str) -> dict[str, Any]:
    rdir, _spec, status = _load_authenticated(root, run_id, token)
    if status.get("state") not in {"complete", "failed"}:
        raise DurableError("cannot clean an unresolved durable run")
    shutil.rmtree(rdir)
    _fsync_dir(rdir.parent)
    return {"schema": SCHEMA, "run_id": run_id, "cleaned": True}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="remrun-durable-runner")
    sub = parser.add_subparsers(dest="operation", required=True)
    launch = sub.add_parser("launch")
    launch.add_argument("--state-root", required=True)
    status = sub.add_parser("status")
    status.add_argument("--state-root", required=True)
    status.add_argument("--run-id", required=True)
    status.add_argument("--resume-token", required=True)
    status.add_argument("--include-logs", action="store_true")
    cleanup = sub.add_parser("cleanup")
    cleanup.add_argument("--state-root", required=True)
    cleanup.add_argument("--run-id", required=True)
    cleanup.add_argument("--resume-token", required=True)
    supervise = sub.add_parser("supervise")
    supervise.add_argument("--state-root", required=True)
    supervise.add_argument("--run-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = _state_root(args.state_root)
    try:
        if args.operation == "launch":
            payload = _launch(root, sys.stdin.buffer.read(MAX_SPEC_BYTES + 1))
        elif args.operation == "status":
            payload = _status(root, args.run_id, args.resume_token, args.include_logs)
        elif args.operation == "cleanup":
            payload = _cleanup(root, args.run_id, args.resume_token)
        else:
            return _supervise(root, args.run_id)
        sys.stdout.write(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return 0
    except Exception as exc:
        sys.stderr.write(f"{type(exc).__name__}: {exc}\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
