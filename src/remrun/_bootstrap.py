"""Target-side stdlib-only implementation for :mod:`remrun.bootstrap`.

This file is copied into the target state root and executed as a standalone
script.  It intentionally accepts only argv arrays and never invokes a shell.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

try:  # POSIX advisory lock; Windows uses msvcrt below.
    import fcntl  # type: ignore
except ImportError:  # pragma: no cover - exercised on Windows targets
    fcntl = None
    import msvcrt  # type: ignore

SCHEMA = "remrun.bootstrap"
VERSION = 1
MAX_OUTPUT = 8 * 1024
MAX_RECEIPT_BYTES = 64 * 1024
ENVIRONMENT_MARKER_SCHEMA = "remrun.bootstrap.environment"
ENVIRONMENT_MARKER_NAME = ".remrun-bootstrap-v1.json"


def _short(value: object) -> str:
    text = str(value or "")
    return text if len(text) <= MAX_OUTPUT else text[:MAX_OUTPUT] + "...[truncated]"


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp-{os.getpid()}")
    tmp.write_text(json.dumps(data, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)


def _inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _usable_interpreter(path: Path) -> bool:
    """Return whether ``path`` can actually be launched on this target.

    ``Path.is_file`` alone is not enough: a regular file without execute
    permission is not an interpreter on POSIX, and accepting it lets a later
    bare command resolve from the system PATH.  Windows has no POSIX execute
    bit, so the native executable suffix is the portable check there.
    """
    if not path.is_file():
        return False
    if os.name == "nt":
        return path.suffix.lower() == ".exe"
    return os.access(path, os.X_OK)


def _environment_state(
    payload: dict, *, require_ready: bool, require_interpreter: bool = False
) -> tuple[bool, str]:
    """Validate the target-local external environment and its marker."""
    environment = payload.get("environment")
    project_root_raw = payload.get("project_root")
    if not isinstance(environment, dict) or not isinstance(project_root_raw, str):
        return False, "bootstrap payload has no managed environment"
    if set(environment) != {"root", "path", "interpreter", "marker"}:
        return False, "bootstrap environment identity is malformed"
    expected = {
        key: environment.get(key)
        for key in ("root", "path", "interpreter", "marker")
    }
    if any(not isinstance(value, str) or not value for value in expected.values()):
        return False, "bootstrap environment identity is malformed"
    if any(value == "~" or value.startswith(("~/", "~\\")) for value in expected.values()):
        return False, "bootstrap environment paths were not expanded"
    try:
        absolute_paths = [Path(value).expanduser().is_absolute() for value in expected.values()]
        project_absolute = Path(project_root_raw).expanduser().is_absolute()
    except (RuntimeError, ValueError):
        return False, "bootstrap environment paths are unresolved"
    if not all(absolute_paths):
        return False, "bootstrap environment paths must be absolute"
    if not project_absolute:
        return False, "bootstrap project root must be absolute"
    root = Path(expected["root"]).expanduser()
    venv = Path(expected["path"]).expanduser()
    interpreter = Path(expected["interpreter"]).expanduser()
    marker = Path(expected["marker"]).expanduser()
    project_root = Path(project_root_raw).expanduser()
    try:
        root_c = root.resolve(strict=False)
        venv_c = venv.resolve(strict=False)
        project_c = project_root.resolve(strict=False)
    except OSError as exc:
        return False, f"bootstrap environment path could not be resolved: {exc}"
    if not root_c.is_absolute() or not venv_c.is_absolute():
        return False, "bootstrap environment paths must be absolute"
    if not _inside(venv_c, root_c) or venv_c == root_c:
        return False, "bootstrap environment is outside its configured root"
    if _inside(venv_c, project_c) or venv_c == project_c:
        return False, "bootstrap environment must be external to the project"
    expected_marker = venv / ENVIRONMENT_MARKER_NAME
    if marker != expected_marker:
        return False, "bootstrap environment marker path is not environment-local"
    expected_interpreter = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if interpreter != expected_interpreter:
        return False, "bootstrap environment interpreter path is malformed"
    # Resolve the executable directory independently of the environment path.
    # A post-bootstrap replacement can leave ``venv/bin``/``Scripts`` present
    # while redirecting that directory outside the managed environment.  A
    # normal Python venv may, however, use a file symlink from ``bin/python``
    # to the interpreter installed by the target OS, so the *entry path* is
    # confined while its normal symlink target need not be.
    try:
        bindir_c = (venv / ("Scripts" if os.name == "nt" else "bin")).resolve(
            strict=False
        )
    except OSError as exc:
        return False, f"bootstrap environment executable could not be resolved: {exc}"
    if not _inside(bindir_c, venv_c):
        return False, "bootstrap environment executable escapes its managed environment"
    if (require_interpreter or require_ready) and not _usable_interpreter(interpreter):
        return False, "bootstrap environment interpreter is missing or not executable"
    if not require_ready:
        return True, "bootstrap environment identity is valid"
    if not root.is_dir() or not venv.is_dir():
        return False, "bootstrap environment directory is missing"
    if not marker.is_file():
        return False, "bootstrap environment marker is missing"
    if marker.is_symlink():
        return False, "bootstrap environment marker must not be a symlink"
    try:
        if marker.stat().st_size > MAX_RECEIPT_BYTES:
            return False, "bootstrap environment marker exceeds the size limit"
        marker_data = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        return False, f"bootstrap environment marker is malformed: {exc}"
    expected_marker_data = {
        "schema": ENVIRONMENT_MARKER_SCHEMA,
        "version": VERSION,
        "project_id": payload["project_id"],
        "fingerprint": payload["fingerprint"],
        "environment": expected,
    }
    if marker_data != expected_marker_data:
        return False, "bootstrap environment marker is stale or unauthenticated"
    return True, "bootstrap environment is ready"


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _validate_lock_inputs(payload: dict, project_root: Path) -> tuple[bool, str]:
    """Validate declared lock bytes without changing the target filesystem."""
    try:
        root = project_root.resolve(strict=True)
    except OSError as exc:
        return False, f"bootstrap project root could not be resolved: {exc}"
    for item in payload["lock_digests"]:
        candidate = root / item["path"]
        if candidate.is_symlink():
            return False, f"declared lock input is a symlink on target: {item['path']}"
        try:
            path = candidate.resolve(strict=True)
        except OSError:
            return False, f"declared lock input is missing on target: {item['path']}"
        if not _inside(path, root) or not path.is_file():
            return False, f"declared lock input is missing on target: {item['path']}"
        try:
            digest = _digest(path)
        except OSError as exc:
            return False, f"declared lock input could not be read: {item['path']}: {exc}"
        if digest != item["sha256"]:
            return False, f"declared lock input differs on target: {item['path']}"
    return True, ""


def _result(status: str, detail: str = "", **extra: object) -> int:
    data = {"schema": SCHEMA, "version": VERSION, "status": status, "detail": detail or None}
    data.update(extra)
    print(json.dumps(data, sort_keys=True, separators=(",", ":")))
    return 0 if status in {"ready", "bootstrapped"} else 1


def _load_payload(encoded: str) -> dict:
    try:
        payload = json.loads(base64.urlsafe_b64decode(encoded.encode("ascii")).decode("utf-8"))
    except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"bootstrap payload is malformed: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("bootstrap payload is not an object")
    if payload.get("schema") != SCHEMA or payload.get("version") != VERSION:
        raise ValueError("bootstrap payload schema/version is unsupported")
    project_id = payload.get("project_id")
    declaration_fingerprint = payload.get("declaration_fingerprint")
    fingerprint = payload.get("fingerprint")
    steps = payload.get("steps")
    lock_digests = payload.get("lock_digests")
    if (
        not isinstance(project_id, str) or not project_id
        or not isinstance(declaration_fingerprint, str)
        or len(declaration_fingerprint) != 64
        or not isinstance(fingerprint, str) or not fingerprint
        or not isinstance(steps, list) or not steps
        or not isinstance(lock_digests, list)
    ):
        raise ValueError("bootstrap payload is incomplete")
    clean_steps = []
    for index, step in enumerate(steps):
        if not isinstance(step, list) or not step or any(
            not isinstance(token, str) or not token or "\x00" in token for token in step
        ):
            raise ValueError(f"bootstrap step {index} is not a valid argv array")
        clean_steps.append(step)
    clean_inputs = []
    for item in lock_digests:
        if not isinstance(item, dict):
            raise ValueError("bootstrap lock digest is malformed")
        path, digest = item.get("path"), item.get("sha256")
        if (
            not isinstance(path, str) or not path or "\\" in path or "\x00" in path
            or Path(path).is_absolute() or any(part in {"", ".", ".."} for part in Path(path).parts)
            or not isinstance(digest, str) or len(digest) != 64
        ):
            raise ValueError("bootstrap lock digest has an invalid path or digest")
        clean_inputs.append({"path": path, "sha256": digest})
    payload["steps"] = clean_steps
    payload["lock_digests"] = clean_inputs
    environment = payload.get("environment")
    project_root = payload.get("project_root")
    if (
        not isinstance(environment, dict)
        or set(environment) != {"root", "path", "interpreter", "marker"}
        or any(not isinstance(environment.get(key), str) or not environment[key]
               for key in ("root", "path", "interpreter", "marker"))
        or not isinstance(project_root, str)
        or not project_root
    ):
        raise ValueError("bootstrap payload has an invalid managed environment")
    effective = hashlib.sha256(json.dumps(
        {
            "declaration_fingerprint": declaration_fingerprint,
            "steps": clean_steps,
            "environment": environment,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()
    if effective != fingerprint:
        raise ValueError("bootstrap effective fingerprint is invalid")
    return payload


def _inspect_existing_bootstrap(
    payload: dict, *, receipt_path: Path, state_root: Path
) -> tuple[str, str]:
    """Check a completed setup without creating or changing target files."""
    try:
        receipt_c = receipt_path.resolve(strict=False)
        state_c = state_root.resolve(strict=False)
    except OSError as exc:
        return "unsupported", f"bootstrap state path could not be resolved: {exc}"
    if not _inside(receipt_c, state_c) or receipt_c == state_c:
        return "unsupported", "bootstrap receipt path is outside the target state root"
    if not receipt_path.is_file() or receipt_path.is_symlink():
        return "bootstrap-needed", "bootstrap receipt is missing"
    try:
        if receipt_path.stat().st_size > MAX_RECEIPT_BYTES:
            return "unsupported", "bootstrap receipt exceeds the size limit"
        existing = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        return "bootstrap-needed", f"bootstrap receipt is malformed: {exc}"
    if not (
        isinstance(existing, dict)
        and existing.get("schema") == SCHEMA
        and existing.get("version") == VERSION
        and existing.get("project_id") == payload["project_id"]
        and existing.get("fingerprint") == payload["fingerprint"]
        and existing.get("status") == "complete"
    ):
        return "bootstrap-needed", "bootstrap receipt is stale or incomplete"
    ready, detail = _environment_state(payload, require_ready=True)
    return ("ready", detail) if ready else ("bootstrap-needed", detail)


def _lock(lock) -> None:  # noqa: ANN001
    if fcntl is not None:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return
    lock.seek(0)
    if not lock.read(1):
        lock.seek(0)
        lock.write(b"0")
        lock.flush()
    lock.seek(0)
    msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)


def _unlock(lock) -> None:  # noqa: ANN001
    if fcntl is not None:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    else:  # pragma: no cover - exercised on Windows targets
        lock.seek(0)
        msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="remrun-bootstrap")
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--lock-path", required=True)
    parser.add_argument("--receipt-path", required=True)
    parser.add_argument("--payload", required=True)
    parser.add_argument(
        "--check", action="store_true",
        help="inspect a completed bootstrap without creating or changing files",
    )
    args = parser.parse_args(argv)
    try:
        payload = _load_payload(args.payload)
        valid_environment, environment_detail = _environment_state(
            payload, require_ready=False
        )
        if not valid_environment:
            return _result("unsupported", environment_detail)
        state_root = Path(args.state_root).expanduser().resolve()
        lock_path = Path(args.lock_path).expanduser()
        receipt_path = Path(args.receipt_path).expanduser()
        if not state_root.is_absolute() or not _inside(lock_path, state_root) or not _inside(receipt_path, state_root):
            return _result("unsupported", "bootstrap state paths must stay under the target state root")
        project_root = Path.cwd().resolve()
        if args.check:
            lock_inputs_valid, lock_detail = _validate_lock_inputs(payload, project_root)
            if not lock_inputs_valid:
                return _result("bootstrap-needed", lock_detail)
            status, detail = _inspect_existing_bootstrap(
                payload, receipt_path=receipt_path, state_root=state_root
            )
            return _result(status, detail)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
    except (OSError, ValueError) as exc:
        return _result("unsupported", str(exc))

    try:
        lock = lock_path.open("a+b")
    except OSError as exc:
        return _result("unsupported", f"cannot open bootstrap lock: {exc}")
    try:
        try:
            _lock(lock)
        except (BlockingIOError, OSError) as exc:
            if isinstance(exc, BlockingIOError) or getattr(exc, "errno", None) in {11, 35, 37}:
                return _result("locked", "another bootstrap is running")
            return _result("unsupported", f"cannot acquire bootstrap lock: {exc}")

        lock_inputs_valid, lock_detail = _validate_lock_inputs(payload, project_root)
        if not lock_inputs_valid:
            return _result("unsupported", lock_detail)

        # A matching receipt is only reusable after the target bytes have been
        # rechecked.  An external writer may have changed a lock input since
        # the prior setup, even though the receipt itself still matches.
        try:
            if receipt_path.exists() and receipt_path.stat().st_size > MAX_RECEIPT_BYTES:
                return _result("unsupported", "bootstrap receipt exceeds the size limit")
            existing = json.loads(receipt_path.read_text(encoding="utf-8")) if receipt_path.exists() else None
        except (OSError, ValueError, json.JSONDecodeError):
            existing = None
        if (
            isinstance(existing, dict)
            and existing.get("schema") == SCHEMA
            and existing.get("version") == VERSION
            and existing.get("project_id") == payload["project_id"]
            and existing.get("fingerprint") == payload["fingerprint"]
            and existing.get("status") == "complete"
        ):
            valid_environment, environment_detail = _environment_state(
                payload, require_ready=True
            )
            if valid_environment:
                return _result(
                    "ready",
                    "matching completed bootstrap receipt and environment marker",
                )

        common = {
            "schema": SCHEMA,
            "version": VERSION,
            "project_id": payload["project_id"],
            "fingerprint": payload["fingerprint"],
            "declaration": payload.get("declaration"),
            "lock_digests": payload["lock_digests"],
            "steps": payload["steps"],
            "environment": payload["environment"],
            "project_root": payload["project_root"],
            "started_at": time.time(),
        }
        _write_json(receipt_path, {**common, "status": "running"})
        output: list[str] = []
        for index, step in enumerate(payload["steps"]):
            try:
                proc = subprocess.run(step, cwd=str(project_root), capture_output=True, text=True, check=False)
            except (OSError, ValueError) as exc:
                detail = f"bootstrap step {index} could not start: {exc}"
                _write_json(receipt_path, {**common, "status": "failed", "step": index, "detail": detail, "ended_at": time.time()})
                return _result("failed", detail, receipt_path=str(receipt_path))
            step_output = "".join([proc.stdout or "", proc.stderr or ""])
            if step_output:
                output.append(step_output)
            if proc.returncode != 0:
                detail = f"bootstrap step {index} exited {proc.returncode}"
                _write_json(receipt_path, {
                    **common, "status": "failed", "step": index, "exit_code": proc.returncode,
                    "detail": detail, "output": _short(step_output), "ended_at": time.time(),
                })
                return _result("failed", detail, receipt_path=str(receipt_path), output=_short(step_output))
        for item in payload["lock_digests"]:
            candidate = project_root / item["path"]
            if candidate.is_symlink():
                detail = f"bootstrap step changed a lock input into a symlink: {item['path']}"
                _write_json(receipt_path, {
                    **common, "status": "failed", "detail": detail,
                    "ended_at": time.time(),
                })
                return _result("failed", detail, receipt_path=str(receipt_path))
            try:
                path = candidate.resolve(strict=True)
            except OSError:
                path = candidate
            if (
                not _inside(path, project_root)
                or not path.is_file()
                or _digest(path) != item["sha256"]
            ):
                detail = f"bootstrap step changed a declared lock input: {item['path']}"
                _write_json(receipt_path, {
                    **common, "status": "failed", "detail": detail,
                    "ended_at": time.time(),
                })
                return _result("failed", detail, receipt_path=str(receipt_path))
        valid_environment, environment_detail = _environment_state(
            payload, require_ready=False, require_interpreter=True
        )
        if not valid_environment:
            detail = environment_detail
            _write_json(receipt_path, {
                **common, "status": "failed", "detail": detail,
                "ended_at": time.time(),
            })
            return _result("failed", detail, receipt_path=str(receipt_path))
        marker_data = {
            "schema": ENVIRONMENT_MARKER_SCHEMA,
            "version": VERSION,
            "project_id": payload["project_id"],
            "fingerprint": payload["fingerprint"],
            "environment": payload["environment"],
        }
        try:
            _write_json(Path(payload["environment"]["marker"]), marker_data)
        except OSError as exc:
            detail = f"bootstrap environment marker could not be written: {exc}"
            _write_json(receipt_path, {
                **common, "status": "failed", "detail": detail,
                "ended_at": time.time(),
            })
            return _result("failed", detail, receipt_path=str(receipt_path))
        detail = "bootstrap completed"
        _write_json(receipt_path, {
            **common, "status": "complete", "exit_code": 0,
            "output": _short("".join(output)), "ended_at": time.time(),
        })
        return _result("bootstrapped", detail, receipt_path=str(receipt_path), output=_short("".join(output)))
    finally:
        try:
            _unlock(lock)
        finally:
            lock.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
