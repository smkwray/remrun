"""Opt-in, project-declared target environment bootstrap.

Bootstrap is deliberately a small protocol rather than an ecosystem adapter.  A
project supplies lock-input paths and one or more argv arrays.  Remrun fingerprints
those declarations and lock bytes locally; the target helper then serializes and
records the setup before an ordinary command is allowed to start.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import ntpath
import posixpath
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .manifest import sha256_file
from .models import Device, ProjectContext
from .runenv import RunEnv, _safe_venv_name
from .transport import TransportError

SCHEMA = "remrun.bootstrap"
VERSION = 1
MAX_RECEIPT_BYTES = 64 * 1024
DEFAULT_TIMEOUT_SECONDS = 30 * 60
MAX_TIMEOUT_SECONDS = 24 * 60 * 60
ENVIRONMENT_MARKER_NAME = ".remrun-bootstrap-v1.json"
_PLACEHOLDER = re.compile(r"\{([^{}]+)\}")
_ALLOWED_PLACEHOLDERS = frozenset(
    {"project_root", "python", "venv", "venv_python", "state_root"}
)


class BootstrapConfigError(ValueError):
    """A project bootstrap declaration is malformed or cannot be fingerprinted."""


@dataclass(frozen=True)
class BootstrapPlan:
    """Validated bootstrap declaration and its local lock-input identity."""

    status: str
    detail: str = ""
    steps: tuple[tuple[str, ...], ...] = ()
    lock_inputs: tuple[str, ...] = ()
    lock_digests: tuple[tuple[str, str], ...] = ()
    fingerprint: str | None = None
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS

    @property
    def configured(self) -> bool:
        return self.status != "disabled"

    def declaration(self) -> dict[str, Any]:
        return {
            "schema": VERSION,
            "steps": [list(step) for step in self.steps],
            "lock_inputs": list(self.lock_inputs),
            "timeout_seconds": self.timeout_seconds,
        }

    def as_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "schema": SCHEMA,
            "version": VERSION,
            "status": self.status,
            "detail": self.detail or None,
            "steps": [list(step) for step in self.steps],
            "lock_inputs": [
                {"path": path, "sha256": digest}
                for path, digest in self.lock_digests
            ],
            "fingerprint": self.fingerprint,
            "timeout_seconds": self.timeout_seconds,
        }
        return data


@dataclass(frozen=True)
class BootstrapResult:
    """Target bootstrap outcome returned to the controller."""

    status: str
    detail: str = ""
    fingerprint: str | None = None
    receipt_path: str | None = None
    output: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "detail": self.detail or None,
            "fingerprint": self.fingerprint,
            "receipt_path": self.receipt_path,
            "output": self.output or None,
        }


def _invalid_path(raw: object) -> str | None:
    if not isinstance(raw, str) or not raw or "\x00" in raw:
        return "lock input must be a non-empty string"
    # Project declarations use POSIX relative paths on every controller.  Reject
    # backslashes as well as absolute/parent paths instead of letting a target OS
    # reinterpret the authority outside the project root.
    if "\\" in raw:
        return f"lock input path must use relative POSIX syntax: {raw!r}"
    path = Path(raw)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        return f"lock input path must be relative and stay inside the project: {raw!r}"
    return None


def _check_placeholders(value: str) -> None:
    for match in _PLACEHOLDER.finditer(value):
        name = match.group(1)
        if name not in _ALLOWED_PLACEHOLDERS:
            raise BootstrapConfigError(
                f"unsupported bootstrap placeholder {{{name}}}; allowed: "
                + ", ".join(sorted(_ALLOWED_PLACEHOLDERS))
            )


def _steps(raw: object) -> tuple[tuple[str, ...], ...]:
    # `argv=[...]` is the concise one-step form.  `steps=[[...], [...]]` is the
    # multi-step form needed for venv creation followed by a pinned installer.
    if isinstance(raw, dict):
        if set(raw) - {"argv", "steps"}:
            raise BootstrapConfigError(
                "bootstrap supports only argv or steps; shell commands are not supported"
            )
        if "argv" in raw and "steps" in raw:
            raise BootstrapConfigError("bootstrap must declare argv or steps, not both")
        if "argv" in raw:
            raw = [raw["argv"]]
        else:
            raw = raw.get("steps")
    if raw is None:
        raise BootstrapConfigError("bootstrap requires non-empty argv or steps")
    if not isinstance(raw, list) or not raw:
        raise BootstrapConfigError("bootstrap steps must be a non-empty array")
    # A flat array is one argv; an array of arrays is a sequence of argv arrays.
    if all(isinstance(value, str) for value in raw):
        raw = [raw]
    if not isinstance(raw, list):  # pragma: no cover - narrowed above
        raise BootstrapConfigError("bootstrap steps must be arrays of argv strings")
    out: list[tuple[str, ...]] = []
    for index, step in enumerate(raw):
        if not isinstance(step, list) or not step:
            raise BootstrapConfigError(f"bootstrap step {index} must be a non-empty argv array")
        if any(not isinstance(token, str) or not token or "\x00" in token for token in step):
            raise BootstrapConfigError(
                f"bootstrap step {index} must contain only non-empty argv strings"
            )
        for token in step:
            _check_placeholders(token)
        out.append(tuple(step))
    return tuple(out)


def _canonical_fingerprint(
    declaration: dict[str, Any], lock_digests: tuple[tuple[str, str], ...]
) -> str:
    payload = {
        "declaration": declaration,
        "lock_digests": [
            {"path": path, "sha256": digest} for path, digest in lock_digests
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _effective_fingerprint(
    plan: BootstrapPlan,
    steps: tuple[tuple[str, ...], ...],
    environment: dict[str, str] | None = None,
) -> str:
    """Bind a portable declaration to the concrete target paths it will use."""
    payload = {
        "declaration_fingerprint": plan.fingerprint,
        "steps": [list(step) for step in steps],
    }
    if environment is not None:
        payload["environment"] = environment
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_target_path(raw: str, *, windows: bool, label: str) -> str:
    """Canonicalize an already-expanded target path without filesystem access."""
    if not isinstance(raw, str) or not raw or "\x00" in raw:
        raise BootstrapConfigError(f"{label} must be a non-empty path")
    if "~" == raw or raw.startswith(("~/", "~\\")):
        raise BootstrapConfigError(f"{label} was not expanded before bootstrap")
    if windows:
        if not ntpath.isabs(raw):
            raise BootstrapConfigError(f"{label} must be an absolute target path")
        return ntpath.normpath(raw)
    if "\\" in raw or not posixpath.isabs(raw):
        raise BootstrapConfigError(f"{label} must be an absolute native target path")
    return posixpath.normpath(raw)


def _target_environment(
    *, device: Device, runenv: RunEnv, remote_root: str, transport
) -> dict[str, str]:  # noqa: ANN001
    """Resolve the external environment and prove its target-side boundaries."""
    if not runenv.managed or not runenv.venv or not runenv.venv_root:
        raise BootstrapConfigError(
            "[run.bootstrap] requires a configured external virtualenv"
        )
    root = _canonical_target_path(
        transport.expand_remote(runenv.venv_root),
        windows=device.is_windows,
        label="target environment root",
    )
    venv = _canonical_target_path(
        transport.expand_remote(runenv.venv),
        windows=device.is_windows,
        label="target virtualenv",
    )
    path_fn = ntpath if device.is_windows else posixpath
    norm_root = path_fn.normcase(path_fn.normpath(root)).rstrip("\\/")
    norm_venv = path_fn.normcase(path_fn.normpath(venv)).rstrip("\\/")
    separator = "\\" if device.is_windows else "/"
    if norm_venv == norm_root or not norm_venv.startswith(norm_root + separator):
        raise BootstrapConfigError(
            "target virtualenv must be strictly under device.venv_root"
        )
    canonical_project = _canonical_target_path(
        transport.expand_remote(remote_root),
        windows=device.is_windows,
        label="remote project root",
    )
    norm_project = path_fn.normcase(path_fn.normpath(canonical_project)).rstrip("\\/")
    if norm_venv == norm_project or norm_venv.startswith(norm_project + separator):
        raise BootstrapConfigError(
            "target virtualenv must be external to the remote project"
        )
    bindir = "Scripts" if device.is_windows else "bin"
    interpreter = transport.native_join(venv, bindir, "python.exe" if device.is_windows else "python")
    marker = transport.native_join(venv, ENVIRONMENT_MARKER_NAME)
    return {
        "root": root,
        "path": venv,
        "interpreter": interpreter,
        "marker": marker,
    }


def bootstrap_inputs_match(
    plan: BootstrapPlan, *, project_root: Path
) -> tuple[bool, str]:
    """Recheck controller lock bytes immediately before and after setup."""
    root = project_root.resolve(strict=True)
    for relative, expected in plan.lock_digests:
        candidate = root / relative
        if candidate.is_symlink():
            return False, f"bootstrap lock input became a symlink: {relative}"
        try:
            path = candidate.resolve(strict=True)
            path.relative_to(root)
        except (OSError, ValueError):
            return False, f"bootstrap lock input disappeared or escaped: {relative}"
        if not path.is_file():
            return False, f"bootstrap lock input is no longer a regular file: {relative}"
        try:
            actual = sha256_file(path)
        except OSError as exc:
            return False, f"bootstrap lock input could not be read: {relative}: {exc}"
        if actual != expected:
            return False, f"bootstrap lock input changed during the run: {relative}"
    return True, "declared bootstrap inputs still match"


def parse_bootstrap(
    project_config: dict[str, Any] | None,
    *,
    project_root: Path,
) -> BootstrapPlan | None:
    """Validate and fingerprint the optional project bootstrap declaration.

    The returned plan is controller-local and read-only.  No target probing or
    filesystem mutation occurs here, so it is safe for the default `plan` path.
    """
    config = project_config or {}
    if "bootstrap" in config:
        return BootstrapPlan(
            status="unsupported",
            detail="bootstrap must be declared under [run.bootstrap]",
        )
    run_cfg = config.get("run")
    if not isinstance(run_cfg, dict) or "bootstrap" not in run_cfg:
        return None
    raw = run_cfg.get("bootstrap")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        return BootstrapPlan(status="unsupported", detail="[run.bootstrap] must be a table")
    try:
        if run_cfg.get("use_venv") is not True:
            raise BootstrapConfigError(
                "[run.bootstrap] requires [run] use_venv = true"
            )
        layout = run_cfg.get("venv_layout")
        overrides = run_cfg.get("venv")
        if layout is not None and layout != "external":
            raise BootstrapConfigError(
                "[run.bootstrap] requires venv_layout = 'external'"
            )
        if layout is None and not isinstance(overrides, dict):
            raise BootstrapConfigError(
                "[run.bootstrap] requires venv_layout = 'external' or per-device venv overrides"
            )
        if layout is None and not overrides:
            raise BootstrapConfigError(
                "[run.bootstrap] requires venv_layout = 'external' or per-device venv overrides"
            )
        if "venv_name" in run_cfg:
            _safe_venv_name(run_cfg["venv_name"])
        allowed = {"schema", "enabled", "argv", "steps", "lock_inputs", "timeout_seconds"}
        unknown = sorted(set(raw) - allowed)
        if unknown:
            raise BootstrapConfigError(
                "unknown [run.bootstrap] key(s): " + ", ".join(unknown)
            )
        if (
            isinstance(raw.get("schema"), bool)
            or not isinstance(raw.get("schema"), int)
            or raw.get("schema") != VERSION
        ):
            raise BootstrapConfigError("[run.bootstrap] schema must be 1")
        enabled = raw.get("enabled", True)
        if not isinstance(enabled, bool):
            raise BootstrapConfigError("[run.bootstrap] enabled must be a boolean")
        if not enabled:
            return BootstrapPlan(status="disabled")
        timeout = raw.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise BootstrapConfigError("[run.bootstrap] timeout_seconds must be a number")
        timeout = float(timeout)
        if not math.isfinite(timeout) or not (0 < timeout <= MAX_TIMEOUT_SECONDS):
            raise BootstrapConfigError(
                f"[run.bootstrap] timeout_seconds must be between 0 and {MAX_TIMEOUT_SECONDS}"
            )
        steps_raw: object
        if "steps" in raw or "argv" in raw:
            steps_raw = {key: raw[key] for key in ("steps", "argv") if key in raw}
        else:
            raise BootstrapConfigError("bootstrap requires argv or steps")
        steps = _steps(steps_raw)
        lock_raw = raw.get("lock_inputs")
        if not isinstance(lock_raw, list):
            raise BootstrapConfigError("bootstrap requires a lock_inputs array")
        lock_inputs: list[str] = []
        lock_digests: list[tuple[str, str]] = []
        root = project_root.resolve(strict=True)
        for value in lock_raw:
            error = _invalid_path(value)
            if error:
                raise BootstrapConfigError(error)
            assert isinstance(value, str)
            if value in lock_inputs:
                raise BootstrapConfigError(f"duplicate bootstrap lock input: {value}")
            candidate = root / Path(value)
            if candidate.is_symlink():
                raise BootstrapConfigError(
                    f"bootstrap lock input must not be a symlink: {value!r}"
                )
            path = candidate.resolve(strict=True)
            try:
                path.relative_to(root)
            except ValueError as exc:
                raise BootstrapConfigError(
                    f"bootstrap lock input escapes the project: {value!r}"
                ) from exc
            if path.is_symlink() or not path.is_file():
                raise BootstrapConfigError(
                    f"bootstrap lock input is not a regular file: {value!r}"
                )
            lock_inputs.append(value)
            lock_digests.append((value, sha256_file(path)))
        declaration = {
            "schema": VERSION,
            "steps": [list(step) for step in steps],
            "lock_inputs": lock_inputs,
            "timeout_seconds": timeout,
        }
        fingerprint = _canonical_fingerprint(declaration, tuple(lock_digests))
    except (OSError, TypeError, ValueError, BootstrapConfigError) as exc:
        return BootstrapPlan(status="unsupported", detail=str(exc))
    return BootstrapPlan(
        status="bootstrap-needed",
        steps=steps,
        lock_inputs=tuple(lock_inputs),
        lock_digests=tuple(lock_digests),
        fingerprint=fingerprint,
        timeout_seconds=timeout,
    )


def _replace_token(token: str, values: dict[str, str]) -> str:
    return _PLACEHOLDER.sub(lambda match: values[match.group(1)], token)


def render_steps(
    plan: BootstrapPlan,
    *,
    device: Device,
    project: ProjectContext,
    runenv: RunEnv,
    remote_root: str,
    state_root: str,
    transport,
) -> tuple[tuple[str, ...], ...]:  # noqa: ANN001
    """Render only the explicitly supported argv placeholders for one target."""
    if plan.status != "bootstrap-needed" or plan.fingerprint is None:
        raise BootstrapConfigError("bootstrap plan is not executable")
    del project
    environment = _target_environment(
        device=device, runenv=runenv, remote_root=remote_root, transport=transport
    )
    target_venv = environment["path"]
    values = {
        "project_root": remote_root,
        "python": device.remote_python or ("python" if device.is_windows else "python3"),
        "venv": target_venv,
        "venv_python": environment["interpreter"],
        "state_root": state_root,
    }
    return tuple(
        tuple(_replace_token(token, values) for token in step) for step in plan.steps
    )


def receipt_status(
    raw: bytes,
    *,
    project_id: str,
    fingerprint: str,
    marker_raw: bytes | None = None,
    environment: dict[str, str] | None = None,
    interpreter_present: bool = False,
) -> tuple[str, str]:
    """Validate a bounded target receipt without treating malformed bytes as ready."""
    if len(raw) > MAX_RECEIPT_BYTES:
        return "unsupported", "bootstrap receipt exceeds the size limit"
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return "unsupported", f"bootstrap receipt is malformed: {exc}"
    if not isinstance(data, dict):
        return "unsupported", "bootstrap receipt is not an object"
    if (
        data.get("schema") != SCHEMA
        or data.get("version") != VERSION
        or data.get("project_id") != project_id
    ):
        return "unsupported", "bootstrap receipt identity is malformed"
    status = data.get("status")
    if status not in {"running", "failed", "complete"}:
        return "unsupported", "bootstrap receipt status is malformed"
    stored_fingerprint = data.get("fingerprint")
    if not isinstance(stored_fingerprint, str) or len(stored_fingerprint) != 64:
        return "unsupported", "bootstrap receipt fingerprint is malformed"
    if status == "complete" and stored_fingerprint == fingerprint:
        if environment is None or marker_raw is None or not interpreter_present:
            return "bootstrap-needed", "bootstrap environment is missing or incomplete"
        try:
            marker = json.loads(marker_raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return "bootstrap-needed", f"bootstrap environment marker is malformed: {exc}"
        if not isinstance(marker, dict) or marker != {
            "schema": "remrun.bootstrap.environment",
            "version": VERSION,
            "project_id": project_id,
            "fingerprint": fingerprint,
            "environment": environment,
        }:
            return "bootstrap-needed", "bootstrap environment marker is stale or unauthenticated"
        return "ready", "matching completed bootstrap receipt and environment marker"
    return "bootstrap-needed", "bootstrap receipt is stale or incomplete"


def _helper_payload(
    plan: BootstrapPlan,
    *,
    project_id: str,
    steps: tuple[tuple[str, ...], ...],
    fingerprint: str,
    environment: dict[str, str],
    remote_root: str,
) -> str:
    payload = {
        "schema": SCHEMA,
        "version": VERSION,
        "project_id": project_id,
        "declaration_fingerprint": plan.fingerprint,
        "fingerprint": fingerprint,
        "declaration": plan.declaration(),
        "lock_digests": [
            {"path": path, "sha256": digest} for path, digest in plan.lock_digests
        ],
        "steps": [list(step) for step in steps],
        "environment": environment,
        "project_root": remote_root,
    }
    return base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")


def validate_managed_command(
    transport, *, device: Device, runenv: RunEnv, command: list[str]
) -> list[str]:  # noqa: ANN001
    """Reject a bare command that would fall through to the system PATH.

    The managed environment's executable directory is intentionally checked
    directly after bootstrap.  PATH prepending alone is not a proof: a missing
    executable would otherwise resolve to an unrelated system installation.
    Explicit paths remain explicit and are not rewritten.
    """
    if not runenv.managed or not command or not runenv.path_prepend:
        return list(command)
    if not runenv.venv or not runenv.venv_root:
        raise BootstrapConfigError(
            "managed environment is missing its configured external root"
        )
    token = command[0]
    if "/" in token or "\\" in token:
        return list(command)
    bindir = runenv.path_prepend[0]
    try:
        resolved = transport.resolve_managed_executable(
            bindir,
            token,
            managed_root=runenv.venv,
            configured_root=runenv.venv_root,
        )
    except (TransportError, OSError, ValueError) as exc:
        raise BootstrapConfigError(
            f"managed environment executable is missing or not executable: {token!r}; "
            "refusing system PATH fallback"
        ) from exc
    return [resolved, *command[1:]]


def inspect_bootstrap(
    transport,  # noqa: ANN001
    *,
    device: Device,
    project: ProjectContext,
    plan: BootstrapPlan | None,
    remote_root: str,
    runenv: RunEnv,
) -> BootstrapPlan | None:
    """Read target-authoritative readiness for ``plan --probe``.

    A controller-side ``exists`` probe cannot authenticate a managed
    environment: the environment directory may have been replaced by a
    symlink after bootstrap, and a present interpreter may not be runnable.
    Use the same stdlib-only target helper that performs setup, in read-only
    check mode, so planning and execution share one readiness authority.
    """
    if plan is None or plan.status != "bootstrap-needed":
        return plan
    state_root = transport.expand_remote(device.state_root)
    if not state_root:
        return BootstrapPlan(**{**plan.__dict__, "status": "unsupported", "detail": "target state_root is empty"})
    try:
        environment = _target_environment(
            device=device, runenv=runenv, remote_root=remote_root, transport=transport
        )
        steps = render_steps(
            plan,
            device=device,
            project=project,
            runenv=runenv,
            remote_root=remote_root,
            state_root=state_root,
            transport=transport,
        )
    except (BootstrapConfigError, KeyError, ValueError) as exc:
        return BootstrapPlan(
            **{**plan.__dict__, "status": "unsupported", "detail": str(exc)}
        )
    fingerprint = _effective_fingerprint(plan, steps, environment)
    token = hashlib.sha256(project.project_id.encode("utf-8")).hexdigest()
    bootstrap_root = transport.native_join(state_root, "bootstrap")
    receipt_path = transport.native_join(bootstrap_root, f"{token}.json")
    lock_path = transport.native_join(bootstrap_root, f"{token}.lock")
    helper_path = transport.native_join(bootstrap_root, "remrun_bootstrap_v1.py")
    helper_source = Path(__file__).with_name("_bootstrap.py")
    try:
        # Inspection is genuinely read-only.  Setup owns staging the helper;
        # a probe may only use an already-present, byte-identical helper.
        if not transport.remote_path_exists(helper_path):
            return BootstrapPlan(
                **{**plan.__dict__, "detail": "bootstrap readiness helper is missing"}
            )
        try:
            if transport.hash_file(helper_path) != sha256_file(helper_source):
                return BootstrapPlan(
                    **{**plan.__dict__, "detail": "bootstrap readiness helper is stale"}
                )
        except Exception as exc:  # transport implementations expose different errors
            return BootstrapPlan(
                **{**plan.__dict__, "status": "unsupported", "detail": str(exc)}
            )
        payload = _helper_payload(
            plan,
            project_id=project.project_id,
            steps=steps,
            fingerprint=fingerprint,
            environment=environment,
            remote_root=remote_root,
        )
        helper_argv = [
            device.remote_python or ("python" if device.is_windows else "python3"),
            "-S", helper_path,
            "--state-root", state_root,
            "--lock-path", lock_path,
            "--receipt-path", receipt_path,
            "--payload", payload,
            "--check",
        ]
        result = transport.exec_control(
            helper_argv,
            cwd=remote_root,
            env=runenv.env,
            # The managed bin/Scripts directory must never choose the helper
            # interpreter itself.  Device-configured paths remain available.
            path_prepend=runenv.path_prepend[1:],
            timeout=plan.timeout_seconds,
        )
        data = json.loads(result.stdout)
    except (OSError, TransportError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return BootstrapPlan(
            **{**plan.__dict__, "status": "unsupported", "detail": str(exc)}
        )
    if not isinstance(data, dict):
        return BootstrapPlan(
            **{**plan.__dict__, "status": "unsupported", "detail": "bootstrap helper returned a non-object"}
        )
    status = data.get("status")
    detail = str(data.get("detail") or "")
    if status == "ready" and result.exit_code == 0:
        return BootstrapPlan(**{**plan.__dict__, "status": "ready", "detail": detail})
    if status == "bootstrap-needed":
        return BootstrapPlan(**{**plan.__dict__, "detail": detail})
    return BootstrapPlan(
        **{**plan.__dict__, "status": "unsupported", "detail": detail or "bootstrap helper returned an invalid status"}
    )


def execute_bootstrap(
    transport,  # noqa: ANN001
    *,
    device: Device,
    project: ProjectContext,
    plan: BootstrapPlan | None,
    remote_root: str,
    runenv: RunEnv,
    memory_reservation=None,  # noqa: ANN001
) -> BootstrapResult | None:
    """Run target bootstrap under the target-local receipt/lock protocol."""
    if plan is None or plan.status == "disabled":
        return None
    if plan.status != "bootstrap-needed" or not plan.fingerprint:
        raise BootstrapConfigError(plan.detail or "bootstrap declaration is unsupported")
    state_root = transport.expand_remote(device.state_root)
    if not state_root:
        raise BootstrapConfigError("target state_root is empty")
    try:
        environment = _target_environment(
            device=device, runenv=runenv, remote_root=remote_root, transport=transport
        )
        steps = render_steps(
            plan, device=device, project=project, runenv=runenv,
            remote_root=remote_root, state_root=state_root, transport=transport,
        )
    except (KeyError, ValueError) as exc:
        raise BootstrapConfigError(str(exc)) from exc
    token = hashlib.sha256(project.project_id.encode("utf-8")).hexdigest()
    bootstrap_root = transport.native_join(state_root, "bootstrap")
    receipt_path = transport.native_join(bootstrap_root, f"{token}.json")
    lock_path = transport.native_join(bootstrap_root, f"{token}.lock")
    helper_path = transport.native_join(bootstrap_root, "remrun_bootstrap_v1.py")
    helper_source = Path(__file__).with_name("_bootstrap.py")
    transport.ensure_remote_dir(bootstrap_root)
    transport.push_file(helper_source, helper_path)
    fingerprint = _effective_fingerprint(plan, steps, environment)
    payload = _helper_payload(
        plan,
        project_id=project.project_id,
        steps=steps,
        fingerprint=fingerprint,
        environment=environment,
        remote_root=remote_root,
    )
    helper_argv = [
        device.remote_python or ("python" if device.is_windows else "python3"),
        "-S", helper_path,
        "--state-root", state_root,
        "--lock-path", lock_path,
        "--receipt-path", receipt_path,
        "--payload", payload,
    ]
    exec_kwargs: dict[str, Any] = {
        "env": runenv.env,
        # The helper is controller-owned setup code.  Do not let a managed
        # environment entry (for example a forged ``bin/python3``) counterfeit
        # setup or receipt reuse; retain only device-configured PATH entries.
        "path_prepend": runenv.path_prepend[1:],
        "timeout": plan.timeout_seconds,
    }
    if memory_reservation is not None:
        exec_kwargs["memory_reservation"] = memory_reservation
    result = transport.exec(helper_argv, cwd=remote_root, **exec_kwargs)
    try:
        data = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        return BootstrapResult(
            status="unsupported", detail=f"bootstrap helper returned malformed JSON: {exc}",
            fingerprint=fingerprint, receipt_path=receipt_path,
        )
    if not isinstance(data, dict):
        return BootstrapResult(
            status="unsupported", detail="bootstrap helper returned a non-object",
            fingerprint=fingerprint, receipt_path=receipt_path,
        )
    status = str(data.get("status", "unsupported"))
    if result.exit_code != 0 and status in {"ready", "bootstrapped"}:
        status = "failed"
    if result.exit_code != 0 and status not in {"failed", "unsupported", "locked"}:
        status = "failed"
    return BootstrapResult(
        status=status,
        detail=str(data.get("detail") or ""),
        fingerprint=fingerprint,
        receipt_path=receipt_path,
        output=str(data.get("output") or ""),
    )


def main(argv: list[str] | None = None) -> int:
    """Target-side entrypoint; intentionally stdlib-only and never shell based."""
    from ._bootstrap import main as helper_main

    return helper_main(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
