from __future__ import annotations

import base64
import codecs
import io
import json
import os
import posixpath
import shlex
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

from .frame import decode_file_frame, encode_file_frame
from .manifest import Manifest, build_manifest, sha256_file
from .models import Device, ProjectContext
from .state import manifest_from_json

# Suppress the console window that ssh/local subprocesses would otherwise flash on Windows when
# remrun is launched from a GUI trigger. 0 (no-op) everywhere else.
_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)
_NEW_PROCESS_GROUP = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)

_RUNNER_SOURCE: str | None = None


def _runner_source() -> bytes:
    global _RUNNER_SOURCE
    if _RUNNER_SOURCE is None:
        path = Path(__file__).resolve().parent / "remote" / "runner.py"
        _RUNNER_SOURCE = path.read_text(encoding="utf-8")
    return _RUNNER_SOURCE.encode("utf-8")


@dataclass(frozen=True)
class ExecResult:
    exit_code: int
    stdout: str
    stderr: str
    telemetry: dict | None = None


# Sink for live remote output. `exec(on_stdout=...)` calls it with each decoded chunk
# as it arrives so the caller can tee to a log; the full text is still returned in
# ExecResult, so nothing downstream has to change.
StreamSink = Callable[[str], None]


def _stream_spawn_kwargs() -> dict[str, object]:
    """Spawn streamed commands in a group remrun can terminate as one unit."""
    if os.name == "posix":
        return {"creationflags": _NO_WINDOW, "start_new_session": True}
    return {"creationflags": _NO_WINDOW | _NEW_PROCESS_GROUP}


def _kill_stream_process(proc: subprocess.Popen, *, process_group: bool) -> None:
    """Best-effort bounded termination, including inherited-pipe descendants.

    Known and accepted limit: a descendant that puts ITSELF in a new session/process
    group (``setsid``, ``start_new_session=True``) leaves this group and survives. That
    is not fixable here — and for the SSH backends the real work runs on another machine
    anyway, where no local signal reaches it. What IS guaranteed is that such an escapee
    cannot hold the call open: the post-kill drain is bounded, so ``_stream_process``
    still returns/raises near its deadline even while an escaped process holds a pipe
    (verified: an escaped, SIGTERM-ignoring grandchild still returned in 0.40s against a
    0.3s deadline). Bounded return is the property callers depend on; universal descendant
    reaping is not claimed.

    On Windows, ``taskkill /T`` can only discover the process tree while the root
    PID still exists. If that leader has already exited, descendants may survive;
    the bounded post-kill drain still preserves the timeout contract.
    """
    if process_group and os.name == "posix":
        try:
            os.killpg(proc.pid, signal.SIGKILL)
            return
        except OSError:
            pass
    if proc.poll() is not None:
        return
    if process_group and os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=0.2,
                creationflags=_NO_WINDOW,
            )
        except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
            pass
    if proc.poll() is None:
        try:
            proc.kill()
        except OSError:
            pass


def _stream_process(
    proc: subprocess.Popen,
    on_stdout: StreamSink | None,
    timeout: float | None,
    *,
    process_group: bool = False,
) -> tuple[int, str, str]:
    """Drain a running process's stdout/stderr concurrently, returning both in full.

    ``subprocess.run(capture_output=True)`` blocks until exit, so nothing could be
    written to the run log until the command finished — a multi-hour run looked
    byte-for-byte identical to a hang, and agents either waited on nothing or killed
    healthy runs. Reading both pipes here lets stdout reach the log as it is produced.

    The streams stay SEPARATE (no ``stderr=STDOUT`` merge): the telemetry samplers
    round-trip a ``__REMRUN_TELEMETRY__`` sentinel through stderr, and merging would
    interleave it into stdout and corrupt both the parse and the command's real output.
    Dedicated readers keep both pipes draining while the caller independently enforces
    one deadline measured from entry. Incremental decoders preserve UTF-8 characters
    split across arbitrary pipe reads.
    """
    started = time.monotonic()
    deadline = started + timeout if timeout is not None else None
    chunks: list[str] = []
    err_chunks: list[str] = []
    reader_errors: list[BaseException] = []
    error_lock = threading.Lock()

    def record_reader_error(exc: BaseException) -> None:
        with error_lock:
            reader_errors.append(exc)

    def drain(pipe, target: list[str], sink: StreamSink | None = None) -> None:
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        live_sink = sink
        read = getattr(pipe, "read1", pipe.read)
        try:
            while True:
                block = read(65536)
                if not block:
                    break
                text = decoder.decode(block)
                if text:
                    target.append(text)
                    if live_sink is not None:
                        try:
                            live_sink(text)
                        except BaseException as exc:  # preserve failure, keep draining
                            record_reader_error(exc)
                            live_sink = None
            tail = decoder.decode(b"", final=True)
            if tail:
                target.append(tail)
                if live_sink is not None:
                    try:
                        live_sink(tail)
                    except BaseException as exc:
                        record_reader_error(exc)
        except BaseException as exc:
            record_reader_error(exc)
            _kill_stream_process(proc, process_group=process_group)

    out_thread = threading.Thread(
        target=drain,
        args=(proc.stdout, chunks, on_stdout),
        name="remrun-stdout-reader",
        daemon=True,
    )
    err_thread = threading.Thread(
        target=drain,
        args=(proc.stderr, err_chunks),
        name="remrun-stderr-reader",
        daemon=True,
    )
    readers = ((out_thread, proc.stdout), (err_thread, proc.stderr))
    out_thread.start()
    err_thread.start()

    timed_out = False
    try:
        if deadline is None:
            proc.wait()
        else:
            proc.wait(timeout=max(0.0, deadline - time.monotonic()))
    except subprocess.TimeoutExpired:
        timed_out = True

    # A direct child can exit while a descendant keeps an inherited pipe open. Pipe
    # draining therefore shares the command deadline instead of starting an unbounded
    # second wait after proc.wait() succeeds.
    if not timed_out:
        for thread, _pipe in readers:
            if deadline is None:
                thread.join()
            else:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
        timed_out = deadline is not None and any(thread.is_alive() for thread, _ in readers)

    if timed_out:
        _kill_stream_process(proc, process_group=process_group)
        # Give killed processes a small, shared budget to flush already-buffered bytes.
        # Never wait indefinitely: a descendant outside the group may still own a pipe.
        drain_budget = min(0.2, max(0.05, float(timeout or 0.2) * 0.25))
        drain_deadline = time.monotonic() + drain_budget
        try:
            proc.wait(timeout=max(0.0, drain_deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            pass
        for thread, _pipe in readers:
            thread.join(timeout=max(0.0, drain_deadline - time.monotonic()))

    # Closing a buffered pipe while another thread is blocked in read() can itself block
    # on the stream lock. Close only readers known to have reached EOF; daemon readers are
    # allowed to finish later on degraded platforms rather than postponing the timeout.
    for thread, pipe in readers:
        if thread.is_alive():
            continue
        try:
            pipe.close()
        except OSError:
            pass

    stdout = "".join(chunks)
    stderr = "".join(err_chunks)
    if timed_out:
        timeout_error = subprocess.TimeoutExpired(
            proc.args, timeout, output=stdout, stderr=stderr
        )
        if reader_errors:
            raise timeout_error from reader_errors[0]
        raise timeout_error
    if reader_errors:
        raise reader_errors[0]
    return proc.returncode, stdout, stderr


# Stdlib-only POSIX sampler: runs the wrapped command, then reads
# getrusage(RUSAGE_CHILDREN) for peak RSS and CPU seconds (no polling, no
# psutil) and emits a telemetry sentinel on stderr. Exits with the child code.
_POSIX_TELEMETRY_SAMPLER = (
    "import sys,time,json,resource,platform,subprocess\n"
    "i=sys.argv.index('--');cmd=sys.argv[i+1:]\n"
    "t0=time.time();rc=subprocess.call(cmd);wall=time.time()-t0\n"
    "try:\n"
    " ru=resource.getrusage(resource.RUSAGE_CHILDREN)\n"
    " mss=ru.ru_maxrss\n"
    " rss_mb=mss/1048576.0 if platform.system()=='Darwin' else mss/1024.0\n"
    " cpu=ru.ru_utime+ru.ru_stime\n"
    " avg=round(cpu/wall*100,1) if wall>0 else 0.0\n"
    " sys.stderr.write('\\n__REMRUN_TELEMETRY__ '+json.dumps("
    "{'peak_rss_mb':round(rss_mb,1),'avg_cpu_pct':avg,'cpu_sec':round(cpu,3),"
    "'wall_sec':round(wall,3)})+'\\n')\n"
    "except Exception:\n pass\n"
    "sys.exit(rc)\n"
)

_TELEMETRY_MARKER = "\n__REMRUN_TELEMETRY__ "


def _extract_telemetry(stderr: str) -> tuple[str, dict | None]:
    """Split a telemetry sentinel off the tail of captured stderr."""
    idx = stderr.rfind(_TELEMETRY_MARKER)
    if idx == -1:
        return stderr, None
    payload = stderr[idx + len(_TELEMETRY_MARKER):].split("\n", 1)[0].strip()
    try:
        data = json.loads(payload)
    except (json.JSONDecodeError, ValueError):
        return stderr, None
    return stderr[:idx], data


@dataclass(frozen=True)
class ProbeResult:
    reachable: bool
    address: str | None
    detail: str = ""
    remote_os: str | None = None


class TransportError(RuntimeError):
    pass


class BaseTransport:
    """Abstract backend contract.

    Remote paths are opaque strings in the backend's native form. The
    reconciliation engine works in project-relative POSIX paths and asks the
    transport to join them onto a remote root via ``remote_join``.
    """

    def __init__(self, device: Device) -> None:
        self.device = device

    def kill_workers(self) -> bool:
        """Stop configured in-flight workers for `fleet cancel`.

        No-op by default (for example, local simulation has no remote workers);
        SSH backends use the device's `[devices.<name>.cancel]` config.
        """
        return True

    def workers_running(self) -> bool:
        """Whether configured worker process patterns still match after a batch.

        Used by fleet's Invariant-0 health audit. Empty/missing process-pattern config is a
        harmless "no owned worker known" result.
        """
        return False

    # --- path mapping -----------------------------------------------------
    def remote_project_path(self, project: ProjectContext) -> str:
        raise NotImplementedError

    def remote_join(self, remote_root: str, rel_posix: str) -> str:
        raise NotImplementedError

    # --- execution --------------------------------------------------------
    def exec(
        self,
        command: list[str],
        cwd: str,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
        path_prepend: list[str] | None = None,
        telemetry: bool = False,
        on_stdout: StreamSink | None = None,
    ) -> ExecResult:
        """Run ``command`` remotely and return its full output.

        ``on_stdout`` (optional) receives stdout chunks as they arrive, so a caller can
        tee a live log. Backends that cannot stream may ignore it; the returned
        ExecResult is authoritative either way.
        """
        raise NotImplementedError

    # --- filesystem -------------------------------------------------------
    def ensure_remote_dir(self, remote_path: str) -> None:
        raise NotImplementedError

    def push_file(self, local_path: Path, remote_path: str) -> None:
        raise NotImplementedError

    def pull_file(self, remote_path: str, local_path: Path) -> None:
        raise NotImplementedError

    def push_files(self, local_root: Path, remote_root: str, rel_paths: list[str]) -> None:
        """Push several relative files under one root.

        Backends may override with a streaming archive implementation. The default preserves the
        single-file semantics exactly.
        """
        for rel in rel_paths:
            self.push_file(local_root / _rel_to_path(rel), self.remote_join(remote_root, rel))

    def pull_files(self, remote_root: str, local_root: Path, rel_paths: list[str]) -> None:
        """Pull several relative files under one root. See ``push_files``."""
        for rel in rel_paths:
            self.pull_file(self.remote_join(remote_root, rel), local_root / _rel_to_path(rel))

    def delete_remote(self, remote_path: str) -> None:
        raise NotImplementedError

    def remote_path_exists(self, remote_path: str) -> bool:
        """Whether ``remote_path`` exists on the remote. Used only by the
        vanished-root safety guard, so it must be precise: return True only when
        existence is confirmed."""
        raise NotImplementedError

    def manifest(
        self,
        remote_root: str,
        exclude_patterns: Iterable[str],
        hash_below_bytes: int = 0,
    ) -> Manifest:
        raise NotImplementedError

    def hash_file(self, remote_path: str) -> str:
        """SHA-256 (hex) of a single remote file, regardless of size. Lets `run`'s pullback
        strongly compare an output candidate even when it's above the manifest hash cap (so the
        idempotent skip-if-identical works for >64 MB outputs too)."""
        raise NotImplementedError

    # --- diagnostics ------------------------------------------------------
    def probe(self) -> ProbeResult:
        raise NotImplementedError

    def sample_load(self) -> float | None:
        """Best-effort current CPU congestion as a 0-100 percentage.

        Returns None when the backend can't measure it. Used only by --auto load
        balancing; must never raise (a failure yields None = "unknown").
        """
        return None

    # --- versioned runner groundwork (coordination design Step 3) --------
    def install_versioned_runner(
        self, source: bytes, remote_path: str, expected_sha256: str
    ) -> None:
        """Install a content-addressed self-contained runner from an RRFRAME2 payload."""
        raise NotImplementedError

    def runner_rpc(self, runner_path: str, state_root: str, request_frame: bytes) -> bytes:
        """Invoke one short-lived versioned-runner RPC and return its framed response."""
        raise NotImplementedError

    def runner_stream_argv(
        self, runner_path: str, state_root: str, operation: str,
        arguments: list[str] | None = None,
    ) -> list[str]:
        """Build a no-shell argv for directly piping one runner process to another."""
        raise NotImplementedError

    # --- project-less helpers (fleet mode) --------------------------------
    def native_join(self, *parts: str) -> str:
        """Join path parts in the remote's native form (default POSIX).

        Preserves a leading separator on the FIRST part so an absolute root (e.g. a
        ``/tmp/...`` temp dir from ``remote_temp_dir``) stays absolute; only the
        separators *between* parts are collapsed. (The old ``strip('/')`` on every
        part silently turned ``/tmp/x`` + ``in`` into the relative ``tmp/x/in``,
        which broke fleet staging — inputs landed under $HOME, not the temp dir.)"""
        items = [str(p) for p in parts if p]
        if not items:
            return ""
        head = items[0].rstrip("/")
        tail = [p.strip("/") for p in items[1:]]
        return "/".join([head, *(t for t in tail if t)])

    def expand_remote(self, path: str) -> str:
        """Expand a leading ``~`` in an arbitrary (project-less) remote path to the
        remote home. Default: opaque pass-through. The SSH backends override this to
        use the home captured during ``probe()`` — so ``probe()`` must run first."""
        return path

    def remote_temp_dir(self, prefix: str = "remrun-fleet") -> str:
        """Create and return a fresh remote temp dir for staging a fleet job.
        Project-less: not under any project tree."""
        raise NotImplementedError

    def remove_remote_tree(self, remote_path: str) -> None:
        """Recursively remove a project-less remote directory (the counterpart to
        ``remote_temp_dir`` — ``delete_remote`` is file-only). Best-effort."""
        raise NotImplementedError


class LocalSimTransport(BaseTransport):
    """Local filesystem simulation transport.

    It maps the target project root to a local directory and runs commands
    locally. This is for tests and development; it exercises the full
    reconciliation engine without SSH.
    """

    def remote_project_path(self, project: ProjectContext) -> str:
        root = Path(self.device.project_root).expanduser()
        return str(root / Path(project.project_id))

    def remote_join(self, remote_root: str, rel_posix: str) -> str:
        if rel_posix in ("", "."):
            return str(Path(remote_root))
        return str(Path(remote_root) / Path(rel_posix))

    def exec(
        self,
        command: list[str],
        cwd: str,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
        path_prepend: list[str] | None = None,
        telemetry: bool = False,
        on_stdout: StreamSink | None = None,
    ) -> ExecResult:
        merged_env = None
        if env or path_prepend:
            merged_env = {**os.environ, **(env or {})}
            if path_prepend:
                expanded = [str(Path(p).expanduser()) for p in path_prepend]
                existing = merged_env.get("PATH", "")
                merged_env["PATH"] = os.pathsep.join([*expanded, existing]) if existing \
                    else os.pathsep.join(expanded)
        Path(cwd).mkdir(parents=True, exist_ok=True)
        try:
            if on_stdout is None:
                proc = subprocess.run(
                    command,
                    cwd=cwd,
                    text=True,
                    capture_output=True,
                    check=False,
                    env=merged_env,
                    timeout=timeout,
                    creationflags=_NO_WINDOW,
                )
                return ExecResult(proc.returncode, proc.stdout, proc.stderr)
            # Binary pipes + the shared streamer, so the sim exercises the same
            # incremental-output path the SSH backends take.
            proc = subprocess.Popen(
                command,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=merged_env,
                **_stream_spawn_kwargs(),
            )
            code, out, err = _stream_process(
                proc, on_stdout, timeout, process_group=True
            )
            return ExecResult(code, out, err)
        except FileNotFoundError as exc:
            raise TransportError(f"command not found: {command[0]}: {exc}") from exc

    def ensure_remote_dir(self, remote_path: str) -> None:
        Path(remote_path).mkdir(parents=True, exist_ok=True)

    def push_file(self, local_path: Path, remote_path: str) -> None:
        def fill(tmp: Path) -> None:
            shutil.copyfile(local_path, tmp)
            shutil.copystat(local_path, tmp)   # preserve mtime/mode (like copy2)
        _atomic_write_local(Path(remote_path), fill)

    def pull_file(self, remote_path: str, local_path: Path) -> None:
        src = Path(remote_path)

        def fill(tmp: Path) -> None:
            shutil.copyfile(src, tmp)
            shutil.copystat(src, tmp)
        _atomic_write_local(local_path, fill)

    def push_files(self, local_root: Path, remote_root: str, rel_paths: list[str]) -> None:
        if len(rel_paths) < 2:
            return super().push_files(local_root, remote_root, rel_paths)
        for rel in rel_paths:
            self.push_file(local_root / _rel_to_path(rel), self.remote_join(remote_root, rel))

    def pull_files(self, remote_root: str, local_root: Path, rel_paths: list[str]) -> None:
        if len(rel_paths) < 2:
            return super().pull_files(remote_root, local_root, rel_paths)
        for rel in rel_paths:
            self.pull_file(self.remote_join(remote_root, rel), local_root / _rel_to_path(rel))

    def delete_remote(self, remote_path: str) -> None:
        Path(remote_path).unlink(missing_ok=True)

    def remote_path_exists(self, remote_path: str) -> bool:
        return Path(remote_path).exists()

    def manifest(
        self,
        remote_root: str,
        exclude_patterns: Iterable[str],
        hash_below_bytes: int = 0,
    ) -> Manifest:
        return build_manifest(
            Path(remote_root),
            exclude_patterns,
            hash_below_bytes=hash_below_bytes or None,
        )

    def hash_file(self, remote_path: str) -> str:
        return sha256_file(Path(remote_path))

    def probe(self) -> ProbeResult:
        return ProbeResult(reachable=True, address="localhost", detail="local-sim", remote_os="posix")

    def install_versioned_runner(
        self, source: bytes, remote_path: str, expected_sha256: str
    ) -> None:
        header, payload = decode_file_frame(encode_file_frame(
            source, transfer_id=f"runner-{expected_sha256}", mode=0o700))
        if header["sha256"] != expected_sha256:
            raise TransportError("versioned runner source digest mismatch")
        dest = Path(remote_path)
        if dest.is_symlink():
            raise TransportError(f"refusing symlinked runner destination: {dest}")
        if dest.exists() and sha256_file(dest) == expected_sha256:
            return

        def fill(tmp: Path) -> None:
            tmp.write_bytes(payload)
            try:
                tmp.chmod(0o700)
            except OSError:
                pass

        _atomic_write_local(dest, fill)

    def runner_rpc(self, runner_path: str, state_root: str, request_frame: bytes) -> bytes:
        proc = subprocess.run(
            [sys.executable, runner_path, "rpc", state_root],
            input=request_frame,
            capture_output=True,
            check=False,
            creationflags=_NO_WINDOW,
        )
        if proc.returncode != 0:
            raise TransportError(
                f"versioned runner RPC failed: {proc.stderr.decode('utf-8', 'replace').strip()}"
            )
        return proc.stdout

    def runner_stream_argv(
        self, runner_path: str, state_root: str, operation: str,
        arguments: list[str] | None = None,
    ) -> list[str]:
        return [sys.executable, runner_path, operation, state_root, *(arguments or [])]

    def native_join(self, *parts: str) -> str:
        cleaned = [str(p) for p in parts if p]
        return str(Path(*cleaned)) if cleaned else ""

    def expand_remote(self, path: str) -> str:
        return str(Path(path).expanduser())

    def remote_temp_dir(self, prefix: str = "remrun-fleet") -> str:
        base = Path(self.device.cache_root or tempfile.gettempdir())
        d = base / "fleet" / f"{prefix}-{uuid.uuid4().hex[:8]}"
        d.mkdir(parents=True, exist_ok=True)
        return str(d)

    def remove_remote_tree(self, remote_path: str) -> None:
        shutil.rmtree(remote_path, ignore_errors=True)


def _cancel_list(cancel: dict, key: str) -> list[str]:
    """Config value as a list of non-empty strings."""
    raw = cancel.get(key, []) if isinstance(cancel, dict) else []
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, Iterable):
        return []
    return [str(item) for item in raw if str(item)]


def _regex_or(patterns: list[str]) -> str:
    return "|".join(f"(?:{p})" for p in patterns)


def _posix_cancel_script(cancel: dict) -> str:
    parts: list[str] = []
    for pattern in _cancel_list(cancel, "process_patterns"):
        parts.append(f"pkill -f {shlex.quote(pattern)} 2>/dev/null || true")
    for lock_path in _cancel_list(cancel, "lock_paths"):
        parts.append(f"rm -rf -- {shlex.quote(lock_path)} 2>/dev/null || true")
    if not parts:
        parts.append(":")
    parts.append("echo cancelled")
    return "; ".join(parts)


def _powershell_cancel_script(cancel: dict) -> str:
    parts: list[str] = []
    native_patterns = _cancel_list(cancel, "process_patterns")
    if native_patterns:
        parts.extend([
            "$remrunCancelPattern = " + _ps_squote(_regex_or(native_patterns)),
            "Get-CimInstance Win32_Process | "
            "Where-Object { $_.CommandLine -and ($_.CommandLine -match $remrunCancelPattern) } "
            "| ForEach-Object { Stop-Process -Id $_.ProcessId -Force "
            "-ErrorAction SilentlyContinue }",
        ])
    for lock_path in _cancel_list(cancel, "lock_paths"):
        parts.append(
            "Remove-Item -LiteralPath " + _ps_squote(lock_path)
            + " -Recurse -Force -ErrorAction SilentlyContinue"
        )
    wsl_commands: list[str] = []
    for pattern in _cancel_list(cancel, "wsl_process_patterns"):
        wsl_commands.append(f"pkill -f {shlex.quote(pattern)} 2>/dev/null || true")
    for lock_path in _cancel_list(cancel, "wsl_lock_paths"):
        wsl_commands.append(f"rm -rf -- {shlex.quote(lock_path)} 2>/dev/null || true")
    for command in wsl_commands:
        parts.append("try { wsl.exe -- bash -lc " + _ps_squote(command) + " } catch {}")
    if not parts:
        parts.append("$null = $true")
    parts.append("Write-Output cancelled")
    return "; ".join(parts)


def _posix_workers_running_script(cancel: dict) -> str:
    parts = ["found=0"]
    for pattern in _cancel_list(cancel, "process_patterns"):
        parts.append(
            "ps ax -o pid= -o command= | "
            f"grep -E -- {shlex.quote(pattern)} | grep -v grep | "
            "grep -v __REMRUN_WORKERS__ | grep -q . && found=1 || true"
        )
    parts.append('echo "__REMRUN_WORKERS__$found"')
    return "; ".join(parts)


def _powershell_workers_running_script(cancel: dict) -> str:
    parts = ["$found = $false"]
    native_patterns = _cancel_list(cancel, "process_patterns")
    if native_patterns:
        parts.extend([
            "$remrunWorkerPattern = " + _ps_squote(_regex_or(native_patterns)),
            "try { if (Get-CimInstance Win32_Process | "
            "Where-Object { $_.CommandLine -and ($_.CommandLine -match $remrunWorkerPattern) } "
            "| Select-Object -First 1) { $found = $true } } catch {}",
        ])
    for pattern in _cancel_list(cancel, "wsl_process_patterns"):
        command = (
            "ps ax -o pid= -o command= | "
            f"grep -E -- {shlex.quote(pattern)} | grep -v grep | "
            "grep -v __REMRUN_WORKERS__ | grep -q ."
        )
        parts.append(
            "try { wsl.exe -- bash -lc " + _ps_squote(command)
            + "; if ($LASTEXITCODE -eq 0) { $found = $true } } catch {}"
        )
    parts.append("Write-Output ('__REMRUN_WORKERS__' + $(if ($found) { '1' } else { '0' }))")
    return "; ".join(parts)


class _SSHCommon(BaseTransport):
    """Shared OpenSSH plumbing for the ssh-posix and ssh-powershell backends."""

    def __init__(self, device: Device) -> None:
        super().__init__(device)
        self._address: str | None = None
        self._remote_home: str | None = None

    def expand_remote(self, path: str) -> str:
        # Both ssh backends define _expand_remote (POSIX vs backslash ~ handling).
        return self._expand_remote(path)

    def _ssh_base(self, address: str, connect_timeout: int | None = None) -> list[str]:
        opts = [
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=accept-new",
        ]
        if connect_timeout:
            opts += ["-o", f"ConnectTimeout={connect_timeout}"]
        # subprocess argv bypasses shell tilde expansion. Device config is synced
        # across controllers, so keep identity paths portable instead of pinning a
        # controller's absolute home directory.
        opts += [str(Path(opt).expanduser()) if opt.startswith(("~/", "~\\")) else opt
                 for opt in self.device.ssh_opts]
        target = f"{self.device.user}@{address}" if self.device.user else address
        return ["ssh", *opts, target]

    def _run(
        self,
        argv: list[str],
        input_bytes: bytes | None = None,
        timeout: float | None = None,
        on_stdout: StreamSink | None = None,
    ) -> subprocess.CompletedProcess:
        """Run an ssh subprocess in binary mode. Mockable in tests.

        With ``on_stdout`` the process is streamed (see ``_stream_process``) instead of
        buffered to exit; the return shape is identical either way, so every caller and
        existing test mock is unaffected. Transfer calls keep the buffered path — they
        pass tar payloads through this same seam and have no use for partial output.
        """
        try:
            if on_stdout is None:
                return subprocess.run(
                    argv,
                    input=input_bytes,
                    capture_output=True,
                    timeout=timeout,
                    check=False,
                    creationflags=_NO_WINDOW,
                )
            proc = subprocess.Popen(
                argv,
                stdin=subprocess.PIPE if input_bytes is not None else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                **_stream_spawn_kwargs(),
            )
            if input_bytes is not None:
                proc.stdin.write(input_bytes)
                proc.stdin.close()
            code, out, err = _stream_process(
                proc, on_stdout, timeout, process_group=True
            )
            return subprocess.CompletedProcess(argv, code, out.encode("utf-8", "replace"),
                                               err.encode("utf-8", "replace"))
        except FileNotFoundError as exc:
            raise TransportError(f"ssh executable not found: {exc}") from exc
        except subprocess.TimeoutExpired as exc:
            # A blackholed / very slow candidate must not abort --auto failover:
            # surface the timeout as a typed transport error so probe() and
            # sample_load() can treat this candidate as unreachable/unknown and
            # move on to the next one instead of letting the run crash.
            raise TransportError(f"ssh timed out after {exc.timeout}s") from exc

    def _address_or_resolve(self) -> str:
        if self._address is None:
            probe = self.probe()
            if not probe.reachable or not probe.address:
                raise TransportError(f"{self.device.name} unreachable: {probe.detail}")
        return self._address  # type: ignore[return-value]

    def _remote(self, address: str, script: str, input_bytes: bytes | None = None,
                timeout: float | None = None,
                on_stdout: StreamSink | None = None) -> subprocess.CompletedProcess:
        return self._run([*self._ssh_base(address), script], input_bytes, timeout,
                         on_stdout=on_stdout)

    def kill_workers(self) -> bool:
        """Best-effort: run this device's configured cancel actions.

        The core has no built-in worker names, model names, or lock paths. Users
        may configure native process regexes, WSL process regexes, and lock paths
        under `[devices.<name>.cancel]`. Missing/empty config is a harmless no-op.
        Returns True iff the remote command ran cleanly enough for best-effort
        cancellation (False on transport errors).
        """
        cancel = self.device.cancel or {}
        try:
            if self.device.kind == "ssh-powershell":
                script = _powershell_cancel_script(cancel)
                # -EncodedCommand sidesteps quoting (raw _remote mangles braces/pipes).
                runner = self._ps_remote
            else:
                script = _posix_cancel_script(cancel)
                runner = self._remote
            proc = runner(self._address_or_resolve(), script, timeout=20)
            # Best-effort; key success on the sentinel the script prints when it runs to completion.
            return b"cancelled" in (proc.stdout or b"") or proc.returncode == 0
        except (TransportError, OSError, subprocess.SubprocessError):
            return False

    def workers_running(self) -> bool:
        """Best-effort health probe using this device's configured worker patterns."""
        cancel = self.device.cancel or {}
        if not _cancel_list(cancel, "process_patterns") \
                and not _cancel_list(cancel, "wsl_process_patterns"):
            return False
        try:
            if self.device.kind == "ssh-powershell":
                script = _powershell_workers_running_script(cancel)
                runner = self._ps_remote
            else:
                script = _posix_workers_running_script(cancel)
                runner = self._remote
            proc = runner(self._address_or_resolve(), script, timeout=20)
            out = (proc.stdout or b"").decode("utf-8", "replace")
            return "__REMRUN_WORKERS__1" in out
        except (TransportError, OSError, subprocess.SubprocessError):
            return False


_SET_MTIME_PROG = "import os,sys;t=float(sys.argv[2]);os.utime(sys.argv[1],(t,t))"
_GET_MTIME_PROG = "import os,sys;print(os.stat(sys.argv[1]).st_mtime_ns)"

# Remote-side atomic commit (POSIX): set mtime on the temp, fsync it, atomically replace
# the destination, then best-effort fsync the parent dir. Run as `python -c PROG tmp dest ns`.
_ATOMIC_COMMIT_PROG = (
    "import os,sys\n"
    "tmp,dest,ns=sys.argv[1],sys.argv[2],int(sys.argv[3])\n"
    "os.utime(tmp,ns=(ns,ns))\n"
    "fd=os.open(tmp,os.O_RDONLY)\n"
    "try:\n os.fsync(fd)\n"
    "finally:\n os.close(fd)\n"
    "os.replace(tmp,dest)\n"
    "try:\n"
    " d=os.open(os.path.dirname(dest) or '.',os.O_RDONLY)\n"
    " try:\n  os.fsync(d)\n"
    " finally:\n  os.close(d)\n"
    "except OSError:\n pass\n"
)


def _fsync_dir(directory: Path) -> None:
    """Best-effort fsync of a directory so a rename is durable. No-op where unsupported
    (e.g. Windows cannot fsync a directory handle this way)."""
    try:
        fd = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except (OSError, ValueError, AttributeError):
        pass


def _atomic_write_local(dest: Path, fill) -> None:
    """Durably + atomically write a LOCAL file. ``fill(tmp_path)`` fully populates a temp
    file beside ``dest`` (and sets its mtime); we then fsync it and ``os.replace`` it onto
    ``dest`` (atomic on the same filesystem). An interrupted/failed write thus never leaves
    a partial or truncated ``dest`` — the prior contents survive."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.parent / f".remrun-tmp-{dest.name}-{uuid.uuid4().hex}.tmp"
    try:
        fill(tmp)
        # fsync via a writable handle (Windows os.fsync needs write access, unlike POSIX).
        with tmp.open("r+b") as f:
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, dest)
        _fsync_dir(dest.parent)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _ps_squote(value: str) -> str:
    """Quote a string as a PowerShell single-quoted literal."""
    return "'" + value.replace("'", "''") + "'"


def _ps_encode(script: str) -> str:
    """Encode a PowerShell script for `-EncodedCommand` (base64 UTF-16LE)."""
    return base64.b64encode(script.encode("utf-16-le")).decode("ascii")


def _safe_rel(rel: str) -> str:
    """A POSIX relative path safe for archive member names and root joins."""
    parts = str(rel).replace("\\", "/").split("/")
    if not parts or str(rel).startswith(("/", "\\")):
        raise TransportError(f"unsafe transfer path: {rel!r}")
    if any(p in ("", ".", "..") or ":" in p for p in parts):
        raise TransportError(f"unsafe transfer path: {rel!r}")
    return "/".join(parts)


def _rel_to_path(rel: str) -> Path:
    return Path(*_safe_rel(rel).split("/"))


def _request_b64(payload: dict) -> str:
    return base64.b64encode(json.dumps(payload, sort_keys=True).encode("utf-8")).decode("ascii")


def _tar_bytes_from_local(local_root: Path, rel_paths: list[str]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for rel in rel_paths:
            safe = _safe_rel(rel)
            src = local_root / _rel_to_path(safe)
            st = src.stat()
            info = tarfile.TarInfo(safe)
            info.size = st.st_size
            info.mode = st.st_mode & 0o777
            info.mtime = st.st_mtime_ns / 1_000_000_000.0
            with src.open("rb") as f:
                tf.addfile(info, f)
    return buf.getvalue()


def _extract_tar_to_local(data: bytes, local_root: Path) -> None:
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as tf:
        for member in tf.getmembers():
            rel = _safe_rel(member.name)
            if not member.isfile():
                continue
            dest = local_root / _rel_to_path(rel)
            src = tf.extractfile(member)

            def fill(tmp: Path) -> None:
                with tmp.open("wb") as out:
                    if src is not None:
                        shutil.copyfileobj(src, out)
                ns = int(float(member.mtime) * 1_000_000_000)
                try:
                    os.utime(tmp, ns=(ns, ns))
                except (ValueError, OSError):
                    pass
            _atomic_write_local(dest, fill)


_TAR_EXTRACT_PROG = r"""
import base64, io, json, os, shutil, sys, tarfile, tempfile
req = json.loads(base64.b64decode(sys.argv[1]).decode("utf-8"))
root = req["root"]
os.makedirs(root, exist_ok=True)
def safe(name):
    name = str(name).replace("\\", "/")
    parts = name.split("/")
    if name.startswith("/") or any(p in ("", ".", "..") or ":" in p for p in parts):
        raise ValueError("unsafe archive path: " + name)
    return name
with tarfile.open(fileobj=io.BytesIO(sys.stdin.buffer.read()), mode="r:*") as tf:
    for member in tf.getmembers():
        rel = safe(member.name)
        if not member.isfile():
            continue
        final = os.path.join(root, *rel.split("/"))
        os.makedirs(os.path.dirname(final), exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".remrun-tar-", suffix=".tmp",
                                  dir=os.path.dirname(final))
        try:
            with os.fdopen(fd, "wb") as out:
                src = tf.extractfile(member)
                if src is not None:
                    shutil.copyfileobj(src, out)
                out.flush()
                os.fsync(out.fileno())
            ns = int(float(member.mtime) * 1000000000)
            os.utime(tmp, ns=(ns, ns))
            os.replace(tmp, final)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
"""


_TAR_CREATE_PROG = r"""
import base64, json, os, sys, tarfile
req = json.loads(base64.b64decode(sys.argv[1]).decode("utf-8"))
root = req["root"]
def safe(name):
    name = str(name).replace("\\", "/")
    parts = name.split("/")
    if name.startswith("/") or any(p in ("", ".", "..") or ":" in p for p in parts):
        raise ValueError("unsafe archive path: " + name)
    return name
with tarfile.open(fileobj=sys.stdout.buffer, mode="w|") as tf:
    for name in req["paths"]:
        rel = safe(name)
        path = os.path.join(root, *rel.split("/"))
        st = os.stat(path)
        info = tarfile.TarInfo(rel)
        info.size = st.st_size
        info.mode = st.st_mode & 0o777
        info.mtime = st.st_mtime_ns / 1000000000.0
        with open(path, "rb") as f:
            tf.addfile(info, f)
"""


# Bootstrap only: verify a complete RRFRAME2 file payload before atomically installing the
# content-addressed remote helper. Kept self-contained because no remrun package exists remotely.
_INSTALL_RUNNER_PROG = r"""
import base64,binascii,hashlib,json,os,sys,tempfile
dest,expected=sys.argv[1],sys.argv[2]
data=sys.stdin.buffer.read()
nl=data.find(b"\n")
if nl<0: raise ValueError("no frame header line")
parts=data[:nl].split(b" ")
if len(parts)!=3 or parts[0]!=b"RRFRAME2": raise ValueError("bad frame header")
hlen,blen=int(parts[1]),int(parts[2])
rest=data[nl+1:]
if hlen<0 or blen<0 or hlen>1048576 or len(rest)!=hlen+blen:
    raise ValueError("frame size mismatch")
header=json.loads(rest[:hlen].decode("utf-8"))
body=base64.b64decode(rest[hlen:],validate=True)
digest=hashlib.sha256(body).hexdigest()
if header.get("kind")!="file" or header.get("v")!=2:
    raise ValueError("not a v2 file frame")
if len(body)!=header.get("decoded_length") or digest!=header.get("sha256"):
    raise ValueError("payload integrity mismatch")
if digest!=expected: raise ValueError("source digest mismatch")
parent=os.path.dirname(dest) or "."
os.makedirs(parent,exist_ok=True)
if os.path.islink(dest): raise ValueError("runner destination is a symlink")
reused=False
if os.path.isfile(dest):
    h=hashlib.sha256()
    with open(dest,"rb") as f:
        for chunk in iter(lambda:f.read(1048576),b""): h.update(chunk)
    reused=h.hexdigest()==expected
if not reused:
    fd,tmp=tempfile.mkstemp(prefix=".remrun-runner-",suffix=".tmp",dir=parent)
    try:
        with os.fdopen(fd,"wb") as f:
            f.write(body);f.flush();os.fsync(f.fileno())
        try: os.chmod(tmp,0o700)
        except OSError: pass
        os.replace(tmp,dest)
        try:
            d=os.open(parent,os.O_RDONLY)
            try: os.fsync(d)
            finally: os.close(d)
        except OSError: pass
    except BaseException:
        try: os.unlink(tmp)
        except OSError: pass
        raise
sys.stdout.write(json.dumps({"path":dest,"sha256":digest,"reused":reused}))
"""


class SSHPosixTransport(_SSHCommon):
    """POSIX backend over OpenSSH.

    Transfers stream file bytes through ``ssh`` (no rsync/scp dependency, so it
    works from a Windows controller too) and preserve mtimes so unchanged files
    classify as "same". The remote manifest is produced by piping the
    self-contained ``remote/runner.py`` into the remote ``python3``.
    """

    # --- path mapping -----------------------------------------------------
    def _expand_remote(self, path: str) -> str:
        """Expand a leading ``~`` using the remote $HOME captured during probe.

        Remote paths are shlex-quoted before use, which would otherwise prevent
        the remote shell from expanding ``~`` itself.
        """
        if self._remote_home and (path == "~" or path.startswith("~/")):
            return self._remote_home.rstrip("/") + path[1:]
        return path

    def remote_project_path(self, project: ProjectContext) -> str:
        root = self._expand_remote(self.device.project_root).rstrip("/")
        return f"{root}/{project.project_id}"

    def remote_join(self, remote_root: str, rel_posix: str) -> str:
        if rel_posix in ("", "."):
            return remote_root.rstrip("/")
        return f"{remote_root.rstrip('/')}/{rel_posix.strip('/')}"

    # --- diagnostics ------------------------------------------------------
    def probe(self) -> ProbeResult:
        last_detail = "no address candidates configured"
        for address in self.device.all_addresses():
            try:
                proc = self._run(
                    [*self._ssh_base(address, connect_timeout=8),
                     'echo remrun-ok && uname -s && printf %s "$HOME"'],
                    timeout=20,
                )
            except TransportError as exc:
                last_detail = str(exc)
                continue
            if proc.returncode == 0 and b"remrun-ok" in proc.stdout:
                lines = proc.stdout.decode("utf-8", "replace").splitlines()
                remote_os = lines[1].strip().lower() if len(lines) > 1 else None
                self._remote_home = lines[2].strip() if len(lines) > 2 else None
                self._address = address
                return ProbeResult(reachable=True, address=address,
                                   detail="ssh ok", remote_os=remote_os)
            last_detail = (proc.stderr or proc.stdout).decode("utf-8", "replace").strip() \
                or f"exit {proc.returncode}"
        return ProbeResult(reachable=False, address=None, detail=last_detail)

    def sample_load(self) -> float | None:
        """Current CPU utilization % (busy = 100 - idle).

        macOS: `top -l 2 -n 0` (the 2nd sample is an accurate interval reading).
        Falls back to 1-min load-average-per-core on systems without that `top`
        (e.g. Linux) — coarser, since load average is demand, not utilization.
        """
        try:
            address = self._address_or_resolve()
        except TransportError:
            return None
        try:
            proc = self._run([*self._ssh_base(address, connect_timeout=8),
                              "top -l 2 -n 0 | grep -i 'CPU usage' | tail -1"], timeout=20)
        except TransportError:
            return None
        if proc.returncode == 0:
            parts = proc.stdout.decode("utf-8", "replace").replace("%", "").split()
            if "idle" in parts:
                try:
                    return round(100.0 - float(parts[parts.index("idle") - 1]), 1)
                except (ValueError, IndexError):
                    pass
        py = self.device.remote_python or "python3"
        prog = "import os;print(round(os.getloadavg()[0]/(os.cpu_count() or 1)*100,1))"
        try:
            proc = self._run([*self._ssh_base(address, connect_timeout=6),
                              f"{shlex.quote(py)} -c {shlex.quote(prog)}"], timeout=15)
        except TransportError:
            return None
        if proc.returncode != 0:
            return None
        try:
            return float(proc.stdout.decode("utf-8", "replace").strip().split()[-1])
        except (ValueError, IndexError):
            return None

    def install_versioned_runner(
        self, source: bytes, remote_path: str, expected_sha256: str
    ) -> None:
        address = self._address_or_resolve()
        frame = encode_file_frame(
            source, transfer_id=f"runner-{expected_sha256}", mode=0o700)
        script = (
            f"{shlex.quote(self.device.remote_python)} -c "
            f"{shlex.quote(_INSTALL_RUNNER_PROG)} {shlex.quote(remote_path)} "
            f"{shlex.quote(expected_sha256)}"
        )
        proc = self._remote(address, script, input_bytes=frame, timeout=60)
        if proc.returncode != 0:
            raise TransportError(
                "versioned runner install failed: "
                + proc.stderr.decode("utf-8", "replace").strip()
            )

    def runner_rpc(self, runner_path: str, state_root: str, request_frame: bytes) -> bytes:
        address = self._address_or_resolve()
        script = (
            f"{shlex.quote(self.device.remote_python)} {shlex.quote(runner_path)} "
            f"rpc {shlex.quote(state_root)}"
        )
        proc = self._remote(address, script, input_bytes=request_frame, timeout=60)
        if proc.returncode != 0:
            raise TransportError(
                "versioned runner RPC failed: "
                + proc.stderr.decode("utf-8", "replace").strip()
            )
        return proc.stdout

    def runner_stream_argv(
        self, runner_path: str, state_root: str, operation: str,
        arguments: list[str] | None = None,
    ) -> list[str]:
        address = self._address_or_resolve()
        values = [runner_path, operation, state_root, *(arguments or [])]
        script = " ".join([
            shlex.quote(self.device.remote_python),
            *(shlex.quote(value) for value in values),
        ])
        return [*self._ssh_base(address), script]

    # --- execution --------------------------------------------------------
    def exec(self, command, cwd, env=None, timeout=None, path_prepend=None,  # noqa: ANN001
             telemetry=False, on_stdout=None) -> ExecResult:
        address = self._address_or_resolve()
        parts: list[str] = []
        for key, value in (env or {}).items():
            parts.append(f"export {key}={shlex.quote(self._expand_remote(str(value)))}")
        if path_prepend:
            joined = ":".join(shlex.quote(self._expand_remote(p)) for p in path_prepend)
            parts.append(f'export PATH={joined}:"$PATH"')
        parts.append(f"cd {shlex.quote(cwd)} && {shlex.join(command)}")
        inner = "; ".join(parts)

        # A login shell makes the remote PATH match the user's normal environment
        # (e.g. Homebrew's Rscript), which a bare non-interactive ssh shell omits.
        shell_flag = "-lc" if self.device.login_shell else "-c"
        if telemetry:
            # Wrap the (unchanged) shell invocation in the stdlib rusage sampler.
            measured = " ".join(shlex.quote(a) for a in (self.device.shell, shell_flag, inner))
            script = (
                f"{shlex.quote(self.device.remote_python)} -c "
                f"{shlex.quote(_POSIX_TELEMETRY_SAMPLER)} -- {measured}"
            )
        elif self.device.login_shell:
            script = f"{self.device.shell} -lc {shlex.quote(inner)}"
        else:
            script = inner

        # Streaming leaves the telemetry contract untouched: the sampler's sentinel
        # travels on stderr, which is drained separately and parsed below exactly as
        # in the buffered path.
        proc = self._remote(address, script, timeout=timeout, on_stdout=on_stdout)
        if proc.returncode == 255:
            raise TransportError(
                f"ssh connection failed (exit 255): "
                f"{proc.stderr.decode('utf-8', 'replace').strip()}"
            )
        stderr = proc.stderr.decode("utf-8", "replace")
        telem = None
        if telemetry:
            stderr, telem = _extract_telemetry(stderr)
        return ExecResult(proc.returncode, proc.stdout.decode("utf-8", "replace"), stderr, telem)

    # --- filesystem -------------------------------------------------------
    def ensure_remote_dir(self, remote_path: str) -> None:
        address = self._address_or_resolve()
        proc = self._remote(address, f"mkdir -p {shlex.quote(remote_path)}")
        if proc.returncode != 0:
            raise TransportError(f"mkdir -p failed: {proc.stderr.decode('utf-8', 'replace')}")

    def push_file(self, local_path: Path, remote_path: str) -> None:
        address = self._address_or_resolve()
        parent = posixpath.dirname(remote_path) or "."
        mtime_ns = local_path.stat().st_mtime_ns
        # Stream into a temp beside the final path, then atomically commit (replace) it, so
        # an interrupted SSH stream never leaves a partial/truncated destination file.
        tmp = f"{remote_path}.remrun-tmp-{uuid.uuid4().hex}.tmp"
        # Bind the temp path to a shell var so the EXIT-cleanup trap can reference it safely.
        # Embedding shlex.quote(tmp) (single-quoted) *inside* the single-quoted trap command
        # breaks on paths with spaces/commas (single quotes can't nest) — a PDF named
        # A filename with spaces and commas produced `zsh: trap: undefined signal` and the push
        # failed. `t=<path>; trap 'rm -f "$t"' EXIT` defers expansion to exit time, when $t is set.
        script = (
            f"t={shlex.quote(tmp)} && "
            f"mkdir -p {shlex.quote(parent)} && "
            "trap 'rm -f \"$t\"' EXIT && "
            'cat > "$t" && '
            f"{shlex.quote(self.device.remote_python)} -c {shlex.quote(_ATOMIC_COMMIT_PROG)} "
            f'"$t" {shlex.quote(remote_path)} {mtime_ns} && '
            "trap - EXIT"
        )
        data = local_path.read_bytes()
        proc = self._remote(address, script, input_bytes=data)
        if proc.returncode != 0:
            raise TransportError(
                f"push {remote_path} failed: {proc.stderr.decode('utf-8', 'replace')}"
            )

    def push_files(self, local_root: Path, remote_root: str, rel_paths: list[str]) -> None:
        if len(rel_paths) < 2:
            return super().push_files(local_root, remote_root, rel_paths)
        address = self._address_or_resolve()
        req = _request_b64({"root": remote_root, "paths": [_safe_rel(p) for p in rel_paths]})
        script = (
            f"{shlex.quote(self.device.remote_python)} -c "
            f"{shlex.quote(_TAR_EXTRACT_PROG)} {shlex.quote(req)}"
        )
        proc = self._remote(address, script, input_bytes=_tar_bytes_from_local(local_root, rel_paths))
        if proc.returncode != 0:
            raise TransportError(
                f"push archive to {remote_root} failed: {proc.stderr.decode('utf-8', 'replace')}"
            )

    def pull_file(self, remote_path: str, local_path: Path) -> None:
        address = self._address_or_resolve()
        proc = self._remote(address, f"cat {shlex.quote(remote_path)}")
        if proc.returncode != 0:
            raise TransportError(
                f"pull {remote_path} failed: {proc.stderr.decode('utf-8', 'replace')}"
            )
        data = proc.stdout
        # Best-effort remote mtime so large (unhashed) files stay "same" for `run`.
        ns: int | None = None
        mt = self._remote(
            address,
            f"{shlex.quote(self.device.remote_python)} -c {shlex.quote(_GET_MTIME_PROG)} "
            f"{shlex.quote(remote_path)}",
        )
        if mt.returncode == 0:
            try:
                ns = int(mt.stdout.decode("utf-8", "replace").strip())
            except ValueError:
                ns = None

        def fill(tmp: Path) -> None:
            tmp.write_bytes(data)
            if ns is not None:
                try:
                    os.utime(tmp, ns=(ns, ns))
                except OSError:
                    pass
        _atomic_write_local(local_path, fill)

    def pull_files(self, remote_root: str, local_root: Path, rel_paths: list[str]) -> None:
        if len(rel_paths) < 2:
            return super().pull_files(remote_root, local_root, rel_paths)
        address = self._address_or_resolve()
        req = _request_b64({"root": remote_root, "paths": [_safe_rel(p) for p in rel_paths]})
        script = (
            f"{shlex.quote(self.device.remote_python)} -c "
            f"{shlex.quote(_TAR_CREATE_PROG)} {shlex.quote(req)}"
        )
        proc = self._remote(address, script)
        if proc.returncode != 0:
            raise TransportError(
                f"pull archive from {remote_root} failed: {proc.stderr.decode('utf-8', 'replace')}"
            )
        _extract_tar_to_local(proc.stdout, local_root)

    def delete_remote(self, remote_path: str) -> None:
        address = self._address_or_resolve()
        proc = self._remote(address, f"rm -f {shlex.quote(remote_path)}")
        if proc.returncode != 0:
            raise TransportError(
                f"delete {remote_path} failed: {proc.stderr.decode('utf-8', 'replace')}"
            )

    def remote_path_exists(self, remote_path: str) -> bool:
        address = self._address_or_resolve()
        p = self._expand_remote(remote_path)
        proc = self._remote(address, f"test -e {shlex.quote(p)} && echo __REMRUN_EXISTS__")
        return proc.returncode == 0 and b"__REMRUN_EXISTS__" in proc.stdout

    def remote_temp_dir(self, prefix: str = "remrun-fleet") -> str:
        address = self._address_or_resolve()
        path = f"/tmp/{prefix}-{uuid.uuid4().hex[:8]}"
        proc = self._remote(address, f"mkdir -p {shlex.quote(path)}")
        if proc.returncode != 0:
            raise TransportError(f"remote_temp_dir failed: {proc.stderr.decode('utf-8', 'replace')}")
        return path

    def remove_remote_tree(self, remote_path: str) -> None:
        address = self._address_or_resolve()
        self._remote(address, f"rm -rf {shlex.quote(self._expand_remote(remote_path))}")

    def manifest(self, remote_root, exclude_patterns, hash_below_bytes=0) -> Manifest:  # noqa: ANN001
        address = self._address_or_resolve()
        request = base64.b64encode(json.dumps({
            "op": "manifest",
            "root": remote_root,
            "exclude": list(exclude_patterns),
            "hash_below_bytes": int(hash_below_bytes),
        }).encode("utf-8")).decode("ascii")
        # Pipe runner source to the remote python via stdin; pass the request as
        # a bare (base64, shell-safe) argv to python's "-" program.
        script = f"{shlex.quote(self.device.remote_python)} - {request}"
        proc = self._remote(address, script, input_bytes=_runner_source())
        if proc.returncode != 0:
            raise TransportError(
                f"remote manifest failed: {proc.stderr.decode('utf-8', 'replace')}"
            )
        try:
            data = json.loads(proc.stdout.decode("utf-8", "replace") or "{}")
        except json.JSONDecodeError as exc:
            raise TransportError(f"remote manifest returned invalid JSON: {exc}") from exc
        return manifest_from_json(data)

    def hash_file(self, remote_path: str) -> str:
        address = self._address_or_resolve()
        request = base64.b64encode(json.dumps(
            {"op": "hash_file", "path": remote_path}).encode("utf-8")).decode("ascii")
        script = f"{shlex.quote(self.device.remote_python)} - {request}"
        proc = self._remote(address, script, input_bytes=_runner_source())
        if proc.returncode != 0:
            raise TransportError(f"hash {remote_path} failed: "
                                 f"{proc.stderr.decode('utf-8', 'replace')}")
        return json.loads(proc.stdout.decode("utf-8", "replace") or "{}").get("sha256", "")


class SSHPowerShellTransport(_SSHCommon):
    """Windows backend over OpenSSH + PowerShell.

    Commands and filesystem ops are sent as PowerShell scripts via
    ``-EncodedCommand`` (base64 UTF-16LE), which sidesteps all cmd/pwsh quoting
    regardless of the remote default shell. File transfers stream base64 over
    stdin (push) / stdout (pull) so they are binary-safe and need no scp.

    The PowerShell transport keeps quoting and telemetry handling generic;
    deployment validation belongs in the user's own fleet notes/config.
    """

    # --- path mapping -----------------------------------------------------
    def _ps_exe(self) -> str:
        shell = self.device.shell
        return shell if shell in ("pwsh", "powershell") else "powershell"

    def _expand_remote(self, path: str) -> str:
        if self._remote_home and path[:1] == "~" and (len(path) == 1 or path[1] in "/\\"):
            return self._remote_home.rstrip("\\/") + path[1:].replace("/", "\\")
        return path

    def remote_project_path(self, project: ProjectContext) -> str:
        root = self._expand_remote(self.device.project_root).rstrip("\\/")
        return root + "\\" + project.project_id.replace("/", "\\")

    def remote_join(self, remote_root: str, rel_posix: str) -> str:
        if rel_posix in ("", "."):
            return remote_root.rstrip("\\/")
        return remote_root.rstrip("\\/") + "\\" + rel_posix.strip("/").replace("/", "\\")

    def _ps_remote(self, address: str, script: str, input_bytes: bytes | None = None,
                   timeout: float | None = None,
                   on_stdout: StreamSink | None = None) -> subprocess.CompletedProcess:
        cmd = f"{self._ps_exe()} -NoProfile -NonInteractive -EncodedCommand {_ps_encode(script)}"
        return self._remote(address, cmd, input_bytes=input_bytes, timeout=timeout,
                            on_stdout=on_stdout)

    # --- diagnostics ------------------------------------------------------
    def probe(self) -> ProbeResult:
        script = (
            "Write-Output 'remrun-ok'; "
            "Write-Output ([System.Environment]::OSVersion.Platform.ToString()); "
            "Write-Output $env:USERPROFILE"
        )
        encoded = f"{self._ps_exe()} -NoProfile -NonInteractive -EncodedCommand {_ps_encode(script)}"
        last_detail = "no address candidates configured"
        for address in self.device.all_addresses():
            try:
                proc = self._run([*self._ssh_base(address, connect_timeout=8), encoded], timeout=30)
            except TransportError as exc:
                last_detail = str(exc)
                continue
            if proc.returncode == 0 and b"remrun-ok" in proc.stdout:
                lines = [ln.strip() for ln in proc.stdout.decode("utf-8", "replace").splitlines()
                         if ln.strip()]
                self._remote_home = lines[2] if len(lines) > 2 else None
                self._address = address
                return ProbeResult(reachable=True, address=address, detail="ssh ok",
                                   remote_os="windows")
            last_detail = (proc.stderr or proc.stdout).decode("utf-8", "replace").strip() \
                or f"exit {proc.returncode}"
        return ProbeResult(reachable=False, address=None, detail=last_detail)

    def sample_load(self) -> float | None:
        """Current CPU utilization % via a 1-second performance-counter sample
        (falls back to the coarser Win32_Processor LoadPercentage snapshot)."""
        try:
            address = self._address_or_resolve()
        except TransportError:
            return None
        script = (
            "try { $v = (Get-Counter '\\Processor(_Total)\\% Processor Time' "
            "-SampleInterval 1 -MaxSamples 2 -ErrorAction Stop).CounterSamples[-1].CookedValue } "
            "catch { $v = (Get-CimInstance Win32_Processor | "
            "Measure-Object -Property LoadPercentage -Average).Average }; "
            "Write-Output ([math]::Round([double]$v,1))"
        )
        try:
            proc = self._ps_remote(address, script, timeout=20)
        except TransportError:
            return None
        if proc.returncode != 0:
            return None
        try:
            return float(proc.stdout.decode("utf-8", "replace").strip().split()[-1])
        except (ValueError, IndexError):
            return None

    def install_versioned_runner(
        self, source: bytes, remote_path: str, expected_sha256: str
    ) -> None:
        address = self._address_or_resolve()
        frame = encode_file_frame(
            source, transfer_id=f"runner-{expected_sha256}", mode=0o700)
        script = (
            "$ErrorActionPreference='Stop'\n"
            f"& {_ps_squote(self.device.remote_python)} '-c' "
            f"{_ps_squote(_INSTALL_RUNNER_PROG)} {_ps_squote(remote_path)} "
            f"{_ps_squote(expected_sha256)}\n"
            "exit $LASTEXITCODE"
        )
        proc = self._ps_remote(address, script, input_bytes=frame, timeout=60)
        if proc.returncode != 0:
            raise TransportError(
                "versioned runner install failed: "
                + proc.stderr.decode("utf-8", "replace").strip()
            )

    def runner_rpc(self, runner_path: str, state_root: str, request_frame: bytes) -> bytes:
        address = self._address_or_resolve()
        script = (
            "$ErrorActionPreference='Stop'\n"
            f"& {_ps_squote(self.device.remote_python)} {_ps_squote(runner_path)} "
            f"'rpc' {_ps_squote(state_root)}\n"
            "exit $LASTEXITCODE"
        )
        proc = self._ps_remote(address, script, input_bytes=request_frame, timeout=60)
        if proc.returncode != 0:
            raise TransportError(
                "versioned runner RPC failed: "
                + proc.stderr.decode("utf-8", "replace").strip()
            )
        return proc.stdout

    def runner_stream_argv(
        self, runner_path: str, state_root: str, operation: str,
        arguments: list[str] | None = None,
    ) -> list[str]:
        address = self._address_or_resolve()
        values = [runner_path, operation, state_root, *(arguments or [])]
        command = " ".join(
            [_ps_squote(self.device.remote_python),
             *(_ps_squote(value) for value in values)]
        )
        script = (
            "$ErrorActionPreference='Stop'\n"
            f"& {command}\n"
            "exit $LASTEXITCODE"
        )
        remote = f"{self._ps_exe()} -NoProfile -NonInteractive -EncodedCommand {_ps_encode(script)}"
        return [*self._ssh_base(address), remote]

    # --- execution --------------------------------------------------------
    def exec(self, command, cwd, env=None, timeout=None, path_prepend=None,  # noqa: ANN001
             telemetry=False, on_stdout=None) -> ExecResult:
        address = self._address_or_resolve()
        lines = [
            "$ErrorActionPreference = 'Stop'",
            # Preserve native command exit codes (PS 7.4 would otherwise throw).
            "$PSNativeCommandUseErrorActionPreference = $false",
        ]
        for key, value in (env or {}).items():
            lines.append(f"$env:{key} = {_ps_squote(self._expand_remote(str(value)))}")
        if path_prepend:
            joined = ";".join(self._expand_remote(p) for p in path_prepend) + ";"
            lines.append(f"$env:PATH = {_ps_squote(joined)} + $env:PATH")
        lines.append(f"Set-Location -LiteralPath {_ps_squote(self._expand_remote(cwd))}")
        cmd_tokens = " ".join(_ps_squote(c) for c in command)
        if telemetry:
            # Run the command under a Win32 Job Object sampler (peak memory + CPU
            # of the whole process tree). The sampler is ~400 lines — too large to
            # embed in a -EncodedCommand (Windows ~32KB command-line limit, the
            # WinError 206 trap) — so stage it as a file via push_file (streamed
            # over stdin, no cap) and invoke it by path. It emits the same
            # __REMRUN_TELEMETRY__ stderr sentinel as the POSIX sampler; any
            # staging failure degrades to a plain run with no telemetry.
            try:
                home = (self._remote_home or "").rstrip("\\/")
                if not home:
                    raise TransportError("remote home unknown; skipping telemetry")
                wrapper_remote = home + "\\AppData\\Local\\Temp\\remrun_win_telemetry.py"
                self.push_file(Path(__file__).parent / "_win_telemetry.py", wrapper_remote)
                py = _ps_squote(self.device.remote_python or "python")
                lines.append(f"& {py} {_ps_squote(wrapper_remote)} '--' {cmd_tokens}")
                lines.append("exit $LASTEXITCODE")
            except Exception:
                telemetry = False
                lines.append("& " + cmd_tokens)
                lines.append("exit $LASTEXITCODE")
        else:
            lines.append("& " + cmd_tokens)
            lines.append("exit $LASTEXITCODE")
        # As on POSIX: the Job Object sampler's sentinel rides stderr, which streaming
        # keeps separate from stdout, so the parse below is unchanged.
        proc = self._ps_remote(address, "\n".join(lines), timeout=timeout, on_stdout=on_stdout)
        if proc.returncode == 255:
            raise TransportError(
                f"ssh connection failed (exit 255): "
                f"{proc.stderr.decode('utf-8', 'replace').strip()}"
            )
        stdout = proc.stdout.decode("utf-8", "replace")
        stderr = proc.stderr.decode("utf-8", "replace")
        telem = None
        if telemetry:
            stderr, telem = _extract_telemetry(stderr)
        return ExecResult(proc.returncode, stdout, stderr, telem)

    # --- filesystem -------------------------------------------------------
    def ensure_remote_dir(self, remote_path: str) -> None:
        address = self._address_or_resolve()
        script = (
            "$ErrorActionPreference='Stop'; "
            f"New-Item -ItemType Directory -Force -Path {_ps_squote(remote_path)} | Out-Null"
        )
        proc = self._ps_remote(address, script)
        if proc.returncode != 0:
            raise TransportError(f"mkdir failed: {proc.stderr.decode('utf-8', 'replace')}")

    def push_file(self, local_path: Path, remote_path: str) -> None:
        address = self._address_or_resolve()
        b64 = base64.b64encode(local_path.read_bytes()).decode("ascii")
        # ticks (100 ns) computed here to avoid PowerShell float division precision loss.
        mtime_ticks = local_path.stat().st_mtime_ns // 100
        # Write to a temp beside the final path (Flush($true) = durable), then atomically
        # File.Replace/Move it — an interrupted write never leaves a partial destination.
        # Some Windows/.NET combinations reject a null File.Replace backup path, so use
        # a same-directory temporary backup and delete it after a successful replacement.
        script = (
            "$ErrorActionPreference='Stop'\n"
            f"$p = {_ps_squote(remote_path)}\n"
            "$dir = [System.IO.Path]::GetDirectoryName($p)\n"
            "if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }\n"
            "$tmp = [IO.Path]::Combine($dir, '.remrun-tmp-' + [IO.Path]::GetFileName($p) + "
            "'-' + [guid]::NewGuid().ToString('N') + '.tmp')\n"
            "$backup = [IO.Path]::Combine($dir, '.remrun-backup-' + [IO.Path]::GetFileName($p) + "
            "'-' + [guid]::NewGuid().ToString('N') + '.bak')\n"
            "try {\n"
            "  $bytes = [Convert]::FromBase64String(([Console]::In.ReadToEnd()).Trim())\n"
            "  $fs = [IO.File]::Open($tmp,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,"
            "[IO.FileShare]::None)\n"
            "  try { $fs.Write($bytes,0,$bytes.Length); $fs.Flush($true) } finally { $fs.Dispose() }\n"
            "  $epoch = [DateTime]::new(1970,1,1,0,0,0,[DateTimeKind]::Utc)\n"
            f"  [IO.File]::SetLastWriteTimeUtc($tmp, $epoch.AddTicks([int64]{mtime_ticks}))\n"
            "  if ([IO.File]::Exists($p)) { "
            "[IO.File]::Replace($tmp,$p,$backup,$true); "
            "Remove-Item -LiteralPath $backup -Force -ErrorAction SilentlyContinue } "
            "else { [IO.File]::Move($tmp,$p) }\n"
            "} catch { "
            "Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue; "
            "Remove-Item -LiteralPath $backup -Force -ErrorAction SilentlyContinue; throw }\n"
        )
        proc = self._ps_remote(address, script, input_bytes=b64.encode("ascii"))
        if proc.returncode != 0:
            raise TransportError(
                f"push {remote_path} failed: {proc.stderr.decode('utf-8', 'replace')}"
            )

    def push_files(self, local_root: Path, remote_root: str, rel_paths: list[str]) -> None:
        if len(rel_paths) < 2:
            return super().push_files(local_root, remote_root, rel_paths)
        address = self._address_or_resolve()
        req = _request_b64({"root": remote_root, "paths": [_safe_rel(p) for p in rel_paths]})
        script = (
            "$ErrorActionPreference='Stop'\n"
            f"& {_ps_squote(self.device.remote_python)} '-c' {_ps_squote(_TAR_EXTRACT_PROG)} "
            f"{_ps_squote(req)}\n"
            "exit $LASTEXITCODE"
        )
        proc = self._ps_remote(address, script, input_bytes=_tar_bytes_from_local(local_root, rel_paths))
        if proc.returncode != 0:
            raise TransportError(
                f"push archive to {remote_root} failed: {proc.stderr.decode('utf-8', 'replace')}"
            )

    def pull_file(self, remote_path: str, local_path: Path) -> None:
        address = self._address_or_resolve()
        script = (
            "$ErrorActionPreference='Stop'\n"
            f"$p = {_ps_squote(remote_path)}\n"
            "$ts = [IO.File]::GetLastWriteTimeUtc($p)\n"
            "$epoch = [DateTime]::new(1970,1,1,0,0,0,[DateTimeKind]::Utc)\n"
            "Write-Output ([long]((($ts - $epoch).Ticks) * 100))\n"
            "Write-Output ([Convert]::ToBase64String([IO.File]::ReadAllBytes($p)))\n"
        )
        proc = self._ps_remote(address, script)
        if proc.returncode != 0:
            raise TransportError(
                f"pull {remote_path} failed: {proc.stderr.decode('utf-8', 'replace')}"
            )
        lines = [ln.strip() for ln in proc.stdout.decode("utf-8", "replace").splitlines()
                 if ln.strip()]
        if not lines:
            raise TransportError(f"pull {remote_path}: empty response")
        ns = int(lines[0])
        data = base64.b64decode("".join(lines[1:]) or "")

        def fill(tmp: Path) -> None:
            tmp.write_bytes(data)
            try:
                os.utime(tmp, ns=(ns, ns))
            except (ValueError, OSError):
                pass
        _atomic_write_local(local_path, fill)

    def pull_files(self, remote_root: str, local_root: Path, rel_paths: list[str]) -> None:
        if len(rel_paths) < 2:
            return super().pull_files(remote_root, local_root, rel_paths)
        address = self._address_or_resolve()
        req = _request_b64({"root": remote_root, "paths": [_safe_rel(p) for p in rel_paths]})
        script = (
            "$ErrorActionPreference='Stop'\n"
            f"& {_ps_squote(self.device.remote_python)} '-c' {_ps_squote(_TAR_CREATE_PROG)} "
            f"{_ps_squote(req)}\n"
            "exit $LASTEXITCODE"
        )
        proc = self._ps_remote(address, script)
        if proc.returncode != 0:
            raise TransportError(
                f"pull archive from {remote_root} failed: {proc.stderr.decode('utf-8', 'replace')}"
            )
        _extract_tar_to_local(proc.stdout, local_root)

    def delete_remote(self, remote_path: str) -> None:
        address = self._address_or_resolve()
        # Fail LOUD: the old form used -ErrorAction SilentlyContinue and only treated an
        # ssh-255 as failure, so a locked / ACL-denied remote file was reported deleted
        # while it still existed (and the run continued against a stale file). Make the
        # delete terminating and VERIFY the path is gone before reporting success.
        script = (
            "$ErrorActionPreference='Stop'\n"
            f"$p = {_ps_squote(remote_path)}\n"
            "if (Test-Path -LiteralPath $p) { Remove-Item -LiteralPath $p -Force }\n"
            "if (Test-Path -LiteralPath $p) { throw ('still present after delete: ' + $p) }\n"
        )
        proc = self._ps_remote(address, script)
        if proc.returncode != 0:
            detail = proc.stderr.decode("utf-8", "replace").strip() or f"exit {proc.returncode}"
            raise TransportError(f"delete {remote_path} failed: {detail}")

    def remote_path_exists(self, remote_path: str) -> bool:
        address = self._address_or_resolve()
        p = self._expand_remote(remote_path)
        script = (f"if (Test-Path -LiteralPath {_ps_squote(p)}) "
                  "{ Write-Output '__REMRUN_EXISTS__' }")
        proc = self._ps_remote(address, script)
        return proc.returncode == 0 and b"__REMRUN_EXISTS__" in proc.stdout

    def native_join(self, *parts: str) -> str:
        # Preserve the first part (a drive/UNC root) intact; collapse only the
        # separators between parts. (See the base native_join note.)
        items = [str(p) for p in parts if p]
        if not items:
            return ""
        head = items[0].rstrip("\\/")
        tail = [p.strip("\\/") for p in items[1:]]
        return "\\".join([head, *(t for t in tail if t)])

    def remote_temp_dir(self, prefix: str = "remrun-fleet") -> str:
        self._address_or_resolve()       # resolves the address and remote $HOME
        home = (self._remote_home or "").rstrip("\\/")
        base = self.native_join(home, "AppData", "Local", "Temp") if home else "C:\\Windows\\Temp"
        path = base + "\\" + f"{prefix}-{uuid.uuid4().hex[:8]}"
        self.ensure_remote_dir(path)
        return path

    def remove_remote_tree(self, remote_path: str) -> None:
        address = self._address_or_resolve()
        p = self._expand_remote(remote_path)
        self._ps_remote(address, f"Remove-Item -LiteralPath {_ps_squote(p)} -Recurse "
                                 "-Force -ErrorAction SilentlyContinue")

    def manifest(self, remote_root, exclude_patterns, hash_below_bytes=0) -> Manifest:  # noqa: ANN001
        address = self._address_or_resolve()
        request = base64.b64encode(json.dumps({
            "op": "manifest",
            "root": remote_root,
            "exclude": list(exclude_patterns),
            "hash_below_bytes": int(hash_below_bytes),
        }).encode("utf-8")).decode("ascii")
        # Pipe runner source to the remote python via stdin; the base64 request is
        # a bare, shell-safe argv that survives whether the shell is pwsh or cmd.
        script = f"{self.device.remote_python} - {request}"
        proc = self._remote(address, script, input_bytes=_runner_source())
        if proc.returncode != 0:
            raise TransportError(
                f"remote manifest failed: {proc.stderr.decode('utf-8', 'replace')}"
            )
        try:
            data = json.loads(proc.stdout.decode("utf-8", "replace") or "{}")
        except json.JSONDecodeError as exc:
            raise TransportError(f"remote manifest returned invalid JSON: {exc}") from exc
        return manifest_from_json(data)

    def hash_file(self, remote_path: str) -> str:
        address = self._address_or_resolve()
        request = base64.b64encode(json.dumps(
            {"op": "hash_file", "path": remote_path}).encode("utf-8")).decode("ascii")
        script = f"{self.device.remote_python} - {request}"
        proc = self._remote(address, script, input_bytes=_runner_source())
        if proc.returncode != 0:
            raise TransportError(f"hash {remote_path} failed: "
                                 f"{proc.stderr.decode('utf-8', 'replace')}")
        return json.loads(proc.stdout.decode("utf-8", "replace") or "{}").get("sha256", "")


def make_transport(device: Device) -> BaseTransport:
    if device.kind == "local-sim":
        return LocalSimTransport(device)
    if device.kind == "ssh-posix":
        return SSHPosixTransport(device)
    if device.kind == "ssh-powershell":
        return SSHPowerShellTransport(device)
    raise TransportError(f"Unsupported device kind: {device.kind}")
