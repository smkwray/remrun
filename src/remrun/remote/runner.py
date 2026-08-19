#!/usr/bin/env python3
"""Self-contained remrun remote helper.

Legacy manifest/hash/probe calls still pipe this source through ``python -``. The
versioned Step-3 path installs these exact bytes under the runner state root and
invokes ``<installed-path> rpc <state-root>`` with RRFRAME2 requests. The new path
creates durable participant metadata only; command execution remains on the legacy
transport until later coordination phases are enabled.
"""
from __future__ import annotations

import base64
import binascii
import ctypes
import errno
import fnmatch
import hashlib
import hmac
import json
import os
import platform
import re
import secrets
import select
import shutil
import stat
import struct
import subprocess
import sys
import tempfile
import time
import uuid

RUNNER_FORMAT = "remrun-runner-v1"
RUNNER_SCHEMA_VERSION = 3
RUNNER_V1_SCHEMA_COUNT = 8
RUNNER_V2_SCHEMA_COUNT = 9
RUNNER_PROTOCOLS = [1]
AUTHORITY_SCHEMA_VERSION = 5
DEFAULT_LEASE_SECONDS = 120
FRAME_MAGIC = b"RRFRAME2"
MAX_HEADER_BYTES = 1 << 20
NETWORK_FILESYSTEMS = {
    "9p", "afpfs", "cifs", "fuse.sshfs", "nfs", "nfs4", "smb", "smb2", "smbfs", "sshfs",
}

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS runner_meta (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        schema_version INTEGER NOT NULL,
        device_id TEXT NOT NULL,
        created_at_ns INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS project_fences (
        cluster_id TEXT NOT NULL,
        project_key TEXT NOT NULL,
        authority_epoch INTEGER NOT NULL,
        max_fence INTEGER NOT NULL,
        PRIMARY KEY(cluster_id, project_key)
    )""",
    """CREATE TABLE IF NOT EXISTS accepted_grants (
        grant_id TEXT PRIMARY KEY,
        cluster_id TEXT NOT NULL,
        project_key TEXT NOT NULL,
        authority_epoch INTEGER NOT NULL,
        fence INTEGER NOT NULL,
        operation TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        state TEXT NOT NULL,
        accepted_at_ns INTEGER NOT NULL,
        result_json BLOB,
        UNIQUE(project_key, operation_id)
    )""",
    """CREATE TABLE IF NOT EXISTS executions (
        execution_id TEXT PRIMARY KEY,
        project_key TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        accepted_grant_id TEXT NOT NULL UNIQUE,
        accepted_epoch INTEGER NOT NULL,
        accepted_fence INTEGER NOT NULL,
        state TEXT NOT NULL CHECK (state IN
            ('PREPARED','LAUNCH_ARMED','RUNNING','EXITED','SNAPSHOTTING',
             'COMPLETE','START_FAILED_NO_CHILD','ABORTED_NO_CHILD','UNRESOLVED')),
        child_start_state TEXT NOT NULL CHECK (child_start_state IN ('NO','MAYBE','YES')),
        supervisor_pid INTEGER,
        supervisor_start_token TEXT,
        worker_heartbeat_at_ns INTEGER,
        started_at_ns INTEGER,
        exited_at_ns INTEGER,
        exit_code INTEGER,
        stdout_length INTEGER,
        stdout_sha256 TEXT,
        stderr_length INTEGER,
        stderr_sha256 TEXT,
        post_manifest_sha256 TEXT,
        error TEXT,
        created_at_ns INTEGER NOT NULL,
        updated_at_ns INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS participant_transactions (
        txn_id TEXT PRIMARY KEY,
        project_key TEXT NOT NULL,
        accepted_grant_id TEXT NOT NULL UNIQUE,
        plan_sha256 TEXT NOT NULL,
        intended_manifest_sha256 TEXT NOT NULL,
        state TEXT NOT NULL,
        decision TEXT,
        created_at_ns INTEGER NOT NULL,
        updated_at_ns INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS mutations (
        txn_id TEXT NOT NULL,
        op_index INTEGER NOT NULL,
        path TEXT NOT NULL,
        action TEXT NOT NULL CHECK(action IN ('CREATE','REPLACE','DELETE','MODE')),
        expected_source_json BLOB,
        expected_dest_json BLOB,
        desired_json BLOB,
        payload_id TEXT,
        stage_path TEXT,
        predecessor_path TEXT,
        state TEXT NOT NULL,
        error TEXT,
        PRIMARY KEY(txn_id, op_index),
        FOREIGN KEY(txn_id) REFERENCES participant_transactions(txn_id)
    )""",
    """CREATE TABLE IF NOT EXISTS logical_modes (
        project_key TEXT NOT NULL,
        path TEXT NOT NULL,
        mode INTEGER NOT NULL,
        last_generation INTEGER NOT NULL,
        PRIMARY KEY(project_key, path)
    )""",
    # Side-effecting RPCs need durable request identity even before grants land in Step 4.
    """CREATE TABLE IF NOT EXISTS rpc_requests (
        rpc_id TEXT PRIMARY KEY,
        operation TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        response_json BLOB NOT NULL,
        created_at_ns INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS enrolled_authorities (
        cluster_id TEXT NOT NULL,
        authority_epoch INTEGER NOT NULL,
        key_id TEXT NOT NULL,
        key_sha256 TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('ENROLLED','RETIRED')),
        enrolled_at_ns INTEGER NOT NULL,
        retired_at_ns INTEGER,
        PRIMARY KEY(cluster_id,authority_epoch,key_id),
        UNIQUE(cluster_id,authority_epoch)
    )""",
    """CREATE TABLE IF NOT EXISTS target_resource_policy (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        generation INTEGER NOT NULL CHECK(generation>=1),
        digest TEXT NOT NULL,
        document_json BLOB NOT NULL,
        installed_at_ns INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS target_resource_fence (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        last_fence INTEGER NOT NULL CHECK(last_fence>=0)
    )""",
    """CREATE TABLE IF NOT EXISTS target_resource_allocations (
        allocation_id TEXT PRIMARY KEY,
        operation_id TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        allocation_spec_sha256 TEXT NOT NULL,
        resource_keys_json BLOB NOT NULL,
        policy_generation INTEGER NOT NULL,
        policy_digest TEXT NOT NULL,
        fence INTEGER NOT NULL UNIQUE,
        token_sha256 TEXT NOT NULL UNIQUE,
        state TEXT NOT NULL CHECK(state IN
            ('RESERVED','CLAIMED','QUARANTINED','RELEASED','EXPIRED',
             'CANCELLED','REBOOTED')),
        reservation_boot_id TEXT NOT NULL,
        reserved_mono_ns INTEGER NOT NULL,
        expires_mono_ns INTEGER NOT NULL,
        claim_boot_id TEXT,
        claimed_mono_ns INTEGER,
        owner_kind TEXT,
        owner_key TEXT,
        owner_pid INTEGER,
        owner_start_id TEXT,
        root_pid INTEGER,
        root_start_id TEXT,
        user_pid INTEGER,
        user_start_id TEXT,
        command_start_state TEXT NOT NULL CHECK(command_start_state IN ('NO','MAYBE','YES')),
        terminal_reason TEXT,
        created_at_ns INTEGER NOT NULL,
        updated_at_ns INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS target_resource_holds (
        resource_key TEXT PRIMARY KEY,
        allocation_id TEXT NOT NULL,
        fence INTEGER NOT NULL,
        FOREIGN KEY(allocation_id) REFERENCES target_resource_allocations(allocation_id)
    )""",
]

RESOURCE_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RESOURCE_RESERVATION_NS = 30_000_000_000
MAX_RESOURCE_FENCE = (1 << 63) - 1

_WIN_DWORD = ctypes.c_uint32
_WIN_BOOL = ctypes.c_int
_WIN_HANDLE = ctypes.c_void_p
_WIN_LPVOID = ctypes.c_void_p
_WIN_ULONG_PTR = ctypes.c_size_t
_WIN_INVALID_HANDLE = ctypes.c_void_p(-1).value
_WIN_CREATE_SUSPENDED = 0x00000004
_WIN_DETACHED_PROCESS = 0x00000008
_WIN_CREATE_UNICODE_ENVIRONMENT = 0x00000400
_WIN_CREATE_BREAKAWAY_FROM_JOB = 0x01000000
_WIN_STARTF_USESTDHANDLES = 0x00000100
_WIN_INFINITE = 0xFFFFFFFF
_WIN_WAIT_OBJECT_0 = 0
_WIN_WAIT_TIMEOUT = 258
_WIN_WAIT_FAILED = 0xFFFFFFFF
_WIN_DWORD_MINUS_ONE = 0xFFFFFFFF
_WIN_JOB_OBJECT_QUERY = 0x0004
_WIN_JOB_BASIC_PROCESS_ID_LIST = 3
_WIN_JOB_EXTENDED_LIMIT_INFORMATION = 9
_WIN_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
_WIN_ERROR_ALREADY_EXISTS = 183
_WIN_ERROR_FILE_NOT_FOUND = 2
_WIN_ERROR_INVALID_PARAMETER = 87
_WIN_ERROR_MORE_DATA = 234
_WIN_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_WIN_K32 = None


class _WinFileTime(ctypes.Structure):
    _fields_ = [("low", _WIN_DWORD), ("high", _WIN_DWORD)]


class _WinJobBasicLimitInformation(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_int64),
        ("PerJobUserTimeLimit", ctypes.c_int64),
        ("LimitFlags", _WIN_DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", _WIN_DWORD),
        ("Affinity", _WIN_ULONG_PTR),
        ("PriorityClass", _WIN_DWORD),
        ("SchedulingClass", _WIN_DWORD),
    ]


class _WinIoCounters(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount", ctypes.c_uint64),
        ("WriteOperationCount", ctypes.c_uint64),
        ("OtherOperationCount", ctypes.c_uint64),
        ("ReadTransferCount", ctypes.c_uint64),
        ("WriteTransferCount", ctypes.c_uint64),
        ("OtherTransferCount", ctypes.c_uint64),
    ]


class _WinJobExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _WinJobBasicLimitInformation),
        ("IoInfo", _WinIoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


class _WinStartupInfo(ctypes.Structure):
    _fields_ = [
        ("cb", _WIN_DWORD), ("lpReserved", ctypes.c_wchar_p),
        ("lpDesktop", ctypes.c_wchar_p), ("lpTitle", ctypes.c_wchar_p),
        ("dwX", _WIN_DWORD), ("dwY", _WIN_DWORD), ("dwXSize", _WIN_DWORD),
        ("dwYSize", _WIN_DWORD), ("dwXCountChars", _WIN_DWORD),
        ("dwYCountChars", _WIN_DWORD), ("dwFillAttribute", _WIN_DWORD),
        ("dwFlags", _WIN_DWORD), ("wShowWindow", ctypes.c_uint16),
        ("cbReserved2", ctypes.c_uint16), ("lpReserved2", ctypes.POINTER(ctypes.c_ubyte)),
        ("hStdInput", _WIN_HANDLE), ("hStdOutput", _WIN_HANDLE), ("hStdError", _WIN_HANDLE),
    ]


class _WinProcessInformation(ctypes.Structure):
    _fields_ = [
        ("hProcess", _WIN_HANDLE), ("hThread", _WIN_HANDLE),
        ("dwProcessId", _WIN_DWORD), ("dwThreadId", _WIN_DWORD),
    ]

AUTHORITY_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS authority_meta (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        schema_version INTEGER NOT NULL,
        cluster_id TEXT NOT NULL,
        coordinator_device_id TEXT NOT NULL,
        authority_epoch INTEGER NOT NULL,
        sealed INTEGER NOT NULL DEFAULT 0 CHECK(sealed IN (0,1)),
        lease_seconds INTEGER NOT NULL,
        created_at_ns INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS projects (
        project_key TEXT PRIMARY KEY,
        project_id TEXT NOT NULL UNIQUE,
        policy_sha256 TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('BOOTSTRAP','READY','RECOVERY','SEALED')),
        head_generation INTEGER NOT NULL DEFAULT 0,
        head_manifest_sha256 TEXT NOT NULL DEFAULT '',
        next_fence INTEGER NOT NULL DEFAULT 0,
        current_lease_id TEXT,
        active_run_id TEXT,
        pending_txn_id TEXT,
        recovery_prior_state TEXT CHECK(recovery_prior_state IS NULL OR
            recovery_prior_state IN ('BOOTSTRAP','READY')),
        created_at_ns INTEGER NOT NULL,
        updated_at_ns INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS leases (
        lease_id TEXT PRIMARY KEY,
        project_key TEXT NOT NULL,
        acquire_id TEXT NOT NULL UNIQUE,
        request_sha256 TEXT NOT NULL,
        fence INTEGER NOT NULL,
        owner_token_sha256 TEXT NOT NULL,
        controller_id TEXT NOT NULL,
        controller_replica_id TEXT NOT NULL,
        owner_host TEXT,
        owner_pid INTEGER,
        phase TEXT NOT NULL CHECK(phase IN ('NORMAL','RECOVERY')),
        state TEXT NOT NULL CHECK(state IN ('ACTIVE','SUPERSEDED','RELEASED')),
        recovery_of_lease_id TEXT,
        heartbeat_seq INTEGER NOT NULL DEFAULT 0,
        acquired_at_ns INTEGER NOT NULL,
        heartbeat_at_ns INTEGER NOT NULL,
        lease_until_ns INTEGER NOT NULL,
        released_at_ns INTEGER,
        UNIQUE(project_key,fence),
        FOREIGN KEY(project_key) REFERENCES projects(project_key)
    )""",
    """CREATE TABLE IF NOT EXISTS authority_targets (
        target_device_id TEXT NOT NULL,
        authority_epoch INTEGER NOT NULL,
        key_id TEXT NOT NULL,
        key_sha256 TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('PENDING','ENROLLED','RETIRED')),
        created_at_ns INTEGER NOT NULL,
        PRIMARY KEY(target_device_id,authority_epoch,key_id),
        UNIQUE(target_device_id,authority_epoch)
    )""",
    """CREATE TABLE IF NOT EXISTS grants (
        grant_id TEXT PRIMARY KEY,
        grant_request_id TEXT NOT NULL UNIQUE,
        project_key TEXT NOT NULL,
        lease_id TEXT NOT NULL,
        authority_epoch INTEGER NOT NULL,
        fence INTEGER NOT NULL,
        target_device_id TEXT NOT NULL,
        operation TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        issue_sha256 TEXT NOT NULL,
        capability_json BLOB NOT NULL,
        state TEXT NOT NULL CHECK(state IN
            ('ISSUED','ACCEPTED','TERMINAL','FENCED','UNKNOWN')),
        issued_at_ns INTEGER NOT NULL,
        accepted_at_ns INTEGER,
        terminal_at_ns INTEGER,
        result_json BLOB,
        last_receipt_sha256 TEXT,
        FOREIGN KEY(project_key) REFERENCES projects(project_key),
        FOREIGN KEY(lease_id) REFERENCES leases(lease_id)
    )""",
    """CREATE TABLE IF NOT EXISTS snapshots (
        project_key TEXT NOT NULL,
        generation INTEGER NOT NULL CHECK(generation>=0),
        parent_generation INTEGER,
        policy_sha256 TEXT NOT NULL,
        manifest_sha256 TEXT NOT NULL,
        manifest_zlib BLOB NOT NULL,
        committed_by_run_id TEXT,
        committed_by_txn_id TEXT NOT NULL,
        committed_at_ns INTEGER NOT NULL,
        PRIMARY KEY(project_key,generation),
        FOREIGN KEY(project_key) REFERENCES projects(project_key),
        CHECK((generation=0 AND parent_generation IS NULL) OR
              (generation>0 AND parent_generation>=0 AND parent_generation<generation))
    )""",
    """CREATE TABLE IF NOT EXISTS tombstone_events (
        project_key TEXT NOT NULL,
        path TEXT NOT NULL,
        deleted_generation INTEGER NOT NULL CHECK(deleted_generation>0),
        prior_identity_json BLOB NOT NULL,
        deleted_by_txn_id TEXT NOT NULL,
        PRIMARY KEY(project_key,path,deleted_generation),
        FOREIGN KEY(project_key) REFERENCES projects(project_key)
    )""",
    """CREATE TABLE IF NOT EXISTS replicas (
        project_key TEXT NOT NULL,
        replica_id TEXT NOT NULL,
        replica_kind TEXT NOT NULL CHECK(replica_kind IN ('CONTROLLER','RUNNER')),
        endpoint_id TEXT NOT NULL,
        root_fingerprint TEXT NOT NULL,
        credential_sha256 TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('ACTIVE','DIRTY','UNVERIFIED','RETIRED')),
        ack_generation INTEGER,
        ack_manifest_sha256 TEXT,
        pending_txn_id TEXT,
        last_seen_at_ns INTEGER,
        PRIMARY KEY(project_key,replica_id),
        FOREIGN KEY(project_key) REFERENCES projects(project_key),
        CHECK((ack_generation IS NULL AND ack_manifest_sha256 IS NULL) OR
              (ack_generation>=0 AND ack_manifest_sha256 IS NOT NULL)),
        CHECK(state!='ACTIVE' OR ack_generation IS NOT NULL),
        CHECK(state!='UNVERIFIED' OR ack_generation IS NULL)
    )""",
    """CREATE TABLE IF NOT EXISTS authority_rpc_requests (
        rpc_id TEXT PRIMARY KEY,
        operation TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        response_json BLOB NOT NULL,
        created_at_ns INTEGER NOT NULL
    )""",
]


class RunnerError(RuntimeError):
    pass


def _test_fault_point(name: str) -> None:
    configured = os.environ.get("REMRUN_TEST_ONLY_FAULT_POINT", "")
    if configured == name:
        raise RunnerError(f"injected test fault: {name}")


def _test_pause_point(name: str) -> None:
    if os.environ.get("REMRUN_TEST_ONLY_FAULT_POINT", "") == f"pause_{name}":
        time.sleep(60)


def canonical_json(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def encode_frame(header: dict, body: bytes) -> bytes:
    packed = dict(header)
    packed["decoded_length"] = len(body)
    packed["sha256"] = hashlib.sha256(body).hexdigest()
    header_bytes = canonical_json(packed)
    encoded = base64.b64encode(body)
    return b"%s %d %d\n" % (FRAME_MAGIC, len(header_bytes), len(encoded)) + header_bytes + encoded


def decode_frame(data: bytes):
    nl = data.find(b"\n")
    if nl < 0:
        raise RunnerError("no frame header line")
    parts = data[:nl].split(b" ")
    if len(parts) != 3 or parts[0] != FRAME_MAGIC:
        raise RunnerError("bad frame magic or header line")
    try:
        header_len, body_len = int(parts[1]), int(parts[2])
    except ValueError as exc:
        raise RunnerError("bad frame lengths") from exc
    if header_len < 0 or body_len < 0 or header_len > MAX_HEADER_BYTES:
        raise RunnerError("invalid frame lengths")
    rest = data[nl + 1:]
    if len(rest) != header_len + body_len:
        raise RunnerError("frame size mismatch")
    try:
        header = json.loads(rest[:header_len].decode("utf-8"))
        body = base64.b64decode(rest[header_len:], validate=True)
    except (UnicodeDecodeError, json.JSONDecodeError, binascii.Error, ValueError) as exc:
        raise RunnerError("invalid framed payload") from exc
    if not isinstance(header, dict):
        raise RunnerError("frame header is not an object")
    if len(body) != header.get("decoded_length"):
        raise RunnerError("decoded length mismatch")
    if hashlib.sha256(body).hexdigest() != header.get("sha256"):
        raise RunnerError("payload digest mismatch")
    return header, body


def _read_frame_from_stream(stream) -> tuple[dict, bytes, bytes]:
    first = stream.readline(256)
    if not first or len(first) >= 256 or not first.endswith(b"\n"):
        raise RunnerError("runner stream frame header is missing or oversized")
    parts = first[:-1].split(b" ")
    if len(parts) != 3 or parts[0] != FRAME_MAGIC:
        raise RunnerError("runner stream frame magic is invalid")
    try:
        header_len = int(parts[1])
        body_len = int(parts[2])
    except ValueError as exc:
        raise RunnerError("runner stream frame lengths are invalid") from exc
    if header_len < 0 or header_len > (1 << 20) or body_len < 0 or body_len > (1 << 28):
        raise RunnerError("runner stream frame length exceeds bound")
    remainder = _read_fd_stream_exact(stream, header_len + body_len)
    raw = first + remainder
    header, body = decode_frame(raw)
    return header, body, raw


def _read_fd_stream_exact(stream, length: int) -> bytes:
    chunks = []
    remaining = length
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            raise RunnerError("runner stream frame is truncated")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def should_exclude(rel_posix: str, patterns) -> bool:
    rel = rel_posix.strip("/")
    for pattern in patterns:
        pat = pattern.strip()
        if not pat:
            continue
        if pat.endswith("/**"):
            prefix = pat[:-3].strip("/")
            if rel == prefix or rel.startswith(prefix + "/"):
                return True
        if fnmatch.fnmatch(rel, pat) or fnmatch.fnmatch("/" + rel, pat):
            return True
    return False


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_directory_link(path: str) -> bool:
    if os.path.islink(path):
        return True
    isjunction = getattr(os.path, "isjunction", None)
    if isjunction is not None:
        return bool(isjunction(path))
    if os.name != "nt":
        return False
    item = os.lstat(path)
    mount_point_tag = getattr(stat, "IO_REPARSE_TAG_MOUNT_POINT", 0xA0000003)
    return getattr(item, "st_reparse_tag", None) == mount_point_tag


def build_manifest(root: str, excludes, hash_below_bytes: int, always_hash: bool = False) -> dict:
    files: dict = {}
    if not os.path.isdir(root):
        return files

    def walk_error(error):
        raise RunnerError(f"manifest walk failed: {error}")

    for dirpath, dirnames, filenames in os.walk(root, onerror=walk_error):
        rel_dir = os.path.relpath(dirpath, root).replace(os.sep, "/")
        if rel_dir == ".":
            rel_dir = ""
        kept_dirs = []
        for name in dirnames:
            rel = f"{rel_dir}/{name}" if rel_dir else name
            if should_exclude(rel, excludes):
                continue
            try:
                if _is_directory_link(os.path.join(dirpath, name)):
                    continue
            except OSError as exc:
                raise RunnerError(f"manifest directory inspection failed for {rel}: {exc}") from exc
            kept_dirs.append(name)
        dirnames[:] = kept_dirs
        for name in filenames:
            full = os.path.join(dirpath, name)
            rel = f"{rel_dir}/{name}" if rel_dir else name
            if should_exclude(rel, excludes) or os.path.islink(full):
                continue
            try:
                item = os.stat(full)
            except OSError as exc:
                raise RunnerError(f"manifest stat failed for {rel}: {exc}") from exc
            if not os.path.isfile(full):
                continue
            digest = None
            if always_hash or (hash_below_bytes and item.st_size <= hash_below_bytes):
                try:
                    digest = sha256_file(full)
                except OSError as exc:
                    raise RunnerError(f"manifest hash failed for {rel}: {exc}") from exc
            files[rel] = {
                "kind": "file",
                "size": item.st_size,
                "mtime_ns": item.st_mtime_ns,
                "sha256": digest,
                "mode": stat.S_IMODE(item.st_mode),
            }
    return files


def _nearest_existing(path: str) -> str:
    current = os.path.abspath(os.path.expanduser(path))
    while not os.path.exists(current):
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return current


def filesystem_probe(path: str) -> dict:
    existing = _nearest_existing(path)
    if os.name == "nt":
        import ctypes
        resolved = os.path.abspath(existing)
        if resolved.startswith("\\\\"):
            return {"local": False, "kind": "unc", "path": resolved}
        drive = os.path.splitdrive(resolved)[0] + "\\"
        drive_type = int(ctypes.windll.kernel32.GetDriveTypeW(drive))
        # DRIVE_REMOTE=4; DRIVE_UNKNOWN/NO_ROOT also fail closed.
        return {"local": drive_type in {2, 3, 5, 6}, "kind": f"win-drive-{drive_type}",
                "path": resolved}

    kind = "unknown-local"
    if sys.platform == "darwin":
        try:
            import plistlib
            df_lines = subprocess.check_output(
                ["/bin/df", "-P", existing], text=True, timeout=10
            ).splitlines()
            if len(df_lines) < 2:
                raise RunnerError("df returned no mounted device")
            mounted_device = df_lines[-1].split()[0]
            raw = subprocess.check_output(
                ["/usr/sbin/diskutil", "info", "-plist", mounted_device], timeout=10
            )
            disk = plistlib.loads(raw)
            kind = str(
                disk.get("FilesystemType") or disk.get("FilesystemName") or "unknown"
            ).lower()
            network = bool(disk.get("VolumeNetworkMount", False)) \
                or str(disk.get("BusProtocol", "")).lower() == "network"
            return {"local": not network and kind != "unknown", "kind": kind,
                    "path": existing}
        except Exception:
            return {"local": False, "kind": "unknown", "path": existing}
    elif os.path.exists("/proc/mounts"):
        best = ("", "unknown-local")
        try:
            with open("/proc/mounts", encoding="utf-8") as mounts:
                for line in mounts:
                    fields = line.split()
                    if len(fields) < 3:
                        continue
                    mount = fields[1].replace("\\040", " ")
                    if existing == mount or existing.startswith(mount.rstrip("/") + "/"):
                        if len(mount) > len(best[0]):
                            best = (mount, fields[2].lower())
            kind = best[1]
        except OSError:
            kind = "unknown-local"
    return {"local": kind not in NETWORK_FILESYSTEMS and kind != "unknown-local",
            "kind": kind, "path": existing}


def _load_sqlite(sqlite_module=None):
    if sqlite_module is False:
        raise RunnerError("remote Python has no sqlite3 module")
    if sqlite_module is not None:
        return sqlite_module
    try:
        import sqlite3
    except ImportError as exc:
        raise RunnerError("remote Python has no sqlite3 module") from exc
    return sqlite3


def open_participant_store(state_root: str, sqlite_module=None):
    sqlite3 = _load_sqlite(sqlite_module)
    runner_root = os.path.join(os.path.abspath(os.path.expanduser(state_root)), "runner", "v1")
    probe = filesystem_probe(runner_root)
    if not probe["local"]:
        raise RunnerError(
            f"runner state root must be on a local filesystem; got {probe['kind']}: {probe['path']}"
        )
    os.makedirs(runner_root, exist_ok=True)
    db_path = os.path.join(runner_root, "runner.sqlite3")
    conn = sqlite3.connect(db_path, timeout=10, isolation_level=None)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        journal_row = conn.execute("PRAGMA journal_mode = DELETE").fetchone()
        journal_mode = str(journal_row[0]).lower() if journal_row else ""
        if journal_mode != "delete":
            raise RunnerError(
                f"runner store requires rollback journal mode; got {journal_mode or 'unknown'}"
            )
        conn.execute("PRAGMA synchronous = EXTRA")
        conn.execute("PRAGMA busy_timeout = 10000")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("BEGIN IMMEDIATE")
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version not in (0, 1, 2, RUNNER_SCHEMA_VERSION):
            raise RunnerError(
                f"runner store schema {version} is incompatible with "
                f"supported {RUNNER_SCHEMA_VERSION}"
            )
        if version == 0:
            for statement in SCHEMA:
                conn.execute(statement)
            now = time.time_ns()
            conn.execute(
                "INSERT OR IGNORE INTO runner_meta "
                "(singleton,schema_version,device_id,created_at_ns) VALUES (1,?,?,?)",
                (RUNNER_SCHEMA_VERSION, str(uuid.uuid4()), now),
            )
            conn.execute(f"PRAGMA user_version = {RUNNER_SCHEMA_VERSION}")
        elif version in (1, 2):
            prior_count = RUNNER_V1_SCHEMA_COUNT if version == 1 else RUNNER_V2_SCHEMA_COUNT
            _assert_exact_schema(conn, sqlite3, SCHEMA[:prior_count], f"runner v{version}")
            for statement in SCHEMA[prior_count:]:
                conn.execute(statement)
            conn.execute(
                "UPDATE runner_meta SET schema_version=? WHERE singleton=1",
                (RUNNER_SCHEMA_VERSION,),
            )
            conn.execute(f"PRAGMA user_version = {RUNNER_SCHEMA_VERSION}")
        _assert_exact_schema(conn, sqlite3, SCHEMA, "runner v3")
        meta = conn.execute(
            "SELECT schema_version,device_id,created_at_ns FROM runner_meta WHERE singleton=1"
        ).fetchone()
        if meta is None or int(meta[0]) != RUNNER_SCHEMA_VERSION:
            raise RunnerError("runner store metadata is missing or inconsistent")
        if int(conn.execute("PRAGMA user_version").fetchone()[0]) != RUNNER_SCHEMA_VERSION:
            raise RunnerError("runner store user_version is inconsistent")
        conn.execute("COMMIT")
        return conn, runner_root, {
            "schema_version": int(meta[0]), "device_id": str(meta[1]),
            "created_at_ns": int(meta[2]), "filesystem": probe,
            "sqlite_version": sqlite3.sqlite_version,
            "journal_mode": journal_mode,
        }
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        conn.close()
        raise


def _configure_sqlite(conn) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = DELETE")
    conn.execute("PRAGMA synchronous = EXTRA")
    conn.execute("PRAGMA busy_timeout = 10000")
    conn.execute("PRAGMA temp_store = MEMORY")


def _schema_descriptor(conn) -> list[dict]:
    tables = conn.execute(
        "SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    ).fetchall()
    result = []
    for name, sql in tables:
        quoted = '"' + str(name).replace('"', '""') + '"'
        indexes = []
        for index in conn.execute(f"PRAGMA index_list({quoted})"):
            index_name = '"' + str(index[1]).replace('"', '""') + '"'
            columns = [str(row[2]) for row in conn.execute(f"PRAGMA index_info({index_name})")]
            indexes.append({
                "unique": int(index[2]), "origin": str(index[3]),
                "partial": int(index[4]), "columns": columns,
            })
        result.append({
            "name": str(name),
            "sql": " ".join(str(sql).lower().split()),
            "columns": [list(row[1:]) for row in conn.execute(f"PRAGMA table_info({quoted})")],
            "foreign_keys": [list(row[2:]) for row in conn.execute(
                f"PRAGMA foreign_key_list({quoted})"
            )],
            "indexes": sorted(indexes, key=lambda value: canonical_json(value)),
        })
    return result


def _assert_exact_schema(conn, sqlite3, statements: list[str], label: str) -> None:
    expected = sqlite3.connect(":memory:")
    try:
        expected.execute("PRAGMA foreign_keys = ON")
        for statement in statements:
            expected.execute(statement)
        expected_descriptor = _schema_descriptor(expected)
    finally:
        expected.close()
    if _schema_descriptor(conn) != expected_descriptor:
        raise RunnerError(f"{label} schema does not match the complete expected definition")


def open_authority_store(state_root: str, cluster_id: str, coordinator_device_id: str,
                         *, create: bool = False, lease_seconds: int = DEFAULT_LEASE_SECONDS,
                         sqlite_module=None):
    sqlite3 = _load_sqlite(sqlite_module)
    coord_root = os.path.join(os.path.abspath(os.path.expanduser(state_root)), "coord", "v1")
    probe = filesystem_probe(coord_root)
    if not probe["local"]:
        raise RunnerError(
            f"authority state root must be on a local filesystem; got {probe['kind']}: "
            f"{probe['path']}"
        )
    db_path = os.path.join(coord_root, "authority.sqlite3")
    if not create and not os.path.isfile(db_path):
        raise RunnerError("authority is not initialized")
    os.makedirs(coord_root, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=10, isolation_level=None)
    try:
        _configure_sqlite(conn)
        conn.execute("BEGIN IMMEDIATE")
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version not in (0, AUTHORITY_SCHEMA_VERSION):
            raise RunnerError(
                f"shadow authority schema {version} is incompatible with "
                f"{AUTHORITY_SCHEMA_VERSION}; reset the disposable shadow authority state"
            )
        if version == 0:
            if not create:
                raise RunnerError("authority is not initialized")
            if not cluster_id or not coordinator_device_id:
                raise RunnerError("cluster and coordinator identities are required")
            if not 1 <= int(lease_seconds) <= 86400:
                raise RunnerError("lease_seconds must be between 1 and 86400")
            for statement in AUTHORITY_SCHEMA:
                conn.execute(statement)
            now = time.time_ns()
            conn.execute(
                "INSERT INTO authority_meta "
                "(singleton,schema_version,cluster_id,coordinator_device_id,authority_epoch,"
                "sealed,lease_seconds,created_at_ns) VALUES (1,?,?,?,?,0,?,?)",
                (AUTHORITY_SCHEMA_VERSION, cluster_id, coordinator_device_id, 1,
                 int(lease_seconds), now),
            )
            conn.execute(f"PRAGMA user_version = {AUTHORITY_SCHEMA_VERSION}")
            version = AUTHORITY_SCHEMA_VERSION
        meta = conn.execute(
            "SELECT schema_version,cluster_id,coordinator_device_id,authority_epoch,sealed,"
            "lease_seconds,created_at_ns FROM authority_meta WHERE singleton=1"
        ).fetchone()
        if meta is None or str(meta[1]) != cluster_id:
            raise RunnerError("authority cluster identity mismatch")
        if int(meta[0]) != AUTHORITY_SCHEMA_VERSION or version != AUTHORITY_SCHEMA_VERSION:
            raise RunnerError("authority schema metadata/user_version mismatch")
        if str(meta[2]) != coordinator_device_id:
            raise RunnerError("authority coordinator device identity mismatch")
        _assert_exact_schema(conn, sqlite3, AUTHORITY_SCHEMA, "authority v5")
        conn.execute("COMMIT")
        return conn, coord_root, {
            "schema_version": int(meta[0]), "cluster_id": str(meta[1]),
            "coordinator_device_id": str(meta[2]), "authority_epoch": int(meta[3]),
            "sealed": bool(meta[4]), "lease_seconds": int(meta[5]),
            "created_at_ns": int(meta[6]), "filesystem": probe,
            "sqlite_version": sqlite3.sqlite_version,
        }
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        conn.close()
        raise


def _token_sha256(token: str) -> str:
    if not isinstance(token, str) or not token:
        raise RunnerError("owner_token is required")
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
    except (ValueError, binascii.Error) as exc:
        raise RunnerError("owner_token is not valid base64url") from exc
    if len(raw) != 32:
        raise RunnerError("owner_token must contain 32 random bytes")
    return hashlib.sha256(raw).hexdigest()


def _secret_path(root: str, cluster_id: str, target_device_id: str,
                 authority_epoch: int, key_id: str) -> str:
    safe = (cluster_id + target_device_id + str(authority_epoch) + key_id).replace("-", "")
    if not safe.isalnum():
        raise RunnerError("invalid key identity")
    return os.path.join(
        root, "keys", f"{cluster_id}.{target_device_id}.e{authority_epoch}.{key_id}.key"
    )


def _write_secret(path: str, secret: bytes) -> None:
    _publish_secret(path, secret, replace=False)


def _replace_unenrolled_secret(path: str, secret: bytes) -> None:
    _publish_secret(path, secret, replace=True)


def _publish_secret(path: str, secret: bytes, *, replace: bool) -> None:
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=".enrollment-key-", suffix=".tmp", dir=parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(secret)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.chmod(temp_path, 0o600)
        except OSError:
            pass
        if replace:
            os.replace(temp_path, path)
        else:
            os.link(temp_path, path)
            os.unlink(temp_path)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
        _fsync_directory(parent)
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _lease_dict(row) -> dict:
    return {
        "lease_id": str(row[0]), "project_key": str(row[1]), "acquire_id": str(row[2]),
        "fence": int(row[3]), "controller_id": str(row[4]),
        "controller_replica_id": str(row[5]), "owner_host": row[6], "owner_pid": row[7],
        "phase": str(row[8]), "state": str(row[9]), "recovery_of_lease_id": row[10],
        "heartbeat_seq": int(row[11]), "acquired_at_ns": int(row[12]),
        "heartbeat_at_ns": int(row[13]), "lease_until_ns": int(row[14]),
        "released_at_ns": row[15],
    }


_LEASE_SELECT = (
    "SELECT lease_id,project_key,acquire_id,fence,controller_id,controller_replica_id,"
    "owner_host,owner_pid,phase,state,recovery_of_lease_id,heartbeat_seq,acquired_at_ns,"
    "heartbeat_at_ns,lease_until_ns,released_at_ns FROM leases"
)


def _request_digest(fields: dict) -> str:
    return hashlib.sha256(canonical_json(fields)).hexdigest()


def _acquire_request_digest(body: dict, token_hash: str) -> str:
    return _request_digest({
        "cluster_id": str(body.get("cluster_id", "")),
        "project_key": str(body.get("project_key", "")),
        "policy_sha256": str(body.get("policy_sha256", "")),
        "acquire_id": str(body.get("acquire_id", "")),
        "owner_token_sha256": token_hash,
        "controller_id": str(body.get("controller_id", "")),
        "controller_replica_id": str(body.get("controller_replica_id", "")),
        "owner_host": body.get("owner_host"),
        "owner_pid": body.get("owner_pid"),
    })


def _unresolved_grants(conn, project_key: str, *, below_fence: int | None = None):
    sql = (
        "SELECT grant_id,fence,target_device_id,operation,operation_id,state FROM grants "
        "WHERE project_key=? AND state IN ('ISSUED','UNKNOWN','ACCEPTED')"
    )
    params = [project_key]
    if below_fence is not None:
        sql += " AND fence<?"
        params.append(below_fence)
    return conn.execute(sql + " ORDER BY fence,grant_id", params).fetchall()


def authority_rpc(state_root: str, runner_meta: dict, operation: str, body: dict,
                  rpc_id: str, request_sha: str) -> dict:
    cluster_id = str(body.get("cluster_id", ""))
    create = operation == "authority_init"
    conn, coord_root, meta = open_authority_store(
        state_root, cluster_id, runner_meta["device_id"], create=create,
        lease_seconds=int(body.get("lease_seconds", DEFAULT_LEASE_SECONDS)),
    )
    try:
        if operation == "authority_init" or operation == "authority_probe":
            return {"ok": True, "authority_root": coord_root, **meta}
        if meta["sealed"]:
            raise RunnerError("authority is sealed")
        now = time.time_ns()
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT operation,request_sha256,response_json FROM authority_rpc_requests "
                "WHERE rpc_id=?", (rpc_id,),
            ).fetchone()
            if existing:
                if existing[0] != operation or existing[1] != request_sha:
                    raise RunnerError("authority rpc_id reused with a different request")
                response = json.loads(bytes(existing[2]).decode("utf-8"))
                conn.execute("COMMIT")
                return response
            if operation == "authority_project_register":
                project_key = str(body.get("project_key", ""))
                project_id = str(body.get("project_id", ""))
                policy = str(body.get("policy_sha256", ""))
                if len(project_key) != 64 or len(policy) != 64 or not project_id:
                    raise RunnerError("project_key, project_id, and policy_sha256 are required")
                prior = conn.execute(
                    "SELECT project_id,policy_sha256,state FROM projects WHERE project_key=?",
                    (project_key,),
                ).fetchone()
                if prior and (prior[0] != project_id or prior[1] != policy):
                    raise RunnerError("project registration mismatch")
                if not prior:
                    conn.execute(
                        "INSERT INTO projects "
                        "(project_key,project_id,policy_sha256,state,created_at_ns,updated_at_ns) "
                        "VALUES (?,?,?,'BOOTSTRAP',?,?)",
                        (project_key, project_id, policy, now, now),
                    )
                response = {"ok": True, "status": "REGISTERED", "project_key": project_key}
            elif operation == "authority_acquire":
                response = _authority_acquire(conn, body, meta, now)
            elif operation == "authority_heartbeat":
                response = _authority_heartbeat(conn, body, meta, now)
            elif operation == "authority_release":
                response = _authority_release(conn, body, meta, now)
            elif operation == "authority_target_key_create":
                response = _authority_target_key_create(
                    conn, coord_root, body, meta, now
                )
            elif operation == "authority_target_key_finalize":
                response = _authority_target_key_finalize(conn, coord_root, body, meta)
            elif operation == "authority_epoch_rotate":
                response = _authority_epoch_rotate(conn, body, meta)
            elif operation == "authority_grant_issue":
                response = _authority_grant_issue(conn, coord_root, body, meta, now)
            elif operation == "authority_grant_import":
                response = _authority_grant_import(conn, coord_root, body, meta, now)
            elif operation == "authority_recovery_complete":
                response = _authority_recovery_complete(conn, body, now)
            elif operation == "authority_recovery_worklist":
                response = _authority_recovery_worklist(conn, body, now)
            else:
                raise RunnerError(f"unknown authority RPC operation: {operation!r}")
            conn.execute(
                "INSERT INTO authority_rpc_requests "
                "(rpc_id,operation,request_sha256,response_json,created_at_ns) "
                "VALUES (?,?,?,?,?)",
                (rpc_id, operation, request_sha, canonical_json(response), now),
            )
            conn.execute("COMMIT")
            _test_fault_point("after_authority_commit")
            return response
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
    finally:
        conn.close()


def _authority_acquire(conn, body: dict, meta: dict, now: int) -> dict:
    project_key = str(body.get("project_key", ""))
    acquire_id = str(body.get("acquire_id", ""))
    token_hash = _token_sha256(body.get("owner_token"))
    request_sha = _acquire_request_digest(body, token_hash)
    if not acquire_id:
        raise RunnerError("acquire_id is required")
    prior = conn.execute(
        _LEASE_SELECT + " WHERE acquire_id=?", (acquire_id,),
    ).fetchone()
    if prior:
        stored_hash, stored_request = conn.execute(
            "SELECT owner_token_sha256,request_sha256 FROM leases WHERE acquire_id=?",
            (acquire_id,),
        ).fetchone()
        if stored_hash != token_hash or stored_request != request_sha:
            raise RunnerError("acquire_id reused with a different request")
        current = conn.execute(
            "SELECT current_lease_id FROM projects WHERE project_key=?", (prior[1],)
        ).fetchone()
        if prior[9] == "ACTIVE" and current and current[0] == prior[0] \
                and now < int(prior[14]):
            status = "ACQUIRED"
        elif prior[9] in ("SUPERSEDED", "RELEASED"):
            status = prior[9]
        else:
            status = "LOST"
        return {"ok": True, "status": status, "idempotent": True,
                "lease": _lease_dict(prior)}
    project = conn.execute(
        "SELECT policy_sha256,next_fence,current_lease_id,active_run_id,pending_txn_id,state,"
        "recovery_prior_state "
        "FROM projects WHERE project_key=?", (project_key,),
    ).fetchone()
    if project is None:
        raise RunnerError("project is not registered")
    if str(body.get("policy_sha256", "")) != project[0]:
        raise RunnerError("project surface policy mismatch")
    current = None
    if project[2]:
        current = conn.execute(_LEASE_SELECT + " WHERE lease_id=?", (project[2],)).fetchone()
    if current and current[9] == "ACTIVE" and now < int(current[14]):
        return {"ok": True, "status": "BUSY", "holder": {
            "lease_id": current[0], "fence": int(current[3]), "controller_id": current[4],
            "owner_host": current[6], "owner_pid": current[7],
            "lease_until_ns": int(current[14]),
        }}
    fence = int(project[1]) + 1
    lease_id = str(uuid.uuid4())
    phase = "RECOVERY" if project[5] == "RECOVERY" or project[3] or project[4] \
        or _unresolved_grants(conn, project_key) else "NORMAL"
    until = now + int(meta["lease_seconds"]) * 1_000_000_000
    conn.execute(
        "INSERT INTO leases (lease_id,project_key,acquire_id,request_sha256,fence,owner_token_sha256,"
        "controller_id,controller_replica_id,owner_host,owner_pid,phase,state,"
        "recovery_of_lease_id,acquired_at_ns,heartbeat_at_ns,lease_until_ns) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,'ACTIVE',?,?,?,?)",
        (lease_id, project_key, acquire_id, request_sha, fence, token_hash,
         str(body.get("controller_id", "")), str(body.get("controller_replica_id", "")),
         body.get("owner_host"), body.get("owner_pid"), phase,
         current[0] if current else None, now, now, until),
    )
    if current:
        conn.execute("UPDATE leases SET state='SUPERSEDED' WHERE lease_id=?", (current[0],))
    next_state = "RECOVERY" if phase == "RECOVERY" else project[5]
    prior_state = project[6]
    if phase == "RECOVERY" and project[5] != "RECOVERY":
        prior_state = project[5]
    conn.execute(
        "UPDATE projects SET next_fence=?,current_lease_id=?,state=?,"
        "recovery_prior_state=?,updated_at_ns=? WHERE project_key=?",
        (fence, lease_id, next_state, prior_state, now, project_key),
    )
    row = conn.execute(_LEASE_SELECT + " WHERE lease_id=?", (lease_id,)).fetchone()
    return {"ok": True, "status": "ACQUIRED", "idempotent": False,
            "lease": _lease_dict(row)}


def _lease_credential(body: dict) -> tuple[str, str, int, str]:
    return (str(body.get("project_key", "")), str(body.get("lease_id", "")),
            int(body.get("fence", -1)), _token_sha256(body.get("owner_token")))


def _authority_heartbeat(conn, body: dict, meta: dict, now: int) -> dict:
    project_key, lease_id, fence, token_hash = _lease_credential(body)
    until = now + int(meta["lease_seconds"]) * 1_000_000_000
    cur = conn.execute(
        "UPDATE leases SET heartbeat_seq=heartbeat_seq+1,heartbeat_at_ns=?,"
        "lease_until_ns=max(lease_until_ns,?) WHERE lease_id=? AND project_key=? AND fence=? "
        "AND owner_token_sha256=? AND state='ACTIVE' AND lease_id=(SELECT current_lease_id "
        "FROM projects WHERE project_key=?)",
        (now, until, lease_id, project_key, fence, token_hash, project_key),
    )
    if cur.rowcount != 1:
        return {"ok": True, "status": "LOST"}
    row = conn.execute(_LEASE_SELECT + " WHERE lease_id=?", (lease_id,)).fetchone()
    return {"ok": True, "status": "OWNED", "lease": _lease_dict(row)}


def _authority_release(conn, body: dict, meta: dict, now: int) -> dict:
    del meta
    project_key, lease_id, fence, token_hash = _lease_credential(body)
    project = conn.execute(
        "SELECT current_lease_id,active_run_id,pending_txn_id,state "
        "FROM projects WHERE project_key=?",
        (project_key,),
    ).fetchone()
    if project is None or project[0] != lease_id:
        return {"ok": True, "status": "LOST"}
    lease_phase = conn.execute(
        "SELECT phase FROM leases WHERE lease_id=?", (lease_id,),
    ).fetchone()
    if project[3] == "RECOVERY" or not lease_phase or lease_phase[0] == "RECOVERY" \
            or project[1] or project[2] or _unresolved_grants(conn, project_key):
        return {"ok": True, "status": "RECOVERY_REQUIRED"}
    cur = conn.execute(
        "UPDATE leases SET state='RELEASED',released_at_ns=? WHERE lease_id=? AND "
        "project_key=? AND fence=? AND owner_token_sha256=? AND state='ACTIVE'",
        (now, lease_id, project_key, fence, token_hash),
    )
    if cur.rowcount != 1:
        return {"ok": True, "status": "LOST"}
    cleared = conn.execute(
        "UPDATE projects SET current_lease_id=NULL,updated_at_ns=? WHERE project_key=? "
        "AND current_lease_id=?", (now, project_key, lease_id),
    )
    if cleared.rowcount != 1:
        raise RunnerError("conditional lease release lost the project pointer")
    return {"ok": True, "status": "RELEASED"}


def _authority_target_key_create(conn, coord_root: str, body: dict,
                                 meta: dict, now: int) -> dict:
    target = str(body.get("target_device_id", ""))
    if not target:
        raise RunnerError("target_device_id is required")
    prior = conn.execute(
        "SELECT key_id,key_sha256,state FROM authority_targets WHERE target_device_id=? "
        "AND authority_epoch=?",
        (target, meta["authority_epoch"]),
    ).fetchone()
    if prior:
        return {"ok": True, "status": prior[2], "target_device_id": target,
                "authority_epoch": meta["authority_epoch"], "key_id": prior[0],
                "key_sha256": prior[1], "idempotent": True}
    key_id = str(uuid.uuid4())
    secret = os.urandom(32)
    digest = hashlib.sha256(secret).hexdigest()
    coord_key = _secret_path(
        coord_root, meta["cluster_id"], target, meta["authority_epoch"], key_id
    )
    _write_secret(coord_key, secret)
    conn.execute(
        "INSERT INTO authority_targets "
        "(target_device_id,authority_epoch,key_id,key_sha256,state,created_at_ns) "
        "VALUES (?,?,?,?,?,?)",
        (target, meta["authority_epoch"], key_id, digest, "PENDING", now),
    )
    return {"ok": True, "status": "PENDING", "target_device_id": target,
            "authority_epoch": meta["authority_epoch"], "key_id": key_id,
            "key_sha256": digest, "idempotent": False}


def _authority_target_key_finalize(conn, coord_root: str, body: dict, meta: dict) -> dict:
    receipt = body.get("receipt")
    if not isinstance(receipt, dict) or receipt.get("sig_alg") != "hmac-sha256":
        raise RunnerError("invalid target enrollment receipt")
    receipt_body = receipt.get("body")
    if not isinstance(receipt_body, dict) or receipt_body.get("kind") != "key-enrollment" \
            or int(receipt_body.get("v", 0)) != 1:
        raise RunnerError("invalid target enrollment receipt body")
    target = str(receipt_body.get("target_device_id", ""))
    epoch = int(receipt_body.get("authority_epoch", 0))
    key_id = str(receipt_body.get("key_id", ""))
    digest = str(receipt_body.get("key_sha256", ""))
    if str(receipt_body.get("cluster_id", "")) != meta["cluster_id"] \
            or epoch != int(meta["authority_epoch"]):
        raise RunnerError("target enrollment receipt identity or epoch mismatch")
    row = conn.execute(
        "SELECT key_id,key_sha256,state FROM authority_targets WHERE target_device_id=? "
        "AND authority_epoch=?", (target, epoch),
    ).fetchone()
    if row is None or row[0] != key_id or row[1] != digest:
        raise RunnerError("target enrollment receipt does not match the prepared key")
    key_path = _secret_path(coord_root, meta["cluster_id"], target, epoch, key_id)
    with open(key_path, "rb") as stream:
        secret = stream.read()
    expected = hmac.new(secret, canonical_json(receipt_body), hashlib.sha256).digest()
    if not hmac.compare_digest(
            expected, _decode_b64url(str(receipt.get("sig", "")))):
        raise RunnerError("target enrollment receipt signature mismatch")
    if row[2] == "ENROLLED":
        return {"ok": True, "status": "ENROLLED", "target_device_id": target,
                "authority_epoch": epoch, "key_id": key_id,
                "key_sha256": digest, "idempotent": True}
    if row[2] != "PENDING":
        raise RunnerError("target enrollment is not pending")
    conn.execute(
        "UPDATE authority_targets SET state='ENROLLED' WHERE target_device_id=? "
        "AND authority_epoch=? AND key_id=? AND state='PENDING'",
        (target, epoch, key_id),
    )
    return {"ok": True, "status": "ENROLLED", "target_device_id": target,
            "authority_epoch": epoch, "key_id": key_id,
            "key_sha256": digest, "idempotent": False}


def _authority_epoch_rotate(conn, body: dict, meta: dict) -> dict:
    expected = int(body.get("expected_authority_epoch", 0))
    if expected != int(meta["authority_epoch"]):
        raise RunnerError("authority epoch rotation expectation mismatch")
    busy = conn.execute(
        "SELECT project_key FROM projects WHERE current_lease_id IS NOT NULL "
        "OR active_run_id IS NOT NULL OR pending_txn_id IS NOT NULL LIMIT 1"
    ).fetchone()
    unresolved = conn.execute(
        "SELECT grant_id FROM grants WHERE state IN ('ISSUED','UNKNOWN','ACCEPTED') LIMIT 1"
    ).fetchone()
    pending_target = conn.execute(
        "SELECT target_device_id FROM authority_targets WHERE state='PENDING' LIMIT 1"
    ).fetchone()
    if busy or unresolved or pending_target:
        return {"ok": True, "status": "NOT_QUIESCENT"}
    next_epoch = expected + 1
    conn.execute(
        "UPDATE authority_targets SET state='RETIRED' WHERE authority_epoch=? "
        "AND state='ENROLLED'", (expected,),
    )
    conn.execute(
        "UPDATE authority_meta SET authority_epoch=? WHERE singleton=1", (next_epoch,)
    )
    return {"ok": True, "status": "ROTATED", "authority_epoch": next_epoch,
            "previous_authority_epoch": expected}


def _authority_grant_issue(conn, coord_root: str, body: dict, meta: dict, now: int) -> dict:
    project_key, lease_id, fence, token_hash = _lease_credential(body)
    request_id = str(body.get("grant_request_id", ""))
    target = str(body.get("target_device_id", ""))
    operation = str(body.get("grant_operation", ""))
    operation_id = str(body.get("operation_id", ""))
    request_sha = str(body.get("request_sha256", ""))
    if not request_id:
        raise RunnerError("grant_request_id is required")
    if not operation or not operation_id or len(request_sha) != 64:
        raise RunnerError("grant operation, operation_id, and request_sha256 are required")
    issue_sha = _request_digest({
        "project_key": project_key, "lease_id": lease_id, "fence": fence,
        "owner_token_sha256": token_hash, "grant_request_id": request_id,
        "target_device_id": target, "operation": operation,
        "operation_id": operation_id, "request_sha256": request_sha,
    })
    prior = conn.execute(
        "SELECT issue_sha256,capability_json,state FROM grants WHERE grant_request_id=?",
        (request_id,),
    ).fetchone()
    if prior:
        if prior[0] != issue_sha:
            raise RunnerError("grant_request_id reused with a different request")
        return {"ok": True, "status": prior[2], "idempotent": True,
                "capability": json.loads(bytes(prior[1]).decode("utf-8"))}
    lease = conn.execute(
        "SELECT leases.state,leases.lease_until_ns,leases.phase,projects.state FROM leases "
        "JOIN projects ON projects.project_key=leases.project_key "
        "WHERE leases.lease_id=? AND leases.project_key=? AND leases.fence=? "
        "AND owner_token_sha256=? AND leases.lease_id=(SELECT current_lease_id FROM projects "
        "WHERE project_key=?)",
        (lease_id, project_key, fence, token_hash, project_key),
    ).fetchone()
    if not lease or lease[0] != "ACTIVE" or now >= int(lease[1]):
        return {"ok": True, "status": "LOST"}
    recovery_operations = {"fence_barrier"}
    if operation not in recovery_operations \
            and (lease[2] == "RECOVERY" or lease[3] == "RECOVERY"
                 or _unresolved_grants(conn, project_key)):
        return {"ok": True, "status": "RECOVERY_REQUIRED"}
    target_row = conn.execute(
        "SELECT key_id FROM authority_targets WHERE target_device_id=? AND authority_epoch=? "
        "AND state='ENROLLED'",
        (target, meta["authority_epoch"]),
    ).fetchone()
    if target_row is None:
        raise RunnerError("target is not enrolled")
    grant_id = str(uuid.uuid4())
    cap_body = {
        "v": 1, "cluster_id": meta["cluster_id"],
        "authority_epoch": meta["authority_epoch"], "grant_id": grant_id,
        "project_key": project_key, "lease_id": lease_id, "fence": fence,
        "target_device_id": target, "operation": operation,
        "operation_id": operation_id, "request_sha256": request_sha,
    }
    key_id = str(target_row[0])
    key_path = _secret_path(
        coord_root, meta["cluster_id"], target, meta["authority_epoch"], key_id
    )
    with open(key_path, "rb") as stream:
        secret = stream.read()
    signature = _b64url(hmac.new(secret, canonical_json(cap_body), hashlib.sha256).digest())
    capability = {"body": cap_body, "sig_alg": "hmac-sha256",
                  "key_id": key_id, "sig": signature}
    packed = canonical_json(capability)
    conn.execute(
        "INSERT INTO grants (grant_id,grant_request_id,project_key,lease_id,authority_epoch,"
        "fence,target_device_id,operation,operation_id,request_sha256,issue_sha256,"
        "capability_json,state,issued_at_ns) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'ISSUED',?)",
        (grant_id, request_id, project_key, lease_id, meta["authority_epoch"], fence,
         target, operation, operation_id, request_sha, issue_sha, packed, now),
    )
    return {"ok": True, "status": "ISSUED", "idempotent": False,
            "capability": capability}


def _verify_status_receipt(conn, coord_root: str, receipt: dict,
                           meta: dict) -> tuple[dict, str]:
    if not isinstance(receipt, dict) or receipt.get("sig_alg") != "hmac-sha256":
        raise RunnerError("invalid grant status receipt envelope")
    body = receipt.get("body")
    if not isinstance(body, dict) or int(body.get("v", 0)) != 1 \
            or body.get("kind") != "grant-status":
        raise RunnerError("invalid grant status receipt body")
    cluster = str(body.get("cluster_id", ""))
    target = str(body.get("target_device_id", ""))
    epoch = int(body.get("authority_epoch", 0))
    key_id = str(receipt.get("key_id", ""))
    if cluster != meta["cluster_id"] or epoch != int(meta["authority_epoch"]):
        raise RunnerError("grant status receipt authority mismatch")
    target_row = conn.execute(
        "SELECT key_id,key_sha256,state FROM authority_targets WHERE target_device_id=? "
        "AND authority_epoch=?", (target, epoch),
    ).fetchone()
    if target_row is None or target_row[2] != "ENROLLED" or target_row[0] != key_id:
        raise RunnerError("grant status receipt key is not enrolled")
    key_path = _secret_path(coord_root, cluster, target, epoch, key_id)
    try:
        with open(key_path, "rb") as stream:
            secret = stream.read()
    except OSError as exc:
        raise RunnerError("grant status receipt key is unavailable") from exc
    if hashlib.sha256(secret).hexdigest() != target_row[1]:
        raise RunnerError("grant status receipt key fingerprint mismatch")
    expected = hmac.new(secret, canonical_json(body), hashlib.sha256).digest()
    actual = _decode_b64url(str(receipt.get("sig", "")))
    if not hmac.compare_digest(expected, actual):
        raise RunnerError("grant status receipt signature mismatch")
    return body, hashlib.sha256(canonical_json(receipt)).hexdigest()


def _authority_grant_import(conn, coord_root: str, body: dict,
                            meta: dict, now: int) -> dict:
    project_key, lease_id, fence, token_hash = _lease_credential(body)
    lease = conn.execute(
        "SELECT state,lease_until_ns,phase FROM leases WHERE lease_id=? AND project_key=? "
        "AND fence=? AND owner_token_sha256=? AND lease_id=(SELECT current_lease_id "
        "FROM projects WHERE project_key=?)",
        (lease_id, project_key, fence, token_hash, project_key),
    ).fetchone()
    if not lease or lease[0] != "ACTIVE" or now >= int(lease[1]):
        return {"ok": True, "status": "LOST"}
    if lease[2] != "RECOVERY":
        return {"ok": True, "status": "NOT_RECOVERING"}
    receipt_body, receipt_sha = _verify_status_receipt(
        conn, coord_root, body.get("receipt"), meta
    )
    grant_id = str(receipt_body.get("grant_id", ""))
    grant = conn.execute(
        "SELECT project_key,lease_id,authority_epoch,fence,target_device_id,operation,"
        "operation_id,request_sha256,state,result_json,last_receipt_sha256 FROM grants "
        "WHERE grant_id=?", (grant_id,),
    ).fetchone()
    if grant is None:
        raise RunnerError("grant status receipt names an unknown grant")
    expected = (
        grant[0], grant[1], int(grant[2]), int(grant[3]), grant[4], grant[5],
        grant[6], grant[7],
    )
    observed = (
        str(receipt_body.get("project_key", "")),
        str(receipt_body.get("lease_id", "")),
        int(receipt_body.get("authority_epoch", 0)),
        int(receipt_body.get("fence", -1)),
        str(receipt_body.get("target_device_id", "")),
        str(receipt_body.get("operation", "")),
        str(receipt_body.get("operation_id", "")),
        str(receipt_body.get("request_sha256", "")),
    )
    if expected != observed or grant[0] != project_key:
        raise RunnerError("grant status receipt does not match the authority grant")
    if grant[10] == receipt_sha:
        return {"ok": True, "status": grant[8], "idempotent": True,
                "grant_id": grant_id}
    receipt_status = str(receipt_body.get("status", ""))
    if receipt_status not in {"ABSENT", "ACCEPTED", "TERMINAL"}:
        raise RunnerError("unsupported grant status receipt state")
    current_state = str(grant[8])
    if current_state in {"TERMINAL", "FENCED"}:
        if receipt_status != current_state:
            raise RunnerError("grant status receipt would regress a terminal grant")
        stored_result = json.loads(bytes(grant[9]).decode("utf-8")) if grant[9] else None
        if stored_result != receipt_body.get("result"):
            raise RunnerError("terminal grant receipt result mismatch")
        conn.execute(
            "UPDATE grants SET last_receipt_sha256=? WHERE grant_id=?",
            (receipt_sha, grant_id),
        )
        return {"ok": True, "status": current_state, "idempotent": True,
                "grant_id": grant_id}
    if receipt_status == "ABSENT":
        if current_state == "ACCEPTED":
            raise RunnerError("grant status receipt would regress an accepted grant")
        next_state = "UNKNOWN"
        result = None
    elif receipt_status == "ACCEPTED":
        next_state = "ACCEPTED"
        result = receipt_body.get("result")
    else:
        next_state = "TERMINAL"
        result = receipt_body.get("result")

    if next_state == "TERMINAL" and grant[5] == "fence_barrier":
        _reconcile_barrier_receipt(
            conn, project_key, int(grant[2]), int(grant[3]), str(grant[4]),
            result, now,
        )
    conn.execute(
        "UPDATE grants SET state=?,result_json=?,accepted_at_ns=CASE WHEN ? IN "
        "('ACCEPTED','TERMINAL') THEN coalesce(accepted_at_ns,?) ELSE accepted_at_ns END,"
        "terminal_at_ns=CASE WHEN ?='TERMINAL' THEN coalesce(terminal_at_ns,?) "
        "ELSE terminal_at_ns END,last_receipt_sha256=? WHERE grant_id=?",
        (next_state, canonical_json(result) if result is not None else None,
         next_state, now, next_state, now, receipt_sha, grant_id),
    )
    return {"ok": True, "status": next_state, "idempotent": False,
            "grant_id": grant_id}


def _reconcile_barrier_receipt(conn, project_key: str, epoch: int, barrier_fence: int,
                               target: str, result, now: int) -> None:
    if not isinstance(result, dict) or not isinstance(
            result.get("lower_fence_operations"), list):
        raise RunnerError("barrier receipt is missing lower-fence observations")
    observed: dict[str, dict] = {}
    for item in result["lower_fence_operations"]:
        if not isinstance(item, dict):
            raise RunnerError("invalid barrier lower-fence observation")
        grant_id = str(item.get("grant_id", ""))
        if not grant_id or grant_id in observed:
            raise RunnerError("duplicate or empty barrier lower-fence grant")
        row = conn.execute(
            "SELECT project_key,authority_epoch,fence,target_device_id,operation,operation_id,"
            "request_sha256,state,result_json FROM grants WHERE grant_id=?", (grant_id,),
        ).fetchone()
        expected = (
            project_key, epoch, int(item.get("fence", -1)), target,
            str(item.get("operation", "")), str(item.get("operation_id", "")),
            str(item.get("request_sha256", "")),
        )
        item_state = str(item.get("state", ""))
        if row is None or tuple(row[:7]) != expected or int(row[2]) >= barrier_fence \
                or item_state not in {"ACCEPTED", "TERMINAL"} or row[7] == "FENCED":
            raise RunnerError("barrier lower-fence observation is inconsistent")
        stored_result = json.loads(bytes(row[8]).decode("utf-8")) if row[8] else None
        if row[7] == "TERMINAL" \
                and (item_state != "TERMINAL" or stored_result != item.get("result")):
            raise RunnerError("barrier terminal observation conflicts with authority state")
        observed[grant_id] = item
    unresolved = conn.execute(
        "SELECT grant_id,state FROM grants WHERE project_key=? AND authority_epoch=? "
        "AND target_device_id=? AND fence<? AND state IN "
        "('ISSUED','UNKNOWN','ACCEPTED')",
        (project_key, epoch, target, barrier_fence),
    ).fetchall()
    for grant_id, state in unresolved:
        if grant_id in observed:
            item = observed[grant_id]
            if item["state"] == "TERMINAL":
                result_json = canonical_json(item.get("result")) \
                    if item.get("result") is not None else None
                conn.execute(
                    "UPDATE grants SET state='TERMINAL',result_json=?,"
                    "accepted_at_ns=coalesce(accepted_at_ns,?),"
                    "terminal_at_ns=coalesce(terminal_at_ns,?) WHERE grant_id=?",
                    (result_json, now, now, grant_id),
                )
            else:
                conn.execute(
                    "UPDATE grants SET state='ACCEPTED',"
                    "accepted_at_ns=coalesce(accepted_at_ns,?) WHERE grant_id=?",
                    (now, grant_id),
                )
        elif state == "ACCEPTED":
            raise RunnerError("barrier omitted a grant already known accepted")
        else:
            conn.execute(
                "UPDATE grants SET state='FENCED',terminal_at_ns=coalesce(terminal_at_ns,?) "
                "WHERE grant_id=?", (now, grant_id),
            )


def _authority_recovery_complete(conn, body: dict, now: int) -> dict:
    project_key, lease_id, fence, token_hash = _lease_credential(body)
    lease = conn.execute(
        "SELECT state,lease_until_ns,phase FROM leases WHERE lease_id=? AND project_key=? "
        "AND fence=? AND owner_token_sha256=? AND lease_id=(SELECT current_lease_id "
        "FROM projects WHERE project_key=?)",
        (lease_id, project_key, fence, token_hash, project_key),
    ).fetchone()
    if not lease or lease[0] != "ACTIVE" or now >= int(lease[1]):
        return {"ok": True, "status": "LOST"}
    project = conn.execute(
        "SELECT state,recovery_prior_state,active_run_id,pending_txn_id FROM projects "
        "WHERE project_key=?", (project_key,),
    ).fetchone()
    if lease[2] != "RECOVERY" and project and project[0] != "RECOVERY":
        return {"ok": True, "status": "NORMAL", "idempotent": True}
    unresolved = _unresolved_grants(conn, project_key)
    if not project or project[2] or project[3] or unresolved:
        return {"ok": True, "status": "RECOVERY_REQUIRED", "operations": [
            {"grant_id": row[0], "fence": int(row[1]), "target_device_id": row[2],
             "operation": row[3], "operation_id": row[4], "state": row[5]}
            for row in unresolved
        ]}
    restored = project[1] or "BOOTSTRAP"
    conn.execute("UPDATE leases SET phase='NORMAL' WHERE lease_id=?", (lease_id,))
    conn.execute(
        "UPDATE projects SET state=?,recovery_prior_state=NULL,updated_at_ns=? "
        "WHERE project_key=? AND current_lease_id=?",
        (restored, now, project_key, lease_id),
    )
    return {"ok": True, "status": "NORMAL", "idempotent": False,
            "restored_project_state": restored}


def _authority_recovery_worklist(conn, body: dict, now: int) -> dict:
    project_key, lease_id, fence, token_hash = _lease_credential(body)
    lease = conn.execute(
        "SELECT leases.state,leases.lease_until_ns,leases.phase,projects.state "
        "FROM leases JOIN projects ON projects.project_key=leases.project_key "
        "WHERE leases.lease_id=? AND leases.project_key=? AND leases.fence=? "
        "AND leases.owner_token_sha256=? AND leases.state='ACTIVE' "
        "AND leases.lease_id=projects.current_lease_id",
        (lease_id, project_key, fence, token_hash),
    ).fetchone()
    if not lease or now >= int(lease[1]):
        return {"ok": True, "status": "LOST"}
    if lease[2] != "RECOVERY" or lease[3] != "RECOVERY":
        return {"ok": True, "status": "NOT_RECOVERING"}
    rows = conn.execute(
        "SELECT grant_id,lease_id,authority_epoch,fence,target_device_id,operation,"
        "operation_id,request_sha256,state,capability_json FROM grants WHERE project_key=? "
        "AND state IN ('ISSUED','UNKNOWN','ACCEPTED') ORDER BY fence,grant_id",
        (project_key,),
    ).fetchall()
    return {"ok": True, "status": "RECOVERY", "grants": [
        {"grant_id": str(row[0]), "lease_id": str(row[1]),
         "authority_epoch": int(row[2]), "fence": int(row[3]),
         "target_device_id": str(row[4]), "operation": str(row[5]),
         "operation_id": str(row[6]), "request_sha256": str(row[7]),
         "state": str(row[8]),
         "capability": json.loads(bytes(row[9]).decode("utf-8"))}
        for row in rows
    ]}


def _decode_b64url(value: str) -> bytes:
    try:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (ValueError, binascii.Error) as exc:
        raise RunnerError("invalid base64url value") from exc


def _verify_capability(conn, runner_root: str, runner_meta: dict,
                       capability: dict) -> tuple[dict, str, bytes]:
    if not isinstance(capability, dict) or capability.get("sig_alg") != "hmac-sha256":
        raise RunnerError("invalid capability envelope")
    body = capability.get("body")
    if not isinstance(body, dict) or int(body.get("v", 0)) != 1:
        raise RunnerError("invalid capability body")
    target = str(body.get("target_device_id", ""))
    if target != runner_meta["device_id"]:
        raise RunnerError("capability target mismatch")
    cluster = str(body.get("cluster_id", ""))
    epoch = int(body.get("authority_epoch", 0))
    key_id = str(capability.get("key_id", ""))
    enrollment = conn.execute(
        "SELECT key_sha256,state FROM enrolled_authorities WHERE cluster_id=? "
        "AND authority_epoch=? AND key_id=?",
        (cluster, epoch, key_id),
    ).fetchone()
    if enrollment is None or enrollment[1] != "ENROLLED":
        raise RunnerError("capability key is not enrolled for this authority epoch")
    key_path = _secret_path(runner_root, cluster, target, epoch, key_id)
    try:
        with open(key_path, "rb") as stream:
            secret = stream.read()
    except OSError as exc:
        raise RunnerError("capability key is not enrolled") from exc
    if hashlib.sha256(secret).hexdigest() != enrollment[0]:
        raise RunnerError("capability key fingerprint mismatch")
    expected = hmac.new(secret, canonical_json(body), hashlib.sha256).digest()
    actual = _decode_b64url(str(capability.get("sig", "")))
    if not hmac.compare_digest(expected, actual):
        raise RunnerError("capability signature mismatch")
    return body, key_id, secret


def _accept_capability(conn, runner_root: str, runner_meta: dict, capability: dict) -> dict:
    body, _key_id, _secret = _verify_capability(
        conn, runner_root, runner_meta, capability
    )
    cluster = str(body.get("cluster_id", ""))
    epoch = int(body.get("authority_epoch", 0))
    grant_id = str(body.get("grant_id", ""))
    project_key = str(body.get("project_key", ""))
    fence = int(body.get("fence", -1))
    request_sha = str(body.get("request_sha256", ""))
    operation = str(body.get("operation", ""))
    operation_id = str(body.get("operation_id", ""))
    prior = conn.execute(
        "SELECT cluster_id,project_key,authority_epoch,fence,operation,operation_id,"
        "request_sha256,state,result_json FROM accepted_grants WHERE grant_id=?",
        (grant_id,),
    ).fetchone()
    immutable = (cluster, project_key, epoch, fence, operation, operation_id, request_sha)
    if prior:
        if tuple(prior[:7]) != immutable:
            raise RunnerError("grant_id reused with different capability fields")
        result = json.loads(bytes(prior[8]).decode("utf-8")) if prior[8] else None
        return {"ok": True, "status": prior[7], "idempotent": True, "result": result}
    stored = conn.execute(
        "SELECT authority_epoch,max_fence FROM project_fences "
        "WHERE cluster_id=? AND project_key=?", (cluster, project_key),
    ).fetchone()
    if stored and epoch < int(stored[0]):
        return {"ok": True, "status": "FENCED", "reason": "older authority epoch"}
    max_fence = int(stored[1]) if stored and epoch == int(stored[0]) else -1
    if fence < max_fence:
        return {"ok": True, "status": "FENCED", "reason": "stale fence"}
    lower = conn.execute(
        "SELECT grant_id,operation,operation_id,fence,request_sha256,state,result_json "
        "FROM accepted_grants "
        "WHERE cluster_id=? AND project_key=? AND authority_epoch=? AND fence<? "
        "AND state!='FENCED' ORDER BY fence,grant_id",
        (cluster, project_key, epoch, fence),
    ).fetchall()
    blocking_lower = [row for row in lower if row[5] != "TERMINAL"]
    if operation != "fence_barrier" and blocking_lower:
        return {"ok": True, "status": "RECOVERY_REQUIRED",
                "operations": [_accepted_grant_observation(row) for row in blocking_lower]}
    conn.execute(
        "INSERT INTO project_fences (cluster_id,project_key,authority_epoch,max_fence) "
        "VALUES (?,?,?,?) ON CONFLICT(cluster_id,project_key) DO UPDATE SET "
        "authority_epoch=excluded.authority_epoch,max_fence=excluded.max_fence",
        (cluster, project_key, epoch, max(max_fence, fence)),
    )
    result = None
    state = "ACCEPTED"
    status = "ACCEPTED"
    if operation == "fence_barrier":
        result = {"lower_fence_operations": [
            _accepted_grant_observation(row) for row in lower
        ]}
        state = "TERMINAL"
        status = "BARRIER"
    conn.execute(
        "INSERT INTO accepted_grants (grant_id,cluster_id,project_key,authority_epoch,fence,"
        "operation,operation_id,request_sha256,state,accepted_at_ns,result_json) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (grant_id, cluster, project_key, epoch, fence, operation, operation_id,
         request_sha, state, time.time_ns(), canonical_json(result) if result else None),
    )
    return {"ok": True, "status": status, "idempotent": False, "result": result}


def _accepted_grant_observation(row) -> dict:
    return {
        "grant_id": str(row[0]), "operation": str(row[1]),
        "operation_id": str(row[2]), "fence": int(row[3]),
        "request_sha256": str(row[4]), "state": str(row[5]),
        "result": json.loads(bytes(row[6]).decode("utf-8")) if row[6] else None,
    }


def _grant_status_receipt(conn, runner_root: str, runner_meta: dict,
                          capability: dict) -> dict:
    body, key_id, secret = _verify_capability(conn, runner_root, runner_meta, capability)
    immutable = (
        str(body.get("cluster_id", "")), str(body.get("project_key", "")),
        int(body.get("authority_epoch", 0)), int(body.get("fence", -1)),
        str(body.get("operation", "")), str(body.get("operation_id", "")),
        str(body.get("request_sha256", "")),
    )
    row = conn.execute(
        "SELECT cluster_id,project_key,authority_epoch,fence,operation,operation_id,"
        "request_sha256,state,result_json FROM accepted_grants WHERE grant_id=?",
        (str(body.get("grant_id", "")),),
    ).fetchone()
    result = None
    status = "ABSENT"
    if row:
        if tuple(row[:7]) != immutable:
            raise RunnerError("grant_id reused with different capability fields")
        status = str(row[7])
        result = json.loads(bytes(row[8]).decode("utf-8")) if row[8] else None
    receipt_body = {
        "v": 1, "kind": "grant-status", "observed_at_ns": time.time_ns(),
        **body, "status": status, "result": result,
    }
    signature = _b64url(
        hmac.new(secret, canonical_json(receipt_body), hashlib.sha256).digest()
    )
    return {
        "ok": True, "status": status,
        "receipt": {"body": receipt_body, "sig_alg": "hmac-sha256",
                    "key_id": key_id, "sig": signature},
    }


RESOURCE_ALLOCATION_COLUMNS = (
    "allocation_id", "operation_id", "request_sha256", "allocation_spec_sha256",
    "resource_keys_json", "policy_generation", "policy_digest", "fence",
    "token_sha256", "state", "reservation_boot_id", "reserved_mono_ns",
    "expires_mono_ns", "claim_boot_id", "claimed_mono_ns", "owner_kind",
    "owner_key", "owner_pid", "owner_start_id", "root_pid", "root_start_id",
    "user_pid", "user_start_id",
    "command_start_state", "terminal_reason", "created_at_ns", "updated_at_ns",
)


def _strict_boot_id() -> str:
    """Return an OS-native boot-session identity or fail closed."""
    system = platform.system().lower()
    if system == "linux":
        try:
            with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as stream:
                value = stream.read().strip().lower()
        except OSError as exc:
            raise RunnerError("strict target boot identity is unavailable") from exc
        try:
            return "linux:" + str(uuid.UUID(value))
        except (ValueError, AttributeError) as exc:
            raise RunnerError("strict target boot identity is invalid") from exc
    if system == "darwin":
        try:
            value = subprocess.check_output(
                ["/usr/sbin/sysctl", "-n", "kern.bootsessionuuid"],
                text=True,
                timeout=10,
            ).strip().lower()
        except (OSError, subprocess.SubprocessError) as exc:
            raise RunnerError("strict target boot identity is unavailable") from exc
        try:
            return "darwin:" + str(uuid.UUID(value))
        except (ValueError, AttributeError) as exc:
            raise RunnerError("strict target boot identity is invalid") from exc
    if system == "windows":
        try:
            import ctypes

            class _Guid(ctypes.Structure):
                _fields_ = [
                    ("data1", ctypes.c_uint32),
                    ("data2", ctypes.c_uint16),
                    ("data3", ctypes.c_uint16),
                    ("data4", ctypes.c_ubyte * 8),
                ]

            class _BootEnvironment(ctypes.Structure):
                _fields_ = [
                    ("boot_identifier", _Guid),
                    ("firmware_type", ctypes.c_uint32),
                    ("boot_flags", ctypes.c_uint64),
                ]

            value = _BootEnvironment()
            returned = ctypes.c_ulong()
            status = ctypes.windll.ntdll.NtQuerySystemInformation(
                90, ctypes.byref(value), ctypes.sizeof(value), ctypes.byref(returned)
            )
            if status != 0:
                raise OSError(f"NtQuerySystemInformation returned 0x{status & 0xffffffff:08x}")
            guid = value.boot_identifier
            raw = (
                int(guid.data1).to_bytes(4, "little")
                + int(guid.data2).to_bytes(2, "little")
                + int(guid.data3).to_bytes(2, "little")
                + bytes(guid.data4)
            )
            return "windows:" + str(uuid.UUID(bytes_le=raw))
        except (AttributeError, OSError, ValueError) as exc:
            raise RunnerError("strict target boot identity is unavailable") from exc
    raise RunnerError(f"strict target boot identity is unsupported on {system or 'unknown'}")


def _strict_monotonic_ns() -> int:
    """Return a boot-relative clock whose epoch is stable across helper processes."""
    system = platform.system().lower()
    if system == "linux":
        clock = getattr(time, "CLOCK_BOOTTIME", None)
        if clock is None or not hasattr(time, "clock_gettime_ns"):
            raise RunnerError("strict target monotonic clock is unavailable")
        try:
            return int(time.clock_gettime_ns(clock))
        except OSError as exc:
            raise RunnerError("strict target monotonic clock is unavailable") from exc
    if system == "darwin":
        class _MachTimebaseInfo(ctypes.Structure):
            _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]

        try:
            system_lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
            system_lib.mach_continuous_time.argtypes = ()
            system_lib.mach_continuous_time.restype = ctypes.c_uint64
            system_lib.mach_timebase_info.argtypes = (
                ctypes.POINTER(_MachTimebaseInfo),
            )
            system_lib.mach_timebase_info.restype = ctypes.c_int
            scale = _MachTimebaseInfo()
            if system_lib.mach_timebase_info(ctypes.byref(scale)) != 0 or scale.denom == 0:
                raise OSError("mach_timebase_info failed")
            ticks = int(system_lib.mach_continuous_time())
            return ticks * int(scale.numer) // int(scale.denom)
        except (AttributeError, OSError) as exc:
            raise RunnerError("strict target monotonic clock is unavailable") from exc
    if system == "windows":
        try:
            get_ticks = ctypes.windll.kernel32.GetTickCount64
            get_ticks.argtypes = ()
            get_ticks.restype = ctypes.c_uint64
            return int(get_ticks()) * 1_000_000
        except (AttributeError, OSError) as exc:
            raise RunnerError("strict target monotonic clock is unavailable") from exc
    raise RunnerError(f"strict target monotonic clock is unsupported on {system or 'unknown'}")


def _bounded_resource_text(value, label: str, *, limit: int = 200) -> str:
    if not isinstance(value, str) or not value or len(value) > limit or "\x00" in value:
        raise RunnerError(f"{label} is invalid")
    return value


def _resource_sha256(value, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise RunnerError(f"{label} must be a lowercase SHA-256")
    return value


def _resource_keys(value) -> list[str]:
    if not isinstance(value, list) or not value or len(value) > 64:
        raise RunnerError("resource_keys must be a non-empty bounded array")
    keys = []
    for key in value:
        if not isinstance(key, str) or not RESOURCE_KEY_RE.fullmatch(key):
            raise RunnerError("resource key is invalid")
        keys.append(key)
    if len(set(keys)) != len(keys):
        raise RunnerError("duplicate resource key")
    return sorted(keys)


def _canonical_resource_policy(value) -> tuple[dict, bytes, str]:
    if not isinstance(value, dict) or set(value) != {
        "schema", "version", "generation", "resources"
    }:
        raise RunnerError("target resource policy has unknown or missing fields")
    if value.get("schema") != "remrun.target-resource-policy" or value.get("version") != 1:
        raise RunnerError("target resource policy schema/version is unsupported")
    generation = value.get("generation")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
        raise RunnerError("target resource policy generation is invalid")
    resources = value.get("resources")
    if not isinstance(resources, list) or not resources or len(resources) > 256:
        raise RunnerError("target resource policy resources are invalid")
    normalized = []
    seen = set()
    for resource in resources:
        if not isinstance(resource, dict) or set(resource) != {"key", "capacity"}:
            raise RunnerError("target resource policy resource is malformed")
        key = resource.get("key")
        if not isinstance(key, str) or not RESOURCE_KEY_RE.fullmatch(key):
            raise RunnerError("target resource policy key is invalid")
        if key in seen:
            raise RunnerError("duplicate target resource policy key")
        seen.add(key)
        capacity = resource.get("capacity")
        if isinstance(capacity, bool) or capacity != 1:
            raise RunnerError("target resource policy capacity must be exactly 1")
        normalized.append({"key": key, "capacity": 1})
    normalized.sort(key=lambda item: item["key"])
    document = {
        "schema": "remrun.target-resource-policy",
        "version": 1,
        "generation": generation,
        "resources": normalized,
    }
    packed = canonical_json(document)
    return document, packed, hashlib.sha256(packed).hexdigest()


def _resource_policy_row(conn):
    return conn.execute(
        "SELECT generation,digest,document_json,installed_at_ns "
        "FROM target_resource_policy WHERE singleton=1"
    ).fetchone()


def _resource_policy_get(conn) -> dict:
    row = _resource_policy_row(conn)
    if row is None:
        return {"ok": True, "status": "absent", "policy": None, "holds_active": False}
    return {
        "ok": True,
        "status": "installed",
        "policy": {
            "generation": int(row[0]),
            "digest": str(row[1]),
            "document": json.loads(bytes(row[2]).decode("utf-8")),
            "installed_at_ns": int(row[3]),
        },
        "holds_active": bool(
            conn.execute("SELECT 1 FROM target_resource_holds LIMIT 1").fetchone()
        ),
    }


def _resource_policy_install(conn, body: dict) -> dict:
    document, packed, digest = _canonical_resource_policy(body.get("policy_document"))
    supplied = _resource_sha256(body.get("supplied_digest"), "supplied policy digest")
    if supplied != digest:
        raise RunnerError("supplied target resource policy digest mismatch")
    generation = int(document["generation"])
    current = _resource_policy_row(conn)
    expected_generation = body.get("expected_generation")
    expected_digest = body.get("expected_digest")
    if current is None:
        if expected_generation is not None or expected_digest is not None:
            raise RunnerError("first target resource policy install must expect no policy")
        if generation != 1:
            raise RunnerError("first target resource policy generation must be generation 1")
        conn.execute(
            "INSERT INTO target_resource_policy "
            "(singleton,generation,digest,document_json,installed_at_ns) VALUES (1,?,?,?,?)",
            (generation, digest, packed, time.time_ns()),
        )
        return {"ok": True, "status": "installed", "generation": generation,
                "digest": digest, "idempotent": False}
    current_generation = int(current[0])
    current_digest = str(current[1])
    if generation == current_generation and digest == current_digest \
            and bytes(current[2]) == packed:
        return {"ok": True, "status": "installed", "generation": generation,
                "digest": digest, "idempotent": True}
    if generation == current_generation:
        raise RunnerError("target resource policy generation has conflicting content")
    if expected_generation != current_generation or expected_digest != current_digest:
        raise RunnerError("target resource policy expected head mismatch")
    if generation != current_generation + 1:
        raise RunnerError("target resource policy generation must advance by exactly one")
    if conn.execute("SELECT 1 FROM target_resource_holds LIMIT 1").fetchone():
        raise RunnerError("target resource policy cannot change while holds are active")
    conn.execute(
        "UPDATE target_resource_policy SET generation=?,digest=?,document_json=?,"
        "installed_at_ns=? WHERE singleton=1",
        (generation, digest, packed, time.time_ns()),
    )
    return {"ok": True, "status": "installed", "generation": generation,
            "digest": digest, "idempotent": False}


def _allocation(conn, allocation_id: str) -> dict | None:
    row = conn.execute(
        "SELECT " + ",".join(RESOURCE_ALLOCATION_COLUMNS)
        + " FROM target_resource_allocations WHERE allocation_id=?",
        (allocation_id,),
    ).fetchone()
    return dict(zip(RESOURCE_ALLOCATION_COLUMNS, row)) if row else None


def _resource_receipt(row: dict, boot_id: str) -> dict:
    owner = None
    if row.get("owner_kind"):
        owner = {
            "kind": str(row["owner_kind"]),
            "key": row.get("owner_key"),
            "pid": row.get("owner_pid"),
            "start_id": row.get("owner_start_id"),
            "root_pid": row.get("root_pid"),
            "root_start_id": row.get("root_start_id"),
            "user_pid": row.get("user_pid"),
            "user_start_id": row.get("user_start_id"),
        }
    return {
        "schema": "remrun.target-resource-receipt",
        "version": 1,
        "status": str(row["state"]).lower(),
        "allocation_id": str(row["allocation_id"]),
        "operation_id": str(row["operation_id"]),
        "request_sha256": str(row["request_sha256"]),
        "resource_keys": json.loads(bytes(row["resource_keys_json"]).decode("utf-8")),
        "policy_generation": int(row["policy_generation"]),
        "policy_digest": str(row["policy_digest"]),
        "fence": int(row["fence"]),
        "target_boot_id": boot_id,
        "reservation_expires_mono_ns": int(row["expires_mono_ns"]),
        "state": str(row["state"]),
        "command_start_state": str(row["command_start_state"]),
        "owner": owner,
        "terminal_reason": row.get("terminal_reason"),
        "updated_at_ns": int(row["updated_at_ns"]),
    }


def _terminalize_resource(conn, allocation_id: str, state: str, reason: str) -> None:
    conn.execute("DELETE FROM target_resource_holds WHERE allocation_id=?", (allocation_id,))
    conn.execute(
        "UPDATE target_resource_allocations SET state=?,terminal_reason=?,updated_at_ns=? "
        "WHERE allocation_id=?",
        (state, reason, time.time_ns(), allocation_id),
    )


def _reconcile_resources(conn, boot_id: str, mono_ns: int) -> None:
    rows = conn.execute(
        "SELECT allocation_id,state,reservation_boot_id,claim_boot_id,expires_mono_ns "
        "FROM target_resource_allocations WHERE state IN ('RESERVED','CLAIMED','QUARANTINED')"
    ).fetchall()
    for allocation_id, state, reservation_boot, claim_boot, expires in rows:
        active_boot = claim_boot if state in {"CLAIMED", "QUARANTINED"} else reservation_boot
        if str(active_boot) != boot_id:
            _terminalize_resource(conn, str(allocation_id), "REBOOTED", "target_rebooted")
        elif state == "RESERVED" and mono_ns >= int(expires):
            _terminalize_resource(conn, str(allocation_id), "EXPIRED", "reservation_expired")
        elif state in {"CLAIMED", "QUARANTINED"}:
            allocation = _allocation(conn, str(allocation_id))
            assert allocation is not None
            cleanup = _resource_owner_cleanup_state(allocation)
            if cleanup == "gone":
                _terminalize_resource(
                    conn, str(allocation_id), "RELEASED", "process_tree_exited"
                )
            elif cleanup == "unknown" and state == "CLAIMED":
                conn.execute(
                    "UPDATE target_resource_allocations SET state='QUARANTINED',"
                    "terminal_reason='process_cleanup_unverified',updated_at_ns=? "
                    "WHERE allocation_id=? AND state='CLAIMED'",
                    (time.time_ns(), str(allocation_id)),
                )


def _require_resource_policy(conn, generation, digest: str) -> tuple[dict, set[str]]:
    row = _resource_policy_row(conn)
    if row is None:
        raise RunnerError("target resource policy is not installed")
    if generation != int(row[0]) or digest != str(row[1]):
        raise RunnerError("target resource policy identity mismatch")
    document = json.loads(bytes(row[2]).decode("utf-8"))
    return document, {str(item["key"]) for item in document["resources"]}


def _resource_reserve(conn, body: dict, boot_id: str, mono_ns: int) -> dict:
    allocation_id = _bounded_resource_text(body.get("allocation_id"), "allocation_id")
    operation_id = _bounded_resource_text(body.get("operation_id"), "operation_id")
    request_sha = _resource_sha256(body.get("request_sha256"), "request_sha256")
    keys = _resource_keys(body.get("resource_keys"))
    generation = body.get("expected_policy_generation")
    if isinstance(generation, bool) or not isinstance(generation, int):
        raise RunnerError("expected policy generation is invalid")
    digest = _resource_sha256(body.get("expected_policy_digest"), "expected policy digest")
    _reconcile_resources(conn, boot_id, mono_ns)
    _document, policy_keys = _require_resource_policy(conn, generation, digest)
    unknown = [key for key in keys if key not in policy_keys]
    if unknown:
        raise RunnerError("resource key is absent from target policy: " + ", ".join(unknown))
    spec = {
        "allocation_id": allocation_id,
        "operation_id": operation_id,
        "request_sha256": request_sha,
        "resource_keys": keys,
        "policy_generation": generation,
        "policy_digest": digest,
    }
    spec_sha = hashlib.sha256(canonical_json(spec)).hexdigest()
    prior = _allocation(conn, allocation_id)
    if prior is not None:
        if prior["allocation_spec_sha256"] != spec_sha:
            raise RunnerError("allocation_id reused with different immutable fields")
        return {"ok": True, "status": "allocation_exists",
                "receipt": _resource_receipt(prior, boot_id)}
    placeholders = ",".join("?" for _ in keys)
    busy = [
        str(row[0]) for row in conn.execute(
            f"SELECT resource_key FROM target_resource_holds "
            f"WHERE resource_key IN ({placeholders}) ORDER BY resource_key", keys
        )
    ]
    if busy:
        return {"ok": True, "status": "resource_busy", "busy_keys": busy}
    conn.execute(
        "INSERT OR IGNORE INTO target_resource_fence(singleton,last_fence) VALUES(1,0)"
    )
    last_fence = int(conn.execute(
        "SELECT last_fence FROM target_resource_fence WHERE singleton=1"
    ).fetchone()[0])
    if last_fence >= MAX_RESOURCE_FENCE:
        raise RunnerError("target resource fence exhausted")
    fence = last_fence + 1
    conn.execute("UPDATE target_resource_fence SET last_fence=? WHERE singleton=1", (fence,))
    token = secrets.token_urlsafe(32)
    token_sha = hashlib.sha256(token.encode("ascii")).hexdigest()
    now = time.time_ns()
    expires = mono_ns + RESOURCE_RESERVATION_NS
    conn.execute(
        "INSERT INTO target_resource_allocations ("
        "allocation_id,operation_id,request_sha256,allocation_spec_sha256,resource_keys_json,"
        "policy_generation,policy_digest,fence,token_sha256,state,reservation_boot_id,"
        "reserved_mono_ns,expires_mono_ns,command_start_state,created_at_ns,updated_at_ns) "
        "VALUES (?,?,?,?,?,?,?,?,?,'RESERVED',?,?,?,'NO',?,?)",
        (allocation_id, operation_id, request_sha, spec_sha, canonical_json(keys), generation,
         digest, fence, token_sha, boot_id, mono_ns, expires, now, now),
    )
    _test_fault_point("after_resource_allocation_insert")
    for key in keys:
        conn.execute(
            "INSERT INTO target_resource_holds(resource_key,allocation_id,fence) VALUES(?,?,?)",
            (key, allocation_id, fence),
        )
    row = _allocation(conn, allocation_id)
    assert row is not None
    return {"ok": True, "status": "reserved", "token": token,
            "receipt": _resource_receipt(row, boot_id)}


def _resource_auth(conn, body: dict, *, require_fence: bool) -> dict:
    allocation_id = _bounded_resource_text(body.get("allocation_id"), "allocation_id")
    row = _allocation(conn, allocation_id)
    if row is None:
        raise RunnerError("target resource allocation is absent")
    token = body.get("token")
    if not isinstance(token, str) or not token or len(token) > 256:
        raise RunnerError("target resource token is invalid")
    if not hmac.compare_digest(
        hashlib.sha256(token.encode("utf-8")).hexdigest(), str(row["token_sha256"])
    ):
        raise RunnerError("target resource token mismatch")
    if require_fence:
        fence = body.get("fence")
        if isinstance(fence, bool) or not isinstance(fence, int) or fence != int(row["fence"]):
            raise RunnerError("target resource fence mismatch")
    return row


def _resource_renew(conn, body: dict, boot_id: str, mono_ns: int) -> dict:
    row = _resource_auth(conn, body, require_fence=True)
    _reconcile_resources(conn, boot_id, mono_ns)
    row = _allocation(conn, str(row["allocation_id"]))
    assert row is not None
    _require_resource_policy(
        conn, body.get("expected_policy_generation"),
        _resource_sha256(body.get("expected_policy_digest"), "expected policy digest"),
    )
    if row["state"] != "RESERVED":
        return {"ok": True, "status": str(row["state"]).lower(),
                "receipt": _resource_receipt(row, boot_id)}
    expires = mono_ns + RESOURCE_RESERVATION_NS
    conn.execute(
        "UPDATE target_resource_allocations SET expires_mono_ns=?,updated_at_ns=? "
        "WHERE allocation_id=? AND state='RESERVED'",
        (expires, time.time_ns(), row["allocation_id"]),
    )
    renewed = _allocation(conn, str(row["allocation_id"]))
    assert renewed is not None
    return {"ok": True, "status": "reserved", "receipt": _resource_receipt(renewed, boot_id)}


def _resource_cancel(conn, body: dict, boot_id: str, mono_ns: int) -> dict:
    row = _resource_auth(conn, body, require_fence=True)
    _reconcile_resources(conn, boot_id, mono_ns)
    row = _allocation(conn, str(row["allocation_id"]))
    assert row is not None
    if row["state"] == "CANCELLED":
        return {"ok": True, "status": "cancelled", "receipt": _resource_receipt(row, boot_id)}
    if row["state"] != "RESERVED":
        raise RunnerError("only a reserved target resource allocation may be cancelled")
    _terminalize_resource(conn, str(row["allocation_id"]), "CANCELLED", "controller_cancelled")
    cancelled = _allocation(conn, str(row["allocation_id"]))
    assert cancelled is not None
    return {"ok": True, "status": "cancelled",
            "receipt": _resource_receipt(cancelled, boot_id)}


def _resource_status(conn, body: dict, boot_id: str, mono_ns: int) -> dict:
    row = _resource_auth(conn, body, require_fence=False)
    _reconcile_resources(conn, boot_id, mono_ns)
    current = _allocation(conn, str(row["allocation_id"]))
    assert current is not None
    return {"ok": True, "status": "found", "receipt": _resource_receipt(current, boot_id)}


def _resource_owner_claim(conn, body: dict, owner: dict, boot_id: str, mono_ns: int) -> dict:
    row = _resource_auth(conn, body, require_fence=True)
    _reconcile_resources(conn, boot_id, mono_ns)
    row = _allocation(conn, str(row["allocation_id"]))
    assert row is not None
    if row["state"] != "RESERVED":
        raise RunnerError("target owner requires a live reserved allocation")
    generation = body.get("policy_generation")
    digest = _resource_sha256(body.get("policy_digest"), "owner policy digest")
    if generation != int(row["policy_generation"]) or digest != row["policy_digest"]:
        raise RunnerError("target owner policy identity mismatch")
    _require_resource_policy(conn, generation, digest)
    if mono_ns >= int(row["expires_mono_ns"]):
        _terminalize_resource(
            conn, str(row["allocation_id"]), "EXPIRED", "reservation_expired"
        )
        raise RunnerError("target resource reservation expired before owner claim")
    required = {
        "kind", "key", "pid", "start_id", "root_pid", "root_start_id"
    }
    if not isinstance(owner, dict) or set(owner) != required:
        raise RunnerError("target owner identity is malformed")
    kind = str(owner["kind"])
    if kind not in {"posix_pgid_v1", "windows_job_v1"}:
        raise RunnerError("target owner kind is unsupported")
    owner_pid = owner["pid"]
    root_pid = owner["root_pid"]
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 1
           for value in (owner_pid, root_pid)):
        raise RunnerError("target owner PID identity is invalid")
    for field in ("key", "start_id", "root_start_id"):
        _bounded_resource_text(owner[field], f"target owner {field}", limit=512)
    now = time.time_ns()
    conn.execute(
        "UPDATE target_resource_allocations SET state='CLAIMED',claim_boot_id=?,"
        "claimed_mono_ns=?,owner_kind=?,owner_key=?,owner_pid=?,owner_start_id=?,"
        "root_pid=?,root_start_id=?,command_start_state='NO',updated_at_ns=? "
        "WHERE allocation_id=? AND state='RESERVED'",
        (boot_id, mono_ns, kind, owner["key"], owner_pid, owner["start_id"],
         root_pid, owner["root_start_id"], now, row["allocation_id"]),
    )
    claimed = _allocation(conn, str(row["allocation_id"]))
    assert claimed is not None
    return _resource_receipt(claimed, boot_id)


def _resource_owner_start_state(
    conn, body: dict, boot_id: str, state: str, *, explicit_no_start: bool = False
) -> dict:
    row = _resource_auth(conn, body, require_fence=True)
    if row["state"] != "CLAIMED" or row["claim_boot_id"] != boot_id:
        raise RunnerError("target resource claim is not current")
    current = str(row["command_start_state"])
    allowed = (
        (current == "NO" and state == "MAYBE")
        or (current == "MAYBE" and state == "YES")
        or (current == "MAYBE" and state == "NO" and explicit_no_start)
        or current == state
    )
    if not allowed:
        raise RunnerError(f"invalid target owner start transition: {current} -> {state}")
    conn.execute(
        "UPDATE target_resource_allocations SET command_start_state=?,updated_at_ns=? "
        "WHERE allocation_id=? AND state='CLAIMED'",
        (state, time.time_ns(), row["allocation_id"]),
    )
    updated = _allocation(conn, str(row["allocation_id"]))
    assert updated is not None
    return _resource_receipt(updated, boot_id)


def _resource_owner_exec_confirm(
    conn, body: dict, boot_id: str, user_pid: int, user_start_id: str
) -> dict:
    row = _resource_auth(conn, body, require_fence=True)
    if row["state"] != "CLAIMED" or row["claim_boot_id"] != boot_id:
        raise RunnerError("target resource claim is not current")
    if row["command_start_state"] != "MAYBE":
        raise RunnerError("target resource exec confirmation requires MAYBE")
    if isinstance(user_pid, bool) or not isinstance(user_pid, int) or user_pid < 1:
        raise RunnerError("target resource user PID is invalid")
    _bounded_resource_text(user_start_id, "target resource user start identity", limit=512)
    conn.execute(
        "UPDATE target_resource_allocations SET command_start_state='YES',user_pid=?,"
        "user_start_id=?,updated_at_ns=? WHERE allocation_id=? AND state='CLAIMED' "
        "AND command_start_state='MAYBE'",
        (user_pid, user_start_id, time.time_ns(), row["allocation_id"]),
    )
    updated = _allocation(conn, str(row["allocation_id"]))
    assert updated is not None
    return _resource_receipt(updated, boot_id)


def _resource_owner_release(conn, body: dict, boot_id: str, reason: str) -> dict:
    row = _resource_auth(conn, body, require_fence=True)
    if row["state"] not in {"CLAIMED", "QUARANTINED"}:
        raise RunnerError("only a target-owned claim may be released")
    if row["claim_boot_id"] != boot_id:
        raise RunnerError("target resource claim boot identity mismatch")
    _terminalize_resource(conn, str(row["allocation_id"]), "RELEASED", reason)
    released = _allocation(conn, str(row["allocation_id"]))
    assert released is not None
    return _resource_receipt(released, boot_id)


def _resource_owner_quarantine(conn, body: dict, boot_id: str, reason: str) -> dict:
    row = _resource_auth(conn, body, require_fence=True)
    if row["state"] == "QUARANTINED":
        return _resource_receipt(row, boot_id)
    if row["state"] != "CLAIMED" or row["claim_boot_id"] != boot_id:
        raise RunnerError("only a current target-owned claim may be quarantined")
    conn.execute(
        "UPDATE target_resource_allocations SET state='QUARANTINED',terminal_reason=?,"
        "updated_at_ns=? WHERE allocation_id=? AND state='CLAIMED'",
        (reason, time.time_ns(), row["allocation_id"]),
    )
    quarantined = _allocation(conn, str(row["allocation_id"]))
    assert quarantined is not None
    return _resource_receipt(quarantined, boot_id)


def _resource_owner_mutation(state_root: str, operation: str, body: dict, **values) -> dict:
    conn, _runner_root, _meta = open_participant_store(state_root)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            boot_id = _strict_boot_id()
            mono_ns = _strict_monotonic_ns()
            if operation == "claim":
                receipt = _resource_owner_claim(
                    conn, body, values["owner"], boot_id, mono_ns
                )
            elif operation == "start":
                receipt = _resource_owner_start_state(
                    conn, body, boot_id, values["state"],
                    explicit_no_start=bool(values.get("explicit_no_start")),
                )
            elif operation == "exec_confirm":
                receipt = _resource_owner_exec_confirm(
                    conn, body, boot_id, values["user_pid"], values["user_start_id"]
                )
            elif operation == "release":
                receipt = _resource_owner_release(conn, body, boot_id, values["reason"])
            elif operation == "quarantine":
                receipt = _resource_owner_quarantine(conn, body, boot_id, values["reason"])
            else:
                raise RunnerError("unknown target owner mutation")
            conn.execute("COMMIT")
            return receipt
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def _darwin_start_id(pid: int) -> str:
    try:
        import ctypes

        class _ProcBSDInfo(ctypes.Structure):
            _fields_ = [
                ("pbi_flags", ctypes.c_uint32), ("pbi_status", ctypes.c_uint32),
                ("pbi_xstatus", ctypes.c_uint32), ("pbi_pid", ctypes.c_uint32),
                ("pbi_ppid", ctypes.c_uint32), ("pbi_uid", ctypes.c_uint32),
                ("pbi_gid", ctypes.c_uint32), ("pbi_ruid", ctypes.c_uint32),
                ("pbi_rgid", ctypes.c_uint32), ("pbi_svuid", ctypes.c_uint32),
                ("pbi_svgid", ctypes.c_uint32), ("rfu_1", ctypes.c_uint32),
                ("pbi_comm", ctypes.c_char * 16), ("pbi_name", ctypes.c_char * 32),
                ("pbi_nfiles", ctypes.c_uint32), ("pbi_pgid", ctypes.c_uint32),
                ("pbi_pjobc", ctypes.c_uint32), ("e_tdev", ctypes.c_uint32),
                ("e_tpgid", ctypes.c_uint32), ("pbi_nice", ctypes.c_int32),
                ("pbi_start_tvsec", ctypes.c_uint64),
                ("pbi_start_tvusec", ctypes.c_uint64),
            ]

        libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        info = _ProcBSDInfo()
        size = libproc.proc_pidinfo(
            int(pid), 3, 0, ctypes.byref(info), ctypes.sizeof(info)
        )
        if size != ctypes.sizeof(info) or int(info.pbi_pid) != int(pid):
            raise OSError("proc_pidinfo returned no exact identity")
        return f"darwin:{pid}:{int(info.pbi_start_tvsec)}:{int(info.pbi_start_tvusec)}"
    except (AttributeError, OSError) as exc:
        raise RunnerError("exact process start identity is unavailable") from exc


def _parse_linux_proc_stat(text: str, expected_pid: int) -> tuple[int, int]:
    """Return (process group, start ticks) from one exact /proc PID stat row."""
    start = text.find("(")
    end = text.rfind(")")
    if start <= 0 or end <= start or text[end + 1:end + 2] != " ":
        raise ValueError("malformed Linux process stat framing")
    if int(text[:start].strip()) != expected_pid:
        raise ValueError("Linux process stat PID mismatch")
    fields = text[end + 2:].split()
    if len(fields) < 20:
        raise ValueError("short Linux process stat row")
    return int(fields[2]), int(fields[19])


def _process_start_id(pid: int) -> str:
    system = platform.system().lower()
    if system == "linux":
        try:
            with open(f"/proc/{int(pid)}/stat", encoding="ascii") as stream:
                text = stream.read()
            _pgid, start_ticks = _parse_linux_proc_stat(text, int(pid))
            return f"linux:{pid}:{start_ticks}"
        except (OSError, ValueError, IndexError) as exc:
            raise RunnerError("exact process start identity is unavailable") from exc
    if system == "darwin":
        return _darwin_start_id(pid)
    if system == "windows":
        return _windows_process_start_id(pid)
    raise RunnerError("exact process start identity is unsupported")


def _posix_group_members(pgid: int) -> list[int] | None:
    system = platform.system().lower()
    if system == "linux":
        try:
            names = os.listdir("/proc")
        except OSError:
            return None
        members = []
        for name in names:
            if not name.isdigit():
                continue
            try:
                with open(f"/proc/{name}/stat", encoding="ascii") as stream:
                    text = stream.read()
                member_pgid, _start_ticks = _parse_linux_proc_stat(text, int(name))
                if member_pgid == pgid:
                    members.append(int(name))
            except OSError as exc:
                if exc.errno in {errno.ENOENT, errno.ESRCH}:
                    continue
                return None
            except (ValueError, IndexError):
                return None
        return members
    if system == "darwin":
        try:
            output = subprocess.check_output(
                ["/bin/ps", "-axo", "pid=,pgid="], text=True, timeout=10
            )
        except (OSError, subprocess.SubprocessError):
            return None
        members = []
        try:
            for line in output.splitlines():
                pid_text, group_text = line.split()
                if int(group_text) == pgid:
                    members.append(int(pid_text))
        except (ValueError, IndexError):
            return None
        return members
    return None


def _win_kernel32():
    global _WIN_K32
    if _WIN_K32 is None:
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.CreateJobObjectW.argtypes = (_WIN_LPVOID, ctypes.c_wchar_p)
        k32.CreateJobObjectW.restype = _WIN_HANDLE
        k32.OpenJobObjectW.argtypes = (_WIN_DWORD, _WIN_BOOL, ctypes.c_wchar_p)
        k32.OpenJobObjectW.restype = _WIN_HANDLE
        k32.AssignProcessToJobObject.argtypes = (_WIN_HANDLE, _WIN_HANDLE)
        k32.AssignProcessToJobObject.restype = _WIN_BOOL
        k32.SetInformationJobObject.argtypes = (
            _WIN_HANDLE, ctypes.c_int, _WIN_LPVOID, _WIN_DWORD
        )
        k32.SetInformationJobObject.restype = _WIN_BOOL
        k32.QueryInformationJobObject.argtypes = (
            _WIN_HANDLE, ctypes.c_int, _WIN_LPVOID, _WIN_DWORD,
            ctypes.POINTER(_WIN_DWORD),
        )
        k32.QueryInformationJobObject.restype = _WIN_BOOL
        k32.CreateProcessW.argtypes = (
            ctypes.c_wchar_p, ctypes.c_wchar_p, _WIN_LPVOID, _WIN_LPVOID, _WIN_BOOL,
            _WIN_DWORD, _WIN_LPVOID, ctypes.c_wchar_p,
            ctypes.POINTER(_WinStartupInfo), ctypes.POINTER(_WinProcessInformation),
        )
        k32.CreateProcessW.restype = _WIN_BOOL
        k32.ResumeThread.argtypes = (_WIN_HANDLE,)
        k32.ResumeThread.restype = _WIN_DWORD
        k32.WaitForSingleObject.argtypes = (_WIN_HANDLE, _WIN_DWORD)
        k32.WaitForSingleObject.restype = _WIN_DWORD
        k32.GetExitCodeProcess.argtypes = (_WIN_HANDLE, ctypes.POINTER(_WIN_DWORD))
        k32.GetExitCodeProcess.restype = _WIN_BOOL
        k32.GetProcessTimes.argtypes = (
            _WIN_HANDLE,
            ctypes.POINTER(_WinFileTime), ctypes.POINTER(_WinFileTime),
            ctypes.POINTER(_WinFileTime), ctypes.POINTER(_WinFileTime),
        )
        k32.GetProcessTimes.restype = _WIN_BOOL
        k32.OpenProcess.argtypes = (_WIN_DWORD, _WIN_BOOL, _WIN_DWORD)
        k32.OpenProcess.restype = _WIN_HANDLE
        k32.TerminateProcess.argtypes = (_WIN_HANDLE, _WIN_DWORD)
        k32.TerminateProcess.restype = _WIN_BOOL
        k32.CloseHandle.argtypes = (_WIN_HANDLE,)
        k32.CloseHandle.restype = _WIN_BOOL
        _WIN_K32 = k32
    return _WIN_K32


def _win_error(where: str) -> OSError:
    code = ctypes.get_last_error()
    try:
        detail = ctypes.FormatError(code).strip()
    except Exception:
        detail = f"Win32 error {code}"
    return OSError(code, f"{where} failed: {detail}")


def _win_valid_handle(handle) -> bool:
    value = handle.value if hasattr(handle, "value") else handle
    return bool(value) and value != _WIN_INVALID_HANDLE


def _win_close(handle) -> None:
    if _win_valid_handle(handle):
        try:
            _win_kernel32().CloseHandle(handle)
        except Exception:
            pass


def _win_create_resource_job(name: str):
    ctypes.set_last_error(0)
    handle = _win_kernel32().CreateJobObjectW(None, name)
    if not _win_valid_handle(handle):
        raise _win_error("CreateJobObjectW")
    if ctypes.get_last_error() == _WIN_ERROR_ALREADY_EXISTS:
        _win_close(handle)
        raise RunnerError("target resource Job Object already exists")
    limits = _WinJobExtendedLimitInformation()
    limits.BasicLimitInformation.LimitFlags = _WIN_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if not _win_kernel32().SetInformationJobObject(
        handle,
        _WIN_JOB_EXTENDED_LIMIT_INFORMATION,
        ctypes.byref(limits),
        ctypes.sizeof(limits),
    ):
        error = _win_error("SetInformationJobObject")
        _win_close(handle)
        raise error
    return handle


def _win_open_resource_job(name: str):
    ctypes.set_last_error(0)
    handle = _win_kernel32().OpenJobObjectW(_WIN_JOB_OBJECT_QUERY, False, name)
    if _win_valid_handle(handle):
        return handle
    if ctypes.get_last_error() == _WIN_ERROR_FILE_NOT_FOUND:
        return None
    raise _win_error("OpenJobObjectW")


def _win_resource_job_pids(handle) -> set[int]:
    capacity = 64
    while capacity <= 16_384:
        offset = ctypes.sizeof(_WIN_DWORD) * 2
        size = offset + ctypes.sizeof(_WIN_ULONG_PTR) * capacity
        buffer = ctypes.create_string_buffer(size)
        returned = _WIN_DWORD()
        ctypes.set_last_error(0)
        ok = _win_kernel32().QueryInformationJobObject(
            handle,
            _WIN_JOB_BASIC_PROCESS_ID_LIST,
            buffer,
            size,
            ctypes.byref(returned),
        )
        assigned = _WIN_DWORD.from_buffer(buffer, 0).value
        listed = _WIN_DWORD.from_buffer(buffer, ctypes.sizeof(_WIN_DWORD)).value
        if ok and assigned <= capacity and listed <= capacity:
            array_type = _WIN_ULONG_PTR * int(listed)
            values = array_type.from_buffer(buffer, offset)
            return {int(value) for value in values if int(value) > 0}
        if ctypes.get_last_error() != _WIN_ERROR_MORE_DATA and assigned <= capacity:
            raise _win_error("QueryInformationJobObject")
        capacity = max(capacity * 2, int(assigned) + 16)
    raise RunnerError("target resource Job process list exceeds bound")


def _win_process_start_from_handle(handle, pid: int) -> str:
    created, exited, kernel, user = (
        _WinFileTime(), _WinFileTime(), _WinFileTime(), _WinFileTime()
    )
    if not _win_kernel32().GetProcessTimes(
        handle,
        ctypes.byref(created), ctypes.byref(exited),
        ctypes.byref(kernel), ctypes.byref(user),
    ):
        raise _win_error("GetProcessTimes")
    start = (int(created.high) << 32) | int(created.low)
    return f"windows:{pid}:{start}"


def _windows_process_start_id(pid: int) -> str:
    handle = _win_kernel32().OpenProcess(
        _WIN_PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid)
    )
    if not _win_valid_handle(handle):
        raise RunnerError("exact process start identity is unavailable")
    try:
        return _win_process_start_from_handle(handle, int(pid))
    finally:
        _win_close(handle)


def _win_job_name(allocation_id: str, fence: int) -> str:
    digest = hashlib.sha256(f"{allocation_id}:{fence}".encode("utf-8")).hexdigest()[:32]
    return f"Global\\remrun-target-resource-v1-{digest}"


def _win_create_suspended_resource_process(
    argv: list[str], cwd: str | None, env: dict[str, str], stdout_file, stderr_file
) -> _WinProcessInformation:
    import msvcrt

    executable = shutil.which(argv[0], path=env.get("PATH"))
    application = executable or None
    values = [executable, *argv[1:]] if executable else argv
    command_line = ctypes.create_unicode_buffer(subprocess.list2cmdline(values))
    startup = _WinStartupInfo()
    startup.cb = ctypes.sizeof(startup)
    null_file = open(os.devnull, "rb")
    handles = [
        msvcrt.get_osfhandle(null_file.fileno()),
        msvcrt.get_osfhandle(stdout_file.fileno()),
        msvcrt.get_osfhandle(stderr_file.fileno()),
    ]
    for handle in handles:
        os.set_handle_inheritable(handle, True)
    startup.dwFlags = _WIN_STARTF_USESTDHANDLES
    startup.hStdInput, startup.hStdOutput, startup.hStdError = handles
    environment = ctypes.create_unicode_buffer(
        "\x00".join(f"{key}={value}" for key, value in sorted(env.items())) + "\x00\x00"
    )
    process = _WinProcessInformation()
    try:
        if not _win_kernel32().CreateProcessW(
            application,
            command_line,
            None,
            None,
            True,
            _WIN_CREATE_SUSPENDED
            | _WIN_CREATE_BREAKAWAY_FROM_JOB
            | _WIN_CREATE_UNICODE_ENVIRONMENT,
            ctypes.cast(environment, _WIN_LPVOID),
            cwd,
            ctypes.byref(startup),
            ctypes.byref(process),
        ):
            raise _win_error("CreateProcessW")
        return process
    finally:
        for handle in handles:
            try:
                os.set_handle_inheritable(handle, False)
            except OSError:
                pass
        null_file.close()


def _win_wait_process(process: _WinProcessInformation) -> int:
    wait = _win_kernel32().WaitForSingleObject(process.hProcess, _WIN_INFINITE)
    if wait == _WIN_WAIT_FAILED:
        raise _win_error("WaitForSingleObject")
    if wait != _WIN_WAIT_OBJECT_0:
        raise RunnerError(f"WaitForSingleObject returned {wait}")
    code = _WIN_DWORD()
    if not _win_kernel32().GetExitCodeProcess(process.hProcess, ctypes.byref(code)):
        raise _win_error("GetExitCodeProcess")
    return int(code.value)


def _win_terminate_process(process: _WinProcessInformation) -> None:
    try:
        _win_kernel32().TerminateProcess(process.hProcess, 125)
        _win_kernel32().WaitForSingleObject(process.hProcess, 5_000)
    finally:
        _win_close(process.hThread)
        process.hThread = None
        _win_close(process.hProcess)
        process.hProcess = None


def _run_windows_resource_owner(
    state_root: str, request: dict, claim_callback=None
) -> dict:
    reservation = dict(request["reservation"])
    argv = list(request["argv"])
    child_env = {**os.environ, **request.get("env", {})}
    stdout_file = tempfile.TemporaryFile()
    stderr_file = tempfile.TemporaryFile()
    process = None
    job = None
    claim_receipt = None
    try:
        job_name = _win_job_name(
            str(reservation["allocation_id"]), int(reservation["fence"])
        )
        job = _win_create_resource_job(job_name)
        process = _win_create_suspended_resource_process(
            argv, request.get("cwd"), child_env, stdout_file, stderr_file
        )
        if not _win_kernel32().AssignProcessToJobObject(job, process.hProcess):
            raise _win_error("AssignProcessToJobObject")
        root_pid = int(process.dwProcessId)
        root_start = _win_process_start_from_handle(process.hProcess, root_pid)
        owner_pid = os.getpid()
        owner_start = _windows_process_start_id(owner_pid)
        owner = {
            "kind": "windows_job_v1",
            "key": job_name,
            "pid": owner_pid,
            "start_id": owner_start,
            "root_pid": root_pid,
            "root_start_id": root_start,
        }
        _test_pause_point("before_resource_claim")
        _test_fault_point("before_resource_claim")
        claim_receipt = _resource_owner_mutation(
            state_root, "claim", reservation, owner=owner
        )
        if claim_callback is not None:
            claim_callback(claim_receipt)
        _test_fault_point("after_resource_claim")
        _resource_owner_mutation(state_root, "start", reservation, state="MAYBE")
        try:
            _test_fault_point("during_resource_gate_release")
        except RunnerError:
            _win_terminate_process(process)
            process = None
            receipt = _resource_owner_mutation(
                state_root, "quarantine", reservation, reason="gate_release_uncertain"
            )
            return {
                "ok": True, "exit_code": 125, "exec_confirmed": False,
                "claim_receipt": claim_receipt, "receipt": receipt,
                "stdout_b64": "", "stderr_b64": "",
                "stdout_truncated": False, "stderr_truncated": False,
            }
        previous = _win_kernel32().ResumeThread(process.hThread)
        if previous == _WIN_DWORD_MINUS_ONE:
            raise _win_error("ResumeThread")
        _win_close(process.hThread)
        process.hThread = None
        _resource_owner_mutation(
            state_root,
            "exec_confirm",
            reservation,
            user_pid=root_pid,
            user_start_id=root_start,
        )
        exit_code = _win_wait_process(process)
        while _win_resource_job_pids(job):
            time.sleep(0.1)
        if os.environ.get("REMRUN_TEST_ONLY_FAULT_POINT") == "resource_cleanup_unknown":
            receipt = _resource_owner_mutation(
                state_root, "quarantine", reservation, reason="process_cleanup_unverified"
            )
        else:
            receipt = _resource_owner_mutation(
                state_root, "release", reservation, reason="process_tree_exited"
            )
        stdout_b64, stdout_truncated = _bounded_owner_output(stdout_file)
        stderr_b64, stderr_truncated = _bounded_owner_output(stderr_file)
        return {
            "ok": True, "exit_code": exit_code, "exec_confirmed": True,
            "claim_receipt": claim_receipt, "receipt": receipt,
            "stdout_b64": stdout_b64, "stderr_b64": stderr_b64,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
        }
    except BaseException:
        if process is not None:
            try:
                _win_terminate_process(process)
            except Exception:
                pass
            process = None
        if claim_receipt is not None:
            try:
                _resource_owner_mutation(
                    state_root, "quarantine", reservation, reason="owner_failure_uncertain"
                )
            except Exception:
                pass
        raise
    finally:
        if process is not None:
            _win_close(process.hThread)
            _win_close(process.hProcess)
        _win_close(job)
        stdout_file.close()
        stderr_file.close()


def _resource_owner_cleanup_state(row: dict) -> str:
    """Return live, gone, or unknown without ever treating doubt as cleanup."""
    if os.environ.get("REMRUN_TEST_ONLY_FAULT_POINT") == "resource_cleanup_unknown":
        return "unknown"
    kind = str(row.get("owner_kind") or "")
    if kind == "posix_pgid_v1":
        try:
            pgid = int(row["owner_key"])
        except (TypeError, ValueError):
            return "unknown"
        members = _posix_group_members(pgid)
        if members is None:
            return "unknown"
        return "live" if members else "gone"
    if kind == "windows_job_v1":
        try:
            job = _win_open_resource_job(str(row["owner_key"]))
        except Exception:
            return "unknown"
        if job is not None:
            try:
                return "live" if _win_resource_job_pids(job) else "gone"
            except Exception:
                return "unknown"
            finally:
                _win_close(job)
        handle = _win_kernel32().OpenProcess(
            _WIN_PROCESS_QUERY_LIMITED_INFORMATION, False, int(row["root_pid"])
        )
        if not _win_valid_handle(handle):
            return (
                "gone"
                if ctypes.get_last_error() == _WIN_ERROR_INVALID_PARAMETER
                else "unknown"
            )
        try:
            current = _win_process_start_from_handle(handle, int(row["root_pid"]))
        except Exception:
            return "unknown"
        finally:
            _win_close(handle)
        return "unknown" if current == row.get("root_start_id") else "gone"
    return "unknown"


def _atomic_json(path: str, value: dict) -> None:
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=".runner-json-", suffix=".tmp", dir=parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(canonical_json(value) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
        _fsync_directory(parent)
    except BaseException:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise


def _fsync_directory(path: str) -> None:
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _source_sha256() -> str:
    return sha256_file(os.path.abspath(__file__))


def _runner_document(conn, runner_root: str, meta: dict) -> dict:
    authorities = [
        {"cluster_id": str(row[0]), "authority_epoch": int(row[1]),
         "key_id": str(row[2]), "key_sha256": str(row[3]), "state": str(row[4])}
        for row in conn.execute(
            "SELECT cluster_id,authority_epoch,key_id,key_sha256,state "
            "FROM enrolled_authorities ORDER BY cluster_id,authority_epoch,key_id"
        )
    ]
    return {
        "format": RUNNER_FORMAT,
        "schema_version": RUNNER_SCHEMA_VERSION,
        "device_id": meta["device_id"],
        "supported_protocols": RUNNER_PROTOCOLS,
        "runner_source_sha256": _source_sha256(),
        "authorities": authorities,
        "runner_root": runner_root,
    }


def _refresh_runner_document(conn, runner_root: str, meta: dict) -> dict:
    value = _runner_document(conn, runner_root, meta)
    _atomic_json(os.path.join(runner_root, "runner.json"), value)
    return value


def _record_enrollment(conn, runner_root: str, runner_meta: dict,
                       enrollment: dict, secret: bytes) -> bool:
    cluster = str(enrollment.get("cluster_id", ""))
    target = str(enrollment.get("target_device_id", ""))
    epoch = int(enrollment.get("authority_epoch", 0))
    key_id = str(enrollment.get("key_id", ""))
    digest = str(enrollment.get("key_sha256", ""))
    if target != runner_meta["device_id"] or not cluster or epoch < 1 or not key_id:
        raise RunnerError("key enrollment target or identity is invalid")
    if len(secret) != 32 or hashlib.sha256(secret).hexdigest() != digest:
        raise RunnerError("key enrollment secret fingerprint mismatch")
    prior_epoch = conn.execute(
        "SELECT max(authority_epoch) FROM enrolled_authorities WHERE cluster_id=? "
        "AND state='ENROLLED'", (cluster,),
    ).fetchone()[0]
    if prior_epoch is not None and epoch < int(prior_epoch):
        raise RunnerError("key enrollment authority epoch is stale")
    same_epoch = conn.execute(
        "SELECT key_id,key_sha256,state FROM enrolled_authorities WHERE cluster_id=? "
        "AND authority_epoch=?", (cluster, epoch),
    ).fetchone()
    if same_epoch:
        if same_epoch[0] != key_id or same_epoch[1] != digest:
            raise RunnerError("authority epoch already has a different enrolled key")
        if same_epoch[2] != "ENROLLED":
            conn.execute(
                "UPDATE enrolled_authorities SET state='ENROLLED',retired_at_ns=NULL "
                "WHERE cluster_id=? AND authority_epoch=? AND key_id=?",
                (cluster, epoch, key_id),
            )
        return True
    now = time.time_ns()
    conn.execute(
        "UPDATE enrolled_authorities SET state='RETIRED',retired_at_ns=? "
        "WHERE cluster_id=? AND authority_epoch<? AND state='ENROLLED'",
        (now, cluster, epoch),
    )
    conn.execute(
        "INSERT INTO enrolled_authorities (cluster_id,authority_epoch,key_id,key_sha256,"
        "state,enrolled_at_ns) VALUES (?,?,?,?,'ENROLLED',?)",
        (cluster, epoch, key_id, digest, now),
    )
    return False


def key_export_main(state_root: str, cluster: str, target: str,
                    epoch: int, key_id: str) -> int:
    participant, _runner_root, runner_meta = open_participant_store(state_root)
    participant.close()
    authority, coord_root, meta = open_authority_store(
        state_root, cluster, runner_meta["device_id"]
    )
    try:
        row = authority.execute(
            "SELECT key_sha256,state FROM authority_targets WHERE target_device_id=? "
            "AND authority_epoch=? AND key_id=?", (target, epoch, key_id),
        ).fetchone()
        if row is None or row[1] not in {"PENDING", "ENROLLED"} \
                or epoch != int(meta["authority_epoch"]):
            raise RunnerError("key export is not a current prepared enrollment")
        key_path = _secret_path(coord_root, cluster, target, epoch, key_id)
        with open(key_path, "rb") as stream:
            secret = stream.read()
        if hashlib.sha256(secret).hexdigest() != row[0]:
            raise RunnerError("prepared enrollment key fingerprint mismatch")
        payload = canonical_json({
            "v": 1, "kind": "key-enrollment", "cluster_id": cluster,
            "target_device_id": target, "authority_epoch": epoch,
            "key_id": key_id, "key_sha256": row[0], "secret": _b64url(secret),
        })
        sys.stdout.buffer.write(encode_frame({
            "v": 2, "kind": "key-enrollment", "encoding": "base64",
            "cluster_id": cluster, "target_device_id": target,
            "authority_epoch": epoch, "key_id": key_id,
        }, payload))
        return 0
    finally:
        authority.close()


def key_import_main(state_root: str) -> int:
    header, payload = decode_frame(sys.stdin.buffer.read())
    if header.get("v") != 2 or header.get("kind") != "key-enrollment":
        raise RunnerError("not a key enrollment frame")
    enrollment = json.loads(payload.decode("utf-8"))
    if not isinstance(enrollment, dict) or enrollment.get("kind") != "key-enrollment" \
            or int(enrollment.get("v", 0)) != 1:
        raise RunnerError("invalid key enrollment payload")
    for field in ("cluster_id", "target_device_id", "authority_epoch", "key_id"):
        if header.get(field) != enrollment.get(field):
            raise RunnerError("key enrollment frame identity mismatch")
    secret = _decode_b64url(str(enrollment.get("secret", "")))
    conn, runner_root, meta = open_participant_store(state_root)
    try:
        key_path = _secret_path(
            runner_root, str(enrollment["cluster_id"]), meta["device_id"],
            int(enrollment["authority_epoch"]), str(enrollment["key_id"]),
        )
        conn.execute("BEGIN IMMEDIATE")
        try:
            committed = conn.execute(
                "SELECT state FROM enrolled_authorities WHERE cluster_id=? "
                "AND authority_epoch=? AND key_id=?",
                (str(enrollment["cluster_id"]), int(enrollment["authority_epoch"]),
                 str(enrollment["key_id"])),
            ).fetchone()
            try:
                _write_secret(key_path, secret)
            except FileExistsError:
                with open(key_path, "rb") as stream:
                    existing_secret = stream.read()
                if not hmac.compare_digest(existing_secret, secret):
                    if committed:
                        raise RunnerError(
                            "committed enrollment key has a different fingerprint"
                        )
                    _replace_unenrolled_secret(key_path, secret)
            _test_fault_point("after_enrollment_key_create")
            idempotent = _record_enrollment(conn, runner_root, meta, enrollment, secret)
            _refresh_runner_document(conn, runner_root, meta)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        receipt_body = {
            "v": 1, "kind": "key-enrollment",
            "cluster_id": str(enrollment["cluster_id"]),
            "target_device_id": meta["device_id"],
            "authority_epoch": int(enrollment["authority_epoch"]),
            "key_id": str(enrollment["key_id"]),
            "key_sha256": str(enrollment["key_sha256"]),
        }
        receipt = {
            "body": receipt_body, "sig_alg": "hmac-sha256",
            "sig": _b64url(hmac.new(
                secret, canonical_json(receipt_body), hashlib.sha256
            ).digest()),
        }
        sys.stdout.buffer.write(canonical_json({
            "ok": True, "status": "ENROLLED", "idempotent": idempotent,
            "receipt": receipt,
        }))
        return 0
    finally:
        conn.close()


def _owner_request(raw: bytes) -> tuple[dict, str]:
    header, payload = decode_frame(raw)
    request_sha = hashlib.sha256(payload).hexdigest()
    if header.get("v") != 2 or header.get("kind") != "target-resource-owner-request" \
            or header.get("request_sha256") != request_sha:
        raise RunnerError("target owner request frame identity mismatch")
    try:
        request = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RunnerError("target owner request is not valid JSON") from exc
    if not isinstance(request, dict) or set(request) != {
        "schema", "version", "reservation", "argv", "cwd", "env"
    }:
        raise RunnerError("target owner request has unknown or missing fields")
    if request.get("schema") != "remrun.target-resource-owner-request" \
            or request.get("version") != 1:
        raise RunnerError("target owner request schema/version is unsupported")
    reservation = request.get("reservation")
    if not isinstance(reservation, dict) or set(reservation) != {
        "allocation_id", "fence", "token", "policy_generation", "policy_digest"
    }:
        raise RunnerError("target owner reservation envelope is malformed")
    argv = request.get("argv")
    if not isinstance(argv, list) or not argv or len(argv) > 4096:
        raise RunnerError("target owner argv is invalid")
    if any(not isinstance(token, str) or not token or "\x00" in token or len(token) > 65536
           for token in argv):
        raise RunnerError("target owner argv token is invalid")
    cwd = request.get("cwd")
    if cwd is not None and (
        not isinstance(cwd, str) or not cwd or "\x00" in cwd or len(cwd) > 4096
    ):
        raise RunnerError("target owner cwd is invalid")
    env = request.get("env")
    if not isinstance(env, dict) or len(env) > 128:
        raise RunnerError("target owner environment is invalid")
    for key, value in env.items():
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) \
                or not isinstance(value, str) or "\x00" in value or len(value) > 65536:
            raise RunnerError("target owner environment entry is invalid")
    return request, request_sha


def _kill_posix_group(pgid: int) -> None:
    try:
        os.killpg(pgid, 9)
    except (ProcessLookupError, PermissionError):
        pass


def _wait_posix(pid: int) -> int:
    while True:
        try:
            _waited, status = os.waitpid(pid, 0)
            code = os.waitstatus_to_exitcode(status)
            return 128 + (-code) if code < 0 else code
        except InterruptedError:
            continue


def _bounded_owner_output(stream) -> tuple[str, bool]:
    stream.seek(0)
    data = stream.read((1 << 20) + 1)
    truncated = len(data) > (1 << 20)
    if truncated:
        data = data[: 1 << 20]
    return _b64url(data), truncated


def _write_posix_exec_record(fd: int, record: dict) -> None:
    payload = canonical_json(record)
    if len(payload) > 4096:
        raise RunnerError("POSIX exec record exceeds bound")
    os.write(fd, struct.pack("!I", len(payload)) + payload)


def _read_fd_exact(fd: int, length: int) -> bytes | None:
    chunks = []
    remaining = length
    while remaining:
        chunk = os.read(fd, remaining)
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_posix_exec_record(fd: int, timeout: float = 15.0) -> dict | None:
    readable, _writable, _errors = select.select([fd], [], [], timeout)
    if not readable:
        return None
    header = _read_fd_exact(fd, 4)
    if header is None:
        return None
    length = struct.unpack("!I", header)[0]
    if length < 2 or length > 4096:
        return None
    payload = _read_fd_exact(fd, length)
    if payload is None:
        return None
    try:
        record = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return record if isinstance(record, dict) else None


def _run_posix_resource_owner(state_root: str, request: dict, claim_callback=None) -> dict:
    reservation = dict(request["reservation"])
    argv = list(request["argv"])
    cwd = request.get("cwd")
    child_env = {**os.environ, **request.get("env", {})}
    gate_r, gate_w = os.pipe()
    status_r, status_w = os.pipe()
    ready_r, ready_w = os.pipe()
    os.set_inheritable(status_w, False)
    stdout_file = tempfile.TemporaryFile()
    stderr_file = tempfile.TemporaryFile()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(gate_w)
            os.close(status_r)
            os.close(ready_r)
            os.setsid()
            os.dup2(stdout_file.fileno(), 1)
            os.dup2(stderr_file.fileno(), 2)
            os.write(ready_w, b"R")
            os.close(ready_w)
            allowed = os.read(gate_r, 1)
            os.close(gate_r)
            if allowed != b"G":
                os._exit(125)
            try:
                _test_fault_point("after_posix_gate_before_exec")
                process = subprocess.Popen(argv, cwd=cwd, env=child_env)
                user_start_id = _process_start_id(int(process.pid))
                _write_posix_exec_record(
                    status_w,
                    {
                        "kind": "EXEC_CONFIRMED",
                        "user_pid": int(process.pid),
                        "user_start_id": user_start_id,
                    },
                )
                os.close(status_w)
                return_code = int(process.wait())
                if return_code < 0:
                    return_code = 128 + (-return_code)
                os._exit(min(return_code, 255))
            except OSError as exc:
                _write_posix_exec_record(
                    status_w,
                    {
                        "kind": "EXEC_FAILED",
                        "errno": exc.errno,
                        "type": type(exc).__name__,
                        "detail": str(exc)[:1000],
                    },
                )
                os.close(status_w)
                os._exit(127)
        except BaseException:
            os._exit(126)
    os.close(gate_r)
    os.close(status_w)
    os.close(ready_w)
    claim_receipt = None
    child_reaped = False
    try:
        if os.read(ready_r, 1) != b"R":
            raise RunnerError("POSIX target owner failed before closing the launch gate")
        os.close(ready_r)
        root_start = _process_start_id(pid)
        owner = {
            "kind": "posix_pgid_v1",
            "key": str(pid),
            "pid": pid,
            "start_id": root_start,
            "root_pid": pid,
            "root_start_id": root_start,
        }
        _test_pause_point("before_resource_claim")
        _test_fault_point("before_resource_claim")
        claim_receipt = _resource_owner_mutation(
            state_root, "claim", reservation, owner=owner
        )
        if claim_callback is not None:
            claim_callback(claim_receipt)
        _test_fault_point("after_resource_claim")
        _resource_owner_mutation(
            state_root, "start", reservation, state="MAYBE"
        )
        try:
            _test_fault_point("during_resource_gate_release")
        except RunnerError:
            _kill_posix_group(pid)
            exit_code = _wait_posix(pid)
            child_reaped = True
            receipt = _resource_owner_mutation(
                state_root,
                "quarantine",
                reservation,
                reason="gate_release_uncertain",
            )
            stdout_b64, stdout_truncated = _bounded_owner_output(stdout_file)
            stderr_b64, stderr_truncated = _bounded_owner_output(stderr_file)
            return {
                "ok": True,
                "exit_code": exit_code,
                "exec_confirmed": False,
                "claim_receipt": claim_receipt,
                "receipt": receipt,
                "stdout_b64": stdout_b64,
                "stderr_b64": stderr_b64,
                "stdout_truncated": stdout_truncated,
                "stderr_truncated": stderr_truncated,
            }
        os.write(gate_w, b"G")
        os.close(gate_w)
        gate_w = -1
        exec_record = _read_posix_exec_record(status_r)
        os.close(status_r)
        status_r = -1
        exec_confirmed = False
        exec_error = "affirmative exec record absent or invalid"
        if exec_record and exec_record.get("kind") == "EXEC_CONFIRMED":
            user_pid = exec_record.get("user_pid")
            user_start_id = exec_record.get("user_start_id")
            try:
                if _process_start_id(int(user_pid)) != user_start_id:
                    raise RunnerError("POSIX exec identity mismatch")
                _resource_owner_mutation(
                    state_root,
                    "exec_confirm",
                    reservation,
                    user_pid=int(user_pid),
                    user_start_id=str(user_start_id),
                )
                exec_confirmed = True
                exec_error = None
            except (RunnerError, TypeError, ValueError):
                pass
        elif exec_record and exec_record.get("kind") == "EXEC_FAILED":
            exec_error = str(exec_record.get("detail") or "exec failed")
            _resource_owner_mutation(
                state_root, "start", reservation, state="NO", explicit_no_start=True
            )
        exit_code = _wait_posix(pid)
        child_reaped = True
        if os.environ.get("REMRUN_TEST_ONLY_FAULT_POINT") == "resource_cleanup_unknown":
            members = None
        else:
            members = _posix_group_members(pid)
        while members:
            time.sleep(0.1)
            members = _posix_group_members(pid)
        if members is None:
            receipt = _resource_owner_mutation(
                state_root, "quarantine", reservation, reason="process_cleanup_unverified"
            )
        else:
            receipt = _resource_owner_mutation(
                state_root, "release", reservation, reason="process_tree_exited"
            )
        stdout_b64, stdout_truncated = _bounded_owner_output(stdout_file)
        stderr_b64, stderr_truncated = _bounded_owner_output(stderr_file)
        return {
            "ok": True,
            "exit_code": exit_code,
            "exec_confirmed": exec_confirmed,
            "exec_error": exec_error,
            "claim_receipt": claim_receipt,
            "receipt": receipt,
            "stdout_b64": stdout_b64,
            "stderr_b64": stderr_b64,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
        }
    except BaseException:
        _kill_posix_group(pid)
        if not child_reaped:
            try:
                _wait_posix(pid)
            except ChildProcessError:
                pass
        if claim_receipt is not None:
            try:
                _resource_owner_mutation(
                    state_root, "quarantine", reservation, reason="owner_failure_uncertain"
                )
            except Exception:
                pass
        raise
    finally:
        for fd in (gate_w, status_r, ready_r):
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
        stdout_file.close()
        stderr_file.close()


def _owner_response_frame(kind: str, request_sha: str, response: dict) -> bytes:
    return encode_frame(
        {
            "v": 2,
            "kind": kind,
            "request_sha256": request_sha,
            "ok": bool(response.get("ok")),
        },
        canonical_json(response),
    )


def resource_owner_detached_main(state_root: str) -> int:
    request_sha = ""
    try:
        request, request_sha = _owner_request(sys.stdin.buffer.read())

        def emit_claim(receipt: dict) -> None:
            try:
                sys.stdout.buffer.write(
                    _owner_response_frame(
                        "target-resource-claim-receipt",
                        request_sha,
                        {"ok": True, "claim_receipt": receipt},
                    )
                )
                sys.stdout.buffer.flush()
            except OSError:
                pass

        if os.name == "posix":
            response = _run_posix_resource_owner(
                state_root, request, claim_callback=emit_claim
            )
        elif os.name == "nt":
            response = _run_windows_resource_owner(
                state_root, request, claim_callback=emit_claim
            )
        else:
            raise RunnerError("target resource owner is unsupported on this platform")
    except Exception as exc:
        response = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    try:
        sys.stdout.buffer.write(
            _owner_response_frame(
                "target-resource-owner-response", request_sha, response
            )
        )
        sys.stdout.buffer.flush()
    except OSError:
        pass
    return 0


def resource_owner_main(state_root: str) -> int:
    request_frame = sys.stdin.buffer.read()
    command = [
        sys.executable,
        os.path.abspath(__file__),
        "resource-owner-detached",
        state_root,
    ]
    kwargs = {
        "stdin": subprocess.PIPE,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.DEVNULL,
        "close_fds": True,
    }
    if os.name == "nt":
        kwargs["creationflags"] = _WIN_DETACHED_PROCESS | _WIN_CREATE_BREAKAWAY_FROM_JOB
    else:
        kwargs["start_new_session"] = True
    owner = subprocess.Popen(command, **kwargs)
    assert owner.stdin is not None and owner.stdout is not None
    owner.stdin.write(request_frame)
    owner.stdin.close()
    try:
        first_header, _first_body, first_raw = _read_frame_from_stream(owner.stdout)
        sys.stdout.buffer.write(first_raw)
        sys.stdout.buffer.flush()
        if first_header.get("kind") == "target-resource-claim-receipt":
            _terminal_header, _terminal_body, terminal_raw = _read_frame_from_stream(
                owner.stdout
            )
            sys.stdout.buffer.write(terminal_raw)
            sys.stdout.buffer.flush()
    except (BrokenPipeError, OSError):
        return 0
    finally:
        owner.stdout.close()
    owner.wait(timeout=10)
    return 0


def participant_rpc(state_root: str, header: dict, request: dict) -> dict:
    if header.get("v") != 2 or header.get("kind") != "rpc-request":
        raise RunnerError("not a versioned RPC request")
    if int(header.get("protocol", 0)) not in RUNNER_PROTOCOLS:
        raise RunnerError(f"unsupported runner protocol: {header.get('protocol')!r}")
    rpc_id = str(header.get("rpc_id", ""))
    operation = str(header.get("operation", ""))
    request_sha = hashlib.sha256(canonical_json(request)).hexdigest()
    if not rpc_id or request_sha != header.get("request_sha256"):
        raise RunnerError("RPC identity/digest mismatch")
    if request.get("operation") != operation:
        raise RunnerError("RPC operation mismatch")

    conn, runner_root, meta = open_participant_store(state_root)
    try:
        # Authority mutations have their own exact replay records in the same
        # database transaction. Do not serialize them behind the participant
        # writer or create a second crash gap in participant rpc_requests.
        if operation.startswith("authority_"):
            return authority_rpc(
                state_root, meta, operation, request.get("body", {}),
                rpc_id, request_sha,
            )
        # Serialize the durable response record AND runner.json refresh. Without the
        # same DB mutex, concurrent Windows RPCs can race os.replace on runner.json.
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT operation,request_sha256,response_json FROM rpc_requests WHERE rpc_id=?",
                (rpc_id,),
            ).fetchone()
            if existing:
                if existing[0] != operation or existing[1] != request_sha:
                    raise RunnerError("rpc_id reused with a different request")
                response = json.loads(bytes(existing[2]).decode("utf-8"))
                conn.execute("COMMIT")
                return response

            source_sha = _source_sha256()
            _refresh_runner_document(conn, runner_root, meta)
            for relative in ("keys", "runs", "txns", "bin"):
                os.makedirs(os.path.join(runner_root, relative), exist_ok=True)

            if operation == "participant_probe":
                response = {
                    "ok": True,
                    "format": RUNNER_FORMAT,
                    "protocols": RUNNER_PROTOCOLS,
                    "runner_source_sha256": source_sha,
                    "runner_root": runner_root,
                    **meta,
                }
            elif operation == "participant_touch":
                response = {"ok": True, "rpc_id": rpc_id, "echo": request.get("body", {})}
            elif operation == "participant_grant_accept":
                response = _accept_capability(
                    conn, runner_root, meta, request.get("body", {}).get("capability")
                )
            elif operation == "participant_grant_status":
                response = _grant_status_receipt(
                    conn, runner_root, meta, request.get("body", {}).get("capability")
                )
            elif operation == "target_resource_policy_get":
                response = _resource_policy_get(conn)
            elif operation == "target_resource_policy_install":
                response = _resource_policy_install(conn, request.get("body", {}))
            elif operation in {
                "target_resource_reserve", "target_resource_renew",
                "target_resource_cancel", "target_resource_status",
            }:
                boot_id = _strict_boot_id()
                mono_ns = _strict_monotonic_ns()
                resource_body = request.get("body", {})
                if operation == "target_resource_reserve":
                    response = _resource_reserve(conn, resource_body, boot_id, mono_ns)
                elif operation == "target_resource_renew":
                    response = _resource_renew(conn, resource_body, boot_id, mono_ns)
                elif operation == "target_resource_cancel":
                    response = _resource_cancel(conn, resource_body, boot_id, mono_ns)
                else:
                    response = _resource_status(conn, resource_body, boot_id, mono_ns)
            else:
                raise RunnerError(f"unknown participant RPC operation: {operation!r}")

            packed = canonical_json(response)
            conn.execute(
                "INSERT INTO rpc_requests "
                "(rpc_id,operation,request_sha256,response_json,created_at_ns) VALUES (?,?,?,?,?)",
                (rpc_id, operation, request_sha, packed, time.time_ns()),
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        return response
    finally:
        conn.close()


def rpc_main(state_root: str) -> int:
    rpc_id = ""
    request_sha = ""
    try:
        header, body = decode_frame(sys.stdin.buffer.read())
        rpc_id = str(header.get("rpc_id", ""))
        request_sha = str(header.get("request_sha256", ""))
        request = json.loads(body.decode("utf-8"))
        if not isinstance(request, dict):
            raise RunnerError("RPC body is not an object")
        response = participant_rpc(state_root, header, request)
        ok = True
    except BaseException as exc:
        response = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        ok = False
    payload = canonical_json(response)
    sys.stdout.buffer.write(encode_frame({
        "v": 2,
        "kind": "rpc-response",
        "rpc_id": rpc_id,
        "request_sha256": request_sha,
        "ok": ok,
    }, payload))
    return 0


def legacy_main(argv) -> int:
    if len(argv) < 2:
        sys.stderr.write("remrun-runner: missing request\n")
        return 2
    request = json.loads(base64.b64decode(argv[1]).decode("utf-8"))
    operation = request.get("op")
    if operation == "manifest":
        files = build_manifest(
            request["root"], request.get("exclude", []),
            int(request.get("hash_below_bytes", 0)), bool(request.get("always_hash", False)),
        )
        sys.stdout.write(json.dumps({"version": 1, "files": files}))
        return 0
    if operation == "hash_file":
        sys.stdout.write(json.dumps({"sha256": sha256_file(request["path"])}))
        return 0
    if operation == "probe":
        sqlite_ok = True
        try:
            _load_sqlite()
        except RunnerError:
            sqlite_ok = False
        sys.stdout.write(json.dumps({
            "os": platform.system().lower(),
            "python": platform.python_version(),
            "machine": platform.machine(),
            "sqlite3": sqlite_ok,
        }))
        return 0
    sys.stderr.write(f"remrun-runner: unknown op {operation!r}\n")
    return 2


def main(argv) -> int:
    if len(argv) >= 2 and argv[1] == "rpc":
        if len(argv) != 3:
            sys.stderr.write("remrun-runner: rpc requires state root\n")
            return 2
        return rpc_main(argv[2])
    if len(argv) == 7 and argv[1] == "key-export":
        return key_export_main(argv[2], argv[3], argv[4], int(argv[5]), argv[6])
    if len(argv) == 3 and argv[1] == "key-import":
        return key_import_main(argv[2])
    if len(argv) == 3 and argv[1] == "resource-owner-run":
        return resource_owner_main(argv[2])
    if len(argv) == 3 and argv[1] == "resource-owner-detached":
        return resource_owner_detached_main(argv[2])
    return legacy_main(argv)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
