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
import fnmatch
import hashlib
import hmac
import json
import os
import platform
import stat
import subprocess
import sys
import tempfile
import time
import uuid

RUNNER_FORMAT = "remrun-runner-v1"
RUNNER_SCHEMA_VERSION = 2
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
        dirnames[:] = [
            name for name in dirnames
            if not should_exclude(f"{rel_dir}/{name}" if rel_dir else name, excludes)
        ]
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
        conn.execute("PRAGMA journal_mode = DELETE")
        conn.execute("PRAGMA synchronous = EXTRA")
        conn.execute("PRAGMA busy_timeout = 10000")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("BEGIN IMMEDIATE")
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version not in (0, 1, RUNNER_SCHEMA_VERSION):
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
        elif version == 1 and RUNNER_SCHEMA_VERSION == 2:
            _assert_exact_schema(conn, sqlite3, SCHEMA[:-1], "runner v1")
            conn.execute(SCHEMA[-1])
            conn.execute(
                "UPDATE runner_meta SET schema_version=? WHERE singleton=1",
                (RUNNER_SCHEMA_VERSION,),
            )
            conn.execute(f"PRAGMA user_version = {RUNNER_SCHEMA_VERSION}")
        _assert_exact_schema(conn, sqlite3, SCHEMA, "runner v2")
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
    return legacy_main(argv)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
