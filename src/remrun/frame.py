"""RRFRAME2 — length + digest framed transfer payloads (design sections 11.2 / 11.3).

Groundwork for the crash-safe transaction engine. This module is NOT wired into the
live transport yet — a future protocol flag will select it — so importing/using it does
not change any current transfer behavior. Stdlib only, so it runs identically behind the
ssh-posix and ssh-powershell backends.

The point: a truncated or corrupted SSH stream must never be mistaken for a valid
(empty/short) file. Every frame carries an exact header length, an exact base64 body
length, the decoded length, and a SHA-256 — all verified before a payload may enter
transaction staging. For tar bundles, every member is verified and staged before any
destination is touched, so any failure commits nothing (folds in audit finding B5).

Wire format::

    RRFRAME2 <header_len_decimal> <base64_body_len_decimal>\\n
    <header_len exact UTF-8 bytes of canonical JSON>
    <base64_body_len exact ASCII bytes; no trailing newline>
    <EOF>
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import io
import json
import shutil
import tarfile
from pathlib import Path

MAGIC = b"RRFRAME2"
_MAX_HEADER_BYTES = 1 << 20  # 1 MiB hard cap on the JSON header line


class FrameError(Exception):
    """A frame was malformed, truncated, or failed integrity verification."""


def encode_frame(header: dict, body: bytes) -> bytes:
    """Frame ``body`` with ``header`` (decoded_length + sha256 are set from the body)."""
    h = dict(header)
    h["decoded_length"] = len(body)
    h["sha256"] = hashlib.sha256(body).hexdigest()
    header_bytes = json.dumps(h, sort_keys=True, separators=(",", ":")).encode("utf-8")
    b64 = base64.b64encode(body)
    first = b"%s %d %d\n" % (MAGIC, len(header_bytes), len(b64))
    return first + header_bytes + b64


def decode_frame(data: bytes) -> tuple[dict, bytes]:
    """Parse and fully verify a frame. Raises FrameError on any truncation/corruption."""
    nl = data.find(b"\n")
    if nl == -1:
        raise FrameError("no frame header line")
    parts = data[:nl].split(b" ")
    if len(parts) != 3 or parts[0] != MAGIC:
        raise FrameError("bad frame magic or header line")
    try:
        header_len = int(parts[1])
        body_len = int(parts[2])
    except ValueError as exc:
        raise FrameError(f"bad frame lengths: {exc}") from exc
    if header_len < 0 or body_len < 0:
        raise FrameError("negative frame length")
    if header_len > _MAX_HEADER_BYTES:
        raise FrameError("frame header too large")
    rest = data[nl + 1:]
    if len(rest) != header_len + body_len:
        raise FrameError(
            f"frame size mismatch: declared {header_len + body_len} bytes after the header "
            f"line, got {len(rest)} (truncated stream or extra bytes)"
        )
    header_bytes = rest[:header_len]
    b64 = rest[header_len:]
    try:
        header = json.loads(header_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FrameError(f"bad header json: {exc}") from exc
    if not isinstance(header, dict):
        raise FrameError("header is not a JSON object")
    try:
        body = base64.b64decode(b64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise FrameError(f"bad base64 body: {exc}") from exc
    if len(body) != header.get("decoded_length"):
        raise FrameError("decoded length mismatch")
    if hashlib.sha256(body).hexdigest() != header.get("sha256"):
        raise FrameError("payload digest mismatch")
    return header, body


def encode_file_frame(body: bytes, *, transfer_id: str, mtime_ns: int = 0,
                      mode: int | None = None) -> bytes:
    return encode_frame(
        {"v": 2, "transfer_id": transfer_id, "kind": "file", "encoding": "base64",
         "mtime_ns": mtime_ns, "mode": mode},
        body,
    )


def decode_file_frame(data: bytes) -> tuple[dict, bytes]:
    header, body = decode_frame(data)
    if header.get("v") != 2 or header.get("kind") != "file":
        raise FrameError("not a v2 file frame")
    return header, body


def _safe_member_path(name: str) -> str:
    """Normalize a tar member path or raise. Rejects absolute paths, drive/colon, `..`,
    `.`, and empty components — no member may escape the staging root."""
    if not name:
        raise FrameError("empty member path")
    norm = name.replace("\\", "/")
    if norm.startswith("/") or ":" in norm:
        raise FrameError(f"unsafe member path: {name!r}")
    parts = norm.split("/")
    if any(p in ("", ".", "..") for p in parts):
        raise FrameError(f"unsafe member path: {name!r}")
    return "/".join(parts)


def encode_tar_frame(members: list[tuple[str, bytes, int | None]], *,
                     transfer_id: str) -> bytes:
    """Build a v2 tar frame from ``[(path, data, mode|None), ...]``."""
    buf = io.BytesIO()
    specs: list[dict] = []
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for path, data, mode in members:
            safe = _safe_member_path(path)
            info = tarfile.TarInfo(name=safe)
            info.size = len(data)
            info.mtime = 0
            if mode is not None:
                info.mode = mode
            tf.addfile(info, io.BytesIO(data))
            specs.append({"path": safe, "size": len(data),
                          "sha256": hashlib.sha256(data).hexdigest(),
                          "mtime_ns": 0, "mode": mode})
    archive = buf.getvalue()
    return encode_frame(
        {"v": 2, "transfer_id": transfer_id, "kind": "tar", "members": specs}, archive)


def verify_tar_frame(data: bytes, staging_dir) -> dict[str, Path]:
    """Decode + fully verify a v2 tar frame, staging EVERY member before returning.

    Returns ``{member_path: staged Path}``. Raises FrameError — and removes the staging
    dir — on ANY problem (bad outer digest, corrupt tar, unsafe/duplicate/undeclared/
    missing member, per-member size/digest mismatch, or a non-regular member). So a
    caller commits nothing on failure: this is the all-or-nothing property behind B5.
    """
    header, archive = decode_frame(data)
    if header.get("v") != 2 or header.get("kind") != "tar":
        raise FrameError("not a v2 tar frame")
    declared: dict[str, dict] = {}
    for member in header.get("members", []):
        path = _safe_member_path(member["path"])
        if path in declared:
            raise FrameError(f"duplicate declared member: {path}")
        declared[path] = member
    staging_dir = Path(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)
    observed: dict[str, Path] = {}
    try:
        try:
            with tarfile.open(fileobj=io.BytesIO(archive), mode="r:*") as tf:
                for info in tf:
                    if info.isdir():
                        continue  # structural dir — allowed, not a member
                    if not info.isreg():
                        raise FrameError(f"unexpected non-regular tar member: {info.name!r}")
                    path = _safe_member_path(info.name)
                    if path not in declared:
                        raise FrameError(f"undeclared member in archive: {path}")
                    if path in observed:
                        raise FrameError(f"duplicate member in archive: {path}")
                    spec = declared[path]
                    if info.size != spec["size"]:
                        raise FrameError(f"member size mismatch: {path}")
                    extracted = tf.extractfile(info)
                    if extracted is None:
                        raise FrameError(f"member has no data: {path}")
                    payload = extracted.read()
                    if hashlib.sha256(payload).hexdigest() != spec["sha256"]:
                        raise FrameError(f"member digest mismatch: {path}")
                    dest = staging_dir / path
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    with open(dest, "wb") as out:
                        out.write(payload)
                    observed[path] = dest
        except tarfile.TarError as exc:
            raise FrameError(f"corrupt tar archive: {exc}") from exc
        if set(observed) != set(declared):
            missing = sorted(set(declared) - set(observed))
            raise FrameError(f"declared members missing from archive: {missing}")
        return observed
    except BaseException:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise
