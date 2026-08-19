"""Tests for RRFRAME2 framing (design Step 2). A truncated/corrupt stream must never be
mistaken for a valid file, and a bad tar bundle must stage/commit nothing."""
from __future__ import annotations

import hashlib
import io
import tarfile
from pathlib import Path

import pytest

from remrun.frame import (
    FrameError,
    decode_file_frame,
    encode_file_frame,
    encode_frame,
    encode_tar_frame,
    verify_tar_frame,
)


# --- single-file frames -------------------------------------------------------

def test_file_frame_roundtrip():
    body = b"hello world\n" * 100
    frame = encode_file_frame(body, transfer_id="t1", mtime_ns=123, mode=0o644)
    header, out = decode_file_frame(frame)
    assert out == body
    assert header["decoded_length"] == len(body)
    assert header["mode"] == 0o644
    assert header["mtime_ns"] == 123


def test_file_frame_truncated_body_rejected():
    frame = encode_file_frame(b"abcdefghij" * 10, transfer_id="t")
    with pytest.raises(FrameError):
        decode_file_frame(frame[:-5])  # SSH cut mid-body


def test_file_frame_extra_bytes_rejected():
    frame = encode_file_frame(b"abc", transfer_id="t")
    with pytest.raises(FrameError):
        decode_file_frame(frame + b"junk")


def test_file_frame_no_newline_header_rejected():
    with pytest.raises(FrameError):
        decode_file_frame(b"RRFRAME2 10 10")  # header line never terminated


def test_file_frame_bad_magic_rejected():
    with pytest.raises(FrameError):
        decode_file_frame(b"NOTAFRAME 1 1\nx")


def test_file_frame_digest_mismatch_rejected():
    # 9 raw bytes -> 12 base64 chars (no padding). Flipping the last base64 char keeps the
    # length valid and the alphabet valid, but changes the bytes -> digest must catch it.
    frame = bytearray(encode_file_frame(b"\x00" * 9, transfer_id="t"))
    frame[-1] = ord("Z") if chr(frame[-1]) != "Z" else ord("Y")
    with pytest.raises(FrameError):
        decode_file_frame(bytes(frame))


def test_file_frame_invalid_base64_rejected():
    frame = bytearray(encode_file_frame(b"abc", transfer_id="t"))
    frame[-1] = ord("!")  # not in the base64 alphabet
    with pytest.raises(FrameError):
        decode_file_frame(bytes(frame))


# --- tar bundles: verify-all-then-stage --------------------------------------

def _raw_tar_frame(archive_members, declared, transfer_id="t"):
    """Build a tar frame from raw (TarInfo, data|None) pairs and an explicit declared list —
    lets tests craft archives encode_tar_frame would refuse to build."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for info, data in archive_members:
            tf.addfile(info, io.BytesIO(data) if data is not None else None)
    return encode_frame(
        {"v": 2, "kind": "tar", "transfer_id": transfer_id, "members": declared},
        buf.getvalue(),
    )


def _spec(path, data, mode=None):
    return {"path": path, "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(), "mtime_ns": 0, "mode": mode}


def test_tar_frame_roundtrip(tmp_path: Path):
    frame = encode_tar_frame(
        [("a/b.txt", b"hello", 0o644), ("c.txt", b"world", None)], transfer_id="t")
    staged = verify_tar_frame(frame, tmp_path / "stage")
    assert set(staged) == {"a/b.txt", "c.txt"}
    assert (tmp_path / "stage" / "a" / "b.txt").read_bytes() == b"hello"
    assert (tmp_path / "stage" / "c.txt").read_bytes() == b"world"


def test_tar_frame_truncated_archive_rejected(tmp_path: Path):
    frame = encode_tar_frame([("a.txt", b"x" * 1000, None)], transfer_id="t")
    with pytest.raises(FrameError):
        verify_tar_frame(frame[:-50], tmp_path / "stage")  # frame body cut


def test_tar_frame_traversal_member_in_archive_rejected(tmp_path: Path):
    info = tarfile.TarInfo("../evil.txt")
    info.size = 3
    frame = _raw_tar_frame([(info, b"bad")], [_spec("ok.txt", b"bad")])
    with pytest.raises(FrameError):
        verify_tar_frame(frame, tmp_path / "stage")
    assert not (tmp_path / "stage").exists()


def test_tar_frame_symlink_member_rejected(tmp_path: Path):
    info = tarfile.TarInfo("link")
    info.type = tarfile.SYMTYPE
    info.linkname = "/etc/passwd"
    frame = _raw_tar_frame([(info, None)], [_spec("link", b"")])
    with pytest.raises(FrameError):
        verify_tar_frame(frame, tmp_path / "stage")


def test_tar_frame_undeclared_member_rejected(tmp_path: Path):
    a = tarfile.TarInfo("a.txt")
    a.size = 1
    b = tarfile.TarInfo("b.txt")
    b.size = 1
    frame = _raw_tar_frame([(a, b"x"), (b, b"y")], [_spec("a.txt", b"x")])  # b.txt undeclared
    with pytest.raises(FrameError):
        verify_tar_frame(frame, tmp_path / "stage")


def test_tar_frame_missing_declared_member_rejected(tmp_path: Path):
    a = tarfile.TarInfo("a.txt")
    a.size = 1
    frame = _raw_tar_frame([(a, b"x")], [_spec("a.txt", b"x"), _spec("b.txt", b"y")])
    with pytest.raises(FrameError):
        verify_tar_frame(frame, tmp_path / "stage")


def test_tar_frame_member_digest_mismatch_rejected(tmp_path: Path):
    a = tarfile.TarInfo("a.txt")
    a.size = 3
    frame = _raw_tar_frame([(a, b"xyz")], [_spec("a.txt", b"ZZZ")])  # declared sha != data
    with pytest.raises(FrameError):
        verify_tar_frame(frame, tmp_path / "stage")
    assert not (tmp_path / "stage").exists()


def test_tar_frame_all_or_nothing_staging(tmp_path: Path):
    # A good member followed by a bad one: the good member must NOT be left staged.
    good = tarfile.TarInfo("good.txt")
    good.size = 4
    bad = tarfile.TarInfo("bad.txt")
    bad.size = 3
    frame = _raw_tar_frame(
        [(good, b"good"), (bad, b"BAD")],
        [_spec("good.txt", b"good"), _spec("bad.txt", b"nope")],  # bad.txt sha wrong
    )
    stage = tmp_path / "stage"
    with pytest.raises(FrameError):
        verify_tar_frame(frame, stage)
    assert not stage.exists()  # zero commits on any failure
