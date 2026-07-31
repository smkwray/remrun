from __future__ import annotations

import pytest

from scripts.public_release_check import iter_public_files, scan


def test_uv_lock_is_scanned(tmp_path):
    lock = tmp_path / "uv.lock"
    lock.write_text("PRIVATE_MARKER\n")
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("PRIVATE_MARKER\n")

    assert lock in iter_public_files(tmp_path)
    hits = scan(tmp_path, pattern_file=patterns)
    assert [(path, line) for path, line, _pattern, _text in hits] == [(lock, 1)]


def test_native_gate_is_scanned(tmp_path):
    gate_dir = tmp_path / "native-gates"
    gate_dir.mkdir()
    gate = gate_dir / "cross_platform_gate.py"
    gate.write_text("PRIVATE_MARKER\n")
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("PRIVATE_MARKER\n")

    assert gate in iter_public_files(tmp_path)
    hits = scan(tmp_path, pattern_file=patterns)
    assert [(path, line) for path, line, _pattern, _text in hits] == [(gate, 1)]


@pytest.mark.parametrize("address", [
    ".".join(("10", "1", "2", "3")),
    ".".join(("172", "16", "2", "3")),
    ".".join(("192", "168", "2", "3")),
])
def test_private_network_literals_are_rejected(tmp_path, address):
    readme = tmp_path / "README.md"
    readme.write_text(f"private endpoint: {address}\n")
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("")

    hits = scan(tmp_path, pattern_file=patterns)
    assert [(path, line) for path, line, _pattern, _text in hits] == [(readme, 1)]
