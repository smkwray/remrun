from __future__ import annotations

import subprocess

import pytest

from scripts.public_release_check import iter_public_files, scan, scan_history


def _git(root, *args):  # noqa: ANN001, ANN202
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_uv_lock_is_scanned(tmp_path):
    lock = tmp_path / "uv.lock"
    lock.write_text("PRIVATE_MARKER\n")
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("PRIVATE_MARKER\n")

    assert lock in iter_public_files(tmp_path)
    hits = scan(tmp_path, pattern_file=patterns)
    assert [(path, line) for path, line, _pattern, _text in hits] == [(lock, 1)]


def test_example_json_is_scanned(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    manifest = config / "private_worker_scripts.example.json"
    manifest.write_text('{"worker": "PRIVATE_MARKER"}\n')
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("PRIVATE_MARKER\n")

    assert manifest in iter_public_files(tmp_path)
    hits = scan(tmp_path, pattern_file=patterns)
    assert [(path, line) for path, line, _pattern, _text in hits] == [(manifest, 1)]


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


def test_history_scan_rejects_private_blob_removed_from_clean_tip(tmp_path):
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.name", "smkwray")
    _git(tmp_path, "config", "user.email", "45633267+smkwray@users.noreply.github.com")
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("PRIVATE_DEPLOYMENT_LABEL\n")
    readme = tmp_path / "README.md"
    readme.write_text("public\n")
    _git(tmp_path, "add", "README.md")
    _git(tmp_path, "commit", "-qm", "baseline")
    baseline = _git(tmp_path, "rev-parse", "HEAD")

    tests = tmp_path / "tests"
    tests.mkdir()
    private_file = tests / "test_private.py"
    private_file.write_text("PRIVATE_DEPLOYMENT_LABEL\n")
    _git(tmp_path, "add", "tests/test_private.py")
    _git(tmp_path, "commit", "-qm", "private intermediate")
    private_commit = _git(tmp_path, "rev-parse", "HEAD")
    private_file.unlink()
    _git(tmp_path, "commit", "-qam", "clean tip")

    assert scan(tmp_path, pattern_file=patterns) == []
    hits = scan_history(tmp_path, base=baseline, pattern_file=patterns)
    assert [(hit.commit, hit.path, hit.line, hit.pattern) for hit in hits] == [
        (private_commit, "tests/test_private.py", 1, "PRIVATE_DEPLOYMENT_LABEL")
    ]
