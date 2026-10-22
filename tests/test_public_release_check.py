from __future__ import annotations

import subprocess

import pytest

from pathlib import Path

from scripts.public_release_check import (
    COMMIT_IDENTITY, COMMIT_MESSAGE, IGNORED_PATH, NESTED_REPOSITORY, PRIVATE_PATH, PRIVATE_PATH_GLOBS,
    GitScanError, iter_public_files, scan, scan_history, uncovered_identities,
)

PROJECT_GITIGNORE = Path(__file__).resolve().parents[1] / ".gitignore"


def _git(root, *args):  # noqa: ANN001, ANN202
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repo(root, *local_excludes):  # noqa: ANN001, ANN002, ANN202
    """A git checkout whose test pattern file is ignored locally, as the real one is."""
    _git(root, "init", "-q")
    _git(root, "config", "user.name", "smkwray")
    _git(root, "config", "user.email", "45633267+smkwray@users.noreply.github.com")
    (root / ".git" / "info" / "exclude").write_text(
        "\n".join(["patterns.txt", *local_excludes]) + "\n")
    return root


def _marker_patterns(root):  # noqa: ANN001, ANN202
    patterns = root / "patterns.txt"
    patterns.write_text("PRIVATE_MARKER\n")
    return patterns


def test_uv_lock_is_scanned(tmp_path):
    _repo(tmp_path)
    lock = tmp_path / "uv.lock"
    lock.write_text("PRIVATE_MARKER\n")
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("PRIVATE_MARKER\n")

    assert lock in iter_public_files(tmp_path)
    hits = scan(tmp_path, pattern_file=patterns)
    assert [(where, line) for where, line, _pattern, _text in hits] == [("uv.lock", 1)]


def test_example_json_is_scanned(tmp_path):
    _repo(tmp_path)
    config = tmp_path / "config"
    config.mkdir()
    manifest = config / "private_worker_scripts.example.json"
    manifest.write_text('{"worker": "PRIVATE_MARKER"}\n')
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("PRIVATE_MARKER\n")

    assert manifest in iter_public_files(tmp_path)
    hits = scan(tmp_path, pattern_file=patterns)
    assert [(where, line) for where, line, _pattern, _text in hits] == [
        ("config/private_worker_scripts.example.json", 1)]


def test_native_gate_is_scanned(tmp_path):
    _repo(tmp_path)
    gate_dir = tmp_path / "native-gates"
    gate_dir.mkdir()
    gate = gate_dir / "cross_platform_gate.py"
    gate.write_text("PRIVATE_MARKER\n")
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("PRIVATE_MARKER\n")

    assert gate in iter_public_files(tmp_path)
    hits = scan(tmp_path, pattern_file=patterns)
    assert [(where, line) for where, line, _pattern, _text in hits] == [
        ("native-gates/cross_platform_gate.py", 1)]


@pytest.mark.parametrize("address", [
    ".".join(("10", "1", "2", "3")),
    ".".join(("172", "16", "2", "3")),
    ".".join(("192", "168", "2", "3")),
])
def test_private_network_literals_are_rejected(tmp_path, address):
    _repo(tmp_path)
    readme = tmp_path / "README.md"
    readme.write_text(f"private endpoint: {address}\n")
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("")

    hits = scan(tmp_path, pattern_file=patterns)
    assert [(where, line) for where, line, _pattern, _text in hits] == [("README.md", 1)]


def test_history_scan_rejects_private_blob_removed_from_clean_tip(tmp_path):
    _repo(tmp_path)
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


def test_history_scan_rejects_a_private_name_in_a_commit_message(tmp_path):
    """Every commit message is published with the branch, even when all its files are clean."""
    _repo(tmp_path)
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("PRIVATE_DEPLOYMENT_LABEL\n")
    readme = tmp_path / "README.md"
    readme.write_text("public\n")
    _git(tmp_path, "add", "README.md")
    _git(tmp_path, "commit", "-qm", "baseline")
    baseline = _git(tmp_path, "rev-parse", "HEAD")
    readme.write_text("public, revised\n")
    _git(tmp_path, "commit", "-qam",
         "Fix the runner\n\nSeen on PRIVATE_DEPLOYMENT_LABEL, whose projects live elsewhere.")
    leaky = _git(tmp_path, "rev-parse", "HEAD")

    assert scan(tmp_path, pattern_file=patterns) == []
    hits = scan_history(tmp_path, base=baseline, pattern_file=patterns)
    assert [(hit.commit, hit.path, hit.line, hit.pattern) for hit in hits] == [
        (leaky, COMMIT_MESSAGE, 3, "PRIVATE_DEPLOYMENT_LABEL")
    ]


def test_every_configured_identity_must_be_in_the_private_pattern_file(tmp_path):
    """A covered device name does not cover its user or addresses; each can leak on its own."""
    # Built from parts so this file carries no literal home path of its own.
    posix_home = "/".join(("", "home", "listeduser", "projects"))
    windows_home = "\\\\".join(("C:", "Users", "privateuser", "projects"))
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "devices.toml").write_text(
        '[devices.LISTEDBOX]\nkind = "ssh-posix"\nuser = "listeduser"\n'
        'address_candidates = ["listedbox"]\n'
        f'project_root = "{posix_home}"\n'
        '[devices.NEWBOX]\nkind = "ssh-posix"\nuser = "privateuser"\n'
        'address_candidates = ["newbox", "lan-host-7"]\n'
        f'tailscale_ip = "{".".join(("100", "64", "0", "9"))}"\n'
        f'project_root = "{windows_home}"\n'
        '[devices.SIM]\nkind = "local-sim"\nuser = "anyone"\n'
    )
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("(?<![A-Za-z0-9])listedbox(?![A-Za-z0-9])\n"
                        "(?<![A-Za-z0-9])listeduser(?![A-Za-z0-9])\n"
                        "(?<![A-Za-z0-9])newbox(?![A-Za-z0-9])\n")
    # The tailnet address is caught by a generic pattern; the user and LAN alias are not.
    assert uncovered_identities(tmp_path, pattern_file=patterns) == [
        "NEWBOX.user", "NEWBOX.address_candidates", "NEWBOX.home path account",
    ]
    patterns.write_text(patterns.read_text() + "(?<![A-Za-z0-9])privateuser(?![A-Za-z0-9])\n"
                        "lan-host-7\n")
    assert uncovered_identities(tmp_path, pattern_file=patterns) == []


def test_every_file_git_would_publish_is_scanned_whatever_its_type(tmp_path):
    """An unlisted path or suffix ships like any other file, tracked or about to be added."""
    _repo(tmp_path)
    patterns = _marker_patterns(tmp_path)
    (tmp_path / ".gitignore").write_text("ignored/\n")
    tracked = tmp_path / "misc" / "release-notes.data"
    tracked.parent.mkdir()
    tracked.write_text("PRIVATE_MARKER\n")
    _git(tmp_path, "add", ".gitignore", "misc/release-notes.data")
    addable = tmp_path / "notes.unknown"
    addable.write_text("PRIVATE_MARKER\n")
    (tmp_path / "ignored").mkdir()
    (tmp_path / "ignored" / "local.txt").write_text("PRIVATE_MARKER\n")

    hits = {where for where, *_ in scan(tmp_path, pattern_file=patterns)}
    # The staged copy is named `:path`, git's name for what the next commit records.
    assert hits == {":misc/release-notes.data", "notes.unknown"}


def test_undecodable_bytes_do_not_hide_a_private_string(tmp_path):
    _repo(tmp_path)
    patterns = _marker_patterns(tmp_path)
    (tmp_path / "README.md").write_text("public\n")
    _git(tmp_path, "add", "README.md")
    _git(tmp_path, "commit", "-qm", "baseline")
    baseline = _git(tmp_path, "rev-parse", "HEAD")
    (tmp_path / "README.md").write_bytes(b"public\n\xff\xfe broken PRIVATE_MARKER\n")
    _git(tmp_path, "commit", "-qam", "revise")

    assert [line for _path, line, *_ in scan(tmp_path, pattern_file=patterns)] == [2]
    assert [(hit.path, hit.line) for hit in scan_history(tmp_path, base=baseline, pattern_file=patterns)] == [
        ("README.md", 2)
    ]


def test_a_force_added_private_path_fails_even_without_a_private_string(tmp_path):
    _repo(tmp_path)
    patterns = _marker_patterns(tmp_path)
    (tmp_path / ".gitignore").write_text("/config/devices.toml\n")
    _git(tmp_path, "add", ".gitignore")
    _git(tmp_path, "commit", "-qm", "baseline")
    baseline = _git(tmp_path, "rev-parse", "HEAD")
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "devices.toml").write_text("[devices.EXAMPLE]\n")
    _git(tmp_path, "add", "-f", "config/devices.toml")
    _git(tmp_path, "commit", "-qm", "oops")

    assert [(where, pattern) for where, _line, pattern, _text in scan(tmp_path, pattern_file=patterns)] == [
        (":config/devices.toml", PRIVATE_PATH)
    ]
    assert [(hit.path, hit.pattern) for hit in scan_history(tmp_path, base=baseline, pattern_file=patterns)] == [
        ("config/devices.toml", PRIVATE_PATH)
    ]


def test_a_symlink_is_scanned_as_the_target_git_publishes(tmp_path):
    _repo(tmp_path, "PRIVATE_MARKER-target")
    patterns = _marker_patterns(tmp_path)
    (tmp_path / "PRIVATE_MARKER-target").write_text("contents git never publishes\n")
    (tmp_path / "link").symlink_to("PRIVATE_MARKER-target")

    hits = scan(tmp_path, pattern_file=patterns)
    assert [(where, text) for where, _line, _pattern, text in hits] == [
        ("link", "PRIVATE_MARKER-target")
    ]


def test_the_check_refuses_a_directory_that_is_not_a_git_checkout(tmp_path):
    with pytest.raises(GitScanError):
        iter_public_files(tmp_path)


def _committed_repo(tmp_path):  # noqa: ANN001, ANN202
    """A repository with the project's real ignore rules and one published baseline commit."""
    _repo(tmp_path)
    (tmp_path / ".gitignore").write_text(PROJECT_GITIGNORE.read_text())
    (tmp_path / "README.md").write_text("public\n")
    _git(tmp_path, "add", ".gitignore", "README.md")
    _git(tmp_path, "commit", "-qm", "baseline")
    return _git(tmp_path, "rev-parse", "HEAD")


def test_the_staged_blob_is_checked_even_when_the_worktree_copy_is_clean(tmp_path):
    """The next commit records the index, not the working tree."""
    _committed_repo(tmp_path)
    patterns = _marker_patterns(tmp_path)
    (tmp_path / "README.md").write_text("public\nPRIVATE_MARKER\n")
    _git(tmp_path, "add", "README.md")
    (tmp_path / "README.md").write_text("public\n")

    assert [(where, line) for where, line, *_ in scan(tmp_path, pattern_file=patterns)] == [
        (":README.md", 2)
    ]


def test_staged_and_untracked_nested_repositories_fail(tmp_path):
    baseline = _committed_repo(tmp_path)
    patterns = _marker_patterns(tmp_path)
    for name in ("vendor-lib", "scratch"):
        nested = tmp_path / name
        nested.mkdir()
        _repo(nested)
        (nested / "file.txt").write_text("public\n")
        _git(nested, "add", "file.txt")
        _git(nested, "commit", "-qm", "nested")
    sha = _git(tmp_path / "vendor-lib", "rev-parse", "HEAD")
    _git(tmp_path, "update-index", "--add", "--cacheinfo", f"160000,{sha},vendor-lib")

    hits = [(where, pattern) for where, _line, pattern, _text in scan(tmp_path, pattern_file=patterns)]
    assert sorted(hits) == [(":vendor-lib", NESTED_REPOSITORY), ("scratch/", NESTED_REPOSITORY)]
    _git(tmp_path, "commit", "-qm", "gitlink")
    assert [(hit.path, hit.pattern) for hit in scan_history(tmp_path, base=baseline, pattern_file=patterns)] == [
        ("vendor-lib", NESTED_REPOSITORY)
    ]


def test_a_private_name_in_a_file_name_alone_fails(tmp_path):
    """A file name is public git metadata, published like the file's contents."""
    baseline = _committed_repo(tmp_path)
    patterns = _marker_patterns(tmp_path)
    (tmp_path / "notes").mkdir()
    (tmp_path / "notes" / "PRIVATE_MARKER.txt").write_text("public\n")
    _git(tmp_path, "add", "notes")

    assert [(where, line) for where, line, *_ in scan(tmp_path, pattern_file=patterns)] == [
        (":notes/PRIVATE_MARKER.txt", 0)
    ]
    _git(tmp_path, "commit", "-qm", "notes")
    assert [(hit.path, hit.line) for hit in scan_history(tmp_path, base=baseline, pattern_file=patterns)] == [
        ("notes/PRIVATE_MARKER.txt", 0)
    ]


def test_force_added_local_paths_fail_by_class_whatever_they_contain(tmp_path):
    """No secret value has to be known: the path's class is enough."""
    baseline = _committed_repo(tmp_path)
    patterns = _marker_patterns(tmp_path)
    for relative in (".env", ".claude/settings.json", "logs/run.log", "build/output.txt"):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("an otherwise unknown value\n")
        _git(tmp_path, "add", "-f", relative)

    expected = [(":.claude/settings.json", PRIVATE_PATH), (":.env", PRIVATE_PATH),
                (":build/output.txt", IGNORED_PATH), (":logs/run.log", PRIVATE_PATH)]
    assert sorted((where, pattern) for where, _l, pattern, _t in scan(tmp_path, pattern_file=patterns)) == expected
    _git(tmp_path, "commit", "-qm", "oops")
    history = scan_history(tmp_path, base=baseline, pattern_file=patterns)
    assert sorted((":" + hit.path, hit.pattern) for hit in history) == expected


def test_private_path_classes_agree_with_the_project_ignore_rules(tmp_path):
    """Each private class must also be ignored, so `git add -A` never stages it."""
    _repo(tmp_path)
    (tmp_path / ".gitignore").write_text(PROJECT_GITIGNORE.read_text())
    samples = [glob.replace("**", "x/y").replace("*", "x") for glob in PRIVATE_PATH_GLOBS]
    result = subprocess.run(
        ["git", "-C", str(tmp_path), "check-ignore", "--no-index", "-z", "--stdin"],
        input=b"".join(sample.encode() + b"\0" for sample in samples),
        capture_output=True, check=False)
    assert sorted(name.decode() for name in result.stdout.split(b"\0") if name) == sorted(samples)


def test_windows_home_accounts_match_in_any_case_and_with_spaces(tmp_path):
    (tmp_path / "config").mkdir()
    home = "\\\\".join(("c:", "users", "Hidden Account", "projects"))
    (tmp_path / "config" / "devices.toml").write_text(
        f'[devices.LISTEDBOX]\nkind = "ssh-powershell"\nproject_root = "{home}"\n')
    patterns = tmp_path / "patterns.txt"
    patterns.write_text("(?<![A-Za-z0-9])listedbox(?![A-Za-z0-9])\n")
    assert uncovered_identities(tmp_path, pattern_file=patterns) == ["LISTEDBOX.home path account"]
    patterns.write_text(patterns.read_text() + "hidden account\n")
    assert uncovered_identities(tmp_path, pattern_file=patterns) == []


def test_a_private_author_identity_fails_like_a_message(tmp_path):
    """Author and committer names and emails ship with every commit."""
    baseline = _committed_repo(tmp_path)
    patterns = _marker_patterns(tmp_path)
    (tmp_path / "README.md").write_text("public, revised\n")
    _git(tmp_path, "-c", "user.email=PRIVATE_MARKER@example.invalid", "commit", "-qam", "revise")

    hits = scan_history(tmp_path, base=baseline, pattern_file=patterns)
    assert [(hit.path, hit.line) for hit in hits] == [(COMMIT_IDENTITY, 1), (COMMIT_IDENTITY, 2)]
