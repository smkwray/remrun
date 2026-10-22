"""Check everything a release would publish for private or local-only strings.

Both surfaces of the next commit are scanned: every staged index entry (reported as
``:path``, git's name for the staged blob), and every working-tree file that is tracked or
untracked but not ignored, because the next commit can add it. Path and file type do not
matter, file names are scanned like contents, and bytes that are not valid UTF-8 are read
loss-tolerantly rather than skipped. A path that ``.gitignore`` ignores or that is listed as
private, and any nested repository, fails the check outright once git would publish it.
With --history-base, every path, every blob and every message of every successor commit is
scanned too. Owner-specific denylist terms live in an ignored local file so the checker
itself can be published without publishing the private vocabulary.
"""
from __future__ import annotations

import argparse
import fnmatch
import os
import re
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path


# Paths that hold private configuration, session state or continuity notes. `.gitignore` is the
# policy: any path it ignores fails once git would publish it. This list repeats the private
# classes so losing an ignore rule cannot reopen them; a test keeps the two in agreement.
PRIVATE_PATH_GLOBS = [
    "do/**",
    "AGENTS.md",
    "CLAUDE.md",
    "HANDOFF.md",
    "PLAN.md",
    "FLEET-*.md",
    "GIT-SYNC-PLAN.md",
    "OFFLOAD-*.md",
    "SYNC-PLAN.md",
    "docs/*_HANDOFF.md",
    "config/devices.toml",
    "config/fleet_costs.toml",
    "config/private_release_patterns.txt",
    "config/private_worker_scripts.json",
    ".private-denylist",
    "*.sync-conflict-*",
    ".worktrees/**",
    ".claude/**",
    ".delegate-worktrees/**",
    ".codex/**",
    ".env",
    ".env.*",
    "state/**",
    "cache/**",
    "logs/**",
    "run-state/**",
    "*.local.toml",
]
PRIVATE_PATH = "private path would be published"
IGNORED_PATH = "ignored path would be published"
NESTED_REPOSITORY = "nested repository would be published"

GENERIC_PRIVATE_PATTERNS = [
    r"\b100\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
    r"\b10\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
    r"\b172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}\b",
    r"\b192\.168\.\d{1,3}\.\d{1,3}\b",
    r"\\Users\\[^\\\s]+\\OneDrive\\",
    "Proprietary" + " seed",
    "prepare public license" + " before release",
]

DEFAULT_PRIVATE_PATTERN_FILE = Path("config/private_release_patterns.txt")
# An absolute home path names an account: the segment after a POSIX or Windows home root.
_HOME_ACCOUNT = re.compile(r"(?:^|[\\/])(?:users|home)[\\/]+([^\\/]+)", re.IGNORECASE)


def _matches_any(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def _text(data: bytes) -> str:
    return data.decode("utf-8", "replace")


def _git_paths(root: Path, *args: str) -> list[str]:
    listing = _git(root, "ls-files", "-z", *args).stdout
    return sorted({name.decode("utf-8", "surrogateescape")
                   for name in listing.split(b"\0") if name})


def _index_entries(root: Path) -> list[tuple[str, str, str]]:
    """``(mode, object id, path)`` for every staged index entry."""
    entries = []
    for record in _git(root, "ls-files", "--stage", "-z").stdout.split(b"\0"):
        if record:
            metadata, raw_path = record.split(b"\t", 1)
            mode, object_id, _stage = metadata.decode("ascii").split()
            entries.append((mode, object_id, raw_path.decode("utf-8", "surrogateescape")))
    return entries


def iter_public_files(root: Path) -> list[Path]:
    """Every working-tree file git would publish: tracked, or untracked and not ignored."""
    names = _git_paths(root, "--cached", "--others", "--exclude-standard")
    return [root / name for name in names if not name.endswith("/")
            and ((root / name).is_symlink() or (root / name).is_file())]


COMMIT_MESSAGE = "(commit message)"
COMMIT_IDENTITY = "(commit author and committer)"


@dataclass(frozen=True)
class HistoryHit:
    """One private-pattern or private-path match in a successor commit."""

    commit: str
    path: str
    line: int
    pattern: str
    text: str


class GitScanError(RuntimeError):
    """The checkout or the requested Git range could not be read."""


class MissingPatternFile(RuntimeError):
    """The deployment-specific denylist is absent, so this check cannot be trusted."""


def _load_private_patterns(root: Path, pattern_file: Path | None,
                           *, generic_only: bool = False) -> list[str]:
    """Load the deployment-specific regex denylist.

    Raises when the file is missing. It is deliberately gitignored, so a fresh clone or a
    different release machine has no copy — and silently returning [] there left only the
    four generic patterns while still printing "ok", which is the most dangerous possible
    result for a check whose whole job is catching private strings before publication.
    Callers who genuinely want the generic-only scan must say so explicitly.
    """
    if pattern_file is None:
        pattern_file = DEFAULT_PRIVATE_PATTERN_FILE
    path = pattern_file if pattern_file.is_absolute() else root / pattern_file
    if not path.exists():
        if generic_only:
            return []
        raise MissingPatternFile(
            f"private pattern file not found: {path}\n"
            "It is gitignored by design, so it does not travel with a clone — restore it on "
            "this machine, pass --patterns PATH, or re-run with --generic-only to accept a "
            "weaker scan that checks only the built-in patterns."
        )
    patterns: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if text and not text.startswith("#"):
            patterns.append(text)
    return patterns


def _compiled(root: Path, pattern_file: Path | None, generic_only: bool):
    patterns = [*GENERIC_PRIVATE_PATTERNS,
                *_load_private_patterns(root, pattern_file, generic_only=generic_only)]
    return [(label, re.compile(label, re.IGNORECASE)) for label in patterns]


def _strings(value: object):
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from _strings(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _strings(item)


def uncovered_identities(root: Path, *, pattern_file: Path | None = None) -> list[str]:
    """Deployment identities in config/devices.toml that no pattern would catch.

    For each non-simulation device: its name, ``user``, ``tailscale_ip``, every address
    candidate, and the account named by any absolute home path in its settings. An
    identity the patterns miss can reach a fixture or commit message unnoticed, so the
    check fails until the private pattern file covers it. Returns ``DEVICE.field`` labels.
    """
    devices_toml = root / "config" / "devices.toml"
    if not devices_toml.exists():
        return []
    devices = tomllib.loads(devices_toml.read_text(encoding="utf-8")).get("devices", {})
    compiled = [regex for _label, regex in _compiled(root, pattern_file, False)]
    missing: list[str] = []
    for name, table in sorted(devices.items()):
        if not isinstance(table, dict) or table.get("kind") == "local-sim":
            continue
        identities = [("name", name)]
        identities += [(key, table[key]) for key in ("user", "tailscale_ip")
                       if isinstance(table.get(key), str) and table[key]]
        identities += [("address_candidates", value)
                       for value in _strings(table.get("address_candidates", []))]
        identities += [("home path account", match)
                       for value in _strings(table) for match in _HOME_ACCOUNT.findall(value)]
        for field, value in identities:
            label = f"{name}.{field}"
            if label not in missing and not any(regex.search(value) for regex in compiled):
                missing.append(label)
    return missing


def _scan_text(text: str, compiled) -> list[tuple[int, str, str]]:  # noqa: ANN001
    return [(lineno, label, line.strip())
            for lineno, line in enumerate(text.splitlines(), start=1)
            for label, regex in compiled if regex.search(line)]


def _path_hits(location: str, relative: str, compiled) -> list[tuple[str, int, str, str]]:  # noqa: ANN001
    """A file name is published like its contents."""
    return [(location, 0, label, relative) for label, regex in compiled if regex.search(relative)]


def scan(root: Path, *, pattern_file: Path | None = None,
         generic_only: bool = False) -> list[tuple[str, int, str, str]]:
    """Hits in the next commit: staged entries as ``:path``, working-tree files as ``path``."""
    compiled = _compiled(root, pattern_file, generic_only)
    ignored = set(_git_paths(root, "--cached", "--ignored", "--exclude-standard"))
    hits: list[tuple[str, int, str, str]] = []
    entries = _index_entries(root)
    staged = _read_git_blobs(root, {oid for mode, oid, _path in entries if mode != "160000"})
    staged_text: dict[str, str] = {}
    for mode, oid, relative in entries:
        location = ":" + relative
        if mode == "160000":
            hits.append((location, 0, NESTED_REPOSITORY, relative))
            continue
        staged_text[relative] = staged[oid]
        if _matches_any(relative, PRIVATE_PATH_GLOBS):
            hits.append((location, 0, PRIVATE_PATH, relative))
        elif relative in ignored:
            hits.append((location, 0, IGNORED_PATH, relative))
        else:
            hits += _path_hits(location, relative, compiled)
            hits += [(location, lineno, label, line)
                     for lineno, label, line in _scan_text(staged[oid], compiled)]
    # Tracked files whose working copy differs from the staged entry, as git sees them after
    # its own line-ending and filter conversion; the rest were scanned through the index.
    modified = {name.decode("utf-8", "surrogateescape")
                for name in _git(root, "diff", "--name-only", "-z").stdout.split(b"\0") if name}
    for relative in _git_paths(root, "--cached", "--others", "--exclude-standard"):
        if relative.endswith("/"):  # an untracked directory git reports whole: another repository
            hits.append((relative, 0, NESTED_REPOSITORY, relative.rstrip("/")))
            continue
        path = root / relative
        if relative in ignored or not (path.is_symlink() or path.is_file()):
            continue  # already reported from the index, deleted, or a staged gitlink
        if relative not in staged_text and _matches_any(relative, PRIVATE_PATH_GLOBS):
            hits.append((relative, 0, PRIVATE_PATH, relative))
            continue
        # Git publishes a symlink's target, never the file it points at.
        text = _text(os.readlink(path).encode("utf-8", "surrogateescape") if path.is_symlink()
                     else path.read_bytes())
        if relative not in staged_text:
            hits += _path_hits(relative, relative, compiled)
        elif relative not in modified or _matches_any(relative, PRIVATE_PATH_GLOBS):
            continue  # same as the staged entry, or already failed as a private path
        hits += [(relative, lineno, label, line)
                 for lineno, label, line in _scan_text(text, compiled)]
    return hits


def _git(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            check=check,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = ""
        if isinstance(exc, subprocess.CalledProcessError):
            detail = (exc.stderr or exc.stdout or b"").decode("utf-8", "replace").strip()
        raise GitScanError(detail or f"git {' '.join(args)} failed") from exc


def _read_git_blobs(root: Path, blob_ids: set[str]) -> dict[str, str]:
    """Read a set of Git blobs through one batch process."""
    if not blob_ids:
        return {}
    ordered = sorted(blob_ids)
    request = ("\n".join(ordered) + "\n").encode("ascii")
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "cat-file", "--batch"],
            input=request,
            capture_output=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = ""
        if isinstance(exc, subprocess.CalledProcessError):
            detail = (exc.stderr or exc.stdout or b"").decode("utf-8", "replace").strip()
        raise GitScanError(detail or "git cat-file --batch failed") from exc

    output = result.stdout
    offset = 0
    blobs: dict[str, str] = {}
    for requested in ordered:
        header_end = output.find(b"\n", offset)
        if header_end < 0:
            raise GitScanError("git cat-file --batch returned a truncated header")
        header = output[offset:header_end].decode("ascii", "replace")
        offset = header_end + 1
        fields = header.split()
        if len(fields) != 3 or fields[0] != requested or fields[1] != "blob":
            raise GitScanError(f"unexpected git cat-file response: {header}")
        try:
            size = int(fields[2])
        except ValueError as exc:
            raise GitScanError(f"invalid git blob size: {header}") from exc
        raw = output[offset:offset + size]
        offset += size
        if len(raw) != size or output[offset:offset + 1] != b"\n":
            raise GitScanError(f"truncated git blob response for {requested}")
        offset += 1
        blobs[requested] = _text(raw)
    if offset != len(output):
        raise GitScanError("git cat-file --batch returned trailing data")
    return blobs


def _ignored_paths(root: Path, paths: list[str]) -> set[str]:
    """The subset of ``paths`` the checkout's ignore rules match, tracked or not."""
    if not paths:
        return set()
    request = b"".join(path.encode("utf-8", "surrogateescape") + b"\0" for path in paths)
    try:
        result = subprocess.run(["git", "-C", str(root), "check-ignore", "--no-index", "-z",
                                 "--stdin"], input=request, capture_output=True, check=False)
    except OSError as exc:
        raise GitScanError(f"git check-ignore failed: {exc}") from exc
    if result.returncode not in (0, 1):  # 1 = nothing ignored
        raise GitScanError(result.stderr.decode("utf-8", "replace").strip()
                           or "git check-ignore failed")
    return {name.decode("utf-8", "surrogateescape")
            for name in result.stdout.split(b"\0") if name}


def scan_history(
    root: Path,
    *,
    base: str,
    head: str = "HEAD",
    pattern_file: Path | None = None,
    generic_only: bool = False,
) -> list[HistoryHit]:
    """Scan every path, blob, message and author identity of every commit in ``base..head``.

    A clean candidate checkout is insufficient for publication: an intermediate
    tree remains downloadable whenever its commit is reachable from the branch,
    whatever the path or type of each file in it, and every commit message and file
    name is published with it. The public baseline is already published, so only
    successor commits are read. Paths are judged against the checkout's `.gitignore`.
    """
    root = root.resolve()
    ancestry = _git(root, "merge-base", "--is-ancestor", base, head, check=False)
    if ancestry.returncode != 0:
        detail = ancestry.stderr.decode("utf-8", "replace").strip()
        raise GitScanError(
            detail or f"history base {base!r} is not an ancestor of {head!r}"
        )
    commits = [
        value for value in
        _git(root, "rev-list", "--reverse", f"{base}..{head}").stdout
        .decode("ascii", "strict").splitlines()
        if value
    ]
    compiled = _compiled(root, pattern_file, generic_only)
    hits: list[HistoryHit] = []
    flagged_paths: set[str] = set()
    first_seen: dict[str, str] = {}
    tree_entries: list[tuple[str, str, str]] = []
    blob_ids: set[str] = set()
    for commit in commits:
        listing = _git(root, "ls-tree", "-r", "-z", commit).stdout
        for record in listing.split(b"\0"):
            if not record:
                continue
            try:
                metadata, raw_path = record.split(b"\t", 1)
                _mode, kind, blob = metadata.decode("ascii").split()
                relative = raw_path.decode("utf-8", "surrogateescape")
            except (UnicodeDecodeError, ValueError) as exc:
                raise GitScanError(f"malformed git tree record in {commit}") from exc
            first_seen.setdefault(relative, commit)
            if _matches_any(relative, PRIVATE_PATH_GLOBS) or kind == "commit":
                if relative not in flagged_paths:  # report where it first appears
                    flagged_paths.add(relative)
                    label = NESTED_REPOSITORY if kind == "commit" else PRIVATE_PATH
                    hits.append(HistoryHit(commit, relative, 0, label, relative))
            elif kind == "blob":
                tree_entries.append((commit, relative, blob))
                blob_ids.add(blob)

    ignored = _ignored_paths(root, [path for path in first_seen if path not in flagged_paths])
    for relative, commit in first_seen.items():
        if relative in flagged_paths:
            continue
        if relative in ignored:
            hits.append(HistoryHit(commit, relative, 0, IGNORED_PATH, relative))
        else:
            hits += [HistoryHit(commit, relative, 0, label, relative)
                     for label, regex in compiled if regex.search(relative)]

    blob_matches = {blob: _scan_text(text, compiled)
                    for blob, text in _read_git_blobs(root, blob_ids).items()}

    seen: set[tuple[str, str, int, str]] = set()
    for commit in commits:
        message = _text(_git(root, "log", "-1", "--format=%B", commit).stdout)
        hits += [HistoryHit(commit=commit, path=COMMIT_MESSAGE, line=lineno,
                            pattern=label, text=line)
                 for lineno, label, line in _scan_text(message, compiled)]
        identity = _text(_git(root, "log", "-1", "--format=%an <%ae>%n%cn <%ce>", commit).stdout)
        hits += [HistoryHit(commit=commit, path=COMMIT_IDENTITY, line=lineno,
                            pattern=label, text=line)
                 for lineno, label, line in _scan_text(identity, compiled)]
    for commit, relative, blob in tree_entries:
        for lineno, label, line in blob_matches[blob]:
            key = (blob, relative, lineno, label)
            if key not in seen:
                seen.add(key)
                hits.append(HistoryHit(
                    commit=commit,
                    path=relative,
                    line=lineno,
                    pattern=label,
                    text=line,
                ))
    return hits


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--patterns", type=Path, default=DEFAULT_PRIVATE_PATTERN_FILE,
                        help="ignored local regex denylist, relative to --root by default")
    parser.add_argument("--list-files", action="store_true",
                        help="print every file git would publish, as scanned")
    parser.add_argument("--generic-only", action="store_true",
                        help="proceed without the deployment-specific denylist (weaker scan; "
                             "not sufficient for a release)")
    parser.add_argument("--history-base",
                        help="also scan every blob and commit message in "
                             "BASE..history-head (required before a push)")
    parser.add_argument("--history-head", default="HEAD",
                        help="candidate ref for --history-base (default: HEAD)")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    try:
        files = iter_public_files(root)
        hits = scan(root, pattern_file=args.patterns, generic_only=args.generic_only)
        history_hits = (
            scan_history(
                root,
                base=args.history_base,
                head=args.history_head,
                pattern_file=args.patterns,
                generic_only=args.generic_only,
            )
            if args.history_base else []
        )
        uncovered = ([] if args.generic_only
                     else uncovered_identities(root, pattern_file=args.patterns))
    except (MissingPatternFile, GitScanError) as exc:
        print(f"public release check FAILED: {exc}")
        return 2
    if args.list_files:
        for path in files:
            print(path.relative_to(root).as_posix())
    if uncovered:
        print("public release check FAILED: config/devices.toml identities the private pattern "
              f"file does not cover: {', '.join(uncovered)}")
    for location, lineno, pattern, line in hits:
        print(f"{location}:{lineno}: {pattern}: {line}")
    for hit in history_hits:
        print(f"{hit.commit}:{hit.path}:{hit.line}: {hit.pattern}: {hit.text}")
    if uncovered or hits or history_hits:
        return 1
    scope = "generic patterns only" if args.generic_only else "generic + deployment patterns"
    history = f", history {args.history_base}..{args.history_head}" if args.history_base else ""
    print(f"public release check ok ({len(files)} files, {scope}{history})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
