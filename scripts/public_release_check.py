"""Check the intended public release surface for private or local-only strings.

The public checker scans every text file that would be shipped in the public
artifact. Owner-specific denylist terms live in an ignored local file so the
checker itself can be published without publishing the private vocabulary.
"""
from __future__ import annotations

import argparse
import fnmatch
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


PUBLIC_INCLUDE_GLOBS = [
    ".gitattributes",
    ".gitignore",
    "LICENSE",
    "README.md",
    "pyproject.toml",
    "uv.lock",
    "bin/*",
    "config/*.example.toml",
    "config/*.example.json",
    "config/defaults.toml",
    "docs/**/*",
    "examples/**/*",
    "native-gates/*",
    "schemas/**/*",
    "scripts/**/*",
    "src/**/*",
    "tests/**/*",
]

PUBLIC_EXCLUDE_GLOBS = [
    ".git/**",
    ".mypy_cache/**",
    ".pytest_cache/**",
    ".ruff_cache/**",
    ".venv/**",
    "build/**",
    "cache/**",
    "dist/**",
    "logs/**",
    "run-state/**",
    "state/**",
    "*.egg-info/**",
    "**/__pycache__/**",
    "**/*.pyc",
    "AGENTS.md",
    "HANDOFF.md",
    "PLAN.md",
    "FLEET-*.md",
    "GIT-SYNC-PLAN.md",
    "OFFLOAD-*.md",
    "SYNC-PLAN.md",
    "config/devices.toml",
    "config/fleet_costs.toml",
    "config/private_release_patterns.txt",
    "docs/*_HANDOFF.md",
]

TEXT_SUFFIXES = {
    "",
    ".cmd",
    ".json",
    ".lock",
    ".md",
    ".ps1",
    ".py",
    ".sh",
    ".toml",
    ".txt",
}

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


def _matches_any(path: str, patterns: list[str]) -> bool:
    for pattern in patterns:
        if fnmatch.fnmatch(path, pattern):
            return True
        # pathlib.Path.glob("tests/**/*") includes a file immediately under
        # tests/, while fnmatch requires another slash. Git-tree paths must use
        # the same release-surface semantics as the checkout scan.
        if pattern.endswith("**/*") and path.startswith(pattern.removesuffix("**/*")):
            return True
    return False


def _is_text_candidate(path: Path) -> bool:
    return path.suffix.lower() in TEXT_SUFFIXES


def _is_public_path(relative: str) -> bool:
    return (
        _matches_any(relative, PUBLIC_INCLUDE_GLOBS)
        and not _matches_any(relative, PUBLIC_EXCLUDE_GLOBS)
        and PurePosixPath(relative).suffix.lower() in TEXT_SUFFIXES
    )


def iter_public_files(root: Path) -> list[Path]:
    files: set[Path] = set()
    for glob in PUBLIC_INCLUDE_GLOBS:
        for path in root.glob(glob):
            if not path.is_file() or not _is_text_candidate(path):
                continue
            rel = path.relative_to(root).as_posix()
            if not _matches_any(rel, PUBLIC_EXCLUDE_GLOBS):
                files.add(path)
    return sorted(files)


@dataclass(frozen=True)
class HistoryHit:
    """One unique private-pattern match in a successor Git blob."""

    commit: str
    path: str
    line: int
    pattern: str
    text: str


class HistoryScanError(RuntimeError):
    """The requested Git range could not be authenticated or read."""


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


def scan(root: Path, *, pattern_file: Path | None = None,
         generic_only: bool = False) -> list[tuple[Path, int, str, str]]:
    patterns = [*GENERIC_PRIVATE_PATTERNS,
                *_load_private_patterns(root, pattern_file, generic_only=generic_only)]
    compiled = [(pat, re.compile(pat, re.IGNORECASE)) for pat in patterns]
    hits: list[tuple[Path, int, str, str]] = []
    for path in iter_public_files(root):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            for label, regex in compiled:
                if regex.search(line):
                    hits.append((path, lineno, label, line.strip()))
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
        raise HistoryScanError(detail or f"git {' '.join(args)} failed") from exc


def _read_git_blobs(root: Path, blob_ids: set[str]) -> dict[str, str | None]:
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
        raise HistoryScanError(detail or "git cat-file --batch failed") from exc

    output = result.stdout
    offset = 0
    blobs: dict[str, str | None] = {}
    for requested in ordered:
        header_end = output.find(b"\n", offset)
        if header_end < 0:
            raise HistoryScanError("git cat-file --batch returned a truncated header")
        header = output[offset:header_end].decode("ascii", "replace")
        offset = header_end + 1
        fields = header.split()
        if len(fields) != 3 or fields[0] != requested or fields[1] != "blob":
            raise HistoryScanError(f"unexpected git cat-file response: {header}")
        try:
            size = int(fields[2])
        except ValueError as exc:
            raise HistoryScanError(f"invalid git blob size: {header}") from exc
        raw = output[offset:offset + size]
        offset += size
        if len(raw) != size or output[offset:offset + 1] != b"\n":
            raise HistoryScanError(f"truncated git blob response for {requested}")
        offset += 1
        try:
            blobs[requested] = raw.decode("utf-8")
        except UnicodeDecodeError:
            blobs[requested] = None
    if offset != len(output):
        raise HistoryScanError("git cat-file --batch returned trailing data")
    return blobs


def scan_history(
    root: Path,
    *,
    base: str,
    head: str = "HEAD",
    pattern_file: Path | None = None,
    generic_only: bool = False,
) -> list[HistoryHit]:
    """Scan public-surface blobs in every commit reachable through ``base..head``.

    A clean candidate checkout is insufficient for publication: an intermediate
    tree remains downloadable whenever its commit is reachable from the branch.
    The public baseline is already published, so only successor commits are read.
    """
    root = root.resolve()
    ancestry = _git(root, "merge-base", "--is-ancestor", base, head, check=False)
    if ancestry.returncode != 0:
        detail = ancestry.stderr.decode("utf-8", "replace").strip()
        raise HistoryScanError(
            detail or f"history base {base!r} is not an ancestor of {head!r}"
        )
    commits = [
        value for value in
        _git(root, "rev-list", "--reverse", f"{base}..{head}").stdout
        .decode("ascii", "strict").splitlines()
        if value
    ]
    patterns = [
        *GENERIC_PRIVATE_PATTERNS,
        *_load_private_patterns(root, pattern_file, generic_only=generic_only),
    ]
    compiled = [(label, re.compile(label, re.IGNORECASE)) for label in patterns]
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
                relative = raw_path.decode("utf-8")
            except (UnicodeDecodeError, ValueError) as exc:
                raise HistoryScanError(f"malformed git tree record in {commit}") from exc
            if kind == "blob" and _is_public_path(relative):
                tree_entries.append((commit, relative, blob))
                blob_ids.add(blob)

    blob_cache = _read_git_blobs(root, blob_ids)
    blob_matches: dict[str, list[tuple[int, str, str]]] = {}
    for blob, text in blob_cache.items():
        matches: list[tuple[int, str, str]] = []
        if text is not None:
            for lineno, line in enumerate(text.splitlines(), start=1):
                for label, regex in compiled:
                    if regex.search(line):
                        matches.append((lineno, label, line.strip()))
        blob_matches[blob] = matches

    seen: set[tuple[str, str, int, str]] = set()
    hits: list[HistoryHit] = []
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
                        help="print the public files being scanned")
    parser.add_argument("--generic-only", action="store_true",
                        help="proceed without the deployment-specific denylist (weaker scan; "
                             "not sufficient for a release)")
    parser.add_argument("--history-base",
                        help="also scan every public blob in commits BASE..history-head")
    parser.add_argument("--history-head", default="HEAD",
                        help="candidate ref for --history-base (default: HEAD)")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    files = iter_public_files(root)
    if args.list_files:
        for path in files:
            print(path.relative_to(root).as_posix())
    try:
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
    except (MissingPatternFile, HistoryScanError) as exc:
        print(f"public release check FAILED: {exc}")
        return 2
    if hits:
        for path, lineno, pattern, line in hits:
            rel = path.relative_to(root).as_posix()
            print(f"{rel}:{lineno}: {pattern}: {line}")
        return 1
    if history_hits:
        for hit in history_hits:
            print(
                f"{hit.commit}:{hit.path}:{hit.line}: "
                f"{hit.pattern}: {hit.text}"
            )
        return 1
    scope = "generic patterns only" if args.generic_only else "generic + deployment patterns"
    history = f", history {args.history_base}..{args.history_head}" if args.history_base else ""
    print(f"public release check ok ({len(files)} files, {scope}{history})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
