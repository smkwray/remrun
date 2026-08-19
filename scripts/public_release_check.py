"""Check the intended public release surface for private or local-only strings.

The public checker scans every text file that would be shipped in the public
artifact. Owner-specific denylist terms live in an ignored local file so the
checker itself can be published without publishing the private vocabulary.
"""
from __future__ import annotations

import argparse
import fnmatch
import re
from pathlib import Path


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
    return any(fnmatch.fnmatch(path, pat) for pat in patterns)


def _is_text_candidate(path: Path) -> bool:
    return path.suffix.lower() in TEXT_SUFFIXES


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
    args = parser.parse_args(argv)
    root = args.root.resolve()
    files = iter_public_files(root)
    if args.list_files:
        for path in files:
            print(path.relative_to(root).as_posix())
    try:
        hits = scan(root, pattern_file=args.patterns, generic_only=args.generic_only)
    except MissingPatternFile as exc:
        print(f"public release check FAILED: {exc}")
        return 2
    if hits:
        for path, lineno, pattern, line in hits:
            rel = path.relative_to(root).as_posix()
            print(f"{rel}:{lineno}: {pattern}: {line}")
        return 1
    scope = "generic patterns only" if args.generic_only else "generic + deployment patterns"
    print(f"public release check ok ({len(files)} files, {scope})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
