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
    "config/defaults.toml",
    "docs/**/*",
    "examples/**/*",
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
    ".md",
    ".ps1",
    ".py",
    ".sh",
    ".toml",
    ".txt",
}

GENERIC_PRIVATE_PATTERNS = [
    r"\b100\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
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


def _load_private_patterns(root: Path, pattern_file: Path | None) -> list[str]:
    if pattern_file is None:
        pattern_file = DEFAULT_PRIVATE_PATTERN_FILE
    path = pattern_file if pattern_file.is_absolute() else root / pattern_file
    if not path.exists():
        return []
    patterns: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if text and not text.startswith("#"):
            patterns.append(text)
    return patterns


def scan(root: Path, *, pattern_file: Path | None = None) -> list[tuple[Path, int, str, str]]:
    patterns = [*GENERIC_PRIVATE_PATTERNS, *_load_private_patterns(root, pattern_file)]
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
    args = parser.parse_args(argv)
    root = args.root.resolve()
    files = iter_public_files(root)
    if args.list_files:
        for path in files:
            print(path.relative_to(root).as_posix())
    hits = scan(root, pattern_file=args.patterns)
    if hits:
        for path, lineno, pattern, line in hits:
            rel = path.relative_to(root).as_posix()
            print(f"{rel}:{lineno}: {pattern}: {line}")
        return 1
    print(f"public release check ok ({len(files)} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
