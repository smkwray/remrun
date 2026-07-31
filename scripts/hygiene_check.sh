#!/usr/bin/env bash
set -euo pipefail

# Public-surface hygiene gate. Repo mode scans tracked public candidates; staged
# mode scans changed index paths. Commit attribution is always checked across HEAD.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="repo"
if [[ "${1:-}" == "--staged" ]]; then
  MODE="staged"
elif [[ -n "${1:-}" ]]; then
  echo "hygiene_check: unknown argument: $1" >&2
  exit 2
fi

EXPECTED_NAME="smkwray"
EXPECTED_EMAIL="45633267+smkwray@users.noreply.github.com"
SCAN_ROOTS=(src scripts config docs examples native-gates schemas tests bin)
TOP_LEVEL_FILES=(README.md LICENSE pyproject.toml uv.lock)
violations=0
warnings=0

is_public_candidate() {
  local path="$1"
  case "$path" in
    scripts/hygiene_check.sh) return 1 ;;
    do/*|var/*|data/*|outputs/*|dist/*|build/*|logs/*|scratch/*|.git/*) return 1 ;;
  esac
  local root
  for root in "${SCAN_ROOTS[@]}"; do
    [[ "$path" == "$root"/* ]] && return 0
  done
  local file
  for file in "${TOP_LEVEL_FILES[@]}"; do
    [[ "$path" == "$file" ]] && return 0
  done
  return 1
}

is_text_candidate() {
  case "$1" in
    bin/*|*.py|*.md|*.yml|*.yaml|*.toml|*.sh|*.ps1|*.cmd|*.cfg|*.ini|*.json|*.txt|*.lock)
      return 0
      ;;
  esac
  return 1
}

gather_files() {
  if [[ "$MODE" == "staged" ]]; then
    git diff --cached --name-only --diff-filter=ACMR
  else
    git ls-files
  fi
}

FILES=()
while IFS= read -r path; do
  if [[ "$MODE" == "staged" ]]; then
    git cat-file -e ":$path" 2>/dev/null || continue
  else
    [[ -f "$path" ]] || continue
  fi
  is_public_candidate "$path" || continue
  is_text_candidate "$path" || continue
  FILES+=("$path")
done < <(gather_files)

grep_content() {
  local flags="$1"
  local pattern="$2"
  local path="$3"
  if [[ "$MODE" == "staged" ]]; then
    git show ":$path" | grep "$flags" -- "$pattern"
  else
    grep "$flags" -- "$pattern" "$path"
  fi
}

absolute_hits=""
data_inventory_hits=""
ai_hits=""
for path in ${FILES[@]+"${FILES[@]}"}; do
  if hits="$(grep_content -nE '(/Users/[A-Za-z0-9._-]+|/home/[A-Za-z0-9._-]+|[A-Za-z]:[\\/]Users)' "$path" 2>/dev/null)"; then
    while IFS= read -r line; do
      [[ -n "$line" ]] && absolute_hits+="${path}:${line}"$'\n'
    done <<< "$hits"
  fi
  case "$path" in
    src/*|tests/*|config/*|configs/*)
      if hits="$(grep_content -nE '(/|\.\./)data\.md\b' "$path" 2>/dev/null)"; then
        while IFS= read -r line; do
          [[ -n "$line" ]] && data_inventory_hits+="${path}:${line}"$'\n'
        done <<< "$hits"
      fi
      ;;
  esac
  if hits="$(grep_content -niE '\b(codex|claude|gpt[- ]?pro|gpt-?5|openai|anthropic|orca|mako|dairy|tandy)\b' "$path" 2>/dev/null)"; then
    while IFS= read -r line; do
      [[ -n "$line" ]] && ai_hits+="${path}:${line}"$'\n'
    done <<< "$hits"
  fi
done

if [[ -n "$absolute_hits" ]]; then
  echo "hygiene_check: FAIL — absolute system paths:" >&2
  printf '%s' "$absolute_hits" >&2
  violations=$((violations + 1))
fi

if [[ -n "$data_inventory_hits" ]]; then
  echo "hygiene_check: FAIL — data.md inventory referenced by code/config/tests:" >&2
  printf '%s' "$data_inventory_hits" >&2
  violations=$((violations + 1))
fi

if [[ -n "$ai_hits" ]]; then
  count="$(printf '%s' "$ai_hits" | grep -c .)"
  echo "hygiene_check: WARN — ${count} AI/tool-name mention(s); verify they are intentional:" >&2
  printf '%s' "$ai_hits" | head -20 >&2
  [[ "$count" -gt 20 ]] && echo "  ... and $((count - 20)) more." >&2
  warnings=$((warnings + 1))
fi

if [[ "$MODE" == "staged" ]]; then
  if ! git diff --cached --check; then
    echo "hygiene_check: FAIL — git diff --cached --check" >&2
    violations=$((violations + 1))
  fi
elif ! git diff --check; then
  echo "hygiene_check: FAIL — git diff --check" >&2
  violations=$((violations + 1))
fi

identity_hits=""
while IFS=$'\t' read -r hash author_name author_email committer_name committer_email; do
  if [[ "$author_name" != "$EXPECTED_NAME" || "$author_email" != "$EXPECTED_EMAIL" \
        || "$committer_name" != "$EXPECTED_NAME" || "$committer_email" != "$EXPECTED_EMAIL" ]]; then
    identity_hits+="${hash} ${author_name} <${author_email}> / ${committer_name} <${committer_email}>"$'\n'
  fi
done < <(git log --format='%H%x09%an%x09%ae%x09%cn%x09%ce' HEAD)

if [[ -n "$identity_hits" ]]; then
  echo "hygiene_check: FAIL — non-smkwray commit identity:" >&2
  printf '%s' "$identity_hits" >&2
  violations=$((violations + 1))
fi

coauthor_hits="$(git log --format='%H%n%B%n---' HEAD | grep -ni '^Co-Authored-By:' || true)"
if [[ -n "$coauthor_hits" ]]; then
  echo "hygiene_check: FAIL — Co-Authored-By trailers are not allowed:" >&2
  printf '%s\n' "$coauthor_hits" >&2
  violations=$((violations + 1))
fi

if [[ "$violations" -gt 0 ]]; then
  echo "hygiene_check: FAIL ($violations violation(s), $warnings warning(s))" >&2
  exit 1
fi

if [[ "$warnings" -gt 0 ]]; then
  echo "hygiene_check: pass with $warnings warning(s)"
else
  echo "hygiene_check: pass"
fi
