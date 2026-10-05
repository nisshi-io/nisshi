#!/usr/bin/env bash
#
# CI check: a PR that changes non-doc/non-test source should also add an
# entry under CHANGELOG.md's [Unreleased] section.
#
# This is intentionally ADVISORY (warns, never blocks the merge or ci-gate) —
# nisshi has no release-time changelog gate to fall back on, so treat this as
# a reminder, not enforcement. Set CHANGELOG_GATE_MODE=blocking to fail
# instead of warn.
#
# Env:
#   BASE_SHA             base commit of the PR (defaults to origin/main)
#   HEAD_SHA             head commit of the PR (defaults to HEAD)
#   LABELS               comma-joined PR label names
#   CHANGELOG_GATE_MODE  advisory (default) | blocking
#
set -euo pipefail

BASE="${BASE_SHA:-origin/main}"
HEAD="${HEAD_SHA:-HEAD}"

# Escape hatch — pure refactor/test/docs PRs can opt out with a label.
case ",${LABELS:-}," in
  *,no-changelog,*)
    echo "no-changelog label present — skipping CHANGELOG gate."
    exit 0
    ;;
esac

# advisory (default) warns without blocking; blocking fails the PR. Decide once
# so every non-OK exit below honors the mode.
MODE="${CHANGELOG_GATE_MODE:-advisory}"
if [ "$MODE" = "blocking" ]; then LEVEL="::error::"; EC=1; else LEVEL="::warning::"; EC=0; fi

# Diff from the merge-base, not the base tip: if main advances while the PR is
# open, base.sha moves forward, and a two-dot diff would misattribute unrelated
# commits. The merge-base isolates this PR's own changes.
MERGE_BASE=$(git merge-base "$BASE" "$HEAD") || {
  echo "${LEVEL}Could not compute a merge-base for '$BASE' and '$HEAD' — the branch shares no common ancestor with the base (force-pushed base or shallow clone?). Skipping the CHANGELOG check." >&2
  exit "$EC"
}

# Files that never need a CHANGELOG entry on their own: docs, generated/CI-only
# changes, and test-only files. Everything else (crate `src/`, `build.rs`,
# `Cargo.toml`/`Cargo.lock`, protocol descriptors, etc.) counts as source.
NON_SOURCE_RE='(\.md$|^\.github/|^docs/|(^|/)tests/)'

changed=$(git diff --name-only "$MERGE_BASE" "$HEAD")
surface_hits=$(grep -Ev "$NON_SOURCE_RE" <<<"$changed" || true)

if [ -z "$surface_hits" ]; then
  echo "No non-doc/non-test source changed — CHANGELOG entry not required."
  exit 0
fi

# Content of the [Unreleased] section at a given ref (empty if the file/section
# is absent). Stops at the next top-level version header.
extract_unreleased() {
  # || true so a missing CHANGELOG.md at this ref yields empty output rather than
  # tripping `set -o pipefail` (the section is then correctly treated as absent).
  { git show "$1:CHANGELOG.md" 2>/dev/null || true; } | awk '
    /^## \[Unreleased\]/ { p = 1; next }
    p && /^## \[/        { exit }
    p                    { print }
  '
}

if [ "$(extract_unreleased "$MERGE_BASE")" = "$(extract_unreleased "$HEAD")" ]; then
  echo "${LEVEL}This PR changes source but CHANGELOG.md's [Unreleased] section is unchanged."
  echo "Triggered by:"
  sed 's/^/  - /' <<<"$surface_hits"
  echo
  echo "Add an entry under [Unreleased] and push again, or apply the 'no-changelog'"
  echo "label (takes effect on the next push) if this change has no user- or"
  echo "operator-visible surface (pure test/refactor/docs)."
  exit "$EC"
fi

echo "CHANGELOG [Unreleased] updated — OK."
