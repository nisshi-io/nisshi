#!/usr/bin/env bash
#
# Self-test for ci-changes.sh. The regression that matters is a Rust,
# Cargo or workflow change classified as skippable, which would let it
# reach main with no build or test on the PR.
#
# Run manually:  .github/scripts/ci-changes.test.sh
# Runs in CI in each `changes` job (base copy) and in workflow-lint.yml's
# actionlint job (head copy).
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
RESOLVE="${SCRIPT_DIR}/ci-changes.sh"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

pass_count=0
fail_count=0

# expect <case> <want> <path>... - feed the paths to the script, assert rust=<want>.
expect() {
  local case="$1" want="$2" got
  shift 2
  : > "$WORK/out"
  printf '%s\n' "$@" | GITHUB_OUTPUT="$WORK/out" bash "$RESOLVE" > /dev/null
  got=$(sed -n 's/^rust=//p' "$WORK/out")
  if [ "$got" = "$want" ]; then
    pass_count=$((pass_count + 1)); printf 'ok   - %s: rust=%s\n' "$case" "$want"
  else
    fail_count=$((fail_count + 1)); printf 'FAIL - %s: want [%s] got [%s]\n' "$case" "$want" "$got"
  fi
}

expect "other workflows and repo config" false \
  .github/workflows/tier-c.yml .github/workflows/pr-title.yml .github/dependabot.yml .github/CODEOWNERS
expect "docs only" false docs/sarama.md README.md CHANGELOG.md
expect "license files" false LICENSE NOTICE
expect "ci.yml" true .github/workflows/ci.yml
expect "dependencies.yml" true .github/workflows/dependencies.yml
expect "codeql.yml" true .github/workflows/codeql.yml
expect "compat report sql" true .github/workflows/scripts/pivot-integration-report.sql
expect "ci scripts" true .github/scripts/ci-changes.sh
expect "rust source" true nisshi-broker/src/lib.rs
expect "nested markdown in a crate" true nisshi-broker/README.md
expect "cargo files" true Cargo.toml
expect "deny config" true deny.toml
expect "mixed" true .github/dependabot.yml nisshi/src/main.rs
expect "empty list" true ""

printf '\n%d passed, %d failed\n' "$pass_count" "$fail_count"
[ "$fail_count" -eq 0 ]
