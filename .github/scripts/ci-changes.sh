#!/usr/bin/env bash
#
# Decides whether a pull request needs the Rust jobs in ci.yml,
# dependencies.yml and codeql.yml. Reads the PR's changed paths, one per
# line, on stdin and writes `rust=true` or `rust=false` to $GITHUB_OUTPUT
# (stdout when unset).
#
# `rust=false` only when every path is on the skip list below: CI config
# that none of those three workflows reads, and prose. Anything else,
# including an empty list, means `rust=true`, so an unexpected path runs
# the full suite rather than skipping it. The three workflows themselves,
# and the scripts and SQL their jobs run, are excluded from the skip list so
# a change to them is exercised by the jobs that use them.
#
# The `changes` jobs run the PR base's copy of this script, after its
# ci-changes.test.sh. workflow-lint.yml runs the PR head's test.
#
set -euo pipefail

skippable() {
  case "$1" in
    .github/workflows/ci.yml | .github/workflows/dependencies.yml | .github/workflows/codeql.yml) return 1 ;;
    .github/workflows/scripts/* | .github/scripts/*) return 1 ;;
    .github/*) return 0 ;;
    docs/*.md) return 0 ;;
    */*) return 1 ;;
    *.md | LICENSE | NOTICE) return 0 ;;
    *) return 1 ;;
  esac
}

rust=false
seen=0
while IFS= read -r path || [ -n "$path" ]; do
  [ -n "$path" ] || continue
  seen=$((seen + 1))
  if ! skippable "$path"; then
    rust=true
    echo "runs Rust jobs: $path"
    break
  fi
done
[ "$seen" -gt 0 ] || { rust=true; echo "no changed paths listed, running everything"; }

echo "rust=$rust" >> "${GITHUB_OUTPUT:-/dev/stdout}"
