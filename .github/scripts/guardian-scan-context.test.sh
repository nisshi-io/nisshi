#!/usr/bin/env bash
#
# Self-test for guardian-scan-context.sh. Runs the real script under each event
# shape and asserts the full output tuple, so the pr/queue/trunk classification
# is verified on every run rather than trusted. The dangerous regressions this
# pins: a queue entry, or a dispatch from a feature branch, falling through to
# trunk, which would upload scan results, run the Guardian DB sync + Jira
# report, and block on Guardian's verdict for a ref that isn't main; and a
# value the PR author chose reaching the shared actions' bash unfiltered.
#
# Run manually:  .github/scripts/guardian-scan-context.test.sh
# Runs in CI as a step of the `setup` job in .github/workflows/guardian-scan.yaml.
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
RESOLVE="${SCRIPT_DIR}/guardian-scan-context.sh"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

pass_count=0
fail_count=0
pass() { pass_count=$((pass_count + 1)); printf 'ok   - %s\n' "$1"; }
fail() { fail_count=$((fail_count + 1)); printf 'FAIL - %s\n' "$1"; [ -n "${2:-}" ] && printf '       %s\n' "$2"; return 0; }

# run - execute the script with the env given as KEY=VALUE args; set globals
# STDOUT and OUT (path to the captured $GITHUB_OUTPUT file). The script must
# exit 0 on every shape tested here; a crash is a failure in its own right,
# not just empty outputs downstream.
run() {
  OUT="$WORK/output.$RANDOM"
  : > "$OUT"
  local rc=0
  STDOUT=$(env -i PATH="$PATH" HOME="${HOME:-}" GITHUB_OUTPUT="$OUT" "$@" bash "$RESOLVE" 2>&1) || rc=$?
  if [ "$rc" -ne 0 ]; then fail "script exited $rc" "env: $* / stdout=[$STDOUT]"; fi
  return 0
}

# get <key> - value of <key> from the captured output file.
get() { sed -n "s/^$1=//p" "$OUT" | tail -n 1; }

# expect <case> <key> <want> - assert one key of the tuple.
expect() {
  local case="$1" key="$2" want="$3" got
  got=$(get "$key")
  if [ "$got" = "$want" ]; then pass "$case: $key=$want"; else fail "$case: $key" "want [$want] got [$got]"; fi
}

# --- Case 1: pull_request -> pr mode, nothing enforced ------------------------
run EVENT_NAME=pull_request HEAD_SHA=0123456789abcdef0123456789abcdef01234567 FULL_VERSION_INPUT=v1.2.3 \
    GITHUB_REF=refs/pull/7/merge IS_FORK=false
expect "pr" mode pr
expect "pr" is_pr true
expect "pr" is_trunk false
expect "pr" is_queue false
expect "pr" enforce false
expect "pr" fossa_mode REPORT
expect "pr" fossa_branch PR
expect "pr" fossa_revision 0123456789abcdef0123456789abcdef01234567

# --- Case 2: merge_group -> queue mode, NOT trunk ------------------------------
run EVENT_NAME=merge_group FULL_VERSION_INPUT=v1.2.3-4-gabc \
    GITHUB_REF=refs/heads/gh-readonly-queue/main/pr-9 IS_FORK=false
expect "queue" mode queue
expect "queue" is_pr false
expect "queue" is_trunk false
expect "queue" is_queue true
expect "queue" enforce false
expect "queue" fossa_mode REPORT
expect "queue" fossa_branch merge-queue
expect "queue" fossa_revision v1.2.3-4-gabc
if grep -q 'Resolved: mode=queue' <<<"$STDOUT"; then
  pass "queue: resolved mode is logged to stdout"
else
  fail "queue: resolved mode is logged to stdout" "stdout=[$STDOUT]"
fi

# --- Case 3: push -> trunk mode, enforcing -------------------------------------
run EVENT_NAME=push FULL_VERSION_INPUT=v1.2.3 \
    GITHUB_REF=refs/heads/main IS_FORK=false
expect "push" mode trunk
expect "push" is_trunk true
expect "push" is_queue false
expect "push" enforce true
expect "push" fossa_mode BLOCK
expect "push" fossa_branch main

# --- Case 4: workflow_dispatch is trunk only when it scans main or a tag ------
run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1.2.3 \
    GITHUB_REF=refs/heads/main IS_FORK=false
expect "dispatch-main" mode trunk
expect "dispatch-main" enforce true
run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1.2.3 \
    GITHUB_REF=refs/heads/main GIT_REF_INPUT=v0.7.0-pre.2 IS_FORK=false
expect "dispatch-tag" mode trunk
expect "dispatch-tag" fossa_branch main
# Dispatched from a feature branch, or from main with `git_ref` set to one.
run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1.2.3-4-gabc \
    GITHUB_REF=refs/heads/feature/x IS_FORK=false
expect "dispatch-branch" mode branch
expect "dispatch-branch" is_trunk false
expect "dispatch-branch" enforce false
expect "dispatch-branch" fossa_mode REPORT
expect "dispatch-branch" fossa_branch feature/x
expect "dispatch-branch" fossa_revision v1.2.3-4-gabc
run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1.2.3 \
    GITHUB_REF=refs/heads/main GIT_REF_INPUT=maintenance IS_FORK=false
expect "dispatch-input-branch" mode branch
expect "dispatch-input-branch" fossa_branch maintenance

# --- Case 5: a Dependabot PR skips the scans; other runs scan ------------------
# Its run holds Dependabot's secrets, not the Environment's. Only a PR run
# skips: a push by the same actor would be trunk and must scan.
run EVENT_NAME=pull_request HEAD_SHA=0123456789abcdef0123456789abcdef01234567 FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/pull/7/merge IS_FORK=false ACTOR='dependabot[bot]'
expect "dependabot" mode pr
expect "dependabot" is_dependabot true
expect "dependabot" scan false
run EVENT_NAME=push FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main IS_FORK=false ACTOR='dependabot[bot]'
expect "dependabot-push" is_dependabot false
expect "dependabot-push" scan true
run EVENT_NAME=pull_request HEAD_SHA=0123456789abcdef0123456789abcdef01234567 FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/pull/7/merge IS_FORK=false ACTOR=someone
expect "pr-human" scan true
run EVENT_NAME=pull_request HEAD_SHA=0123456789abcdef0123456789abcdef01234567 FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/pull/7/merge IS_FORK=true
expect "pr-fork" scan false
run EVENT_NAME=merge_group FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/gh-readonly-queue/main/pr-9 IS_FORK=false
expect "queue-scan" scan false

# --- Case 5a: FOSSA_LICENSING_BLOCK turns off only the licensing block ---------
# Exactly "false" makes licensing REPORT on trunk while enforce (the Guardian
# gate) stays on; any other value fails closed.
run EVENT_NAME=push FULL_VERSION_INPUT=v1.2.3 \
    GITHUB_REF=refs/heads/main IS_FORK=false FOSSA_LICENSING_BLOCK=false
expect "licensing-off" mode trunk
expect "licensing-off" enforce true
expect "licensing-off" fossa_mode REPORT
case "$STDOUT" in
  *"::warning::FOSSA licensing guard is REPORT-only"*) pass "licensing-off: warns" ;;
  *) fail "licensing-off: warns" "stdout=[$STDOUT]" ;;
esac
for value in true no FALSE; do
  run EVENT_NAME=push FULL_VERSION_INPUT=v1.2.3 \
      GITHUB_REF=refs/heads/main IS_FORK=false FOSSA_LICENSING_BLOCK="$value"
  expect "licensing-$value" fossa_mode BLOCK
done
run EVENT_NAME=pull_request HEAD_SHA=0123456789abcdef0123456789abcdef01234567 FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/pull/7/merge IS_FORK=false FOSSA_LICENSING_BLOCK=false
expect "licensing-off-pr" enforce false
expect "licensing-off-pr" fossa_mode REPORT

# --- Case 6: ref precedence - explicit input wins, else github.ref -------------
run EVENT_NAME=push FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main \
    GIT_REF_INPUT=refs/tags/v9.9.9 IS_FORK=false
expect "ref-input" ref refs/tags/v9.9.9
expect "ref-input" mode trunk
expect "ref-input" enforce true
run EVENT_NAME=push FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main \
    GIT_REF_INPUT="" IS_FORK=false
expect "ref-fallback" ref refs/heads/main

# --- Case 6a: the edges of the trunk regex ---------------------------------------
# A qualified tag is trunk; a branch that merely contains `main` or a version
# is not. These pin the anchors and the optional `refs/tags/` prefix.
for ref in refs/tags/v1.2.3 v1.2.3 refs/tags/v0.7.0-pre.2 main refs/heads/main; do
  run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main GIT_REF_INPUT="$ref" IS_FORK=false
  expect "trunk-ref $ref" mode trunk
  expect "trunk-ref $ref" enforce true
done
for ref in refs/heads/feature/main refs/heads/release/v1.2.3 refs/heads/v1.2.3 refs/heads/main2 \
           refs/tags/v1.2 refs/tags/release-v1.2.3 xmain refs/heads/mainline; do
  run EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main GIT_REF_INPUT="$ref" IS_FORK=false
  expect "branch-ref $ref" mode branch
  expect "branch-ref $ref" enforce false
done

# --- Case 6b: sha is the checked-out commit -----------------------------------
run EVENT_NAME=push FULL_VERSION_INPUT=v1 GITHUB_REF=refs/heads/main IS_FORK=false
expect "sha" sha "$(git -C "$SCRIPT_DIR" rev-parse HEAD)"

# --- Case 7: FULL_VERSION_INPUT is used verbatim; empty falls back to describe -
run EVENT_NAME=push FULL_VERSION_INPUT=7.7.7-custom \
    GITHUB_REF=refs/heads/main IS_FORK=false
expect "version-input" full_version 7.7.7-custom
expect "version-input" fossa_revision 7.7.7-custom
# Runs `git describe` for real, so this case needs the repo checkout (CI uses
# fetch-depth: 0). Assert non-empty rather than a value that moves per commit.
run EVENT_NAME=push FULL_VERSION_INPUT="" \
    GITHUB_REF=refs/heads/main IS_FORK=false
# `v*` only: the repo also carries `tansu-*` and bare `0.x` tags.
case "$(get full_version)" in
  v*) pass "version-describe: git describe fallback picks a v* tag" ;;
  *) fail "version-describe: git describe fallback picks a v* tag" "got [$(get full_version)] stdout=[$STDOUT]" ;;
esac

# --- Case 8: is_fork passes through untouched -----------------------------------
run EVENT_NAME=pull_request HEAD_SHA=0123456789abcdef0123456789abcdef01234567 FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/pull/7/merge IS_FORK=true
expect "fork" is_fork true

# --- Case 9: the PR revision is the head SHA, never the branch name ---------
# shellcheck disable=SC2016 # the literal `$(id)` is the point
run EVENT_NAME=pull_request HEAD_REF='x";$(id)' HEAD_SHA=0123456789abcdef0123456789abcdef01234567 \
    FULL_VERSION_INPUT=v1 GITHUB_REF=refs/pull/7/merge IS_FORK=false
expect "hostile-ref" fossa_revision 0123456789abcdef0123456789abcdef01234567

# --- Case 10: hostile or malformed inputs are rejected, not passed through ----
# run_fails - like run, but the script must exit non-zero.
run_fails() {
  OUT="$WORK/output.$RANDOM"
  : > "$OUT"
  if STDOUT=$(env -i PATH="$PATH" HOME="${HOME:-}" GITHUB_OUTPUT="$OUT" "$@" bash "$RESOLVE" 2>&1); then
    fail "script accepted a hostile input" "env: $* / stdout=[$STDOUT]"
  else
    pass "hostile input rejected: $*"
  fi
}
# shellcheck disable=SC2016 # the literal `$(id)` is the point
run_fails EVENT_NAME=push FULL_VERSION_INPUT='v1";$(id)' \
    GITHUB_REF=refs/heads/main IS_FORK=false
run_fails EVENT_NAME=push FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/heads/main GIT_REF_INPUT=$'main\nenforce=false' IS_FORK=false
# shellcheck disable=SC2016 # the literal `$(id)` is the point
run_fails EVENT_NAME=workflow_dispatch FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/heads/main GIT_REF_INPUT='x";$(id)' IS_FORK=false
run_fails EVENT_NAME=pull_request HEAD_SHA=abc FULL_VERSION_INPUT=v1 \
    GITHUB_REF=refs/pull/7/merge IS_FORK=false

printf '\n%d passed, %d failed\n' "$pass_count" "$fail_count"
[ "$fail_count" -eq 0 ]
