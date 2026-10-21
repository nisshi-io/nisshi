#!/usr/bin/env bash
#
# Table test for guardian-scan-gate.sh: every row is one run shape and the
# expected verdict. The rows that matter most are the trunk ones, where a skipped
# or cancelled job must fail the gate so the manifest row doesn't move without a scan.
#
# Run manually:  .github/scripts/guardian-scan-gate.test.sh
# Runs in CI as a step of the `setup` job in .github/workflows/guardian-scan.yaml.
#
set -euo pipefail

GATE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/guardian-scan-gate.sh"

pass_count=0
fail_count=0

# row <name> <want: pass|fail> <setup> <fork> <trunk> <queue> <dependabot> <build> <fossa> <prisma> <guardian-gate>
row() {
  local name="$1" want="$2" rc=0 out got
  out=$(env -i PATH="$PATH" R_SETUP="$3" IS_FORK="$4" IS_TRUNK="$5" IS_QUEUE="$6" IS_DEPENDABOT="$7" \
    R_BUILD="$8" R_FOSSA="$9" R_PRISMA="${10}" R_GATE="${11}" bash "$GATE" 2>&1) || rc=$?
  if [ "$rc" -eq 0 ]; then got=pass; else got=fail; fi
  if [ "$got" = "$want" ]; then
    pass_count=$((pass_count + 1)); printf 'ok   - %s\n' "$name"
  else
    fail_count=$((fail_count + 1)); printf 'FAIL - %s: want %s got %s\n       %s\n' "$name" "$want" "$got" "$out"
  fi
}

S=success K=skipped C=cancelled F=failure
#   name                          want  setup fork    trunk   queue   dep     build fossa prisma gate
row "fork PR, scans skipped"      pass  $S    true    false   false   false   $K    $K    $K     $K
row "merge queue entry"           pass  $S    false   false   true    false   $K    $K    $K     $K
row "dependabot PR"               pass  $S    false   false   false   true    $K    $K    $K     $K
row "PR, fossa ok"                pass  $S    false   false   false   false   $K    $S    $K     $K
row "PR, fossa failed"            pass  $S    false   false   false   false   $K    $F    $K     $K
row "PR, fossa cancelled"         pass  $S    false   false   false   false   $K    $C    $K     $K
row "branch dispatch, fossa failed" pass $S   false   false   false   false   $K    $F    $K     $K
row "setup failed"                fail  $F    ""      ""      ""      ""      $K    $K    $K     $K
row "setup cancelled"             fail  $C    ""      ""      ""      ""      $K    $K    $K     $K
row "setup skipped"               fail  $K    ""      ""      ""      ""      $K    $K    $K     $K
row "setup failed, fork output"   fail  $F    true    false   false   false   $K    $K    $K     $K
row "trunk all green"             pass  $S    false   true    false   false   $S    $S    $S     $S
for job in build fossa prisma gate; do
  for res in $K $C $F; do
    b=$S f=$S p=$S g=$S
    case "$job" in build) b=$res ;; fossa) f=$res ;; prisma) p=$res ;; gate) g=$res ;; esac
    row "trunk, $job $res"        fail  $S    false   true    false   false   "$b"  "$f"  "$p"   "$g"
  done
done

printf '\n%d passed, %d failed\n' "$pass_count" "$fail_count"
[ "$fail_count" -eq 0 ]
