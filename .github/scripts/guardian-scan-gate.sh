#!/usr/bin/env bash
#
# Decides whether `Guardian scan gate`, the required check, passes. Kept out of
# the workflow so the verdict is testable (guardian-scan-gate.test.sh):
# `update-manifest` runs only when this passes, so a wrong pass moves the release
# manifest row forward with no scan behind it.
#
# Env (the workflow maps job results and setup outputs in):
#   R_SETUP, R_BUILD, R_FOSSA, R_PRISMA, R_GATE   needs.<job>.result
#   IS_FORK, IS_TRUNK, IS_QUEUE, IS_DEPENDABOT    setup outputs ("true" / "false")
#
# Exits 0 to pass, 1 to fail.
#
set -euo pipefail

# A failed setup leaves every output empty, which would otherwise read as "not a
# fork, not a queue entry" and report the skips below.
if [ "${R_SETUP:-}" != "success" ]; then
  echo "::error::setup = ${R_SETUP:-}"; exit 1
fi
if [ "${IS_FORK:-}" = "true" ]; then
  echo "::notice::Fork PR - scans skipped (no Environment secrets)."
  exit 0
fi
if [ "${IS_QUEUE:-}" = "true" ]; then
  echo "::notice::Merge queue entry - scans skipped (PR run already reported; trunk run enforces after merge)."
  exit 0
fi
if [ "${IS_DEPENDABOT:-}" = "true" ]; then
  echo "::notice::Dependabot PR - scans skipped (its run holds Dependabot secrets, not the Environment's; trunk run scans the bump after merge)."
  exit 0
fi
# Off trunk every guard is REPORT-only, so a failed FOSSA job can only be the
# scanner, its key or the network. That is a warning, not a red check: a required
# check that fails on an outage would stop every PR from entering the queue. The
# trunk run after the merge still enforces.
if [ "${IS_TRUNK:-}" != "true" ]; then
  if [ "${R_FOSSA:-}" != "success" ]; then
    echo "::warning::fossa = ${R_FOSSA:-} - report-only off trunk, so not blocking; see the FOSSA job log."
  fi
  echo "Guardian scan passed (report only)."
  exit 0
fi
# Trunk: every job must have succeeded. A skipped or cancelled job is a failure,
# not a pass, or a missing scan would let the manifest row advance.
fail=0
for pair in "build:${R_BUILD:-}" "fossa:${R_FOSSA:-}" "prisma:${R_PRISMA:-}" "guardian-gate:${R_GATE:-}"; do
  name="${pair%%:*}"; res="${pair#*:}"
  if [ "$res" != "success" ]; then echo "::error::$name = $res"; fail=1; fi
done
if [ "$fail" -eq 0 ]; then echo "Guardian scan passed."; else exit 1; fi
