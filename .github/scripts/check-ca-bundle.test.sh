#!/usr/bin/env bash
#
# Self-test for check-ca-bundle.sh. The regression that matters is a bundle
# with a distrusted root that passes, which would let a base image update
# publish an image that trusts it again. The fixtures are self-signed
# certificates made here, so the test does not depend on the real roots.
#
# Run manually:  .github/scripts/check-ca-bundle.test.sh
# Runs in CI in workflow-lint.yml's actionlint job.
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CHECK="${SCRIPT_DIR}/check-ca-bundle.sh"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

pass_count=0
fail_count=0

# make_certificate <name> - write a self-signed certificate with CN=<name> to $WORK/<name>.pem.
make_certificate() {
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 \
    -subj "/CN=$1" -keyout "$WORK/$1.key" -out "$WORK/$1.pem" 2> /dev/null
}

# expect <case> <want> <bundle> <list> [<text>] - run the check, assert it
# exits 0 (want=pass) or non-zero (want=fail), and that its output contains
# <text> when given.
expect() {
  local case="$1" want="$2" bundle="$3" list="$4" text="${5:-}" got=pass
  bash "$CHECK" "$bundle" "$list" > "$WORK/out" 2>&1 || got=fail
  if [ "$got" = "$want" ] && { [ -z "$text" ] || grep -q -F "$text" "$WORK/out"; }; then
    pass_count=$((pass_count + 1)); printf 'ok   - %s: %s\n' "$case" "$want"
  else
    fail_count=$((fail_count + 1)); printf 'FAIL - %s: want %s %s, got %s:\n' "$case" "$want" "$text" "$got"
    sed 's/^/       /' "$WORK/out"
  fi
}

make_certificate trusted-a
make_certificate trusted-b
make_certificate distrusted

fingerprint=$(openssl x509 -in "$WORK/distrusted.pem" -noout -fingerprint -sha256)
fingerprint="${fingerprint#*=}"

printf '# Distrusted\n%s\n' "$fingerprint" > "$WORK/list"
printf '\n  # comment\n\n%s\n\n# trailing comment\n' "$(tr '[:upper:]' '[:lower:]' <<<"$fingerprint")" > "$WORK/list-commented"
printf '# only a comment\n\n' > "$WORK/list-empty"
printf '# Distrusted\r\n%s\r\n' "$fingerprint" > "$WORK/list-crlf"
printf '%s # Distrusted\n' "$fingerprint" > "$WORK/list-inline-comment"
printf '%s\n' "${fingerprint%:*}" > "$WORK/list-truncated"
printf '%s\n' "$(openssl x509 -in "$WORK/distrusted.pem" -noout -fingerprint -sha1 | sed 's/^[^=]*=//')" > "$WORK/list-sha1"

cat "$WORK/trusted-a.pem" "$WORK/trusted-b.pem" > "$WORK/clean"
cat "$WORK/distrusted.pem" "$WORK/trusted-a.pem" "$WORK/trusted-b.pem" > "$WORK/listed-first"
cat "$WORK/trusted-a.pem" "$WORK/distrusted.pem" "$WORK/trusted-b.pem" > "$WORK/listed-middle"
cat "$WORK/trusted-a.pem" "$WORK/trusted-b.pem" "$WORK/distrusted.pem" > "$WORK/listed-last"
{ echo "# a label"; cat "$WORK/trusted-a.pem"; echo; echo "# another label"; cat "$WORK/trusted-b.pem"; } > "$WORK/clean-labelled"
: > "$WORK/empty"
echo "not a certificate" > "$WORK/no-certificate"

expect "clean bundle" pass "$WORK/clean" "$WORK/list" "Checked 2 certificates"
expect "clean bundle with text between certificates" pass "$WORK/clean-labelled" "$WORK/list" "Checked 2 certificates"
expect "listed certificate first" fail "$WORK/listed-first" "$WORK/list" "CN=distrusted"
expect "listed certificate in the middle" fail "$WORK/listed-middle" "$WORK/list" "CN=distrusted"
expect "listed certificate last" fail "$WORK/listed-last" "$WORK/list" "CN=distrusted"
expect "list with comments, blank lines and lower case" fail "$WORK/listed-middle" "$WORK/list-commented" "CN=distrusted"
expect "list with comments only" fail "$WORK/clean" "$WORK/list-empty" "holds no fingerprint"
expect "list with CRLF line endings" fail "$WORK/listed-middle" "$WORK/list-crlf" "CN=distrusted"
expect "list entry with an inline comment" fail "$WORK/clean" "$WORK/list-inline-comment" "is not a SHA-256 fingerprint"
expect "truncated list entry" fail "$WORK/clean" "$WORK/list-truncated" "is not a SHA-256 fingerprint"
expect "SHA-1 list entry" fail "$WORK/clean" "$WORK/list-sha1" "is not a SHA-256 fingerprint"
expect "empty bundle" fail "$WORK/empty" "$WORK/list" "missing or empty"
expect "missing bundle" fail "$WORK/does-not-exist" "$WORK/list" "missing or empty"
expect "bundle without a certificate" fail "$WORK/no-certificate" "$WORK/list" "holds no PEM certificate"
expect "repository list is not empty" pass "$WORK/clean" "${SCRIPT_DIR}/distrusted-roots.txt" "Checked 2 certificates"

printf '\n%d passed, %d failed\n' "$pass_count" "$fail_count"
[ "$fail_count" -eq 0 ]
