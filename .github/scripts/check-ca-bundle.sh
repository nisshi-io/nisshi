#!/usr/bin/env bash
#
# Fails when a PEM CA bundle contains a certificate whose SHA-256
# fingerprint is on the distrust list. ci.yml's image-ca-certificates job
# runs it on the container image's /etc/ssl, so a base image update that
# brings a distrusted root back fails CI before the image is published.
#
# The script also fails when the bundle is missing or holds no certificate,
# or when the list holds no fingerprint, because a check that compares
# nothing would pass.
#
# Usage: check-ca-bundle.sh <bundle> [list]
#        list defaults to distrusted-roots.txt next to this script.
# Test:  .github/scripts/check-ca-bundle.test.sh
#
set -euo pipefail

BUNDLE="${1:?usage: check-ca-bundle.sh <bundle> [list]}"
LIST="${2:-$(dirname "${BASH_SOURCE[0]}")/distrusted-roots.txt}"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

if [ ! -s "$BUNDLE" ]; then
  echo "::error::CA bundle ${BUNDLE} is missing or empty."
  exit 1
fi

# OpenSSL and LibreSSL print the fingerprint in upper case, but a list
# entry copied from elsewhere can be lower case or end in CRLF.
DISTRUSTED=$(tr -d '\r' < "$LIST" | grep -v -E '^[[:space:]]*(#|$)' | tr '[:lower:]' '[:upper:]' | tr -d '[:blank:]' || true)
if [ -z "$DISTRUSTED" ]; then
  echo "::error::Distrust list ${LIST} holds no fingerprint."
  exit 1
fi
# An entry in any other form can never match, so the check would pass
# without comparing it.
MALFORMED=$(grep -v -x -E '([0-9A-F]{2}:){31}[0-9A-F]{2}' <<<"$DISTRUSTED" || true)
if [ -n "$MALFORMED" ]; then
  while IFS= read -r entry; do
    echo "::error::Distrust list ${LIST} entry '${entry}' is not a SHA-256 fingerprint of the form AB:CD:...:EF."
  done <<<"$MALFORMED"
  exit 1
fi

# `openssl x509` reads only the first certificate of its input, so each
# certificate goes to a file of its own.
awk -v dir="$WORK" '
  /-----BEGIN CERTIFICATE-----/ { inside = 1; file = sprintf("%s/%05d.pem", dir, ++count) }
  inside { print > file }
  /-----END CERTIFICATE-----/ { inside = 0; close(file) }
' "$BUNDLE"

certificates=0
distrusted=0
for certificate in "$WORK"/*.pem; do
  [ -e "$certificate" ] || break
  certificates=$((certificates + 1))
  fingerprint=$(openssl x509 -in "$certificate" -noout -fingerprint -sha256)
  fingerprint=$(tr '[:lower:]' '[:upper:]' <<<"${fingerprint#*=}")
  if grep -q -x -F "$fingerprint" <<<"$DISTRUSTED"; then
    subject=$(openssl x509 -in "$certificate" -noout -subject)
    echo "::error::${BUNDLE} contains a distrusted CA certificate: ${subject#subject=} (SHA-256 ${fingerprint})"
    distrusted=$((distrusted + 1))
  fi
done

if [ "$certificates" -eq 0 ]; then
  echo "::error::CA bundle ${BUNDLE} holds no PEM certificate."
  exit 1
fi
if [ "$distrusted" -gt 0 ]; then
  echo "::error::${LIST} says why each CA is distrusted. If a base image update brought it back, keep the previous base image digest in the Dockerfile until a newer base image drops it."
  exit 1
fi
echo "Checked ${certificates} certificates in ${BUNDLE}: none is on the distrust list"
