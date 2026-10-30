#!/usr/bin/env bash
#
# Fails when a build with one storage engine's cargo feature compiles a
# database client that belongs to another engine. ci.yml's build-storage job
# runs it once per engine, so a feature that turns on another engine's client
# again (as nisshi-storage-sql once did for libsql and tokio-postgres) fails
# CI instead of quietly lengthening every single-engine build.
#
# Reads the package list on stdin, one package per line, as printed by:
#
#   cargo tree -p nisshi --no-default-features --features <feature> \
#     -e normal,build --prefix none -f '{p}'
#
# The script also fails when the list is empty or has no `nisshi` package,
# or when no forbidden crate is given, because a check that compares nothing
# would pass.
#
# Usage: check-storage-deps.sh <feature> <forbidden-crate>...
# Local: just check-storage-deps
# Test:  .github/scripts/check-storage-deps.test.sh
#
set -euo pipefail

FEATURE="${1:?usage: check-storage-deps.sh <feature> <forbidden-crate>...}"
shift

if [ "$#" -eq 0 ]; then
  echo "::error::No forbidden crate given for --features ${FEATURE}."
  exit 1
fi
for crate in "$@"; do
  # A crate name is matched as part of a regular expression below, so only
  # characters that a crate name can hold are accepted.
  if ! grep -q -x -E '[A-Za-z0-9_-]+' <<<"$crate"; then
    echo "::error::'${crate}' is not a crate name."
    exit 1
  fi
done

# cargo tree prints LF, but a list saved on Windows can end lines in CRLF.
PACKAGES=$(tr -d '\r')
if [ -z "$PACKAGES" ]; then
  echo "::error::The package list for --features ${FEATURE} is empty."
  exit 1
fi
# If cargo tree's output format changes, no line would match a forbidden
# crate and the check would pass, so the root package must be found first.
if ! grep -q -E '^nisshi v[0-9]' <<<"$PACKAGES"; then
  echo "::error::The package list for --features ${FEATURE} has no 'nisshi v<version>' line. Is it the output of cargo tree --prefix none -f '{p}' for -p nisshi?"
  exit 1
fi

# `^<name> v<digit>` so that tokio-postgres doesn't match
# tokio-postgres-rustls, and libsql doesn't match libsql-sys.
found=0
for crate in "$@"; do
  version=$(grep -m 1 -o -E "^${crate} v[0-9][^ ]*" <<<"$PACKAGES" || true)
  if [ -n "$version" ]; then
    echo "::error::--features ${FEATURE} compiles ${version}, which belongs to another storage engine."
    found=$((found + 1))
  fi
done

if [ "$found" -gt 0 ]; then
  echo "::error::Find what pulls it in with: cargo tree -p nisshi --no-default-features --features ${FEATURE} -e normal,build -i <crate>"
  exit 1
fi
echo "--features ${FEATURE} compiles none of: $*"
