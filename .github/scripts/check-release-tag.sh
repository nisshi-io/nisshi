#!/usr/bin/env bash
#
# Checks a release tag against the commit it tags, in the current
# directory's Cargo.toml and CHANGELOG.md. release.yml's `release-tag` job
# runs it, and publish-image needs that job, so a tag that fails publishes
# nothing. It publishes nothing itself.
#
# - Every tag: the version (the tag without its `v`) must equal
#   `[workspace.package] version` in Cargo.toml. publish-crates publishes
#   the Cargo.toml version, and it runs last, so without this check a
#   mismatched tag fails only after the image and the GitHub release are out.
# - A stable tag (no `-`): CHANGELOG.md must have a `## [X.Y.Z]` section
#   with at least one `- ` entry. The script prints the section. A
#   pre-release tag (v0.8.0-pre.1) needs no section, because the next
#   stable section covers everything since the previous stable tag.
#
# Usage: check-release-tag.sh <tag>
# Test:  .github/scripts/check-release-tag.test.sh
#
set -euo pipefail

TAG="${1:?usage: check-release-tag.sh <tag>}"
VERSION="${TAG#v}"

# refuse <cause> - print the cause and what to do as workflow errors, then
# fail. The job runs before anything is published, and a tag is never moved,
# so the fix is a new version.
refuse() {
  echo "::error::$1"
  echo "::error::Nothing was published for ${TAG}. Fix main, then release the next version with /cut-release."
  exit 1
}

# The version goes into an awk regex below, so it must be plain SemVer:
# digits and dots, then an optional pre-release suffix.
if [ "$VERSION" = "$TAG" ] || ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  refuse "${TAG} is not a release tag of the form vX.Y.Z or vX.Y.Z-<pre-release>."
fi

CRATE_VERSION=$(awk '
  /^\[/ { section = $0; next }
  section == "[workspace.package]" && /^version *=/ { gsub(/^version *= *"|".*$/, ""); print; exit }
' Cargo.toml)

if [ "$CRATE_VERSION" != "$VERSION" ]; then
  refuse "${TAG} tags a commit whose Cargo.toml [workspace.package] version is '${CRATE_VERSION}', not ${VERSION}."
fi
echo "Cargo.toml version is ${VERSION}"

case "$VERSION" in
  *-*)
    echo "${TAG} is a pre-release, which needs no CHANGELOG.md section"
    exit 0
    ;;
esac

# The section ends at the next `## [` heading. The last section ends at the
# first link reference definition (`[0.7.0]: https://...`) instead.
SECTION=$(awk -v v="$VERSION" '
  BEGIN { gsub(/\./, "\\.", v) }
  $0 ~ ("^## \\[" v "\\]") { p = 1; print; next }
  p && (/^## \[/ || /^\[[^]]+\]: /) { exit }
  p { print }
' CHANGELOG.md)

if [ -z "$SECTION" ]; then
  refuse "CHANGELOG.md has no '## [${VERSION}]' section, which the stable tag ${TAG} needs."
fi

# A herestring, not `printf | grep -q`: grep -q exits on the first match, so
# with pipefail a section larger than the pipe buffer fails on SIGPIPE.
if ! grep -q '^- ' <<<"$SECTION"; then
  refuse "The CHANGELOG.md '## [${VERSION}]' section has no '- ' entry."
fi

printf '%s\n' "$SECTION"
