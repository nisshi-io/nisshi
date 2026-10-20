#!/usr/bin/env bash
#
# Self-test for check-release-tag.sh. The regressions that matter are a tag
# that passes with the wrong Cargo.toml version, and a stable tag that passes
# with a missing or empty CHANGELOG.md section: either one publishes a
# release that is broken or has no changelog.
#
# Run manually:  .github/scripts/check-release-tag.test.sh
# Runs in CI in workflow-lint.yml's actionlint job.
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CHECK="${SCRIPT_DIR}/check-release-tag.sh"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

pass_count=0
fail_count=0

# fixture <name> <workspace version> - make a directory whose Cargo.toml has
# that version and whose CHANGELOG.md is read from stdin; print its path.
fixture() {
  local dir="$WORK/$1"
  mkdir -p "$dir"
  printf '[workspace]\nmembers = ["a"]\n\n[workspace.package]\nedition = "2024"\nversion = "%s"\n\n[workspace.dependencies]\na = { version = "%s", path = "a" }\n' "$2" "$2" > "$dir/Cargo.toml"
  cat > "$dir/CHANGELOG.md"
  printf '%s' "$dir"
}

# expect <case> <want: pass|fail> <dir> <tag> <pattern> [absent pattern] -
# run the script in <dir>, assert its exit status, and assert that its output
# matches <pattern> and does not match <absent pattern>. A failing case
# names its error text, so a case that fails for another reason fails here.
expect() {
  local case="$1" want="$2" dir="$3" tag="$4" present="$5" absent="${6:-}" got=pass
  ( cd "$dir" && bash "$CHECK" "$tag" > out.log 2>&1 ) || got=fail
  # grep reads the log file, not `printf | grep -q`, because the
  # large-section case would fail on SIGPIPE.
  if ! grep -q -e "$present" "$dir/out.log"; then got="$got without [$present]"; fi
  if [ -n "$absent" ] && grep -q -e "$absent" "$dir/out.log"; then got="$got with [$absent]"; fi
  if [ "$got" = "$want" ]; then
    pass_count=$((pass_count + 1)); printf 'ok   - %s: %s\n' "$case" "$want"
  else
    fail_count=$((fail_count + 1)); printf 'FAIL - %s: want [%s] got [%s]\n' "$case" "$want" "$got"
    sed 's/^/       /' "$dir/out.log"
  fi
}

d=$(fixture valid 0.7.0 <<'EOF'
# Changelog

## [0.7.0] - 2026-01-02

### Added

- seven entry.

## [0.6.0] - 2026-01-01

- six entry.

[0.7.0]: https://example.com/compare/v0.6.0...v0.7.0
EOF
)
expect "valid section" pass "$d" v0.7.0 'seven entry' 'six entry'
expect "tag without v" fail "$d" 0.7.0 'is not a release tag'
expect "tag that is not a version" fail "$d" vlatest 'is not a release tag'
expect "tag on another version" fail "$d" v0.5.0 "version is '0.7.0', not 0.5.0"

d=$(fixture missing 0.8.0 < "$WORK/valid/CHANGELOG.md")
expect "missing section" fail "$d" v0.8.0 "has no '## \\[0.8.0\\]' section"

d=$(fixture last 0.6.0 < "$WORK/valid/CHANGELOG.md")
expect "last section stops at the link references" pass "$d" v0.6.0 'six entry' 'example.com'

d=$(fixture headingonly 0.8.0 <<'EOF'
# Changelog

## [0.8.0] - 2026-01-03

### Added

## [0.7.0] - 2026-01-02

- seven entry.
EOF
)
expect "heading-only section" fail "$d" v0.8.0 'section has no'

# The heading `## [0.1.1]` is a prefix of `## [0.1.10]` without its `]`.
d=$(fixture prefix 0.1.1 <<'EOF'
# Changelog

## [0.1.10] - 2026-01-03

- ten entry.

## [0.1.1] - 2026-01-02

## [0.1.0] - 2026-01-01

- zero entry.
EOF
)
expect "0.1.1 does not match 0.1.10" fail "$d" v0.1.1 'section has no'

d=$(fixture mismatch 0.7.0-pre.3 < "$WORK/valid/CHANGELOG.md")
expect "stable tag on a pre-release version" fail "$d" v0.7.0 "version is '0.7.0-pre.3'"

d=$(fixture pre 0.8.0-pre.1 < "$WORK/valid/CHANGELOG.md")
expect "pre-release without a section" pass "$d" v0.8.0-pre.1 'pre-release'
expect "pre-release tag on another version" fail "$d" v0.8.0-pre.2 "not 0.8.0-pre.2"

# A [package] version above [workspace.package] is not the workspace version.
d=$(fixture package 0.7.0 < "$WORK/valid/CHANGELOG.md")
printf '[package]\nversion = "9.9.9"\n\n%s\n' "$(cat "$d/Cargo.toml")" > "$d/Cargo.toml"
expect "reads [workspace.package], not [package]" pass "$d" v0.7.0 'seven entry'
expect "ignores the [package] version" fail "$d" v9.9.9 "version is '0.7.0'"

# Regression for the `printf | grep -q` SIGPIPE under pipefail, which failed
# a valid section larger than the pipe buffer (~64KB).
d="$WORK/large"
mkdir -p "$d"
cp "$WORK/valid/Cargo.toml" "$d/Cargo.toml"
{ printf '# Changelog\n\n## [0.7.0] - 2026-01-02\n\n- early entry.\n'
  head -c 200000 /dev/zero | tr '\0' 'x' | fold -w 120
  printf '\n## [0.6.0] - 2026-01-01\n\n- six entry.\n'; } > "$d/CHANGELOG.md"
expect "section larger than the pipe buffer" pass "$d" v0.7.0 'early entry' 'six entry'

printf '\n%d passed, %d failed\n' "$pass_count" "$fail_count"
[ "$fail_count" -eq 0 ]
