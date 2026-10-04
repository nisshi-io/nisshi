# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- The S3/memory (DynoStore) storage engine now stores each consumer group id
  as a single, opaque, percent-encoded path segment, instead of interpolating
  it directly into an object store key. This is a breaking change for groups
  whose id contains a character `object_store` treats as reserved (`/`,
  control characters, and about twenty others), is exactly `.` or `..`, or is
  the empty string `""`: such a group's committed offsets and group state,
  if any exist under the old key layout, are orphaned (not deleted, not
  overwritten) and are no longer found under the new layout. The group's
  next fetch falls back to `-1`/`auto.offset.reset`, the same observable
  behavior as any ordinary offset-retention expiry. There is no migration.
  Group ids without any of these characters are unaffected.
