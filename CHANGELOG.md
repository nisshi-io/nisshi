# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `DeleteRecords` is implemented on the libSQL, Turso and DynoStore storage
  engines instead of panicking the connection task, and the PostgreSQL
  implementation no longer fails at SQL-prepare time (it referenced columns
  that don't exist in the current schema). All five storage engines
  (PostgreSQL, libSQL, Turso, DynoStore, SlateDB) now validate the requested
  offset identically: `-1` deletes up to the high watermark, an offset above
  the high watermark or any other negative offset is rejected with
  `OFFSET_OUT_OF_RANGE`, and an offset at or below the current log start is a
  no-op. The record or batch holding the last committed offset is never
  physically removed, even when deleting everything, so `ListOffsets(Latest)`
  is unaffected by a `DeleteRecords` call. An unknown topic or an
  out-of-range partition is reported per-partition and never fails a sibling
  partition in the same request.
