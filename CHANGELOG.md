# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Fetch answers `UNKNOWN_TOPIC_OR_PARTITION` immediately for a topic known only by name. Previously, pg, lite and slatedb closed the connection, and the in-memory backend silently waited out `max_wait` before answering `NONE` (and inserted a phantom, zeroed watermark entry for the nonexistent topic while doing so). This change carries, for the new code path, the long-poll early-break and the no-leader-hint fix also carried by SOL-155090 (#826); see that PR for the by-topic-id case this shares the mechanism with.
