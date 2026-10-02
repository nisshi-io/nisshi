# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Fetch answers `OFFSET_OUT_OF_RANGE` for a fetch offset below the partition's log start offset or above its high watermark, instead of passing the offset to storage. A Fetch with a partition error is answered immediately rather than after `max_wait`.
- Fetch no longer sends a leader hint with `UNKNOWN_TOPIC_OR_PARTITION` for an unknown topic id. Kafka sends one only with a leadership error, and librdkafka's consumer close hung or crashed on a deleted topic once that response came back without waiting.
