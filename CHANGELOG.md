# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- A listener with SASL configured closes a connection that sends a frame larger than 512KiB before the client authenticates, matching the Apache Kafka default for `sasl.server.max.receive.size`. The same limit applies while a client re-authenticates. The broker logs this rejection as `PreAuthenticationFrameTooBig`, and counts it in `nisshi_frames_rejected`.

### Security

- A produce batch is limited to 100 MiB of decoded memory: its decompressed
  bytes plus the per-record and per-header struct cost. Previously a few KB
  of compressed data could decompress into gigabytes, and a record carrying
  millions of empty headers expanded 32x on decode. A batch over the limit,
  or one whose `record_count` alone would exceed it, is rejected with
  `MESSAGE_TOO_LARGE`, which a producer can recover from by splitting the
  batch (the Java producer does this on its own for a batch of more than one
  record).

### Fixed

- A Snappy batch with a truncated xerial header is rejected with an error
  instead of panicking the decoder.
- SlateDB compaction skips a stored batch it cannot inflate, with a warning,
  instead of abandoning the whole maintenance pass.
