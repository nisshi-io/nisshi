# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- A listener with SASL configured closes a connection that sends a frame larger than 512KiB before the client authenticates, matching the Apache Kafka default for `sasl.server.max.receive.size`. The same limit applies while a client re-authenticates. The broker logs this rejection as `PreAuthenticationFrameTooBig`, and counts it in `nisshi_frames_rejected`.

### Security

- Decoding a produce batch is bounded. Previously a few KB of compressed
  data could decompress into gigabytes, and a record carrying millions of
  empty headers expanded 32x on decode. Now a batch is rejected with
  `MESSAGE_TOO_LARGE` once its decompressed bytes, or its decoded records
  and headers, pass a 100 MiB budget, or up front when its `record_count`
  alone would. A producer can recover by splitting the batch (the Java
  producer does this on its own for a batch of more than one record).
- Peak memory while decoding one batch is a small multiple of that budget
  (roughly 2 to 3x), not 100 MiB exactly:
  - an uncompressed record's headers are charged after they are allocated,
    so the record that crosses the budget can add up to about 100 MiB more;
  - a Snappy batch holds its decompressed block (up to 100 MiB) alongside
    the records decoded from it;
  - zstd's decoder window, up to 128 MiB, sits outside the budget.
  The budget applies per batch; concurrent batches each have their own.

### Fixed

- A Snappy batch with a truncated xerial header is rejected with an error
  instead of panicking the decoder.
- SlateDB compaction skips a stored batch it cannot inflate, with a warning,
  instead of abandoning the whole maintenance pass.
- A produced batch whose records do not decode now gets `INVALID_RECORD` instead of `UNKNOWN_SERVER_ERROR`. PostgreSQL and SQLite always decode a produced batch; S3, memory and SlateDB decode it only when a schema registry or data lake is configured, and otherwise store it as sent. A batch with an unknown compression codec id gets `INVALID_RECORD` on every backend. A batch over the decoded-size limit still gets `MESSAGE_TOO_LARGE`.
- A Produce request with other than exactly one record batch for a partition is now rejected with `INVALID_RECORD` before any batch is written, as Kafka does. Before, each batch was stored in turn, so a failure on a later batch reported the whole partition as failed while the earlier batches stayed in the log.
- On S3 and memory storage, an idempotent batch rejected for a decode or schema failure no longer advances the producer's sequence. Before, the producer's next batch could get `DUPLICATE_SEQUENCE_NUMBER`, which a client reports as success, and that batch was lost.
