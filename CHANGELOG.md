# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- `ApiVersions` advertises only the versions the broker implements: Produce 3-11, ListOffsets 0-6 and AddPartitionsToTxn 0-3. Clients that negotiate versions pick a supported one automatically.
- Java admin clients fail `OffsetSpec.maxTimestamp()`, `OffsetSpec.earliestLocal()` and `OffsetSpec.latestTiered()` before they send a request, because these need ListOffsets v7 or later.
- The broker no longer advertises the client telemetry APIs (`GetTelemetrySubscriptions`, `PushTelemetry`), so clients do not send telemetry requests to it.
- The broker closes the connection when a request uses a version outside the range it advertises for that API, as Apache Kafka does. An `ApiVersions` request at a newer version than the broker knows gets an `UNSUPPORTED_VERSION` reply at v0, so the client can retry at a supported version.
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

- A ListOffsets v0 response includes the requested offset in `OldStyleOffsets`. It was empty before.
- The broker no longer routes AddPartitionsToTxn v4 and later, which panicked every storage backend except SlateDB.
- A Snappy batch with a truncated xerial header is rejected with an error
  instead of panicking the decoder.
- SlateDB compaction skips a stored batch it cannot inflate, with a warning,
  instead of abandoning the whole maintenance pass.
