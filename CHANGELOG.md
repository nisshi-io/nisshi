# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `nisshi-sans-io` has an `arbitrary` feature that derives `arbitrary::Arbitrary` on every generated message type. The dev-only `fuzz` crate uses it to run a fuzzed request through each non-transaction storage service, and it gains a target for each of them.

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
- A `Fetch` request with an invalid `isolation_level`, `max_wait_ms`, `min_bytes` or `max_bytes` gets `INVALID_REQUEST` on the response and on every requested partition, instead of an internal error. `FetchService` takes a `ValidatedFetchRequest`, which parses these fields once.
- A `ListOffsets` request with an invalid `isolation_level` gets `INVALID_REQUEST`, and one with an invalid timestamp gets `INVALID_TIMESTAMP` on that partition, instead of an internal error.
- An `AlterUserScramCredentials` request with an unrecognized SASL mechanism gets `UNSUPPORTED_SASL_MECHANISM` on that item, instead of an internal error.
- A `Fetch` or `Metadata` request for a topic with neither a name nor a topic id no longer panics.
