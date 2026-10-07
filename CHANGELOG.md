# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- A listener with SASL configured closes a connection that sends a frame larger than 512KiB before the client authenticates, matching the Apache Kafka default for `sasl.server.max.receive.size`. The same limit applies while a client re-authenticates. The broker logs this rejection as `PreAuthenticationFrameTooBig`, and counts it in `nisshi_frames_rejected`.
- InitProducerId follows Apache Kafka 3.9.1 on every storage engine when a producer
  sends its current producer ID and epoch to bump its epoch after an error (KIP-360):
  - Without a transactional ID, the producer gets a new producer ID, whatever it
    sends.
  - With a transactional ID that has no producer yet, the broker creates one.
  - With a transactional ID that has a producer, the broker bumps the epoch only
    when the request has that producer's ID and current epoch. Otherwise it
    answers `PRODUCER_FENCED`. On PostgreSQL, this replaces `UNKNOWN_PRODUCER_ID`
    for an unknown producer ID.
  - A request that sets only one of producer ID and epoch to -1 gets
    `INVALID_REQUEST`.
  - A failed InitProducerId answers producer ID and epoch -1, and the broker logs
    the request at info.
- Two differences from Apache Kafka remain. Kafka also accepts the previous epoch,
  from a producer that retries a bump whose response it lost; the broker answers
  `PRODUCER_FENCED`, and the producer must initialise again. Kafka answers
  `INVALID_PRODUCER_EPOCH` instead of `PRODUCER_FENCED` to InitProducerId v3 and
  older; the broker answers `PRODUCER_FENCED` at every version.

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
- InitProducerId v0 to v2 works on every storage engine. These versions do not
  carry a producer ID or epoch. libSQL and Turso panicked on them, and on a
  KIP-360 epoch bump, ending the connection. The object store engines
  (`memory://`, `s3://`, `gs://`) answered `UNKNOWN_SERVER_ERROR`, and so did
  SlateDB without a transactional ID.
- On PostgreSQL, concurrent InitProducerId requests for one transactional ID
  bump its epoch one at a time. Before, two of them could both succeed, and one
  could fail with a storage error.
