# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- A listener with SASL configured closes a connection that sends a frame larger than 512KiB before the client authenticates, matching the Apache Kafka default for `sasl.server.max.receive.size`. The same limit applies while a client re-authenticates. The broker logs this rejection as `PreAuthenticationFrameTooBig`, and counts it in `nisshi_frames_rejected`.
- IncrementalAlterConfigs checks each resource the way Apache Kafka 3.9.1
  does, and answers each failing resource with its own error, without
  closing the connection:
  - `INVALID_REQUEST` for a resource that appears twice, duplicate config
    keys, a null value on any operation except `DELETE`, or an unknown
    operation;
  - `INVALID_CONFIG` for `APPEND` or `SUBTRACT` on a key whose type isn't a
    list. The topic list keys are `cleanup.policy`,
    `leader.replication.throttled.replicas` and
    `follower.replication.throttled.replicas`;
  - `UNKNOWN_TOPIC_OR_PARTITION` for a topic that doesn't exist, on every
    storage engine. The memory, S3, GCS and SlateDB engines used to report
    success.
- IncrementalAlterConfigs with `validate_only` no longer changes anything.
  It used to apply the changes.
- A topic applies all the changes of one IncrementalAlterConfigs resource,
  or none of them. A storage failure fails only that resource, with
  `UNKNOWN_SERVER_ERROR`.
- `APPEND` and `SUBTRACT` follow Kafka: each item of a comma-separated value
  is added if missing, or its first occurrence is removed. An unset value is
  an empty list. Kafka starts from the key's default instead, so `APPEND
  compact` to an unset `cleanup.policy` gives `compact` here and
  `delete,compact` in Kafka. Nisshi deletes old records only when
  `cleanup.policy` contains `delete`.

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

- IncrementalAlterConfigs `APPEND` and `SUBTRACT` on a topic no longer panic
  the connection on the PostgreSQL, SQLite and Turso engines, and are no
  longer ignored on SlateDB.
- An OffsetFetch whose storage fails no longer panics the connection. The
  response reports `COORDINATOR_NOT_AVAILABLE`, which clients retry, for a
  transient failure, or the storage's own error code. From version 8 each
  group reports its own error, and the other groups still get their offsets.
- A Snappy batch with a truncated xerial header is rejected with an error
  instead of panicking the decoder.
- SlateDB compaction skips a stored batch it cannot inflate, with a warning,
  instead of abandoning the whole maintenance pass.
