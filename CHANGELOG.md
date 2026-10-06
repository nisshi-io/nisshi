# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- The broker's default `--kafka-listener-url` is now `tcp://[::]:9092`, which accepts both IPv6 and IPv4 clients. On a host without IPv6 support, the broker falls back to `0.0.0.0` on the same port and logs a warning; set `LISTENER_URL=tcp://0.0.0.0:9092` to listen on IPv4 only.
- The default advertised listener, and the default broker URL of the `nisshi` client subcommands, are now `127.0.0.1:9092` instead of `localhost:9092`. A client whose resolver returns `::1` for `localhost` first no longer gets connection refused.
- A consumer whose position is below a partition's log start offset now applies `auto.offset.reset`, as with Apache Kafka: Fetch answers `OFFSET_OUT_OF_RANGE` straight away, where it used to return the earliest surviving record. This shows on slatedb after retention or DeleteRecords: with the default `latest`, the consumer skips the retained backlog, and with `none`, the application gets an exception. Before you upgrade, compare consumer positions with the log start offset. These answers are counted in `nisshi_fetch_offset_out_of_bounds` with `bound` set to `below_log_start`.
- Fetch answers straight away, instead of after `max_wait`, when any partition is `OFFSET_OUT_OF_RANGE` or every partition has an error. A consumer assigned only to a deleted topic now fetches at its error backoff rather than once per `max_wait`.
- A listener with SASL configured closes a connection that sends a frame larger than 512KiB before the client authenticates, matching the Apache Kafka default for `sasl.server.max.receive.size`. The same limit applies while a client re-authenticates. The broker logs this rejection as `PreAuthenticationFrameTooBig`, and counts it in `nisshi_frames_rejected`.
- The broker stops at startup when its storage URL has a query option that the storage engine does not read, such as a misspelt option or `vacuum_into` on a `postgres://` URL. The error names the option and the engine. Previously the broker ignored the option.
- The broker stops at startup when `maintenance_interval` or `transaction_maintenance_interval` has an invalid value: unparsable, zero, longer than 365 days, or a bare number without a unit. Previously the broker ignored an invalid value and used the default interval. Give a bare number its unit, for example `600s` or `10m` instead of `600`. Compound values such as `1h30m` and `5min` still work.
- A `parquet`, `iceberg` or `delta` broker without `--schema-registry` stops at startup with an error that names the missing option, instead of panicking. `--schema-registry` is accepted before or after the subcommand.
- When the broker closes the connection of a client that sends a request other than ApiVersions, SaslHandshake or SaslAuthenticate before it authenticates, it logs an ERROR line that names the client's address.
- A broker on S3 or Google Cloud Storage exits at startup when it cannot list its cluster's prefix in the bucket, or when its AWS credential provider does not return a credential. The error names the storage URL and the cause. Previously the broker started and failed on its first request.

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
- CreateTopics rejects a `replication_factor` of 0 or below -1 with `INVALID_REPLICATION_FACTOR` (38), as Apache Kafka does. -1 still selects the default (1).
- SlateDB compaction skips a stored batch it cannot inflate, with a warning,
  instead of abandoning the whole maintenance pass.
- DeleteTopics on the object-store backends (`s3://`, `gs://`, `memory://`) no longer deletes other topics' data or committed offsets when the deleted topic's name is empty or contains `/`. Such a topic is removed from metadata and the request answers `NONE`, but its data objects stay in the bucket, and so do its committed offsets when the name has an empty segment (`a/`, `/a`, `a//b`, or empty). The broker logs each kept location at `warn` with the topic id. Do not delete those objects by key prefix: for several of these names they share keys with a live topic of the plain name, and must stay while it exists. Creating a topic whose name collapses onto such kept objects (`a` after `a/` was deleted) finds them in place and its first produce fails; delete `a` and create it again.
- ListOffsets by timestamp now answers offset -1 and timestamp -1 with error NONE when no record has a timestamp at or after the target, as Apache Kafka does. The broker answered offset 0 before. That made the Java consumer's `offsetsForTimes()` throw `IllegalArgumentException: Invalid negative timestamp`, and it sent a client that seeks to the returned offset back to the start of the partition. A partition answered with an error code now also carries offset -1 instead of 0.
- Fetch for a topic name that doesn't exist now answers `UNKNOWN_TOPIC_OR_PARTITION`. Before, `postgres://`, `sqlite://` and `slatedb://` brokers closed the connection, and `memory://` and `s3://` brokers waited out `max_wait` and then answered with no error. A fetch that names only unknown topics answers at once; one that also names an existing topic waits for that topic's data as usual. A fetch by topic id for an id that doesn't exist gets its requested id back instead of the null id, so a Java consumer on a deleted topic refreshes its metadata instead of fetching again.
- Fetch checks the fetch offset before it reaches the storage engine, so a storage engine never receives an offset such as `i64::MAX`. Above the high watermark, Fetch answers `NONE` with no records on every engine. Apache Kafka answers `NONE` only up to the log end offset and `OFFSET_OUT_OF_RANGE` above it; Nisshi answers `NONE` because a broker on dynostore can read a high watermark that lags a write through another broker. A consumer whose position is past the end of the log therefore waits there instead of applying `auto.offset.reset`. Such a consumer shows in `nisshi_fetch_offset_out_of_bounds` with `bound` set to `above_high_watermark`, counted once per poll round of a Fetch.
- SlateDB compaction no longer moves a partition's log start offset, as in Apache Kafka, whose cleaner never does. A consumer behind the first surviving batch of a compacted topic reads from that batch instead of resetting, and ListOffsets `earliest` on a compacted topic answers the log start offset rather than the first surviving batch. A log start already moved by compaction in an earlier version stays where it is, so on a slatedb topic compacted before the upgrade, a consumer behind it still resets.
- Fetch no longer sends a `current_leader` hint with `UNKNOWN_TOPIC_OR_PARTITION`, as in Apache Kafka.
