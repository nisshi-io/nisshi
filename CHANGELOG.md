# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- The S3 and in-memory (DynoStore) storage engine stores each consumer
  group id as exactly one segment of its object key. Keys for other ids
  are unchanged. These keys move, and data under the old keys is not
  migrated:
  - an id containing `/`: its committed offsets and group state;
  - the empty id `""`, which consumers using `assign()` commit under: its
    committed offsets and group state;
  - `.` and `..`: the group state only.
  An affected group's consumers find no committed offset and start from
  `auto.offset.reset`. With the Java client default, `latest`, they skip
  records they had not yet consumed. Old data stays visible in ListGroups:
  old `""` data lists as a group named `offsets`, and old `a/b` data as a
  group named `a`. Deleting that group removes it.
- Upgrade every broker sharing a bucket before using these ids. An old and
  a new broker read and write different keys for them, so members connected
  to different brokers form two separate groups. A rollback returns these
  groups to the offsets they had before the upgrade.
- The broker's default `--kafka-listener-url` is now `tcp://[::]:9092`, which accepts both IPv6 and IPv4 clients. On a host without IPv6 support, the broker falls back to `0.0.0.0` on the same port and logs a warning; set `LISTENER_URL=tcp://0.0.0.0:9092` to listen on IPv4 only.
- The default advertised listener, and the default broker URL of the `nisshi` client subcommands, are now `127.0.0.1:9092` instead of `localhost:9092`. A client whose resolver returns `::1` for `localhost` first no longer gets connection refused.
- A consumer whose position is below a partition's log start offset now applies `auto.offset.reset`, as with Apache Kafka: Fetch answers `OFFSET_OUT_OF_RANGE` straight away, where it used to return the earliest surviving record. This shows on slatedb after retention, DeleteRecords, or compaction by an earlier version: `v0.7.0-pre.1` and `v0.7.0-pre.2` moved the log start to the first surviving batch on compaction, and their Fetch had no bounds check, so a consumer behind that log start read the first surviving batch. After the upgrade that log start stays where it is, so on a compacted slatedb topic a group that is offline or lagging resets for the first time. With the default `latest`, the consumer skips the retained backlog, which on a topic that rewrites a few keys often (such as a config topic) can be nearly all of it; with `none`, the application gets an exception. Before you upgrade, compare each group's committed offsets with ListOffsets `earliest` on slatedb topics, compacted ones in particular, and reset lagging groups to `earliest`. Run consumers of compacted slatedb topics with `auto.offset.reset=earliest` across the upgrade, because compaction by the old version keeps raising the log start between that check and the upgrade itself. The log start is the batch the old version would have served, so a reset to it loses nothing. These answers are counted in `nisshi_fetch_offset_out_of_bounds` with `bound` set to `below_log_start`, which after the upgrade also shows consumers that the group comparison misses, such as those that use `assign` with offsets stored outside Kafka.
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

- On S3 and in-memory storage, a consumer group whose id contains `/` or is
  empty no longer shares offsets or group state with another group. Group
  `a/` no longer reads or overwrites the committed offsets of group `a`, and
  deleting group `a` no longer deletes group `a/b`. Deleting a topic now also
  removes the committed offsets that such groups hold for it.
- On S3 and in-memory storage, ListGroups returns each group's real id. For
  an id with a reserved character it returned the encoded form (`a%23b` for
  `a#b`, `%2E` for `.`), which DescribeGroups and DeleteGroups could not
  find.
- On S3 and in-memory storage, DeleteGroups accepts the empty group id, as
  the other storage engines do, instead of answering `INVALID_GROUP_ID`.
- A Snappy batch with a truncated xerial header is rejected with an error
  instead of panicking the decoder.
- CreateTopics rejects a `replication_factor` of 0 or below -1 with `INVALID_REPLICATION_FACTOR` (38), as Apache Kafka does. -1 still selects the default (1).
- SlateDB compaction skips a stored batch it cannot inflate, with a warning,
  instead of abandoning the whole maintenance pass.
- DeleteTopics on the object-store backends (`s3://`, `gs://`, `memory://`) no longer deletes other topics' data or committed offsets when the deleted topic's name is empty or contains `/`. Such a topic is removed from metadata and the request answers `NONE`, but its data objects stay in the bucket, and so do its committed offsets when the name has an empty segment (`a/`, `/a`, `a//b`, or empty). The broker logs each kept location at `warn` with the topic id. Do not delete those objects by key prefix: for several of these names they share keys with a live topic of the plain name, and must stay while it exists. Creating a topic whose name collapses onto such kept objects (`a` after `a/` was deleted) finds them in place and its first produce fails; delete `a` and create it again.
- ListOffsets by timestamp now answers offset -1 and timestamp -1 with error NONE when no record has a timestamp at or after the target, as Apache Kafka does. The broker answered offset 0 before. That made the Java consumer's `offsetsForTimes()` throw `IllegalArgumentException: Invalid negative timestamp`, and it sent a client that seeks to the returned offset back to the start of the partition. A partition answered with an error code now also carries offset -1 instead of 0.
- Fetch for a topic name that doesn't exist now answers `UNKNOWN_TOPIC_OR_PARTITION`. Before, `postgres://`, `sqlite://` and `slatedb://` brokers closed the connection, and `memory://` and `s3://` brokers waited out `max_wait` and then answered with no error. A fetch that names only unknown topics answers at once; one that also names an existing topic waits for that topic's data as usual. A fetch by topic id for an id that doesn't exist gets its requested id back instead of the null id, so a Java consumer on a deleted topic refreshes its metadata instead of fetching again.
- Fetch checks the fetch offset before it reaches the storage engine, so a storage engine never receives an offset such as `i64::MAX`. Above the high watermark, Fetch answers `NONE` with no records on every engine. Apache Kafka answers `NONE` only up to the log end offset and `OFFSET_OUT_OF_RANGE` above it; Nisshi answers `NONE` because a broker on dynostore can read a high watermark that lags a write through another broker. A consumer whose position is past the end of the log therefore waits there instead of applying `auto.offset.reset`. Such a consumer shows in `nisshi_fetch_offset_out_of_bounds` with `bound` set to `above_high_watermark`, counted once per poll round of a Fetch, so the counter shows whether parked fetches exist rather than how many; a short burst on dynostore with several brokers is expected.
- SlateDB compaction no longer moves a partition's log start offset, as in Apache Kafka, whose cleaner never does. ListOffsets `earliest` and the `log_start_offset` in Fetch now stay at the log start through compaction, rather than moving to the first surviving batch.
- Fetch no longer sends a `current_leader` hint with `UNKNOWN_TOPIC_OR_PARTITION`, as in Apache Kafka.
