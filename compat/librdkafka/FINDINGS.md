# librdkafka v2.14.2 sweep findings

Sweeps of the librdkafka integration test suite against
`nisshi broker --storage-engine=memory://`. Passing tests are in
`tests.allow`.

## Fixed

1. **No automatic topic creation on MetadataRequest** *(fixed 2026-06-11)* —
   implemented in `MetadataService`: unknown topics named in a
   MetadataRequest with `allow_auto_topic_creation` are created with four
   partitions (the `num.partitions` the Kafka client test suites assume)
   and replication factor 1. Invalid topic names are reported as
   `INVALID_TOPIC_EXCEPTION` and not created. This unlocked ~100 tests
   that previously could not run.

2. **IncrementalAlterConfigs APPEND/SUBTRACT panicked the broker**
   *(fixed 2026-06-11)* — `todo!()` in dynostore `alter_topic` panicked
   while holding the topics lock, poisoning it: every subsequent request
   failed with `Poison` until restart (hit by 0011). Now applies
   comma-separated-list append/subtract semantics. The same `todo!()`
   remains in the pg, lite and limbo backends.

## Open gaps, most impactful first

1. **Produce does not reject keyless records on compacted topics** —
   Kafka fails them per-record with `INVALID_RECORD`; nisshi accepts them
   (0011 `test_message_single_partition_record_fail`). Related to the
   deliberate retention of keyless records in batch compaction
   (commit 6db7a49).

2. **ListOffsets by timestamp returns wrong/no offsets** — offsets-for-
   times queries return incorrect offsets or time out (0031, 0034, 0054,
   0059, and 0015/0030 which seek by time).

3. **CreatePartitions returns an empty topics array** —
   `CreatePartitions_result_topics returned NULL` (0044, 0069, 0112).

4. **OffsetCommit `metadata` is not round-tripped** — committed metadata
   comes back empty from OffsetFetch (0099, 0140).

5. **DescribeConfigs returns no config entries** — topics report zero
   ConfigEntry rows where Kafka reports every config with its default
   (0092 via AlterConfigs read-modify-write).

6. **InitProducerId does not validate `transaction.timeout.ms`** — Kafka
   rejects values above `transaction.max.timeout.ms` with fatal
   `INVALID_TRANSACTION_TIMEOUT`; nisshi accepts them (0103
   `do_test_misuse_txn`; the rest of 0103 passes).

7. **EndTxn unsupported for abort from some clients** — 0129 fails with
   `EndTxnRequest (KIP-98) not supported`, suggesting the advertised
   ApiVersions range for EndTxn does not cover what librdkafka needs
   for `abort_transaction`.

8. **Consumer group edge cases** — one extra rebalance versus Kafka in
   max-poll-exceeded scenarios (0091, 0093); commit-during-rebalance
   error codes differ (0118); message expected after `offsets_store`
   of an unassigned partition (0130).

9. **CreateTopics corner cases** — 0081 (admin) fails waiting for a
   metadata update after creating topics with mixed valid/invalid
   configs.

## Not nisshi issues (environmental skips/failures)

- 0052, 0077, 0109, 0115, 0119: need `KAFKA_PATH`/`ZK_ADDRESS` (drive
  Kafka's own CLI tools).
- 0064: needs an SSL-enabled librdkafka build.
- 0028/0075/0088: sockem tests, filtered by `-E`.
- 0101: needs RapidJSON. 0107: interactive. 0142: needs SASL listener.
- 0086 (purge) *(excluded 2026-09-23)*: `0086_purge_remote` is
  timing-sensitive against any broker that advertises the KIP-714 client
  telemetry APIs. The test sets `max.in.flight=1`, produces 29 messages,
  adds a 50 s sockem delay to the socket as soon as the first
  ProduceResponse arrives, then asserts that a second ProduceRequest is
  written within 15 s. librdkafka also sends `GetTelemetrySubscriptions`
  to every learned broker that advertises it, and whether that request is
  queued before or after the first Produce is a thread race. When it lands
  after, its response sits behind the 50 s delay and, because
  `max.in.flight` counts every request type, the second Produce is never
  sent: `"produce_sent_req_cnt == 2" ... 1 sent produce requests`. Kafka
  only advertises the telemetry APIs when a client-telemetry plugin is
  configured (`ApiVersionsResponse.filterApis`), so upstream never sees
  this; nisshi always advertises them and answers with an empty
  subscription (`GetTelemetrySubscriptionsService`). Reproduced locally
  against `memory://` at 5/25 under CPU load (0/5 idle), and 0/25 under
  the same load with `enable.metrics.push=false`; ~3% in CI. The test
  uses sockem but is
  not flagged `TEST_F_SOCKEM`, so `-E` does not filter it, and `TESTS`
  selects by test number, so `0086_purge_local` (which never contacts the
  broker) cannot be kept on its own. Re-enable if nisshi stops advertising
  the telemetry APIs when it has no subscription to offer, or if the suite
  is run with `enable.metrics.push=false` in `test.conf`.
