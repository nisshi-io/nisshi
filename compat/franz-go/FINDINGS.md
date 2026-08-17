# franz-go v1.21.3 sweep findings

Sweep of the franz-go `pkg/kgo` integration test suite against
`nisshi broker --storage-engine=memory://`, 2026-06-12. Passing tests are
in `tests.allow`; 61 of 64 pass (the SCRAM, share-group and KIP-848
tests self-skip).

## Fixed

1. **Fetch skipped the record batch containing a mid-batch offset**
   *(fixed 2026-06-12)* — when a fetch offset fell inside a batch (not
   at the batch base offset), the dynostore (`memory://`, `s3://`) and
   slatedb engines omitted that batch from the response instead of
   returning it whole and letting the client filter, as Kafka does.
   Records became unreachable, and a consumer positioned inside the
   *last* batch of a partition got empty fetches forever — the client
   hung (`TestAddRemovePartitions`, `TestIssue865`). With one
   partition holding batch `[a0 a1]` then batch `[b2 b3]`, a fetch at
   offset 1 returned `b2 b3` (losing `a1`) and a fetch at offset 3
   returned nothing; they now return `a1 b2 b3` and `b3`. The pg and
   lite engines rebuild batches from per-record rows and were not
   affected. Regression test: `mid_batch` in
   `nisshi-broker/tests/fetch.rs`, run against every engine.

## Open gaps, most impactful first

1. **Group coordinator returns UNKNOWN_MEMBER_ID during concurrent
   rebalances** — with three consumers in chained ETL groups joining
   and rebalancing concurrently, SyncGroup/Heartbeat fail with
   `UNKNOWN_MEMBER_ID` for members the coordinator should still know,
   for both dynamic and KIP-345 static (`group.instance.id`) members;
   one run also saw a duplicate offset delivered after a rebalance.
   Fails all `TestGroupETL` variants (range, cooperative-sticky,
   static). Single-group rebalancing (`TestGroupSimple`,
   `TestConsumeRegex`, pause/resume tests) passes.

2. **Aborted transactional records are visible to read_committed
   consumers** — in `TestTxnEtl`, a `read_committed` consumer observed
   more unique partition-offsets than committed records (aborted data
   leaking through), tripping the test's "consumed too much" guard.
   The same run also saw one SyncGroup response whose member assignment
   failed to parse ("response did not contain enough data to be
   valid"), suggesting an empty/truncated assignment payload under
   rebalance.

3. **`max.message.bytes` is not enforced on Produce** — a 10 MiB record
   produced to a topic created with `max.message.bytes=50KiB` is
   accepted; Kafka rejects the batch with `MESSAGE_TOO_LARGE`
   (`TestClient_ProduceLargeMessages/LargeMessage_FailureBroker`; the
   client-side subtest passes).

## Feature gaps observed via self-skips (not failures)

- **KIP-932 share groups** — `TestShareGroup*` skip: ShareFetch v2 /
  ShareAcknowledge v2 not advertised.
- **KIP-848 next-gen consumer groups** — the `848` subtests of
  `TestGroupETL` skip: ConsumerGroupHeartbeat v1 not advertised.
- **SCRAM** — `TestSCRAMAuth*` skip: the suite needs a SCRAM-enabled
  listener (`KGO_TEST_SCRAM`/`KGO_TEST_SCRAM_SEEDS`); not wired up in
  `just compat-franz-go`.
