// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tests for the `time_index`-backed `ListOffsets(Timestamp)` implementation.
//! Calls `DynoStore`'s private helpers directly (this module is
//! a descendant of `dynostore`, so it can see them) rather than only going
//! through the `Storage` trait, because some of the scenarios below need to
//! control the exact interleaving between a backfill's read and its write.

use std::{
    collections::BTreeMap,
    fmt::Display,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::dynostore::{
    DynoStore, Txn, TxnDetail, TxnProduceOffset, Watermark,
    tests::init_tracing,
    time_index::{MAX_ENTRIES, TimeIndex},
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use nisshi_sans_io::{
    IsolationLevel, ListOffset, create_topics_request::CreatableTopic, to_system_time, to_timestamp,
};
use nisshi_storage::{Error, Result, Storage, Topition, TxnState};
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    memory::InMemory, path::Path,
};

/// An arbitrary fixed point in 2020, in Kafka's millisecond-since-epoch
/// timestamp representation. A real, far-from-`now` value keeps a test from
/// passing by coincidence the way a `SystemTime::now()`-based one could.
const T0: i64 = 1_577_836_800_000; // 2020-01-01T00:00:00Z

fn storage() -> DynoStore {
    DynoStore::new("nisshi", 111, InMemory::new())
}

async fn create_topic(storage: &DynoStore, topic: &str, num_partitions: i32) -> Result<()> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.into())
                .num_partitions(num_partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

/// Builds a single-record batch with an explicit, fully-controlled
/// `base_timestamp`/`max_timestamp`, carrying `last_offset_delta` extra
/// records beyond the first so a caller can build a multi-record batch by
/// supplying more than one timestamp.
fn batch_with_timestamps(base_offset: i64, timestamps: &[i64]) -> Result<deflated_batch::Batch> {
    deflated_batch::build(base_offset, timestamps)
}

// Keeps the `deflated`/`inflated` plumbing out of the test bodies above.
mod deflated_batch {
    use super::*;
    use nisshi_sans_io::record::{Record, deflated, inflated};

    pub(super) use deflated::Batch;

    pub(super) fn build(base_offset: i64, timestamps: &[i64]) -> Result<Batch> {
        let base_timestamp = timestamps[0];
        let max_timestamp = timestamps.iter().copied().max().unwrap_or(base_timestamp);

        let mut builder = inflated::Batch::builder()
            .base_offset(base_offset)
            .base_timestamp(base_timestamp)
            .max_timestamp(max_timestamp)
            .last_offset_delta(i32::try_from(timestamps.len() - 1).unwrap_or_default());

        for (i, timestamp) in timestamps.iter().enumerate() {
            builder = builder.record(
                Record::builder()
                    .value(Some(Bytes::from_static(b"v")))
                    .timestamp_delta(timestamp - base_timestamp)
                    .offset_delta(i32::try_from(i).unwrap_or_default()),
            );
        }

        builder
            .build()
            .and_then(Batch::try_from)
            .map_err(Into::into)
    }
}

/// Produces one batch through the normal path (so `time_index` is maintained
/// the way a real produce maintains it), returning its base offset.
async fn produce(storage: &DynoStore, topition: &Topition, timestamps: &[i64]) -> Result<i64> {
    let batch = batch_with_timestamps(0, timestamps)?;
    storage.produce(None, topition, batch).await
}

/// Writes a raw batch object directly to the object store, bypassing
/// `produce` entirely - simulating a batch written before this code (and its
/// `time_index` maintenance) existed.
async fn write_legacy_batch(
    storage: &DynoStore,
    topition: &Topition,
    base_offset: i64,
    timestamp: i64,
) -> Result<()> {
    let batch = batch_with_timestamps(base_offset, &[timestamp])?;
    let path = storage.batch_path(topition, base_offset);
    let payload = storage.encode(batch)?;

    _ = storage
        .object_store
        .put_opts(
            &path,
            payload,
            PutOptions {
                mode: PutMode::Create,
                attributes: Attributes::new(),
                ..Default::default()
            },
        )
        .await
        .map_err(Error::from)?;

    Ok(())
}

/// Directly sets a partition's watermark fields, bypassing both `produce`
/// and `create_topic`'s reset - used to set up pre-migration ("legacy") or
/// post-`DeleteRecords` (`low > 0`) states that nothing in the normal write
/// path produces on its own.
async fn seed_watermark(
    storage: &DynoStore,
    topition: &Topition,
    f: impl Fn(&mut Watermark) -> Result<()>,
) -> Result<()> {
    let watermark = storage.watermark_for(topition)?;
    watermark.with_mut(&storage.object_store, f).await
}

async fn list_offsets_timestamp(
    storage: &DynoStore,
    topition: &Topition,
    target: i64,
) -> Result<(Option<i64>, Option<i64>)> {
    list_offsets_timestamp_at(storage, IsolationLevel::ReadUncommitted, topition, target).await
}

async fn list_offsets_timestamp_at(
    storage: &DynoStore,
    isolation_level: IsolationLevel,
    topition: &Topition,
    target: i64,
) -> Result<(Option<i64>, Option<i64>)> {
    let responses = storage
        .list_offsets(
            isolation_level,
            &[(
                topition.to_owned(),
                ListOffset::Timestamp(to_system_time(target)?),
            )],
        )
        .await?;

    assert_eq!(1, responses.len());

    Ok((
        responses[0].1.offset,
        responses[0]
            .1
            .timestamp
            .map(|t| to_timestamp(&t))
            .transpose()?,
    ))
}

/// Splits the backfill into its two steps and interleaves a concurrent
/// produce between them, to confirm a produce landing mid-backfill is never
/// lost: a write-side guard that defers inserting into `time_index` until
/// the index is complete would miss this entry (the produce never touches
/// `time_index`, so the backfill's commit has nothing to merge it with),
/// which is why `produce` carries no such guard.
#[tokio::test]
async fn backfill_commit_picks_up_concurrent_produce() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "legacy";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    // Two legacy batches, written directly (not through `produce`): off0 has
    // the higher timestamp, off1 a lower one that the monotonic rule must
    // not let override it.
    write_legacy_batch(&storage, &topition, 0, T0).await?;
    write_legacy_batch(&storage, &topition, 1, T0 - 50).await?;

    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(2);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    // Step 1: collect a candidate from the pre-concurrent-produce state.
    let (candidate, batches) = storage.collect_time_index_candidate(&topition).await?;
    assert_eq!(2, batches);
    assert_eq!(&BTreeMap::from([(T0, 0)]), candidate.entries());
    assert_eq!(T0, candidate.max_timestamp());

    // Step 2: a concurrent produce lands before the backfill commits, with a
    // new running max above everything seen so far.
    let concurrent_timestamp = T0 + 50;
    let offset = produce(&storage, &topition, &[concurrent_timestamp]).await?;
    assert_eq!(2, offset);

    // Step 3: commit the now-stale candidate.
    let merged = storage
        .commit_time_index_backfill(&topition, &candidate)
        .await?;
    assert_eq!(
        &BTreeMap::from([(T0, 0), (concurrent_timestamp, 2)]),
        merged.entries()
    );
    assert_eq!(concurrent_timestamp, merged.max_timestamp());
    assert!(merged.is_complete());

    // Step 4: the concurrent produce's entry must still be answerable.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 25).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(concurrent_timestamp), timestamp);

    Ok(())
}

/// A plain `BTreeMap` union of a backfill candidate and whatever is live
/// would keep offset 2's entry below offset 0's already-established higher
/// one. A floor lookup between the two would then start its scan at offset
/// 2 and miss offset 0's record. The correct merge (replay the append rule
/// over the union, sorted by offset) drops that entry, so the lookup
/// answers offset 0.
#[tokio::test]
async fn merge_does_not_let_a_lower_concurrent_entry_outrank_an_established_one() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "legacy-union";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    write_legacy_batch(&storage, &topition, 0, T0).await?; // the eventual floor
    write_legacy_batch(&storage, &topition, 1, T0 - 50).await?;

    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(2);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    // A post-upgrade produce lands before any read ever triggers a backfill:
    // `time_index` is still `None` at this point, so this insert goes into a
    // fresh map, exactly the scenario that makes a plain union wrong.
    let offset = produce(&storage, &topition, &[T0 - 20]).await?;
    assert_eq!(2, offset);

    // The first read now triggers the backfill. The target is above off2's
    // T0 - 20, so an entry for off2 would be the floor, and a scan from
    // there finds nothing. Off0's T0 is the first record at or after it.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 - 10).await?;
    assert_eq!(Some(0), offset);
    assert_eq!(Some(T0), timestamp);

    Ok(())
}

/// A `low > 0` straddling batch: one batch holds records at offsets 0, 1 and
/// 2, and `DeleteRecords` (simulated here by setting `low` directly, since
/// `delete_records` is still `todo!()` on this branch) has logically removed
/// offsets 0 and 1. A Timestamp lookup must skip the deleted prefix and only
/// match a record at or after `low`, even though the matching batch's own
/// base offset (0) is below `low`.
#[tokio::test]
async fn timestamp_lookup_skips_deleted_prefix_of_a_straddling_batch() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "straddling";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    // One batch, three records: offsets 0, 1, 2 with increasing timestamps.
    let timestamps = [T0, T0 + 50, T0 + 100];
    let offset = produce(&storage, &topition, &timestamps).await?;
    assert_eq!(0, offset);

    // Simulate DeleteRecords(cutoff = 2): offsets 0 and 1 are logically
    // gone, and the index still points at the batch's base offset (0).
    seed_watermark(&storage, &topition, |w| {
        w.low = Some(2);
        Ok(())
    })
    .await?;

    // Target below every surviving record's timestamp: must still answer the
    // first surviving record (offset 2), not offset 0 or 1.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 100), timestamp);

    Ok(())
}

/// The sequential scan helper must return `Ok(None)`, not error, when
/// `start_offset` is past every real batch - the case both an out-of-range
/// query and the lake-sink produce path (which never writes a `.batch`
/// object at all) hit.
#[tokio::test]
async fn sequential_scan_past_every_real_batch_returns_none() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "empty";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    _ = produce(&storage, &topition, &[T0]).await?;

    let found = storage
        .sequential_timestamp_scan(&topition, 1_000_000, T0, 0)
        .await?;
    assert_eq!(None, found);

    Ok(())
}

/// A Timestamp lookup against a partition with no batches at all (never
/// produced to) must answer "no match", not error and not offset 0.
#[tokio::test]
async fn empty_partition_no_match() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "never-produced";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0).await?;
    assert_eq!(None, offset);
    assert_eq!(None, timestamp);

    Ok(())
}

/// A Timestamp lookup above every record's timestamp must answer "no
/// match", found directly from the index's greatest timestamp - no scan
/// required.
#[tokio::test]
async fn future_timestamp_no_match() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "future";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    _ = produce(&storage, &topition, &[T0]).await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 1_000_000).await?;
    assert_eq!(None, offset);
    assert_eq!(None, timestamp);

    Ok(())
}

/// A brand-new partition's first produce must mark its index complete
/// immediately: nothing predates offset 0, so no backfill is ever needed for
/// it.
#[tokio::test]
async fn new_partition_first_produce_marks_index_complete() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "brand-new";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    _ = produce(&storage, &topition, &[T0]).await?;

    let watermark = storage.watermark_for(&topition)?;
    let complete = watermark
        .with(&storage.object_store, |w| Ok(w.time_index.is_complete()))
        .await?;
    assert!(complete);

    Ok(())
}

/// Pins the ticket's two remaining named scenarios, both inside one
/// multi-record batch: a target equal to a record's own timestamp (the
/// inclusive boundary) and a target that falls strictly between two records'
/// timestamps (a genuine mid-batch match). One batch, offsets 0/1/2 at
/// T0/T0+50/T0+100, is indexed under a single `time_index` entry keyed by its
/// `max_timestamp` (T0+100), so every assertion here is answered by the
/// sequential scan walking records inside that one batch, not by a floor
/// lookup landing on a different batch.
#[tokio::test]
async fn equal_and_mid_batch_timestamps_match_inside_a_batch() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "boundary";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    let timestamps = [T0, T0 + 50, T0 + 100];
    let offset = produce(&storage, &topition, &timestamps).await?;
    assert_eq!(0, offset);

    // Inclusive boundary: target equals offset 1's own timestamp exactly.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 50).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 50), timestamp);

    // Mid-batch match: target falls strictly between offset 0's and offset
    // 1's timestamps, so the first record at or after it is offset 1.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 25).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 50), timestamp);

    // Mid-batch match on the last record: target falls strictly between
    // offset 1's and offset 2's timestamps.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 51).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 100), timestamp);

    Ok(())
}

/// Each produce into a freshly created topic maintains the index under the
/// monotonic rule: a batch whose `max_timestamp` is below the running max
/// adds no entry. An entry for offset 1 here would be the floor of a lookup
/// between the two timestamps, and a scan from offset 1 would miss offset
/// 0, which holds the first record at or after the target.
#[tokio::test]
async fn produce_skips_an_entry_below_the_running_max() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "fresh-out-of-order";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    seed_dense_index(&storage, &topition).await?;

    assert_eq!(0, produce(&storage, &topition, &[T0]).await?);
    assert_eq!(1, produce(&storage, &topition, &[T0 - 50]).await?);

    assert_eq!(
        BTreeMap::from([(T0, 0)]),
        time_index_entries(&storage, &topition).await?
    );

    for target in [T0 - 60, T0 - 40] {
        let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, target).await?;
        assert_eq!(Some(0), offset, "target {target}");
        assert_eq!(Some(T0), timestamp, "target {target}");
    }

    Ok(())
}

/// Sets a fresh partition's index interval to one byte, so that every batch
/// that raises the maximum timestamp is indexed.
async fn seed_dense_index(storage: &DynoStore, topition: &Topition) -> Result<()> {
    seed_index_interval(storage, topition, 1).await
}

async fn seed_index_interval(
    storage: &DynoStore,
    topition: &Topition,
    interval_bytes: u64,
) -> Result<()> {
    seed_watermark(storage, topition, |w| {
        w.time_index = TimeIndex::complete_with_interval(interval_bytes);
        Ok(())
    })
    .await
}

async fn time_index_entries(
    storage: &DynoStore,
    topition: &Topition,
) -> Result<BTreeMap<i64, i64>> {
    storage
        .watermark_for(topition)?
        .with(&storage.object_store, |w| {
            Ok(w.time_index.entries().clone())
        })
        .await
}

/// The index is sparse: a batch that raises the maximum timestamp inside
/// the interval gets no entry. A lookup for a target between two entries
/// must scan from the greatest entry at or below the target (the floor), so
/// that it finds the unindexed batch. A scan from the entry above the
/// target would answer that entry's batch, a later offset than Kafka's.
#[tokio::test]
async fn lookup_scans_from_the_floor_entry_past_an_unindexed_batch() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "sparse";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    // Every batch here is one record of the same value, so they are all the
    // same size. The interval admits an entry every third batch.
    let batch_bytes = u64::try_from(batch_with_timestamps(0, &[T0])?.batch_length)?;
    seed_index_interval(&storage, &topition, 2 * batch_bytes + 1).await?;

    for (expected, timestamp) in (0..).zip([T0, T0 + 100, T0 + 200, T0 + 300]) {
        assert_eq!(expected, produce(&storage, &topition, &[timestamp]).await?);
    }

    assert_eq!(
        BTreeMap::from([(T0, 0), (T0 + 300, 3)]),
        time_index_entries(&storage, &topition).await?
    );

    // Offset 2 holds the first record at or after the target, and has no
    // entry.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 150).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 200), timestamp);

    // A target equal to an entry's timestamp answers that entry's batch.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 300).await?;
    assert_eq!(Some(3), offset);
    assert_eq!(Some(T0 + 300), timestamp);

    // A target between the maximum timestamp and the last entry's timestamp
    // is a match; only a target above the maximum is no match.
    assert_eq!(4, produce(&storage, &topition, &[T0 + 400]).await?);
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 350).await?;
    assert_eq!(Some(4), offset);
    assert_eq!(Some(T0 + 400), timestamp);
    assert_eq!(
        (None, None),
        list_offsets_timestamp(&storage, &topition, T0 + 401).await?
    );

    Ok(())
}

/// The index holds at most `MAX_ENTRIES` entries however many batches the
/// partition has, and still answers each lookup with the first record at or
/// after the target once it has been thinned.
#[tokio::test]
async fn index_stays_within_the_cap_and_answers_after_thinning() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "capped";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    seed_dense_index(&storage, &topition).await?;

    let batches = i64::try_from(MAX_ENTRIES)? + 1;
    for i in 0..batches {
        assert_eq!(i, produce(&storage, &topition, &[T0 + i]).await?);
    }

    let entries = time_index_entries(&storage, &topition).await?;
    assert!(entries.len() <= MAX_ENTRIES, "{} entries", entries.len());
    assert!(
        entries.len() >= MAX_ENTRIES / 2,
        "{} entries",
        entries.len()
    );

    for target in [T0, T0 + 1, T0 + 2, T0 + batches / 2, T0 + batches - 1] {
        let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, target).await?;
        assert_eq!(Some(target - T0), offset, "target {target}");
        assert_eq!(Some(target), timestamp, "target {target}");
    }

    Ok(())
}

/// A lookup on a partition without a watermark document answers no match,
/// and does not create the document.
#[tokio::test]
async fn lookup_does_not_create_a_watermark_document() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topition = Topition::new("absent", 0);

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0).await?;
    assert_eq!(None, offset);
    assert_eq!(None, timestamp);

    let path = Path::from("clusters/nisshi/topics/absent/partitions/0000000000/watermark.json");
    assert!(matches!(
        storage.object_store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    Ok(())
}

/// Under READ_COMMITTED, a match at or after the first offset of an open
/// transaction is no match, as in Kafka. READ_UNCOMMITTED still answers it.
#[tokio::test]
async fn read_committed_lookup_stops_at_the_last_stable_offset() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "open-transaction";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    assert_eq!(0, produce(&storage, &topition, &[T0]).await?);
    assert_eq!(1, produce(&storage, &topition, &[T0 + 50]).await?);

    storage
        .meta
        .with_mut(&storage.object_store, |meta| {
            _ = meta.transactions.insert(
                "txn".into(),
                Txn {
                    producer: 1,
                    epochs: BTreeMap::from([(
                        0,
                        TxnDetail {
                            state: Some(TxnState::Begin),
                            produces: BTreeMap::from([(
                                topic.into(),
                                BTreeMap::from([(
                                    0,
                                    Some(TxnProduceOffset {
                                        offset_start: 1,
                                        offset_end: 2,
                                    }),
                                )]),
                            )]),
                            ..Default::default()
                        },
                    )]),
                },
            );
            Ok(())
        })
        .await?;

    let committed =
        list_offsets_timestamp_at(&storage, IsolationLevel::ReadCommitted, &topition, T0 + 25)
            .await?;
    assert_eq!((None, None), committed);

    let committed =
        list_offsets_timestamp_at(&storage, IsolationLevel::ReadCommitted, &topition, T0).await?;
    assert_eq!((Some(0), Some(T0)), committed);

    let uncommitted = list_offsets_timestamp(&storage, &topition, T0 + 25).await?;
    assert_eq!((Some(1), Some(T0 + 50)), uncommitted);

    Ok(())
}

/// The error that [`Faulty`] returns for the next GET of its armed path.
#[derive(Clone, Copy, Debug)]
enum Fault {
    NotFound,
    Unavailable,
}

/// An object store that fails the next GET of one armed path, and counts
/// the GETs of batch objects. Every GET first yields to the runtime, so
/// that concurrent requests in a test interleave.
#[derive(Clone, Debug, Default)]
struct Faulty {
    inner: Arc<InMemory>,
    armed: Arc<Mutex<Option<(Path, Fault)>>>,
    batch_gets: Arc<AtomicUsize>,
}

impl Faulty {
    fn arm(&self, path: Path, fault: Fault) -> Result<()> {
        self.armed
            .lock()
            .map(|mut armed| _ = armed.replace((path, fault)))
            .map_err(Into::into)
    }

    fn batch_gets(&self) -> usize {
        self.batch_gets.load(Ordering::SeqCst)
    }
}

impl Display for Faulty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Faulty").finish()
    }
}

#[async_trait]
impl ObjectStore for Faulty {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        tokio::task::yield_now().await;

        if location.as_ref().ends_with(".batch") {
            _ = self.batch_gets.fetch_add(1, Ordering::SeqCst);
        }

        let fault = self
            .armed
            .lock()
            .ok()
            .and_then(|mut armed| armed.take_if(|(path, _)| path == location))
            .map(|(_, fault)| fault);

        let source = || "injected by test".into();

        match fault {
            Some(Fault::NotFound) => Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: source(),
            }),
            Some(Fault::Unavailable) => Err(object_store::Error::Generic {
                store: "Faulty",
                source: source(),
            }),
            None => self.inner.get_opts(location, options).await,
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// Writes three legacy batches, with the running max at offset 1, into a
/// partition whose index is incomplete.
async fn legacy_partition_with_max_at_offset_1(
    storage: &DynoStore,
    topic: &str,
) -> Result<Topition> {
    let topition = Topition::new(topic, 0);

    create_topic(storage, topic, 1).await?;

    write_legacy_batch(storage, &topition, 0, T0).await?;
    write_legacy_batch(storage, &topition, 1, T0 + 100).await?;
    write_legacy_batch(storage, &topition, 2, T0 + 50).await?;

    seed_watermark(storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(3);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    Ok(topition)
}

async fn time_index_complete(storage: &DynoStore, topition: &Topition) -> Result<bool> {
    storage
        .watermark_for(topition)?
        .with(&storage.object_store, |w| Ok(w.time_index.is_complete()))
        .await
}

/// A produce into a legacy partition before its first lookup indexes its
/// batch against an empty live index, whatever the legacy batches hold.
/// The backfill's candidate is sparse, so its maximum (offset 1 here) has
/// no entry of its own. The merge must still drop the live entry (offset
/// 3), which is below that maximum: kept, it would be the floor of a lookup
/// between the two, and a scan from offset 3 would miss offset 1.
#[tokio::test]
async fn backfill_merge_drops_a_live_entry_below_an_unindexed_legacy_batch() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topition = legacy_partition_with_max_at_offset_1(&storage, "legacy-sparse").await?;

    assert_eq!(3, produce(&storage, &topition, &[T0 + 70]).await?);

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 80).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 100), timestamp);

    assert_eq!(
        BTreeMap::from([(T0, 0)]),
        time_index_entries(&storage, &topition).await?
    );

    Ok(())
}

/// A backfill that cannot read a batch fails the request and leaves the
/// index incomplete, so the next request runs the backfill again. An index
/// committed without offset 1 would answer no match for this target.
#[tokio::test]
async fn backfill_get_failure_leaves_the_index_incomplete() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let storage = DynoStore::new("nisshi", 111, faulty.clone());
    let topition = legacy_partition_with_max_at_offset_1(&storage, "backfill-failure").await?;

    faulty.arm(storage.batch_path(&topition, 1), Fault::Unavailable)?;

    assert!(
        list_offsets_timestamp(&storage, &topition, T0 + 60)
            .await
            .is_err()
    );
    assert!(!time_index_complete(&storage, &topition).await?);

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 60).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 100), timestamp);
    assert!(time_index_complete(&storage, &topition).await?);

    Ok(())
}

/// A backfill skips a batch that was deleted after the LIST, and completes
/// the index.
#[tokio::test]
async fn backfill_skips_a_batch_deleted_after_listing() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let storage = DynoStore::new("nisshi", 111, faulty.clone());
    let topition = legacy_partition_with_max_at_offset_1(&storage, "backfill-not-found").await?;

    faulty.arm(storage.batch_path(&topition, 2), Fault::NotFound)?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 60).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 100), timestamp);
    assert!(time_index_complete(&storage, &topition).await?);

    Ok(())
}

/// A scan that cannot read a batch fails the request, instead of answering
/// a later offset from the next batch.
#[tokio::test]
async fn scan_get_failure_fails_the_request() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let storage = DynoStore::new("nisshi", 111, faulty.clone());
    let topic = "scan-failure";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    assert_eq!(0, produce(&storage, &topition, &[T0]).await?);
    assert_eq!(1, produce(&storage, &topition, &[T0 + 50]).await?);
    assert_eq!(2, produce(&storage, &topition, &[T0 + 100]).await?);

    faulty.arm(storage.batch_path(&topition, 1), Fault::Unavailable)?;

    assert!(
        list_offsets_timestamp(&storage, &topition, T0 + 25)
            .await
            .is_err()
    );

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 25).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 50), timestamp);

    Ok(())
}

/// Concurrent lookups on one partition in one process share one backfill:
/// the backfill reads each of the three batches once, and each lookup's
/// scan then reads the one batch that holds its match.
#[tokio::test]
async fn concurrent_lookups_share_one_backfill() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let storage = DynoStore::new("nisshi", 111, faulty.clone());
    let topition = legacy_partition_with_max_at_offset_1(&storage, "single-flight").await?;

    let before = faulty.batch_gets();

    let (first, second) = tokio::join!(
        list_offsets_timestamp(&storage, &topition, T0),
        list_offsets_timestamp(&storage, &topition, T0),
    );
    assert_eq!((Some(0), Some(T0)), first?);
    assert_eq!((Some(0), Some(T0)), second?);

    assert_eq!(3 + 2, faulty.batch_gets() - before);

    Ok(())
}
