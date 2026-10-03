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

//! Tests for the `time_index`-backed `ListOffsets(Timestamp)` implementation
//! (SOL-155076). Calls `DynoStore`'s private helpers directly (this module is
//! a descendant of `dynostore`, so it can see them) rather than only going
//! through the `Storage` trait, because some of the scenarios below need to
//! control the exact interleaving between a backfill's read and its write.

use std::collections::BTreeMap;

use crate::dynostore::{DynoStore, Watermark, tests::init_tracing};
use bytes::Bytes;
use nisshi_sans_io::{
    IsolationLevel, ListOffset, create_topics_request::CreatableTopic, to_system_time, to_timestamp,
};
use nisshi_storage::{Error, Result, Storage, Topition};
use object_store::{Attributes, PutMode, PutOptions, memory::InMemory};

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
    let responses = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
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
/// which is why `produce` carries no such guard. See SOL-155076.
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
        w.time_index = None;
        w.time_index_complete = false;
        Ok(())
    })
    .await?;

    // Step 1: collect a candidate from the pre-concurrent-produce state.
    let candidate = storage.collect_time_index_candidate(&topition).await?;
    assert_eq!(BTreeMap::from([(T0, 0)]), candidate);

    // Step 2: a concurrent produce lands before the backfill commits, with a
    // new running max above everything seen so far.
    let concurrent_timestamp = T0 + 50;
    let offset = produce(&storage, &topition, &[concurrent_timestamp]).await?;
    assert_eq!(2, offset);

    // Step 3: commit the now-stale candidate.
    let merged = storage
        .commit_time_index_backfill(&topition, &candidate)
        .await?;
    assert_eq!(BTreeMap::from([(T0, 0), (concurrent_timestamp, 2)]), merged);

    // Step 4: the concurrent produce's entry must still be answerable.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 25).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(concurrent_timestamp), timestamp);

    Ok(())
}

/// A plain `BTreeMap` union of a backfill candidate and whatever is live
/// would let offset 2's lower timestamp outrank offset 0's
/// already-established higher one, corrupting the ceiling lookup. The
/// correct merge (replay the monotonic rule over the union, sorted by
/// offset) must answer offset 0, not offset 2. See SOL-155076.
#[tokio::test]
async fn merge_does_not_let_a_lower_concurrent_entry_outrank_an_established_one() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage();
    let topic = "legacy-union";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    write_legacy_batch(&storage, &topition, 0, T0).await?; // the eventual ceiling
    write_legacy_batch(&storage, &topition, 1, T0 - 50).await?;

    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(2);
        w.time_index = None;
        w.time_index_complete = false;
        Ok(())
    })
    .await?;

    // A post-upgrade produce lands before any read ever triggers a backfill:
    // `time_index` is still `None` at this point, so this insert goes into a
    // fresh map, exactly the scenario that makes a plain union wrong.
    let offset = produce(&storage, &topition, &[T0 - 20]).await?;
    assert_eq!(2, offset);

    // The first read now triggers the backfill. `target = 70` is below both
    // off1 (T0 - 50) and off2 (T0 - 20), relative to off0's T0 - so the only
    // correct ceiling is off0.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 - 70).await?;
    assert_eq!(Some(0), offset);
    assert_eq!(Some(T0), timestamp);

    Ok(())
}

/// A `low > 0` straddling batch: one batch holds records at offsets 0, 1 and
/// 2, and `DeleteRecords` (simulated here by setting `low` directly, since
/// `delete_records` is still `todo!()` on this branch) has logically removed
/// offsets 0 and 1. A Timestamp lookup must skip the deleted prefix and only
/// match a record at or after `low`, even though the matching batch's own
/// base offset (0) is below `low`. See SOL-155076.
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
    // gone, but `time_index` is never pruned, so the index still points at
    // the batch's base offset (0).
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
/// match", found directly from a complete, non-empty index with no entry
/// reaching the target - no scan required.
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
        .with(&storage.object_store, |w| Ok(w.time_index_complete))
        .await?;
    assert!(complete);

    Ok(())
}
