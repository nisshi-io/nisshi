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

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt as _;
use nisshi_sans_io::{
    ErrorCode, IsolationLevel, ListOffset,
    create_topics_request::CreatableTopic,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    record::{Record, inflated},
};
use nisshi_storage::{Error, Storage as _, Topition};
use object_store::{ObjectStore as _, memory::InMemory, path::Path};

use crate::dynostore::{DynoStore, tests::init_tracing};

/// Returns the base offsets of the batch objects stored under one partition's
/// `records/` prefix, in ascending order.
async fn stored_base_offsets(bucket: &InMemory, topition: &Topition) -> Result<Vec<i64>, Error> {
    let prefix = Path::from(format!(
        "clusters/nisshi/topics/{}/partitions/{:0>10}/records/",
        topition.topic(),
        topition.partition()
    ));

    let mut base_offsets = bucket
        .list(Some(&prefix))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter_map(|location| {
            location
                .parts()
                .next_back()
                .and_then(|name| name.as_ref().get(0..20)?.parse::<i64>().ok())
        })
        .collect::<Vec<_>>();

    base_offsets.sort_unstable();

    Ok(base_offsets)
}

async fn delete_records_partition(
    storage: &DynoStore,
    topic: &str,
    offset: i64,
) -> Result<(ErrorCode, i64), Error> {
    let results = storage
        .delete_records(&[DeleteRecordsTopic::default()
            .name(topic.into())
            .partitions(Some(
                [DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(offset)]
                .into(),
            ))])
        .await?;

    let partitions = results[0].partitions.clone().unwrap_or_default();

    Ok((
        ErrorCode::try_from(partitions[0].error_code)?,
        partitions[0].low_watermark,
    ))
}

/// A DeleteRecords cut removes the batch objects that lie wholly below it,
/// and keeps the object of a batch that straddles the cut. A cut at the
/// high watermark removes every object. The watermark alone does not show
/// this, so the test lists the bucket.
#[tokio::test]
async fn delete_records_removes_batch_objects_below_the_cut() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    let name = "abc";

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(name, 0);

    // single-record batches at offsets 0, 1 and 2
    for _ in 0..3 {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from_static(b"abc"))))
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;
    }

    // a two-record batch at offsets 3 and 4
    let batch = inflated::Batch::builder()
        .last_offset_delta(1)
        .record(
            Record::builder()
                .offset_delta(0)
                .value(Some(Bytes::from_static(b"abc"))),
        )
        .record(
            Record::builder()
                .offset_delta(1)
                .value(Some(Bytes::from_static(b"abc"))),
        )
        .build()
        .and_then(TryInto::try_into)?;

    _ = storage.produce(None, &topition, batch).await?;

    // a single-record batch at offset 5, the last object
    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"abc"))))
        .build()
        .and_then(TryInto::try_into)?;

    _ = storage.produce(None, &topition, batch).await?;

    assert_eq!(
        vec![0, 1, 2, 3, 5],
        stored_base_offsets(&bucket, &topition).await?
    );

    // A cut inside the two-record batch removes the objects wholly below it
    // and keeps that batch whole.
    assert_eq!(
        (ErrorCode::None, 4),
        delete_records_partition(&storage, name, 4).await?
    );
    assert_eq!(vec![3, 5], stored_base_offsets(&bucket, &topition).await?);
    assert_eq!(4, storage.offset_stage(&topition).await?.log_start());

    // A cut to the high watermark removes every batch object, the last one
    // too. Earliest and Latest still answer the high watermark.
    assert_eq!(
        (ErrorCode::None, 6),
        delete_records_partition(&storage, name, -1).await?
    );
    assert_eq!(
        Vec::<i64>::new(),
        stored_base_offsets(&bucket, &topition).await?
    );
    assert_eq!(6, storage.offset_stage(&topition).await?.log_start());

    for list_offset in [ListOffset::Earliest, ListOffset::Latest] {
        let responses = storage
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(topition.clone(), list_offset)],
            )
            .await?;

        assert_eq!(1, responses.len());
        assert_eq!(ErrorCode::None, responses[0].1.error_code);
        assert_eq!(Some(6), responses[0].1.offset, "{list_offset:?}");
    }

    Ok(())
}

/// Creates the topic `name` with one partition in `storage`.
async fn create_topic(storage: &DynoStore, name: &str) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

/// Produces one batch of `records` records to `topition`.
async fn produce_batch(
    storage: &DynoStore,
    topition: &Topition,
    records: i32,
) -> Result<i64, Error> {
    let mut builder = inflated::Batch::builder().last_offset_delta(records - 1);

    for offset_delta in 0..records {
        builder = builder.record(
            Record::builder()
                .offset_delta(offset_delta)
                .value(Some(Bytes::from_static(b"abc"))),
        );
    }

    let batch = builder.build().and_then(TryInto::try_into)?;

    storage.produce(None, topition, batch).await
}

/// A cut below the high watermark keeps the last object, because that
/// batch can hold records at and above the cut.
#[tokio::test]
async fn delete_records_keeps_a_last_batch_that_straddles_the_cut() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    let name = "straddle";
    create_topic(&storage, name).await?;
    let topition = Topition::new(name, 0);

    assert_eq!(0, produce_batch(&storage, &topition, 4).await?);

    assert_eq!(
        (ErrorCode::None, 2),
        delete_records_partition(&storage, name, 2).await?
    );
    assert_eq!(vec![0], stored_base_offsets(&bucket, &topition).await?);

    Ok(())
}

/// A produce that races a cut at the high watermark writes its batch at
/// the cut. The cleanup keeps that batch, although it is the last object
/// and the cut equals the high watermark that the delete read.
#[tokio::test]
async fn delete_records_keeps_a_batch_written_at_the_cut() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    let name = "race";
    create_topic(&storage, name).await?;
    let topition = Topition::new(name, 0);

    assert_eq!(0, produce_batch(&storage, &topition, 6).await?);
    assert_eq!(6, produce_batch(&storage, &topition, 2).await?);

    // The delete read a high watermark of 6, and the batch at 6 was written
    // after that.
    storage.delete_records_batches(&topition, 6, 6).await;

    assert_eq!(vec![6], stored_base_offsets(&bucket, &topition).await?);

    Ok(())
}
