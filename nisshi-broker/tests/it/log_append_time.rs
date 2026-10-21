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

//! The topic's `message.timestamp.type` selects the timestamp of each stored
//! record. On a `LogAppendTime` topic the broker stamps each batch with its own
//! clock. On a `CreateTime` topic it keeps the producer's timestamps, also for
//! a batch that the producer marks `LogAppendTime`.

use crate::{
    common::{StorageType, init_tracing},
    produce::{
        create_time_batch, create_topic_with_configs, fetch_batches, now_ms, produce_batches,
    },
};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    BatchAttribute, Compression, ErrorCode, TimestampType,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{ArcDynStorage, Storage};
use rand::{RngExt as _, rng};
use std::time::Duration;
use url::Url;
use uuid::Uuid;

const PARTITION: i32 = 0;

const LOG_APPEND_TIME: (&str, &str) = ("message.timestamp.type", "LogAppendTime");

const TEN_YEARS_MS: i64 = 10 * 365 * 24 * 60 * 60 * 1000;

async fn storage(storage_type: StorageType) -> Result<ArcDynStorage> {
    crate::common::storage_container(
        storage_type,
        Uuid::now_v7(),
        rng().random_range(0..i32::MAX),
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

/// A batch with one record `timestamp_delta` after `base_timestamp`, marked
/// `LogAppendTime` by the producer.
fn marked_log_append_time(base_timestamp: i64, timestamp_delta: i64) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .attributes(
            BatchAttribute::default()
                .timestamp(TimestampType::LogAppendTime)
                .into(),
        )
        .base_timestamp(base_timestamp)
        .max_timestamp(base_timestamp + timestamp_delta)
        .record(
            Record::builder()
                .value(Bytes::from_static(b"marked log append time").into())
                .timestamp_delta(timestamp_delta),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// The produce response and every fetched record carry the broker's clock at
/// the time of the produce, not the producer's timestamps, and the stored
/// batch is marked `LogAppendTime`.
async fn stamps_each_record_with_the_broker_clock(storage: impl Storage + Clone) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[LOG_APPEND_TIME]).await?;
    let a_day_ago = now_ms()? - 24 * 60 * 60 * 1000;

    let before = now_ms()?;
    let response = produce_batches(
        storage.clone(),
        &topic,
        PARTITION,
        vec![create_time_batch(
            Compression::Gzip,
            a_day_ago,
            -1,
            &[0, 5, 10],
        )?],
    )
    .await?;
    let after = now_ms()?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

    let log_append_time = response
        .log_append_time_ms
        .expect("a produce response carries log_append_time_ms");
    assert!(
        (before..=after).contains(&log_append_time),
        "{log_append_time} is not between {before} and {after}"
    );

    let fetched = fetch_batches(storage, &topic, PARTITION).await?;
    assert_eq!(1, fetched.len());
    assert_eq!(fetched[0].computed_crc(), fetched[0].crc);

    let batch = inflated::Batch::try_from(&fetched[0])?;
    assert_eq!(
        TimestampType::LogAppendTime,
        TimestampType::from(batch.attributes)
    );
    assert_eq!(3, batch.records.len());

    for record in &batch.records {
        assert_eq!(log_append_time, batch.record_timestamp(record));
    }

    Ok(())
}

/// Each record of a fetch carries the broker's clock at the produce of its own
/// batch, also when two batches from different produce requests come back in
/// one fetch.
async fn each_batch_keeps_its_own_broker_timestamp(storage: impl Storage + Clone) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[LOG_APPEND_TIME]).await?;

    let mut log_append_times = vec![];

    for _ in 0..2 {
        let response = produce_batches(
            storage.clone(),
            &topic,
            PARTITION,
            vec![create_time_batch(
                Compression::None,
                now_ms()?,
                -1,
                &[0, 1],
            )?],
        )
        .await?;
        assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

        log_append_times.push(
            response
                .log_append_time_ms
                .expect("a produce response carries log_append_time_ms"),
        );

        // The two batches must get different times.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut record_timestamps = vec![];

    for fetched in fetch_batches(storage, &topic, PARTITION).await? {
        let batch = inflated::Batch::try_from(&fetched)?;

        for record in &batch.records {
            record_timestamps.push(batch.record_timestamp(record));
        }
    }

    assert_eq!(
        vec![
            log_append_times[0],
            log_append_times[0],
            log_append_times[1],
            log_append_times[1]
        ],
        record_timestamps
    );

    Ok(())
}

/// The broker replaces each record timestamp on a `LogAppendTime` topic, so it
/// does not check them against the `CreateTime` timestamp window.
async fn accepts_a_record_outside_the_timestamp_window(
    storage: impl Storage + Clone,
) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[LOG_APPEND_TIME]).await?;

    let response = produce_batches(
        storage,
        &topic,
        PARTITION,
        vec![create_time_batch(
            Compression::None,
            now_ms()?,
            -1,
            &[TEN_YEARS_MS],
        )?],
    )
    .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    assert_eq!(0, response.base_offset);

    Ok(())
}

/// On a `CreateTime` topic, the broker validates a batch that the producer
/// marks `LogAppendTime` like any other batch, and stores it as `CreateTime`,
/// as Kafka does.
async fn create_time_topic_validates_a_batch_marked_log_append_time(
    storage: impl Storage + Clone,
) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[]).await?;
    let now = now_ms()?;

    let rejected = produce_batches(
        storage.clone(),
        &topic,
        PARTITION,
        vec![marked_log_append_time(now, TEN_YEARS_MS)?],
    )
    .await?;
    assert_eq!(
        ErrorCode::InvalidTimestamp,
        ErrorCode::try_from(rejected.error_code)?
    );

    let accepted = produce_batches(
        storage.clone(),
        &topic,
        PARTITION,
        vec![marked_log_append_time(now, 7)?],
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(accepted.error_code)?);
    assert_eq!(0, accepted.base_offset);
    assert_eq!(Some(-1), accepted.log_append_time_ms);

    let fetched = fetch_batches(storage, &topic, PARTITION).await?;
    assert_eq!(1, fetched.len());

    let batch = inflated::Batch::try_from(&fetched[0])?;
    assert_eq!(
        TimestampType::CreateTime,
        TimestampType::from(batch.attributes)
    );
    assert_eq!(1, batch.records.len());
    assert_eq!(now + 7, batch.record_timestamp(&batch.records[0]));

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    #[tokio::test]
    async fn stamps_each_record_with_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::stamps_each_record_with_the_broker_clock(storage(StorageType::InMemory).await?).await
    }

    #[tokio::test]
    async fn each_batch_keeps_its_own_broker_timestamp() -> Result<()> {
        let _guard = init_tracing()?;
        super::each_batch_keeps_its_own_broker_timestamp(storage(StorageType::InMemory).await?)
            .await
    }

    #[tokio::test]
    async fn accepts_a_record_outside_the_timestamp_window() -> Result<()> {
        let _guard = init_tracing()?;
        super::accepts_a_record_outside_the_timestamp_window(storage(StorageType::InMemory).await?)
            .await
    }

    #[tokio::test]
    async fn create_time_topic_validates_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_time_topic_validates_a_batch_marked_log_append_time(
            storage(StorageType::InMemory).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    #[tokio::test]
    async fn stamps_each_record_with_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::stamps_each_record_with_the_broker_clock(storage(StorageType::Lite).await?).await
    }

    #[tokio::test]
    async fn each_batch_keeps_its_own_broker_timestamp() -> Result<()> {
        let _guard = init_tracing()?;
        super::each_batch_keeps_its_own_broker_timestamp(storage(StorageType::Lite).await?).await
    }

    #[tokio::test]
    async fn accepts_a_record_outside_the_timestamp_window() -> Result<()> {
        let _guard = init_tracing()?;
        super::accepts_a_record_outside_the_timestamp_window(storage(StorageType::Lite).await?)
            .await
    }

    #[tokio::test]
    async fn create_time_topic_validates_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_time_topic_validates_a_batch_marked_log_append_time(
            storage(StorageType::Lite).await?,
        )
        .await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    #[tokio::test]
    async fn stamps_each_record_with_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::stamps_each_record_with_the_broker_clock(storage(StorageType::Postgres).await?).await
    }

    #[tokio::test]
    async fn each_batch_keeps_its_own_broker_timestamp() -> Result<()> {
        let _guard = init_tracing()?;
        super::each_batch_keeps_its_own_broker_timestamp(storage(StorageType::Postgres).await?)
            .await
    }

    #[tokio::test]
    async fn accepts_a_record_outside_the_timestamp_window() -> Result<()> {
        let _guard = init_tracing()?;
        super::accepts_a_record_outside_the_timestamp_window(storage(StorageType::Postgres).await?)
            .await
    }

    #[tokio::test]
    async fn create_time_topic_validates_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_time_topic_validates_a_batch_marked_log_append_time(
            storage(StorageType::Postgres).await?,
        )
        .await
    }
}

#[cfg(feature = "slatedb")]
mod slate {
    use super::*;

    #[tokio::test]
    async fn stamps_each_record_with_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::stamps_each_record_with_the_broker_clock(storage(StorageType::SlateDb).await?).await
    }

    #[tokio::test]
    async fn each_batch_keeps_its_own_broker_timestamp() -> Result<()> {
        let _guard = init_tracing()?;
        super::each_batch_keeps_its_own_broker_timestamp(storage(StorageType::SlateDb).await?).await
    }

    #[tokio::test]
    async fn accepts_a_record_outside_the_timestamp_window() -> Result<()> {
        let _guard = init_tracing()?;
        super::accepts_a_record_outside_the_timestamp_window(storage(StorageType::SlateDb).await?)
            .await
    }

    #[tokio::test]
    async fn create_time_topic_validates_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_time_topic_validates_a_batch_marked_log_append_time(
            storage(StorageType::SlateDb).await?,
        )
        .await
    }
}
