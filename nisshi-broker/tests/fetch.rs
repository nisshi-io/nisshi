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

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use common::{StorageType, alphanumeric_string, init_tracing, register_broker};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, FetchRequest, FetchResponse, IsolationLevel, ListOffset, NULL_TOPIC_ID,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    fetch_request::{FetchPartition, FetchTopic},
    record::{Header, Record, inflated},
};
use nisshi_storage::{FetchService, ListOffsetResponse, Storage, Topition};
use rama::{Context, Service};
use rand::{prelude::*, rng};
use tracing::{debug, error};
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn empty_topic<G>(cluster_id: Uuid, broker_id: i32, sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let record_count = 0;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(vec![
            FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None),
        ]))];

    let ctx = Context::with_state(sc);

    let fetch = FetchService
        .serve(
            ctx,
            FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
        )
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    for response in fetch.responses.as_ref().unwrap_or(&vec![]) {
        for partition in response.partitions.as_ref().unwrap_or(&vec![]) {
            for batch in &partition.records.as_ref().unwrap().batches {
                assert_eq!(-1, batch.producer_id);
                assert_eq!(-1, batch.producer_epoch);
            }
        }
    }

    assert_eq!(
        record_count,
        fetch
            .responses
            .unwrap_or_default()
            .iter()
            .map(|response| {
                response
                    .partitions
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(|partition| {
                        partition
                            .records
                            .as_ref()
                            .unwrap()
                            .batches
                            .iter()
                            .map(|batch| batch.record_count as i64)
                            .sum::<i64>()
                    })
                    .sum::<i64>()
            })
            .sum::<i64>()
    );

    Ok(())
}

pub async fn kv_header(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let record_count = 6;
    let header_count = 3;

    let mut messages = BTreeMap::new();

    let random_data = || Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

    for n in 0..record_count {
        let message_key = random_data();
        let message_value = random_data();

        let headers = (0..header_count)
            .map(|_| (random_data(), random_data()))
            .collect::<Vec<_>>();

        let batch = inflated::Batch::builder()
            .record(
                headers.iter().cloned().fold(
                    Record::builder()
                        .key(message_key.clone().into())
                        .value(message_value.clone().into()),
                    |builder, (key, value)| builder.header(Header::builder().key(key).value(value)),
                ),
            )
            .producer_id(-1)
            .producer_epoch(-1)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        debug!(n, ?batch);

        let offset = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?;

        debug!(offset);

        assert_eq!(
            None,
            messages.insert(
                offset,
                FullFatRecord {
                    key: Some(message_key),
                    value: Some(message_value),
                    headers: headers
                        .iter()
                        .cloned()
                        .map(|(key, value)| Header::builder().key(key).value(value).build())
                        .collect(),
                }
            )
        );
    }

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let fetch_offset = 0;

    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(
            [FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(fetch_offset)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None)]
            .into(),
        ))];

    let ctx = Context::with_state(sc);

    let fetched = FetchService
        .serve(
            ctx,
            FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
        )
        .await
        .map(records)?;

    assert_eq!(messages.len(), fetched.len());

    for (offset, record) in fetched {
        assert_eq!(
            messages
                .get(&offset)
                .and_then(|message| message.key.clone())
                .unwrap_or_default(),
            record.key.clone().unwrap_or_default()
        );

        assert_eq!(
            messages
                .get(&offset)
                .and_then(|message| message.value.clone())
                .unwrap_or_default(),
            record.value.clone().unwrap_or_default()
        );

        assert_eq!(
            messages
                .get(&offset)
                .map(|message| message.headers.clone())
                .unwrap_or_default()
                .iter()
                .collect::<BTreeSet<_>>(),
            record.headers.clone().iter().collect::<BTreeSet<_>>()
        );
    }

    Ok(())
}

impl From<Record> for FullFatRecord {
    fn from(record: Record) -> Self {
        Self {
            key: record.key,
            value: record.value,
            headers: record.headers,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FullFatRecord {
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<Header>,
}

fn records(fetch: FetchResponse) -> BTreeMap<i64, FullFatRecord> {
    fetch
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| {
            topic
                .partitions
                .unwrap_or_default()
                .into_iter()
                .flat_map(|partition| {
                    partition
                        .records
                        .map(|records| records.batches)
                        .unwrap_or_default()
                })
        })
        .filter_map(|deflated| inflated::Batch::try_from(deflated).ok())
        .flat_map(|batch| {
            batch.records.into_iter().map(move |record| {
                (
                    batch.base_offset + (record.offset_delta as i64),
                    record.into(),
                )
            })
        })
        .collect()
}

// regression test for headers being attached to the wrong record when the
// first record visible to a fetch is not the record at the requested fetch
// offset (here because an earlier record sharing the same key has been
// removed by `cleanup.policy=compact`)
pub async fn compacted_header(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 1;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some(
        [CreatableTopicConfig::default()
            .name("cleanup.policy".into())
            .value(Some("compact".into()))]
        .into(),
    );

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments)
                .configs(configs),
            false,
        )
        .await?;
    debug!(?topic_id);

    let topition = Topition::new(topic_name.clone(), 0);

    let key = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

    let compacted_away = vec![
        Header::builder()
            .key(Bytes::from_static(b"version"))
            .value(Bytes::from_static(b"compacted-away"))
            .build(),
    ];

    let retained = vec![
        Header::builder()
            .key(Bytes::from_static(b"version"))
            .value(Bytes::from_static(b"retained"))
            .build(),
    ];

    // two records sharing the same key: the first (offset 0) is removed by
    // compaction, leaving only the second (offset 1)
    for (value, headers) in [
        (Bytes::from_static(b"older"), &compacted_away),
        (Bytes::from_static(b"newer"), &retained),
    ] {
        let batch = inflated::Batch::builder()
            .record(headers.iter().cloned().fold(
                Record::builder().key(Some(key.clone())).value(Some(value)),
                |builder, header| builder.header(header.into()),
            ))
            .producer_id(-1)
            .producer_epoch(-1)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        _ = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?;
    }

    sc.maintain(SystemTime::now())
        .await
        .inspect_err(|err| error!(?err))?;

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let fetch_offset = 0;

    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(
            [FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(fetch_offset)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None)]
            .into(),
        ))];

    let ctx = Context::with_state(sc);

    let fetched = FetchService
        .serve(
            ctx,
            FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
        )
        .await
        .map(records)?;

    // the record at offset 0 has been compacted away, leaving only the
    // record at offset 1, which must be fetched with its own headers
    assert_eq!(1, fetched.len());

    let (offset, record) = fetched.into_iter().next().expect("single record");
    assert_eq!(1, offset);
    assert_eq!(Some(key), record.key);
    assert_eq!(Some(Bytes::from_static(b"newer")), record.value);
    assert_eq!(retained, record.headers);

    Ok(())
}

pub async fn simple_non_txn<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let list_offsets = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await
        .inspect_err(|err| error!(?err))
        .inspect(|list_offsets| debug!(?list_offsets))?;

    assert_eq!(1, list_offsets.len());

    assert!(matches!(
        list_offsets[0].1,
        ListOffsetResponse {
            offset: Some(0),
            ..
        }
    ));

    let record_count = 6;

    let mut offset_producer = BTreeMap::new();
    let mut bytes = 0;

    for n in 0..record_count {
        let producer = sc
            .init_producer(None, transaction_timeout_ms, Some(-1), Some(-1))
            .await?;

        let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
        bytes += value.len();

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(value.clone().into()))
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        debug!(n, bytes, ?batch);

        let offset = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?;

        debug!(offset);

        assert_eq!(None, offset_producer.insert(offset, (producer, value)));
    }

    let list_offsets = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await
        .inspect_err(|err| error!(?err))
        .inspect(|list_offsets| debug!(?list_offsets))?;

    assert_eq!(1, list_offsets.len());

    assert!(matches!(
        list_offsets[0].1,
        ListOffsetResponse {
            offset: Some(records),
            ..
            } if records == record_count
    ));

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(
            [FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None)]
            .into(),
        ))];

    let ctx = Context::with_state(sc);

    let fetch = FetchService
        .serve(
            ctx,
            FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
        )
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    assert_eq!(
        record_count,
        fetch
            .responses
            .unwrap_or_default()
            .iter()
            .map(|response| {
                response
                    .partitions
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(|partition| {
                        partition
                            .records
                            .as_ref()
                            .unwrap()
                            .batches
                            .iter()
                            .map(|batch| batch.record_count as i64)
                            .sum::<i64>()
                    })
                    .sum::<i64>()
            })
            .sum::<i64>()
    );

    Ok(())
}

/// A fetch offset falling inside a batch returns that batch whole, as
/// Kafka does: the client skips the records below the fetch offset. Only
/// batches ending before the fetch offset may be omitted.
pub async fn mid_batch<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;
    debug!(?topic_id);

    let topition = Topition::new(topic_name.clone(), 0);

    let values: Vec<Bytes> = (0..6)
        .map(|_| Bytes::copy_from_slice(alphanumeric_string(15).as_bytes()))
        .collect();

    // two batches of three records: offsets 0..=2 and 3..=5
    for batch_values in values.chunks(3) {
        let producer = sc.init_producer(None, 10_000, Some(-1), Some(-1)).await?;

        let mut builder = inflated::Batch::builder()
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .last_offset_delta(batch_values.len() as i32 - 1);

        for (delta, value) in batch_values.iter().enumerate() {
            builder = builder.record(
                Record::builder()
                    .offset_delta(delta as i32)
                    .value(Some(value.clone())),
            );
        }

        let batch = builder.build().and_then(TryInto::try_into)?;

        _ = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(offset))?;
    }

    let min_bytes = 1;
    let max_bytes = 50 * 1024;
    let isolation = IsolationLevel::ReadUncommitted;
    let max_wait = Duration::from_millis(500);

    let high_watermark = values.len() as i64;

    for fetch_offset in 0..=high_watermark {
        let batches = sc
            .fetch(
                &topition,
                fetch_offset,
                min_bytes,
                max_bytes,
                isolation,
                max_wait,
            )
            .await
            .inspect_err(|err| error!(?err, fetch_offset))?
            .into_iter()
            .try_fold(Vec::new(), |mut acc, batch| {
                inflated::Batch::try_from(batch)
                    .map(|inflated| {
                        acc.push(inflated);
                        acc
                    })
                    .map_err(nisshi_broker::Error::from)
            })?;

        // every record from the fetch offset to the high watermark is
        // returned, in order, and no returned batch ends before the
        // fetch offset
        let mut expected = fetch_offset;

        for batch in &batches {
            // the pg and lite engines return an empty placeholder batch
            // when there are no records to fetch
            if batch.records.is_empty() {
                continue;
            }

            assert!(
                batch.base_offset + i64::from(batch.last_offset_delta) >= fetch_offset,
                "fetch at {fetch_offset} returned a batch ending before it"
            );

            for record in &batch.records {
                let offset = batch.base_offset + i64::from(record.offset_delta);

                // a whole batch can begin before the fetch offset: the
                // client skips the records below it
                if offset < fetch_offset {
                    continue;
                }

                assert_eq!(expected, offset, "fetch at {fetch_offset}");
                assert_eq!(
                    values.get(offset as usize).cloned(),
                    record.value(),
                    "fetch at {fetch_offset}, record at {offset}"
                );

                expected += 1;
            }
        }

        assert_eq!(
            high_watermark, expected,
            "fetch at {fetch_offset} is missing records"
        );
    }

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use std::sync::Arc;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn kv_header() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::kv_header(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn mid_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::mid_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use std::sync::Arc;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn kv_header() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::kv_header(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn mid_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::mid_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use std::sync::Arc;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::Lite,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn kv_header() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::kv_header(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn compacted_header() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::compacted_header(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn mid_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::mid_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use std::sync::Arc;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::SlateDb,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn kv_header() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::kv_header(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn mid_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::mid_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
