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

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use common::{StorageType, alphanumeric_string, init_tracing, register_broker};
use nisshi_broker::{Error, Result, service::storage};
use nisshi_sans_io::{
    Ack, CreateTopicsRequest, ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest,
    ProduceRequest,
    create_topics_request::CreatableTopic,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
    to_system_time, to_timestamp,
};
use nisshi_service::{
    BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
    FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService,
};
use nisshi_storage::{Storage, Topition};
use rama::{Context, Layer as _, Service};
use rand::{prelude::*, rng};
use tokio::time::sleep;
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub mod common;

type Broker = RequestFrameService<
    FrameBytesService<BytesService<BytesFrameService<FrameRouteService<(), Error>>>>,
>;

fn broker<S>(storage: S) -> Result<Broker>
where
    S: Storage + Clone,
{
    storage::services(FrameRouteService::<(), Error>::builder(), storage)
        .inspect(|builder| debug!(?builder))
        .and_then(|builder| builder.build().map_err(Into::into))
        .map(|frame_route| {
            (
                RequestFrameLayer,
                FrameBytesLayer,
                BytesLayer,
                BytesFrameLayer::default(),
            )
                .into_layer(frame_route)
        })
}

pub async fn multiple_record(broker: Broker) -> Result<()> {
    let topic_name = &alphanumeric_string(15)[..];
    debug!(?topic_name);

    let timeout = 5_000;
    let num_partitions = 6;
    let replication_factor = 0;

    let response = broker
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .timeout_ms(timeout)
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .num_partitions(num_partitions)
                        .configs(Some([].into()))
                        .assignments(Some([].into()))
                        .replication_factor(replication_factor)
                        .name(topic_name.into())]
                    .into(),
                )),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(i16::from(ErrorCode::None), topics[0].error_code);

    let replica_id = -1;
    let max_num_offsets = Some(6);
    let current_leader_epoch = -1;

    debug!(phase = "empty topic: uncommitted latest offset");
    let isolation = Some(IsolationLevel::ReadUncommitted.into());
    let timestamp = ListOffset::Latest.try_into()?;

    let response = broker
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    for partition in partitions {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "empty topic: uncommitted earliest offset");
    let timestamp = ListOffset::Earliest.try_into()?;

    let response = broker
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    for partition in partitions {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    sleep(Duration::from_millis(500)).await;
    let first = SystemTime::now();

    let frame = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"one"))))
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let partition = 0;

    let response = broker
        .serve(
            Context::default(),
            ProduceRequest::default()
                .timeout_ms(timeout)
                .acks(Ack::Leader.into())
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(topic_name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(partition)
                                .records(Some(frame))]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await
        .inspect(|response| debug!("{response:?}"))?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(partition, partitions[0].index);
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(0, partitions[0].base_offset);

    sleep(Duration::from_millis(500)).await;
    let second = SystemTime::now();

    let frame = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"two"))))
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let response = broker
        .serve(
            Context::default(),
            ProduceRequest::default()
                .timeout_ms(timeout)
                .acks(Ack::Leader.into())
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(topic_name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(partition)
                                .records(Some(frame))]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await
        .inspect(|response| debug!("{response:?}"))?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(partition, partitions[0].index);
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(1, partitions[0].base_offset);

    sleep(Duration::from_millis(500)).await;
    let third = SystemTime::now();

    let frame = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"three"))))
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let response = broker
        .serve(
            Context::default(),
            ProduceRequest::default()
                .timeout_ms(timeout)
                .acks(Ack::Leader.into())
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(topic_name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(partition)
                                .records(Some(frame))]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await
        .inspect(|response| debug!("{response:?}"))?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(partition, partitions[0].index);
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(2, partitions[0].base_offset);

    debug!(phase = "topic: uncommitted earliest offset");
    let timestamp = ListOffset::Earliest.try_into()?;

    let response = broker
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(0), partitions[0].offset);
    assert!(
        partitions[0]
            .timestamp
            .is_some_and(|timestamp| timestamp > 0)
    );

    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "topic: uncommitted latest offset");
    let timestamp = ListOffset::Latest.try_into()?;

    let response = broker
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(3), partitions[0].offset);
    assert!(
        partitions[0]
            .timestamp
            .is_some_and(|timestamp| timestamp > 0)
    );

    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "first offset");
    let timestamp = ListOffset::Timestamp(first)
        .try_into()
        .inspect(|first| debug!(?first))?;

    let response = broker
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(0), partitions[0].offset);
    assert!(
        partitions[0]
            .timestamp
            .is_some_and(|timestamp| timestamp > 0)
    );

    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "second offset");
    let timestamp = ListOffset::Timestamp(second)
        .try_into()
        .inspect(|second| debug!(?second))?;

    let response = broker
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(1), partitions[0].offset);
    assert!(
        partitions[0]
            .timestamp
            .is_some_and(|timestamp| timestamp > 0)
    );

    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "third");
    let timestamp = ListOffset::Timestamp(third)
        .try_into()
        .inspect(|third| debug!(?third))?;

    let response = broker
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(2), partitions[0].offset);
    assert!(
        partitions[0]
            .timestamp
            .is_some_and(|timestamp| timestamp > 0)
    );

    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    Ok(())
}

pub async fn new_topic<G>(cluster_id: impl Into<String>, broker_id: i32, sc: G) -> Result<()>
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

    let offsets = (0..num_partitions)
        .map(|partition| {
            (
                Topition::new(topic_name.clone(), partition),
                ListOffset::Latest,
            )
        })
        .collect::<Vec<_>>();

    let items = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await
        .inspect(|response| debug!(?response))?;

    assert!(!items.is_empty());

    for (_toptition, response) in items {
        assert_eq!(Some(0), response.offset);
        assert_eq!(None, response.timestamp);
    }

    let offsets = (0..num_partitions)
        .map(|partition| {
            (
                Topition::new(topic_name.clone(), partition),
                ListOffset::Earliest,
            )
        })
        .collect::<Vec<_>>();

    let items = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await
        .inspect(|response| debug!(?response))?;

    assert!(!items.is_empty());

    for (_toptition, response) in items {
        assert_eq!(Some(0), response.offset);
        assert_eq!(None, response.timestamp);
    }

    let timestamp = to_system_time(0)?;

    let offsets = (0..num_partitions)
        .map(|partition| {
            (
                Topition::new(topic_name.clone(), partition),
                ListOffset::Timestamp(timestamp),
            )
        })
        .collect::<Vec<_>>();

    let items = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await
        .inspect(|response| debug!(?response))?;

    assert!(!items.is_empty());

    for (_toptition, response) in items {
        assert_eq!(Some(0), response.offset);
        assert_eq!(None, response.timestamp);
    }

    Ok(())
}

pub async fn single_record<G>(cluster_id: impl Into<String>, broker_id: i32, sc: G) -> Result<()>
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

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

    let before = SystemTime::now();
    debug!(before = to_timestamp(&before)?);
    sleep(Duration::from_millis(500)).await;

    let batch = inflated::Batch::builder()
        .record(Record::builder().value(value.clone().into()))
        .build()
        .and_then(TryInto::try_into)
        .inspect(|deflated| debug!(?deflated))?;

    assert_eq!(
        0,
        sc.produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?
    );

    let after = SystemTime::now();
    debug!(after = to_timestamp(&after)?);

    let offsets = [(topition.clone(), ListOffset::Latest)];

    let responses = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await
        .inspect(|responses| debug!(?responses))?;

    assert_eq!(1, responses.len());
    assert_eq!(Some(1), responses[0].1.offset);

    let offsets = [(topition.clone(), ListOffset::Earliest)];

    let responses = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await
        .inspect(|responses| debug!(?responses))?;

    assert_eq!(1, responses.len());
    assert_eq!(Some(0), responses[0].1.offset);

    let offsets = [(topition.clone(), ListOffset::Timestamp(before))];

    let responses = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await?;

    assert_eq!(1, responses.len());
    assert_eq!(Some(0), responses[0].1.offset);

    let offsets = [(topition.clone(), ListOffset::Timestamp(after))];

    let responses = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await?;

    assert_eq!(1, responses.len());
    assert_eq!(Some(0), responses[0].1.offset);

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
    async fn multiple_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multiple_record(broker).await
    }

    #[tokio::test]
    async fn new_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::new_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_record(
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
    async fn multiple_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multiple_record(broker).await
    }

    #[tokio::test]
    async fn new_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::new_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_record(
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
    async fn multiple_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multiple_record(broker).await
    }

    #[tokio::test]
    async fn new_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::new_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_record(
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
    async fn multiple_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multiple_record(broker).await
    }

    #[tokio::test]
    async fn new_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::new_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_record(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
