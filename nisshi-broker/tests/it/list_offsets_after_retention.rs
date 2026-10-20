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

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use bytes::Bytes;
use nisshi_broker::{Error, Result, service::storage};
use nisshi_sans_io::{
    Ack, BatchAttribute, CreateTopicsRequest, ErrorCode, IsolationLevel, ListOffset,
    ListOffsetsRequest, ProduceRequest, RequestInput,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use nisshi_service::{
    BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
    FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService,
};
use nisshi_storage::{Storage, Topition, TxnAddPartitionsRequest};
use rama::{Layer as _, Service, extensions::Extensions};
use rand::{prelude::*, rng};
use tracing::debug;
use url::Url;
use uuid::Uuid;

type Broker = RequestFrameService<
    FrameBytesService<BytesService<BytesFrameService<FrameRouteService<Error>>>>,
>;

fn broker<S>(storage: S) -> Result<Broker>
where
    S: Storage + Clone,
{
    storage::services(FrameRouteService::<Error>::builder(), storage)
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

/// How many records a test produces before retention deletes them all, at offsets 0 to 4.
const PRODUCED: i64 = 5;

/// A `retention.ms` of ten minutes, which a maintenance run an hour later has expired.
const TEN_MINUTE_RETENTION: &str = "600000";

/// How far after producing the records a test runs maintenance, past their `retention.ms`.
const AN_HOUR: Duration = Duration::from_secs(3_600);

async fn create_topic(
    broker: &Broker,
    extensions: &Extensions,
    name: &str,
    configs: &[(&str, &str)],
) -> Result<()> {
    let response = broker
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .timeout_ms(5_000)
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .num_partitions(1)
                        .configs(Some(
                            configs
                                .iter()
                                .map(|(name, value)| {
                                    CreatableTopicConfig::default()
                                        .name((*name).into())
                                        .value(Some((*value).into()))
                                })
                                .collect(),
                        ))
                        .assignments(Some([].into()))
                        .replication_factor(1)
                        .name(name.into())]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(i16::from(ErrorCode::None), topics[0].error_code);
    Ok(())
}

async fn produce(
    broker: &Broker,
    extensions: &Extensions,
    name: &str,
    value: Bytes,
) -> Result<i64> {
    let frame = inflated::Batch::builder()
        .record(Record::builder().value(Some(value)))
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let response = broker
        .serve(RequestInput {
            request: ProduceRequest::default()
                .timeout_ms(5_000)
                .acks(Ack::Leader.into())
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(0)
                                .records(Some(frame))]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await
        .inspect(|response| debug!("{response:?}"))?;

    let partition = &response.responses.as_deref().unwrap_or_default()[0]
        .partition_responses
        .as_deref()
        .unwrap_or_default()[0];
    assert_eq!(i16::from(ErrorCode::None), partition.error_code);
    Ok(partition.base_offset)
}

async fn list_offset(
    broker: &Broker,
    extensions: &Extensions,
    name: &str,
    isolation_level: IsolationLevel,
    which: ListOffset,
) -> Result<Option<i64>> {
    let response = broker
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(Some(isolation_level.into()))
                .replica_id(-1)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(name.into())
                        .partitions(Some(
                            [ListOffsetsPartition::default()
                                .partition_index(0)
                                .max_num_offsets(Some(1))
                                .timestamp(which.try_into()?)
                                .current_leader_epoch(Some(-1))]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let partition = &response.topics.as_deref().unwrap_or_default()[0]
        .partitions
        .as_deref()
        .unwrap_or_default()[0];
    assert_eq!(i16::from(ErrorCode::None), partition.error_code);
    Ok(partition.offset)
}

/// When retention deletes every record of a partition, the earliest and the latest offset are both
/// the offset after the last deleted record, and the next record gets that offset.
pub async fn offsets_kept_when_retention_empties_partition<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];

    create_topic(
        &broker,
        &extensions,
        topic,
        &[
            ("cleanup.policy", "delete"),
            ("retention.ms", TEN_MINUTE_RETENTION),
        ],
    )
    .await?;

    for n in 0..PRODUCED {
        let value = Bytes::from(format!("value-{n}"));
        assert_eq!(n, produce(&broker, &extensions, topic, value).await?);
    }

    sc.maintain(SystemTime::now() + AN_HOUR).await?;

    for isolation_level in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
    ] {
        for which in [ListOffset::Earliest, ListOffset::Latest] {
            assert_eq!(
                Some(PRODUCED),
                list_offset(&broker, &extensions, topic, isolation_level, which).await?,
                "{isolation_level:?} {which:?}"
            );
        }
    }

    let value = Bytes::from_static(b"after-retention");
    assert_eq!(PRODUCED, produce(&broker, &extensions, topic, value).await?);

    Ok(())
}

/// A partition that has never held a record answers 0 for its earliest and latest offset.
pub async fn offsets_of_new_partition_are_zero<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];

    create_topic(&broker, &extensions, topic, &[("cleanup.policy", "delete")]).await?;

    for isolation_level in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
    ] {
        for which in [ListOffset::Earliest, ListOffset::Latest] {
            assert_eq!(
                Some(0),
                list_offset(&broker, &extensions, topic, isolation_level, which).await?,
                "{isolation_level:?} {which:?}"
            );
        }
    }

    Ok(())
}

/// An open transaction pins `read_committed` Latest at the transaction's first offset, even after
/// retention deletes the record at that offset. A latest offset above it would let a
/// `read_committed` consumer read past records that the transaction may still abort.
pub async fn open_transaction_pins_read_committed_latest_after_retention<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];
    let topition = Topition::new(topic, 0);

    create_topic(
        &broker,
        &extensions,
        topic,
        &[
            ("cleanup.policy", "delete"),
            ("retention.ms", TEN_MINUTE_RETENTION),
        ],
    )
    .await?;

    assert_eq!(
        0,
        produce(&broker, &extensions, topic, Bytes::from_static(b"before")).await?
    );

    let transaction_id = alphanumeric_string(10);
    let producer = sc
        .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
        .await?;

    _ = sc
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: transaction_id.clone(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name(topic.into())
                    .partitions(Some(vec![0])),
            ],
        })
        .await?;

    let transaction_start = 1;
    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"uncommitted"))))
        .attributes(BatchAttribute::default().transaction(true).into())
        .producer_id(producer.id)
        .producer_epoch(producer.epoch)
        .base_sequence(0)
        .build()
        .and_then(TryInto::try_into)?;
    assert_eq!(
        transaction_start,
        sc.produce(Some(transaction_id.as_str()), &topition, batch)
            .await?
    );

    assert_eq!(
        2,
        produce(&broker, &extensions, topic, Bytes::from_static(b"after")).await?
    );

    sc.maintain(SystemTime::now() + AN_HOUR).await?;

    assert_eq!(
        Some(transaction_start),
        list_offset(
            &broker,
            &extensions,
            topic,
            IsolationLevel::ReadCommitted,
            ListOffset::Latest
        )
        .await?
    );
    assert_eq!(
        Some(3),
        list_offset(
            &broker,
            &extensions,
            topic,
            IsolationLevel::ReadUncommitted,
            ListOffset::Latest
        )
        .await?
    );

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use nisshi_storage::ArcDynStorage;

            use super::*;

            async fn storage_container() -> Result<ArcDynStorage> {
                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                let sc = common::storage_container(
                    $storage_type,
                    cluster_id,
                    broker_id,
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await?;
                register_broker(cluster_id, broker_id, &sc).await?;
                Ok(sc)
            }

            #[tokio::test]
            async fn offsets_kept_when_retention_empties_partition() -> Result<()> {
                let _guard = init_tracing()?;
                super::offsets_kept_when_retention_empties_partition(storage_container().await?)
                    .await
            }

            #[tokio::test]
            async fn open_transaction_pins_read_committed_latest_after_retention() -> Result<()> {
                let _guard = init_tracing()?;
                super::open_transaction_pins_read_committed_latest_after_retention(
                    storage_container().await?,
                )
                .await
            }

            #[tokio::test]
            async fn offsets_of_new_partition_are_zero() -> Result<()> {
                let _guard = init_tracing()?;
                super::offsets_of_new_partition_are_zero(storage_container().await?).await
            }
        }
    };
}

backend!(pg, "postgres", StorageType::Postgres);
backend!(lite, "libsql", StorageType::Lite);
