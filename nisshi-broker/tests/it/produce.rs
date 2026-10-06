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

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{BufMut as _, Bytes, BytesMut};
use nisshi_broker::Result;
use nisshi_sans_io::{
    BatchAttribute, Compression, CreateTopicsRequest, DeleteTopicsRequest, ErrorCode, FetchRequest,
    InitProducerIdRequest, IsolationLevel, ListOffset, ListOffsetsRequest, NULL_TOPIC_ID,
    ProduceRequest, ProduceResponse, RequestInput, TimestampType,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    fetch_request::{FetchPartition, FetchTopic},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use nisshi_storage::{
    ArcDynStorage, CreateTopicsService, DeleteTopicsService, FetchService, InitProducerIdService,
    ListOffsetsService, ProduceService, Storage,
};
use rama::{Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use tracing::debug;
use uuid::Uuid;

fn topic_data(
    topic: &str,
    index: i32,
    builder: inflated::Builder,
) -> Result<Option<Vec<TopicProduceData>>> {
    builder
        .build()
        .and_then(deflated::Batch::try_from)
        .map(|deflated| {
            let partition_data =
                PartitionProduceData::default()
                    .index(index)
                    .records(Some(Frame {
                        batches: vec![deflated],
                    }));

            Some(vec![
                TopicProduceData::default()
                    .name(topic.into())
                    .partition_data(Some(vec![partition_data])),
            ])
        })
        .map_err(Into::into)
}

/// The batches stored for `topic`/`index` from offset 0, as the broker wrote
/// them: letting a test inspect a stored header (`max_timestamp`, `crc`)
/// rather than just the produce response's error code.
async fn fetch_batches(
    storage: impl Storage + Clone,
    topic: &str,
    index: i32,
) -> Result<Vec<deflated::Batch>> {
    let response = FetchService { storage }
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(500)
                .min_bytes(1)
                .max_bytes(Some(50 * 1024))
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(topic.into()))
                        .topic_id(Some(NULL_TOPIC_ID))
                        .partitions(Some(
                            [FetchPartition::default()
                                .partition(index)
                                .current_leader_epoch(Some(-1))
                                .fetch_offset(0)
                                .last_fetched_epoch(Some(-1))
                                .log_start_offset(Some(-1))
                                .partition_max_bytes(50 * 1024)
                                .replica_directory_id(None)]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());

    let partitions = responses[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    Ok(partitions[0]
        .records
        .as_ref()
        .map(|frame| frame.batches.clone())
        .unwrap_or_default())
}

async fn non_txn_idempotent_unknown_producer_id(storage: impl Storage + Clone) -> Result<()> {
    let topic = &alphanumeric_string(15)[..];

    let extensions = Extensions::default();

    let create_topic = CreateTopicsService {
        storage: storage.clone(),
    };

    let num_partitions = rng().random_range(1..64);
    let replication_factor = rng().random_range(0..64);

    {
        let response = create_topic
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(topic.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    }

    let index = rng().random_range(0..num_partitions);

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let response = produce
        .serve(RequestInput {
            request: ProduceRequest::default()
                .transactional_id(transactional_id)
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(Record::builder().value(Bytes::from_static(b"lorem").into()))
                        .producer_id(54345),
                )?),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::UnknownProducerId.into())
                            .base_offset(-1)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    Ok(())
}

async fn non_txn_idempotent(storage: impl Storage + Clone) -> Result<()> {
    let topic = &alphanumeric_string(15)[..];

    let extensions = Extensions::default();

    let create_topic = CreateTopicsService {
        storage: storage.clone(),
    };

    let num_partitions = rng().random_range(1..64);
    let replication_factor = rng().random_range(0..64);

    {
        let response = create_topic
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(topic.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    }

    let index = rng().random_range(0..num_partitions);

    let init_producer_id = InitProducerIdService {
        storage: storage.clone(),
    };

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let producer = init_producer_id
        .serve(RequestInput {
            request: InitProducerIdRequest::default()
                .transactional_id(None)
                .transaction_timeout_ms(0)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
            extensions: extensions.clone(),
        })
        .await?;

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    let response = produce
        .serve(RequestInput {
            request: ProduceRequest::default()
                .transactional_id(transactional_id.clone())
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()),
                        )
                        .producer_id(producer.producer_id),
                )?),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::None.into())
                            .base_offset(0)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    let response = produce
        .serve(RequestInput {
            request: ProduceRequest::default()
                .transactional_id(transactional_id.clone())
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"consectetur adipiscing elit").into()),
                        )
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"sed do eiusmod tempor").into()),
                        )
                        .base_sequence(1)
                        .last_offset_delta(1)
                        .producer_id(producer.producer_id),
                )?),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::None.into())
                            .base_offset(1)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    let response = produce
        .serve(RequestInput {
            request: ProduceRequest::default()
                .transactional_id(transactional_id.clone())
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"incididunt ut labore").into()),
                        )
                        .base_sequence(3)
                        .producer_id(producer.producer_id),
                )?),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::None.into())
                            .base_offset(3)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    Ok(())
}

async fn non_txn_idempotent_duplicate_sequence(storage: impl Storage + Clone) -> Result<()> {
    let topic = &alphanumeric_string(15)[..];

    let extensions = Extensions::default();

    let create_topic = CreateTopicsService {
        storage: storage.clone(),
    };

    let num_partitions = rng().random_range(1..64);
    let replication_factor = rng().random_range(0..64);

    {
        let response = create_topic
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(topic.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    }

    let index = rng().random_range(0..num_partitions);

    let init_producer_id = InitProducerIdService {
        storage: storage.clone(),
    };

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let producer = init_producer_id
        .serve(RequestInput {
            request: InitProducerIdRequest::default()
                .transactional_id(None)
                .transaction_timeout_ms(0)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
            extensions: extensions.clone(),
        })
        .await?;

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    let response = produce
        .serve(RequestInput {
            request: ProduceRequest::default()
                .transactional_id(transactional_id.clone())
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()),
                        )
                        .producer_id(producer.producer_id),
                )?),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::None.into())
                            .base_offset(0)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    let response = produce
        .serve(RequestInput {
            request: ProduceRequest::default()
                .transactional_id(transactional_id)
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()),
                        )
                        .producer_id(producer.producer_id),
                )?),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::DuplicateSequenceNumber.into())
                            .base_offset(-1)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    Ok(())
}

async fn non_txn_idempotent_sequence_out_of_order(storage: impl Storage + Clone) -> Result<()> {
    let extensions = Extensions::default();

    let init_producer_id = InitProducerIdService {
        storage: storage.clone(),
    };

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let topic = &alphanumeric_string(15)[..];

    let create_topic = CreateTopicsService {
        storage: storage.clone(),
    };

    let num_partitions = rng().random_range(1..64);
    let replication_factor = rng().random_range(0..64);

    {
        let response = create_topic
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(topic.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    }

    let index = rng().random_range(0..num_partitions);

    let producer = init_producer_id
        .serve(RequestInput {
            request: InitProducerIdRequest::default()
                .transactional_id(None)
                .transaction_timeout_ms(0)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
            extensions: extensions.clone(),
        })
        .await?;

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    let response = produce
        .serve(RequestInput {
            request: ProduceRequest::default()
                .transactional_id(transactional_id.clone())
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()),
                        )
                        .producer_id(producer.producer_id),
                )?),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::None.into())
                            .base_offset(0)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    let response = produce
        .serve(RequestInput {
            request: ProduceRequest::default()
                .transactional_id(transactional_id)
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()),
                        )
                        .base_sequence(2)
                        .producer_id(producer.producer_id),
                )?),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::OutOfOrderSequenceNumber.into())
                            .base_offset(-1)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    Ok(())
}

async fn list_offsets(storage: impl Storage + Clone) -> Result<()> {
    let extensions = Extensions::default();

    let create_topic = CreateTopicsService {
        storage: storage.clone(),
    };

    let delete_topic = DeleteTopicsService {
        storage: storage.clone(),
    };

    let list_offsets = ListOffsetsService {
        storage: storage.clone(),
    };

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let name = &alphanumeric_string(15)[..];

    let num_partitions = rng().random_range(1..64);
    let replication_factor = rng().random_range(0..64);

    {
        let response = create_topic
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    }

    let partition = rng().random_range(0..num_partitions);

    let before_produce_earliest = {
        let response = list_offsets
            .serve(RequestInput {
                request: ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .topics(Some(
                        [ListOffsetsTopic::default()
                            .name(name.into())
                            .partitions(Some(
                                [ListOffsetsPartition::default()
                                    .partition_index(partition)
                                    .timestamp(ListOffset::Earliest.try_into()?)]
                                .into(),
                            ))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        partitions[0].offset
    };

    let before_produce_latest = {
        let response = list_offsets
            .serve(RequestInput {
                request: ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .topics(Some(
                        [ListOffsetsTopic::default()
                            .name(name.into())
                            .partitions(Some(
                                [ListOffsetsPartition::default()
                                    .partition_index(partition)
                                    .timestamp(ListOffset::Latest.try_into()?)]
                                .into(),
                            ))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        partitions[0].offset
    };

    let offset = {
        let deflated = inflated::Batch::builder()
            .record(
                Record::builder().value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()),
            )
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        let response = produce
            .serve(RequestInput {
                request: ProduceRequest::default().topic_data(Some(
                    [TopicProduceData::default()
                        .name(name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(partition)
                                .records(Some(Frame {
                                    batches: vec![deflated],
                                }))]
                            .into(),
                        ))]
                    .into(),
                )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        partitions[0].base_offset
    };

    assert_eq!(before_produce_latest, Some(offset));

    let after_produce_earliest = {
        let response = list_offsets
            .serve(RequestInput {
                request: ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .topics(Some(
                        [ListOffsetsTopic::default()
                            .name(name.into())
                            .partitions(Some(
                                [ListOffsetsPartition::default()
                                    .partition_index(partition)
                                    .timestamp(ListOffset::Earliest.try_into()?)]
                                .into(),
                            ))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        partitions[0].offset
    };

    assert_eq!(before_produce_earliest, after_produce_earliest);

    let after_produce_latest = {
        let response = list_offsets
            .serve(RequestInput {
                request: ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .topics(Some(
                        [ListOffsetsTopic::default()
                            .name(name.into())
                            .partitions(Some(
                                [ListOffsetsPartition::default()
                                    .partition_index(partition)
                                    .timestamp(ListOffset::Latest.try_into()?)]
                                .into(),
                            ))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        partitions[0].offset
    };

    assert_eq!(Some(offset + 1), after_produce_latest);

    let response = delete_topic
        .serve(RequestInput {
            request: DeleteTopicsRequest::default().topic_names(Some([name.into()].into())),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    Ok(())
}

/// Every storage backend trusted the batch
/// header's `last_offset_delta` to advance the high watermark, without
/// checking it against the number of records actually decoded from the
/// batch. A mismatched batch (or a negative `last_offset_delta`) must be
/// rejected with `INVALID_RECORD` before anything is written, and must not
/// wedge the partition for subsequent well-formed produces.
async fn produce_rejects_last_offset_delta_mismatch(storage: impl Storage + Clone) -> Result<()> {
    let topic = &alphanumeric_string(15)[..];

    let extensions = Extensions::default();

    let create_topic = CreateTopicsService {
        storage: storage.clone(),
    };

    {
        let response = create_topic
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(topic.into())
                            .num_partitions(1)
                            .replication_factor(0)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    }

    let index = 0;

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let list_offsets = ListOffsetsService {
        storage: storage.clone(),
    };

    // Five records, but `last_offset_delta` left at its builder default of
    // 0: `last_offset_delta + 1 == record_count` is violated (0 + 1 != 5).
    // Must be rejected, and nothing from it written.
    let mismatched = produce
        .serve(RequestInput {
            request: ProduceRequest::default().topic_data(topic_data(
                topic,
                index,
                inflated::Batch::builder()
                    .record(Record::builder().value(Bytes::from_static(b"a").into()))
                    .record(Record::builder().value(Bytes::from_static(b"b").into()))
                    .record(Record::builder().value(Bytes::from_static(b"c").into()))
                    .record(Record::builder().value(Bytes::from_static(b"d").into()))
                    .record(Record::builder().value(Bytes::from_static(b"e").into())),
            )?),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let topics = mismatched.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::InvalidRecord,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(-1, partitions[0].base_offset);
    }

    // A negative `last_offset_delta` must also be rejected.
    let negative = produce
        .serve(RequestInput {
            request: ProduceRequest::default().topic_data(topic_data(
                topic,
                index,
                inflated::Batch::builder()
                    .record(Record::builder().value(Bytes::from_static(b"a").into()))
                    .last_offset_delta(-1),
            )?),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let topics = negative.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::InvalidRecord,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(-1, partitions[0].base_offset);
    }

    // A `last_offset_delta` that is too large must also be rejected: two
    // records claiming a delta of 2 is the off-by-one the generator and perf
    // tools used to send.
    let too_large = produce
        .serve(RequestInput {
            request: ProduceRequest::default().topic_data(topic_data(
                topic,
                index,
                inflated::Batch::builder()
                    .record(Record::builder().value(Bytes::from_static(b"a").into()))
                    .record(Record::builder().value(Bytes::from_static(b"b").into()))
                    .last_offset_delta(2),
            )?),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let topics = too_large.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::InvalidRecord,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(-1, partitions[0].base_offset);
    }

    // An empty batch (no records at all) must also be rejected. This is a
    // distinct condition from the count/delta mismatch above: zero records
    // with `last_offset_delta(-1)` actually satisfies
    // `last_offset_delta + 1 == record_count` (-1 + 1 == 0), so
    // `record_count >= 1` has to be checked on its own to catch it.
    let empty = produce
        .serve(RequestInput {
            request: ProduceRequest::default().topic_data(topic_data(
                topic,
                index,
                inflated::Batch::builder().last_offset_delta(-1),
            )?),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let topics = empty.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::InvalidRecord,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(-1, partitions[0].base_offset);
    }

    // The partition must not be wedged: a well-formed batch to the same
    // topic/partition afterwards must still succeed, landing at offset 0 --
    // proving none of the four rejected batches above wrote or advanced
    // anything (on Postgres/libSQL, a partial write from any of them would
    // instead make this insert collide with an existing primary key).
    let well_formed = produce
        .serve(RequestInput {
            request: ProduceRequest::default().topic_data(topic_data(
                topic,
                index,
                inflated::Batch::builder()
                    .record(Record::builder().value(Bytes::from_static(b"well formed").into())),
            )?),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let topics = well_formed.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(0, partitions[0].base_offset);
    }

    // Confirm via ListOffsets(latest) that the high watermark only moved
    // past the one well-formed record -- if any rejected batch above had
    // moved it too, this would be something other than 1. (That none of
    // them left partial rows behind is what the offset-0 produce above
    // already proved.)
    let response = list_offsets
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic.into())
                        .partitions(Some(
                            [ListOffsetsPartition::default()
                                .partition_index(index)
                                .timestamp(ListOffset::Latest.try_into()?)]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(Some(1), partitions[0].offset);

    Ok(())
}

/// A client-authored batch with the control bit set must be rejected before
/// anything is written: only the broker may write transaction commit/abort
/// markers, and Kafka's `LogValidator` rejects a client-origin control batch
/// with `INVALID_RECORD`.
async fn produce_rejects_control_batch(storage: impl Storage + Clone) -> Result<()> {
    let extensions = Extensions::default();

    let create_topic = CreateTopicsService {
        storage: storage.clone(),
    };

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let list_offsets = ListOffsetsService {
        storage: storage.clone(),
    };

    let name = &alphanumeric_string(15)[..];

    let num_partitions = rng().random_range(1..64);
    let replication_factor = rng().random_range(0..64);

    {
        let response = create_topic
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    }

    let partition = rng().random_range(0..num_partitions);

    // A legitimate batch ahead of the forged one: the whole partition must be
    // rejected before anything is written, not just the offending batch, so a
    // future refactor that moves the check into the per-batch loop can't
    // silently start writing the batches ahead of a forged one.
    let legit = inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()))
        .build()
        .and_then(TryInto::try_into)
        .inspect(|deflated| debug!(?deflated))?;

    // Shaped like a real COMMIT/ABORT marker (transactional, with a producer
    // id and epoch), so the test still fails if the check is ever narrowed to
    // let "well-formed" transactional markers through.
    let forged = inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(b"forged control batch").into()))
        .attributes(
            BatchAttribute::default()
                .control(true)
                .transaction(true)
                .into(),
        )
        .producer_id(1)
        .producer_epoch(0)
        .build()
        .and_then(TryInto::try_into)
        .inspect(|deflated| debug!(?deflated))?;

    let response = produce
        .serve(RequestInput {
            request: ProduceRequest::default().topic_data(Some(
                [TopicProduceData::default()
                    .name(name.into())
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(partition)
                            .records(Some(Frame {
                                batches: vec![legit, forged],
                            }))]
                        .into(),
                    ))]
                .into(),
            )),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(name.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(partition)
                            .error_code(ErrorCode::InvalidRecord.into())
                            .base_offset(-1)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(Some("clients may not write control batches".into()))
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    // Nothing was written: the latest offset is still the topic's initial offset.
    let latest = {
        let response = list_offsets
            .serve(RequestInput {
                request: ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .topics(Some(
                        [ListOffsetsTopic::default()
                            .name(name.into())
                            .partitions(Some(
                                [ListOffsetsPartition::default()
                                    .partition_index(partition)
                                    .timestamp(ListOffset::Latest.try_into()?)]
                                .into(),
                            ))]
                        .into(),
                    )),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        partitions[0].offset
    };

    assert_eq!(Some(0), latest);

    // A normal produce still lands at offset 0: the rejected batch consumed nothing.
    let ordinary = inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()))
        .build()
        .and_then(TryInto::try_into)
        .inspect(|deflated| debug!(?deflated))?;

    let response = produce
        .serve(RequestInput {
            request: ProduceRequest::default().topic_data(Some(
                [TopicProduceData::default()
                    .name(name.into())
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(partition)
                            .records(Some(Frame {
                                batches: vec![ordinary],
                            }))]
                        .into(),
                    ))]
                .into(),
            )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(0, partitions[0].base_offset);

    Ok(())
}

/// Kafka's `log.message.timestamp.after.max.ms` default from 4.0, which the
/// broker applies to every `CreateTime` record.
const TIMESTAMP_AFTER_MAX_MS: i64 = 60 * 60 * 1000;

fn now_ms() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(Into::into)
        .and_then(|duration| i64::try_from(duration.as_millis()).map_err(Into::into))
}

/// Creates a topic with one partition and `configs`, and returns its name.
async fn create_topic_with_configs(
    storage: impl Storage + Clone,
    configs: &[(&str, &str)],
) -> Result<String> {
    let topic = alphanumeric_string(15);

    let response = CreateTopicsService { storage }
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .name(topic.clone())
                        .num_partitions(1)
                        .replication_factor(0)
                        .assignments(Some([].into()))
                        .configs(Some(
                            configs
                                .iter()
                                .map(|(name, value)| {
                                    CreatableTopicConfig::default()
                                        .name((*name).into())
                                        .value(Some((*value).into()))
                                })
                                .collect(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    Ok(topic)
}

/// Sends `batches` to one partition with `acks=-1`, and returns that
/// partition's response.
async fn produce_batches(
    storage: impl Storage + Clone,
    topic: &str,
    index: i32,
    batches: Vec<deflated::Batch>,
) -> Result<PartitionProduceResponse> {
    let response = ProduceService { storage }
        .serve(RequestInput {
            request: ProduceRequest::default().acks(-1).topic_data(Some(vec![
                TopicProduceData::default()
                    .name(topic.into())
                    .partition_data(Some(vec![
                        PartitionProduceData::default()
                            .index(index)
                            .records(Some(Frame { batches })),
                    ])),
            ])),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.responses.unwrap_or_default();
    assert_eq!(1, topics.len());

    let mut partitions = topics[0].partition_responses.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());

    Ok(partitions.remove(0))
}

/// A `CreateTime` batch with one record for each of `timestamp_deltas`.
fn create_time_batch(
    compression: Compression,
    base_timestamp: i64,
    max_timestamp: i64,
    timestamp_deltas: &[i64],
) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder()
        .attributes(BatchAttribute::default().compression(compression).into())
        .base_timestamp(base_timestamp)
        .max_timestamp(max_timestamp)
        .last_offset_delta(i32::try_from(timestamp_deltas.len())? - 1);

    for (offset_delta, timestamp_delta) in timestamp_deltas.iter().enumerate() {
        builder = builder.record(
            Record::builder()
                .offset_delta(i32::try_from(offset_delta)?)
                .timestamp_delta(*timestamp_delta)
                .value(Bytes::from(format!("record {offset_delta}")).into()),
        );
    }

    builder
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Compresses an uncompressed batch the way the Java producer does: a
/// snappy-java (xerial) stream with a new block for every 32 KiB of
/// uncompressed records.
fn xerial_snappy(batch: deflated::Batch) -> Result<deflated::Batch> {
    const BLOCK: usize = 32 * 1024;

    let mut framed = BytesMut::new();
    framed.put_slice(b"\x82SNAPPY\0");
    framed.put_i32(1);
    framed.put_i32(1);

    for chunk in batch.record_data.chunks(BLOCK) {
        let block = snap::raw::Encoder::new()
            .compress_vec(chunk)
            .map_err(nisshi_sans_io::Error::from)?;
        framed.put_i32(i32::try_from(block.len())?);
        framed.put_slice(&block);
    }

    let record_data = framed.freeze();

    let mut batch = deflated::Batch {
        attributes: BatchAttribute::default()
            .compression(Compression::Snappy)
            .into(),
        batch_length: batch.batch_length - i32::try_from(batch.record_data.len())?
            + i32::try_from(record_data.len())?,
        record_data,
        ..batch
    };

    batch.crc = batch.computed_crc();
    Ok(batch)
}

fn assert_accepted_at(base_offset: i64, response: &PartitionProduceResponse) -> Result<()> {
    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    assert_eq!(base_offset, response.base_offset);
    Ok(())
}

/// A `CreateTime` record more than one hour ahead of the broker's clock is
/// rejected with `INVALID_TIMESTAMP` before anything is written. The record
/// error names the record's index in its batch, so a client can find it.
async fn produce_rejects_future_timestamp(storage: impl Storage + Clone) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[]).await?;
    let index = 0;
    let now = now_ms()?;

    let rejected = produce_batches(
        storage.clone(),
        &topic,
        index,
        vec![create_time_batch(
            Compression::None,
            now,
            now,
            &[0, 2 * TIMESTAMP_AFTER_MAX_MS],
        )?],
    )
    .await?;

    assert_eq!(
        ErrorCode::InvalidTimestamp,
        ErrorCode::try_from(rejected.error_code)?
    );
    assert_eq!(-1, rejected.base_offset);
    assert_eq!(
        Some("One or more records have been rejected due to invalid timestamp".into()),
        rejected.error_message
    );

    let record_errors = rejected.record_errors.unwrap_or_default();
    assert_eq!(1, record_errors.len());
    assert_eq!(1, record_errors[0].batch_index);
    assert!(
        record_errors[0]
            .batch_index_error_message
            .as_deref()
            .is_some_and(|message| message.contains("is out of range"))
    );

    // A well-formed produce afterwards lands at offset 0, so the rejected
    // batch wrote nothing.
    let well_formed = produce_batches(
        storage,
        &topic,
        index,
        vec![create_time_batch(Compression::None, now, now, &[0])?],
    )
    .await?;

    assert_accepted_at(0, &well_formed)
}

/// The broker validates every batch for a partition before it writes any of
/// them, so a rejected second batch leaves the first one unwritten too.
async fn produce_rejects_every_batch_when_a_later_one_is_invalid(
    storage: impl Storage + Clone,
) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[]).await?;
    let index = 0;
    let now = now_ms()?;

    let rejected = produce_batches(
        storage.clone(),
        &topic,
        index,
        vec![
            create_time_batch(Compression::None, now, -1, &[0])?,
            create_time_batch(Compression::None, now, now, &[2 * TIMESTAMP_AFTER_MAX_MS])?,
        ],
    )
    .await?;

    assert_eq!(
        ErrorCode::InvalidTimestamp,
        ErrorCode::try_from(rejected.error_code)?
    );

    let well_formed = produce_batches(
        storage,
        &topic,
        index,
        vec![create_time_batch(Compression::None, now, now, &[0])?],
    )
    .await?;

    assert_accepted_at(0, &well_formed)
}

/// A batch whose header `max_timestamp` differs from its largest record
/// timestamp is stored with the largest record timestamp, not rejected.
/// Kafka's `LogValidator` overwrites the header in the same way, and some
/// clients (sarama releases before 2025-02-28, for one) send every batch
/// with the header at -1.
///
/// Each case produces to its own topic, so a backend that combines batches
/// on fetch does not change the result. `assert_stored_header` is true only
/// for a backend whose fetch returns the stored bytes. A SQL backend builds
/// a new batch from its records on fetch, so its header and `crc` say
/// nothing about what [`ProduceService`] wrote.
async fn produce_rewrites_header_max_timestamp(
    storage: impl Storage + Clone,
    assert_stored_header: bool,
) -> Result<()> {
    let index = 0;
    let now = now_ms()?;
    let ten_hours = 10 * TIMESTAMP_AFTER_MAX_MS;

    for (base_timestamp, header_max_timestamp, timestamp_deltas, expected) in [
        // The sarama case: the header says -1.
        (now, -1, vec![5], now + 5),
        // The header claims ten hours ahead of the broker's clock, and the
        // record is sane.
        (now, now + ten_hours, vec![0], now),
        // The largest record timestamp is not the last one.
        (now, -1, vec![0, 300, 100], now + 300),
    ] {
        let topic = create_topic_with_configs(storage.clone(), &[]).await?;

        let response = produce_batches(
            storage.clone(),
            &topic,
            index,
            vec![create_time_batch(
                Compression::None,
                base_timestamp,
                header_max_timestamp,
                &timestamp_deltas,
            )?],
        )
        .await?;

        assert_accepted_at(0, &response)?;

        let fetched = fetch_batches(storage.clone(), &topic, index).await?;
        assert_eq!(1, fetched.len());

        if assert_stored_header {
            assert_eq!(expected, fetched[0].max_timestamp);
            assert_eq!(fetched[0].computed_crc(), fetched[0].crc);
        }
    }

    Ok(())
}

/// The header rewrite decodes the records, so it must accept each codec a
/// client can send, including a snappy-java stream of several blocks.
async fn produce_rewrites_header_for_every_codec(
    storage: impl Storage + Clone,
    assert_stored_header: bool,
) -> Result<()> {
    let index = 0;
    let now = now_ms()?;

    let mut batches = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ]
    .into_iter()
    .map(|compression| create_time_batch(compression, now, -1, &[0, 7]))
    .collect::<Result<Vec<_>>>()?;

    // One record of 40,000 bytes needs two 32 KiB snappy-java blocks.
    batches.push(xerial_snappy(
        inflated::Batch::builder()
            .base_timestamp(now)
            .max_timestamp(-1)
            .record(
                Record::builder()
                    .timestamp_delta(7)
                    .value(Bytes::from(vec![b'x'; 40_000]).into()),
            )
            .build()
            .and_then(deflated::Batch::try_from)?,
    )?);

    for batch in batches {
        let attributes = batch.attributes;
        let topic = create_topic_with_configs(storage.clone(), &[]).await?;

        let response = produce_batches(storage.clone(), &topic, index, vec![batch]).await?;
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(response.error_code)?,
            "attributes: {attributes}"
        );

        if assert_stored_header {
            let fetched = fetch_batches(storage.clone(), &topic, index).await?;
            assert_eq!(1, fetched.len());
            assert_eq!(
                now + 7,
                fetched[0].max_timestamp,
                "attributes: {attributes}"
            );
            assert_eq!(fetched[0].computed_crc(), fetched[0].crc);
        }
    }

    Ok(())
}

/// A header rewrite gives the batch a new CRC, so the broker checks the
/// client's CRC first. A batch whose CRC does not match its contents is
/// rejected with `CORRUPT_MESSAGE`, as Kafka does.
async fn produce_rejects_corrupt_batch_before_rewrite(storage: impl Storage + Clone) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[]).await?;
    let index = 0;
    let now = now_ms()?;

    let mut corrupt = create_time_batch(Compression::None, now, -1, &[0])?;
    corrupt.crc ^= 1;

    let rejected = produce_batches(storage.clone(), &topic, index, vec![corrupt]).await?;

    assert_eq!(
        ErrorCode::CorruptMessage,
        ErrorCode::try_from(rejected.error_code)?
    );

    let well_formed = produce_batches(
        storage,
        &topic,
        index,
        vec![create_time_batch(Compression::None, now, now, &[0])?],
    )
    .await?;

    assert_accepted_at(0, &well_formed)
}

/// SlateDB retention deletes a batch whose `max_timestamp` is older than
/// `retention.ms`. A header of -1 stored as sent makes a new batch eligible
/// at once, so the stored header must hold the largest record timestamp.
#[cfg(feature = "slatedb")]
async fn produce_rewritten_header_keeps_a_new_batch_from_retention(
    storage: impl Storage + Clone,
) -> Result<()> {
    let topic = create_topic_with_configs(
        storage.clone(),
        &[("cleanup.policy", "delete"), ("retention.ms", "60000")],
    )
    .await?;
    let index = 0;
    let now = now_ms()?;

    let response = produce_batches(
        storage.clone(),
        &topic,
        index,
        vec![create_time_batch(Compression::None, now, -1, &[0])?],
    )
    .await?;
    assert_accepted_at(0, &response)?;

    storage.maintain(SystemTime::now()).await?;

    let fetched = fetch_batches(storage, &topic, index).await?;
    assert_eq!(1, fetched.len());
    assert_eq!(now, fetched[0].max_timestamp);

    Ok(())
}

/// The broker honours a client-set `LogAppendTime` bit, and that batch skips
/// the `CreateTime` timestamp window. Kafka takes the timestamp type from the
/// topic's `message.timestamp.type` instead: on a `CreateTime` topic it
/// validates such a batch like any `CreateTime` batch and clears the bit, so
/// Kafka rejects this batch.
///
/// The test also checks that the broker gives the rewritten batch a new
/// `crc`. `assert_stored_header` gates that check as in
/// [`produce_rewrites_header_max_timestamp`].
async fn produce_log_append_time_ignores_bounds_check(
    storage: impl Storage + Clone,
    assert_stored_header: bool,
) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[]).await?;
    let index = 0;

    const TEN_YEARS_MS: i64 = 10 * 365 * 24 * 60 * 60 * 1000;

    let batch = inflated::Batch::builder()
        .attributes(
            BatchAttribute::default()
                .timestamp(TimestampType::LogAppendTime)
                .into(),
        )
        .record(
            Record::builder()
                .value(Bytes::from_static(b"far future, but log append time").into())
                .timestamp_delta(TEN_YEARS_MS),
        )
        .build()
        .and_then(deflated::Batch::try_from)?;

    let response = produce_batches(storage.clone(), &topic, index, vec![batch]).await?;
    assert_accepted_at(0, &response)?;

    if assert_stored_header {
        let fetched = fetch_batches(storage.clone(), &topic, index).await?;
        assert_eq!(1, fetched.len());
        assert_eq!(fetched[0].computed_crc(), fetched[0].crc);
    }

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        memory_storage(cluster, node).await
    }

    #[tokio::test]
    async fn non_txn_idempotent_unknown_producer_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent_unknown_producer_id(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent_duplicate_sequence() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent_duplicate_sequence(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent_sequence_out_of_order() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent_sequence_out_of_order(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn list_offsets() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::list_offsets(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn produce_rejects_last_offset_delta_mismatch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_last_offset_delta_mismatch(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn produce_rejects_control_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_control_batch(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn produce_rejects_future_timestamp() -> Result<()> {
        {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            let storage = storage_container(cluster_id, broker_id).await?;

            super::produce_rejects_future_timestamp(storage).await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn produce_rewrites_header_max_timestamp() -> Result<()> {
        {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            let storage = storage_container(cluster_id, broker_id).await?;

            super::produce_rewrites_header_max_timestamp(storage, true).await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn produce_log_append_time_ignores_bounds_check() -> Result<()> {
        {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            let storage = storage_container(cluster_id, broker_id).await?;

            super::produce_log_append_time_ignores_bounds_check(storage, true).await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn produce_rejects_every_batch_when_a_later_one_is_invalid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_every_batch_when_a_later_one_is_invalid(storage).await
    }

    #[tokio::test]
    async fn produce_rewrites_header_for_every_codec() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rewrites_header_for_every_codec(storage, true).await
    }

    #[tokio::test]
    async fn produce_rejects_corrupt_batch_before_rewrite() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_corrupt_batch_before_rewrite(storage).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        lite_storage(cluster, node).await
    }

    #[tokio::test]
    async fn non_txn_idempotent_unknown_producer_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent_unknown_producer_id(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent_duplicate_sequence() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent_duplicate_sequence(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent_sequence_out_of_order() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent_sequence_out_of_order(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn list_offsets() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::list_offsets(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn produce_rejects_last_offset_delta_mismatch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_last_offset_delta_mismatch(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn produce_rejects_control_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_control_batch(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn produce_rejects_future_timestamp() -> Result<()> {
        {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            let storage = storage_container(cluster_id, broker_id).await?;

            super::produce_rejects_future_timestamp(storage).await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn produce_rewrites_header_max_timestamp() -> Result<()> {
        {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            let storage = storage_container(cluster_id, broker_id).await?;

            super::produce_rewrites_header_max_timestamp(storage, false).await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn produce_log_append_time_ignores_bounds_check() -> Result<()> {
        {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            let storage = storage_container(cluster_id, broker_id).await?;

            super::produce_log_append_time_ignores_bounds_check(storage, false).await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn produce_rejects_every_batch_when_a_later_one_is_invalid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_every_batch_when_a_later_one_is_invalid(storage).await
    }

    #[tokio::test]
    async fn produce_rewrites_header_for_every_codec() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rewrites_header_for_every_codec(storage, false).await
    }

    #[tokio::test]
    async fn produce_rejects_corrupt_batch_before_rewrite() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_corrupt_batch_before_rewrite(storage).await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        slate_storage(cluster, node).await
    }

    #[tokio::test]
    async fn non_txn_idempotent_unknown_producer_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent_unknown_producer_id(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent_duplicate_sequence() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent_duplicate_sequence(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent_sequence_out_of_order() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent_sequence_out_of_order(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn list_offsets() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::list_offsets(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn produce_rejects_last_offset_delta_mismatch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_last_offset_delta_mismatch(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn produce_rejects_control_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_control_batch(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn produce_rejects_future_timestamp() -> Result<()> {
        {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            let storage = storage_container(cluster_id, broker_id).await?;

            super::produce_rejects_future_timestamp(storage).await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn produce_rewrites_header_max_timestamp() -> Result<()> {
        {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            let storage = storage_container(cluster_id, broker_id).await?;

            super::produce_rewrites_header_max_timestamp(storage, true).await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn produce_log_append_time_ignores_bounds_check() -> Result<()> {
        {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            let storage = storage_container(cluster_id, broker_id).await?;

            super::produce_log_append_time_ignores_bounds_check(storage, true).await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn produce_rejects_every_batch_when_a_later_one_is_invalid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_every_batch_when_a_later_one_is_invalid(storage).await
    }

    #[tokio::test]
    async fn produce_rewrites_header_for_every_codec() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rewrites_header_for_every_codec(storage, true).await
    }

    #[tokio::test]
    async fn produce_rejects_corrupt_batch_before_rewrite() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_corrupt_batch_before_rewrite(storage).await
    }

    #[tokio::test]
    async fn produce_rewritten_header_keeps_a_new_batch_from_retention() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rewritten_header_keeps_a_new_batch_from_retention(storage).await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        postgres_storage(cluster, node).await
    }

    #[tokio::test]
    async fn non_txn_idempotent_unknown_producer_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent_unknown_producer_id(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent_duplicate_sequence() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent_duplicate_sequence(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent_sequence_out_of_order() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_txn_idempotent_sequence_out_of_order(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn list_offsets() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::list_offsets(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn produce_rejects_last_offset_delta_mismatch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_last_offset_delta_mismatch(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn produce_rejects_control_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_control_batch(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn produce_rejects_future_timestamp() -> Result<()> {
        {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            let storage = storage_container(cluster_id, broker_id).await?;

            super::produce_rejects_future_timestamp(storage).await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn produce_rewrites_header_max_timestamp() -> Result<()> {
        {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            let storage = storage_container(cluster_id, broker_id).await?;

            super::produce_rewrites_header_max_timestamp(storage, false).await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn produce_log_append_time_ignores_bounds_check() -> Result<()> {
        {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            let storage = storage_container(cluster_id, broker_id).await?;

            super::produce_log_append_time_ignores_bounds_check(storage, false).await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn produce_rejects_every_batch_when_a_later_one_is_invalid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_every_batch_when_a_later_one_is_invalid(storage).await
    }

    #[tokio::test]
    async fn produce_rewrites_header_for_every_codec() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rewrites_header_for_every_codec(storage, false).await
    }

    #[tokio::test]
    async fn produce_rejects_corrupt_batch_before_rewrite() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::produce_rejects_corrupt_batch_before_rewrite(storage).await
    }
}
