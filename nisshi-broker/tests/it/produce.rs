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
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    BatchAttribute, CreateTopicsRequest, DeleteTopicsRequest, ErrorCode, FetchRequest,
    InitProducerIdRequest, IsolationLevel, ListOffset, ListOffsetsRequest, NULL_TOPIC_ID,
    ProduceRequest, ProduceResponse, RequestInput, TimestampType,
    create_topics_request::CreatableTopic,
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

/// A `CreateTime` record more than Kafka's own
/// `log.message.timestamp.after.max.ms` default (one hour) ahead of the
/// broker's clock must be rejected with `INVALID_TIMESTAMP` before anything
/// is written, matching Kafka's own `LogValidator`, and must not wedge the
/// partition for a subsequent well-formed produce.
async fn produce_rejects_future_timestamp(storage: impl Storage + Clone) -> Result<()> {
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

    const TWO_HOURS_MS: i64 = 2 * 60 * 60 * 1000;

    let future = produce
        .serve(RequestInput {
            request: ProduceRequest::default().topic_data(topic_data(
                topic,
                index,
                inflated::Batch::builder().record(
                    Record::builder()
                        .value(Bytes::from_static(b"two hours from now").into())
                        .timestamp_delta(TWO_HOURS_MS),
                ),
            )?),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let topics = future.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::InvalidTimestamp,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(-1, partitions[0].base_offset);
        assert_eq!(
            Some("record timestamp is outside the allowed window".into()),
            partitions[0].error_message
        );
    }

    // The partition must not be wedged: a well-formed produce afterwards
    // still lands at offset 0, proving the rejected batch wrote nothing.
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

    Ok(())
}

/// A batch whose header `max_timestamp` disagrees with its records' actual
/// maximum must be stored with the corrected value, not rejected: real
/// Kafka's `LogValidator` recomputes and overwrites rather than rejecting a
/// header/records mismatch, and a client (sarama releases before 2025-02-28,
/// for one) that never set `max_timestamp` sends every batch with it at -1.
///
/// Each case below produces to its own topic, one batch each, so the
/// assertion on `fetch_batches`'s result doesn't depend on a backend's
/// batch-boundary fidelity: the SQL backends reconstruct one combined batch
/// per fetch from their flat record storage, while the byte-preserving
/// backends (in-memory, slatedb) return exactly the batches produced.
///
/// `assert_stored_header` is only meaningful on a byte-preserving backend:
/// only there does the fetched batch carry back the exact bytes
/// [`ProduceService`] wrote. A SQL backend's fetch reconstructs a fresh
/// batch from its flat record storage rather than returning the stored
/// bytes, and (a separate, pre-existing gap from this ticket) never derives
/// that reconstructed batch's `max_timestamp` from the records it just read,
/// so neither its `max_timestamp` nor its `crc` says anything about what
/// [`ProduceService`] wrote here.
async fn produce_rewrites_header_max_timestamp(
    storage: impl Storage + Clone,
    assert_stored_header: bool,
) -> Result<()> {
    let create_topic = CreateTopicsService {
        storage: storage.clone(),
    };

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let index = 0;

    // Reproduces the real sarama wire shape: `max_timestamp` left at -1,
    // never derived from the records.
    let base_timestamp = 1_700_000_000_000;

    {
        let topic = &alphanumeric_string(15)[..];
        let extensions = Extensions::default();

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

        let response = produce
            .serve(RequestInput {
                request: ProduceRequest::default().topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .base_timestamp(base_timestamp)
                        .max_timestamp(-1)
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"header says -1").into())
                                .timestamp_delta(5),
                        ),
                )?),
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

        let fetched = fetch_batches(storage.clone(), topic, index).await?;
        assert_eq!(1, fetched.len());

        if assert_stored_header {
            assert_eq!(base_timestamp + 5, fetched[0].max_timestamp);
            assert_eq!(fetched[0].computed_crc()?, fetched[0].crc);
        }
    }

    // A header that claims a time far in the future, but whose records are
    // sane, must also be accepted and corrected -- the retention/time-index
    // poisoning scenario a reject-only design would leave unfixed.
    {
        let topic = &alphanumeric_string(15)[..];
        let extensions = Extensions::default();

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

        let far_future_header = base_timestamp + 10 * 60 * 60 * 1000;

        let response = produce
            .serve(RequestInput {
                request: ProduceRequest::default().topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .base_timestamp(base_timestamp)
                        .max_timestamp(far_future_header)
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"header lies, record is sane").into())
                                .timestamp_delta(0),
                        ),
                )?),
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

        let fetched = fetch_batches(storage.clone(), topic, index).await?;
        assert_eq!(1, fetched.len());

        if assert_stored_header {
            assert_eq!(base_timestamp, fetched[0].max_timestamp);
            assert_eq!(fetched[0].computed_crc()?, fetched[0].crc);
        }
    }

    Ok(())
}

/// nisshi honors a client-set `LogAppendTime` bit by design (unlike real
/// Kafka, which would reject it on a `CreateTime` topic, the only mode
/// nisshi supports) -- this test confirms the new bounds/mismatch check for
/// `CreateTime` batches doesn't interfere with that existing behavior. A
/// record timestamped far enough in the future to fail the new check must
/// still be accepted, because the `LogAppendTime` bit routes the batch past
/// it entirely.
///
/// It also guards the pre-existing `recompute_crc()` call on this
/// `LogAppendTime` rewrite path: without it, a byte-preserving backend would
/// hand a consumer back a batch whose header no longer matches its stored
/// CRC. `assert_stored_header` gates this the same way
/// [`produce_rewrites_header_max_timestamp`] does, since only a
/// byte-preserving backend's fetch returns the exact bytes
/// [`ProduceService`] wrote.
async fn produce_log_append_time_ignores_bounds_check(
    storage: impl Storage + Clone,
    assert_stored_header: bool,
) -> Result<()> {
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

    const TEN_YEARS_MS: i64 = 10 * 365 * 24 * 60 * 60 * 1000;

    let response = produce
        .serve(RequestInput {
            request: ProduceRequest::default().topic_data(topic_data(
                topic,
                index,
                inflated::Batch::builder()
                    .attributes(
                        BatchAttribute::default()
                            .timestamp(TimestampType::LogAppendTime)
                            .into(),
                    )
                    .record(
                        Record::builder()
                            .value(Bytes::from_static(b"far future, but log append time").into())
                            .timestamp_delta(TEN_YEARS_MS),
                    ),
            )?),
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

    if assert_stored_header {
        let fetched = fetch_batches(storage.clone(), topic, index).await?;
        assert_eq!(1, fetched.len());
        assert_eq!(fetched[0].computed_crc()?, fetched[0].crc);
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
}
