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

use std::time::{SystemTime, UNIX_EPOCH};

use nisshi_sans_io::{
    Ack, ApiKey, BatchAttribute, ErrorCode, ProduceRequest, ProduceResponse, RequestInput,
    SuppressResponseExtension, TimestampType,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::deflated,
};
use rama::Service;
use tracing::{error, instrument, warn};

use crate::{Error, Result, Storage, Topition};

/// Why a client batch must be rejected before anything is written, with the
/// error code to send, or `None` if it may be stored. Kafka's `LogValidator`
/// rejects the same batches.
///
/// - Only the broker writes control batches (transaction commit/abort
///   markers), and those go directly to storage rather than through
///   [`ProduceService`]. Every backend skips schema validation and lake
///   writes for a control batch, so a client must not be able to set the bit.
///   `INVALID_RECORD`.
/// - A consistent header has at least one record and
///   `last_offset_delta + 1 == record_count`. Every backend uses
///   `last_offset_delta` to advance the high watermark, so a mismatch corrupts
///   or wedges the partition. `record_count` is an int32 on the wire, so a
///   value above `i32::MAX` is rejected too. `INVALID_RECORD`.
/// - `record_count` alone must not imply more decoded memory than the
///   decoded-size limit allows. `nisshi-sans-io` enforces the limit during
///   decode as well; checking here rejects the batch before `storage.produce()`
///   and before any decompression is paid for. `MESSAGE_TOO_LARGE`, the same
///   code the decode-time rejection maps to (see [`storage_error_code`]).
fn rejection(batch: &deflated::Batch) -> Option<(ErrorCode, &'static str)> {
    if batch.is_control() {
        return Some((
            ErrorCode::InvalidRecord,
            "clients may not write control batches",
        ));
    }

    let Ok(record_count) = i32::try_from(batch.record_count) else {
        return Some((ErrorCode::InvalidRecord, "record_count exceeds i32::MAX"));
    };

    if record_count < 1 {
        Some((ErrorCode::InvalidRecord, "batch has no records"))
    } else if batch.last_offset_delta.checked_add(1) != Some(record_count) {
        Some((
            ErrorCode::InvalidRecord,
            "last_offset_delta + 1 does not equal record_count",
        ))
    } else if batch.exceeds_decoded_record_count_limit() {
        Some((
            ErrorCode::MessageTooLarge,
            "record_count exceeds the maximum decoded batch size",
        ))
    } else {
        None
    }
}

/// The error code for a failed `storage.produce()`.
///
/// A batch over the decoded-size limit is reported as `MESSAGE_TOO_LARGE`:
/// Kafka's code for "this batch is too large for the broker", which a
/// producer can recover from on its own (the Java producer splits a batch of
/// more than one record and retries). `UNKNOWN_SERVER_ERROR` is reserved for
/// failures the client did not cause.
fn storage_error_code(error: &Error) -> ErrorCode {
    match error {
        Error::Api(error_code) => *error_code,
        Error::SansIo(nisshi_sans_io::Error::MessageMaxSizeExceeded(_)) => {
            ErrorCode::MessageTooLarge
        }
        _ => ErrorCode::UnknownServerError,
    }
}

#[cfg(test)]
mod rejection_tests {
    use super::*;

    #[test]
    fn oversized_record_count_is_rejected_before_storage_is_called() {
        let batch = deflated::Batch {
            record_count: 1_000_000,
            last_offset_delta: 999_999,
            ..Default::default()
        };

        assert_eq!(
            Some((
                ErrorCode::MessageTooLarge,
                "record_count exceeds the maximum decoded batch size"
            )),
            rejection(&batch)
        );
    }

    #[test]
    fn a_record_count_within_the_limit_is_not_rejected_for_this_reason() {
        let batch = deflated::Batch {
            record_count: 1,
            last_offset_delta: 0,
            ..Default::default()
        };

        assert_eq!(None, rejection(&batch));
    }

    #[test]
    fn an_inconsistent_header_is_an_invalid_record() {
        let batch = deflated::Batch {
            record_count: 2,
            last_offset_delta: 0,
            ..Default::default()
        };

        assert_eq!(
            Some((
                ErrorCode::InvalidRecord,
                "last_offset_delta + 1 does not equal record_count"
            )),
            rejection(&batch)
        );
    }

    #[test]
    fn decoded_size_limit_from_storage_is_message_too_large() {
        assert_eq!(
            ErrorCode::MessageTooLarge,
            storage_error_code(&Error::SansIo(
                nisshi_sans_io::Error::MessageMaxSizeExceeded(1)
            ))
        );
    }

    #[test]
    fn api_errors_from_storage_pass_through() {
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            storage_error_code(&Error::Api(ErrorCode::UnknownTopicOrPartition))
        );
    }

    #[test]
    fn other_storage_errors_are_unknown_server_error() {
        assert_eq!(
            ErrorCode::UnknownServerError,
            storage_error_code(&Error::SansIo(nisshi_sans_io::Error::Overflow))
        );
    }
}

/// The first partition in `responses` carrying a non-[`ErrorCode::None`]
/// error code, for the message an `acks=0` closed connection logs: the
/// client gets no response at all, so an operator needs to know which
/// partition actually failed.
fn first_error(responses: &[TopicProduceResponse]) -> Option<(String, i32, ErrorCode)> {
    responses.iter().find_map(|topic| {
        topic
            .partition_responses
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find(|partition| partition.error_code != i16::from(ErrorCode::None))
            .map(|partition| {
                (
                    topic.name.clone(),
                    partition.index,
                    ErrorCode::try_from(partition.error_code)
                        .unwrap_or(ErrorCode::UnknownServerError),
                )
            })
    })
}

/// A [`Service`] using its [`Storage`] taking [`ProduceRequest`] returning [`ProduceResponse`].
/// ```no_run
/// use bytes::Bytes;
/// use rama::Service as _;
/// use nisshi_sans_io::{
///     CreateTopicsRequest, ErrorCode, ProduceRequest,
///     create_topics_request::CreatableTopic,
///     produce_request::{PartitionProduceData, TopicProduceData},
///     record::{Record, deflated::Frame, inflated},
/// };
/// use nisshi_storage::{CreateTopicsService, Error, ProduceService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const CLUSTER_ID: &str = "nisshi";
/// const NODE_ID: i32 = 111;
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
///
/// let storage = StorageContainer::builder()
///     .cluster_id(CLUSTER_ID)
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let create_topic = CreateTopicsService {
///     storage: storage.clone(),
/// };
///
/// let name = "abcba";
///
/// let response = create_topic
///     .serve(
///         CreateTopicsRequest::default()
///             .topics(Some(vec![
///                 CreatableTopic::default()
///                     .name(name.into())
///                     .num_partitions(5)
///                     .replication_factor(3)
///                     .assignments(Some([].into()))
///                     .configs(Some([].into())),
///             ]))
///             .validate_only(Some(false)),
///     )
///     .await?;
///
/// let topics = response.topics.unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
///
/// let produce = ProduceService {
///     storage: storage.clone(),
/// };
///
/// let partition = 0;
///
/// let response = produce
///     .serve(
///         ProduceRequest::default().topic_data(Some(
///             [TopicProduceData::default()
///                 .name(name.into())
///                 .partition_data(Some(
///                     [PartitionProduceData::default()
///                         .index(partition)
///                         .records(Some(Frame {
///                             batches: vec![
///                                 inflated::Batch::builder()
///                                     .record(
///                                         Record::builder().value(
///                                             Bytes::from_static(
///                                                 b"Lorem ipsum dolor sit amet",
///                                             )
///                                             .into(),
///                                         ),
///                                     )
///                                     .build()
///                                     .and_then(TryInto::try_into)?,
///                             ],
///                         }))]
///                     .into(),
///                 ))]
///             .into(),
///         )),
///     )
///     .await?;
///
/// let topics = response.responses.as_deref().unwrap_or_default();
/// assert_eq!(1, topics.len());
/// let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(partitions[0].error_code)?
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ProduceService<G> {
    pub storage: G,
}

impl<G> ApiKey for ProduceService<G> {
    const KEY: i16 = ProduceRequest::KEY;
}

impl<G> ProduceService<G>
where
    G: Storage,
{
    fn error(&self, index: i32, error_code: ErrorCode) -> PartitionProduceResponse {
        PartitionProduceResponse::default()
            .index(index)
            .error_code(error_code.into())
            .base_offset(-1)
            .log_append_time_ms(Some(-1))
            .log_start_offset(Some(0))
            .record_errors(Some([].into()))
            .error_message(None)
            .current_leader(None)
    }

    #[instrument(skip_all)]
    async fn partition(
        &self,
        transaction_id: Option<&str>,
        name: &str,
        partition: PartitionProduceData,
    ) -> PartitionProduceResponse {
        if let Some(records) = partition.records {
            if let Some((rejected, error_code, reason)) = records.batches.iter().find_map(|batch| {
                rejection(batch).map(|(error_code, reason)| (batch, error_code, reason))
            }) {
                warn!(
                    topic = name,
                    partition = partition.index,
                    record_count = rejected.record_count,
                    last_offset_delta = rejected.last_offset_delta,
                    ?error_code,
                    reason,
                    "rejecting produce batch",
                );

                return self
                    .error(partition.index, error_code)
                    .error_message(Some(reason.into()));
            }

            let mut base_offset = None;

            for mut batch in records.batches {
                let tp = Topition::new(name, partition.index);

                if BatchAttribute::try_from(batch.attributes)
                    .map(|attributes| attributes.timestamp == TimestampType::LogAppendTime)
                    .unwrap_or_default()
                {
                    let base_timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|duration| duration.as_millis() as i64)
                        .unwrap_or_default();

                    batch.base_timestamp = base_timestamp;
                    batch.max_timestamp = base_timestamp;
                }

                match self.storage.produce(transaction_id, &tp, batch).await {
                    Ok(offset) => _ = base_offset.get_or_insert(offset),

                    Err(error) => {
                        let error_code = storage_error_code(&error);

                        // Logged once, here, with the partition: a rejection
                        // the client caused is a warning, an internal failure
                        // is an error.
                        if error_code == ErrorCode::UnknownServerError {
                            error!(
                                topic = name,
                                partition = partition.index,
                                ?error,
                                "produce failed"
                            );
                        } else {
                            warn!(
                                topic = name,
                                partition = partition.index,
                                ?error_code,
                                %error,
                                "rejecting produce batch"
                            );
                        }

                        return self.error(partition.index, error_code);
                    }
                }
            }

            if let Some(base_offset) = base_offset {
                PartitionProduceResponse::default()
                    .index(partition.index)
                    .error_code(ErrorCode::None.into())
                    .base_offset(base_offset)
                    .log_append_time_ms(Some(-1))
                    .log_start_offset(Some(0))
                    .record_errors(Some([].into()))
                    .error_message(None)
                    .current_leader(None)
            } else {
                self.error(partition.index, ErrorCode::UnknownServerError)
            }
        } else {
            self.error(partition.index, ErrorCode::UnknownServerError)
        }
    }

    #[instrument(skip_all)]
    async fn topic(
        &self,
        transaction_id: Option<&str>,
        topic: TopicProduceData,
    ) -> TopicProduceResponse {
        let mut partitions = vec![];

        if let Some(partition_data) = topic.partition_data {
            for partition in partition_data {
                partitions.push(self.partition(transaction_id, &topic.name, partition).await)
            }
        }

        TopicProduceResponse::default()
            .name(topic.name)
            .partition_responses(Some(partitions))
    }
}

impl<G, I> Service<I> for ProduceService<G>
where
    G: Storage,
    I: Into<RequestInput<ProduceRequest>> + Send + 'static,
{
    type Output = ProduceResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let mut responses = Vec::with_capacity(
            input
                .request
                .topic_data
                .as_ref()
                .map_or(0, |topic_data| topic_data.len()),
        );

        if let Some(topics) = input.request.topic_data {
            for topic in topics {
                responses.push(
                    self.topic(input.request.transactional_id.as_deref(), topic)
                        .await,
                )
            }
        }

        // Kafka's `KafkaApis.handleProduceRequest` closes the connection for an
        // `acks=0` request whenever any partition in the assembled response map
        // carries an error, not only a storage failure: `unauthorizedTopicResponses`,
        // `nonExistingTopicResponses` and `invalidRequestResponses` are merged into
        // that map before the check runs. The client reads no response under
        // `acks=0` regardless, so closing on any error (rather than silently
        // dropping the batch) matches that behaviour and gives a real Kafka
        // producer a retriable `NetworkException` instead of nothing at all.
        if input.request.acks == i16::from(Ack::None) {
            if let Some((topic, partition, error_code)) = first_error(&responses) {
                return Err(Error::AcksZeroProduceFailed {
                    topic,
                    partition,
                    error_code,
                });
            }

            SuppressResponseExtension::mark(&input.extensions);
        }

        Ok(ProduceResponse::default()
            .responses(Some(responses))
            .throttle_time_ms(Some(0))
            .node_endpoints(None))
    }
}

// #[cfg(all(test, feature = "dynostore"))]
// mod tests {
//     use super::*;
//     use crate::{Error, service::init_producer_id::InitProducerIdService};
//     use bytes::Bytes;
//     use nisshi_sans_io::{
//         ErrorCode, InitProducerIdRequest,
//         record::{
//             Record,
//             deflated::{self, Frame},
//             inflated,
//         },
//     };
//     use object_store::memory::InMemory;
//     use rama::Context;
//     use tracing::subscriber::DefaultGuard;

//     fn init_tracing() -> Result<DefaultGuard> {
//         use std::{fs::File, sync::Arc, thread};

//         use tracing::Level;
//         use tracing_subscriber::fmt::format::FmtSpan;

//         Ok(tracing::subscriber::set_default(
//             tracing_subscriber::fmt()
//                 .with_level(true)
//                 .with_line_number(true)
//                 .with_thread_names(false)
//                 .with_max_level(Level::DEBUG)
//                 .with_span_events(FmtSpan::ACTIVE)
//                 .with_writer(
//                     thread::current()
//                         .name()
//                         .ok_or(Error::Message(String::from("unnamed thread")))
//                         .and_then(|name| {
//                             File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
//                                 .map_err(Into::into)
//                         })
//                         .map(Arc::new)?,
//                 )
//                 .finish(),
//         ))
//     }

//     fn topic_data(
//         topic: &str,
//         index: i32,
//         builder: inflated::Builder,
//     ) -> Result<Option<Vec<TopicProduceData>>> {
//         builder
//             .build()
//             .and_then(deflated::Batch::try_from)
//             .map(|deflated| {
//                 let partition_data =
//                     PartitionProduceData::default()
//                         .index(index)
//                         .records(Some(Frame {
//                             batches: vec![deflated],
//                         }));

//                 Some(vec![
//                     TopicProduceData::default()
//                         .name(topic.into())
//                         .partition_data(Some(vec![partition_data])),
//                 ])
//             })
//             .map_err(Into::into)
//     }

//     #[tokio::test]
//     async fn non_txn_idempotent_unknown_producer_id() -> Result<()> {
//         let _guard = init_tracing()?;

//         let cluster = "abc";
//         let node = 12321;

//         let topic = "pqr";
//         let index = 0;

//         let transactional_id = None;
//         let acks = 0;
//         let timeout_ms = 0;

//         let storage = DynoStore::new(cluster, node, InMemory::new());
//         let ctx = Context::with_state(storage);
//         let service = ProduceService;

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::UnknownProducerId.into())
//                                 .base_offset(-1)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             service
//                 .serve(
//                     ctx,
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id)
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(
//                                     Record::builder().value(Bytes::from_static(b"lorem").into())
//                                 )
//                                 .producer_id(54345)
//                         )?)
//                 )
//                 .await?
//         );

//         Ok(())
//     }

//     #[tokio::test]
//     async fn non_txn_idempotent() -> Result<()> {
//         let _guard = init_tracing()?;

//         let cluster = "abc";
//         let node = 12321;
//         let topic = "pqr";
//         let index = 0;

//         let storage = DynoStore::new(cluster, node, InMemory::new());
//         let ctx = Context::with_state(storage);

//         let init_producer_id = InitProducerIdService;

//         let producer = init_producer_id
//             .serve(
//                 ctx.clone(),
//                 InitProducerIdRequest::default()
//                     .transactional_id(None)
//                     .transaction_timeout_ms(0)
//                     .producer_id(Some(-1))
//                     .producer_epoch(Some(-1)),
//             )
//             .await?;

//         let request = ProduceService;

//         let transactional_id = None;
//         let acks = 0;
//         let timeout_ms = 0;

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::None.into())
//                                 .base_offset(0)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx.clone(),
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id.clone())
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(Record::builder().value(
//                                     Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
//                                 ))
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::None.into())
//                                 .base_offset(1)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx.clone(),
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id.clone())
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(Record::builder().value(
//                                     Bytes::from_static(b"consectetur adipiscing elit").into()
//                                 ))
//                                 .record(
//                                     Record::builder()
//                                         .value(Bytes::from_static(b"sed do eiusmod tempor").into())
//                                 )
//                                 .base_sequence(1)
//                                 .last_offset_delta(1)
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::None.into())
//                                 .base_offset(3)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx,
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id.clone())
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(
//                                     Record::builder()
//                                         .value(Bytes::from_static(b"incididunt ut labore").into())
//                                 )
//                                 .base_sequence(3)
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         Ok(())
//     }

//     #[tokio::test]
//     async fn non_txn_idempotent_duplicate_sequence() -> Result<()> {
//         let _guard = init_tracing()?;

//         let cluster = "abc";
//         let node = 12321;
//         let topic = "pqr";
//         let index = 0;

//         let storage = DynoStore::new(cluster, node, InMemory::new());
//         let ctx = Context::with_state(storage);

//         let init_producer_id = InitProducerIdService;

//         let producer = init_producer_id
//             .serve(
//                 ctx.clone(),
//                 InitProducerIdRequest::default()
//                     .transactional_id(None)
//                     .transaction_timeout_ms(0)
//                     .producer_id(Some(-1))
//                     .producer_epoch(Some(-1)),
//             )
//             .await?;

//         let request = ProduceService;

//         let transactional_id = None;
//         let acks = 0;
//         let timeout_ms = 0;

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::None.into())
//                                 .base_offset(0)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx.clone(),
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id.clone())
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(Record::builder().value(
//                                     Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
//                                 ))
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::DuplicateSequenceNumber.into())
//                                 .base_offset(-1)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx,
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id)
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(Record::builder().value(
//                                     Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
//                                 ))
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         Ok(())
//     }

//     #[tokio::test]
//     async fn non_txn_idempotent_sequence_out_of_order() -> Result<()> {
//         let _guard = init_tracing()?;

//         let cluster = "abc";
//         let node = 12321;
//         let topic = "pqr";
//         let index = 0;

//         let storage = DynoStore::new(cluster, node, InMemory::new());
//         let ctx = Context::with_state(storage);

//         let init_producer_id = InitProducerIdService;

//         let producer = init_producer_id
//             .serve(
//                 ctx.clone(),
//                 InitProducerIdRequest::default()
//                     .transactional_id(None)
//                     .transaction_timeout_ms(0)
//                     .producer_id(Some(-1))
//                     .producer_epoch(Some(-1)),
//             )
//             .await?;

//         let request = ProduceService;

//         let transactional_id = None;
//         let acks = 0;
//         let timeout_ms = 0;

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::None.into())
//                                 .base_offset(0)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx.clone(),
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id.clone())
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(Record::builder().value(
//                                     Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
//                                 ))
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         assert_eq!(
//             ProduceResponse::default()
//                 .responses(Some(vec![
//                     TopicProduceResponse::default()
//                         .name(topic.into())
//                         .partition_responses(Some(vec![
//                             PartitionProduceResponse::default()
//                                 .index(index)
//                                 .error_code(ErrorCode::OutOfOrderSequenceNumber.into())
//                                 .base_offset(-1)
//                                 .log_append_time_ms(Some(-1))
//                                 .log_start_offset(Some(0))
//                                 .record_errors(Some(vec![]))
//                                 .error_message(None)
//                                 .current_leader(None)
//                         ]))
//                 ]))
//                 .throttle_time_ms(Some(0))
//                 .node_endpoints(None),
//             request
//                 .serve(
//                     ctx,
//                     ProduceRequest::default()
//                         .transactional_id(transactional_id)
//                         .acks(acks)
//                         .timeout_ms(timeout_ms)
//                         .topic_data(topic_data(
//                             topic,
//                             index,
//                             inflated::Batch::builder()
//                                 .record(Record::builder().value(
//                                     Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
//                                 ))
//                                 .base_sequence(2)
//                                 .producer_id(producer.producer_id)
//                         )?)
//                 )
//                 .await?
//         );

//         Ok(())
//     }
// }
