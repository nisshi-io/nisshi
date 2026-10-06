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
    borrow::Cow,
    time::{SystemTime, UNIX_EPOCH},
};

use nisshi_sans_io::{
    ApiKey, BatchAttribute, ErrorCode, ProduceRequest, ProduceResponse, RequestInput,
    TimestampType,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{BatchIndexAndErrorMessage, PartitionProduceResponse, TopicProduceResponse},
    record::{deflated, inflated},
};
use rama::Service;
use tracing::{error, instrument, warn};

use crate::{Error, Result, Storage, Topition};

/// Kafka's wire sentinel for "no timestamp", also the fold-start value
/// `LogValidator` uses for a batch's running maximum.
const NO_TIMESTAMP: i64 = -1;

/// How far ahead of the broker's clock a `CreateTime` record may claim to be.
///
/// This is the `log.message.timestamp.after.max.ms` default of Kafka 4.0
/// ([KIP-1030], [`ServerLogConfigs` 4.0.0]). It differs from Kafka 3.9.1,
/// whose protocol this broker follows: 3.9.1 defaults to `Long.MAX_VALUE`
/// ([`ServerLogConfigs` 3.9.1]) and accepts any future timestamp. A record
/// far in the future keeps its batch's `max_timestamp` ahead of every
/// retention deadline, so the broker uses the 4.0 limit.
///
/// [KIP-1030]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-1030%3A+Change+constraints+and+default+values+for+various+configurations
/// [`ServerLogConfigs` 4.0.0]: https://github.com/apache/kafka/blob/4.0.0/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java#L121
/// [`ServerLogConfigs` 3.9.1]: https://github.com/apache/kafka/blob/3.9.1/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java#L148
const TIMESTAMP_AFTER_MAX_MS: i64 = 3_600_000;

/// Kafka's own `log.message.timestamp.before.max.ms` default
/// (`Long.MAX_VALUE`): no lower bound.
const TIMESTAMP_BEFORE_MAX_MS: i64 = i64::MAX;

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

/// A batch that the broker rejects before it writes anything, with what the
/// client and the operator need to find the cause.
#[derive(Clone, Debug, PartialEq)]
struct Rejection {
    error_code: ErrorCode,
    message: Cow<'static, str>,
    record_error: Option<BatchIndexAndErrorMessage>,
}

impl Rejection {
    fn new(error_code: ErrorCode, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            error_code,
            message: message.into(),
            record_error: None,
        }
    }
}

impl From<(ErrorCode, &'static str)> for Rejection {
    fn from((error_code, message): (ErrorCode, &'static str)) -> Self {
        Self::new(error_code, message)
    }
}

/// Validates the timestamp of each record in a `CreateTime` batch, and
/// rewrites the batch's `max_timestamp` to the largest record timestamp
/// when the header claims a different value.
///
/// Kafka's `LogValidator` does the same: it starts from [`NO_TIMESTAMP`],
/// takes the maximum record timestamp, and overwrites only the header. It
/// exempts a record timestamped [`NO_TIMESTAMP`] from the bounds check.
///
/// Returns `Ok(Some(_))` for a batch that the client must not store: a
/// record timestamp outside the window that [`TIMESTAMP_BEFORE_MAX_MS`] and
/// [`TIMESTAMP_AFTER_MAX_MS`] set around `now`, a timestamp that overflows,
/// a batch that does not decode, or a header rewrite on a batch whose CRC
/// does not match its contents. `Err` is a broker-side failure.
fn rewrite_create_time(batch: &mut deflated::Batch, now: i64) -> Result<Option<Rejection>> {
    let inflated = match inflated::Batch::try_from(&*batch) {
        Ok(inflated) => inflated,
        Err(error) => {
            let compression = BatchAttribute::try_from(batch.attributes)
                .map(|attributes| attributes.compression)
                .ok();

            warn!(?compression, ?error, "produce batch does not decode");

            return Ok(Some(
                if matches!(error, nisshi_sans_io::Error::MessageMaxSizeExceeded(_)) {
                    Rejection::new(
                        ErrorCode::MessageTooLarge,
                        "batch exceeds the maximum decoded batch size",
                    )
                } else {
                    Rejection::new(ErrorCode::InvalidRecord, "batch could not be decoded")
                },
            ));
        }
    };

    let lower = now.saturating_sub(TIMESTAMP_BEFORE_MAX_MS);
    let upper = now.saturating_add(TIMESTAMP_AFTER_MAX_MS);

    let mut actual_max = NO_TIMESTAMP;

    for (batch_index, record) in inflated.records.iter().enumerate() {
        let batch_index = i32::try_from(batch_index)?;

        let Some(timestamp) = batch.base_timestamp.checked_add(record.timestamp_delta) else {
            return Ok(Some(Rejection {
                record_error: Some(
                    BatchIndexAndErrorMessage::default()
                        .batch_index(batch_index)
                        .batch_index_error_message(Some(format!(
                            "base_timestamp {} plus timestamp_delta {} of the record at batch \
                             index {batch_index} overflows",
                            batch.base_timestamp, record.timestamp_delta
                        ))),
                ),
                ..Rejection::new(ErrorCode::InvalidRecord, "record timestamp overflows i64")
            }));
        };

        // Kafka's check is strict, so a timestamp exactly at a bound is
        // accepted.
        if timestamp != NO_TIMESTAMP && (timestamp > upper || timestamp < lower) {
            return Ok(Some(Rejection {
                record_error: Some(
                    BatchIndexAndErrorMessage::default()
                        .batch_index(batch_index)
                        .batch_index_error_message(Some(format!(
                            "Timestamp {timestamp} of the record at batch index {batch_index} \
                             is out of range. The timestamp should be within [{lower}, \
                             {upper}], from the broker's clock at {now}"
                        ))),
                ),
                ..Rejection::new(
                    ErrorCode::InvalidTimestamp,
                    "One or more records have been rejected due to invalid timestamp",
                )
            }));
        }

        actual_max = actual_max.max(timestamp);
    }

    if actual_max == batch.max_timestamp {
        return Ok(None);
    }

    match batch.set_timestamps(batch.base_timestamp, actual_max) {
        Ok(()) => Ok(None),
        Err(nisshi_sans_io::Error::ApiError(ErrorCode::CorruptMessage)) => {
            Ok(Some(corrupt_batch()))
        }
        Err(error) => Err(error.into()),
    }
}

/// The rejection for a batch whose CRC does not match its contents. Kafka
/// rejects such a batch before it validates timestamps.
fn corrupt_batch() -> Rejection {
    Rejection::new(
        ErrorCode::CorruptMessage,
        "batch crc does not match its contents",
    )
}

#[cfg(test)]
mod rewrite_create_time_tests {
    use bytes::{BufMut as _, BytesMut};
    use nisshi_sans_io::{Compression, record::Record};

    use super::*;

    const FIXED_NOW: i64 = 1_800_000_000_000;

    fn batch(base_timestamp: i64, max_timestamp: i64, deltas: &[i64]) -> Result<deflated::Batch> {
        let builder = deltas.iter().enumerate().try_fold(
            inflated::Batch::builder()
                .base_timestamp(base_timestamp)
                .max_timestamp(max_timestamp)
                .last_offset_delta(i32::try_from(deltas.len())? - 1),
            |builder, (offset_delta, timestamp_delta)| {
                i32::try_from(offset_delta).map(|offset_delta| {
                    builder.record(
                        Record::builder()
                            .offset_delta(offset_delta)
                            .timestamp_delta(*timestamp_delta),
                    )
                })
            },
        )?;

        builder
            .build()
            .and_then(deflated::Batch::try_from)
            .map_err(Into::into)
    }

    fn record_error_index(rejection: &Rejection) -> Option<i32> {
        rejection
            .record_error
            .as_ref()
            .map(|record_error| record_error.batch_index)
    }

    #[test]
    fn max_timestamp_is_the_largest_record_not_the_last() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[0, 300, 100])?;

        assert_eq!(None, rewrite_create_time(&mut batch, FIXED_NOW)?);
        assert_eq!(FIXED_NOW + 300, batch.max_timestamp);
        assert_eq!(batch.computed_crc(), batch.crc);

        Ok(())
    }

    #[test]
    fn negative_delta_folds_into_the_maximum() -> Result<()> {
        let mut batch = batch(FIXED_NOW, FIXED_NOW, &[-5_000, -1_000])?;

        assert_eq!(None, rewrite_create_time(&mut batch, FIXED_NOW)?);
        assert_eq!(FIXED_NOW - 1_000, batch.max_timestamp);

        Ok(())
    }

    #[test]
    fn a_header_that_already_matches_is_unchanged() -> Result<()> {
        let mut batch = batch(FIXED_NOW, FIXED_NOW + 7, &[0, 7])?;
        let original = batch.clone();

        assert_eq!(None, rewrite_create_time(&mut batch, FIXED_NOW)?);
        assert_eq!(original, batch);

        Ok(())
    }

    #[test]
    fn exactly_the_upper_bound_is_accepted() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[TIMESTAMP_AFTER_MAX_MS])?;

        assert_eq!(None, rewrite_create_time(&mut batch, FIXED_NOW)?);
        assert_eq!(FIXED_NOW + TIMESTAMP_AFTER_MAX_MS, batch.max_timestamp);

        Ok(())
    }

    #[test]
    fn one_past_the_upper_bound_is_rejected() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[TIMESTAMP_AFTER_MAX_MS + 1])?;

        let rejection = rewrite_create_time(&mut batch, FIXED_NOW)?
            .expect("one millisecond past the upper bound must be rejected");

        assert_eq!(ErrorCode::InvalidTimestamp, rejection.error_code);
        assert_eq!(Some(0), record_error_index(&rejection));

        Ok(())
    }

    #[test]
    fn a_future_record_after_a_sane_one_is_rejected_with_its_index() -> Result<()> {
        let mut batch = batch(FIXED_NOW, FIXED_NOW, &[0, 2 * TIMESTAMP_AFTER_MAX_MS, 0])?;
        let original = batch.clone();

        let rejection = rewrite_create_time(&mut batch, FIXED_NOW)?
            .expect("a record two hours ahead must be rejected");

        assert_eq!(ErrorCode::InvalidTimestamp, rejection.error_code);
        assert_eq!(Some(1), record_error_index(&rejection));

        let message = rejection
            .record_error
            .and_then(|record_error| record_error.batch_index_error_message)
            .unwrap_or_default();
        assert!(message.contains(&(FIXED_NOW + 2 * TIMESTAMP_AFTER_MAX_MS).to_string()));
        assert!(message.contains(&(FIXED_NOW + TIMESTAMP_AFTER_MAX_MS).to_string()));
        assert!(message.contains(&FIXED_NOW.to_string()));

        assert_eq!(original, batch);

        Ok(())
    }

    #[test]
    fn a_timestamp_that_overflows_is_rejected() -> Result<()> {
        let mut batch = batch(i64::MAX - 1, -1, &[10])?;

        let rejection = rewrite_create_time(&mut batch, FIXED_NOW)?
            .expect("an overflowing timestamp must be rejected");

        assert_eq!(ErrorCode::InvalidRecord, rejection.error_code);
        assert_eq!(Some(0), record_error_index(&rejection));

        Ok(())
    }

    #[test]
    fn no_timestamp_is_exempt_from_the_bounds_check() -> Result<()> {
        let mut only_no_timestamp = batch(NO_TIMESTAMP, FIXED_NOW, &[0])?;

        assert_eq!(
            None,
            rewrite_create_time(&mut only_no_timestamp, FIXED_NOW)?
        );
        assert_eq!(NO_TIMESTAMP, only_no_timestamp.max_timestamp);

        let mut mixed = batch(NO_TIMESTAMP, NO_TIMESTAMP, &[0, FIXED_NOW + 1])?;

        assert_eq!(None, rewrite_create_time(&mut mixed, FIXED_NOW)?);
        assert_eq!(FIXED_NOW, mixed.max_timestamp);

        Ok(())
    }

    #[test]
    fn a_batch_that_does_not_decode_is_an_invalid_record() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[0])?;
        batch.record_data = bytes::Bytes::from_static(&[0xff, 0xff, 0xff]);

        let rejection = rewrite_create_time(&mut batch, FIXED_NOW)?
            .expect("a batch that does not decode must be rejected");

        assert_eq!(ErrorCode::InvalidRecord, rejection.error_code);

        Ok(())
    }

    #[test]
    fn a_batch_over_the_decoded_size_limit_is_message_too_large() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[0])?;
        batch.attributes = BatchAttribute::default()
            .compression(Compression::Snappy)
            .into();

        // A raw snappy block starts with its uncompressed length as a
        // varint. This one claims 2,000,000,000 bytes.
        let mut claimed = 2_000_000_000u64;
        let mut record_data = BytesMut::new();
        while claimed >= 0x80 {
            record_data.put_u8((claimed as u8) | 0x80);
            claimed >>= 7;
        }
        record_data.put_u8(claimed as u8);
        batch.record_data = record_data.freeze();

        let rejection = rewrite_create_time(&mut batch, FIXED_NOW)?
            .expect("a batch over the decoded size limit must be rejected");

        assert_eq!(ErrorCode::MessageTooLarge, rejection.error_code);

        Ok(())
    }

    #[test]
    fn a_rewrite_of_a_batch_with_a_bad_crc_is_a_corrupt_message() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[0])?;
        batch.crc ^= 1;
        let damaged = batch.clone();

        assert_eq!(
            Some(corrupt_batch()),
            rewrite_create_time(&mut batch, FIXED_NOW)?
        );
        assert_eq!(damaged, batch);

        Ok(())
    }
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

    fn reject(
        &self,
        name: &str,
        index: i32,
        batch: &deflated::Batch,
        rejection: Rejection,
    ) -> PartitionProduceResponse {
        warn!(
            topic = name,
            partition = index,
            record_count = batch.record_count,
            last_offset_delta = batch.last_offset_delta,
            base_timestamp = batch.base_timestamp,
            max_timestamp = batch.max_timestamp,
            error_code = ?rejection.error_code,
            reason = %rejection.message,
            record_error = rejection
                .record_error
                .as_ref()
                .and_then(|record_error| record_error.batch_index_error_message.as_deref()),
            "rejecting produce batch",
        );

        self.error(index, rejection.error_code)
            .error_message(Some(rejection.message.into_owned()))
            .record_errors(Some(rejection.record_error.into_iter().collect()))
    }

    #[instrument(skip_all)]
    async fn partition(
        &self,
        transaction_id: Option<&str>,
        name: &str,
        partition: PartitionProduceData,
    ) -> PartitionProduceResponse {
        if let Some(records) = partition.records {
            let mut batches = records.batches;

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis() as i64)
                .unwrap_or_default();

            // Every batch is validated and, where needed, rewritten before any
            // of them is written to storage, so a later batch's rejection
            // can't leave an earlier batch's rewrite stored with nothing to
            // follow it.
            for batch in batches.iter_mut() {
                if let Some(rejection) = rejection(batch) {
                    return self.reject(name, partition.index, batch, rejection.into());
                }

                // The batch's own timestamp-type bit selects `LogAppendTime`
                // here. Kafka takes the type from the topic's
                // `message.timestamp.type` instead: on a `CreateTime` topic it
                // validates such a batch like any `CreateTime` batch and
                // clears the bit. A client that sets the bit here skips the
                // timestamp window.
                if BatchAttribute::try_from(batch.attributes)
                    .map(|attributes| attributes.timestamp == TimestampType::LogAppendTime)
                    .unwrap_or_default()
                {
                    match batch.set_timestamps(now, now) {
                        Ok(()) => {}
                        Err(nisshi_sans_io::Error::ApiError(ErrorCode::CorruptMessage)) => {
                            return self.reject(name, partition.index, batch, corrupt_batch());
                        }
                        Err(err) => {
                            error!(?err);
                            return self.error(partition.index, ErrorCode::UnknownServerError);
                        }
                    }
                } else {
                    match rewrite_create_time(batch, now) {
                        Ok(Some(rejection)) => {
                            return self.reject(name, partition.index, batch, rejection);
                        }
                        Ok(None) => {}
                        Err(err) => {
                            error!(?err);
                            return self.error(partition.index, ErrorCode::UnknownServerError);
                        }
                    }
                }
            }

            let mut base_offset = None;

            for batch in batches {
                let tp = Topition::new(name, partition.index);

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
