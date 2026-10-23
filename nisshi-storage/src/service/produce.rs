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
    sync::LazyLock,
    time::{SystemTime, UNIX_EPOCH},
};

use nisshi_sans_io::{
    Ack, ApiKey, BatchAttribute, ErrorCode, ProduceRequest, ProduceResponse, RequestInput,
    SuppressResponseExtension, TimestampType,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{BatchIndexAndErrorMessage, PartitionProduceResponse, TopicProduceResponse},
    record::{deflated, inflated},
};
use opentelemetry::{KeyValue, metrics::Counter};
use rama::Service;
use tracing::{debug, error, instrument, warn};

use crate::{Error, METER, Result, Storage, Topition, offload};

/// Counts the connections closed by a failed `acks=0` Produce, by the error
/// code of the first failing partition.
///
/// The failed request does not reach the request and error counts, because
/// it ends the connection before a response is encoded. A producer that
/// keeps failing shows here as a reconnect loop.
static ACKS_ZERO_PRODUCE_FAILED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_produce_acks_zero_failed")
        .with_description("Connections closed by a failed acks=0 Produce")
        .build()
});

/// Inflate a batch that a client sent in a Produce request, mapping a
/// decode failure to `INVALID_RECORD`.
///
/// Accepts both a `deflated::Batch` and a `&deflated::Batch`. The two
/// conversions take different decode paths and fail with different
/// [`nisshi_sans_io::Error`] variants for the same bytes: by value, an
/// uncompressed batch is parsed directly and a short record fails with
/// `Overflow` or `TryGet`. By reference, every batch goes through the serde
/// decoder and the same record fails with `Io(UnexpectedEof)`. Mapping every
/// error keeps the client-visible code the same on every backend, whichever
/// path it uses.
///
/// Every error is safe to treat as the client's fault, because decoding only
/// reads bytes already in memory: decompression, varints, lengths and record
/// fields all come from the request. Retrying the same bytes fails the same
/// way, so the client needs `INVALID_RECORD` rather than
/// `UNKNOWN_SERVER_ERROR`.
///
/// For a compressed batch this differs from Kafka on purpose. Kafka answers
/// `INVALID_RECORD` for a truncated uncompressed record, but it wraps a
/// decompression failure, including record data that is not a stream of the
/// declared codec, in a `KafkaException`, and the client gets
/// `UNKNOWN_SERVER_ERROR`. That code invites a retry of the same bytes, which
/// cannot succeed. See [DefaultRecordBatch.java#L633-L641].
///
/// The decode error is logged at debug: [`ProduceService`] logs each
/// rejection once, at warn, with the topic and partition, and a client can
/// send any number of batches that do not decode.
///
/// A batch over the decoded-size limit keeps its
/// [`nisshi_sans_io::Error::MessageMaxSizeExceeded`], so that
/// [`ProduceService`] answers `MESSAGE_TOO_LARGE`, which a producer can
/// recover from by splitting the batch.
///
/// Only use this on a batch from a client Produce request. A stored batch
/// that fails to decode is a broker fault, and must keep its own error.
///
/// [DefaultRecordBatch.java#L633-L641]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L633-L641
pub fn inflate_produced<B>(batch: B) -> Result<inflated::Batch>
where
    inflated::Batch: TryFrom<B, Error = nisshi_sans_io::Error>,
{
    inflated::Batch::try_from(batch).map_err(|err| match err {
        nisshi_sans_io::Error::MessageMaxSizeExceeded(_) => Error::SansIo(err),
        err => {
            debug!(?err, "produce batch does not decode");
            Error::Api(ErrorCode::InvalidRecord)
        }
    })
}

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

/// The message sent with `CORRUPT_MESSAGE` for a batch whose CRC does not
/// match its contents.
const CRC_MISMATCH: &str = "batch crc does not match its contents";

/// Why a client batch must be rejected before anything is written, with the
/// error code to send, or `None` if it may be stored. Kafka's `UnifiedLog` and
/// `LogValidator` reject the same batches.
///
/// - The CRC must match the batch's contents, from `attributes` through the
///   end of the records, as Kafka computes it ([DefaultRecordBatch.java#L393-L398]).
///   Kafka checks it before the batch's header fields
///   ([UnifiedLog.scala#L1189-L1193]): when the CRC is wrong, the attributes,
///   the record count and the control bit are suspect too, so this check runs
///   first. The memory and object store backends store the bytes as sent, so
///   a consumer that checks CRCs would fail on every fetch of the batch.
///   `CORRUPT_MESSAGE`.
/// - Only the broker writes control batches (transaction commit/abort
///   markers), and those go directly to storage rather than through
///   [`ProduceService`]. Every backend skips schema validation and lake
///   writes for a control batch, so a client must not be able to set the bit.
///   `INVALID_RECORD`.
/// - The compression codec in the attributes must be one Kafka defines. An
///   unknown codec id is a defect in the client's batch header, like a bad
///   record count. Kafka returns `UNKNOWN_SERVER_ERROR` for it only because
///   its codec lookup throws an unchecked exception that reaches the generic
///   handler. Checking here also stops a backend from advancing the high
///   watermark before it parses the attributes. `INVALID_RECORD`.
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
///
/// [DefaultRecordBatch.java#L393-L398]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L393-L398
/// [UnifiedLog.scala#L1189-L1193]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/UnifiedLog.scala#L1189-L1193
fn rejection(batch: &deflated::Batch) -> Option<(ErrorCode, &'static str)> {
    if batch.computed_crc() != batch.crc {
        return Some((ErrorCode::CorruptMessage, CRC_MISMATCH));
    }

    if batch.is_control() {
        return Some((
            ErrorCode::InvalidRecord,
            "clients may not write control batches",
        ));
    }

    if BatchAttribute::try_from(batch.attributes).is_err() {
        return Some((ErrorCode::InvalidRecord, "unknown compression codec"));
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
    use nisshi_sans_io::Compression;

    use super::*;

    /// `batch` with a CRC that matches its contents, so that [`rejection`]
    /// gets past its CRC check to the check under test.
    fn with_valid_crc(mut batch: deflated::Batch) -> deflated::Batch {
        batch.crc = batch.computed_crc();
        batch
    }

    #[test]
    fn a_batch_whose_crc_does_not_match_is_a_corrupt_message() {
        let mut batch = with_valid_crc(deflated::Batch {
            record_count: 1,
            last_offset_delta: 0,
            ..Default::default()
        });
        assert_eq!(None, rejection(&batch));

        batch.crc ^= 1;
        assert_eq!(
            Some((ErrorCode::CorruptMessage, CRC_MISMATCH)),
            rejection(&batch)
        );
    }

    /// Kafka checks the CRC before the control bit, so a control batch with
    /// a bad CRC is corrupt, not an invalid record.
    #[test]
    fn the_crc_is_checked_before_the_control_bit() {
        let mut batch = with_valid_crc(deflated::Batch {
            attributes: BatchAttribute::default().control(true).into(),
            record_count: 1,
            last_offset_delta: 0,
            ..Default::default()
        });
        assert_eq!(
            Some((
                ErrorCode::InvalidRecord,
                "clients may not write control batches"
            )),
            rejection(&batch)
        );

        batch.crc ^= 1;
        assert_eq!(
            Some((ErrorCode::CorruptMessage, CRC_MISMATCH)),
            rejection(&batch)
        );
    }

    #[test]
    fn oversized_record_count_is_rejected_before_storage_is_called() {
        let batch = with_valid_crc(deflated::Batch {
            record_count: 1_000_000,
            last_offset_delta: 999_999,
            ..Default::default()
        });

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
        let batch = with_valid_crc(deflated::Batch {
            record_count: 1,
            last_offset_delta: 0,
            ..Default::default()
        });

        assert_eq!(None, rejection(&batch));
    }

    #[test]
    fn an_inconsistent_header_is_an_invalid_record() {
        let batch = with_valid_crc(deflated::Batch {
            record_count: 2,
            last_offset_delta: 0,
            ..Default::default()
        });

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

    /// The batch is compressed: on the by-value path an uncompressed batch
    /// whose `record_count` exceeds what its `record_data` could hold is
    /// reported as corrupt before the decoded-size limit is checked.
    #[test]
    fn a_produced_batch_over_the_decoded_size_limit_is_message_too_large() {
        let oversized = || deflated::Batch {
            attributes: BatchAttribute::default()
                .compression(Compression::Gzip)
                .into(),
            record_count: 1_000_000,
            last_offset_delta: 999_999,
            ..Default::default()
        };

        for result in [
            inflate_produced(&oversized()),
            inflate_produced(oversized()),
        ] {
            let error = result.expect_err("an oversized batch must not decode");
            assert_eq!(ErrorCode::MessageTooLarge, storage_error_code(&error));
        }
    }

    #[test]
    fn a_produced_batch_that_does_not_decode_is_an_invalid_record() {
        let truncated = || deflated::Batch {
            record_count: 1,
            last_offset_delta: 0,
            ..Default::default()
        };

        for result in [
            inflate_produced(&truncated()),
            inflate_produced(truncated()),
        ] {
            let error = result.expect_err("a batch without record data must not decode");
            assert_eq!(ErrorCode::InvalidRecord, storage_error_code(&error));
        }
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
///
/// The batch decodes through [`inflate_produced`], so a batch that does not
/// decode gets the same error code here as on every other produce path.
fn rewrite_create_time(batch: &mut deflated::Batch, now: i64) -> Result<Option<Rejection>> {
    let inflated = match inflate_produced(&*batch) {
        Ok(inflated) => inflated,
        Err(error) => return Ok(Some(undecodable_batch(&error))),
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

/// Runs [`rewrite_create_time`] on Tokio's blocking thread pool, through
/// [`offload`], and hands the batch back with the outcome.
///
/// The batch moves into the closure and back out, because `offload` needs a
/// `'static` closure. A clone would only add a handle to the same record
/// bytes, so moving the batch keeps one owner.
async fn rewrite_create_time_offloaded(
    batch: deflated::Batch,
    now: i64,
) -> Result<(deflated::Batch, Option<Rejection>)> {
    offload(move || {
        let mut batch = batch;
        let rejection = rewrite_create_time(&mut batch, now)?;
        Ok((batch, rejection))
    })
    .await
}

/// The rejection for a client batch that [`inflate_produced`] did not decode,
/// with the error code [`storage_error_code`] gives `error`: `MESSAGE_TOO_LARGE`
/// for a batch over the decoded-size limit, which a producer recovers from by
/// splitting the batch, and `INVALID_RECORD` for any other decode failure.
fn undecodable_batch(error: &Error) -> Rejection {
    let error_code = storage_error_code(error);

    let message = if error_code == ErrorCode::MessageTooLarge {
        "batch exceeds the maximum decoded batch size"
    } else {
        "batch could not be decoded"
    };

    Rejection::new(error_code, message)
}

/// The rejection for a batch whose CRC does not match its contents, as
/// [`rejection`] answers it.
fn corrupt_batch() -> Rejection {
    Rejection::new(ErrorCode::CorruptMessage, CRC_MISMATCH)
}

#[cfg(test)]
mod rewrite_create_time_tests {
    use bytes::{BufMut as _, BytesMut};
    use nisshi_sans_io::{Compression, record::Record};
    use tokio::task::yield_now;

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

    // The test runtime has a single thread. If the rewrite decoded the batch
    // inline, the spawned task would decode, rewrite and return within its
    // first poll, so it would already be finished when `yield_now` returns.
    // Offloaded, it is still waiting on the decode.
    #[tokio::test]
    async fn rewrite_runs_off_the_runtime_thread() -> Result<()> {
        const RECORDS: usize = 1_000;
        const RECORD_SIZE: usize = 1_024;

        // Pseudo-random values, so gzip has real work to do on decode.
        let mut seed = 0x2545_f491_u32;
        let mut builder = inflated::Batch::builder()
            .attributes(
                BatchAttribute::default()
                    .compression(Compression::Gzip)
                    .into(),
            )
            .base_timestamp(FIXED_NOW)
            .max_timestamp(NO_TIMESTAMP)
            .last_offset_delta(i32::try_from(RECORDS)? - 1);

        for offset_delta in 0..RECORDS {
            let value = (0..RECORD_SIZE)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 17;
                    seed ^= seed << 5;
                    seed as u8
                })
                .collect::<bytes::Bytes>();

            builder = builder.record(
                Record::builder()
                    .value(value.into())
                    .offset_delta(i32::try_from(offset_delta)?)
                    .timestamp_delta(i64::try_from(offset_delta)?),
            );
        }

        let batch: deflated::Batch = builder.build().and_then(TryInto::try_into)?;

        let rewrite = tokio::spawn(rewrite_create_time_offloaded(batch, FIXED_NOW));

        yield_now().await;

        assert!(
            !rewrite.is_finished(),
            "the batch was decoded on the runtime thread"
        );

        let (batch, rejection) = rewrite.await??;

        assert_eq!(None, rejection);
        assert_eq!(FIXED_NOW + i64::try_from(RECORDS)? - 1, batch.max_timestamp);
        assert_eq!(batch.computed_crc(), batch.crc);

        Ok(())
    }
}

/// A [`Service`] using its [`Storage`] taking [`ProduceRequest`] returning [`ProduceResponse`].
/// ```no_run
/// use bytes::Bytes;
/// use rama::{Layer as _, Service as _};
/// use nisshi_sans_io::{
///     CreateTopicsRequest, ErrorCode, ProduceRequest,
///     create_topics_request::CreatableTopic,
///     produce_request::{PartitionProduceData, TopicProduceData},
///     record::{Record, deflated::Frame, inflated},
/// };
/// use nisshi_storage::{CreateTopicsService, CreateTopicsValidationLayer, Error, ProduceService, StorageContainer};
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
/// let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
///     storage: storage.clone(),
/// });
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
            producer_id = batch.producer_id,
            attributes = batch.attributes,
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
        // The decoder turns a zero-length records field into `None`, so it
        // counts as zero batches.
        let batches = partition
            .records
            .map(|records| records.batches)
            .unwrap_or_default();

        // Kafka allows exactly one batch per partition from Produce v3, the
        // first version that carries record batches, which is the only format
        // this broker decodes. Kafka checks the count before it validates any
        // batch, so this check runs before `rejection`. See
        // [ProduceRequest.java#L225-L246].
        //
        // [ProduceRequest.java#L225-L246]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java#L225-L246
        let batch_count = batches.len();
        let Ok([mut batch]) = <[deflated::Batch; 1]>::try_from(batches) else {
            const REASON: &str =
                "a produce request must contain exactly one record batch per partition";

            warn!(
                topic = name,
                partition = partition.index,
                batch_count,
                reason = REASON,
                "rejecting produce request",
            );

            return self
                .error(partition.index, ErrorCode::InvalidRecord)
                .error_message(Some(REASON.into()));
        };

        if let Some(rejection) = rejection(&batch) {
            return self.reject(name, partition.index, &batch, rejection.into());
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default();

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
                    return self.reject(name, partition.index, &batch, corrupt_batch());
                }
                Err(err) => {
                    error!(?err);
                    return self.error(partition.index, ErrorCode::UnknownServerError);
                }
            }
        } else {
            match rewrite_create_time_offloaded(batch, now).await {
                Ok((rewritten, None)) => batch = rewritten,
                Ok((rewritten, Some(rejection))) => {
                    return self.reject(name, partition.index, &rewritten, rejection);
                }
                Err(err) => {
                    error!(?err);
                    return self.error(partition.index, ErrorCode::UnknownServerError);
                }
            }
        }

        let tp = Topition::new(name, partition.index);
        let producer_id = batch.producer_id;

        match self.storage.produce(transaction_id, &tp, batch).await {
            Ok(base_offset) => PartitionProduceResponse::default()
                .index(partition.index)
                .error_code(ErrorCode::None.into())
                .base_offset(base_offset)
                .log_append_time_ms(Some(-1))
                .log_start_offset(Some(0))
                .record_errors(Some([].into()))
                .error_message(None)
                .current_leader(None),

            Err(error) => {
                let error_code = storage_error_code(&error);

                // Logged once, here, with the partition: a rejection
                // the client caused is a warning, an internal failure
                // is an error.
                if error_code == ErrorCode::UnknownServerError {
                    error!(
                        topic = name,
                        partition = partition.index,
                        producer_id,
                        ?error,
                        "produce failed"
                    );
                } else {
                    warn!(
                        topic = name,
                        partition = partition.index,
                        producer_id,
                        ?error_code,
                        %error,
                        "rejecting produce batch"
                    );
                }

                self.error(partition.index, error_code)
            }
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
                ACKS_ZERO_PRODUCE_FAILED
                    .add(1, &[KeyValue::new("error_code", format!("{error_code:?}"))]);

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
