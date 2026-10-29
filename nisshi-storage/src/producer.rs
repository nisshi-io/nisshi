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

//! Rules that Apache Kafka applies to a producer's ID and epoch, in an InitProducerId
//! request and in a produced batch, shared by every [`Storage`](crate::Storage) backend.

use std::cmp::Ordering;

use nisshi_sans_io::ErrorCode;

use crate::ProducerIdResponse;

/// The producer ID and epoch in an InitProducerId request.
///
/// Returns `Ok(None)` for a fresh producer, and `Ok(Some((id, epoch)))` when the
/// producer claims the ID and epoch it already holds, to bump its epoch after an
/// error ([KIP-360]).
///
/// v0-2 have no ProducerId or ProducerEpoch fields, so they decode as `None`. Kafka
/// gives an absent field its default of -1, so `(None, None)` is a fresh request.
///
/// A request with only one of the two set to -1 is an
/// [`InvalidRequest`](ErrorCode::InvalidRequest), as in Kafka 3.9.1
/// ([KafkaApis.scala]).
///
/// [KIP-360]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-360%3A+Improve+reliability+of+idempotent%2Ftransactional+producer
/// [KafkaApis.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L2343-L2347
pub fn producer_claim(
    producer_id: Option<i64>,
    producer_epoch: Option<i16>,
) -> Result<Option<(i64, i16)>, ErrorCode> {
    match (producer_id, producer_epoch) {
        (None, None) | (Some(-1), Some(-1)) => Ok(None),
        (Some(id), Some(epoch)) if id != -1 && epoch != -1 => Ok(Some((id, epoch))),
        _ => Err(ErrorCode::InvalidRequest),
    }
}

/// Checks a producer's claim against the producer ID and epoch stored for its
/// transactional ID: [`None`](ErrorCode::None) on an exact match, otherwise
/// [`ProducerFenced`](ErrorCode::ProducerFenced).
///
/// Kafka 3.9.1 also accepts a claim of the previous epoch, from a producer retrying
/// a bump whose response it never received, and answers with the current epoch
/// ([TransactionMetadata.scala]). The backends do not store the previous epoch, so
/// this check fences that claim, and the producer initialises again. We do not take
/// the previous epoch to be the current one minus one, because after a fresh
/// InitProducerId from another producer that guess lets the fenced producer take
/// over.
///
/// After an exhausted epoch moves the transactional ID to a new producer ID (see
/// [`epoch_is_exhausted`]), this check also fences a claim of the previous producer
/// ID. Kafka accepts that claim from a producer retrying the request that moved it
/// ([TransactionMetadata.scala#isValidProducerId]), because it keeps the previous
/// producer ID and epoch in the transaction's metadata. This check compares the claim
/// with the current producer ID and epoch only, and the producer initialises again.
///
/// [TransactionMetadata.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/transaction/TransactionMetadata.scala#L294-L299
/// [TransactionMetadata.scala#isValidProducerId]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/transaction/TransactionMetadata.scala#L212-L224
pub fn check_claim(stored: (i64, i16), claim: (i64, i16)) -> ErrorCode {
    if stored == claim {
        ErrorCode::None
    } else {
        ErrorCode::ProducerFenced
    }
}

/// Whether a producer has exhausted its epoch: an InitProducerId can no longer bump
/// it, and the transactional ID moves to a new producer ID at epoch 0 instead.
///
/// The limit is `i16::MAX - 1`, as in Kafka 3.9.1 ([TransactionMetadata.scala]),
/// so that `i16::MAX` stays free to fence the previous producer ID.
///
/// [TransactionMetadata.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/transaction/TransactionMetadata.scala#L180
pub fn epoch_is_exhausted(epoch: i16) -> bool {
    epoch >= i16::MAX - 1
}

/// The epochs stored for a producer, which a produced batch's epoch is checked against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredEpochs {
    /// Whether the producer is transactional: the request has a transactional ID, the batch
    /// is transactional, or a transactional ID owns the producer ID.
    pub transactional: bool,

    /// The producer's newest epoch.
    pub newest: i16,

    /// The producer's newest epoch that holds a sequence on the batch's partition, or `None`
    /// when the producer has not written to the partition.
    pub partition: Option<i16>,
}

/// How a produced batch's epoch relates to the epochs stored for its producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProduceEpoch {
    /// The batch continues the epoch that the partition holds for the producer, or the
    /// transactional producer's newest epoch.
    Continue,

    /// The batch starts its epoch on the partition, at sequence 0. The backend stores the
    /// epoch for the producer when the producer does not have it yet.
    Start,
}

impl StoredEpochs {
    /// Checks a produced batch's epoch and first sequence against these epochs.
    ///
    /// An idempotent producer bumps its own epoch after an error ([KIP-360]): it keeps its
    /// producer ID, adds one to its epoch, and restarts its sequence at 0. Kafka 3.9.1 keeps
    /// the epoch for each partition. It accepts any epoch higher than the partition's whose
    /// first sequence is 0, and answers
    /// [`OutOfOrderSequenceNumber`](ErrorCode::OutOfOrderSequenceNumber) for any other
    /// sequence ([ProducerAppendInfo.java]). We compare with the partition's epoch too,
    /// because the Java client retries a batch at its old epoch on one partition after it
    /// bumps the epoch for another ([RecordAccumulator.java]).
    ///
    /// A transactional producer's epoch moves only through InitProducerId, so its batch must
    /// be at the producer's newest epoch. [`transactional`](Self::transactional) includes a
    /// producer ID that a transactional ID owns, so that a non-transactional batch cannot
    /// move a transactional producer's epoch.
    ///
    /// Returns [`ProducerFenced`](ErrorCode::ProducerFenced) for:
    /// - an epoch lower than the partition's. Kafka answers
    ///   [`InvalidProducerEpoch`](ErrorCode::InvalidProducerEpoch).
    /// - a transactional producer's epoch other than its newest.
    /// - an epoch that [`epoch_is_exhausted`], because `i16::MAX` stays free to fence a
    ///   retired producer ID. Kafka's partition state accepts it.
    ///
    /// [KIP-360]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-360%3A+Improve+reliability+of+idempotent%2Ftransactional+producer
    /// [ProducerAppendInfo.java]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerAppendInfo.java#L96-L143
    /// [RecordAccumulator.java]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java#L920-L935
    pub fn check_batch(
        &self,
        producer_epoch: i16,
        base_sequence: i32,
    ) -> Result<ProduceEpoch, ErrorCode> {
        if self.transactional {
            return if producer_epoch == self.newest {
                Ok(ProduceEpoch::Continue)
            } else {
                Err(ErrorCode::ProducerFenced)
            };
        }

        match self
            .partition
            .map(|partition| producer_epoch.cmp(&partition))
        {
            Some(Ordering::Equal) => Ok(ProduceEpoch::Continue),
            Some(Ordering::Less) => Err(ErrorCode::ProducerFenced),
            Some(Ordering::Greater) | None if epoch_is_exhausted(producer_epoch) => {
                Err(ErrorCode::ProducerFenced)
            }
            Some(Ordering::Greater) | None if base_sequence != 0 => {
                Err(ErrorCode::OutOfOrderSequenceNumber)
            }
            Some(Ordering::Greater) | None => Ok(ProduceEpoch::Start),
        }
    }
}

impl ProducerIdResponse {
    /// A failed InitProducerId, with no producer ID or epoch, as Kafka answers.
    pub fn failed(error: ErrorCode) -> Self {
        Self {
            error,
            id: -1,
            epoch: -1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh() {
        assert_eq!(Ok(None), producer_claim(None, None));
        assert_eq!(Ok(None), producer_claim(Some(-1), Some(-1)));
    }

    #[test]
    fn claim() {
        assert_eq!(Ok(Some((3, 0))), producer_claim(Some(3), Some(0)));
        assert_eq!(Ok(Some((3, 7))), producer_claim(Some(3), Some(7)));
    }

    #[test]
    fn mixed_is_invalid() {
        for (id, epoch) in [
            (Some(-1), Some(0)),
            (Some(3), Some(-1)),
            (Some(-1), None),
            (None, Some(-1)),
            (Some(3), None),
            (None, Some(0)),
        ] {
            assert_eq!(
                Err(ErrorCode::InvalidRequest),
                producer_claim(id, epoch),
                "{id:?}, {epoch:?}"
            );
        }
    }

    #[test]
    fn check() {
        assert_eq!(ErrorCode::None, check_claim((3, 2), (3, 2)));
        assert_eq!(ErrorCode::ProducerFenced, check_claim((3, 2), (3, 1)));
        assert_eq!(ErrorCode::ProducerFenced, check_claim((3, 2), (3, 3)));
        assert_eq!(ErrorCode::ProducerFenced, check_claim((3, 2), (4, 2)));
    }

    #[test]
    fn exhausted() {
        assert!(!epoch_is_exhausted(0));
        assert!(!epoch_is_exhausted(32765));
        assert!(epoch_is_exhausted(32766));
        assert!(epoch_is_exhausted(i16::MAX));
    }

    const IDEMPOTENT: StoredEpochs = StoredEpochs {
        transactional: false,
        newest: 5,
        partition: Some(3),
    };

    const TRANSACTIONAL: StoredEpochs = StoredEpochs {
        transactional: true,
        newest: 3,
        partition: Some(3),
    };

    #[test]
    fn idempotent_continues_partition_epoch() {
        assert_eq!(Ok(ProduceEpoch::Continue), IDEMPOTENT.check_batch(3, 7));
    }

    #[test]
    fn idempotent_below_partition_epoch_is_fenced() {
        assert_eq!(Err(ErrorCode::ProducerFenced), IDEMPOTENT.check_batch(2, 0));
    }

    #[test]
    fn idempotent_bump_at_sequence_zero_starts_epoch() {
        assert_eq!(Ok(ProduceEpoch::Start), IDEMPOTENT.check_batch(4, 0));
        assert_eq!(Ok(ProduceEpoch::Start), IDEMPOTENT.check_batch(6, 0));
        assert_eq!(Ok(ProduceEpoch::Start), IDEMPOTENT.check_batch(32765, 0));
    }

    #[test]
    fn idempotent_bump_at_other_sequence_is_out_of_order() {
        assert_eq!(
            Err(ErrorCode::OutOfOrderSequenceNumber),
            IDEMPOTENT.check_batch(4, 1)
        );
    }

    #[test]
    fn idempotent_bump_into_exhausted_epoch_is_fenced() {
        assert_eq!(
            Err(ErrorCode::ProducerFenced),
            IDEMPOTENT.check_batch(32766, 0)
        );
        assert_eq!(
            Err(ErrorCode::ProducerFenced),
            IDEMPOTENT.check_batch(i16::MAX, 0)
        );
    }

    #[test]
    fn idempotent_first_batch_on_partition_starts_its_epoch() {
        let unwritten = StoredEpochs {
            partition: None,
            ..IDEMPOTENT
        };

        assert_eq!(Ok(ProduceEpoch::Start), unwritten.check_batch(5, 0));
        assert_eq!(Ok(ProduceEpoch::Start), unwritten.check_batch(4, 0));
        assert_eq!(Ok(ProduceEpoch::Start), unwritten.check_batch(6, 0));
        assert_eq!(
            Err(ErrorCode::OutOfOrderSequenceNumber),
            unwritten.check_batch(5, 1)
        );
    }

    #[test]
    fn transactional_continues_newest_epoch_only() {
        assert_eq!(Ok(ProduceEpoch::Continue), TRANSACTIONAL.check_batch(3, 7));
        assert_eq!(
            Err(ErrorCode::ProducerFenced),
            TRANSACTIONAL.check_batch(2, 0)
        );
        assert_eq!(
            Err(ErrorCode::ProducerFenced),
            TRANSACTIONAL.check_batch(4, 0)
        );

        let unwritten = StoredEpochs {
            partition: None,
            ..TRANSACTIONAL
        };
        assert_eq!(Ok(ProduceEpoch::Continue), unwritten.check_batch(3, 0));
        assert_eq!(Err(ErrorCode::ProducerFenced), unwritten.check_batch(4, 0));
    }
}
