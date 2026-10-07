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

//! Rules that Apache Kafka applies to an InitProducerId request, shared by every
//! [`Storage`](crate::Storage) backend.

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
/// ([KafkaApis.scala](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L2343-L2347)).
///
/// [KIP-360]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-360%3A+Improve+reliability+of+idempotent%2Ftransactional+producer
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
/// ([TransactionMetadata.scala](https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/transaction/TransactionMetadata.scala#L294-L299)).
/// The backends do not store the previous epoch, so this check fences that claim,
/// and the producer initialises again. We do not take the previous epoch to be the
/// current one minus one, because after a fresh InitProducerId from another producer
/// that guess lets the fenced producer take over.
pub fn check_claim(stored: (i64, i16), claim: (i64, i16)) -> ErrorCode {
    if stored == claim {
        ErrorCode::None
    } else {
        ErrorCode::ProducerFenced
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
}
