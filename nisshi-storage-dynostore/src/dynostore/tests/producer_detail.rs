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

use std::assert_matches;

use nisshi_sans_io::ErrorCode;
use nisshi_storage::{Error, Topition};

use crate::dynostore::{
    ProducerDetail,
    produced_batches::{self, SequenceCheck},
};

#[test]
fn producer_stored_before_remembered_batches_keeps_the_duplicate_answer() {
    let producer: ProducerDetail =
        serde_json::from_str(r#"{"sequences":{"0":{"orders":{"0":3}}}}"#)
            .expect("producer detail without batches");

    let topition = Topition::new("orders", 0);

    assert_eq!(3, producer.next_sequence(0, &topition));
    assert_eq!(None, producer.remembered_batches(0, &topition));

    assert_matches!(
        produced_batches::check_sequence(
            producer.next_sequence(0, &topition),
            producer.remembered_batches(0, &topition),
            0,
            2,
        ),
        Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
    );
}

#[test]
fn advance_remembers_the_batch_and_forgets_older_epochs() {
    let mut producer = ProducerDetail::default();
    let topition = Topition::new("orders", 0);

    producer.advance(0, &topition, 0, 2, 10);
    producer.advance(1, &topition, 0, 1, 13);

    assert_eq!(3, producer.next_sequence(0, &topition));
    assert_eq!(2, producer.next_sequence(1, &topition));
    assert_eq!(None, producer.remembered_batches(0, &topition));

    assert_matches!(
        produced_batches::check_sequence(
            producer.next_sequence(1, &topition),
            producer.remembered_batches(1, &topition),
            0,
            1,
        ),
        Ok(SequenceCheck::Duplicate { base_offset: 13 })
    );
}
