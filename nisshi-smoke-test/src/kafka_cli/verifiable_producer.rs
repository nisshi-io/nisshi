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

//! `kafka-verifiable-producer`, which reports each record the broker acknowledges.

use super::{KafkaCli, Output, Tool};

/// The command of [`KafkaCli::verifiable_produce`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum VerifiableProduce {}

/// What `kafka-verifiable-producer` prints in the JSON line for each record the broker acknowledges.
const ACKNOWLEDGED_RECORD_MARKER: &str = r#""name":"producer_send_success""#;

/// When a producer counts a record as sent. The names match the broker's `Ack`.
#[derive(Clone, Copy, Debug)]
pub enum Acks {
    /// When the record is written to the network, without a response from the broker.
    None,
    /// When the partition leader has stored the record.
    Leader,
    /// When every in-sync replica has stored the record.
    FullIsr,
}

impl Acks {
    /// The value of the producer's `acks` setting.
    fn setting(self) -> &'static str {
        match self {
            Self::None => "0",
            Self::Leader => "1",
            Self::FullIsr => "-1",
        }
    }
}

impl KafkaCli {
    /// Produces `max_messages` records to `topic` with `kafka-verifiable-producer`, which prints
    /// one JSON line per record it sends, with `"name":"producer_send_success"` for each record
    /// the broker acknowledges. Record `n`'s value is `<value_prefix>.<n>`.
    ///
    /// Sends `Produce`. The tool sets `retries=0`, and the Java client then turns idempotence off,
    /// whatever `acks` is. [`KafkaCli::idempotent_verifiable_produce`] turns it on.
    pub fn verifiable_produce(
        &self,
        topic: &str,
        max_messages: usize,
        acks: Acks,
        value_prefix: u32,
    ) -> Output<VerifiableProduce> {
        let max_messages = max_messages.to_string();
        let value_prefix = value_prefix.to_string();

        self.run(
            Tool::VerifiableProducer,
            &[
                "--topic",
                topic,
                "--max-messages",
                &max_messages,
                "--acks",
                acks.setting(),
                "--value-prefix",
                &value_prefix,
            ],
        )
    }

    /// Like [`KafkaCli::verifiable_produce`] with [`Acks::FullIsr`], but the producer is
    /// idempotent: it numbers its batches, so the broker can drop a batch that it gets twice.
    ///
    /// Sends `InitProducerId` and `Produce`.
    pub fn idempotent_verifiable_produce(
        &self,
        topic: &str,
        max_messages: usize,
        value_prefix: u32,
    ) -> Output<VerifiableProduce> {
        // The tool sets `retries=0`, and the Java client refuses `enable.idempotence=true` with
        // it, so the producer is idempotent or the tool fails. The tool reads this file after its
        // own settings, so the file's `retries` replaces that 0.
        self.with_client_properties("enable.idempotence=true\nretries=2147483647\n")
            .verifiable_produce(topic, max_messages, Acks::FullIsr, value_prefix)
    }
}

impl Output<VerifiableProduce> {
    /// Returns how many records the producer says the broker acknowledged.
    #[track_caller]
    pub fn acknowledged_record_count(&self) -> usize {
        self.succeeded()
            .lines()
            .iter()
            .filter(|line| line.contains(ACKNOWLEDGED_RECORD_MARKER))
            .count()
    }
}

/// Returns the values of the first `count` records that [`KafkaCli::verifiable_produce`] sends
/// with `value_prefix`: `<value_prefix>.<n>`.
pub fn verifiable_producer_values(value_prefix: u32, count: usize) -> Vec<String> {
    (0..count).map(|n| format!("{value_prefix}.{n}")).collect()
}
