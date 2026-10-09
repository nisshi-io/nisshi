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

//! Harness for the smoke suite: run the real Kafka CLI tools against a
//! broker on a chosen storage engine, the way a user would.
//!
//! `just smoke <engine>` passes the suite its configuration through the environment, which
//! [`settings`] reads.

use std::time::{SystemTime, UNIX_EPOCH};

use nanoid::nanoid;

pub mod broker;
mod kafka_cli;
mod record;
pub mod settings;
mod storage_url;
mod timed_command;

pub use broker::{Broker, LaunchOptions, free_port};
pub use kafka_cli::{
    Acks, AnyCommand, ApiVersions, ClusterId, Consume, ConsumeMatching, ConsumedRecord,
    CreateTopic, DeleteTopic, DescribeConfigs, DescribeGroup, DescribeGroupState, DescribeTopic,
    GetOffsets, GroupOffsets, KafkaCli, LOG_APPEND_TIME, ListTopics, Output, PartitionReplicas,
    PrintsPartitionLines, RECORD_TOO_LARGE_EXCEPTION, RequiresExistingTopic,
    SASL_AUTHENTICATION_EXCEPTION, ScramLogin, ScramMechanism, TOPIC_EXISTS_EXCEPTION,
    UNSUPPORTED_VERSION_EXCEPTION, VerifiableProduce, verifiable_producer_values,
};
pub use record::Record;
pub use storage_url::StorageUrl;

/// The cluster id of the broker that all tests share.
pub const SHARED_CLUSTER_ID: &str = "nisshi-smoke";

/// Returns a name that no other test, process or earlier run uses, so tests can share one broker
/// and one database.
///
/// The random part of the name contains only lowercase letters and digits.
pub fn unique_name(prefix: &str) -> String {
    // This alphabet replaces nanoid's default alphabet, which contains `_` and uppercase letters.
    // `kafka-topics` prints a warning before its output for a topic name that contains `_`, and
    // the tests read the first line of that output.
    const NAME_ALPHABET: &str = "abcdefghijklmnopqrstuvwxyz0123456789";

    let alphabet = NAME_ALPHABET.chars().collect::<Vec<_>>();
    format!("{prefix}-{}", nanoid!(21, &alphabet))
}

/// Returns a random password for a SCRAM user that a test creates.
///
/// The password is a random number in decimal. CodeQL reports a password built from any constant,
/// such as a literal or the alphabet that [`unique_name`] picks from, as a hard-coded cryptographic
/// value, so this one is built from none.
pub fn random_password() -> String {
    rand::random::<u128>().to_string()
}

/// The time now, in milliseconds since the Unix epoch, as Kafka gives record timestamps.
pub fn now_in_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

/// The `docker --label` on every container and volume this run creates.
fn label() -> String {
    format!("--label=nisshi-smoke={}", settings::run_id())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_name_appends_only_lowercase_letters_and_digits() {
        let name = unique_name("topic");
        let random = name.strip_prefix("topic-").unwrap_or_default();

        assert_eq!(random.len(), 21, "{name}");
        assert!(
            random
                .chars()
                .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit()),
            "{name}"
        );
    }
}
