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
//! Configuration comes from the environment, which `just smoke <engine>` sets up:
//!
//! - `NISSHI_SMOKE_BOOTSTRAP`: the shared broker's address
//! - `NISSHI_SMOKE_STORAGE`: the storage engine URL
//! - `NISSHI_SMOKE_BIN` or `NISSHI_SMOKE_IMAGE`: what [`Broker::isolated`] launches
//! - `NISSHI_SMOKE_KAFKA`: the running container with the Kafka CLI tools
//! - `NISSHI_SMOKE_RUN`: the run's id, which labels every container and
//!   volume the suite creates, so a run removes only its own

use nanoid::nanoid;

mod broker;
mod kafka_cli;
mod record;
mod timed_command;

pub use broker::{Broker, LaunchOptions, free_port};
pub use kafka_cli::{KafkaCli, Output};
pub use record::Record;

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

/// The `docker --label` on every container and volume this run creates.
fn label() -> String {
    format!(
        "--label=nisshi-smoke={}",
        env("NISSHI_SMOKE_RUN").unwrap_or_else(|| "local".to_owned())
    )
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
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
