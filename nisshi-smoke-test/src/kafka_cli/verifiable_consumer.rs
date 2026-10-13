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

//! `kafka-verifiable-consumer`, which reports each record it consumes and each offset it commits.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::{BackgroundTool, KafkaCli, Output, StoredRecord, TOOL_TIMEOUT, Tool};

/// The consumer's session timeout, the lowest that a Kafka broker accepts by default.
const SESSION_TIMEOUT_MILLIS: &str = "6000";

/// The command of [`KafkaCli::verifiable_consume`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum VerifiableConsume {}

/// A JSON line that `kafka-verifiable-consumer` prints. The `name` field selects the variant, and
/// `Other` matches the lines that the tests don't read.
#[derive(Deserialize)]
#[serde(tag = "name", rename_all = "snake_case")]
enum ConsumerEvent {
    /// A record that the consumer read. The tool prints this line only with `--verbose`.
    RecordData {
        value: String,
        partition: u32,
        offset: i64,
    },
    /// A commit, with the next offset to read on each partition it committed.
    OffsetsCommitted {
        timestamp: i64,
        offsets: Vec<CommittedOffset>,
        success: bool,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct CommittedOffset {
    partition: u32,
    offset: i64,
}

/// Returns the arguments of `kafka-verifiable-consumer`. Without `max_messages`, the consumer reads
/// until it is stopped.
fn verifiable_consumer_args(topic: &str, group: &str, max_messages: Option<usize>) -> Vec<String> {
    let max_messages = max_messages.map(|max| max.to_string());

    [
        "--topic",
        topic,
        "--group-id",
        group,
        "--verbose",
        "--session-timeout",
        SESSION_TIMEOUT_MILLIS,
    ]
    .into_iter()
    .chain(
        max_messages
            .iter()
            .flat_map(|max| ["--max-messages", max.as_str()]),
    )
    .map(ToOwned::to_owned)
    .collect()
}

impl KafkaCli {
    /// Reads `max_messages` records of `topic` with `kafka-verifiable-consumer`, as a member of
    /// `group`. The consumer starts each partition at the group's committed offset, or at the
    /// beginning when the group has none, and commits after each batch.
    ///
    /// The harness stops a consumer that hasn't read `max_messages` records within a minute. This
    /// function then returns what the consumer read, with [`Output::timeout`] set, instead of
    /// panicking.
    ///
    /// Sends `FindCoordinator`, `JoinGroup`, `SyncGroup`, `OffsetFetch`, `Fetch`, `OffsetCommit`
    /// and `LeaveGroup`.
    pub fn verifiable_consume(
        &self,
        topic: &str,
        group: &str,
        max_messages: usize,
    ) -> Output<VerifiableConsume> {
        let args = verifiable_consumer_args(topic, group, Some(max_messages));

        self.exec_until_timeout(
            Tool::VerifiableConsumer,
            &args.iter().map(String::as_str).collect::<Vec<_>>(),
            None,
            TOOL_TIMEOUT,
        )
    }

    /// Starts a consumer that consumes `topic` with `kafka-verifiable-consumer`, as a member of
    /// `group`, until the test stops it. The consumer starts each partition as
    /// [`KafkaCli::verifiable_consume`] does.
    pub fn start_verifiable_consume(
        &self,
        topic: &str,
        group: &str,
    ) -> BackgroundTool<VerifiableConsume> {
        let args = verifiable_consumer_args(topic, group, None);

        self.start(
            Tool::VerifiableConsumer,
            &args.iter().map(String::as_str).collect::<Vec<_>>(),
        )
    }
}

impl Output<VerifiableConsume> {
    /// Returns each record the consumer read, in the order it printed them.
    #[track_caller]
    pub fn consumed_records(&self) -> Vec<StoredRecord> {
        self.json_events::<ConsumerEvent>()
            .filter_map(|event| match event {
                ConsumerEvent::RecordData {
                    value,
                    partition,
                    offset,
                } => Some(StoredRecord {
                    partition,
                    offset,
                    value,
                }),
                ConsumerEvent::OffsetsCommitted { .. } | ConsumerEvent::Other => None,
            })
            .collect()
    }

    /// Returns the last offset that the consumer committed on each partition, from the commits that
    /// the broker acknowledged before `reported_before`, in milliseconds since the Unix epoch.
    #[track_caller]
    pub fn last_acknowledged_commits_before(&self, reported_before: i64) -> BTreeMap<u32, i64> {
        let mut commits = BTreeMap::new();

        for event in self.json_events::<ConsumerEvent>() {
            if let ConsumerEvent::OffsetsCommitted {
                timestamp,
                offsets,
                success: true,
            } = event
                && timestamp < reported_before
            {
                for committed in offsets {
                    _ = commits.insert(committed.partition, committed.offset);
                }
            }
        }

        commits
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Output, StoredRecord, VerifiableConsume};

    /// What `kafka-verifiable-consumer` 3.9 printed with `--verbose`, shortened, plus a failed
    /// commit.
    const PRINTED: &str = r#"{"timestamp":1791572882769,"name":"startup_complete"}
{"timestamp":1791572898259,"name":"partitions_assigned","partitions":[{"topic":"t","partition":0},{"topic":"t","partition":1}]}
{"timestamp":1791572898483,"name":"record_data","key":null,"value":"494","topic":"t","partition":1,"offset":0}
{"timestamp":1791572898524,"name":"records_consumed","count":1,"partitions":[{"topic":"t","partition":1,"count":1,"minOffset":0,"maxOffset":0}]}
{"timestamp":1791572898535,"name":"offsets_committed","offsets":[{"topic":"t","partition":1,"offset":1},{"topic":"t","partition":0,"offset":136}],"success":true}
{"timestamp":1791572898561,"name":"offsets_committed","offsets":[{"topic":"t","partition":0,"offset":636}],"success":false,"error":"timed out"}
"#;

    #[test]
    fn consumed_records_are_read() {
        assert_eq!(
            Output::<VerifiableConsume>::printed(PRINTED).consumed_records(),
            [StoredRecord {
                partition: 1,
                offset: 0,
                value: "494".to_owned(),
            }]
        );
    }

    #[test]
    fn a_failed_commit_is_left_out() {
        assert_eq!(
            Output::<VerifiableConsume>::printed(PRINTED)
                .last_acknowledged_commits_before(i64::MAX),
            BTreeMap::from([(0, 136), (1, 1)])
        );
    }

    #[test]
    fn a_commit_reported_later_is_left_out() {
        assert_eq!(
            Output::<VerifiableConsume>::printed(PRINTED)
                .last_acknowledged_commits_before(1791572898535),
            BTreeMap::new()
        );
    }
}
