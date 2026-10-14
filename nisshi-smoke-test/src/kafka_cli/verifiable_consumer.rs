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

//! `kafka-verifiable-consumer`, which consumes as a member of a group and prints a JSON line for
//! each change to its partitions, each poll, each record it reads and each offset it commits.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use serde::Deserialize;

use super::{BackgroundTool, KafkaCli, Output, StdoutLines, StoredRecord, TOOL_TIMEOUT, Tool};

/// The session timeout of a consumer that [`KafkaVerifiableConsumer::consume`] runs, the lowest
/// that a Kafka broker accepts by default.
const VERIFIABLE_CONSUME_SESSION_TIMEOUT: Duration = Duration::from_secs(6);

/// The command of [`KafkaVerifiableConsumer::consume`]'s [`Output`], and of
/// [`KafkaVerifiableConsumer::start`]'s [`BackgroundTool`].
#[derive(Clone, Copy, Debug)]
pub enum VerifiableConsume {}

/// A topic's partition.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

/// The offsets that a consumer read from one partition in one poll, from `min_offset` to
/// `max_offset`, both included.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumedOffsets {
    pub topic: String,
    pub partition: u32,
    pub min_offset: i64,
    pub max_offset: i64,
}

/// A partition's offset in a commit: the next offset that the group reads there.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub struct CommittedOffset {
    pub partition: u32,
    pub offset: i64,
}

/// An event that `kafka-verifiable-consumer` prints.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "name", rename_all = "snake_case")]
pub enum ConsumerEvent {
    /// The consumer joined its group, or finished a rebalance, and got `partitions`.
    PartitionsAssigned { partitions: Vec<TopicPartition> },
    /// The consumer gave up `partitions`, at the start of a rebalance or when it closes.
    PartitionsRevoked { partitions: Vec<TopicPartition> },
    /// The consumer read records in one poll.
    RecordsConsumed { partitions: Vec<ConsumedOffsets> },
    /// A record that the consumer read. The tool prints this line only with `--verbose`.
    RecordData {
        value: String,
        partition: u32,
        offset: i64,
    },
    /// A commit, with the next offset to read on each partition it committed, and the time the
    /// consumer reported it, in milliseconds since the Unix epoch.
    OffsetsCommitted {
        timestamp: i64,
        offsets: Vec<CommittedOffset>,
        success: bool,
    },
    /// An event that no test reads, such as `startup_complete`.
    #[serde(other)]
    Other,
}

/// Returns the arguments of `kafka-verifiable-consumer`. The consumer starts each partition at the
/// group's committed offset, or at the beginning when the group has none, and commits after each
/// poll. Without `max_messages`, it reads until it is stopped.
fn verifiable_consumer_args(
    topic: &str,
    group: &str,
    session_timeout: Duration,
    group_instance_id: Option<&str>,
    max_messages: Option<usize>,
) -> Vec<String> {
    let mut args = [
        "--topic",
        topic,
        "--group-id",
        group,
        "--verbose",
        "--session-timeout",
        &session_timeout.as_millis().to_string(),
        "--reset-policy",
        "earliest",
    ]
    .map(ToOwned::to_owned)
    .to_vec();

    if let Some(group_instance_id) = group_instance_id {
        args.extend([
            "--group-instance-id".to_owned(),
            group_instance_id.to_owned(),
        ]);
    }

    if let Some(max_messages) = max_messages {
        args.extend(["--max-messages".to_owned(), max_messages.to_string()]);
    }

    args
}

/// Runs `kafka-verifiable-consumer` against the broker of a [`KafkaCli`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaVerifiableConsumer<'cli> {
    cli: &'cli KafkaCli,
}

impl<'cli> KafkaVerifiableConsumer<'cli> {
    pub(super) fn new(cli: &'cli KafkaCli) -> Self {
        Self { cli }
    }
    /// Reads `max_messages` records of `topic` with `kafka-verifiable-consumer`, as a member of
    /// `group`.
    ///
    /// The harness stops a consumer that hasn't read `max_messages` records within a minute. This
    /// function then returns what the consumer read, with [`Output::timeout`] set, instead of
    /// panicking.
    ///
    /// Sends `FindCoordinator`, `JoinGroup`, `SyncGroup`, `OffsetFetch`, `Fetch`, `OffsetCommit`
    /// and `LeaveGroup`.
    #[track_caller]
    pub fn consume(
        &self,
        topic: &str,
        group: &str,
        max_messages: usize,
    ) -> Output<VerifiableConsume> {
        let args = verifiable_consumer_args(
            topic,
            group,
            VERIFIABLE_CONSUME_SESSION_TIMEOUT,
            None,
            Some(max_messages),
        );

        self.cli.exec_until_timeout(
            Tool::VerifiableConsumer,
            &args.iter().map(String::as_str).collect::<Vec<_>>(),
            None,
            TOOL_TIMEOUT,
        )
    }

    /// Starts `kafka-verifiable-consumer`, which consumes `topic` as a member of `group` until the
    /// test stops it, as [`KafkaVerifiableConsumer::consume`] does, and prints a [`ConsumerEvent`]
    /// per line.
    ///
    /// With a `group_instance_id`, the consumer is a static member: a consumer that restarts with
    /// the same id within `session_timeout` gets its partitions back without a rebalance.
    ///
    /// Sends `FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `OffsetFetch`, `Fetch`,
    /// `OffsetCommit` and `LeaveGroup`.
    #[track_caller]
    pub fn start(
        &self,
        topic: &str,
        group: &str,
        session_timeout: Duration,
        group_instance_id: Option<&str>,
    ) -> BackgroundTool<VerifiableConsume> {
        let args = verifiable_consumer_args(topic, group, session_timeout, group_instance_id, None);

        BackgroundTool::start(
            self.cli,
            Tool::VerifiableConsumer,
            &args.iter().map(String::as_str).collect::<Vec<_>>(),
        )
    }

    /// Starts a consumer with [`KafkaVerifiableConsumer::start`], and waits until it has
    /// joined `group` and has its partitions. Returns the consumer and its partitions.
    ///
    /// Panics with what the consumer printed if it does not have its partitions within
    /// `join_timeout`.
    #[track_caller]
    pub fn join_group(
        &self,
        topic: &str,
        group: &str,
        session_timeout: Duration,
        group_instance_id: Option<&str>,
        join_timeout: Duration,
    ) -> (BackgroundTool<VerifiableConsume>, Vec<TopicPartition>) {
        let mut member = self.start(topic, group, session_timeout, group_instance_id);

        match member.wait_for_partitions_assigned(0, join_timeout) {
            Ok(partitions) => (member, partitions),
            Err(reason) => panic!("the consumer didn't join {group}: {reason}"),
        }
    }
}

impl BackgroundTool<VerifiableConsume> {
    /// Waits until the consumer prints a `partitions_assigned` event on a stdout line at or after
    /// `from_line`, and returns the assigned partitions. The consumer prints
    /// the event when it has joined its group and has its partitions. Returns an error with what
    /// the consumer printed if the consumer does not print the event within `timeout`, or exits
    /// before it does.
    pub fn wait_for_partitions_assigned(
        &mut self,
        from_line: usize,
        timeout: Duration,
    ) -> Result<Vec<TopicPartition>, String> {
        self.wait_for_line(
            from_line,
            timeout,
            |stdout_line| match ConsumerEvent::parse(&stdout_line.line) {
                Some(ConsumerEvent::PartitionsAssigned { partitions }) => Some(partitions),
                _ => None,
            },
        )
    }

    /// Waits until the consumer prints a `records_consumed` event on a stdout line at or after
    /// `from_line`. Returns an error with what the consumer printed if the consumer does not print
    /// the event within `timeout`, or exits before it does.
    pub fn wait_for_records_consumed(
        &mut self,
        from_line: usize,
        timeout: Duration,
    ) -> Result<(), String> {
        self.wait_for_line(from_line, timeout, |stdout_line| {
            matches!(
                ConsumerEvent::parse(&stdout_line.line),
                Some(ConsumerEvent::RecordsConsumed { .. })
            )
            .then_some(())
        })
    }
}

impl StdoutLines<VerifiableConsume> {
    /// Returns each event the consumer printed, with the time at which the harness read it, in the
    /// order the consumer printed them.
    pub fn consumer_events(&self) -> Vec<(Instant, ConsumerEvent)> {
        self.lines
            .iter()
            .filter_map(|stdout_line| {
                ConsumerEvent::parse(&stdout_line.line).map(|event| (stdout_line.read_at, event))
            })
            .collect()
    }
}

impl Output<VerifiableConsume> {
    /// Returns each record the consumer read, in the order it printed them.
    #[track_caller]
    pub fn consumed_records(&self) -> Vec<StoredRecord> {
        self.json_events::<ConsumerEvent>(Tool::VerifiableConsumer)
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
                _ => None,
            })
            .collect()
    }

    /// Returns the last offset that the consumer committed on each partition, from the commits that
    /// the broker acknowledged before `reported_before`, in milliseconds since the Unix epoch.
    #[track_caller]
    pub fn last_acknowledged_commits_before(&self, reported_before: i64) -> BTreeMap<u32, i64> {
        let mut commits = BTreeMap::new();

        for event in self.json_events::<ConsumerEvent>(Tool::VerifiableConsumer) {
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

impl ConsumerEvent {
    /// Reads an event from one of the consumer's lines, as [`Tool::read_json_event`] does.
    #[track_caller]
    fn parse(line: &str) -> Option<Self> {
        Tool::VerifiableConsumer.read_json_event(line)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        ConsumedOffsets, ConsumerEvent, Output, StoredRecord, TopicPartition, VerifiableConsume,
    };

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

    #[test]
    fn events_are_parsed_and_other_lines_left_out() {
        let lines = [
            r#"{"timestamp":1791395908729,"name":"partitions_assigned","partitions":[{"topic":"t1","partition":0}]}"#,
            r#"{"timestamp":1791395908781,"name":"records_consumed","count":5,"partitions":[{"topic":"t1","partition":2,"count":5,"minOffset":0,"maxOffset":4}]}"#,
            r#"{"timestamp":1791395908791,"name":"offsets_committed","offsets":[],"success":true}"#,
            "WARNING: something is deprecated",
            "{not an event, although the line starts with a brace",
        ];

        assert_eq!(
            lines
                .into_iter()
                .filter_map(ConsumerEvent::parse)
                .collect::<Vec<_>>(),
            [
                ConsumerEvent::PartitionsAssigned {
                    partitions: vec![TopicPartition {
                        topic: "t1".to_owned(),
                        partition: 0
                    }],
                },
                ConsumerEvent::RecordsConsumed {
                    partitions: vec![ConsumedOffsets {
                        topic: "t1".to_owned(),
                        partition: 2,
                        min_offset: 0,
                        max_offset: 4,
                    }],
                },
                ConsumerEvent::OffsetsCommitted {
                    timestamp: 1791395908791,
                    offsets: Vec::new(),
                    success: true,
                },
            ]
        );
    }

    #[test]
    #[should_panic(expected = "printed an event that this reader doesn't know")]
    fn event_without_the_fields_this_reader_needs_panics() {
        _ = ConsumerEvent::parse(
            r#"{"timestamp":1791395908781,"name":"records_consumed","count":5,"partitions":[{"topic":"t1","partition":2,"count":5,"firstOffset":0,"lastOffset":4}]}"#,
        );
    }
}
