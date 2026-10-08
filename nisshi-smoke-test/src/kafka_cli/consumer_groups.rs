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

//! `kafka-consumer-groups`, which describes consumer groups.

use super::{KafkaCli, Output, Tool};

/// The command of [`KafkaCli::describe_group`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum DescribeGroup {}

/// The command of [`KafkaCli::describe_group_state`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum DescribeGroupState {}

impl KafkaCli {
    /// Describes `group`'s members and offsets.
    ///
    /// Sends `ConsumerGroupDescribe`, `DescribeGroups` and `OffsetFetch`. The tool tries
    /// `ConsumerGroupDescribe` first, and falls back to `DescribeGroups` for a group that uses the
    /// classic protocol, as the console consumer's groups do.
    pub fn describe_group(&self, group: &str) -> Output<DescribeGroup> {
        self.run(Tool::ConsumerGroups, &["--describe", "--group", group])
    }

    /// Describes `group`'s state, such as `Stable` while it has members and `Empty` when it has
    /// none, in a table with a `STATE` column.
    ///
    /// Sends `ConsumerGroupDescribe` and `DescribeGroups`.
    pub fn describe_group_state(&self, group: &str) -> Output<DescribeGroupState> {
        self.run(
            Tool::ConsumerGroups,
            &["--describe", "--group", group, "--state"],
        )
    }
}

/// A partition's offsets in a consumer group, from `kafka-consumer-groups --describe`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupOffsets {
    pub committed: u64,
    pub end: u64,
    pub lag: u64,
}

/// The heading of the table that [`KafkaCli::describe_group_state`] prints, split at whitespace.
const GROUP_STATE_HEADING: &[&str] = &[
    "GROUP",
    "COORDINATOR",
    "(ID)",
    "ASSIGNMENT-STRATEGY",
    "STATE",
    "#MEMBERS",
];

/// The heading of the table that [`KafkaCli::describe_group`] prints, split at whitespace.
const GROUP_OFFSETS_HEADING: &[&str] = &[
    "GROUP",
    "TOPIC",
    "PARTITION",
    "CURRENT-OFFSET",
    "LOG-END-OFFSET",
    "LAG",
    "CONSUMER-ID",
    "HOST",
    "CLIENT-ID",
];

impl Output<DescribeGroupState> {
    /// Returns `group`'s state, or `None` if the table has no row for `group`. Kafka 3.9 leaves the assignment strategy blank for a group
    /// without members, so the reader counts the state from the end of the row.
    pub fn group_state(&self, group: &str) -> Option<&str> {
        self.group_table_rows(GROUP_STATE_HEADING)
            .into_iter()
            .find_map(|columns| match columns[..] {
                [row_group, .., state, _members] if row_group == group => Some(state),
                _ => None,
            })
    }
}

impl Output<DescribeGroup> {
    /// Returns the offsets of `group` on `topic`'s `partition`, or `None` if the table has no row
    /// for them.
    pub fn group_offsets(&self, group: &str, topic: &str, partition: u32) -> Option<GroupOffsets> {
        self.group_table_rows(GROUP_OFFSETS_HEADING)
            .into_iter()
            .find_map(|columns| match columns[..] {
                [row_group, row_topic, row_partition, committed, end, lag, ..]
                    if row_group == group
                        && row_topic == topic
                        && row_partition == partition.to_string() =>
                {
                    Some(GroupOffsets {
                        committed: committed.parse().ok()?,
                        end: end.parse().ok()?,
                        lag: lag.parse().ok()?,
                    })
                }

                _ => None,
            })
    }
}

impl<PrintedBy> Output<PrintedBy> {
    /// Returns the rows of the `kafka-consumer-groups` table under `heading`, each split at
    /// whitespace, or no rows if the output has no table. Panics if the table has another heading,
    /// because the readers find each column by its position under `heading`.
    #[track_caller]
    fn group_table_rows(&self, heading: &[&str]) -> Vec<Vec<&str>> {
        let mut lines = self
            .stdout
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>());

        let Some(found_heading) = lines.find(|columns| columns.first() == Some(&"GROUP")) else {
            return Vec::new();
        };

        assert_eq!(
            found_heading, heading,
            "kafka-consumer-groups printed a table with a heading this reader doesn't know: {self}"
        );

        lines.filter(|columns| !columns.is_empty()).collect()
    }
}
