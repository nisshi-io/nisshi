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

//! `kafka-consumer-groups`, which lists, describes and deletes consumer groups, and moves their
//! offsets.

use std::fmt::Display;

use super::{KafkaCli, Output, Tool};

/// A consumer group's state, as `kafka-consumer-groups` prints it. The variants are the states of
/// the classic group protocol and of the consumer group protocol (KIP-848), because a tool that
/// describes a group with `ConsumerGroupDescribe` prints the latter's states.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupState {
    /// The group does not have members.
    Empty,
    /// The broker waits for the members to join again, at the start of a rebalance.
    PreparingRebalance,
    /// The broker waits for the group's leader to send the members' assignments.
    CompletingRebalance,
    /// The broker computes a new assignment for the group's members.
    Assigning,
    /// The group's members move to their new assignment.
    Reconciling,
    /// Every member of the group has its partitions.
    Stable,
    /// The broker is removing the group.
    Dead,
    /// The client does not know the state that the broker sent.
    Unknown,
}

impl GroupState {
    pub fn printed_name(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Assigning => "Assigning",
            Self::Reconciling => "Reconciling",
            Self::Stable => "Stable",
            Self::Dead => "Dead",
            Self::Unknown => "Unknown",
        }
    }

    /// Returns the state that `printed_name` names. Panics with `output` if no state has that
    /// name, so that a state from a new Kafka release fails with a clear message.
    #[track_caller]
    fn from_printed_name(printed_name: &str, output: &impl Display) -> Self {
        let Some(state) = [
            Self::Empty,
            Self::PreparingRebalance,
            Self::CompletingRebalance,
            Self::Assigning,
            Self::Reconciling,
            Self::Stable,
            Self::Dead,
            Self::Unknown,
        ]
        .into_iter()
        .find(|state| state.printed_name() == printed_name) else {
            panic!(
                "kafka-consumer-groups printed a group state this reader doesn't know, \
                 `{printed_name}`: {output}"
            )
        };

        state
    }
}

/// The command of [`KafkaCli::describe_group`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum DescribeGroup {}

/// The command of [`KafkaCli::describe_group_state`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum DescribeGroupState {}

/// The command of [`KafkaCli::describe_group_members`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum DescribeGroupMembers {}

/// The command of [`KafkaCli::list_groups`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum ListGroups {}

/// The command of [`KafkaCli::list_groups_in_state`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum ListGroupsInState {}

impl KafkaCli {
    /// Describes `group`'s members and offsets.
    ///
    /// Sends `ConsumerGroupDescribe`, `DescribeGroups` and `OffsetFetch`. The tool tries
    /// `ConsumerGroupDescribe` first, and falls back to `DescribeGroups` for a group that uses the
    /// classic protocol, as the console consumer's groups do.
    pub fn describe_group(&self, group: &str) -> Output<DescribeGroup> {
        self.run(Tool::ConsumerGroups, &["--describe", "--group", group])
    }

    /// Describes `group`'s [`GroupState`], in a table with a `STATE` column.
    ///
    /// Sends `ConsumerGroupDescribe` and `DescribeGroups`.
    pub fn describe_group_state(&self, group: &str) -> Output<DescribeGroupState> {
        self.run(
            Tool::ConsumerGroups,
            &["--describe", "--group", group, "--state"],
        )
    }

    /// Describes `group`'s members, in a table with a row per member and the number of
    /// partitions it reads.
    ///
    /// Sends `ConsumerGroupDescribe` and `DescribeGroups`.
    pub fn describe_group_members(&self, group: &str) -> Output<DescribeGroupMembers> {
        self.run(
            Tool::ConsumerGroups,
            &["--describe", "--group", group, "--members"],
        )
    }

    /// Lists the cluster's consumer groups, one name per line.
    ///
    /// Sends `ListGroups`.
    pub fn list_groups(&self) -> Output<ListGroups> {
        self.run(Tool::ConsumerGroups, &["--list"])
    }

    /// Lists the consumer groups in `state`, in a table with a `GROUP` and a `STATE` column.
    ///
    /// Sends `ListGroups`, with a filter on the state.
    pub fn list_groups_in_state(&self, state: GroupState) -> Output<ListGroupsInState> {
        self.run(
            Tool::ConsumerGroups,
            &["--list", "--state", state.printed_name()],
        )
    }

    /// Lists the offsets that `reset_option` gives `group` in `topic`, with `--dry-run`, which
    /// does not commit them. `reset_option` holds one option of `--reset-offsets` and its value as
    /// separate arguments, such as `["--shift-by", "-2"]`.
    ///
    /// Sends `OffsetFetch` and `ListOffsets`.
    pub fn plan_offset_reset(&self, group: &str, topic: &str, reset_option: &[&str]) -> Output {
        self.reset_offsets_with(group, topic, reset_option, "--dry-run")
    }

    /// Commits the offsets that `reset_option` gives `group` in `topic`, and lists them as
    /// [`KafkaCli::plan_offset_reset`] does. The tool refuses a group that has members.
    ///
    /// Sends `OffsetFetch`, `ListOffsets` and `OffsetCommit`.
    pub fn reset_offsets(&self, group: &str, topic: &str, reset_option: &[&str]) -> Output {
        self.reset_offsets_with(group, topic, reset_option, "--execute")
    }

    fn reset_offsets_with(
        &self,
        group: &str,
        topic: &str,
        reset_option: &[&str],
        apply_option: &str,
    ) -> Output {
        let mut args = vec!["--reset-offsets", "--group", group, "--topic", topic];
        args.extend(reset_option);
        args.push(apply_option);

        self.run(Tool::ConsumerGroups, &args)
    }

    /// Deletes `group` and its offsets.
    ///
    /// Sends `DeleteGroups`.
    pub fn delete_group(&self, group: &str) -> Output {
        self.run(Tool::ConsumerGroups, &["--delete", "--group", group])
    }

    /// Deletes `group`'s offsets in `topic`, and keeps its offsets in other topics.
    ///
    /// Sends `OffsetDelete`.
    pub fn delete_group_offsets(&self, group: &str, topic: &str) -> Output {
        self.run(
            Tool::ConsumerGroups,
            &["--delete-offsets", "--group", group, "--topic", topic],
        )
    }
}

/// A partition's offsets in a consumer group, from `kafka-consumer-groups --describe`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupOffsets {
    pub committed: u64,
    pub end: u64,
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

/// The heading of the table that [`KafkaCli::describe_group_members`] prints (split by whitespace).
const GROUP_MEMBERS_HEADING: &[&str] =
    &["GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS"];

/// The heading of the table that [`KafkaCli::list_groups_in_state`] prints (split by whitespace).
const GROUPS_IN_STATE_HEADING: &[&str] = &["GROUP", "STATE"];

impl Output<ListGroups> {
    /// Returns whether the list names `group`.
    pub fn lists_group(&self, group: &str) -> bool {
        self.lines().contains(&group)
    }
}

impl Output<ListGroupsInState> {
    /// Returns the state that the list shows for `group`, or `None` if the list has no row for it.
    #[track_caller]
    pub fn group_state(&self, group: &str) -> Option<GroupState> {
        let state = self
            .group_table_rows(GROUPS_IN_STATE_HEADING)
            .into_iter()
            .find_map(|columns| match columns[..] {
                [row_group, state] if row_group == group => Some(state),
                _ => None,
            })?;

        Some(GroupState::from_printed_name(state, self))
    }
}

impl Output<DescribeGroupMembers> {
    /// Returns the number of partitions that each of `group`'s members reads, in the order of the
    /// table's rows.
    #[track_caller]
    pub fn member_partition_counts(&self, group: &str) -> Vec<u32> {
        self.group_table_rows(GROUP_MEMBERS_HEADING)
            .into_iter()
            .filter_map(|columns| match columns[..] {
                [row_group, _, _, _, partition_count, ..] if row_group == group => {
                    partition_count.parse().ok()
                }
                _ => None,
            })
            .collect()
    }
}

impl Output<DescribeGroupState> {
    /// Returns `group`'s state, or `None` if the table has no row for `group`.
    ///
    /// The reader reads the state second from the right, because `STATE` and `#MEMBERS` end every
    /// row, and the assignment strategy before them is one word, `-` or nothing.
    #[track_caller]
    pub fn group_state(&self, group: &str) -> Option<GroupState> {
        let state = self
            .group_table_rows(GROUP_STATE_HEADING)
            .into_iter()
            .find_map(|columns| match columns[..] {
                [row_group, .., state, _members] if row_group == group => Some(state),
                _ => None,
            })?;

        Some(GroupState::from_printed_name(state, self))
    }
}

impl Output<DescribeGroup> {
    /// Returns the offsets of `group` on `topic`'s `partition`, or `None` if the table has no row
    /// for them.
    #[track_caller]
    pub fn group_offsets(&self, group: &str, topic: &str, partition: u32) -> Option<GroupOffsets> {
        self.group_table_rows(GROUP_OFFSETS_HEADING)
            .into_iter()
            .find_map(|columns| match columns[..] {
                [row_group, row_topic, row_partition, committed, end, ..]
                    if row_group == group
                        && row_topic == topic
                        && row_partition == partition.to_string() =>
                {
                    Some(GroupOffsets {
                        committed: committed.parse().ok()?,
                        end: end.parse().ok()?,
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

#[cfg(test)]
mod tests {
    use super::{DescribeGroupState, GroupState, Output};

    /// What `kafka-consumer-groups --describe --state` printed, with the assignment strategy
    /// left out, as `-`, and as a member id.
    const GROUP_STATES_PRINTED: &str = concat!(
        "\n",
        "GROUP                     COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE           #MEMBERS\n",
        "empty-9658                127.0.0.1:29092 (111)                          Empty           0\n",
        "empty-21620               127.0.0.1:29092  (111)    -                    Empty           0\n",
        "stable-16188              127.0.0.1:29092  (111)    consumer-stable-16188-1-411daec8-2436-498b-9916-59ca8b8876cb Stable          0\n",
    );

    #[test]
    fn group_state_is_read_whatever_the_assignment_strategy_column_holds() {
        let output = Output::<DescribeGroupState>::printed(GROUP_STATES_PRINTED);

        assert_eq!(output.group_state("empty-9658"), Some(GroupState::Empty));
        assert_eq!(output.group_state("empty-21620"), Some(GroupState::Empty));
        assert_eq!(output.group_state("stable-16188"), Some(GroupState::Stable));
        assert_eq!(output.group_state("missing"), None);
    }
}
