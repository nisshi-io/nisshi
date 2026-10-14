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

//! Tests that `kafka-consumer-groups` shows and manages consumer groups correctly. An operator
//! reads a group's committed and end offsets to see whether its consumers keep up, its members to
//! see who reads which partition, and its state to see whether it is running. An operator deletes
//! a group or its offsets to make its consumers start over. A wrong answer makes the operator look
//! for a problem that does not exist, or hides a problem that does.

use std::time::Duration;

use nisshi_smoke_test::{
    BackgroundTool, GROUP_ID_NOT_FOUND_EXCEPTION, GROUP_NOT_EMPTY_EXCEPTION, GroupOffsets,
    GroupState, KafkaCli, VerifiableConsume, unique_name,
};

/// How long a consumer has to join its group and get its partitions.
const JOIN_TIMEOUT: Duration = Duration::from_secs(60);

/// The session timeout of the consumers these tests start, so that the broker removes a member
/// within seconds after it stops sending heartbeats.
const SESSION_TIMEOUT: Duration = Duration::from_secs(6);

/// Starts a consumer in `group`, and waits until it has its partitions.
fn start_member(cli: &KafkaCli, topic: &str, group: &str) -> BackgroundTool<VerifiableConsume> {
    cli.join_group(topic, group, SESSION_TIMEOUT, None, JOIN_TIMEOUT)
        .0
}

/// A group that a consumer has used must be in the list of groups, so an operator can find it.
/// The broker must answer `ListGroups` with every group it stores.
#[test]
fn list_includes_a_group_that_consumed() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(1, &[]);
    let group = unique_name("group");

    _ = cli.produce_values(&topic, &["r0"]);
    _ = cli.consume(&topic, &group, 1).succeeded();

    let listed = cli.list_groups();

    assert!(listed.succeeded().lists_group(&group), "{listed}");
}

/// Listing the groups in a state must show the groups in that state, and only those, so an
/// operator can find the groups that are running, or the ones that are idle. The broker must
/// answer `ListGroups` with each group's state, and apply the state filter.
#[test]
#[ignore = "ListGroups answers every group's state as Unknown, and ignores the state filter (#960)"]
fn list_by_state_shows_only_groups_in_that_state() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(1, &[]);
    let (stable_group, empty_group) = (unique_name("stable"), unique_name("empty"));

    _ = cli.produce_values(&topic, &["r0"]);
    _ = cli.consume(&topic, &empty_group, 1).succeeded();
    let member = start_member(&cli, &topic, &stable_group);

    let listed_stable = cli.list_groups_in_state(GroupState::Stable);
    let listed_empty = cli.list_groups_in_state(GroupState::Empty);

    drop(member.terminate());

    assert_eq!(
        (
            listed_stable.succeeded().group_state(&stable_group),
            listed_stable.group_state(&empty_group),
        ),
        (Some(GroupState::Stable), None),
        "{listed_stable}"
    );
    assert_eq!(
        (
            listed_empty.succeeded().group_state(&empty_group),
            listed_empty.group_state(&stable_group),
        ),
        (Some(GroupState::Empty), None),
        "{listed_empty}"
    );
}

/// Describing a group must show the group's committed offset in a partition and the partition's
/// end offset, which is how an operator sees whether consumers keep up. The
/// broker must answer `OffsetFetch` with the group's committed offset and `ListOffsets` with the
/// partition's end.
#[test]
#[cfg_attr(
    feature = "memory",
    ignore = "the latest offset is the newest batch's first offset plus 1, not the log end (#798)"
)]
fn describe_shows_committed_and_end_offsets() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(1, &[]);
    let group = unique_name("group");

    _ = cli.produce_values(&topic, &["r0", "r1", "r2", "r3", "r4"]);
    _ = cli.consume(&topic, &group, 3).succeeded();

    let described = cli.describe_group(&group);

    assert_eq!(
        described.succeeded().group_offsets(&group, &topic, 0),
        Some(GroupOffsets {
            committed: 3,
            end: 5,
        }),
        "{described}"
    );
}

/// Describing the members of a running group must show each member and how many partitions it
/// reads, so an operator sees who reads what. The broker must answer `DescribeGroups` with each
/// member and its assignment.
#[test]
#[ignore = "DescribeGroups answers no members for a group that has them (#962)"]
fn describe_members_shows_each_member_and_its_partitions() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(2, &[]);
    let group = unique_name("group");

    let (mut first_member, _) = cli.join_group(&topic, &group, SESSION_TIMEOUT, None, JOIN_TIMEOUT);
    let first_member_lines_before_second_join = first_member.line_count();
    let (second_member, second_member_partitions) =
        cli.join_group(&topic, &group, SESSION_TIMEOUT, None, JOIN_TIMEOUT);
    let first_member_partitions = first_member
        .wait_for_partitions_assigned(first_member_lines_before_second_join, JOIN_TIMEOUT)
        .unwrap_or_else(|reason| {
            panic!("the first member didn't get its partitions after the second joined: {reason}")
        });

    let described = cli.describe_group_members(&group);

    drop(first_member.terminate());
    drop(second_member.terminate());

    // The group's leader, a client, decides how many partitions each member gets, so the test
    // compares the broker's answer with the partitions that the members report.
    let mut reported_counts = [
        first_member_partitions.len(),
        second_member_partitions.len(),
    ]
    .map(|count| u32::try_from(count).expect("a partition count fits in a u32"));
    reported_counts.sort_unstable();
    let mut described_counts = described.succeeded().member_partition_counts(&group);
    described_counts.sort_unstable();

    assert_eq!(described_counts, reported_counts, "{described}");
}

/// A group with a running consumer must be `Stable`, and `Empty` once the consumer has left, so
/// an operator can tell a running group from an idle one. The broker must answer `DescribeGroups`
/// with the group's state.
#[test]
fn state_is_stable_while_consuming_and_empty_after() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(1, &[]);
    let group = unique_name("group");

    let member = start_member(&cli, &topic, &group);
    let while_consuming = cli.describe_group_state(&group);

    drop(member.terminate());
    let after_leaving = cli.describe_group_state(&group);

    assert_eq!(
        (
            while_consuming.succeeded().group_state(&group),
            after_leaving.succeeded().group_state(&group),
        ),
        (Some(GroupState::Stable), Some(GroupState::Empty)),
        "while consuming: {while_consuming}\nafter leaving: {after_leaving}"
    );
}

/// Describing a group that never existed must say that it doesn't exist. Showing an empty group
/// instead hides a mistyped group name. The broker must answer `DescribeGroups` for an unknown
/// group with `GROUP_ID_NOT_FOUND`.
#[test]
#[cfg_attr(
    feature = "memory",
    ignore = "DescribeGroups answers an empty group for a group that doesn't exist (#963)"
)]
fn describe_of_unknown_group_says_it_does_not_exist() {
    let cli = KafkaCli::shared();

    let described = cli.describe_group(&unique_name("never-used"));

    assert!(
        described.mentions(GROUP_ID_NOT_FOUND_EXCEPTION),
        "{described}"
    );
}

/// Deleting an empty group must remove it, so that a consumer that joins a group of the same name
/// starts without committed offsets. The broker must delete the group on `DeleteGroups`.
#[test]
fn delete_removes_an_empty_group() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(1, &[]);
    let group = unique_name("group");

    _ = cli.produce_values(&topic, &["r0", "r1"]);
    _ = cli.consume(&topic, &group, 1).succeeded();

    let listed_before = cli.list_groups();
    assert!(
        listed_before.succeeded().lists_group(&group),
        "{listed_before}"
    );

    _ = cli.delete_group(&group).succeeded();

    let listed_after = cli.list_groups();
    assert!(
        !listed_after.succeeded().lists_group(&group),
        "{listed_after}"
    );

    // The group committed offset 1 before the delete, so a consumer that kept that offset would
    // read `r1`.
    let consumed = cli.consume(&topic, &group, 1);
    assert_eq!(
        consumed.succeeded().consumed_values(),
        ["r0"],
        "the group kept its committed offset after the delete: {consumed}"
    );
}

/// Deleting a group that still has members must fail, so that an operator can't remove the
/// offsets of consumers that are running. The broker must answer `DeleteGroups` for such a group
/// with `NON_EMPTY_GROUP`, and keep the group.
#[test]
#[cfg_attr(
    feature = "memory",
    ignore = "ListGroups leaves out a group without committed offsets (#961)"
)]
fn delete_refuses_a_group_with_members() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(1, &[]);
    let group = unique_name("group");

    let member = start_member(&cli, &topic, &group);
    let deleted = cli.delete_group(&group);
    let listed = cli.list_groups();

    drop(member.terminate());

    assert!(deleted.mentions(GROUP_NOT_EMPTY_EXCEPTION), "{deleted}");
    assert!(listed.succeeded().lists_group(&group), "{listed}");
}

/// Deleting a group's offsets for one topic must remove those offsets and keep the group's
/// offsets for its other topics, so that an operator can make the group's consumers read one topic
/// again from the reset policy, and keep their committed offsets in the other topics. The broker
/// must delete only the named topic's offsets on `OffsetDelete`.
#[test]
#[ignore = "the broker doesn't support OffsetDelete (#964)"]
fn delete_offsets_removes_only_that_topics_offsets() {
    let cli = KafkaCli::shared();
    let removed_topic = cli.create_unique_topic(1, &[]);
    let kept_topic = cli.create_unique_topic(1, &[]);
    let group = unique_name("group");

    _ = cli.produce_values(&removed_topic, &["r0"]);
    _ = cli.produce_values(&kept_topic, &["r0"]);
    _ = cli.consume(&removed_topic, &group, 1).succeeded();
    _ = cli.consume(&kept_topic, &group, 1).succeeded();

    _ = cli.delete_group_offsets(&group, &removed_topic).succeeded();

    let described = cli.describe_group(&group);

    assert_eq!(
        (
            described
                .succeeded()
                .group_offsets(&group, &removed_topic, 0),
            described
                .group_offsets(&group, &kept_topic, 0)
                .map(|offsets| offsets.committed),
        ),
        (None, Some(1)),
        "{described}"
    );
}
