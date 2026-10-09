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

//! A topic's life through the Kafka CLI tools, from creation to deletion:
//! creating, listing and describing a topic, reading its offsets before and
//! after producing, producing records with keys and headers, consuming them
//! through a group, describing the group, and deleting the topic.

use std::collections::BTreeSet;

use nisshi_smoke_test::{Broker, KafkaCli, Output, Record, unique_name};

/// Three records with keys and headers.
const RECORDS: [Record<'static>; 3] = [
    Record {
        headers: &[("h1", "pqr"), ("h2", "jkl"), ("h3", "uio")],
        key: "qwerty",
        value: "poiuy",
    },
    Record {
        headers: &[("h1", "def"), ("h2", "lmn"), ("h3", "xyz")],
        key: "asdfgh",
        value: "lkj",
    },
    Record {
        headers: &[("h1", "stu"), ("h2", "fgh"), ("h3", "ijk")],
        key: "zxcvbn",
        value: "mnbvc",
    },
];

/// The partition the producer's default partitioner puts each of
/// [`RECORDS`] on, by its key.
const PARTITIONS: [i32; 3] = [0, 2, 1];

/// Each test topic's partition count.
const PARTITION_COUNT: u32 = 3;

/// Each test topic's configs.
const CONFIGS: &[&str] = &["cleanup.policy=compact"];

/// Creates a test topic with a name no other test uses.
fn topic(cli: &KafkaCli) -> String {
    let topic = unique_name("topic");
    _ = cli
        .create_topic(&topic, PARTITION_COUNT, CONFIGS)
        .succeeded();
    topic
}

/// Creates a test topic and produces [`RECORDS`] to it.
fn topic_with_records(cli: &KafkaCli) -> String {
    let topic = topic(cli);
    produce_records(cli, &topic);
    topic
}

/// Produces [`RECORDS`] to `topic`. The console producer exits with 0 even when the broker
/// rejects a record, and only logs the error, so the log is checked as well.
fn produce_records(cli: &KafkaCli, topic: &str) {
    let produced = cli.produce(topic, &RECORDS);

    assert!(
        !produced
            .succeeded()
            .stderr
            .contains("Error when sending message"),
        "{produced:?}"
    );
}

fn offsets(cli: &KafkaCli, topic: &str, time: &str) -> Vec<String> {
    cli.get_offsets(topic, time)
        .succeeded()
        .stdout
        .lines()
        .map(str::to_owned)
        .collect()
}

/// A partition's offsets in a consumer group, from `kafka-consumer-groups --describe`.
#[derive(Debug, PartialEq)]
struct GroupOffsets {
    committed: u64,
    end: u64,
    lag: u64,
}

/// Finds `partition`'s row in `kafka-consumer-groups --describe` output. A row starts with the
/// group, topic, partition, committed offset, end offset and lag.
fn group_offsets(
    described: &Output,
    group: &str,
    topic: &str,
    partition: u32,
) -> Option<GroupOffsets> {
    described.stdout.lines().find_map(|line| {
        let columns = line.split_whitespace().collect::<Vec<_>>();

        match columns[..] {
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
        }
    })
}

fn each_partition_at(topic: &str, offset: i64) -> Vec<String> {
    (0..PARTITION_COUNT)
        .map(|partition| format!("{topic}:{partition}:{offset}"))
        .collect()
}

/// `kafka-topics` lists the topics first, and finds the topic missing itself, so this checks
/// `Metadata`, not the broker's answer to `DeleteTopics`.
#[test]
fn delete_missing_topic() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = unique_name("topic");

    let deleted = cli.delete_topic(&topic);
    let error = deleted
        .exited(1)
        .lines()
        .first()
        .copied()
        .unwrap_or_default();

    // Kafka 3.9 prints the name as `Optional[<topic>]`.
    assert!(
        error.starts_with("Error while executing topic command : Topic '")
            && error.contains(&topic)
            && error.ends_with("' does not exist as expected"),
        "{deleted:?}"
    );
}

#[test]
fn create_topic_reports_it() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = unique_name("topic");

    let created = cli.create_topic(&topic, PARTITION_COUNT, CONFIGS);

    assert_eq!(
        created.succeeded().lines().first().copied(),
        Some(format!("Created topic {topic}.").as_str())
    );
}

#[test]
fn list_topics() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = topic(&cli);

    let listed = cli.list_topics();

    assert!(
        listed.succeeded().lines().contains(&topic.as_str()),
        "{topic} is not listed: {listed:?}"
    );
}

#[test]
fn create_duplicate_topic() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = topic(&cli);

    let duplicate = cli.create_topic(&topic, PARTITION_COUNT, CONFIGS);

    assert_eq!(
        duplicate.exited(1).lines().first().copied(),
        Some("Error while executing topic command : Topic with this name already exists.")
    );
}

#[test]
fn describe_topic() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = topic(&cli);

    let described = cli.describe_topic(&topic);
    let expected = format!("PartitionCount: {PARTITION_COUNT}");

    assert!(
        described.succeeded().stdout.contains(&expected),
        "no {expected}: {described:?}"
    );
}

/// `kafka-topics --describe` shows only the configs that a topic sets itself, not defaults.
#[test]
#[ignore = "the broker labels a topic's own configs as defaults, so kafka-topics hides them"]
fn describe_topic_configs() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = topic(&cli);

    let described = cli.describe_topic(&topic);

    for config in CONFIGS {
        assert!(
            described.succeeded().stdout.contains(config),
            "no {config}: {described:?}"
        );
    }
}

#[test]
fn earliest_offsets_before_produce() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = topic(&cli);

    assert_eq!(
        offsets(&cli, &topic, "earliest"),
        each_partition_at(&topic, 0)
    );
}

#[test]
fn latest_offsets_before_produce() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = topic(&cli);

    assert_eq!(
        offsets(&cli, &topic, "latest"),
        each_partition_at(&topic, 0)
    );
}

#[test]
fn produce_with_keys_and_headers() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = topic(&cli);

    produce_records(&cli, &topic);
}

#[test]
fn earliest_offsets_after_produce() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = topic_with_records(&cli);

    assert_eq!(
        offsets(&cli, &topic, "earliest"),
        each_partition_at(&topic, 0)
    );
}

#[test]
fn latest_offsets_after_produce() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = topic_with_records(&cli);

    // The producer's default partitioner puts each of the three keys on a
    // different partition.
    assert_eq!(
        offsets(&cli, &topic, "latest"),
        each_partition_at(&topic, 1)
    );
}

#[test]
fn consume_through_a_group() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = topic_with_records(&cli);

    let consumed = cli.consume(&topic, &unique_name("group"), RECORDS.len());
    _ = consumed.succeeded();

    // Kafka orders records within a partition, not across partitions, so
    // the records are compared as a set. Kafka 4 tools also print
    // deprecation warnings on stdout, which aren't records.
    assert_eq!(
        consumed
            .stdout
            .lines()
            .filter(|line| line.starts_with("Partition:"))
            .map(str::to_owned)
            .collect::<BTreeSet<_>>(),
        RECORDS
            .iter()
            .zip(PARTITIONS)
            .map(|(record, partition)| {
                format!("Partition:{partition}\tOffset:0\t{}", record.console_line())
            })
            .collect::<BTreeSet<_>>()
    );

    assert!(
        consumed
            .stderr
            .contains(&format!("Processed a total of {} messages", RECORDS.len())),
        "{consumed:?}"
    );
}

#[test]
fn describe_group_after_consume() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = topic_with_records(&cli);
    let group = unique_name("group");

    _ = cli.consume(&topic, &group, RECORDS.len()).succeeded();

    let described = cli.describe_group(&group);

    for partition in 0..PARTITION_COUNT {
        // The consumer read every record on the partition and committed past them, so the
        // committed offset is the partition's end offset, and nothing is left to read.
        let records = PARTITIONS
            .iter()
            .filter(|&&on| on == partition as i32)
            .count() as u64;

        assert_eq!(
            group_offsets(&described, &group, &topic, partition),
            Some(GroupOffsets {
                committed: records,
                end: records,
                lag: 0,
            }),
            "partition {partition}: {described:?}"
        );
    }
}

#[test]
fn delete_topic() {
    let cli = KafkaCli::new(Broker::shared().bootstrap());
    let topic = topic(&cli);

    let deleted = cli.delete_topic(&topic);

    assert_eq!(deleted.succeeded().stdout, "");

    let listed = cli.list_topics();

    assert!(
        !listed.succeeded().lines().contains(&topic.as_str()),
        "{topic} is still listed: {listed:?}"
    );
}
