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

//! Tests that `kafka-topics` describes and changes a topic as it does against Apache Kafka, so that
//! admin and monitoring scripts written for Kafka work unchanged.

use nisshi_smoke_test::{KafkaCli, PartitionReplicas, unique_name};

/// The partition count of a topic the broker creates for a producer.
const AUTO_CREATE_PARTITIONS: u32 = 4;

/// Every record on each partition, as offset and value.
fn records_on_each_partition(cli: &KafkaCli, topic: &str) -> Vec<Vec<(i64, String)>> {
    cli.partition_offsets_at(topic, "latest")
        .into_iter()
        .zip(0..)
        .map(|(end, partition)| {
            if end == 0 {
                return Vec::new();
            }

            cli.read_records(topic, partition, 0, end as usize)
                .into_iter()
                .map(|record| (record.offset, record.value))
                .collect()
        })
        .collect()
}

/// Nisshi is one broker, node 111, which holds the only replica of every partition. Monitoring
/// tools read the leader, replicas and in-sync replicas from `--describe` to find partitions that
/// have no leader or too few replicas, so all three must be 111.
#[test]
fn describe_shows_node_111_as_leader_replica_and_isr() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(3, &[]);

    let described = cli.describe_topic(&topic);

    for partition in 0..3 {
        assert_eq!(
            described.succeeded().partition_replicas(partition),
            Some(PartitionReplicas {
                leader: "111",
                replicas: "111",
                isr: "111",
            }),
            "partition {partition}: {described}"
        );
    }
}

/// `kafka-topics --describe` with a mistyped topic name must print an error that names the topic,
/// and must not hang.
#[test]
fn describing_missing_topic_fails_with_its_name() {
    let cli = KafkaCli::shared();
    let topic = unique_name("topic");

    cli.describe_topic(&topic)
        .assert_topic_does_not_exist(&topic);
}

/// A topic deleted and created again under the same name is a new topic: none of the old records
/// come back, and its offsets start again at 0.
#[test]
fn recreated_topic_starts_at_offset_0() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(1, &[]);

    _ = cli.produce_values(&topic, &["old-0", "old-1"]);
    _ = cli.delete_topic(&topic).succeeded();
    _ = cli.create_topic(&topic, 1, &[]).succeeded();
    _ = cli.produce_values(&topic, &["new-0"]);

    assert_eq!(
        records_on_each_partition(&cli, &topic),
        [[(0, "new-0".to_owned())]]
    );
}

/// Producing to a deleted topic recreates it, as Apache Kafka does by default. The new topic must
/// hold only the records produced after the delete, starting at offset 0.
#[test]
fn producing_to_deleted_topic_recreates_it_empty() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(1, &[]);

    _ = cli.produce_values(&topic, &["old-0", "old-1"]);
    _ = cli.delete_topic(&topic).succeeded();
    _ = cli.produce_values(&topic, &["new-0"]);

    assert_eq!(
        records_on_each_partition(&cli, &topic)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>(),
        [(0, "new-0".to_owned())]
    );
}

/// Like Apache Kafka with its default settings, the broker creates a topic when a producer writes
/// to one that doesn't exist. It gives the topic [`AUTO_CREATE_PARTITIONS`] partitions, where
/// Kafka gives it 1.
#[test]
fn produce_to_missing_topic_creates_it() {
    let cli = KafkaCli::shared();
    let topic = unique_name("topic");

    _ = cli.produce_values(&topic, &["created"]);

    let described = cli.describe_topic(&topic);

    assert_eq!(
        described.succeeded().partition_count(),
        Some(AUTO_CREATE_PARTITIONS),
        "{described}"
    );

    assert_eq!(
        records_on_each_partition(&cli, &topic)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>(),
        [(0, "created".to_owned())]
    );
}
