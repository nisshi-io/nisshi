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

//! The Kafka CLI commands that tests run: creating, listing, describing and deleting topics,
//! producing and consuming records, reading offsets, and describing consumer groups.
//!
//! [`KafkaCli`] runs any tool. These methods build each command's arguments, so a test states what
//! it does and not how the tool spells it.
//!
//! Each method's doc ends with the Kafka APIs that the command is for. A command sends other
//! requests too, such as `ApiVersions` and `Metadata` first, and which ones it sends can change
//! between Kafka versions.

use super::{KafkaCli, Output};
use crate::Record;

impl KafkaCli {
    /// Creates `topic` with `partitions` partitions and `configs`, each a `name=value` topic
    /// config. The broker has one node, so the replication factor is 1.
    ///
    /// Sends `CreateTopics`.
    pub fn create_topic(&self, topic: &str, partitions: u32, configs: &[&str]) -> Output {
        let partitions = format!("--partitions={partitions}");
        let mut args = vec![
            "--create",
            "--topic",
            topic,
            &partitions,
            "--replication-factor=1",
        ];

        for config in configs {
            args.extend(["--config", config]);
        }

        self.run("kafka-topics", &args)
    }

    /// Lists the cluster's topics, one name per line.
    ///
    /// Sends `Metadata`.
    pub fn list_topics(&self) -> Output {
        self.run("kafka-topics", &["--list"])
    }

    /// Describes `topic`'s partitions and configs.
    ///
    /// Sends `DescribeTopicPartitions` and `DescribeConfigs`.
    pub fn describe_topic(&self, topic: &str) -> Output {
        self.run("kafka-topics", &["--describe", "--topic", topic])
    }

    /// Sends `DeleteTopics`.
    pub fn delete_topic(&self, topic: &str) -> Output {
        self.run("kafka-topics", &["--delete", "--topic", topic])
    }

    /// Produces `records` to `topic`, keys and headers included, with the console producer.
    ///
    /// Sends `InitProducerId`, because the producer is idempotent, and `Produce`.
    pub fn produce(&self, topic: &str, records: &[Record<'_>]) -> Output {
        self.run_with_input(
            "kafka-console-producer",
            &[
                "--topic",
                topic,
                "--property",
                "parse.headers=true",
                "--property",
                "parse.key=true",
            ],
            &Record::console_input(records),
        )
    }

    /// Consumes `topic` from the beginning as a member of `group`, and stops after `max_messages`
    /// records or 30 seconds without one.
    ///
    /// Sends `FindCoordinator`, `JoinGroup`, `SyncGroup`, `Fetch`, `OffsetCommit` and `LeaveGroup`.
    ///
    /// The consumer prints each record as `Partition:<n>\tOffset:<n>\t` followed by
    /// [`Record::console_line`].
    pub fn consume(&self, topic: &str, group: &str, max_messages: usize) -> Output {
        let max_messages = max_messages.to_string();

        self.run(
            "kafka-console-consumer",
            &[
                "--timeout-ms",
                "30000",
                "--max-messages",
                &max_messages,
                "--consumer-property",
                "fetch.max.wait.ms=15000",
                "--consumer-property",
                "session.timeout.ms=6000",
                "--group",
                group,
                "--topic",
                topic,
                "--from-beginning",
                "--property",
                "print.key=true",
                "--property",
                "print.offset=true",
                "--property",
                "print.partition=true",
                "--property",
                "print.headers=true",
                "--property",
                "print.value=true",
            ],
        )
    }

    /// Reads each partition's offset of `topic` at `time`, which is `earliest`, `latest` or a
    /// timestamp in milliseconds. The tool prints one `<topic>:<partition>:<offset>` line per
    /// partition.
    ///
    /// Sends `ListOffsets`.
    pub fn get_offsets(&self, topic: &str, time: &str) -> Output {
        self.run("kafka-get-offsets", &["--topic", topic, "--time", time])
    }

    /// Prints the cluster's id, as `Cluster ID: <id>`.
    ///
    /// Sends `DescribeCluster`.
    pub fn cluster_id(&self) -> Output {
        self.run("kafka-cluster", &["cluster-id"])
    }

    /// Describes `group`'s members and offsets.
    ///
    /// Sends `ConsumerGroupDescribe`, `DescribeGroups` and `OffsetFetch`. The tool tries
    /// `ConsumerGroupDescribe` first, and falls back to `DescribeGroups` for a group that uses the
    /// classic protocol, as the console consumer's groups do.
    pub fn describe_group(&self, group: &str) -> Output {
        self.run("kafka-consumer-groups", &["--describe", "--group", group])
    }
}
