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

//! `kafka-topics`, which creates, lists, describes, alters and deletes topics.

use super::{KafkaCli, Output, TOPIC_EXISTS_EXCEPTION, Tool};
use crate::unique_name;

/// How `kafka-topics --describe` starts the line for the topic and the line for each partition.
const DESCRIBED_LINE_PREFIX: &str = "Topic: ";
/// The name of the field that holds the topic's partition count, on the topic's line.
const PARTITION_COUNT_FIELD: &str = "PartitionCount";
/// The name of the field that holds the configs the topic sets itself, on the topic's line.
const CONFIGS_FIELD: &str = "Configs";
/// The names of the fields on each partition's line.
const PARTITION_FIELD: &str = "Partition";
const LEADER_FIELD: &str = "Leader";
const REPLICAS_FIELD: &str = "Replicas";
const ISR_FIELD: &str = "Isr";

/// The command of [`KafkaCli::create_topic`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum CreateTopic {}

/// The command of [`KafkaCli::list_topics`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum ListTopics {}

/// The command of [`KafkaCli::describe_topic`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum DescribeTopic {}

/// The command of [`KafkaCli::delete_topic`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum DeleteTopic {}

/// A `kafka-topics` command that names one topic, and fails if that topic doesn't exist.
pub trait RequiresExistingTopic {}

impl RequiresExistingTopic for DescribeTopic {}
impl RequiresExistingTopic for DeleteTopic {}

impl KafkaCli {
    /// Creates `topic` with `partitions` partitions and `configs`, each a `name=value` topic
    /// config. The broker has one node, so the replication factor is 1.
    ///
    /// Sends `CreateTopics`.
    pub fn create_topic(
        &self,
        topic: &str,
        partitions: u32,
        configs: &[&str],
    ) -> Output<CreateTopic> {
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

        self.run(Tool::Topics, &args)
    }

    /// Creates a topic with a name no other test uses, as [`KafkaCli::create_topic`] does, asserts
    /// that the tool succeeded, and returns the topic's name.
    pub fn create_unique_topic(&self, partitions: u32, configs: &[&str]) -> String {
        let topic = unique_name("topic");
        _ = self.create_topic(&topic, partitions, configs).succeeded();
        topic
    }

    /// Lists the cluster's topics, one name per line.
    ///
    /// Sends `Metadata`.
    pub fn list_topics(&self) -> Output<ListTopics> {
        self.run(Tool::Topics, &["--list"])
    }

    /// Describes `topic`'s partitions and configs.
    ///
    /// Sends `DescribeTopicPartitions` and `DescribeConfigs`.
    pub fn describe_topic(&self, topic: &str) -> Output<DescribeTopic> {
        self.run(Tool::Topics, &["--describe", "--topic", topic])
    }

    /// Sends `DeleteTopics`.
    pub fn delete_topic(&self, topic: &str) -> Output<DeleteTopic> {
        self.run(Tool::Topics, &["--delete", "--topic", topic])
    }
}

/// A partition's leader and replicas, as [`KafkaCli::describe_topic`] shows them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionReplicas<'a> {
    pub leader: &'a str,
    /// The node ids of the partition's replicas, comma separated.
    pub replicas: &'a str,
    /// The node ids of the replicas in sync with the leader, comma separated.
    pub isr: &'a str,
}

impl Output<CreateTopic> {
    /// Asserts that the tool exited with 1 because the topic already exists. The tool names the
    /// broker's error by its Kafka exception, [`TOPIC_EXISTS_EXCEPTION`].
    #[track_caller]
    pub fn assert_topic_already_exists(&self) {
        assert!(
            self.exited(1).mentions(TOPIC_EXISTS_EXCEPTION),
            "the tool didn't report {TOPIC_EXISTS_EXCEPTION}: {self}"
        );
    }
}

impl Output<ListTopics> {
    /// Returns whether the list names `topic`.
    pub fn lists_topic(&self, topic: &str) -> bool {
        self.lines().contains(&topic)
    }
}

impl Output<DescribeTopic> {
    /// Returns the topic's `PartitionCount`.
    pub fn partition_count(&self) -> Option<u32> {
        self.described_fields()
            .find_map(|fields| {
                fields
                    .into_iter()
                    .find(|(name, _)| *name == PARTITION_COUNT_FIELD)
            })?
            .1
            .parse()
            .ok()
    }

    /// Returns the configs that the output shows for the topic, each a `name=value` pair.
    /// `kafka-topics` shows only the configs the topic sets itself.
    pub fn topic_configs(&self) -> Vec<&str> {
        self.described_fields()
            .find_map(|fields| fields.into_iter().find(|(name, _)| *name == CONFIGS_FIELD))
            .map(|(_, configs)| {
                configs
                    .split(',')
                    .filter(|config| !config.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns `partition`'s leader and replicas.
    pub fn partition_replicas(&self, partition: u32) -> Option<PartitionReplicas<'_>> {
        let partition = partition.to_string();

        self.described_fields().find_map(|fields| {
            let field = |wanted: &str| {
                fields
                    .iter()
                    .find(|(name, _)| *name == wanted)
                    .map(|(_, value)| *value)
            };

            (field(PARTITION_FIELD)? == partition).then_some(PartitionReplicas {
                leader: field(LEADER_FIELD)?,
                replicas: field(REPLICAS_FIELD)?,
                isr: field(ISR_FIELD)?,
            })
        })
    }

    /// Each line that starts with `Topic:`, as its tab-separated `<name>: <value>` fields.
    fn described_fields(&self) -> impl Iterator<Item = Vec<(&str, &str)>> {
        self.stdout
            .lines()
            .map(str::trim_start)
            .filter(|line| line.starts_with(DESCRIBED_LINE_PREFIX))
            .map(|line| {
                line.split('\t')
                    .filter_map(|field| field.split_once(':'))
                    .map(|(name, value)| (name.trim(), value.trim()))
                    .collect()
            })
    }
}

impl<PrintedBy: RequiresExistingTopic> Output<PrintedBy> {
    /// Asserts that `kafka-topics` exited with 1 and named the missing `topic`. The tool raises
    /// the error itself, after the broker's metadata doesn't list the topic, so there is no Kafka
    /// exception to check for, and the wording differs between Kafka versions.
    #[track_caller]
    pub fn assert_topic_does_not_exist(&self, topic: &str) {
        assert!(
            self.exited(1).mentions(topic),
            "the tool's error doesn't name {topic}: {self}"
        );
    }
}
