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

//! Tests that a broker stopped with SIGTERM and started again on the same storage still has
//! everything clients stored before: topics, records, committed group offsets and topic configs. A
//! user who restarts the broker, for example to upgrade it, must lose none of it.
//!
//! Memory storage keeps nothing, so on memory the tests check only that the broker starts again,
//! empty and without errors.

/// What clients stored survives a restart, on the engines that keep it.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod persistence {
    use nisshi_smoke_test::{Broker, KafkaCli, unique_name};

    /// A topic config that isn't the default, to find after the restart.
    const NON_DEFAULT_RETENTION: &str = "retention.ms=123456";

    const PRODUCED_RECORDS: [(&str, Option<&str>); 3] =
        [("k0", Some("v0")), ("k1", Some("v1")), ("k2", Some("v2"))];

    /// Where a client stored data before the broker restarted: a topic created with
    /// [`NON_DEFAULT_RETENTION`] that holds [`PRODUCED_RECORDS`], and a group that consumed them.
    struct StoredBeforeRestart {
        topic: String,
        group: String,
    }

    /// Starts a broker, stores data on it as a client would, and restarts it on the same storage.
    fn store_data_and_restart() -> (Broker, StoredBeforeRestart) {
        let broker = Broker::isolated();
        let cli = KafkaCli::new(broker.bootstrap());

        let stored = StoredBeforeRestart {
            topic: unique_name("topic"),
            group: unique_name("group"),
        };

        _ = cli
            .create_topic(&stored.topic, 1, &[NON_DEFAULT_RETENTION])
            .succeeded();
        _ = cli.produce_keyed(&stored.topic, &PRODUCED_RECORDS);
        _ = cli
            .consume(&stored.topic, &stored.group, PRODUCED_RECORDS.len())
            .succeeded();

        (broker.restart(), stored)
    }

    #[test]
    fn topic_survives_restart() {
        let (broker, StoredBeforeRestart { topic, .. }) = &store_data_and_restart();
        let cli = KafkaCli::new(broker.bootstrap());

        let listed = cli.list_topics();

        assert!(
            listed.succeeded().lists_topic(topic),
            "{topic} is not listed: {listed}"
        );
    }

    #[test]
    fn records_survive_restart() {
        let (broker, StoredBeforeRestart { topic, .. }) = &store_data_and_restart();
        let cli = KafkaCli::new(broker.bootstrap());

        assert_eq!(
            cli.read_records(topic, 0, 0, PRODUCED_RECORDS.len())
                .into_iter()
                .map(|record| (record.offset, record.key, record.value))
                .collect::<Vec<_>>(),
            PRODUCED_RECORDS
                .iter()
                .zip(0..)
                .map(|((key, value), offset)| {
                    (
                        offset,
                        (*key).to_owned(),
                        value.unwrap_or_default().to_owned(),
                    )
                })
                .collect::<Vec<_>>()
        );
    }

    /// Without its committed offsets, a consumer group starts again from the beginning after the
    /// restart, and reads every record twice.
    #[test]
    fn committed_offsets_survive_restart() {
        let (broker, StoredBeforeRestart { topic, group }) = &store_data_and_restart();
        let cli = KafkaCli::new(broker.bootstrap());

        let described = cli.describe_group(group);

        assert_eq!(
            described
                .succeeded()
                .group_offsets(group, topic, 0)
                .map(|offsets| offsets.committed),
            Some(PRODUCED_RECORDS.len() as u64),
            "{described}"
        );
    }

    /// A topic's own configs must survive a restart. A broker that loses them applies the defaults
    /// instead, such as a different retention, and nothing tells the user.
    #[test]
    fn topic_config_survives_restart() {
        let (broker, StoredBeforeRestart { topic, .. }) = &store_data_and_restart();
        let cli = KafkaCli::new(broker.bootstrap());

        // `--all` lists every config whatever source the broker gives it, so this check doesn't
        // depend on how the broker labels a topic's own configs.
        let configs = cli.describe_topic_configs(topic, true);

        assert!(
            configs.succeeded().describes_config(NON_DEFAULT_RETENTION),
            "the output does not contain {NON_DEFAULT_RETENTION}: {configs}"
        );
    }
}

/// Memory storage keeps nothing across a restart.
#[cfg(feature = "memory")]
mod memory_storage {
    use nisshi_smoke_test::{Broker, KafkaCli};

    /// The broker must start again empty, without errors.
    #[test]
    fn starts_empty_after_restart() {
        let broker = Broker::isolated();
        let cli = KafkaCli::new(broker.bootstrap());
        let topic = cli.create_unique_topic(1, &[]);

        let broker = broker.restart();
        let cli = KafkaCli::new(broker.bootstrap());

        let listed = cli.list_topics();
        assert!(
            !listed.succeeded().lists_topic(&topic),
            "{topic} is still listed: {listed}"
        );
    }
}
