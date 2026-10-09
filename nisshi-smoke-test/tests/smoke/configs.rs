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

//! Tests that `kafka-configs` shows the settings a user set on a topic, stops showing one after it
//! is removed, and shows the configs set for every broker, so the user can tell which settings are
//! in force.

use nisshi_smoke_test::KafkaCli;

/// A topic config that isn't the default, so `kafka-configs` shows it as the topic's own.
const NON_DEFAULT_RETENTION: &str = "retention.ms=123456";

/// A setting given when the topic was created must show.
#[test]
#[ignore = "a topic's own configs are reported as defaults, so kafka-configs hides them (#904)"]
fn describe_shows_topic_overrides() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(1, &[NON_DEFAULT_RETENTION]);

    let described = cli.describe_topic_configs(&topic, false);

    assert!(
        described
            .succeeded()
            .describes_config(NON_DEFAULT_RETENTION),
        "{described}"
    );
}

/// A setting added to an existing topic must show.
#[test]
#[ignore = "a topic's own configs are reported as defaults, so kafka-configs hides them (#904)"]
fn add_config_shows_in_describe() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(1, &[]);

    _ = cli
        .add_topic_configs(&topic, &[NON_DEFAULT_RETENTION])
        .succeeded();

    let described = cli.describe_topic_configs(&topic, false);
    assert!(
        described
            .succeeded()
            .describes_config(NON_DEFAULT_RETENTION),
        "{described}"
    );
}

/// A setting removed from a topic must stop showing, because the topic now uses the default.
#[test]
#[ignore = "a topic's own configs are reported as defaults, so kafka-configs hides them (#904)"]
fn delete_config_removes_it_from_describe() {
    let cli = KafkaCli::shared();
    let topic = cli.create_unique_topic(1, &[NON_DEFAULT_RETENTION]);

    let before = cli.describe_topic_configs(&topic, false);
    assert!(
        before.succeeded().describes_config(NON_DEFAULT_RETENTION),
        "{before}"
    );

    _ = cli
        .delete_topic_configs(&topic, &["retention.ms"])
        .succeeded();

    let after = cli.describe_topic_configs(&topic, false);
    assert!(
        !after.succeeded().describes_config(NON_DEFAULT_RETENTION),
        "{after}"
    );
}

/// A user sets a config for every broker in the cluster with `--entity-default`, and must be able
/// to read those settings back. The tool exits with an error when the broker answers with one.
#[test]
#[cfg_attr(
    any(feature = "postgres", feature = "sqlite"),
    ignore = "DescribeConfigs for a broker answers UNKNOWN_TOPIC_OR_PARTITION (#905)"
)]
fn broker_defaults_are_described() {
    let cli = KafkaCli::shared();

    _ = cli.describe_broker_defaults().succeeded();
}
