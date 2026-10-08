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

//! `kafka-configs`, which describes and changes the configs of topics, brokers and users.

use super::{KafkaCli, Output, ScramMechanism, Tool};

/// The command of the [`Output`] of `kafka-configs --describe`, from
/// [`KafkaCli::describe_topic_configs`] or [`KafkaCli::describe_broker_defaults`].
#[derive(Clone, Copy, Debug)]
pub enum DescribeConfigs {}

impl KafkaCli {
    /// Describes `topic`'s config overrides, one `<name>=<value> sensitive=<bool> synonyms={...}`
    /// line each. With `all_configs`, it describes every config the topic has, defaults included.
    ///
    /// Sends `DescribeConfigs`.
    pub fn describe_topic_configs(
        &self,
        topic: &str,
        all_configs: bool,
    ) -> Output<DescribeConfigs> {
        let mut args = vec![
            "--entity-type",
            "topics",
            "--entity-name",
            topic,
            "--describe",
        ];

        if all_configs {
            args.push("--all");
        }

        self.run(Tool::Configs, &args)
    }

    /// Sets `configs`, each a `name=value` topic config, on `topic`.
    ///
    /// Sends `IncrementalAlterConfigs`.
    pub fn add_topic_configs(&self, topic: &str, configs: &[&str]) -> Output {
        let configs = configs.join(",");

        self.run(
            Tool::Configs,
            &[
                "--entity-type",
                "topics",
                "--entity-name",
                topic,
                "--alter",
                "--add-config",
                &configs,
            ],
        )
    }

    /// Removes the topic configs `names` from `topic`, so the topic uses the default values of
    /// those configs.
    ///
    /// Sends `IncrementalAlterConfigs`.
    pub fn delete_topic_configs(&self, topic: &str, names: &[&str]) -> Output {
        let names = names.join(",");

        self.run(
            Tool::Configs,
            &[
                "--entity-type",
                "topics",
                "--entity-name",
                topic,
                "--alter",
                "--delete-config",
                &names,
            ],
        )
    }

    /// Describes the cluster-wide broker defaults that `--entity-default` selects.
    ///
    /// Sends `DescribeConfigs`.
    pub fn describe_broker_defaults(&self) -> Output<DescribeConfigs> {
        self.run(
            Tool::Configs,
            &["--entity-type", "brokers", "--entity-default", "--describe"],
        )
    }

    /// Creates credentials for `user` with each mechanism in [`ScramMechanism::ALL`].
    ///
    /// Sends `AlterUserScramCredentials`.
    pub fn add_scram_user(&self, user: &str, password: &str) -> Output {
        let credentials = ScramMechanism::ALL
            .map(|mechanism| format!("{}=[password={password}]", mechanism.name()))
            .join(",");

        self.run(
            Tool::Configs,
            &[
                "--entity-type",
                "users",
                "--entity-name",
                user,
                "--alter",
                "--add-config",
                &credentials,
            ],
        )
    }
}

impl Output<DescribeConfigs> {
    /// Returns whether the output shows `config`, a `name=value` pair.
    pub fn describes_config(&self, config: &str) -> bool {
        self.lines()
            .iter()
            .any(|line| line.trim_start().starts_with(&format!("{config} ")))
    }
}
