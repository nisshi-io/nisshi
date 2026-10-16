// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{collections::HashMap, error::Error, str::FromStr};

use crate::Result;
use clap::Subcommand;
use nisshi_sans_io::ErrorCode;
use nisshi_topic::Topic;
use url::Url;

use super::DEFAULT_BROKER;

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    /// Create a topic
    Create {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// The name of the topic to create
        #[clap(value_parser)]
        name: String,

        /// The number of partitions to create
        #[arg(long, default_value = "3")]
        partitions: i32,

        #[arg(long, value_parser = parse_key_val::<String, String>)]
        config: Vec<(String, String)>,
    },

    /// Delete an existing topic
    Delete {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// The name of the topic to delete
        #[clap(value_parser)]
        name: String,
    },

    /// List existing topics
    List {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,
    },
}

impl From<Command> for Topic {
    fn from(value: Command) -> Self {
        match value {
            Command::Create {
                broker,
                name,
                partitions,
                config,
            } => Topic::create()
                .broker(broker)
                .name(name)
                .partitions(partitions)
                .config(HashMap::from_iter(config))
                .build(),

            Command::Delete { broker, name } => Topic::delete().broker(broker).name(name).build(),

            Command::List { broker } => Topic::list().broker(broker).build(),
        }
    }
}

impl Command {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        Topic::from(self).main().await.map_err(Into::into)
    }
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}
