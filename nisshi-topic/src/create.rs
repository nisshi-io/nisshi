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

use std::collections::HashMap;

use nisshi_client::{Client, ConnectionManager};
use nisshi_sans_io::{
    CreateTopicsRequest, CreateTopicsResponse, ErrorCode,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    create_topics_response::CreatableTopicResult,
};
use tracing::debug;
use url::Url;

use crate::{Error, Result};

use super::Topic;

#[derive(Clone, Debug, Default)]
pub struct Builder<B, N, P> {
    broker: B,
    name: N,
    partitions: P,
    configs: HashMap<String, String>,
}

impl<B, N, P> Builder<B, N, P> {
    pub fn broker(self, broker: Url) -> Builder<Url, N, P> {
        Builder {
            broker,
            name: self.name,
            partitions: self.partitions,
            configs: self.configs,
        }
    }

    pub fn name(self, name: impl Into<String>) -> Builder<B, String, P> {
        Builder {
            broker: self.broker,
            name: name.into(),
            partitions: self.partitions,
            configs: self.configs,
        }
    }

    pub fn partitions(self, partitions: i32) -> Builder<B, N, i32> {
        Builder {
            broker: self.broker,
            name: self.name,
            partitions,
            configs: self.configs,
        }
    }

    pub fn config(self, configs: HashMap<String, String>) -> Builder<B, N, P> {
        Self { configs, ..self }
    }
}

impl Builder<Url, String, i32> {
    pub fn build(self) -> Topic {
        Topic::Create(Configuration {
            broker: self.broker,
            name: self.name,
            partitions: self.partitions,
            configs: self.configs,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Configuration {
    broker: Url,
    name: String,
    partitions: i32,
    configs: HashMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Create {
    configuration: Configuration,
}

impl TryFrom<Configuration> for Create {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        Ok(Create { configuration })
    }
}

impl Create {
    pub(crate) async fn main(self) -> Result<ErrorCode> {
        let client = ConnectionManager::builder(self.configuration.broker.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        let timeout_ms = 30_000;
        let validate_only = Some(false);

        let req = CreateTopicsRequest::default()
            .topics(Some(
                [CreatableTopic::default()
                    .name(self.configuration.name)
                    .num_partitions(self.configuration.partitions)
                    .replication_factor(-1)
                    .assignments(Some([].into()))
                    .configs(Some(
                        self.configuration
                            .configs
                            .into_iter()
                            .map(|(name, value)| {
                                CreatableTopicConfig::default()
                                    .name(name)
                                    .value(Some(value))
                            })
                            .collect(),
                    ))]
                .into(),
            ))
            .timeout_ms(timeout_ms)
            .validate_only(validate_only);

        let CreateTopicsResponse { topics, .. } = client
            .call(req)
            .await
            .inspect(|response| debug!(?response))?;

        let topics = topics.unwrap_or_default();
        assert_eq!(1, topics.len());

        let CreatableTopicResult { error_code, .. } = topics.first().expect("topics: {topics:?}");

        ErrorCode::try_from(error_code).map_err(Into::into)
    }
}
