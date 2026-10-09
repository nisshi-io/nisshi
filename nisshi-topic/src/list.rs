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

use nisshi_client::{Client, ConnectionManager};
use nisshi_sans_io::{ErrorCode, MetadataRequest, MetadataResponse};
use tracing::debug;
use url::Url;

use crate::{Error, Result, Topic};

#[derive(Clone, Debug, Default)]
pub struct Builder<B> {
    broker: B,
}

impl<B> Builder<B> {
    pub fn broker(self, broker: Url) -> Builder<Url> {
        Builder { broker }
    }
}

impl Builder<Url> {
    pub fn build(self) -> Topic {
        Topic::List(Configuration {
            broker: self.broker,
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Configuration {
    broker: Url,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct List {
    configuration: Configuration,
}

impl TryFrom<Configuration> for List {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        Ok(List { configuration })
    }
}

impl List {
    pub(crate) async fn main(self) -> Result<ErrorCode> {
        let client = ConnectionManager::builder(self.configuration.broker.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        let req = MetadataRequest::default()
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .topics(None);

        let MetadataResponse { topics, .. } = client
            .call(req)
            .await
            .inspect(|response| debug!(?response))?;

        serde_json::to_string(topics.as_deref().unwrap_or_default())
            .inspect(|topics| println!("{topics}"))
            .map_err(Into::into)
            .and(Ok(ErrorCode::None))
    }
}
