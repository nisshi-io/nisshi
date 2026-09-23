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

use std::sync::Arc;

use async_trait::async_trait;
use nisshi_storage::{ArcDynStorage, Result, StorageFactory, StorageFactoryConfiguration};
use regex::Regex;

use super::Postgres;

#[derive(Clone, Copy, Debug)]
pub struct PostgresFactory;

#[async_trait]
impl StorageFactory for PostgresFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"postgres|postgresql").map_err(Into::into)
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        Postgres::builder(configuration.storage.to_string().as_str())
            .map(|builder| builder.cluster(&configuration.cluster))
            .map(|builder| builder.node(configuration.node_id))
            .map(|builder| builder.advertised_listener(configuration.advertised_listener.clone()))
            .map(|builder| builder.schemas(configuration.schema_registry.clone()))
            .map(|builder| builder.lake(configuration.lake_house.clone()))
            .map(|builder| builder.build())
            .map(Box::new)
            .map(|storage| Arc::new(storage) as ArcDynStorage)
    }
}
