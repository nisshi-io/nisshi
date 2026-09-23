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

use super::Engine;

#[derive(Clone, Copy, Debug)]
pub struct LimboFactory;

#[async_trait]
impl StorageFactory for LimboFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"turso").map_err(Into::into)
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        Engine::builder()
            .storage(configuration.storage)
            .node(configuration.node_id)
            .cluster(configuration.cluster)
            .advertised_listener(configuration.advertised_listener)
            .schemas(configuration.schema_registry)
            .lake(configuration.lake_house)
            .build()
            .await
            .map(Box::new)
            .map(|storage| Arc::new(storage) as ArcDynStorage)
    }
}
