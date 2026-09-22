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
use nisshi_storage::{ArcDynStorage, Error, Result, StorageFactory, StorageFactoryConfiguration};
use regex::Regex;
use slatedb::{
    Db,
    object_store::{
        ObjectStore,
        aws::{AmazonS3Builder, S3ConditionalPut},
        memory::InMemory,
    },
};

use crate::Engine;

#[derive(Clone, Copy, Debug)]
pub struct EngineFactory;

#[async_trait]
impl StorageFactory for EngineFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"slatedb").map_err(Into::into)
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        let host = configuration.storage.host_str().unwrap_or("nisshi");
        let db_path = format!("nisshi-{}.slatedb", configuration.cluster);

        // Support memory backend for testing: slatedb://memory
        let object_store: Arc<dyn ObjectStore> = if host == "memory" {
            Arc::new(InMemory::new())
        } else {
            // Use S3 backend with host as bucket name
            AmazonS3Builder::from_env()
                .with_bucket_name(host)
                .with_conditional_put(S3ConditionalPut::ETagMatch)
                .build()
                .map(Arc::new)
                .map_err(|e| Error::Message(e.to_string()))?
        };

        Db::open(db_path, object_store)
            .await
            .map(Arc::new)
            .map(|db| {
                Engine::builder()
                    .cluster(configuration.cluster)
                    .node(configuration.node_id)
                    .advertised_listener(configuration.advertised_listener)
                    .db(db)
                    .schemas(configuration.schema_registry)
                    .lake(configuration.lake_house)
                    .build()
            })
            .map(Box::new)
            .map(|storage| Arc::new(storage) as ArcDynStorage)
            .map_err(Into::into)
    }
}
