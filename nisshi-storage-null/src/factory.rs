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

use crate::Engine;
use async_trait::async_trait;
use nisshi_storage::{ArcDynStorage, Result, StorageFactory, StorageFactoryConfiguration};
use regex::Regex;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub struct EngineFactory;

#[async_trait]
impl StorageFactory for EngineFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"null").map_err(Into::into)
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        Ok(Arc::new(Box::new(Engine::new(
            configuration.cluster,
            configuration.node_id,
            configuration.advertised_listener,
        ))) as ArcDynStorage)
    }
}
