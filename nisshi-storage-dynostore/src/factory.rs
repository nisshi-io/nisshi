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

use std::{num::NonZeroU32, str::FromStr as _, sync::Arc, time::Duration};

use async_trait::async_trait;
use nisshi_schema::redact_url;
use nisshi_storage::{
    ArcDynStorage, ProduceRequestBatcher, Result, StorageFactory, StorageFactoryConfiguration,
};
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    gcp::GoogleCloudStorageBuilder,
    memory::InMemory,
};
use regex::Regex;
use tracing::{debug, warn};

use crate::{dynostore::DynoStore, gcs::limit::PutRateLimiter};

#[derive(Clone, Copy, Debug)]
pub struct MemoryEngineFactory;

#[async_trait]
impl StorageFactory for MemoryEngineFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"memory").map_err(Into::into)
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        Ok(Arc::new(Box::new(
            DynoStore::new(
                configuration.cluster.as_str(),
                configuration.node_id,
                InMemory::new(),
            )
            .advertised_listener(configuration.advertised_listener.clone())
            .schemas(configuration.schema_registry)
            .lake(configuration.lake_house.clone()),
        )) as ArcDynStorage)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct S3OptimisticConcurrencyEngineFactory;

#[async_trait]
impl StorageFactory for S3OptimisticConcurrencyEngineFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"s3").map_err(Into::into)
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        let bucket_name = configuration.storage.host_str().unwrap_or("nisshi");

        let minimum_size = configuration.storage.query_pairs().find_map(|(k, v)| {
            if k == "batch_min_size" {
                human_units::Size::from_str(v.as_ref())
                    .map(|size| size.0)
                    .inspect_err(|err| {
                        warn!(storage = %redact_url(&configuration.storage), v = v.as_ref(), ?err)
                    })
                    .ok()
                    .and_then(|size| usize::try_from(size).ok())
            } else {
                None
            }
        });

        let maximum_delay = configuration.storage.query_pairs().find_map(|(k, v)| {
            if k == "batch_max_delay" {
                human_units::Duration::from_str(v.as_ref())
                    .map(|duration| duration.0)
                    .inspect_err(|err| {
                        warn!(storage = %redact_url(&configuration.storage), v = v.as_ref(), ?err)
                    })
                    .ok()
            } else {
                None
            }
        });

        debug!(?minimum_size, ?maximum_delay);

        AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .map(|object_store| {
                DynoStore::new(
                    configuration.cluster.as_str(),
                    configuration.node_id,
                    object_store,
                )
                .advertised_listener(configuration.advertised_listener.clone())
                .schemas(configuration.schema_registry)
                .lake(configuration.lake_house.clone())
            })
            .map(|storage| {
                ProduceRequestBatcher::new(storage)
                    .with_minimum_size(minimum_size)
                    .with_maximum_delay(maximum_delay)
            })
            .map(Box::new)
            .map(|storage| Arc::new(storage) as ArcDynStorage)
            .map_err(Into::into)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GoogleCloudStorageEngineFactory;

#[async_trait]
impl StorageFactory for GoogleCloudStorageEngineFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"gs").map_err(Into::into)
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        let bucket_name = configuration.storage.host_str().unwrap_or("nisshi");

        let minimum_size = configuration.storage.query_pairs().find_map(|(k, v)| {
            if k == "batch_min_size" {
                human_units::Size::from_str(v.as_ref())
                    .map(|size| size.0)
                    .inspect_err(|err| {
                        warn!(storage = %redact_url(&configuration.storage), v = v.as_ref(), ?err)
                    })
                    .ok()
                    .and_then(|size| usize::try_from(size).ok())
            } else {
                None
            }
        });

        let maximum_delay = configuration.storage.query_pairs().find_map(|(k, v)| {
            if k == "batch_max_delay" {
                human_units::Duration::from_str(v.as_ref())
                    .map(|duration| duration.0)
                    .inspect_err(|err| {
                        warn!(storage = %redact_url(&configuration.storage), v = v.as_ref(), ?err)
                    })
                    .ok()
            } else {
                None
            }
        });

        GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket_name)
            .build()
            .map(|object_store| {
                PutRateLimiter::new(object_store, Duration::from_mins(5))
                    .with_rate_per_second(NonZeroU32::new(1))
                    .with_jitter(Some(Duration::from_millis(50)))
            })
            .map(|object_store| {
                DynoStore::new(
                    configuration.cluster.as_str(),
                    configuration.node_id,
                    object_store,
                )
                .advertised_listener(configuration.advertised_listener.clone())
                .schemas(configuration.schema_registry)
                .lake(configuration.lake_house.clone())
            })
            .map(|storage| {
                ProduceRequestBatcher::new(storage)
                    .with_minimum_size(minimum_size)
                    .with_maximum_delay(maximum_delay)
            })
            .map(Box::new)
            .map(|storage| Arc::new(storage) as ArcDynStorage)
            .map_err(Into::into)
    }
}
