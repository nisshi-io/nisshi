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
    reject_unrecognized_options,
};
use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut},
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
        reject_unrecognized_options(&configuration.storage, &[])?;

        Ok(Arc::new(Box::new(
            DynoStore::new(
                configuration.cluster.as_str(),
                configuration.node_id,
                InMemory::new(),
            )?
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
        reject_unrecognized_options(
            &configuration.storage,
            &["batch_min_size", "batch_max_delay"],
        )?;

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

        let builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .with_conditional_put(S3ConditionalPut::ETagMatch);

        // With `AWS_SKIP_SIGNATURE` on, object_store sends unsigned requests
        // and never asks the credential provider for a credential. The provider
        // still exists and, with no keys set, falls back to the instance
        // metadata service, so this check would fail a setup that works.
        let skip_signature = builder
            .get_config_value(&AmazonS3ConfigKey::SkipSignature)
            .is_some_and(|value| skip_signature(&value));

        let object_store = builder.build().map_err(nisshi_storage::Error::from)?;

        // We get a credential from the configured provider now, so that a
        // provider failure is reported as `NoCredentials` and not as a failed
        // request. The provider caches the credential, so the `ping()` startup
        // check that follows does not fetch it again.
        if !skip_signature {
            let _ = object_store
                .credentials()
                .get_credential()
                .await
                .map_err(|source| nisshi_storage::Error::NoCredentials(Arc::new(source)))?;
        }

        let storage = DynoStore::new(
            configuration.cluster.as_str(),
            configuration.node_id,
            object_store,
        )?
        .advertised_listener(configuration.advertised_listener.clone())
        .schemas(configuration.schema_registry)
        .lake(configuration.lake_house.clone());

        let storage = ProduceRequestBatcher::new(storage)
            .with_minimum_size(minimum_size)
            .with_maximum_delay(maximum_delay);

        Ok(Arc::new(Box::new(storage)) as ArcDynStorage)
    }
}

/// Returns whether `object_store` reads `value`, the raw `AWS_SKIP_SIGNATURE`
/// setting, as true.
///
/// [`AmazonS3Builder::get_config_value`] returns the unparsed string, and
/// `object_store` parses it only in `build()`, with a crate-private parser
/// that this function must keep matching: `1`, `true`, `on`, `yes` and `y`
/// in any case are true. Every other value is either false or rejected by
/// `build()` before the credential check runs.
fn skip_signature(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "on" | "yes" | "y"
    )
}

#[derive(Clone, Copy, Debug)]
pub struct GoogleCloudStorageEngineFactory;

#[async_trait]
impl StorageFactory for GoogleCloudStorageEngineFactory {
    fn scheme(&self) -> Result<Regex> {
        Regex::new(r"gs").map_err(Into::into)
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        reject_unrecognized_options(
            &configuration.storage,
            &["batch_min_size", "batch_max_delay"],
        )?;

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
            .map_err(Into::into)
            .and_then(|object_store| {
                PutRateLimiter::new(object_store, Duration::from_mins(5)).map(|object_store| {
                    object_store
                        .with_rate_per_second(NonZeroU32::new(1))
                        .with_jitter(Some(Duration::from_millis(50)))
                })
            })
            .and_then(|object_store| {
                DynoStore::new(
                    configuration.cluster.as_str(),
                    configuration.node_id,
                    object_store,
                )
                .map(|storage| {
                    storage
                        .advertised_listener(configuration.advertised_listener.clone())
                        .schemas(configuration.schema_registry)
                        .lake(configuration.lake_house.clone())
                })
            })
            .map(|storage| {
                ProduceRequestBatcher::new(storage)
                    .with_minimum_size(minimum_size)
                    .with_maximum_delay(maximum_delay)
            })
            .map(Box::new)
            .map(|storage| Arc::new(storage) as ArcDynStorage)
    }
}

#[cfg(test)]
mod tests;
