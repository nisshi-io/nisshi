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

//! `update_group` is a conditional write: `version: None` means "create
//! only", so it must not overwrite a group that already exists.
//!
//! The group coordinator relies on this. It caches each group's state in
//! memory, and a request that arrives while another request for the same
//! group is in flight finds no cached entry, so it starts from an empty
//! group with no version. The storage must reject that write as outdated
//! and hand back the stored group. SlateDB used to skip the check when no
//! version was given and overwrote the group, dropping every member.

mod common;

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use nisshi_broker::{Error, Result};
use nisshi_storage::{ArcDynStorage, GroupDetail, Storage, UpdateError};
use rand::{prelude::*, rng};
use uuid::Uuid;

async fn create_only_without_version(storage: impl Storage + Clone) -> Result<(), Error> {
    let group_id = &alphanumeric_string(15)[..];

    let created = GroupDetail {
        generation_id: 1,
        ..Default::default()
    };

    let version = storage
        .update_group(group_id, created, None)
        .await
        .map_err(|err| Error::Message(format!("create: {err:?}")))?;

    let overwrite = GroupDetail {
        generation_id: 2,
        ..Default::default()
    };

    match storage.update_group(group_id, overwrite, None).await {
        Err(UpdateError::Outdated {
            current,
            version: current_version,
        }) => {
            assert_eq!(1, current.generation_id);
            assert_eq!(version, current_version);
        }

        otherwise => panic!("expected outdated, got: {otherwise:?}"),
    }

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        memory_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create_only_without_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_only_without_version(storage).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        lite_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create_only_without_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_only_without_version(storage).await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        slate_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create_only_without_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_only_without_version(storage).await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        postgres_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create_only_without_version() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_only_without_version(storage).await
    }
}
