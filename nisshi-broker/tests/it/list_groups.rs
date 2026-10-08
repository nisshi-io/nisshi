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

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, ListGroupsRequest, RequestInput, create_topics_request::CreatableTopic,
};
use nisshi_storage::{ArcDynStorage, ListGroupsService, OffsetCommitRequest, Storage, Topition};
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

async fn simple(storage: impl Storage + Clone) -> Result<()> {
    let service = ListGroupsService { storage };

    let response = service
        .serve(RequestInput {
            request: ListGroupsRequest::default().states_filter(Some(["Empty".into()].into())),
            extensions: Extensions::default(),
        })
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    assert_eq!(Some([].into()), response.groups);

    Ok(())
}

/// Regression coverage for the specific symptom `EMPTY_GROUP_SENTINEL`
/// exists to prevent, surfaced at the `list_groups` layer rather than at the
/// sentinel's own encode/decode unit tests.
///
/// On dynostore, `list_groups` lists via `list_with_delimiter`, a different
/// `object_store` operation to the plain prefix `list` every other group
/// operation uses. Without the sentinel, the empty group id would produce a
/// key containing a doubled slash (`consumers//offsets/...`), and
/// `list_with_delimiter`'s common-prefix computation normalizes that doubled
/// slash away - surfacing a phantom group under the wrong name instead of
/// the real empty-id group.
///
/// This commits an offset under group id `""`, then asserts `list_groups`
/// reports a group with id exactly `""` and does not report any phantom
/// group under a name a doubled-slash key could plausibly decode to.
#[cfg(feature = "dynostore")]
async fn empty_group_id_is_not_surfaced_as_phantom_group(
    storage: impl Storage + Clone,
) -> Result<()> {
    let topic_name = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic_name, 0);
    let offset = rng().random_range(0..i64::MAX);

    let commit = storage
        .offset_commit(
            "",
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;
    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let groups = storage.list_groups(None).await?;

    assert!(
        groups.iter().any(|group| group.group_id.is_empty()),
        "expected a group with id \"\" in {groups:?}"
    );

    for phantom in ["consumers", "offsets", "%empty"] {
        assert!(
            !groups.iter().any(|group| group.group_id == phantom),
            "list_groups surfaced a phantom group named {phantom:?}: {groups:?}"
        );
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
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn empty_group_id_is_not_surfaced_as_phantom_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::empty_group_id_is_not_surfaced_as_phantom_group(storage).await?;

        Ok(())
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
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
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
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
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
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
    }
}
