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
use nisshi_broker::{Error, Result};
use nisshi_sans_io::{
    ErrorCode, ListGroupsRequest, RequestInput, create_topics_request::CreatableTopic,
};
use nisshi_storage::{
    ArcDynStorage, GroupDetail, ListGroupsService, OffsetCommitRequest, Storage, Topition,
};
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

/// A group that the coordinator has stored, but that has not committed an offset, must be listed
/// exactly once, because Kafka lists every group the coordinator knows. A group has no committed
/// offset until its first commit, and an operator must be able to find it before then.
async fn group_without_committed_offsets_is_listed_once(
    storage: impl Storage + Clone,
) -> Result<()> {
    let group_id = alphanumeric_string(15);

    _ = storage
        .update_group(&group_id, GroupDetail::default(), None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let groups = storage.list_groups(None).await?;

    assert_eq!(
        1,
        groups
            .iter()
            .filter(|group| group.group_id == group_id)
            .count(),
        "expected {group_id:?} exactly once in {groups:?}"
    );

    Ok(())
}

/// A group that the coordinator has stored, and that has committed an offset, must be listed
/// once, not once for its state and once for its offsets.
async fn group_with_committed_offsets_is_listed_once(storage: impl Storage + Clone) -> Result<()> {
    let topic_name = alphanumeric_string(15);
    let group_id = alphanumeric_string(15);

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

    _ = storage
        .update_group(&group_id, GroupDetail::default(), None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let commit = storage
        .offset_commit(
            &group_id,
            None,
            &[(
                Topition::new(topic_name, 0),
                OffsetCommitRequest::default().offset(rng().random_range(0..i64::MAX)),
            )],
        )
        .await?;
    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let groups = storage.list_groups(None).await?;

    assert_eq!(
        1,
        groups
            .iter()
            .filter(|group| group.group_id == group_id)
            .count(),
        "expected {group_id:?} exactly once in {groups:?}"
    );

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

/// A group with the empty id and only a state object must be listed once, as `""`. On dynostore
/// that state object is `%empty.json`, because the empty id is stored as the sentinel `%empty`,
/// and `list_groups` must decode the sentinel instead of listing a group named `%empty`.
#[cfg(feature = "dynostore")]
async fn empty_group_id_without_committed_offsets_is_listed(
    storage: impl Storage + Clone,
) -> Result<()> {
    _ = storage
        .update_group("", GroupDetail::default(), None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let groups = storage.list_groups(None).await?;

    assert_eq!(
        1,
        groups
            .iter()
            .filter(|group| group.group_id.is_empty())
            .count(),
        "expected a group with id \"\" exactly once in {groups:?}"
    );

    for phantom in ["%empty", "%empty.json"] {
        assert!(
            !groups.iter().any(|group| group.group_id == phantom),
            "list_groups surfaced a phantom group named {phantom:?}: {groups:?}"
        );
    }

    Ok(())
}

/// On dynostore, a group's state object is its encoded id followed by `.json`, so `list_groups`
/// must remove the `.json` before it decodes the id. A group named `foo.json` must stay distinct
/// from the state object of a group named `foo`, and an id with reserved characters must be
/// listed as it was stored.
#[cfg(feature = "dynostore")]
async fn state_only_group_ids_are_decoded(storage: impl Storage + Clone) -> Result<()> {
    let base = alphanumeric_string(15);
    let group_ids = [base.clone(), format!("{base}.json"), format!("{base}/a%b")];

    for group_id in &group_ids {
        _ = storage
            .update_group(group_id, GroupDetail::default(), None)
            .await
            .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;
    }

    let groups = storage.list_groups(None).await?;

    for group_id in &group_ids {
        assert_eq!(
            1,
            groups
                .iter()
                .filter(|group| &group.group_id == group_id)
                .count(),
            "expected {group_id:?} exactly once in {groups:?}"
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
    async fn group_without_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_without_committed_offsets_is_listed_once(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_with_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_with_committed_offsets_is_listed_once(storage).await?;

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

    #[tokio::test]
    async fn empty_group_id_without_committed_offsets_is_listed() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::empty_group_id_without_committed_offsets_is_listed(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn state_only_group_ids_are_decoded() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::state_only_group_ids_are_decoded(storage).await?;

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

    #[tokio::test]
    async fn group_without_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_without_committed_offsets_is_listed_once(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_with_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_with_committed_offsets_is_listed_once(storage).await?;

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

    #[tokio::test]
    async fn group_without_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_without_committed_offsets_is_listed_once(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_with_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_with_committed_offsets_is_listed_once(storage).await?;

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

    #[tokio::test]
    async fn group_without_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_without_committed_offsets_is_listed_once(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_with_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_with_committed_offsets_is_listed_once(storage).await?;

        Ok(())
    }
}
