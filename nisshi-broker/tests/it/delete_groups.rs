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

use std::{assert_matches, slice, time::Duration};

use crate::common::{self, Formed, alphanumeric_string, form_group};
use nisshi_broker::{
    Error,
    coordinator::group::{Coordinator as _, administrator::Controller},
};
use nisshi_sans_io::{
    DeleteGroupsRequest, DeleteGroupsResponse, ErrorCode, RequestInput,
    create_topics_request::CreatableTopic,
};
use nisshi_storage::{
    DeleteGroupsService, GroupDetail, GroupDetailResponse, OffsetCommitRequest, Storage, Topition,
};
use rama::{Service, extensions::Extensions};
use rand::{RngExt as _, rng};
use tokio::time::sleep;

/// A consumer joined to a group blocks its deletion (`NON_EMPTY_GROUP`), and
/// the group's committed offsets survive the refused attempt; once the
/// consumer leaves, the now-empty group deletes cleanly and its offsets are
/// gone. This exercises the coordinator-level route
/// (`Coordinator::delete_groups`), not the raw storage primitive the other
/// tests in this file use.
async fn non_empty_group_then_empty(storage: impl Storage + Clone) -> Result<(), Error> {
    let mut coordinator = Controller::with_storage(storage.clone())?;

    let Formed {
        group, member_id, ..
    } = form_group(&coordinator)
        .await
        .map_err(|err| Error::Message(err.to_string()))?;

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
            &group,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;
    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let response: DeleteGroupsResponse =
        coordinator
            .delete_groups(slice::from_ref(&group))
            .await
            .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(group, results[0].group_id);
    assert_eq!(
        ErrorCode::NonEmptyGroup,
        ErrorCode::try_from(results[0].error_code)?
    );

    // refused delete must not have touched storage: the offset is still
    // there.
    let offset_fetch = storage
        .offset_fetch(Some(&group), slice::from_ref(&topition), None)
        .await?;
    assert_eq!(Some(&offset), offset_fetch.get(&topition));

    _ = common::leave(&mut coordinator, &group, &member_id, None).await?;

    let response: DeleteGroupsResponse =
        coordinator
            .delete_groups(slice::from_ref(&group))
            .await
            .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(group, results[0].group_id);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);

    let offset_fetch = storage
        .offset_fetch(Some(&group), slice::from_ref(&topition), None)
        .await?;
    assert_ne!(Some(&offset), offset_fetch.get(&topition));

    Ok(())
}

/// Deleting a group must forget whatever the coordinator cached for it: a
/// brand-new member joining under the same, just-deleted group name must
/// succeed cleanly rather than tripping over a stale cached version that
/// still points at the now-gone storage row. (`common::join`'s own
/// assertions are the proof here: they panic on anything but a clean
/// `ErrorCode::None` solo-leader join.)
async fn delete_evicts_coordinator_cache(storage: impl Storage + Clone) -> Result<(), Error> {
    let mut coordinator = Controller::with_storage(storage.clone())?;

    let Formed {
        group, member_id, ..
    } = form_group(&coordinator)
        .await
        .map_err(|err| Error::Message(err.to_string()))?;

    _ = common::leave(&mut coordinator, &group, &member_id, None).await?;

    let response: DeleteGroupsResponse =
        coordinator
            .delete_groups(slice::from_ref(&group))
            .await
            .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);

    _ = common::join(
        &mut coordinator,
        &group,
        None,
        None,
        None,
        45_000,
        Some(300_000),
    )
    .await?;

    Ok(())
}

/// Member expiry in this codebase is lazy: a member that misses its
/// heartbeat is only evicted when some other request for the same group
/// happens to arrive. `DeleteGroups` must apply that same eviction itself
/// (via `missed_heartbeat`) before deciding a group is non-empty, or a
/// consumer that died without `LeaveGroup` would make its group
/// permanently undeletable.
async fn delete_after_session_expiry(storage: impl Storage + Clone) -> Result<(), Error> {
    let mut coordinator = Controller::with_storage(storage.clone())?;

    let group_id = alphanumeric_string(15);
    let session_timeout_ms = 500;

    let joined = common::join(
        &mut coordinator,
        &group_id,
        None,
        None,
        None,
        session_timeout_ms,
        Some(300_000),
    )
    .await
    .map_err(|err| Error::Message(err.to_string()))?;
    assert!(joined.is_leader());

    sleep(Duration::from_millis(
        u64::try_from(session_timeout_ms).unwrap_or(500) + 200,
    ))
    .await;

    let response: DeleteGroupsResponse = coordinator
        .delete_groups(slice::from_ref(&group_id))
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(group_id, results[0].group_id);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);

    Ok(())
}

/// A group that only ever committed offsets (no `JoinGroup` ever happened)
/// is a cache miss on every coordinator: `delete_groups` has to fall back to
/// `Storage::describe_groups`, which used to error on this group's NULL
/// `detail` column on the SQL backends (a group row with no detail row)
/// instead of treating it as empty.
async fn delete_offsets_only_group(storage: impl Storage + Clone) -> Result<(), Error> {
    let coordinator = Controller::with_storage(storage.clone())?;

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
    let group_id = alphanumeric_string(15);
    let offset = rng().random_range(0..i64::MAX);

    let commit = storage
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;
    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let response: DeleteGroupsResponse = coordinator
        .delete_groups(slice::from_ref(&group_id))
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(group_id, results[0].group_id);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);

    Ok(())
}

async fn delete_non_existent(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = DeleteGroupsService {
        storage: storage.clone(),
    };

    let group_id = alphanumeric_string(15);

    let response = service
        .serve(RequestInput {
            request: DeleteGroupsRequest::default().groups_names(Some([group_id.clone()].into())),
            extensions: Extensions::default(),
        })
        .await?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(group_id, results[0].group_id);

    assert_matches!(
        ErrorCode::try_from(results[0].error_code)?,
        ErrorCode::None | ErrorCode::GroupIdNotFound
    );

    Ok(())
}

/// A DeleteGroups request naming an empty group id alongside a real one must
/// never touch groups that were not named. On dynostore an empty id used to
/// widen the delete's key prefix to every group, wiping all group state and
/// committed offsets.
///
/// Kafka still accepts `""` in DeleteGroups for backwards compatibility, so
/// the SQL and SlateDB backends, which store `""` exactly, treat it like any
/// other id (`GROUP_ID_NOT_FOUND` here, since nothing was stored under it).
/// Dynostore can't represent it and answers `INVALID_GROUP_ID`.
async fn empty_group_id_mixed_list(
    storage: impl Storage + Clone,
    expected_for_empty: ErrorCode,
) -> Result<(), Error> {
    let topic_name = alphanumeric_string(15);
    let num_partitions = 6;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic_name, 0);

    let group_a = alphanumeric_string(15);
    let group_b = alphanumeric_string(15);

    let offset_a = rng().random_range(0..i64::MAX);
    let offset_b = rng().random_range(0..i64::MAX);

    for (group_id, offset) in [(&group_a, offset_a), (&group_b, offset_b)] {
        let commit = storage
            .offset_commit(
                group_id,
                None,
                &[(
                    topition.clone(),
                    OffsetCommitRequest::default().offset(offset),
                )],
            )
            .await?;

        assert_eq!(1, commit.len());
        assert_eq!(ErrorCode::None, commit[0].1);
    }

    // group_a must still be describable after the delete, which (on the SQL
    // backends) requires a persisted GroupDetail row, not just committed
    // offsets - establish one the same way a real join/sync would.
    _ = storage
        .update_group(&group_a, GroupDetail::default(), None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let service = DeleteGroupsService {
        storage: storage.clone(),
    };

    // group_a is never named: only "" and group_b are in the request.
    let response = service
        .serve(RequestInput {
            request: DeleteGroupsRequest::default()
                .groups_names(Some(["".into(), group_b.clone()].into())),
            extensions: Extensions::default(),
        })
        .await?;

    let results = response.results.unwrap_or_default();
    assert_eq!(2, results.len());

    let empty_result = results
        .iter()
        .find(|result| result.group_id.is_empty())
        .expect("missing result for the empty group id");
    assert_eq!(
        expected_for_empty,
        ErrorCode::try_from(empty_result.error_code)?
    );

    let group_b_result = results
        .iter()
        .find(|result| result.group_id == group_b)
        .expect("missing result for group_b");
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(group_b_result.error_code)?
    );

    // group_b was named, so its committed offset must be gone. On dynostore
    // a NONE result alone doesn't prove this: deleting a missing key is Ok.
    let offset_fetch = storage
        .offset_fetch(Some(&group_b), slice::from_ref(&topition), None)
        .await?;
    assert_ne!(Some(&offset_b), offset_fetch.get(&topition));

    // group_a was never named in the request, so its committed offset must
    // still be fetchable, and it must still be listable/describable.
    let offset_fetch = storage
        .offset_fetch(Some(&group_a), slice::from_ref(&topition), None)
        .await?;
    assert_eq!(Some(&offset_a), offset_fetch.get(&topition));

    let groups = storage.list_groups(None).await?;
    assert!(groups.iter().any(|group| group.group_id == group_a));

    let described = storage
        .describe_groups(Some(slice::from_ref(&group_a)), false)
        .await?;
    assert_eq!(1, described.len());
    assert_eq!(group_a, described[0].name);
    assert!(matches!(
        described[0].response,
        GroupDetailResponse::Found(_)
    ));

    Ok(())
}

/// `Storage::delete_groups` has callers other than `DeleteGroupsService`, and
/// the service passes every id through. Dynostore must itself refuse any id with an
/// empty path segment ("", "/", "//", "a/"), since `Path::from` drops empty
/// segments and the prefix delete would then cover every group.
#[cfg(feature = "dynostore")]
async fn direct_storage_call_rejects_unrepresentable_group_ids(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
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
    let group_a = alphanumeric_string(15);
    let offset_a = rng().random_range(0..i64::MAX);

    let commit = storage
        .offset_commit(
            &group_a,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset_a),
            )],
        )
        .await?;
    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let results = storage
        .delete_groups(Some(&["".into(), "/".into(), "//".into(), "a/".into()]))
        .await?;

    assert_eq!(4, results.len());
    for result in &results {
        assert_eq!(
            ErrorCode::InvalidGroupId,
            ErrorCode::try_from(result.error_code)?
        );
    }

    let offset_fetch = storage
        .offset_fetch(Some(&group_a), slice::from_ref(&topition), None)
        .await?;
    assert_eq!(Some(&offset_a), offset_fetch.get(&topition));

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use nisshi_broker::Result;
    use nisshi_sans_io::ErrorCode;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    use crate::common::{init_tracing, memory_storage};

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        memory_storage(cluster, node).await
    }

    #[tokio::test]
    async fn delete_non_existent() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_non_existent(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn empty_group_id_mixed_list() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::empty_group_id_mixed_list(storage, ErrorCode::InvalidGroupId).await?;

        Ok(())
    }

    #[tokio::test]
    async fn direct_storage_call_rejects_unrepresentable_group_ids() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::direct_storage_call_rejects_unrepresentable_group_ids(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_empty_group_then_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_empty_group_then_empty(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_evicts_coordinator_cache() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_evicts_coordinator_cache(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_after_session_expiry() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_after_session_expiry(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_offsets_only_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_offsets_only_group(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use crate::common::{init_tracing, lite_storage};
    use nisshi_broker::Result;
    use nisshi_sans_io::ErrorCode;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        lite_storage(cluster, node).await
    }

    #[tokio::test]
    async fn delete_non_existent() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_non_existent(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn empty_group_id_mixed_list() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::empty_group_id_mixed_list(storage, ErrorCode::GroupIdNotFound).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_empty_group_then_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_empty_group_then_empty(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_evicts_coordinator_cache() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_evicts_coordinator_cache(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_after_session_expiry() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_after_session_expiry(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_offsets_only_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_offsets_only_group(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use crate::common::{init_tracing, slate_storage};
    use nisshi_broker::Result;
    use nisshi_sans_io::ErrorCode;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        slate_storage(cluster, node).await
    }

    #[tokio::test]
    async fn delete_non_existent() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_non_existent(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn empty_group_id_mixed_list() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::empty_group_id_mixed_list(storage, ErrorCode::GroupIdNotFound).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_empty_group_then_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_empty_group_then_empty(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_evicts_coordinator_cache() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_evicts_coordinator_cache(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_after_session_expiry() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_after_session_expiry(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_offsets_only_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_offsets_only_group(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use crate::common::{init_tracing, postgres_storage};
    use nisshi_broker::Result;
    use nisshi_sans_io::ErrorCode;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        postgres_storage(cluster, node).await
    }

    #[tokio::test]
    async fn delete_non_existent() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_non_existent(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn empty_group_id_mixed_list() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::empty_group_id_mixed_list(storage, ErrorCode::GroupIdNotFound).await?;

        Ok(())
    }

    #[tokio::test]
    async fn non_empty_group_then_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::non_empty_group_then_empty(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_evicts_coordinator_cache() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_evicts_coordinator_cache(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_after_session_expiry() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_after_session_expiry(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_offsets_only_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_offsets_only_group(storage).await?;

        Ok(())
    }
}
