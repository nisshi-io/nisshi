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

use std::{assert_matches, slice};

use crate::common::alphanumeric_string;
use nisshi_broker::Error;
use nisshi_sans_io::{
    DeleteGroupsRequest, ErrorCode, RequestInput, create_topics_request::CreatableTopic,
};
use nisshi_storage::{
    DeleteGroupsService, GroupDetail, GroupDetailResponse, OffsetCommitRequest, Storage, Topition,
};
use rama::{Service, extensions::Extensions};
use rand::{RngExt as _, rng};

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
/// Kafka still accepts `""` in DeleteGroups for backwards compatibility.
/// Every backend - SQL, SlateDB, and dynostore alike - treats it like any
/// other opaque group id that nothing was ever committed under: SQL and
/// SlateDB answer `GROUP_ID_NOT_FOUND`. Dynostore's object store layer
/// treats `delete` as idempotent (deleting an absent key still succeeds, the
/// same as a real S3 `DeleteObject`), so it answers `NONE` instead - the same
/// ambiguity `delete_non_existent` above already tolerates for any id, not
/// something specific to the empty id.
async fn empty_group_id_mixed_list(storage: impl Storage + Clone) -> Result<(), Error> {
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
    assert_matches!(
        ErrorCode::try_from(empty_result.error_code)?,
        ErrorCode::None | ErrorCode::GroupIdNotFound
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
/// the service passes every id through unchanged, so every group id -
/// including `""`, `"/"`, `"//"`, `"a/"`, and the literal string `"%empty"` -
/// must be its own fully opaque, independently addressable group.
///
/// Before the fix, dynostore built an object store key by interpolating the
/// raw group id into a path string and reparsing it: a `/` in the id split
/// into extra path segments, an empty id collapsed onto the `consumers/`
/// prefix shared by every group, and a literal `"%empty"` id risked colliding
/// with the sentinel segment chosen to represent the empty id. Any of those
/// could make one group's delete or fetch quietly observe or destroy another
/// group's committed offsets.
///
/// This commits a distinct offset under each id (plus a real baseline id,
/// `"a"`, for comparison), confirms all six are independently fetchable with
/// their own correct value, then deletes them one at a time - never as a
/// batch, which could mask a collision - confirming at each step that only
/// the just-deleted group's offset is gone and every other group's offset is
/// still intact.
async fn slash_and_empty_group_ids_are_distinct_groups(
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

    let group_ids = [
        "a".to_string(),
        String::new(),
        "/".to_string(),
        "//".to_string(),
        "a/".to_string(),
        "%empty".to_string(),
    ];

    let offsets = group_ids
        .iter()
        .map(|_| rng().random_range(0..i64::MAX))
        .collect::<Vec<_>>();

    for (group_id, offset) in group_ids.iter().zip(&offsets) {
        let commit = storage
            .offset_commit(
                group_id,
                None,
                &[(
                    topition.clone(),
                    OffsetCommitRequest::default().offset(*offset),
                )],
            )
            .await?;

        assert_eq!(1, commit.len());
        assert_eq!(ErrorCode::None, commit[0].1);
    }

    for (group_id, offset) in group_ids.iter().zip(&offsets) {
        let offset_fetch = storage
            .offset_fetch(Some(group_id), slice::from_ref(&topition), None)
            .await?;
        assert_eq!(
            Some(offset),
            offset_fetch.get(&topition),
            "group {group_id:?} fetched the wrong committed offset"
        );
    }

    for (deleted_index, group_id) in group_ids.iter().enumerate() {
        let results = storage
            .delete_groups(Some(slice::from_ref(group_id)))
            .await?;

        assert_eq!(1, results.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);

        for (other_index, (other_group_id, other_offset)) in
            group_ids.iter().zip(&offsets).enumerate()
        {
            let offset_fetch = storage
                .offset_fetch(Some(other_group_id), slice::from_ref(&topition), None)
                .await?;

            if other_index <= deleted_index {
                // The exact "not found" sentinel differs subtly between
                // backends; what matters here is that this group's committed
                // value is gone, not what replaces it.
                assert_ne!(
                    Some(other_offset),
                    offset_fetch.get(&topition),
                    "group {other_group_id:?} should have been deleted by now (deleting {group_id:?})"
                );
            } else {
                assert_eq!(
                    Some(other_offset),
                    offset_fetch.get(&topition),
                    "group {other_group_id:?} should still be intact after deleting {group_id:?}"
                );
            }
        }
    }

    Ok(())
}

/// Regression coverage for `committed_offset_topitions`'s positional
/// path-segment parsing (`.parts().nth(6)`, `.nth(8)`), independent of
/// `delete_groups`: that parsing assumes the group id is exactly one path
/// segment. A group id like `"a/b"` would otherwise split into two segments,
/// shifting every later segment and turning the partition-number parse into
/// a `ParseIntError`. Encoding the whole group id as one opaque segment keeps
/// that assumption true for every group id.
#[cfg(feature = "dynostore")]
async fn slash_in_group_id_does_not_break_committed_offset_topitions(
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
    let group_id = "a/b";
    let offset = rng().random_range(0..i64::MAX);

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

    let topitions = storage.committed_offset_topitions(group_id).await?;
    assert_eq!(Some(&offset), topitions.get(&topition));

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use nisshi_broker::Result;
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

        super::empty_group_id_mixed_list(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn slash_and_empty_group_ids_are_distinct_groups() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::slash_and_empty_group_ids_are_distinct_groups(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn slash_in_group_id_does_not_break_committed_offset_topitions() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::slash_in_group_id_does_not_break_committed_offset_topitions(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use crate::common::{init_tracing, lite_storage};
    use nisshi_broker::Result;
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

        super::empty_group_id_mixed_list(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn slash_and_empty_group_ids_are_distinct_groups() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::slash_and_empty_group_ids_are_distinct_groups(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use crate::common::{init_tracing, slate_storage};
    use nisshi_broker::Result;
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

        super::empty_group_id_mixed_list(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn slash_and_empty_group_ids_are_distinct_groups() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::slash_and_empty_group_ids_are_distinct_groups(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use crate::common::{init_tracing, postgres_storage};
    use nisshi_broker::Result;
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

        super::empty_group_id_mixed_list(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn slash_and_empty_group_ids_are_distinct_groups() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::slash_and_empty_group_ids_are_distinct_groups(storage).await?;

        Ok(())
    }
}
