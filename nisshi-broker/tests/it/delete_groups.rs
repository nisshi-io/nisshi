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

use std::{assert_matches, collections::BTreeMap, slice, time::Duration};

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

/// Deleting a group must forget whatever the coordinator cached for it, or
/// a brand-new member joining under the same, just-deleted group name would
/// silently reuse the deleted group's stale state instead of starting a
/// genuinely new one. On Postgres and libSQL this isn't hypothetical: the
/// conditional group-detail write is an upsert whose `where e_tag = ...`
/// guard only applies to the UPDATE arm, so once the row is gone a stale
/// cached version doesn't fail the write, it just inserts - the rejoin
/// succeeds either way, with or without this fix, so a bare "the join call
/// didn't error" assertion proves nothing on those two backends. The
/// reliable signal is whether the rejoin actually started a fresh
/// generation or continued the deleted group's: compare it against a
/// genuinely new control group joined the same way.
async fn delete_evicts_coordinator_cache(
    storage: impl Storage + Clone,
    after_delete_not_found: Option<ErrorCode>,
) -> Result<(), Error> {
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

    // On dynostore and SlateDB a missing group fails open to
    // `Found(GroupDetail::default())` (pre-existing, accepted elsewhere in
    // this file): only Postgres and libSQL actually report `GroupIdNotFound`
    // here, so this check only applies to them.
    if let Some(expected) = after_delete_not_found {
        let described = storage
            .describe_groups(Some(slice::from_ref(&group)), false)
            .await?;
        assert_eq!(1, described.len());
        assert!(
            matches!(described[0].response, GroupDetailResponse::ErrorCode(code) if code == expected),
            "expected {expected:?}, got {:?}",
            described[0].response
        );
    }

    // A short timeout: the coordinator's own join-rebalance wait sleeps
    // until half the session timeout has elapsed before closing a solo
    // dynamic join (see `administrator.rs`'s `join` loop), and this test
    // doesn't need to exercise that wait.
    let session_timeout_ms = 1_000;

    let rejoined = common::join(
        &mut coordinator,
        &group,
        None,
        None,
        None,
        session_timeout_ms,
        Some(300_000),
    )
    .await?;

    let control_group = alphanumeric_string(15);
    let control = common::join(
        &mut coordinator,
        &control_group,
        None,
        None,
        None,
        session_timeout_ms,
        Some(300_000),
    )
    .await?;

    assert_eq!(
        control.generation(),
        rejoined.generation(),
        "rejoin under a just-deleted group name must start the same first \
         generation as a brand-new group, not continue the deleted group's"
    );

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
/// Kafka still accepts `""` in DeleteGroups for backwards compatibility, and
/// every backend treats it like any other id that nothing was stored under.
/// SQL and SlateDB answer `GROUP_ID_NOT_FOUND`. Dynostore answers `NONE`,
/// because deleting an absent object store key succeeds, as an S3
/// `DeleteObject` does.
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

/// The generation of `group_id`'s stored group state, or `None` when
/// `describe_groups` finds no state for it.
async fn described_generation(
    storage: &(impl Storage + Clone),
    group_id: &str,
) -> Result<Option<i32>, Error> {
    let described = storage
        .describe_groups(Some(&[group_id.to_owned()]), false)
        .await?;
    assert_eq!(1, described.len());
    assert_eq!(group_id, described[0].name);

    Ok(match &described[0].response {
        GroupDetailResponse::Found(detail) => Some(detail.generation_id),
        GroupDetailResponse::ErrorCode(_) => None,
    })
}

/// `Storage::delete_groups` has callers other than `DeleteGroupsService`, and
/// the service passes every id through unchanged, so every group id,
/// including `""`, `"/"`, `"//"`, `"a/"` and the literal string `"%empty"`,
/// must be its own independently addressable group.
///
/// This gives each id (plus a baseline id, `"a"`) a distinct committed offset
/// and a distinct group state (its own generation), confirms each reads back
/// its own, then deletes them one at a time, never as a batch, which could
/// mask a collision. After each delete, only the deleted group's offset and
/// state are gone, and every other group still reads back its own.
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

    for (generation_id, group_id) in (1..).zip(&group_ids) {
        _ = storage
            .update_group(
                group_id,
                GroupDetail {
                    generation_id,
                    ..GroupDetail::default()
                },
                None,
            )
            .await
            .map_err(|err| Error::Message(format!("update_group {group_id:?}: {err:?}")))?;
    }

    for (generation_id, (group_id, offset)) in (1..).zip(group_ids.iter().zip(&offsets)) {
        let offset_fetch = storage
            .offset_fetch(Some(group_id), slice::from_ref(&topition), None)
            .await?;
        assert_eq!(
            Some(offset),
            offset_fetch.get(&topition),
            "group {group_id:?} fetched the wrong committed offset"
        );

        assert_eq!(
            Some(generation_id),
            described_generation(&storage, group_id).await?,
            "group {group_id:?} described the wrong group state"
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
            let other_generation = i32::try_from(other_index + 1).expect("few groups");
            let described = described_generation(&storage, other_group_id).await?;

            if other_index <= deleted_index {
                // Backends answer a deleted group differently; what matters
                // is that its committed offset and state are gone.
                assert_ne!(
                    Some(other_offset),
                    offset_fetch.get(&topition),
                    "group {other_group_id:?} offset should have been deleted by now (deleting {group_id:?})"
                );
                assert_ne!(
                    Some(other_generation),
                    described,
                    "group {other_group_id:?} state should have been deleted by now (deleting {group_id:?})"
                );
            } else {
                assert_eq!(
                    Some(other_offset),
                    offset_fetch.get(&topition),
                    "group {other_group_id:?} offset should still be intact after deleting {group_id:?}"
                );
                assert_eq!(
                    Some(other_generation),
                    described,
                    "group {other_group_id:?} state should still be intact after deleting {group_id:?}"
                );
            }
        }
    }

    Ok(())
}

/// `committed_offset_topitions` parses the topic and partition from each key
/// below the group's offsets prefix. A group id containing `/` must not shift
/// those segments, which would turn the partition parse into a
/// `ParseIntError`.
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

/// `delete_topic` removes a topic's committed offsets by skipping one
/// segment, the group id, in each key below `consumers/` and matching the
/// rest against `offsets/{topic}/partitions/`. That holds only while a group
/// id is one segment: interpolated raw, "a/b" took two segments and ""
/// none, so those groups kept their offsets for a deleted topic.
#[cfg(feature = "dynostore")]
async fn delete_topic_removes_offsets_of_slash_and_empty_group_ids(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
    let deleted_name = alphanumeric_string(15);
    let kept_name = alphanumeric_string(15);

    for name in [deleted_name.clone(), kept_name.clone()] {
        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(name)
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;
    }

    let deleted = Topition::new(deleted_name.clone(), 0);
    let kept = Topition::new(kept_name, 0);

    let group_ids = ["a/b", "", "a"];
    let offsets = group_ids.map(|_| rng().random_range(0..i64::MAX));

    for (group_id, offset) in group_ids.iter().zip(&offsets) {
        let commit = storage
            .offset_commit(
                group_id,
                None,
                &[
                    (
                        deleted.clone(),
                        OffsetCommitRequest::default().offset(*offset),
                    ),
                    (kept.clone(), OffsetCommitRequest::default().offset(*offset)),
                ],
            )
            .await?;

        assert_eq!(2, commit.len());
        assert!(
            commit.iter().all(|(_, code)| *code == ErrorCode::None),
            "{commit:?}"
        );
    }

    assert_eq!(
        ErrorCode::None,
        storage.delete_topic(&deleted_name.into()).await?
    );

    for (group_id, offset) in group_ids.iter().zip(&offsets) {
        let fetched = storage
            .offset_fetch(Some(group_id), &[deleted.clone(), kept.clone()], None)
            .await?;
        assert_ne!(
            Some(offset),
            fetched.get(&deleted),
            "group {group_id:?} kept its offset for the deleted topic"
        );
        assert_eq!(
            Some(offset),
            fetched.get(&kept),
            "group {group_id:?} lost its offset for the kept topic"
        );

        let committed = storage.committed_offset_topitions(group_id).await?;
        assert_eq!(
            [(kept.clone(), *offset)]
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
            committed,
            "group {group_id:?}"
        );
    }

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

        super::empty_group_id_mixed_list(storage, ErrorCode::None).await?;

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

    #[tokio::test]
    async fn delete_topic_removes_offsets_of_slash_and_empty_group_ids() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_topic_removes_offsets_of_slash_and_empty_group_ids(storage).await?;

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

        super::delete_evicts_coordinator_cache(storage, None).await?;

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
    async fn slash_and_empty_group_ids_are_distinct_groups() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::slash_and_empty_group_ids_are_distinct_groups(storage).await?;

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

        super::delete_evicts_coordinator_cache(storage, Some(ErrorCode::GroupIdNotFound)).await?;

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
    async fn slash_and_empty_group_ids_are_distinct_groups() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::slash_and_empty_group_ids_are_distinct_groups(storage).await?;

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

        super::delete_evicts_coordinator_cache(storage, None).await?;

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
    async fn slash_and_empty_group_ids_are_distinct_groups() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::slash_and_empty_group_ids_are_distinct_groups(storage).await?;

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

        super::delete_evicts_coordinator_cache(storage, Some(ErrorCode::GroupIdNotFound)).await?;

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
