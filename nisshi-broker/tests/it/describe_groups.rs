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

use std::{
    assert_matches, slice,
    time::{Duration, SystemTime},
};

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use bytes::Bytes;
use nisshi_broker::{
    Error, Result,
    coordinator::group::{Coordinator, administrator::Controller},
};
use nisshi_sans_io::{
    DescribeGroupsRequest, DescribeGroupsResponse, ErrorCode, RequestInput,
    create_topics_request::CreatableTopic, describe_groups_response::DescribedGroup,
    join_group_response::JoinGroupResponseMember,
};
use nisshi_storage::{
    ArcDynStorage, DescribeGroupsService, GroupDetail, GroupMember, GroupState,
    OffsetCommitRequest, Storage, Topition,
};
use rama::{Service, extensions::Extensions};
use rand::{RngExt as _, rng};
use uuid::Uuid;

/// The authorized operations that Kafka 3.9.1 answers for a group when the broker has no
/// authorizer: the group's supported operations READ (3), DELETE (6) and DESCRIBE (8), as the
/// bitfield `1 << 3 | 1 << 6 | 1 << 8` ([`AuthHelper.authorizedOperations`],
/// [`AclEntry.supportedOperations`]).
///
/// [`AuthHelper.authorizedOperations`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/AuthHelper.scala#L62-L76
/// [`AclEntry.supportedOperations`]: https://github.com/apache/kafka/blob/3.9.1/server/src/main/java/org/apache/kafka/security/authorizer/AclEntry.java#L150-L167
const SUPPORTED_GROUP_OPERATIONS: i32 = 328;

async fn create_group(storage: &impl Storage, group_id: &str) -> Result<()> {
    _ = storage
        .update_group(group_id, GroupDetail::default(), None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;
    Ok(())
}

/// Returns each group in `group_ids` as the group coordinator describes it.
async fn describe(
    storage: impl Storage + Clone,
    group_ids: &[String],
    include_authorized_operations: bool,
) -> Result<Vec<DescribedGroup>> {
    let response: DescribeGroupsResponse = Controller::with_storage(storage)?
        .describe_groups(Some(group_ids), include_authorized_operations)
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    Ok(response.groups.unwrap_or_default())
}

async fn operations_omitted_unless_requested(storage: impl Storage + Clone) -> Result<()> {
    let group_id = alphanumeric_string(15);
    create_group(&storage, &group_id).await?;

    let groups = describe(storage, &[group_id], false).await?;
    assert_eq!(1, groups.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
    assert_eq!(Some(i32::MIN), groups[0].authorized_operations);

    Ok(())
}

async fn found_group_answers_supported_operations_when_asked(
    storage: impl Storage + Clone,
) -> Result<()> {
    let group_id = alphanumeric_string(15);
    create_group(&storage, &group_id).await?;

    let groups = describe(storage, &[group_id], true).await?;
    assert_eq!(1, groups.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
    assert_eq!(
        Some(SUPPORTED_GROUP_OPERATIONS),
        groups[0].authorized_operations
    );

    Ok(())
}

/// An engine answers an unknown group with an error row or with an empty group. Kafka answers
/// an unknown group with error NONE and state `Dead`, and reports the operations on every row
/// whose error is NONE.
async fn unknown_group_answers_operations_only_on_a_row_without_error(
    storage: impl Storage + Clone,
) -> Result<()> {
    for include_authorized_operations in [false, true] {
        let groups = describe(
            storage.clone(),
            &[alphanumeric_string(15)],
            include_authorized_operations,
        )
        .await?;
        assert_eq!(1, groups.len());

        let expected = if include_authorized_operations
            && ErrorCode::try_from(groups[0].error_code)? == ErrorCode::None
        {
            SUPPORTED_GROUP_OPERATIONS
        } else {
            i32::MIN
        };
        assert_eq!(Some(expected), groups[0].authorized_operations);
    }

    Ok(())
}

/// Requires an engine that answers an unknown group with `GROUP_ID_NOT_FOUND`.
#[cfg(any(feature = "dynostore", feature = "libsql", feature = "postgres"))]
async fn error_row_omits_authorized_operations(storage: impl Storage + Clone) -> Result<()> {
    for include_authorized_operations in [false, true] {
        let groups = describe(
            storage.clone(),
            &[alphanumeric_string(15)],
            include_authorized_operations,
        )
        .await?;
        assert_eq!(1, groups.len());
        assert_eq!(
            ErrorCode::GroupIdNotFound,
            ErrorCode::try_from(groups[0].error_code)?
        );
        assert_eq!(Some(i32::MIN), groups[0].authorized_operations);
    }

    Ok(())
}

/// A group that only committed offsets, and never joined, exists: the broker must answer it as
/// an empty group, not with `GROUP_ID_NOT_FOUND`.
#[cfg(any(feature = "dynostore", feature = "libsql", feature = "postgres"))]
async fn group_with_only_committed_offsets_is_empty(storage: impl Storage + Clone) -> Result<()> {
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

    let group_id = alphanumeric_string(15);
    let topition = Topition::new(topic_name, 0);
    let committed = storage
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(rng().random_range(0..i64::MAX)),
            )],
        )
        .await?;
    assert_eq!(vec![(topition, ErrorCode::None)], committed);

    let groups = describe(storage, slice::from_ref(&group_id), false).await?;
    assert_eq!(1, groups.len());
    assert_eq!(group_id, groups[0].group_id);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
    assert_eq!("Empty", groups[0].group_state);

    Ok(())
}

/// A group whose consumers stopped without sending `LeaveGroup` must be described as `Empty` once
/// their sessions expire, as Kafka describes it, so that tools that need an empty group, such as
/// an offset reset, accept it. The broker must remove each member whose session has expired before
/// it describes the group, also when no member has sent a request since.
async fn group_whose_sessions_expired_is_described_as_empty(
    storage: impl Storage + Clone,
) -> Result<()> {
    let group_id = alphanumeric_string(15);
    let member_id = alphanumeric_string(15);

    let session_timeout =
        Duration::from_millis(u64::try_from(GroupDetail::default().session_timeout_ms)?);

    let detail = GroupDetail {
        members: [(
            member_id.clone(),
            GroupMember {
                join_response: JoinGroupResponseMember::default().member_id(member_id.clone()),
                last_contact: Some(SystemTime::now() - session_timeout - Duration::from_secs(1)),
            },
        )]
        .into(),
        generation_id: 1,
        state: GroupState::Formed {
            protocol_type: "consumer".into(),
            protocol_name: "range".into(),
            leader: member_id.clone(),
            assignments: [(member_id, Bytes::new())].into(),
        },
        ..GroupDetail::default()
    };

    _ = storage
        .update_group(&group_id, detail, None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let groups = describe(storage, &[group_id], false).await?;
    assert_eq!(1, groups.len());
    assert_eq!("Empty", groups[0].group_state);
    assert_eq!(Some(vec![]), groups[0].members);

    Ok(())
}

async fn simple(storage: impl Storage + Clone) -> Result<()> {
    let service = DescribeGroupsService {
        storage: storage.clone(),
    };

    let group_id = &alphanumeric_string(15)[..];

    let response = service
        .serve(RequestInput {
            request: DescribeGroupsRequest::default()
                .groups(Some([group_id.into()].into()))
                .include_authorized_operations(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let groups = response.groups.unwrap_or_default();
    assert_eq!(1, groups.len());

    assert_matches!(
        ErrorCode::try_from(groups[0].error_code)?,
        ErrorCode::None | ErrorCode::GroupIdNotFound
    );
    assert_eq!(group_id, groups[0].group_id.as_str());
    assert_matches!(groups[0].group_state.as_str(), "Empty" | "Unknown");

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

    async fn storage() -> Result<ArcDynStorage> {
        storage_container(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
    }

    #[tokio::test]
    async fn operations_omitted_unless_requested() -> Result<()> {
        let _guard = init_tracing()?;
        super::operations_omitted_unless_requested(storage().await?).await
    }

    #[tokio::test]
    async fn found_group_answers_supported_operations_when_asked() -> Result<()> {
        let _guard = init_tracing()?;
        super::found_group_answers_supported_operations_when_asked(storage().await?).await
    }

    #[tokio::test]
    async fn unknown_group_answers_operations_only_on_a_row_without_error() -> Result<()> {
        let _guard = init_tracing()?;
        super::unknown_group_answers_operations_only_on_a_row_without_error(storage().await?).await
    }

    #[tokio::test]
    async fn error_row_omits_authorized_operations() -> Result<()> {
        let _guard = init_tracing()?;
        super::error_row_omits_authorized_operations(storage().await?).await
    }

    #[tokio::test]
    async fn group_with_only_committed_offsets_is_empty() -> Result<()> {
        let _guard = init_tracing()?;
        super::group_with_only_committed_offsets_is_empty(storage().await?).await
    }

    #[tokio::test]
    async fn group_whose_sessions_expired_is_described_as_empty() -> Result<()> {
        let _guard = init_tracing()?;
        super::group_whose_sessions_expired_is_described_as_empty(storage().await?).await
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

    async fn storage() -> Result<ArcDynStorage> {
        storage_container(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
    }

    #[tokio::test]
    async fn operations_omitted_unless_requested() -> Result<()> {
        let _guard = init_tracing()?;
        super::operations_omitted_unless_requested(storage().await?).await
    }

    #[tokio::test]
    async fn found_group_answers_supported_operations_when_asked() -> Result<()> {
        let _guard = init_tracing()?;
        super::found_group_answers_supported_operations_when_asked(storage().await?).await
    }

    #[tokio::test]
    async fn unknown_group_answers_operations_only_on_a_row_without_error() -> Result<()> {
        let _guard = init_tracing()?;
        super::unknown_group_answers_operations_only_on_a_row_without_error(storage().await?).await
    }
    #[tokio::test]
    async fn error_row_omits_authorized_operations() -> Result<()> {
        let _guard = init_tracing()?;
        super::error_row_omits_authorized_operations(storage().await?).await
    }

    #[tokio::test]
    async fn group_with_only_committed_offsets_is_empty() -> Result<()> {
        let _guard = init_tracing()?;
        super::group_with_only_committed_offsets_is_empty(storage().await?).await
    }

    #[tokio::test]
    async fn group_whose_sessions_expired_is_described_as_empty() -> Result<()> {
        let _guard = init_tracing()?;
        super::group_whose_sessions_expired_is_described_as_empty(storage().await?).await
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

    async fn storage() -> Result<ArcDynStorage> {
        storage_container(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
    }

    #[tokio::test]
    async fn operations_omitted_unless_requested() -> Result<()> {
        let _guard = init_tracing()?;
        super::operations_omitted_unless_requested(storage().await?).await
    }

    #[tokio::test]
    async fn found_group_answers_supported_operations_when_asked() -> Result<()> {
        let _guard = init_tracing()?;
        super::found_group_answers_supported_operations_when_asked(storage().await?).await
    }

    #[tokio::test]
    async fn unknown_group_answers_operations_only_on_a_row_without_error() -> Result<()> {
        let _guard = init_tracing()?;
        super::unknown_group_answers_operations_only_on_a_row_without_error(storage().await?).await
    }
    #[tokio::test]
    async fn error_row_omits_authorized_operations() -> Result<()> {
        let _guard = init_tracing()?;
        super::error_row_omits_authorized_operations(storage().await?).await
    }

    #[tokio::test]
    async fn group_with_only_committed_offsets_is_empty() -> Result<()> {
        let _guard = init_tracing()?;
        super::group_with_only_committed_offsets_is_empty(storage().await?).await
    }

    #[tokio::test]
    async fn group_whose_sessions_expired_is_described_as_empty() -> Result<()> {
        let _guard = init_tracing()?;
        super::group_whose_sessions_expired_is_described_as_empty(storage().await?).await
    }
}
