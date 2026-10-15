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

//! How the group coordinator fences an `OffsetCommit` when brokers share storage.
//!
//! Kafka rejects an offset commit from a member that is no longer in the group, and stores none of
//! its offsets. A Nisshi broker keeps a cached copy of each group that it coordinates, which can be
//! older than another broker's change, so the commit must be fenced against the group in storage.

use std::{slice, time::Duration};

use bytes::Bytes;
use nisshi_broker::{
    Result,
    coordinator::group::{
        Coordinator as _, OffsetCommit,
        administrator::{Controller, GroupCoordinatorConfig},
    },
};
use nisshi_sans_io::{
    Body, ErrorCode, OffsetCommitResponse,
    create_topics_request::CreatableTopic,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    sync_group_request::SyncGroupRequestAssignment,
};
use nisshi_storage::{Storage, Topition};
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

use crate::common::{
    self, PROTOCOL_TYPE, RANGE, StorageType, alphanumeric_string, init_tracing, join, leave,
    register_broker, sync_group,
};

const SESSION_TIMEOUT_MS: i32 = 45_000;

const REBALANCE_TIMEOUT_MS: i32 = 300_000;

/// A short initial delay, so that the group forms without waiting Kafka's default 3 s.
const GROUP_COORDINATOR: GroupCoordinatorConfig = GroupCoordinatorConfig {
    initial_rebalance_delay: Duration::from_millis(100),
    poll_interval: Duration::from_millis(500),
};

/// A member forms a group on broker A, which caches the group. The member then leaves through
/// broker B, and commits an offset through A with the generation that it had. A's cache still
/// holds the member, but storage doesn't, so the commit is rejected and stores nothing.
async fn commit_after_leaving_through_another_broker_is_fenced<G>(
    cluster_id: Uuid,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone + 'static,
{
    register_broker(cluster_id, broker_id, &storage).await?;

    let topic = alphanumeric_string(15);
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let mut broker_a = Controller::new(storage.clone(), GROUP_COORDINATOR);
    let mut broker_b = Controller::new(storage.clone(), GROUP_COORDINATOR);
    let group = alphanumeric_string(15);

    let member = join(
        &mut broker_a,
        &group,
        None,
        None,
        None,
        SESSION_TIMEOUT_MS,
        Some(REBALANCE_TIMEOUT_MS),
    )
    .await?;

    let synced = sync_group(
        &mut broker_a,
        &group,
        member.generation(),
        member.id(),
        None,
        PROTOCOL_TYPE,
        RANGE,
        &[SyncGroupRequestAssignment::default()
            .member_id(member.id().to_owned())
            .assignment(Bytes::from_static(b"partition-0"))],
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(synced.error_code)?);

    let left = leave(&mut broker_b, &group, member.id(), None).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(left.error_code)?);

    let topics = [OffsetCommitRequestTopic::default()
        .name(topic.clone())
        .partitions(Some(vec![
            OffsetCommitRequestPartition::default()
                .partition_index(0)
                .committed_offset(12_321),
        ]))];

    let committed = broker_a
        .offset_commit(OffsetCommit {
            group_id: &group,
            generation_id_or_member_epoch: Some(member.generation()),
            member_id: Some(member.id()),
            group_instance_id: None,
            retention_time_ms: None,
            topics: Some(&topics),
        })
        .await?;

    let Body::OffsetCommitResponse(OffsetCommitResponse {
        topics: Some(committed_topics),
        ..
    }) = committed
    else {
        panic!("expected an offset commit response: {committed:?}");
    };

    let partition_error_codes = committed_topics
        .iter()
        .flat_map(|topic| topic.partitions.iter().flatten())
        .map(|partition| ErrorCode::try_from(partition.error_code))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(vec![ErrorCode::UnknownMemberId], partition_error_codes);

    let topition = Topition::new(topic, 0);
    let stored = storage
        .offset_fetch(Some(&group), slice::from_ref(&topition), None)
        .await?;
    assert!(
        stored.get(&topition).is_none_or(|offset| *offset == -1),
        "a fenced commit stored offset {:?}",
        stored.get(&topition)
    );

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use super::*;

            #[tokio::test]
            async fn commit_after_leaving_through_another_broker_is_fenced() -> Result<()> {
                let _guard = init_tracing()?;
                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                super::commit_after_leaving_through_another_broker_is_fenced(
                    cluster_id,
                    broker_id,
                    common::storage_container(
                        $storage_type,
                        cluster_id,
                        broker_id,
                        Url::parse("tcp://127.0.0.1/")?,
                        None,
                    )
                    .await?,
                )
                .await
            }
        }
    };
}

backend!(in_memory, "dynostore", StorageType::InMemory);
backend!(lite, "libsql", StorageType::Lite);
backend!(pg, "postgres", StorageType::Postgres);
backend!(slatedb, "slatedb", StorageType::SlateDb);
