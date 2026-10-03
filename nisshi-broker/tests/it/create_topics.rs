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

use crate::common::{alphanumeric_string, init_tracing};
use nisshi_broker::Error;
use nisshi_sans_io::{
    CreateTopicsRequest, DescribeTopicPartitionsRequest, ErrorCode, NULL_TOPIC_ID, RequestInput,
    create_topics_request::CreatableTopic, describe_topic_partitions_request::TopicRequest,
};
use nisshi_storage::{CreateTopicsService, DescribeTopicPartitionsService, Storage};
use rama::{Service as _, extensions::Extensions};

async fn create(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsService { storage };

    let name = alphanumeric_string(15);
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    Ok(())
}

async fn create_with_default(node_id: i32, storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsService {
        storage: storage.clone(),
    };

    let name = alphanumeric_string(15);
    let num_partitions = -1;
    let replication_factor = -1;
    let assignments = Some([].into());
    let configs = Some([].into());

    let extensions = Extensions::default();

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(3), topics[0].num_partitions);
    assert_eq!(Some(1), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let service = DescribeTopicPartitionsService {
        storage: storage.clone(),
    };

    let response = service
        .serve(RequestInput {
            request: DescribeTopicPartitionsRequest::default()
                .topics(Some([TopicRequest::default().name(name.clone())].into())),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(Some(name), topics[0].name);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();

    assert_eq!(3, partitions.len());

    for (index, partition) in partitions.iter().enumerate() {
        assert_eq!(index as i32, partition.partition_index);
        assert_eq!(node_id, partition.leader_id);
        assert_eq!(0, partition.leader_epoch);
        assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
    }

    let offline_replicas = partitions[0]
        .offline_replicas
        .as_deref()
        .unwrap_or_default();
    assert!(offline_replicas.is_empty());

    let last_known_elr = partitions[0].last_known_elr.as_deref().unwrap_or_default();
    assert!(last_known_elr.is_empty());

    let eligible_leader_replicas = partitions[0]
        .eligible_leader_replicas
        .as_deref()
        .unwrap_or_default();
    assert!(eligible_leader_replicas.is_empty());

    let isr_nodes = partitions[0].isr_nodes.as_deref().unwrap_or_default();
    assert_eq!(1, isr_nodes.len());
    assert!(isr_nodes.iter().all(|isr_node| *isr_node == node_id));

    Ok(())
}

async fn duplicate(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsService {
        storage: storage.clone(),
    };

    let name = alphanumeric_string(15);
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let extensions = Extensions::default();

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(
        ErrorCode::TopicAlreadyExists,
        ErrorCode::try_from(topics[0].error_code)?
    );
    Ok(())
}

/// Topics with an invalid name must be rejected with `InvalidTopicException`,
/// and must never actually reach storage. `DescribeTopicPartitions` never
/// auto-creates (only `Metadata` does), so if the rejected name shows up as
/// `UnknownTopicOrPartition` there, `CreateTopics` genuinely never called
/// `Storage::create_topic` for it.
async fn invalid_name_rejected(storage: impl Storage + Clone) -> Result<(), Error> {
    let create = CreateTopicsService {
        storage: storage.clone(),
    };
    let describe = DescribeTopicPartitionsService {
        storage: storage.clone(),
    };

    let num_partitions = 3;
    let replication_factor = 1;
    let assignments = Some([].into());
    let configs = Some([].into());

    let too_long: String = "a".repeat(250);
    let invalid_names: [&str; 5] = ["", "a/b", ".", "..", too_long.as_str()];

    for name in invalid_names {
        let response = create
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments.clone())
                            .configs(configs.clone()),
                    ]))
                    .validate_only(Some(false)),
                extensions: Extensions::default(),
            })
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(name, topics[0].name.as_str());
        assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);
        assert_eq!(
            ErrorCode::InvalidTopicException,
            ErrorCode::try_from(topics[0].error_code)?,
            "name = {name:?}"
        );

        let describe_response = describe
            .serve(RequestInput {
                request: DescribeTopicPartitionsRequest::default()
                    .topics(Some([TopicRequest::default().name(name.into())].into())),
                extensions: Extensions::default(),
            })
            .await?;

        let describe_topics = describe_response.topics.unwrap_or_default();
        assert_eq!(1, describe_topics.len());
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::try_from(describe_topics[0].error_code)?,
            "name = {name:?} must never have reached storage"
        );
    }

    // boundary: 249 characters is the longest valid name, and must succeed.
    let boundary_name: String = "a".repeat(249);

    let response = create
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(boundary_name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(boundary_name, topics[0].name.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    Ok(())
}

/// `num_partitions` of `0` or less than `-1` must be rejected with
/// `InvalidPartitions`; `-1` (the "use the default" sentinel) must still
/// succeed.
async fn invalid_partitions_rejected(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsService {
        storage: storage.clone(),
    };

    let replication_factor = 1;
    let assignments = Some([].into());
    let configs = Some([].into());

    for num_partitions in [0, -2] {
        let name = alphanumeric_string(15);

        let response = service
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.clone())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments.clone())
                            .configs(configs.clone()),
                    ]))
                    .validate_only(Some(false)),
                extensions: Extensions::default(),
            })
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(name, topics[0].name.as_str());
        assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);
        assert_eq!(
            ErrorCode::InvalidPartitions,
            ErrorCode::try_from(topics[0].error_code)?,
            "num_partitions = {num_partitions}"
        );
    }

    // -1 still means "use the broker default".
    let name = alphanumeric_string(15);

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(-1)
                        .replication_factor(-1)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    assert_eq!(Some(3), topics[0].num_partitions);

    Ok(())
}

/// `replication_factor` of `0` or less than `-1` must be rejected with
/// `InvalidReplicationFactor`; `-1` (the "use the default" sentinel) must
/// still succeed.
async fn invalid_replication_factor_rejected(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsService {
        storage: storage.clone(),
    };

    let num_partitions = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    for replication_factor in [0, -2] {
        let name = alphanumeric_string(15);

        let response = service
            .serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.clone())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments.clone())
                            .configs(configs.clone()),
                    ]))
                    .validate_only(Some(false)),
                extensions: Extensions::default(),
            })
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(name, topics[0].name.as_str());
        assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);
        assert_eq!(
            ErrorCode::InvalidReplicationFactor,
            ErrorCode::try_from(topics[0].error_code)?,
            "replication_factor = {replication_factor}"
        );
    }

    // -1 still means "use the broker default".
    let name = alphanumeric_string(15);

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.clone())
                        .num_partitions(-1)
                        .replication_factor(-1)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    assert_eq!(Some(1), topics[0].replication_factor);

    Ok(())
}

/// One invalid topic in a batch must not affect the others: the invalid
/// entry is rejected, the valid one is still created.
async fn mixed_batch_partial_success(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = CreateTopicsService {
        storage: storage.clone(),
    };

    let valid_name = alphanumeric_string(15);
    let num_partitions = 3;
    let replication_factor = 1;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name("".into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                    CreatableTopic::default()
                        .name(valid_name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(2, topics.len());

    assert_eq!("", topics[0].name.as_str());
    assert_eq!(
        ErrorCode::InvalidTopicException,
        ErrorCode::try_from(topics[0].error_code)?
    );
    assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);

    assert_eq!(valid_name, topics[1].name.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[1].error_code)?);
    assert_ne!(Some(NULL_TOPIC_ID), topics[1].topic_id);

    Ok(())
}

/// One invalid topic in a batch must not affect the others: the topic with
/// an invalid `replication_factor` is rejected, the valid one is still
/// created. The rejected topic's name must itself be valid, so that
/// `InvalidReplicationFactor` is what rejects it rather than the name check
/// (which runs first) firing on a bad name instead.
async fn mixed_batch_partial_success_replication_factor(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
    let service = CreateTopicsService {
        storage: storage.clone(),
    };

    let invalid_name = alphanumeric_string(15);
    let valid_name = alphanumeric_string(15);
    let num_partitions = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = service
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(invalid_name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(0)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                    CreatableTopic::default()
                        .name(valid_name.clone())
                        .num_partitions(num_partitions)
                        .replication_factor(1)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(2, topics.len());

    assert_eq!(invalid_name, topics[0].name.as_str());
    assert_eq!(
        ErrorCode::InvalidReplicationFactor,
        ErrorCode::try_from(topics[0].error_code)?
    );
    assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);

    assert_eq!(valid_name, topics[1].name.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[1].error_code)?);
    assert_ne!(Some(NULL_TOPIC_ID), topics[1].topic_id);

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use nisshi_broker::Result;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    use crate::common::memory_storage;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        memory_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_with_default() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_with_default(broker_id, storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn duplicate() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::duplicate(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_name_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_name_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_partitions_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_partitions_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_replication_factor_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_replication_factor_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success_replication_factor() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success_replication_factor(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use crate::common::lite_storage;
    use nisshi_broker::Result;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        lite_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_with_default() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_with_default(broker_id, storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn duplicate() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::duplicate(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_name_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_name_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_partitions_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_partitions_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_replication_factor_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_replication_factor_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success_replication_factor() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success_replication_factor(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use crate::common::slate_storage;
    use nisshi_broker::Result;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        slate_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_with_default() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_with_default(broker_id, storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn duplicate() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::duplicate(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_name_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_name_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_partitions_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_partitions_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_replication_factor_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_replication_factor_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success_replication_factor() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success_replication_factor(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use crate::common::postgres_storage;
    use nisshi_broker::Result;
    use nisshi_storage::ArcDynStorage;
    use rand::{RngExt as _, rng};
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        postgres_storage(cluster, node).await
    }

    #[tokio::test]
    async fn create() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_with_default() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_with_default(broker_id, storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn duplicate() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::duplicate(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_name_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_name_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_partitions_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_partitions_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn invalid_replication_factor_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::invalid_replication_factor_rejected(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn mixed_batch_partial_success_replication_factor() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::mixed_batch_partial_success_replication_factor(storage).await?;

        Ok(())
    }
}
