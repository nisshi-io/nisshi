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

mod common;

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
}
