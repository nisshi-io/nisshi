// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use crate::common::{Error, init_tracing};
use nisshi_sans_io::{
    CreateTopicsRequest, DescribeTopicPartitionsRequest, ErrorCode, NULL_TOPIC_ID,
    create_topics_request::CreatableTopic, describe_topic_partitions_request::TopicRequest,
};
use nisshi_storage::{CreateTopicsService, DescribeTopicPartitionsService, StorageContainer};
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use url::Url;

mod common;

#[tokio::test]
async fn create() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let node_id = 12321;

    let storage = StorageContainer::builder()
        .cluster_id("nisshi")
        .node_id(node_id)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://nisshi/")?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(CreateTopicsService);

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = service
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
        )
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

#[tokio::test]
async fn create_with_default() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let node_id = 12321;

    let storage = StorageContainer::builder()
        .cluster_id("nisshi")
        .node_id(node_id)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://nisshi/")?)
        .build()
        .await?;

    let service = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
    };

    let name = "pqr";
    let num_partitions = -1;
    let replication_factor = -1;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = service
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(3), topics[0].num_partitions);
    assert_eq!(Some(1), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let service = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(DescribeTopicPartitionsService)
    };

    let response = service
        .serve(
            Context::default(),
            DescribeTopicPartitionsRequest::default()
                .topics(Some([TopicRequest::default().name(name.into())].into())),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(Some(name), topics[0].name.as_deref());
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

#[tokio::test]
async fn duplicate() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let node_id = 12321;

    let storage = StorageContainer::builder()
        .cluster_id("nisshi")
        .node_id(node_id)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://nisshi/")?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(CreateTopicsService);

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = service
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let response = service
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
        )
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
