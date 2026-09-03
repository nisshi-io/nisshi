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
use assert_matches::assert_matches;
use nisshi_broker::Error;
use nisshi_broker::Result;
use nisshi_sans_io::{
    CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest, DeleteTopicsResponse,
    ErrorCode, NULL_TOPIC_ID, RequestInput, create_topics_request::CreatableTopic,
    delete_topics_request::DeleteTopicState, delete_topics_response::DeletableTopicResult,
};
use nisshi_storage::{ArcDynStorage, CreateTopicsService, DeleteTopicsService, Storage};
use rama::{Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use uuid::Uuid;

mod common;

async fn delete_unknown_by_name(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = DeleteTopicsService {
        storage: storage.clone(),
    };

    let topic = alphanumeric_string(15);

    let error_code = ErrorCode::UnknownTopicOrPartition;

    assert_eq!(
        DeleteTopicsResponse::default()
            .throttle_time_ms(Some(0))
            .responses(Some(vec![
                DeletableTopicResult::default()
                    .error_code(error_code.into())
                    .error_message(Some(error_code.to_string()))
                    .name(Some(topic.clone())),
            ])),
        service
            .serve(RequestInput {
                request: DeleteTopicsRequest::default().topic_names(Some(vec![topic])),
                extensions: Extensions::default()
            })
            .await?
    );

    Ok(())
}

async fn delete_unknown_by_uuid(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = DeleteTopicsService {
        storage: storage.clone(),
    };

    let topic = Uuid::new_v4();

    let error_code = ErrorCode::UnknownTopicOrPartition;

    assert_eq!(
        DeleteTopicsResponse::default()
            .throttle_time_ms(Some(0))
            .responses(Some(vec![
                DeletableTopicResult::default()
                    .error_code(error_code.into())
                    .error_message(Some(error_code.to_string()))
                    .topic_id(Some(topic.into_bytes()))
            ])),
        service
            .serve(RequestInput {
                request: DeleteTopicsRequest::default().topics(Some(vec![
                    DeleteTopicState::default().topic_id(topic.into_bytes())
                ])),
                extensions: Extensions::default()
            },)
            .await?
    );

    Ok(())
}

async fn create_delete_create_by_name(storage: impl Storage + Clone) -> Result<(), Error> {
    let create_topics = CreateTopicsService {
        storage: storage.clone(),
    };

    let name = alphanumeric_string(15);
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let error_code = ErrorCode::None;

    let extensions = Extensions::default();

    assert_matches!(
        create_topics
            .serve(
                RequestInput{
                request: CreateTopicsRequest::default()
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(name.clone())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments.clone())
                            .configs(configs.clone()),]
                        .into()
                    ))
                    .validate_only(Some(false)), extensions: extensions.clone()},
            )
            .await?,
        CreateTopicsResponse { topics: Some(topics), ..} => {
            assert_eq!(topics.len(), 1);
            assert_eq!(name, topics[0].name.as_str());
            assert_matches!(topics[0].configs.as_ref(), Some(configs) if configs.is_empty());
            assert_eq!(topics[0].topic_config_error_code, Some(0));
            assert_eq!(topics[0].num_partitions, Some(num_partitions));
            assert_eq!(topics[0].replication_factor, Some(replication_factor));
            assert_eq!(topics[0].error_code, i16::from(error_code));
        }
    );

    let delete_topics = DeleteTopicsService {
        storage: storage.clone(),
    };

    let error_code = ErrorCode::None;

    assert_eq!(
        DeleteTopicsResponse::default()
            .throttle_time_ms(Some(0))
            .responses(Some(vec![
                DeletableTopicResult::default()
                    .error_code(error_code.into())
                    .error_message(Some(error_code.to_string()))
                    .name(Some(name.clone()))
                    .topic_id(Some(NULL_TOPIC_ID)),
            ])),
        delete_topics
            .serve(RequestInput {
                request: DeleteTopicsRequest::default().topics(Some(vec![
                    DeleteTopicState::default()
                        .name(Some(name.clone()))
                        .topic_id(NULL_TOPIC_ID),
                ])),
                extensions: extensions.clone()
            })
            .await?
    );

    assert_matches!(
        create_topics
            .serve(
                RequestInput {
                request: CreateTopicsRequest::default()
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(name.clone())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments.clone())
                            .configs(configs.clone()),]
                        .into()
                    ))
                    .validate_only(Some(false)),
                extensions: extensions.clone()
                }
            ).await?,
        CreateTopicsResponse { topics: Some(topics), ..} => {
            assert_eq!(topics.len(), 1);
            assert_eq!(name, topics[0].name.as_str());
            assert_matches!(topics[0].configs.as_ref(), Some(configs) if configs.is_empty());
            assert_eq!(topics[0].topic_config_error_code, Some(0));
            assert_eq!(topics[0].num_partitions, Some(num_partitions));
            assert_eq!(topics[0].replication_factor, Some(replication_factor));
            assert_eq!(topics[0].error_code, i16::from(error_code));
        }
    );

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
    async fn delete_unknown_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_unknown_by_uuid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_uuid(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_delete_create_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_delete_create_by_name(storage).await?;

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
    async fn delete_unknown_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_unknown_by_uuid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_uuid(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_delete_create_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_delete_create_by_name(storage).await?;

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
    async fn delete_unknown_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_unknown_by_uuid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_uuid(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_delete_create_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_delete_create_by_name(storage).await?;

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
    async fn delete_unknown_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_name(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn delete_unknown_by_uuid() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::delete_unknown_by_uuid(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_delete_create_by_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::create_delete_create_by_name(storage).await?;

        Ok(())
    }
}
