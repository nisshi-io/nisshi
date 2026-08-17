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
use assert_matches::assert_matches;
use nisshi_sans_io::{
    CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest, DeleteTopicsResponse,
    ErrorCode, NULL_TOPIC_ID, create_topics_request::CreatableTopic,
    delete_topics_request::DeleteTopicState, delete_topics_response::DeletableTopicResult,
};
use nisshi_storage::{CreateTopicsService, DeleteTopicsService, StorageContainer};
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use url::Url;
use uuid::Uuid;

mod common;

#[tokio::test]
async fn delete_unknown_by_name() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let node_id = 12321;

    let storage = StorageContainer::builder()
        .cluster_id("nisshi")
        .node_id(node_id)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://nisshi/")?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(DeleteTopicsService);

    let topic = "pqr";

    let error_code = ErrorCode::UnknownTopicOrPartition;

    assert_eq!(
        DeleteTopicsResponse::default()
            .throttle_time_ms(Some(0))
            .responses(Some(vec![
                DeletableTopicResult::default()
                    .error_code(error_code.into())
                    .error_message(Some(error_code.to_string()))
                    .name(Some(topic.into())),
            ])),
        service
            .serve(
                Context::default(),
                DeleteTopicsRequest::default().topic_names(Some(vec![topic.into()]))
            )
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn delete_unknown_by_uuid() -> Result<(), Error> {
    let node_id = 12321;

    let storage = StorageContainer::builder()
        .cluster_id("nisshi")
        .node_id(node_id)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://nisshi/")?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(DeleteTopicsService);

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
            .serve(
                Context::default(),
                DeleteTopicsRequest::default().topics(Some(vec![
                    DeleteTopicState::default().topic_id(topic.into_bytes())
                ]))
            )
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn create_delete_create_by_name() -> Result<(), Error> {
    let node_id = 12321;

    let storage = StorageContainer::builder()
        .cluster_id("nisshi")
        .node_id(node_id)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://nisshi/")?)
        .build()
        .await?;

    let create_topics = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
    };

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic = "pqr";

    let error_code = ErrorCode::None;

    assert_matches!(
        create_topics
            .serve(
                Context::default(),
                CreateTopicsRequest::default()
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments.clone())
                            .configs(configs.clone()),]
                        .into()
                    ))
                    .validate_only(Some(false))
            )
            .await?,
        CreateTopicsResponse { topics: Some(topics), ..} => {
            assert_eq!(topics.len(), 1);
            assert_eq!(topic, topics[0].name.as_str());
            assert_matches!(topics[0].configs.as_ref(), Some(configs) if configs.is_empty());
            assert_eq!(topics[0].topic_config_error_code, Some(0));
            assert_eq!(topics[0].num_partitions, Some(num_partitions));
            assert_eq!(topics[0].replication_factor, Some(replication_factor));
            assert_eq!(topics[0].error_code, i16::from(error_code));
        }
    );

    let delete_topics = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(DeleteTopicsService)
    };

    let error_code = ErrorCode::None;

    assert_eq!(
        DeleteTopicsResponse::default()
            .throttle_time_ms(Some(0))
            .responses(Some(vec![
                DeletableTopicResult::default()
                    .error_code(error_code.into())
                    .error_message(Some(error_code.to_string()))
                    .name(Some(name.into()))
                    .topic_id(Some(NULL_TOPIC_ID)),
            ])),
        delete_topics
            .serve(
                Context::default(),
                DeleteTopicsRequest::default().topics(Some(vec![
                    DeleteTopicState::default()
                        .name(Some(name.into()))
                        .topic_id(NULL_TOPIC_ID),
                ]))
            )
            .await?
    );

    assert_matches!(
        create_topics
            .serve(
                Context::default(),
                CreateTopicsRequest::default()
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(assignments.clone())
                            .configs(configs.clone()),]
                        .into()
                    ))
                    .validate_only(Some(false))
            )
            .await?,
        CreateTopicsResponse { topics: Some(topics), ..} => {
            assert_eq!(topics.len(), 1);
            assert_eq!(topic, topics[0].name.as_str());
            assert_matches!(topics[0].configs.as_ref(), Some(configs) if configs.is_empty());
            assert_eq!(topics[0].topic_config_error_code, Some(0));
            assert_eq!(topics[0].num_partitions, Some(num_partitions));
            assert_eq!(topics[0].replication_factor, Some(replication_factor));
            assert_eq!(topics[0].error_code, i16::from(error_code));
        }
    );

    Ok(())
}
