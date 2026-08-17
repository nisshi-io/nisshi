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
    ErrorCode, MetadataRequest, NULL_TOPIC_ID, metadata_request::MetadataRequestTopic,
};
use nisshi_storage::{MetadataService, Storage, StorageContainer};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use std::sync::Arc;
use url::Url;

mod common;

async fn storage() -> Result<Arc<Box<dyn Storage>>, Error> {
    StorageContainer::builder()
        .cluster_id("nisshi")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://nisshi/")?)
        .build()
        .await
        .map_err(Into::into)
}

#[tokio::test]
async fn req() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id("nisshi")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(Url::parse("memory://nisshi/")?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(MetadataService);

    let response = service
        .serve(
            Context::default(),
            MetadataRequest::default()
                .allow_auto_topic_creation(Some(false))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .topics(Some([].into())),
        )
        .await?;

    let brokers = response.brokers.as_deref().unwrap_or_default();
    assert_eq!(1, brokers.len());
    assert_eq!(HOST, brokers[0].host);
    assert_eq!(PORT, brokers[0].port);
    assert_eq!(NODE_ID, brokers[0].node_id);
    assert!(brokers[0].rack.is_none());

    Ok(())
}

#[tokio::test]
async fn auto_create_topic() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let service = MapStateLayer::new(|_| storage).into_layer(MetadataService);

    let name = "auto-created";

    let response = service
        .serve(
            Context::default(),
            MetadataRequest::default()
                .allow_auto_topic_creation(Some(true))
                .topics(Some(vec![
                    MetadataRequestTopic::default().name(Some(name.into())),
                ])),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    assert_eq!(Some(name.into()), topics[0].name);
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(4, topics[0].partitions.as_deref().unwrap_or_default().len());

    Ok(())
}

#[tokio::test]
async fn auto_create_topic_invalid_name() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let service = MapStateLayer::new(|_| storage).into_layer(MetadataService);

    let name = "not a valid topic name";

    let response = service
        .serve(
            Context::default(),
            MetadataRequest::default()
                .allow_auto_topic_creation(Some(true))
                .topics(Some(vec![
                    MetadataRequestTopic::default().name(Some(name.into())),
                ])),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(
        ErrorCode::InvalidTopicException,
        ErrorCode::try_from(topics[0].error_code)?
    );

    Ok(())
}

#[tokio::test]
async fn auto_create_topic_not_allowed() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let service = MapStateLayer::new(|_| storage).into_layer(MetadataService);

    let name = "not-auto-created";

    let response = service
        .serve(
            Context::default(),
            MetadataRequest::default()
                .allow_auto_topic_creation(Some(false))
                .topics(Some(vec![
                    MetadataRequestTopic::default().name(Some(name.into())),
                ])),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(topics[0].error_code)?
    );

    Ok(())
}
