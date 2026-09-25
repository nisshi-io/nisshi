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
use nisshi_broker::Result;
use nisshi_sans_io::{
    ConfigResource, CreateTopicsRequest, DescribeConfigsRequest, ErrorCode,
    IncrementalAlterConfigsRequest, OpType, RequestInput,
    create_topics_request::CreatableTopic,
    describe_configs_request::DescribeConfigsResource,
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
};
use nisshi_storage::{
    ArcDynStorage, CreateTopicsService, DescribeConfigsService, IncrementalAlterConfigsService,
    Storage,
};
use rama::{Service, extensions::Extensions};
use rand::{RngExt as _, rng};
use tracing::debug;
use uuid::Uuid;

async fn simple(storage: impl Storage + Clone) -> Result<()> {
    let resource_name = &alphanumeric_string(15)[..];

    let create_topic = CreateTopicsService {
        storage: storage.clone(),
    };

    let extensions = Extensions::default();

    let response = create_topic
        .serve(RequestInput {
            request: CreateTopicsRequest::default().topics(Some(
                [CreatableTopic::default()
                    .name(resource_name.into())
                    .num_partitions(3)
                    .replication_factor(1)]
                .into(),
            )),
            extensions: extensions.clone(),
        })
        .await
        .inspect(|create_topic_response| debug!(?create_topic_response))?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(response.topics.unwrap_or_default()[0].error_code)?
    );

    let config_name = "x.y.z";
    let config_value = "pqr";

    let describe_configs = DescribeConfigsService {
        storage: storage.clone(),
    };

    let response = describe_configs
        .serve(RequestInput {
            request: DescribeConfigsRequest::default()
                .include_documentation(Some(false))
                .include_synonyms(Some(false))
                .resources(Some(
                    [DescribeConfigsResource::default()
                        .resource_name(resource_name.into())
                        .resource_type(ConfigResource::Topic.into())
                        .configuration_keys(Some([config_name.into()].into()))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await
        .inspect(|describe_configs_response| debug!(?describe_configs_response))?;

    assert!(
        response
            .results
            .unwrap_or_default()
            .first()
            .is_some_and(|first| first.configs.as_deref().unwrap_or_default().is_empty())
    );

    let alter_configs = IncrementalAlterConfigsService {
        storage: storage.clone(),
    };

    let _response = alter_configs
        .serve(RequestInput {
            request: IncrementalAlterConfigsRequest::default().resources(Some(
                [AlterConfigsResource::default()
                    .resource_name(resource_name.into())
                    .resource_type(ConfigResource::Topic.into())
                    .configs(Some(
                        [AlterableConfig::default()
                            .config_operation(OpType::Set.into())
                            .name(config_name.into())
                            .value(Some(config_value.into()))]
                        .into(),
                    ))]
                .into(),
            )),
            extensions: extensions.clone(),
        })
        .await?;

    let response = describe_configs
        .serve(RequestInput {
            request: DescribeConfigsRequest::default()
                .include_documentation(Some(false))
                .include_synonyms(Some(false))
                .resources(Some(
                    [DescribeConfigsResource::default()
                        .resource_name(resource_name.into())
                        .resource_type(ConfigResource::Topic.into())
                        .configuration_keys(Some([config_name.into()].into()))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let results = response.results.as_deref().unwrap_or(&[]);
    assert_eq!(1, results.len());
    assert_eq!(resource_name, results[0].resource_name.as_str());

    let configs = results[0].configs.as_deref().unwrap_or(&[]);
    assert_eq!(1, configs.len());
    assert_eq!(Some(config_value), configs[0].value.as_deref());

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
}
