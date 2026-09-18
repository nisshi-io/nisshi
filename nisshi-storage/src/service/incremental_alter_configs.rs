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

use nisshi_sans_io::{ApiKey, IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`IncrementalAlterConfigsRequest`] returning [`IncrementalAlterConfigsResponse`].
/// ```
/// use rama::{Context, Layer as _, Service, layer::MapStateLayer};
/// use nisshi_sans_io::{
///     ConfigResource, CreateTopicsRequest, DescribeConfigsRequest, ErrorCode,
///     IncrementalAlterConfigsRequest, OpType,
///     create_topics_request::CreatableTopic,
///     describe_configs_request::DescribeConfigsResource,
///     incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
/// };
/// use nisshi_storage::{
///     CreateTopicsService, DescribeConfigsService, Error,
///     IncrementalAlterConfigsService, StorageContainer,
/// };
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
/// const NODE_ID: i32 = 111;
///
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let resource_name = "abcba";
///
/// let create_topic = {
///     let storage = storage.clone();
///     MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
/// };
///
/// let response = create_topic
///     .serve(
///         Context::default(),
///         CreateTopicsRequest::default().topics(Some(
///             [CreatableTopic::default()
///                 .name(resource_name.into())
///                 .num_partitions(3)
///                 .replication_factor(1)]
///             .into(),
///         )),
///     )
///     .await?;
///
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(response.topics.unwrap_or_default()[0].error_code)?
/// );
///
/// let config_name = "x.y.z";
/// let config_value = "pqr";
///
/// let describe_configs = {
///     let storage = storage.clone();
///     MapStateLayer::new(|_| storage).into_layer(DescribeConfigsService)
/// };
///
/// let response = describe_configs
///     .serve(
///         Context::default(),
///         DescribeConfigsRequest::default()
///             .include_documentation(Some(false))
///             .include_synonyms(Some(false))
///             .resources(Some(
///                 [DescribeConfigsResource::default()
///                     .resource_name(resource_name.into())
///                     .resource_type(ConfigResource::Topic.into())
///                     .configuration_keys(Some([config_name.into()].into()))]
///                 .into(),
///             )),
///     )
///     .await?;
///
/// assert!(response.results.unwrap_or_default()[0].configs.is_none());
///
/// let alter_configs = {
///     let storage = storage.clone();
///     MapStateLayer::new(|_| storage).into_layer(IncrementalAlterConfigsService)
/// };
///
/// let _response = alter_configs
///     .serve(
///         Context::default(),
///         IncrementalAlterConfigsRequest::default().resources(Some(
///             [AlterConfigsResource::default()
///                 .resource_name(resource_name.into())
///                 .resource_type(ConfigResource::Topic.into())
///                 .configs(Some(
///                     [AlterableConfig::default()
///                         .config_operation(OpType::Set.into())
///                         .name(config_name.into())
///                         .value(Some(config_value.into()))]
///                     .into(),
///                 ))]
///             .into(),
///         )),
///     )
///     .await?;
///
/// let response = describe_configs
///     .serve(
///         Context::default(),
///         DescribeConfigsRequest::default()
///             .include_documentation(Some(false))
///             .include_synonyms(Some(false))
///             .resources(Some(
///                 [DescribeConfigsResource::default()
///                     .resource_name(resource_name.into())
///                     .resource_type(ConfigResource::Topic.into())
///                     .configuration_keys(Some([config_name.into()].into()))]
///                 .into(),
///             )),
///     )
///     .await?;
///
/// let results = response.results.as_deref().unwrap_or(&[]);
/// assert_eq!(1, results.len());
/// assert_eq!(resource_name, results[0].resource_name.as_str());
///
/// let configs = results[0].configs.as_deref().unwrap_or(&[]);
/// assert_eq!(1, configs.len());
/// assert_eq!(Some(config_value), configs[0].value.as_deref());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IncrementalAlterConfigsService;

impl ApiKey for IncrementalAlterConfigsService {
    const KEY: i16 = IncrementalAlterConfigsRequest::KEY;
}

impl<G> Service<G, IncrementalAlterConfigsRequest> for IncrementalAlterConfigsService
where
    G: Storage,
{
    type Response = IncrementalAlterConfigsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: IncrementalAlterConfigsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let mut responses = vec![];

        for resource in req.resources.unwrap_or_default() {
            responses.push(ctx.state().incremental_alter_resource(resource).await?);
        }

        Ok(IncrementalAlterConfigsResponse::default()
            .throttle_time_ms(0)
            .responses(Some(responses)))
    }
}
