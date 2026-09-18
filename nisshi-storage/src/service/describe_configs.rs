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

use nisshi_sans_io::{ApiKey, ConfigResource, DescribeConfigsRequest, DescribeConfigsResponse};
use rama::{Context, Service};
use tracing::{error, instrument};

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`DescribeConfigsRequest`] returning [`DescribeConfigsResponse`].
/// ```
/// use rama::{Context, Layer, Service as _, layer::MapStateLayer};
/// use nisshi_sans_io::{ConfigResource, DescribeConfigsRequest,
///     EndpointType, ErrorCode, describe_configs_request::DescribeConfigsResource};
/// use nisshi_storage::{DescribeConfigsService, Error, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(111)
///     .advertised_listener(Url::parse("tcp://localhost:9092")?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let service = MapStateLayer::new(|_| storage).into_layer(DescribeConfigsService);
///
/// let response = service
///     .serve(
///         Context::default(),
///         DescribeConfigsRequest::default()
///             .include_documentation(Some(false))
///             .include_synonyms(Some(false))
///             .resources(Some(
///                 [DescribeConfigsResource::default()
///                     .resource_name("abcba".into())
///                     .resource_type(ConfigResource::Topic.into())
///                     .configuration_keys(Some([].into()))]
///                 .into(),
///             )),
///     )
///     .await?;
///
/// let results = response.results.unwrap_or_default();
/// assert_eq!(1, results.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);
/// assert!(results[0].configs.as_deref().unwrap_or_default().is_empty());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeConfigsService;

impl ApiKey for DescribeConfigsService {
    const KEY: i16 = DescribeConfigsRequest::KEY;
}

impl<G> Service<G, DescribeConfigsRequest> for DescribeConfigsService
where
    G: Storage,
{
    type Response = DescribeConfigsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: DescribeConfigsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let mut results = vec![];

        for resource in req.resources.unwrap_or_default() {
            results.push(
                ctx.state()
                    .describe_config(
                        resource.resource_name.as_str(),
                        ConfigResource::from(resource.resource_type),
                        resource.configuration_keys.as_deref(),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?,
            );
        }

        Ok(DescribeConfigsResponse::default().results(Some(results)))
    }
}
