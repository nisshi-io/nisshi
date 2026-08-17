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

use nisshi_sans_io::{ApiKey, DescribeClusterRequest, DescribeClusterResponse, ErrorCode};
use rama::{Context, Service};
use tracing::{debug, instrument};

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`DescribeClusterRequest`] returning [`DescribeClusterResponse`].
/// ```
/// use rama::{Context, Layer, Service as _, layer::MapStateLayer};
/// use nisshi_sans_io::{DescribeClusterRequest, EndpointType, ErrorCode};
/// use nisshi_storage::{DescribeClusterService, Error, StorageContainer};
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
/// let service = MapStateLayer::new(|_| storage).into_layer(DescribeClusterService);
///
/// let response = service
///     .serve(
///         Context::default(),
///         DescribeClusterRequest::default()
///             .endpoint_type(Some(EndpointType::Broker.into()))
///             .include_cluster_authorized_operations(false),
///     )
///     .await?;
///
/// let brokers = response.brokers.unwrap_or_default();
/// assert_eq!(1, brokers.len());
/// assert_eq!(NODE_ID, brokers[0].broker_id);
/// assert_eq!(HOST, brokers[0].host.as_str());
/// assert_eq!(PORT, brokers[0].port);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeClusterService;

impl ApiKey for DescribeClusterService {
    const KEY: i16 = DescribeClusterRequest::KEY;
}

impl<G> Service<G, DescribeClusterRequest> for DescribeClusterService
where
    G: Storage,
{
    type Response = DescribeClusterResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: DescribeClusterRequest,
    ) -> Result<Self::Response, Self::Error> {
        let brokers = ctx.state().brokers().await?;
        debug!(?brokers);

        let cluster_id = ctx.state().cluster_id().await?;

        Ok(DescribeClusterResponse::default()
            .throttle_time_ms(0)
            .error_code(ErrorCode::None.into())
            .error_message(None)
            .endpoint_type(req.endpoint_type)
            .controller_id(brokers.first().map(|broker| broker.broker_id).unwrap_or(-1))
            .cluster_id(cluster_id.to_owned())
            .brokers(Some(brokers))
            .cluster_authorized_operations(-2_147_483_648))
    }
}
