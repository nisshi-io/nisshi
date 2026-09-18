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

use nisshi_sans_io::{
    ApiKey, DescribeGroupsRequest, DescribeGroupsResponse, describe_groups_response::DescribedGroup,
};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`DescribeGroupsRequest`] returning [`DescribeGroupsResponse`].
/// ```
/// use rama::{Context, Layer as _, Service, layer::MapStateLayer};
/// use nisshi_sans_io::{DescribeGroupsRequest, ErrorCode};
/// use nisshi_storage::{DescribeGroupsService, Error, StorageContainer};
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
/// let service = MapStateLayer::new(|_| storage).into_layer(DescribeGroupsService);
///
/// let group_id = "abcba";
///
/// let response = service
///     .serve(
///         Context::default(),
///         DescribeGroupsRequest::default()
///             .groups(Some([group_id.into()].into()))
///             .include_authorized_operations(Some(false)),
///     )
///     .await?;
///
/// let groups = response.groups.unwrap_or_default();
/// assert_eq!(1, groups.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
/// assert_eq!(group_id, groups[0].group_id.as_str());
/// assert_eq!("Empty", groups[0].group_state.as_str());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeGroupsService;

impl ApiKey for DescribeGroupsService {
    const KEY: i16 = DescribeGroupsRequest::KEY;
}

impl<G> Service<G, DescribeGroupsRequest> for DescribeGroupsService
where
    G: Storage,
{
    type Response = DescribeGroupsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: DescribeGroupsRequest,
    ) -> Result<Self::Response, Self::Error> {
        ctx.state()
            .describe_groups(
                req.groups.as_deref(),
                req.include_authorized_operations.unwrap_or(false),
            )
            .await
            .map(|described| {
                described
                    .iter()
                    .map(DescribedGroup::from)
                    .collect::<Vec<_>>()
            })
            .map(Some)
            .map(|groups| {
                DescribeGroupsResponse::default()
                    .throttle_time_ms(Some(0))
                    .groups(groups)
            })
    }
}
