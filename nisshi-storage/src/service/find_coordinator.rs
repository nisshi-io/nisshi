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
    ApiKey, ErrorCode, FindCoordinatorRequest, FindCoordinatorResponse,
    find_coordinator_response::Coordinator,
};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`FindCoordinatorRequest`] returning [`FindCoordinatorResponse`].
/// ```
/// use rama::{Context, Layer as _, Service, layer::MapStateLayer};
/// use nisshi_sans_io::{ErrorCode, FindCoordinatorRequest};
/// use nisshi_storage::{Error, FindCoordinatorService, StorageContainer};
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
/// let service = MapStateLayer::new(|_| storage).into_layer(FindCoordinatorService);
///
/// let response = service
///     .serve(
///         Context::default(),
///         FindCoordinatorRequest::default()
///             .key(Some("abcba".into()))
///             .key_type(Some(0))
///             .coordinator_keys(Some(["xyzyx".into()].into())),
///     )
///     .await?;
///
/// assert_eq!(
///     Some(ErrorCode::None.into()),
///     if let Some(error_code) = response.error_code {
///         ErrorCode::try_from(error_code).map(Some)?
///     } else {
///         None
///     }
/// );
///
/// assert_eq!(Some(NODE_ID), response.node_id);
/// assert_eq!(Some(HOST), response.host.as_deref());
/// assert_eq!(Some(PORT), response.port);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FindCoordinatorService;

impl ApiKey for FindCoordinatorService {
    const KEY: i16 = FindCoordinatorRequest::KEY;
}

impl<G> Service<G, FindCoordinatorRequest> for FindCoordinatorService
where
    G: Storage,
{
    type Response = FindCoordinatorResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: FindCoordinatorRequest,
    ) -> Result<Self::Response, Self::Error> {
        let node_id = ctx.state().node().await?;

        let listener = ctx.state().advertised_listener().await?;
        let host = listener.host_str().unwrap_or("localhost");
        let port = i32::from(listener.port().unwrap_or(9092));

        Ok(FindCoordinatorResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(Some(ErrorCode::None.into()))
            .error_message(Some("NONE".into()))
            .node_id(Some(node_id))
            .host(Some(host.into()))
            .port(Some(port))
            .coordinators(req.coordinator_keys.map(|keys| {
                keys.iter()
                    .map(|key| {
                        Coordinator::default()
                            .key(key.to_string())
                            .node_id(node_id)
                            .host(host.into())
                            .port(port)
                            .error_code(ErrorCode::None.into())
                            .error_message(None)
                    })
                    .collect()
            })))
    }
}
