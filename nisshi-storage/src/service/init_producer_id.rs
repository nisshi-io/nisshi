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

use nisshi_sans_io::{ApiKey, InitProducerIdRequest, InitProducerIdResponse};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`InitProducerIdRequest`] returning [`InitProducerIdResponse`].
/// ```
/// use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
/// use nisshi_sans_io::{ErrorCode, InitProducerIdRequest, InitProducerIdResponse};
/// use nisshi_storage::{Error, InitProducerIdService, StorageContainer};
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
/// let service = MapStateLayer::new(|_| storage).into_layer(InitProducerIdService);
///
/// let transactional_id = None;
/// let transaction_timeout_ms = 0;
/// let producer_id = Some(-1);
/// let producer_epoch = Some(-1);
///
/// assert_eq!(
///     service
///         .serve(
///             Context::default(),
///             InitProducerIdRequest::default()
///                 .transactional_id(transactional_id.clone())
///                 .transaction_timeout_ms(transaction_timeout_ms)
///                 .producer_id(producer_id)
///                 .producer_epoch(producer_epoch)
///         )
///         .await?,
///     InitProducerIdResponse::default()
///         .error_code(ErrorCode::None.into())
///         .producer_id(1)
///         .producer_epoch(0)
/// );
///
/// assert_eq!(
///     service
///         .serve(
///             Context::default(),
///             InitProducerIdRequest::default()
///                 .transactional_id(transactional_id)
///                 .transaction_timeout_ms(transaction_timeout_ms)
///                 .producer_id(producer_id)
///                 .producer_epoch(producer_epoch)
///         )
///         .await?,
///     InitProducerIdResponse::default()
///         .error_code(ErrorCode::None.into())
///         .producer_id(2)
///         .producer_epoch(0)
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InitProducerIdService;

impl ApiKey for InitProducerIdService {
    const KEY: i16 = InitProducerIdRequest::KEY;
}

impl<G> Service<G, InitProducerIdRequest> for InitProducerIdService
where
    G: Storage,
{
    type Response = InitProducerIdResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: InitProducerIdRequest,
    ) -> Result<Self::Response, Self::Error> {
        ctx.state()
            .init_producer(
                req.transactional_id.as_deref(),
                req.transaction_timeout_ms,
                req.producer_id,
                req.producer_epoch,
            )
            .await
            .map(|response| {
                InitProducerIdResponse::default()
                    .throttle_time_ms(0)
                    .error_code(response.error.into())
                    .producer_id(response.id)
                    .producer_epoch(response.epoch)
            })
    }
}
