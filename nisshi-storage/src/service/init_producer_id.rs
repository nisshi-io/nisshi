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

use nisshi_sans_io::{ApiKey, InitProducerIdRequest, InitProducerIdResponse, RequestInput};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`InitProducerIdRequest`] returning [`InitProducerIdResponse`].
/// ```no_run
/// use rama::Service as _;
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
/// let service = InitProducerIdService { storage };
///
/// let transactional_id = None;
/// let transaction_timeout_ms = 0;
/// let producer_id = Some(-1);
/// let producer_epoch = Some(-1);
///
/// assert_eq!(
///     service
///         .serve(
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
#[derive(Clone, Debug)]
pub struct InitProducerIdService<G> {
    pub storage: G,
}

impl<G> ApiKey for InitProducerIdService<G> {
    const KEY: i16 = InitProducerIdRequest::KEY;
}

impl<G, I> Service<I> for InitProducerIdService<G>
where
    G: Storage,
    I: Into<RequestInput<InitProducerIdRequest>> + Send + 'static,
{
    type Output = InitProducerIdResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        self.storage
            .init_producer(
                input.request.transactional_id.as_deref(),
                input.request.transaction_timeout_ms,
                input.request.producer_id,
                input.request.producer_epoch,
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
