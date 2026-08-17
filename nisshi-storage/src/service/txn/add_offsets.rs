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

use nisshi_sans_io::{AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, ApiKey};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`AddOffsetsToTxnRequest`] returning [`AddOffsetsToTxnResponse`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AddOffsetsService;

impl ApiKey for AddOffsetsService {
    const KEY: i16 = AddOffsetsToTxnRequest::KEY;
}

impl<G> Service<G, AddOffsetsToTxnRequest> for AddOffsetsService
where
    G: Storage,
{
    type Response = AddOffsetsToTxnResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: AddOffsetsToTxnRequest,
    ) -> Result<Self::Response, Self::Error> {
        ctx.state()
            .txn_add_offsets(
                req.transactional_id.as_str(),
                req.producer_id,
                req.producer_epoch,
                req.group_id.as_str(),
            )
            .await
            .map(|error_code| {
                AddOffsetsToTxnResponse::default()
                    .throttle_time_ms(0)
                    .error_code(error_code.into())
            })
    }
}
