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

use nisshi_sans_io::{ApiKey, EndTxnRequest, EndTxnResponse};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Result, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EndService;

impl ApiKey for EndService {
    const KEY: i16 = EndTxnRequest::KEY;
}

impl<G> Service<G, EndTxnRequest> for EndService
where
    G: Storage,
{
    type Response = EndTxnResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: EndTxnRequest,
    ) -> Result<Self::Response, Self::Error> {
        ctx.state()
            .txn_end(
                req.transactional_id.as_str(),
                req.producer_id,
                req.producer_epoch,
                req.committed,
            )
            .await
            .map(|error_code| EndTxnResponse::default().error_code(i16::from(error_code)))
    }
}
