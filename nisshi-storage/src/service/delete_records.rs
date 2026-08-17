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

use nisshi_sans_io::{ApiKey, DeleteRecordsRequest, DeleteRecordsResponse};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`DeleteRecordsRequest`] returning [`DeleteRecordsResponse`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeleteRecordsService;

impl ApiKey for DeleteRecordsService {
    const KEY: i16 = DeleteRecordsRequest::KEY;
}

impl<G> Service<G, DeleteRecordsRequest> for DeleteRecordsService
where
    G: Storage,
{
    type Response = DeleteRecordsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: DeleteRecordsRequest,
    ) -> Result<Self::Response, Self::Error> {
        ctx.state()
            .delete_records(req.topics.as_deref().unwrap_or_default())
            .await
            .map(Some)
            .map(|topics| {
                DeleteRecordsResponse::default()
                    .throttle_time_ms(0)
                    .topics(topics)
            })
    }
}
