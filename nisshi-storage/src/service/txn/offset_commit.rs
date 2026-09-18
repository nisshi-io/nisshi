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

use nisshi_sans_io::{ApiKey, TxnOffsetCommitResponse};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`nisshi_sans_io::TxnOffsetCommitRequest`] returning [`TxnOffsetCommitResponse`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OffsetCommitService;

impl ApiKey for OffsetCommitService {
    const KEY: i16 = nisshi_sans_io::TxnOffsetCommitRequest::KEY;
}

impl<G> Service<G, nisshi_sans_io::TxnOffsetCommitRequest> for OffsetCommitService
where
    G: Storage,
{
    type Response = TxnOffsetCommitResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: nisshi_sans_io::TxnOffsetCommitRequest,
    ) -> Result<Self::Response, Self::Error> {
        let responses = ctx
            .state()
            .txn_offset_commit(crate::TxnOffsetCommitRequest {
                transaction_id: req.transactional_id.to_owned(),
                group_id: req.group_id.to_owned(),
                producer_id: req.producer_id,
                producer_epoch: req.producer_epoch,
                generation_id: req.generation_id,
                member_id: req.member_id,
                group_instance_id: req.group_instance_id,
                topics: req.topics.unwrap_or_default(),
            })
            .await?;

        Ok(TxnOffsetCommitResponse::default()
            .throttle_time_ms(0)
            .topics(Some(responses)))
    }
}
