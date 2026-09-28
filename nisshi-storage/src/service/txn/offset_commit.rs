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

use nisshi_sans_io::{ApiKey, RequestInput, TxnOffsetCommitResponse};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`nisshi_sans_io::TxnOffsetCommitRequest`] returning [`TxnOffsetCommitResponse`].
#[derive(Clone, Debug)]
pub struct OffsetCommitService<G> {
    pub storage: G,
}

impl<G> ApiKey for OffsetCommitService<G> {
    const KEY: i16 = nisshi_sans_io::TxnOffsetCommitRequest::KEY;
}

impl<G, I> Service<I> for OffsetCommitService<G>
where
    G: Storage,
    I: Into<RequestInput<nisshi_sans_io::TxnOffsetCommitRequest>> + Send + 'static,
{
    type Output = TxnOffsetCommitResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let responses = self
            .storage
            .txn_offset_commit(crate::TxnOffsetCommitRequest {
                transaction_id: input.request.transactional_id.to_owned(),
                group_id: input.request.group_id.to_owned(),
                producer_id: input.request.producer_id,
                producer_epoch: input.request.producer_epoch,
                generation_id: input.request.generation_id,
                member_id: input.request.member_id,
                group_instance_id: input.request.group_instance_id,
                topics: input.request.topics.unwrap_or_default(),
            })
            .await?;

        Ok(TxnOffsetCommitResponse::default()
            .throttle_time_ms(0)
            .topics(Some(responses)))
    }
}
