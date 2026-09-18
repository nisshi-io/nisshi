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

use nisshi_sans_io::{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey, ErrorCode};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Result, Storage, TxnAddPartitionsRequest, TxnAddPartitionsResponse};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`AddPartitionsToTxnRequest`] returning [`AddPartitionsToTxnResponse`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AddPartitionService;

impl ApiKey for AddPartitionService {
    const KEY: i16 = AddPartitionsToTxnRequest::KEY;
}

impl<G> Service<G, AddPartitionsToTxnRequest> for AddPartitionService
where
    G: Storage,
{
    type Response = AddPartitionsToTxnResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: AddPartitionsToTxnRequest,
    ) -> Result<Self::Response, Self::Error> {
        let req = TxnAddPartitionsRequest::try_from(req)?;

        match ctx.state().txn_add_partitions(req).await? {
            TxnAddPartitionsResponse::VersionZeroToThree(results_by_topic_v_3_and_below) => {
                Ok(AddPartitionsToTxnResponse::default()
                    .throttle_time_ms(0)
                    .error_code(Some(ErrorCode::None.into()))
                    .results_by_transaction(Some([].into()))
                    .results_by_topic_v_3_and_below(Some(results_by_topic_v_3_and_below)))
            }

            TxnAddPartitionsResponse::VersionFourPlus(results_by_transaction) => {
                Ok(AddPartitionsToTxnResponse::default()
                    .throttle_time_ms(0)
                    .error_code(Some(ErrorCode::None.into()))
                    .results_by_transaction(Some(results_by_transaction))
                    .results_by_topic_v_3_and_below(Some([].into())))
            }
        }
    }
}
