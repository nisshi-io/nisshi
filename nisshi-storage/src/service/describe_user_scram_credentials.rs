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

use nisshi_sans_io::{
    ApiKey, DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse,
};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeUserScramCredentialsService;

impl ApiKey for DescribeUserScramCredentialsService {
    const KEY: i16 = DescribeUserScramCredentialsRequest::KEY;
}

impl<G> Service<G, DescribeUserScramCredentialsRequest> for DescribeUserScramCredentialsService
where
    G: Storage,
{
    type Response = DescribeUserScramCredentialsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: DescribeUserScramCredentialsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let _ = (ctx, req);

        Ok(DescribeUserScramCredentialsResponse::default())
    }
}
