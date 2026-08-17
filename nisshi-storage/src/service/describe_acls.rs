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

use nisshi_sans_io::{ApiKey, DescribeAclsRequest, DescribeAclsResponse, ErrorCode};
use rama::{Context, Service};

use crate::{Error, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeAclsService;

impl ApiKey for DescribeAclsService {
    const KEY: i16 = DescribeAclsRequest::KEY;
}

impl<G> Service<G, DescribeAclsRequest> for DescribeAclsService
where
    G: Storage,
{
    type Response = DescribeAclsResponse;
    type Error = Error;

    async fn serve(
        &self,
        _ctx: Context<G>,
        _req: DescribeAclsRequest,
    ) -> Result<Self::Response, Self::Error> {
        Ok(DescribeAclsResponse::default()
            .error_code(i16::from(ErrorCode::None))
            .resources(Some([].into())))
    }
}
