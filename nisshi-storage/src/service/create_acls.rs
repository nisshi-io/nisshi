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

use nisshi_sans_io::{ApiKey, CreateAclsRequest, CreateAclsResponse};
use rama::{Context, Service};

use crate::{Error, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CreateAclsService;

impl ApiKey for CreateAclsService {
    const KEY: i16 = CreateAclsRequest::KEY;
}

impl<G> Service<G, CreateAclsRequest> for CreateAclsService
where
    G: Storage,
{
    type Response = CreateAclsResponse;
    type Error = Error;

    async fn serve(
        &self,
        _ctx: Context<G>,
        _req: CreateAclsRequest,
    ) -> Result<Self::Response, Self::Error> {
        Ok(CreateAclsResponse::default())
    }
}
