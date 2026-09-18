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

use nisshi_sans_io::{ApiKey, Frame, Header, JoinGroupRequest};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Result, coordinator::group::Coordinator};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JoinGroupService;

impl ApiKey for JoinGroupService {
    const KEY: i16 = JoinGroupRequest::KEY;
}

impl<C> Service<C, Frame> for JoinGroupService
where
    C: Coordinator,
{
    type Response = Frame;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(&self, mut ctx: Context<C>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;
        let coordinator = ctx.state_mut();

        let client_id = req
            .client_id()
            .map(|client_id| client_id.map(|client_id| client_id.to_owned()))?;

        let join_group = JoinGroupRequest::try_from(req.body)?;

        coordinator
            .join(
                client_id.as_deref(),
                join_group.group_id.as_str(),
                join_group.session_timeout_ms,
                join_group.rebalance_timeout_ms,
                join_group.member_id.as_str(),
                join_group.group_instance_id.as_deref(),
                join_group.protocol_type.as_str(),
                join_group.protocols.as_deref(),
                join_group.reason.as_deref(),
            )
            .await
            .map(|body| Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
    }
}
