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

use nisshi_sans_io::{ApiKey, Frame, Header, HeartbeatRequest};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Result, coordinator::group::Coordinator};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HeartbeatService;

impl ApiKey for HeartbeatService {
    const KEY: i16 = HeartbeatRequest::KEY;
}

impl<C> Service<C, Frame> for HeartbeatService
where
    C: Coordinator,
{
    type Response = Frame;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(&self, mut ctx: Context<C>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;

        let coordinator = ctx.state_mut();

        let req = HeartbeatRequest::try_from(req.body)?;

        coordinator
            .heartbeat(
                req.group_id.as_str(),
                req.generation_id,
                req.member_id.as_str(),
                req.group_instance_id.as_deref(),
            )
            .await
            .map(|body| Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
    }
}
