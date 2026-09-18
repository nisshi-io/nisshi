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

use nisshi_sans_io::{
    ApiKey as _, HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest, OffsetCommitRequest,
    OffsetFetchRequest, SyncGroupRequest,
};
use nisshi_service::FrameRouteBuilder;
use rama::{
    Layer as _, Service as _,
    layer::{MapErrLayer, MapStateLayer},
};

use crate::{
    Error,
    broker::group::{
        heartbeat::HeartbeatService, join::JoinGroupService, leave::LeaveGroupService,
        offset_commit::OffsetCommitService, offset_fetch::OffsetFetchService,
        sync::SyncGroupService,
    },
    coordinator::group::Coordinator,
};

pub fn services<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    [
        heartbeat,
        join_group,
        leave_group,
        offset_commit,
        offset_fetch,
        sync_group,
    ]
    .iter()
    .try_fold(builder, |builder, service| {
        service(builder, coordinator.clone())
    })
}

pub fn heartbeat<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            HeartbeatRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| coordinator),
            )
                .into_layer(HeartbeatService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn join_group<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            JoinGroupRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| coordinator),
            )
                .into_layer(JoinGroupService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn leave_group<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            LeaveGroupRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| coordinator),
            )
                .into_layer(LeaveGroupService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn offset_commit<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            OffsetCommitRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| coordinator),
            )
                .into_layer(OffsetCommitService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn offset_fetch<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            OffsetFetchRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| coordinator),
            )
                .into_layer(OffsetFetchService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn sync_group<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            SyncGroupRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| coordinator),
            )
                .into_layer(SyncGroupService)
                .boxed(),
        )
        .map_err(Into::into)
}
