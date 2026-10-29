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

//! `TxnOffsetCommit`: the group's member, generation and group instance ID are checked by the
//! group coordinator, which holds the group. The storage checks the producer.

use nisshi_sans_io::{
    ApiKey as _, RequestInput, TxnOffsetCommitRequest, TxnOffsetCommitResponse,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use nisshi_service::{FrameRequestLayer, FrameRouteBuilder};
use nisshi_storage::{Storage, TxnOffsetCommitService};
use rama::{Layer, Service, layer::MapErrLayer};

use crate::{Error, Result, coordinator::group::Coordinator};

/// Registers the route of `TxnOffsetCommit`: the group coordinator fences the commit, then the
/// storage commits it.
pub fn txn_offset_commit<C, S>(
    builder: FrameRouteBuilder<Error>,
    coordinator: C,
    storage: S,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    C: Coordinator,
    S: Storage,
{
    builder
        .with_route(
            TxnOffsetCommitRequest::KEY,
            (
                FrameRequestLayer::<TxnOffsetCommitRequest>::new(),
                TxnOffsetCommitFenceLayer { coordinator },
                MapErrLayer::new(Error::from),
            )
                .into_layer(TxnOffsetCommitService { storage })
                .boxed(),
        )
        .map_err(Into::into)
}

/// A [`Layer`] that answers a `TxnOffsetCommit` that the group's coordinator fences, and passes
/// any other to its inner service.
#[derive(Clone, Debug)]
pub struct TxnOffsetCommitFenceLayer<C> {
    pub coordinator: C,
}

impl<C, S> Layer<S> for TxnOffsetCommitFenceLayer<C>
where
    C: Clone,
{
    type Service = TxnOffsetCommitFenceService<C, S>;

    fn layer(&self, inner: S) -> Self::Service {
        TxnOffsetCommitFenceService {
            coordinator: self.coordinator.clone(),
            inner,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TxnOffsetCommitFenceService<C, S> {
    coordinator: C,
    inner: S,
}

impl<C, S> Service<RequestInput<TxnOffsetCommitRequest>> for TxnOffsetCommitFenceService<C, S>
where
    C: Coordinator,
    S: Service<
            RequestInput<TxnOffsetCommitRequest>,
            Output = TxnOffsetCommitResponse,
            Error = Error,
        >,
{
    type Output = TxnOffsetCommitResponse;
    type Error = Error;

    async fn serve(
        &self,
        input: RequestInput<TxnOffsetCommitRequest>,
    ) -> Result<Self::Output, Self::Error> {
        let request = &input.request;
        let generation_id = request.generation_id.unwrap_or(-1);
        let member_id = request.member_id.as_deref().unwrap_or_default();

        // A commit without a generation, a member or a group instance ID (Produce versions
        // before 3 have none) is not checked against the group, whatever its state, as in
        // Kafka's `validateOffsetCommit` for a transactional commit.
        if generation_id < 0 && member_id.is_empty() && request.group_instance_id.is_none() {
            return self.inner.serve(input).await;
        }

        match self
            .coordinator
            .txn_offset_commit_fence(
                &request.group_id,
                generation_id,
                member_id,
                request.group_instance_id.as_deref(),
            )
            .await?
        {
            None => self.inner.serve(input).await,

            Some(error_code) => Ok(TxnOffsetCommitResponse::default()
                .throttle_time_ms(0)
                .topics(Some(
                    request
                        .topics
                        .iter()
                        .flatten()
                        .map(|topic| {
                            TxnOffsetCommitResponseTopic::default()
                                .name(topic.name.clone())
                                .partitions(Some(
                                    topic
                                        .partitions
                                        .iter()
                                        .flatten()
                                        .map(|partition| {
                                            TxnOffsetCommitResponsePartition::default()
                                                .partition_index(partition.partition_index)
                                                .error_code(i16::from(error_code))
                                        })
                                        .collect(),
                                ))
                        })
                        .collect(),
                ))),
        }
    }
}
