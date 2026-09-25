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

use crate::{Error, Result, Storage, TopicId};
use nisshi_sans_io::{
    ApiKey, ErrorCode, ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse,
    RequestInput,
    list_partition_reassignments_response::{
        OngoingPartitionReassignment, OngoingTopicReassignment,
    },
};
use rama::Service;
use tracing::instrument;

/// A [`Service`] using [`Storage`] as [`Context`] taking [`ListPartitionReassignmentsRequest`] returning [`ListPartitionReassignmentsResponse`].
#[derive(Clone, Debug)]
pub struct ListPartitionReassignmentsService<G> {
    pub storage: G,
}

impl<G> ApiKey for ListPartitionReassignmentsService<G> {
    const KEY: i16 = ListPartitionReassignmentsRequest::KEY;
}

impl<G, I> Service<I> for ListPartitionReassignmentsService<G>
where
    G: Storage,
    I: Into<RequestInput<ListPartitionReassignmentsRequest>> + Send + 'static,
{
    type Output = ListPartitionReassignmentsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();
        let topics = input.request.topics.map(|topics| {
            topics
                .iter()
                .map(|topic| TopicId::Name(topic.name.as_str().into()))
                .collect::<Vec<_>>()
        });

        let metadata = self.storage.metadata(topics.as_deref()).await?;

        let mut ongoing = vec![];

        for topic in metadata.topics() {
            let Some(ref name) = topic.name else { continue };

            ongoing.push(
                OngoingTopicReassignment::default()
                    .name(name.into())
                    .partitions(topic.partitions.as_ref().map(|partitions| {
                        partitions
                            .iter()
                            .map(|partition| {
                                OngoingPartitionReassignment::default()
                                    .partition_index(partition.partition_index)
                                    .replicas(partition.replica_nodes.clone())
                                    .adding_replicas(Some(vec![]))
                                    .removing_replicas(Some(vec![]))
                            })
                            .collect::<Vec<_>>()
                    })),
            );
        }

        Ok(ListPartitionReassignmentsResponse::default()
            .throttle_time_ms(0)
            .error_code(ErrorCode::None.into())
            .error_message(None)
            .topics(Some(ongoing)))
    }
}
