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

use std::{collections::BTreeSet, ops::Deref as _};

use nisshi_sans_io::{
    ApiKey, IsolationLevel, ListOffset, ListOffsetsRequest, ListOffsetsResponse,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
};
use rama::{Context, Service};
use tracing::{debug, error, instrument};

use crate::{Error, Result, Storage, Topition};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`ListOffsetsRequest`] returning [`ListOffsetsResponse`].
/// ```
/// use rama::{Context, Layer as _, Service, layer::MapStateLayer};
/// use nisshi_sans_io::{
///     ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest,
///     list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
/// };
/// use nisshi_storage::{Error, ListOffsetsService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
/// const NODE_ID: i32 = 111;
///
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let service = MapStateLayer::new(|_| storage).into_layer(ListOffsetsService);
///
/// let topic = "abcba";
///
/// let response = service
///     .serve(
///         Context::default(),
///         ListOffsetsRequest::default()
///             .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
///             .replica_id(NODE_ID)
///             .topics(Some(
///                 [ListOffsetsTopic::default()
///                     .name(topic.into())
///                     .partitions(Some(
///                         [ListOffsetsPartition::default()
///                             .current_leader_epoch(Some(-1))
///                             .max_num_offsets(Some(3))
///                             .partition_index(0)
///                             .timestamp(ListOffset::Earliest.try_into()?)]
///                         .into(),
///                     ))]
///                 .into(),
///             )),
///     )
///     .await?;
///
/// let topics = response.topics.as_deref().unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(topic, topics[0].name);
///
/// let partitions = topics[0].partitions.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(0, partitions[0].partition_index);
/// assert!(partitions[0].old_style_offsets.is_none());
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(partitions[0].error_code)?
/// );
/// assert_eq!(Some(-1), partitions[0].timestamp);
/// assert_eq!(Some(0), partitions[0].offset);
/// assert_eq!(Some(0), partitions[0].leader_epoch);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListOffsetsService;

impl ApiKey for ListOffsetsService {
    const KEY: i16 = ListOffsetsRequest::KEY;
}

impl<G> Service<G, ListOffsetsRequest> for ListOffsetsService
where
    G: Storage,
{
    type Response = ListOffsetsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: ListOffsetsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let throttle_time_ms = Some(0);

        let isolation_level = req
            .isolation_level
            .map_or(Ok(IsolationLevel::ReadUncommitted), |isolation_level| {
                IsolationLevel::try_from(isolation_level)
            })?;

        let topics = if let Some(topics) = req.topics {
            let mut offsets = vec![];

            for topic in topics {
                if let Some(ref partitions) = topic.partitions {
                    for partition in partitions {
                        let tp = Topition::new(topic.name.clone(), partition.partition_index);
                        let offset = ListOffset::try_from(partition.timestamp)?;

                        offsets.push((tp, offset));
                    }
                }
            }

            ctx.state()
                .list_offsets(isolation_level, offsets.deref())
                .await
                .inspect(|r| debug!(?r, ?offsets))
                .inspect_err(|err| error!(?err, ?offsets))
                .map(|offsets| {
                    offsets
                        .iter()
                        .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                            _ = topics.insert(topition.topic());
                            topics
                        })
                        .iter()
                        .map(|topic_name| {
                            ListOffsetsTopicResponse::default()
                                .name((*topic_name).into())
                                .partitions(Some(
                                    offsets
                                        .iter()
                                        .filter_map(|(topition, offset)| {
                                            if topition.topic() == *topic_name {
                                                Some(
                                                    ListOffsetsPartitionResponse::default()
                                                        .partition_index(topition.partition())
                                                        .error_code(offset.error_code().into())
                                                        .old_style_offsets(None)
                                                        .timestamp(
                                                            offset
                                                                .timestamp()
                                                                .unwrap_or(Some(-1))
                                                                .or(Some(-1)),
                                                        )
                                                        .offset(offset.offset().or(Some(0)))
                                                        .leader_epoch(Some(0)),
                                                )
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                ))
                        })
                        .collect()
                })
                .map(Some)?
        } else {
            None
        };

        Ok(ListOffsetsResponse::default()
            .throttle_time_ms(throttle_time_ms)
            .topics(topics))
        .inspect(|r| debug!(?r))
    }
}
