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
    ApiKey, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, NULL_TOPIC_ID,
    create_topics_response::CreatableTopicResult,
};
use rama::{Context, Service};
use tracing::{debug, instrument};

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`CreateTopicsRequest`] returning [`CreateTopicsResponse`].
/// ```
/// use rama::{Context, Layer, Service as _, layer::MapStateLayer};
/// use nisshi_sans_io::{NULL_TOPIC_ID, CreateTopicsRequest,
///     create_topics_request::CreatableTopic, ErrorCode};
/// use nisshi_storage::{CreateTopicsService, Error, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(111)
///     .advertised_listener(Url::parse("tcp://localhost:9092")?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let service = MapStateLayer::new(|_| storage).into_layer(CreateTopicsService);
///
/// let name = "abcba";
///
/// let response = service
///     .serve(
///         Context::default(),
///         CreateTopicsRequest::default()
///             .topics(Some(vec![
///                 CreatableTopic::default()
///                     .name(name.into())
///                     .num_partitions(1)
///                     .replication_factor(3)
///                     .assignments(Some([].into()))
///                     .configs(Some([].into())),
///             ]))
///             .validate_only(Some(false)),
///     )
///     .await?;
///
/// let topics = response.topics.unwrap_or_default();
///
/// assert_eq!(1, topics.len());
/// assert_eq!(name, topics[0].name.as_str());
/// assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
/// assert_eq!(Some(1), topics[0].num_partitions);
/// assert_eq!(Some(3), topics[0].replication_factor);
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CreateTopicsService;

impl ApiKey for CreateTopicsService {
    const KEY: i16 = CreateTopicsRequest::KEY;
}

impl<G> Service<G, CreateTopicsRequest> for CreateTopicsService
where
    G: Storage,
{
    type Response = CreateTopicsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: CreateTopicsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let mut topics = vec![];

        for mut topic in req.topics.unwrap_or_default() {
            let name = topic.name.clone();

            let num_partitions = Some(match topic.num_partitions {
                -1 => {
                    topic.num_partitions = 3;
                    topic.num_partitions
                }
                otherwise => otherwise,
            });

            let replication_factor = Some(match topic.replication_factor {
                -1 => {
                    topic.replication_factor = 1;
                    topic.replication_factor
                }
                otherwise => otherwise,
            });

            match ctx
                .state()
                .create_topic(topic, req.validate_only.unwrap_or_default())
                .await
            {
                Ok(topic_id) => {
                    debug!(?topic_id);

                    topics.push(
                        CreatableTopicResult::default()
                            .name(name)
                            .topic_id(Some(topic_id.into_bytes()))
                            .error_code(ErrorCode::None.into())
                            .error_message(None)
                            .topic_config_error_code(Some(ErrorCode::None.into()))
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .configs(Some([].into())),
                    );
                }

                Err(Error::Api(error_code)) => topics.push(
                    CreatableTopicResult::default()
                        .name(name)
                        .topic_id(Some(NULL_TOPIC_ID))
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .topic_config_error_code(None)
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .configs(Some([].into())),
                ),

                Err(error) => {
                    debug!(?error);

                    topics.push(
                        CreatableTopicResult::default()
                            .name(name)
                            .topic_id(Some(NULL_TOPIC_ID))
                            .error_code(ErrorCode::UnknownServerError.into())
                            .error_message(None)
                            .topic_config_error_code(None)
                            .num_partitions(None)
                            .replication_factor(None)
                            .configs(Some([].into())),
                    )
                }
            }
        }

        Ok(CreateTopicsResponse::default()
            .topics(Some(topics))
            .throttle_time_ms(Some(0)))
    }
}
