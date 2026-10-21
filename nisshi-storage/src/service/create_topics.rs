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
    ApiKey, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, NULL_TOPIC_ID,
    create_topics_response::CreatableTopicResult,
};
use rama::Service;
use tracing::{debug, instrument};
use uuid::Uuid;

use validation::{ValidatedCreateTopicsRequest, ValidatedTopic, error_result};

use crate::{Error, Result, Storage, TopicId};

pub(crate) mod validation;

/// A [`Service`] using its [`Storage`] taking [`ValidatedCreateTopicsRequest`] returning [`CreateTopicsResponse`].
///
/// Wrap it in a [`CreateTopicsValidationLayer`](validation::CreateTopicsValidationLayer) to serve a
/// [`CreateTopicsRequest`].
///
/// ```no_run
/// use rama::{Layer as _, Service as _};
/// use nisshi_sans_io::{NULL_TOPIC_ID, CreateTopicsRequest,
///     create_topics_request::CreatableTopic, ErrorCode};
/// use nisshi_storage::{CreateTopicsService, CreateTopicsValidationLayer, Error, StorageContainer};
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
/// let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService { storage });
///
/// let name = "abcba";
///
/// let response = service
///     .serve(
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
#[derive(Clone, Debug)]
pub struct CreateTopicsService<G> {
    pub storage: G,
}

impl<G> ApiKey for CreateTopicsService<G> {
    const KEY: i16 = CreateTopicsRequest::KEY;
}

impl<G> CreateTopicsService<G>
where
    G: Storage,
{
    /// Whether the topic `name` exists. A topic whose lookup fails counts as missing, so that its
    /// rejection keeps the error that the request caused.
    async fn topic_exists(&self, name: &str) -> bool {
        self.storage
            .metadata(Some(&[TopicId::Name(name.to_owned())]))
            .await
            .inspect_err(|error| debug!(topic = name, ?error))
            .is_ok_and(|metadata| {
                // A backend can return topics other than the one that the lookup names.
                metadata.topics().iter().any(|topic| {
                    topic.name.as_deref() == Some(name)
                        && topic.error_code == i16::from(ErrorCode::None)
                })
            })
    }
}

impl<G> Service<ValidatedCreateTopicsRequest> for CreateTopicsService<G>
where
    G: Storage,
{
    type Output = CreateTopicsResponse;
    type Error = Error;

    #[instrument(skip(self, request))]
    async fn serve(
        &self,
        request: ValidatedCreateTopicsRequest,
    ) -> Result<Self::Output, Self::Error> {
        let mut topics = vec![];

        for topic in request.topics {
            let topic = match topic {
                ValidatedTopic::Valid(topic) => topic,

                ValidatedTopic::Rejected(result) => {
                    // Kafka answers TOPIC_ALREADY_EXISTS for an existing topic before it checks
                    // the configs ([ReplicationControlManager.java#L601-L609]), and a client that
                    // treats that answer as success repeats a create with the same configs.
                    //
                    // [ReplicationControlManager.java#L601-L609]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L601-L609
                    let result = if result.error_code == i16::from(ErrorCode::InvalidConfig)
                        && self.topic_exists(&result.name).await
                    {
                        error_result(
                            result.name,
                            result.num_partitions,
                            result.replication_factor,
                            ErrorCode::TopicAlreadyExists,
                        )
                    } else {
                        result
                    };

                    topics.push(result);
                    continue;
                }
            };

            let name = topic.name.clone();
            let num_partitions = Some(topic.num_partitions);
            let replication_factor = Some(topic.replication_factor);

            let created = if request.validate_only {
                self.dry_run(&name).await
            } else {
                self.storage.create_topic(topic, false).await
            };

            match created {
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

                Err(Error::Api(error_code)) => topics.push(error_result(
                    name,
                    num_partitions,
                    replication_factor,
                    error_code,
                )),

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

impl<G> CreateTopicsService<G>
where
    G: Storage,
{
    /// Answers a dry run of creating the topic `name`, for a `validate_only` request. The
    /// dry run checks whether the topic exists and does not create it.
    ///
    /// An existing topic fails with [`ErrorCode::TopicAlreadyExists`]. Any other storage error
    /// code fails with [`ErrorCode::UnknownServerError`]. A new topic succeeds with a random
    /// topic id instead of the null topic id, because Kafka 3.9.1 answers a `validate_only`
    /// request with a random id ([`createTopic`]). No stored topic has that id.
    ///
    /// [`createTopic`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L778
    async fn dry_run(&self, name: &str) -> Result<Uuid> {
        let metadata = self
            .storage
            .metadata(Some(&[TopicId::Name(name.into())]))
            .await?;

        // `Storage::metadata` includes every requested topic that exists, and may leave out
        // one that does not, so a name that is missing from the response is a topic that
        // does not exist.
        let Some(topic) = metadata
            .topics()
            .iter()
            .find(|topic| topic.name.as_deref() == Some(name))
        else {
            return Ok(Uuid::new_v4());
        };

        match ErrorCode::try_from(topic.error_code) {
            Ok(ErrorCode::None) => Err(Error::Api(ErrorCode::TopicAlreadyExists)),
            Ok(ErrorCode::UnknownTopicOrPartition) => Ok(Uuid::new_v4()),
            _ => Err(Error::Api(ErrorCode::UnknownServerError)),
        }
    }
}
