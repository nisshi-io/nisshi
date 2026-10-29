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

use std::collections::BTreeSet;

use nisshi_sans_io::{
    ApiKey, CreateTopicsRequest, ErrorCode, MetadataRequest, MetadataResponse, RequestInput,
    create_topics_request::CreatableTopic, topic::is_valid_topic_name,
};
use rama::Service;
use tracing::{debug, error, info, instrument, warn};

use super::create_topics::validation::ValidatedCreateTopicsRequest;
use crate::{
    CreateTopicsService, Error, MAX_PARTITIONS_PER_REQUEST, Result, Storage, TopicCreationConfig,
    TopicId,
};

/// A [`Service`] using its [`Storage`] taking [`MetadataRequest`] returning [`MetadataResponse`].
/// ```no_run
/// use rama::Service;
/// use nisshi_sans_io::MetadataRequest;
/// use nisshi_storage::{Error, MetadataService, StorageContainer, TopicCreationConfig};
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
/// let service = MetadataService {
///     storage,
///     topic_creation: TopicCreationConfig::default(),
/// };
///
/// let response = service
///     .serve(
///         MetadataRequest::default()
///             .allow_auto_topic_creation(Some(false))
///             .include_cluster_authorized_operations(Some(false))
///             .include_topic_authorized_operations(Some(false))
///             .topics(Some([].into())),
///     )
///     .await?;
///
/// let brokers = response.brokers.as_deref().unwrap_or_default();
/// assert_eq!(1, brokers.len());
/// assert_eq!(HOST, brokers[0].host);
/// assert_eq!(PORT, brokers[0].port);
/// assert_eq!(NODE_ID, brokers[0].node_id);
/// assert!(brokers[0].rack.is_none());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct MetadataService<G> {
    pub storage: G,
    pub topic_creation: TopicCreationConfig,
}

impl<G> ApiKey for MetadataService<G> {
    const KEY: i16 = MetadataRequest::KEY;
}

impl<G, I> Service<I> for MetadataService<G>
where
    G: Storage + Clone,
    I: Into<RequestInput<MetadataRequest>> + Send + 'static,
{
    type Output = MetadataResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let topics = input
            .request
            .topics
            .map(|topics| topics.iter().map(TopicId::from).collect::<Vec<_>>());

        let mut response = self
            .storage
            .metadata(topics.as_deref())
            .await
            .inspect_err(|err| error!(?err))?;

        // versions prior to 4 do not have allow_auto_topic_creation,
        // and behave as if it were true
        if input.request.allow_auto_topic_creation.unwrap_or(true)
            && self.topic_creation.auto_create_topics_enable
        {
            let unknown = response
                .topics()
                .iter()
                .filter(|topic| topic.error_code == i16::from(ErrorCode::UnknownTopicOrPartition))
                .filter_map(|topic| topic.name.clone())
                .filter(|name| is_valid_topic_name(name))
                .collect::<BTreeSet<_>>();

            // We create the unknown topics with one `CreateTopics` that asks for the broker
            // defaults, so they get the checks of any other `CreateTopics`, and we create none
            // of them when together they exceed the cap, instead of creating as many as fit.
            // Kafka sends every unknown name of one request to the controller as one
            // `CreateTopics` ([`sendCreateTopicRequest`]), and the controller rejects all of it
            // over the cap ([`validateTotalNumberOfPartitions`]). So each name stays
            // `UNKNOWN_TOPIC_OR_PARTITION`.
            //
            // [`sendCreateTopicRequest`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/AutoTopicCreationManager.scala#L158-L224
            // [`validateTotalNumberOfPartitions`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L1152-L1169
            let request = CreateTopicsRequest::default()
                .validate_only(Some(false))
                .topics(Some(
                    unknown
                        .into_iter()
                        .map(|name| {
                            CreatableTopic::default()
                                .name(name)
                                .num_partitions(-1)
                                .replication_factor(-1)
                                .assignments(Some([].into()))
                                .configs(Some([].into()))
                        })
                        .collect(),
                ));

            let results = match ValidatedCreateTopicsRequest::new(
                request,
                self.topic_creation.num_partitions,
            ) {
                Ok(validated) => CreateTopicsService {
                    storage: self.storage.clone(),
                }
                .serve(validated)
                .await
                .inspect_err(|err| error!(?err))?
                .topics
                .unwrap_or_default(),

                Err(malformed) => {
                    warn!(
                        requested_partitions = malformed.total,
                        num_partitions = self.topic_creation.num_partitions,
                        limit = MAX_PARTITIONS_PER_REQUEST,
                        "not auto-creating topics: too many partitions in one request"
                    );
                    Vec::new()
                }
            };

            let mut created = false;

            for result in results {
                match ErrorCode::try_from(result.error_code) {
                    Ok(ErrorCode::None) => {
                        info!(topic = %result.name, num_partitions = ?result.num_partitions, "auto-created topic");
                        created = true;
                    }

                    // concurrent metadata requests can race to create
                    Ok(ErrorCode::TopicAlreadyExists) => created = true,

                    _ => debug!(?result, "not auto-creating topic"),
                }
            }

            if created {
                response = self
                    .storage
                    .metadata(topics.as_deref())
                    .await
                    .inspect_err(|err| error!(?err))?;
            }
        }
        let brokers = Some(response.brokers().to_owned());
        let cluster_id = response.cluster().map(|s| s.into());
        let controller_id = response.controller();
        let topics = Some(
            response
                .topics()
                .iter()
                .map(|topic| {
                    if topic.error_code == i16::from(ErrorCode::UnknownTopicOrPartition)
                        && topic
                            .name
                            .as_deref()
                            .is_some_and(|name| !is_valid_topic_name(name))
                    {
                        topic
                            .clone()
                            .error_code(ErrorCode::InvalidTopicException.into())
                    } else {
                        topic.clone()
                    }
                })
                .collect::<Vec<_>>(),
        );
        let cluster_authorized_operations = Some(-1);

        let throttle_time_ms = Some(0);

        Ok(MetadataResponse::default()
            .throttle_time_ms(throttle_time_ms)
            .brokers(brokers)
            .cluster_id(cluster_id)
            .controller_id(controller_id)
            .topics(topics)
            .cluster_authorized_operations(cluster_authorized_operations))
    }
}
