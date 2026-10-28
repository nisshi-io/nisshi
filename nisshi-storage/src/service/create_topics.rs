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

use std::collections::{BTreeMap, BTreeSet};

use nisshi_sans_io::{
    ApiKey, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, NULL_TOPIC_ID,
    create_topics_response::CreatableTopicResult,
    topic::{has_collision_chars, unify_collision_chars},
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

    /// Returns the existing topics whose names have collision characters.
    ///
    /// A listing that holds a topic without a name is a failed listing, and returns an error.
    async fn topics_with_collision_chars(&self) -> Result<TopicsWithCollisionChars> {
        let metadata = self.storage.metadata(None).await?;

        let mut by_unified_name = BTreeMap::<String, BTreeSet<String>>::new();

        for topic in metadata.topics() {
            // An engine can answer a failed listing with `Ok` and one topic that has no name.
            // Without this check, the service would read that as "no existing topics" and
            // create a colliding topic.
            let Some(name) = topic.name.as_deref() else {
                return Err(Error::Message(format!(
                    "listing the existing topics failed: {:?}",
                    ErrorCode::try_from(topic.error_code)
                )));
            };

            if topic.error_code == i16::from(ErrorCode::None) && has_collision_chars(name) {
                _ = by_unified_name
                    .entry(unify_collision_chars(name))
                    .or_default()
                    .insert(name.to_owned());
            }
        }

        Ok(TopicsWithCollisionChars(by_unified_name))
    }
}

/// The existing topics whose names have collision characters, keyed by their unified name (see
/// [`unify_collision_chars`]).
#[derive(Clone, Debug, Default)]
struct TopicsWithCollisionChars(BTreeMap<String, BTreeSet<String>>);

impl TopicsWithCollisionChars {
    /// Returns the existing topic that `name` collides with: a topic whose name differs from
    /// `name` only in its collision characters.
    ///
    /// Returns `None` when a topic named `name` exists, because that topic is answered with
    /// `TOPIC_ALREADY_EXISTS` instead.
    fn colliding_topic(&self, name: &str) -> Option<&str> {
        self.0
            .get(&unify_collision_chars(name))
            .filter(|existing| !existing.contains(name))
            .and_then(|existing| existing.first())
            .map(String::as_str)
    }
}

/// Builds the [`CreatableTopicResult`] for a topic whose create failed with a storage error that
/// is not a Kafka error code.
fn unknown_server_error(name: String) -> CreatableTopicResult {
    CreatableTopicResult::default()
        .name(name)
        .topic_id(Some(NULL_TOPIC_ID))
        .error_code(ErrorCode::UnknownServerError.into())
        .error_message(None)
        .topic_config_error_code(None)
        .num_partitions(None)
        .replication_factor(None)
        .configs(Some([].into()))
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

        // Kafka refuses a topic whose name collides with an existing topic's name, because its
        // per-topic metric names do not tell '.' and '_' apart. It checks every topic, also a
        // topic with an invalid name, against the topics that exist before the request. It
        // checks the names before it looks for an existing topic, and before it checks the
        // configs ([`createTopics`], [`validateNewTopicNames`]).
        //
        // [`createTopics`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L598-L603
        // [`validateNewTopicNames`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L846-L867
        let topics_with_collision_chars = if request
            .topics
            .iter()
            .any(|topic| has_collision_chars(topic.name()))
        {
            self.topics_with_collision_chars()
                .await
                .inspect_err(|error| debug!(?error))
        } else {
            Ok(TopicsWithCollisionChars::default())
        };

        for topic in request.topics {
            // Kafka's controller serves one create at a time, and nisshi serves creates
            // concurrently. Two concurrent creates of colliding names can both pass this check,
            // and both topics then exist. We accept this race instead of adding a lock or a
            // unique index to every storage engine (SOL-156700).
            let collision = match &topics_with_collision_chars {
                _ if !has_collision_chars(topic.name()) => None,

                Ok(existing) => existing.colliding_topic(topic.name()).map(|colliding| {
                    topic
                        .error_result(ErrorCode::InvalidTopicException)
                        .error_message(Some(format!(
                            "Topic '{}' collides with existing topic: {colliding}",
                            topic.name()
                        )))
                }),

                // A rejected topic keeps its own error. A valid topic fails, instead of being
                // created without the check.
                Err(_) => matches!(topic, ValidatedTopic::Valid(_))
                    .then(|| unknown_server_error(topic.name().to_owned())),
            };

            if let Some(result) = collision {
                topics.push(result);
                continue;
            }

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

                    topics.push(unknown_server_error(name))
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

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
        time::SystemTime,
    };

    use async_trait::async_trait;
    use nisshi_sans_io::{
        ConfigResource, CreateTopicsRequest, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
        create_topics_request::CreatableTopic, delete_groups_response::DeletableGroupResult,
        delete_records_request::DeleteRecordsTopic,
        delete_records_response::DeleteRecordsTopicResult,
        describe_cluster_response::DescribeClusterBroker,
        describe_configs_response::DescribeConfigsResult,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        fetch_response::AbortedTransaction,
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
        metadata_response::MetadataResponseTopic, record::deflated,
        txn_offset_commit_response::TxnOffsetCommitResponseTopic,
    };
    use rama::{Layer as _, Service as _};
    use tokio::time::Duration;
    use url::Url;
    use uuid::Uuid;

    use super::CreateTopicsService;
    use crate::{
        BrokerRegistrationRequest, CommittedOffset, CreateTopicsValidationLayer, Error,
        GroupDetail, ListOffsetResponse, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
        OffsetStage, ProducerIdResponse, Result, ScramCredential, Storage, TopicId, Topition,
        TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError,
        Version,
    };

    /// How the all-topics `metadata` listing fails.
    #[derive(Clone, Copy, Debug)]
    enum Listing {
        Error,
        /// One topic without a name, with an error code: the PostgreSQL engine answers a failed
        /// listing query this way.
        NamelessTopic,
    }

    /// Storage whose all-topics `metadata` listing fails. Records the name of each topic that
    /// it creates.
    #[derive(Clone, Debug)]
    struct FailedListing {
        listing: Listing,
        created: Arc<Mutex<Vec<String>>>,
    }

    impl FailedListing {
        fn new(listing: Listing) -> Self {
            Self {
                listing,
                created: Arc::default(),
            }
        }

        fn created(&self) -> Vec<String> {
            self.created.lock().expect("created").clone()
        }
    }

    #[async_trait]
    impl Storage for FailedListing {
        async fn fetch(
            &self,
            _topition: &Topition,
            _offset: i64,
            _min_bytes: u32,
            _max_bytes: u32,
            _isolation_level: IsolationLevel,
            _max_wait: Duration,
        ) -> Result<Vec<deflated::Batch>> {
            unimplemented!()
        }

        async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
            unimplemented!()
        }

        async fn list_offsets(
            &self,
            _isolation_level: IsolationLevel,
            _offsets: &[(Topition, ListOffset)],
        ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
            unimplemented!()
        }

        async fn register_broker(
            &self,
            _broker_registration: BrokerRegistrationRequest,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
            unimplemented!()
        }

        async fn create_topic(&self, topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
            self.created.lock()?.push(topic.name);
            Ok(Uuid::new_v4())
        }

        async fn delete_records(
            &self,
            _topics: &[DeleteRecordsTopic],
        ) -> Result<Vec<DeleteRecordsTopicResult>> {
            unimplemented!()
        }

        async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
            unimplemented!()
        }

        async fn incremental_alter_resource(
            &self,
            _resource: AlterConfigsResource,
        ) -> Result<AlterConfigsResourceResponse> {
            unimplemented!()
        }

        async fn produce(
            &self,
            _transaction_id: Option<&str>,
            _topition: &Topition,
            _deflated: deflated::Batch,
        ) -> Result<i64> {
            unimplemented!()
        }

        async fn offset_commit(
            &self,
            _group: &str,
            _retention: Option<Duration>,
            _offsets: &[(Topition, OffsetCommitRequest)],
        ) -> Result<Vec<(Topition, ErrorCode)>> {
            unimplemented!()
        }

        async fn committed_offset_topitions(
            &self,
            _group_id: &str,
        ) -> Result<BTreeMap<Topition, CommittedOffset>> {
            unimplemented!()
        }

        async fn offset_fetch(
            &self,
            _group_id: Option<&str>,
            _topics: &[Topition],
            _require_stable: Option<bool>,
        ) -> Result<BTreeMap<Topition, CommittedOffset>> {
            unimplemented!()
        }

        async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
            match self.listing {
                Listing::Error => Err(Error::Message("listing failed".into())),

                Listing::NamelessTopic => Ok(MetadataResponse {
                    cluster: None,
                    controller: None,
                    brokers: vec![],
                    topics: vec![
                        MetadataResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(None),
                    ],
                }),
            }
        }

        async fn describe_config(
            &self,
            _name: &str,
            _resource: ConfigResource,
            _keys: Option<&[String]>,
        ) -> Result<DescribeConfigsResult> {
            unimplemented!()
        }

        async fn describe_topic_partitions(
            &self,
            _topics: Option<&[TopicId]>,
            _partition_limit: i32,
            _cursor: Option<Topition>,
        ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
            unimplemented!()
        }

        async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
            unimplemented!()
        }

        async fn delete_groups(
            &self,
            _group_ids: Option<&[String]>,
        ) -> Result<Vec<DeletableGroupResult>> {
            unimplemented!()
        }

        async fn describe_groups(
            &self,
            _group_ids: Option<&[String]>,
            _include_authorized_operations: bool,
        ) -> Result<Vec<NamedGroupDetail>> {
            unimplemented!()
        }

        async fn group_with_version(
            &self,
            _group_id: &str,
        ) -> Result<Option<(GroupDetail, Version)>> {
            unimplemented!()
        }

        async fn update_group(
            &self,
            _group_id: &str,
            _detail: GroupDetail,
            _version: Option<Version>,
        ) -> Result<Version, UpdateError<GroupDetail>> {
            unimplemented!()
        }

        async fn init_producer(
            &self,
            _transaction_id: Option<&str>,
            _transaction_timeout_ms: i32,
            _producer_id: Option<i64>,
            _producer_epoch: Option<i16>,
        ) -> Result<ProducerIdResponse> {
            unimplemented!()
        }

        async fn txn_add_offsets(
            &self,
            _transaction_id: &str,
            _producer_id: i64,
            _producer_epoch: i16,
            _group_id: &str,
        ) -> Result<ErrorCode> {
            unimplemented!()
        }

        async fn txn_add_partitions(
            &self,
            _partitions: TxnAddPartitionsRequest,
        ) -> Result<TxnAddPartitionsResponse> {
            unimplemented!()
        }

        async fn txn_offset_commit(
            &self,
            _offsets: TxnOffsetCommitRequest,
        ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
            unimplemented!()
        }

        async fn txn_end(
            &self,
            _transaction_id: &str,
            _producer_id: i64,
            _producer_epoch: i16,
            _committed: bool,
        ) -> Result<ErrorCode> {
            unimplemented!()
        }

        async fn maintain(&self, _now: SystemTime) -> Result<()> {
            unimplemented!()
        }

        async fn maintain_transactions(&self, _now: SystemTime) -> Result<()> {
            unimplemented!()
        }

        async fn aborted_transactions(
            &self,
            _topition: &Topition,
            _offset: i64,
            _last_stable_offset: i64,
        ) -> Result<Vec<AbortedTransaction>> {
            unimplemented!()
        }

        async fn cluster_id(&self) -> Result<String> {
            unimplemented!()
        }

        async fn node(&self) -> Result<i32> {
            unimplemented!()
        }

        async fn advertised_listener(&self) -> Result<Url> {
            unimplemented!()
        }

        async fn ping(&self) -> Result<()> {
            unimplemented!()
        }

        async fn delete_user_scram_credential(
            &self,
            _user: &str,
            _mechanism: ScramMechanism,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn upsert_user_scram_credential(
            &self,
            _user: &str,
            _mechanism: ScramMechanism,
            _credential: ScramCredential,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn user_scram_credential(
            &self,
            _user: &str,
            _mechanism: ScramMechanism,
        ) -> Result<Option<ScramCredential>> {
            unimplemented!()
        }
    }

    fn topic(name: &str, num_partitions: i32) -> CreatableTopic {
        CreatableTopic::default()
            .name(name.into())
            .num_partitions(num_partitions)
            .replication_factor(1)
            .assignments(Some([].into()))
            .configs(Some([].into()))
    }

    /// The service fails a valid topic with a collision character instead of creating it
    /// unchecked when it cannot list the existing topics. A rejected topic keeps its own error,
    /// and a topic without a collision character does not need the listing.
    #[tokio::test]
    async fn failed_listing_fails_a_name_with_collision_chars() -> Result<()> {
        for listing in [Listing::Error, Listing::NamelessTopic] {
            let storage = FailedListing::new(listing);
            let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
                storage: storage.clone(),
            });

            let response = service
                .serve(
                    CreateTopicsRequest::default()
                        .topics(Some(vec![topic("a.b", 1), topic("a_b", 0), topic("ab", 1)]))
                        .validate_only(Some(false)),
                )
                .await?;

            let error_codes = response
                .topics
                .unwrap_or_default()
                .iter()
                .map(|topic| ErrorCode::try_from(topic.error_code))
                .collect::<Result<Vec<_>, _>>()?;

            assert_eq!(
                vec![
                    ErrorCode::UnknownServerError,
                    ErrorCode::InvalidPartitions,
                    ErrorCode::None,
                ],
                error_codes,
                "{listing:?}"
            );
            assert_eq!(vec![String::from("ab")], storage.created(), "{listing:?}");
        }

        Ok(())
    }
}
