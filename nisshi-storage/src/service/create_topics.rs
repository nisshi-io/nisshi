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
    ApiKey, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, NULL_TOPIC_ID, RequestInput,
    create_topics_response::CreatableTopicResult,
};
use rama::Service;
use tracing::{debug, instrument};

use crate::{Error, Result, Storage};

use super::metadata::is_valid_topic_name;

/// Build the [`CreatableTopicResult`] for a topic that was rejected, either
/// by storage or by validation performed before storage is ever consulted.
fn error_result(
    name: String,
    num_partitions: Option<i32>,
    replication_factor: Option<i16>,
    error_code: ErrorCode,
) -> CreatableTopicResult {
    CreatableTopicResult::default()
        .name(name)
        .topic_id(Some(NULL_TOPIC_ID))
        .error_code(error_code.into())
        .error_message(Some(error_code.to_string()))
        .topic_config_error_code(None)
        .num_partitions(num_partitions)
        .replication_factor(replication_factor)
        .configs(Some([].into()))
}

/// Kafka 3.9.1 rejects a whole `CreateTopics` request whose topics add up
/// to more than `MAX_PARTITIONS_PER_BATCH` (10,000) partitions, before any
/// per-topic validation runs, including the name check
/// ([ReplicationControlManager.java#L1152-L1169](https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L1152-L1169)).
///
/// nisshi counts partitions only. Kafka 3.9.1 also caps the metadata
/// records a request produces at 10,000 (one `TopicRecord` per topic, one
/// per config, one per partition), so a request for exactly 10,000
/// partitions fails there with `POLICY_VIOLATION`. nisshi has no metadata
/// records, so it deliberately allows exactly 10,000 partitions.
const MAX_PARTITIONS_PER_REQUEST: i64 = 10_000;

/// Partitions created for a topic whose `num_partitions` is `-1` (the
/// "use the broker default" sentinel). Shared between the per-topic
/// substitution in [`CreateTopicsService::serve`] and
/// [`total_requested_partitions`], which must count a `-1` topic the same
/// way the substitution below will.
const BROKER_DEFAULT_NUM_PARTITIONS: i32 = 3;

/// Sum of partitions this request would create, computed before any
/// per-topic validation runs, counted as Kafka 3.9.1 does: a topic with
/// manual `assignments` counts one partition per assignment, a topic
/// requesting the broker default (`-1`) counts as
/// [`BROKER_DEFAULT_NUM_PARTITIONS`]. A topic whose `num_partitions` is
/// otherwise invalid (`0`, or another negative value) counts as zero, so it
/// can't pull the total under the cap; the per-topic check in `serve`
/// rejects it on its own merits.
fn total_requested_partitions(
    topics: &[nisshi_sans_io::create_topics_request::CreatableTopic],
) -> i64 {
    topics
        .iter()
        .map(
            |topic| match (topic.assignments.as_deref(), topic.num_partitions) {
                (Some(assignments), _) if !assignments.is_empty() => {
                    i64::try_from(assignments.len()).unwrap_or(i64::MAX)
                }
                (_, -1) => i64::from(BROKER_DEFAULT_NUM_PARTITIONS),
                (_, n) if n > 0 => i64::from(n),
                _ => 0,
            },
        )
        .fold(0i64, i64::saturating_add)
}

/// A [`Service`] using its [`Storage`] taking [`CreateTopicsRequest`] returning [`CreateTopicsResponse`].
/// ```no_run
/// use rama::Service as _;
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
/// let service = CreateTopicsService { storage };
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

impl<G, I> Service<I> for CreateTopicsService<G>
where
    G: Storage,
    I: Into<RequestInput<CreateTopicsRequest>> + Send + 'static,
{
    type Output = CreateTopicsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let requested = input.request.topics.as_deref().unwrap_or(&[]);

        // Reject the whole request, before any per-topic validation or
        // storage call, when the partitions requested across every topic
        // exceed the cap. Kafka also runs this before the name check.
        let total = total_requested_partitions(requested);
        if total > MAX_PARTITIONS_PER_REQUEST {
            debug!(
                total,
                limit = MAX_PARTITIONS_PER_REQUEST,
                "rejecting CreateTopics: too many partitions in one request"
            );

            let message = format!(
                "Excessively large number of partitions per request: \
                 {total} requested, limit {MAX_PARTITIONS_PER_REQUEST}."
            );

            let topics = requested
                .iter()
                .map(|topic| {
                    error_result(
                        topic.name.clone(),
                        Some(topic.num_partitions),
                        Some(topic.replication_factor),
                        ErrorCode::PolicyViolation,
                    )
                    .error_message(Some(message.clone()))
                })
                .collect();

            return Ok(CreateTopicsResponse::default()
                .topics(Some(topics))
                .throttle_time_ms(Some(0)));
        }

        let mut topics = vec![];

        for mut topic in input.request.topics.unwrap_or_default() {
            let name = topic.name.clone();

            let num_partitions = Some(match topic.num_partitions {
                -1 => {
                    topic.num_partitions = BROKER_DEFAULT_NUM_PARTITIONS;
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

            // Validate before storage is ever consulted, so invalid topics
            // are rejected regardless of `validate_only`, and so a bad name
            // (e.g. "", ".", "..", or one containing "/") can never reach a
            // backend whose delete/list operations are prefix based.
            if !is_valid_topic_name(&name) {
                topics.push(error_result(
                    name,
                    num_partitions,
                    replication_factor,
                    ErrorCode::InvalidTopicException,
                ));
                continue;
            }

            // -1 (broker default) was already replaced above, so anything
            // below 1 is invalid.
            if num_partitions.is_some_and(|partitions| partitions < 1) {
                topics.push(error_result(
                    name,
                    num_partitions,
                    replication_factor,
                    ErrorCode::InvalidPartitions,
                ));
                continue;
            }

            match self
                .storage
                .create_topic(topic, input.request.validate_only.unwrap_or_default())
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
