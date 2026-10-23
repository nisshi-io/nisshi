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

use std::fmt::{self, Debug};

use nisshi_sans_io::{
    CreateTopicsRequest, CreateTopicsResponse, ErrorCode, NULL_TOPIC_ID, RequestInput,
    create_topics_request::CreatableTopic, create_topics_response::CreatableTopicResult,
    topic::is_valid_topic_name,
};
use rama::{Layer, Service, extensions::Extensions};
use tracing::{debug, instrument, warn};

use crate::{Error, MAX_REPLICATION_FACTOR, Result, max_message_bytes};

/// Build the [`CreatableTopicResult`] for a topic that was rejected, either
/// by storage or by validation performed before storage is ever consulted.
pub(super) fn error_result(
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
/// substitution in [`ValidatedCreateTopicsRequest::try_from`] and
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
fn total_requested_partitions(topics: &[CreatableTopic]) -> i64 {
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

/// A topic of a [`CreateTopicsRequest`] after validation: either ready for storage, with the
/// broker defaults for `-1` already substituted, or already answered.
#[derive(Clone, Debug)]
pub(super) enum ValidatedTopic {
    Valid(CreatableTopic),
    Rejected(CreatableTopicResult),
}

/// A [`CreateTopicsRequest`] whose topics have each been validated.
///
/// [`CreateTopicsService`](super::CreateTopicsService) takes this instead of a
/// [`CreateTopicsRequest`], so nothing reaches storage unchecked.
#[derive(Clone, Debug)]
pub struct ValidatedCreateTopicsRequest {
    pub(super) validate_only: bool,
    pub(super) topics: Vec<ValidatedTopic>,
    /// The request's extensions, carried so a service behind the layer sees the same
    /// per-connection state (e.g. the authenticated principal) as one without it.
    #[allow(dead_code)]
    pub(super) extensions: Extensions,
}

/// A [`CreateTopicsRequest`] rejected as a whole: its topics add up to more partitions than a
/// single request may create.
#[derive(Clone, Debug)]
pub struct MalformedCreateTopicsRequest {
    total: i64,
    topics: Vec<CreatableTopic>,
}

impl MalformedCreateTopicsRequest {
    /// Builds the [`CreateTopicsResponse`] reporting `POLICY_VIOLATION` for every topic.
    pub fn into_response(self) -> CreateTopicsResponse {
        let message = format!(
            "Excessively large number of partitions per request: \
             {} requested, limit {MAX_PARTITIONS_PER_REQUEST}.",
            self.total
        );

        let topics = self
            .topics
            .into_iter()
            .map(|topic| {
                error_result(
                    topic.name,
                    Some(topic.num_partitions),
                    Some(topic.replication_factor),
                    ErrorCode::PolicyViolation,
                )
                .error_message(Some(message.clone()))
            })
            .collect();

        CreateTopicsResponse::default()
            .topics(Some(topics))
            .throttle_time_ms(Some(0))
    }
}

impl TryFrom<CreateTopicsRequest> for ValidatedCreateTopicsRequest {
    type Error = MalformedCreateTopicsRequest;

    fn try_from(request: CreateTopicsRequest) -> Result<Self, Self::Error> {
        let requested = request.topics.unwrap_or_default();

        // Reject the whole request, before any per-topic validation, when the partitions
        // requested across every topic exceed the cap. Kafka also runs this before the name
        // check.
        let total = total_requested_partitions(&requested);
        if total > MAX_PARTITIONS_PER_REQUEST {
            return Err(MalformedCreateTopicsRequest {
                total,
                topics: requested,
            });
        }

        Ok(Self {
            validate_only: request.validate_only.unwrap_or_default(),
            topics: requested.into_iter().map(validate_topic).collect(),
            extensions: Extensions::default(),
        })
    }
}

/// Validates one topic, in the order Kafka's KRaft controller does
/// ([ReplicationControlManager.java](https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java)),
/// so the invalid ones are answered whether or not `validate_only` is set, and a bad name
/// (e.g. "", ".", "..", or one containing "/") can never reach a backend whose delete/list
/// operations are prefix based.
fn validate_topic(mut topic: CreatableTopic) -> ValidatedTopic {
    let name = topic.name.clone();

    let explicit_partitions = topic.num_partitions != -1;
    let explicit_replication_factor = topic.replication_factor != -1;

    // A topic with manual assignments has one partition per assignment, which is also how the
    // partition cap counts it. Storage creates `num_partitions` partitions and ignores the
    // assignments, so the count has to come from them, not the broker default.
    let assigned_partitions = topic
        .assignments
        .as_deref()
        .filter(|assignments| !assignments.is_empty())
        .map(|assignments| i32::try_from(assignments.len()).unwrap_or(i32::MAX));

    let num_partitions = Some(match topic.num_partitions {
        -1 => {
            topic.num_partitions = assigned_partitions.unwrap_or(BROKER_DEFAULT_NUM_PARTITIONS);
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

    let reject = |error_code| {
        ValidatedTopic::Rejected(error_result(
            name.clone(),
            num_partitions,
            replication_factor,
            error_code,
        ))
    };

    if !is_valid_topic_name(&name) {
        return reject(ErrorCode::InvalidTopicException);
    }

    // Manual assignments fix the partitions and replicas, so the counts must be left at
    // `-1`. Storage creates `num_partitions` partitions, so an explicit count beside an
    // assignment would also bypass the partition cap, which counts the assignments instead.
    // The replication factor is checked first, with the messages Kafka's KRaft controller
    // sends, so the client is told which field to fix.
    if topic.assignments.as_deref().is_some_and(|a| !a.is_empty()) {
        let message = if explicit_replication_factor {
            Some(
                "A manual partition assignment was specified, but replication factor was not set to -1.",
            )
        } else if explicit_partitions {
            Some(
                "A manual partition assignment was specified, but numPartitions was not set to -1.",
            )
        } else {
            None
        };

        if let Some(message) = message {
            warn!(topic = %name, message, "rejecting topic, answering INVALID_REQUEST");

            return ValidatedTopic::Rejected(
                error_result(
                    name.clone(),
                    num_partitions,
                    replication_factor,
                    ErrorCode::InvalidRequest,
                )
                .error_message(Some(message.into())),
            );
        }
    }

    // Replication factor is checked before partition count, matching Kafka's KRaft
    // controller (ReplicationControlManager), the only mode from Kafka 4.0. -1 (broker
    // default) was already replaced above, so anything below 1 is invalid, as is anything
    // above `MAX_REPLICATION_FACTOR`.
    if let Some(factor) =
        replication_factor.filter(|factor| !(1..=MAX_REPLICATION_FACTOR).contains(factor))
    {
        debug!(
            topic = %name,
            replication_factor = factor,
            limit = MAX_REPLICATION_FACTOR,
            "rejecting CreateTopics: invalid replication factor"
        );

        // The generic text for this error code blames the number of available brokers, which
        // is wrong here, because the factor can exceed the broker count. The message below 1
        // is Kafka's own.
        let message = if factor < 1 {
            String::from(
                "Replication factor must be larger than 0, or -1 to use the default value.",
            )
        } else {
            format!("Replication factor {factor} is above the maximum of {MAX_REPLICATION_FACTOR}.")
        };

        return ValidatedTopic::Rejected(
            error_result(
                name,
                num_partitions,
                replication_factor,
                ErrorCode::InvalidReplicationFactor,
            )
            .error_message(Some(message)),
        );
    }

    if num_partitions.is_some_and(|partitions| partitions < 1) {
        return reject(ErrorCode::InvalidPartitions);
    }

    let max_message_bytes_rejection = topic
        .configs
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|config| config.name == max_message_bytes::KEY)
        .filter_map(|config| config.value.as_deref())
        .find_map(|configured| max_message_bytes::parse(configured).err());

    if let Some(reason) = max_message_bytes_rejection {
        debug!(topic = %name, reason, "rejecting CreateTopics: invalid max.message.bytes");

        return ValidatedTopic::Rejected(
            error_result(
                name,
                num_partitions,
                replication_factor,
                ErrorCode::InvalidConfig,
            )
            .error_message(Some(reason)),
        );
    }

    ValidatedTopic::Valid(topic)
}

/// A [`Layer`] converting a [`CreateTopicsRequest`] into a [`ValidatedCreateTopicsRequest`] for
/// the wrapped [`Service`], and answering a request rejected as a whole itself.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CreateTopicsValidationLayer;

impl CreateTopicsValidationLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for CreateTopicsValidationLayer {
    type Service = CreateTopicsValidationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CreateTopicsValidationService { inner }
    }
}

/// A [`Service`] passing a [`ValidatedCreateTopicsRequest`] to `inner`, or answering
/// `POLICY_VIOLATION` for a [`CreateTopicsRequest`] asking for too many partitions.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CreateTopicsValidationService<S> {
    inner: S,
}

impl<S> Debug for CreateTopicsValidationService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(CreateTopicsValidationService))
            .finish()
    }
}

impl<S, I> Service<I> for CreateTopicsValidationService<S>
where
    S: Service<ValidatedCreateTopicsRequest, Output = CreateTopicsResponse, Error = Error>,
    I: Into<RequestInput<CreateTopicsRequest>> + Send + 'static,
{
    type Output = CreateTopicsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output> {
        let RequestInput {
            request,
            extensions,
        } = input.into();

        match ValidatedCreateTopicsRequest::try_from(request) {
            Ok(request) => {
                self.inner
                    .serve(ValidatedCreateTopicsRequest {
                        extensions,
                        ..request
                    })
                    .await
            }

            Err(malformed) => {
                debug!(
                    total = malformed.total,
                    "too many partitions in one request"
                );
                Ok(malformed.into_response())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use nisshi_sans_io::{
        CreateTopicsRequest, CreateTopicsResponse, ErrorCode, RequestInput,
        create_topics_request::{CreatableReplicaAssignment, CreatableTopic},
    };
    use rama::{Layer as _, Service as _, extensions::Extensions};

    use super::{CreateTopicsValidationLayer, ValidatedCreateTopicsRequest, ValidatedTopic};
    use crate::{Error, Result};

    /// Records how many of the topics it was given were valid.
    #[derive(Clone, Debug, Default)]
    struct CountingInner {
        valid: Arc<AtomicUsize>,
        calls: Arc<AtomicUsize>,
    }

    impl rama::Service<ValidatedCreateTopicsRequest> for CountingInner {
        type Output = CreateTopicsResponse;
        type Error = Error;

        async fn serve(&self, input: ValidatedCreateTopicsRequest) -> Result<Self::Output> {
            _ = self.calls.fetch_add(1, Ordering::SeqCst);
            _ = self.valid.fetch_add(
                input
                    .topics
                    .iter()
                    .filter(|topic| matches!(topic, ValidatedTopic::Valid(_)))
                    .count(),
                Ordering::SeqCst,
            );

            Ok(CreateTopicsResponse::default().topics(Some([].into())))
        }
    }

    fn topic(
        name: &str,
        num_partitions: i32,
        replication_factor: i16,
        assignments: usize,
    ) -> CreatableTopic {
        CreatableTopic::default()
            .name(name.into())
            .num_partitions(num_partitions)
            .replication_factor(replication_factor)
            .assignments(Some(
                (0..assignments)
                    .map(|partition_index| {
                        CreatableReplicaAssignment::default()
                            .partition_index(i32::try_from(partition_index).expect("small"))
                            .broker_ids(Some(vec![111]))
                    })
                    .collect(),
            ))
            .configs(Some([].into()))
    }

    async fn serve(topics: Vec<CreatableTopic>) -> Result<(CreateTopicsResponse, CountingInner)> {
        let inner = CountingInner::default();

        CreateTopicsValidationLayer::new()
            .layer(inner.clone())
            .serve(RequestInput {
                request: CreateTopicsRequest::default().topics(Some(topics)),
                extensions: Extensions::default(),
            })
            .await
            .map(|response| (response, inner))
    }

    /// A topic with manual assignments must leave both counts at `-1`. Storage creates
    /// `num_partitions` partitions, while the request cap counts the assignments, so an
    /// explicit count beside one assignment would let a single request ask storage for
    /// millions of partitions (`fuzz_create_topics_storage` found one at 9,667,468).
    #[test]
    fn assignments_with_an_explicit_count_are_rejected() {
        for (num_partitions, replication_factor) in
            [(9_667_468, -1), (-1, 3), (3, 3), (i32::MAX, i16::MAX)]
        {
            let validated =
                ValidatedCreateTopicsRequest::try_from(CreateTopicsRequest::default().topics(
                    Some(vec![topic("a", num_partitions, replication_factor, 1)]),
                ))
                .expect("one assignment is under the cap");

            let [ValidatedTopic::Rejected(result)] = &validated.topics[..] else {
                panic!("{num_partitions}/{replication_factor} should be rejected");
            };

            assert_eq!(
                ErrorCode::InvalidRequest,
                ErrorCode::try_from(result.error_code).expect("error code"),
                "{num_partitions}/{replication_factor}"
            );

            // Kafka checks the replication factor first, so its message wins when both are set.
            let expected = if replication_factor != -1 {
                "A manual partition assignment was specified, but replication factor was not set to -1."
            } else {
                "A manual partition assignment was specified, but numPartitions was not set to -1."
            };

            assert_eq!(Some(expected), result.error_message.as_deref());
        }
    }

    #[test]
    fn assignments_with_default_counts_are_valid() {
        let validated = ValidatedCreateTopicsRequest::try_from(
            CreateTopicsRequest::default().topics(Some(vec![topic("a", -1, -1, 2)])),
        )
        .expect("two assignments are under the cap");

        assert!(matches!(&validated.topics[..], [ValidatedTopic::Valid(_)]));
    }

    /// Storage creates `num_partitions` partitions and ignores the assignments, so a topic
    /// with six assignments must reach it asking for six partitions, not the default three.
    #[test]
    fn assignments_set_the_partition_count() {
        let validated = ValidatedCreateTopicsRequest::try_from(
            CreateTopicsRequest::default().topics(Some(vec![topic("a", -1, -1, 6)])),
        )
        .expect("six assignments are under the cap");

        let [ValidatedTopic::Valid(topic)] = &validated.topics[..] else {
            panic!("a topic with assignments and default counts is valid");
        };

        assert_eq!(6, topic.num_partitions);
        assert_eq!(1, topic.replication_factor);
    }

    #[test]
    fn broker_defaults_are_substituted() {
        let validated = ValidatedCreateTopicsRequest::try_from(
            CreateTopicsRequest::default().topics(Some(vec![topic("a", -1, -1, 0)])),
        )
        .expect("under the cap");

        let [ValidatedTopic::Valid(topic)] = &validated.topics[..] else {
            panic!("a default topic is valid");
        };

        assert_eq!(super::BROKER_DEFAULT_NUM_PARTITIONS, topic.num_partitions);
        assert_eq!(1, topic.replication_factor);
    }

    /// A rejected topic is answered on its own: the valid topic beside it still reaches the
    /// inner service.
    #[tokio::test]
    async fn one_invalid_topic_does_not_stop_the_others() -> Result<()> {
        let (_, inner) =
            serve(vec![topic("good", 1, 1, 0), topic("bad", 9_667_468, -1, 1)]).await?;

        assert_eq!(1, inner.calls.load(Ordering::SeqCst));
        assert_eq!(1, inner.valid.load(Ordering::SeqCst));

        Ok(())
    }

    /// Over the cap the whole request is answered `POLICY_VIOLATION` without calling the inner
    /// service.
    #[tokio::test]
    async fn too_many_partitions_is_policy_violation_without_calling_inner() -> Result<()> {
        let (response, inner) = serve(vec![topic("a", 10_000, 1, 0), topic("b", 1, 1, 0)]).await?;

        assert_eq!(0, inner.calls.load(Ordering::SeqCst));

        let topics = response.topics.expect("topics");
        assert_eq!(2, topics.len());

        for topic in topics {
            assert_eq!(
                ErrorCode::PolicyViolation,
                ErrorCode::try_from(topic.error_code)?
            );
        }

        Ok(())
    }
}
