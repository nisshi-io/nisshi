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

use std::{collections::BTreeSet, slice};

use futures::{StreamExt as _, stream};
use nisshi_sans_io::{
    ApiKey, ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest, ListOffsetsResponse,
    RequestInput,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
};
use rama::Service;
use tokio::time::Instant;
use tracing::{debug, error, instrument};

use crate::{
    Error, ListOffsetResponse, Result, Storage, Topition,
    service::deadline::{LIST_OFFSETS_READ_DEADLINE, within},
};

/// How many partitions of one ListOffsets request are read from storage at
/// once. Each read may hold a pooled database connection (16 for
/// PostgreSQL), and a read abandoned at the deadline can keep its
/// connection busy until the server finishes the statement.
const LIST_OFFSETS_CONCURRENCY: usize = 4;

/// A [`Service`] using its [`Storage`] taking [`ListOffsetsRequest`] returning [`ListOffsetsResponse`].
/// ```no_run
/// use rama::Service;
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
/// let service = ListOffsetsService { storage };
///
/// let topic = "abcba";
///
/// let response = service
///     .serve(
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
#[derive(Clone, Debug)]
pub struct ListOffsetsService<G> {
    pub storage: G,
}

impl<G> ApiKey for ListOffsetsService<G> {
    const KEY: i16 = ListOffsetsRequest::KEY;
}

impl<G> ListOffsetsService<G>
where
    G: Storage,
{
    /// Reads each partition's offset separately, a few at a time, each
    /// bounded by one deadline for the whole request. A partition still
    /// unanswered at the deadline gets `REQUEST_TIMED_OUT`, which clients
    /// retry, so one slow partition neither delays the others nor holds the
    /// request past the client's own timeout. A storage error still fails
    /// the whole request.
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let deadline = Instant::now() + LIST_OFFSETS_READ_DEADLINE;

        // the reads are built up front (but not started) rather than in a
        // `StreamExt::map` closure, whose lifetimes keep `serve` from being `Send`
        let reads = offsets
            .iter()
            .enumerate()
            .map(|(index, offset)| async move {
                let answer = within(
                    "list_offsets",
                    deadline,
                    self.storage
                        .list_offsets(isolation_level, slice::from_ref(offset)),
                )
                .await;

                (index, offset, answer)
            })
            .collect::<Vec<_>>();

        let mut answers = stream::iter(reads).buffer_unordered(LIST_OFFSETS_CONCURRENCY);

        let mut responses = vec![Vec::new(); offsets.len()];

        while let Some((index, (topition, _), answer)) = answers.next().await {
            responses[index] = match answer {
                Some(answer) => answer?,

                None => {
                    debug!(?topition, "storage read past the deadline");

                    vec![(
                        topition.clone(),
                        ListOffsetResponse {
                            error_code: ErrorCode::RequestTimedOut,
                            timestamp: None,
                            offset: Some(-1),
                        },
                    )]
                }
            };
        }

        Ok(responses.into_iter().flatten().collect())
    }
}

impl<G, I> Service<I> for ListOffsetsService<G>
where
    G: Storage,
    I: Into<RequestInput<ListOffsetsRequest>> + Send + 'static,
{
    type Output = ListOffsetsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();
        let throttle_time_ms = Some(0);

        let isolation_level = input
            .request
            .isolation_level
            .map_or(Ok(IsolationLevel::ReadUncommitted), |isolation_level| {
                IsolationLevel::try_from(isolation_level)
            })?;

        let topics = if let Some(topics) = input.request.topics {
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

            self.list_offsets(isolation_level, &offsets)
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

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        future::pending,
        sync::{Arc, Mutex},
        time::SystemTime,
    };

    use async_trait::async_trait;
    use nisshi_sans_io::{
        ConfigResource, ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest, ScramMechanism,
        create_topics_request::CreatableTopic,
        delete_groups_response::DeletableGroupResult,
        delete_records_request::DeleteRecordsTopic,
        delete_records_response::DeleteRecordsTopicResult,
        describe_cluster_response::DescribeClusterBroker,
        describe_configs_response::DescribeConfigsResult,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        fetch_response::AbortedTransaction,
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
        list_groups_response::ListedGroup,
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        list_offsets_response::ListOffsetsPartitionResponse,
        record::deflated,
        txn_offset_commit_response::TxnOffsetCommitResponseTopic,
    };
    use rama::Service as _;
    use tokio::time::{Duration, Instant, timeout};
    use url::Url;
    use uuid::Uuid;

    use super::{LIST_OFFSETS_CONCURRENCY, ListOffsetsService};
    use crate::{
        BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
        NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result,
        ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest,
        TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError, Version,
        service::deadline::LIST_OFFSETS_READ_DEADLINE,
    };

    const TOPIC: &str = "abc";

    /// Fails a test that would otherwise hang on a storage read that never
    /// finishes, as every stalled read did before the deadline.
    const HANG: Duration = Duration::from_secs(60);

    #[derive(Clone, Copy, Debug)]
    enum Answer {
        Offset(i64),
        Stall,
        Fail,
    }

    /// Records the partition of a stalled storage read when nisshi drops it.
    #[derive(Debug)]
    struct Dropped {
        partition: i32,
        dropped: Arc<Mutex<Vec<i32>>>,
    }

    impl Drop for Dropped {
        fn drop(&mut self) {
            if let Ok(mut dropped) = self.dropped.lock() {
                dropped.push(self.partition);
            }
        }
    }

    /// Storage answering `list_offsets` per partition: an offset, a read that
    /// never finishes, or an error. Records each call and each dropped read.
    #[derive(Clone, Debug, Default)]
    struct Stub {
        partitions: BTreeMap<i32, Answer>,
        calls: Arc<Mutex<Vec<i32>>>,
        dropped: Arc<Mutex<Vec<i32>>>,
    }

    impl Stub {
        fn new(answers: &[Answer]) -> Self {
            Self {
                partitions: (0..).zip(answers.iter().copied()).collect(),
                ..Self::default()
            }
        }

        fn calls(&self) -> Vec<i32> {
            let mut calls = self.calls.lock().expect("calls").clone();
            calls.sort_unstable();
            calls
        }

        fn dropped(&self) -> Vec<i32> {
            let mut dropped = self.dropped.lock().expect("dropped").clone();
            dropped.sort_unstable();
            dropped
        }
    }

    #[async_trait]
    impl Storage for Stub {
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
            offsets: &[(Topition, ListOffset)],
        ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
            let [(topition, _)] = offsets else {
                return Err(Error::Message(format!(
                    "expected one partition per storage call, got {offsets:?}"
                )));
            };

            let partition = topition.partition();
            self.calls.lock()?.push(partition);

            match self.partitions.get(&partition) {
                Some(Answer::Offset(offset)) => Ok(vec![(
                    topition.clone(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        timestamp: None,
                        offset: Some(*offset),
                    },
                )]),

                Some(Answer::Stall) => {
                    let _guard = Dropped {
                        partition,
                        dropped: self.dropped.clone(),
                    };

                    pending().await
                }

                Some(Answer::Fail) | None => Err(Error::Message(format!(
                    "storage failed for partition {partition}"
                ))),
            }
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

        async fn create_topic(&self, _topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
            unimplemented!()
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
        ) -> Result<BTreeMap<Topition, i64>> {
            unimplemented!()
        }

        async fn offset_fetch(
            &self,
            _group_id: Option<&str>,
            _topics: &[Topition],
            _require_stable: Option<bool>,
        ) -> Result<BTreeMap<Topition, i64>> {
            unimplemented!()
        }

        async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
            unimplemented!()
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

        async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
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

    /// Asks for the latest offset of `partitions` partitions of one topic.
    async fn list_offsets(
        storage: Stub,
        partitions: i32,
    ) -> Result<Vec<ListOffsetsPartitionResponse>> {
        let request = ListOffsetsRequest::default()
            .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
            .replica_id(-1)
            .topics(Some(vec![
                ListOffsetsTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(
                        (0..partitions)
                            .map(|partition| {
                                ListOffsetsPartition::default()
                                    .partition_index(partition)
                                    .current_leader_epoch(Some(-1))
                                    .timestamp(ListOffset::Latest.try_into().expect("latest"))
                            })
                            .collect(),
                    )),
            ]));

        let response = timeout(HANG, ListOffsetsService { storage }.serve(request))
            .await
            .map_err(|_| Error::Message("list offsets hung on a stalled read".into()))??;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        Ok(topics[0].partitions.clone().unwrap_or_default())
    }

    fn answers(
        partitions: &[ListOffsetsPartitionResponse],
    ) -> Result<Vec<(i32, ErrorCode, Option<i64>)>> {
        partitions
            .iter()
            .map(|partition| {
                ErrorCode::try_from(partition.error_code)
                    .map(|error_code| (partition.partition_index, error_code, partition.offset))
                    .map_err(Into::into)
            })
            .collect()
    }

    /// A partition that is slow in storage answers `REQUEST_TIMED_OUT` at
    /// the deadline, and the partitions either side of it get their offsets.
    #[tokio::test(start_paused = true)]
    async fn slow_partition_times_out_alone() -> Result<()> {
        let storage = Stub::new(&[Answer::Offset(10), Answer::Stall, Answer::Offset(30)]);

        let started_at = Instant::now();
        let partitions = list_offsets(storage.clone(), 3).await?;

        assert_eq!(LIST_OFFSETS_READ_DEADLINE, started_at.elapsed());
        assert_eq!(
            vec![
                (0, ErrorCode::None, Some(10)),
                (1, ErrorCode::RequestTimedOut, Some(-1)),
                (2, ErrorCode::None, Some(30)),
            ],
            answers(&partitions)?
        );
        assert_eq!(vec![0, 1, 2], storage.calls());
        assert_eq!(vec![1], storage.dropped());

        Ok(())
    }

    /// Without a slow partition, every partition is answered straight away,
    /// in the order asked.
    #[tokio::test(start_paused = true)]
    async fn answers_in_request_order() -> Result<()> {
        let offsets = [50, 40, 30, 20, 10, 0];
        let storage = Stub::new(&offsets.map(Answer::Offset));

        let started_at = Instant::now();
        let partitions = list_offsets(storage.clone(), 6).await?;

        assert_eq!(Duration::ZERO, started_at.elapsed());
        assert_eq!(
            (0..)
                .zip(offsets)
                .map(|(partition, offset)| (partition, ErrorCode::None, Some(offset)))
                .collect::<Vec<_>>(),
            answers(&partitions)?
        );

        Ok(())
    }

    /// At most `LIST_OFFSETS_CONCURRENCY` reads run at once, and a read not
    /// started by the deadline is never sent to storage.
    #[tokio::test(start_paused = true)]
    async fn bounded_and_not_started_after_the_deadline() -> Result<()> {
        let partitions = i32::try_from(LIST_OFFSETS_CONCURRENCY + 2)?;
        let storage = Stub::new(&vec![Answer::Stall; partitions as usize]);

        let started_at = Instant::now();
        let answered = list_offsets(storage.clone(), partitions).await?;

        assert_eq!(LIST_OFFSETS_READ_DEADLINE, started_at.elapsed());
        assert!(
            answers(&answered)?
                .iter()
                .all(|(_, error_code, _)| *error_code == ErrorCode::RequestTimedOut)
        );
        assert_eq!(
            (0..i32::try_from(LIST_OFFSETS_CONCURRENCY)?).collect::<Vec<_>>(),
            storage.calls()
        );
        assert_eq!(storage.calls(), storage.dropped());

        Ok(())
    }

    /// A storage error still fails the whole request, dropping the reads
    /// still in flight without reading any partition twice.
    #[tokio::test(start_paused = true)]
    async fn storage_error_fails_the_request() -> Result<()> {
        let storage = Stub::new(&[Answer::Stall, Answer::Fail, Answer::Offset(30)]);

        let outcome = list_offsets(storage.clone(), 3).await;

        assert!(
            matches!(outcome, Err(Error::Message(ref message)) if message.contains("partition 1")),
            "{outcome:?}"
        );
        assert_eq!(vec![0], storage.dropped());

        let calls = storage.calls();
        assert!(calls.starts_with(&[0, 1]), "{calls:?}");
        assert!(calls.len() <= 3, "{calls:?}");

        Ok(())
    }
}
