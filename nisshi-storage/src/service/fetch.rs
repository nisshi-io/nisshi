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

use std::{cmp::min, time::SystemTime};

use nisshi_sans_io::{
    ApiKey, ErrorCode, FetchRequest, FetchResponse, IsolationLevel,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::{
        EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
    },
    metadata_response::MetadataResponseTopic,
    record::deflated::{Batch, Frame},
};
use rama::{Context, Service};
use tokio::time::{Duration, Instant, sleep};
use tracing::{debug, error, instrument};

use crate::{Error, Result, Storage, Topition};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`FetchRequest`] returning [`FetchResponse`].
/// ```
/// use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
/// use nisshi_sans_io::{
///     CreateTopicsRequest, ErrorCode, FetchRequest,
///     create_topics_request::CreatableTopic,
///     fetch_request::{FetchPartition, FetchTopic},
/// };
/// use nisshi_storage::{CreateTopicsService, Error, FetchService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const CLUSTER_ID: &str = "nisshi";
/// const NODE_ID: i32 = 111;
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
///
/// let storage = StorageContainer::builder()
///     .cluster_id(CLUSTER_ID)
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let create_topic = {
///     let storage = storage.clone();
///     MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
/// };
///
/// let name = "abcba";
///
/// let response = create_topic
///     .serve(
///         Context::default(),
///         CreateTopicsRequest::default()
///             .topics(Some(vec![
///                 CreatableTopic::default()
///                     .name(name.into())
///                     .num_partitions(5)
///                     .replication_factor(3)
///                     .assignments(Some([].into()))
///                     .configs(Some([].into())),
///             ]))
///             .validate_only(Some(false)),
///     )
///     .await?;
///
/// let topics = response.topics.unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
///
/// let fetch = {
///     let storage = storage.clone();
///     MapStateLayer::new(|_| storage).into_layer(FetchService)
/// };
///
/// let partition = 0;
///
/// let response = fetch
///     .serve(
///         Context::default(),
///         FetchRequest::default()
///             .topics(Some(
///                 [FetchTopic::default()
///                     .topic(Some(name.into()))
///                     .partitions(Some(
///                         [FetchPartition::default().partition(partition)].into(),
///                     ))]
///                 .into(),
///             ))
///             .max_bytes(Some(0))
///             .max_wait_ms(5_000),
///     )
///     .await?;
///
/// let topics = response.responses.as_deref().unwrap_or_default();
/// assert_eq!(1, topics.len());
/// let partitions = topics[0].partitions.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(partitions[0].error_code)?
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FetchService;

impl ApiKey for FetchService {
    const KEY: i16 = FetchRequest::KEY;
}

impl FetchService {
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self,ctx,min_bytes,isolation,fetch_partition), fields(partition = fetch_partition.partition))]
    async fn fetch_partition<G>(
        &self,
        ctx: &Context<G>,
        max_wait: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topic: &str,
        fetch_partition: &FetchPartition,
    ) -> Result<PartitionData>
    where
        G: Storage,
    {
        let started_at = Instant::now();

        let partition_index = fetch_partition.partition;
        let tp = Topition::new(topic, partition_index);

        let mut batches = Vec::new();

        let mut offset = fetch_partition.fetch_offset;

        loop {
            if *max_bytes == 0 {
                break;
            }

            debug!(offset);

            let mut fetched = ctx
                .state()
                .fetch(
                    &tp,
                    offset,
                    min_bytes,
                    *max_bytes,
                    isolation,
                    max_wait.saturating_sub(started_at.elapsed()),
                )
                .await
                .inspect(|r| debug!(?tp, ?offset, ?r))
                .inspect_err(|error| error!(?tp, ?error))?;

            *max_bytes =
                u32::try_from(fetched.byte_size()).map(|bytes| max_bytes.saturating_sub(bytes))?;

            debug!(?offset, ?fetched, max_bytes);

            if fetched.is_empty() || fetched.first().is_some_and(|batch| batch.record_count == 0) {
                break;
            }

            // the offset after the last one returned; a batch can have gaps
            // (compaction), so this is not the record count
            if let Some(latest) = fetched
                .iter()
                .map(|batch| batch.max_offset() + 1)
                .max()
                .inspect(|latest| debug!(latest))
            {
                offset = latest;
            }

            batches.append(&mut fetched);

            // max_wait bounds the response: engines that assemble batches
            // from rows stop at the deadline but return what they have, so
            // another round now would return one record per round trip
            // until max_bytes is spent
            if started_at.elapsed() >= max_wait {
                debug!(?offset, elapsed = ?started_at.elapsed(), ?max_wait);
                break;
            }
        }

        let offset_stage = ctx
            .state()
            .offset_stage(&tp)
            .await
            .inspect_err(|error| error!(?error, ?tp))?;

        Ok(PartitionData::default()
            .partition_index(partition_index)
            .error_code(ErrorCode::None.into())
            .high_watermark(offset_stage.high_watermark())
            .last_stable_offset(Some(offset_stage.last_stable()))
            .log_start_offset(Some(offset_stage.log_start()))
            .diverging_epoch(None)
            .current_leader(None)
            .snapshot_id(None)
            .aborted_transactions(Some([].into()))
            .preferred_read_replica(Some(-1))
            .records(if batches.is_empty() {
                None
            } else {
                Some(Frame { batches })
            }))
        .inspect(|r| debug!(?r, elapsed = ?started_at.elapsed()))
    }

    fn unknown_topic_response(&self, fetch: &FetchTopic) -> Result<FetchableTopicResponse> {
        Ok(FetchableTopicResponse::default()
            .topic(fetch.topic.clone())
            .topic_id(Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
            .partitions(fetch.partitions.as_ref().map(|partitions| {
                partitions
                    .iter()
                    .map(|partition| {
                        PartitionData::default()
                            .partition_index(partition.partition)
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .high_watermark(0)
                            .last_stable_offset(Some(0))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(Some(
                                EpochEndOffset::default().epoch(-1).end_offset(-1),
                            ))
                            .current_leader(Some(
                                LeaderIdAndEpoch::default().leader_id(0).leader_epoch(0),
                            ))
                            .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None)
                    })
                    .collect()
            })))
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, ctx, min_bytes, isolation, fetch))]
    async fn fetch_topic<G>(
        &self,
        ctx: &Context<G>,
        max_wait: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        fetch: &FetchTopic,
        is_first_non_empty: &mut bool,
    ) -> Result<FetchableTopicResponse>
    where
        G: Storage,
    {
        let started_at = Instant::now();

        let metadata = ctx.state().metadata(Some(&[fetch.into()])).await?;

        if let Some(MetadataResponseTopic {
            topic_id,
            name: Some(name),
            ..
        }) = metadata.topics().first()
        {
            let mut partitions = Vec::new();

            for fetch_partition in fetch.partitions.as_ref().unwrap_or(&Vec::new()) {
                let partition_max_bytes = if *is_first_non_empty {
                    *max_bytes
                } else {
                    min(fetch_partition.partition_max_bytes as u32, *max_bytes)
                };

                let mut partition_bytes = partition_max_bytes;
                debug!(partition_bytes, is_first_non_empty);

                let remaining = max_wait.saturating_sub(started_at.elapsed());

                let partition = self
                    .fetch_partition(
                        ctx,
                        remaining,
                        min_bytes,
                        &mut partition_bytes,
                        isolation,
                        name,
                        fetch_partition,
                    )
                    .await?;

                *is_first_non_empty = *is_first_non_empty
                    && partition
                        .records
                        .as_ref()
                        .is_some_and(|records| records.batches.is_empty());

                debug!(partition_bytes, is_first_non_empty);

                *max_bytes =
                    max_bytes.saturating_sub(partition_max_bytes.saturating_sub(partition_bytes));

                partitions.push(partition);
            }

            Ok(FetchableTopicResponse::default()
                .topic(fetch.topic.to_owned())
                .topic_id(topic_id.to_owned())
                .partitions(Some(partitions)))
        } else {
            self.unknown_topic_response(fetch)
        }
    }

    #[instrument(skip(self, ctx, isolation, topics))]
    pub(crate) async fn fetch<G>(
        &self,
        ctx: &Context<G>,
        max_wait: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topics: &[FetchTopic],
    ) -> Result<Vec<FetchableTopicResponse>>
    where
        G: Storage,
    {
        debug!(?isolation, ?topics);

        if topics.is_empty() {
            Ok(vec![])
        } else {
            let started_at = SystemTime::now();
            let mut responses = vec![];
            let mut iteration = 0;
            let mut bytes = 0;
            let mut is_first_non_empty = true;

            while !max_wait.saturating_sub(started_at.elapsed()?).is_zero() && bytes <= min_bytes {
                debug!(?bytes, remaining = ?max_wait.saturating_sub(started_at.elapsed()?));

                responses.clear();

                let fetch_started_at = SystemTime::now();
                for fetch in topics.iter() {
                    let fetch_response = self
                        .fetch_topic(
                            ctx,
                            max_wait.saturating_sub(started_at.elapsed()?),
                            min_bytes,
                            max_bytes,
                            isolation,
                            fetch,
                            &mut is_first_non_empty,
                        )
                        .await?;

                    responses.push(fetch_response);
                }

                bytes += u32::try_from(responses.byte_size())?;

                let remaining = max_wait.saturating_sub(started_at.elapsed()?);

                debug!(?iteration, ?max_wait, ?remaining, ?bytes, ?min_bytes);

                if bytes > min_bytes {
                    break;
                }

                {
                    let fetch_elapsed = fetch_started_at.elapsed()?;

                    // we have some data to return to the client,
                    // we haven't met the minimum size requirement,
                    // but we don't have enough (estimated) time remaining to do another round
                    if !responses.is_empty() && remaining < fetch_elapsed {
                        debug!(responses.len = responses.len(), ?remaining, ?fetch_elapsed);
                        break;
                    }
                }

                sleep(remaining / 2).await;

                iteration += 1;
            }

            Ok(responses)
        }
    }
}

impl<G> Service<G, FetchRequest> for FetchService
where
    G: Storage,
{
    type Response = FetchResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: FetchRequest,
    ) -> Result<Self::Response, Self::Error> {
        let started_at = SystemTime::now();

        let responses = Some(if let Some(topics) = req.topics {
            let isolation_level = req
                .isolation_level
                .map_or(Ok(IsolationLevel::ReadUncommitted), |isolation| {
                    IsolationLevel::try_from(isolation)
                })?;

            let max_wait_ms = u64::try_from(req.max_wait_ms).map(Duration::from_millis)?;

            let min_bytes = u32::try_from(req.min_bytes)?;

            const DEFAULT_MAX_BYTES: u32 = 5 * 1024 * 1024;

            let mut max_bytes = req.max_bytes.map_or(Ok(DEFAULT_MAX_BYTES), |max_bytes| {
                u32::try_from(max_bytes).map(|max_bytes| max_bytes.min(DEFAULT_MAX_BYTES))
            })?;

            self.fetch(
                &ctx,
                max_wait_ms,
                min_bytes,
                &mut max_bytes,
                isolation_level,
                topics.as_ref(),
            )
            .await?
        } else {
            vec![]
        });

        Ok(FetchResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(Some(ErrorCode::None.into()))
            .session_id(Some(0))
            .node_endpoints(Some([].into()))
            .responses(responses))
        .inspect(|r| debug!(?r, elapsed = ?started_at.elapsed().ok()))
    }
}

trait ByteSize {
    fn byte_size(&self) -> u64;
}

impl<T> ByteSize for Vec<T>
where
    T: ByteSize,
{
    fn byte_size(&self) -> u64 {
        self.iter().map(|item| item.byte_size()).sum()
    }
}

impl<T> ByteSize for Option<T>
where
    T: ByteSize,
{
    fn byte_size(&self) -> u64 {
        self.as_ref().map_or(0, |some| some.byte_size())
    }
}

impl ByteSize for Batch {
    fn byte_size(&self) -> u64 {
        self.record_data.len() as u64
    }
}

impl ByteSize for Frame {
    fn byte_size(&self) -> u64 {
        self.batches.byte_size()
    }
}

impl ByteSize for PartitionData {
    fn byte_size(&self) -> u64 {
        self.records.byte_size()
    }
}

impl ByteSize for FetchableTopicResponse {
    fn byte_size(&self) -> u64 {
        self.partitions.byte_size()
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
    use bytes::Bytes;
    use nisshi_sans_io::{
        ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
        create_topics_request::CreatableTopic,
        delete_groups_response::DeletableGroupResult,
        delete_records_request::DeleteRecordsTopic,
        delete_records_response::DeleteRecordsTopicResult,
        describe_cluster_response::DescribeClusterBroker,
        describe_configs_response::DescribeConfigsResult,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        fetch_request::FetchPartition,
        fetch_response::AbortedTransaction,
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
        list_groups_response::ListedGroup,
        record::{Record, deflated, inflated},
        txn_offset_commit_response::TxnOffsetCommitResponseTopic,
    };
    use rama::Context;
    use tokio::time::{Duration, advance};
    use url::Url;
    use uuid::Uuid;

    use super::FetchService;
    use crate::{
        BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
        NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result,
        ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest,
        TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError, Version,
    };

    /// A batch at `base_offset` holding one record per entry in `deltas`,
    /// each at that offset delta.
    fn batch(base_offset: i64, deltas: &[i32]) -> Result<deflated::Batch> {
        let mut builder = inflated::Batch::builder()
            .base_offset(base_offset)
            .last_offset_delta(deltas.last().copied().unwrap_or_default());

        for delta in deltas {
            builder = builder.record(
                Record::builder()
                    .offset_delta(*delta)
                    .value(Some(Bytes::from_static(b"foobarfoobarfoobarfoobar"))),
            );
        }

        builder
            .build()
            .and_then(deflated::Batch::try_from)
            .map_err(Into::into)
    }

    /// Storage whose `fetch` replays scripted responses, records the offset
    /// and remaining `max_wait` of each call, and optionally advances the
    /// (paused) clock past the deadline on the first call, as an engine
    /// that spends the whole budget assembling one truncated batch does.
    #[derive(Clone, Debug)]
    struct Scripted {
        responses: Arc<Mutex<Vec<Vec<deflated::Batch>>>>,
        calls: Arc<Mutex<Vec<(i64, Duration)>>>,
        consume_budget_on_first_call: Option<Duration>,
    }

    impl Scripted {
        fn calls(&self) -> Vec<(i64, Duration)> {
            self.calls.lock().expect("calls").clone()
        }
    }

    #[async_trait]
    impl Storage for Scripted {
        async fn fetch(
            &self,
            _topition: &Topition,
            offset: i64,
            _min_bytes: u32,
            _max_bytes: u32,
            _isolation_level: IsolationLevel,
            max_wait: Duration,
        ) -> Result<Vec<deflated::Batch>> {
            let calls = {
                let mut calls = self.calls.lock()?;
                calls.push((offset, max_wait));
                calls.len()
            };

            let response = {
                let mut responses = self.responses.lock()?;

                if responses.is_empty() {
                    return Err(Error::Message(format!(
                        "storage fetch called {calls} times, past the end of the script"
                    )));
                }

                responses.remove(0)
            };

            if calls == 1
                && let Some(budget) = self.consume_budget_on_first_call
            {
                advance(budget).await;
            }

            Ok(response)
        }

        async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
            Ok(OffsetStage {
                last_stable: 1_000,
                high_watermark: 1_000,
                log_start: 0,
            })
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

        async fn list_offsets(
            &self,
            _isolation_level: IsolationLevel,
            _offsets: &[(Topition, ListOffset)],
        ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
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

    async fn fetch_partition(
        storage: Scripted,
        max_wait: Duration,
        max_bytes: u32,
    ) -> Result<Vec<deflated::Batch>> {
        let ctx = Context::with_state(storage);
        let mut remaining = max_bytes;

        FetchService
            .fetch_partition(
                &ctx,
                max_wait,
                1,
                &mut remaining,
                IsolationLevel::ReadUncommitted,
                "abc",
                &FetchPartition::default()
                    .partition(0)
                    .fetch_offset(0)
                    .partition_max_bytes(max_bytes as i32),
            )
            .await
            .map(|partition| {
                partition
                    .records
                    .map(|frame| frame.batches)
                    .unwrap_or_default()
            })
    }

    /// Engines that rebuild batches from rows stop assembling at the
    /// deadline, but still return the record they were on, so after the
    /// deadline every storage call yields one record. Once the deadline
    /// has passed the loop must return what it has rather than issue a
    /// storage round trip per remaining record until `max_bytes` is spent.
    #[tokio::test(start_paused = true)]
    async fn stops_at_the_deadline() -> Result<()> {
        let max_wait = Duration::from_millis(100);

        // one truncated batch spends the whole budget, then a single record
        // per call forever
        let responses = (0..10_000)
            .map(|offset| batch(offset, &[0]).map(|batch| vec![batch]))
            .collect::<Result<Vec<_>>>()?;

        let storage = Scripted {
            responses: Arc::new(Mutex::new(responses)),
            calls: Arc::new(Mutex::new(vec![])),
            consume_budget_on_first_call: Some(max_wait),
        };

        let batches = fetch_partition(storage.clone(), max_wait, 1024 * 1024).await?;

        assert_eq!(1, batches.len());
        assert_eq!(0, batches[0].base_offset);
        assert_eq!(
            vec![(0, max_wait)],
            storage.calls(),
            "storage was called again after the deadline"
        );

        Ok(())
    }

    /// The next fetch offset follows the last offset in the batch, not its
    /// record count: compaction leaves gaps, and re-reading from inside a
    /// batch that has already been returned duplicates records.
    #[tokio::test(start_paused = true)]
    async fn next_offset_follows_the_last_offset_in_the_batch() -> Result<()> {
        let max_wait = Duration::from_millis(100);

        // records at offsets 0 and 3 (1 and 2 compacted away), then nothing
        let responses = vec![vec![batch(0, &[0, 3])?], vec![]];

        let storage = Scripted {
            responses: Arc::new(Mutex::new(responses)),
            calls: Arc::new(Mutex::new(vec![])),
            consume_budget_on_first_call: None,
        };

        let batches = fetch_partition(storage.clone(), max_wait, 1024 * 1024).await?;

        assert_eq!(1, batches.len());
        assert_eq!(
            vec![0, 4],
            storage
                .calls()
                .into_iter()
                .map(|(offset, _)| offset)
                .collect::<Vec<_>>()
        );

        Ok(())
    }
}
