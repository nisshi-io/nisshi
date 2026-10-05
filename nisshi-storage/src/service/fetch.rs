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

use std::{cmp::min, future::Future, sync::LazyLock};

use nisshi_sans_io::{
    ApiKey, ErrorCode, FetchRequest, FetchResponse, IsolationLevel, RequestInput,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::{EpochEndOffset, FetchableTopicResponse, PartitionData, SnapshotId},
    metadata_response::MetadataResponseTopic,
    record::deflated::{Batch, Frame},
};
use opentelemetry::{KeyValue, metrics::Counter};
use rama::Service;
use tokio::time::{Duration, Instant, sleep, timeout_at};
use tracing::{debug, error, instrument, warn};

use crate::{Error, METER, OffsetStage, Result, Storage, Topition};

/// Partition reads whose fetch offset falls outside the partition, by
/// `topic` and by `bound`: `below_log_start` (answered with
/// `OFFSET_OUT_OF_RANGE`, once per Fetch) or `above_high_watermark`
/// (answered with `NONE` and no records). The second does not end the wait
/// for `max_wait`, so one Fetch counts once per poll round: its rate follows
/// storage latency, not the number of parked consumers, and on dynostore
/// with several brokers it also counts, for the life of a cached high
/// watermark, consumers that are only ahead of a stale one.
static FETCH_OFFSET_OUT_OF_BOUNDS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_fetch_offset_out_of_bounds")
        .with_description(
            "Partition reads whose fetch offset is below the log start (once per Fetch) or above \
             the high watermark (once per poll round: shows only whether parked fetches exist; \
             a short burst on multi-broker dynostore is expected)",
        )
        .build()
});

/// How long past the client's `max_wait` a Fetch may spend reading storage.
///
/// A client abandons a Fetch at its own read deadline, then sends it again
/// while the abandoned one keeps running:
///
/// | Client     | Fetch read deadline            |
/// |------------|--------------------------------|
/// | Java       | 30s (`max_wait` not added)     |
/// | librdkafka | 60s + `fetch.wait.max.ms`      |
/// | franz-go   | 10s + `max_wait`               |
///
/// `max_wait` plus this sits under all three with room for the response,
/// for any `max_wait` under Java's 30s less this.
const READ_DEADLINE_OVERHEAD: Duration = Duration::from_secs(5);

/// Runs `read` until `deadline`, returning `None` if it did not finish.
///
/// A read is not started at all once the deadline has passed:
/// [`tokio::time::timeout_at`] polls its future once before looking at the
/// clock, which would send a storage request only to drop it.
///
/// Dropping the read cancels nisshi's side of it. Work the storage engine
/// runs on its own tasks, or a statement already sent to a database server,
/// carries on until it finishes.
async fn before<F>(deadline: Instant, read: F) -> Option<F::Output>
where
    F: Future,
{
    if Instant::now() >= deadline {
        None
    } else {
        timeout_at(deadline, read).await.ok()
    }
}

/// When the next partition's reads must finish, with `left` partitions,
/// this one included, still to read before `read_deadline`.
///
/// Each partition may spend half of the time left, and the last all of it,
/// so one partition that never answers delays the rest of the request by
/// that half, rather than holding it until the client gives up. The half is
/// taken of the time left now, so a partition that answers quickly leaves
/// its unused share to those after it.
fn partition_deadline(read_deadline: Instant, left: usize) -> Instant {
    if left <= 1 {
        read_deadline
    } else {
        let now = Instant::now();
        now + read_deadline.saturating_duration_since(now) / 2
    }
}

/// The deadlines of one Fetch request.
#[derive(Clone, Copy, Debug)]
struct Deadlines {
    /// The client's `max_wait`.
    max_wait: Duration,

    /// When the client's `max_wait` ends.
    client: Instant,

    /// When every storage read must have finished.
    read: Instant,
}

impl Deadlines {
    fn new(started_at: Instant, max_wait: Duration) -> Self {
        Self {
            max_wait,
            client: started_at + max_wait,
            read: started_at + max_wait + READ_DEADLINE_OVERHEAD,
        }
    }

    /// The storage budget of a partition whose reads start now and must
    /// finish by `cap`.
    ///
    /// The client's `max_wait` bounds the response: engines stop assembling
    /// batches at their budget and return what they have. A partition that
    /// starts with less than half of `max_wait` left, because one before it
    /// was slow, still gets that half, rather than a budget engines would
    /// read nothing with.
    fn storage(&self, cap: Instant) -> Instant {
        self.client.max(Instant::now() + self.max_wait / 2).min(cap)
    }
}

/// A [`Service`] using its [`Storage`] taking [`FetchRequest`] returning [`FetchResponse`].
/// ```no_run
/// use rama::Service as _;
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
/// let create_topic = CreateTopicsService {
///     storage: storage.clone(),
/// };
///
/// let name = "abcba";
///
/// let response = create_topic
///     .serve(
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
/// let fetch = FetchService {
///     storage: storage.clone(),
/// };
///
/// let partition = 0;
///
/// let response = fetch
///     .serve(
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
#[derive(Clone, Debug)]
pub struct FetchService<G> {
    pub storage: G,
}

impl<G> ApiKey for FetchService<G> {
    const KEY: i16 = FetchRequest::KEY;
}

impl<G> FetchService<G>
where
    G: Storage,
{
    /// Reads one partition, with `left` partitions, this one included, still
    /// to read before the read deadline.
    ///
    /// A read that has not finished by this partition's share of the read
    /// deadline (see [`partition_deadline`]) is abandoned. Each read is
    /// given the time left of its storage budget (see [`Deadlines::storage`])
    /// and the reads stop once that is spent. What is left of the client's
    /// `max_wait` alone may have been spent by a slow partition earlier in
    /// the request, and engines read nothing with no budget.
    ///
    /// A partition whose read does not finish in time is answered with
    /// [`ErrorCode::None`], whatever batches it had already read, and the
    /// offset stage read before them, so the client fetches it again on its
    /// next request. A partition whose offset stage is not read in time is
    /// answered with unknown offsets; see [`Self::unknown_offsets`].
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self,min_bytes,isolation,fetch_partition), fields(partition = fetch_partition.partition))]
    async fn fetch_partition(
        &self,
        deadlines: Deadlines,
        left: usize,
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
        let cap = partition_deadline(deadlines.read, left);

        let partition_index = fetch_partition.partition;
        let tp = Topition::new(topic, partition_index);

        let fetch_offset = fetch_partition.fetch_offset;

        // The fetch offset comes from the client, and storage may build a key
        // range from it, so the offset is checked against the partition
        // before it reaches storage. The bounds are the same for every
        // isolation level, as in Kafka.
        let Some(offset_stage) = before(cap, self.storage.offset_stage(&tp)).await else {
            warn!(?tp, offset = fetch_offset, elapsed = ?started_at.elapsed(), "fetch offset stage deadline exceeded");
            return Ok(Self::unknown_offsets(partition_index, ErrorCode::None));
        };

        let offset_stage = offset_stage.inspect_err(|error| error!(?error, ?tp))?;

        // Below the log start offset the records are gone, so Kafka answers
        // OFFSET_OUT_OF_RANGE, and the client applies `auto.offset.reset`.
        // The answer does not wait for `max_wait`, so a client that keeps
        // fetching this offset does so at round-trip speed: the counter, not
        // a log line per fetch, is what makes it visible.
        if fetch_offset < offset_stage.log_start() {
            debug!(
                topic,
                partition = partition_index,
                fetch_offset,
                log_start = offset_stage.log_start(),
                high_watermark = offset_stage.high_watermark(),
                "fetch offset is below the log start",
            );

            FETCH_OFFSET_OUT_OF_BOUNDS.add(
                1,
                &[
                    KeyValue::new("topic", topic.to_owned()),
                    KeyValue::new("bound", "below_log_start"),
                ],
            );

            return Ok(Self::offset_out_of_range(partition_index));
        }

        // Above the high watermark, every engine answers NONE with no
        // records. Kafka does so only up to the log end offset, and answers
        // OFFSET_OUT_OF_RANGE above it:
        // https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/LocalLog.scala#L368-L371
        // This is a deliberate difference. On dynostore, the high watermark
        // that this broker reads can lag a write through another broker, so
        // a client can be ahead of it, and an error would reset the client's
        // position. One rule for every engine keeps the answer independent
        // of the engine. The consequence: a consumer whose position is past
        // the end of the log (after a restore, or a seek past the end) never
        // applies `auto.offset.reset`. It resumes at its position once the
        // log reaches it, and skips the records written below that position.
        // The counter makes such a parked consumer visible.
        if fetch_offset > offset_stage.high_watermark() {
            debug!(
                topic,
                partition = partition_index,
                fetch_offset,
                log_start = offset_stage.log_start(),
                high_watermark = offset_stage.high_watermark(),
                "fetch offset is above the high watermark",
            );

            FETCH_OFFSET_OUT_OF_BOUNDS.add(
                1,
                &[
                    KeyValue::new("topic", topic.to_owned()),
                    KeyValue::new("bound", "above_high_watermark"),
                ],
            );

            return Ok(Self::partition_data(partition_index, offset_stage, vec![]));
        }

        // The budget starts after the offset stage is read, so a partition
        // whose stage was slow still gets its half of `max_wait` for records.
        let budget = deadlines.storage(cap);

        let mut batches = Vec::new();

        let mut offset = fetch_offset;

        let mut stalled = false;

        loop {
            if *max_bytes == 0 {
                break;
            }

            debug!(offset);

            let Some(fetched) = before(
                cap,
                self.storage.fetch(
                    &tp,
                    offset,
                    min_bytes,
                    *max_bytes,
                    isolation,
                    budget.saturating_duration_since(Instant::now()),
                ),
            )
            .await
            else {
                warn!(?tp, offset, elapsed = ?started_at.elapsed(), "fetch read deadline exceeded");
                stalled = true;
                break;
            };

            let mut fetched = fetched
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

            // The budget bounds the response: engines that assemble
            // batches from rows stop at the budget but return what they
            // have, so another round now would return one record per round
            // trip until max_bytes is spent
            if Instant::now() >= budget {
                debug!(?offset, elapsed = ?started_at.elapsed(), ?budget);
                break;
            }
        }

        // While there are no records, or they end at or below the stage read
        // before the fetch (a consumer catching up), that stage answers
        // the client: its offsets only grow, and the next fetch reads them
        // again. The bound is the one the engine reads to: the last stable
        // offset under ReadCommitted, which a transaction completing during
        // the fetch can move, and the high watermark otherwise. Records past
        // it were written or committed since, so the stage is read again,
        // so that the answer covers them.
        let covered_to = match isolation {
            IsolationLevel::ReadCommitted => offset_stage.last_stable(),
            IsolationLevel::ReadUncommitted => offset_stage.high_watermark(),
        };

        let offset_stage = if batches.is_empty() || offset <= covered_to {
            offset_stage
        } else if stalled {
            // A read that stalled is not followed by another to the same
            // partition.
            Self::covering(offset_stage, offset, isolation)
        } else {
            // A read that used its whole budget assembling batches did not
            // stall, so the stage gets a fresh share of the time left rather
            // than what may be none at all.
            match before(
                partition_deadline(deadlines.read, left),
                self.storage.offset_stage(&tp),
            )
            .await
            {
                Some(offset_stage) => offset_stage.inspect_err(|error| error!(?error, ?tp))?,
                None => {
                    warn!(?tp, offset, elapsed = ?started_at.elapsed(), "fetch offset stage deadline exceeded");
                    Self::covering(offset_stage, offset, isolation)
                }
            }
        };

        Ok(Self::partition_data(partition_index, offset_stage, batches))
            .inspect(|r| debug!(?r, elapsed = ?started_at.elapsed()))
    }

    /// `offset_stage`, read before the records, raised to cover records
    /// that reach past it when the stage could not be read again in time.
    ///
    /// The high watermark is at least `next`, the offset after the last
    /// record returned. Under `ReadCommitted` the engine returns only
    /// committed records, so the last stable offset is at least `next` too.
    /// Unlike Kafka, which answers a remote read that runs out of time with
    /// the offsets it already holds, nisshi holds only the stage it read
    /// before the records, so the answer can lag a write made since.
    fn covering(offset_stage: OffsetStage, next: i64, isolation: IsolationLevel) -> OffsetStage {
        OffsetStage {
            high_watermark: offset_stage.high_watermark.max(next),
            last_stable: match isolation {
                IsolationLevel::ReadCommitted => offset_stage.last_stable.max(next),
                IsolationLevel::ReadUncommitted => offset_stage.last_stable,
            },
            log_start: offset_stage.log_start,
        }
    }

    fn partition_data(
        partition_index: i32,
        offset_stage: OffsetStage,
        batches: Vec<Batch>,
    ) -> PartitionData {
        PartitionData::default()
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
            })
    }

    /// The partition answer for a fetch offset outside the partition, with
    /// the unknown (-1) offsets that Kafka sends alongside this error.
    fn offset_out_of_range(partition_index: i32) -> PartitionData {
        Self::unknown_offsets(partition_index, ErrorCode::OffsetOutOfRange)
    }

    /// The answer for a partition with `error_code`, no records, and
    /// unknown (-1) offsets.
    ///
    /// With [`ErrorCode::None`] it answers a partition whose offset stage
    /// was not read in time, so nothing of it is known: the client fetches
    /// it again on its next request. Java ignores negative offsets and
    /// keeps the ones it had; librdkafka and franz-go report `-1` as the
    /// partition's watermarks until the next answer that has them.
    fn unknown_offsets(partition_index: i32, error_code: ErrorCode) -> PartitionData {
        PartitionData::default()
            .partition_index(partition_index)
            .error_code(error_code.into())
            .high_watermark(-1)
            .last_stable_offset(Some(-1))
            .log_start_offset(Some(-1))
            .diverging_epoch(None)
            .current_leader(None)
            .snapshot_id(None)
            .aborted_transactions(Some([].into()))
            .preferred_read_replica(Some(-1))
            .records(None)
    }

    fn unknown_topic_response(&self, fetch: &FetchTopic) -> Result<FetchableTopicResponse> {
        Ok(FetchableTopicResponse::default()
            .topic(fetch.topic.clone())
            // From v13 the client matches a response topic by id and drops
            // one it cannot match, so the requested id is echoed: the client
            // then sees the error and refreshes its metadata.
            .topic_id(fetch.topic_id)
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
                            // Kafka sends a leader hint only with a leadership
                            // error; librdkafka acts on one here, and its
                            // consumer close then hangs on a deleted topic
                            .current_leader(None)
                            .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None)
                    })
                    .collect()
            })))
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, min_bytes, isolation, fetch))]
    async fn fetch_topic(
        &self,
        deadlines: Deadlines,
        left: &mut usize,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        fetch: &FetchTopic,
        is_first_non_empty: &mut bool,
    ) -> Result<FetchableTopicResponse> {
        let metadata = self.storage.metadata(Some(&[fetch.into()])).await?;

        if let Some(MetadataResponseTopic {
            topic_id,
            name: Some(name),
            error_code,
            ..
        }) = metadata.topics().first()
            && *error_code == i16::from(ErrorCode::None)
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

                let partition = self
                    .fetch_partition(
                        deadlines,
                        *left,
                        min_bytes,
                        &mut partition_bytes,
                        isolation,
                        name,
                        fetch_partition,
                    )
                    .await?;

                *left = left.saturating_sub(1);

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
            // Any metadata error answers the client as an unknown topic, so a
            // storage failure would otherwise leave no trace naming the topic.
            if let Some(MetadataResponseTopic { error_code, .. }) = metadata.topics().first()
                && *error_code != i16::from(ErrorCode::None)
                && *error_code != i16::from(ErrorCode::UnknownTopicOrPartition)
            {
                tracing::warn!(
                    topic = ?fetch.topic,
                    topic_id = ?fetch.topic_id,
                    error_code,
                    error = ?ErrorCode::try_from(*error_code).ok(),
                    "metadata error, answering UNKNOWN_TOPIC_OR_PARTITION"
                );
            }

            *left = left.saturating_sub(fetch.partitions.as_ref().map_or(0, Vec::len));
            self.unknown_topic_response(fetch)
        }
    }

    #[instrument(skip(self, isolation, topics))]
    pub(crate) async fn fetch(
        &self,
        max_wait: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topics: &[FetchTopic],
    ) -> Result<Vec<FetchableTopicResponse>> {
        debug!(?isolation, ?topics);

        if topics.is_empty() {
            Ok(vec![])
        } else {
            // Instant rather than SystemTime: the deadlines below must not
            // move with the wall clock
            let started_at = Instant::now();
            let deadlines = Deadlines::new(started_at, max_wait);
            let mut responses = vec![];
            let mut iteration = 0;
            let mut bytes = 0;
            let mut is_first_non_empty = true;

            while !max_wait.saturating_sub(started_at.elapsed()).is_zero() && bytes <= min_bytes {
                debug!(?bytes, remaining = ?max_wait.saturating_sub(started_at.elapsed()));

                responses.clear();

                // Every round reads every partition again
                let mut left = topics
                    .iter()
                    .map(|topic| topic.partitions.as_ref().map_or(0, Vec::len))
                    .sum::<usize>();

                let fetch_started_at = Instant::now();
                for fetch in topics.iter() {
                    let fetch_response = self
                        .fetch_topic(
                            deadlines,
                            &mut left,
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

                let remaining = max_wait.saturating_sub(started_at.elapsed());

                debug!(?iteration, ?max_wait, ?remaining, ?bytes, ?min_bytes);

                if bytes > min_bytes {
                    break;
                }

                if answers_without_waiting(&responses) {
                    debug!(?iteration, "partition error, not waiting for min_bytes");
                    break;
                }

                {
                    let fetch_elapsed = fetch_started_at.elapsed();

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

impl<G, I> Service<I> for FetchService<G>
where
    G: Storage,
    I: Into<RequestInput<FetchRequest>> + Send + 'static,
{
    type Output = FetchResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let started_at = Instant::now();

        let input = input.into();

        let responses = Some(if let Some(topics) = input.request.topics {
            let isolation_level = input
                .request
                .isolation_level
                .map_or(Ok(IsolationLevel::ReadUncommitted), |isolation| {
                    IsolationLevel::try_from(isolation)
                })?;

            let max_wait_ms =
                u64::try_from(input.request.max_wait_ms).map(Duration::from_millis)?;

            let min_bytes = u32::try_from(input.request.min_bytes)?;

            const DEFAULT_MAX_BYTES: u32 = 5 * 1024 * 1024;

            let mut max_bytes =
                input
                    .request
                    .max_bytes
                    .map_or(Ok(DEFAULT_MAX_BYTES), |max_bytes| {
                        u32::try_from(max_bytes).map(|max_bytes| max_bytes.min(DEFAULT_MAX_BYTES))
                    })?;

            self.fetch(
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
        .inspect(|r| debug!(?r, elapsed = ?started_at.elapsed()))
    }
}

/// Returns true when a fetch answers before `min_bytes` or `max_wait`, as
/// Kafka does: a log read error, such as `OFFSET_OUT_OF_RANGE`, in any
/// partition ([ReplicaManager.scala]), or an error in every partition.
///
/// An unknown topic alone does not end the wait while another partition has
/// no error, because Kafka sets unknown topics aside before it reads the log,
/// and waits for the other partitions ([KafkaApis.scala]). A consumer still
/// assigned to a deleted topic then fetches once per `max_wait`, instead of
/// at round trip speed.
///
/// [ReplicaManager.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1550-L1558
/// [KafkaApis.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1031-L1032
fn answers_without_waiting(responses: &[FetchableTopicResponse]) -> bool {
    let unknown_topic = i16::from(ErrorCode::UnknownTopicOrPartition);
    let none = i16::from(ErrorCode::None);

    let mut error_codes = responses
        .iter()
        .flat_map(|topic| topic.partitions.as_deref().unwrap_or_default())
        .map(|partition| partition.error_code)
        .peekable();

    if error_codes.peek().is_none() {
        return false;
    }

    let mut every_partition_has_error = true;

    for error_code in error_codes {
        if error_code == none {
            every_partition_has_error = false;
        } else if error_code != unknown_topic {
            return true;
        }
    }

    every_partition_has_error
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
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::SystemTime,
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use nisshi_sans_io::{
        ConfigResource, ErrorCode, FetchRequest, IsolationLevel, ListOffset, ScramMechanism,
        create_topics_request::CreatableTopic,
        delete_groups_response::DeletableGroupResult,
        delete_records_request::DeleteRecordsTopic,
        delete_records_response::DeleteRecordsTopicResult,
        describe_cluster_response::DescribeClusterBroker,
        describe_configs_response::DescribeConfigsResult,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        fetch_request::{FetchPartition, FetchTopic},
        fetch_response::{AbortedTransaction, PartitionData},
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
        list_groups_response::ListedGroup,
        metadata_response::MetadataResponseTopic,
        record::{Record, deflated, inflated},
        txn_offset_commit_response::TxnOffsetCommitResponseTopic,
    };
    use rama::Service as _;
    use tokio::time::{Duration, Instant, advance};
    use url::Url;
    use uuid::Uuid;

    use super::{Deadlines, FetchService, READ_DEADLINE_OVERHEAD, before};
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

    /// Implements [`Storage`] for a double with the methods given,
    /// leaving those the doubles here never call unimplemented.
    macro_rules! storage_double {
        ($double:ty { $($method:item)* }) => {
            #[async_trait]
            impl Storage for $double {
                $($method)*

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
        };
    }

    /// Storage whose `fetch` replays scripted responses, records the offset
    /// and remaining `max_wait` of each call, and optionally advances the
    /// (paused) clock past the deadline on the first call, as an engine
    /// that spends the whole budget assembling one truncated batch does.
    /// Each read of the offset stage moves its last stable offset and high
    /// watermark up by `stage_growth`, as writes during a fetch do, so a
    /// test can tell which read answered.
    #[derive(Clone, Debug)]
    struct Scripted {
        responses: Arc<Mutex<Vec<Vec<deflated::Batch>>>>,
        calls: Arc<Mutex<Vec<(i64, Duration)>>>,
        consume_budget_on_first_call: Option<Duration>,
        offset_stage: OffsetStage,
        offset_stage_reads: Arc<AtomicUsize>,
        stage_growth: i64,
    }

    impl Scripted {
        fn new(responses: Vec<Vec<deflated::Batch>>, offset_stage: OffsetStage) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses)),
                calls: Arc::new(Mutex::new(vec![])),
                consume_budget_on_first_call: None,
                offset_stage,
                offset_stage_reads: Arc::new(AtomicUsize::new(0)),
                stage_growth: 0,
            }
        }

        fn calls(&self) -> Vec<(i64, Duration)> {
            self.calls.lock().expect("calls").clone()
        }

        fn call_offsets(&self) -> Vec<i64> {
            self.calls().into_iter().map(|(offset, _)| offset).collect()
        }

        fn offset_stage_reads(&self) -> usize {
            self.offset_stage_reads.load(Ordering::SeqCst)
        }
    }

    storage_double!(Scripted {
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
            let reads = self.offset_stage_reads.fetch_add(1, Ordering::SeqCst);
            let growth = self.stage_growth * i64::try_from(reads)?;

            Ok(OffsetStage {
                last_stable: self.offset_stage.last_stable + growth,
                high_watermark: self.offset_stage.high_watermark + growth,
                ..self.offset_stage
            })
        }

        async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
            unimplemented!()
        }

    });

    const OFFSET_STAGE: OffsetStage = OffsetStage {
        last_stable: 1_000,
        high_watermark: 1_000,
        log_start: 0,
    };

    async fn fetch_partition_at(
        storage: Scripted,
        max_wait: Duration,
        max_bytes: u32,
        isolation: IsolationLevel,
        fetch_offset: i64,
    ) -> Result<PartitionData> {
        let mut remaining = max_bytes;

        FetchService { storage }
            .fetch_partition(
                Deadlines::new(Instant::now(), max_wait),
                1,
                1,
                &mut remaining,
                isolation,
                "abc",
                &FetchPartition::default()
                    .partition(0)
                    .fetch_offset(fetch_offset)
                    .partition_max_bytes(max_bytes as i32),
            )
            .await
    }

    async fn fetch_partition(
        storage: Scripted,
        max_wait: Duration,
        max_bytes: u32,
    ) -> Result<Vec<deflated::Batch>> {
        fetch_partition_at(
            storage,
            max_wait,
            max_bytes,
            IsolationLevel::ReadUncommitted,
            0,
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
            consume_budget_on_first_call: Some(max_wait),
            ..Scripted::new(responses, OFFSET_STAGE)
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

        let storage = Scripted::new(responses, OFFSET_STAGE);

        let batches = fetch_partition(storage.clone(), max_wait, 1024 * 1024).await?;

        assert_eq!(1, batches.len());
        assert_eq!(vec![0, 4], storage.call_offsets());

        Ok(())
    }

    /// A partition whose log starts at 100, whose last stable offset is
    /// 500, and whose high watermark is 1,000.
    fn bounded() -> Scripted {
        Scripted::new(
            vec![],
            OffsetStage {
                last_stable: 500,
                high_watermark: 1_000,
                log_start: 100,
            },
        )
    }

    /// A fetch offset below the log start is answered with
    /// `OFFSET_OUT_OF_RANGE` and never reaches storage, which may build an
    /// invalid key range from it (a negative offset, as a big-endian key).
    #[tokio::test(start_paused = true)]
    async fn below_log_start_is_out_of_range() -> Result<()> {
        for fetch_offset in [i64::MIN, -5, -1, 0, 99] {
            let storage = bounded();

            let partition = fetch_partition_at(
                storage.clone(),
                Duration::from_millis(100),
                1024,
                IsolationLevel::ReadUncommitted,
                fetch_offset,
            )
            .await?;

            assert_eq!(
                ErrorCode::OffsetOutOfRange,
                ErrorCode::try_from(partition.error_code)?,
                "fetch at {fetch_offset}"
            );
            assert_eq!(-1, partition.high_watermark, "fetch at {fetch_offset}");
            assert_eq!(
                Some(-1),
                partition.last_stable_offset,
                "fetch at {fetch_offset}"
            );
            assert_eq!(
                Some(-1),
                partition.log_start_offset,
                "fetch at {fetch_offset}"
            );
            assert!(partition.records.is_none(), "fetch at {fetch_offset}");
            assert!(
                storage.calls().is_empty(),
                "fetch at {fetch_offset} reached storage: {:?}",
                storage.calls()
            );
        }

        Ok(())
    }

    /// A fetch offset above the high watermark is answered with `NONE` and
    /// no records, on every engine. Kafka does so only up to the log end
    /// offset; here the high watermark that a broker reads can lag a write
    /// through another broker (dynostore). The offset never reaches
    /// storage, which may build an invalid key range from it (`i64::MAX`,
    /// as a big-endian key).
    #[tokio::test(start_paused = true)]
    async fn above_high_watermark_answers_none_without_storage() -> Result<()> {
        for fetch_offset in [1_001, i64::MAX] {
            let storage = bounded();

            let partition = fetch_partition_at(
                storage.clone(),
                Duration::from_millis(100),
                1024,
                IsolationLevel::ReadUncommitted,
                fetch_offset,
            )
            .await?;

            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(partition.error_code)?,
                "fetch at {fetch_offset}"
            );
            assert_eq!(1_000, partition.high_watermark, "fetch at {fetch_offset}");
            assert_eq!(Some(500), partition.last_stable_offset);
            assert_eq!(Some(100), partition.log_start_offset);
            assert!(partition.records.is_none(), "fetch at {fetch_offset}");
            assert!(
                storage.calls().is_empty(),
                "fetch at {fetch_offset} reached storage: {:?}",
                storage.calls()
            );
        }

        Ok(())
    }

    /// The log start offset and the high watermark are both valid fetch
    /// offsets: the first is the oldest record, the second the next one to
    /// be written, where a caught-up consumer waits.
    #[tokio::test(start_paused = true)]
    async fn boundary_offsets_reach_storage() -> Result<()> {
        for fetch_offset in [100, 1_000] {
            let storage = bounded();
            storage.responses.lock()?.push(vec![]);

            let partition = fetch_partition_at(
                storage.clone(),
                Duration::from_millis(100),
                1024,
                IsolationLevel::ReadUncommitted,
                fetch_offset,
            )
            .await?;

            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(partition.error_code)?,
                "fetch at {fetch_offset}"
            );
            assert_eq!(1_000, partition.high_watermark);
            assert_eq!(Some(100), partition.log_start_offset);
            assert_eq!(vec![fetch_offset], storage.call_offsets());
        }

        Ok(())
    }

    /// The upper bound is the high watermark for every isolation level: a
    /// `ReadCommitted` fetch between the last stable offset and the high
    /// watermark reaches storage, which bounds it by the last stable offset.
    #[tokio::test(start_paused = true)]
    async fn read_committed_above_last_stable_reaches_storage() -> Result<()> {
        for fetch_offset in [700, 1_000] {
            let storage = bounded();
            storage.responses.lock()?.push(vec![]);

            let partition = fetch_partition_at(
                storage.clone(),
                Duration::from_millis(100),
                1024,
                IsolationLevel::ReadCommitted,
                fetch_offset,
            )
            .await?;

            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(partition.error_code)?,
                "fetch at {fetch_offset}"
            );
            assert_eq!(vec![fetch_offset], storage.call_offsets());
        }

        Ok(())
    }

    /// A fetch whose records end at or below the high watermark read for
    /// the bounds check (none at all, as for an idle consumer on every long
    /// poll, or a consumer catching up) reads the offset stage once, for
    /// both the check and the answer. A fetch whose records reach past that
    /// high watermark reads the stage again, so that the answer covers them.
    /// The stage grows by 10 on each read, so the answered offsets show
    /// which read answered.
    #[tokio::test(start_paused = true)]
    async fn offset_stage_reads() -> Result<()> {
        const GROWTH: i64 = 10;

        struct Case {
            name: &'static str,
            responses: Vec<Vec<deflated::Batch>>,
            stage: OffsetStage,
            isolation: IsolationLevel,
            fetch_offset: i64,
            reads: usize,
        }

        let cases = [
            Case {
                name: "idle",
                responses: vec![vec![]],
                stage: OFFSET_STAGE,
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 1_000,
                reads: 1,
            },
            Case {
                name: "catching up",
                responses: vec![vec![batch(0, &[0])?], vec![]],
                stage: OFFSET_STAGE,
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 0,
                reads: 1,
            },
            // records up to offset 3, past a high watermark of 1
            Case {
                name: "past the high watermark",
                responses: vec![vec![batch(0, &[0, 3])?], vec![]],
                stage: OffsetStage {
                    last_stable: 1,
                    high_watermark: 1,
                    log_start: 0,
                },
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 0,
                reads: 2,
            },
            // the last record is at 999, so the next offset is the high
            // watermark, which covers it
            Case {
                name: "ending at the high watermark",
                responses: vec![vec![batch(996, &[0, 3])?], vec![]],
                stage: OFFSET_STAGE,
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 996,
                reads: 1,
            },
            // the last record is at 1,000, one past what the high watermark
            // covers
            Case {
                name: "ending one past the high watermark",
                responses: vec![vec![batch(997, &[0, 3])?], vec![]],
                stage: OFFSET_STAGE,
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 997,
                reads: 2,
            },
            // under ReadCommitted, records past the last stable offset read
            // before the fetch (a transaction completed meanwhile) read the
            // stage again, though they end below the high watermark
            Case {
                name: "committed during the fetch",
                responses: vec![vec![batch(0, &[0, 3])?], vec![]],
                stage: OffsetStage {
                    last_stable: 1,
                    high_watermark: 1_000,
                    log_start: 0,
                },
                isolation: IsolationLevel::ReadCommitted,
                fetch_offset: 0,
                reads: 2,
            },
            // under ReadCommitted, a fetch between the last stable offset
            // and the high watermark returns nothing, and the first read
            // answers it
            Case {
                name: "ReadCommitted above the last stable offset",
                responses: vec![vec![]],
                stage: OffsetStage {
                    last_stable: 500,
                    high_watermark: 1_000,
                    log_start: 0,
                },
                isolation: IsolationLevel::ReadCommitted,
                fetch_offset: 700,
                reads: 1,
            },
        ];

        for case in cases {
            let storage = Scripted {
                stage_growth: GROWTH,
                ..Scripted::new(case.responses, case.stage)
            };

            let partition = fetch_partition_at(
                storage.clone(),
                Duration::from_millis(100),
                1024,
                case.isolation,
                case.fetch_offset,
            )
            .await?;

            assert_eq!(case.reads, storage.offset_stage_reads(), "{}", case.name);

            // the answer comes from the last read
            let growth = GROWTH * i64::try_from(case.reads - 1)?;
            assert_eq!(
                case.stage.high_watermark + growth,
                partition.high_watermark,
                "{}",
                case.name
            );
            assert_eq!(
                Some(case.stage.last_stable + growth),
                partition.last_stable_offset,
                "{}",
                case.name
            );

            // the answered bound covers every record returned
            let next_offset = partition
                .records
                .iter()
                .flat_map(|frame| &frame.batches)
                .map(|batch| batch.max_offset() + 1)
                .max();

            if let Some(next_offset) = next_offset {
                let covered_to = match case.isolation {
                    IsolationLevel::ReadCommitted => partition.last_stable_offset,
                    IsolationLevel::ReadUncommitted => Some(partition.high_watermark),
                };

                assert!(
                    covered_to.is_some_and(|covered_to| next_offset <= covered_to),
                    "{}: records end at {next_offset}, answer covers {covered_to:?}",
                    case.name
                );
            }
        }

        Ok(())
    }

    /// What one storage read of a partition does, in [`Partitions`].
    #[derive(Clone, Debug)]
    enum Read {
        /// Returns this batch, or nothing given no budget, as engines that
        /// stop assembling at the deadline do.
        Batch(deflated::Batch),

        /// Returns nothing.
        Empty,

        /// Does not answer for an hour.
        Stall,
    }

    /// One storage read of a partition, as [`Partitions`] saw it.
    #[derive(Clone, Debug, PartialEq)]
    struct Call {
        partition: i32,
        max_bytes: u32,
        max_wait: Duration,
    }

    /// Storage with one topic, `abc`, whose partitions each answer their
    /// reads in turn from their own script, with nothing once it runs out.
    #[derive(Clone, Debug, Default)]
    struct Partitions {
        scripts: Arc<Mutex<BTreeMap<i32, Vec<Read>>>>,
        calls: Arc<Mutex<Vec<Call>>>,
    }

    impl Partitions {
        fn new(scripts: impl IntoIterator<Item = (i32, Vec<Read>)>) -> Self {
            Self {
                scripts: Arc::new(Mutex::new(scripts.into_iter().collect())),
                ..Default::default()
            }
        }

        fn calls(&self, partition: i32) -> Vec<Call> {
            self.calls
                .lock()
                .expect("calls")
                .iter()
                .filter(|call| call.partition == partition)
                .cloned()
                .collect()
        }
    }

    storage_double!(Partitions {
        async fn fetch(
            &self,
            topition: &Topition,
            _offset: i64,
            _min_bytes: u32,
            max_bytes: u32,
            _isolation_level: IsolationLevel,
            max_wait: Duration,
        ) -> Result<Vec<deflated::Batch>> {
            self.calls.lock()?.push(Call {
                partition: topition.partition(),
                max_bytes,
                max_wait,
            });

            let read = {
                let mut scripts = self.scripts.lock()?;
                let script = scripts.entry(topition.partition()).or_default();

                if script.is_empty() {
                    Read::Empty
                } else {
                    script.remove(0)
                }
            };

            match read {
                Read::Batch(_) if max_wait.is_zero() => Ok(vec![]),
                Read::Batch(batch) => Ok(vec![batch]),
                Read::Empty => Ok(vec![]),
                Read::Stall => {
                    tokio::time::sleep(Duration::from_secs(3_600)).await;
                    Ok(vec![])
                }
            }
        }

        async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
            Ok(OffsetStage {
                last_stable: 1_000,
                high_watermark: 1_000,
                log_start: 0,
            })
        }

        async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
            Ok(MetadataResponse {
                cluster: None,
                controller: None,
                brokers: vec![],
                topics: vec![
                    MetadataResponseTopic::default()
                        .error_code(ErrorCode::None.into())
                        .name(Some(TOPIC.into()))
                        .topic_id(Some([1; 16])),
                ],
            })
        }
    });

    const TOPIC: &str = "abc";
    const MAX_WAIT: Duration = Duration::from_millis(500);
    const MAX_BYTES: i32 = 1024 * 1024;

    /// Fetches `partitions` of [`TOPIC`] from offset 0 with [`MAX_WAIT`],
    /// returning each partition's answer and how long the request took.
    async fn fetch(
        storage: Partitions,
        partitions: &[i32],
    ) -> Result<(Vec<PartitionData>, Duration)> {
        let started_at = Instant::now();

        let response = FetchService { storage }
            .serve(
                FetchRequest::default()
                    .max_wait_ms(i32::try_from(MAX_WAIT.as_millis())?)
                    .min_bytes(1)
                    .max_bytes(Some(MAX_BYTES))
                    .topics(Some(vec![
                        FetchTopic::default()
                            .topic(Some(TOPIC.into()))
                            .partitions(Some(
                                partitions
                                    .iter()
                                    .map(|partition| {
                                        FetchPartition::default()
                                            .partition(*partition)
                                            .fetch_offset(0)
                                            .partition_max_bytes(MAX_BYTES)
                                    })
                                    .collect(),
                            )),
                    ])),
            )
            .await?;

        let elapsed = started_at.elapsed();

        let mut topics = response.responses.unwrap_or_default();
        assert_eq!(1, topics.len());

        Ok((topics.remove(0).partitions.unwrap_or_default(), elapsed))
    }

    fn batches(partition: &PartitionData) -> usize {
        partition
            .records
            .as_ref()
            .map_or(0, |records| records.batches.len())
    }

    /// The read deadline of a request with [`MAX_WAIT`].
    const READ_DEADLINE: Duration = MAX_WAIT.saturating_add(READ_DEADLINE_OVERHEAD);

    /// A partition that never answers holds the request for half of the
    /// read deadline, and the partitions after it are then read with a
    /// budget of their own: engines read nothing given none, so a budget
    /// taken from the spent `max_wait` would leave them as starved as
    /// before.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_partition_does_not_starve_the_rest() -> Result<()> {
        let storage = Partitions::new([
            (0, vec![Read::Stall]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
            (2, vec![Read::Batch(batch(0, &[0])?)]),
            (3, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let (partitions, elapsed) = fetch(storage.clone(), &[0, 1, 2, 3]).await?;

        assert_eq!(READ_DEADLINE / 2, elapsed);

        assert_eq!(0, partitions[0].partition_index);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(0, batches(&partitions[0]));

        // the stage read before the records answers the stalled partition
        assert_eq!(1_000, partitions[0].high_watermark);
        assert_eq!(Some(1_000), partitions[0].last_stable_offset);
        assert_eq!(Some(0), partitions[0].log_start_offset);

        for partition in &partitions[1..] {
            assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
            assert_eq!(
                1,
                batches(partition),
                "partition {}",
                partition.partition_index
            );
            assert_eq!(1_000, partition.high_watermark);
        }

        // max_wait was spent by the stall, and each read after it had half
        // of max_wait of its own
        for partition in 1..=3 {
            assert_eq!(MAX_WAIT / 2, storage.calls(partition)[0].max_wait);
        }

        Ok(())
    }

    /// However many partitions never answer, the request is answered by
    /// its read deadline.
    #[tokio::test(start_paused = true)]
    async fn stalled_partitions_end_at_the_read_deadline() -> Result<()> {
        let storage = Partitions::new([
            (0, vec![Read::Stall]),
            (1, vec![Read::Stall]),
            (2, vec![Read::Stall]),
        ]);

        let (partitions, elapsed) = fetch(storage, &[0, 1, 2]).await?;

        assert_eq!(READ_DEADLINE, elapsed);
        assert_eq!(3, partitions.len());

        for partition in &partitions {
            assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
            assert_eq!(0, batches(partition));
        }

        Ok(())
    }

    /// A partition that stalls after reading some batches returns them,
    /// with the stage read before them, and the bytes they took are not
    /// given again to the partitions after it.
    #[tokio::test(start_paused = true)]
    async fn a_partition_that_stalls_keeps_what_it_read() -> Result<()> {
        let first = batch(0, &[0, 1])?;
        let size = u32::try_from(first.record_data.len())?;

        let storage = Partitions::new([
            (0, vec![Read::Batch(first), Read::Stall]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let (partitions, elapsed) = fetch(storage.clone(), &[0, 1]).await?;

        assert_eq!(READ_DEADLINE / 2, elapsed);

        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(1_000, partitions[0].high_watermark);
        assert_eq!(Some(1_000), partitions[0].last_stable_offset);
        assert_eq!(Some(0), partitions[0].log_start_offset);

        assert_eq!(1, batches(&partitions[1]));

        let max_bytes = u32::try_from(MAX_BYTES)?;
        assert_eq!(max_bytes - size, storage.calls(0)[1].max_bytes);
        assert_eq!(max_bytes - size, storage.calls(1)[0].max_bytes);

        Ok(())
    }

    /// Every round of the long poll shares the read deadline among all of
    /// the partitions again: a count of partitions left over from the
    /// round before would give the first partition the whole deadline.
    #[tokio::test(start_paused = true)]
    async fn every_round_shares_the_read_deadline() -> Result<()> {
        let storage = Partitions::new([
            (0, vec![Read::Empty, Read::Stall]),
            (1, vec![Read::Empty, Read::Batch(batch(0, &[0])?)]),
        ]);

        let (partitions, elapsed) = fetch(storage, &[0, 1]).await?;

        // The first round finds nothing, and the long poll waits half of
        // max_wait before the second
        let second_round = MAX_WAIT / 2;
        assert_eq!(second_round + (READ_DEADLINE - second_round) / 2, elapsed);

        assert_eq!(0, batches(&partitions[0]));
        assert_eq!(1, batches(&partitions[1]));

        Ok(())
    }

    /// Once the deadline has passed, the read is never polled.
    #[tokio::test(start_paused = true)]
    async fn before_does_not_start_after_the_deadline() {
        let deadline = Instant::now();
        advance(Duration::from_millis(1)).await;

        let polled = Arc::new(AtomicBool::new(false));

        let read = {
            let polled = polled.clone();
            async move { polled.store(true, Ordering::SeqCst) }
        };

        assert_eq!(None, before(deadline, read).await);
        assert!(!polled.load(Ordering::SeqCst));
    }
}
