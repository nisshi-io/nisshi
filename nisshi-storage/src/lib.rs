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
//
//! Nisshi Storage Abstraction
//!
//! [`StorageContainer`] provides an abstraction over [`Storage`] and can
//! be configured to use memory, [S3](https://en.wikipedia.org/wiki/Amazon_S3),
//! [PostgreSQL](https://postgresql.org/),
//! [libSQL](https://github.com/tursodatabase/libsql) and
//! [Turso](https://github.com/tursodatabase/turso) (alpha: currently feature locked).
//!
//! ## Memory
//!
//! ```
//! # use nisshi_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("nisshi")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("memory://nisshi/")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## S3
//!
//! ```no_run
//! # use nisshi_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("nisshi")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("s3://nisshi/")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## PostgreSQL
//!
//! ```no_run
//! # use nisshi_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("nisshi")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("postgres://postgres:postgres@localhost")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## libSQL (SQLite)
//!
//! ```no_run
//! # use nisshi_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("nisshi")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("sqlite://nisshi.db")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Turso
//!
//! ```no_run
//! # use nisshi_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("nisshi")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("turso://nisshi.db")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!

use async_trait::async_trait;
use bytes::{Bytes, TryGetError};

use console::Emoji;
#[cfg(any(feature = "libsql", feature = "postgres"))]
use deadpool::managed::PoolError;
#[cfg(feature = "dynostore")]
use dynostore::DynoStore;

use glob::{GlobError, PatternError};

use indicatif::{ProgressBar, ProgressStyle};
#[cfg(feature = "dynostore")]
use object_store::memory::InMemory;

#[cfg(feature = "dynostore")]
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};

use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;

#[cfg(feature = "postgres")]
use pg::Postgres;

use governor::InsufficientCapacity;
use nisshi_sans_io::{
    Body, ConfigResource, ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, ScramMechanism,
    add_partitions_to_txn_request::{
        AddPartitionsToTxnRequest, AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
    },
    add_partitions_to_txn_response::{AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult},
    consumer_group_describe_response,
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    delete_topics_request::DeleteTopicState,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_groups_response,
    describe_topic_partitions_request::{Cursor, TopicRequest},
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    fetch_request::FetchTopic,
    fetch_response::AbortedTransaction,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    join_group_response::JoinGroupResponseMember,
    list_groups_response::ListedGroup,
    metadata_request::MetadataRequestTopic,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
    offset_commit_request::OffsetCommitRequestPartition,
    record::deflated,
    to_system_time, to_timestamp,
    txn_offset_commit_request::TxnOffsetCommitRequestTopic,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use nisshi_schema::{Registry, lake::House};
use regex::Regex;
use serde::{Deserialize, Serialize};
#[cfg(any(feature = "libsql", feature = "postgres"))]
use std::error;
use std::{
    array::TryFromSliceError,
    collections::BTreeMap,
    ffi::OsString,
    fmt::{self, Debug, Display, Formatter},
    fs::DirEntry,
    io,
    marker::PhantomData,
    num::{ParseIntError, TryFromIntError},
    path::PathBuf,
    result,
    str::FromStr,
    sync::{Arc, LazyLock, PoisonError},
    time::{Duration, SystemTime, SystemTimeError},
};
use tokio::sync::AcquireError;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument};
use tracing_subscriber::filter::ParseError;
use url::Url;
use uuid::Uuid;

#[cfg(feature = "dynostore")]
use tracing::warn;

#[cfg(feature = "dynostore")]
mod dynostore;

#[cfg(feature = "postgres")]
mod pg;

#[cfg(feature = "dynostore")]
mod batch;
mod latency;

mod null;

#[cfg(feature = "libsql")]
mod proxy;

mod service;

pub use latency::LatencyIntroducingStorage;

pub use service::{
    AlterUserScramCredentialsService, ChannelRequestLayer, ChannelRequestService,
    ConsumerGroupDescribeService, CreateAclsService, CreateTopicsService, DeleteGroupsService,
    DeleteRecordsService, DeleteTopicsService, DescribeAclsService, DescribeClusterService,
    DescribeConfigsService, DescribeGroupsService, DescribeTopicPartitionsService,
    DescribeUserScramCredentialsService, FetchService, FindCoordinatorService,
    GetTelemetrySubscriptionsService, IncrementalAlterConfigsService, InitProducerIdService,
    ListGroupsService, ListOffsetsService, ListPartitionReassignmentsService, MetadataService,
    ProduceService, Request, RequestChannelService, RequestLayer, RequestReceiver, RequestSender,
    RequestService, RequestStorageService, Response, TxnAddOffsetsService, TxnAddPartitionService,
    TxnEndService, TxnOffsetCommitService, bounded_channel,
};

#[cfg(feature = "slatedb")]
pub mod slate;

#[cfg(any(feature = "libsql", feature = "postgres", feature = "turso"))]
pub(crate) mod sql;

#[cfg(feature = "libsql")]
mod lite;

#[cfg(feature = "dynostore")]
mod gcs;

#[cfg(feature = "dynostore")]
mod os;

#[cfg(feature = "turso")]
mod limbo;

/// Storage Errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    Acquire(Arc<AcquireError>),

    Api(ErrorCode),

    ChronoParse(#[from] chrono::ParseError),

    #[cfg(any(feature = "postgres", feature = "libsql"))]
    DeadPoolBuild(#[from] deadpool::managed::BuildError),

    Decode(Bytes),

    FeatureNotEnabled {
        feature: String,
        message: String,
    },

    Glob(Arc<GlobError>),
    InsufficientCapacity(#[from] InsufficientCapacity),
    Io(Arc<io::Error>),

    LessThanBaseOffset {
        offset: i64,
        base_offset: i64,
    },
    LessThanLastOffset {
        offset: i64,
        last_offset: Option<i64>,
    },

    #[cfg(feature = "libsql")]
    LibSql(Arc<libsql::Error>),

    LessThanMaxTime {
        time: i64,
        max_time: Option<i64>,
    },
    LessThanMinTime {
        time: i64,
        min_time: Option<i64>,
    },
    Message(String),
    NoSuchEntry {
        nth: u32,
    },
    NoSuchOffset(i64),
    OsString(OsString),

    #[cfg(any(feature = "dynostore", feature = "slatedb"))]
    ObjectStore(Arc<object_store::Error>),

    ParseFilter(Arc<ParseError>),
    Pattern(Arc<PatternError>),
    ParseInt(#[from] ParseIntError),
    PhantomCached(),
    Poison,

    #[cfg(any(feature = "libsql", feature = "postgres"))]
    Pool(Arc<Box<dyn error::Error + Send + Sync>>),

    #[cfg(feature = "slatedb")]
    Postcard(#[from] postcard::Error),

    Regex(#[from] regex::Error),

    SansIo(#[from] nisshi_sans_io::Error),

    Schema(Arc<nisshi_schema::Error>),

    Rustls(#[from] rustls::Error),

    SegmentEmpty(Topition),

    SegmentMissing {
        topition: Topition,
        offset: Option<i64>,
    },

    SerdeJson(Arc<serde_json::Error>),

    #[cfg(feature = "slatedb")]
    Slate(Arc<slatedb::Error>),

    SystemTime(#[from] SystemTimeError),

    #[cfg(feature = "postgres")]
    TokioPostgres(Arc<tokio_postgres::error::Error>),
    TryFromInt(#[from] TryFromIntError),
    TryFromSlice(#[from] TryFromSliceError),

    TryGet(Arc<TryGetError>),

    #[cfg(feature = "turso")]
    Turso(Arc<turso::Error>),

    UnexpectedBody(Box<Body>),

    UnexpectedServiceResponse(Box<Response>),

    #[cfg(feature = "turso")]
    UnexpectedValue(turso::Value),

    UnknownCacheKey(String),

    UnsupportedStorageUrl(Url),
    UnexpectedAddPartitionsToTxnRequest(Box<AddPartitionsToTxnRequest>),
    Url(#[from] url::ParseError),
    UnknownTxnState(String),

    Uuid(#[from] uuid::Error),

    UnableToSend,
    OneshotRecv,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<TryGetError> for Error {
    fn from(value: TryGetError) -> Self {
        Self::TryGet(Arc::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<AcquireError> for Error {
    fn from(value: AcquireError) -> Self {
        Self::Acquire(Arc::new(value))
    }
}

#[cfg(any(feature = "libsql", feature = "postgres"))]
impl<E> From<PoolError<E>> for Error
where
    E: error::Error + Send + Sync + 'static,
{
    fn from(value: PoolError<E>) -> Self {
        Self::Pool(Arc::new(Box::new(value)))
    }
}

#[cfg(feature = "libsql")]
impl From<libsql::Error> for Error {
    fn from(value: libsql::Error) -> Self {
        Self::LibSql(Arc::new(value))
    }
}

#[cfg(feature = "slatedb")]
impl From<slatedb::Error> for Error {
    fn from(value: slatedb::Error) -> Self {
        Self::Slate(Arc::new(value))
    }
}

#[cfg(feature = "turso")]
impl From<turso::Error> for Error {
    fn from(value: turso::Error) -> Self {
        Self::Turso(Arc::new(value))
    }
}

impl From<GlobError> for Error {
    fn from(value: GlobError) -> Self {
        Self::Glob(Arc::new(value))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

#[cfg(any(feature = "dynostore", feature = "slatedb"))]
impl From<Arc<object_store::Error>> for Error {
    fn from(value: Arc<object_store::Error>) -> Self {
        Self::ObjectStore(value)
    }
}

#[cfg(any(feature = "dynostore", feature = "slatedb"))]
impl From<object_store::Error> for Error {
    fn from(value: object_store::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

impl From<PatternError> for Error {
    fn from(value: PatternError) -> Self {
        Self::Pattern(Arc::new(value))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

impl From<Arc<serde_json::Error>> for Error {
    fn from(value: Arc<serde_json::Error>) -> Self {
        Self::SerdeJson(value)
    }
}

#[cfg(feature = "postgres")]
impl From<tokio_postgres::error::Error> for Error {
    fn from(value: tokio_postgres::error::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

#[cfg(feature = "postgres")]
impl From<Arc<tokio_postgres::error::Error>> for Error {
    fn from(value: Arc<tokio_postgres::error::Error>) -> Self {
        Self::TokioPostgres(value)
    }
}

impl From<nisshi_schema::Error> for Error {
    fn from(value: nisshi_schema::Error) -> Self {
        if let nisshi_schema::Error::Api(error_code) = value {
            Self::Api(error_code)
        } else {
            Self::Schema(Arc::new(value))
        }
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

/// Topic Partition (topition)
///
/// A topic partition pair.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Topition {
    topic: String,
    partition: i32,
}

impl Topition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        let topic = topic.into();
        Self { topic, partition }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }
}

impl From<Cursor> for Topition {
    fn from(value: Cursor) -> Self {
        Self {
            topic: value.topic_name,
            partition: value.partition_index,
        }
    }
}

impl TryFrom<&DirEntry> for Topition {
    type Error = Error;

    fn try_from(value: &DirEntry) -> result::Result<Self, Self::Error> {
        Regex::new(r"^(?<topic>.+)-(?<partition>\d{10})$")
            .map_err(Into::into)
            .and_then(|re| {
                value
                    .file_name()
                    .into_string()
                    .map_err(Error::OsString)
                    .and_then(|ref file_name| {
                        re.captures(file_name)
                            .ok_or(Error::Message(format!("no captures for {file_name}")))
                            .and_then(|ref captures| {
                                let topic = captures
                                    .name("topic")
                                    .ok_or(Error::Message(format!("missing topic for {file_name}")))
                                    .map(|s| s.as_str().to_owned())?;

                                let partition = captures
                                    .name("partition")
                                    .ok_or(Error::Message(format!(
                                        "missing partition for: {file_name}"
                                    )))
                                    .map(|s| s.as_str())
                                    .and_then(|s| str::parse(s).map_err(Into::into))?;

                                Ok(Self { topic, partition })
                            })
                    })
            })
    }
}

impl FromStr for Topition {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        i32::from_str(&s[s.len() - 10..])
            .map(|partition| {
                let topic = String::from(&s[..s.len() - 11]);

                Self { topic, partition }
            })
            .map_err(Into::into)
    }
}

impl From<&Topition> for PathBuf {
    fn from(value: &Topition) -> Self {
        let topic = value.topic.as_str();
        let partition = value.partition;
        PathBuf::from(format!("{topic}-{partition:0>10}"))
    }
}

/// Topic Partition Offset
///
/// A topic partition with an offset.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopitionOffset {
    topition: Topition,
    offset: i64,
}

impl TopitionOffset {
    pub fn new(topition: Topition, offset: i64) -> Self {
        Self { topition, offset }
    }

    pub fn topition(&self) -> &Topition {
        &self.topition
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }
}

impl From<&TopitionOffset> for PathBuf {
    fn from(value: &TopitionOffset) -> Self {
        let offset = value.offset;
        PathBuf::from(value.topition()).join(format!("{offset:0>20}"))
    }
}

pub type ListOffsetRequest = ListOffset;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListOffsetResponse {
    pub error_code: ErrorCode,
    pub timestamp: Option<SystemTime>,
    pub offset: Option<i64>,
}

impl Default for ListOffsetResponse {
    fn default() -> Self {
        Self {
            error_code: ErrorCode::None,
            timestamp: None,
            offset: None,
        }
    }
}

impl ListOffsetResponse {
    pub fn offset(&self) -> Option<i64> {
        self.offset
    }

    pub fn timestamp(&self) -> Result<Option<i64>> {
        self.timestamp.map_or(Ok(None), |system_time| {
            to_timestamp(&system_time).map(Some).map_err(Into::into)
        })
    }

    pub fn error_code(&self) -> ErrorCode {
        self.error_code
    }
}

/// Offset Commit Request
///
/// A structure representing an [`nisshi_sans_io::OffsetCommitRequestPartition](OffsetCommitRequestPartition).
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct OffsetCommitRequest {
    offset: i64,
    leader_epoch: Option<i32>,
    timestamp: Option<SystemTime>,
    metadata: Option<String>,
}

impl OffsetCommitRequest {
    pub fn offset(self, offset: i64) -> Self {
        Self { offset, ..self }
    }
}

impl TryFrom<&OffsetCommitRequestPartition> for OffsetCommitRequest {
    type Error = Error;

    fn try_from(value: &OffsetCommitRequestPartition) -> Result<Self, Self::Error> {
        value
            .commit_timestamp
            .map_or(Ok(None), |commit_timestamp| {
                to_system_time(commit_timestamp)
                    .map(Some)
                    .map_err(Into::into)
            })
            .map(|timestamp| Self {
                offset: value.committed_offset,
                leader_epoch: value.committed_leader_epoch,
                timestamp,
                metadata: value.committed_metadata.clone(),
            })
    }
}

/// Topic Id
///
/// An enumeration of either the name or UUID of a topic.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TopicId {
    Name(String),
    Id(Uuid),
}

impl FromStr for TopicId {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        Ok(Self::Name(s.into()))
    }
}

impl From<&str> for TopicId {
    fn from(value: &str) -> Self {
        Self::Name(value.to_owned())
    }
}

impl From<String> for TopicId {
    fn from(value: String) -> Self {
        Self::Name(value)
    }
}

impl From<Uuid> for TopicId {
    fn from(value: Uuid) -> Self {
        Self::Id(value)
    }
}

impl From<[u8; 16]> for TopicId {
    fn from(value: [u8; 16]) -> Self {
        Self::Id(Uuid::from_bytes(value))
    }
}

impl From<&TopicId> for [u8; 16] {
    fn from(value: &TopicId) -> Self {
        match value {
            TopicId::Id(id) => id.into_bytes(),
            TopicId::Name(_) => NULL_TOPIC_ID,
        }
    }
}

impl From<&FetchTopic> for TopicId {
    fn from(value: &FetchTopic) -> Self {
        if let Some(ref name) = value.topic {
            Self::Name(name.into())
        } else if let Some(ref id) = value.topic_id {
            Self::Id(Uuid::from_bytes(*id))
        } else {
            panic!("neither name nor uuid")
        }
    }
}

impl From<&MetadataRequestTopic> for TopicId {
    fn from(value: &MetadataRequestTopic) -> Self {
        if let Some(ref name) = value.name {
            Self::Name(name.into())
        } else if let Some(ref id) = value.topic_id {
            Self::Id(Uuid::from_bytes(*id))
        } else {
            panic!("neither name nor uuid")
        }
    }
}

impl From<DeleteTopicState> for TopicId {
    fn from(value: DeleteTopicState) -> Self {
        match value {
            DeleteTopicState {
                name: Some(name),
                topic_id,
                ..
            } if topic_id == NULL_TOPIC_ID => name.into(),

            DeleteTopicState { topic_id, .. } => topic_id.into(),
        }
    }
}

impl From<&TopicRequest> for TopicId {
    fn from(value: &TopicRequest) -> Self {
        value.name.to_owned().into()
    }
}

impl From<&Topition> for TopicId {
    fn from(value: &Topition) -> Self {
        value.topic.to_owned().into()
    }
}

/// Broker Registration Request
///
/// A broker will register with storage using this structure.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BrokerRegistrationRequest {
    pub broker_id: i32,
    pub cluster_id: String,
    pub incarnation_id: Uuid,
    pub rack: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct MetadataResponse {
    cluster: Option<String>,
    controller: Option<i32>,
    brokers: Vec<MetadataResponseBroker>,
    topics: Vec<MetadataResponseTopic>,
}

impl MetadataResponse {
    pub fn cluster(&self) -> Option<&str> {
        self.cluster.as_deref()
    }

    pub fn controller(&self) -> Option<i32> {
        self.controller
    }

    pub fn brokers(&self) -> &[MetadataResponseBroker] {
        self.brokers.as_ref()
    }

    pub fn topics(&self) -> &[MetadataResponseTopic] {
        self.topics.as_ref()
    }
}

/// Offset Stage
///
/// An offset stage structure represents the `last_stable`, `high_watermark` and `log_start` offsets.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct OffsetStage {
    last_stable: i64,
    high_watermark: i64,
    log_start: i64,
}

impl OffsetStage {
    pub fn last_stable(&self) -> i64 {
        self.last_stable
    }

    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    pub fn log_start(&self) -> i64 {
        self.log_start
    }
}

/// Group Member
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupMember {
    pub join_response: JoinGroupResponseMember,
    pub last_contact: Option<SystemTime>,
}

/// Group State
///
/// A group is either in the process of [`GroupState::Forming`] or has [`GroupState::Formed`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum GroupState {
    Forming {
        protocol_type: Option<String>,
        protocol_name: Option<String>,
        leader: Option<String>,
    },

    Formed {
        protocol_type: String,
        protocol_name: String,
        leader: String,
        assignments: BTreeMap<String, Bytes>,
    },
}

impl GroupState {
    pub fn protocol_type(&self) -> Option<String> {
        match self {
            Self::Forming { protocol_type, .. } => protocol_type.clone(),
            Self::Formed { protocol_type, .. } => Some(protocol_type.clone()),
        }
    }

    pub fn protocol_name(&self) -> Option<String> {
        match self {
            Self::Forming { protocol_name, .. } => protocol_name.clone(),
            Self::Formed { protocol_name, .. } => Some(protocol_name.clone()),
        }
    }

    pub fn leader(&self) -> Option<String> {
        match self {
            Self::Forming { leader, .. } => leader.clone(),
            Self::Formed { leader, .. } => Some(leader.clone()),
        }
    }

    pub fn assignments(&self) -> BTreeMap<String, Bytes> {
        match self {
            Self::Forming { .. } => BTreeMap::new(),
            Self::Formed { assignments, .. } => assignments.clone(),
        }
    }
}

impl Default for GroupState {
    fn default() -> Self {
        Self::Forming {
            protocol_type: None,
            protocol_name: Some("".into()),
            leader: None,
        }
    }
}

/// Consumer Group State
///
/// A helper type for conversion into [`consumer_group_describe_response::DescribedGroup`].
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum ConsumerGroupState {
    Unknown,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
    Empty,
    Assigning,
    Reconciling,
}

impl Display for ConsumerGroupState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("Unknown"),
            Self::PreparingRebalance => f.write_str("PreparingRebalance"),
            Self::CompletingRebalance => f.write_str("CompletingRebalance"),
            Self::Stable => f.write_str("Stable"),
            Self::Dead => f.write_str("Dead"),
            Self::Empty => f.write_str("Empty"),
            Self::Assigning => f.write_str("Assigning"),
            Self::Reconciling => f.write_str("Reconciling"),
        }
    }
}

/// Group Detail
///
/// A helper type that can be easily converted into [`consumer_group_describe_response::DescribedGroup`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupDetail {
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: Option<i32>,
    pub members: BTreeMap<String, GroupMember>,
    pub generation_id: i32,
    pub skip_assignment: Option<bool>,
    pub inception: SystemTime,
    pub state: GroupState,
}

impl Default for GroupDetail {
    fn default() -> Self {
        Self {
            session_timeout_ms: 45_000,
            rebalance_timeout_ms: Default::default(),
            members: Default::default(),
            generation_id: Default::default(),
            skip_assignment: Some(false),
            inception: SystemTime::now(),
            state: GroupState::default(),
        }
    }
}

impl From<&GroupDetail> for ConsumerGroupState {
    fn from(value: &GroupDetail) -> Self {
        match value {
            GroupDetail { members, .. } if members.is_empty() => Self::Empty,

            GroupDetail {
                state: GroupState::Forming { leader: None, .. },
                ..
            } => Self::Assigning,

            GroupDetail {
                state: GroupState::Formed { .. },
                ..
            } => Self::Stable,

            _ => {
                debug!(unknown = ?value);
                Self::Unknown
            }
        }
    }
}

impl From<&GroupDetail> for consumer_group_describe_response::DescribedGroup {
    fn from(value: &GroupDetail) -> Self {
        let assignor_name = match value.state {
            GroupState::Forming { ref leader, .. } => leader.clone().unwrap_or_default(),
            GroupState::Formed { ref leader, .. } => leader.clone(),
        };

        let group_state = ConsumerGroupState::from(value).to_string();

        Self::default()
            .error_code(ErrorCode::None.into())
            .error_message(Some(ErrorCode::None.to_string()))
            .group_id(Default::default())
            .group_state(group_state)
            .group_epoch(-1)
            .assignment_epoch(-1)
            .assignor_name(assignor_name)
            .members(Some([].into()))
            .authorized_operations(-1)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum GroupDetailResponse {
    ErrorCode(ErrorCode),
    Found(GroupDetail),
}

/// NamedGroupDetail
///
/// A helper type that can be easily converted into [`consumer_group_describe_response::DescribedGroup`]
/// or [`describe_groups_response::DescribedGroup`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NamedGroupDetail {
    name: String,
    response: GroupDetailResponse,
}

impl NamedGroupDetail {
    pub fn error_code(name: String, error_code: ErrorCode) -> Self {
        Self {
            name,
            response: GroupDetailResponse::ErrorCode(error_code),
        }
    }

    pub fn found(name: String, found: GroupDetail) -> Self {
        Self {
            name,
            response: GroupDetailResponse::Found(found),
        }
    }
}

impl From<&NamedGroupDetail> for consumer_group_describe_response::DescribedGroup {
    fn from(value: &NamedGroupDetail) -> Self {
        match value {
            NamedGroupDetail {
                name,
                response: GroupDetailResponse::Found(group_detail),
            } => {
                let assignor_name = match group_detail.state {
                    GroupState::Forming { ref leader, .. } => leader.clone().unwrap_or_default(),
                    GroupState::Formed { ref leader, .. } => leader.clone(),
                };

                let group_state = ConsumerGroupState::from(group_detail).to_string();

                Self::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some(ErrorCode::None.to_string()))
                    .group_id(name.into())
                    .group_state(group_state)
                    .group_epoch(-1)
                    .assignment_epoch(-1)
                    .assignor_name(assignor_name)
                    .members(Some([].into()))
                    .authorized_operations(-1)
            }

            NamedGroupDetail {
                name,
                response: GroupDetailResponse::ErrorCode(error_code),
            } => Self::default()
                .error_code((*error_code).into())
                .error_message(Some(error_code.to_string()))
                .group_id(name.into())
                .group_state("Unknown".into())
                .group_epoch(-1)
                .assignment_epoch(-1)
                .assignor_name("".into())
                .members(Some([].into()))
                .authorized_operations(-1),
        }
    }
}

impl From<&NamedGroupDetail> for describe_groups_response::DescribedGroup {
    fn from(value: &NamedGroupDetail) -> Self {
        match value {
            NamedGroupDetail {
                name,
                response: GroupDetailResponse::Found(group_detail),
            } => {
                let group_state = ConsumerGroupState::from(group_detail).to_string();

                let members = group_detail
                    .members
                    .keys()
                    .map(|member_id| {
                        describe_groups_response::DescribedGroupMember::default()
                            .member_id(member_id.into())
                            .group_instance_id(None)
                            .client_id("".into())
                            .client_host("".into())
                            .member_metadata(Bytes::new())
                            .member_assignment(Bytes::new())
                    })
                    .collect::<Vec<_>>();

                Self::default()
                    .error_code(ErrorCode::None.into())
                    .group_id(name.clone())
                    .group_state(group_state)
                    .protocol_type(group_detail.state.protocol_type().unwrap_or_default())
                    .protocol_data("".into())
                    .members(Some(members))
                    .authorized_operations(Some(-1))
            }

            NamedGroupDetail {
                name,
                response: GroupDetailResponse::ErrorCode(error_code),
            } => Self::default()
                .error_code((*error_code).into())
                .group_id(name.clone())
                .group_state("Unknown".into())
                .protocol_type("".into())
                .protocol_data("".into())
                .members(Some(vec![]))
                .authorized_operations(Some(-1)),
        }
    }
}

/// Topition (topic partition) Detail
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopitionDetail {
    error: ErrorCode,
    topic: TopicId,
    partitions: Option<Vec<PartitionDetail>>,
}

/// Partition Detail
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PartitionDetail {
    error: ErrorCode,
    partition_index: i32,
}

/// Version representing an `e_tag` and `version` used in conditional writes to an object store.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Version {
    e_tag: Option<String>,
    version: Option<String>,
}

impl From<&Uuid> for Version {
    fn from(value: &Uuid) -> Self {
        Self {
            e_tag: Some(value.to_string()),
            version: None,
        }
    }
}

/// Producer Id Response
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ProducerIdResponse {
    pub error: ErrorCode,
    pub id: i64,
    pub epoch: i16,
}

impl Default for ProducerIdResponse {
    fn default() -> Self {
        Self {
            error: ErrorCode::None,
            id: 1,
            epoch: 0,
        }
    }
}

/// For protocol versions 0..=3 using [`AddPartitionsToTxnTopic`],
/// thereafter using [`AddPartitionsToTxnTransaction`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TxnAddPartitionsRequest {
    VersionZeroToThree {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        topics: Vec<AddPartitionsToTxnTopic>,
    },

    VersionFourPlus {
        transactions: Vec<AddPartitionsToTxnTransaction>,
    },
}

impl TryFrom<AddPartitionsToTxnRequest> for TxnAddPartitionsRequest {
    type Error = Error;

    fn try_from(value: AddPartitionsToTxnRequest) -> result::Result<Self, Self::Error> {
        match value {
            AddPartitionsToTxnRequest {
                transactions: None,
                v_3_and_below_transactional_id: Some(transactional_id),
                v_3_and_below_producer_id: Some(producer_id),
                v_3_and_below_producer_epoch: Some(producer_epoch),
                v_3_and_below_topics: Some(topics),
                ..
            } => Ok(Self::VersionZeroToThree {
                transaction_id: transactional_id,
                producer_id,
                producer_epoch,
                topics,
            }),

            AddPartitionsToTxnRequest {
                transactions: Some(transactions),
                v_3_and_below_transactional_id: None,
                v_3_and_below_producer_id: None,
                v_3_and_below_producer_epoch: None,
                v_3_and_below_topics: None,
                ..
            } => Ok(Self::VersionFourPlus { transactions }),

            unexpected => Err(Error::UnexpectedAddPartitionsToTxnRequest(Box::new(
                unexpected,
            ))),
        }
    }
}

/// Transaction Add Partitions Response
///
/// For protocol versions 0..=3 using `AddPartitionsToTxnTopic`, thereafter using `AddPartitionsToTxnTransaction`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TxnAddPartitionsResponse {
    VersionZeroToThree(Vec<AddPartitionsToTxnTopicResult>),
    VersionFourPlus(Vec<AddPartitionsToTxnResult>),
}

impl TxnAddPartitionsResponse {
    pub fn zero_to_three(&self) -> &[AddPartitionsToTxnTopicResult] {
        match self {
            Self::VersionZeroToThree(result) => result.as_slice(),
            Self::VersionFourPlus(_) => &[][..],
        }
    }

    pub fn four_plus(&self) -> &[AddPartitionsToTxnResult] {
        match self {
            Self::VersionZeroToThree(_) => &[][..],
            Self::VersionFourPlus(result) => result.as_slice(),
        }
    }
}

/// Transaction Offset Commit Request
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TxnOffsetCommitRequest {
    pub transaction_id: String,
    pub group_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub generation_id: Option<i32>,
    pub member_id: Option<String>,
    pub group_instance_id: Option<String>,
    pub topics: Vec<TxnOffsetCommitRequestTopic>,
}

/// Transaction State
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TxnState {
    Begin,
    PrepareCommit,
    PrepareAbort,
    Committed,
    Aborted,
}

impl TxnState {
    pub fn is_prepared(&self) -> bool {
        match self {
            Self::PrepareAbort | Self::PrepareCommit => true,
            _otherwise => false,
        }
    }
}

impl FromStr for TxnState {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ABORTED" => Ok(TxnState::Aborted),
            "BEGIN" => Ok(TxnState::Begin),
            "COMMITTED" => Ok(TxnState::Committed),
            "PREPARE_ABORT" => Ok(TxnState::PrepareAbort),
            "PREPARE_COMMIT" => Ok(TxnState::PrepareCommit),
            otherwise => Err(Error::UnknownTxnState(otherwise.to_owned())),
        }
    }
}

impl TryFrom<String> for TxnState {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<TxnState> for String {
    fn from(value: TxnState) -> Self {
        match value {
            TxnState::Begin => "BEGIN".into(),
            TxnState::PrepareCommit => "PREPARE_COMMIT".into(),
            TxnState::PrepareAbort => "PREPARE_ABORT".into(),
            TxnState::Committed => "COMMITTED".into(),
            TxnState::Aborted => "ABORTED".into(),
        }
    }
}

/// Storage
///
/// The Core storage abstraction. All storage engines implement this type.
#[async_trait]
pub trait Storage: Debug + Send + Sync + 'static {
    /// On startup a broker will register with storage.
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()>;

    /// Create a topic on this storage.
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid>;

    /// Incrementally alter a resource on this storage.
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse>;

    /// Delete records on this storage.
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>>;

    /// Delete a topic from this storage.
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode>;

    /// Query the brokers registered with this storage.
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>>;

    /// Produce a deflated batch to this storage.
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64>;

    /// Fetch deflated batches from storage.
    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>>;

    /// Query the offset stage for a topic partition.
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage>;

    /// Query the offsets for one or more topic partitions.
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>>;

    /// Commit offsets for one or more topic partitions in a consumer group.
    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>>;

    /// Fetch committed offsets for one or more topic partitions in a consumer group.
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>>;

    /// Fetch all committed offsets in a consumer group.
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>>;

    /// Query broker and topic metadata.
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse>;

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()>;

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()>;

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>>;

    /// Query the configuration of a resource in this storage.
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult>;

    /// Query available groups optionally with a state filter.
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>>;

    /// Delete one or more groups from storage.
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>>;

    /// Describe the groups found in this storage.
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>>;

    /// Describe the topic partitions found in this storage.
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>>;

    /// Conditionally update the state of a group in this storage.
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>>;

    /// Initialise a transactional or idempotent producer in this storage.
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse>;

    /// Add offsets to a transaction for a producer.
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode>;

    /// Add partitions to a transaction.
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse>;

    /// Commit an offset within a transaction.
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>>;

    /// Commit or abort a running transaction.
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode>;

    /// Run periodic maintenance on this storage.
    async fn maintain(&self, _now: SystemTime) -> Result<()>;

    /// Abort transactions whose timeout has elapsed, releasing the Last Stable Offset they pin.
    async fn maintain_transactions(&self, _now: SystemTime) -> Result<()>;

    /// Aborted transactions overlapping `[offset, last_stable_offset)`, reporting which producer
    /// ranges a `read_committed` Fetch client should discard. Postgres implements this; other
    /// backends return `Ok(vec![])` or forward to an inner storage.
    async fn aborted_transactions(
        &self,
        _topition: &Topition,
        _offset: i64,
        _last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>>;

    async fn cluster_id(&self) -> Result<String>;

    async fn node(&self) -> Result<i32>;

    async fn advertised_listener(&self) -> Result<Url>;

    async fn ping(&self) -> Result<()>;
}

// The existence of this function makes the compiler catch if the Storage
// trait is "object-safe" or not.
fn _assert_trait_object(_s: &dyn Storage) {}

#[async_trait]
impl<T> Storage for Arc<T>
where
    T: Storage + ?Sized,
{
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.as_ref().register_broker(broker_registration).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.as_ref().create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.as_ref().incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.as_ref().delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.as_ref().delete_topic(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.as_ref().brokers().await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        self.as_ref().produce(transaction_id, topition, batch).await
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        self.as_ref()
            .fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.as_ref().offset_stage(topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.as_ref().list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.as_ref()
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.as_ref()
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        self.as_ref().committed_offset_topitions(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.as_ref().metadata(topics).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.as_ref()
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.as_ref()
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.as_ref().user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.as_ref().describe_config(name, resource, keys).await
    }

    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        self.as_ref().list_groups(states_filter).await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.as_ref().delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.as_ref()
            .describe_groups(group_ids, include_authorized_operations)
            .await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.as_ref()
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.as_ref().update_group(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.as_ref()
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        self.as_ref()
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.as_ref().txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.as_ref().txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.as_ref()
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.as_ref().maintain(now).await
    }

    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        self.as_ref().maintain_transactions(now).await
    }

    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        self.as_ref()
            .aborted_transactions(topition, offset, last_stable_offset)
            .await
    }

    async fn cluster_id(&self) -> Result<String> {
        self.as_ref().cluster_id().await
    }

    async fn node(&self) -> Result<i32> {
        self.as_ref().node().await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.as_ref().advertised_listener().await
    }

    async fn ping(&self) -> Result<()> {
        self.as_ref().ping().await
    }
}

#[async_trait]
impl<T> Storage for Box<T>
where
    T: Storage + ?Sized,
{
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.as_ref().register_broker(broker_registration).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.as_ref().create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.as_ref().incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.as_ref().delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.as_ref().delete_topic(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.as_ref().brokers().await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        self.as_ref().produce(transaction_id, topition, batch).await
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        self.as_ref()
            .fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.as_ref().offset_stage(topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.as_ref().list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.as_ref()
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        self.as_ref()
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        self.as_ref().committed_offset_topitions(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.as_ref().metadata(topics).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.as_ref()
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.as_ref()
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.as_ref().user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.as_ref().describe_config(name, resource, keys).await
    }

    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        self.as_ref().list_groups(states_filter).await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.as_ref().delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.as_ref()
            .describe_groups(group_ids, include_authorized_operations)
            .await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.as_ref()
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.as_ref().update_group(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.as_ref()
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        self.as_ref()
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.as_ref().txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.as_ref().txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.as_ref()
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.as_ref().maintain(now).await
    }

    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        self.as_ref().maintain_transactions(now).await
    }

    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        self.as_ref()
            .aborted_transactions(topition, offset, last_stable_offset)
            .await
    }

    async fn cluster_id(&self) -> Result<String> {
        self.as_ref().cluster_id().await
    }

    async fn node(&self) -> Result<i32> {
        self.as_ref().node().await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.as_ref().advertised_listener().await
    }

    async fn ping(&self) -> Result<()> {
        self.as_ref().ping().await
    }
}

pub type DynStorage = dyn Storage;
pub type ArcDynStorage = Arc<DynStorage>;

/// Conditional Update Errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum UpdateError<T> {
    Error(#[from] Error),

    MissingEtag,

    Outdated { current: Box<T>, version: Version },

    SerdeJson(Arc<serde_json::Error>),

    Uuid(#[from] uuid::Error),
}

#[cfg(feature = "libsql")]
impl<T> From<libsql::Error> for UpdateError<T> {
    fn from(value: libsql::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

#[cfg(feature = "turso")]
impl<T> From<turso::Error> for UpdateError<T> {
    fn from(value: turso::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

#[cfg(any(feature = "dynostore", feature = "slatedb"))]
impl<T> From<object_store::Error> for UpdateError<T> {
    fn from(value: object_store::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

impl<T> From<serde_json::Error> for UpdateError<T> {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeJson(Arc::new(value))
    }
}

#[cfg(feature = "postgres")]
impl<T> From<tokio_postgres::error::Error> for UpdateError<T> {
    fn from(value: tokio_postgres::error::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

/// Storage Container
#[derive(Clone)]
#[cfg_attr(
    not(any(
        feature = "dynostore",
        feature = "libsql",
        feature = "postgres",
        feature = "slatedb",
        feature = "turso"
    )),
    allow(missing_copy_implementations)
)]
pub enum StorageContainer {
    Null(null::Engine),

    #[cfg(feature = "postgres")]
    Postgres(Postgres),

    #[cfg(feature = "dynostore")]
    DynoStore(DynoStore),

    #[cfg(feature = "libsql")]
    Lite(lite::Engine),

    #[cfg(feature = "slatedb")]
    Slate(slate::Engine),

    #[cfg(feature = "turso")]
    Turso(limbo::Engine),
}

impl Debug for StorageContainer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null(_) => f.debug_tuple(stringify!(StorageContainer::Null)).finish(),

            #[cfg(feature = "postgres")]
            Self::Postgres(_) => f
                .debug_tuple(stringify!(StorageContainer::Postgres))
                .finish(),

            #[cfg(feature = "dynostore")]
            Self::DynoStore(_) => f
                .debug_tuple(stringify!(StorageContainer::DynoStore))
                .finish(),

            #[cfg(feature = "libsql")]
            Self::Lite(_) => f.debug_tuple(stringify!(StorageContainer::Lite)).finish(),

            #[cfg(feature = "slatedb")]
            Self::Slate(_) => f.debug_tuple(stringify!(StorageContainer::Slate)).finish(),

            #[cfg(feature = "turso")]
            Self::Turso(_) => f.debug_tuple(stringify!(StorageContainer::Turso)).finish(),
        }
    }
}

impl StorageContainer {
    pub fn builder() -> PhantomBuilder {
        PhantomBuilder::default()
    }
}

/// A [`StorageContainer`] builder
#[derive(Clone, Debug, Default)]
pub struct Builder<N, C, A, S> {
    node_id: N,
    cluster_id: C,
    advertised_listener: A,
    storage: S,
    schema_registry: Option<Registry>,
    lake_house: Option<House>,
    silent: bool,

    cancellation: CancellationToken,
}

type PhantomBuilder =
    Builder<PhantomData<i32>, PhantomData<String>, PhantomData<Url>, PhantomData<Url>>;

impl<N, C, A, S> Builder<N, C, A, S> {
    pub fn node_id(self, node_id: i32) -> Builder<i32, C, A, S> {
        Builder {
            node_id,
            cluster_id: self.cluster_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            cancellation: self.cancellation,
        }
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> Builder<N, String, A, S> {
        Builder {
            node_id: self.node_id,
            cluster_id: cluster_id.into(),
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            cancellation: self.cancellation,
        }
    }

    pub fn advertised_listener(self, advertised_listener: impl Into<Url>) -> Builder<N, C, Url, S> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            advertised_listener: advertised_listener.into(),
            storage: self.storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            cancellation: self.cancellation,
        }
    }

    pub fn storage(self, storage: Url) -> Builder<N, C, A, Url> {
        debug!(%storage);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            advertised_listener: self.advertised_listener,
            storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            cancellation: self.cancellation,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Registry>) -> Self {
        _ = schema_registry
            .as_ref()
            .inspect(|schema_registry| debug!(?schema_registry));

        Self {
            schema_registry,
            ..self
        }
    }

    pub fn lake_house(self, lake_house: Option<House>) -> Self {
        _ = lake_house
            .as_ref()
            .inspect(|lake_house| debug!(?lake_house));

        Self { lake_house, ..self }
    }

    pub fn cancellation(self, cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            ..self
        }
    }

    pub fn silent(self, silent: bool) -> Self {
        Self { silent, ..self }
    }
}

impl Builder<i32, String, Url, Url> {
    pub async fn build(self) -> Result<Arc<Box<dyn Storage>>> {
        let storage = match self.storage.scheme() {
            #[cfg(feature = "postgres")]
            "postgres" | "postgresql" => Postgres::builder(self.storage.to_string().as_str())
                .map(|builder| builder.cluster(self.cluster_id.as_str()))
                .map(|builder| builder.node(self.node_id))
                .map(|builder| builder.advertised_listener(self.advertised_listener.clone()))
                .map(|builder| builder.schemas(self.schema_registry))
                .map(|builder| builder.lake(self.lake_house.clone()))
                .map(|builder| builder.build())
                .map(|storage| Box::new(storage) as Box<dyn Storage>)
                .map(Arc::new),

            #[cfg(not(feature = "postgres"))]
            "postgres" | "postgresql" => Err(Error::FeatureNotEnabled {
                feature: "postgres".into(),
                message: self.storage.to_string(),
            }),

            #[cfg(feature = "dynostore")]
            "s3" => {
                use crate::batch::ProduceRequestBatcher;

                let bucket_name = self.storage.host_str().unwrap_or("nisshi");

                let minimum_size = self.storage.query_pairs().find_map(|(k, v)| {
                    if k == "batch_min_size" {
                        human_units::Size::from_str(v.as_ref())
                            .map(|size| size.0)
                            .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                            .ok()
                            .and_then(|size| usize::try_from(size).ok())
                    } else {
                        None
                    }
                });

                let maximum_delay = self.storage.query_pairs().find_map(|(k, v)| {
                    if k == "batch_max_delay" {
                        human_units::Duration::from_str(v.as_ref())
                            .map(|duration| duration.0)
                            .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                            .ok()
                    } else {
                        None
                    }
                });

                debug!(?minimum_size, ?maximum_delay);

                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .build()
                    .map(|object_store| {
                        DynoStore::new(self.cluster_id.as_str(), self.node_id, object_store)
                            .advertised_listener(self.advertised_listener.clone())
                            .schemas(self.schema_registry)
                            .lake(self.lake_house.clone())
                    })
                    .map(|storage| {
                        ProduceRequestBatcher::new(storage)
                            .with_minimum_size(minimum_size)
                            .with_maximum_delay(maximum_delay)
                    })
                    .map(|storage| Box::new(storage) as Box<dyn Storage>)
                    .map(Arc::new)
                    .map_err(Into::into)
            }

            #[cfg(feature = "dynostore")]
            "gs" => {
                use std::num::NonZeroU32;

                use object_store::gcp::GoogleCloudStorageBuilder;

                use crate::{batch::ProduceRequestBatcher, gcs::limit::PutRateLimiter};

                let bucket_name = self.storage.host_str().unwrap_or("nisshi");

                let minimum_size = self.storage.query_pairs().find_map(|(k, v)| {
                    if k == "batch_min_size" {
                        human_units::Size::from_str(v.as_ref())
                            .map(|size| size.0)
                            .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                            .ok()
                            .and_then(|size| usize::try_from(size).ok())
                    } else {
                        None
                    }
                });

                let maximum_delay = self.storage.query_pairs().find_map(|(k, v)| {
                    if k == "batch_max_delay" {
                        human_units::Duration::from_str(v.as_ref())
                            .map(|duration| duration.0)
                            .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                            .ok()
                    } else {
                        None
                    }
                });

                GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket_name)
                    .build()
                    .map(|object_store| {
                        PutRateLimiter::new(object_store, Duration::from_mins(5))
                            .with_rate_per_second(NonZeroU32::new(1))
                            .with_jitter(Some(Duration::from_millis(50)))
                    })
                    .map(|object_store| {
                        DynoStore::new(self.cluster_id.as_str(), self.node_id, object_store)
                            .advertised_listener(self.advertised_listener.clone())
                            .schemas(self.schema_registry)
                            .lake(self.lake_house.clone())
                    })
                    .map(|storage| {
                        ProduceRequestBatcher::new(storage)
                            .with_minimum_size(minimum_size)
                            .with_maximum_delay(maximum_delay)
                    })
                    .map(|storage| Box::new(storage) as Box<dyn Storage>)
                    .map(Arc::new)
                    .map_err(Into::into)
            }

            #[cfg(feature = "dynostore")]
            "memory" => Ok(
                DynoStore::new(self.cluster_id.as_str(), self.node_id, InMemory::new())
                    .advertised_listener(self.advertised_listener.clone())
                    .schemas(self.schema_registry)
                    .lake(self.lake_house.clone()),
            )
            .map(|storage| Box::new(storage) as Box<dyn Storage>)
            .map(Arc::new),

            #[cfg(not(feature = "dynostore"))]
            "s3" | "memory" => Err(Error::FeatureNotEnabled {
                feature: "dynostore".into(),
                message: self.storage.to_string(),
            }),

            #[cfg(feature = "libsql")]
            "sqlite" => {
                lite::Engine::builder()
                    .storage(self.storage.clone())
                    .node(self.node_id)
                    .cluster(self.cluster_id.clone())
                    .advertised_listener(self.advertised_listener.clone())
                    .schemas(self.schema_registry)
                    .lake(self.lake_house.clone())
                    .cancellation(self.cancellation.clone())
                    .build()
                    .await
            }

            #[cfg(not(feature = "libsql"))]
            "sqlite" => Err(Error::FeatureNotEnabled {
                feature: "libsql".into(),
                message: self.storage.to_string(),
            }),

            #[cfg(feature = "slatedb")]
            "slatedb" => {
                use slatedb::Db;
                use slatedb::object_store::{
                    ObjectStore as SlateObjectStore,
                    aws::{
                        AmazonS3Builder as SlateS3Builder,
                        S3ConditionalPut as SlateS3ConditionalPut,
                    },
                    memory::InMemory as SlateInMemory,
                };

                let host = self.storage.host_str().unwrap_or("nisshi");
                let db_path = format!("nisshi-{}.slatedb", self.cluster_id);

                // Support memory backend for testing: slatedb://memory
                let object_store: Arc<dyn SlateObjectStore> = if host == "memory" {
                    Arc::new(SlateInMemory::new())
                } else {
                    // Use S3 backend with host as bucket name
                    SlateS3Builder::from_env()
                        .with_bucket_name(host)
                        .with_conditional_put(SlateS3ConditionalPut::ETagMatch)
                        .build()
                        .map(Arc::new)
                        .map_err(|e| Error::Message(e.to_string()))?
                };

                Db::open(db_path, object_store)
                    .await
                    .map(Arc::new)
                    .map(|db| {
                        slate::Engine::builder()
                            .cluster(self.cluster_id.clone())
                            .node(self.node_id)
                            .advertised_listener(self.advertised_listener.clone())
                            .db(db)
                            .schemas(self.schema_registry)
                            .lake(self.lake_house)
                            .build()
                    })
                    .map(|storage| Box::new(storage) as Box<dyn Storage>)
                    .map(Arc::new)
                    .map_err(Into::into)
            }

            #[cfg(not(feature = "slatedb"))]
            "slatedb" => Err(Error::FeatureNotEnabled {
                feature: "slatedb".into(),
                message: self.storage.to_string(),
            }),

            #[cfg(feature = "turso")]
            "turso" => limbo::Engine::builder()
                .storage(self.storage.clone())
                .node(self.node_id)
                .cluster(self.cluster_id.clone())
                .advertised_listener(self.advertised_listener.clone())
                .schemas(self.schema_registry)
                .lake(self.lake_house.clone())
                .build()
                .await
                .map(|storage| Box::new(storage) as Box<dyn Storage>)
                .map(Arc::new),

            #[cfg(not(feature = "turso"))]
            "turso" => Err(Error::FeatureNotEnabled {
                feature: "turso".into(),
                message: self.storage.to_string(),
            }),

            "null" => Ok(null::Engine::new(
                self.cluster_id.clone(),
                self.node_id,
                self.advertised_listener.clone(),
            ))
            .map(|storage| Box::new(storage) as Box<dyn Storage>)
            .map(Arc::new),

            #[cfg(not(any(
                feature = "dynostore",
                feature = "libsql",
                feature = "postgres",
                feature = "slatedb",
                feature = "turso"
            )))]
            _storage => Ok(null::Engine::new(
                self.cluster_id.clone(),
                self.node_id,
                self.advertised_listener.clone(),
            ))
            .map(|storage| Box::new(storage) as Box<dyn Storage>)
            .map(Arc::new),

            #[cfg(any(
                feature = "dynostore",
                feature = "libsql",
                feature = "postgres",
                feature = "slatedb",
                feature = "turso"
            ))]
            _unsupported => Err(Error::UnsupportedStorageUrl(self.storage.clone())),
        }?;

        let pb = if self.silent {
            None
        } else {
            let pb = ProgressBar::new(1);
            pb.set_style(
                ProgressStyle::with_template("[{elapsed}] {bar:40.cyan/blue} {msg}")
                    .unwrap()
                    .progress_chars("##-"),
            );

            pb.set_message("connecting to storage");

            Some(pb)
        };

        storage.ping().await?;

        if let Some(pb) = pb {
            pb.inc(1);
            pb.finish_with_message(format!("{} connected to storage", Emoji("✅", ""),));
        }

        Ok(storage)
    }
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

static STORAGE_CONTAINER_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_container_requests")
        .with_description("nisshi storage container requests")
        .build()
});

static STORAGE_CONTAINER_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_container_errors")
        .with_description("nisshi storage container errors")
        .build()
});

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScramCredential {
    pub salt: Bytes,
    pub iterations: i32,
    pub stored_key: Bytes,
    pub server_key: Bytes,
}

#[async_trait]
impl Storage for StorageContainer {
    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        let attributes = [KeyValue::new("method", "register_broker")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.register_broker(broker_registration),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.register_broker(broker_registration),

            Self::Null(engine) => engine.register_broker(broker_registration),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.register_broker(broker_registration),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.register_broker(broker_registration),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.register_broker(broker_registration),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let attributes = [KeyValue::new("method", "incremental_alter_resource")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.incremental_alter_resource(resource),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.incremental_alter_resource(resource),

            Self::Null(engine) => engine.incremental_alter_resource(resource),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.incremental_alter_resource(resource),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.incremental_alter_resource(resource),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.incremental_alter_resource(resource),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        let attributes = [KeyValue::new("method", "create_topic")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.create_topic(topic, validate_only),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.create_topic(topic, validate_only),

            Self::Null(engine) => engine.create_topic(topic, validate_only),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.create_topic(topic, validate_only),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.create_topic(topic, validate_only),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.create_topic(topic, validate_only),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let attributes = [KeyValue::new("method", "delete_records")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.delete_records(topics),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.delete_records(topics),

            Self::Null(engine) => engine.delete_records(topics),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.delete_records(topics),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.delete_records(topics),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.delete_records(topics),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let attributes = [KeyValue::new("method", "delete_topic")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.delete_topic(topic),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.delete_topic(topic),

            Self::Null(engine) => engine.delete_topic(topic),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.delete_topic(topic),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.delete_topic(topic),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.delete_topic(topic),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let attributes = [KeyValue::new("method", "brokers")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.brokers(),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.brokers(),

            Self::Null(engine) => engine.brokers(),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.brokers(),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.brokers(),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.brokers(),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        let attributes = [KeyValue::new("method", "produce")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.produce(transaction_id, topition, batch),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.produce(transaction_id, topition, batch),

            Self::Null(engine) => engine.produce(transaction_id, topition, batch),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.produce(transaction_id, topition, batch),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.produce(transaction_id, topition, batch),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.produce(transaction_id, topition, batch),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let attributes = [KeyValue::new("method", "fetch")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => {
                engine.fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            }

            Self::Null(engine) => {
                engine.fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            }

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine.fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => {
                engine.fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            }

            #[cfg(feature = "turso")]
            Self::Turso(engine) => {
                engine.fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            }
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let attributes = [KeyValue::new("method", "offset_stage")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.offset_stage(topition),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.offset_stage(topition),

            Self::Null(engine) => engine.offset_stage(topition),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.offset_stage(topition),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_stage(topition),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.offset_stage(topition),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let attributes = [KeyValue::new("method", "list_offsets")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.list_offsets(isolation_level, offsets),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.list_offsets(isolation_level, offsets),

            Self::Null(engine) => engine.list_offsets(isolation_level, offsets),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.list_offsets(isolation_level, offsets),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.list_offsets(isolation_level, offsets),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.list_offsets(isolation_level, offsets),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let attributes = [KeyValue::new("method", "offset_commit")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            Self::Null(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.offset_commit(group_id, retention_time_ms, offsets),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let attributes = [KeyValue::new("method", "committed_offset_topitions")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.committed_offset_topitions(group_id),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.committed_offset_topitions(group_id),

            Self::Null(engine) => engine.committed_offset_topitions(group_id),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.committed_offset_topitions(group_id),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.committed_offset_topitions(group_id),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.committed_offset_topitions(group_id),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let attributes = [KeyValue::new("method", "offset_fetch")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.offset_fetch(group_id, topics, require_stable),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.offset_fetch(group_id, topics, require_stable),

            Self::Null(engine) => engine.offset_fetch(group_id, topics, require_stable),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.offset_fetch(group_id, topics, require_stable),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.offset_fetch(group_id, topics, require_stable),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.offset_fetch(group_id, topics, require_stable),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let attributes = [KeyValue::new("method", "metadata")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.metadata(topics),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.metadata(topics),

            Self::Null(engine) => engine.metadata(topics),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.metadata(topics),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.metadata(topics),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.metadata(topics),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let attributes = [KeyValue::new("method", "describe_config")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.describe_config(name, resource, keys),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.describe_config(name, resource, keys),

            Self::Null(engine) => engine.describe_config(name, resource, keys),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.describe_config(name, resource, keys),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.describe_config(name, resource, keys),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.describe_config(name, resource, keys),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let attributes = [KeyValue::new("method", "describe_topic_partitions")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.describe_topic_partitions(topics, partition_limit, cursor)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.describe_topic_partitions(topics, partition_limit, cursor),

            Self::Null(engine) => engine.describe_topic_partitions(topics, partition_limit, cursor),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine.describe_topic_partitions(topics, partition_limit, cursor)
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => {
                engine.describe_topic_partitions(topics, partition_limit, cursor)
            }

            #[cfg(feature = "turso")]
            Self::Turso(engine) => {
                engine.describe_topic_partitions(topics, partition_limit, cursor)
            }
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        let attributes = [KeyValue::new("method", "list_groups")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.list_groups(states_filter),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.list_groups(states_filter),

            Self::Null(engine) => engine.list_groups(states_filter),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.list_groups(states_filter),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.list_groups(states_filter),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.list_groups(states_filter),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let attributes = [KeyValue::new("method", "delete_groups")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.delete_groups(group_ids),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.delete_groups(group_ids),

            Self::Null(engine) => engine.delete_groups(group_ids),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.delete_groups(group_ids),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.delete_groups(group_ids),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.delete_groups(group_ids),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let attributes = [KeyValue::new("method", "describe_groups")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.describe_groups(group_ids, include_authorized_operations)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.describe_groups(group_ids, include_authorized_operations),

            Self::Null(engine) => engine.describe_groups(group_ids, include_authorized_operations),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine.describe_groups(group_ids, include_authorized_operations)
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.describe_groups(group_ids, include_authorized_operations),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.describe_groups(group_ids, include_authorized_operations),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let attributes = [KeyValue::new("method", "update_group")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.update_group(group_id, detail, version),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.update_group(group_id, detail, version),

            Self::Null(engine) => engine.update_group(group_id, detail, version),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.update_group(group_id, detail, version),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.update_group(group_id, detail, version),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.update_group(group_id, detail, version),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let attributes = [KeyValue::new("method", "init_producer")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            ),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            ),

            Self::Null(engine) => engine.init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            ),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            ),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            ),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            ),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let attributes = [KeyValue::new("method", "txn_add_offsets")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => {
                engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            }

            Self::Null(engine) => {
                engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            }

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => {
                engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            }

            #[cfg(feature = "turso")]
            Self::Turso(engine) => {
                engine.txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            }
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let attributes = [KeyValue::new("method", "txn_add_partitions")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.txn_add_partitions(partitions),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.txn_add_partitions(partitions),

            Self::Null(engine) => engine.txn_add_partitions(partitions),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.txn_add_partitions(partitions),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.txn_add_partitions(partitions),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.txn_add_partitions(partitions),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let attributes = [KeyValue::new("method", "txn_offset_commit")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.txn_offset_commit(offsets),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.txn_offset_commit(offsets),

            Self::Null(engine) => engine.txn_offset_commit(offsets),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.txn_offset_commit(offsets),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.txn_offset_commit(offsets),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.txn_offset_commit(offsets),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let attributes = [KeyValue::new("method", "txn_end")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => {
                engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
            }

            Self::Null(engine) => {
                engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
            }

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => {
                engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
            }

            #[cfg(feature = "turso")]
            Self::Turso(engine) => {
                engine.txn_end(transaction_id, producer_id, producer_epoch, committed)
            }
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        let attributes = [KeyValue::new("method", "maintain")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.maintain(now),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.maintain(now),

            Self::Null(engine) => engine.maintain(now),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.maintain(now),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.maintain(now),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.maintain(now),
        }
        .await
        .inspect(|maintain| {
            debug!(?maintain);
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|err| {
            debug!(?err);
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        let attributes = [KeyValue::new("method", "maintain_transactions")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.maintain_transactions(now),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.maintain_transactions(now),

            Self::Null(engine) => engine.maintain_transactions(now),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.maintain_transactions(now),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.maintain_transactions(now),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.maintain_transactions(now),
        }
        .await
        .inspect(|maintain| {
            debug!(?maintain);
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|err| {
            debug!(?err);
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        let attributes = [KeyValue::new("method", "aborted_transactions")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine.aborted_transactions(topition, offset, last_stable_offset)
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.aborted_transactions(topition, offset, last_stable_offset),

            Self::Null(engine) => engine.aborted_transactions(topition, offset, last_stable_offset),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine.aborted_transactions(topition, offset, last_stable_offset)
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => {
                engine.aborted_transactions(topition, offset, last_stable_offset)
            }

            #[cfg(feature = "turso")]
            Self::Turso(engine) => {
                engine.aborted_transactions(topition, offset, last_stable_offset)
            }
        }
        .await
        .inspect(|aborted_transactions| {
            debug!(?aborted_transactions);
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|err| {
            debug!(?err);
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.cluster_id().await,

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.cluster_id().await,

            Self::Null(engine) => engine.cluster_id().await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.cluster_id().await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.cluster_id().await,

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.cluster_id().await,
        }
    }

    #[instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.node().await,

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.node().await,

            Self::Null(engine) => engine.node().await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.node().await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.node().await,

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.node().await,
        }
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.advertised_listener().await,

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.advertised_listener().await,

            Self::Null(engine) => engine.advertised_listener().await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.advertised_listener().await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.advertised_listener().await,

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.advertised_listener().await,
        }
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            Self::Null(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.delete_user_scram_credential(user, mechanism).await,

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.delete_user_scram_credential(user, mechanism).await,
        }
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            Self::Null(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }

            #[cfg(feature = "turso")]
            Self::Turso(engine) => {
                engine
                    .upsert_user_scram_credential(user, mechanism, credential)
                    .await
            }
        }
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.user_scram_credential(user, mechanism).await,

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.user_scram_credential(user, mechanism).await,

            Self::Null(engine) => engine.user_scram_credential(user, mechanism).await,

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.user_scram_credential(user, mechanism).await,

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.user_scram_credential(user, mechanism).await,

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.user_scram_credential(user, mechanism).await,
        }
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        let attributes = [KeyValue::new("method", "ping")];

        match self {
            #[cfg(feature = "dynostore")]
            Self::DynoStore(engine) => engine.ping(),

            #[cfg(feature = "libsql")]
            Self::Lite(engine) => engine.ping(),

            Self::Null(engine) => engine.ping(),

            #[cfg(feature = "postgres")]
            Self::Postgres(engine) => engine.ping(),

            #[cfg(feature = "turso")]
            Self::Turso(engine) => engine.ping(),

            #[cfg(feature = "slatedb")]
            Self::Slate(engine) => engine.ping(),
        }
        .await
        .inspect(|_| {
            STORAGE_CONTAINER_REQUESTS.add(1, &attributes);
        })
        .inspect_err(|_| {
            STORAGE_CONTAINER_ERRORS.add(1, &attributes);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topition_from_str() -> Result<()> {
        let topition = Topition::from_str("qwerty-2147483647")?;
        assert_eq!("qwerty", topition.topic());
        assert_eq!(i32::MAX, topition.partition());
        Ok(())
    }

    #[test]
    fn topic_with_dashes_in_name() -> Result<()> {
        let topition = Topition::from_str("test-topic-0000000-eFC79C8-2147483647")?;
        assert_eq!("test-topic-0000000-eFC79C8", topition.topic());
        assert_eq!(i32::MAX, topition.partition());
        Ok(())
    }
}
