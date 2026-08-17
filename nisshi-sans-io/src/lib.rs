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
//! A Kafka protocol implementation that performs no I/O (it operates only on bytes)
//!
//! ## Design
//!
//! Apache Kafka defines each API message with a JSON message descriptor. Each descriptor
//! contains a list of fields together with their associated type. Each field can
//! include a range of versions for which it is valid, its encoding and whether it
//! includes tagged fields. Further background on the protocol and implementation
//! used are in the
//! [Apache Kafka protocol with serde, quote, syn and proc_macro2](https://blog.nisshi.io/articles/serde-kafka-protocol)
//! article.
//!
//! Some useful starting points:
//!
//! - **Data Structures** - [`Frame`], [`Request`], [`Response`], [`Header`] and [`Body`].
//! - **Producing or fetching messages** - [`record`], [`ProduceRequest`] and [`FetchRequest`]
//!
//! ## Examples
//!
//! Encoding a [`CreateTopicsRequest`] request:
//!
//! ```
//! # use nisshi_sans_io::Error;
//! # fn main() -> Result<(), Error> {
//! use nisshi_sans_io::{
//!     ApiKey as _, CreateTopicsRequest, Frame, Header,
//!     create_topics_request::{CreatableTopic, CreatableTopicConfig},
//! };
//!
//! let header = Header::Request {
//!     api_key: CreateTopicsRequest::KEY,
//!     api_version: 7,
//!     correlation_id: 298,
//!     client_id: Some("adminclient-1".into()),
//! };
//!
//! let body = CreateTopicsRequest::default()
//!     .topics(Some(
//!         [CreatableTopic::default()
//!             .name("balances".into())
//!             .num_partitions(-1)
//!             .replication_factor(-1)
//!             .assignments(Some([].into()))
//!             .configs(Some(
//!                 [CreatableTopicConfig::default()
//!                     .name("cleanup.policy".into())
//!                     .value(Some("compact".into()))]
//!                 .into(),
//!             ))]
//!         .into(),
//!     ))
//!     .timeout_ms(30_000)
//!     .validate_only(Some(false))
//!     .into();
//!
//! let encoded = Frame::request(header, body)?;
//! # Ok(())
//! # }
//! ```
//!
//! Decoding a [`FindCoordinatorRequest`]:
//!
//! ```
//! # use nisshi_sans_io::Error;
//! # fn main() -> Result<(), Error> {
//! use nisshi_sans_io::{ApiKey as _, FindCoordinatorRequest, Frame, Header};
//!
//! let encoded = vec![
//!     0, 0, 0, 50, 0, 10, 0, 4, 0, 0, 0, 0, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99, 111,
//!     110, 115, 117, 109, 101, 114, 0, 0, 2, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115, 117,
//!     109, 101, 114, 45, 103, 114, 111, 117, 112, 0,
//! ];
//!
//! assert_eq!(
//!     Frame {
//!         size: 50,
//!         header: Header::Request {
//!             api_key: FindCoordinatorRequest::KEY,
//!             api_version: 4,
//!             correlation_id: 0,
//!             client_id: Some("console-consumer".into())
//!         },
//!         body: FindCoordinatorRequest::default()
//!             .key(None)
//!             .key_type(Some(0))
//!             .coordinator_keys(Some(["test-consumer-group".into()].into()))
//!             .into()
//!     },
//!     Frame::request_from_bytes(&encoded[..])?
//! );
//! # Ok(())
//! # }
//! ```
//!
//! This crate includes a build time proc macro that generates simple Rust structures
//! containing all the fields present in the the Kafka message descriptor. Each generated
//! type implements [`serde::Serialize`] and [`serde::Deserialize`] traits. As part of
//! the generation phase [`MESSAGE_META`] is created, which is used by the actual message serializers.
//!
//! The Kafka protocol is implemented by [`ser::Encoder`] and [`de::Decoder`],
//! using [`MESSAGE_META`] to determine which fields are present, their serialization type
//! and whether any tagged fields can be present for a particular message version. Serializers
//! map from the [Serde Data Model](https://serde.rs/data-model.html) to the Kafka protocol or vice versa.

pub mod acl;
pub mod consumer;
pub mod de;
pub mod primitive;
pub mod record;
pub mod resource;
pub mod ser;

use bytes::{Buf, BufMut, Bytes, BytesMut, TryGetError};
pub use de::Decoder;
use flate2::read::GzDecoder;
use nisshi_model::{MessageKind, MessageMeta};
use primitive::tagged::TagBuffer;
use record::deflated::Frame as RecordBatch;
pub use ser::Encoder;
use serde::{Deserialize, Serialize};
use std::{
    array::TryFromSliceError,
    collections::HashMap,
    env::VarError,
    fmt::{self, Display, Formatter},
    io::{self, BufRead, Cursor, Read},
    num,
    process::{ExitCode, Termination},
    str::{self, FromStr},
    string,
    sync::{Arc, OnceLock, PoisonError},
    time::{Duration, SystemTime, SystemTimeError},
};
use tracing::{debug, error, instrument, warn};
use tracing_subscriber::filter::ParseError;

/// The null topic identifier.
pub const NULL_TOPIC_ID: [u8; 16] = [0; 16];

pub trait ByteSize {
    fn size_in_bytes(&self) -> Result<usize>;
}

pub trait MaximumAllocationSize {
    fn maximum_allocation_size(&self) -> Result<usize>;
}

impl<T> MaximumAllocationSize for T
where
    T: ByteSize,
{
    fn maximum_allocation_size(&self) -> Result<usize> {
        self.size_in_bytes()
    }
}

impl MaximumAllocationSize for Bytes {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<i32>() + self.len())
    }
}

impl MaximumAllocationSize for f64 {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<f64>())
    }
}

impl MaximumAllocationSize for i16 {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<i16>())
    }
}

impl MaximumAllocationSize for i32 {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<i32>())
    }
}

impl MaximumAllocationSize for i64 {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<i64>())
    }
}

impl MaximumAllocationSize for bool {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<bool>())
    }
}

impl MaximumAllocationSize for i8 {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<i8>())
    }
}

impl MaximumAllocationSize for String {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<i16>() + self.len())
    }
}

impl MaximumAllocationSize for u16 {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<u16>())
    }
}

impl MaximumAllocationSize for [u8; 16] {
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(size_of::<u8>() * 16)
    }
}

impl<T> MaximumAllocationSize for Vec<T>
where
    T: MaximumAllocationSize,
{
    fn maximum_allocation_size(&self) -> Result<usize> {
        self.iter()
            .map(MaximumAllocationSize::maximum_allocation_size)
            .collect::<Result<Vec<_>>>()
            .map(|elements| size_of::<i32>() + elements.iter().sum::<usize>())
    }
}

impl<T> MaximumAllocationSize for Option<T>
where
    T: MaximumAllocationSize,
{
    fn maximum_allocation_size(&self) -> Result<usize> {
        self.as_ref().map_or(
            Ok(size_of::<i32>()),
            MaximumAllocationSize::maximum_allocation_size,
        )
    }
}

#[derive(Debug)]
pub struct RootMessageMeta {
    pub(crate) requests: HashMap<i16, &'static MessageMeta>,
    pub(crate) responses: HashMap<i16, &'static MessageMeta>,
}

impl RootMessageMeta {
    fn new() -> Self {
        let (requests, responses) = MESSAGE_META.iter().fold(
            (HashMap::new(), HashMap::new()),
            |(mut requests, mut responses), (_, meta)| {
                match meta.message_kind {
                    MessageKind::Request => {
                        _ = requests.insert(meta.api_key, *meta);
                    }

                    MessageKind::Response => {
                        _ = responses.insert(meta.api_key, *meta);
                    }
                }

                (requests, responses)
            },
        );

        Self {
            requests,
            responses,
        }
    }

    pub fn messages() -> &'static RootMessageMeta {
        static MAPPING: OnceLock<RootMessageMeta> = OnceLock::new();
        MAPPING.get_or_init(RootMessageMeta::new)
    }

    #[must_use]
    pub const fn requests(&self) -> &HashMap<i16, &'static MessageMeta> {
        &self.requests
    }

    #[must_use]
    pub const fn responses(&self) -> &HashMap<i16, &'static MessageMeta> {
        &self.responses
    }
}

pub trait ApiKey {
    const KEY: i16;
}

pub trait ApiName {
    const NAME: &'static str;
}

/// All Kafka API requests implement this trait
pub trait Request:
    ApiKey + ApiName + fmt::Debug + Default + Into<Body> + Send + Sync + TryFrom<Body> + 'static
{
    type Response: Response;
}

/// All Kafka API responses implement this trait
pub trait Response:
    ApiKey + ApiName + fmt::Debug + Default + Into<Body> + Send + Sync + TryFrom<Body> + 'static
{
    type Request: Request;
}

pub trait IntoVersion {
    fn into_version(self, api_version: i16) -> Self;
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    ApiError(ErrorCode),
    EnvVar(VarError),
    FromUtf8(string::FromUtf8Error),
    InvalidAckValue(i16),
    InvalidConsumerProtocolSubscriptionVersion(i16),
    InvalidCoordinatorType(i8),
    InvalidIsolationLevel(i8),
    InvalidOpType(i8),
    InvalidScramMechanism(i8),
    Io(Arc<io::Error>),
    Message(String),
    MessageMaxSizeExceeded(usize),
    NoSuchField(&'static str),
    NoSuchMessage(&'static str),
    NoSuchRequest(i16),
    NotAuthenticated,
    Overflow,
    ParseFilter(Arc<ParseError>),
    ParseScram(String),
    Poison,
    ResponseFrame,
    Snap(#[from] snap::Error),
    StringWithoutApiVersion,
    StringWithoutLength,
    SystemTime(SystemTimeError),
    NisshiModel(nisshi_model::Error),
    TryFromInt(#[from] num::TryFromIntError),
    TryFromSlice(#[from] TryFromSliceError),
    TryGet(Arc<TryGetError>),
    UnexpectedType(String),
    UnknownApiErrorCode(i16),
    UnknownAssignor(String),
    UnknownCompressionType(i16),
    UnknownScramMechanism(i8),
    UnknownContainer,
    Utf8(str::Utf8Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::Message(e) => f.write_str(e),
            e => write!(f, "{e:?}"),
        }
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl serde::ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl From<TryGetError> for Error {
    fn from(value: TryGetError) -> Self {
        Self::TryGet(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

impl From<str::Utf8Error> for Error {
    fn from(value: str::Utf8Error) -> Self {
        Self::Utf8(value)
    }
}

impl From<string::FromUtf8Error> for Error {
    fn from(value: string::FromUtf8Error) -> Self {
        Self::FromUtf8(value)
    }
}

impl From<nisshi_model::Error> for Error {
    fn from(value: nisshi_model::Error) -> Self {
        Self::NisshiModel(value)
    }
}

impl From<VarError> for Error {
    fn from(value: VarError) -> Self {
        Self::EnvVar(value)
    }
}

impl From<SystemTimeError> for Error {
    fn from(value: SystemTimeError) -> Self {
        Self::SystemTime(value)
    }
}

/// A Kafka API frame prefixed with its length, followed by a header and the message body.
///
/// # Examples
///
/// ## Encoding
///
/// Encoding a [`CreateTopicsRequest`] request:
///
/// ```
/// use nisshi_sans_io::{
///     ApiKey as _, CreateTopicsRequest, Frame, Header,
///     create_topics_request::{CreatableTopic, CreatableTopicConfig},
/// };
///
/// let header = Header::Request {
///     api_key: CreateTopicsRequest::KEY,
///     api_version: 7,
///     correlation_id: 298,
///     client_id: Some("adminclient-1".into()),
/// };
///
/// let body = CreateTopicsRequest::default()
///     .topics(Some(
///         [CreatableTopic::default()
///             .name("balances".into())
///             .num_partitions(-1)
///             .replication_factor(-1)
///             .assignments(Some([].into()))
///             .configs(Some(
///                 [CreatableTopicConfig::default()
///                     .name("cleanup.policy".into())
///                     .value(Some("compact".into()))]
///                 .into(),
///             ))]
///         .into(),
///     ))
///     .timeout_ms(30_000)
///     .validate_only(Some(false))
///     .into();
///
/// let encoded = Frame::request(header, body).unwrap();
/// ```
///
/// ## Decoding
///
/// Decoding a [`FindCoordinatorRequest`]:
///
/// ```
/// use nisshi_sans_io::{ApiKey as _, FindCoordinatorRequest, Frame, Header};
///
/// let encoded = vec![
///     0, 0, 0, 50, 0, 10, 0, 4, 0, 0, 0, 0, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99, 111,
///     110, 115, 117, 109, 101, 114, 0, 0, 2, 20, 116, 101, 115, 116, 45, 99, 111, 110, 115, 117,
///     109, 101, 114, 45, 103, 114, 111, 117, 112, 0,
/// ];
///
/// assert_eq!(
///     Frame {
///         size: 50,
///         header: Header::Request {
///             api_key: FindCoordinatorRequest::KEY,
///             api_version: 4,
///             correlation_id: 0,
///             client_id: Some("console-consumer".into())
///         },
///         body: FindCoordinatorRequest::default()
///             .key(None)
///             .key_type(Some(0))
///             .coordinator_keys(Some(["test-consumer-group".into()].into()))
///             .into()
///     },
///     Frame::request_from_bytes(&encoded[..]).unwrap()
/// );
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
pub struct Frame {
    /// The size of this frame.
    pub size: i32,

    /// The frame header.
    pub header: Header,

    /// The frame body.
    pub body: Body,
}

impl IntoVersion for Frame {
    fn into_version(self, api_version: i16) -> Self {
        Self {
            header: self.header.into_version(api_version),
            body: self.body.into_version(api_version),
            ..self
        }
    }
}

impl MaximumAllocationSize for Frame {
    #[instrument(skip_all, ret)]
    fn maximum_allocation_size(&self) -> Result<usize> {
        Ok(self.size.maximum_allocation_size()?
            + self.header.maximum_allocation_size()?
            + self.body.maximum_allocation_size()?)
    }
}

fn fix_length(mut encoded: BytesMut) -> Result<Bytes> {
    let mut sz = encoded.split_to(size_of::<i32>());
    sz.clear();
    sz.put_i32(i32::try_from(encoded.len())?);
    sz.unsplit(encoded);
    sz.truncate(sz.len());
    Ok(sz.freeze())
}

impl Frame {
    fn elapsed_millis(start: SystemTime) -> u64 {
        start
            .elapsed()
            .map_or(0, |duration| duration.as_millis() as u64)
    }

    /// serialize an API request into a frame of bytes
    #[instrument(skip_all)]
    pub fn request(header: Header, body: Body) -> Result<Bytes> {
        let Header::Request { api_version, .. } = header else {
            return Err(Error::ResponseFrame);
        };

        let frame = Frame {
            size: 0,
            header: header.into_version(api_version),
            body: body.into_version(api_version),
        };

        let mut serializer = Encoder::request(
            frame
                .maximum_allocation_size()
                .map(BytesMut::with_capacity)?,
        );
        frame.serialize(&mut serializer)?;
        fix_length(BytesMut::from(serializer)).inspect(|encoded| debug!(encoded = ?&encoded[..]))
    }

    /// deserialize bytes into an API request frame
    #[instrument(skip_all)]
    pub fn request_from_bytes(encoded: impl Buf) -> Result<Frame> {
        let start = SystemTime::now();

        let mut reader = encoded.reader();
        let mut deserializer = Decoder::request(&mut reader);
        Frame::deserialize(&mut deserializer)
            .inspect(|frame| debug!(?frame, elapsed_millis = Self::elapsed_millis(start)))
    }

    /// serialize an API response into a frame of bytes
    #[instrument(skip(header, body))]
    pub fn response(header: Header, body: Body, api_key: i16, api_version: i16) -> Result<Bytes> {
        let frame = Frame {
            size: 0,
            header: header.into_version(api_version),
            body: body.into_version(api_version),
        };

        let mut encoder = Encoder::response(
            frame
                .maximum_allocation_size()
                .map(BytesMut::with_capacity)?,
            api_key,
            api_version,
        );
        frame.serialize(&mut encoder)?;
        fix_length(BytesMut::from(encoder)).inspect(|encoded| debug!(encoded = ?&encoded[..]))
    }

    /// deserialize bytes into an API response frame
    #[instrument(skip_all)]
    pub fn response_from_bytes(bytes: impl Buf, api_key: i16, api_version: i16) -> Result<Frame> {
        let start = SystemTime::now();

        let mut reader = bytes.reader();
        let mut deserializer = Decoder::response(&mut reader, api_key, api_version);
        Frame::deserialize(&mut deserializer)
            .inspect(|encoded| debug!(elapsed_millis = Self::elapsed_millis(start)))
    }

    /// API request key
    pub fn api_key(&self) -> Result<i16> {
        if let Header::Request { api_key, .. } = self.header {
            Ok(api_key)
        } else {
            Err(Error::ResponseFrame)
        }
    }

    /// API name
    pub fn api_name(&self) -> &str {
        self.body.api_name()
    }

    /// API request version
    pub fn api_version(&self) -> Result<i16> {
        if let Header::Request { api_version, .. } = self.header {
            Ok(api_version)
        } else {
            Err(Error::ResponseFrame)
        }
    }

    /// API request/response correlation ID
    pub fn correlation_id(&self) -> Result<i32> {
        match self.header {
            Header::Request { correlation_id, .. } | Header::Response { correlation_id } => {
                Ok(correlation_id)
            }
        }
    }

    /// API request client ID
    pub fn client_id(&self) -> Result<Option<&str>> {
        if let Header::Request { ref client_id, .. } = self.header {
            Ok(client_id.as_deref())
        } else {
            Err(Error::ResponseFrame)
        }
    }

    pub fn is_request(&self) -> bool {
        matches!(self.header, Header::Request { .. })
    }

    pub fn is_response(&self) -> bool {
        matches!(self.header, Header::Response { .. })
    }
}

/// A Kafka API request or response header.
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "HeaderMezzanine")]
#[serde(into = "HeaderMezzanine")]
pub enum Header {
    /// An API request header.
    Request {
        /// The API key being used for this request.
        api_key: i16,

        /// The API version being used for this request.
        api_version: i16,

        /// The correlation ID that should be used by the response to this request.
        correlation_id: i32,

        /// An optional client ID.
        client_id: Option<String>,
    },

    /// An API Response header.
    Response {
        /// The correlation ID for the corresponding request.
        correlation_id: i32,
    },
}

impl IntoVersion for Header {
    fn into_version(self, _api_version: i16) -> Self {
        self
    }
}

impl MaximumAllocationSize for Header {
    fn maximum_allocation_size(&self) -> Result<usize> {
        match self {
            Self::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            } => Ok(api_key.maximum_allocation_size()?
                + api_version.maximum_allocation_size()?
                + correlation_id.maximum_allocation_size()?
                + client_id.maximum_allocation_size()?),
            Self::Response { correlation_id } => correlation_id.maximum_allocation_size(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
pub(crate) enum HeaderMezzanine {
    Request {
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
        tag_buffer: Option<TagBuffer>,
    },
    Response {
        correlation_id: i32,
        tag_buffer: Option<TagBuffer>,
    },
}

impl TryFrom<HeaderMezzanine> for Header {
    type Error = Error;

    fn try_from(value: HeaderMezzanine) -> Result<Self, Self::Error> {
        debug!(?value);

        match value {
            HeaderMezzanine::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
                ..
            } => Ok(Self::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            }),

            HeaderMezzanine::Response { correlation_id, .. } => {
                Ok(Self::Response { correlation_id })
            }
        }
    }
}

impl From<Header> for HeaderMezzanine {
    fn from(value: Header) -> Self {
        debug!("value: {value:?}");

        match value {
            Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            } => HeaderMezzanine::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
                tag_buffer: Some(TagBuffer([].into())),
            },

            Header::Response { correlation_id } => HeaderMezzanine::Response {
                correlation_id,
                tag_buffer: Some(TagBuffer([].into())),
            },
        }
    }
}

impl Termination for ErrorCode {
    fn report(self) -> ExitCode {
        if let Self::None = self {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        }
    }
}

impl TryFrom<i16> for ErrorCode {
    type Error = Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl TryFrom<&i16> for ErrorCode {
    type Error = Error;

    #[allow(clippy::too_many_lines)]
    fn try_from(value: &i16) -> Result<Self, Self::Error> {
        match value {
            -1 => Ok(Self::UnknownServerError),
            0 => Ok(Self::None),
            1 => Ok(Self::OffsetOutOfRange),
            2 => Ok(Self::CorruptMessage),
            3 => Ok(Self::UnknownTopicOrPartition),
            4 => Ok(Self::InvalidFetchSize),
            5 => Ok(Self::LeaderNotAvailable),
            6 => Ok(Self::NotLeaderOrFollower),
            7 => Ok(Self::RequestTimedOut),
            8 => Ok(Self::BrokerNotAvailable),
            9 => Ok(Self::ReplicaNotAvailable),
            10 => Ok(Self::MessageTooLarge),
            11 => Ok(Self::StaleControllerEpoch),
            12 => Ok(Self::OffsetMetadataTooLarge),
            13 => Ok(Self::NetworkException),
            14 => Ok(Self::CoordinatorLoadInProgress),
            15 => Ok(Self::CoordinatorNotAvailable),
            16 => Ok(Self::NotCoordinator),
            17 => Ok(Self::InvalidTopicException),
            18 => Ok(Self::RecordListTooLarge),
            19 => Ok(Self::NotEnoughReplicas),
            20 => Ok(Self::NotEnoughReplicasAfterAppend),
            21 => Ok(Self::InvalidRequiredAcks),
            22 => Ok(Self::IllegalGeneration),
            23 => Ok(Self::InconsistentGroupProtocol),
            24 => Ok(Self::InvalidGroupId),
            25 => Ok(Self::UnknownMemberId),
            26 => Ok(Self::InvalidSessionTimeout),
            27 => Ok(Self::RebalanceInProgress),
            28 => Ok(Self::InvalidCommitOffsetSize),
            29 => Ok(Self::TopicAuthorizationFailed),
            30 => Ok(Self::GroupAuthorizationFailed),
            31 => Ok(Self::ClusterAuthorizationFailed),
            32 => Ok(Self::InvalidTimestamp),
            33 => Ok(Self::UnsupportedSaslMechanism),
            34 => Ok(Self::IllegalSaslState),
            35 => Ok(Self::UnsupportedVersion),
            36 => Ok(Self::TopicAlreadyExists),
            37 => Ok(Self::InvalidPartitions),
            38 => Ok(Self::InvalidReplicationFactor),
            39 => Ok(Self::InvalidReplicaAssignment),
            40 => Ok(Self::InvalidConfig),
            41 => Ok(Self::NotController),
            42 => Ok(Self::InvalidRequest),
            43 => Ok(Self::UnsupportedForMessageFormat),
            44 => Ok(Self::PolicyViolation),
            45 => Ok(Self::OutOfOrderSequenceNumber),
            46 => Ok(Self::DuplicateSequenceNumber),
            47 => Ok(Self::InvalidProducerEpoch),
            48 => Ok(Self::InvalidTxnState),
            49 => Ok(Self::InvalidProducerIdMapping),
            50 => Ok(Self::InvalidTransactionTimeout),
            51 => Ok(Self::ConcurrentTransactions),
            52 => Ok(Self::TransactionCoordinatorFenced),
            53 => Ok(Self::TransactionalIdAuthorizationFailed),
            54 => Ok(Self::SecurityDisabled),
            55 => Ok(Self::OperationNotAttempted),
            56 => Ok(Self::KafkaStorageError),
            57 => Ok(Self::LogDirNotFound),
            58 => Ok(Self::SaslAuthenticationFailed),
            59 => Ok(Self::UnknownProducerId),
            60 => Ok(Self::ReassignmentInProgress),
            61 => Ok(Self::DelegationTokenAuthDisabled),
            62 => Ok(Self::DelegationTokenNotFound),
            63 => Ok(Self::DelegationTokenOwnerMismatch),
            64 => Ok(Self::DelegationTokenRequestNotAllowed),
            65 => Ok(Self::DelegationTokenAuthorizationFailed),
            66 => Ok(Self::DelegationTokenExpired),
            67 => Ok(Self::InvalidPrincipalType),
            68 => Ok(Self::NonEmptyGroup),
            69 => Ok(Self::GroupIdNotFound),
            70 => Ok(Self::FetchSessionIdNotFound),
            71 => Ok(Self::InvalidFetchSessionEpoch),
            72 => Ok(Self::ListenerNotFound),
            73 => Ok(Self::TopicDeletionDisabled),
            74 => Ok(Self::FencedLeaderEpoch),
            75 => Ok(Self::UnknownLeaderEpoch),
            76 => Ok(Self::UnsupportedCompressionType),
            77 => Ok(Self::StaleBrokerEpoch),
            78 => Ok(Self::OffsetNotAvailable),
            79 => Ok(Self::MemberIdRequired),
            80 => Ok(Self::PreferredLeaderNotAvailable),
            81 => Ok(Self::GroupMaxSizeReached),
            82 => Ok(Self::FencedInstanceId),
            83 => Ok(Self::EligibleLeadersNotAvailable),
            84 => Ok(Self::ElectionNotNeeded),
            85 => Ok(Self::NoReassignmentInProgress),
            86 => Ok(Self::GroupSubscribedToTopic),
            87 => Ok(Self::InvalidRecord),
            88 => Ok(Self::UnstableOffsetCommit),
            89 => Ok(Self::ThrottlingQuotaExceeded),
            90 => Ok(Self::ProducerFenced),
            91 => Ok(Self::ResourceNotFound),
            92 => Ok(Self::DuplicateResource),
            93 => Ok(Self::UnacceptableCredential),
            94 => Ok(Self::InconsistentVoterSet),
            95 => Ok(Self::InvalidUpdateVersion),
            96 => Ok(Self::FeatureUpdateFailed),
            97 => Ok(Self::PrincipalDeserializationFailure),
            98 => Ok(Self::SnapshotNotFound),
            99 => Ok(Self::PositionOutOfRange),
            100 => Ok(Self::UnknownTopicId),
            101 => Ok(Self::DuplicateBrokerRegistration),
            102 => Ok(Self::BrokerIdNotRegistered),
            103 => Ok(Self::InconsistentTopicId),
            104 => Ok(Self::InconsistentClusterId),
            105 => Ok(Self::TransactionalIdNotFound),
            106 => Ok(Self::FetchSessionTopicIdError),
            107 => Ok(Self::IneligibleReplica),
            108 => Ok(Self::NewLeaderElected),
            109 => Ok(Self::OffsetMovedToTieredStorage),
            110 => Ok(Self::FencedMemberEpoch),
            111 => Ok(Self::UnreleasedInstanceId),
            112 => Ok(Self::UnsupportedAssignor),
            113 => Ok(Self::StaleMemberEpoch),
            114 => Ok(Self::MismatchedEndpointType),
            115 => Ok(Self::UnsupportedEndpointType),
            116 => Ok(Self::UnknownControllerId),
            117 => Ok(Self::UnknownSubscriptionId),
            118 => Ok(Self::TelemetryTooLarge),
            119 => Ok(Self::InvalidRegistration),
            otherwise => Err(Error::UnknownApiErrorCode(*otherwise)),
        }
    }
}

impl From<ErrorCode> for i16 {
    fn from(value: ErrorCode) -> Self {
        Self::from(&value)
    }
}

impl From<&ErrorCode> for i16 {
    #[allow(clippy::too_many_lines)]
    fn from(value: &ErrorCode) -> Self {
        match value {
            ErrorCode::UnknownServerError => -1,
            ErrorCode::None => 0,
            ErrorCode::OffsetOutOfRange => 1,
            ErrorCode::CorruptMessage => 2,
            ErrorCode::UnknownTopicOrPartition => 3,
            ErrorCode::InvalidFetchSize => 4,
            ErrorCode::LeaderNotAvailable => 5,
            ErrorCode::NotLeaderOrFollower => 6,
            ErrorCode::RequestTimedOut => 7,
            ErrorCode::BrokerNotAvailable => 8,
            ErrorCode::ReplicaNotAvailable => 9,
            ErrorCode::MessageTooLarge => 10,
            ErrorCode::StaleControllerEpoch => 11,
            ErrorCode::OffsetMetadataTooLarge => 12,
            ErrorCode::NetworkException => 13,
            ErrorCode::CoordinatorLoadInProgress => 14,
            ErrorCode::CoordinatorNotAvailable => 15,
            ErrorCode::NotCoordinator => 16,
            ErrorCode::InvalidTopicException => 17,
            ErrorCode::RecordListTooLarge => 18,
            ErrorCode::NotEnoughReplicas => 19,
            ErrorCode::NotEnoughReplicasAfterAppend => 20,
            ErrorCode::InvalidRequiredAcks => 21,
            ErrorCode::IllegalGeneration => 22,
            ErrorCode::InconsistentGroupProtocol => 23,
            ErrorCode::InvalidGroupId => 24,
            ErrorCode::UnknownMemberId => 25,
            ErrorCode::InvalidSessionTimeout => 26,
            ErrorCode::RebalanceInProgress => 27,
            ErrorCode::InvalidCommitOffsetSize => 28,
            ErrorCode::TopicAuthorizationFailed => 29,
            ErrorCode::GroupAuthorizationFailed => 30,
            ErrorCode::ClusterAuthorizationFailed => 31,
            ErrorCode::InvalidTimestamp => 32,
            ErrorCode::UnsupportedSaslMechanism => 33,
            ErrorCode::IllegalSaslState => 34,
            ErrorCode::UnsupportedVersion => 35,
            ErrorCode::TopicAlreadyExists => 36,
            ErrorCode::InvalidPartitions => 37,
            ErrorCode::InvalidReplicationFactor => 38,
            ErrorCode::InvalidReplicaAssignment => 39,
            ErrorCode::InvalidConfig => 40,
            ErrorCode::NotController => 41,
            ErrorCode::InvalidRequest => 42,
            ErrorCode::UnsupportedForMessageFormat => 43,
            ErrorCode::PolicyViolation => 44,
            ErrorCode::OutOfOrderSequenceNumber => 45,
            ErrorCode::DuplicateSequenceNumber => 46,
            ErrorCode::InvalidProducerEpoch => 47,
            ErrorCode::InvalidTxnState => 48,
            ErrorCode::InvalidProducerIdMapping => 49,
            ErrorCode::InvalidTransactionTimeout => 50,
            ErrorCode::ConcurrentTransactions => 51,
            ErrorCode::TransactionCoordinatorFenced => 52,
            ErrorCode::TransactionalIdAuthorizationFailed => 53,
            ErrorCode::SecurityDisabled => 54,
            ErrorCode::OperationNotAttempted => 55,
            ErrorCode::KafkaStorageError => 56,
            ErrorCode::LogDirNotFound => 57,
            ErrorCode::SaslAuthenticationFailed => 58,
            ErrorCode::UnknownProducerId => 59,
            ErrorCode::ReassignmentInProgress => 60,
            ErrorCode::DelegationTokenAuthDisabled => 61,
            ErrorCode::DelegationTokenNotFound => 62,
            ErrorCode::DelegationTokenOwnerMismatch => 63,
            ErrorCode::DelegationTokenRequestNotAllowed => 64,
            ErrorCode::DelegationTokenAuthorizationFailed => 65,
            ErrorCode::DelegationTokenExpired => 66,
            ErrorCode::InvalidPrincipalType => 67,
            ErrorCode::NonEmptyGroup => 68,
            ErrorCode::GroupIdNotFound => 69,
            ErrorCode::FetchSessionIdNotFound => 70,
            ErrorCode::InvalidFetchSessionEpoch => 71,
            ErrorCode::ListenerNotFound => 72,
            ErrorCode::TopicDeletionDisabled => 73,
            ErrorCode::FencedLeaderEpoch => 74,
            ErrorCode::UnknownLeaderEpoch => 75,
            ErrorCode::UnsupportedCompressionType => 76,
            ErrorCode::StaleBrokerEpoch => 77,
            ErrorCode::OffsetNotAvailable => 78,
            ErrorCode::MemberIdRequired => 79,
            ErrorCode::PreferredLeaderNotAvailable => 80,
            ErrorCode::GroupMaxSizeReached => 81,
            ErrorCode::FencedInstanceId => 82,
            ErrorCode::EligibleLeadersNotAvailable => 83,
            ErrorCode::ElectionNotNeeded => 84,
            ErrorCode::NoReassignmentInProgress => 85,
            ErrorCode::GroupSubscribedToTopic => 86,
            ErrorCode::InvalidRecord => 87,
            ErrorCode::UnstableOffsetCommit => 88,
            ErrorCode::ThrottlingQuotaExceeded => 89,
            ErrorCode::ProducerFenced => 90,
            ErrorCode::ResourceNotFound => 91,
            ErrorCode::DuplicateResource => 92,
            ErrorCode::UnacceptableCredential => 93,
            ErrorCode::InconsistentVoterSet => 94,
            ErrorCode::InvalidUpdateVersion => 95,
            ErrorCode::FeatureUpdateFailed => 96,
            ErrorCode::PrincipalDeserializationFailure => 97,
            ErrorCode::SnapshotNotFound => 98,
            ErrorCode::PositionOutOfRange => 99,
            ErrorCode::UnknownTopicId => 100,
            ErrorCode::DuplicateBrokerRegistration => 101,
            ErrorCode::BrokerIdNotRegistered => 102,
            ErrorCode::InconsistentTopicId => 103,
            ErrorCode::InconsistentClusterId => 104,
            ErrorCode::TransactionalIdNotFound => 105,
            ErrorCode::FetchSessionTopicIdError => 106,
            ErrorCode::IneligibleReplica => 107,
            ErrorCode::NewLeaderElected => 108,
            ErrorCode::OffsetMovedToTieredStorage => 109,
            ErrorCode::FencedMemberEpoch => 110,
            ErrorCode::UnreleasedInstanceId => 111,
            ErrorCode::UnsupportedAssignor => 112,
            ErrorCode::StaleMemberEpoch => 113,
            ErrorCode::MismatchedEndpointType => 114,
            ErrorCode::UnsupportedEndpointType => 115,
            ErrorCode::UnknownControllerId => 116,
            ErrorCode::UnknownSubscriptionId => 117,
            ErrorCode::TelemetryTooLarge => 118,
            ErrorCode::InvalidRegistration => 119,
        }
    }
}

impl Display for ErrorCode {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCode::UnknownServerError => f.write_str(
                "The server experienced an unexpected error when processing the request.",
            ),
            ErrorCode::None => f.write_str("No error."),
            ErrorCode::OffsetOutOfRange => f.write_str(
                "The requested offset is not within the range of offsets maintained by the server.",
            ),
            ErrorCode::CorruptMessage => f.write_str(
                "This message has failed its CRC checksum, exceeds the valid size, has a null key \
                 for a compacted topic, or is otherwise corrupt.",
            ),
            ErrorCode::UnknownTopicOrPartition => {
                f.write_str("This server does not host this topic-partition.")
            }
            ErrorCode::InvalidFetchSize => f.write_str("The requested fetch size is invalid."),
            ErrorCode::LeaderNotAvailable => f.write_str(
                "There is no leader for this topic-partition as we are in the middle of a \
                 leadership election.",
            ),
            ErrorCode::NotLeaderOrFollower => f.write_str(
                "For requests intended only for the leader, this error indicates that the broker \
                 is not the current leader. For requests intended for any replica, this error \
                 indicates that the broker is not a replica of the topic partition.",
            ),
            ErrorCode::RequestTimedOut => f.write_str("The request timed out."),
            ErrorCode::BrokerNotAvailable => f.write_str("The broker is not available."),
            ErrorCode::ReplicaNotAvailable => f.write_str(
                "The replica is not available for the requested topic-partition. Produce/Fetch \
                 requests and other requests intended only for the leader or follower return \
                 NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.",
            ),
            ErrorCode::MessageTooLarge => f.write_str(
                "The request included a message larger than the max message size the server will \
                 accept.",
            ),
            ErrorCode::StaleControllerEpoch => {
                f.write_str("The controller moved to another broker.")
            }
            ErrorCode::OffsetMetadataTooLarge => {
                f.write_str("The metadata field of the offset request was too large.")
            }
            ErrorCode::NetworkException => {
                f.write_str("The server disconnected before a response was received.")
            }
            ErrorCode::CoordinatorLoadInProgress => {
                f.write_str("The coordinator is loading and hence can't process requests.")
            }
            ErrorCode::CoordinatorNotAvailable => f.write_str("The coordinator is not available."),
            ErrorCode::NotCoordinator => f.write_str("This is not the correct coordinator."),
            ErrorCode::InvalidTopicException => {
                f.write_str("The request attempted to perform an operation on an invalid topic.")
            }
            ErrorCode::RecordListTooLarge => f.write_str(
                "The request included message batch larger than the configured segment size on \
                 the server.",
            ),
            ErrorCode::NotEnoughReplicas => f.write_str(
                "Messages are rejected since there are fewer in-sync replicas than required.",
            ),
            ErrorCode::NotEnoughReplicasAfterAppend => f.write_str(
                "Messages are written to the log, but to fewer in-sync replicas than required.",
            ),
            ErrorCode::InvalidRequiredAcks => {
                f.write_str("Produce request specified an invalid value for required acks.")
            }
            ErrorCode::IllegalGeneration => {
                f.write_str("Specified group generation id is not valid.")
            }
            ErrorCode::InconsistentGroupProtocol => f.write_str(
                "The group member's supported protocols are incompatible with those of existing \
                 members or first group member tried to join with empty protocol type or empty \
                 protocol list.",
            ),
            ErrorCode::InvalidGroupId => f.write_str("The configured groupId is invalid."),
            ErrorCode::UnknownMemberId => {
                f.write_str("The coordinator is not aware of this member.")
            }
            ErrorCode::InvalidSessionTimeout => f.write_str(
                "The session timeout is not within the range allowed by the broker (as configured \
                 by group.min.session.timeout.ms and group.max.session.timeout.ms).",
            ),
            ErrorCode::RebalanceInProgress => {
                f.write_str("The group is rebalancing, so a rejoin is needed.")
            }
            ErrorCode::InvalidCommitOffsetSize => {
                f.write_str("The committing offset data size is not valid.")
            }
            ErrorCode::TopicAuthorizationFailed => f.write_str("Topic authorization failed."),
            ErrorCode::GroupAuthorizationFailed => f.write_str("Group authorization failed."),
            ErrorCode::ClusterAuthorizationFailed => f.write_str("Cluster authorization failed."),
            ErrorCode::InvalidTimestamp => {
                f.write_str("The timestamp of the message is out of acceptable range.")
            }
            ErrorCode::UnsupportedSaslMechanism => {
                f.write_str("The broker does not support the requested SASL mechanism.")
            }
            ErrorCode::IllegalSaslState => {
                f.write_str("Request is not valid given the current SASL state.")
            }
            ErrorCode::UnsupportedVersion => f.write_str("The version of API is not supported."),
            ErrorCode::TopicAlreadyExists => f.write_str("Topic with this name already exists."),
            ErrorCode::InvalidPartitions => f.write_str("Number of partitions is below 1."),
            ErrorCode::InvalidReplicationFactor => f.write_str(
                "Replication factor is below 1 or larger than the number of available brokers.",
            ),
            ErrorCode::InvalidReplicaAssignment => f.write_str("Replica assignment is invalid."),
            ErrorCode::InvalidConfig => f.write_str("Configuration is invalid."),
            ErrorCode::NotController => {
                f.write_str("This is not the correct controller for this cluster.")
            }
            ErrorCode::InvalidRequest => f.write_str(
                "This most likely occurs because of a request being malformed by the client \
                 library or the message was sent to an incompatible broker. See the broker logs \
                 for more details.",
            ),
            ErrorCode::UnsupportedForMessageFormat => f.write_str(
                "The message format version on the broker does not support the request.",
            ),
            ErrorCode::PolicyViolation => {
                f.write_str("Request parameters do not satisfy the configured policy.")
            }
            ErrorCode::OutOfOrderSequenceNumber => {
                f.write_str("The broker received an out of order sequence number.")
            }
            ErrorCode::DuplicateSequenceNumber => {
                f.write_str("The broker received a duplicate sequence number.")
            }
            ErrorCode::InvalidProducerEpoch => {
                f.write_str("Producer attempted to produce with an old epoch.")
            }
            ErrorCode::InvalidTxnState => {
                f.write_str("The producer attempted a transactional operation in an invalid state.")
            }
            ErrorCode::InvalidProducerIdMapping => f.write_str(
                "The producer attempted to use a producer id which is not currently assigned to \
                 its transactional id.",
            ),
            ErrorCode::InvalidTransactionTimeout => f.write_str(
                "The transaction timeout is larger than the maximum value allowed by the broker \
                 (as configured by transaction.max.timeout.ms).",
            ),
            ErrorCode::ConcurrentTransactions => f.write_str(
                "The producer attempted to update a transaction while another concurrent \
                 operation on the same transaction was ongoing.",
            ),
            ErrorCode::TransactionCoordinatorFenced => f.write_str(
                "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer \
                 the current coordinator for a given producer.",
            ),
            ErrorCode::TransactionalIdAuthorizationFailed => {
                f.write_str("Transactional Id authorization failed.")
            }
            ErrorCode::SecurityDisabled => f.write_str("Security features are disabled."),
            ErrorCode::OperationNotAttempted => f.write_str(
                "The broker did not attempt to execute this operation. This may happen for \
                 batched RPCs where some operations in the batch failed, causing the broker to \
                 respond without trying the rest.",
            ),
            ErrorCode::KafkaStorageError => {
                f.write_str("Disk error when trying to access log file on the disk.")
            }
            ErrorCode::LogDirNotFound => {
                f.write_str("The user-specified log directory is not found in the broker config.")
            }
            ErrorCode::SaslAuthenticationFailed => f.write_str("SASL Authentication failed."),
            ErrorCode::UnknownProducerId => f.write_str(
                "This exception is raised by the broker if it could not locate the producer \
                 metadata associated with the producerId in question. This could happen if, for \
                 instance, the producer's records were deleted because their retention time had \
                 elapsed. Once the last records of the producerId are removed, the producer's \
                 metadata is removed from the broker, and future appends by the producer will \
                 return this exception.",
            ),
            ErrorCode::ReassignmentInProgress => {
                f.write_str("A partition reassignment is in progress.")
            }
            ErrorCode::DelegationTokenAuthDisabled => {
                f.write_str("Delegation Token feature is not enabled.")
            }
            ErrorCode::DelegationTokenNotFound => {
                f.write_str("Delegation Token is not found on server.")
            }
            ErrorCode::DelegationTokenOwnerMismatch => {
                f.write_str("Specified Principal is not valid Owner/Renewer.")
            }
            ErrorCode::DelegationTokenRequestNotAllowed => f.write_str(
                "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on \
                 delegation token authenticated channels.",
            ),
            ErrorCode::DelegationTokenAuthorizationFailed => {
                f.write_str("Delegation Token authorization failed.")
            }
            ErrorCode::DelegationTokenExpired => f.write_str("Delegation Token is expired."),
            ErrorCode::InvalidPrincipalType => {
                f.write_str("Supplied principalType is not supported.")
            }
            ErrorCode::NonEmptyGroup => f.write_str("The group is not empty."),
            ErrorCode::GroupIdNotFound => f.write_str("The group id does not exist."),
            ErrorCode::FetchSessionIdNotFound => f.write_str("The fetch session ID was not found."),
            ErrorCode::InvalidFetchSessionEpoch => {
                f.write_str("The fetch session epoch is invalid.")
            }
            ErrorCode::ListenerNotFound => f.write_str(
                "There is no listener on the leader broker that matches the listener on which \
                 metadata request was processed.",
            ),
            ErrorCode::TopicDeletionDisabled => f.write_str("Topic deletion is disabled."),
            ErrorCode::FencedLeaderEpoch => f.write_str(
                "The leader epoch in the request is older than the epoch on the broker.",
            ),
            ErrorCode::UnknownLeaderEpoch => f.write_str(
                "The leader epoch in the request is newer than the epoch on the broker.",
            ),
            ErrorCode::UnsupportedCompressionType => f.write_str(
                "The requesting client does not support the compression type of given partition.",
            ),
            ErrorCode::StaleBrokerEpoch => f.write_str("Broker epoch has changed."),
            ErrorCode::OffsetNotAvailable => f.write_str(
                "The leader high watermark has not caught up from a recent leader election so the \
                 offsets cannot be guaranteed to be monotonically increasing.",
            ),
            ErrorCode::MemberIdRequired => f.write_str(
                "The group member needs to have a valid member id before actually entering a \
                 consumer group.",
            ),
            ErrorCode::PreferredLeaderNotAvailable => {
                f.write_str("The preferred leader was not available.")
            }
            ErrorCode::GroupMaxSizeReached => {
                f.write_str("The consumer group has reached its max size.")
            }
            ErrorCode::FencedInstanceId => f.write_str(
                "The broker rejected this static consumer since another consumer with the same \
                 group.instance.id has registered with a different member.id.",
            ),
            ErrorCode::EligibleLeadersNotAvailable => {
                f.write_str("Eligible topic partition leaders are not available.")
            }
            ErrorCode::ElectionNotNeeded => {
                f.write_str("Leader election not needed for topic partition.")
            }
            ErrorCode::NoReassignmentInProgress => {
                f.write_str("No partition reassignment is in progress.")
            }
            ErrorCode::GroupSubscribedToTopic => f.write_str(
                "Deleting offsets of a topic is forbidden while the consumer group is actively \
                 subscribed to it.",
            ),
            ErrorCode::InvalidRecord => f.write_str(
                "This record has failed the validation on broker and hence will be rejected.",
            ),
            ErrorCode::UnstableOffsetCommit => {
                f.write_str("There are unstable offsets that need to be cleared.")
            }
            ErrorCode::ThrottlingQuotaExceeded => {
                f.write_str("The throttling quota has been exceeded.")
            }
            ErrorCode::ProducerFenced => f.write_str(
                "There is a newer producer with the same transactionalId which fences the current \
                 one.",
            ),
            ErrorCode::ResourceNotFound => {
                f.write_str("A request illegally referred to a resource that does not exist.")
            }
            ErrorCode::DuplicateResource => {
                f.write_str("A request illegally referred to the same resource twice.")
            }
            ErrorCode::UnacceptableCredential => {
                f.write_str("Requested credential would not meet criteria for acceptability.")
            }
            ErrorCode::InconsistentVoterSet => f.write_str(
                "Indicates that the either the sender or recipient of a voter-only request is not \
                 one of the expected voters",
            ),
            ErrorCode::InvalidUpdateVersion => f.write_str("The given update version was invalid."),
            ErrorCode::FeatureUpdateFailed => f.write_str(
                "Unable to update finalized features due to an unexpected server error.",
            ),
            ErrorCode::PrincipalDeserializationFailure => f.write_str(
                "Request principal deserialization failed during forwarding. This indicates an \
                 internal error on the broker cluster security setup.",
            ),
            ErrorCode::SnapshotNotFound => f.write_str("Requested snapshot was not found"),
            ErrorCode::PositionOutOfRange => f.write_str(
                "Requested position is not greater than or equal to zero, and less than the size \
                 of the snapshot.",
            ),
            ErrorCode::UnknownTopicId => f.write_str("This server does not host this topic ID."),
            ErrorCode::DuplicateBrokerRegistration => {
                f.write_str("This broker ID is already in use.")
            }
            ErrorCode::BrokerIdNotRegistered => {
                f.write_str("The given broker ID was not registered.")
            }
            ErrorCode::InconsistentTopicId => {
                f.write_str("The log's topic ID did not match the topic ID in the request")
            }
            ErrorCode::InconsistentClusterId => {
                f.write_str("The clusterId in the request does not match that found on the server")
            }
            ErrorCode::TransactionalIdNotFound => {
                f.write_str("The transactionalId could not be found")
            }
            ErrorCode::FetchSessionTopicIdError => {
                f.write_str("The fetch session encountered inconsistent topic ID usage")
            }
            ErrorCode::IneligibleReplica => {
                f.write_str("The new ISR contains at least one ineligible replica.")
            }
            ErrorCode::NewLeaderElected => f.write_str(
                "The AlterPartition request successfully updated the partition state but the \
                 leader has changed.",
            ),
            ErrorCode::OffsetMovedToTieredStorage => {
                f.write_str("The requested offset is moved to tiered storage.")
            }
            ErrorCode::FencedMemberEpoch => f.write_str(
                "The member epoch is fenced by the group coordinator. The member must abandon all \
                 its partitions and rejoin.",
            ),
            ErrorCode::UnreleasedInstanceId => f.write_str(
                "The instance ID is still used by another member in the consumer group. That \
                 member must leave first.",
            ),
            ErrorCode::UnsupportedAssignor => f.write_str(
                "The assignor or its version range is not supported by the consumer group.",
            ),
            ErrorCode::StaleMemberEpoch => f.write_str(
                "The member epoch is stale. The member must retry after receiving its updated \
                 member epoch via the ConsumerGroupHeartbeat API.",
            ),
            ErrorCode::MismatchedEndpointType => {
                f.write_str("The request was sent to an endpoint of the wrong type.")
            }
            ErrorCode::UnsupportedEndpointType => {
                f.write_str("This endpoint type is not supported yet.")
            }
            ErrorCode::UnknownControllerId => f.write_str("This controller ID is not known."),
            ErrorCode::UnknownSubscriptionId => f.write_str(
                "Client sent a push telemetry request with an invalid or outdated subscription ID.",
            ),
            ErrorCode::TelemetryTooLarge => f.write_str(
                "Client sent a push telemetry request larger than the maximum size the broker \
                 will accept.",
            ),
            ErrorCode::InvalidRegistration => {
                f.write_str("The controller has considered the broker registration to be invalid.")
            }
        }
    }
}

#[non_exhaustive]
#[derive(
    Clone, Copy, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize,
)]
/// Kafka API response error codes.
pub enum ErrorCode {
    UnknownServerError,
    #[default]
    None,
    OffsetOutOfRange,
    CorruptMessage,
    UnknownTopicOrPartition,
    InvalidFetchSize,
    LeaderNotAvailable,
    NotLeaderOrFollower,
    RequestTimedOut,
    BrokerNotAvailable,
    ReplicaNotAvailable,
    MessageTooLarge,
    StaleControllerEpoch,
    OffsetMetadataTooLarge,
    NetworkException,
    CoordinatorLoadInProgress,
    CoordinatorNotAvailable,
    NotCoordinator,
    InvalidTopicException,
    RecordListTooLarge,
    NotEnoughReplicas,
    NotEnoughReplicasAfterAppend,
    InvalidRequiredAcks,
    IllegalGeneration,
    InconsistentGroupProtocol,
    InvalidGroupId,
    UnknownMemberId,
    InvalidSessionTimeout,
    RebalanceInProgress,
    InvalidCommitOffsetSize,
    TopicAuthorizationFailed,
    GroupAuthorizationFailed,
    ClusterAuthorizationFailed,
    InvalidTimestamp,
    UnsupportedSaslMechanism,
    IllegalSaslState,
    UnsupportedVersion,
    TopicAlreadyExists,
    InvalidPartitions,
    InvalidReplicationFactor,
    InvalidReplicaAssignment,
    InvalidConfig,
    NotController,
    InvalidRequest,
    UnsupportedForMessageFormat,
    PolicyViolation,
    OutOfOrderSequenceNumber,
    DuplicateSequenceNumber,
    InvalidProducerEpoch,
    InvalidTxnState,
    InvalidProducerIdMapping,
    InvalidTransactionTimeout,
    ConcurrentTransactions,
    TransactionCoordinatorFenced,
    TransactionalIdAuthorizationFailed,
    SecurityDisabled,
    OperationNotAttempted,
    KafkaStorageError,
    LogDirNotFound,
    SaslAuthenticationFailed,
    UnknownProducerId,
    ReassignmentInProgress,
    DelegationTokenAuthDisabled,
    DelegationTokenNotFound,
    DelegationTokenOwnerMismatch,
    DelegationTokenRequestNotAllowed,
    DelegationTokenAuthorizationFailed,
    DelegationTokenExpired,
    InvalidPrincipalType,
    NonEmptyGroup,
    GroupIdNotFound,
    FetchSessionIdNotFound,
    InvalidFetchSessionEpoch,
    ListenerNotFound,
    TopicDeletionDisabled,
    FencedLeaderEpoch,
    UnknownLeaderEpoch,
    UnsupportedCompressionType,
    StaleBrokerEpoch,
    OffsetNotAvailable,
    MemberIdRequired,
    PreferredLeaderNotAvailable,
    GroupMaxSizeReached,
    FencedInstanceId,
    EligibleLeadersNotAvailable,
    ElectionNotNeeded,
    NoReassignmentInProgress,
    GroupSubscribedToTopic,
    InvalidRecord,
    UnstableOffsetCommit,
    ThrottlingQuotaExceeded,
    ProducerFenced,
    ResourceNotFound,
    DuplicateResource,
    UnacceptableCredential,
    InconsistentVoterSet,
    InvalidUpdateVersion,
    FeatureUpdateFailed,
    PrincipalDeserializationFailure,
    SnapshotNotFound,
    PositionOutOfRange,
    UnknownTopicId,
    DuplicateBrokerRegistration,
    BrokerIdNotRegistered,
    InconsistentTopicId,
    InconsistentClusterId,
    TransactionalIdNotFound,
    FetchSessionTopicIdError,
    IneligibleReplica,
    NewLeaderElected,
    OffsetMovedToTieredStorage,
    FencedMemberEpoch,
    UnreleasedInstanceId,
    UnsupportedAssignor,
    StaleMemberEpoch,
    MismatchedEndpointType,
    UnsupportedEndpointType,
    UnknownControllerId,
    UnknownSubscriptionId,
    TelemetryTooLarge,
    InvalidRegistration,
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
/// The fetch isolation level.
pub enum IsolationLevel {
    #[default]
    ReadUncommitted,
    ReadCommitted,
}

impl TryFrom<i8> for IsolationLevel {
    type Error = Error;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::ReadUncommitted),
            1 => Ok(Self::ReadCommitted),
            _ => Err(Error::InvalidIsolationLevel(value)),
        }
    }
}

impl From<IsolationLevel> for i8 {
    fn from(value: IsolationLevel) -> Self {
        Self::from(&value)
    }
}

impl From<&IsolationLevel> for i8 {
    fn from(value: &IsolationLevel) -> Self {
        match value {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
/// Produce message acknowledgement.
pub enum Ack {
    None,
    Leader,
    FullIsr,
}

impl Ack {
    const FULL_ISR: i16 = -1;
    const NONE: i16 = 0;
    const LEADER: i16 = 1;
}

impl From<Ack> for i16 {
    fn from(value: Ack) -> Self {
        match value {
            Ack::FullIsr => Ack::FULL_ISR,
            Ack::None => Ack::NONE,
            Ack::Leader => Ack::LEADER,
        }
    }
}

impl TryFrom<i16> for Ack {
    type Error = Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            Self::FULL_ISR => Ok(Self::FullIsr),
            Self::NONE => Ok(Self::None),
            Self::LEADER => Ok(Self::Leader),
            _ => Err(Error::InvalidAckValue(value)),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
/// The timestamp type.
pub enum TimestampType {
    #[default]
    CreateTime,
    LogAppendTime,
}

impl TimestampType {
    const TIMESTAMP_TYPE_BITMASK: i16 = 8;
}

impl From<i16> for TimestampType {
    fn from(value: i16) -> Self {
        if value & Self::TIMESTAMP_TYPE_BITMASK == Self::TIMESTAMP_TYPE_BITMASK {
            Self::LogAppendTime
        } else {
            Self::CreateTime
        }
    }
}

impl From<TimestampType> for i16 {
    fn from(value: TimestampType) -> Self {
        match value {
            TimestampType::CreateTime => 0,
            TimestampType::LogAppendTime => TimestampType::TIMESTAMP_TYPE_BITMASK,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
/// Kafka message compression types.
pub enum Compression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl TryFrom<i16> for Compression {
    type Error = Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value & 0b111i16 {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Lz4),
            4 => Ok(Self::Zstd),
            otherwise => Err(Error::UnknownCompressionType(otherwise)),
        }
    }
}

impl From<Compression> for i16 {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => 0,
            Compression::Gzip => 1,
            Compression::Snappy => 2,
            Compression::Lz4 => 3,
            Compression::Zstd => 4,
        }
    }
}

impl Compression {
    fn inflator(&self, mut deflated: impl BufRead + 'static) -> Result<Box<dyn Read>> {
        match self {
            Compression::None => Ok(Box::new(deflated)),
            Compression::Gzip => Ok(Box::new(GzDecoder::new(deflated))),
            Compression::Snappy => {
                let mut input = vec![];
                _ = deflated.read_to_end(&mut input)?;
                debug!(?input);

                let mut decoder = snap::raw::Decoder::new();

                decoder
                    .decompress_vec(
                        // https://github.com/xerial/snappy-java/tree/master?tab=readme-ov-file#compatibility-notes
                        if input.starts_with(b"\x82SNAPPY\0") {
                            if let (b"\x82SNAPPY\0", remainder) = input.split_at(8) {
                                let (version, remainder) = remainder.split_at(4);
                                let version: i32 = version.try_into().map(i32::from_be_bytes)?;

                                let (compatible_version, remainder) = remainder.split_at(4);
                                let compatible_version: i32 =
                                    compatible_version.try_into().map(i32::from_be_bytes)?;

                                let (block_size, _) = remainder.split_at(4);
                                let block_size: i32 =
                                    block_size.try_into().map(i32::from_be_bytes)?;

                                debug!(version, compatible_version, block_size);
                            }

                            let skip_header = &input[20..];
                            debug!(?skip_header);
                            skip_header
                        } else {
                            &input[..]
                        },
                    )
                    .map_err(Into::into)
                    .map(Bytes::from)
                    .map(|bytes| bytes.reader())
                    .map(Box::new)
                    .map(|boxed| boxed as Box<dyn Read>)
                    .inspect_err(|err| error!(?err))
            }
            Compression::Lz4 => lz4::Decoder::new(deflated)
                .map(Box::new)
                .map(|boxed| boxed as Box<dyn Read>)
                .map_err(Into::into),
            Compression::Zstd => zstd::stream::read::Decoder::with_buffer(deflated)
                .map(Box::new)
                .map(|boxed| boxed as Box<dyn Read>)
                .map_err(Into::into),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
/// The produce batch attributes.
pub struct BatchAttribute {
    pub compression: Compression,
    pub timestamp: TimestampType,
    pub transaction: bool,
    pub control: bool,
    pub delete_horizon: bool,
}

impl BatchAttribute {
    const TRANSACTION_BITMASK: i16 = 16;
    const CONTROL_BITMASK: i16 = 32;
    const DELETE_HORIZON_BITMASK: i16 = 64;

    pub fn compression(self, compression: Compression) -> Self {
        Self {
            compression,
            ..self
        }
    }

    pub fn timestamp(self, timestamp: TimestampType) -> Self {
        Self { timestamp, ..self }
    }

    pub fn transaction(self, transaction: bool) -> Self {
        Self {
            transaction,
            ..self
        }
    }

    pub fn control(self, control: bool) -> Self {
        Self { control, ..self }
    }

    pub fn delete_horizon(self, delete_horizon: bool) -> Self {
        Self {
            delete_horizon,
            ..self
        }
    }
}

impl From<BatchAttribute> for i16 {
    fn from(value: BatchAttribute) -> Self {
        let mut attributes = i16::from(value.compression);
        attributes |= i16::from(value.timestamp);

        if value.transaction {
            attributes |= BatchAttribute::TRANSACTION_BITMASK;
        }

        if value.control {
            attributes |= BatchAttribute::CONTROL_BITMASK;
        }

        if value.delete_horizon {
            attributes |= BatchAttribute::DELETE_HORIZON_BITMASK;
        }

        attributes
    }
}

impl TryFrom<i16> for BatchAttribute {
    type Error = Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        Compression::try_from(value).map(|compression| {
            Self::default()
                .compression(compression)
                .timestamp(TimestampType::from(value))
                .transaction(value & Self::TRANSACTION_BITMASK == Self::TRANSACTION_BITMASK)
                .control(value & Self::CONTROL_BITMASK == Self::CONTROL_BITMASK)
                .delete_horizon(
                    value & Self::DELETE_HORIZON_BITMASK == Self::DELETE_HORIZON_BITMASK,
                )
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
/// The control batch marker.
pub struct ControlBatch {
    pub version: i16,
    pub r#type: i16,
}

impl ByteSize for ControlBatch {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(size_of_val(&self.version) + size_of_val(&self.r#type))
    }
}

impl ControlBatch {
    const ABORT: i16 = 0;
    const COMMIT: i16 = 1;

    pub fn is_abort(&self) -> bool {
        self.r#type == Self::ABORT
    }

    pub fn is_commit(&self) -> bool {
        self.r#type == Self::COMMIT
    }

    pub fn version(self, version: i16) -> Self {
        Self { version, ..self }
    }

    pub fn commit(self) -> Self {
        Self {
            r#type: Self::COMMIT,
            ..self
        }
    }

    pub fn abort(self) -> Self {
        Self {
            r#type: Self::ABORT,
            ..self
        }
    }
}

impl TryFrom<Bytes> for ControlBatch {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let mut c = Cursor::new(value);
        let mut deserializer = Decoder::new(&mut c);
        Self::deserialize(&mut deserializer)
    }
}

impl TryFrom<ControlBatch> for Bytes {
    type Error = Error;

    fn try_from(value: ControlBatch) -> Result<Self, Self::Error> {
        let mut encoder = Encoder::new(BytesMut::with_capacity(value.size_in_bytes()?));
        value.serialize(&mut encoder)?;

        Ok(Bytes::from(encoder))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
/// An end transaction marker.
pub struct EndTransactionMarker {
    pub version: i16,
    pub coordinator_epoch: i32,
}

impl ByteSize for EndTransactionMarker {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(size_of_val(&self.version) + size_of_val(&self.coordinator_epoch))
    }
}

impl TryFrom<Bytes> for EndTransactionMarker {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let mut c = Cursor::new(value);
        let mut deserializer = Decoder::new(&mut c);
        Self::deserialize(&mut deserializer)
    }
}

impl TryFrom<EndTransactionMarker> for Bytes {
    type Error = Error;

    fn try_from(value: EndTransactionMarker) -> Result<Self, Self::Error> {
        let mut encoder = Encoder::new(BytesMut::with_capacity(value.size_in_bytes()?));
        value.serialize(&mut encoder)?;
        Ok(Bytes::from(encoder))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The endpoint type.
pub enum EndpointType {
    #[default]
    Unknown,
    Broker,
    Controller,
}

impl From<i8> for EndpointType {
    fn from(value: i8) -> Self {
        match value {
            1 => Self::Broker,
            2 => Self::Controller,
            _ => Self::Unknown,
        }
    }
}

impl From<EndpointType> for i8 {
    fn from(value: EndpointType) -> Self {
        match value {
            EndpointType::Unknown => 0,
            EndpointType::Broker => 1,
            EndpointType::Controller => 2,
        }
    }
}

/// The coordinator type.
pub enum CoordinatorType {
    Group,
    Transaction,
    Share,
}

impl TryFrom<i8> for CoordinatorType {
    type Error = Error;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Group),
            1 => Ok(Self::Transaction),
            2 => Ok(Self::Share),
            otherwise => Err(Error::InvalidCoordinatorType(otherwise)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// What type of resource is the configuration describing.
pub enum ConfigResource {
    Group,
    ClientMetric,
    BrokerLogger,
    Broker,
    Topic,
    Unknown,
}

impl From<i8> for ConfigResource {
    fn from(value: i8) -> Self {
        match value {
            2 => Self::Topic,
            4 => Self::Broker,
            8 => Self::BrokerLogger,
            16 => Self::ClientMetric,
            32 => Self::Group,
            _ => Self::Unknown,
        }
    }
}

impl From<CoordinatorType> for i8 {
    fn from(value: CoordinatorType) -> Self {
        match value {
            CoordinatorType::Group => 0,
            CoordinatorType::Transaction => 1,
            CoordinatorType::Share => 2,
        }
    }
}

impl From<ConfigResource> for i8 {
    fn from(value: ConfigResource) -> Self {
        match value {
            ConfigResource::Unknown => 0,
            ConfigResource::Topic => 2,
            ConfigResource::Broker => 4,
            ConfigResource::BrokerLogger => 8,
            ConfigResource::ClientMetric => 16,
            ConfigResource::Group => 32,
        }
    }
}

impl From<ConfigResource> for i32 {
    fn from(value: ConfigResource) -> Self {
        match value {
            ConfigResource::Unknown => 0,
            ConfigResource::Topic => 2,
            ConfigResource::Broker => 4,
            ConfigResource::BrokerLogger => 8,
            ConfigResource::ClientMetric => 16,
            ConfigResource::Group => 32,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The type of configuration.
pub enum ConfigType {
    #[default]
    Unknown,
    Boolean,
    String,
    Int,
    Short,
    Long,
    Double,
    List,
    Class,
    Password,
}

impl From<i8> for ConfigType {
    fn from(value: i8) -> Self {
        match value {
            1 => Self::Boolean,
            2 => Self::String,
            3 => Self::Int,
            4 => Self::Short,
            5 => Self::Long,
            6 => Self::Double,
            7 => Self::List,
            8 => Self::Class,
            9 => Self::Password,
            _ => Self::Unknown,
        }
    }
}

impl From<ConfigType> for i8 {
    fn from(value: ConfigType) -> i8 {
        match value {
            ConfigType::Boolean => 1,
            ConfigType::String => 2,
            ConfigType::Int => 3,
            ConfigType::Short => 4,
            ConfigType::Long => 5,
            ConfigType::Double => 6,
            ConfigType::List => 7,
            ConfigType::Class => 8,
            ConfigType::Password => 9,
            ConfigType::Unknown => 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// From which source was the configuration provided.
pub enum ConfigSource {
    DynamicTopicConfig,
    DynamicBrokerLoggerConfig,
    DynamicBrokerConfig,
    DynamicDefaultBrokerConfig,
    DynamicClientMetricsConfig,
    DynamicGroupConfig,
    StaticBrokerConfig,
    DefaultConfig,
    Unknown,
}

impl From<i8> for ConfigSource {
    fn from(value: i8) -> Self {
        match value {
            1 => Self::DynamicTopicConfig,
            2 => Self::DynamicBrokerConfig,
            3 => Self::DynamicDefaultBrokerConfig,
            4 => Self::StaticBrokerConfig,
            5 => Self::DefaultConfig,
            6 => Self::DynamicBrokerLoggerConfig,
            7 => Self::DynamicClientMetricsConfig,
            8 => Self::DynamicGroupConfig,
            _ => Self::Unknown,
        }
    }
}

impl From<ConfigSource> for i8 {
    fn from(value: ConfigSource) -> i8 {
        match value {
            ConfigSource::DynamicTopicConfig => 1,
            ConfigSource::DynamicBrokerConfig => 2,
            ConfigSource::DynamicDefaultBrokerConfig => 3,
            ConfigSource::StaticBrokerConfig => 4,
            ConfigSource::DefaultConfig => 5,
            ConfigSource::DynamicBrokerLoggerConfig => 6,
            ConfigSource::DynamicClientMetricsConfig => 7,
            ConfigSource::DynamicGroupConfig => 8,
            ConfigSource::Unknown => 0,
        }
    }
}

/// The configuration operation type.
pub enum OpType {
    Set,
    Delete,
    Append,
    Subtract,
}

impl TryFrom<i8> for OpType {
    type Error = Error;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Set),
            1 => Ok(Self::Delete),
            2 => Ok(Self::Append),
            3 => Ok(Self::Subtract),
            otherwise => Err(Error::InvalidOpType(otherwise)),
        }
    }
}

impl From<OpType> for i8 {
    fn from(value: OpType) -> Self {
        match value {
            OpType::Set => 0,
            OpType::Delete => 1,
            OpType::Append => 2,
            OpType::Subtract => 3,
        }
    }
}

/// convert a Kafka timestamp into system time
pub fn to_system_time(timestamp: i64) -> Result<SystemTime> {
    u64::try_from(timestamp)
        .map(|timestamp| SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp))
        .map_err(Into::into)
}

/// convert system time into a kafka timestamp
pub fn to_timestamp(system_time: &SystemTime) -> Result<i64> {
    system_time
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(Into::into)
        .map(|since_epoch| since_epoch.as_millis())
        .and_then(|since_epoch| i64::try_from(since_epoch).map_err(Into::into))
}

/// List Offset
///
/// An enumeration of offset request types, with conversion from/to an i64 protocol representation.
///
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ListOffset {
    Earliest,
    Latest,
    Timestamp(SystemTime),
}

impl ListOffset {
    const EARLIEST_OFFSET: i64 = -2;
    const LATEST_OFFSET: i64 = -1;
}

impl TryFrom<ListOffset> for i64 {
    type Error = Error;

    fn try_from(value: ListOffset) -> Result<Self, Self::Error> {
        match value {
            ListOffset::Earliest => Ok(ListOffset::EARLIEST_OFFSET),
            ListOffset::Latest => Ok(ListOffset::LATEST_OFFSET),
            ListOffset::Timestamp(timestamp) => to_timestamp(&timestamp),
        }
    }
}

impl TryFrom<i64> for ListOffset {
    type Error = Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            Self::EARLIEST_OFFSET => Ok(Self::Earliest),
            Self::LATEST_OFFSET => Ok(Self::Latest),
            timestamp => to_system_time(timestamp).map(Self::Timestamp),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ScramMechanism {
    Scram256,
    Scram512,
}

impl FromStr for ScramMechanism {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "SCRAM-SHA-256" => Ok(ScramMechanism::Scram256),
            "SCRAM-SHA-512" => Ok(ScramMechanism::Scram512),
            otherwise => Err(Error::ParseScram(otherwise.to_string())),
        }
    }
}

impl TryFrom<i8> for ScramMechanism {
    type Error = Error;

    fn try_from(value: i8) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(ScramMechanism::Scram256),
            2 => Ok(ScramMechanism::Scram512),
            otherwise => Err(Error::UnknownScramMechanism(value)),
        }
    }
}

impl From<ScramMechanism> for i32 {
    fn from(value: ScramMechanism) -> Self {
        match value {
            ScramMechanism::Scram256 => 1,
            ScramMechanism::Scram512 => 2,
        }
    }
}

impl From<ScramMechanism> for i8 {
    fn from(value: ScramMechanism) -> Self {
        match value {
            ScramMechanism::Scram256 => 1,
            ScramMechanism::Scram512 => 2,
        }
    }
}

pub trait Encode {
    fn encode(&self) -> Result<Bytes>;
}

pub trait Decode: Sized {
    fn decode(encoded: &mut Bytes) -> Result<Self>;
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use super::*;

    #[test]
    fn frame_elapsed_millis() {
        let pause = 6;
        let now = SystemTime::now();
        sleep(Duration::from_millis(pause));

        assert!(Frame::elapsed_millis(now) >= pause);
    }

    #[test]
    fn batch_attribute() {
        assert_eq!(0, i16::from(BatchAttribute::default()));
        assert_eq!(
            0,
            i16::from(BatchAttribute::default().compression(Compression::None))
        );
        assert_eq!(
            1,
            i16::from(BatchAttribute::default().compression(Compression::Gzip))
        );
        assert_eq!(
            2,
            i16::from(BatchAttribute::default().compression(Compression::Snappy))
        );
        assert_eq!(
            3,
            i16::from(BatchAttribute::default().compression(Compression::Lz4))
        );
        assert_eq!(
            4,
            i16::from(BatchAttribute::default().compression(Compression::Zstd))
        );
        assert_eq!(
            8,
            i16::from(BatchAttribute::default().timestamp(TimestampType::LogAppendTime))
        );
        assert_eq!(16, i16::from(BatchAttribute::default().transaction(true)));
        assert_eq!(32, i16::from(BatchAttribute::default().control(true)));
        assert_eq!(
            64,
            i16::from(BatchAttribute::default().delete_horizon(true))
        );
    }

    #[test]
    fn reset_compression() -> Result<()> {
        let mut attribute: i16 = BatchAttribute::default()
            .compression(Compression::Gzip)
            .into();

        assert_eq!(
            Compression::Gzip,
            BatchAttribute::try_from(attribute).map(|attribute| attribute.compression)?
        );

        attribute = BatchAttribute::try_from(attribute)
            .map(|attribute| attribute.compression(Compression::None).into())?;

        assert_eq!(
            Compression::None,
            BatchAttribute::try_from(attribute).map(|attribute| attribute.compression)?
        );

        Ok(())
    }
}

include!(concat!(env!("OUT_DIR"), "/generate.rs"));
