// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

//! Nisshi Client
//!
//! Nisshi API client.
//!
//! # Simple [`Request`] client
//!
//! ```no_run
//! use nisshi_client::{Client, ConnectionManager, Error};
//! use nisshi_sans_io::MetadataRequest;
//! use rama::{Service as _, Context};
//! use url::Url;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let origin = ConnectionManager::builder(Url::parse("tcp://localhost:9092")?)
//!     .client_id(Some(env!("CARGO_PKG_NAME").into()))
//!     .build()
//!     .await
//!     .map(Client::new)?;
//!
//! let response = origin
//!     .call(
//!         MetadataRequest::default()
//!             .topics(Some([].into()))
//!             .allow_auto_topic_creation(Some(false))
//!             .include_cluster_authorized_operations(Some(false))
//!             .include_topic_authorized_operations(Some(false)),
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Proxy: [`Layer`] Composition
//!
//! An example API proxy listening for requests on `tcp://localhost:9092` that
//! forwards each [`Frame`] to an origin broker on `tcp://example.com:9092`:
//!
//! ```no_run
//! use rama::{Context, Layer as _, Service as _};
//! use nisshi_client::{
//!     BytesConnectionService, ConnectionManager, Error, FrameConnectionLayer,
//!     FramePoolLayer,
//! };
//! use nisshi_service::{
//!     BytesFrameLayer, FrameBytesLayer, TcpBytesLayer, TcpContextLayer, TcpListenerLayer,
//!     host_port,
//! };
//! use tokio::net::TcpListener;
//! use tokio_util::sync::CancellationToken;
//! use url::Url;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! // forward protocol frames to the origin using a connection pool:
//! let origin = ConnectionManager::builder(Url::parse("tcp://example.com:9092")?)
//!     .client_id(Some(env!("CARGO_PKG_NAME").into()))
//!     .build()
//!     .await?;
//!
//! // a tcp listener used by the proxy
//! let listener =
//!     TcpListener::bind(host_port(Url::parse("tcp://localhost:9092")?).await?).await?;
//!
//! // listen for requests until cancelled
//! let token = CancellationToken::new();
//!
//! let stack = (
//!     // server layers: reading tcp -> bytes -> frames:
//!     TcpListenerLayer::new(token),
//!     TcpContextLayer::default(),
//!     TcpBytesLayer::<()>::default(),
//!     BytesFrameLayer::default(),
//!
//!     // client layers: writing frames -> connection pool -> bytes -> origin:
//!     FramePoolLayer::new(origin),
//!     FrameConnectionLayer,
//!     FrameBytesLayer,
//! )
//!     .into_layer(BytesConnectionService);
//!
//! stack.serve(Context::default(), listener).await?;
//!
//! # Ok(())
//! # }
//! ```

use std::{
    collections::BTreeMap,
    error, fmt, io,
    sync::{Arc, LazyLock, PoisonError},
    time::SystemTime,
};

use backoff::{ExponentialBackoffBuilder, future::retry};
use bytes::Bytes;
use deadpool::managed::{self, BuildError, Object, PoolError};
use nisshi_sans_io::{ApiKey, ApiVersionsRequest, Body, Frame, Header, Request, RootMessageMeta};
use nisshi_service::{FrameBytesLayer, FrameBytesService, host_port};
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Gauge, Histogram, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rama::{Context, Layer, Service};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    task::JoinError,
    time::Duration,
};
use tracing::{Instrument, Level, debug, span};
use tracing_subscriber::filter::ParseError;
use url::Url;

mod consumer;

pub use consumer::{ConsumerGroupLayer, ConsumerGroupService};

/// Client Errors
#[derive(thiserror::Error, Clone, Debug)]
pub enum Error {
    DeadPoolBuild(#[from] BuildError),
    Io(Arc<io::Error>),
    Join(Arc<JoinError>),
    Message(String),
    ParseFilter(Arc<ParseError>),
    ParseUrl(#[from] url::ParseError),
    Poison,
    Pool(Arc<Box<dyn error::Error + Send + Sync>>),
    Protocol(#[from] nisshi_sans_io::Error),
    Service(#[from] nisshi_service::Error),
    UnknownApiKey(i16),
    UnknownHost(Url),
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        Self::Join(Arc::new(value))
    }
}

impl<E> From<PoolError<E>> for Error
where
    E: error::Error + Send + Sync + 'static,
{
    fn from(value: PoolError<E>) -> Self {
        Self::Pool(Arc::new(Box::new(value)))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
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

///  Broker connection stream with [`correlation id`][`Header#variant.Request.field.correlation_id`]
#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    correlation_id: i32,
}

/// Manager of supported API versions for a broker
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionManager {
    broker: Url,
    client_id: Option<String>,
    versions: BTreeMap<i16, i16>,
}

impl ConnectionManager {
    /// Build a manager with a broker endpoint
    pub fn builder(broker: Url) -> Builder {
        Builder::broker(broker)
    }

    /// Client id used in requests to the broker
    pub fn client_id(&self) -> Option<String> {
        self.client_id.clone()
    }

    /// The version supported by the broker for a given api key
    pub fn api_version(&self, api_key: i16) -> Result<i16, Error> {
        self.versions
            .get(&api_key)
            .copied()
            .ok_or(Error::UnknownApiKey(api_key))
    }
}

const INITIAL_CONNECTION_TIMEOUT_MILLIS: u64 = 30_000;

impl managed::Manager for ConnectionManager {
    type Type = Connection;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        debug!(%self.broker);

        let attributes = [KeyValue::new("broker", self.broker.to_string())];
        let start = SystemTime::now();

        let addr = host_port(self.broker.clone()).await?;

        let backoff = ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(Duration::from_millis(
                INITIAL_CONNECTION_TIMEOUT_MILLIS,
            )))
            .build();
        retry(backoff, || async {
            Ok(TcpStream::connect(addr)
                .await
                .inspect(|_| {
                    TCP_CONNECT_DURATION.record(
                        start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &attributes,
                    )
                })
                .inspect_err(|err| {
                    debug!(broker = %self.broker, ?err, elapsed = start.elapsed().map_or(0, |duration| duration.as_millis() as u64));
                    TCP_CONNECT_ERRORS.add(1, &attributes);
                })
                .map(|stream| Connection {
                    stream,
                    correlation_id: 0,
                })?)
        })
        .await
        .map_err(Into::into)
    }

    async fn recycle(
        &self,
        obj: &mut Self::Type,
        metrics: &managed::Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        debug!(obj.correlation_id, metrics.recycle_count);
        Ok(())
    }
}

/// A managed [`Pool`] of broker [`Connection`]s
pub type Pool = managed::Pool<ConnectionManager>;

fn status_update(pool: &Pool) {
    let status = pool.status();
    POOL_AVAILABLE.record(status.available as u64, &[]);
    POOL_CURRENT_SIZE.record(status.size as u64, &[]);
    POOL_MAX_SIZE.record(status.max_size as u64, &[]);
    POOL_WAITING.record(status.waiting as u64, &[]);
}

/// [Build][`Builder#method.build`] a [`Connection`] [`Pool`] to a [broker][`Builder#method.broker`]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder {
    broker: Url,
    client_id: Option<String>,
}

impl Builder {
    /// Broker URL
    pub fn broker(broker: Url) -> Self {
        Self {
            broker,
            client_id: None,
        }
    }

    /// Client id used when making requests to the broker
    pub fn client_id(self, client_id: Option<String>) -> Self {
        Self { client_id, ..self }
    }

    /// Inquire with the broker supported api versions
    async fn bootstrap(&self) -> Result<BTreeMap<i16, i16>, Error> {
        // Create a temporary pool to establish the API requests
        // and versions supported by the broker
        let versions = BTreeMap::from([(ApiVersionsRequest::KEY, 0)]);

        let req = ApiVersionsRequest::default()
            .client_software_name(Some(env!("CARGO_PKG_NAME").into()))
            .client_software_version(Some(env!("CARGO_PKG_VERSION").into()));

        let client = Pool::builder(ConnectionManager {
            broker: self.broker.clone(),
            client_id: self.client_id.clone(),
            versions,
        })
        .build()
        .map(Client::new)?;

        let supported = RootMessageMeta::messages().requests();

        client.call(req).await.map(|response| {
            response
                .api_keys
                .unwrap_or_default()
                .into_iter()
                .filter_map(|api| {
                    supported.get(&api.api_key).and_then(|supported| {
                        if api.min_version >= supported.version.valid.start {
                            Some((
                                api.api_key,
                                api.max_version.min(supported.version.valid.end),
                            ))
                        } else {
                            None
                        }
                    })
                })
                .collect()
        })
    }

    /// Establish the API versions supported by the broker returning a [`Pool`]
    pub async fn build(self) -> Result<Pool, Error> {
        self.bootstrap().await.and_then(|versions| {
            Pool::builder(ConnectionManager {
                broker: self.broker,
                client_id: self.client_id,
                versions,
            })
            .build()
            .map_err(Into::into)
        })
    }
}

/// Inject the [`Pool`][`Pool`] into the [`Service`] [`Context`] of this [`Layer`] using [`FramePoolService`]
#[derive(Clone, Debug)]
pub struct FramePoolLayer {
    pool: Pool,
}

impl FramePoolLayer {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for FramePoolLayer {
    type Service = FramePoolService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FramePoolService {
            pool: self.pool.clone(),
            inner,
        }
    }
}

/// Inject the [`Pool`][`Pool`] into the [`Service`] [`Context`] of the inner [`Service`]
#[derive(Clone, Debug)]
pub struct FramePoolService<S> {
    pool: Pool,
    inner: S,
}

impl<State, S> Service<State, Frame> for FramePoolService<S>
where
    S: Service<Pool, Frame, Response = Frame>,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let (ctx, _) = ctx.swap_state(self.pool.clone());
        self.inner.serve(ctx, req).await
    }
}

/// Inject the [`Pool`][`Pool`] into the [`Service`] [`Context`] of this [`Layer`] using [`RequestPoolService`]
#[derive(Clone, Debug)]
pub struct RequestPoolLayer {
    pool: Pool,
}

impl RequestPoolLayer {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for RequestPoolLayer {
    type Service = RequestPoolService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestPoolService {
            pool: self.pool.clone(),
            inner,
        }
    }
}

/// Inject the [`Pool`][`Pool`] into the [`Service`] [`Context`] of the inner [`Service`]
#[derive(Clone, Debug)]
pub struct RequestPoolService<S> {
    pool: Pool,
    inner: S,
}

impl<State, S, Q> Service<State, Q> for RequestPoolService<S>
where
    Q: Request,
    S: Service<Pool, Q>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    /// serve the request, injecting the pool into the context of the inner service
    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        let (ctx, _) = ctx.swap_state(self.pool.clone());
        self.inner.serve(ctx, req).await
    }
}

/// API client using a [`Connection`] [`Pool`]
#[derive(Clone, Debug)]
pub struct Client {
    service:
        RequestPoolService<RequestConnectionService<FrameBytesService<BytesConnectionService>>>,
}

impl Client {
    /// Create a new client using the supplied pool
    pub fn new(pool: Pool) -> Self {
        let service = (
            RequestPoolLayer::new(pool),
            RequestConnectionLayer,
            FrameBytesLayer,
        )
            .into_layer(BytesConnectionService);

        Self { service }
    }

    /// Make an API request using the connection from the pool
    pub async fn call<Q>(&self, req: Q) -> Result<Q::Response, Error>
    where
        Q: Request,
        Error: From<<<Q as Request>::Response as TryFrom<Body>>::Error>,
    {
        self.service.serve(Context::default(), req).await
    }
}

/// A [`Layer`] that takes a [`Connection`] from the [`Pool`] calling an inner [`Service`] with that [`Connection`] as [`Context`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameConnectionLayer;

impl<S> Layer<S> for FrameConnectionLayer {
    type Service = FrameConnectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that takes a [`Connection`] from the [`Pool`] calling an inner [`Service`] with that [`Connection`] as [`Context`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameConnectionService<S> {
    inner: S,
}

impl<S> Service<Pool, Frame> for FrameConnectionService<S>
where
    S: Service<Object<ConnectionManager>, Frame, Response = Frame>,
    S::Error: From<Error> + From<PoolError<Error>> + From<nisshi_sans_io::Error>,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<Pool>, req: Frame) -> Result<Self::Response, Self::Error> {
        debug!(?req);

        let api_key = req.api_key()?;
        let api_version = req.api_version()?;
        let client_id = req
            .client_id()
            .map(|client_id| client_id.map(|client_id| client_id.to_string()))?;

        let pool = ctx.state();
        status_update(pool);

        let connection = {
            let start = SystemTime::now();
            pool.get().await.inspect(|_| {
                POOL_GET_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[],
                );
            })?
        };

        let correlation_id = connection.correlation_id;

        let frame = Frame {
            size: 0,
            header: Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            },
            body: req.body,
        };

        let (ctx, _) = ctx.swap_state(connection);

        self.inner.serve(ctx, frame).await
    }
}

/// A [`Layer`] of [`RequestConnectionService`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestConnectionLayer;

impl<S> Layer<S> for RequestConnectionLayer {
    type Service = RequestConnectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// Take a [`Connection`] from the [`Pool`]. Enclose the [`Request`]
/// in a [`Frame`] using latest API version supported by the broker. Call the
/// inner service with the [`Frame`] using the [`Connection`] as [`Context`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestConnectionService<S> {
    inner: S,
}

impl<Q, S> Service<Pool, Q> for RequestConnectionService<S>
where
    Q: Request,
    S: Service<Object<ConnectionManager>, Frame, Response = Frame>,
    S::Error: From<Error>
        + From<PoolError<Error>>
        + From<nisshi_sans_io::Error>
        + From<<Q::Response as TryFrom<Body>>::Error>,
{
    type Response = Q::Response;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<Pool>, req: Q) -> Result<Self::Response, Self::Error> {
        debug!(?req);
        let pool = ctx.state();
        status_update(pool);

        let api_key = Q::KEY;
        let api_version = pool.manager().api_version(api_key)?;
        let client_id = pool.manager().client_id();
        let connection = {
            let start = SystemTime::now();
            pool.get().await.inspect(|_| {
                POOL_GET_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[],
                );
            })?
        };

        let correlation_id = connection.correlation_id;

        let frame = Frame {
            size: 0,
            header: Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            },
            body: req.into(),
        };

        let (ctx, _) = ctx.swap_state(connection);

        let frame = self.inner.serve(ctx, frame).await?;

        Q::Response::try_from(frame.body)
            .inspect(|response| debug!(?response))
            .map_err(Into::into)
    }
}

/// A [`Service`] that writes a frame represented by [`Bytes`] to a [`Connection`] [`Context`], returning the [`Bytes`] frame response.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesConnectionService;

impl BytesConnectionService {
    async fn write(
        &self,
        stream: &mut TcpStream,
        frame: Bytes,
        attributes: &[KeyValue],
    ) -> Result<(), Error> {
        debug!(frame = ?&frame[..]);

        let start = SystemTime::now();

        stream
            .write_all(&frame[..])
            .await
            .inspect(|_| {
                TCP_SEND_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes,
                );

                TCP_BYTES_SENT.add(frame.len() as u64, attributes);
            })
            .inspect_err(|_| {
                TCP_SEND_ERRORS.add(1, attributes);
            })
            .map_err(Into::into)
    }

    async fn read(&self, stream: &mut TcpStream, attributes: &[KeyValue]) -> Result<Bytes, Error> {
        let start = SystemTime::now();

        let mut size = [0u8; 4];
        _ = stream.read_exact(&mut size).await?;

        let mut buffer: Vec<u8> = vec![0u8; frame_length(size)];
        buffer[0..size.len()].copy_from_slice(&size[..]);
        _ = stream
            .read_exact(&mut buffer[4..])
            .await
            .inspect(|_| {
                TCP_RECEIVE_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes,
                );

                TCP_BYTES_RECEIVED.add(buffer.len() as u64, attributes);
            })
            .inspect_err(|_| {
                TCP_RECEIVE_ERRORS.add(1, attributes);
            })?;

        Ok(Bytes::from(buffer)).inspect(|frame| debug!(frame = ?&frame[..]))
    }
}

impl Service<Object<ConnectionManager>, Bytes> for BytesConnectionService {
    type Response = Bytes;
    type Error = Error;

    async fn serve(
        &self,
        mut ctx: Context<Object<ConnectionManager>>,
        req: Bytes,
    ) -> Result<Self::Response, Self::Error> {
        let c = ctx.state_mut();

        let local = c.stream.local_addr()?;
        let peer = c.stream.peer_addr()?;

        let attributes = [KeyValue::new("peer", peer.to_string())];

        let span = span!(Level::DEBUG, "client", local = %local, peer = %peer);

        async move {
            self.write(&mut c.stream, req, &attributes).await?;

            c.correlation_id += 1;

            self.read(&mut c.stream, &attributes).await
        }
        .instrument(span)
        .await
    }
}

fn frame_length(encoded: [u8; 4]) -> usize {
    i32::from_be_bytes(encoded) as usize + encoded.len()
}

static TCP_CONNECT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tcp_connect_duration")
        .with_unit("ms")
        .with_description("The TCP connect latencies in milliseconds")
        .build()
});

static TCP_CONNECT_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_connect_errors")
        .with_description("TCP connect errors")
        .build()
});

static TCP_SEND_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tcp_send_duration")
        .with_unit("ms")
        .with_description("The TCP send latencies in milliseconds")
        .build()
});

static TCP_SEND_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_send_errors")
        .with_description("TCP send errors")
        .build()
});

static TCP_RECEIVE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tcp_receive_duration")
        .with_unit("ms")
        .with_description("The TCP receive latencies in milliseconds")
        .build()
});

static TCP_RECEIVE_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_receive_errors")
        .with_description("TCP receive errors")
        .build()
});

static TCP_BYTES_SENT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_bytes_sent")
        .with_description("TCP bytes sent")
        .build()
});

static TCP_BYTES_RECEIVED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_bytes_received")
        .with_description("TCP bytes received")
        .build()
});

static POOL_GET_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("pool_get_duration")
        .with_unit("ms")
        .with_description("The Pool Get latencies in milliseconds")
        .build()
});

static POOL_MAX_SIZE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("pool_max_size")
        .with_description("The maximum size of the pool")
        .build()
});

static POOL_CURRENT_SIZE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("pool_current_size")
        .with_description("The current size of the pool")
        .build()
});

static POOL_AVAILABLE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("pool_available")
        .with_description("The number of available objects in the pool")
        .build()
});

static POOL_WAITING: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("pool_waiting")
        .with_description("The number of waiting objects in the pool")
        .build()
});

#[cfg(test)]
mod tests {
    use std::{fs::File, thread};

    use nisshi_sans_io::{MetadataRequest, MetadataResponse};
    use nisshi_service::{
        BytesFrameLayer, FrameRouteService, RequestLayer, ResponseService, TcpBytesLayer,
        TcpContextLayer, TcpListenerLayer,
    };
    use tokio::{net::TcpListener, task::JoinSet};
    use tokio_util::sync::CancellationToken;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;

    fn init_tracing() -> Result<DefaultGuard, Error> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    async fn server(cancellation: CancellationToken, listener: TcpListener) -> Result<(), Error> {
        let server = (
            TcpListenerLayer::new(cancellation),
            TcpContextLayer::default(),
            TcpBytesLayer::default(),
            BytesFrameLayer::default(),
        )
            .into_layer(
                FrameRouteService::builder()
                    .with_service(RequestLayer::<MetadataRequest>::new().into_layer(
                        ResponseService::new(|_ctx: Context<()>, _req: MetadataRequest| {
                            Ok::<_, Error>(
                                MetadataResponse::default()
                                    .brokers(Some([].into()))
                                    .topics(Some([].into()))
                                    .cluster_id(Some("abc".into()))
                                    .controller_id(Some(111))
                                    .throttle_time_ms(Some(0))
                                    .cluster_authorized_operations(Some(-1)),
                            )
                        }),
                    ))
                    .and_then(|builder| builder.build())?,
            );

        server.serve(Context::default(), listener).await
    }

    #[tokio::test]
    async fn tcp_client_server() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let cancellation = CancellationToken::new();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;

        let mut join = JoinSet::new();

        let _server = {
            let cancellation = cancellation.clone();
            join.spawn(async move { server(cancellation, listener).await })
        };

        let origin = (
            RequestPoolLayer::new(
                ConnectionManager::builder(
                    Url::parse(&format!("tcp://{local_addr}")).inspect(|url| debug!(%url))?,
                )
                .client_id(Some(env!("CARGO_PKG_NAME").into()))
                .build()
                .await
                .inspect(|pool| debug!(?pool))?,
            ),
            RequestConnectionLayer,
            FrameBytesLayer,
        )
            .into_layer(BytesConnectionService);

        let response = origin
            .serve(
                Context::default(),
                MetadataRequest::default()
                    .topics(Some([].into()))
                    .allow_auto_topic_creation(Some(false))
                    .include_cluster_authorized_operations(Some(false))
                    .include_topic_authorized_operations(Some(false)),
            )
            .await?;

        assert_eq!(Some("abc"), response.cluster_id.as_deref());
        assert_eq!(Some(111), response.controller_id);

        cancellation.cancel();

        let joined = join.join_all().await;
        debug!(?joined);

        Ok(())
    }
}
