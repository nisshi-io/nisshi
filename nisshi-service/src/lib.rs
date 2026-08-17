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

//! Common service layers used in other Nisshi crates.
//!
//! ## Overview
//!
//! This crate provides [Layer][`rama::Layer`] and [Service][`rama::Service`]
//! implementations for operating on [`Frame`], [`Body`], [Request][`nisshi_sans_io::Request`]
//! and [Response][`nisshi_sans_io::Response`].
//!
//! The following transports are provided:
//!
//! - TCP with [`TcpBytesLayer`] and [`BytesTcpService`].
//! - [`MPSC channel`][`tokio::sync::mpsc`] with [`ChannelFrameLayer`] and [`ChannelFrameService`].
//! - [`Bytes`][`bytes::Bytes`] with [`BytesLayer`] (designed primarily for protocol testing)
//!
//! ### Routing
//!
//! Route [`Frame`] to services using [`FrameRouteService`] to automatically
//! implement [`ApiVersionsRequest`][`nisshi_sans_io::ApiVersionsRequest`]
//! with valid protocol ranges:
//!
//! ```
//! # use nisshi_service::Error;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! # use rama::{Context, Layer as _, Service as _};
//! # use nisshi_sans_io::{ApiKey as _, ApiVersionsRequest, MetadataRequest, MetadataResponse};
//! # use nisshi_service::{
//! #     BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
//! #     FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService, RequestLayer,
//! #     ResponseService,
//! # };
//! let frame_route = FrameRouteService::<(), Error>::builder()
//!     .with_service(
//!         RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_, _| {
//!             Ok(MetadataResponse::default()
//!                 .brokers(Some([].into()))
//!                 .topics(Some([].into()))
//!                 .cluster_id(Some("nisshi".into()))
//!                 .controller_id(Some(111))
//!                 .throttle_time_ms(Some(0))
//!                 .cluster_authorized_operations(Some(-1)))
//!         })),
//!     )
//!     .and_then(|builder| builder.build())?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Layering
//!
//! Composing [`RequestFrameLayer`], [`FrameBytesLayer`], [`BytesLayer`],
//! [`BytesFrameLayer`] together into `frame_route` to implement a test protocol stack.
//!
//! A "client" [`Frame`] is marshalled into bytes using [`FrameBytesLayer`], with [`BytesLayer`] connecting
//! to a "server" that demarshalls using [`BytesFrameLayer`] back into frames,
//! routing into `frame_route` (above) to [`MetadataRequest`][`nisshi_sans_io::MetadataRequest`]
//! or [`ApiVersionsRequest`][`nisshi_sans_io::ApiVersionsRequest`] depending on the
//! [API key][`Frame#method.api_key`]:
//!
//! ```
//! # use nisshi_service::Error;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! # use rama::{Context, Layer as _, Service as _};
//! # use nisshi_sans_io::{ApiKey as _, ApiVersionsRequest, MetadataRequest, MetadataResponse};
//! # use nisshi_service::{
//! #     BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
//! #     FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService, RequestLayer,
//! #     ResponseService,
//! # };
//! # let frame_route = FrameRouteService::<(), Error>::builder()
//! #     .with_service(
//! #         RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_, _| {
//! #             Ok(MetadataResponse::default()
//! #                 .brokers(Some([].into()))
//! #                 .topics(Some([].into()))
//! #                 .cluster_id(Some("nisshi".into()))
//! #                 .controller_id(Some(111))
//! #                 .throttle_time_ms(Some(0))
//! #                 .cluster_authorized_operations(Some(-1)))
//! #         })),
//! #     )
//! #     .and_then(|builder| builder.build())?;
//!   let service = (
//!       // "client" initiator side:
//!       RequestFrameLayer,
//!       FrameBytesLayer,
//!
//!       // transport
//!       BytesLayer,
//!
//!       // "server" side:
//!       BytesFrameLayer::default(),
//!   )
//!       .into_layer(frame_route);
//! # Ok(())
//! # }
//! ```
//!
//! In the broker, proxy and CLI clients, [`BytesLayer`] is replaced with
//! [`TcpBytesLayer`] (server side) or [`BytesTcpService`] (client/initiator side).
//!
//! ### Servicing
//!
//! We construct a default [`service context`][`rama::Context`] and a
//! [`MetadataRequest`][`nisshi_sans_io::MetadataRequest`] to initiate a request
//! on the `service`. The request passes through the protocol stack
//! and routed into our service. The service responds with a
//! [`MetadataResponse`][`nisshi_sans_io::MetadataResponse`], so that we can
//! verify the expected `response.cluster_id`:
//!
//! ```
//! # use nisshi_service::Error;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! # use rama::{Context, Layer as _, Service as _};
//! # use nisshi_sans_io::{ApiKey as _, ApiVersionsRequest, MetadataRequest, MetadataResponse};
//! # use nisshi_service::{
//! #     BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
//! #     FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService, RequestLayer,
//! #     ResponseService,
//! # };
//! # let frame_route = FrameRouteService::<(), Error>::builder()
//! #     .with_service(
//! #         RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_, _| {
//! #             Ok(MetadataResponse::default()
//! #                 .brokers(Some([].into()))
//! #                 .topics(Some([].into()))
//! #                 .cluster_id(Some("nisshi".into()))
//! #                 .controller_id(Some(111))
//! #                 .throttle_time_ms(Some(0))
//! #                 .cluster_authorized_operations(Some(-1)))
//! #         })),
//! #     )
//! #     .and_then(|builder| builder.build())?;
//! # let service = (
//! #      RequestFrameLayer,
//! #      FrameBytesLayer,
//! #      BytesLayer,
//! #      BytesFrameLayer::default(),
//! #  )
//! #      .into_layer(frame_route);
//!   let request = MetadataRequest::default()
//!       .topics(Some([].into()))
//!       .allow_auto_topic_creation(Some(false))
//!       .include_cluster_authorized_operations(Some(false))
//!       .include_topic_authorized_operations(Some(false));
//!
//!   let response = service.serve(Context::default(), request).await?;
//!
//!   assert_eq!(Some("nisshi".into()), response.cluster_id);
//! # Ok(())
//! # }
//! ```
//!
//! The [`FrameRouteService`] automatically implements
//! [`ApiVersionsRequest`][`nisshi_sans_io::ApiVersionsRequest`]
//! with valid protocol ranges for all defined services. An
//! [`ApiVersionsResponse`][`nisshi_sans_io::ApiVersionsResponse`] contains
//! version information for both [`MetadataRequest`][`nisshi_sans_io::MetadataRequest`]
//! and [`ApiVersionsRequest`][`nisshi_sans_io::ApiVersionsRequest`]:
//!
//! ```
//! # use nisshi_service::Error;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! # use rama::{Context, Layer as _, Service as _};
//! # use nisshi_sans_io::{ApiKey as _, ApiVersionsRequest, MetadataRequest, MetadataResponse};
//! # use nisshi_service::{
//! #     BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
//! #     FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService, RequestLayer,
//! #     ResponseService,
//! # };
//! # let frame_route = FrameRouteService::<(), Error>::builder()
//! #     .with_service(
//! #         RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_, _| {
//! #             Ok(MetadataResponse::default()
//! #                 .brokers(Some([].into()))
//! #                 .topics(Some([].into()))
//! #                 .cluster_id(Some("nisshi".into()))
//! #                 .controller_id(Some(111))
//! #                 .throttle_time_ms(Some(0))
//! #                 .cluster_authorized_operations(Some(-1)))
//! #         })),
//! #     )
//! #     .and_then(|builder| builder.build())?;
//! # let service = (
//! #      RequestFrameLayer,
//! #      FrameBytesLayer,
//! #      BytesLayer,
//! #      BytesFrameLayer::default(),
//! #  )
//! #      .into_layer(frame_route);
//! let response = service
//!     .serve(
//!         Context::default(),
//!         ApiVersionsRequest::default()
//!             .client_software_name(Some("abcba".into()))
//!             .client_software_version(Some("1.2321".into())),
//!     )
//!     .await?;
//!
//! let api_versions = response
//!     .api_keys
//!     .unwrap_or_default()
//!     .into_iter()
//!     .map(|api_version| api_version.api_key)
//!     .collect::<Vec<_>>();
//!
//! assert_eq!(2, api_versions.len());
//! assert!(api_versions.contains(&ApiVersionsRequest::KEY));
//! assert!(api_versions.contains(&MetadataRequest::KEY));
//! # Ok(())
//! # }
//! ```

use std::{
    fmt, io,
    net::SocketAddr,
    ops::Range,
    sync::{Arc, LazyLock, Mutex, PoisonError},
    time::{Duration, SystemTime},
};

use nisshi_sans_io::{Body, Frame};
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Histogram, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rama::{Context, Layer, Service};
use rand::{prelude::*, rngs::SmallRng};
use tokio::{net::lookup_host, sync::oneshot, task::JoinError, time::sleep};
use tracing::{debug, instrument};
use url::Url;

mod api;
mod channel;
mod consumer;
mod frame;
mod stream;

pub use api::{ApiVersionsService, FrameRouteBuilder, FrameRouteService};

pub use channel::{
    ChannelFrameLayer, ChannelFrameService, FrameChannelService, FrameReceiver, FrameSender,
    bounded_channel,
};

pub use consumer::{ConsumerGroupLayer, ConsumerGroupService};

pub use frame::{
    BodyRequestLayer, BytesFrameLayer, BytesFrameService, FrameApiKeyMatcher, FrameBodyLayer,
    FrameBytesLayer, FrameBytesService, FrameRequestLayer, FrameService, RequestApiKeyMatcher,
    RequestFrameLayer, RequestFrameService, RequestLayer, ResponseService,
};

pub use stream::{
    BytesLayer, BytesService, BytesTcpService, TcpBytesLayer, TcpBytesService, TcpContext,
    TcpContextLayer, TcpContextService, TcpListenerLayer,
};

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    Auth(#[from] nisshi_auth::Error),
    DuplicateRoute(i16),
    FrameTooBig(usize),
    Io(Arc<io::Error>),
    Join(Arc<JoinError>),
    Message(String),
    OneshotRecv(oneshot::error::RecvError),
    Poison,
    Parse(#[from] url::ParseError),
    Protocol(#[from] nisshi_sans_io::Error),
    UnableToSend(Box<Frame>),
    UnknownHost(Url),
    UnknownServiceBody(Box<Body>),
    UnknownServiceFrame(Box<Frame>),
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

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

fn frame_length(encoded: [u8; 4]) -> usize {
    i32::from_be_bytes(encoded) as usize + encoded.len()
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

/// Return the socket address for a given URL
///
/// Recording DNS lookup timings in the `DNS_LOOKUP_DURATION` histogram.
///
/// ```rust
/// # use nisshi_service::{Error, host_port};
/// # use url::Url;
/// # use std::net::Ipv4Addr;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let earl = Url::parse("tcp://localhost:9092")?;
/// let sock_addr = host_port(earl).await?;
/// assert_eq!(sock_addr.ip(), Ipv4Addr::new(127, 0, 0, 1));
/// assert_eq!(sock_addr.port(), 9092);
/// # Ok(())
/// # }
/// ```
pub async fn host_port(url: Url) -> Result<SocketAddr, Error> {
    if let Some(host) = url.host_str()
        && let Some(port) = url.port()
    {
        let attributes = [KeyValue::new("url", url.to_string())];
        let start = SystemTime::now();

        let mut addresses = lookup_host(format!("{host}:{port}"))
            .await
            .inspect(|_| {
                DNS_LOOKUP_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &attributes,
                )
            })?
            .filter(|socket_addr| matches!(socket_addr, SocketAddr::V4(_)));

        if let Some(socket_addr) = addresses.next().inspect(|socket_addr| debug!(?socket_addr)) {
            return Ok(socket_addr);
        }
    }

    Err(Error::UnknownHost(url))
}

pub(crate) static DNS_LOOKUP_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("dns_lookup_duration")
        .with_unit("ms")
        .with_description("DNS lookup latencies")
        .build()
});

pub(crate) static REQUEST_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_request_size")
        .with_unit("By")
        .with_description("The API request size in bytes")
        .build()
});

pub(crate) static RESPONSE_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_response_size")
        .with_unit("By")
        .with_description("The API response size in bytes")
        .build()
});

pub(crate) static REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_request_duration")
        .with_unit("ms")
        .with_description("The API request latencies in milliseconds")
        .build()
});

pub(crate) static API_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_api_requests")
        .with_description("The number of API requests made")
        .build()
});

pub(crate) static API_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_api_errors")
        .with_description("The number of API errors")
        .build()
});

pub(crate) static BYTES_SENT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_bytes_sent")
        .with_description("The number of bytes sent")
        .build()
});

pub(crate) static BYTES_RECEIVED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_bytes_received")
        .with_description("The number of bytes received")
        .build()
});

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct LatencyIntroducingLayer {
    seed: u64,
    latency: Range<u64>,
}

impl LatencyIntroducingLayer {
    pub fn with_seed(self, seed: u64) -> Self {
        Self { seed, ..self }
    }

    pub fn with_latency_millis(self, latency: Range<u64>) -> Self {
        Self { latency, ..self }
    }
}

impl<S> Layer<S> for LatencyIntroducingLayer {
    type Service = LatencyIntroducingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service::new(inner)
            .with_seed(self.seed)
            .with_latency(self.latency.clone())
    }
}

#[derive(Clone)]
pub struct LatencyIntroducingService<S> {
    inner: S,
    rng: Arc<Mutex<SmallRng>>,
    latency: Range<u64>,
}

impl<S> fmt::Debug for LatencyIntroducingService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(LatencyIntroducingService))
            .field("latency", &self.latency)
            .finish_non_exhaustive()
    }
}

impl<S> LatencyIntroducingService<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            rng: Arc::new(Mutex::new(SmallRng::seed_from_u64(0))),
            latency: 50..150,
        }
    }

    pub fn with_seed(self, seed: u64) -> Self {
        Self {
            rng: Arc::new(Mutex::new(SmallRng::seed_from_u64(seed))),
            ..self
        }
    }

    pub fn with_latency(self, latency: Range<u64>) -> Self {
        Self { latency, ..self }
    }

    #[instrument(skip_all)]
    async fn introduce_latency(&self) -> Result<(), Error> {
        let latency = self
            .rng
            .lock()
            .map(|mut rng| rng.random_range(self.latency.clone()))
            .map(Duration::from_millis)
            .inspect(|latency| debug!(?latency))?;

        sleep(latency).await;

        Ok(())
    }
}

impl<S, Q, State> Service<State, Q> for LatencyIntroducingService<S>
where
    S: Service<State, Q>,
    Q: Send + 'static,
    S::Error: From<Error>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;

    type Error = S::Error;

    #[instrument(skip(ctx, req))]
    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        self.introduce_latency().await?;
        let result = self.inner.serve(ctx, req).await;
        self.introduce_latency().await?;
        result
    }
}
