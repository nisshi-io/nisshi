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

use std::{
    error::{self},
    fmt::Debug,
    io,
    marker::PhantomData,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use nanoid::nanoid;
use opentelemetry::KeyValue;
use rama::{Context, Layer, Service};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument};

use crate::{
    BYTES_RECEIVED, BYTES_SENT, Error, REQUEST_DURATION, REQUEST_SIZE, RESPONSE_SIZE, frame_length,
    frame_size,
};

/// The largest request payload a listener accepts unless [`TcpContext::maximum_frame_size`]
/// says otherwise, matching the Apache Kafka default for `socket.request.max.bytes`.
///
/// The size prefix is read before authentication, so an unbounded listener lets
/// a client make it allocate up to 2 GiB per connection by sending 4 bytes.
pub const DEFAULT_MAXIMUM_FRAME_SIZE: usize = 100 * 1024 * 1024;

/// How long an otherwise-idle connection may wait for the next request to begin
/// before it's closed, unless [`TcpContext::connection_idle_timeout`] says
/// otherwise, matching the Apache Kafka default for `connections.max.idle.ms`.
///
/// Long by design: a legitimate consumer or producer connection can sit idle
/// between requests for a while.
pub const DEFAULT_CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// How long a request or response transfer already in progress may stall
/// between successive chunks before it's abandoned, unless
/// [`TcpContext::io_idle_timeout`] says otherwise.
///
/// Deliberately much shorter than [`DEFAULT_CONNECTION_IDLE_TIMEOUT`]: once a
/// peer has committed to sending or receiving, it shouldn't stall for minutes.
/// Without this, a peer that declares a large frame and then goes fully
/// quiet mid-transfer would hold the connection open indefinitely even
/// though the connection idle timeout never fires (that one only guards the
/// gap *before* a request starts).
///
/// This bounds a stall, not a slow trickle: a peer sending one byte just
/// under this deadline, repeatedly, still resets it every time and can hold
/// a single connection open indefinitely. Closing that fully needs a
/// per-frame minimum-throughput floor or a connection cap; out of scope
/// here, tracked as a follow-up.
pub const DEFAULT_IO_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Bytes read from (or written to) the peer in one chunk while a transfer is
/// in progress, keeping [`TcpBytesService::read`]'s buffer growth bounded and
/// the idle-timeout deadline reset at a reasonable cadence.
const IO_CHUNK_SIZE: usize = 64 * 1024;

/// Reads one chunk into `buf`, bounding the wait by `idle_timeout` (if any).
///
/// Unlike wrapping a whole `read_exact` in one deadline, resetting this timeout
/// on every chunk means a slow-but-progressing transfer is never killed, only
/// a genuinely stalled one.
async fn read_chunk<R>(
    req: &mut R,
    buf: &mut [u8],
    idle_timeout: Option<Duration>,
) -> io::Result<usize>
where
    R: AsyncReadExt + Unpin,
{
    match idle_timeout {
        Some(idle_timeout) => timeout(idle_timeout, req.read(buf))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?,
        None => req.read(buf).await,
    }
}

/// Writes one chunk from `buf`, bounding the wait by `idle_timeout` (if any).
async fn write_chunk<W>(
    req: &mut W,
    buf: &[u8],
    idle_timeout: Option<Duration>,
) -> io::Result<usize>
where
    W: AsyncWriteExt + Unpin,
{
    match idle_timeout {
        Some(idle_timeout) => timeout(idle_timeout, req.write(buf))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?,
        None => req.write(buf).await,
    }
}

/// Reads exactly `buf.len()` bytes, chunk by chunk, resetting `idle_timeout`
/// after every chunk of forward progress.
async fn read_exact_with_idle_timeout<R>(
    req: &mut R,
    buf: &mut [u8],
    idle_timeout: Option<Duration>,
) -> io::Result<()>
where
    R: AsyncReadExt + Unpin,
{
    let mut read = 0;

    while read < buf.len() {
        let n = read_chunk(req, &mut buf[read..], idle_timeout).await?;

        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }

        read += n;
    }

    Ok(())
}

/// A [`Layer`] that listens for TCP connections
#[derive(Clone, Debug, Default)]
pub struct TcpListenerLayer {
    cancellation: CancellationToken,
}

impl TcpListenerLayer {
    pub fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }
}

impl<S> Layer<S> for TcpListenerLayer {
    type Service = TcpListenerService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            cancellation: self.cancellation.clone(),
            inner,
        }
    }
}

/// A [`Service`] that listens for TCP connections
#[derive(Clone, Default)]
pub struct TcpListenerService<S> {
    cancellation: CancellationToken,
    inner: S,
}

impl<S> Debug for TcpListenerService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TcpListenerService)).finish()
    }
}

impl<State, S> Service<State, TcpListener> for TcpListenerService<S>
where
    S: Service<State, TcpStream> + Clone,
    S::Response: Debug,
    S::Error: error::Error,
    State: Clone + Send + Sync + 'static,
{
    type Response = ();
    type Error = S::Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<State>,
        req: TcpListener,
    ) -> Result<Self::Response, Self::Error> {
        let mut set = JoinSet::new();

        loop {
            tokio::select! {
                Ok((stream, addr)) = req.accept() => {
                    debug!(?req, ?stream, %addr);

                    let service = self.inner.clone();
                    let ctx = ctx.clone();

                    let handle = set.spawn(async move {
                            match service.serve(ctx, stream).await {
                                Err(error) => {
                                    debug!(%addr, %error);
                                },

                                Ok(response) => {
                                    debug!(%addr, ?response)
                                }
                        }
                    });

                    debug!(?handle);
                    continue;
                }

                v = set.join_next(), if !set.is_empty() => {
                    debug!(?v);
                }

                cancelled = self.cancellation.cancelled() => {
                    debug!(?cancelled);
                    break;
                }
            }
        }

        Ok(())
    }
}

/// A [context state][`Context#method.state`] state used by [`TcpContextLayer`] and [`TcpContextService`]
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct TcpContext {
    cluster_id: Option<String>,
    maximum_frame_size: Option<usize>,
    connection_idle_timeout: Option<Duration>,
    io_idle_timeout: Option<Duration>,
}

impl Default for TcpContext {
    fn default() -> Self {
        Self {
            cluster_id: None,
            maximum_frame_size: Some(DEFAULT_MAXIMUM_FRAME_SIZE),
            connection_idle_timeout: Some(DEFAULT_CONNECTION_IDLE_TIMEOUT),
            io_idle_timeout: Some(DEFAULT_IO_IDLE_TIMEOUT),
        }
    }
}

impl TcpContext {
    pub fn cluster_id(self, cluster_id: Option<String>) -> Self {
        Self { cluster_id, ..self }
    }

    /// Largest request payload (excluding the 4 byte size prefix) this listener
    /// reads, or `None` for no limit. Defaults to [`DEFAULT_MAXIMUM_FRAME_SIZE`].
    pub fn maximum_frame_size(self, maximum_frame_size: Option<usize>) -> Self {
        Self {
            maximum_frame_size,
            ..self
        }
    }

    /// How long an otherwise-idle connection may wait for the next request to
    /// begin, or `None` for no limit. Defaults to
    /// [`DEFAULT_CONNECTION_IDLE_TIMEOUT`].
    pub fn connection_idle_timeout(self, connection_idle_timeout: Option<Duration>) -> Self {
        Self {
            connection_idle_timeout,
            ..self
        }
    }

    /// How long a request or response transfer already in progress may stall
    /// between chunks before it's abandoned, or `None` for no limit. Defaults
    /// to [`DEFAULT_IO_IDLE_TIMEOUT`].
    pub fn io_idle_timeout(self, io_idle_timeout: Option<Duration>) -> Self {
        Self {
            io_idle_timeout,
            ..self
        }
    }
}

/// A [`Layer`] that injects the [`TcpContext`] into the service [`Context`] state
#[derive(Clone, Debug, Default)]
pub struct TcpContextLayer {
    state: TcpContext,
}

impl TcpContextLayer {
    pub fn new(state: TcpContext) -> Self {
        Self { state }
    }
}

impl<S> Layer<S> for TcpContextLayer {
    type Service = TcpContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            state: self.state.clone(),
        }
    }
}

/// A [`Service`] that requires the [`TcpContext`] as the service [`Context`] state
#[derive(Clone)]
pub struct TcpContextService<S> {
    inner: S,
    state: TcpContext,
}

impl<S> Debug for TcpContextService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TcpContextService)).finish()
    }
}

impl<State, S> Service<State, TcpStream> for TcpContextService<S>
where
    S: Service<TcpContext, TcpStream>,
    S::Error: From<io::Error>,
    State: Clone + Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    #[instrument(skip_all, fields(peer = %req.peer_addr()?))]
    async fn serve(
        &self,
        ctx: Context<State>,
        req: TcpStream,
    ) -> Result<Self::Response, Self::Error> {
        let (ctx, _) = ctx.swap_state(self.state.clone());

        self.inner.serve(ctx, req).await
    }
}

/// A [`Service`] writing [`Bytes`] into a [`TcpStream`], responding with a length delimited frame of [`Bytes`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesTcpService;

impl Service<TcpStream, Bytes> for BytesTcpService {
    type Response = Bytes;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        mut ctx: Context<TcpStream>,
        req: Bytes,
    ) -> Result<Self::Response, Self::Error> {
        let stream = ctx.state_mut();

        stream.write_all(&req[..]).await?;
        BYTES_SENT.add(req.len() as u64, &[]);

        let mut size = [0u8; 4];
        _ = stream.read_exact(&mut size).await?;

        let mut buffer: Vec<u8> = vec![0u8; frame_length(size)?];
        buffer[0..size.len()].copy_from_slice(&size[..]);
        _ = stream.read_exact(&mut buffer[4..]).await?;
        BYTES_RECEIVED.add(buffer.len() as u64, &[]);

        Ok(Bytes::from(buffer))
    }
}

/// A [`Layer`] receiving [`Bytes`] from a [`TcpStream`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpBytesLayer<State = ()> {
    _state: PhantomData<State>,
}

impl<S, State> Layer<S> for TcpBytesLayer<State> {
    type Service = TcpBytesService<S, State>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            _state: PhantomData,
        }
    }
}

/// A [`Service`] receiving [`Bytes`] from a [`TcpStream`], calling an inner [`Service`] and sending [`Bytes`] into the [`TcpStream`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpBytesService<S, State> {
    inner: S,
    _state: PhantomData<State>,
}

impl<S, State> Debug for TcpBytesService<S, State> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TcpBytesService)).finish()
    }
}

impl<S, State> TcpBytesService<S, State> {
    fn elapsed_millis(&self, start: SystemTime) -> u64 {
        start
            .elapsed()
            .map_or(0, |duration| duration.as_millis() as u64)
    }
}

/// The [`TcpContext`] limits relevant to a single connection's request/response
/// loop, extracted once per [`TcpBytesService::serve`] call instead of growing
/// the positional argument list on [`TcpBytesService::wait`]/`read`/`write`.
#[derive(Clone, Copy, Debug, Default)]
struct ConnectionLimits {
    maximum_frame_size: Option<usize>,
    connection_idle_timeout: Option<Duration>,
    io_idle_timeout: Option<Duration>,
}

impl<S, State> TcpBytesService<S, State>
where
    S: Service<State, Bytes, Response = Bytes>,
    S::Error: From<Error> + From<io::Error> + Debug,
    State: Clone + Default + Send + Sync + 'static,
{
    #[instrument(skip_all)]
    async fn wait<R>(&self, req: &mut R, limits: ConnectionLimits) -> Result<[u8; 4], S::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let mut size = [0u8; 4];

        read_exact_with_idle_timeout(req, &mut size, limits.connection_idle_timeout)
            .await
            .inspect_err(|err| debug!(?err))?;

        let frame_size = frame_size(size)?;

        if limits
            .maximum_frame_size
            .is_some_and(|maximum_frame_size| frame_size > maximum_frame_size)
        {
            return Err(Into::into(Error::FrameTooBig(frame_size)));
        }

        Ok(size)
    }

    /// Reads the frame body declared by `size`, growing the buffer as bytes
    /// actually arrive rather than allocating the full declared length up
    /// front.
    ///
    /// Without this, a peer that declares a near-maximum frame and then
    /// stalls (or trickles bytes just under [`TcpContext::io_idle_timeout`])
    /// would still pin the full declared size in memory immediately, even
    /// though the idle timeout bounds *how long* that can go on. Growing
    /// incrementally bounds *how much* an unfinished, stalled transfer can
    /// pin at any point before its own idle timeout catches it. This alone
    /// doesn't bound how many such connections a peer can open at once — a
    /// connection-count limit is the complementary control for that, and is
    /// tracked separately.
    #[instrument(skip_all)]
    async fn read<R>(
        &self,
        req: &mut R,
        size: [u8; 4],
        limits: ConnectionLimits,
    ) -> Result<Bytes, S::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let declared = frame_length(size)?;

        let mut request: Vec<u8> = Vec::with_capacity(declared.min(size.len() + IO_CHUNK_SIZE));
        request.extend_from_slice(&size[..]);

        while request.len() < declared {
            let start = request.len();
            let want = (declared - start).min(IO_CHUNK_SIZE);
            request.resize(start + want, 0);

            let n = read_chunk(req, &mut request[start..], limits.io_idle_timeout)
                .await
                .inspect_err(|err| debug!(?err))?;

            if n == 0 {
                return Err(Into::into(io::Error::from(io::ErrorKind::UnexpectedEof)));
            }

            request.truncate(start + n);
        }

        BYTES_RECEIVED.add(request.len() as u64, &[]);

        Ok(Bytes::from(request))
    }

    #[instrument(skip_all)]
    async fn process(
        &self,
        attributes: &[KeyValue],
        ctx: Context<TcpContext>,
        request: Bytes,
    ) -> Result<Bytes, S::Error> {
        REQUEST_SIZE.record(request.len() as u64, attributes);

        let (ctx, _) = ctx.swap_state(State::default());
        let request_start = SystemTime::now();

        self.inner
            .serve(ctx, request)
            .await
            .inspect_err(|err| error!(?err))
            .inspect(|response| {
                RESPONSE_SIZE.record(response.len() as u64, attributes);

                let elapsed_millis = self.elapsed_millis(request_start);

                REQUEST_DURATION.record(elapsed_millis, attributes);
            })
    }

    #[instrument(skip_all)]
    async fn write<W>(
        &self,
        req: &mut W,
        frame: Bytes,
        limits: ConnectionLimits,
    ) -> Result<(), S::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        let mut written = 0;

        while written < frame.len() {
            let n = write_chunk(req, &frame[written..], limits.io_idle_timeout)
                .await
                .inspect_err(|err| debug!(?err))?;

            if n == 0 {
                return Err(Into::into(io::Error::from(io::ErrorKind::WriteZero)));
            }

            written += n;
        }

        BYTES_SENT.add(frame.len() as u64, &[]);

        Ok(())
    }

    #[instrument(skip_all, fields(id = nanoid!()))]
    async fn req<R>(
        &self,
        req: &mut R,
        limits: ConnectionLimits,
        attributes: &[KeyValue],
        ctx: Context<TcpContext>,
    ) -> Result<(), S::Error>
    where
        R: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        let size = self.wait(req, limits).await?;
        let request = self.read(req, size, limits).await?;
        let response = self.process(attributes, ctx, request).await?;
        self.write(req, response, limits).await
    }
}

impl<S, State, Stream> Service<TcpContext, Stream> for TcpBytesService<S, State>
where
    S: Service<State, Bytes, Response = Bytes>,
    S::Error: From<Error> + From<io::Error> + Debug,
    State: Clone + Default + Send + Sync + 'static,
    Stream: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static,
{
    type Response = ();

    type Error = S::Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<TcpContext>,
        mut req: Stream,
    ) -> Result<Self::Response, Self::Error> {
        let attributes = {
            let state = ctx.state();

            let mut attributes = vec![];

            if let Some(cluster_id) = state.cluster_id.clone() {
                attributes.push(KeyValue::new("cluster_id", cluster_id))
            }

            attributes
        };

        let limits = ConnectionLimits {
            maximum_frame_size: ctx.state().maximum_frame_size,
            connection_idle_timeout: ctx.state().connection_idle_timeout,
            io_idle_timeout: ctx.state().io_idle_timeout,
        };

        loop {
            let ctx = ctx.clone();
            let attributes = attributes.clone();

            self.req(&mut req, limits, &attributes[..], ctx).await?
        }
    }
}

/// A [`Layer`] that handles and responds with [`Bytes`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesLayer;

impl<S> Layer<S> for BytesLayer {
    type Service = BytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that handles and responds with [`Bytes`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesService<S> {
    inner: S,
}

impl<S> Debug for BytesService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BytesService)).finish()
    }
}

impl<S, State> Service<State, Bytes> for BytesService<S>
where
    S: Service<State, Bytes, Response = Bytes>,
    State: Clone + Send + Sync + 'static,
{
    type Response = Bytes;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, ctx: Context<State>, req: Bytes) -> Result<Self::Response, Self::Error> {
        debug!(req = ?&req[..]);
        self.inner
            .serve(ctx, req)
            .await
            .inspect(|response| debug!(response = ?&response[..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Inner service standing in for the frame router: echoes the request bytes.
    #[derive(Clone, Copy, Debug, Default)]
    struct Echo;

    impl Service<(), Bytes> for Echo {
        type Response = Bytes;
        type Error = Error;

        async fn serve(&self, _ctx: Context<()>, req: Bytes) -> Result<Bytes, Error> {
            Ok(req)
        }
    }

    fn service() -> TcpBytesService<Echo, ()> {
        TcpBytesLayer::<()>::default().into_layer(Echo)
    }

    fn header(size: i32) -> [u8; 4] {
        size.to_be_bytes()
    }

    /// Limits with only `maximum_frame_size` set; the two idle timeouts stay
    /// disabled (`None`) so frame-size tests aren't sensitive to timing.
    fn limits(maximum_frame_size: Option<usize>) -> ConnectionLimits {
        ConnectionLimits {
            maximum_frame_size,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn frame_within_limit_is_accepted() -> Result<(), Error> {
        let mut reader = &header(12)[..];

        let size = service().wait(&mut reader, limits(Some(1024))).await?;

        assert_eq!(header(12), size);
        Ok(())
    }

    #[tokio::test]
    async fn frame_exactly_at_limit_is_accepted() -> Result<(), Error> {
        let mut reader = &header(1024)[..];

        let size = service().wait(&mut reader, limits(Some(1024))).await?;

        assert_eq!(header(1024), size);
        Ok(())
    }

    #[tokio::test]
    async fn frame_over_limit_is_rejected() {
        let mut reader = &header(1025)[..];

        let err = service()
            .wait(&mut reader, limits(Some(1024)))
            .await
            .expect_err("oversized frame must be rejected before the body is read");

        assert!(matches!(err, Error::FrameTooBig(1025)), "{err:?}");
    }

    #[test]
    fn listeners_are_bounded_by_default() {
        assert_eq!(
            Some(DEFAULT_MAXIMUM_FRAME_SIZE),
            TcpContext::default().maximum_frame_size
        );
    }

    #[test]
    fn idle_timeouts_are_set_by_default() {
        let ctx = TcpContext::default();
        assert_eq!(
            Some(DEFAULT_CONNECTION_IDLE_TIMEOUT),
            ctx.connection_idle_timeout
        );
        assert_eq!(Some(DEFAULT_IO_IDLE_TIMEOUT), ctx.io_idle_timeout);
    }

    #[tokio::test]
    async fn negative_frame_length_is_rejected() {
        let mut reader = &header(-1)[..];

        let err = service()
            .wait(&mut reader, limits(None))
            .await
            .expect_err("negative frame length must be rejected");

        assert!(matches!(err, Error::InvalidFrameLength(-1)), "{err:?}");
    }

    #[tokio::test]
    async fn serve_rejects_oversized_frame_without_reading_body() -> Result<(), Error> {
        let (mut client, server) = tokio::io::duplex(64);

        let ctx = Context::with_state(TcpContext::default().maximum_frame_size(Some(1024)));
        let handle = tokio::spawn(async move { service().serve(ctx, server).await });

        // Only the length prefix is sent. If the guard admitted the frame,
        // `read` would block waiting for a body that never arrives, so the
        // timeout is what turns that into a failure.
        client.write_all(&header(65_536)).await?;

        let outcome = timeout(Duration::from_secs(5), handle)
            .await
            .expect("oversized frame was admitted: serve is blocked reading the body")?;

        assert!(
            matches!(outcome, Err(Error::FrameTooBig(65_536))),
            "{outcome:?}"
        );
        Ok(())
    }

    /// A peer that opens a connection and never sends anything must not hold
    /// the connection (and its task) open forever — this is the Slowloris
    /// vector the connection idle timeout closes. `start_paused` lets tokio
    /// fast-forward straight to the timeout deadline instead of a real sleep.
    #[tokio::test(start_paused = true)]
    async fn quiet_connection_is_closed_after_connection_idle_timeout() -> Result<(), Error> {
        let (_client, server) = tokio::io::duplex(64);

        // io_idle_timeout disabled: this test must fail (not hang, thanks to
        // the outer guard below) if wait() is ever wired to the wrong tier.
        let ctx = Context::with_state(
            TcpContext::default()
                .connection_idle_timeout(Some(Duration::from_millis(50)))
                .io_idle_timeout(None),
        );

        let outcome = timeout(Duration::from_secs(5), service().serve(ctx, server))
            .await
            .expect("wait() did not respect connection_idle_timeout");

        assert!(
            matches!(&outcome, Err(Error::Io(err)) if err.kind() == io::ErrorKind::TimedOut),
            "{outcome:?}"
        );
        Ok(())
    }

    /// A peer that declares a frame and then stops sending mid-body must not
    /// hold the connection open forever either — the connection idle timeout
    /// only guards the gap *before* a request starts, so this is what the
    /// (shorter) io idle timeout closes instead.
    #[tokio::test(start_paused = true)]
    async fn stalled_mid_frame_read_times_out() -> Result<(), Error> {
        let (mut client, server) = tokio::io::duplex(4096);

        // connection_idle_timeout disabled: this test must fail (not hang,
        // thanks to the outer guard below) if read() is ever wired to the
        // wrong tier.
        let ctx = Context::with_state(
            TcpContext::default()
                .io_idle_timeout(Some(Duration::from_millis(50)))
                .connection_idle_timeout(None),
        );
        let handle = tokio::spawn(async move { service().serve(ctx, server).await });

        // Declare a 100 byte body, send 10 bytes of it, then go quiet.
        client.write_all(&header(100)).await?;
        client.write_all(&[0u8; 10]).await?;

        let outcome = timeout(Duration::from_secs(5), handle)
            .await
            .expect("read() did not respect io_idle_timeout")?;

        assert!(
            matches!(&outcome, Err(Error::Io(err)) if err.kind() == io::ErrorKind::TimedOut),
            "{outcome:?}"
        );
        Ok(())
    }

    /// A slow-but-steadily-progressing transfer must not be penalized: each
    /// chunk resets the idle deadline, so a transfer that would exceed a
    /// single flat whole-transfer timeout still succeeds as long as no
    /// individual gap between chunks exceeds `io_idle_timeout`. Uses real
    /// time with a generous margin (chunk gap well under the deadline, total
    /// well over it) rather than paused time, to avoid orchestrating a
    /// multi-step manual clock advance around the two concurrent tasks below.
    #[tokio::test]
    async fn slow_but_steady_transfer_is_not_penalized() -> Result<(), Error> {
        let (mut client, server) = tokio::io::duplex(4096);

        let ctx = Context::with_state(
            TcpContext::default().io_idle_timeout(Some(Duration::from_millis(300))),
        );
        let handle = tokio::spawn(async move { service().serve(ctx, server).await });

        let body = vec![7u8; 300];
        client.write_all(&header(body.len() as i32)).await?;

        // Three chunks, ~150ms apart: no single gap exceeds the 300ms idle
        // timeout, but the ~450ms total would have exceeded a single flat
        // whole-transfer deadline of that same size.
        for chunk in body.chunks(100) {
            tokio::time::sleep(Duration::from_millis(150)).await;
            client.write_all(chunk).await?;
        }

        // `serve` loops forever reading requests, so it never returns `Ok`;
        // instead prove the slow send wasn't penalized by reading back the
        // echoed response in full before doing anything else.
        let mut response_size = [0u8; 4];
        _ = client.read_exact(&mut response_size).await?;
        let mut response_body = vec![0u8; frame_length(response_size)? - 4];
        _ = client.read_exact(&mut response_body).await?;
        assert_eq!(body, response_body);

        // Now end the connection; `serve`'s next `wait()` should see a clean
        // EOF, not the timeout a penalized slow transfer would have produced
        // instead.
        drop(client);

        let outcome = timeout(Duration::from_secs(5), handle)
            .await
            .expect("serve should observe EOF promptly after the client disconnects")?;

        assert!(
            matches!(&outcome, Err(Error::Io(err)) if err.kind() == io::ErrorKind::UnexpectedEof),
            "{outcome:?}"
        );
        Ok(())
    }
}
