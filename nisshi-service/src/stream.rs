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
    future::Future,
    io,
    marker::PhantomData,
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use nanoid::nanoid;
use opentelemetry::KeyValue;
use rama::{Context, Layer, Service};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
    task::JoinSet,
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

/// A source of accepted TCP connections.
///
/// Exists so [`TcpListenerService::serve`] is testable against a scripted sequence of
/// accept outcomes (in particular, an `Err` followed by an `Ok`) without depending on
/// triggering a real OS-level `accept()` failure, which is fragile and platform-dependent.
trait Acceptor {
    fn accept(&self) -> impl Future<Output = io::Result<(TcpStream, SocketAddr)>> + Send;
}

impl Acceptor for TcpListener {
    fn accept(&self) -> impl Future<Output = io::Result<(TcpStream, SocketAddr)>> + Send {
        TcpListener::accept(self)
    }
}

/// Backs off after a run of consecutive `accept()` errors that look like resource
/// exhaustion (e.g. `EMFILE`/`ENFILE`), so a persistent failure doesn't spin the loop
/// at 100% CPU. `ConnectionAborted` is a routine, expected per-connection error (a peer
/// reset before we could accept it) and never backs off.
///
/// The backoff sleep runs inside the `select!` arm, blocking the whole `select!` call
/// for its duration -- delaying cancellation and any other periodic branch a caller
/// composes alongside this one -- so it is kept short and capped.
struct AcceptBackoff {
    consecutive_errors: u32,
}

impl AcceptBackoff {
    const CAP: Duration = Duration::from_millis(200);
    const INITIAL: Duration = Duration::from_millis(5);

    const fn new() -> Self {
        Self {
            consecutive_errors: 0,
        }
    }

    fn reset(&mut self) {
        self.consecutive_errors = 0;
    }

    /// Returns the backoff to sleep for, or `None` if this error shouldn't back off.
    fn on_error(&mut self, err: &io::Error) -> Option<Duration> {
        if err.kind() == io::ErrorKind::ConnectionAborted {
            self.reset();
            return None;
        }

        let backoff = Self::INITIAL
            .saturating_mul(1 << self.consecutive_errors.min(6))
            .min(Self::CAP);

        self.consecutive_errors = self.consecutive_errors.saturating_add(1);

        Some(backoff)
    }
}

impl<State, S, A> Service<State, A> for TcpListenerService<S>
where
    S: Service<State, TcpStream> + Clone,
    S::Response: Debug,
    S::Error: error::Error,
    State: Clone + Send + Sync + 'static,
    A: Acceptor + Debug + Send + Sync + 'static,
{
    type Response = ();
    type Error = S::Error;

    #[instrument(skip(ctx, req))]
    async fn serve(&self, ctx: Context<State>, req: A) -> Result<Self::Response, Self::Error> {
        let mut set = JoinSet::new();
        let mut backoff = AcceptBackoff::new();

        loop {
            tokio::select! {
                result = req.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            backoff.reset();
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
                        }

                        Err(err) => {
                            error!(?err, "accept() failed; continuing to listen");

                            if let Some(backoff) = backoff.on_error(&err) {
                                tokio::time::sleep(backoff).await;
                            }
                        }
                    }

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
}

impl Default for TcpContext {
    fn default() -> Self {
        Self {
            cluster_id: None,
            maximum_frame_size: Some(DEFAULT_MAXIMUM_FRAME_SIZE),
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

impl<S, State> TcpBytesService<S, State>
where
    S: Service<State, Bytes, Response = Bytes>,
    S::Error: From<Error> + From<io::Error> + Debug,
    State: Clone + Default + Send + Sync + 'static,
{
    #[instrument(skip_all)]
    async fn wait<R>(
        &self,
        req: &mut R,
        maximum_frame_size: Option<usize>,
    ) -> Result<[u8; 4], S::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let mut size = [0u8; 4];

        _ = req
            .read_exact(&mut size)
            .await
            .inspect_err(|err| debug!(?err))?;

        let frame_size = frame_size(size)?;

        if maximum_frame_size.is_some_and(|maximum_frame_size| frame_size > maximum_frame_size) {
            return Err(Into::into(Error::FrameTooBig(frame_size)));
        }

        Ok(size)
    }

    #[instrument(skip_all)]
    async fn read<R>(&self, req: &mut R, size: [u8; 4]) -> Result<Bytes, S::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let mut request: Vec<u8> = vec![0u8; frame_length(size)?];

        request[0..size.len()].copy_from_slice(&size[..]);

        _ = req
            .read_exact(&mut request[4..])
            .await
            .inspect_err(|err| error!(?err))?;
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
    async fn write<W>(&self, req: &mut W, frame: Bytes) -> Result<(), S::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        let mut w = BufWriter::new(req);
        w.write_all(&frame).await.inspect_err(|err| error!(?err))?;
        BYTES_SENT.add(frame.len() as u64, &[]);
        w.flush().await.map_err(Into::into)
    }

    #[instrument(skip_all, fields(id = nanoid!()))]
    async fn req<R>(
        &self,
        req: &mut R,
        maximum_frame_size: Option<usize>,
        attributes: &[KeyValue],
        ctx: Context<TcpContext>,
    ) -> Result<(), S::Error>
    where
        R: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        let size = self.wait(req, maximum_frame_size).await?;
        let request = self.read(req, size).await?;
        let response = self.process(attributes, ctx, request).await?;
        self.write(req, response).await
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

        let maximum_frame_size = ctx.state().maximum_frame_size;

        loop {
            let ctx = ctx.clone();
            let attributes = attributes.clone();

            self.req(&mut req, maximum_frame_size, &attributes[..], ctx)
                .await?
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
    use std::{collections::VecDeque, time::Duration};

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

    #[tokio::test]
    async fn frame_within_limit_is_accepted() -> Result<(), Error> {
        let mut reader = &header(12)[..];

        let size = service().wait(&mut reader, Some(1024)).await?;

        assert_eq!(header(12), size);
        Ok(())
    }

    #[tokio::test]
    async fn frame_exactly_at_limit_is_accepted() -> Result<(), Error> {
        let mut reader = &header(1024)[..];

        let size = service().wait(&mut reader, Some(1024)).await?;

        assert_eq!(header(1024), size);
        Ok(())
    }

    #[tokio::test]
    async fn frame_over_limit_is_rejected() {
        let mut reader = &header(1025)[..];

        let err = service()
            .wait(&mut reader, Some(1024))
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

    #[tokio::test]
    async fn negative_frame_length_is_rejected() {
        let mut reader = &header(-1)[..];

        let err = service()
            .wait(&mut reader, None)
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
        // `read` would block in `read_exact` waiting for a body that never
        // arrives, so the timeout is what turns that into a failure.
        client.write_all(&header(65_536)).await?;

        let outcome = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("oversized frame was admitted: serve is blocked reading the body")?;

        assert!(
            matches!(outcome, Err(Error::FrameTooBig(65_536))),
            "{outcome:?}"
        );
        Ok(())
    }

    /// A single `accept()` error must not stop the loop from serving the next,
    /// successful accept -- the bug this guards against is `tokio::select!`
    /// disabling the `Ok((stream, addr)) = req.accept()` arm for the rest of the
    /// macro invocation whenever that future resolves to `Err`, which, combined
    /// with `join_next` being gated on a non-empty `set`, could wedge the loop
    /// until cancellation. This fails against the pre-fix pattern-matched arm and
    /// passes against the `match`-in-the-body fix.
    #[tokio::test]
    async fn accept_error_does_not_wedge_the_loop() -> Result<(), Box<dyn error::Error>> {
        /// Proves a connection was actually handed to the inner service (not just
        /// accepted) by echoing one byte back over the raw stream.
        #[derive(Clone, Copy, Debug, Default)]
        struct RawEcho;

        impl Service<(), TcpStream> for RawEcho {
            type Response = ();
            type Error = Error;

            async fn serve(&self, _ctx: Context<()>, mut stream: TcpStream) -> Result<(), Error> {
                let mut buf = [0u8; 1];
                _ = stream.read_exact(&mut buf).await?;
                stream.write_all(&buf).await?;
                Ok(())
            }
        }

        /// Yields a scripted sequence of accept outcomes, then hangs (as a real
        /// listener with nothing pending would) once the script is exhausted.
        #[derive(Debug)]
        struct ScriptedAcceptor {
            results: tokio::sync::Mutex<VecDeque<io::Result<(TcpStream, SocketAddr)>>>,
        }

        impl Acceptor for ScriptedAcceptor {
            async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
                match self.results.lock().await.pop_front() {
                    Some(result) => result,
                    None => std::future::pending().await,
                }
            }
        }

        let real_listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = real_listener.local_addr()?;

        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(local_addr).await?;
            stream.write_all(b"x").await?;

            let mut buf = [0u8; 1];
            _ = stream.read_exact(&mut buf).await?;

            Ok::<_, io::Error>(buf[0])
        });

        let (stream, addr) = real_listener.accept().await?;
        drop(real_listener);

        let mut results = VecDeque::new();
        results.push_back(Err(io::Error::from(io::ErrorKind::ConnectionAborted)));
        results.push_back(Ok((stream, addr)));

        let acceptor = ScriptedAcceptor {
            results: tokio::sync::Mutex::new(results),
        };

        let cancellation = CancellationToken::new();
        let service = TcpListenerLayer::new(cancellation.clone()).layer(RawEcho);

        let handle = tokio::spawn(async move { service.serve(Context::default(), acceptor).await });

        let echoed = tokio::time::timeout(Duration::from_secs(5), client)
            .await
            .expect(
                "accept() error wedged the loop: the Ok connection scripted after it \
                 was never served",
            )??;

        assert_eq!(b'x', echoed);

        cancellation.cancel();
        handle.await??;

        Ok(())
    }
}
