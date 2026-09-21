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

use std::{
    error::{self},
    fmt::Debug,
    io,
    time::SystemTime,
};

use bytes::Bytes;
use nanoid::nanoid;
use nisshi_sans_io::BytesInput;
use opentelemetry::KeyValue;
use rama::{
    Layer, Service,
    extensions::{Extension, Extensions, ExtensionsRef},
    tcp::TcpStream,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    sync::Mutex,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument};

use crate::{
    BYTES_RECEIVED, BYTES_SENT, Error, REQUEST_DURATION, REQUEST_SIZE, RESPONSE_SIZE,
    TcpListenerInput, frame_length, frame_size,
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

impl<S> Service<TcpListenerInput> for TcpListenerService<S>
where
    S: Service<TcpStream> + Clone,
    S::Output: Debug,
    S::Error: error::Error,
{
    type Output = ();
    type Error = S::Error;

    #[instrument(skip(req))]
    async fn serve(&self, req: TcpListenerInput) -> Result<Self::Output, Self::Error> {
        let mut set = JoinSet::new();

        let extensions = req.extensions.clone();

        loop {
            tokio::select! {
                Ok((stream, addr)) = req.listener.accept() => {
                    debug!(?req, ?stream, %addr);

                    let service = self.inner.clone();
                    let extensions = extensions.clone();

                    let handle = set.spawn(async move {

                        match service.serve(TcpStream::from_tokio_tcp_stream(stream, extensions)).await {
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
#[derive(Clone, Debug, Extension)]
pub struct TcpContext {
    cluster_id: Option<String>,
    maximum_frame_size: Option<usize>,
}

impl Default for TcpContext {
    fn default() -> Self {
        Self {
            cluster_id: Default::default(),
            maximum_frame_size: Some(DEFAULT_MAXIMUM_FRAME_SIZE),
        }
    }
}

#[derive(Clone, Debug, Extension)]
struct ClusterIdExtension(String);

#[derive(Clone, Debug, Extension)]
struct MaximumFrameSizeExtension(usize);

impl Default for MaximumFrameSizeExtension {
    fn default() -> Self {
        Self(DEFAULT_MAXIMUM_FRAME_SIZE)
    }
}

impl From<&MaximumFrameSizeExtension> for usize {
    fn from(value: &MaximumFrameSizeExtension) -> Self {
        value.0
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

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpStreamLayer;

impl<S> Layer<S> for TcpStreamLayer {
    type Service = TcpStreamService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpStreamService<S> {
    inner: S,
}

impl<S> Service<TcpStream> for TcpStreamService<S>
where
    S: Service<BytesInput, Output = Bytes>,
    S::Error: Into<Error>,
{
    type Output = TcpStream;
    type Error = Error;

    async fn serve(&self, stream: TcpStream) -> Result<Self::Output, Self::Error> {
        let (frame, stream) = ReadHalfService.serve(stream).await?;

        let extensions = stream.extensions.fork();

        let frame = self
            .inner
            .serve(BytesInput {
                bytes: frame,
                extensions,
            })
            .await
            .map_err(Into::into)?;

        WriteHalfService.serve((frame, stream)).await
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct WriteHalfService;

impl Service<(Bytes, TcpStream)> for WriteHalfService {
    type Output = TcpStream;
    type Error = Error;

    async fn serve(
        &self,
        (frame, mut stream): (Bytes, TcpStream),
    ) -> Result<Self::Output, Self::Error> {
        stream.write_all(&frame[..]).await?;

        BYTES_SENT.add(frame.len() as u64, &[]);

        Ok(stream)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ReadHalfService;

impl Service<TcpStream> for ReadHalfService {
    type Output = (Bytes, TcpStream);
    type Error = Error;

    async fn serve(&self, mut input: TcpStream) -> Result<Self::Output, Self::Error> {
        let mut size = [0u8; 4];
        _ = input.read_exact(&mut size).await?;

        let frame_size = frame_size(size)?;

        if frame_size
            > input
                .extensions()
                .get_ref_or_insert(MaximumFrameSizeExtension::default)
                .into()
        {
            return Err(Into::into(Error::FrameTooBig(frame_size)));
        }

        let mut buffer: Vec<u8> = vec![0u8; frame_length(size)?];
        buffer[0..size.len()].copy_from_slice(&size[..]);
        _ = input.read_exact(&mut buffer[4..]).await?;
        BYTES_RECEIVED.add(buffer.len() as u64, &[]);

        Ok((Bytes::from(buffer), input))
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

impl<S> Service<TcpStream> for TcpContextService<S>
where
    S: Service<TcpStream>,
    S::Error: From<io::Error>,
{
    type Output = S::Output;
    type Error = S::Error;

    #[instrument(skip_all, fields(peer = %req.stream.peer_addr()?))]
    async fn serve(&self, req: TcpStream) -> Result<Self::Output, Self::Error> {
        if let Some(cluster_id) = self.state.cluster_id.clone() {
            _ = req.extensions.insert(ClusterIdExtension(cluster_id));
        }

        if let Some(maximum_frame_size) = self.state.maximum_frame_size {
            _ = req
                .extensions
                .insert(MaximumFrameSizeExtension(maximum_frame_size));
        }

        self.inner.serve(req).await
    }
}

/// A [`Service`] writing [`Bytes`] into a [`TcpStream`], responding with a length delimited frame of [`Bytes`]
pub struct BytesTcpService {
    stream: Mutex<TcpStream>,
}

impl BytesTcpService {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: Mutex::new(stream),
        }
    }
}

impl Debug for BytesTcpService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BytesTcpService)).finish()
    }
}

impl Service<BytesInput> for BytesTcpService {
    type Output = Bytes;
    type Error = Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: BytesInput) -> Result<Self::Output, Self::Error> {
        let mut stream = self.stream.lock().await;

        stream.write_all(&req.bytes[..]).await?;
        BYTES_SENT.add(req.bytes.len() as u64, &[]);

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
pub struct TcpBytesLayer;

impl<S> Layer<S> for TcpBytesLayer {
    type Service = TcpBytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] receiving [`Bytes`] from a [`TcpStream`], calling an inner [`Service`] and sending [`Bytes`] into the [`TcpStream`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpBytesService<S> {
    inner: S,
}

impl<S> Debug for TcpBytesService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TcpBytesService)).finish()
    }
}

impl<S> TcpBytesService<S> {
    fn elapsed_millis(&self, start: SystemTime) -> u64 {
        start
            .elapsed()
            .map_or(0, |duration| duration.as_millis() as u64)
    }
}

impl<S> TcpBytesService<S>
where
    S: Service<BytesInput, Output = Bytes>,
    S::Error: From<Error> + From<io::Error> + Debug,
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
        request: Bytes,
        extensions: Extensions,
    ) -> Result<Bytes, S::Error> {
        REQUEST_SIZE.record(request.len() as u64, attributes);

        let request_start = SystemTime::now();

        self.inner
            .serve(BytesInput {
                bytes: request,
                extensions,
            })
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
    ) -> Result<(), S::Error>
    where
        R: AsyncReadExt + AsyncWriteExt + Unpin + ExtensionsRef,
    {
        let size = self.wait(req, maximum_frame_size).await?;
        let request = self.read(req, size).await?;
        let response = self
            .process(attributes, request, req.extensions().clone())
            .await?;
        self.write(req, response).await
    }
}

impl<S, Stream> Service<Stream> for TcpBytesService<S>
where
    S: Service<BytesInput, Output = Bytes>,
    S::Error: From<Error> + From<io::Error> + Debug,
    Stream: AsyncReadExt + AsyncWriteExt + ExtensionsRef + Unpin + Send + Sync + 'static,
{
    type Output = ();

    type Error = S::Error;

    #[instrument(skip(req))]
    async fn serve(&self, mut req: Stream) -> Result<Self::Output, Self::Error> {
        let attributes = {
            let mut attributes = vec![];

            if let Some(cluster_id) = req
                .extensions()
                .get_ref::<ClusterIdExtension>()
                .cloned()
                .map(|cluster_id| cluster_id.0)
            {
                attributes.push(KeyValue::new("cluster_id", cluster_id))
            }

            attributes
        };

        let maximum_frame_size = req
            .extensions()
            .get_ref::<MaximumFrameSizeExtension>()
            .map(|maximum_frame_size| maximum_frame_size.0);

        loop {
            let attributes = attributes.clone();

            self.req(&mut req, maximum_frame_size, &attributes[..])
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

impl<S> Service<BytesInput> for BytesService<S>
where
    S: Service<BytesInput, Output = Bytes>,
{
    type Output = Bytes;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: BytesInput) -> Result<Self::Output, Self::Error> {
        debug!(req = ?&req.bytes[..]);
        self.inner
            .serve(req)
            .await
            .inspect(|response| debug!(response = ?&response[..]))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use tokio::{
        io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, duplex},
        spawn,
    };

    use super::*;

    /// Inner service standing in for the frame router: echoes the request bytes.
    #[derive(Clone, Copy, Debug, Default)]
    struct Echo;

    impl Service<BytesInput> for Echo {
        type Output = Bytes;
        type Error = Error;

        async fn serve(&self, req: BytesInput) -> Result<Bytes, Error> {
            Ok(req.bytes)
        }
    }

    fn service() -> TcpBytesService<Echo> {
        TcpBytesLayer.into_layer(Echo)
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

    struct DuplexStreamWithExtensions {
        stream: DuplexStream,
        extensions: Extensions,
    }

    impl AsRef<DuplexStream> for DuplexStreamWithExtensions {
        fn as_ref(&self) -> &DuplexStream {
            &self.stream
        }
    }

    impl AsyncRead for DuplexStreamWithExtensions {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for DuplexStreamWithExtensions {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.stream).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_shutdown(cx)
        }
    }

    impl ExtensionsRef for DuplexStreamWithExtensions {
        fn extensions(&self) -> &Extensions {
            &self.extensions
        }
    }

    #[tokio::test]
    async fn serve_rejects_oversized_frame_without_reading_body() -> Result<(), Error> {
        let (mut client, server) = duplex(64);

        let handle = spawn(async move {
            let extensions = Extensions::default();
            _ = extensions.insert(MaximumFrameSizeExtension(1_024));

            let input = DuplexStreamWithExtensions {
                stream: server,
                extensions,
            };

            service().serve(input).await
        });

        const SIZE: usize = 65_536;

        // Only the length prefix is sent. If the guard admitted the frame,
        // `read` would block in `read_exact` waiting for a body that never
        // arrives, so the timeout is what turns that into a failure.
        client.write_all(&header(SIZE as i32)).await?;

        let outcome = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("oversized frame was admitted: serve is blocked reading the body")?;

        assert!(
            matches!(outcome, Err(Error::FrameTooBig(SIZE))),
            "{outcome:?}"
        );
        Ok(())
    }
}
