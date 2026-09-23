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
    fmt::{self, Debug},
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use bytes::{BufMut as _, Bytes, BytesMut};
use nisshi_auth::AuthenticationExtension;
use nisshi_sans_io::{
    ApiKey, ApiVersionsRequest, Body, BodyInput, BytesInput, Frame, FrameInput, Header, Request,
    RequestInput, Response, RootMessageMeta, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest,
};
use opentelemetry::KeyValue;
use rama::{Layer, Service, extensions::Extensions, matcher::Matcher, service::BoxService};
use rsasl::config::SASLConfig;
use tokio::task::spawn_blocking;
use tracing::{debug, error, instrument};

use crate::{API_ERRORS, API_REQUESTS, ProgressBarExtension};

/// A [Matcher] of [`Request`]s using their [API key][`ApiKey`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestApiKeyMatcher(pub i16);

impl<Q> Matcher<RequestInput<Q>> for RequestApiKeyMatcher
where
    Q: Request,
{
    fn matches(&self, ext: Option<&Extensions>, req: &RequestInput<Q>) -> bool {
        debug!(?ext, ?req);
        Q::KEY == self.0
    }
}

/// A [`Matcher`] of [`Frame`]s using their [API key][`Frame#method.api_key`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameApiKeyMatcher(pub i16);

impl Matcher<FrameInput> for FrameApiKeyMatcher {
    fn matches(&self, _ext: Option<&Extensions>, req: &FrameInput) -> bool {
        req.frame.api_key().is_ok_and(|api_key| api_key == self.0)
    }
}

/// A [`Layer`] for handling API [`Request`]s responding with an API [`Response`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> RequestLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<S, Q> Layer<S> for RequestLayer<Q> {
    type Service = RequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

/// A [`Service`] that handles API [`Request`]s responding with an API [`Response`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q> Debug for RequestService<S, Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(RequestService)).finish()
    }
}

impl<S, Q> Service<RequestInput<Q>> for RequestService<S, Q>
where
    S: Service<RequestInput<Q>>,
    Q: Request,
    S::Error: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<RequestInput<Q>>>::Error>,
    S::Output: Response,
    Body: From<<S as Service<RequestInput<Q>>>::Output>,
{
    type Output = S::Output;
    type Error = S::Error;

    #[instrument(skip(req))]
    async fn serve(&self, req: RequestInput<Q>) -> Result<Self::Output, Self::Error> {
        debug!(?req);
        self.inner
            .serve(req)
            .await
            .inspect(|response| debug!(?response))
    }
}

impl<S, Q> ApiKey for RequestService<S, Q>
where
    Q: Request,
{
    const KEY: i16 = Q::KEY;
}

/// A [`Layer`] that transforms [`Frame`] into [`Request`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameRequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> FrameRequestLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<S, Q> Layer<S> for FrameRequestLayer<Q> {
    type Service = FrameRequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

/// A [`Service`] that transforms a [`Frame`] into a [`Request`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameRequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q> Debug for FrameRequestService<S, Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(FrameRequestService)).finish()
    }
}

impl<S, Q, I> Service<I> for FrameRequestService<S, Q>
where
    I: Into<FrameInput> + Send + 'static,
    S: Service<RequestInput<Q>>,
    S::Output: Response,
    S::Error: From<nisshi_sans_io::Error>,
    Q: Request + TryFrom<Body>,
    <Q as TryFrom<Body>>::Error: Into<S::Error>,
{
    type Output = Frame;
    type Error = S::Error;

    #[instrument(skip(req))]
    async fn serve(&self, req: I) -> Result<Self::Output, Self::Error> {
        let req = req.into();

        let correlation_id = req.frame.correlation_id()?;

        let req = Q::try_from(req.frame.body)
            .map(|request| RequestInput {
                request,
                extensions: req.extensions,
            })
            .map_err(Into::into)?;

        self.inner.serve(req).await.map(|response| Frame {
            size: 0,
            header: Header::Response { correlation_id },
            body: response.into(),
        })
    }
}

impl<S, Q> Matcher<FrameInput> for FrameRequestService<S, Q>
where
    S: Clone + Send + Sync + 'static,
    Q: Request,
{
    fn matches(&self, ext: Option<&Extensions>, req: &FrameInput) -> bool {
        debug!(?ext, ?req);
        req.frame.api_key().is_ok_and(|api_key| api_key == Q::KEY)
    }
}

/// A [`Layer`] that transforms [`Bytes`] into [`Frame`]s
#[derive(Clone, Debug, Default)]
pub struct BytesFrameLayer {
    sasl_config: Option<Arc<SASLConfig>>,
}

impl BytesFrameLayer {
    pub fn with_sasl_config(self, sasl_config: Option<Arc<SASLConfig>>) -> Self {
        Self { sasl_config }
    }
}

impl<S> Layer<S> for BytesFrameLayer {
    type Service = BytesFrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            af: self
                .sasl_config
                .clone()
                .map(|sasl_config| AuthenticationFrame {
                    authentication: AuthenticationExtension::server(sasl_config),
                    v0: Arc::new(Mutex::new(None)),
                }),
        }
    }
}

#[derive(Clone)]
struct AuthenticationFrame {
    authentication: AuthenticationExtension,
    v0: Arc<Mutex<Option<bool>>>,
}

impl AuthenticationFrame {
    fn is_authenticated(&self) -> bool {
        self.authentication.is_authenticated()
    }
}

/// A [`Service`] transforming [`Bytes`]s into [`Frame`]s
#[derive(Clone, Default)]
pub struct BytesFrameService<S> {
    inner: S,
    af: Option<AuthenticationFrame>,
}

impl<S> Debug for BytesFrameService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(BytesFrameService)).finish()
    }
}

impl<S> BytesFrameService<S> {
    fn is_authenticated(&self, api_key: i16) -> bool {
        self.af.as_ref().is_none_or(|af| {
            af.authentication.is_authenticated()
                || api_key == SaslHandshakeRequest::KEY
                || api_key == SaslAuthenticateRequest::KEY
                || api_key == ApiVersionsRequest::KEY
        })
    }
}

impl<S> Service<BytesInput> for BytesFrameService<S>
where
    S: Service<FrameInput, Output = Frame>,
    S::Error: From<nisshi_sans_io::Error> + From<tokio::task::JoinError> + Debug,
{
    type Output = Bytes;
    type Error = S::Error;

    #[instrument(skip(req))]
    async fn serve(&self, req: BytesInput) -> Result<Self::Output, Self::Error> {
        let sasl_handshake_v0 = self
            .af
            .as_ref()
            .and_then(|af| af.v0.lock().ok())
            .inspect(|v0| debug!(?v0))
            .map(|v0| v0.unwrap_or_default())
            .unwrap_or_default();

        debug!(request = ?&req.bytes[..], sasl_handshake_v0);

        let extensions = req.extensions;

        let req = if sasl_handshake_v0 {
            //  If SaslHandshakeRequest version is v0, a series of SASL client and server tokens
            //  corresponding to the mechanism are sent as opaque packets without wrapping the
            //  messages with Kafka protocol headers. If SaslHandshakeRequest version is v1,
            //  the SaslAuthenticate request/response are used, where the actual SASL tokens
            //  are wrapped in the Kafka protocol. The error code in the final message from
            //  the broker will indicate if authentication succeeded or failed.
            Frame {
                size: 0,
                header: Header::Request {
                    api_key: SaslAuthenticateRequest::KEY,
                    api_version: 0,
                    correlation_id: 0,
                    client_id: None,
                },
                body: Body::SaslAuthenticateRequest(
                    SaslAuthenticateRequest::default().auth_bytes(req.bytes.slice(4..)),
                ),
            }
        } else {
            spawn_blocking(|| Frame::request_from_bytes(req.bytes))
                .await?
                .inspect(|request| debug!(?request))?
        };

        let api_key = req.api_key()?;

        if !self.is_authenticated(api_key) {
            return Err(Into::into(nisshi_sans_io::Error::NotAuthenticated));
        }

        let api_version = req.api_version()?;
        let correlation_id = req.correlation_id()?;

        if let Some(pb) = extensions.get_ref::<ProgressBarExtension>() {
            let api_name = req.api_name();

            pb.as_ref()
                .set_message(format!("{api_name} v{api_version}/{correlation_id}"));
            pb.as_ref().tick();
        }

        let attributes = vec![
            KeyValue::new("api_key", api_key as i64),
            KeyValue::new("api_version", api_version as i64),
        ];

        if !extensions.contains::<AuthenticationExtension>()
            && let Some(authentication) = self.af.as_ref().map(|af| af.authentication.clone())
        {
            _ = extensions.insert(authentication);
        }

        let Frame { body, .. } = {
            self.inner
                .serve(FrameInput {
                    frame: req,
                    extensions,
                })
                .await
                .inspect(|response| debug!(?response))?
        };

        if sasl_handshake_v0 {
            //  If SaslHandshakeRequest version is v0, a series of SASL client and server tokens
            //  corresponding to the mechanism are sent as opaque packets without wrapping the
            //  messages with Kafka protocol headers.

            // when authenticated, this is final handshake:
            if let Some(af) = self.af.as_ref()
                && af.is_authenticated()
                && let Ok(mut v0) = af.v0.lock()
                && v0.is_some()
            {
                *v0 = None
            }

            SaslAuthenticateResponse::try_from(body)
                .and_then(|response| {
                    i32::try_from(response.auth_bytes.len())
                        .map_err(Into::into)
                        .map(|size| {
                            let mut frame = BytesMut::new();
                            frame.put(&size.to_be_bytes()[..]);
                            frame.put(response.auth_bytes);
                            Bytes::from(frame)
                        })
                })
                .map_err(Into::into)
        } else {
            //  If SaslHandshakeRequest version is v0, a series of SASL client and server tokens
            //  corresponding to the mechanism are sent as opaque packets without wrapping the
            //  messages with Kafka protocol headers.
            //
            // Following messages will be opaque:
            if let Some(af) = self.af.as_ref()
                && (api_key == SaslHandshakeRequest::KEY && api_version == 0)
                && let Ok(mut v0) = af.v0.lock()
            {
                *v0 = Some(true)
            }

            spawn_blocking(move || {
                Frame::response(
                    Header::Response { correlation_id },
                    body,
                    api_key,
                    api_version,
                )
            })
            .await?
            .inspect(|response| {
                debug!(response = ?response[..]);
                API_REQUESTS.add(1, &attributes);
            })
            .inspect_err(|err| {
                error!(api_key, api_version, ?err);
                API_ERRORS.add(1, &attributes);
            })
            .map_err(Into::into)
        }
    }
}

/// A [`Layer`] that transforms [`Frame`]s into [`Bytes`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBytesLayer;

impl<S> Layer<S> for FrameBytesLayer {
    type Service = FrameBytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that transforms [`Frame`]s into [`Bytes`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBytesService<S> {
    inner: S,
}

impl<S> Debug for FrameBytesService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(FrameBytesService)).finish()
    }
}

impl<S> Service<FrameInput> for FrameBytesService<S>
where
    S: Service<BytesInput, Output = Bytes>,
    S::Error: From<nisshi_sans_io::Error>,
{
    type Output = Frame;
    type Error = S::Error;

    #[instrument(skip(req), fields(api_key = req.frame.api_key()?, api_version = req.frame.api_version()?, correlation_id = req.frame.correlation_id()?))]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        debug!(?req);

        let api_key = req.frame.api_key()?;
        let api_version = req.frame.api_version()?;

        let req = BytesInput {
            bytes: Frame::request(req.frame.header, req.frame.body)?,
            extensions: req.extensions.fork(),
        };

        self.inner
            .serve(req)
            .await
            .and_then(|response| {
                Frame::response_from_bytes(response, api_key, api_version).map_err(Into::into)
            })
            .inspect(|response| debug!(?response))
    }
}

/// A [`Layer`] that transforms [`Frame`] into [`Body`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBodyLayer;

impl<S> Layer<S> for FrameBodyLayer {
    type Service = FrameBodyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that transforms [`Frame`] into [`Body`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBodyService<S> {
    inner: S,
}

impl<S> Service<FrameInput> for FrameBodyService<S>
where
    S: Service<BodyInput, Output = Body>,
    S::Error: From<nisshi_sans_io::Error>,
{
    type Output = Frame;

    type Error = S::Error;

    #[instrument(skip_all, fields(api_key = req.frame.api_key()?, api_version = req.frame.api_version()?, correlation_id = req.frame.correlation_id()?))]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let correlation_id = req.frame.correlation_id()?;

        self.inner
            .serve(BodyInput {
                body: req.frame.body,
                extensions: req.extensions,
            })
            .await
            .map(|body| Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
    }
}

/// A [`Layer`] that transforms [`Body`] into [`Request`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyRequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> BodyRequestLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<S, Q> Layer<S> for BodyRequestLayer<Q> {
    type Service = BodyRequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

/// A [`Layer`] that transforms [`Body`] into [`Request`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyRequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q> ApiKey for BodyRequestService<S, Q>
where
    Q: Request,
{
    const KEY: i16 = Q::KEY;
}

impl<S, Q> Service<BodyInput> for BodyRequestService<S, Q>
where
    S: Service<RequestInput<Q>>,
    Q: Request,
    S::Error: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<RequestInput<Q>>>::Error>,
    Body: From<<S as Service<RequestInput<Q>>>::Output>,
{
    type Output = Body;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: BodyInput) -> Result<Self::Output, Self::Error> {
        let req = Q::try_from(req.body).map(|request| RequestInput {
            request,
            extensions: req.extensions,
        })?;
        self.inner.serve(req).await.map(Body::from)
    }
}

/// A [`Layer`] that transforms [`Request`] into [`Frame`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestFrameLayer;

impl<S> Layer<S> for RequestFrameLayer {
    type Service = RequestFrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that transforms [`Request`] into [`Frame`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestFrameService<S> {
    inner: S,
}

impl<S, Q> Service<RequestInput<Q>> for RequestFrameService<S>
where
    Q: Request,
    S: Service<FrameInput, Output = Frame>,
    S::Error: From<<<Q as Request>::Response as TryFrom<Body>>::Error>,
{
    type Output = Q::Response;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: RequestInput<Q>) -> Result<Self::Output, Self::Error> {
        debug!(?req);

        let api_key = Q::KEY;
        let api_version = RootMessageMeta::messages()
            .requests()
            .get(&api_key)
            .map(|message_meta| message_meta.version.valid().end)
            .unwrap_or_default();
        let correlation_id = 0;
        let client_id = Some(env!("CARGO_CRATE_NAME").into());

        self.inner
            .serve(FrameInput {
                frame: Frame {
                    size: 0,
                    header: Header::Request {
                        api_key,
                        api_version,
                        correlation_id,
                        client_id,
                    },
                    body: req.request.into(),
                },
                extensions: req.extensions,
            })
            .await
            .and_then(|response| Q::Response::try_from(response.body).map_err(Into::into))
            .inspect(|response| debug!(?response))
    }
}

impl<S, Q, E> From<RequestService<S, Q>> for BoxService<BodyInput, Body, E>
where
    S: Service<RequestInput<Q>, Error = E>,
    Q: Request,
    <S as Service<RequestInput<Q>>>::Output: Response,
    E: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<RequestInput<Q>>>::Error>,
    Body: From<<S as Service<RequestInput<Q>>>::Output>,
{
    fn from(value: RequestService<S, Q>) -> Self {
        BodyRequestLayer::<Q>::new().into_layer(value).boxed()
    }
}

impl<S, Q, E> From<RequestService<S, Q>> for BoxService<FrameInput, Frame, E>
where
    S: Service<RequestInput<Q>, Error = E>,
    Q: Request,
    <S as Service<RequestInput<Q>>>::Output: Response,
    E: From<nisshi_sans_io::Error>
        + From<<Q as TryFrom<Body>>::Error>
        + From<<S as Service<RequestInput<Q>>>::Error>,
    Body: From<<S as Service<RequestInput<Q>>>::Output>,
{
    fn from(value: RequestService<S, Q>) -> Self {
        (FrameBodyLayer, BodyRequestLayer::<Q>::new())
            .into_layer(value)
            .boxed()
    }
}

impl<S, Q, E> From<BodyRequestService<S, Q>> for BoxService<FrameInput, Frame, E>
where
    S: Service<RequestInput<Q>, Error = E>,
    Q: Request,
    E: From<nisshi_sans_io::Error>
        + From<<Q as TryFrom<Body>>::Error>
        + From<<S as Service<RequestInput<Q>>>::Error>,
    Body: From<<S as Service<RequestInput<Q>>>::Output>,
{
    fn from(value: BodyRequestService<S, Q>) -> Self {
        FrameBodyLayer.into_layer(value).boxed()
    }
}

/// A [`Service`] that transforms [`Frame`] into a [`Frame`] using a closure.
#[derive(Clone, Copy, Debug, Hash)]
pub struct FrameService<F> {
    response: F,
}

impl<E, F> Service<FrameInput> for FrameService<F>
where
    F: Fn(FrameInput) -> Result<Frame, E> + Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    type Output = Frame;
    type Error = E;

    #[instrument(skip_all)]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        (self.response)(req)
    }
}

impl<F> FrameService<F> {
    pub fn new<E>(response: F) -> Self
    where
        F: Fn(FrameInput) -> Result<Frame, E> + Clone,
        E: Send + Sync + 'static,
    {
        Self { response }
    }
}

/// A [`Service`] that transforms [`Request`] into a [`Response`] using a closure.
#[derive(Clone, Copy, Hash)]
pub struct ResponseService<F> {
    response: F,
}

impl<F> Debug for ResponseService<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(ResponseService)).finish()
    }
}

impl<Q, E, F> Service<RequestInput<Q>> for ResponseService<F>
where
    F: Fn(RequestInput<Q>) -> Result<Q::Response, E> + Clone + Send + Sync + 'static,
    Q: Request,
    E: Send + Sync + 'static,
{
    type Output = Q::Response;
    type Error = E;

    #[instrument(skip_all)]
    async fn serve(&self, req: RequestInput<Q>) -> Result<Self::Output, Self::Error> {
        (self.response)(req)
    }
}

impl<F> ResponseService<F> {
    pub fn new<Q, E>(response: F) -> Self
    where
        F: Fn(RequestInput<Q>) -> Result<Q::Response, E> + Clone,
        Q: Request,
        E: Send + Sync + 'static,
    {
        Self { response }
    }
}
