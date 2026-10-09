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

use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use nisshi_sans_io::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, Body, BodyInput, ErrorCode, Frame, FrameInput,
    Header, RequestInput, RootMessageMeta, api_versions_response::ApiVersion,
};
use rama::{Service, extensions::Extensions, service::BoxService};

use crate::Error;

/// An [`ApiVersionsResponse`] [`Service`] with a supported set of API and versions from [`RootMessageMeta`].
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersionsService<E> {
    supported: Vec<i16>,
    error: PhantomData<E>,
}

impl<E> Service<RequestInput<ApiVersionsRequest>> for ApiVersionsService<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    type Output = ApiVersionsResponse;
    type Error = E;

    async fn serve(
        &self,
        _req: RequestInput<ApiVersionsRequest>,
    ) -> Result<Self::Output, Self::Error> {
        Ok::<_, E>(
            ApiVersionsResponse::default()
                .finalized_features(Some([].into()))
                .finalized_features_epoch(Some(-1))
                .supported_features(Some([].into()))
                .zk_migration_ready(Some(false))
                .error_code(ErrorCode::None.into())
                .api_keys(Some(
                    RootMessageMeta::messages()
                        .requests()
                        .iter()
                        .filter(|(api_key, _)| self.supported.contains(api_key))
                        .map(|(_, meta)| {
                            ApiVersion::default()
                                .api_key(meta.api_key)
                                .min_version(meta.version.valid.start)
                                .max_version(meta.version.valid.end)
                        })
                        .collect(),
                ))
                .throttle_time_ms(Some(0)),
        )
    }
}

impl<E> Service<BodyInput> for ApiVersionsService<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + Send + Sync + 'static,
{
    type Output = Body;
    type Error = E;

    async fn serve(&self, req: BodyInput) -> Result<Self::Output, Self::Error> {
        let req = ApiVersionsRequest::try_from(req.body).map(|request| RequestInput {
            request,
            extensions: req.extensions,
        })?;
        self.serve(req).await.map(Into::into)
    }
}

impl<E> Service<FrameInput> for ApiVersionsService<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + Send + Sync + 'static,
{
    type Output = Frame;
    type Error = E;

    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let correlation_id = req.frame.correlation_id()?;

        self.serve(BodyInput {
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

/// Route [`Frame`] to a [`Service`] via [API key][`Frame#method.api_key`]
///
/// A simple example that routes [`MetadataRequest`][`nisshi_sans_io::MetadataRequest`]
/// and [`CreateTopicsRequest`][`nisshi_sans_io::CreateTopicsRequest`].
/// [`ApiVersionsRequest`][`nisshi_sans_io::ApiVersionsRequest`] is created by the
///  builder including both of the implemented services using the version ranges
///  from [`RootMessageMeta`][`nisshi_sans_io::RootMessageMeta`].
///
/// ```
/// # use rama::Layer as _;
/// # use nisshi_sans_io::{CreateTopicsRequest, CreateTopicsResponse, MetadataRequest, MetadataResponse};
/// # use nisshi_service::{Error, FrameRouteService, RequestLayer, ResponseService};
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let router = FrameRouteService::<Error>::builder()
///     .with_service(
///         RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_| {
///             Ok(MetadataResponse::default()
///                 .brokers(Some([].into()))
///                 .topics(Some([].into()))
///                 .cluster_id(Some("nisshi".into()))
///                 .controller_id(Some(111))
///                 .throttle_time_ms(Some(0))
///                 .cluster_authorized_operations(Some(-1)))
///         })),
///     )
///     .and_then(|builder| {
///         builder.with_service(RequestLayer::<CreateTopicsRequest>::new().into_layer(
///             ResponseService::new(|_| {
///                 Ok(CreateTopicsResponse::default()
///                     .throttle_time_ms(Some(0))
///                     .topics(Some([].into())))
///             }),
///         ))
///     })
///     .and_then(|builder| builder.build())?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct FrameRouteService<E = Error> {
    routes: Arc<BTreeMap<i16, BoxService<FrameInput, Frame, E>>>,
}

impl<E> FrameRouteService<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    pub fn new(routes: Arc<BTreeMap<i16, BoxService<FrameInput, Frame, E>>>) -> Self {
        Self { routes }
    }

    pub fn builder() -> FrameRouteBuilder<E> {
        FrameRouteBuilder::<E>::new()
    }
}

impl<E> Service<FrameInput> for FrameRouteService<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    type Output = Frame;
    type Error = E;

    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let api_key = req.frame.api_key()?;

        if let Some(service) = self.routes.get(&api_key) {
            service.serve(req).await
        } else {
            Err(E::from(Error::UnknownServiceFrame(Box::new(req.frame))))
        }
    }
}

/// Routes a bare [`Frame`] with no [`Extensions`], for callers on the extensions-free
/// channel-based path (e.g. [`FrameChannelService`][crate::channel::FrameChannelService]),
/// where there is no ambient extensions to propagate in the first place.
impl<E> Service<Frame> for FrameRouteService<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    type Output = Frame;
    type Error = E;

    async fn serve(&self, req: Frame) -> Result<Self::Output, Self::Error> {
        let api_key = req.api_key()?;

        if let Some(service) = self.routes.get(&api_key) {
            service
                .serve(FrameInput {
                    frame: req,
                    extensions: Extensions::default(),
                })
                .await
        } else {
            Err(E::from(Error::UnknownServiceFrame(Box::new(req))))
        }
    }
}

/// A [`Frame`] route builder providing an [`ApiVersionsResponse`] for all available routes
#[derive(Debug)]
pub struct FrameRouteBuilder<E> {
    routes: BTreeMap<i16, BoxService<FrameInput, Frame, E>>,
}

impl<E> FrameRouteBuilder<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + Send + Sync + 'static,
{
    fn new() -> Self {
        Self {
            routes: BTreeMap::new(),
        }
    }

    pub fn with_service<S>(self, service: S) -> Result<Self, Error>
    where
        S: Into<BoxService<FrameInput, Frame, E>> + ApiKey,
    {
        self.with_route(S::KEY, service.into())
    }

    pub fn with_route(
        mut self,
        api_key: i16,
        service: BoxService<FrameInput, Frame, E>,
    ) -> Result<Self, Error> {
        self.routes
            .insert(api_key, service)
            .map_or(Ok(self), |_existing| Err(Error::DuplicateRoute(api_key)))
    }

    pub fn build(self) -> Result<FrameRouteService<E>, Error> {
        let api_key = ApiVersionsRequest::KEY;
        let mut supported = self.routes.keys().copied().collect::<Vec<_>>();
        supported.push(api_key);

        self.with_route(
            api_key,
            ApiVersionsService {
                supported,
                error: PhantomData,
            }
            .boxed(),
        )
        .map(|builder| FrameRouteService {
            routes: Arc::new(builder.routes),
        })
    }
}
