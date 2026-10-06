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
    time::Duration,
};

use nisshi_sans_io::{
    ErrorCode, FetchRequest, FetchResponse, IsolationLevel, RequestInput, fetch_request::FetchTopic,
};
use rama::{Layer, Service};
use tracing::{debug, instrument};

use super::topic_error_response;
use crate::{Error, Result};

const DEFAULT_MAX_BYTES: u32 = 5 * 1024 * 1024;

/// A [`FetchRequest`] whose whole-request fields have decoded to valid values.
///
/// [`FetchService`](super::FetchService) takes this instead of a [`FetchRequest`], so it
/// never parses those fields itself.
#[derive(Clone, Debug)]
pub struct ValidatedFetchRequest {
    pub(super) isolation_level: IsolationLevel,
    pub(super) max_wait: Duration,
    pub(super) min_bytes: u32,
    pub(super) max_bytes: u32,
    pub(super) topics: Vec<FetchTopic>,
}

/// The topics of a [`FetchRequest`] that failed to convert into a [`ValidatedFetchRequest`].
#[derive(Clone, Debug)]
pub struct MalformedFetchRequest {
    topics: Vec<FetchTopic>,
}

impl MalformedFetchRequest {
    /// Builds the [`FetchResponse`] reporting `INVALID_REQUEST` for the whole request.
    ///
    /// `FetchResponse` has a top-level `error_code`, but that field is `versions: 7+`, so the
    /// same error is also mirrored onto every requested partition for clients on earlier
    /// versions.
    pub fn into_response(self) -> Result<FetchResponse> {
        self.topics
            .iter()
            .map(|topic| topic_error_response(topic, ErrorCode::InvalidRequest))
            .collect::<Result<Vec<_>>>()
            .map(|responses| {
                FetchResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(Some(ErrorCode::InvalidRequest.into()))
                    .session_id(Some(0))
                    .node_endpoints(Some([].into()))
                    .responses(Some(responses))
            })
    }
}

impl TryFrom<FetchRequest> for ValidatedFetchRequest {
    type Error = MalformedFetchRequest;

    fn try_from(request: FetchRequest) -> Result<Self, Self::Error> {
        let Some(topics) = request.topics else {
            // A request without topics fetches nothing, so its other fields are never used.
            return Ok(Self {
                isolation_level: IsolationLevel::ReadUncommitted,
                max_wait: Duration::ZERO,
                min_bytes: 0,
                max_bytes: 0,
                topics: vec![],
            });
        };

        let isolation_level = request
            .isolation_level
            .map_or(
                Ok(IsolationLevel::ReadUncommitted),
                IsolationLevel::try_from,
            )
            .map_err(|_| MalformedFetchRequest {
                topics: topics.clone(),
            })?;

        let max_wait = u64::try_from(request.max_wait_ms)
            .map(Duration::from_millis)
            .map_err(|_| MalformedFetchRequest {
                topics: topics.clone(),
            })?;

        let min_bytes = u32::try_from(request.min_bytes).map_err(|_| MalformedFetchRequest {
            topics: topics.clone(),
        })?;

        let max_bytes = request
            .max_bytes
            .map_or(Ok(DEFAULT_MAX_BYTES), |max_bytes| {
                u32::try_from(max_bytes).map(|max_bytes| max_bytes.min(DEFAULT_MAX_BYTES))
            })
            .map_err(|_| MalformedFetchRequest {
                topics: topics.clone(),
            })?;

        Ok(Self {
            isolation_level,
            max_wait,
            min_bytes,
            max_bytes,
            topics,
        })
    }
}

/// A [`Layer`] converting a [`FetchRequest`] into a [`ValidatedFetchRequest`] for the wrapped
/// [`Service`], and answering a malformed one itself.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FetchValidationLayer;

impl FetchValidationLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for FetchValidationLayer {
    type Service = FetchValidationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FetchValidationService { inner }
    }
}

/// A [`Service`] passing a [`ValidatedFetchRequest`] to `inner`, or answering `INVALID_REQUEST`
/// for a [`FetchRequest`] that does not convert into one.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FetchValidationService<S> {
    inner: S,
}

impl<S> Debug for FetchValidationService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(FetchValidationService)).finish()
    }
}

impl<S, I> Service<I> for FetchValidationService<S>
where
    S: Service<ValidatedFetchRequest, Output = FetchResponse, Error = Error>,
    I: Into<RequestInput<FetchRequest>> + Send + 'static,
{
    type Output = FetchResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output> {
        match ValidatedFetchRequest::try_from(input.into().request) {
            Ok(request) => self.inner.serve(request).await,

            Err(malformed) => {
                debug!(?malformed, "malformed fetch request");
                malformed.into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::{ErrorCode, FetchRequest, FetchResponse, RequestInput};
    use rama::{Layer as _, Service as _, extensions::Extensions};

    use super::FetchValidationLayer;
    use crate::{Error, Result};

    #[derive(Clone, Debug, Default)]
    struct CountingInner {
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl rama::Service<super::ValidatedFetchRequest> for CountingInner {
        type Output = FetchResponse;
        type Error = Error;

        async fn serve(&self, _input: super::ValidatedFetchRequest) -> Result<Self::Output> {
            _ = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            Ok(FetchResponse::default()
                .error_code(Some(ErrorCode::None.into()))
                .responses(Some([].into())))
        }
    }

    #[tokio::test]
    async fn malformed_isolation_level_short_circuits() -> Result<()> {
        let inner = CountingInner::default();
        let service = FetchValidationLayer::new().layer(inner.clone());

        let request = FetchRequest::default()
            .isolation_level(Some(111))
            .max_wait_ms(0)
            .min_bytes(0)
            .max_bytes(None)
            .topics(Some(
                [nisshi_sans_io::fetch_request::FetchTopic::default()
                    .topic(Some("t".into()))
                    .partitions(Some(
                        [nisshi_sans_io::fetch_request::FetchPartition::default().partition(0)]
                            .into(),
                    ))]
                .into(),
            ));

        let response = service
            .serve(RequestInput {
                request,
                extensions: Extensions::default(),
            })
            .await?;

        assert_eq!(Some(ErrorCode::InvalidRequest.into()), response.error_code);
        assert_eq!(0, inner.calls.load(std::sync::atomic::Ordering::SeqCst));

        let responses = response.responses.expect("responses");
        assert_eq!(1, responses.len());

        let partitions = responses[0].partitions.as_ref().expect("partitions");
        assert_eq!(
            ErrorCode::InvalidRequest,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn well_formed_request_calls_inner() -> Result<()> {
        let inner = CountingInner::default();
        let service = FetchValidationLayer::new().layer(inner.clone());

        let request = FetchRequest::default()
            .isolation_level(Some(0))
            .max_wait_ms(0)
            .min_bytes(0)
            .max_bytes(None)
            .topics(Some([].into()));

        let response = service
            .serve(RequestInput {
                request,
                extensions: Extensions::default(),
            })
            .await?;

        assert_eq!(Some(ErrorCode::None.into()), response.error_code);
        assert_eq!(1, inner.calls.load(std::sync::atomic::Ordering::SeqCst));

        Ok(())
    }

    #[tokio::test]
    async fn no_topics_calls_inner_without_validating() -> Result<()> {
        let inner = CountingInner::default();
        let service = FetchValidationLayer::new().layer(inner.clone());

        let request = FetchRequest::default()
            .isolation_level(Some(111))
            .max_wait_ms(0)
            .min_bytes(0)
            .max_bytes(None)
            .topics(None);

        let response = service
            .serve(RequestInput {
                request,
                extensions: Extensions::default(),
            })
            .await?;

        assert_eq!(Some(ErrorCode::None.into()), response.error_code);
        assert_eq!(1, inner.calls.load(std::sync::atomic::Ordering::SeqCst));

        Ok(())
    }
}
