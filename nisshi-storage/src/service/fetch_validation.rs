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

use std::fmt::{self, Debug};

use nisshi_sans_io::{ErrorCode, FetchRequest, FetchResponse, RequestInput};
use rama::{Layer, Service};
use tracing::{debug, instrument};

use super::fetch::{parse_fetch_fields, topic_error_response};
use crate::{Error, Result};

/// A [`Layer`] rejecting a malformed [`FetchRequest`] before it reaches the wrapped [`Service`].
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

/// A [`Service`] rejecting a malformed [`FetchRequest`] before it reaches `inner`.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FetchValidationService<S> {
    inner: S,
}

impl<S> Debug for FetchValidationService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(FetchValidationService)).finish()
    }
}

/// Whether `request`'s whole-request fields decode to valid values.
///
/// [`FetchService`](super::fetch::FetchService) re-parses the same fields to use them, so this
/// check only has to decide whether to short-circuit, not produce the parsed values.
fn is_malformed(request: &FetchRequest) -> bool {
    parse_fetch_fields(request).is_err()
}

impl<S> Service<RequestInput<FetchRequest>> for FetchValidationService<S>
where
    S: Service<RequestInput<FetchRequest>, Output = FetchResponse, Error = Error>,
{
    type Output = FetchResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: RequestInput<FetchRequest>) -> Result<Self::Output> {
        if input.request.topics.is_none() || !is_malformed(&input.request) {
            return self.inner.serve(input).await;
        }

        debug!(request = ?input.request, "malformed fetch request");

        input
            .request
            .topics
            .as_deref()
            .unwrap_or_default()
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

    impl rama::Service<RequestInput<FetchRequest>> for CountingInner {
        type Output = FetchResponse;
        type Error = Error;

        async fn serve(&self, _input: RequestInput<FetchRequest>) -> Result<Self::Output> {
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
