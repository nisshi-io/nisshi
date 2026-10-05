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

use std::{fmt, marker::PhantomData, ops::RangeInclusive};

use nisshi_sans_io::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey, Body, ErrorCode, Frame,
    FrameInput, Header, ListOffsetsRequest, ListOffsetsResponse, ProduceRequest, ProduceResponse,
    Request, RequestInput, RootMessageMeta,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult,
    },
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};
use rama::{Layer, Service, layer::MapErrLayer, service::BoxService};
use tracing::instrument;

use crate::{Error, FrameRequestLayer, FrameRouteBuilder};

/// Implemented by a [`Request`] `Q` whose broker-advertised version range is narrower than the
/// Kafka protocol's own range for `Q`'s API key, so a client that negotiated a version within
/// the protocol's range (via `ApiVersions`) can still send a version nisshi does not route.
///
/// [`VersionGateLayer`] rejects such a request before it reaches the route's real service,
/// using [`unsupported_version`][Self::unsupported_version] to build a response that tells the
/// client what failed, rather than an empty-but-wire-valid one it cannot act on.
pub trait SupportedApiVersions: Request {
    /// The version range nisshi routes for this request. [`FrameRouteBuilder::with_capped_route`]
    /// fails at build time if this is not a subset of the protocol's own valid range.
    const SUPPORTED: RangeInclusive<i16>;

    /// Builds the response [`VersionGateLayer`] sends instead of routing `request` to its real
    /// service, for a negotiated version outside [`Self::SUPPORTED`].
    fn unsupported_version(request: Self) -> Self::Response;

    /// Whether [`VersionGateLayer`] drops the connection instead of sending
    /// [`unsupported_version`][Self::unsupported_version]'s response for `request`.
    ///
    /// The default always sends a response. `ProduceRequest` overrides this for `acks == 0`: a
    /// client that asked for no acknowledgment does not read a response to this request at all,
    /// so sending one only queues bytes nobody reads. Real Kafka drops the connection on an
    /// acks=0 produce error instead of responding, and this matches that.
    fn drop_connection_instead(_request: &Self) -> bool {
        false
    }
}

/// A [`Layer`] that rejects a [`SupportedApiVersions`] request whose negotiated version falls
/// outside [`SupportedApiVersions::SUPPORTED`], before [`FrameRequestLayer`] decodes it.
#[derive(Clone, Copy, Default)]
pub struct VersionGateLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> VersionGateLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<Q> fmt::Debug for VersionGateLayer<Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(VersionGateLayer)).finish()
    }
}

impl<S, Q> Layer<S> for VersionGateLayer<Q> {
    type Service = VersionGateService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

/// A [`Service`] enforcing [`SupportedApiVersions::SUPPORTED`] for `Q`, built by
/// [`VersionGateLayer`].
#[derive(Clone, Copy, Default)]
pub struct VersionGateService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q> fmt::Debug for VersionGateService<S, Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(VersionGateService)).finish()
    }
}

impl<S, Q> Service<FrameInput> for VersionGateService<S, Q>
where
    S: Service<FrameInput, Output = Frame>,
    S::Error: From<nisshi_sans_io::Error> + From<<Q as TryFrom<Body>>::Error>,
    Q: SupportedApiVersions,
{
    type Output = Frame;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let api_version = req.frame.api_version()?;

        if Q::SUPPORTED.contains(&api_version) {
            return self.inner.serve(req).await;
        }

        let correlation_id = req.frame.correlation_id()?;
        let request = Q::try_from(req.frame.body)?;

        if Q::drop_connection_instead(&request) {
            return Err(Self::Error::from(
                nisshi_sans_io::Error::UnsupportedVersion {
                    api_key: Q::KEY,
                    api_version,
                },
            ));
        }

        Ok(Frame {
            size: 0,
            header: Header::Response { correlation_id },
            body: Q::unsupported_version(request).into(),
        })
    }
}

/// A boxed route service for a capped API `Q`, producible only via [`capped_service`], which
/// always wires in [`VersionGateLayer<Q>`] -- so a route registered through
/// [`FrameRouteBuilder::with_capped_route`] can never skip the version gate.
pub struct CappedService<E> {
    inner: BoxService<FrameInput, Frame, E>,
}

impl<E> fmt::Debug for CappedService<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(CappedService)).finish()
    }
}

/// Wraps `inner` with [`VersionGateLayer<Q>`] and [`FrameRequestLayer<Q>`], the same layer
/// stack every other route uses with the version gate added in front, and converts its error
/// type to `E`.
pub fn capped_service<Q, S, E>(inner: S) -> CappedService<E>
where
    Q: SupportedApiVersions,
    S: Service<RequestInput<Q>, Output = Q::Response> + Send + Sync + 'static,
    S::Error:
        From<nisshi_sans_io::Error> + From<<Q as TryFrom<Body>>::Error> + Send + Sync + 'static,
    E: std::error::Error + From<nisshi_sans_io::Error> + From<S::Error> + Send + Sync + 'static,
{
    CappedService {
        inner: (
            MapErrLayer::new(E::from),
            VersionGateLayer::<Q>::new(),
            FrameRequestLayer::<Q>::new(),
        )
            .into_layer(inner)
            .boxed(),
    }
}

/// The highest version a request for `api_key` may be sent at without a real `ApiVersions`
/// negotiation to fall back on, or `None` for an api_key this build has no protocol metadata
/// for.
///
/// An internal caller that builds a request directly (`RequestFrameService`, the consumer-group
/// frame path in `nisshi-client`) has no negotiated version to use, because it is not acting as
/// a real client responding to its own `ApiVersions` round trip. For the 3 APIs capped below
/// the protocol's own maximum, that protocol maximum is a version `VersionGateLayer` rejects, so
/// this returns each one's own `SupportedApiVersions::SUPPORTED` maximum instead; every other
/// `api_key` keeps the protocol maximum, unaffected by any cap.
#[must_use]
pub fn routable_max_version(api_key: i16) -> Option<i16> {
    if api_key == ProduceRequest::KEY {
        Some(*ProduceRequest::SUPPORTED.end())
    } else if api_key == ListOffsetsRequest::KEY {
        Some(*ListOffsetsRequest::SUPPORTED.end())
    } else if api_key == AddPartitionsToTxnRequest::KEY {
        Some(*AddPartitionsToTxnRequest::SUPPORTED.end())
    } else {
        RootMessageMeta::messages()
            .requests()
            .get(&api_key)
            .map(|meta| meta.version.valid.end)
    }
}

/// Fails unless `Q::SUPPORTED` is a subset of the protocol's own valid range for `Q::KEY`.
fn validate_capped_range<Q>() -> Result<(), Error>
where
    Q: SupportedApiVersions,
{
    let protocol = RootMessageMeta::messages()
        .requests()
        .get(&Q::KEY)
        .map(|meta| meta.version.valid);

    let declared = Q::SUPPORTED;

    if protocol.is_some_and(|protocol| {
        *declared.start() >= protocol.start && *declared.end() <= protocol.end
    }) {
        Ok(())
    } else {
        Err(Error::CapRangeExceedsProtocolRange {
            api_key: Q::KEY,
            declared,
            protocol: protocol.map(|protocol| protocol.start..=protocol.end),
        })
    }
}

impl SupportedApiVersions for ProduceRequest {
    // Kafka's own Produce range is 0-11. nisshi only decodes the `RecordBatch` (magic v2)
    // format that Produce v3 introduced; versions 0-2 use the older message-set formats this
    // broker has never implemented. In practice a v0-2 request carrying a real legacy
    // MessageSet fails even earlier than this gate, at the raw bytes-layer record decoder
    // (a CRC/size mismatch against the v2 shape it expects), matching this broker's existing
    // handling of a wire format it structurally can't parse: there is no meaningful typed
    // rejection to send for it. Only a trivial/empty v0-2 payload that happens to decode
    // without a protocol error actually reaches `unsupported_version` below.
    const SUPPORTED: RangeInclusive<i16> = 3..=11;

    fn unsupported_version(request: Self) -> Self::Response {
        ProduceResponse::default()
            .responses(request.topic_data.map(|topics| {
                topics
                    .into_iter()
                    .map(|topic| {
                        TopicProduceResponse::default()
                            .name(topic.name)
                            .partition_responses(topic.partition_data.map(|partitions| {
                                partitions
                                    .into_iter()
                                    .map(|partition| {
                                        PartitionProduceResponse::default()
                                            .index(partition.index)
                                            .error_code(ErrorCode::UnsupportedVersion.into())
                                            .base_offset(-1)
                                            .log_append_time_ms(Some(-1))
                                            .log_start_offset(Some(-1))
                                    })
                                    .collect()
                            }))
                    })
                    .collect()
            }))
            .throttle_time_ms(Some(0))
    }

    fn drop_connection_instead(request: &Self) -> bool {
        request.acks == 0
    }
}

impl SupportedApiVersions for ListOffsetsRequest {
    // Kafka's own ListOffsets range is 0-9. Versions 7-9 introduce the MAX_TIMESTAMP
    // (KIP-734), EARLIEST_LOCAL_TIMESTAMP and LATEST_TIERED_TIMESTAMP (KIP-405/KIP-1005)
    // sentinel timestamps, none of which nisshi's storage backends look up
    // (`ListOffset::try_from` rejects them with `UnsupportedListOffsetTimestamp` regardless of
    // version, as defense in depth, but the version itself is still unimplemented).
    //
    // v0 is deliberately included: its `OldStyleOffsets`-never-populated gap is a response-shape
    // bug to fix, not a reason to cap the version -- raising the floor to exclude it would hide
    // that bug behind a version rejection and drop support for old clients that are otherwise
    // fully compatible with this broker's request-side decoding. `ListOffsetsService` populates
    // `OldStyleOffsets` for v0 responses instead of leaving it unset.
    const SUPPORTED: RangeInclusive<i16> = 0..=6;

    fn unsupported_version(request: Self) -> Self::Response {
        ListOffsetsResponse::default()
            .topics(request.topics.map(|topics| {
                topics
                    .into_iter()
                    .map(|topic| {
                        ListOffsetsTopicResponse::default()
                            .name(topic.name)
                            .partitions(topic.partitions.map(|partitions| {
                                partitions
                                    .into_iter()
                                    .map(|partition| {
                                        ListOffsetsPartitionResponse::default()
                                            .partition_index(partition.partition_index)
                                            .error_code(ErrorCode::UnsupportedVersion.into())
                                            // A non-nullable array field: build `Some(vec![])`
                                            // rather than `None` so a future capped API with a
                                            // similar response shape doesn't copy a `None` here
                                            // as a pattern. Moot for ListOffsets itself once the
                                            // floor is 0 -- `OldStyleOffsets` only exists on the
                                            // wire at v0, which this rejection path never runs
                                            // for -- but real shape-correctness in general.
                                            .old_style_offsets(Some(vec![]))
                                            .timestamp(Some(-1))
                                            .offset(Some(-1))
                                            .leader_epoch(Some(-1))
                                    })
                                    .collect()
                            }))
                    })
                    .collect()
            }))
            .throttle_time_ms(Some(0))
    }
}

impl SupportedApiVersions for AddPartitionsToTxnRequest {
    // Kafka's own AddPartitionsToTxn range is 0-5. Version 4 introduced the multi-transaction
    // request shape (`Transactions`), which every nisshi storage backend's
    // `txn_add_partitions` leaves unimplemented for that shape.
    const SUPPORTED: RangeInclusive<i16> = 0..=3;

    fn unsupported_version(request: Self) -> Self::Response {
        AddPartitionsToTxnResponse::default()
            .error_code(Some(ErrorCode::UnsupportedVersion.into()))
            .results_by_transaction(request.transactions.map(|transactions| {
                transactions
                    .into_iter()
                    .map(|transaction| {
                        AddPartitionsToTxnResult::default()
                            .transactional_id(transaction.transactional_id)
                            .topic_results(transaction.topics.map(|topics| {
                                topics
                                    .into_iter()
                                    .map(|topic| {
                                        AddPartitionsToTxnTopicResult::default()
                                            .name(topic.name)
                                            .results_by_partition(topic.partitions.map(
                                                |partitions| {
                                                    partitions
                                                    .into_iter()
                                                    .map(|partition_index| {
                                                        AddPartitionsToTxnPartitionResult::default()
                                                            .partition_index(partition_index)
                                                            .partition_error_code(
                                                                ErrorCode::UnsupportedVersion
                                                                    .into(),
                                                            )
                                                    })
                                                    .collect()
                                                },
                                            ))
                                    })
                                    .collect()
                            }))
                    })
                    .collect()
            }))
            .throttle_time_ms(0)
    }
}

impl<E> FrameRouteBuilder<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + Send + Sync + 'static,
{
    /// Registers `Q` as a route whose broker-supported version range
    /// ([`SupportedApiVersions::SUPPORTED`]) is narrower than the protocol's own range for its
    /// API key, via a [`CappedService`] built by [`capped_service`], and advertises that same
    /// narrower range in `ApiVersions` (via `FrameRouteBuilder::with_capped_range`) so a real
    /// client never negotiates a version this route then rejects.
    ///
    /// Fails with [`Error::CapRangeExceedsProtocolRange`] if `Q::SUPPORTED` is not a subset of
    /// the protocol's own valid range.
    pub fn with_capped_route<Q>(self, service: CappedService<E>) -> Result<Self, Error>
    where
        Q: SupportedApiVersions,
    {
        validate_capped_range::<Q>()?;
        self.with_capped_range(Q::KEY, *Q::SUPPORTED.start(), *Q::SUPPORTED.end())
            .with_route(Q::KEY, service.inner)
    }
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::{ApiKey, MetadataRequest};

    use super::*;

    // `MetadataRequest` is not one of the 3 real capped APIs; this impl exists only so the
    // test below has a `SupportedApiVersions` whose declared range it can set to something
    // deliberately outside the protocol's own range (0-12) for `MetadataRequest`.
    impl SupportedApiVersions for MetadataRequest {
        const SUPPORTED: RangeInclusive<i16> = 0..=999;

        fn unsupported_version(_request: Self) -> Self::Response {
            Self::Response::default()
        }
    }

    #[test]
    fn declared_range_wider_than_protocol_range_fails_validation() {
        let err = validate_capped_range::<MetadataRequest>()
            .expect_err("0..=999 exceeds MetadataRequest's real protocol range of 0-12");

        assert!(matches!(
            err,
            Error::CapRangeExceedsProtocolRange {
                api_key,
                ..
            } if api_key == MetadataRequest::KEY
        ));
    }
}
