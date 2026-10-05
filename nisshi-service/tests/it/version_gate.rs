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

//! These tests route a real `FrameInput` through `capped_service` for each of the 3 capped
//! APIs at an out-of-range version and assert the response genuinely echoes the request's real
//! topics/partitions with `UnsupportedVersion`, plus the separate acks=0 case that drops the
//! connection instead of building a response at all. An `unsupported_version` impl that
//! returns an empty default response instead of echoing the real request's partitions would
//! still satisfy a check that only looks at the error code, so these tests check the response
//! content itself.

use bytes::Bytes;
use nisshi_sans_io::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey as _, ApiVersionsRequest,
    ApiVersionsResponse, BytesInput, ErrorCode, Frame, FrameInput, Header, IsolationLevel,
    ListOffset, ListOffsetsRequest, ListOffsetsResponse, ProduceRequest, ProduceResponse,
    RequestInput,
    add_partitions_to_txn_request::{AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
};
use nisshi_service::{
    BytesFrameLayer, BytesTcpService, FrameRouteService, ResponseService, TcpListenerInput,
    TcpListenerLayer, TcpStreamLayer, capped_service,
};
use rama::{
    Layer as _, Service,
    extensions::Extensions,
    tcp::{TcpStream, TokioTcpStream},
};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::common::Error;

/// A `FrameRouteService` with all 3 real capped routes registered, each backed by a stub inner
/// service. The stub is never actually invoked by the tests below -- the version gate rejects
/// every request here before it would ever reach the inner service -- so its response content
/// does not matter, only that the types line up.
async fn capped_frame_route() -> Result<FrameRouteService<Error>, Error> {
    let builder = FrameRouteService::<Error>::builder()
        .with_capped_route::<ListOffsetsRequest>(capped_service::<ListOffsetsRequest, _, Error>(
            ResponseService::new(|_: RequestInput<ListOffsetsRequest>| {
                Ok::<_, nisshi_service::Error>(ListOffsetsResponse::default())
            }),
        ))
        .map_err(Error::from)?;

    let builder = builder
        .with_capped_route::<ProduceRequest>(capped_service::<ProduceRequest, _, Error>(
            ResponseService::new(|_: RequestInput<ProduceRequest>| {
                Ok::<_, nisshi_service::Error>(ProduceResponse::default())
            }),
        ))
        .map_err(Error::from)?;

    let builder = builder
        .with_capped_route::<AddPartitionsToTxnRequest>(capped_service::<
            AddPartitionsToTxnRequest,
            _,
            Error,
        >(ResponseService::new(
            |_: RequestInput<AddPartitionsToTxnRequest>| {
                Ok::<_, nisshi_service::Error>(AddPartitionsToTxnResponse::default())
            },
        )))
        .map_err(Error::from)?;

    builder.build().map_err(Error::from)
}

#[tokio::test]
async fn list_offsets_out_of_range_version_echoes_the_real_request() -> Result<(), Error> {
    let frame_route = capped_frame_route().await?;

    let topic = "my-topic";

    let request = ListOffsetsRequest::default()
        .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
        .replica_id(-1)
        .topics(Some(vec![
            ListOffsetsTopic::default()
                .name(topic.into())
                .partitions(Some(vec![
                    ListOffsetsPartition::default()
                        .partition_index(3)
                        .max_num_offsets(Some(1))
                        .timestamp(ListOffset::Latest.try_into()?)
                        .current_leader_epoch(Some(-1)),
                ])),
        ]));

    // ListOffsets' own SUPPORTED range is 0-6; 9 is within the protocol's own 0-9 range, so
    // this reaches the route (and its version gate) rather than the generic backstop.
    let response = frame_route
        .serve(FrameInput {
            frame: Frame {
                size: 0,
                header: Header::Request {
                    api_key: ListOffsetsRequest::KEY,
                    api_version: 9,
                    correlation_id: 0,
                    client_id: None,
                },
                body: request.into(),
            },
            extensions: Extensions::default(),
        })
        .await?;

    let response = ListOffsetsResponse::try_from(response.body)?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic, topics[0].name);

    let partitions = topics[0].partitions.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(3, partitions[0].partition_index);
    assert_eq!(
        ErrorCode::UnsupportedVersion,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    Ok(())
}

#[tokio::test]
async fn produce_out_of_range_version_with_acks_echoes_the_real_request() -> Result<(), Error> {
    let frame_route = capped_frame_route().await?;

    let topic = "my-topic";

    let request = ProduceRequest::default()
        .transactional_id(None)
        .acks(1)
        .timeout_ms(0)
        .topic_data(Some(vec![
            TopicProduceData::default()
                .name(topic.into())
                .partition_data(Some(vec![
                    PartitionProduceData::default().index(7).records(None),
                ])),
        ]));

    // Produce's own SUPPORTED range is 3-11; 2 is within the protocol's own 0-11 range.
    let response = frame_route
        .serve(FrameInput {
            frame: Frame {
                size: 0,
                header: Header::Request {
                    api_key: ProduceRequest::KEY,
                    api_version: 2,
                    correlation_id: 0,
                    client_id: None,
                },
                body: request.into(),
            },
            extensions: Extensions::default(),
        })
        .await?;

    let response = ProduceResponse::try_from(response.body)?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());
    assert_eq!(topic, responses[0].name);

    let partitions = responses[0].partition_responses.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(7, partitions[0].index);
    assert_eq!(
        ErrorCode::UnsupportedVersion,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    Ok(())
}

#[tokio::test]
async fn add_partitions_to_txn_out_of_range_version_echoes_the_real_request() -> Result<(), Error> {
    let frame_route = capped_frame_route().await?;

    let transactional_id = "my-txn";
    let topic = "my-topic";

    let request = AddPartitionsToTxnRequest::default().transactions(Some(vec![
        AddPartitionsToTxnTransaction::default()
            .transactional_id(transactional_id.into())
            .producer_id(54345)
            .producer_epoch(0)
            .verify_only(false)
            .topics(Some(vec![
                AddPartitionsToTxnTopic::default()
                    .name(topic.into())
                    .partitions(Some(vec![5])),
            ])),
    ]));

    // AddPartitionsToTxn's own SUPPORTED range is 0-3; 4 is within the protocol's own 0-5
    // range.
    let response = frame_route
        .serve(FrameInput {
            frame: Frame {
                size: 0,
                header: Header::Request {
                    api_key: AddPartitionsToTxnRequest::KEY,
                    api_version: 4,
                    correlation_id: 0,
                    client_id: None,
                },
                body: request.into(),
            },
            extensions: Extensions::default(),
        })
        .await?;

    let response = AddPartitionsToTxnResponse::try_from(response.body)?;

    assert_eq!(
        Some(i16::from(ErrorCode::UnsupportedVersion)),
        response.error_code
    );

    let results = response.results_by_transaction.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(transactional_id, results[0].transactional_id);

    let topics = results[0].topic_results.clone().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic, topics[0].name);

    let partitions = topics[0].results_by_partition.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(5, partitions[0].partition_index);
    assert_eq!(
        ErrorCode::UnsupportedVersion,
        ErrorCode::try_from(partitions[0].partition_error_code)?
    );

    Ok(())
}

#[tokio::test]
async fn produce_out_of_range_version_with_acks_zero_drops_the_connection() -> Result<(), Error> {
    let frame_route = capped_frame_route().await?;

    let request = ProduceRequest::default()
        .transactional_id(None)
        .acks(0)
        .timeout_ms(0)
        .topic_data(Some(vec![
            TopicProduceData::default()
                .name("my-topic".into())
                .partition_data(Some(vec![
                    PartitionProduceData::default().index(0).records(None),
                ])),
        ]));

    let err = frame_route
        .serve(FrameInput {
            frame: Frame {
                size: 0,
                header: Header::Request {
                    api_key: ProduceRequest::KEY,
                    api_version: 2,
                    correlation_id: 0,
                    client_id: None,
                },
                body: request.into(),
            },
            extensions: Extensions::default(),
        })
        .await
        .expect_err("acks=0 drops the connection instead of building a response");

    assert!(matches!(
        err,
        Error::Service(nisshi_service::Error::Protocol(
            nisshi_sans_io::Error::UnsupportedVersion { api_key, api_version }
        )) if api_key == ProduceRequest::KEY && api_version == 2
    ));

    Ok(())
}

/// The unit test alongside `FrameRouteService`'s two `serve` impls in `api.rs` proves the
/// routing exemption (an out-of-range `ApiVersions` request gets an answer, not a closed
/// connection) but calls `serve` directly, never going through `BytesFrameService` -- so it
/// cannot catch a mistake in that service's `encode_version` (frame.rs), which is what actually
/// forces the reply onto the wire at v0 instead of whatever out-of-range version the client
/// sent. This test goes over a real `TcpStream` through the full byte-level stack instead, and
/// decodes the raw response bytes explicitly at both versions: decoding at v0 must succeed and
/// report `UnsupportedVersion`, and decoding the exact same bytes at v9 (the flexible,
/// tagged-field shape) must fail, proving the reply really is v0-shaped rather than happening
/// to decode at any version asked of it.
#[tokio::test]
async fn out_of_range_api_versions_is_encoded_at_v0_over_the_wire() -> Result<(), Error> {
    let _guard = crate::common::init_tracing()?;

    let frame_route = FrameRouteService::<nisshi_service::Error>::builder().build()?;

    let cancellation = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let local_addr = listener.local_addr()?;

    let mut join = JoinSet::new();
    {
        let cancellation = cancellation.clone();
        let server = (
            TcpListenerLayer::new(cancellation),
            TcpStreamLayer,
            BytesFrameLayer::default(),
        )
            .into_layer(frame_route);

        _ = join.spawn(async move {
            server
                .serve(TcpListenerInput {
                    listener,
                    extensions: Extensions::default(),
                })
                .await
        });
    }

    let stream = TcpStream::from_tokio_tcp_stream(
        TokioTcpStream::connect(local_addr).await?,
        Extensions::default(),
    );
    let client = BytesTcpService::new(stream);

    // Hand-built rather than via `Frame::request`: this crate's generated encoder has never
    // been exercised at a version outside the protocol's own declared range (no real client
    // ever sends one), and encoding a v9 `ApiVersionsRequest` through it here produced a
    // malformed, too-short frame the server failed to decode -- an encode-side gap in
    // already-generated code this PR does not touch, not something to paper over inside a
    // test. A real out-of-range client only ever sends bytes that decode as some version this
    // crate's descriptors recognise (that's what "which version" even means on the wire), so a
    // literal byte buffer mirroring the real v9 wire shape (flexible header, compact-null
    // `ClientSoftwareName`/`ClientSoftwareVersion`, empty tag buffers) is the faithful way to
    // construct this request.
    #[rustfmt::skip]
    let request_bytes = Bytes::from_static(&[
        0, 0, 0, 14,     // size
        0, 18,           // api_key = ApiVersionsRequest::KEY
        0, 9,            // api_version = 9 (out of range; valid is 0-4)
        0, 0, 0, 0,      // correlation_id = 0
        0xff, 0xff,      // client_id = null (classic nullable string, even in a flexible header)
        0x00,            // header tag buffer: empty
        0x00,            // client_software_name: compact-null
        0x00,            // client_software_version: compact-null
        0x00,            // body tag buffer: empty
    ]);

    let outcome = client
        .serve(BytesInput {
            bytes: request_bytes,
            extensions: Extensions::default(),
        })
        .await;

    cancellation.cancel();
    let joined = join.join_all().await;
    debug!(?joined);

    let response_bytes = outcome?;

    let at_v0 = Frame::response_from_bytes(response_bytes.clone(), ApiVersionsRequest::KEY, 0)?;
    let response = ApiVersionsResponse::try_from(at_v0.body)?;
    assert_eq!(
        i16::from(ErrorCode::UnsupportedVersion),
        response.error_code
    );

    assert!(
        Frame::response_from_bytes(response_bytes, ApiVersionsRequest::KEY, 9).is_err(),
        "the same bytes decoded as if shaped for v9 should fail to parse: they are v0-shaped"
    );

    Ok(())
}
