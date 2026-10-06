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

//! Tests for the version gate on each route registered through `with_capped_route`, and for
//! the `ApiVersions` reply to a request outside the protocol range.

use bytes::Bytes;
use nisshi_sans_io::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey as _, ApiVersionsRequest,
    ApiVersionsResponse, Body, BytesInput, ErrorCode, Frame, FrameInput, Header,
    ListOffsetsRequest, ListOffsetsResponse, ProduceRequest, ProduceResponse, RequestInput,
};
use nisshi_service::{
    BytesFrameLayer, BytesTcpService, CAPPED_API_VERSIONS, FrameRouteService, ResponseService,
    TcpListenerInput, TcpListenerLayer, TcpStreamLayer,
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

/// A `FrameRouteService` with a stub handler for each capped API.
fn capped_frame_route() -> Result<FrameRouteService<Error>, Error> {
    FrameRouteService::<Error>::builder()
        .with_capped_route::<ListOffsetsRequest, _>(ResponseService::new(
            |_: RequestInput<ListOffsetsRequest>| {
                Ok::<_, nisshi_service::Error>(ListOffsetsResponse::default())
            },
        ))?
        .with_capped_route::<ProduceRequest, _>(ResponseService::new(
            |_: RequestInput<ProduceRequest>| {
                Ok::<_, nisshi_service::Error>(ProduceResponse::default())
            },
        ))?
        .with_capped_route::<AddPartitionsToTxnRequest, _>(ResponseService::new(
            |_: RequestInput<AddPartitionsToTxnRequest>| {
                Ok::<_, nisshi_service::Error>(AddPartitionsToTxnResponse::default())
            },
        ))?
        .build()
        .map_err(Error::from)
}

fn default_body(api_key: i16) -> Body {
    match api_key {
        ProduceRequest::KEY => ProduceRequest::default().into(),
        ListOffsetsRequest::KEY => ListOffsetsRequest::default().into(),
        AddPartitionsToTxnRequest::KEY => AddPartitionsToTxnRequest::default().into(),
        otherwise => panic!("no capped API with key {otherwise}"),
    }
}

async fn serve(
    frame_route: &FrameRouteService<Error>,
    api_key: i16,
    api_version: i16,
) -> Result<Frame, Error> {
    frame_route
        .serve(FrameInput {
            frame: Frame {
                size: 0,
                header: Header::Request {
                    api_key,
                    api_version,
                    correlation_id: 0,
                    client_id: None,
                },
                body: default_body(api_key),
            },
            extensions: Extensions::default(),
        })
        .await
}

#[tokio::test]
async fn version_inside_the_cap_reaches_the_handler() -> Result<(), Error> {
    let frame_route = capped_frame_route()?;

    for (api_key, supported) in CAPPED_API_VERSIONS {
        for api_version in [*supported.start(), *supported.end()] {
            let response = serve(&frame_route, api_key, api_version).await?;
            assert!(
                matches!(response.header, Header::Response { .. }),
                "api_key: {api_key}, api_version: {api_version}"
            );
        }
    }

    Ok(())
}

// Each version below is inside the protocol range, so the request passes the protocol-range
// check in `FrameRouteService` and reaches the version gate.
#[tokio::test]
async fn version_outside_the_cap_closes_the_connection() -> Result<(), Error> {
    let frame_route = capped_frame_route()?;

    for (api_key, api_version) in [
        (ProduceRequest::KEY, 2),
        (ListOffsetsRequest::KEY, 7),
        (ListOffsetsRequest::KEY, 9),
        (AddPartitionsToTxnRequest::KEY, 4),
    ] {
        let err = serve(&frame_route, api_key, api_version)
            .await
            .expect_err("a version outside the cap");

        assert!(
            matches!(
                err,
                Error::Service(nisshi_service::Error::Protocol(
                    nisshi_sans_io::Error::UnsupportedVersion {
                        api_key: rejected_key,
                        api_version: rejected_version,
                    }
                )) if rejected_key == api_key && rejected_version == api_version
            ),
            "api_key: {api_key}, api_version: {api_version}, err: {err:?}"
        );
    }

    Ok(())
}

// `ApiVersionsRequest` v5 adds `ClusterId` and `NodeId` as non-tagged fields
// (https://github.com/apache/kafka/blob/e90d6f42c2957f2b970266e3c5b0ff2ebf972b59/clients/src/main/resources/common/message/ApiVersionsRequest.json#L27-L42).
// This build's decoder knows versions 0-4, so it fails on a v5 body with `ClusterId` set. The
// broker answers without decoding the body, at v0, with `UNSUPPORTED_VERSION` and the
// `ApiVersions` range that the client retries with.
#[tokio::test]
async fn api_versions_above_the_protocol_range_is_answered_at_v0() -> Result<(), Error> {
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

    #[rustfmt::skip]
    let request_bytes = Bytes::from_static(&[
        0, 0, 0, 22,          // size
        0, 18,                // api_key = ApiVersionsRequest::KEY
        0, 5,                 // api_version = 5
        0, 0, 0, 7,           // correlation_id = 7
        0xff, 0xff,           // client_id = null
        0x00,                 // header tag buffer: empty
        0x00,                 // client_software_name: compact null
        0x00,                 // client_software_version: compact null
        0x04, b'a', b'b', b'c', // cluster_id = "abc" (compact string, length + 1)
        0, 0, 0, 111,         // node_id = 111
        0x00,                 // body tag buffer: empty
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

    let response = Frame::response_from_bytes(outcome?, ApiVersionsRequest::KEY, 0)?;
    assert!(matches!(
        response.header,
        Header::Response { correlation_id: 7 }
    ));

    let response = ApiVersionsResponse::try_from(response.body)?;
    assert_eq!(
        ErrorCode::UnsupportedVersion,
        ErrorCode::try_from(response.error_code)?
    );

    let api_versions = response
        .api_keys
        .unwrap_or_default()
        .into_iter()
        .find(|api| api.api_key == ApiVersionsRequest::KEY)
        .expect("the reply lists ApiVersions");
    assert_eq!((0, 4), (api_versions.min_version, api_versions.max_version));

    Ok(())
}
