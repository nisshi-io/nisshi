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

use nisshi_sans_io::{ApiKey as _, Frame, FrameInput, Header, MetadataRequest, MetadataResponse};
use nisshi_service::{
    BytesFrameLayer, BytesTcpService, Error as ServiceError, FrameBytesLayer, FrameService,
    TcpListenerInput, TcpListenerLayer, TcpStreamLayer,
};
use rama::{
    Layer as _, Service as _,
    error::BoxError,
    extensions::Extensions,
    tcp::{TcpStream, TokioTcpStream},
};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::common::{Error, init_tracing};

async fn server(cancellation: CancellationToken, listener: TcpListener) -> Result<(), Error> {
    let server = (
        TcpListenerLayer::new(cancellation),
        TcpStreamLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(FrameService::new(|req: FrameInput| {
            debug!(?req);

            req.frame
                .correlation_id()
                .map(|correlation_id| Frame {
                    size: 0,
                    header: Header::Response { correlation_id },
                    body: MetadataResponse::default()
                        .brokers(Some([].into()))
                        .topics(Some([].into()))
                        .cluster_id(Some("abc".into()))
                        .controller_id(Some(111))
                        .throttle_time_ms(Some(0))
                        .cluster_authorized_operations(Some(-1))
                        .into(),
                })
                .map_err(ServiceError::from)
        }));

    assert!(
        server
            .serve(TcpListenerInput {
                listener,
                extensions: Extensions::default(),
            })
            .await
            .is_ok()
    );

    Ok(())
}

#[tokio::test]
async fn tcp_client_server() -> Result<(), BoxError> {
    let _guard = init_tracing()?;

    let cancellation = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let local_addr = listener.local_addr()?;

    let mut join = JoinSet::new();

    let _server = {
        let cancellation = cancellation.clone();
        join.spawn(async move { server(cancellation, listener).await })
    };

    let extensions = Extensions::default();

    let stream =
        TcpStream::from_tokio_tcp_stream(TokioTcpStream::connect(local_addr).await?, extensions);

    let client = FrameBytesLayer.into_layer(BytesTcpService::new(stream));

    let frame = client
        .serve(FrameInput {
            frame: Frame {
                header: Header::Request {
                    api_key: MetadataRequest::KEY,
                    api_version: 12,
                    correlation_id: 0,
                    client_id: Some(env!("CARGO_PKG_NAME").into()),
                },
                body: MetadataRequest::default()
                    .topics(Some([].into()))
                    .allow_auto_topic_creation(Some(false))
                    .include_cluster_authorized_operations(Some(false))
                    .include_topic_authorized_operations(Some(false))
                    .into(),
                size: 0,
            },
            extensions: Extensions::default(),
        })
        .await?;

    let response = MetadataResponse::try_from(frame.body)?;
    assert_eq!(Some("abc"), response.cluster_id.as_deref());
    assert_eq!(Some(111), response.controller_id);

    cancellation.cancel();

    let joined = join.join_all().await;
    debug!(?joined);

    Ok(())
}
