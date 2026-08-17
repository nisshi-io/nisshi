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

//! Enforcement test for SASL/SCRAM.
//!
//! Security requirement: a client that runs a SCRAM exchange without
//! proving valid credentials must NOT be admitted to any gated API. This
//! test drives that exact attack and asserts the connection is rejected.
//!
//! # Background
//!
//! Before the accompanying fix, `Authentication::is_authenticated()`
//! returned `true` for any `Stage::Finished(_)`, ignoring whether
//! validation produced an identity. rsasl's SCRAM server reaches
//! `State::Finished` even on a bad proof (it writes an `e=` error token
//! and trusts a well-behaved client to abort), so a client that simply
//! did not abort was admitted. The fix requires `Stage::Finished(Some(_))`;
//! this test guards against regressing it.

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use nisshi_broker::{
    Error, Result,
    service::{auth, storage},
};
use nisshi_sans_io::{
    ApiKey, Body, CreateTopicsRequest, ErrorCode, Frame, Header, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
    create_topics_request::CreatableTopic,
};
use nisshi_service::{BytesFrameLayer, BytesFrameService, FrameRouteService};
use nisshi_storage::{Storage, StorageContainer};
use rama::{Context, Layer as _, Service as _};
use rsasl::{
    config::SASLConfig,
    prelude::{Mechname, SASLClient, State},
};
use url::Url;

type Storages = Arc<Box<dyn Storage>>;
type Broker = BytesFrameService<FrameRouteService<(), Error>>;

fn broker<S>(storage: S, sasl_config: Option<Arc<SASLConfig>>) -> Result<Broker>
where
    S: Storage + Clone,
{
    storage::services(FrameRouteService::<(), Error>::builder(), storage)
        .and_then(auth::services)
        .and_then(|builder| builder.build().map_err(Into::into))
        .map(|frame_route| {
            (BytesFrameLayer::default().with_sasl_config(sasl_config),).into_layer(frame_route)
        })
}

async fn memory_storage() -> Result<Storages> {
    StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092").expect("listener"))
        .storage(Url::parse("memory://tansu/").expect("storage"))
        .build()
        .await
        .map_err(Into::into)
}

fn create_topics_frame(correlation_id: i32) -> Result<Bytes> {
    Frame::request(
        Header::Request {
            api_key: CreateTopicsRequest::KEY,
            api_version: 7,
            correlation_id,
            client_id: Some("poc".into()),
        },
        Body::CreateTopicsRequest(
            CreateTopicsRequest::default()
                .timeout_ms(30_000)
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .assignments(Some([].into()))
                        .configs(Some([].into()))
                        .name("gated".into())
                        .num_partitions(1)
                        .replication_factor(1)]
                    .into(),
                )),
        ),
    )
    .map_err(Into::into)
}

fn is_not_authenticated<T>(result: &Result<T>) -> bool {
    matches!(
        result,
        Err(Error::KafkaProtocol(
            nisshi_sans_io::Error::NotAuthenticated
        ))
    )
}

#[tokio::test]
async fn scram_rejects_unverified_client() -> Result<()> {
    // The broker enforces SASL/SCRAM: `sasl_config` is Some(..).
    let engine = memory_storage().await?;
    let sasl_config = nisshi_auth::configuration(engine.clone())
        .map(Some)
        .map_err(Error::from)?;
    let broker = broker(engine, sasl_config)?;

    const API_VERSION: i16 = 1;
    let ctx = Context::default();
    let mut correlation_id = 0;

    // Baseline: a gated request before any SASL is rejected.
    assert!(
        is_not_authenticated(
            &broker
                .serve(ctx.clone(), create_topics_frame(correlation_id)?)
                .await
        ),
        "baseline: an unauthenticated CreateTopics must be rejected",
    );

    // 1. Negotiate SCRAM-SHA-256.
    correlation_id += 1;
    let response = broker
        .serve(
            ctx.clone(),
            Frame::request(
                Header::Request {
                    api_key: SaslHandshakeRequest::KEY,
                    api_version: API_VERSION,
                    correlation_id,
                    client_id: Some("poc".into()),
                },
                Body::SaslHandshakeRequest(
                    SaslHandshakeRequest::default().mechanism("SCRAM-SHA-256".into()),
                ),
            )?,
        )
        .await?;
    let handshake = Frame::response_from_bytes(response, SaslHandshakeResponse::KEY, API_VERSION)
        .and_then(|frame| SaslHandshakeResponse::try_from(frame.body))?;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(handshake.error_code)?,
        "SCRAM-SHA-256 must be negotiable for this test to be meaningful",
    );
    let offered = handshake
        .mechanisms
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|mechanism| Mechname::parse(mechanism.as_bytes()).ok())
        .collect::<Vec<_>>();

    // 2. Run the SCRAM exchange with junk credentials: a user with no
    //    stored credential and an arbitrary password, so no proof can
    //    ever verify. Tolerate the broker rejecting the exchange at any
    //    step (that is acceptable, secure behaviour); we only care about
    //    the gated request below.
    let sasl = SASLClient::new(
        SASLConfig::with_credentials(None, "attacker".into(), "wrong-password".into())
            .expect("client sasl config"),
    );
    let mut session = sasl.start_suggested(&offered).expect("start scram client");
    let mut input: Option<Bytes> = None;

    loop {
        correlation_id += 1;
        let mut output = BytesMut::new().writer();

        match session.step(input.as_deref(), &mut output) {
            Ok(State::Running) => {
                let response = broker
                    .serve(
                        ctx.clone(),
                        Frame::request(
                            Header::Request {
                                api_key: SaslAuthenticateRequest::KEY,
                                api_version: API_VERSION,
                                correlation_id,
                                client_id: Some("poc".into()),
                            },
                            Body::SaslAuthenticateRequest(
                                SaslAuthenticateRequest::default()
                                    .auth_bytes(Bytes::from(output.into_inner())),
                            ),
                        )?,
                    )
                    .await;

                match response {
                    Ok(bytes) => {
                        let authenticate = Frame::response_from_bytes(
                            bytes,
                            SaslAuthenticateResponse::KEY,
                            API_VERSION,
                        )
                        .and_then(|frame| SaslAuthenticateResponse::try_from(frame.body))?;
                        input = Some(authenticate.auth_bytes);
                    }
                    // Broker rejected mid-authentication: fine, it did not
                    // grant access. Stop and check the gate below.
                    Err(_) => break,
                }
            }
            Ok(State::Finished(_)) | Err(_) => break,
        }
    }

    // THE REQUIREMENT: a gated request on this connection must be
    // rejected, because the client never proved a valid identity.
    correlation_id += 1;
    let result = broker
        .serve(ctx.clone(), create_topics_frame(correlation_id)?)
        .await;
    assert!(
        is_not_authenticated(&result),
        "SECURITY: broker admitted a gated request after a SCRAM exchange that proved no identity \
         (got {result:?}). Fails against the current vulnerable gate; must pass once \
         is_authenticated() requires a validated exchange.",
    );

    Ok(())
}
