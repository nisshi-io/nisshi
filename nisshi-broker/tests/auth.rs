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
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use common::init_tracing;
use nisshi_broker::{
    Error, Result,
    service::{auth, storage},
};
use nisshi_sans_io::{
    ApiKey, Body, ConfigResource, CreateTopicsRequest, ErrorCode, Frame, Header, IsolationLevel,
    ListOffset, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse, ScramMechanism, create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult, delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    fetch_response::AbortedTransaction, incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup, record::deflated::Batch,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use nisshi_service::{BytesFrameLayer, BytesFrameService, FrameRouteService};
use nisshi_storage::{
    BrokerRegistrationRequest, GroupDetail, ListOffsetResponse, MetadataResponse, NamedGroupDetail,
    OffsetCommitRequest, OffsetStage, ProducerIdResponse, ScramCredential, Storage, TopicId,
    Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
    UpdateError, Version,
};
use rama::{Context, Layer as _, Service as _};
use rsasl::{
    config::SASLConfig,
    prelude::{Mechname, SASLClient, SessionError, State},
};
use tracing::{debug, instrument};
use url::Url;
use uuid::Uuid;

pub mod common;

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

#[tokio::test]
async fn auth_handshake_scram_256_v1() -> Result<()> {
    let _guard = init_tracing()?;

    const PRINCIPAL: &str = "alice";
    const PASSWORD: &str = "secret";
    const CLIENT_ID: &str = "rdkafka";

    let engine = Engine::default().with_credential(
        PRINCIPAL,
        ScramMechanism::Scram256,
        ScramCredential {
            salt: Bytes::from_static(&[
                107, 53, 50, 116, 97, 121, 49, 118, 118, 116, 105, 97, 53, 101, 54, 108, 99, 51,
                55, 103, 110, 51, 102, 51, 104,
            ]),
            iterations: 8192,
            stored_key: Bytes::from_static(&[
                150, 254, 7, 121, 81, 205, 192, 207, 60, 206, 251, 24, 31, 131, 31, 15, 96, 75, 20,
                228, 251, 132, 22, 235, 160, 72, 200, 130, 127, 49, 29, 150,
            ]),
            server_key: Bytes::from_static(&[
                186, 175, 253, 227, 176, 106, 88, 53, 186, 173, 104, 88, 94, 40, 115, 166, 44, 183,
                199, 177, 137, 41, 225, 132, 56, 32, 70, 255, 223, 209, 22, 146,
            ]),
        },
    );

    let broker = nisshi_auth::configuration(engine.clone())
        .map_err(Into::into)
        .map(Some)
        .and_then(|sasl_config| broker(engine, sasl_config))?;

    const API_VERSION: i16 = 1;
    let mut correlation_id = 0;

    let ctx = Context::default();

    let response = broker
        .serve(
            ctx.clone(),
            Frame::request(
                Header::Request {
                    api_key: SaslHandshakeRequest::KEY,
                    api_version: API_VERSION,
                    correlation_id,
                    client_id: Some(CLIENT_ID.into()),
                },
                Body::SaslHandshakeRequest(
                    SaslHandshakeRequest::default().mechanism("SCRAM-SHA-256".into()),
                ),
            )?,
        )
        .await?;

    let response = Frame::response_from_bytes(response, SaslHandshakeResponse::KEY, API_VERSION)
        .and_then(|response| SaslHandshakeResponse::try_from(response.body))?;
    debug!(?response);

    let offered = response
        .mechanisms
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|mechanism| Mechname::parse(mechanism.as_bytes()).ok())
        .collect::<Vec<_>>();

    let sasl = SASLClient::new(
        SASLConfig::with_credentials(None, PRINCIPAL.into(), PASSWORD.into())
            .expect("sasl credential config"),
    );

    let mut session = sasl.start_suggested(&offered).unwrap();

    assert!(session.are_we_first());

    let mut input = None;

    loop {
        correlation_id += 1;
        let mut output = BytesMut::new().writer();

        match session.step(input.as_deref(), &mut output).unwrap() {
            State::Running => {
                let response = broker
                    .serve(
                        ctx.clone(),
                        Frame::request(
                            Header::Request {
                                api_key: SaslAuthenticateRequest::KEY,
                                api_version: API_VERSION,
                                correlation_id,
                                client_id: Some(CLIENT_ID.into()),
                            },
                            Body::SaslAuthenticateRequest(
                                SaslAuthenticateRequest::default()
                                    .auth_bytes(Bytes::from(output.into_inner())),
                            ),
                        )?,
                    )
                    .await?;

                let response = Frame::response_from_bytes(
                    response,
                    SaslAuthenticateResponse::KEY,
                    API_VERSION,
                )
                .and_then(|response| SaslAuthenticateResponse::try_from(response.body))?;
                debug!(?response);
                assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

                input = Some(response.auth_bytes);
            }
            State::Finished(message_sent) => {
                debug!(?message_sent);
                break;
            }
        }
    }

    Ok(())
}

/// Regression: a successful SASL handshake + authenticate must not
/// prevent a subsequent re-authentication on the same connection
/// (KIP-368, used by Java kafka-clients, Strimzi Kafka Connect, etc).
///
/// Pre-fix the second `SaslHandshakeRequest` failed with
/// `UnsupportedSaslMechanism` because `Authentication`'s Stage had
/// transitioned `Server -> Session -> Finished` and never reset.
#[tokio::test]
async fn auth_handshake_scram_256_v1_reauth() -> Result<()> {
    let _guard = init_tracing()?;

    const PRINCIPAL: &str = "alice";
    const PASSWORD: &str = "secret";
    const CLIENT_ID: &str = "rdkafka";

    let engine = Engine::default().with_credential(
        PRINCIPAL,
        ScramMechanism::Scram256,
        ScramCredential {
            salt: Bytes::from_static(&[
                107, 53, 50, 116, 97, 121, 49, 118, 118, 116, 105, 97, 53, 101, 54, 108, 99, 51,
                55, 103, 110, 51, 102, 51, 104,
            ]),
            iterations: 8192,
            stored_key: Bytes::from_static(&[
                150, 254, 7, 121, 81, 205, 192, 207, 60, 206, 251, 24, 31, 131, 31, 15, 96, 75, 20,
                228, 251, 132, 22, 235, 160, 72, 200, 130, 127, 49, 29, 150,
            ]),
            server_key: Bytes::from_static(&[
                186, 175, 253, 227, 176, 106, 88, 53, 186, 173, 104, 88, 94, 40, 115, 166, 44, 183,
                199, 177, 137, 41, 225, 132, 56, 32, 70, 255, 223, 209, 22, 146,
            ]),
        },
    );

    let broker = nisshi_auth::configuration(engine.clone())
        .map_err(Into::into)
        .map(Some)
        .and_then(|sasl_config| broker(engine, sasl_config))?;

    const API_VERSION: i16 = 1;
    let ctx = Context::default();
    let mut correlation_id = 0;

    // Run a full handshake + authenticate twice on the same broker
    // (i.e. same `Authentication` shared via the BytesFrameLayer).
    for round in 0..2 {
        let response = broker
            .serve(
                ctx.clone(),
                Frame::request(
                    Header::Request {
                        api_key: SaslHandshakeRequest::KEY,
                        api_version: API_VERSION,
                        correlation_id,
                        client_id: Some(CLIENT_ID.into()),
                    },
                    Body::SaslHandshakeRequest(
                        SaslHandshakeRequest::default().mechanism("SCRAM-SHA-256".into()),
                    ),
                )?,
            )
            .await?;

        let response =
            Frame::response_from_bytes(response, SaslHandshakeResponse::KEY, API_VERSION)
                .and_then(|response| SaslHandshakeResponse::try_from(response.body))?;
        debug!(round, ?response);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(response.error_code)?,
            "SaslHandshake (round {round}) must succeed; pre-fix the second one returns \
             UnsupportedSaslMechanism because Stage was already Finished",
        );

        let offered = response
            .mechanisms
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter_map(|mechanism| Mechname::parse(mechanism.as_bytes()).ok())
            .collect::<Vec<_>>();

        let sasl = SASLClient::new(
            SASLConfig::with_credentials(None, PRINCIPAL.into(), PASSWORD.into())
                .expect("sasl credential config"),
        );

        let mut session = sasl.start_suggested(&offered).unwrap();
        assert!(session.are_we_first());
        let mut input = None;

        loop {
            correlation_id += 1;
            let mut output = BytesMut::new().writer();

            match session.step(input.as_deref(), &mut output).unwrap() {
                State::Running => {
                    let response = broker
                        .serve(
                            ctx.clone(),
                            Frame::request(
                                Header::Request {
                                    api_key: SaslAuthenticateRequest::KEY,
                                    api_version: API_VERSION,
                                    correlation_id,
                                    client_id: Some(CLIENT_ID.into()),
                                },
                                Body::SaslAuthenticateRequest(
                                    SaslAuthenticateRequest::default()
                                        .auth_bytes(Bytes::from(output.into_inner())),
                                ),
                            )?,
                        )
                        .await?;

                    let response = Frame::response_from_bytes(
                        response,
                        SaslAuthenticateResponse::KEY,
                        API_VERSION,
                    )
                    .and_then(|response| SaslAuthenticateResponse::try_from(response.body))?;
                    debug!(round, ?response);
                    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
                    input = Some(response.auth_bytes);
                }
                State::Finished(_) => break,
            }
        }
        correlation_id += 1;
    }

    Ok(())
}

#[tokio::test]
async fn auth_handshake_scram_512_v1() -> Result<()> {
    let _guard = init_tracing()?;

    const PRINCIPAL: &str = "alice";
    const PASSWORD: &str = "secret";
    const CLIENT_ID: &str = "rdkafka";

    let engine = Engine::default().with_credential(
        PRINCIPAL,
        ScramMechanism::Scram512,
        ScramCredential {
            salt: Bytes::from_static(&[
                51, 107, 103, 48, 106, 108, 108, 119, 112, 101, 99, 53, 121, 118, 100, 50, 114, 99,
                52, 120, 122, 102, 110, 97, 98,
            ]),
            iterations: 8192,
            stored_key: Bytes::from_static(&[
                196, 134, 61, 247, 249, 195, 172, 31, 25, 47, 16, 203, 108, 147, 118, 10, 196, 207,
                147, 26, 36, 10, 101, 115, 52, 79, 87, 86, 200, 153, 131, 34, 102, 91, 132, 40, 85,
                78, 240, 50, 240, 152, 39, 87, 20, 230, 28, 185, 102, 114, 87, 182, 51, 83, 110,
                35, 77, 93, 186, 24, 204, 17, 121, 100,
            ]),
            server_key: Bytes::from_static(&[
                232, 156, 219, 209, 141, 82, 172, 239, 164, 247, 237, 158, 68, 216, 177, 230, 69,
                25, 238, 6, 99, 212, 9, 238, 103, 116, 42, 34, 119, 91, 207, 81, 77, 81, 235, 48,
                218, 126, 38, 209, 153, 135, 164, 8, 230, 100, 94, 19, 105, 222, 2, 57, 38, 42, 48,
                210, 161, 61, 146, 10, 7, 92, 100, 192,
            ]),
        },
    );

    let broker = nisshi_auth::configuration(engine.clone())
        .map_err(Into::into)
        .map(Some)
        .and_then(|sasl_config| broker(engine, sasl_config))?;

    const API_VERSION: i16 = 1;
    let mut correlation_id = 0;

    let ctx = Context::default();

    let response = broker
        .serve(
            ctx.clone(),
            Frame::request(
                Header::Request {
                    api_key: SaslHandshakeRequest::KEY,
                    api_version: API_VERSION,
                    correlation_id,
                    client_id: Some(CLIENT_ID.into()),
                },
                Body::SaslHandshakeRequest(
                    SaslHandshakeRequest::default().mechanism("SCRAM-SHA-512".into()),
                ),
            )?,
        )
        .await?;

    let response = Frame::response_from_bytes(response, SaslHandshakeResponse::KEY, API_VERSION)
        .and_then(|response| SaslHandshakeResponse::try_from(response.body))?;
    debug!(?response);

    let offered = response
        .mechanisms
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|mechanism| Mechname::parse(mechanism.as_bytes()).ok())
        .collect::<Vec<_>>();

    let sasl = SASLClient::new(
        SASLConfig::with_credentials(None, PRINCIPAL.into(), PASSWORD.into())
            .expect("sasl credential config"),
    );

    let mut session = sasl.start_suggested(&offered).unwrap();

    assert!(session.are_we_first());

    let mut input = None;

    loop {
        correlation_id += 1;
        let mut output = BytesMut::new().writer();

        match session.step(input.as_deref(), &mut output).unwrap() {
            State::Running => {
                let response = broker
                    .serve(
                        ctx.clone(),
                        Frame::request(
                            Header::Request {
                                api_key: SaslAuthenticateRequest::KEY,
                                api_version: API_VERSION,
                                correlation_id,
                                client_id: Some(CLIENT_ID.into()),
                            },
                            Body::SaslAuthenticateRequest(
                                SaslAuthenticateRequest::default()
                                    .auth_bytes(Bytes::from(output.into_inner())),
                            ),
                        )?,
                    )
                    .await?;

                let response = Frame::response_from_bytes(
                    response,
                    SaslAuthenticateResponse::KEY,
                    API_VERSION,
                )
                .and_then(|response| SaslAuthenticateResponse::try_from(response.body))?;
                debug!(?response);
                assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

                input = Some(response.auth_bytes);
            }
            State::Finished(message_sent) => {
                debug!(?message_sent);
                break;
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn auth_handshake_scram_512_bad_password_v1() -> Result<()> {
    let _guard = init_tracing()?;

    const PRINCIPAL: &str = "alice";
    const PASSWORD: &str = "not_secret";
    const CLIENT_ID: &str = "rdkafka";

    let engine = Engine::default().with_credential(
        PRINCIPAL,
        ScramMechanism::Scram512,
        ScramCredential {
            salt: Bytes::from_static(&[
                51, 107, 103, 48, 106, 108, 108, 119, 112, 101, 99, 53, 121, 118, 100, 50, 114, 99,
                52, 120, 122, 102, 110, 97, 98,
            ]),
            iterations: 8192,
            stored_key: Bytes::from_static(&[
                196, 134, 61, 247, 249, 195, 172, 31, 25, 47, 16, 203, 108, 147, 118, 10, 196, 207,
                147, 26, 36, 10, 101, 115, 52, 79, 87, 86, 200, 153, 131, 34, 102, 91, 132, 40, 85,
                78, 240, 50, 240, 152, 39, 87, 20, 230, 28, 185, 102, 114, 87, 182, 51, 83, 110,
                35, 77, 93, 186, 24, 204, 17, 121, 100,
            ]),
            server_key: Bytes::from_static(&[
                232, 156, 219, 209, 141, 82, 172, 239, 164, 247, 237, 158, 68, 216, 177, 230, 69,
                25, 238, 6, 99, 212, 9, 238, 103, 116, 42, 34, 119, 91, 207, 81, 77, 81, 235, 48,
                218, 126, 38, 209, 153, 135, 164, 8, 230, 100, 94, 19, 105, 222, 2, 57, 38, 42, 48,
                210, 161, 61, 146, 10, 7, 92, 100, 192,
            ]),
        },
    );

    let broker = nisshi_auth::configuration(engine.clone())
        .map_err(Into::into)
        .map(Some)
        .and_then(|sasl_config| broker(engine, sasl_config))?;

    const API_VERSION: i16 = 1;
    let mut correlation_id = 0;

    let ctx = Context::default();

    let response = broker
        .serve(
            ctx.clone(),
            Frame::request(
                Header::Request {
                    api_key: SaslHandshakeRequest::KEY,
                    api_version: API_VERSION,
                    correlation_id,
                    client_id: Some(CLIENT_ID.into()),
                },
                Body::SaslHandshakeRequest(
                    SaslHandshakeRequest::default().mechanism("SCRAM-SHA-512".into()),
                ),
            )?,
        )
        .await?;

    let response = Frame::response_from_bytes(response, SaslHandshakeResponse::KEY, API_VERSION)
        .and_then(|response| SaslHandshakeResponse::try_from(response.body))?;
    debug!(?response);

    let offered = response
        .mechanisms
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|mechanism| Mechname::parse(mechanism.as_bytes()).ok())
        .collect::<Vec<_>>();

    let sasl = SASLClient::new(
        SASLConfig::with_credentials(None, PRINCIPAL.into(), PASSWORD.into())
            .expect("sasl credential config"),
    );

    let mut session = sasl.start_suggested(&offered).unwrap();

    assert!(session.are_we_first());

    let mut input = None;

    loop {
        correlation_id += 1;
        let mut output = BytesMut::new().writer();

        match session
            .step(input.as_deref(), &mut output)
            .inspect(|response| debug!(?response))
            .inspect_err(|err| debug!(?err))
        {
            Ok(State::Running) => {
                let response = broker
                    .serve(
                        ctx.clone(),
                        Frame::request(
                            Header::Request {
                                api_key: SaslAuthenticateRequest::KEY,
                                api_version: API_VERSION,
                                correlation_id,
                                client_id: Some(CLIENT_ID.into()),
                            },
                            Body::SaslAuthenticateRequest(
                                SaslAuthenticateRequest::default()
                                    .auth_bytes(Bytes::from(output.into_inner())),
                            ),
                        )?,
                    )
                    .await?;

                let response = Frame::response_from_bytes(
                    response,
                    SaslAuthenticateResponse::KEY,
                    API_VERSION,
                )
                .and_then(|response| SaslAuthenticateResponse::try_from(response.body))?;
                debug!(?response);
                assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

                input = Some(response.auth_bytes);
            }

            Ok(State::Finished(message_sent)) => {
                debug!(?message_sent);
                unimplemented!()
            }

            Err(SessionError::MechanismError(_mechanism)) => break,

            Err(_) => unimplemented!(),
        }
    }

    Ok(())
}

#[tokio::test]
async fn not_authenticated() -> Result<()> {
    let _guard = init_tracing()?;

    let engine = Engine::default();

    let broker = nisshi_auth::configuration(engine.clone())
        .map_err(Into::into)
        .map(Some)
        .and_then(|sasl_config| broker(engine, sasl_config))?;

    const CLIENT_ID: &str = "client";
    const API_VERSION: i16 = 7;

    let ctx = Context::default();

    assert!(matches!(
        broker
            .serve(
                ctx.clone(),
                Frame::request(
                    Header::Request {
                        api_key: CreateTopicsRequest::KEY,
                        api_version: API_VERSION,
                        correlation_id: 1,
                        client_id: Some(CLIENT_ID.into()),
                    },
                    Body::CreateTopicsRequest(
                        CreateTopicsRequest::default()
                            .timeout_ms(30_000)
                            .validate_only(Some(false))
                            .topics(Some(
                                [CreatableTopic::default()
                                    .assignments(Some([].into()))
                                    .configs(Some([].into()))
                                    .name("abc".into())
                                    .num_partitions(6)
                                    .replication_factor(1)]
                                .into(),
                            )),
                    ),
                )?,
            )
            .await,
        Err(Error::KafkaProtocol(
            nisshi_sans_io::Error::NotAuthenticated
        )),
    ));

    Ok(())
}

#[tokio::test]
async fn auth_handshake_scram_256_v0() -> Result<()> {
    let _guard = init_tracing()?;

    const PRINCIPAL: &str = "alice";
    const PASSWORD: &str = "secret";
    const CLIENT_ID: &str = "rdkafka";

    let engine = Engine::default().with_credential(
        PRINCIPAL,
        ScramMechanism::Scram256,
        ScramCredential {
            salt: Bytes::from_static(&[
                107, 53, 50, 116, 97, 121, 49, 118, 118, 116, 105, 97, 53, 101, 54, 108, 99, 51,
                55, 103, 110, 51, 102, 51, 104,
            ]),
            iterations: 8192,
            stored_key: Bytes::from_static(&[
                150, 254, 7, 121, 81, 205, 192, 207, 60, 206, 251, 24, 31, 131, 31, 15, 96, 75, 20,
                228, 251, 132, 22, 235, 160, 72, 200, 130, 127, 49, 29, 150,
            ]),
            server_key: Bytes::from_static(&[
                186, 175, 253, 227, 176, 106, 88, 53, 186, 173, 104, 88, 94, 40, 115, 166, 44, 183,
                199, 177, 137, 41, 225, 132, 56, 32, 70, 255, 223, 209, 22, 146,
            ]),
        },
    );

    let broker = nisshi_auth::configuration(engine.clone())
        .map_err(Into::into)
        .map(Some)
        .and_then(|sasl_config| broker(engine, sasl_config))?;

    const API_VERSION: i16 = 0;

    let ctx = Context::default();

    let response = broker
        .serve(
            ctx.clone(),
            Frame::request(
                Header::Request {
                    api_key: SaslHandshakeRequest::KEY,
                    api_version: API_VERSION,
                    correlation_id: 1,
                    client_id: Some(CLIENT_ID.into()),
                },
                Body::SaslHandshakeRequest(
                    SaslHandshakeRequest::default().mechanism("SCRAM-SHA-256".into()),
                ),
            )?,
        )
        .await?;

    let response = Frame::response_from_bytes(response, SaslHandshakeResponse::KEY, API_VERSION)
        .and_then(|response| SaslHandshakeResponse::try_from(response.body))?;
    debug!(?response);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

    let offered = response
        .mechanisms
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|mechanism| Mechname::parse(mechanism.as_bytes()).ok())
        .collect::<Vec<_>>();

    let sasl = SASLClient::new(
        SASLConfig::with_credentials(None, PRINCIPAL.into(), PASSWORD.into())
            .expect("sasl credential config"),
    );

    let mut session = sasl.start_suggested(&offered).unwrap();

    assert!(session.are_we_first());

    let mut input = None;

    loop {
        let mut output = BytesMut::new().writer();

        match session
            .step(input.as_deref(), &mut output)
            .inspect(|response| debug!(?response))
            .inspect_err(|err| debug!(?err))
            .unwrap()
        {
            State::Running => {
                let auth_bytes = Bytes::from(output.into_inner());
                let frame = i32::try_from(auth_bytes.len()).map(|size| {
                    let mut frame = BytesMut::new();
                    frame.put(&size.to_be_bytes()[..]);
                    frame.put(auth_bytes);
                    Bytes::from(frame)
                })?;

                let response = broker.serve(ctx.clone(), frame).await?;

                input = Some(response.slice(4..));
            }

            State::Finished(message_sent) => {
                debug!(?message_sent);
                break;
            }
        }
    }

    Ok(())
}

#[derive(Clone, Debug, Default, Hash)]
struct Engine {
    credentials: BTreeMap<(String, ScramMechanism), ScramCredential>,
}

impl Engine {
    fn with_credential(
        mut self,
        user: impl Into<String>,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Self {
        _ = self
            .credentials
            .insert((user.into(), mechanism), credential);
        self
    }
}

#[async_trait]
impl Storage for Engine {
    #[instrument(skip_all)]
    async fn register_broker(
        &self,
        _broker_registration: BrokerRegistrationRequest,
    ) -> nisshi_storage::Result<()> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> nisshi_storage::Result<Vec<DescribeClusterBroker>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn create_topic(
        &self,
        _topic: CreatableTopic,
        _validate_only: bool,
    ) -> nisshi_storage::Result<Uuid> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        _topics: &[DeleteRecordsTopic],
    ) -> nisshi_storage::Result<Vec<DeleteRecordsTopicResult>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, _topic: &TopicId) -> nisshi_storage::Result<ErrorCode> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        _resource: AlterConfigsResource,
    ) -> nisshi_storage::Result<AlterConfigsResourceResponse> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        _transaction_id: Option<&str>,
        _topition: &Topition,
        _deflated: Batch,
    ) -> nisshi_storage::Result<i64> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        _topition: &Topition,
        _offset: i64,
        _min_bytes: u32,
        _max_bytes: u32,
        _isolation_level: IsolationLevel,
        _max_wait: Duration,
    ) -> nisshi_storage::Result<Vec<Batch>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, _topition: &Topition) -> nisshi_storage::Result<OffsetStage> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        _group: &str,
        _retention: Option<Duration>,
        _offsets: &[(Topition, OffsetCommitRequest)],
    ) -> nisshi_storage::Result<Vec<(Topition, ErrorCode)>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(
        &self,
        _group_id: &str,
    ) -> nisshi_storage::Result<BTreeMap<Topition, i64>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        _group_id: Option<&str>,
        _topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> nisshi_storage::Result<BTreeMap<Topition, i64>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        _isolation_level: IsolationLevel,
        _offsets: &[(Topition, ListOffset)],
    ) -> nisshi_storage::Result<Vec<(Topition, ListOffsetResponse)>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn metadata(
        &self,
        _topics: Option<&[TopicId]>,
    ) -> nisshi_storage::Result<MetadataResponse> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        _name: &str,
        _resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> nisshi_storage::Result<DescribeConfigsResult> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        _topics: Option<&[TopicId]>,
        _partition_limit: i32,
        _cursor: Option<Topition>,
    ) -> nisshi_storage::Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn list_groups(
        &self,
        _states_filter: Option<&[String]>,
    ) -> nisshi_storage::Result<Vec<ListedGroup>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        _group_ids: Option<&[String]>,
    ) -> nisshi_storage::Result<Vec<DeletableGroupResult>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        _group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> nisshi_storage::Result<Vec<NamedGroupDetail>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        _group_id: &str,
        _detail: GroupDetail,
        _version: Option<Version>,
    ) -> nisshi_storage::Result<Version, UpdateError<GroupDetail>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        _transaction_id: Option<&str>,
        _transaction_timeout_ms: i32,
        _producer_id: Option<i64>,
        _producer_epoch: Option<i16>,
    ) -> nisshi_storage::Result<ProducerIdResponse> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> nisshi_storage::Result<ErrorCode> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        _partitions: TxnAddPartitionsRequest,
    ) -> nisshi_storage::Result<TxnAddPartitionsResponse> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        _offsets: TxnOffsetCommitRequest,
    ) -> nisshi_storage::Result<Vec<TxnOffsetCommitResponseTopic>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _committed: bool,
    ) -> nisshi_storage::Result<ErrorCode> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn maintain(&self, _now: SystemTime) -> nisshi_storage::Result<()> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn maintain_transactions(&self, _now: SystemTime) -> nisshi_storage::Result<()> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn aborted_transactions(
        &self,
        _topition: &Topition,
        _offset: i64,
        _last_stable_offset: i64,
    ) -> nisshi_storage::Result<Vec<AbortedTransaction>> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> nisshi_storage::Result<String> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn node(&self) -> nisshi_storage::Result<i32> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> nisshi_storage::Result<Url> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> nisshi_storage::Result<()> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> nisshi_storage::Result<()> {
        unimplemented!()
    }

    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> nisshi_storage::Result<()> {
        debug!(user, ?mechanism, ?credential);
        unimplemented!()
    }

    #[instrument(fields(user, mechanism))]
    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> nisshi_storage::Result<Option<ScramCredential>> {
        debug!(user, ?mechanism);
        Ok(self.credentials.get(&(user.into(), mechanism)).cloned())
    }
}
