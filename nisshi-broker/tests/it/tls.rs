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

//! Transport level tests for the broker listener: with a TLS configuration
//! every accepted connection must complete a TLS handshake before any Kafka
//! frame is read, and without one the listener stays plain TCP.
//!
//! TLS is independent of the storage backend, so only the memory backend is used.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow};
use bytes::Bytes;
use nisshi_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use nisshi_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, Body, Frame, Header, MetadataRequest,
    MetadataResponse,
};
use nisshi_storage::ArcDynStorage;
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use tokio_rustls::TlsConnector;
use tracing::debug;
use url::Url;
use uuid::Uuid;

use crate::common::init_tracing;

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Largest response the test client will read; anything bigger is not a
/// Kafka frame (a TLS alert read as a length prefix decodes to ~350 MiB).
const MAXIMUM_RESPONSE: usize = 16 * 1024 * 1024;

struct CertifiedKey {
    _dir: TempDir,
    cert: PathBuf,
    key: PathBuf,
}

/// A self-signed certificate for `localhost`, written as PEM into a temporary directory.
fn certified_key() -> Result<CertifiedKey> {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(["localhost".to_owned(), "127.0.0.1".to_owned()])?;

    let dir = tempfile::tempdir()?;
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");

    std::fs::write(&cert_path, cert.pem())?;
    std::fs::write(&key_path, signing_key.serialize_pem())?;

    Ok(CertifiedKey {
        _dir: dir,
        cert: cert_path,
        key: key_path,
    })
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

fn server_config_from_pem(cert: &Path, key: &Path) -> Result<ServerConfig> {
    let certs = CertificateDer::pem_file_iter(cert)?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(key)?;

    ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(Into::into)
}

fn client_config_trusting(cert: &Path) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();

    for cert in CertificateDer::pem_file_iter(cert)? {
        roots.add(cert?)?;
    }

    ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map(|config| config.with_root_certificates(roots))
        .map(|config| config.with_no_client_auth())
        .map_err(Into::into)
}

async fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

struct RunningBroker {
    handle: JoinHandle<()>,
    addr: SocketAddr,
}

impl Drop for RunningBroker {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn spawn_broker(tls: Option<ServerConfig>) -> Result<RunningBroker> {
    let port = free_port().await?;
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = Url::parse(&format!("tcp://{addr}"))?;

    let mut broker = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(format!("tls-{}", Uuid::now_v7()))
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(listener.clone())
        .storage(Url::parse("memory://")?)
        .listener(listener)
        .tls_server_config(tls)
        .silent(true)
        .build()
        .await?;

    let handle = tokio::spawn(async move {
        if let Err(err) = broker.serve(Instant::now()).await {
            debug!(?err);
        }
    });

    // Wait until the listener accepts connections.
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        match TcpStream::connect(addr).await {
            Ok(_) => break,
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            Err(err) => return Err(anyhow!("broker did not start listening on {addr}: {err}")),
        }
    }

    Ok(RunningBroker { handle, addr })
}

/// Send one Kafka request frame and read back the response frame, over any byte stream.
async fn round_trip<S>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: Body,
) -> Result<Frame>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = Frame::request(
        Header::Request {
            api_key,
            api_version,
            correlation_id,
            client_id: Some(env!("CARGO_PKG_NAME").into()),
        },
        body,
    )?;

    stream.write_all(&request).await?;
    stream.flush().await?;

    let mut size = [0u8; 4];
    _ = stream.read_exact(&mut size).await?;

    let length = usize::try_from(i32::from_be_bytes(size))
        .ok()
        .filter(|length| *length <= MAXIMUM_RESPONSE)
        .ok_or_else(|| anyhow!("not a kafka frame length prefix: {size:02x?}"))?;

    let mut buffer = vec![0u8; length + size.len()];
    buffer[..size.len()].copy_from_slice(&size);
    _ = stream.read_exact(&mut buffer[size.len()..]).await?;

    Frame::response_from_bytes(Bytes::from(buffer), api_key, api_version).map_err(Into::into)
}

async fn api_versions<S>(stream: &mut S) -> Result<ApiVersionsResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = round_trip(
        stream,
        ApiVersionsRequest::KEY,
        3,
        1,
        ApiVersionsRequest::default()
            .client_software_name(Some(env!("CARGO_PKG_NAME").into()))
            .client_software_version(Some(env!("CARGO_PKG_VERSION").into()))
            .into(),
    )
    .await?;

    ApiVersionsResponse::try_from(frame.body).map_err(Into::into)
}

async fn metadata<S>(stream: &mut S) -> Result<MetadataResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = round_trip(
        stream,
        MetadataRequest::KEY,
        12,
        2,
        MetadataRequest::default()
            .topics(Some([].into()))
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )
    .await?;

    MetadataResponse::try_from(frame.body).map_err(Into::into)
}

/// Without a TLS configuration the listener keeps speaking plain TCP.
#[tokio::test]
async fn plaintext_listener_without_tls() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;

        let mut stream = TcpStream::connect(broker.addr).await?;
        let response = api_versions(&mut stream).await?;

        assert!(
            !response.api_keys.unwrap_or_default().is_empty(),
            "expected a non-empty api_keys list over plaintext"
        );

        Ok(())
    })
    .await?
}

/// With a TLS configuration a TLS client completes the handshake and Kafka
/// requests are served over the encrypted stream.
#[tokio::test]
async fn tls_handshake_and_kafka_request() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let certified = certified_key()?;
        let server = server_config_from_pem(&certified.cert, &certified.key)?;
        let broker = spawn_broker(Some(server)).await?;

        let connector = TlsConnector::from(Arc::new(client_config_trusting(&certified.cert)?));
        let tcp = TcpStream::connect(broker.addr).await?;
        let mut tls = connector
            .connect(ServerName::try_from("localhost")?, tcp)
            .await
            .context("tls handshake with the broker failed")?;

        let response = api_versions(&mut tls).await?;
        assert!(
            !response.api_keys.unwrap_or_default().is_empty(),
            "expected a non-empty api_keys list over tls"
        );

        let response = metadata(&mut tls).await?;
        assert_eq!(Some(NODE_ID), response.controller_id);
        assert_eq!(
            1,
            response.brokers.unwrap_or_default().len(),
            "expected the single broker in metadata over tls"
        );

        Ok(())
    })
    .await?
}

/// With a TLS configuration a plaintext client must not get a Kafka response:
/// the listener is TLS only, so the handshake fails and the connection closes.
#[tokio::test]
async fn plaintext_rejected_on_tls_listener() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let certified = certified_key()?;
        let server = server_config_from_pem(&certified.cert, &certified.key)?;
        let broker = spawn_broker(Some(server)).await?;

        let mut stream = TcpStream::connect(broker.addr).await?;
        let outcome = api_versions(&mut stream).await;

        assert!(
            outcome.is_err(),
            "plaintext request on a tls listener must fail, got {outcome:?}"
        );

        // The rejected connection must not have taken the listener with it:
        // a proper TLS client is still served afterwards.
        let connector = TlsConnector::from(Arc::new(client_config_trusting(&certified.cert)?));
        let tcp = TcpStream::connect(broker.addr).await?;
        let mut tls = connector
            .connect(ServerName::try_from("localhost")?, tcp)
            .await
            .context("tls handshake after a rejected plaintext peer failed")?;

        let response = api_versions(&mut tls).await?;
        assert!(!response.api_keys.unwrap_or_default().is_empty());

        Ok(())
    })
    .await?
}
