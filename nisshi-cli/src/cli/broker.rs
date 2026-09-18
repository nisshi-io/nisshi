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
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::{EnvVarExp, Error, Result, cli::storage_engines};

use super::DEFAULT_BROKER;
use clap::Parser;
use nisshi_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use nisshi_sans_io::ErrorCode;
use nisshi_schema::Registry;
use nisshi_storage::{ArcDynStorage, StorageContainer};
use owo_colors::{OwoColorize as _, Stream, Style};
use rustls::{
    ServerConfig,
    pki_types::{
        CertificateDer, PrivateKeyDer,
        pem::{Error as TlsPkiPemError, PemObject as _},
    },
};
use tokio::time::Instant;
use tracing::debug;
use url::Url;
use uuid::Uuid;

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use clap::Subcommand;

#[derive(Clone, Debug, Parser)]
pub(super) struct Arg {
    #[command(subcommand)]
    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    command: Option<Lake>,

    /// All members of the same cluster should use the same id
    #[arg(
        long,
        env = "CLUSTER_ID",
        default_value = "nisshi_cluster",
        visible_alias = "kafka-cluster-id"
    )]
    cluster_id: String,

    /// The broker will listen on this address
    #[arg(
        long,
        env = "LISTENER_URL",
        default_value = "tcp://0.0.0.0:9092",
        visible_alias = "kafka-listener-url"
    )]
    listener_url: EnvVarExp<Url>,

    /// This location is advertised to clients in metadata
    #[arg(
        long,
        env = "ADVERTISED_LISTENER_URL",
        default_value = DEFAULT_BROKER,
        visible_alias = "kafka-advertised-listener-url"
    )]
    advertised_listener_url: EnvVarExp<Url>,

    /// Storage engine examples are: postgres://postgres:postgres@localhost, memory://nisshi/ or s3://nisshi/
    #[arg(long, env = "STORAGE_ENGINE", default_value = "memory://nisshi/")]
    storage_engine: EnvVarExp<Url>,

    /// Schema registry examples are: file://./etc/schema or s3://nisshi/, containing: topic.json, topic.proto or topic.avsc
    #[arg(long, env = "SCHEMA_REGISTRY")]
    schema_registry: Option<EnvVarExp<Url>>,

    /// Schema registry cache expiry duration
    #[arg(long,value_parser = humantime::parse_duration)]
    schema_registry_cache_expiry: Option<Duration>,

    /// OTEL Exporter OTLP endpoint
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otlp_endpoint_url: Option<EnvVarExp<Url>>,

    /// When present, client authentication is required
    #[arg(long)]
    authentication: bool,

    /// Transport Layer Security certificate chain (PEM), requires --key.
    /// When present the listener only accepts TLS connections
    #[arg(long, requires = "key")]
    cert: Option<PathBuf>,

    /// Transport Layer Security private key (unencrypted PKCS#8, SEC1 or RSA PEM), requires --cert
    #[arg(long, requires = "cert")]
    key: Option<PathBuf>,

    /// Silent
    #[arg(long)]
    silent: bool,
}

fn load_certs(filename: &Path) -> Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(filename)
        .and_then(|der| der.collect::<Result<Vec<_>, TlsPkiPemError>>())
        .and_then(|certs| {
            // A file with no certificate blocks (garbage, or the key file by
            // mistake) iterates to an empty chain rather than an error.
            if certs.is_empty() {
                Err(TlsPkiPemError::NoItemsFound)
            } else {
                Ok(certs)
            }
        })
        .map_err(|source| Error::TlsCertificate {
            path: filename.to_path_buf(),
            source,
        })
}

fn load_private_key(filename: &Path) -> Result<PrivateKeyDer<'static>> {
    let key_error = |source| Error::TlsPrivateKey {
        path: filename.to_path_buf(),
        source,
    };

    // rustls' PEM loader has no passphrase support and would otherwise report
    // an encrypted key as "no private key found", so name the real problem.
    // Covers PKCS#8 (`ENCRYPTED PRIVATE KEY`) and legacy OpenSSL encrypted
    // PEM (`Proc-Type: 4,ENCRYPTED` header on an RSA/EC key).
    let pem = fs::read_to_string(filename)
        .map_err(TlsPkiPemError::Io)
        .map_err(key_error)?;

    if pem.contains("BEGIN ENCRYPTED PRIVATE KEY") || pem.contains("Proc-Type: 4,ENCRYPTED") {
        return Err(Error::EncryptedTlsKey(filename.to_path_buf()));
    }

    PrivateKeyDer::from_pem_slice(pem.as_bytes()).map_err(key_error)
}

fn server_config(certs: &Path, private_key: &Path) -> Result<ServerConfig> {
    // Both `ring` and `aws-lc-rs` are compiled into this binary (via other
    // dependencies), so the provider must be chosen explicitly: rustls panics
    // when asked to pick a default between two.
    ServerConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(load_certs(certs)?, load_private_key(private_key)?)
        .map_err(Into::into)
}

#[derive(Clone, Debug, Subcommand)]
#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
pub(super) enum Lake {
    /// Schema topics are written as Apache Iceberg tables
    #[cfg(feature = "iceberg")]
    Iceberg {
        /// Apache Parquet files are written to this location, examples are: file://./lake or s3://lake/
        #[arg(long, env = "DATA_LAKE")]
        location: EnvVarExp<Url>,

        /// Apache Iceberg Catalog, examples are: http://localhost:8181/
        #[arg(long, env = "ICEBERG_CATALOG")]
        catalog: EnvVarExp<Url>,

        /// Iceberg namespace
        #[arg(long, env = "ICEBERG_NAMESPACE", default_value = "nisshi")]
        namespace: Option<String>,

        /// Iceberg warehouse
        #[arg(long, env = "ICEBERG_WAREHOUSE")]
        warehouse: Option<String>,
    },

    /// Schema topics are written as Delta Lake tables
    #[cfg(feature = "delta")]
    Delta {
        /// Apache Parquet files are written to this location, examples are: file://./lake or s3://lake/
        #[arg(long, env = "DATA_LAKE")]
        location: EnvVarExp<Url>,

        /// Delta database
        #[arg(long, env = "DELTA_DATABASE", default_value = "nisshi")]
        database: Option<String>,

        /// Throttle the maximum number of records per second
        #[clap(long)]
        records_per_second: Option<u32>,
    },

    /// Schema topics are written in Parquet format
    #[cfg(feature = "parquet")]
    Parquet {
        /// Apache Parquet files are written to this location, examples are: file://./lake or s3://lake/
        #[arg(long, env = "DATA_LAKE")]
        location: EnvVarExp<Url>,
    },
}

fn redact_password(mut url: Url) -> Url {
    if url.password().is_some() {
        _ = url.set_password(None).ok();
    }

    url
}

impl Arg {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        let started = Instant::now();
        self.build()
            .await?
            .main(started)
            .await
            .inspect(|result| debug!(?result))
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    async fn build(self) -> Result<Broker<Controller<ArcDynStorage>, ArcDynStorage>> {
        let cluster_id = self.cluster_id;
        let incarnation_id = Uuid::now_v7();
        let otlp_endpoint_url = self
            .otlp_endpoint_url
            .map(|env_var_exp| env_var_exp.into_inner());

        let storage_engine = self.storage_engine.into_inner();
        let advertised_listener = self.advertised_listener_url.into_inner();
        let listener = self.listener_url.into_inner();

        let schema_registry_url = self
            .schema_registry
            .map(|env_var_exp| env_var_exp.into_inner());

        let schema_registry = schema_registry_url
            .clone()
            .map(|object_store| {
                Registry::builder_try_from_url(&object_store).map(|registry| {
                    registry
                        .with_cache_expiry_after(self.schema_registry_cache_expiry)
                        .build()
                })
            })
            .transpose()?;

        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        let lake_house = match self.command {
            #[cfg(feature = "iceberg")]
            Some(Lake::Iceberg {
                location,
                catalog,
                namespace,
                warehouse,
            }) => Some(
                nisshi_schema::lake::House::iceberg()
                    .location(location.into_inner())
                    .catalog(catalog.into_inner())
                    .schema_registry(schema_registry.clone().unwrap())
                    .namespace(namespace)
                    .warehouse(warehouse)
                    .build()
                    .await?,
            ),

            #[cfg(feature = "delta")]
            Some(Lake::Delta {
                location,
                database,
                records_per_second,
            }) => Some(
                nisshi_schema::lake::House::delta()
                    .location(location.into_inner())
                    .schema_registry(schema_registry.clone().unwrap())
                    .database(database)
                    .records_per_second(records_per_second)
                    .build()?,
            ),

            #[cfg(feature = "parquet")]
            Some(Lake::Parquet { location }) => Some(
                nisshi_schema::lake::House::parquet()
                    .location(location.into_inner())
                    .schema_registry(schema_registry.clone().unwrap())
                    .build()?,
            ),

            None => None,
        };

        // A bad TLS configuration must fail startup loudly rather than silently
        // falling back to a plaintext listener.
        let tls_server_config = match (self.cert.as_deref(), self.key.as_deref()) {
            (Some(cert), Some(key)) => Some(server_config(cert, key)?),
            (None, None) => None,
            // clap enforces this pairing already; keep the invariant if the
            // arguments are ever constructed another way.
            _ => return Err(Error::TlsRequiresCertAndKey),
        };

        let broker = Broker::<Controller<StorageContainer>, StorageContainer>::builder()
            .node_id(NODE_ID)
            .cluster_id(cluster_id)
            .incarnation_id(incarnation_id)
            .advertised_listener(advertised_listener.clone())
            .otlp_endpoint_url(otlp_endpoint_url)
            .schema_registry(schema_registry.clone())
            .storage(storage_engine.clone())
            .listener(listener.clone())
            .authentication(self.authentication)
            .tls_server_config(tls_server_config)
            .silent(self.silent);

        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        let broker = broker.lake_house(lake_house);

        if !self.silent {
            let sheet = Sheet::default();

            println!(
                "nisshi {} {}",
                "broker".if_supports_color(Stream::Stdout, |text| text.style(sheet.headline)),
                env!("CARGO_PKG_VERSION")
                    .if_supports_color(Stream::Stdout, |text| text.style(sheet.version))
            );

            println!(
                "listening on: {} (advertised: {})",
                listener.if_supports_color(Stream::Stdout, |text| text.style(sheet.listener)),
                advertised_listener.if_supports_color(Stream::Stdout, |text| text
                    .style(sheet.advertised_listener))
            );

            println!(
                "storage: {} {:?}",
                redact_password(storage_engine)
                    .if_supports_color(Stream::Stdout, |text| text.style(sheet.storage)),
                storage_engines()
                    .iter()
                    .map(|storage_engine| storage_engine
                        .if_supports_color(Stream::Stdout, |text| text.style(sheet.storage)))
                    .collect::<Vec<_>>()
            );

            if let Some(schema_registry) = schema_registry_url {
                println!(
                    "schema registry: {}",
                    schema_registry.if_supports_color(Stream::Stdout, |text| text
                        .style(sheet.schema_registry))
                );
            }

            if let Some(cert) = self.cert.as_deref() {
                println!(
                    "tls: {} ({})",
                    "enabled".if_supports_color(Stream::Stdout, |text| text.style(sheet.tls)),
                    cert.display()
                        .if_supports_color(Stream::Stdout, |text| text.style(sheet.tls))
                );
            }
        }

        broker.build().await.map_err(Into::into)
    }
}

struct Sheet {
    advertised_listener: Style,
    headline: Style,
    listener: Style,
    schema_registry: Style,
    storage: Style,
    tls: Style,
    version: Style,
}

impl Default for Sheet {
    fn default() -> Self {
        Self {
            advertised_listener: Style::new().magenta().bold(),
            headline: Style::new().green().bold(),
            listener: Style::new().magenta().bold(),
            schema_registry: Style::new().magenta().bold(),
            storage: Style::new().magenta().bold(),
            tls: Style::new().magenta().bold(),
            version: Style::new().magenta().bold(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use clap::Parser as _;
    use tempfile::TempDir;

    use super::*;
    use crate::Error;

    struct Pem {
        _dir: TempDir,
        cert: PathBuf,
        key: PathBuf,
    }

    /// A freshly generated self-signed certificate and its private key as PEM files.
    fn pem() -> Pem {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(["localhost".to_owned()]).expect("self-signed");

        let dir = tempfile::tempdir().expect("tempdir");
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");

        fs::write(&cert_path, cert.pem()).expect("write cert");
        fs::write(&key_path, signing_key.serialize_pem()).expect("write key");

        Pem {
            _dir: dir,
            cert: cert_path,
            key: key_path,
        }
    }

    fn parse(args: &[&str]) -> Result<Arg, clap::Error> {
        Arg::try_parse_from(std::iter::once("nisshi").chain(args.iter().copied()))
    }

    #[test]
    fn cert_requires_key() {
        let pem = pem();

        let err = parse(&["--cert", pem.cert.to_str().unwrap()])
            .expect_err("--cert without --key must be rejected");

        assert_eq!(clap::error::ErrorKind::MissingRequiredArgument, err.kind());
    }

    #[test]
    fn key_requires_cert() {
        let pem = pem();

        let err = parse(&["--key", pem.key.to_str().unwrap()])
            .expect_err("--key without --cert must be rejected");

        assert_eq!(clap::error::ErrorKind::MissingRequiredArgument, err.kind());
    }

    #[test]
    fn cert_and_key_together_parse() {
        let pem = pem();

        let arg = parse(&[
            "--cert",
            pem.cert.to_str().unwrap(),
            "--key",
            pem.key.to_str().unwrap(),
        ])
        .expect("--cert and --key together must parse");

        assert_eq!(Some(pem.cert.as_path()), arg.cert.as_deref());
        assert_eq!(Some(pem.key.as_path()), arg.key.as_deref());
    }

    #[test]
    fn valid_pem_builds_server_config() {
        let pem = pem();

        _ = server_config(&pem.cert, &pem.key).expect("valid cert and key");
    }

    #[test]
    fn missing_files_fail() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist.pem");

        assert!(server_config(&missing, &missing).is_err());
    }

    #[test]
    fn invalid_pem_fails() {
        let dir = tempfile::tempdir().unwrap();
        let garbage = dir.path().join("garbage.pem");
        fs::write(&garbage, "not a pem file").unwrap();

        assert!(server_config(&garbage, &garbage).is_err());
    }

    #[test]
    fn mismatched_key_fails() {
        let a = pem();
        let b = pem();

        let err = server_config(&a.cert, &b.key).expect_err("key from another pair must fail");

        assert!(matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[test]
    fn encrypted_key_rejected() {
        let pem = pem();
        let encrypted = pem.key.with_file_name("encrypted.pem");
        fs::write(
            &encrypted,
            "-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIBvTBXBgkqhkiG9w0BBQ0wSjApBgkqhkiG9w0BBQwwHAQI\n-----END ENCRYPTED PRIVATE KEY-----\n",
        )
        .unwrap();

        let err = server_config(&pem.cert, &encrypted).expect_err("encrypted key must fail");

        assert!(
            matches!(err, Error::EncryptedTlsKey(ref path) if *path == encrypted),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn build_fails_on_bad_tls() {
        let dir = tempfile::tempdir().unwrap();
        let garbage = dir.path().join("garbage.pem");
        fs::write(&garbage, "not a pem file").unwrap();

        let arg = parse(&[
            "--storage-engine",
            "memory://nisshi/",
            "--silent",
            "--cert",
            garbage.to_str().unwrap(),
            "--key",
            garbage.to_str().unwrap(),
        ])
        .expect("arguments parse");

        let err = arg
            .build()
            .await
            .expect_err("build must fail with an unreadable certificate");

        assert!(
            matches!(err, Error::TlsCertificate { ref path, .. } if *path == garbage),
            "expected the certificate error naming its path before anything else, got {err:?}"
        );
    }

    /// A common operator slip: pointing `--cert` at the key file yields an
    /// empty certificate chain, which must be rejected rather than served.
    #[test]
    fn key_file_as_cert_fails() {
        let pem = pem();

        let err = server_config(&pem.key, &pem.key).expect_err("empty certificate chain");

        assert!(
            matches!(
                err,
                Error::TlsCertificate {
                    ref path,
                    source: TlsPkiPemError::NoItemsFound
                } if *path == pem.key
            ),
            "{err:?}"
        );
    }

    #[test]
    fn missing_key_names_its_path() {
        let pem = pem();
        let missing = pem.key.with_file_name("missing.pem");

        let err = server_config(&pem.cert, &missing).expect_err("missing key file");

        assert!(
            matches!(err, Error::TlsPrivateKey { ref path, .. } if *path == missing),
            "{err:?}"
        );
    }

    #[test]
    fn legacy_encrypted_key_rejected() {
        let pem = pem();
        let encrypted = pem.key.with_file_name("legacy.pem");
        fs::write(
            &encrypted,
            "-----BEGIN EC PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-256-CBC,0102030405060708090A0B0C0D0E0F10\n\nMIIB\n-----END EC PRIVATE KEY-----\n",
        )
        .unwrap();

        let err = server_config(&pem.cert, &encrypted).expect_err("encrypted key must fail");

        assert!(
            matches!(err, Error::EncryptedTlsKey(ref path) if *path == encrypted),
            "{err:?}"
        );
    }
}
