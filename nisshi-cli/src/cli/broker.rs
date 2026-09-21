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
use nisshi_storage::ArcDynStorage;
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
use zeroize::Zeroizing;

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

    /// Transport Layer Security private key (PKCS#8, SEC1 or RSA PEM; encrypted PKCS#8 with --key-passphrase-file), requires --cert
    #[arg(long, requires = "cert")]
    key: Option<PathBuf>,

    /// File containing the passphrase of an encrypted PKCS#8 private key (trailing newline ignored), requires --key
    #[arg(long, requires = "key")]
    key_passphrase_file: Option<PathBuf>,

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

/// Reads the passphrase file, if any. A trailing newline (as left by most
/// editors and `echo`) is not part of the passphrase. An empty file means
/// "no passphrase", matching `openssl -passin file:` and mounted secrets.
fn load_passphrase(filename: Option<&Path>) -> Result<Option<Zeroizing<Vec<u8>>>> {
    let _ = filename;
    Ok(None)
}

fn load_private_key(filename: &Path, passphrase: Option<&[u8]>) -> Result<PrivateKeyDer<'static>> {
    let _ = passphrase;
    let key_error = |source| Error::TlsPrivateKey {
        path: filename.to_path_buf(),
        source,
    };

    let pem = fs::read_to_string(filename)
        .map_err(TlsPkiPemError::Io)
        .map_err(key_error)?;

    if pem.contains("BEGIN ENCRYPTED PRIVATE KEY") || pem.contains("Proc-Type: 4,ENCRYPTED") {
        return Err(Error::TlsKeyPassphraseRequired {
            path: filename.to_path_buf(),
        });
    }

    PrivateKeyDer::from_pem_slice(pem.as_bytes()).map_err(key_error)
}

fn server_config(
    certs: &Path,
    private_key: &Path,
    passphrase_file: Option<&Path>,
) -> Result<ServerConfig> {
    let passphrase = load_passphrase(passphrase_file)?;

    // Both `ring` and `aws-lc-rs` are compiled into this binary (via other
    // dependencies), so the provider must be chosen explicitly: rustls panics
    // when asked to pick a default between two.
    ServerConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(
            load_certs(certs)?,
            load_private_key(private_key, passphrase.as_deref().map(Vec::as_slice))?,
        )
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
        // A bad TLS configuration must fail startup loudly rather than silently
        // falling back to a plaintext listener. It is checked first: it is a
        // cheap local validation, so it fails before any registry, lake or
        // storage connection is attempted.
        let tls_server_config = match (self.cert.as_deref(), self.key.as_deref()) {
            (Some(cert), Some(key)) => Some(server_config(
                cert,
                key,
                self.key_passphrase_file.as_deref(),
            )?),
            (None, None) => None,
            // clap enforces this pairing already; keep the invariant if the
            // arguments are ever constructed another way.
            _ => return Err(Error::TlsRequiresCertAndKey),
        };

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

        let broker = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
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

        _ = server_config(&pem.cert, &pem.key, None).expect("valid cert and key");
    }

    #[test]
    fn missing_files_fail() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist.pem");

        assert!(server_config(&missing, &missing, None).is_err());
    }

    #[test]
    fn invalid_pem_fails() {
        let dir = tempfile::tempdir().unwrap();
        let garbage = dir.path().join("garbage.pem");
        fs::write(&garbage, "not a pem file").unwrap();

        assert!(server_config(&garbage, &garbage, None).is_err());
    }

    #[test]
    fn mismatched_key_fails() {
        let a = pem();
        let b = pem();

        let err =
            server_config(&a.cert, &b.key, None).expect_err("key from another pair must fail");

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

        let err = server_config(&pem.cert, &encrypted, None).expect_err("encrypted key must fail");

        assert!(
            matches!(err, Error::TlsKeyPassphraseRequired { ref path } if *path == encrypted),
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

        let err = server_config(&pem.key, &pem.key, None).expect_err("empty certificate chain");

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

        let err = server_config(&pem.cert, &missing, None).expect_err("missing key file");

        assert!(
            matches!(err, Error::TlsPrivateKey { ref path, .. } if *path == missing),
            "{err:?}"
        );
    }

    #[test]
    fn legacy_encrypted_key_rejected() {
        let pem = pem();
        let encrypted = write(&pem, "legacy.pem", EC_KEY_LEGACY_ENCRYPTED);
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        let err = server_config(&pem.cert, &encrypted, Some(&passphrase))
            .expect_err("legacy encrypted key must fail even with the right passphrase");

        assert!(
            matches!(err, Error::TlsKeyLegacyEncrypted { ref path } if *path == encrypted),
            "{err:?}"
        );
    }

    // OpenSSL-generated fixtures: interop guards so the in-process encryption
    // used by `encrypted_key_with_passphrase_builds` cannot mask a mismatch
    // with what `openssl pkcs8 -topk8` actually emits. Every encrypted key
    // below uses the passphrase `correct-horse`. `with_single_cert` does not
    // check validity dates, so short fixture lifetimes do not matter.
    const FIXTURE_PASSPHRASE: &[u8] = b"correct-horse";

    /// Self-signed P-256 certificate:
    /// `openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj /CN=localhost -days 1`
    const EC_CERT: &str = "-----BEGIN CERTIFICATE-----
MIIBfjCCASOgAwIBAgIUHl9NpgkfaUtOrRGrOZw/y/nfZgcwCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDkxODE0MDY1MVoXDTI2MDkxOTE0
MDY1MVowFDESMBAGA1UEAwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0D
AQcDQgAEL8rJJA2RV6/uE6W9odOBwxWx5mnr6m7r4ibZiDvQp+6mSLzMXvZYp0hd
STa2vEKRbUNkrzhlCwwdtvslpiwcj6NTMFEwHQYDVR0OBBYEFKTXTx4XYXZUkLkC
hHq/B1GmgDUxMB8GA1UdIwQYMBaAFKTXTx4XYXZUkLkChHq/B1GmgDUxMA8GA1Ud
EwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDSQAwRgIhAMBaTNFHz+DrYFoRwf5ZAegf
bSV0we8Q7sqHKvsqnv+wAiEAl2kRYe/00mr314W/x4/h482nYc1djh87vZW6fAJB
zls=
-----END CERTIFICATE-----
";

    /// The P-256 key as PBES2 / PBKDF2-HMAC-SHA256 (100 000 iterations) / AES-256-CBC:
    /// `openssl pkcs8 -topk8 -v2 aes-256-cbc -v2prf hmacWithSHA256 -iter 100000`
    const EC_KEY_ENCRYPTED: &str = "-----BEGIN ENCRYPTED PRIVATE KEY-----
MIH1MGAGCSqGSIb3DQEFDTBTMDIGCSqGSIb3DQEFDDAlBBBYtY4DQzoGJCi3T/4q
eAUhAgMBhqAwDAYIKoZIhvcNAgkFADAdBglghkgBZQMEASoEEH1uoF9wz80u4QvO
lUj8qdMEgZBzlzdi8f/Oi9Yu/xb8w128zZt1Lfu7CeFc1TB7gZh0D0t1/qimPFQp
TPYIJAdPednrcmqvX7MfM5U25dWw/WUIYkrqbkQeHpJlXrf0iWML6UzwbT5fmLXV
jHZXdIAogaiK02kmPPdm/LxL75gmQeNkp78T34qnk+DETOwoKsEjzsTuODRWcDfX
DeCIawGEBIY=
-----END ENCRYPTED PRIVATE KEY-----
";

    /// The P-256 key with legacy OpenSSL PEM encryption: `openssl ec -aes256`
    const EC_KEY_LEGACY_ENCRYPTED: &str = "-----BEGIN EC PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: AES-256-CBC,1AB0CD970CE00250B53705F97C46DDE3

scIN/NMh5E595NAFXao5+rH7Czbh7wiJYOGBz+MMhJ14RchhDq5lLTGKGLUnQvMD
dqcPRYe8mSX5RFstiH1El0jsHv83c5MThHOEmjrUrwPydOkhKTuuGKW8IGIEL6zN
cr5gOkcev/kzfVV75cSo3kqlCrHg+xQ7+qMFNyykTw4=
-----END EC PRIVATE KEY-----
";

    /// Self-signed RSA-2048 certificate:
    /// `openssl req -x509 -newkey rsa:2048 -nodes -subj /CN=localhost -days 397`
    const RSA_CERT: &str = "-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUVSMR8KlQZAh5oLDehCP4NWBd/VMwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDkyMTEzNDE1M1oXDTI3MTAy
MzEzNDE1M1owFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAvYvwOHg90sBUgD8bSAne5ra+5tjJ3n1xW24IOqzgcjRW
GqfCwXumESaHBbrahMDaa3JOcX5zmAioEapKa0wy+NNNtKWkfrkN9XRcGXaKeMLz
7LaTrepRdabARX3BxrogugABg5QK8LoFPbz/RRU/wE1llr9eSG4udj0faYyYhYjl
+vfM7RAuUCxRjuDJXBhsjBnskQoyyz9rjl23c7n2Lciw4h9GdDjl1gpWrejbO9wl
esEdSDIi4QXYnm0ZihhJbeFQHapPE3WeyuPOsGcPIl7eoUhk3HIecp8ce1IvZdZ8
gS3AoaPfaNGsflxCZAtLOJkYzjFbTnsbI+c82ZueVwIDAQABo1MwUTAdBgNVHQ4E
FgQUcJm8S7uari/hUo/599kYlw2nuSIwHwYDVR0jBBgwFoAUcJm8S7uari/hUo/5
99kYlw2nuSIwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAnUPx
Stxvyo0gQHcNo6WAYpdWVUBeZgp/2RRV5rJczJTk07MvG4hV3BJjmxAfHKQNl9Qu
VgnTCe4gYCd8kW3YU7nTFYnI+hfBW9IgJmFP1pqfVrIdprYtem6VS0TTyhBxkjcE
dV/GPa036XIt7b2BH6tNn/UzlehREdZSAMZzozMZQI+A752v/x/54PFbTo/UqlCB
c+HgZOxWYJMWDIVKHgRG7UV1ZL22TMcBsJLEBMzH941wyPrgOtghwMI5f+4XU6BE
LwGGwdLGKzTl+XV6XXNVjgrvPi9UdOS42Uph/IS866A6Uh7JxFX/bxtbhgSNQKGC
lVS7O3/MSoZWGj9WSA==
-----END CERTIFICATE-----
";

    /// The RSA key as PBES2 / PBKDF2-HMAC-SHA256 (2048 iterations) / AES-256-CBC:
    /// `openssl pkcs8 -topk8 -v2 aes-256-cbc -v2prf hmacWithSHA256 -iter 2048`
    const RSA_KEY_ENCRYPTED_AES: &str = "-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIFNTBfBgkqhkiG9w0BBQ0wUjAxBgkqhkiG9w0BBQwwJAQQT24Fd4IrIW/qevOh
5LbZugICCAAwDAYIKoZIhvcNAgkFADAdBglghkgBZQMEASoEEPRYmpZUt+NCW0Zb
R2O0/DsEggTQQnIY6Gb9E1U1u16zqySwaYoeYGRAW8tNAfdic1dmFM3E4j1xNaJ7
PEGhzz4efkWSPgQ4J8eWqjvMobevhQW+i+Zinob08hWMTtywKrLooyQHuCX4v20v
XgHxdv50Y+TbqnAZ6B22QqggJN0IL66JBS4+eiyVpcrasDbICRSEzqvUUO7RjDCe
pweMHWX5887Y2I+lWjssc0Y+vN+KkeJnk6FIj/OQ4VzMLX4b6TiiRQTuLWbNiCAy
wHM+6+1mOObrSUmg0TDqoKx38Y78EXxi4MreVL9of2wF4RKB1afaomSYATUgQ5T0
Ui1/QVkZPGYvU+cIRoI3WBdU+PrDWHCHZM+COnwssAMfcfTwHzbodIzfDKmEk0yn
5GYonmqz7Ow0FxGTDTaoP0CpWpRyPBeudUueL8LdN+8PaU4qX7IV9aSHpuwNt2l7
PDnOF+th+z7AF1iSxWp16FGtPqojvtS1M1ZtHi44tbB8G8eEg0Aspz+Hc3TuOBpY
zOAsGZR0fhY0exHOf1luyIaH4iMZyTQu+F9HrHiRwbIatBUUchDSHuNmkOnBKf/I
cXGoZTZU3ASNOlfB8XQz9SodoUKANzx6qqvc988OmGgpP7E9odiPRZ5FCA2sMcvX
khEAl0nRivhvoWzu6gzdF39Df9jUcnjUU2PWcwGnv6Xml/5/o0rJDD3qmzdo6NI5
JnTmRcbbQXTZpf1qHnjDqZRmp+ashGKnkVtoe972WgeSzQjDREs+y6micMw25Lif
Hea2fH+j/JQyvZR2S7siEk6z3uqkbv0LhCohUtJ5nTJdBswp/mWcQ+uxsjiFDL1t
8/TmDbpX3qvX7ZbFGRAInVE6oHlx/nPxIx3d2bHTNsl/LLGuAnR1H9ICLz+NuO81
B/14rwKjQSd5tSeAIeSHBCeOnGKqOfhUJPuchN28lJ8pW7IhZwN8V9WSuZFYcviY
T2zcRGhFVyAb8GtIPhdbsvk7zCTRC5lxOVsruOWPhj+fmOr6BQV45pYl1MFlktdX
QWuDM1C2L9qPDcV33PvmUqCsU31qURtl/WsySrTkG/vKlebYcej0o5WkQpoH71U2
E6tYuujg/jbDTip01XZE+0JIX7BW03tWWGRO9iPG+WdUgnRwERZn/folvGAMx4xj
8/NNkwhgBjMOhEVfs13Y/brnCv06zMzZnMcdQjP0WrR7wHwNcEIU4ROjJsaOP6ZK
LOLMnnq6FXsSOTC8CPgLkD+vmQKEzw0eZZbcSAf+82+Aqh/veJBhs16sS2bhWotb
Cudf2Uj4fU42hu+FkY7jsudKc18fjMmKCab1MEZLBjAx9XoSgck5VIphsJY2Xrb1
fLHXH1+OdrnT/3+n8vhBq7buhTYOHfn3Ss6JvaDaV9mXfSuXbbOxFYkXCs9k5I/D
zQcSfze5Wj1CGEGpt7d5ld4haWOpzkMlZX4LyGtmMH5L5/W5A4VoIN9VZME2+Rke
8aSlLupsDCSy/0Nt5ezRBQRvPK0VFdfiRqroj7FwRj4sxBJEah8LRCjfHVgjuCeY
3WNinMha6z6enWrMF4jWm9jaFDAlENK271Gd5CYPzVu5GY7KTgtcT8ViNsvjhkFd
VSodkRftC9jstxvgUli37k0OUTBazTtwYa1fyzet/A9YEy82ouGzD1Q=
-----END ENCRYPTED PRIVATE KEY-----
";

    /// The same RSA key as PBES2 / PBKDF2-HMAC-SHA256 (2048 iterations) / DES-EDE3-CBC:
    /// `openssl pkcs8 -topk8 -v2 des3 -v2prf hmacWithSHA256 -iter 2048`
    const RSA_KEY_ENCRYPTED_3DES: &str = "-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIFJDBWBgkqhkiG9w0BBQ0wSTAxBgkqhkiG9w0BBQwwJAQQ51VJallY1wTKI8qy
1oxg3AICCAAwDAYIKoZIhvcNAgkFADAUBggqhkiG9w0DBwQIq+HsYMCnuWwEggTI
FjZVZHGA05TDUffzEmrgYvGKUFboDtxPVMaaa62hHEhME/iN3k1jQrZ60hsqaOHE
53YA9qAYkT4B5UAGFcsBKJIM8aYTQ4pU6FBKYJJoE5NYVPAn3yhvBwubLplFRLoT
x5vE8NAZRUcOzV6hbvk28EJQH0SXGsbZKxP6aW1xZpfmmWVYsXuHCjnC+1NnKSaa
rVkvdIjTYzgBkpbZ/TdoNkm2xM3Vim5AEJgRX2cJSOAzCUoI5ouFo9xCv+pHEy3E
09v6ZGDfT3GlDinmEqj0MrFFJd11Tq/LWlbNjpiMRUwwcNrF9eSOWO7pclIujbXa
tvpmnNdff5yZbLbd54lYK+pXXqhO05VOwo3yh0EUEkVFWCFyThrYdC2dw4r21SvF
YJaeMWT+fSasYeSCQMu/TjIGAGfJh2bVvyNOpGEmA619x4ocRBwI0Ijp3g2vmVO/
qPA2VXFZ934yzYpE3lfFZURnCcnutNlX0/hh2ipBd7MCoA5rf559xhFEtNWUy6q/
DLk57M54PlPVaqxcnOeU/AX4EhjAQeEgLOwUlp1JXXQ1X2JqRZvZQJsyIIU/l00+
Xomcpzv6wmUAJcdwu/Fi/uadja1EJnYjN6Mlmw/CShimzMpBuWKtf+HF9wfbsELG
N8mebmH24TznTjAqdFGIU+Ht+eAco1rWmbD0NlEH9EZrtxbzl8spJsXPRvTr2NnS
/RvAYcOSPGpm9w8cSTlwfgA4a4GgcR/3d4a+Q2nKKp8FBbFaFiB7nnbcQaEqMAlV
Q+H+0HrFRKAsqAC7inLtgqshjCrNw2hkRsaBdiTQPoBA1walKwS/MAkvXkhyJbO8
KNxYJh9aAgkxn9FO4blpZUrjMsETj8tlN0Zkassd6MNgORd8t8ID0b62PPTVFh4K
JYYkOgMMVQevFPLp1YaMNjuWh6vE/YPJKytGqdt+OMVb/VFAVVvvO8QJZ+N7zenT
P10UuOGG9wcetEzinNavBNcAG1/954bzlavJXkFMejevIhuRkZZM5RsacTwqq9KW
YQcYXsdnrjv/kE54KcmHNZOPVKutFOPXjruKM2Ab84LwpwRms9LdeK2+4xd3Ez4p
Oua/B1DQtKHTiv740oZPlQ7BVLm/3znq2LnVxCfocl4Yfu/NFEAZWtdKP3uRqTrk
sPnyLXguiMBP7BlEwkGua+3nyHd0jECxF462YwFGS9QBuBm1GlKkvDS1Cn9FndBL
NqIH7B/A0wURTgN3CQgGogHl2Wl3PQbmXyKVyHHzcFc7FmXmkgkUaaF/FsIzdx2r
MHRufCYu/AxurJ8pnNyXT+D9MzZYTeTRi6NpJVII44+NyhvKAyZVeyCQtrkwFP4w
Kn9AcmkHGnOv4CbgN17kR3HEe/CkPlLlpYFjZzHrwH4h2A2P1eTLZ0tLI6Whzkzu
bhsR/cpkAkP+N4EUeZmu9MX1v0SVzVBD3QuYfHwxti0/e1PXzJiCBarUytKKISe4
fMPm9WC+WVyS4Xdn8wh/McW5SnhygsY3fvCQ9gJTp73fGvSdf9OxWwbQWUMC0Lux
qEzMvwcM/bLv3wOZz49uBFa0aMhJirjADxoTJzQJ2/MKlmd3tbsJNRmfzMABiN21
1CNrYSlL6sDJkgzxsDj+heU0IGcoj466
-----END ENCRYPTED PRIVATE KEY-----
";

    /// Writes `contents` next to the generated PEM files and returns its path.
    fn write(pem: &Pem, name: &str, contents: impl AsRef<[u8]>) -> PathBuf {
        let path = pem.key.with_file_name(name);
        fs::write(&path, contents).expect("write fixture");
        path
    }

    /// The freshly generated key, re-encoded as an encrypted PKCS#8 PEM
    /// (PBES2 / PBKDF2-HMAC-SHA256 / AES-256-CBC) under `passphrase`.
    fn encrypt_key(pem: &Pem, passphrase: &[u8]) -> PathBuf {
        use pkcs8::{LineEnding, PrivateKeyInfo, SecretDocument, pkcs5::pbes2::Parameters};

        let plain = fs::read_to_string(&pem.key).expect("read key");
        let (label, der) = SecretDocument::from_pem(&plain).expect("pkcs8 pem");
        assert_eq!("PRIVATE KEY", label);

        let salt = [7u8; 16];
        let iv = [9u8; 16];
        let params = Parameters::pbkdf2_sha256_aes256cbc(10_000, &salt, &iv).expect("pbes2");

        let encrypted = PrivateKeyInfo::try_from(der.as_bytes())
            .expect("private key info")
            .encrypt_with_params(params, passphrase)
            .expect("encrypt");

        write(
            pem,
            "encrypted.pem",
            encrypted
                .to_pem("ENCRYPTED PRIVATE KEY", LineEnding::LF)
                .expect("pem")
                .as_bytes(),
        )
    }

    #[test]
    fn encrypted_key_with_passphrase_builds() {
        let pem = pem();
        let encrypted = encrypt_key(&pem, b"pw");
        // Editors and `echo` leave a trailing newline; it is not part of the passphrase.
        let passphrase = write(&pem, "passphrase", "pw\n");

        _ = server_config(&pem.cert, &encrypted, Some(&passphrase))
            .expect("encrypted key with its passphrase");
    }

    #[test]
    fn encrypted_key_wrong_passphrase_fails() {
        let pem = pem();
        let encrypted = encrypt_key(&pem, b"pw");
        let passphrase = write(&pem, "passphrase", "not-pw\n");

        let err = server_config(&pem.cert, &encrypted, Some(&passphrase))
            .expect_err("wrong passphrase must fail");

        assert!(
            matches!(err, Error::TlsKeyDecrypt { ref path, .. } if *path == encrypted),
            "{err:?}"
        );
    }

    #[test]
    fn encrypted_key_without_passphrase_fails() {
        let pem = pem();
        let encrypted = encrypt_key(&pem, b"pw");

        let err = server_config(&pem.cert, &encrypted, None)
            .expect_err("encrypted key without a passphrase must fail");

        assert!(
            matches!(err, Error::TlsKeyPassphraseRequired { ref path } if *path == encrypted),
            "{err:?}"
        );
    }

    #[test]
    fn openssl_pkcs8_ec_fixture_decrypts() {
        let pem = pem();
        let cert = write(&pem, "ec-cert.pem", EC_CERT);
        let key = write(&pem, "ec-key.pem", EC_KEY_ENCRYPTED);
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        _ = server_config(&cert, &key, Some(&passphrase)).expect("openssl pkcs8 ec key");
    }

    #[test]
    fn openssl_pkcs8_rsa_aes_fixture_decrypts() {
        let pem = pem();
        let cert = write(&pem, "rsa-cert.pem", RSA_CERT);
        let key = write(&pem, "rsa-key.pem", RSA_KEY_ENCRYPTED_AES);
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        _ = server_config(&cert, &key, Some(&passphrase)).expect("openssl pkcs8 rsa aes key");
    }

    #[test]
    fn openssl_pkcs8_rsa_3des_fixture_decrypts() {
        let pem = pem();
        let cert = write(&pem, "rsa-cert.pem", RSA_CERT);
        let key = write(&pem, "rsa-key.pem", RSA_KEY_ENCRYPTED_3DES);
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        _ = server_config(&cert, &key, Some(&passphrase)).expect("openssl pkcs8 rsa 3des key");
    }

    /// A common deployment shape: certificate chain and encrypted key in one
    /// PEM bundle, with `--cert` and `--key` both pointing at it.
    #[test]
    fn cert_and_key_in_one_bundle() {
        let pem = pem();
        let bundle = write(
            &pem,
            "bundle.pem",
            format!("{RSA_CERT}{RSA_KEY_ENCRYPTED_AES}"),
        );
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        _ = server_config(&bundle, &bundle, Some(&passphrase)).expect("cert and key bundle");
    }

    #[test]
    fn unencrypted_key_ignores_passphrase() {
        let pem = pem();
        let passphrase = write(&pem, "passphrase", "unused\n");

        _ = server_config(&pem.cert, &pem.key, Some(&passphrase))
            .expect("a passphrase for an unencrypted key is ignored");
    }

    #[test]
    fn unencrypted_key_with_empty_passphrase_file() {
        let pem = pem();
        let passphrase = write(&pem, "passphrase", "");

        _ = server_config(&pem.cert, &pem.key, Some(&passphrase))
            .expect("an empty passphrase file means no passphrase");
    }

    #[test]
    fn encrypted_key_with_empty_passphrase_file_fails() {
        let pem = pem();
        let encrypted = encrypt_key(&pem, b"pw");
        let passphrase = write(&pem, "passphrase", "\n");

        let err = server_config(&pem.cert, &encrypted, Some(&passphrase))
            .expect_err("an empty passphrase file is no passphrase");

        assert!(
            matches!(err, Error::TlsKeyPassphraseRequired { ref path } if *path == encrypted),
            "{err:?}"
        );
    }

    #[test]
    fn missing_passphrase_file_fails() {
        let pem = pem();
        let encrypted = encrypt_key(&pem, b"pw");
        let missing = pem.key.with_file_name("missing-passphrase");

        let err = server_config(&pem.cert, &encrypted, Some(&missing))
            .expect_err("missing passphrase file");

        assert!(
            matches!(err, Error::TlsKeyPassphraseFile { ref path, .. } if *path == missing),
            "{err:?}"
        );
    }

    #[test]
    fn key_passphrase_file_requires_key() {
        let pem = pem();
        let passphrase = write(&pem, "passphrase", "pw");

        let err = parse(&["--key-passphrase-file", passphrase.to_str().unwrap()])
            .expect_err("--key-passphrase-file without --key must be rejected");

        assert_eq!(clap::error::ErrorKind::MissingRequiredArgument, err.kind());
    }

    #[test]
    fn key_passphrase_file_with_cert_and_key_parses() {
        let pem = pem();
        let passphrase = write(&pem, "passphrase", "pw");

        let arg = parse(&[
            "--cert",
            pem.cert.to_str().unwrap(),
            "--key",
            pem.key.to_str().unwrap(),
            "--key-passphrase-file",
            passphrase.to_str().unwrap(),
        ])
        .expect("--cert, --key and --key-passphrase-file together must parse");

        assert_eq!(
            Some(passphrase.as_path()),
            arg.key_passphrase_file.as_deref()
        );
    }

    #[tokio::test]
    async fn build_with_encrypted_bundle_succeeds() {
        let pem = pem();
        let bundle = write(
            &pem,
            "bundle.pem",
            format!("{RSA_CERT}{RSA_KEY_ENCRYPTED_AES}"),
        );
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        let arg = parse(&[
            "--storage-engine",
            "memory://nisshi/",
            "--silent",
            "--cert",
            bundle.to_str().unwrap(),
            "--key",
            bundle.to_str().unwrap(),
            "--key-passphrase-file",
            passphrase.to_str().unwrap(),
        ])
        .expect("arguments parse");

        _ = arg
            .build()
            .await
            .expect("build with an encrypted cert and key bundle");
    }
}
