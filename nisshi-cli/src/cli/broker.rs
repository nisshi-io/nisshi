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
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{EnvVarExp, Result, cli::storage_engines};

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

    /// Transport Layer Security Certificate
    #[arg(group = "tls", long)]
    cert: Option<PathBuf>,

    /// Transport Layer Security Key
    #[arg(group = "tls", long)]
    key: Option<PathBuf>,

    /// Silent
    #[arg(long)]
    silent: bool,
}

fn load_certs(filename: &Path) -> Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(filename)
        .and_then(|der| der.collect::<Result<Vec<_>, TlsPkiPemError>>())
        .map_err(Into::into)
}

fn load_private_key(filename: &Path) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(filename).map_err(Into::into)
}

fn server_config(certs: &Path, private_key: &Path) -> Result<ServerConfig> {
    ServerConfig::builder()
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

        let tls_server_config = self
            .cert
            .and_then(|certs| self.key.and_then(|key| server_config(&certs, &key).ok()));

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
            version: Style::new().magenta().bold(),
        }
    }
}
