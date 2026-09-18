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

pub mod group;

use crate::{
    CancelKind, Error, Result,
    coordinator::group::{Coordinator, administrator::Controller},
    otel,
    service::services,
};
use console::Term;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use nisshi_sans_io::{ErrorCode, RootMessageMeta};
use nisshi_schema::{Registry, lake::House};
use nisshi_storage::{ArcDynStorage, BrokerRegistrationRequest, Storage, StorageContainer};
use rama::{Context, Service};
use rsasl::config::SASLConfig;
use rustls::ServerConfig;
use std::{
    io::ErrorKind,
    marker::PhantomData,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
    task::{AbortHandle, JoinSet},
    time::{self, Instant, sleep},
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Level, debug, error, span};
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Broker<G, S> {
    node_id: i32,
    cluster_id: String,
    incarnation_id: Uuid,
    listener: Url,
    advertised_listener: Url,
    storage: S,
    groups: G,

    sasl_config: Option<Arc<SASLConfig>>,
    tls_server_config: Option<Arc<ServerConfig>>,
    silent: bool,
    maintenance_interval: Option<Duration>,
    transaction_maintenance_interval: Option<Duration>,

    cancellation: CancellationToken,
}

impl<G, S> Broker<G, S>
where
    G: Coordinator,
    S: Storage + Clone + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: i32,
        cluster_id: &str,
        listener: Url,
        advertised_listener: Url,
        storage: S,
        groups: G,
        incarnation_id: Uuid,
    ) -> Self {
        Self {
            node_id,
            cluster_id: cluster_id.to_owned(),
            incarnation_id,
            listener,
            advertised_listener,
            storage,
            groups,

            sasl_config: None,
            tls_server_config: None,

            silent: false,

            maintenance_interval: None,
            transaction_maintenance_interval: None,

            cancellation: CancellationToken::new(),
        }
    }

    pub fn builder() -> PhantomBuilder {
        Builder::default()
    }

    pub async fn main(mut self, started: Instant) -> Result<ErrorCode> {
        {
            let root_meta = RootMessageMeta::messages();
            debug!(
                messages = root_meta
                    .requests()
                    .values()
                    .map(|meta| meta.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        let mut set = JoinSet::new();

        let mut interrupt_signal = signal(SignalKind::interrupt()).unwrap();
        debug!(?interrupt_signal);

        let mut terminate_signal = signal(SignalKind::terminate()).unwrap();
        debug!(?terminate_signal);

        let silent = self.silent;

        let token = self.cancellation.clone();

        _ = set.spawn(async move {
            self.serve(started)
                .await
                .inspect_err(|err| error!(?err))
                .unwrap();
        });

        let kind = tokio::select! {
            v = set.join_next() => {
                debug!(?v);
                None
            }

            interrupt = interrupt_signal.recv() => {
                debug!(?interrupt);
                Some(CancelKind::Interrupt)
            }

            terminate = terminate_signal.recv() => {
                debug!(?terminate);
                Some(CancelKind::Terminate)
            }
        };

        if let Some(kind) = kind {
            token.cancel();

            let cleanup = async {
                while !set.is_empty() {
                    debug!(len = set.len());

                    _ = set.join_next().await;
                }
            };

            let patience = sleep(Duration::from(kind));

            tokio::select! {
                v = cleanup => {
                    debug!(?v)
                }

                _ = patience => {
                    debug!(aborting = set.len());
                    set.abort_all();

                    while !set.is_empty() {
                        _ = set.join_next().await;
                    }
                }
            }

            if !silent {
                let stdout = Term::stdout();

                if stdout.is_term() {
                    _ = stdout.clear_screen().ok();
                }
            }
        }

        Ok(ErrorCode::None)
    }

    pub async fn serve(&mut self, started: Instant) -> Result<()> {
        self.register().await?;
        self.listen(started).await
    }

    pub async fn register(&mut self) -> Result<()> {
        self.storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: self.node_id,
                cluster_id: self.cluster_id.clone(),
                incarnation_id: self.incarnation_id,
                rack: None,
            })
            .await
            .map_err(Into::into)
    }

    pub async fn listen(&self, started: Instant) -> Result<()> {
        debug!(%self.listener, %self.advertised_listener);

        let listener = TcpListener::bind(self.listener.host().map_or_else(
            || {
                SocketAddr::from((
                    IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                    self.listener.port().unwrap_or(9092),
                ))
            },
            |host| {
                let port = self.listener.port().unwrap_or(9092);
                debug!(?host, port);

                match host {
                    url::Host::Domain(domain) => SocketAddr::from_str(&format!("{domain}:{port}"))
                        .unwrap_or(SocketAddr::from((IpAddr::V6(Ipv6Addr::UNSPECIFIED), port))),
                    url::Host::Ipv4(ipv4_addr) => SocketAddr::from((IpAddr::V4(ipv4_addr), port)),
                    url::Host::Ipv6(ipv6_addr) => SocketAddr::from((IpAddr::V6(ipv6_addr), port)),
                }
            },
        ))
        .await
        .inspect(|listener| debug!(listener = ?listener.local_addr().ok()))
        .inspect_err(|err| error!(?err, %self.advertised_listener))?;

        let mut interval =
            time::interval(self.maintenance_interval.unwrap_or(Duration::from_mins(10)));

        let mut txn_interval = time::interval(
            self.transaction_maintenance_interval
                .unwrap_or(Duration::from_secs(10)),
        );

        // Periodic sweeps: skip missed ticks rather than firing them in a burst.
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        txn_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        // Track the in-flight sweep per interval so a sweep slower than its
        // interval can't overlap itself or pile up: skip the tick while the
        // previous sweep is still running.
        let mut maintenance_sweep: Option<AbortHandle> = None;
        let mut txn_sweep: Option<AbortHandle> = None;

        let mut set = JoinSet::new();

        let m = MultiProgress::new();

        let spinner_style = ProgressStyle::with_template("{prefix:.bold.dim} {spinner} {msg}")
            .unwrap()
            .tick_chars("⠁⠂⠄⡀⢀⠠⠐");

        let ls = if self.silent {
            None
        } else {
            println!("ready in {}ms", started.elapsed().as_millis(),);

            let ls = m.add(ProgressBar::new_spinner());
            ls.set_style(spinner_style.clone());

            if let Ok(local_addr) = listener.local_addr() {
                ls.set_prefix(format!("[{local_addr:?}]"));
            }

            ls.set_message("listening for connection...");

            Some(ls)
        };

        let _acceptor = self.tls_server_config.clone().map(TlsAcceptor::from);

        let mut connections = 0;

        loop {
            connections += 1;

            if let Some(ref ls) = ls {
                ls.tick();
            }

            tokio::select! {
                Ok((stream, addr)) = listener.accept() => {

                    let mut c = Context::default();

                    let pb = if self.silent {
                        None
                    } else {
                        let pb = m.add(ProgressBar::new_spinner());
                        pb.set_style(spinner_style.clone());
                        pb.set_prefix(format!("[{connections}/{:?}]", addr));
                        pb.set_message("connected");
                        pb.tick();

                        _ = c.insert(pb.clone());
                        Some(pb)
                    };


                    stream.set_nodelay(true)?;

                    let service = services(
                        self.cluster_id.as_str(),
                        self.groups.clone(),
                        self.storage.clone(),
                        self.sasl_config.clone()
                    )?;

                    let handle = set.spawn(async move {
                            match service.serve(c, stream).await {
                                Err(Error::Io(ref io))
                                    if io.kind() == ErrorKind::UnexpectedEof
                                        || io.kind() == ErrorKind::BrokenPipe
                                        || io.kind() == ErrorKind::ConnectionReset => {}

                                Err(error) => {
                                    error!(?error);
                                },

                                Ok(response) => {
                                    debug!(?response)
                                }
                        }

                        if let Some(ref pb) = pb {
                            pb.finish_and_clear();
                        }
                    });


                    debug!(?handle);

                    continue;
                }

                _ = interval.tick() => {
                    if maintenance_sweep.as_ref().is_some_and(|handle| !handle.is_finished()) {
                        debug!("maintain still in flight; skipping tick");
                    } else {
                        let storage = self.storage.clone();

                        let handle = set.spawn(async move {
                            let span = span!(Level::DEBUG, "maintenance");

                            async move {
                                if let Err(err) = storage.maintain(SystemTime::now()).await {
                                    debug!(?err);
                                }
                            }
                            .instrument(span)
                            .await
                        });

                        debug!(?handle);
                        maintenance_sweep = Some(handle);
                    }
                }

                _ = txn_interval.tick() => {
                    if txn_sweep.as_ref().is_some_and(|handle| !handle.is_finished()) {
                        debug!("maintain_transactions still in flight; skipping tick");
                    } else {
                        let storage = self.storage.clone();

                        let handle = set.spawn(async move {
                            let span = span!(Level::DEBUG, "maintain_transactions");

                            async move {
                                if let Err(err) = storage.maintain_transactions(SystemTime::now()).await {
                                    debug!(?err);
                                }
                            }
                            .instrument(span)
                            .await
                        });

                        debug!(?handle);
                        txn_sweep = Some(handle);
                    }
                }

                v = set.join_next(), if !set.is_empty() => {
                    debug!(?v);
                }

                message = self.cancellation.cancelled() => {
                    debug!(?message);
                    break;
                }
            }
        }

        while !set.is_empty() {
            debug!(len = set.len());

            _ = set.join_next().await;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct Builder<N, C, I, A, S, L> {
    node_id: N,
    cluster_id: C,
    incarnation_id: I,
    advertised_listener: A,
    storage: S,
    listener: L,
    otlp_endpoint_url: Option<Url>,
    schema_registry: Option<Registry>,
    lake_house: Option<House>,
    authentication: bool,
    tls_server_config: Option<ServerConfig>,
    silent: bool,
    maintenance_interval: Option<Duration>,
    transaction_maintenance_interval: Option<Duration>,

    cancellation: CancellationToken,
}

type PhantomBuilder = Builder<
    PhantomData<i32>,
    PhantomData<String>,
    PhantomData<Uuid>,
    PhantomData<Url>,
    PhantomData<Url>,
    PhantomData<Url>,
>;

impl<N, C, I, A, S, L> Builder<N, C, I, A, S, L> {
    const MAINTENANCE_INTERVAL: &str = "maintenance_interval";
    const TRANSACTION_MAINTENANCE_INTERVAL: &str = "transaction_maintenance_interval";

    pub fn node_id(self, node_id: i32) -> Builder<i32, C, I, A, S, L> {
        Builder {
            node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance_interval: self.maintenance_interval,
            transaction_maintenance_interval: self.transaction_maintenance_interval,
            cancellation: self.cancellation,
        }
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> Builder<N, String, I, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: cluster_id.into(),
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance_interval: self.maintenance_interval,
            transaction_maintenance_interval: self.transaction_maintenance_interval,

            cancellation: self.cancellation,
        }
    }

    pub fn incarnation_id(self, incarnation_id: impl Into<Uuid>) -> Builder<N, C, Uuid, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: incarnation_id.into(),
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance_interval: self.maintenance_interval,
            transaction_maintenance_interval: self.transaction_maintenance_interval,

            cancellation: self.cancellation,
        }
    }

    pub fn advertised_listener(
        self,
        advertised_listener: impl Into<Url>,
    ) -> Builder<N, C, I, Url, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: advertised_listener.into(),
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance_interval: self.maintenance_interval,
            transaction_maintenance_interval: self.transaction_maintenance_interval,

            cancellation: self.cancellation,
        }
    }

    pub fn storage(self, mut storage: Url) -> Builder<N, C, I, A, Url, L> {
        let maintenance_interval = storage.query_pairs().find_map(|(k, v)| {
            if k == Self::MAINTENANCE_INTERVAL {
                v.parse::<humantime::Duration>().map(Into::into).ok()
            } else {
                None
            }
        });

        let transaction_maintenance_interval = storage.query_pairs().find_map(|(k, v)| {
            if k == Self::TRANSACTION_MAINTENANCE_INTERVAL {
                v.parse::<humantime::Duration>().map(Into::into).ok()
            } else {
                None
            }
        });

        let pairs = storage
            .query_pairs()
            .filter_map(|(k, v)| {
                if k == Self::MAINTENANCE_INTERVAL || k == Self::TRANSACTION_MAINTENANCE_INTERVAL {
                    None
                } else {
                    Some((k.to_string(), v.to_string()))
                }
            })
            .collect::<Vec<_>>();

        if pairs.is_empty() {
            storage.set_query(None);
        } else {
            _ = storage.query_pairs_mut().clear().extend_pairs(pairs);
        }

        debug!(?maintenance_interval, ?transaction_maintenance_interval, %storage);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance_interval,
            transaction_maintenance_interval,

            cancellation: self.cancellation,
        }
    }

    pub fn listener(self, listener: Url) -> Builder<N, C, I, A, S, Url> {
        debug!(%listener);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance_interval: self.maintenance_interval,
            transaction_maintenance_interval: self.transaction_maintenance_interval,

            cancellation: self.cancellation,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Registry>) -> Self {
        Self {
            schema_registry,
            ..self
        }
    }

    pub fn lake_house(self, lake_house: Option<House>) -> Self {
        _ = lake_house
            .as_ref()
            .inspect(|lake_house| debug!(?lake_house));

        Self { lake_house, ..self }
    }

    pub fn otlp_endpoint_url(self, otlp_endpoint_url: Option<Url>) -> Self {
        Self {
            otlp_endpoint_url,
            ..self
        }
    }

    pub fn authentication(self, authentication: bool) -> Self {
        Self {
            authentication,
            ..self
        }
    }

    pub fn tls_server_config(self, tls_server_config: Option<ServerConfig>) -> Self {
        Self {
            tls_server_config,
            ..self
        }
    }
    pub fn silent(self, silent: bool) -> Self {
        Self { silent, ..self }
    }
}

impl Builder<i32, String, Uuid, Url, Url, Url> {
    pub async fn build(self) -> Result<Broker<Controller<ArcDynStorage>, ArcDynStorage>> {
        if let Some(otlp_endpoint_url) = self
            .otlp_endpoint_url
            .clone()
            .inspect(|otlp_endpoint_url| debug!(%otlp_endpoint_url))
        {
            otel::metric_exporter(otlp_endpoint_url)?;
        }

        let storage = StorageContainer::builder()
            .cluster_id(self.cluster_id.clone())
            .node_id(self.node_id)
            .advertised_listener(self.advertised_listener.clone())
            .schema_registry(self.schema_registry.clone())
            .lake_house(self.lake_house.clone())
            .storage(self.storage.clone())
            .cancellation(self.cancellation.clone())
            .silent(self.silent)
            .build()
            .await
            .map(|storage| Arc::new(storage) as ArcDynStorage)?;

        let groups = Controller::with_storage(storage.clone())?;

        let sasl_config = if self.authentication {
            nisshi_auth::configuration(storage.clone()).map(Some)?
        } else {
            None
        };

        Ok(Broker {
            node_id: self.node_id,
            cluster_id: self.cluster_id.clone(),
            incarnation_id: self.incarnation_id,
            listener: self.listener,
            advertised_listener: self.advertised_listener,
            storage,
            groups,
            sasl_config,
            tls_server_config: self.tls_server_config.map(Arc::new),

            silent: self.silent,
            maintenance_interval: self.maintenance_interval,
            transaction_maintenance_interval: self.transaction_maintenance_interval,
            cancellation: self.cancellation,
        })
    }
}
