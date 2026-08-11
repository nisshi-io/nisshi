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

//! PostgreSQL Storage engine

use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    str::FromStr,
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod, Transaction};
use futures::pin_mut;
use futures_util::future;
use nisshi_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, OpType, ScramMechanism,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    fetch_response::AbortedTransaction,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{Header, Record, deflated, inflated::Batch},
    to_system_time, to_timestamp,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use nisshi_schema::{
    Registry,
    lake::{House, LakeHouse as _},
};
use opentelemetry::metrics::Histogram;
use opentelemetry::{KeyValue, metrics::Counter};
use rand::{prelude::*, rng};
use serde_json::Value;
use tokio_postgres::{
    Config, Row, RowStream,
    binary_copy::BinaryCopyInWriter,
    error::SqlState,
    types::{BorrowToSql, ToSql, Type},
};
use tracing::{debug, error, instrument};
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, METER, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result,
    ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, TxnState, UpdateError, Version,
    sql::{default_hash, idempotent_sequence_check},
};

/// PostgreSQL Storage Engine
#[derive(Clone, Debug)]
pub struct Postgres {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    pool: Pool,
    schemas: Option<Registry>,
    lake: Option<House>,
}

/// PostgreSQL Storage Builder
#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, L, P> {
    cluster: C,
    node: N,
    advertised_listener: L,
    pool: P,
    schemas: Option<Registry>,
    lake: Option<House>,
}

impl<C, N, L, P> Builder<C, N, L, P> {
    pub fn cluster(self, cluster: impl Into<String>) -> Builder<String, N, L, P> {
        Builder {
            cluster: cluster.into(),
            node: self.node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn node(self, node: i32) -> Builder<C, i32, L, P> {
        Builder {
            cluster: self.cluster,
            node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn advertised_listener(self, advertised_listener: Url) -> Builder<C, N, Url, P> {
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, P> {
        Self { schemas, ..self }
    }

    pub fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }
}

impl Builder<String, i32, Url, Pool> {
    pub fn build(self) -> Postgres {
        Postgres {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            schemas: self.schemas,
            lake: self.lake,
        }
    }
}

impl<C, N> FromStr for Builder<C, N, Url, Pool>
where
    C: Default,
    N: Default,
{
    type Err = Error;

    fn from_str(config: &str) -> Result<Self, Self::Err> {
        let pg_config = Config::from_str(config).inspect(|pg_config| debug!(?pg_config))?;

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let root_store = {
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls_native_certs::load_native_certs().certs {
                roots.add(cert).inspect_err(|err| debug!(?err))?;
            }
            roots
        };

        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map(|config| config.with_root_certificates(root_store))
        .map(|config| config.with_no_client_auth())?;

        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(config);

        let mgr = Manager::from_config(pg_config, tls, mgr_config);
        let advertised_listener = Url::parse("tcp://127.0.0.1/")?;

        Pool::builder(mgr)
            .max_size(16)
            .build()
            .map(|pool| Self {
                pool,
                advertised_listener,
                node: N::default(),
                cluster: C::default(),
                schemas: None,
                lake: None,
            })
            .map_err(Into::into)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Txn {
    name: String,
    producer_id: i64,
    producer_epoch: i16,
    status: TxnState,
}

impl TryFrom<Row> for Txn {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let name = row
            .try_get::<_, String>(0)
            .inspect_err(|err| error!(?err))?;
        let producer_id = row.try_get::<_, i64>(1).inspect_err(|err| error!(?err))?;
        let producer_epoch = row.try_get::<_, i16>(2).inspect_err(|err| error!(?err))?;
        let status = row
            .try_get::<_, Option<String>>(3)
            .map_err(Into::into)
            .and_then(|status| status.map_or(Ok(TxnState::Begin), TxnState::try_from))
            .inspect_err(|err| error!(?err))?;

        Ok(Self {
            name,
            producer_id,
            producer_epoch,
            status,
        })
    }
}

impl Postgres {
    pub fn builder(
        connection: &str,
    ) -> Result<Builder<PhantomData<String>, PhantomData<i32>, Url, Pool>> {
        debug!(connection);
        Builder::from_str(connection)
    }

    async fn connection(&self) -> Result<Object> {
        self.pool.get().await.map_err(Into::into)
    }

    fn sql_lookup(&self, key: &str) -> Result<&str> {
        crate::sql::SQL.get(key)
    }

    async fn idempotent_message_check(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        tx: &Transaction<'_>,
    ) -> Result<()> {
        debug!(transaction_id, ?deflated);

        if let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                "producer_epoch_current_for_producer.sql",
                &[&self.cluster, &deflated.producer_id],
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let current_epoch = row
                .try_get::<_, i16>(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))?;

            let row = self
                .tx_prepare_query_one(
                    tx,
                    "producer_select_for_update.sql",
                    &[
                        &self.cluster,
                        &topition.topic(),
                        &topition.partition(),
                        &deflated.producer_id,
                        &deflated.producer_epoch,
                    ],
                )
                .await
                .inspect_err(|err| {
                    error!(
                        self.cluster,
                        ?topition,
                        deflated.producer_id,
                        deflated.producer_epoch,
                        ?err
                    )
                })?;

            let sequence = row.try_get::<_, i32>(0).inspect_err(|err| error!(?err))?;

            debug!(
                self.cluster,
                ?topition,
                deflated.producer_id,
                deflated.producer_epoch,
                current_epoch,
                sequence,
            );

            let increment = idempotent_sequence_check(&current_epoch, &sequence, deflated)?;

            debug!(increment);

            assert_eq!(
                1,
                self.tx_prepare_execute(
                    tx,
                    "producer_detail_insert.sql",
                    &[
                        &self.cluster,
                        &topition.topic(),
                        &topition.partition(),
                        &deflated.producer_id,
                        &deflated.producer_epoch,
                        &increment,
                    ],
                )
                .await?
            );

            Ok(())
        } else {
            Err(Error::Api(ErrorCode::UnknownProducerId))
        }
    }

    async fn watermark_select_for_update(
        &self,
        topition: &Topition,
        tx: &Transaction<'_>,
    ) -> Result<(Option<i64>, Option<i64>)> {
        if let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                "watermark_select_for_update.sql",
                &[&self.cluster, &topition.topic(), &topition.partition()],
            )
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
        {
            Ok((
                row.try_get::<_, Option<i64>>(0)
                    .inspect_err(|err| error!(?err))?,
                row.try_get::<_, Option<i64>>(1)
                    .inspect_err(|err| error!(?err))?,
            ))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    fn attributes_for_error(
        &self,
        nickname: &str,
        error: &tokio_postgres::error::Error,
    ) -> Vec<KeyValue> {
        let mut attributes = vec![
            KeyValue::new("sql", nickname.to_owned()),
            KeyValue::new("cluster_id", self.cluster.clone()),
        ];

        if let Some(db_error) = error.as_db_error() {
            if let Some(schema) = db_error.schema() {
                attributes.push(KeyValue::new("schema", schema.to_owned()));
            }

            if let Some(table) = db_error.table() {
                attributes.push(KeyValue::new("table", table.to_owned()));
            }

            if let Some(constraint) = db_error.constraint() {
                attributes.push(KeyValue::new("constraint", constraint.to_owned()));
            }
        }

        if let Some(code) = error.code() {
            attributes.push(KeyValue::new("code", format!("{code:?}")));
        }

        attributes
    }

    #[instrument(skip(self, c, params))]
    async fn prepare_execute(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        c.execute(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, c, params))]
    async fn prepare_query(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        c.query(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, c, params))]
    async fn prepare_query_one(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        c.query_one(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                debug!(?err);
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, c, params))]
    async fn prepare_query_opt(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        c.query_opt(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_execute(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        tx.execute(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_query(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        tx.query(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_query_one(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        tx.query_one(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_query_opt(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        tx.query_opt(&prepared, params)
            .await
            .inspect(|_n| {
                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_query_raw<P, I>(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: I,
    ) -> Result<RowStream, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        tx.query_raw(&prepared, params).await.map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn produce_in_tx(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        tx: &Transaction<'_>,
    ) -> Result<i64> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?topition, ?deflated);

        let topic = topition.topic();
        let partition = topition.partition();

        let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                "topition_select_id.sql",
                &[&self.cluster, &topic, &partition],
            )
            .await
            .inspect_err(|err| debug!(?err))?
        else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        let topition_id = row.try_get::<_, i32>(0).inspect_err(|err| error!(?err))?;
        debug!(topition_id);

        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, tx)
                .await
                .inspect_err(|err| error!(?err))?;
        }

        let (low, high) = self.watermark_select_for_update(topition, tx).await?;

        debug!(?low, ?high);

        let inflated = Batch::try_from(deflated).inspect_err(|err| error!(?err))?;

        let attributes = BatchAttribute::try_from(inflated.attributes)?;

        if !attributes.control
            && let Some(ref schemas) = self.schemas
            && self
                .describe_config(topic, ConfigResource::Topic, None)
                .await
                .map(|resources| {
                    resources
                        .configs
                        .as_ref()
                        .and_then(|configs| {
                            configs
                                .iter()
                                .inspect(|config| debug!(?config))
                                .find(|config| config.name.as_str() == "tansu.schema.validation")
                                .and_then(|config| config.value.as_deref())
                                .and_then(|value| bool::from_str(value).ok())
                        })
                        .unwrap_or(true)
                })
                .inspect(|schema_validation| debug!(schema_validation))?
        {
            schemas.validate(topition.topic(), &inflated).await?;
        }

        let last_offset_delta = i64::from(inflated.last_offset_delta);

        if self.schemas.is_none()
            || self.lake.is_none()
            || (self.lake.is_some()
                && !self
                    .describe_config(topic, ConfigResource::Topic, None)
                    .await
                    .inspect(|resources| debug!(?resources))
                    .map(|resources| {
                        resources
                            .configs
                            .as_ref()
                            .and_then(|configs| {
                                configs
                                    .iter()
                                    .inspect(|config| debug!(?config))
                                    .find(|config| config.name.as_str() == "tansu.lake.sink")
                                    .and_then(|config| config.value.as_deref())
                                    .and_then(|value| bool::from_str(value).ok())
                            })
                            .unwrap_or_default()
                    })
                    .inspect(|nisshi_lake_sink| debug!(nisshi_lake_sink))?)
        {
            {
                let record_sink = tx.copy_in(self.sql_lookup("record_copy.sql")?).await?;

                let record_column_types = [
                    Type::INT4,
                    Type::INT8,
                    Type::INT2,
                    Type::INT8,
                    Type::INT2,
                    Type::TIMESTAMPTZ,
                    Type::BYTEA,
                    Type::BYTEA,
                ];

                let record_writer = BinaryCopyInWriter::new(record_sink, &record_column_types);
                pin_mut!(record_writer);

                for (delta, record) in inflated.records.iter().enumerate() {
                    let delta = i64::try_from(delta)?;
                    let offset = high.unwrap_or_default() + delta;
                    let attributes = inflated.attributes;
                    let key = record.key.as_deref();
                    let value = record.value.as_deref();

                    let producer_id = transaction_id.and(Some(inflated.producer_id));
                    let producer_epoch = transaction_id.and(Some(inflated.producer_epoch));
                    let ts = to_system_time(inflated.base_timestamp + record.timestamp_delta)?;

                    let mut row: Vec<&(dyn ToSql + Sync)> =
                        Vec::with_capacity(record_column_types.len());

                    row.push(&topition_id);
                    row.push(&offset);
                    row.push(&attributes);
                    row.push(&producer_id);
                    row.push(&producer_epoch);
                    row.push(&ts);
                    row.push(&key);
                    row.push(&value);

                    record_writer
                        .as_mut()
                        .write(&row)
                        .await
                        .inspect_err(|err| {
                            error!(?err, ?topic, ?partition, ?offset, ?key, ?value)
                        })?;
                }

                _ = record_writer
                    .finish()
                    .await
                    .inspect(|record_row_count| debug!(?record_row_count))
                    .inspect_err(|err| error!(?err))?;
            }

            {
                let header_sink = tx.copy_in(self.sql_lookup("header_copy.sql")?).await?;
                let header_column_types = [Type::INT4, Type::INT8, Type::BYTEA, Type::BYTEA];
                let header_writer = BinaryCopyInWriter::new(header_sink, &header_column_types);
                pin_mut!(header_writer);

                for (delta, record) in inflated.records.iter().enumerate() {
                    let delta = i64::try_from(delta)?;
                    let offset = high.unwrap_or_default() + delta;

                    for header in record.headers.iter().as_ref() {
                        let key = header.key.as_deref();
                        let value = header.value.as_deref();

                        let mut row: Vec<&(dyn ToSql + Sync)> =
                            Vec::with_capacity(header_column_types.len());

                        row.push(&topition_id);
                        row.push(&offset);
                        row.push(&key);
                        row.push(&value);

                        header_writer
                            .as_mut()
                            .write(&row)
                            .await
                            .inspect_err(|err| {
                                error!(?err, ?topic, ?partition, ?offset, ?key, ?value)
                            })?;
                    }
                }

                _ = header_writer
                    .finish()
                    .await
                    .inspect(|header_row_count| debug!(?header_row_count))
                    .inspect_err(|err| error!(?err))?;
            }

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                let offset_start = high.unwrap_or_default();
                let offset_end = high.map_or(last_offset_delta, |high| high + last_offset_delta);

                _ = self
                .tx_prepare_execute(tx,
                    "txn_produce_offset_insert.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &inflated.producer_id,
                        &inflated.producer_epoch,
                        &topic,
                        &partition,
                        &offset_start,
                        &offset_end,
                    ],
                )
                .await
                .inspect(|n| debug!(cluster = ?self.cluster, ?transaction_id, ?inflated.producer_id, ?inflated.producer_epoch, ?topic, ?partition, ?offset_start, ?offset_end, ?n))
                .inspect_err(|err| error!(?err))?;
            }
        }

        _ = self
            .tx_prepare_execute(
                tx,
                "watermark_update.sql",
                &[
                    &self.cluster,
                    &topic,
                    &partition,
                    &low.unwrap_or_default(),
                    &high.map_or(last_offset_delta + 1, |high| high + last_offset_delta + 1),
                ],
            )
            .await
            .inspect(|n| debug!(?n))
            .inspect_err(|err| error!(?err))?;

        self.lake_store(&attributes, topition, high, &inflated)
            .await?;

        Ok(high.unwrap_or_default())
    }

    #[instrument(skip_all)]
    async fn end_in_tx(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        fence: bool,
        tx: &Transaction<'_>,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?committed, fence);

        // Check the producer's identity before touching this specific transaction's state
        // at all: a request carrying a stale epoch is not this transaction's problem, it's
        // an identity problem, and must be reported as such (ProducerFenced) regardless of
        // what the stale epoch's own txn_detail row says.
        let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                "producer_epoch_current_for_producer.sql",
                &[&self.cluster, &producer_id],
            )
            .await?
        else {
            return Ok(ErrorCode::UnknownProducerId);
        };
        let current_epoch = row.try_get::<_, i16>(0)?;

        match producer_epoch.cmp(&current_epoch) {
            Ordering::Less => return Ok(ErrorCode::ProducerFenced),
            Ordering::Greater => return Ok(ErrorCode::InvalidProducerEpoch),
            Ordering::Equal => {}
        }

        // Lock the row before touching anything else: if a concurrent caller (a real
        // EndTxn racing the maintain_transactions sweep, or a retried EndTxn) already
        // finalized this transaction, this blocks until it commits, then sees the
        // terminal status below and no-ops instead of writing a second marker.
        let status = match self
            .tx_prepare_query_opt(
                tx,
                "txn_detail_select_status_for_update.sql",
                &[
                    &self.cluster,
                    &transaction_id,
                    &producer_id,
                    &producer_epoch,
                ],
            )
            .await?
        {
            Some(row) => row
                .try_get::<_, Option<String>>(0)? // nullable transaction status column -> Option<String>
                .map(TxnState::try_from) // parse if present -> Option<Result<TxnState, _>>
                .transpose()?, // Option<Result<_>> -> Result<Option<_>>, then `?` -> Option<TxnState>
            None => None, // no txn_detail row found
        };

        // Outcome-aware idempotency: a retry (or a race between a real EndTxn and the sweep)
        // must only no-op when it agrees with what's already staged or finalized for this
        // transaction. A conflicting request -- e.g. a real commit arriving after the sweep
        // already staged/finalized an abort -- is a genuine protocol error (InvalidTxnState is
        // exactly Kafka's error code for "transactional operation attempted in an invalid
        // state"), not something to silently paper over by claiming success either way.
        //
        // PREPARE_COMMIT/PREPARE_ABORT means an earlier call already wrote this transaction's
        // control marker and is only waiting on an older, still-open transaction on the same
        // partition(s) to resolve first -- so a matching retry must NOT write a second marker,
        // it should just re-check whether those older transactions have since resolved.
        let write_marker = match status {
            Some(TxnState::Committed) => {
                debug!(transaction_id, producer_id, producer_epoch, ?status);
                return Ok(if committed {
                    ErrorCode::None
                } else {
                    ErrorCode::InvalidTxnState
                });
            }
            Some(TxnState::Aborted) => {
                debug!(transaction_id, producer_id, producer_epoch, ?status);
                return Ok(if committed {
                    ErrorCode::InvalidTxnState
                } else {
                    ErrorCode::None
                });
            }
            Some(TxnState::PrepareCommit) => {
                if !committed {
                    debug!(transaction_id, producer_id, producer_epoch, ?status);
                    return Ok(ErrorCode::InvalidTxnState);
                }
                false
            }
            Some(TxnState::PrepareAbort) => {
                if committed {
                    debug!(transaction_id, producer_id, producer_epoch, ?status);
                    return Ok(ErrorCode::InvalidTxnState);
                }
                false
            }
            None | Some(TxnState::Begin) => true,
        };

        // A broker-initiated timeout abort (the sweep) must fence the producer, not just
        // clean up this transaction's own bookkeeping: the producer might still be alive
        // and about to send more data under this same epoch. Bump it now, before doing any
        // of the actual abort work below (which can be deferred behind an older still-open
        // transaction) -- idempotent_sequence_check already rejects a stale epoch with
        // ProducerFenced, this just needs to make the current one stale. Only on the first
        // call that actually touches this transaction (write_marker), not a matching retry.
        if fence && write_marker {
            _ = self
                .tx_prepare_query_one(
                    tx,
                    "producer_epoch_insert.sql",
                    &[&self.cluster, &producer_id],
                )
                .await?;
        }

        let mut overlaps = vec![];

        let rows = self
            .tx_prepare_query(
                tx,
                "txn_select_produced_topitions.sql",
                &[
                    &self.cluster,
                    &transaction_id,
                    &producer_id,
                    &producer_epoch,
                ],
            )
            .await?;

        for row in rows {
            let topic = row.try_get::<_, String>(0)?;
            let partition = row.try_get::<_, i32>(1)?;

            let topition = Topition::new(topic.clone(), partition);

            debug!(?topition);

            // Only write the control marker the first time this transaction is finalized or
            // deferred -- a matching retry while already staged in PREPARE_COMMIT/
            // PREPARE_ABORT must not write a second one (see the status match above).
            if write_marker {
                let control_batch: Bytes = if committed {
                    ControlBatch::default().commit().try_into()?
                } else {
                    ControlBatch::default().abort().try_into()?
                };
                let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

                let batch = Batch::builder()
                    .record(
                        Record::builder()
                            .key(control_batch.into())
                            .value(end_transaction_marker.into()),
                    )
                    .attributes(
                        BatchAttribute::default()
                            .control(true)
                            .transaction(true)
                            .into(),
                    )
                    .producer_id(producer_id)
                    .producer_epoch(producer_epoch)
                    .base_sequence(-1)
                    .build()
                    .and_then(TryInto::try_into)
                    .inspect(|deflated| debug!(?deflated))?;

                let offset = self
                    .produce_in_tx(Some(transaction_id), &topition, batch, tx)
                    .await?;

                debug!(offset, ?topition);
            }

            let row = self
                .tx_prepare_query_one(
                    tx,
                    "txn_produce_offset_select_offset_range.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer_id,
                        &producer_epoch,
                        &topic,
                        &partition,
                    ],
                )
                .await?;

            let offset_start = row.try_get::<_, i64>(0)?;
            let offset_end = row.try_get::<_, i64>(1)?;
            debug!(offset_start, offset_end);

            let rows = self
                .tx_prepare_query(
                    tx,
                    "txn_produce_offset_select_overlapping_txn.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer_id,
                        &producer_epoch,
                        &topic,
                        &partition,
                        &offset_end,
                    ],
                )
                .await?;

            for row in rows {
                overlaps.push(Txn::try_from(row).inspect(|txn| debug!(?txn))?);
            }
        }

        if overlaps.iter().all(|txn| txn.status.is_prepared()) {
            let txns = {
                let mut txns = Vec::with_capacity(overlaps.len() + 1);

                txns.append(&mut overlaps);

                txns.push(Txn {
                    name: transaction_id.into(),
                    producer_id,
                    producer_epoch,
                    status: if committed {
                        TxnState::PrepareCommit
                    } else {
                        TxnState::PrepareAbort
                    },
                });

                txns
            };

            debug!(?txns);

            for txn in txns {
                debug!(?txn);

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_produce_offset_delete_by_txn.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await?;

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_topition_delete_by_txn.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await?;

                if txn.status == TxnState::PrepareCommit {
                    _ = self
                        .tx_prepare_execute(
                            tx,
                            "consumer_offset_insert_from_txn.sql",
                            &[
                                &self.cluster,
                                &txn.name,
                                &txn.producer_id,
                                &txn.producer_epoch,
                            ],
                        )
                        .await?;
                }

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_offset_commit_tp_delete_by_txn.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await?;

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_offset_commit_delete_by_txn.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await?;

                let outcome = if txn.status == TxnState::PrepareCommit {
                    String::from(TxnState::Committed)
                } else if txn.status == TxnState::PrepareAbort {
                    String::from(TxnState::Aborted)
                } else {
                    String::from(txn.status)
                };

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_status_update.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                            &outcome,
                        ],
                    )
                    .await?;
            }
        } else {
            debug!(?overlaps);

            let outcome = if committed {
                String::from(TxnState::PrepareCommit)
            } else {
                String::from(TxnState::PrepareAbort)
            };

            _ = self
                .tx_prepare_execute(
                    tx,
                    "txn_status_update.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer_id,
                        &producer_epoch,
                        &outcome,
                    ],
                )
                .await
                .inspect(|n| {
                    debug!(
                        cluster = self.cluster,
                        transaction_id, producer_id, producer_epoch, outcome, n
                    )
                })?;
        }

        Ok(ErrorCode::None)
    }

    /// Used only by maintain_transactions: aborts a transaction the sweep has decided is
    /// timed out, AND fences the producer's epoch -- unlike a real client's own EndTxn, the
    /// producer here might still be alive and about to send more data, so the broker must
    /// unilaterally invalidate its current epoch rather than just cleaning up bookkeeping.
    #[instrument(skip(self))]
    async fn abort_timed_out(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<ErrorCode> {
        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let error_code = self
            .end_in_tx(
                transaction_id,
                producer_id,
                producer_epoch,
                false,
                true,
                &tx,
            )
            .await?;

        tx.commit().await?;

        Ok(error_code)
    }

    /// Mints a fresh producer, or bumps an existing transactional.id's producer to its next
    /// epoch -- shared by a plain (-1, -1) InitProducerId request and, once validated
    /// against the current record, a KIP-360-style epoch-bump recovery request.
    async fn bump_or_create_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
    ) -> Result<ProducerIdResponse> {
        if let Some(transaction_id) = transaction_id {
            let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
            let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

            if let Some(row) = self
                .tx_prepare_query_opt(
                    &tx,
                    "producer_epoch_for_current_txn.sql",
                    &[&self.cluster, &transaction_id],
                )
                .await
                .inspect_err(|err| error!(?err))?
            {
                let id: i64 = row.try_get(0).inspect_err(|err| error!(?err))?;
                let epoch: i16 = row.try_get(1).inspect_err(|err| error!(?err))?;
                let status = row
                    .try_get::<_, Option<String>>(2)
                    .inspect_err(|err| error!(?err))?
                    .map_or(Ok(None), |status| {
                        TxnState::from_str(status.as_str()).map(Some)
                    })?;

                debug!(transaction_id, id, epoch, ?status);

                if let Some(TxnState::Begin) = status {
                    let error = self
                        .end_in_tx(transaction_id, id, epoch, false, false, &tx)
                        .await?;

                    if error != ErrorCode::None {
                        _ = tx
                            .rollback()
                            .await
                            .inspect_err(|err| error!(?err, ?transaction_id, id, epoch));

                        return Ok(ProducerIdResponse { error, id, epoch });
                    }
                }
            }

            let (producer, epoch) = if let Some(row) = self
                .tx_prepare_query_opt(
                    &tx,
                    "txn_select_name.sql",
                    &[&self.cluster, &transaction_id],
                )
                .await
                .inspect_err(|err| error!(?err))?
            {
                let producer: i64 = row.try_get(0).inspect_err(|err| error!(?err))?;

                let row = self
                    .tx_prepare_query_one(
                        &tx,
                        "producer_epoch_insert.sql",
                        &[&self.cluster, &producer],
                    )
                    .await
                    .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                let epoch: i16 = row.try_get(0)?;

                (producer, epoch)
            } else {
                let row = self
                    .tx_prepare_query_one(&tx, "producer_insert.sql", &[&self.cluster])
                    .await
                    .inspect_err(|err| error!(?err))?;

                let producer: i64 = row.try_get(0).inspect_err(|err| error!(?err))?;

                let row = self
                    .tx_prepare_query_one(
                        &tx,
                        "producer_epoch_insert.sql",
                        &[&self.cluster, &producer],
                    )
                    .await
                    .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                let epoch: i16 = row.try_get(0)?;

                assert_eq!(
                    1,
                    self.tx_prepare_execute(
                        &tx,
                        "txn_insert.sql",
                        &[&self.cluster, &transaction_id, &producer],
                    )
                    .await
                    .inspect_err(|err| error!(
                        self.cluster,
                        transaction_id,
                        producer,
                        ?err
                    ))?
                );

                (producer, epoch)
            };

            debug!(transaction_id, producer, epoch);

            assert_eq!(
                1,
                self.tx_prepare_execute(
                    &tx,
                    "txn_detail_insert.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer,
                        &epoch,
                        &transaction_timeout_ms
                    ],
                )
                .await
                .inspect_err(|err| error!(
                    self.cluster,
                    transaction_id,
                    producer,
                    epoch,
                    transaction_timeout_ms,
                    ?err
                ))?
            );

            let error = match tx.commit().await.inspect_err(|err| {
                error!(
                    ?err,
                    cluster = self.cluster,
                    transaction_id,
                    producer,
                    epoch
                )
            }) {
                Ok(()) => ErrorCode::None,
                Err(_) => ErrorCode::UnknownServerError,
            };

            Ok(ProducerIdResponse {
                error,
                id: producer,
                epoch,
            })
        } else {
            let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
            let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

            let row = self
                .tx_prepare_query_one(&tx, "producer_insert.sql", &[&self.cluster])
                .await
                .inspect_err(|err| error!(self.cluster, ?err))?;

            let producer: i64 = row.try_get(0)?;

            let row = self
                .tx_prepare_query_one(
                    &tx,
                    "producer_epoch_insert.sql",
                    &[&self.cluster, &producer],
                )
                .await
                .inspect_err(|err| error!(self.cluster, producer, ?err))?;

            let epoch: i16 = row.try_get(0)?;

            let error = match tx
                .commit()
                .await
                .inspect_err(|err| error!(?err, ?transaction_id, producer, epoch))
            {
                Ok(()) => ErrorCode::None,
                Err(_) => ErrorCode::UnknownServerError,
            };

            Ok(ProducerIdResponse {
                error,
                id: producer,
                epoch,
            })
        }
    }

    #[instrument(skip_all)]
    async fn lake_store(
        &self,
        attributes: &BatchAttribute,
        topition: &Topition,
        high: Option<i64>,
        inflated: &Batch,
    ) -> Result<()> {
        if !attributes.control
            && let Some(ref lake) = self.lake
        {
            let config = self
                .describe_config(topition.topic(), ConfigResource::Topic, None)
                .await?;

            lake.store(
                topition.topic(),
                topition.partition(),
                high.unwrap_or_default(),
                inflated,
                config,
            )
            .await?;
        }

        Ok(())
    }

    #[instrument(skip(self), ret)]
    async fn policy_compact(&self) -> Result<u64> {
        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let compacted = self
            .tx_prepare_execute(&tx, "policy_compact.sql", &[&self.cluster])
            .await?;

        tx.commit().await.map_err(Into::into).and(Ok(compacted))
    }

    #[instrument(skip(self), ret)]
    async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        let retention_secs = i32::try_from(Duration::from_hours(7 * 24).as_secs())?;

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let deleted = self
            .tx_prepare_execute(
                &tx,
                "policy_delete.sql",
                &[&self.cluster, &now, &retention_secs],
            )
            .await?;

        tx.commit().await.map_err(Into::into).and(Ok(deleted))
    }

    async fn topic_with_key<'a>(&self, topic: &'a str) -> Result<(&'a str, Option<&'a str>)> {
        if let Some((base, key)) = topic.split_once('/')
            && self
                .describe_config(base, ConfigResource::Topic, None)
                .await
                .map(|configs| {
                    configs
                        .configs
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .find_map(|config| {
                            if config.name == "tansu.virtual" {
                                config
                                    .value
                                    .as_deref()
                                    .and_then(|config| bool::from_str(config).ok())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_default()
                })?
        {
            Ok((base, Some(key)))
        } else {
            Ok((topic, None))
        }
    }

    #[instrument(skip(self), ret)]
    async fn base_topic<'a>(&self, topic: &'a str) -> Result<&'a str> {
        self.topic_with_key(topic).await.map(|(topic, _key)| topic)
    }

    #[instrument(skip(self), ret)]
    async fn virtual_topic_id(&self, topic: &str, key: &str) -> Result<Uuid> {
        let uuid = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("tag:nisshi.io,2026-04:virtual:{topic}:{key}",).as_bytes(),
        );

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let row = self
            .prepare_query_one(
                &c,
                "virtual_topic_upsert.sql",
                &[&self.cluster, &topic, &key.as_bytes(), &uuid],
            )
            .await?;

        row.try_get::<_, Uuid>(0)
            .inspect_err(|err| error!(?err))
            .map_err(Into::into)
            .inspect(|vt| debug!(%vt))
    }
}

#[async_trait]
impl Storage for Postgres {
    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        debug!(cluster = self.cluster, ?broker_registration);

        let c = self.connection().await?;

        _ = self
            .prepare_execute(
                &c,
                "register_broker.sql",
                &[&broker_registration.cluster_id],
            )
            .await
            .inspect(|n| debug!(cluster = self.cluster, n))?;

        Ok(())
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        debug!(cluster = self.cluster);

        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        debug!(cluster = self.cluster, ?topic, validate_only);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let uuid = Uuid::new_v4();

        let topic_uuid = self
            .tx_prepare_query_one(
                &tx,
                "topic_insert.sql",
                &[
                    &self.cluster,
                    &topic.name,
                    &uuid,
                    &topic.num_partitions,
                    &(topic.replication_factor as i32),
                ],
            )
            .await
            .inspect_err(|err| debug!(?err, ?topic, ?validate_only))
            .map(|row| row.get(0))
            .map_err(|error| {
                if let Error::TokioPostgres(ref error) = error
                    && error
                        .code()
                        .is_some_and(|code| *code == SqlState::UNIQUE_VIOLATION)
                {
                    Error::Api(ErrorCode::TopicAlreadyExists)
                } else {
                    error
                }
            })?;

        debug!(?topic_uuid, cluster = self.cluster, ?topic);

        _ = future::try_join_all(
            (0..topic.num_partitions)
                .map(|partition| {
                    let cluster = Box::new(self.cluster.clone()) as Box<dyn ToSql + Sync + Send>;
                    let name = Box::new(topic.name.clone()) as Box<dyn ToSql + Sync + Send>;
                    let partition = Box::new(partition) as Box<dyn ToSql + Sync + Send>;
                    [cluster, name, partition]
                })
                .map(|parameters| self.tx_prepare_query_raw(&tx, "topition_insert.sql", parameters))
                .chain(
                    (0..topic.num_partitions)
                        .map(|partition| {
                            let cluster =
                                Box::new(self.cluster.clone()) as Box<dyn ToSql + Sync + Send>;
                            let name = Box::new(topic.name.clone()) as Box<dyn ToSql + Sync + Send>;
                            let partition = Box::new(partition) as Box<dyn ToSql + Sync + Send>;
                            [cluster, name, partition]
                        })
                        .map(|parameters| {
                            self.tx_prepare_query_raw(&tx, "watermark_insert.sql", parameters)
                        }),
                ),
        )
        .await?;

        if let Some(configs) = topic.configs {
            for config in configs {
                debug!(?config);

                _ = self
                    .tx_prepare_execute(
                        &tx,
                        "topic_configuration_upsert.sql",
                        &[
                            &self.cluster,
                            &topic.name,
                            &config.name,
                            &config.value.as_deref(),
                        ],
                    )
                    .await
                    .inspect_err(|err| error!(?err, ?config));
            }
        }

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(topic_uuid)
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(cluster = self.cluster, ?topics);

        let c = self.connection().await?;

        let delete_records = c
            .prepare(concat!(
                "delete from record",
                " using topic, cluster",
                " where",
                " cluster.name=$1",
                " and topic.name = $2",
                " and record.partition = $3",
                " and record.id >= $4",
                " and topic.cluster = cluster.id",
                " and record.topic = topic.id",
            ))
            .await
            .inspect_err(|err| error!(?err, ?topics))?;

        let mut responses = vec![];

        for topic in topics {
            let mut partition_responses = vec![];

            if let Some(ref partitions) = topic.partitions {
                for partition in partitions {
                    _ = c
                        .execute(
                            &delete_records,
                            &[
                                &self.cluster,
                                &topic.name,
                                &partition.partition_index,
                                &partition.offset,
                            ],
                        )
                        .await
                        .inspect_err(|err| {
                            let cluster = self.cluster.as_str();
                            let topic = topic.name.as_str();
                            let partition_index = partition.partition_index;
                            let offset = partition.offset;

                            error!(?err, ?cluster, ?topic, ?partition_index, ?offset)
                        })?;

                    let prepared = c
                        .prepare(concat!(
                            "select",
                            " id as offset",
                            " from",
                            " record",
                            " join (",
                            " select",
                            " coalesce(min(record.id), (select last_value from record_id_seq)) as offset",
                            " from record, topic, cluster",
                            " where",
                            " topic.cluster = cluster.id",
                            " and cluster.name = $1",
                            " and topic.name = $2",
                            " and record.partition = $3",
                            " and record.topic = topic.id) as minimum",
                            " on record.id = minimum.offset",
                        ))
                        .await
                        .inspect_err(|err| {
                            let cluster = self.cluster.as_str();
                            let topic = topic.name.as_str();
                            let partition_index = partition.partition_index;
                            let offset = partition.offset;

                            error!(?err, ?cluster, ?topic, ?partition_index, ?offset)
                        })?;

                    let partition_result = c
                        .query_opt(
                            &prepared,
                            &[&self.cluster, &topic.name, &partition.partition_index],
                        )
                        .await
                        .inspect_err(|err| {
                            let cluster = self.cluster.as_str();
                            let topic = topic.name.as_str();
                            let partition_index = partition.partition_index;
                            let offset = partition.offset;

                            error!(?err, ?cluster, ?topic, ?partition_index, ?offset)
                        })
                        .map_or(
                            Ok(DeleteRecordsPartitionResult::default()
                                .partition_index(partition.partition_index)
                                .low_watermark(0)
                                .error_code(ErrorCode::UnknownServerError.into())),
                            |row| {
                                row.map_or(
                                    Ok(DeleteRecordsPartitionResult::default()
                                        .partition_index(partition.partition_index)
                                        .low_watermark(0)
                                        .error_code(ErrorCode::UnknownServerError.into())),
                                    |row| {
                                        row.try_get::<_, i64>(0).map(|low_watermark| {
                                            DeleteRecordsPartitionResult::default()
                                                .partition_index(partition.partition_index)
                                                .low_watermark(low_watermark)
                                                .error_code(ErrorCode::None.into())
                                        })
                                    },
                                )
                            },
                        )?;

                    partition_responses.push(partition_result);
                }
            }

            responses.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }
        Ok(responses)
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        debug!(cluster = self.cluster, ?topic);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let row = match topic {
            TopicId::Id(id) => {
                self.tx_prepare_query_opt(&tx, "topic_select_uuid.sql", &[&self.cluster, &id])
                    .await?
            }

            TopicId::Name(name) => {
                self.tx_prepare_query_opt(&tx, "topic_select_name.sql", &[&self.cluster, name])
                    .await?
            }
        };

        let Some(row) = row else {
            return Ok(ErrorCode::UnknownTopicOrPartition);
        };

        let topic_name = row.try_get::<_, String>(1)?;

        for (description, sql) in [
            ("consumer_offsets", "consumer_offset_delete_by_topic.sql"),
            (
                "topic_configuration",
                "topic_configuration_delete_by_topic.sql",
            ),
            ("watermarks", "watermark_delete_by_topic.sql"),
            ("headers", "header_delete_by_topic.sql"),
            ("records", "record_delete_by_topic.sql"),
            (
                "txn_offset_commit_tp",
                "txn_offset_commit_tp_delete_by_topic.sql",
            ),
            (
                "txn_produce_offset_delete",
                "txn_produce_offset_delete_by_topic.sql",
            ),
            ("txn_topition", "txn_topition_delete_by_topic.sql"),
            ("producer_detail", "producer_detail_delete_by_topic.sql"),
            ("topitions", "topition_delete_by_topic.sql"),
        ] {
            let rows = self
                .tx_prepare_execute(&tx, sql, &[&self.cluster, &topic_name])
                .await
                .inspect_err(|err| {
                    debug!(?description, ?err);
                })?;

            debug!(?topic, ?rows, ?description);
        }

        _ = self
            .tx_prepare_execute(&tx, "topic_delete_by.sql", &[&self.cluster, &topic_name])
            .await?;

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(ErrorCode::None)
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        match ConfigResource::from(resource.resource_type) {
            ConfigResource::Group => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::ClientMetric => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::BrokerLogger => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Broker => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Topic => {
                let mut error_code = ErrorCode::None;

                for config in resource.configs.unwrap_or_default() {
                    match OpType::try_from(config.config_operation)? {
                        OpType::Set => {
                            let c = self.connection().await?;

                            if self
                                .prepare_query(
                                    &c,
                                    "topic_configuration_upsert.sql",
                                    &[
                                        &self.cluster,
                                        &resource.resource_name,
                                        &config.name,
                                        &config.value,
                                    ],
                                )
                                .await
                                .inspect_err(|err| error!(?err))
                                .is_err()
                            {
                                error_code = ErrorCode::UnknownServerError;
                                break;
                            }
                        }
                        OpType::Delete => {
                            let c = self.connection().await?;

                            if self
                                .prepare_query(
                                    &c,
                                    "topic_configuration_delete.sql",
                                    &[&self.cluster, &resource.resource_name, &config.name],
                                )
                                .await
                                .inspect_err(|err| error!(?err))
                                .is_err()
                            {
                                error_code = ErrorCode::UnknownServerError;
                                break;
                            }
                        }
                        OpType::Append => todo!(),
                        OpType::Subtract => todo!(),
                    }
                }

                Ok(AlterConfigsResourceResponse::default()
                    .error_code(error_code.into())
                    .error_message(Some("".into()))
                    .resource_type(resource.resource_type)
                    .resource_name(resource.resource_name))
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        debug!(cluster = self.cluster, transaction_id, ?topition, ?deflated);

        let mut c = self.connection().await?;

        let tx = c.transaction().await?;

        let high = self
            .produce_in_tx(transaction_id, topition, deflated, &tx)
            .await?;

        tx.commit().await?;

        Ok(high)
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let started_at = SystemTime::now();

        let has_deadline_expired = || {
            started_at
                .elapsed()
                .inspect(|elapsed| debug!(?elapsed, ?max_wait))
                .map(|elapsed| max_wait.saturating_sub(elapsed).is_zero())
                .unwrap_or_default()
        };

        let (base_topic, key_filter): (&str, Option<&str>) =
            self.topic_with_key(topition.topic()).await?;

        let high_watermark = self.offset_stage(topition).await.map(|offset_stage| {
            if isolation_level == IsolationLevel::ReadCommitted {
                offset_stage.last_stable
            } else {
                offset_stage.high_watermark
            }
        })?;

        debug!(
            cluster = self.cluster,
            ?topition,
            offset,
            ?isolation_level,
            high_watermark,
            min_bytes,
            max_bytes
        );

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let records = if let Some(key) = key_filter {
            self.tx_prepare_query(
                &tx,
                "record_fetch_pg_keyed.sql",
                &[
                    &self.cluster,
                    &base_topic,
                    &topition.partition(),
                    &offset,
                    &(max_bytes as i64),
                    &high_watermark,
                    &key,
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?
        } else {
            self.tx_prepare_query(
                &tx,
                "record_fetch_pg.sql",
                &[
                    &self.cluster,
                    &topition.topic(),
                    &topition.partition(),
                    &offset,
                    &(max_bytes as i64),
                    &high_watermark,
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?
        };

        let mut batches = vec![];

        if let Some(first) = records.first() {
            let mut batch_builder = Batch::builder()
                .base_offset(
                    first
                        .try_get::<_, i64>(0)
                        .inspect(|base_offset| debug!(base_offset))
                        .inspect_err(|err| error!(?err))?,
                )
                .attributes(
                    first
                        .try_get::<_, Option<i16>>(1)
                        .map(|attributes| attributes.unwrap_or(0))
                        .inspect_err(|err| error!(?err))?,
                )
                .base_timestamp(
                    first
                        .try_get::<_, SystemTime>(2)
                        .map_err(Error::from)
                        .and_then(|system_time| to_timestamp(&system_time).map_err(Into::into))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_id(
                    first
                        .try_get::<_, Option<i64>>(6)
                        .map(|producer_id| producer_id.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_epoch(
                    first
                        .try_get::<_, Option<i16>>(7)
                        .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                );

            let mut previous_offset = None;

            for record in records.iter() {
                let attributes = record
                    .try_get::<_, Option<i16>>(1)
                    .map(|attributes| attributes.unwrap_or(0))
                    .inspect_err(|err| error!(?err))?;

                let producer_id = record
                    .try_get::<_, Option<i64>>(6)
                    .map(|producer_id| producer_id.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))?;
                let producer_epoch = record
                    .try_get::<_, Option<i16>>(7)
                    .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))?;

                let completed = record
                    .try_get::<_, bool>(8)
                    .inspect(|completed| debug!(?completed))
                    .inspect_err(|err| error!(?err))?;

                if batch_builder.attributes != attributes
                    || batch_builder.producer_id != producer_id
                    || batch_builder.producer_epoch != producer_epoch
                {
                    batches.push(batch_builder.build().and_then(TryInto::try_into)?);

                    batch_builder = Batch::builder()
                        .base_offset(
                            record
                                .try_get::<_, i64>(0)
                                .inspect(|base_offset| debug!(base_offset))
                                .inspect_err(|err| error!(?err))?,
                        )
                        .base_timestamp(
                            record
                                .try_get::<_, SystemTime>(2)
                                .map_err(Error::from)
                                .and_then(|system_time| {
                                    to_timestamp(&system_time).map_err(Into::into)
                                })
                                .inspect_err(|err| error!(?err))?,
                        )
                        .attributes(attributes)
                        .producer_id(producer_id)
                        .producer_epoch(producer_epoch);
                }

                let offset = record
                    .try_get::<_, i64>(0)
                    .inspect(|offset| debug!(offset))
                    .inspect_err(|err| error!(?err))?;

                if !completed
                    && previous_offset
                        .inspect(|previous_offset| debug!(previous_offset))
                        .is_none_or(|previous_offset| previous_offset + 1 != offset)
                {
                    break;
                }

                let offset_delta = i32::try_from(offset - batch_builder.base_offset)?;

                let timestamp_delta = record
                    .try_get::<_, SystemTime>(2)
                    .map_err(Error::from)
                    .and_then(|system_time| {
                        to_timestamp(&system_time)
                            .map(|timestamp| timestamp - batch_builder.base_timestamp)
                            .map_err(Into::into)
                    })
                    .inspect(|timestamp| debug!(?timestamp))
                    .inspect_err(|err| error!(?err))?;

                let k = record
                    .try_get::<_, Option<&[u8]>>(3)
                    .map(|o| o.map(Bytes::copy_from_slice))
                    .inspect(|k| debug!(?k))
                    .inspect_err(|err| error!(?err))?;

                let v = record
                    .try_get::<_, Option<&[u8]>>(4)
                    .map(|o| o.map(Bytes::copy_from_slice))
                    .inspect(|v| debug!(?v))
                    .inspect_err(|err| error!(?err))?;

                let mut record_builder = Record::builder()
                    .offset_delta(offset_delta)
                    .timestamp_delta(timestamp_delta)
                    .key(k)
                    .value(v);

                for header in self
                    .tx_prepare_query(
                        &tx,
                        "header_fetch.sql",
                        &[
                            &self.cluster,
                            &topition.topic(),
                            &topition.partition(),
                            &offset,
                        ],
                    )
                    .await
                    .inspect(|row| debug!(?row))
                    .inspect_err(|err| error!(?err))?
                {
                    let mut header_builder = Header::builder();

                    if let Some(k) = header
                        .try_get::<_, Option<&[u8]>>(0)
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.key(Bytes::copy_from_slice(k));
                    }

                    if let Some(v) = header
                        .try_get::<_, Option<&[u8]>>(1)
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.value(Bytes::copy_from_slice(v));
                    }

                    record_builder = record_builder.header(header_builder);
                }

                previous_offset = Some(offset);

                batch_builder = batch_builder
                    .record(record_builder)
                    .last_offset_delta(offset_delta);

                if has_deadline_expired() {
                    break;
                }
            }

            batches.push(batch_builder.build().and_then(TryInto::try_into)?);
        } else {
            batches.push(Batch::builder().build().and_then(TryInto::try_into)?);
        }

        tx.commit().await?;

        debug!(batches_len = batches.len());

        Ok(batches)
    }

    #[instrument(skip_all)]
    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        let (base_topic, _key_filter) = self.topic_with_key(topition.topic()).await?;

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let mut aborted_transactions = vec![];

        // Every control marker (commit or abort) in [offset, last_stable_offset), each already
        // carrying the first_offset of the transaction it closes -- see
        // pg/record_control_marker_select.sql's header comment for why this is a single
        // lag()-based query rather than a per-marker lookup.
        for row in self
            .tx_prepare_query(
                &tx,
                "record_control_marker_select_pg.sql",
                &[
                    &self.cluster,
                    &base_topic,
                    &topition.partition(),
                    &offset,
                    &last_stable_offset,
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let producer_id = row.try_get::<_, i64>(0)?;
            let key: Vec<u8> = row.try_get(1)?;
            let first_offset = row.try_get::<_, i64>(2)?;

            // The key bytes are the only place we learn commit vs abort -- committed
            // transactions have nothing to report here, so skip them.
            if !ControlBatch::try_from(Bytes::from(key))?.is_abort() {
                continue;
            }

            aborted_transactions.push(
                AbortedTransaction::default()
                    .producer_id(producer_id)
                    .first_offset(first_offset),
            );
        }

        tx.commit().await?;

        debug!(?aborted_transactions);

        Ok(aborted_transactions)
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        debug!(cluster = self.cluster, ?topition);
        let c = self.connection().await?;

        let row = self
            .prepare_query_one(
                &c,
                "watermark_select.sql",
                &[
                    &self.cluster,
                    &self.base_topic(topition.topic()).await?,
                    &topition.partition(),
                ],
            )
            .await
            .inspect_err(|err| error!(?topition, ?err))?;

        let log_start = row
            .try_get::<_, Option<i64>>(0)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or_default();

        let high_watermark = row
            .try_get::<_, Option<i64>>(1)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or_default();

        let last_stable = row
            .try_get::<_, Option<i64>>(2)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(high_watermark);

        debug!(cluster = self.cluster, ?topition, log_start, high_watermark,);

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
        })
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        debug!(cluster = self.cluster, ?group, ?retention);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let mut cg_inserted = false;

        let mut responses = vec![];

        for (topition, offset) in offsets {
            debug!(?topition, ?offset);

            if self
                .tx_prepare_query_opt(
                    &tx,
                    "topition_select.sql",
                    &[
                        &self.cluster,
                        &self.base_topic(topition.topic()).await?,
                        &topition.partition(),
                    ],
                )
                .await
                .inspect_err(|err| error!(?err))?
                .is_some()
            {
                if !cg_inserted {
                    let rows = self
                        .tx_prepare_execute(
                            &tx,
                            "consumer_group_insert.sql",
                            &[&self.cluster, &group],
                        )
                        .await?;
                    debug!(rows);

                    cg_inserted = true;
                }

                let rows = self
                    .tx_prepare_execute(
                        &tx,
                        "consumer_offset_insert.sql",
                        &[
                            &self.cluster,
                            &self.base_topic(topition.topic()).await?,
                            &topition.partition(),
                            &group,
                            &offset.offset,
                            &offset.leader_epoch,
                            &offset.timestamp,
                            &offset.metadata,
                        ],
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                debug!(?rows);

                responses.push((
                    topition.to_owned(),
                    if rows == 0 {
                        ErrorCode::UnknownTopicOrPartition
                    } else {
                        ErrorCode::None
                    },
                ));
            } else {
                responses.push((topition.to_owned(), ErrorCode::UnknownTopicOrPartition))
            }
        }

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(responses)
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        debug!(group_id);

        let mut results = BTreeMap::new();

        let c = self.connection().await?;

        for row in self
            .prepare_query(
                &c,
                "consumer_offset_select_by_group.sql",
                &[&self.cluster, &group_id],
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let topic = row.try_get::<_, String>(0)?;
            let partition = row.try_get::<_, i32>(1)?;
            let offset = row.try_get::<_, i64>(2)?;

            debug!(group_id, topic, partition, offset);

            assert_eq!(
                None,
                results.insert(Topition::new(topic, partition), offset)
            );
        }

        Ok(results)
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        debug!(cluster = self.cluster, ?group_id, ?topics, ?require_stable);

        let c = self.connection().await?;

        let mut offsets = BTreeMap::new();

        for topic in topics {
            let offset = self
                .prepare_query_opt(
                    &c,
                    "consumer_offset_select.sql",
                    &[
                        &self.cluster,
                        &group_id,
                        &self.base_topic(topic.topic()).await?,
                        &topic.partition(),
                    ],
                )
                .await
                .and_then(|maybe| {
                    maybe.map_or(Ok(-1), |row| row.try_get::<_, i64>(0).map_err(Into::into))
                })
                .inspect(|offset| {
                    debug!(
                        cluster = self.cluster,
                        group_id,
                        topic = topic.topic,
                        partition = topic.partition,
                        offset
                    )
                })
                .inspect_err(|err| {
                    error!(
                        ?err,
                        cluster = self.cluster,
                        group_id,
                        topic = topic.topic,
                        partition = topic.partition
                    )
                })?;

            assert_eq!(None, offsets.insert(topic.to_owned(), offset));
        }

        Ok(offsets)
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(cluster = self.cluster, ?isolation_level, ?offsets);

        let c = self.connection().await?;

        let mut responses = vec![];

        for (topition, offset_type) in offsets {
            let query = match (offset_type, isolation_level) {
                (ListOffset::Earliest, _) => "list_earliest_offset.sql",
                (ListOffset::Latest, IsolationLevel::ReadCommitted) => {
                    "list_latest_offset_committed.sql"
                }
                (ListOffset::Latest, IsolationLevel::ReadUncommitted) => {
                    "list_latest_offset_uncommitted.sql"
                }
                (ListOffset::Timestamp(_), _) => "list_latest_offset_timestamp.sql",
            };

            debug!(?query);

            let list_offset = match offset_type {
                ListOffset::Earliest | ListOffset::Latest => self
                    .prepare_query_opt(
                        &c,
                        query,
                        &[&self.cluster, &topition.topic(), &topition.partition()],
                    )
                    .await
                    .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition)),

                ListOffset::Timestamp(timestamp) => self
                    .prepare_query_opt(
                        &c,
                        query,
                        &[
                            &self.cluster.as_str(),
                            &topition.topic(),
                            &topition.partition(),
                            timestamp,
                        ],
                    )
                    .await
                    .inspect_err(|err| error!(?err)),
            }
            .inspect_err(|err| {
                error!(?err, cluster = self.cluster, ?topition);
            })
            .inspect(|result| debug!(?result))?
            .map_or_else(
                || {
                    let timestamp = None;
                    let offset = Some(0);
                    debug!(
                        cluster = self.cluster,
                        ?topition,
                        ?offset_type,
                        offset,
                        ?timestamp
                    );

                    Ok(ListOffsetResponse {
                        timestamp,
                        offset,
                        ..Default::default()
                    })
                },
                |row| {
                    debug!(?row);

                    row.try_get::<_, i64>(0).map(Some).and_then(|offset| {
                        row.try_get::<_, SystemTime>(1).map(Some).map(|timestamp| {
                            debug!(
                                cluster = self.cluster,
                                ?topition,
                                ?offset_type,
                                offset,
                                ?timestamp
                            );

                            ListOffsetResponse {
                                timestamp,
                                offset,
                                ..Default::default()
                            }
                        })
                    })
                },
            )?;

            responses.push((topition.clone(), list_offset));
        }

        Ok(responses).inspect(|r| debug!(?r))
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        debug!(cluster = self.cluster, ?topics);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let brokers = vec![
            MetadataResponseBroker::default()
                .node_id(self.node)
                .host(
                    self.advertised_listener
                        .host_str()
                        .unwrap_or("0.0.0.0")
                        .into(),
                )
                .port(self.advertised_listener.port().unwrap_or(9092).into())
                .rack(None),
        ];

        debug!(?brokers);

        let responses = match topics {
            Some(topics) if !topics.is_empty() => {
                let mut responses = vec![];

                for topic in topics {
                    responses.push(match topic {
                        TopicId::Name(name) => {
                            let (base_topic, key) = self
                                .topic_with_key(name.as_str())
                                .await
                                .inspect(|(base_topic, key)| debug!(base_topic, key))?;

                            let vtid = if let Some(key) = key {
                                self.virtual_topic_id(base_topic, key)
                                    .await
                                    .map(|uuid| uuid.into_bytes())
                                    .map(Some)
                            } else {
                                Ok(None)
                            }?;

                            match self
                                .prepare_query_opt(
                                    &c,
                                    "topic_select_name.sql",
                                    &[&self.cluster, &base_topic],
                                )
                                .await
                                .inspect_err(|err| error!(?err))
                            {
                                Ok(Some(row)) => {
                                    let error_code = ErrorCode::None.into();

                                    let topic_id = vtid.or(row
                                        .try_get::<_, Uuid>(0)
                                        .map(|uuid| uuid.into_bytes())
                                        .map(Some)?);

                                    let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                                    let partitions = row.try_get::<_, i32>(3)?;
                                    let replication_factor = row.try_get::<_, i32>(4)?;

                                    debug!(
                                        ?error_code,
                                        ?topic_id,
                                        ?name,
                                        ?is_internal,
                                        ?partitions,
                                        ?replication_factor
                                    );

                                    let mut rng = rng();
                                    let mut broker_ids: Vec<_> =
                                        brokers.iter().map(|broker| broker.node_id).collect();
                                    broker_ids.shuffle(&mut rng);

                                    let mut brokers = broker_ids.into_iter().cycle();

                                    let partitions = Some(
                                        (0..partitions)
                                            .map(|partition_index| {
                                                let leader_id = brokers.next().expect("cycling");

                                                let replica_nodes = Some(
                                                    (0..replication_factor)
                                                        .map(|_replica| {
                                                            brokers.next().expect("cycling")
                                                        })
                                                        .collect(),
                                                );
                                                let isr_nodes = replica_nodes.clone();

                                                MetadataResponsePartition::default()
                                                    .error_code(error_code)
                                                    .partition_index(partition_index)
                                                    .leader_id(leader_id)
                                                    .leader_epoch(Some(0))
                                                    .replica_nodes(replica_nodes)
                                                    .isr_nodes(isr_nodes)
                                                    .offline_replicas(Some([].into()))
                                            })
                                            .collect(),
                                    );

                                    MetadataResponseTopic::default()
                                        .error_code(error_code)
                                        .name(Some(name.to_owned()))
                                        .topic_id(topic_id)
                                        .is_internal(is_internal)
                                        .partitions(partitions)
                                        .topic_authorized_operations(Some(-2147483648))
                                }

                                Ok(None) => MetadataResponseTopic::default()
                                    .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                    .name(Some(name.into()))
                                    .topic_id(Some(NULL_TOPIC_ID))
                                    .is_internal(Some(false))
                                    .partitions(Some([].into()))
                                    .topic_authorized_operations(Some(-2147483648)),

                                Err(reason) => {
                                    debug!(?reason);
                                    MetadataResponseTopic::default()
                                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                        .name(Some(name.into()))
                                        .topic_id(Some(NULL_TOPIC_ID))
                                        .is_internal(Some(false))
                                        .partitions(Some([].into()))
                                        .topic_authorized_operations(Some(-2147483648))
                                }
                            }
                        }
                        TopicId::Id(id) => {
                            debug!(?id);
                            match self
                                .prepare_query_one(
                                    &c,
                                    "pg/topic_select_uuid.sql",
                                    &[&self.cluster, &id],
                                )
                                .await
                            {
                                Ok(row) => {
                                    let error_code = ErrorCode::None.into();
                                    let topic_id = row
                                        .try_get::<_, Uuid>(0)
                                        .map(|uuid| uuid.into_bytes())
                                        .map(Some)?;
                                    let name = row.try_get::<_, String>(1).map(Some)?;
                                    let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                                    let partitions = row.try_get::<_, i32>(3)?;
                                    let replication_factor = row.try_get::<_, i32>(4)?;

                                    debug!(
                                        ?error_code,
                                        ?topic_id,
                                        ?name,
                                        ?is_internal,
                                        ?partitions,
                                        ?replication_factor
                                    );

                                    let mut rng = rng();
                                    let mut broker_ids: Vec<_> =
                                        brokers.iter().map(|broker| broker.node_id).collect();
                                    broker_ids.shuffle(&mut rng);

                                    let mut brokers = broker_ids.into_iter().cycle();

                                    let partitions = Some(
                                        (0..partitions)
                                            .map(|partition_index| {
                                                let leader_id = brokers.next().expect("cycling");

                                                let replica_nodes = Some(
                                                    (0..replication_factor)
                                                        .map(|_replica| {
                                                            brokers.next().expect("cycling")
                                                        })
                                                        .collect(),
                                                );
                                                let isr_nodes = replica_nodes.clone();

                                                MetadataResponsePartition::default()
                                                    .error_code(error_code)
                                                    .partition_index(partition_index)
                                                    .leader_id(leader_id)
                                                    .leader_epoch(Some(0))
                                                    .replica_nodes(replica_nodes)
                                                    .isr_nodes(isr_nodes)
                                                    .offline_replicas(Some([].into()))
                                            })
                                            .collect(),
                                    );

                                    MetadataResponseTopic::default()
                                        .error_code(error_code)
                                        .name(name)
                                        .topic_id(topic_id)
                                        .is_internal(is_internal)
                                        .partitions(partitions)
                                        .topic_authorized_operations(Some(-2147483648))
                                }
                                Err(reason) => {
                                    debug!(?reason);
                                    MetadataResponseTopic::default()
                                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                        .name(None)
                                        .topic_id(Some(id.into_bytes()))
                                        .is_internal(Some(false))
                                        .partitions(Some([].into()))
                                        .topic_authorized_operations(Some(-2147483648))
                                }
                            }
                        }
                    });
                }

                responses
            }

            _ => {
                let mut responses = vec![];

                match self
                    .prepare_query(&c, "topic_by_cluster.sql", &[&self.cluster])
                    .await
                    .inspect_err(|err| error!(?err))
                {
                    Ok(rows) => {
                        for row in rows {
                            let error_code = ErrorCode::None.into();
                            let topic_id = row
                                .try_get::<_, Uuid>(0)
                                .map(|uuid| uuid.into_bytes())
                                .map(Some)?;
                            let name = row.try_get::<_, String>(1).map(Some)?;
                            let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                            let partitions = row.try_get::<_, i32>(3)?;
                            let replication_factor = row.try_get::<_, i32>(4)?;

                            debug!(
                                ?error_code,
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            let mut rng = rng();
                            let mut broker_ids: Vec<_> =
                                brokers.iter().map(|broker| broker.node_id).collect();
                            broker_ids.shuffle(&mut rng);

                            let mut brokers = broker_ids.into_iter().cycle();

                            let partitions = Some(
                                (0..partitions)
                                    .map(|partition_index| {
                                        let leader_id = brokers.next().expect("cycling");

                                        let replica_nodes = Some(
                                            (0..replication_factor)
                                                .map(|_replica| brokers.next().expect("cycling"))
                                                .collect(),
                                        );
                                        let isr_nodes = replica_nodes.clone();

                                        MetadataResponsePartition::default()
                                            .error_code(error_code)
                                            .partition_index(partition_index)
                                            .leader_id(leader_id)
                                            .leader_epoch(Some(0))
                                            .replica_nodes(replica_nodes)
                                            .isr_nodes(isr_nodes)
                                            .offline_replicas(Some([].into()))
                                    })
                                    .collect(),
                            );

                            responses.push(
                                MetadataResponseTopic::default()
                                    .error_code(error_code)
                                    .name(name)
                                    .topic_id(topic_id)
                                    .is_internal(is_internal)
                                    .partitions(partitions)
                                    .topic_authorized_operations(Some(-2147483648)),
                            );
                        }
                    }
                    Err(reason) => {
                        debug!(?reason);
                        responses.push(
                            MetadataResponseTopic::default()
                                .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                .name(None)
                                .topic_id(Some(NULL_TOPIC_ID))
                                .is_internal(Some(false))
                                .partitions(Some([].into()))
                                .topic_authorized_operations(Some(-2147483648)),
                        );
                    }
                }

                responses
            }
        };

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: responses,
        })
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        debug!(cluster = self.cluster, name, ?resource, ?keys);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let prepared = c
            .prepare_cached(self.sql_lookup("topic_select.sql")?)
            .await
            .inspect_err(|err| error!(?err))?;

        if c.query_opt(&prepared, &[&self.cluster.as_str(), &name])
            .await
            .inspect_err(|err| error!(?err))?
            .is_some()
        {
            let prepared = c
                .prepare_cached(self.sql_lookup("topic_configuration_select.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            let rows = c
                .query(&prepared, &[&self.cluster.as_str(), &name])
                .await
                .inspect_err(|err| error!(?err))?;

            let mut configs = vec![];

            for row in rows {
                let name = row
                    .try_get::<_, String>(0)
                    .inspect_err(|err| error!(?err))?;
                let value = row
                    .try_get::<_, Option<String>>(1)
                    .map(|value| value.unwrap_or_default())
                    .map(Some)
                    .inspect_err(|err| error!(?err))?;

                configs.push(
                    DescribeConfigsResourceResult::default()
                        .name(name)
                        .value(value)
                        .read_only(false)
                        .is_default(None)
                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                        .is_sensitive(false)
                        .synonyms(Some([].into()))
                        .config_type(Some(ConfigType::String.into()))
                        .documentation(Some("".into())),
                );
            }

            let error_code = ErrorCode::None;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some(configs)))
        } else {
            let error_code = ErrorCode::UnknownTopicOrPartition;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some([].into())))
        }
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let _ = (topics, partition_limit, cursor);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            debug!(?topic);

            responses.push(match topic {
                TopicId::Name(name) => {
                    match self
                        .prepare_query_opt(
                            &c,
                            "topic_select_name.sql",
                            &[&self.cluster, &name.as_str()],
                        )
                        .await
                        .inspect_err(|err| error!(?err))
                    {
                        Ok(Some(row)) => {
                            let topic_id =
                                row.try_get::<_, Uuid>(0).map(|uuid| uuid.into_bytes())?;
                            let name = row.try_get::<_, String>(1).map(Some)?;
                            let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                            let partitions = row.try_get::<_, i32>(3)?;
                            let replication_factor = row.try_get::<_, i32>(4)?;

                            debug!(
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(ErrorCode::None.into())
                                .name(name)
                                .topic_id(topic_id)
                                .is_internal(false)
                                .partitions(Some(
                                    (0..partitions)
                                        .map(|partition_index| {
                                            DescribeTopicPartitionsResponsePartition::default()
                                                .error_code(ErrorCode::None.into())
                                                .partition_index(partition_index)
                                                .leader_id(self.node)
                                                .leader_epoch(0)
                                                .replica_nodes(Some(vec![
                                                    self.node;
                                                    replication_factor
                                                        as usize
                                                ]))
                                                .isr_nodes(Some(vec![
                                                    self.node;
                                                    replication_factor as usize
                                                ]))
                                                .eligible_leader_replicas(Some(vec![]))
                                                .last_known_elr(Some(vec![]))
                                                .offline_replicas(Some(vec![]))
                                        })
                                        .collect(),
                                ))
                                .topic_authorized_operations(-2147483648)
                        }

                        Ok(None) => DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => None,
                            })
                            .topic_id(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            })
                            .is_internal(false)
                            .partitions(Some([].into()))
                            .topic_authorized_operations(-2147483648),

                        Err(reason) => {
                            debug!(?reason);
                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(ErrorCode::UnknownServerError.into())
                                .name(match topic {
                                    TopicId::Name(name) => Some(name.into()),
                                    TopicId::Id(_) => None,
                                })
                                .topic_id(match topic {
                                    TopicId::Name(_) => NULL_TOPIC_ID,
                                    TopicId::Id(id) => id.into_bytes(),
                                })
                                .is_internal(false)
                                .partitions(Some([].into()))
                                .topic_authorized_operations(-2147483648)
                        }
                    }
                }
                TopicId::Id(id) => {
                    debug!(?id);
                    match self
                        .prepare_query_one(&c, "topic_select_uuid.sql", &[&self.cluster, &id])
                        .await
                    {
                        Ok(row) => {
                            let topic_id =
                                row.try_get::<_, Uuid>(0).map(|uuid| uuid.into_bytes())?;
                            let name = row.try_get::<_, String>(1).map(Some)?;
                            let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                            let partitions = row.try_get::<_, i32>(3)?;
                            let replication_factor = row.try_get::<_, i32>(4)?;

                            debug!(
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(ErrorCode::None.into())
                                .name(name)
                                .topic_id(topic_id)
                                .is_internal(false)
                                .partitions(Some(
                                    (0..partitions)
                                        .map(|partition_index| {
                                            DescribeTopicPartitionsResponsePartition::default()
                                                .error_code(ErrorCode::None.into())
                                                .partition_index(partition_index)
                                                .leader_id(self.node)
                                                .leader_epoch(0)
                                                .replica_nodes(Some(vec![
                                                    self.node;
                                                    replication_factor
                                                        as usize
                                                ]))
                                                .isr_nodes(Some(vec![
                                                    self.node;
                                                    replication_factor as usize
                                                ]))
                                                .eligible_leader_replicas(Some(vec![]))
                                                .last_known_elr(Some(vec![]))
                                                .offline_replicas(Some(vec![]))
                                        })
                                        .collect(),
                                ))
                                .topic_authorized_operations(-2147483648)
                        }

                        Err(reason) => {
                            debug!(?reason);
                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                .name(match topic {
                                    TopicId::Name(name) => Some(name.into()),
                                    TopicId::Id(_) => None,
                                })
                                .topic_id(match topic {
                                    TopicId::Name(_) => NULL_TOPIC_ID,
                                    TopicId::Id(id) => id.into_bytes(),
                                })
                                .is_internal(false)
                                .partitions(Some([].into()))
                                .topic_authorized_operations(-2147483648)
                        }
                    }
                }
            });
        }

        Ok(responses)
    }

    #[instrument(skip_all)]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        debug!(?states_filter);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut listed_groups = vec![];

        for row in self
            .prepare_query(&c, "consumer_group_select.sql", &[&self.cluster])
            .await
            .inspect_err(|err| error!(?err))?
        {
            let group_id = row.try_get::<_, String>(0)?;

            listed_groups.push(
                ListedGroup::default()
                    .group_id(group_id)
                    .protocol_type("consumer".into())
                    .group_state(Some("unknown".into()))
                    .group_type(Some("classic".into())),
            );
        }

        Ok(listed_groups)
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        debug!(?group_ids);

        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let c = self.connection().await?;

            let consumer_offset = c
                .prepare_cached(self.sql_lookup("consumer_offset_delete_by_cg.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            let group_detail = c
                .prepare_cached(self.sql_lookup("consumer_group_detail_delete_by_cg.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            let group = c
                .prepare_cached(self.sql_lookup("consumer_group_delete.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            for group_id in group_ids {
                _ = c
                    .execute(&consumer_offset, &[&self.cluster, &group_id])
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = c
                    .execute(&group_detail, &[&self.cluster, &group_id])
                    .await
                    .inspect_err(|err| error!(?err))?;

                let rows = c
                    .execute(&group, &[&self.cluster, &group_id])
                    .await
                    .inspect_err(|err| error!(?err))?;

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(
                            if rows == 0 {
                                ErrorCode::GroupIdNotFound
                            } else {
                                ErrorCode::None
                            }
                            .into(),
                        ),
                );
            }
        }

        Ok(results)
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        debug!(?group_ids, include_authorized_operations);

        let mut results = vec![];
        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                if let Some(row) = self
                    .prepare_query_opt(
                        &c,
                        "consumer_group_select_by_name.sql",
                        &[&self.cluster, group_id],
                    )
                    .await
                    .inspect_err(|err| error!(?err, group_id))?
                {
                    let value = row
                        .try_get::<_, Value>(1)
                        .inspect_err(|err| error!(?err, group_id))?;

                    let current = serde_json::from_value::<GroupDetail>(value)
                        .inspect(|current| debug!(?current))?;

                    results.push(NamedGroupDetail::found(group_id.into(), current));
                } else {
                    results.push(NamedGroupDetail::error_code(
                        group_id.into(),
                        ErrorCode::GroupIdNotFound,
                    ));
                }
            }
        }

        Ok(results)
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        debug!(cluster = self.cluster, group_id, ?detail, ?version);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        _ = self
            .tx_prepare_execute(
                &tx,
                "consumer_group_insert.sql",
                &[&self.cluster, &group_id],
            )
            .await?;

        let existing_e_tag = version
            .as_ref()
            .map_or(Ok(Uuid::from_u128(0)), |version| {
                version
                    .e_tag
                    .as_ref()
                    .map_or(Err(UpdateError::MissingEtag::<GroupDetail>), |e_tag| {
                        Uuid::from_str(e_tag.as_str()).map_err(Into::into)
                    })
            })
            .inspect_err(|err| error!(?err))
            .inspect(|existing_e_tag| debug!(?existing_e_tag))?;

        let new_e_tag = default_hash(&detail);
        debug!(?new_e_tag);

        let detail = serde_json::to_value(detail).inspect(|detail| debug!(?detail))?;

        let outcome = if let Some(row) = self
            .tx_prepare_query_opt(
                &tx,
                "consumer_group_detail_insert.sql",
                &[
                    &self.cluster,
                    &group_id,
                    &existing_e_tag,
                    &new_e_tag,
                    &detail,
                ],
            )
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| error!(?err))?
        {
            row.try_get::<_, Uuid>(2)
                .inspect_err(|err| error!(?err))
                .map_err(Into::into)
                .map(|uuid| uuid.to_string())
                .map(Some)
                .map(|e_tag| Version {
                    e_tag,
                    version: None,
                })
                .inspect(|version| debug!(?version))
        } else {
            let row = self
                .tx_prepare_query_one(
                    &tx,
                    "consumer_group_detail.sql",
                    &[&group_id, &self.cluster.as_str()],
                )
                .await
                .inspect(|row| debug!(?row))
                .inspect_err(|err| error!(?err))?;

            let version = row
                .try_get::<_, Uuid>(0)
                .inspect_err(|err| error!(?err))
                .map(|uuid| uuid.to_string())
                .map(Some)
                .map(|e_tag| Version {
                    e_tag,
                    version: None,
                })
                .inspect(|version| debug!(?version))?;

            let value = row.try_get::<_, Value>(1)?;
            let current = serde_json::from_value::<GroupDetail>(value)
                .inspect(|current| debug!(?current))
                .map(Box::new)?;

            Err(UpdateError::Outdated { current, version })
        };

        tx.commit().await.inspect_err(|err| error!(?err))?;

        debug!(?outcome);

        outcome
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch
        );

        // (None, None) means an older InitProducerId API version (<= 2), which has no wire
        // representation for these fields at all -- there is no other possible meaning for
        // those versions, so treat it exactly like an explicit (-1, -1) "give me a fresh
        // epoch" request.
        let requesting_fresh = matches!((producer_id, producer_epoch), (None, None))
            || (producer_id.is_some_and(|producer_id| producer_id == -1)
                && producer_epoch.is_some_and(|producer_epoch| producer_epoch == -1));

        if requesting_fresh {
            return self
                .bump_or_create_producer(transaction_id, transaction_timeout_ms)
                .await;
        }

        let (Some(producer_id), Some(producer_epoch)) = (producer_id, producer_epoch) else {
            // one of producer_id/producer_epoch was set without the other -- not a
            // well-formed request under any InitProducerId version.
            return Ok(ProducerIdResponse {
                error: ErrorCode::InvalidRequest,
                id: producer_id.unwrap_or(-1),
                epoch: producer_epoch.unwrap_or(-1),
            });
        };

        // KIP-360-style epoch-bump recovery: the client claims a specific, already-issued
        // identity (a v3+ producer recovering after e.g. a broker-initiated abort) rather
        // than asking for a brand new one. Validate the claim against what's actually on
        // record before treating it the same as a fresh bump -- a stale claim (exactly what
        // maintain_transactions' sweep produces by fencing a timed-out producer) must be
        // rejected as ProducerFenced, not silently granted a new epoch.
        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let Some(row) = self
            .prepare_query_opt(
                &c,
                "producer_epoch_current_for_producer.sql",
                &[&self.cluster, &producer_id],
            )
            .await
            .inspect_err(|err| error!(?err))?
        else {
            return Ok(ProducerIdResponse {
                error: ErrorCode::UnknownProducerId,
                id: producer_id,
                epoch: producer_epoch,
            });
        };

        let current_epoch = row.try_get::<_, i16>(0).inspect_err(|err| error!(?err))?;

        if producer_epoch != current_epoch {
            return Ok(ProducerIdResponse {
                error: ErrorCode::ProducerFenced,
                id: producer_id,
                epoch: producer_epoch,
            });
        }

        drop(c);

        self.bump_or_create_producer(transaction_id, transaction_timeout_ms)
            .await
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, group_id
        );

        Ok(ErrorCode::None)
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        debug!(cluster = self.cluster, ?partitions);

        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                topics,
            } => {
                debug!(?transaction_id, ?producer_id, ?producer_epoch, ?topics);

                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                let mut results = vec![];

                for topic in topics {
                    let mut results_by_partition = vec![];

                    for partition_index in topic.partitions.unwrap_or(vec![]) {
                        _ = self
                            .tx_prepare_execute(
                                &tx,
                                "txn_topition_insert.sql",
                                &[
                                    &self.cluster,
                                    &topic.name,
                                    &partition_index,
                                    &transaction_id,
                                    &producer_id,
                                    &producer_epoch,
                                ],
                            )
                            .await
                            .inspect_err(|err| {
                                error!(
                                    ?err,
                                    cluster = self.cluster,
                                    topic = topic.name,
                                    ?partition_index,
                                    transaction_id
                                )
                            })?;

                        results_by_partition.push(
                            AddPartitionsToTxnPartitionResult::default()
                                .partition_index(partition_index)
                                .partition_error_code(i16::from(ErrorCode::None)),
                        );
                    }

                    results.push(
                        AddPartitionsToTxnTopicResult::default()
                            .name(topic.name)
                            .results_by_partition(Some(results_by_partition)),
                    )
                }

                _ = self
                    .tx_prepare_execute(
                        &tx,
                        "txn_detail_update_started_at.sql",
                        &[
                            &self.cluster,
                            &transaction_id,
                            &producer_id,
                            &producer_epoch,
                        ],
                    )
                    .await
                    .inspect_err(|err| {
                        error!(
                            ?err,
                            cluster = self.cluster,
                            transaction_id,
                            producer_id,
                            producer_epoch,
                        )
                    })?;

                tx.commit().await?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
            }

            TxnAddPartitionsRequest::VersionFourPlus { .. } => {
                todo!()
            }
        }
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        debug!(cluster = self.cluster, ?offsets);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let (producer_id, producer_epoch) = if let Some(row) = self
            .tx_prepare_query_opt(
                &tx,
                "producer_epoch_for_current_txn.sql",
                &[&self.cluster, &offsets.transaction_id],
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let producer_id = row
                .try_get::<_, i64>(0)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            let epoch = row
                .try_get::<_, i16>(1)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            (producer_id, epoch)
        } else {
            (None, None)
        };

        _ = self
            .tx_prepare_execute(
                &tx,
                "consumer_group_insert.sql",
                &[&self.cluster, &offsets.group_id],
            )
            .await?;

        debug!(?producer_id, ?producer_epoch);

        _ = self
            .tx_prepare_execute(
                &tx,
                "txn_offset_commit_insert.sql",
                &[
                    &self.cluster,
                    &offsets.transaction_id,
                    &offsets.group_id,
                    &offsets.producer_id,
                    &offsets.producer_epoch,
                    &offsets.generation_id,
                    &offsets.member_id,
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?;

        let mut topics = vec![];

        for topic in offsets.topics {
            let mut partitions = vec![];

            for partition in topic.partitions.unwrap_or(vec![]) {
                if producer_id.is_some_and(|producer_id| producer_id == offsets.producer_id) {
                    if producer_epoch
                        .is_some_and(|producer_epoch| producer_epoch == offsets.producer_epoch)
                    {
                        _ = self
                            .tx_prepare_execute(
                                &tx,
                                "txn_offset_commit_tp_insert.sql",
                                &[
                                    &self.cluster,
                                    &offsets.transaction_id,
                                    &offsets.group_id,
                                    &offsets.producer_id,
                                    &offsets.producer_epoch,
                                    &topic.name,
                                    &partition.partition_index,
                                    &partition.committed_offset,
                                    &partition.committed_leader_epoch,
                                    &partition.committed_metadata,
                                ],
                            )
                            .await
                            .inspect_err(|err| error!(?err))?;

                        partitions.push(
                            TxnOffsetCommitResponsePartition::default()
                                .partition_index(partition.partition_index)
                                .error_code(i16::from(ErrorCode::None)),
                        );
                    } else {
                        partitions.push(
                            TxnOffsetCommitResponsePartition::default()
                                .partition_index(partition.partition_index)
                                .error_code(i16::from(ErrorCode::InvalidProducerEpoch)),
                        );
                    }
                } else {
                    partitions.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(i16::from(ErrorCode::UnknownProducerId)),
                    );
                }
            }

            topics.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name)
                    .partitions(Some(partitions)),
            );
        }

        tx.commit().await?;

        Ok(topics)
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, transaction_id, producer_id, producer_epoch, committed);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let error_code = self
            .end_in_tx(
                transaction_id,
                producer_id,
                producer_epoch,
                committed,
                false,
                &tx,
            )
            .await?;

        tx.commit().await?;

        Ok(error_code)
    }

    #[instrument(skip_all)]
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        let deleted = self.policy_delete(now).await?;
        debug!(deleted);

        let compacted = self.policy_compact().await?;
        debug!(compacted);

        if let Some(ref lake) = self.lake {
            return lake.maintain().await.map_err(Into::into);
        }

        Ok(())
    }

    #[instrument(skip(self), ret)]
    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        let c = self.connection().await?;

        let rows = self
            .prepare_query(
                &c,
                "txn_detail_select_timed_out.sql",
                &[&self.cluster, &now],
            )
            .await?;

        // release the listing connection before aborting: each abort_timed_out below checks
        // out its own connection from the same pool, and the pool can be small
        // (or exactly 1), so holding this one idle risks starving/deadlocking them.
        drop(c);

        for row in rows {
            let transaction_id = row.try_get::<_, String>(0)?;
            let producer_id = row.try_get::<_, i64>(1)?;
            let producer_epoch = row.try_get::<_, i16>(2)?;

            match self
                .abort_timed_out(&transaction_id, producer_id, producer_epoch)
                .await
            {
                Ok(ErrorCode::None) => {}
                // Benign, expected outcome: the sweep lost a race against a real client that
                // already finalized this transaction between the SELECT above and this call.
                // Not an operational problem, so debug! rather than error!.
                Ok(error_code) => debug!(
                    ?error_code,
                    transaction_id,
                    producer_id,
                    producer_epoch,
                    "maintain_transactions: abort rejected"
                ),
                Err(ref err) => error!(
                    ?err,
                    transaction_id, producer_id, producer_epoch, "maintain_transactions"
                ),
            }
        }

        Ok(())
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let c = self.connection().await?;

        self.prepare_execute(
            &c,
            "scram_credential_delete.sql",
            &[&self.cluster, &user, &i32::from(mechanism)],
        )
        .await
        .inspect_err(|err| error!(?err, ?user, ?mechanism,))
        .and(Ok(()))
    }

    async fn upsert_user_scram_credential(
        &self,
        username: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let c = self.connection().await?;

        self.prepare_execute(
            &c,
            "scram_credential_insert.sql",
            &[
                &self.cluster,
                &username,
                &i32::from(mechanism),
                &&credential.salt[..],
                &credential.iterations,
                &&credential.stored_key[..],
                &&credential.server_key[..],
            ],
        )
        .await
        .inspect_err(|err| error!(?err, ?username, ?mechanism,))
        .and(Ok(()))
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let c = self.connection().await?;

        self.prepare_query_opt(
            &c,
            "scram_credential_select.sql",
            &[&self.cluster, &user, &i32::from(mechanism)],
        )
        .await
        .and_then(|maybe| {
            if let Some(row) = maybe {
                let salt = row.try_get::<_, &[u8]>(0).map(Bytes::copy_from_slice)?;
                let iterations = row.try_get::<_, i32>(1)?;
                let stored_key = row.try_get::<_, &[u8]>(2).map(Bytes::copy_from_slice)?;
                let server_key = row.try_get::<_, &[u8]>(3).map(Bytes::copy_from_slice)?;

                Ok(Some(ScramCredential {
                    salt,
                    iterations,
                    stored_key,
                    server_key,
                }))
            } else {
                Ok(None)
            }
        })
        .inspect_err(|err| error!(?err, ?user, ?mechanism,))
    }

    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        let c = self.pool.get().await?;
        let _ = self.prepare_query(&c, "ping.sql", &[]).await?;
        Ok(())
    }
}

static SQL_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sql_duration")
        .with_unit("ms")
        .with_description("The SQL request latencies in milliseconds")
        .build()
});

static SQL_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_sql_requests")
        .with_description("The number of SQL requests made")
        .build()
});

static SQL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_sql_error")
        .with_description("The SQL error count")
        .build()
});

#[cfg(test)]
mod tests {
    use super::*;
    use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use rand::distr::Alphanumeric;

    // mirrors nisshi-broker/tests/common/mod.rs storage_container(StorageType::Postgres)
    const CONNECTION: &str = "postgres://postgres:postgres@localhost";

    fn alphanumeric_string(length: usize) -> String {
        rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    /// An open (uncommitted) transaction must pin the read_committed last stable
    /// offset below the high watermark. Regressed when `offset_stage` read the
    /// high-watermark column instead of the stable-offset column, so
    /// `read_committed` consumers saw uncommitted records.
    #[tokio::test]
    async fn read_committed_last_stable_offset_pinned_by_open_transaction() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        // Probe connectivity explicitly: only a genuinely unreachable postgres
        // should skip. Any later schema/query error must fail the test.
        if let Err(err) = storage.connection().await {
            eprintln!(
                "skipping read_committed_last_stable_offset_pinned_by_open_transaction: {err:?}"
            );
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // a transactional producer begins a transaction and produces, never commits
        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"uncommitted").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        let stage = storage.offset_stage(&topition).await?;

        assert!(
            stage.high_watermark > 0,
            "the uncommitted record should advance the high watermark"
        );
        assert!(
            stage.last_stable < stage.high_watermark,
            "an open transaction must pin last_stable below the high watermark \
             (last_stable={}, high_watermark={})",
            stage.last_stable,
            stage.high_watermark,
        );

        Ok(())
    }

    /// A transaction whose timeout has elapsed without the client calling `EndTxn` (a crashed
    /// or abandoned producer) must be aborted by `maintain_transactions`, releasing the
    /// `read_committed` last stable offset it was pinning.
    #[tokio::test]
    async fn maintain_transactions_aborts_timed_out_transaction() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping maintain_transactions_aborts_timed_out_transaction: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // a transactional producer begins a transaction and produces, never commits
        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"abandoned").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        let stage = storage.offset_stage(&topition).await?;

        assert!(
            stage.last_stable < stage.high_watermark,
            "an open transaction must pin last_stable below the high watermark \
             (last_stable={}, high_watermark={})",
            stage.last_stable,
            stage.high_watermark,
        );

        // no need to actually sleep past the timeout: `now` is a plain parameter,
        // so pass a `now` far enough ahead that the 10s timeout has "elapsed".
        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(3600))
            .await?;

        let stage = storage.offset_stage(&topition).await?;

        assert_eq!(
            stage.high_watermark, stage.last_stable,
            "the timed-out transaction should have been aborted, releasing last_stable \
             (last_stable={}, high_watermark={})",
            stage.last_stable, stage.high_watermark,
        );

        Ok(())
    }

    /// A transaction that hasn't reached its own `transaction_timeout_ms` yet must be left
    /// alone by the sweep -- it should not be aborted just because maintain_transactions ran.
    #[tokio::test]
    async fn maintain_transactions_leaves_transaction_before_timeout() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping maintain_transactions_leaves_transaction_before_timeout: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"still-active").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        let before = storage.offset_stage(&topition).await?;

        // `now` is barely past `started_at`, nowhere near the 10s timeout.
        storage
            .maintain_transactions(SystemTime::now() + Duration::from_millis(50))
            .await?;

        let after = storage.offset_stage(&topition).await?;

        assert_eq!(
            before.last_stable, after.last_stable,
            "a transaction within its timeout must not be touched by the sweep \
             (before={}, after={})",
            before.last_stable, after.last_stable,
        );
        assert!(
            after.last_stable < after.high_watermark,
            "the still-open transaction must remain pinned \
             (last_stable={}, high_watermark={})",
            after.last_stable,
            after.high_watermark,
        );

        Ok(())
    }

    /// When a timed-out transaction overlaps an older, still-open transaction on the same
    /// partition, the sweep must not finalize it out of order: it should stage to
    /// PREPARE_ABORT and leave the last stable offset exactly where the older transaction
    /// pins it, until that older transaction resolves too.
    #[tokio::test]
    async fn maintain_transactions_defers_when_older_overlapping_transaction_still_open()
    -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!(
                "skipping maintain_transactions_defers_when_older_overlapping_transaction_still_open: {err:?}"
            );
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // producer A: opens first, given a long timeout, and never resolves -- it's the
        // oldest open transaction on this partition, so it pins the last stable offset.
        let txn_a = alphanumeric_string(10);
        let producer_a = storage
            .init_producer(Some(txn_a.as_str()), 10_000_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_a.clone(),
                producer_id: producer_a.id,
                producer_epoch: producer_a.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch_a = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"a").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_a.id)
            .producer_epoch(producer_a.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_a.as_str()), &topition, batch_a)
            .await?;

        // producer B: opens second, on the same partition, with a short timeout -- it will
        // be picked up by the sweep, but must defer to A.
        let txn_b = alphanumeric_string(10);
        let producer_b = storage
            .init_producer(Some(txn_b.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_b.clone(),
                producer_id: producer_b.id,
                producer_epoch: producer_b.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch_b = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"b").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_b.id)
            .producer_epoch(producer_b.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_b.as_str()), &topition, batch_b)
            .await?;

        let before = storage.offset_stage(&topition).await?;

        // far enough ahead for B's 10s timeout to have elapsed, nowhere near A's ~10,000s one.
        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(20))
            .await?;

        let after = storage.offset_stage(&topition).await?;

        assert_eq!(
            before.last_stable, after.last_stable,
            "B must not advance last_stable while A (older, still open) remains pinned \
             (before={}, after={})",
            before.last_stable, after.last_stable,
        );
        assert!(
            after.last_stable < after.high_watermark,
            "A is still open, so last_stable must remain pinned below the high watermark \
             (last_stable={}, high_watermark={})",
            after.last_stable,
            after.high_watermark,
        );

        Ok(())
    }

    /// The status guard added to end_in_tx must reject a second, stale call that conflicts
    /// with what already happened -- this is the exact race maintain_transactions and a
    /// genuinely concurrent client EndTxn could hit: a candidate found by the sweep's query
    /// that a real client commits first, then the sweep's stale abort arrives late. The guard
    /// must not silently ack the sweep's abort as success (that would tell the caller their
    /// request "worked" when the transaction was actually already committed).
    #[tokio::test]
    async fn txn_end_after_already_finalized_with_conflicting_outcome_is_rejected() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping txn_end_after_already_finalized_is_a_no_op: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"raced").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        // the "real client" wins the race and commits first.
        _ = storage
            .txn_end(&transaction_id, producer.id, producer.epoch, true)
            .await?;

        let after_commit = storage.offset_stage(&topition).await?;

        assert_eq!(
            after_commit.last_stable, after_commit.high_watermark,
            "the committed transaction should have released last_stable \
             (last_stable={}, high_watermark={})",
            after_commit.last_stable, after_commit.high_watermark,
        );

        // the sweep's stale view still thinks it should abort the same transaction -- but the
        // real client already committed it. This must be rejected, not silently acked.
        let error_code = storage
            .txn_end(&transaction_id, producer.id, producer.epoch, false)
            .await?;

        assert_eq!(
            ErrorCode::InvalidTxnState,
            error_code,
            "a stale abort arriving after a real commit must be rejected, not acked as \
             success -- the caller has no other way to learn their request didn't apply"
        );

        let after_conflict = storage.offset_stage(&topition).await?;

        assert_eq!(
            after_commit.high_watermark, after_conflict.high_watermark,
            "the rejected call must not append a second control-batch marker \
             (high_watermark before={}, after={})",
            after_commit.high_watermark, after_conflict.high_watermark,
        );

        Ok(())
    }

    /// A retry (or a genuine race between a real EndTxn and the sweep) that lands while a
    /// transaction is deferred (PREPARE_ABORT/PREPARE_COMMIT -- an older, still-open
    /// transaction on the same partition hasn't resolved yet) must not write a second control
    /// marker. Deferral means the marker was already written on the first call; only the
    /// overlap check should re-run.
    #[tokio::test]
    async fn deferred_txn_end_retry_does_not_duplicate_marker() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping deferred_txn_end_retry_does_not_duplicate_marker: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // producer A: opens first, never resolves -- pins B behind it.
        let txn_a = alphanumeric_string(10);
        let producer_a = storage
            .init_producer(Some(txn_a.as_str()), 10_000_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_a.clone(),
                producer_id: producer_a.id,
                producer_epoch: producer_a.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch_a = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"a").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_a.id)
            .producer_epoch(producer_a.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_a.as_str()), &topition, batch_a)
            .await?;

        // producer B: opens second, overlapping A -- must defer.
        let txn_b = alphanumeric_string(10);
        let producer_b = storage
            .init_producer(Some(txn_b.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_b.clone(),
                producer_id: producer_b.id,
                producer_epoch: producer_b.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch_b = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"b").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_b.id)
            .producer_epoch(producer_b.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_b.as_str()), &topition, batch_b)
            .await?;

        // first call: B defers (A is still open), writing its abort marker but only reaching
        // PREPARE_ABORT, not the terminal ABORTED the guard checks for.
        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&txn_b, producer_b.id, producer_b.epoch, false)
                .await?
        );

        // second call: simulates a retry, or the sweep racing a real client's EndTxn, while B
        // is still sitting in PREPARE_ABORT. The guard should recognize this as already
        // handled and no-op -- if it doesn't, a second marker gets written.
        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&txn_b, producer_b.id, producer_b.epoch, false)
                .await?
        );

        // Two calls while deferred must still result in exactly one control-batch marker
        // record for producer B on this partition -- a real Kafka client seeing two
        // conflicting markers for the same producer is undefined/nonsensical behavior. Counted
        // directly against the record table (aborted_transactions isn't wired up yet at this
        // point in the stack).
        let c = storage.connection().await?;
        let marker_count: i64 = c
            .query_one(
                "select count(*) from cluster c \
                 join topic t on t.cluster = c.id \
                 join topition tp on tp.topic = t.id \
                 join record r on r.topition = tp.id \
                 where c.name = $1 and t.name = $2 and tp.partition = $3 \
                 and r.producer_id = $4 and (r.attributes & 32) = 32",
                &[&cluster, &topic_name, &0i32, &producer_b.id],
            )
            .await?
            .try_get(0)?;

        assert_eq!(
            1, marker_count,
            "expected exactly one control-batch marker for producer B, got {marker_count}",
        );

        Ok(())
    }

    /// Once maintain_transactions sweep-aborts a timed-out transaction, it also fences the
    /// producer's epoch -- so a LATE real EndTxn(commit=true) for that same transaction (e.g.
    /// a slow producer that was declared dead but is actually still alive and finally calls
    /// commit) must be rejected with ProducerFenced, not acked as success -- the data was
    /// already irreversibly reported as aborted, so telling the caller their commit "worked"
    /// would be a lie, and ProducerFenced (rather than a generic InvalidTxnState) is what
    /// tells a real Kafka client library to stop retrying and rebuild its producer.
    #[tokio::test]
    async fn late_commit_after_sweep_abort_is_rejected() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping late_commit_after_sweep_abort_is_rejected: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"slow-producer").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        // the sweep declares this transaction dead and aborts it.
        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(3600))
            .await?;

        let stage = storage.offset_stage(&topition).await?;
        assert_eq!(
            stage.high_watermark, stage.last_stable,
            "the sweep should have aborted the timed-out transaction"
        );

        // the "slow" producer, unaware it's been declared dead, finally calls commit for real.
        let late_commit_result = storage
            .txn_end(&transaction_id, producer.id, producer.epoch, true)
            .await?;

        assert_eq!(
            ErrorCode::ProducerFenced,
            late_commit_result,
            "a late commit() after a sweep-abort must be rejected as ProducerFenced (the \
             sweep fences the producer's epoch on timeout-abort), not acked as success",
        );

        Ok(())
    }

    async fn txn_status(
        storage: &Postgres,
        cluster: &str,
        transaction_id: &str,
        producer: &ProducerIdResponse,
    ) -> Result<Option<String>> {
        let c = storage.connection().await?;

        let row = c
            .query_one(
                "select txn_d.status \
                 from cluster c \
                 join producer p on p.cluster = c.id \
                 join producer_epoch pe on pe.producer = p.id \
                 join txn on txn.cluster = c.id and txn.producer = p.id \
                 join txn_detail txn_d on txn_d.\"transaction\" = txn.id \
                 and txn_d.producer_epoch = pe.id \
                 where c.name = $1 and txn.name = $2 and p.id = $3 and pe.epoch = $4",
                &[
                    &cluster.to_owned(),
                    &transaction_id.to_owned(),
                    &producer.id,
                    &producer.epoch,
                ],
            )
            .await?;

        row.try_get::<_, Option<String>>(0).map_err(Into::into)
    }

    /// Finalizing one epoch's transaction must delete only that epoch's txn_topition and
    /// txn_produce_offset bookkeeping. The delete_by_txn queries once matched every epoch of
    /// the (transaction, producer). Reproduced via an epoch-0 abort deferred in PREPARE_ABORT
    /// (re-init leaves non-BEGIN transactions alone, so the epoch bump keeps it deferred):
    /// its retained bookkeeping is what eventually tells the deferred abort which partitions
    /// get markers, so epoch 1's finalize wiping it loses those markers entirely.
    #[tokio::test]
    async fn txn_end_scopes_bookkeeping_delete_to_its_own_epoch() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping txn_end_scopes_bookkeeping_delete_to_its_own_epoch: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(2)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let partition_0 = Topition::new(topic_name.clone(), 0);
        let partition_1 = Topition::new(topic_name.clone(), 1);

        // pinning producer: an open transaction on partition 0 that epoch 0's abort will
        // defer behind.
        let txn_pin = alphanumeric_string(10);
        let producer_pin = storage
            .init_producer(Some(txn_pin.as_str()), 10_000_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_pin.clone(),
                producer_id: producer_pin.id,
                producer_epoch: producer_pin.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let batch_pin = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"pin").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_pin.id)
            .producer_epoch(producer_pin.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_pin.as_str()), &partition_0, batch_pin)
            .await?;

        // epoch 0: produce on partition 0 behind the pin, then abort -- defers to
        // PREPARE_ABORT, keeping its bookkeeping rows.
        let txn_a = alphanumeric_string(10);
        let epoch0 = storage
            .init_producer(Some(txn_a.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_a.clone(),
                producer_id: epoch0.id,
                producer_epoch: epoch0.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let batch_epoch0 = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"a").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(epoch0.id)
            .producer_epoch(epoch0.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_a.as_str()), &partition_0, batch_epoch0)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&txn_a, epoch0.id, epoch0.epoch, false)
                .await?
        );
        assert_eq!(
            Some("PREPARE_ABORT".to_owned()),
            txn_status(&storage, &cluster, &txn_a, &epoch0).await?,
            "epoch 0's abort should defer behind the pinning transaction",
        );

        // reconnect: PREPARE_ABORT is not BEGIN, so re-init bumps the epoch without touching
        // the deferred transaction.
        let epoch1 = storage
            .init_producer(Some(txn_a.as_str()), 10_000, Some(-1), Some(-1))
            .await?;
        assert_eq!(epoch0.id, epoch1.id);
        assert_eq!(epoch0.epoch + 1, epoch1.epoch);

        // epoch 1: transact on partition 1 (no overlap with the pin) and abort -- finalizes
        // immediately, running the bookkeeping deletes.
        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_a.clone(),
                producer_id: epoch1.id,
                producer_epoch: epoch1.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![1])),
                ],
            })
            .await?;

        let batch_epoch1 = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"b").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(epoch1.id)
            .producer_epoch(epoch1.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_a.as_str()), &partition_1, batch_epoch1)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&txn_a, epoch1.id, epoch1.epoch, false)
                .await?
        );
        assert_eq!(
            Some("ABORTED".to_owned()),
            txn_status(&storage, &cluster, &txn_a, &epoch1).await?,
            "epoch 1 has no overlap and must finalize immediately, running the deletes",
        );

        let bookkeeping = |epoch: &ProducerIdResponse| {
            let storage = &storage;
            let cluster = &cluster;
            let txn_a = &txn_a;
            let (id, epoch) = (epoch.id, epoch.epoch);
            async move {
                let c = storage.connection().await?;
                let row = c
                    .query_one(
                        "select \
                         count(distinct txn_tp.id) as topitions, \
                         count(txn_po.txn_topition) as produce_offsets \
                         from cluster c \
                         join producer p on p.cluster = c.id \
                         join producer_epoch pe on pe.producer = p.id \
                         join txn on txn.cluster = c.id and txn.producer = p.id \
                         join txn_detail txn_d on txn_d.\"transaction\" = txn.id \
                         and txn_d.producer_epoch = pe.id \
                         join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id \
                         left join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id \
                         where c.name = $1 and txn.name = $2 and p.id = $3 and pe.epoch = $4",
                        &[cluster, txn_a, &id, &epoch],
                    )
                    .await?;
                Ok::<_, Error>((row.try_get::<_, i64>(0)?, row.try_get::<_, i64>(1)?))
            }
        };

        assert_eq!(
            (0, 0),
            bookkeeping(&epoch1).await?,
            "epoch 1's own bookkeeping should be deleted by its finalize",
        );
        assert_eq!(
            (1, 1),
            bookkeeping(&epoch0).await?,
            "epoch 1's finalize must not delete the deferred epoch 0 transaction's \
             bookkeeping -- it is what tells the deferred abort which partitions get markers",
        );

        Ok(())
    }

    /// Stress the race end_in_tx's status guard exists to close: a genuinely concurrent
    /// live commit (as a real client's EndTxn) and the maintain_transactions sweep's abort,
    /// both racing txn_end on the exact same transaction. Recovered and adapted from an
    /// earlier, unmerged exploration (orhayat/fix/pg-txn-timeout-abort) that found this race
    /// before the guard existed; run here to confirm the guard actually holds under real
    /// concurrency, not just the sequential simulation above.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn race_concurrent_commit_and_abort() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping race_concurrent_commit_and_abort: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let iterations = 100;
        let mut commit_wins = 0;
        let mut abort_wins = 0;
        let mut both_ok = 0;
        let mut both_err = 0;
        let mut anomalies: Vec<String> = vec![];

        for i in 0..iterations {
            let transaction_id = alphanumeric_string(10);

            let producer = storage
                .init_producer(Some(transaction_id.as_str()), 60_000, Some(-1), Some(-1))
                .await?;

            _ = storage
                .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                    transaction_id: transaction_id.clone(),
                    producer_id: producer.id,
                    producer_epoch: producer.epoch,
                    topics: vec![
                        AddPartitionsToTxnTopic::default()
                            .name(topic_name.clone())
                            .partitions(Some((0..num_partitions).collect())),
                    ],
                })
                .await?;

            let batch = Batch::builder()
                .record(Record::builder().value(Bytes::from_static(b"v").into()))
                .attributes(BatchAttribute::default().transaction(true).into())
                .producer_id(producer.id)
                .producer_epoch(producer.epoch)
                .base_sequence(0)
                .build()
                .and_then(TryInto::try_into)?;

            _ = storage
                .produce(Some(transaction_id.as_str()), &topition, batch)
                .await?;

            let wm_before = storage.offset_stage(&topition).await?.high_watermark;

            let (s1, s2) = (storage.clone(), storage.clone());
            let (t1, t2) = (transaction_id.clone(), transaction_id.clone());
            let (pid, ep) = (producer.id, producer.epoch);

            let commit = tokio::spawn(async move { s1.txn_end(&t1, pid, ep, true).await });
            let abort = tokio::spawn(async move { s2.txn_end(&t2, pid, ep, false).await });

            let r_commit = commit.await.expect("commit task panicked");
            let r_abort = abort.await.expect("abort task panicked");

            let wm_after = storage.offset_stage(&topition).await?.high_watermark;
            let markers = wm_after - wm_before;
            let status = txn_status(&storage, &cluster, &transaction_id, &producer).await?;

            match (r_commit.is_ok(), r_abort.is_ok()) {
                (true, false) => commit_wins += 1,
                (false, true) => abort_wins += 1,
                (true, true) => both_ok += 1,
                (false, false) => both_err += 1,
            }

            // The end_in_tx status guard makes the loser of the race a clean
            // Ok(ErrorCode::None) no-op too (matching how Kafka treats a duplicate
            // EndTxn), so both calls returning Ok is expected -- it no longer signals
            // which side "won". The real invariants: exactly one control marker ever
            // lands, and the transaction settles into a definite terminal state.
            let consistent =
                status.as_deref() == Some("COMMITTED") || status.as_deref() == Some("ABORTED");
            if markers != 1 || !consistent {
                anomalies.push(format!(
                    "iter {i}: markers={markers} commit={r_commit:?} abort={r_abort:?} status={status:?}"
                ));
            }
        }

        eprintln!(
            "summary over {iterations}: commit_wins={commit_wins} abort_wins={abort_wins} both_ok={both_ok} both_err={both_err} anomalies={}",
            anomalies.len()
        );

        assert!(
            anomalies.is_empty(),
            "{} anomalies (expected exactly 1 marker + status matching the winner every time):\n{}",
            anomalies.len(),
            anomalies.join("\n"),
        );

        Ok(())
    }

    /// The actual data-safety guarantee fencing exists for: once maintain_transactions
    /// sweep-aborts a timed-out transaction, a still-alive "zombie" producer trying to send
    /// MORE data under its old epoch must be rejected outright -- not silently accepted and
    /// later delivered to read_committed consumers as if it were ordinary committed data.
    #[tokio::test]
    async fn sweep_fenced_producer_zombie_produce_is_rejected() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping sweep_fenced_producer_zombie_produce_is_rejected: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"before-sweep").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        // the sweep declares this producer's transaction dead -- and must fence it.
        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(3600))
            .await?;

        let stage_before_zombie_write = storage.offset_stage(&topition).await?;

        // the "zombie" producer, still alive, tries to send more data under its old epoch.
        let zombie_batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"zombie-write").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(1)
            .build()
            .and_then(TryInto::try_into)?;

        let zombie_result = storage
            .produce(Some(transaction_id.as_str()), &topition, zombie_batch)
            .await;

        assert!(
            matches!(zombie_result, Err(Error::Api(ErrorCode::ProducerFenced))),
            "a zombie produce under the pre-sweep epoch must be rejected as ProducerFenced, \
             got {zombie_result:?}",
        );

        let stage_after_zombie_write = storage.offset_stage(&topition).await?;

        assert_eq!(
            stage_before_zombie_write.high_watermark, stage_after_zombie_write.high_watermark,
            "the rejected zombie write must not have been appended to the log",
        );

        Ok(())
    }

    /// A producer fenced by the sweep must still be able to recover normally -- reconnecting
    /// via InitProducerId(-1, -1) for the same transactional.id gets a fresh epoch strictly
    /// after the sweep's fence, and can then produce/commit normally under it.
    #[tokio::test]
    async fn sweep_fenced_producer_can_reconnect_and_produce_normally() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping sweep_fenced_producer_can_reconnect_and_produce_normally: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"before-sweep").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(3600))
            .await?;

        let reconnected = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        assert_eq!(
            producer.id, reconnected.id,
            "reconnect must keep the same stable producer_id"
        );
        assert!(
            reconnected.epoch > producer.epoch,
            "reconnect must return an epoch strictly after the sweep's fence (was {}, now {})",
            producer.epoch,
            reconnected.epoch,
        );

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: reconnected.id,
                producer_epoch: reconnected.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"after-reconnect").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(reconnected.id)
            .producer_epoch(reconnected.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&transaction_id, reconnected.id, reconnected.epoch, true)
                .await?,
            "the reconnected producer's transaction must commit normally",
        );

        Ok(())
    }

    /// EndTxn for a producer id that was never issued must be UnknownProducerId -- the
    /// identity check runs before any transaction-state lookup, so this cannot be
    /// misreported as a transaction-state problem.
    #[tokio::test]
    async fn txn_end_for_unknown_producer_is_unknown_producer_id() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping txn_end_for_unknown_producer_is_unknown_producer_id: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        assert_eq!(
            ErrorCode::UnknownProducerId,
            storage
                .txn_end(alphanumeric_string(10).as_str(), i64::MAX, 0, false)
                .await?
        );

        Ok(())
    }

    /// EndTxn carrying an epoch NEWER than the broker ever issued is a protocol violation
    /// (the client invented an epoch), reported as InvalidProducerEpoch -- distinct from
    /// ProducerFenced, which tells a stale producer a NEWER epoch exists and it should
    /// rebuild; here nothing newer exists and retrying with a rebuilt producer won't help.
    #[tokio::test]
    async fn txn_end_with_future_epoch_is_invalid_producer_epoch() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping txn_end_with_future_epoch_is_invalid_producer_epoch: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        assert_eq!(
            ErrorCode::InvalidProducerEpoch,
            storage
                .txn_end(&transaction_id, producer.id, producer.epoch + 1, true)
                .await?
        );

        Ok(())
    }

    /// A retried sweep-abort (the next maintain tick, or another broker instance sharing
    /// the database) carries the victim's original epoch, which the first call's fencing
    /// made stale -- so the retry must be stopped at the identity check as ProducerFenced,
    /// bumping no second epoch and writing no second control marker.
    #[tokio::test]
    async fn sweep_abort_retry_is_fenced_and_does_not_double_bump() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping sweep_abort_retry_is_fenced_and_does_not_double_bump: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // pinning producer: an older open transaction the victim's abort will defer behind,
        // so the victim sits in PREPARE_ABORT when the retry arrives.
        let txn_pin = alphanumeric_string(10);
        let producer_pin = storage
            .init_producer(Some(txn_pin.as_str()), 10_000_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_pin.clone(),
                producer_id: producer_pin.id,
                producer_epoch: producer_pin.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let batch_pin = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"pin").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_pin.id)
            .producer_epoch(producer_pin.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_pin.as_str()), &topition, batch_pin)
            .await?;

        let txn_victim = alphanumeric_string(10);
        let victim = storage
            .init_producer(Some(txn_victim.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_victim.clone(),
                producer_id: victim.id,
                producer_epoch: victim.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let batch_victim = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"v").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(victim.id)
            .producer_epoch(victim.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_victim.as_str()), &topition, batch_victim)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .abort_timed_out(&txn_victim, victim.id, victim.epoch)
                .await?
        );
        assert_eq!(
            Some("PREPARE_ABORT".to_owned()),
            txn_status(&storage, &cluster, &txn_victim, &victim).await?,
            "the victim's abort should defer behind the pinning transaction",
        );

        assert_eq!(
            ErrorCode::ProducerFenced,
            storage
                .abort_timed_out(&txn_victim, victim.id, victim.epoch)
                .await?,
            "the retry carries the epoch the first call fenced",
        );

        let c = storage.connection().await?;

        let current_epoch: i16 = c
            .query_one(
                "select max(pe.epoch) from cluster c \
                 join producer p on p.cluster = c.id \
                 join producer_epoch pe on pe.producer = p.id \
                 where c.name = $1 and p.id = $2",
                &[&cluster, &victim.id],
            )
            .await?
            .try_get(0)?;

        assert_eq!(
            victim.epoch + 1,
            current_epoch,
            "two sweep-abort calls must fence exactly once",
        );

        let marker_count: i64 = c
            .query_one(
                "select count(*) from cluster c \
                 join topic t on t.cluster = c.id \
                 join topition tp on tp.topic = t.id \
                 join record r on r.topition = tp.id \
                 where c.name = $1 and t.name = $2 and tp.partition = $3 \
                 and r.producer_id = $4 and (r.attributes & 32) = 32",
                &[&cluster, &topic_name, &0i32, &victim.id],
            )
            .await?
            .try_get(0)?;

        assert_eq!(
            1, marker_count,
            "the fenced retry must not write a second control marker",
        );

        Ok(())
    }

    /// InitProducerId API versions <= 2 have no wire representation for producer_id/epoch at
    /// all, so they decode as (None, None) -- must be treated as a fresh-epoch request, not
    /// panic.
    #[tokio::test]
    async fn init_producer_old_api_version_is_treated_as_fresh() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping init_producer_old_api_version_is_treated_as_fresh: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let transaction_id = alphanumeric_string(10);

        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;

        assert_eq!(ErrorCode::None, producer.error);

        Ok(())
    }

    /// A v3+ client presenting its current, still-valid (producer_id, producer_epoch) -- the
    /// KIP-360 recovery shape -- must be validated against the record and, once confirmed,
    /// bumped to a new epoch exactly like a fresh (-1, -1) request.
    #[tokio::test]
    async fn init_producer_recovery_with_current_epoch_succeeds() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping init_producer_recovery_with_current_epoch_succeeds: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let transaction_id = alphanumeric_string(10);

        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        let recovered = storage
            .init_producer(
                Some(transaction_id.as_str()),
                10_000,
                Some(producer.id),
                Some(producer.epoch),
            )
            .await?;

        assert_eq!(ErrorCode::None, recovered.error);
        assert_eq!(producer.id, recovered.id);
        assert!(
            recovered.epoch > producer.epoch,
            "a validated recovery request must still bump the epoch (was {}, now {})",
            producer.epoch,
            recovered.epoch,
        );

        Ok(())
    }

    /// A v3+ client presenting a STALE (producer_id, producer_epoch) -- exactly what happens
    /// after maintain_transactions' sweep fences a timed-out producer -- must be rejected as
    /// ProducerFenced, not silently granted a new epoch (and not panic).
    #[tokio::test]
    async fn init_producer_recovery_with_stale_epoch_is_fenced() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping init_producer_recovery_with_stale_epoch_is_fenced: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let transaction_id = alphanumeric_string(10);

        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        // someone else bumps the epoch (a reconnect, or the sweep fencing this producer),
        // making `producer`'s epoch stale.
        _ = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        let stale_recovery = storage
            .init_producer(
                Some(transaction_id.as_str()),
                10_000,
                Some(producer.id),
                Some(producer.epoch),
            )
            .await?;

        assert_eq!(
            ErrorCode::ProducerFenced,
            stale_recovery.error,
            "a recovery request carrying a stale epoch must be rejected as ProducerFenced"
        );

        Ok(())
    }

    /// A malformed InitProducerId request -- one of producer_id/producer_epoch present
    /// without the other -- is not a valid shape under any protocol version and must be
    /// rejected, not panic.
    #[tokio::test]
    async fn init_producer_malformed_partial_fields_is_rejected() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping init_producer_malformed_partial_fields_is_rejected: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let transaction_id = alphanumeric_string(10);

        let response = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(42), None)
            .await?;

        assert_eq!(ErrorCode::InvalidRequest, response.error);

        Ok(())
    }

    /// AddPartitionsToTxn is idempotent in Kafka: a partition already in the transaction is
    /// a no-op, not an error. Clients do re-send it -- a retry, or a produce racing the
    /// first add -- and the unique (txn_detail, topition) constraint turned that into a
    /// database error surfacing as a broken connection mid-transaction.
    #[tokio::test]
    async fn txn_add_partitions_is_idempotent() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping txn_add_partitions_is_idempotent: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        let request = || TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: transaction_id.clone(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name(topic_name.clone())
                    .partitions(Some((0..num_partitions).collect())),
            ],
        };

        for attempt in 1..=2 {
            let response = storage
                .txn_add_partitions(request())
                .await
                .inspect_err(|err| {
                    panic!("add #{attempt} of the same partition must succeed, got {err:?}")
                })?;

            for topic in response.zero_to_three() {
                for partition in topic.results_by_partition.as_deref().unwrap_or_default() {
                    assert_eq!(
                        i16::from(ErrorCode::None),
                        partition.partition_error_code,
                        "add #{attempt} of the same partition must report no error",
                    );
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn aborted_transactions_reports_aborted_producer_range() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping aborted_transactions_reports_aborted_producer_range: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"abc").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        let produced_offset = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&transaction_id, producer.id, producer.epoch, false)
                .await?
        );

        let stage = storage.offset_stage(&topition).await?;

        let aborted = storage
            .aborted_transactions(&topition, produced_offset, stage.last_stable)
            .await?;

        assert_eq!(
            1,
            aborted.len(),
            "expected exactly one aborted transaction, got {aborted:?}"
        );
        assert_eq!(producer.id, aborted[0].producer_id);
        assert_eq!(produced_offset, aborted[0].first_offset);

        Ok(())
    }

    /// Step 3 integration test: goes through the real `FetchService` (not
    /// `Storage::aborted_transactions` called directly, like the test above), proving the
    /// actual wire-facing `FetchResponse` -- after an abort -- both (a) still contains the
    /// aborted producer's data (real Kafka returns it and reports it separately, it doesn't
    /// strip it server-side; this locks that decision in so a future reader doesn't "fix" it
    /// into stripping content by mistake) and (b) has the correct `aborted_transactions` entry.
    #[tokio::test]
    async fn fetch_service_reports_aborted_transactions() -> Result<()> {
        use crate::FetchService;
        use nisshi_sans_io::{
            FetchRequest,
            fetch_request::{FetchPartition, FetchTopic},
        };
        use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};

        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping fetch_service_reports_aborted_transactions: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let secret_payload = Bytes::from_static(b"visible-but-flagged-as-aborted");

        let batch = Batch::builder()
            .record(Record::builder().value(secret_payload.clone().into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        let produced_offset = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&transaction_id, producer.id, producer.epoch, false)
                .await?
        );

        let fetch_service = MapStateLayer::new(move |_| storage.clone()).into_layer(FetchService);

        let response = fetch_service
            .serve(
                Context::default(),
                FetchRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadCommitted.into()))
                    .topics(Some(
                        [FetchTopic::default()
                            .topic(Some(topic_name.clone()))
                            .partitions(Some(
                                [FetchPartition::default()
                                    .partition(0)
                                    .fetch_offset(produced_offset)
                                    .partition_max_bytes(50 * 1024)]
                                .into(),
                            ))]
                        .into(),
                    ))
                    .max_bytes(Some(50 * 1024))
                    .max_wait_ms(100),
            )
            .await?;

        let topics = response.responses.unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.clone().unwrap_or_default();
        assert_eq!(1, partitions.len());

        let partition = &partitions[0];

        // (a) the data is still there -- not stripped server-side.
        let batches = partition
            .records
            .as_ref()
            .map(|frame| frame.batches.clone())
            .unwrap_or_default()
            .into_iter()
            .map(Batch::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let contains_secret = batches
            .iter()
            .flat_map(|batch| batch.records.iter())
            .any(|record| record.value == Some(secret_payload.clone()));

        assert!(
            contains_secret,
            "the aborted producer's data must still be returned (real Kafka doesn't strip \
             server-side, it flags it) -- got batches: {batches:?}"
        );

        // (b) and it's correctly flagged as aborted.
        let aborted = partition.aborted_transactions.clone().unwrap_or_default();
        assert_eq!(
            1,
            aborted.len(),
            "expected exactly one aborted transaction in the FetchResponse, got {aborted:?}"
        );
        assert_eq!(producer.id, aborted[0].producer_id);
        assert_eq!(produced_offset, aborted[0].first_offset);

        Ok(())
    }

    /// A committed transaction must never appear in `aborted_transactions` -- only aborts do.
    #[tokio::test]
    async fn aborted_transactions_excludes_committed_producer() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping aborted_transactions_excludes_committed_producer: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"committed").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        let produced_offset = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&transaction_id, producer.id, producer.epoch, true)
                .await?
        );

        let stage = storage.offset_stage(&topition).await?;

        let aborted = storage
            .aborted_transactions(&topition, produced_offset, stage.last_stable)
            .await?;

        assert!(
            aborted.is_empty(),
            "a committed transaction must never appear in aborted_transactions, got {aborted:?}"
        );

        Ok(())
    }

    /// Two different producers interleave writes on the same partition; one aborts. Proves the
    /// reported range is scoped to just the aborting producer's own data -- a naive "previous
    /// batch in offset order" approach would get this wrong, since the batch right before the
    /// abort marker in the log could belong to the OTHER producer.
    #[tokio::test]
    async fn aborted_transactions_handles_interleaved_producers() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping aborted_transactions_handles_interleaved_producers: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let txn_a = alphanumeric_string(10);
        let producer_a = storage
            .init_producer(Some(txn_a.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        let txn_b = alphanumeric_string(10);
        let producer_b = storage
            .init_producer(Some(txn_b.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_a.clone(),
                producer_id: producer_a.id,
                producer_epoch: producer_a.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_b.clone(),
                producer_id: producer_b.id,
                producer_epoch: producer_b.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        // genuinely interleaved on the same partition: A, B, A, B
        let a_first_offset = storage
            .produce(
                Some(txn_a.as_str()),
                &topition,
                Batch::builder()
                    .record(Record::builder().value(Bytes::from_static(b"a0").into()))
                    .attributes(BatchAttribute::default().transaction(true).into())
                    .producer_id(producer_a.id)
                    .producer_epoch(producer_a.epoch)
                    .base_sequence(0)
                    .build()
                    .and_then(TryInto::try_into)?,
            )
            .await?;

        _ = storage
            .produce(
                Some(txn_b.as_str()),
                &topition,
                Batch::builder()
                    .record(Record::builder().value(Bytes::from_static(b"b0").into()))
                    .attributes(BatchAttribute::default().transaction(true).into())
                    .producer_id(producer_b.id)
                    .producer_epoch(producer_b.epoch)
                    .base_sequence(0)
                    .build()
                    .and_then(TryInto::try_into)?,
            )
            .await?;

        _ = storage
            .produce(
                Some(txn_a.as_str()),
                &topition,
                Batch::builder()
                    .record(Record::builder().value(Bytes::from_static(b"a1").into()))
                    .attributes(BatchAttribute::default().transaction(true).into())
                    .producer_id(producer_a.id)
                    .producer_epoch(producer_a.epoch)
                    .base_sequence(1)
                    .build()
                    .and_then(TryInto::try_into)?,
            )
            .await?;

        _ = storage
            .produce(
                Some(txn_b.as_str()),
                &topition,
                Batch::builder()
                    .record(Record::builder().value(Bytes::from_static(b"b1").into()))
                    .attributes(BatchAttribute::default().transaction(true).into())
                    .producer_id(producer_b.id)
                    .producer_epoch(producer_b.epoch)
                    .base_sequence(1)
                    .build()
                    .and_then(TryInto::try_into)?,
            )
            .await?;

        // A is the older transaction (touched this partition first) so it can resolve
        // immediately; B, resolving right after, isn't blocked by anything older still open.
        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&txn_a, producer_a.id, producer_a.epoch, false)
                .await?
        );
        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&txn_b, producer_b.id, producer_b.epoch, true)
                .await?
        );

        let stage = storage.offset_stage(&topition).await?;

        let aborted = storage
            .aborted_transactions(&topition, a_first_offset, stage.last_stable)
            .await?;

        assert_eq!(
            1,
            aborted.len(),
            "expected exactly one aborted transaction (A), got {aborted:?}"
        );
        assert_eq!(producer_a.id, aborted[0].producer_id);
        assert_eq!(
            a_first_offset, aborted[0].first_offset,
            "A's reported range must start at A's own first offset, not contaminated by B's \
             interleaved records -- got {aborted:?}"
        );

        Ok(())
    }

    /// One producer runs three transactions in sequence on the same partition (abort, commit,
    /// abort). Proves each abort is reported with its own correct, independent first_offset --
    /// exercises walking back across more than one prior transaction for the same producer, not
    /// just "was there ever an earlier abort."
    #[tokio::test]
    async fn aborted_transactions_uses_previous_marker_across_sequential_transactions() -> Result<()>
    {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!(
                "skipping aborted_transactions_uses_previous_marker_across_sequential_transactions: {err:?}"
            );
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        // (payload, commit?) for three sequential transactions from the SAME producer/epoch
        let plan: [(&[u8], bool); 3] = [
            (b"txn1-abort", false),
            (b"txn2-commit", true),
            (b"txn3-abort", false),
        ];
        let mut offsets = vec![];

        for (sequence, (payload, committed)) in plan.into_iter().enumerate() {
            _ = storage
                .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                    transaction_id: transaction_id.clone(),
                    producer_id: producer.id,
                    producer_epoch: producer.epoch,
                    topics: vec![
                        AddPartitionsToTxnTopic::default()
                            .name(topic_name.clone())
                            .partitions(Some((0..num_partitions).collect())),
                    ],
                })
                .await?;

            // idempotent-producer sequence numbers must strictly increase per producer/epoch,
            // even across separate transactions sharing that epoch -- reusing 0 every time
            // gets rejected as a duplicate.
            let batch = Batch::builder()
                .record(Record::builder().value(Bytes::copy_from_slice(payload).into()))
                .attributes(BatchAttribute::default().transaction(true).into())
                .producer_id(producer.id)
                .producer_epoch(producer.epoch)
                .base_sequence(sequence as i32)
                .build()
                .and_then(TryInto::try_into)?;

            let offset = storage
                .produce(Some(transaction_id.as_str()), &topition, batch)
                .await?;
            offsets.push(offset);

            assert_eq!(
                ErrorCode::None,
                storage
                    .txn_end(&transaction_id, producer.id, producer.epoch, committed)
                    .await?
            );
        }

        let stage = storage.offset_stage(&topition).await?;

        let mut aborted = storage
            .aborted_transactions(&topition, offsets[0], stage.last_stable)
            .await?;

        assert_eq!(
            2,
            aborted.len(),
            "expected exactly two aborted transactions (txn1 and txn3), got {aborted:?}"
        );

        aborted.sort_by_key(|a| a.first_offset);

        assert_eq!(
            offsets[0], aborted[0].first_offset,
            "txn1's reported first_offset must be its own start, got {aborted:?}"
        );
        assert_eq!(
            offsets[2], aborted[1].first_offset,
            "txn3's reported first_offset must be its own start, not txn1's or txn2's, \
             got {aborted:?}"
        );

        Ok(())
    }

    /// Requests a range starting after the aborted transaction's true first offset (a fetch
    /// resuming mid-transaction). Proves first_offset is clamped to the requested start rather
    /// than reporting something earlier than what was actually fetched.
    #[tokio::test]
    async fn aborted_transactions_clamps_to_fetch_window() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping aborted_transactions_clamps_to_fetch_window: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        // two records in the same transaction: true_first_offset, true_first_offset + 1
        let true_first_offset = storage
            .produce(
                Some(transaction_id.as_str()),
                &topition,
                Batch::builder()
                    .record(Record::builder().value(Bytes::from_static(b"r0").into()))
                    .attributes(BatchAttribute::default().transaction(true).into())
                    .producer_id(producer.id)
                    .producer_epoch(producer.epoch)
                    .base_sequence(0)
                    .build()
                    .and_then(TryInto::try_into)?,
            )
            .await?;

        let second_offset = storage
            .produce(
                Some(transaction_id.as_str()),
                &topition,
                Batch::builder()
                    .record(Record::builder().value(Bytes::from_static(b"r1").into()))
                    .attributes(BatchAttribute::default().transaction(true).into())
                    .producer_id(producer.id)
                    .producer_epoch(producer.epoch)
                    .base_sequence(1)
                    .build()
                    .and_then(TryInto::try_into)?,
            )
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&transaction_id, producer.id, producer.epoch, false)
                .await?
        );

        let stage = storage.offset_stage(&topition).await?;

        // ask starting at the SECOND record, skipping the true first offset entirely
        let aborted = storage
            .aborted_transactions(&topition, second_offset, stage.last_stable)
            .await?;

        assert_eq!(
            1,
            aborted.len(),
            "expected one aborted transaction, got {aborted:?}"
        );
        assert_eq!(producer.id, aborted[0].producer_id);
        assert_eq!(
            second_offset, aborted[0].first_offset,
            "must clamp to the requested fetch start ({second_offset}), not the transaction's \
             true first offset ({true_first_offset}) which wasn't part of what was fetched"
        );

        Ok(())
    }

    /// `read_uncommitted` fetches must never populate `aborted_transactions`, even when there's
    /// a real abort in range -- this gate lives in `fetch.rs`, not the Postgres method, so it
    /// can only be verified at the `FetchService` layer.
    #[tokio::test]
    async fn aborted_transactions_empty_for_read_uncommitted() -> Result<()> {
        use crate::FetchService;
        use nisshi_sans_io::{
            FetchRequest,
            fetch_request::{FetchPartition, FetchTopic},
        };
        use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};

        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping aborted_transactions_empty_for_read_uncommitted: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"abc").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        let produced_offset = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&transaction_id, producer.id, producer.epoch, false)
                .await?
        );

        let fetch_service = MapStateLayer::new(move |_| storage.clone()).into_layer(FetchService);

        let response = fetch_service
            .serve(
                Context::default(),
                FetchRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .topics(Some(
                        [FetchTopic::default()
                            .topic(Some(topic_name.clone()))
                            .partitions(Some(
                                [FetchPartition::default()
                                    .partition(0)
                                    .fetch_offset(produced_offset)
                                    .partition_max_bytes(50 * 1024)]
                                .into(),
                            ))]
                        .into(),
                    ))
                    .max_bytes(Some(50 * 1024))
                    .max_wait_ms(100),
            )
            .await?;

        let topics = response.responses.unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.clone().unwrap_or_default();
        assert_eq!(1, partitions.len());

        let aborted = partitions[0]
            .aborted_transactions
            .clone()
            .unwrap_or_default();
        assert!(
            aborted.is_empty(),
            "read_uncommitted must never report aborted_transactions, got {aborted:?}"
        );

        Ok(())
    }

    /// A transaction aborted by the `maintain_transactions` sweep (timeout-based, no real
    /// `EndTxn` call) must be reported identically to one aborted via a real client call --
    /// both go through the same `end_in_tx`, so this should "just work," but this test guards
    /// against a future change that special-cases one path.
    #[tokio::test]
    async fn aborted_transactions_reported_for_sweep_aborted_transaction() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!(
                "skipping aborted_transactions_reported_for_sweep_aborted_transaction: {err:?}"
            );
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // a transactional producer begins a transaction and produces, never commits or aborts
        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"abandoned").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        let produced_offset = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        // no real EndTxn -- the sweep finds and aborts it instead
        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(3600))
            .await?;

        let stage = storage.offset_stage(&topition).await?;
        assert_eq!(
            stage.high_watermark, stage.last_stable,
            "the sweep should have aborted the abandoned transaction"
        );

        let aborted = storage
            .aborted_transactions(&topition, produced_offset, stage.last_stable)
            .await?;

        assert_eq!(
            1,
            aborted.len(),
            "expected the sweep-aborted transaction to be reported, got {aborted:?}"
        );
        assert_eq!(producer.id, aborted[0].producer_id);
        assert_eq!(produced_offset, aborted[0].first_offset);

        Ok(())
    }

    /// aborted_transactions's lag() window must look back across a producer's full history on
    /// this partition, not just its current epoch. A producer that commits under one epoch,
    /// then reconnects (bumping epoch) and aborts under the next epoch, must report a range
    /// starting at its own (epoch 1) data -- not swallow the earlier, already-COMMITTED epoch
    /// 0 data because lag() found nothing to look back to within epoch 1 alone.
    #[tokio::test]
    async fn aborted_transactions_looks_back_across_producer_epochs() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping aborted_transactions_looks_back_across_producer_epochs: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);
        let transaction_id = alphanumeric_string(10);

        // epoch 0: produce one record and COMMIT.
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch_epoch0 = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"committed-epoch-0").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        let epoch0_offset = storage
            .produce(Some(transaction_id.as_str()), &topition, batch_epoch0)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&transaction_id, producer.id, producer.epoch, true)
                .await?
        );

        // reconnect: same transactional_id, same producer_id, bumped epoch.
        let producer_epoch1 = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        assert_eq!(
            producer.id, producer_epoch1.id,
            "reconnect must keep the same stable producer_id"
        );
        assert!(
            producer_epoch1.epoch > producer.epoch,
            "reconnect must bump the epoch (was {}, now {})",
            producer.epoch,
            producer_epoch1.epoch
        );

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer_epoch1.id,
                producer_epoch: producer_epoch1.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        // epoch 1: produce one record and ABORT. This is the transaction that's actually
        // aborted.
        let batch_epoch1 = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"aborted-epoch-1").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_epoch1.id)
            .producer_epoch(producer_epoch1.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        let epoch1_offset = storage
            .produce(Some(transaction_id.as_str()), &topition, batch_epoch1)
            .await?;

        assert!(
            epoch1_offset > epoch0_offset,
            "epoch 1's data must come after epoch 0's committed data"
        );

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(
                    &transaction_id,
                    producer_epoch1.id,
                    producer_epoch1.epoch,
                    false
                )
                .await?
        );

        let stage = storage.offset_stage(&topition).await?;

        // fetch from the very beginning of the partition, as a real read_committed consumer
        // catching up from offset 0 would.
        let aborted = storage
            .aborted_transactions(&topition, 0, stage.last_stable)
            .await?;

        assert_eq!(
            1,
            aborted.len(),
            "expected exactly one aborted range, got {aborted:?}"
        );
        assert_eq!(producer.id, aborted[0].producer_id);

        // first_offset must be epoch 1's own start -- not 0 (the fetch's start), which would
        // wrongly swallow epoch 0's already-committed record into the aborted range too.
        assert_eq!(
            epoch1_offset, aborted[0].first_offset,
            "aborted range's first_offset is {} but should be {} (epoch 1's own start) -- got \
             offset {} (epoch 0's COMMITTED record) instead",
            aborted[0].first_offset, epoch1_offset, epoch0_offset,
        );

        Ok(())
    }

    /// Kafka permits TxnOffsetCommit to be sent repeatedly within one transaction -- a
    /// retry after a lost response, or a later sendOffsetsToTransaction for the same
    /// partition -- and the latest committed_offset must win. The staging inserts have no
    /// conflict handling, so the second commit for the same (transaction, consumer group)
    /// violates txn_offset_commit's unique constraint and fails the whole request.
    #[tokio::test]
    async fn txn_offset_commit_second_stage_overwrites_first() -> Result<()> {
        use nisshi_sans_io::txn_offset_commit_request::{
            TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
        };

        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(CONNECTION)?
            .cluster(cluster.as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping txn_offset_commit_second_stage_overwrites_first: {err:?}");
            return Ok(());
        }

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let group_id = alphanumeric_string(10);

        let stage = |committed_offset: i64| TxnOffsetCommitRequest {
            transaction_id: transaction_id.clone(),
            group_id: group_id.clone(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            generation_id: Some(-1),
            member_id: Some("".into()),
            group_instance_id: None,
            topics: vec![
                TxnOffsetCommitRequestTopic::default()
                    .name(topic_name.clone())
                    .partitions(Some(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .partition_index(0)
                            .committed_offset(committed_offset)
                            .committed_leader_epoch(Some(-1))
                            .committed_metadata(None),
                    ])),
            ],
        };

        let per_partition_error = |topics: &[TxnOffsetCommitResponseTopic]| {
            topics
                .iter()
                .flat_map(|topic| topic.partitions.iter().flatten())
                .map(|partition| partition.error_code)
                .collect::<Vec<_>>()
        };

        let first = storage.txn_offset_commit(stage(5)).await?;
        assert_eq!(
            vec![i16::from(ErrorCode::None)],
            per_partition_error(&first),
        );

        let second = storage.txn_offset_commit(stage(9)).await?;
        assert_eq!(
            vec![i16::from(ErrorCode::None)],
            per_partition_error(&second),
            "a repeated TxnOffsetCommit must overwrite the staged offset, not error",
        );

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&transaction_id, producer.id, producer.epoch, true)
                .await?
        );

        let c = storage.connection().await?;
        let committed: i64 = c
            .query_one(
                "select co.committed_offset from cluster c \
                 join consumer_group cg on cg.cluster = c.id \
                 join topic t on t.cluster = c.id \
                 join topition tp on tp.topic = t.id \
                 join consumer_offset co on co.consumer_group = cg.id and co.topition = tp.id \
                 where c.name = $1 and cg.name = $2 and t.name = $3 and tp.partition = $4",
                &[&cluster, &group_id, &topic_name, &0i32],
            )
            .await?
            .try_get(0)?;

        assert_eq!(
            9, committed,
            "the transaction's commit must apply the LATEST staged offset",
        );

        Ok(())
    }
}
