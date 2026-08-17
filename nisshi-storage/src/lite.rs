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
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Debug,
    marker::PhantomData,
    ops::Deref,
    path::PathBuf,
    result,
    str::FromStr,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime},
};

use crate::{
    BrokerRegistrationRequest, ChannelRequestLayer, Error, GroupDetail, ListOffsetResponse, METER,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
    RequestChannelService, RequestStorageService, Result, ScramCredential, Storage, TopicId,
    Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState,
    UpdateError, Version, bounded_channel,
    proxy::SemaphoreProxy,
    sql::{Cache, default_hash, idempotent_sequence_check, remove_comments},
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::NaiveDateTime;
use deadpool::managed;
use libsql::{
    Connection, Database, Row, Rows, Statement, Transaction, TransactionBehavior, Value,
    ffi::SQLITE_CONSTRAINT_UNIQUE, params::IntoParams,
};
use nisshi_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, OpType, ScramMechanism,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
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
    record::{Header, Record, deflated, inflated},
    to_system_time, to_timestamp,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use nisshi_schema::{
    Registry,
    lake::{House, LakeHouse as _},
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use rama::{Context, Layer as _, Service as _};
use rand::{rng, seq::SliceRandom as _};
use regex::Regex;
use tokio::{fs::rename, sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, warn};
use url::Url;
use uuid::Uuid;

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

static SQL_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_duration")
        .with_unit("ms")
        .with_description("The SQL request latencies in milliseconds")
        .build()
});

static CONNECT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_connect_duration")
        .with_unit("ms")
        .with_description("The connection latencies in milliseconds")
        .build()
});

static PRODUCE_IN_TX_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_produce_in_tx_duration")
        .with_unit("ms")
        .with_description("The produce in TX latencies in milliseconds")
        .build()
});

static TRANSACTION_WITH_BEHAVIOR_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_transaction_with_behavior_duration")
        .with_unit("ms")
        .with_description("The transaction with behavior latencies in milliseconds")
        .build()
});

static TRANSACTION_COMMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_transaction_commit_duration")
        .with_unit("ms")
        .with_description("The transaction commit latencies in milliseconds")
        .build()
});

static ENGINE_REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_engine_request_duration")
        .with_unit("ms")
        .with_description("The engine latencies in milliseconds")
        .build()
});

static DELEGATE_REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_delegate_request_duration")
        .with_boundaries(
            [
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0,
                1000.0,
            ]
            .into(),
        )
        .with_unit("ms")
        .with_description("The engine latencies in milliseconds")
        .build()
});

static SQL_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_sqlite_requests")
        .with_description("The number of SQL requests made")
        .build()
});

static SQL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_sqlite_error")
        .with_description("The SQL error count")
        .build()
});

fn elapsed_millis(start: SystemTime) -> u64 {
    start
        .elapsed()
        .map_or(0, |duration| duration.as_millis() as u64)
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
        let name = row.get::<String>(0).inspect_err(|err| error!(?err))?;
        let producer_id = row.get::<i64>(1).inspect_err(|err| error!(?err))?;
        let producer_epoch = row.get::<i32>(2).inspect_err(|err| error!(?err))? as i16;
        let status = row
            .get::<Option<String>>(3)
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

fn is_unique_constraint(error: &libsql::Error) -> bool {
    matches!(
        error,
        libsql::Error::SqliteFailure(SQLITE_CONSTRAINT_UNIQUE, _)
    )
}

/// LibSQL/SQLite storage engine
///
#[derive(Clone, Debug)]
pub(crate) struct Delegate {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    pool: Pool,

    schemas: Option<Registry>,

    lake: Option<House>,

    vacuum_into: Option<PathBuf>,

    maintenance: Arc<Semaphore>,
    compaction: CompactionMode,
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionManager {
    db: Arc<Mutex<Database>>,
    busy_timeout: Duration,
}

pub(crate) struct PoolConnection {
    connection: Connection,
}

impl Debug for PoolConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolConnection")
            .field("connection", &self.connection)
            .finish()
    }
}

impl PoolConnection {
    async fn journal_mode(connection: &Connection) -> Result<()> {
        let mut rows = connection.query("PRAGMA journal_mode = WAL", ()).await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            debug!(journal_mode = row.get_str(0)?);
        }

        Ok(())
    }

    async fn synchronous(connection: &Connection) -> Result<()> {
        _ = connection
            .execute("PRAGMA synchronous = normal", ())
            .await
            .inspect(|rows| debug!(rows))?;

        let mut rows = connection.query("PRAGMA synchronous", ()).await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            debug!(synchronous = row.get_str(0)?);
        }

        Ok(())
    }

    async fn wal_autocheckpoint(connection: &Connection) -> Result<()> {
        let mut rows = connection.query("PRAGMA wal_autocheckpoint", ()).await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            debug!(wal_autocheckpoint = row.get_str(0)?);
        }

        Ok(())
    }

    async fn journal_size_limit(connection: &Connection) -> Result<()> {
        let mut rows = connection.query("PRAGMA journal_size_limit", ()).await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            debug!(journal_size_limit = row.get_str(0)?);
        }

        Ok(())
    }

    async fn foreign_keys(connection: &Connection) -> Result<()> {
        connection
            .execute("PRAGMA foreign_keys = ON", ())
            .await
            .map_err(Into::into)
            .and(Ok(()))
    }

    async fn init(connection: &Connection) -> Result<()> {
        Self::journal_mode(connection).await?;
        Self::synchronous(connection).await?;
        Self::wal_autocheckpoint(connection).await?;
        Self::journal_size_limit(connection).await?;
        Self::foreign_keys(connection).await?;
        Ok(())
    }

    async fn new(connection: Connection) -> Result<Self> {
        Self::init(&connection).await?;

        Ok(Self { connection })
    }

    #[instrument(skip(self))]
    async fn prepared_statement(&self, key: &str) -> Result<Statement, libsql::Error> {
        let sql = SQL
            .0
            .get(key)
            .ok_or(libsql::Error::Misuse(format!("Unknown cache key: {}", key)))?;

        self.connection.prepare(sql).await
    }

    fn attributes_for_error(&self, key: Option<&str>, error: &libsql::Error) -> Vec<KeyValue> {
        debug!(key, ?error);

        let mut attributes = if let Some(sql) = key {
            vec![KeyValue::new("key", sql.to_owned())]
        } else {
            vec![]
        };

        if let libsql::Error::SqliteFailure(code, _) = error {
            attributes.push(KeyValue::new("code", format!("{code}")));
        }

        attributes
    }

    #[instrument(skip_all)]
    async fn transaction(&self) -> Result<Transaction> {
        let start = SystemTime::now();

        self.connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .inspect(|_tx| TRANSACTION_WITH_BEHAVIOR_DURATION.record(elapsed_millis(start), &[]))
            .inspect_err(|err| {
                error!(?err, elapsed_millis = elapsed_millis(start));

                SQL_ERROR.add(1, &self.attributes_for_error(None, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn commit(&self, tx: Transaction) -> Result<()> {
        let start = SystemTime::now();

        tx.commit()
            .await
            .inspect(|_| TRANSACTION_COMMIT_DURATION.record(elapsed_millis(start), &[]))
            .inspect_err(|err| {
                error!(?err, elapsed_millis = elapsed_millis(start));
                SQL_ERROR.add(1, &self.attributes_for_error(None, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn query<P>(&self, sql: &str, params: P) -> result::Result<Rows, libsql::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(sql, ?params);

        let start = SystemTime::now();

        let statement = self.prepared_statement(sql).await?;

        statement
            .query(params)
            .await
            .inspect(|rows| {
                debug!(?rows);

                SQL_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("sql", sql.to_owned())],
                );

                SQL_REQUESTS.add(1, &[KeyValue::new("sql", sql.to_owned())]);
            })
            .inspect_err(|err| {
                error!(?err, elapsed_millis = elapsed_millis(start));

                SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
            })
    }

    #[instrument(skip_all)]
    async fn execute<P>(&self, sql: &str, params: P) -> result::Result<usize, libsql::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(sql, ?params);
        let start = SystemTime::now();

        let statement = self.prepared_statement(sql).await?;

        statement
            .execute(params)
            .await
            .inspect(|rows| {
                let elapsed_millis = elapsed_millis(start);
                debug!(rows, elapsed_millis);

                SQL_DURATION.record(elapsed_millis, &[KeyValue::new("sql", sql.to_owned())]);
                SQL_REQUESTS.add(1, &[KeyValue::new("sql", sql.to_owned())]);
            })
            .inspect_err(|err| {
                error!(?err, sql, elapsed_millis = elapsed_millis(start));

                SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
            })
    }

    #[instrument(skip_all)]
    async fn query_opt<P>(&self, sql: &str, params: P) -> result::Result<Option<Row>, libsql::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(sql, ?params);

        let start = SystemTime::now();

        let statement = self.prepared_statement(sql).await?;

        let mut rows = statement.query(params).await.inspect_err(|err| {
            error!(?err, elapsed_millis = elapsed_millis(start));
            SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
        })?;

        let row = rows.next().await.inspect_err(|err| {
            error!(?err, elapsed_millis = elapsed_millis(start));
            SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
        })?;

        let attributes = [KeyValue::new("sql", sql.to_owned())];

        SQL_DURATION.record(elapsed_millis(start), &attributes);
        SQL_REQUESTS.add(1, &attributes);

        Ok(row)
    }

    #[instrument(skip_all)]
    async fn query_one<P>(&self, sql: &str, params: P) -> result::Result<Row, libsql::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(sql, ?params);

        let start = SystemTime::now();

        let statement = self.prepared_statement(sql).await?;

        let mut rows = statement
            .query(params)
            .await
            .inspect(|rows| debug!(?rows))
            .inspect_err(|err| {
                if is_unique_constraint(err) {
                    debug!(?err);
                } else {
                    error!(?err, elapsed_millis = elapsed_millis(start));
                    SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
                }
            })?;

        if let Some(row) = rows
            .next()
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| {
                if is_unique_constraint(err) {
                    debug!(?err);
                } else {
                    error!(?err, elapsed_millis = elapsed_millis(start));
                    SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
                }
            })?
        {
            let attributes = [KeyValue::new("sql", sql.to_owned())];

            SQL_DURATION.record(elapsed_millis(start), &attributes);

            SQL_REQUESTS.add(1, &attributes);

            Ok(row).inspect(|row| debug!(?row))
        } else {
            Err(libsql::Error::Misuse(format!("sql={sql}")))
        }
    }
}

impl managed::Manager for ConnectionManager {
    type Type = PoolConnection;
    type Error = Error;

    #[instrument(skip_all)]
    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let start = SystemTime::now();

        let connection = {
            let db = self.db.lock()?;
            db.connect()
        }?;

        connection.busy_timeout(self.busy_timeout)?;

        PoolConnection::new(connection)
            .await
            .inspect(|_| CONNECT_DURATION.record(elapsed_millis(start), &[]))
    }

    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &managed::Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        Ok(())
    }
}

pub(crate) type Pool = managed::Pool<ConnectionManager>;

impl Delegate {
    #[instrument(skip_all)]
    async fn connection(&self) -> Result<managed::Object<ConnectionManager>> {
        let start = SystemTime::now();

        self.pool.get().await.map_err(Into::into).inspect(|_| {
            CONNECT_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("cluster_id", self.cluster.clone())],
            )
        })
    }

    #[instrument(skip_all)]
    async fn idempotent_message_check(
        &self,
        _transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &PoolConnection,
    ) -> Result<()> {
        let mut rows = connection
            .query(
                "producer_epoch_current_for_producer.sql",
                (self.cluster.as_str(), deflated.producer_id),
            )
            .await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            let current_epoch = row
                .get::<i32>(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))?
                as i16;

            let row = connection
                .query_one(
                    "producer_select_for_update.sql",
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        deflated.producer_id,
                        deflated.producer_epoch,
                    ),
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

            let sequence = row.get::<i32>(0).inspect_err(|err| error!(?err))?;

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
                connection
                    .execute(
                        "producer_detail_insert.sql",
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            deflated.producer_id,
                            deflated.producer_epoch,
                            increment,
                        ),
                    )
                    .await?
            );

            Ok(())
        } else {
            Err(Error::Api(ErrorCode::UnknownProducerId))
        }
    }

    #[instrument(skip_all)]
    async fn watermark_select_for_update(
        &self,
        topition: &Topition,
        connection: &PoolConnection,
    ) -> Result<(Option<i64>, Option<i64>)> {
        debug!(?topition);

        let mut rows = connection
            .query(
                "watermark_select_no_update.sql",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        if let Some(row) = rows
            .next()
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
        {
            Ok((
                row.get::<Option<i64>>(0).inspect_err(|err| error!(?err))?,
                row.get::<Option<i64>>(1).inspect_err(|err| error!(?err))?,
            ))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    #[instrument(skip_all)]
    async fn produce_in_tx(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        connection: &PoolConnection,
    ) -> Result<i64> {
        let start = SystemTime::now();

        let topic = topition.topic();
        let partition = topition.partition();

        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, connection)
                .await
                .inspect_err(|err| error!(?err))?;
        }

        debug!(after_idempotent_check = elapsed_millis(start));

        let (low, high) = self
            .watermark_select_for_update(topition, connection)
            .await
            .inspect_err(|err| error!(?err))?;

        debug!(after_watermark_select_for_update = elapsed_millis(start));

        debug!(?low, ?high);

        let inflated = inflated::Batch::try_from(deflated).inspect_err(|err| error!(?err))?;

        debug!(after_inflate = elapsed_millis(start));

        let attributes = BatchAttribute::try_from(inflated.attributes)?;

        debug!(after_attributes = elapsed_millis(start));

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

        debug!(after_validation = elapsed_millis(start));

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
            for (delta, record) in inflated.records.iter().enumerate() {
                debug!(delta, elapsed = elapsed_millis(start));

                let delta = i64::try_from(delta)?;
                let offset = high.unwrap_or_default() + delta;
                let key = record.key.as_deref();
                let value = record.value.as_deref();

                debug!(?delta, ?offset);

                _ = connection
                    .execute(
                        "record_insert.sql",
                        (
                            self.cluster.as_str(),
                            topic,
                            partition,
                            offset,
                            inflated.attributes,
                            if transaction_id.is_none() {
                                None
                            } else {
                                Some(inflated.producer_id)
                            },
                            if transaction_id.is_none() {
                                None
                            } else {
                                Some(inflated.producer_epoch)
                            },
                            inflated.base_timestamp + record.timestamp_delta,
                            key,
                            value,
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err, ?topic, ?partition, ?offset, ?key, ?value))
                    .map_err(unique_constraint(ErrorCode::UnknownServerError))?;

                debug!(delta, after_record_insert = elapsed_millis(start));

                for header in record.headers.iter().as_ref() {
                    let key = header.key.as_deref();
                    let value = header.value.as_deref();

                    _ = connection
                        .execute(
                            "header_insert.sql",
                            (self.cluster.as_str(), topic, partition, offset, key, value),
                        )
                        .await
                        .inspect_err(|err| {
                            error!(?err, ?topic, ?partition, ?offset, ?key, ?value);
                        });
                }

                debug!(delta, after_header_insert = elapsed_millis(start));
            }

            debug!(after_record_insert = elapsed_millis(start));

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                let offset_start = high.unwrap_or_default();
                let offset_end = high.map_or(last_offset_delta, |high| high + last_offset_delta);

                _ = connection
                        .execute(
                            "txn_produce_offset_insert.sql",
                            (
                                self.cluster.as_str(),
                                transaction_id,
                                inflated.producer_id,
                                inflated.producer_epoch,
                                topic,
                                partition,
                                offset_start,
                                offset_end,
                            ),
                        )
                        .await
                        .inspect(|n| debug!(cluster = ?self.cluster, ?transaction_id, ?inflated.producer_id, ?inflated.producer_epoch, ?topic, ?partition, ?offset_start, ?offset_end, ?n))
                        .inspect_err(|err| error!(?err))?;
            }

            debug!(after_some_transaction_id = elapsed_millis(start));
        }

        _ = connection
            .execute(
                "watermark_update.sql",
                (
                    self.cluster.as_str(),
                    topic,
                    partition,
                    low.unwrap_or_default(),
                    high.map_or(last_offset_delta + 1, |high| high + last_offset_delta + 1),
                ),
            )
            .await
            .inspect(|n| debug!(?n, after_watermark_update = elapsed_millis(start)))
            .inspect_err(|err| error!(?err))?;

        if !attributes.control
            && let Some(ref lake) = self.lake
        {
            let config = self
                .describe_config(topition.topic(), ConfigResource::Topic, None)
                .await
                .inspect_err(|err| error!(?err))?;

            lake.store(
                topition.topic(),
                topition.partition(),
                high.unwrap_or_default(),
                &inflated,
                config,
            )
            .await
            .inspect_err(|err| error!(?err))?;
        }

        debug!(after_all_done = elapsed_millis(start));

        Ok(high.unwrap_or_default()).inspect(|_| {
            PRODUCE_IN_TX_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("cluster_id", self.cluster.clone())],
            );
        })
    }

    async fn end_in_tx(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        connection: &PoolConnection,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?committed);

        let mut overlaps = vec![];

        let mut rows = connection
            .query(
                "txn_select_produced_topitions.sql",
                (
                    self.cluster.as_str(),
                    transaction_id,
                    producer_id,
                    producer_epoch,
                ),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            let topic = row.get::<String>(0)?;
            let partition = row.get::<i32>(1)?;

            let topition = Topition::new(topic.clone(), partition);

            debug!(?topition);

            let control_batch: Bytes = if committed {
                ControlBatch::default().commit().try_into()?
            } else {
                ControlBatch::default().abort().try_into()?
            };
            let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

            let batch = inflated::Batch::builder()
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
                .produce_in_tx(Some(transaction_id), &topition, batch, connection)
                .await?;

            debug!(offset, ?topition);

            let mut rows = connection
                .query(
                    "txn_produce_offset_select_offset_range.sql",
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                        topic.as_str(),
                        partition,
                    ),
                )
                .await?;

            if let Some(row) = rows.next().await? {
                let offset_start = row.get::<i64>(0)?;
                let offset_end = row.get::<i64>(1)?;
                debug!(offset_start, offset_end);

                let mut rows = connection
                    .query(
                        "txn_produce_offset_select_overlapping_txn.sql",
                        (
                            self.cluster.as_str(),
                            transaction_id,
                            producer_id,
                            producer_epoch,
                            topic.as_str(),
                            partition,
                            offset_end,
                        ),
                    )
                    .await?;

                while let Some(row) = rows.next().await? {
                    overlaps.push(Txn::try_from(row).inspect(|txn| debug!(?txn))?);
                }
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

                _ = connection
                    .execute(
                        "txn_produce_offset_delete_by_txn.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = connection
                    .execute(
                        "txn_topition_delete_by_txn.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                if txn.status == TxnState::PrepareCommit {
                    _ = connection
                        .execute(
                            "consumer_offset_insert_from_txn.sql",
                            (
                                self.cluster.as_str(),
                                txn.name.as_str(),
                                txn.producer_id,
                                txn.producer_epoch,
                            ),
                        )
                        .await?;
                }

                _ = connection
                    .execute(
                        "txn_offset_commit_tp_delete_by_txn.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = connection
                    .execute(
                        "txn_offset_commit_delete_by_txn.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                let outcome = if txn.status == TxnState::PrepareCommit {
                    String::from(TxnState::Committed)
                } else if txn.status == TxnState::PrepareAbort {
                    String::from(TxnState::Aborted)
                } else {
                    String::from(txn.status)
                };

                _ = connection
                    .execute(
                        "txn_status_update.sql",
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                            outcome,
                        ),
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

            _ = connection
                .execute(
                    "txn_status_update.sql",
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                        outcome.as_str(),
                    ),
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

    #[instrument(skip(self), ret)]
    async fn policy_compact_delete(&self, topition: i64, offset_id: i64) -> Result<u64> {
        let pc = self.connection().await?;

        pc.execute("lite/policy_compact_delete.sql", (topition, offset_id))
            .await
            .map(|rows| rows as u64)
            .inspect(|rows| debug!(rows))
            .map_err(Into::into)
    }

    #[instrument(skip(self))]
    async fn policy_compact_compaction(
        &self,
        topition: i64,
        key: &[u8],
        max_offset_id: i64,
    ) -> Result<Vec<i64>> {
        let pc = self.connection().await?;

        let mut rows = pc
            .query(
                "lite/policy_compact_compaction.sql",
                (topition, key, max_offset_id),
            )
            .await?;

        let mut offsets = Vec::new();

        while let Some(row) = rows.next().await? {
            let offset = row.get::<i64>(0)?;
            offsets.push(offset);
        }

        Ok(offsets)
    }

    #[instrument(skip(self))]
    async fn policy_compact_max_offset_id(&self, topition: i64, key: &[u8]) -> Result<Option<i64>> {
        let pc = self.connection().await?;

        if let Some(row) = pc
            .query_opt("lite/policy_compact_max_offset_id.sql", (topition, key))
            .await?
        {
            row.get::<i64>(0).map(Some).map_err(Into::into)
        } else {
            Ok(None)
        }
        .inspect(|max_offset| debug!(max_offset))
    }

    #[instrument(skip(self))]
    async fn policy_compact_distinct_k(&self, topition: i64) -> Result<BTreeSet<Vec<u8>>> {
        let pc = self.connection().await?;

        let mut rows = pc
            .query("lite/policy_compact_distinct_k.sql", [topition])
            .await?;

        let mut keys = BTreeSet::new();

        while let Some(row) = rows.next().await? {
            if let Some(key) = row.get::<Option<Vec<u8>>>(0)? {
                _ = keys.insert(key);
            }
        }

        debug!(keys = keys.len());

        Ok(keys)
    }

    #[instrument(skip(self))]
    async fn policy_compact_topitions(&self) -> Result<BTreeSet<i64>> {
        let pc = self.connection().await?;

        let mut rows = pc
            .query("lite/policy_compact_topitions.sql", [self.cluster.as_str()])
            .await?;

        let mut topitions = BTreeSet::new();

        while let Some(row) = rows.next().await? {
            let topition = row.get::<i64>(0)?;
            _ = topitions.insert(topition);
        }

        Ok(topitions)
    }

    #[instrument(skip(self))]
    async fn policy_compact(&self) -> Result<u64> {
        let start = SystemTime::now();

        match self.compaction {
            CompactionMode::Single => {
                let pc = self.connection().await?;

                pc.execute("policy_compact.sql", [self.cluster.as_str()])
                    .await
                    .map(|compacted| compacted as u64)
                    .inspect(|_| {
                        DELEGATE_REQUEST_DURATION.record(
                            elapsed_millis(start),
                            &[KeyValue::new("operation", "policy_compact_single")],
                        )
                    })
                    .map_err(Into::into)
            }
            CompactionMode::Multi => {
                let mut compacted = 0;

                for topition in self.policy_compact_topitions().await? {
                    debug!(topition);

                    for key in self.policy_compact_distinct_k(topition).await? {
                        debug!(key = ?&key[..]);

                        if let Some(max_offset_id) = self
                            .policy_compact_max_offset_id(topition, &key[..])
                            .await?
                        {
                            debug!(max_offset_id);

                            for offset in self
                                .policy_compact_compaction(topition, &key[..], max_offset_id)
                                .await?
                            {
                                debug!(offset);

                                compacted += self.policy_compact_delete(topition, offset).await?;
                            }
                        }
                    }
                }

                Ok(compacted).inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[KeyValue::new("operation", "policy_compact_multi")],
                    )
                })
            }
        }
    }

    #[instrument(skip(self))]
    async fn vacuum_into(&self) -> Result<()> {
        if let Some(vacuum_into) = self
            .vacuum_into
            .as_deref()
            .inspect(|vacuum_into| debug!(vacuum_into = vacuum_into.to_str()))
        {
            let mut temporary = PathBuf::from(vacuum_into);
            if temporary.add_extension("temporary") {
                debug!(temporary = temporary.to_str());

                if let Some(vacuum_temporary) = temporary.to_str() {
                    let pc = self.connection().await?;
                    let rows = pc
                        .execute("lite/vacuum_into.sql", [vacuum_temporary])
                        .await? as u64;

                    rename(temporary, vacuum_into).await?;
                    debug!(vacuum_into = vacuum_into.to_str(), rows);
                }
            }
        }

        Ok(())
    }

    #[instrument(skip(self, now), ret)]
    async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        let start = SystemTime::now();

        let now = to_timestamp(&now)?;
        let retention_ms = u64::try_from(Duration::from_hours(7 * 24).as_millis())?;

        let pc = self.connection().await?;

        pc.execute(
            "lite/policy_delete.sql",
            (self.cluster.as_str(), now, retention_ms),
        )
        .await
        .map(|deleted| deleted as u64)
        .map_err(Into::into)
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "policy_delete")],
            )
        })
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

        let row = c
            .query_one(
                "virtual_topic_upsert.sql",
                (self.cluster.as_str(), topic, key, uuid.to_string()),
            )
            .await?;

        row.get_str(0)
            .map_err(Error::from)
            .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
            .inspect(|vt| debug!(%vt))
    }
}

#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, L, D> {
    cluster: C,
    node: N,
    advertised_listener: L,
    storage: D,
    schemas: Option<Registry>,
    lake: Option<House>,
    cancellation: CancellationToken,
}

impl<C, N, L, D> Builder<C, N, L, D> {
    pub(crate) fn cluster<T>(self, cluster: T) -> Builder<String, N, L, D>
    where
        T: Into<String>,
    {
        Builder {
            cluster: cluster.into(),
            node: self.node,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn node(self, node: i32) -> Builder<C, i32, L, D> {
        debug!(node);
        Builder {
            cluster: self.cluster,
            node,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn advertised_listener(self, advertised_listener: Url) -> Builder<C, N, Url, D> {
        debug!(%advertised_listener);
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn storage(self, storage: Url) -> Builder<C, N, L, Url> {
        debug!(%storage);
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            storage,
            schemas: self.schemas,
            lake: self.lake,
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, D> {
        Self { schemas, ..self }
    }

    pub(crate) fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }

    pub(crate) fn cancellation(self, cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            ..self
        }
    }
}

static DDL: LazyLock<Cache> = LazyLock::new(|| {
    let mapping = [
        ("010-cluster.sql", include_sql!("ddl/010-cluster.sql")),
        (
            "020-consumer-group.sql",
            include_sql!("ddl/020-consumer-group.sql"),
        ),
        ("020-producer.sql", include_sql!("ddl/020-producer.sql")),
        (
            "020-scram-credential.sql",
            include_sql!("ddl/020-scram-credential.sql"),
        ),
        ("020-topic.sql", include_sql!("ddl/020-topic.sql")),
        (
            "030-consumer-group-detail.sql",
            include_sql!("ddl/030-consumer-group-detail.sql"),
        ),
        (
            "030-producer-epoch.sql",
            include_sql!("ddl/030-producer-epoch.sql"),
        ),
        (
            "030-topic-configuration.sql",
            include_sql!("ddl/030-topic-configuration.sql"),
        ),
        ("030-topition.sql", include_sql!("ddl/030-topition.sql")),
        ("030-txn.sql", include_sql!("ddl/030-txn.sql")),
        (
            "030-virtual-topic.sql",
            include_sql!("ddl/030-virtual-topic.sql"),
        ),
        (
            "040-consumer-offset.sql",
            include_sql!("ddl/040-consumer-offset.sql"),
        ),
        ("040-header.sql", include_sql!("ddl/040-header.sql")),
        (
            "040-producer-detail.sql",
            include_sql!("ddl/040-producer-detail.sql"),
        ),
        ("040-record.sql", include_sql!("ddl/040-record.sql")),
        ("040-txn-detail.sql", include_sql!("ddl/040-txn-detail.sql")),
        ("040-watermark.sql", include_sql!("ddl/040-watermark.sql")),
        (
            "050-txn-offset-commit.sql",
            include_sql!("ddl/050-txn-offset-commit.sql"),
        ),
        (
            "050-txn-topition.sql",
            include_sql!("ddl/050-txn-topition.sql"),
        ),
        (
            "060-txn-offset-commit-tp.sql",
            include_sql!("ddl/060-txn-offset-commit-tp.sql"),
        ),
        (
            "060-txn-produce-offset.sql",
            include_sql!("ddl/060-txn-produce-offset.sql"),
        ),
    ];

    Cache::new(BTreeMap::from(mapping))
});

pub(crate) static SQL: LazyLock<Cache> = LazyLock::new(|| {
    Cache::new(
        crate::sql::SQL
            .iter()
            .map(|(name, sql)| fix_parameters(sql).map(|sql| (*name, sql)))
            .collect::<Result<BTreeMap<_, _>>>()
            .unwrap_or_default(),
    )
});

fn fix_parameters(sql: &str) -> Result<String> {
    Regex::new(r"\$(?<i>\d+)")
        .map(|re| re.replace_all(sql, "?$i").into_owned())
        .map_err(Into::into)
}

#[derive(Clone, Debug)]
pub struct Engine {
    #[allow(dead_code)]
    server: Arc<JoinSet<Result<(), Error>>>,
    inner: RequestChannelService,
}

impl Engine {
    pub fn builder()
    -> Builder<PhantomData<String>, PhantomData<i32>, PhantomData<Url>, PhantomData<Url>> {
        Builder::default()
    }
}

#[async_trait]
impl Storage for Engine {
    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        let start = SystemTime::now();
        self.inner
            .register_broker(broker_registration)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "register_broker")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let start = SystemTime::now();
        self.inner.brokers().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "brokers")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        let start = SystemTime::now();
        self.inner
            .create_topic(topic, validate_only)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "create_topic")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let start = SystemTime::now();
        self.inner.delete_records(topics).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_records")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let start = SystemTime::now();
        self.inner.delete_topic(topic).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_topic")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let start = SystemTime::now();
        self.inner
            .incremental_alter_resource(resource)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "incremental_alter_resource")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let start = SystemTime::now();
        self.inner
            .produce(transaction_id, topition, deflated)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "produce")],
                )
            })
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
        let start = SystemTime::now();
        self.inner
            .fetch(
                topition,
                offset,
                min_bytes,
                max_bytes,
                isolation_level,
                max_wait,
            )
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "fetch")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let start = SystemTime::now();
        self.inner.offset_stage(topition).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "offset_stage")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let start = SystemTime::now();
        self.inner
            .offset_commit(group, retention, offsets)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "offset_commit")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let start = SystemTime::now();
        self.inner
            .committed_offset_topitions(group_id)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "committed_offset_topitions")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let start = SystemTime::now();
        self.inner
            .offset_fetch(group_id, topics, require_stable)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "offset_fetch")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let start = SystemTime::now();
        self.inner
            .list_offsets(isolation_level, offsets)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "list_offsets")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let start = SystemTime::now();
        self.inner.metadata(topics).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "metadata")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let start = SystemTime::now();
        self.inner
            .describe_config(name, resource, keys)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_config")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let start = SystemTime::now();
        self.inner
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_topic_partitions")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        let start = SystemTime::now();
        self.inner.list_groups(states_filter).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "list_groups")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let start = SystemTime::now();
        self.inner.delete_groups(group_ids).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_groups")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();
        self.inner
            .describe_groups(group_ids, include_authorized_operations)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_groups")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let start = SystemTime::now();
        self.inner
            .update_group(group_id, detail, version)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "update_group")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let start = SystemTime::now();
        self.inner
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "init_producer")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();
        self.inner
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "txn_add_offsets")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let start = SystemTime::now();
        self.inner
            .txn_add_partitions(partitions)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "txn_add_partitions")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let start = SystemTime::now();
        self.inner.txn_offset_commit(offsets).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "txn_offset_commit")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();
        self.inner
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "txn_end")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        let start = SystemTime::now();
        self.inner.maintain(now).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "maintain")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        let start = SystemTime::now();
        self.inner.maintain_transactions(now).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "maintain_transactions")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        let start = SystemTime::now();
        self.inner
            .aborted_transactions(topition, offset, last_stable_offset)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "aborted_transactions")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        let start = SystemTime::now();
        self.inner.cluster_id().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "cluster_id")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        let start = SystemTime::now();
        self.inner.node().await.inspect(|_| {
            ENGINE_REQUEST_DURATION
                .record(elapsed_millis(start), &[KeyValue::new("operation", "node")])
        })
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        let start = SystemTime::now();
        self.inner.advertised_listener().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "advertised_listener")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let start = SystemTime::now();
        self.inner
            .delete_user_scram_credential(user, mechanism)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "delete_user_scram_credential")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let start = SystemTime::now();
        self.inner
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "upsert_user_scram_credential")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let start = SystemTime::now();
        self.inner
            .user_scram_credential(user, mechanism)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "user_scram_credential")],
                )
            })
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        let start = SystemTime::now();
        self.inner.ping().await.inspect(|_| {
            ENGINE_REQUEST_DURATION
                .record(elapsed_millis(start), &[KeyValue::new("operation", "ping")])
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum CompactionMode {
    Single,
    #[default]
    Multi,
}

impl FromStr for CompactionMode {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "single" => Ok(Self::Single),
            "multi" => Ok(Self::Multi),
            otherwise => Err(Error::Message(otherwise.to_owned())),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum CommunicationMode {
    Mpsc,
    Direct,
    #[default]
    Semaphore,
}

impl FromStr for CommunicationMode {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "direct" => Ok(Self::Direct),
            "mpsc" => Ok(Self::Mpsc),
            "semaphore" => Ok(Self::Semaphore),
            otherwise => Err(Error::Message(otherwise.to_owned())),
        }
    }
}

impl Builder<String, i32, Url, Url> {
    pub(crate) async fn build(self) -> Result<Arc<Box<dyn Storage>>> {
        debug!(domain = self.storage.domain(), path = self.storage.path());

        let mut path = env::current_dir().inspect(|current_dir| debug!(?current_dir))?;

        if let Some(domain) = self.storage.domain() {
            path.push(domain);
        }

        if let Some(relative) = self.storage.path().strip_prefix("/") {
            path.push(relative);
        } else {
            path.push(self.storage.path());
        }

        debug!(?path);

        let vacuum_into = self.storage.query_pairs().find_map(|(k, v)| {
            if k == "vacuum_into" {
                Some(PathBuf::from(v.as_ref()))
            } else {
                None
            }
        });

        let busy_timeout = self
            .storage
            .query_pairs()
            .find_map(|(k, v)| {
                if k == "busy_timeout" {
                    human_units::Duration::from_str(v.as_ref())
                        .map(|duration| duration.0)
                        .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                        .ok()
                } else {
                    None
                }
            })
            .unwrap_or(Duration::from_secs(5));

        let compaction = self
            .storage
            .query_pairs()
            .find_map(|(k, v)| {
                if k == "compaction" {
                    CompactionMode::from_str(v.as_ref()).ok()
                } else {
                    None
                }
            })
            .unwrap_or_default();

        let db = libsql::Builder::new_local(path).build().await?;

        {
            let connection = db.connect()?;

            for (name, ddl) in DDL.iter() {
                _ = connection
                    .execute(ddl.as_str(), ())
                    .await
                    .inspect(|rows| debug!(name, rows))
                    .inspect_err(|err| error!(name, ?err));
            }
        }

        match self
            .storage
            .query_pairs()
            .find_map(|(k, v)| {
                if k == "mode" {
                    CommunicationMode::from_str(v.as_ref()).ok()
                } else {
                    None
                }
            })
            .unwrap_or_default()
        {
            CommunicationMode::Mpsc => {
                let (sender, receiver) = bounded_channel(1);
                let mut server = JoinSet::new();

                let _ = {
                    let cancellation = self.cancellation.clone();

                    let storage = Delegate {
                        cluster: self.cluster,
                        node: self.node,
                        advertised_listener: self.advertised_listener,
                        pool: Pool::builder(ConnectionManager {
                            db: Arc::new(Mutex::new(db)),
                            busy_timeout,
                        })
                        .build()?,
                        schemas: self.schemas,
                        lake: self.lake,
                        vacuum_into,
                        maintenance: Arc::new(Semaphore::new(1)),
                        compaction,
                    };

                    server.spawn(async move {
                        let server = ChannelRequestLayer::new(cancellation)
                            .into_layer(RequestStorageService::new(storage));

                        server.serve(Context::default(), receiver).await
                    })
                };

                let inner = RequestChannelService::new(sender);

                Ok(Arc::new(Box::new(Engine {
                    server: Arc::new(server),
                    inner,
                }) as Box<dyn Storage>))
            }

            CommunicationMode::Direct => Ok(Arc::new(Box::new(Delegate {
                cluster: self.cluster,
                node: self.node,
                advertised_listener: self.advertised_listener,
                pool: Pool::builder(ConnectionManager {
                    db: Arc::new(Mutex::new(db)),
                    busy_timeout,
                })
                .build()?,
                schemas: self.schemas,
                lake: self.lake,
                vacuum_into,
                maintenance: Arc::new(Semaphore::new(1)),
                compaction,
            }) as Box<dyn Storage>)),

            CommunicationMode::Semaphore => Ok(Arc::new(Box::new(SemaphoreProxy::new(Delegate {
                cluster: self.cluster,
                node: self.node,
                advertised_listener: self.advertised_listener,
                pool: Pool::builder(ConnectionManager {
                    db: Arc::new(Mutex::new(db)),
                    busy_timeout,
                })
                .build()?,
                schemas: self.schemas,
                lake: self.lake,
                vacuum_into,
                maintenance: Arc::new(Semaphore::new(1)),
                compaction,
            })) as Box<dyn Storage>)),
        }
    }
}

fn unique_constraint(error_code: ErrorCode) -> impl Fn(libsql::Error) -> Error {
    move |err| {
        if let libsql::Error::SqliteFailure(code, ref reason) = err {
            debug!(code, reason);

            if code == SQLITE_CONSTRAINT_UNIQUE {
                Error::Api(error_code)
            } else {
                err.into()
            }
        } else {
            err.into()
        }
    }
}

#[async_trait]
impl Storage for Delegate {
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        let start = SystemTime::now();

        debug!(?broker_registration);

        let connection = self.connection().await?;

        connection
            .execute(
                "register_broker.sql",
                &[broker_registration.cluster_id.as_str()],
            )
            .await
            .map_err(Into::into)
            .and(Ok(()))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "register_broker")],
                )
            })
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let start = SystemTime::now();

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
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "brokers")],
            )
        })
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?topic, validate_only);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let uuid = {
            let uuid = Uuid::new_v4();

            let parameters = (
                self.cluster.as_str(),
                topic.name.as_str(),
                uuid.to_string(),
                topic.num_partitions,
                (topic.replication_factor as i32),
            );

            pc.query_one("topic_insert.sql", parameters.clone())
                .await
                .inspect_err(|err| {
                    if is_unique_constraint(err) {
                        debug!(?err);
                    } else {
                        error!(?err)
                    }
                })
                .map_err(unique_constraint(ErrorCode::TopicAlreadyExists))
                .inspect(|row| debug!(?parameters, ?row))
                .and_then(|row| {
                    row.get::<String>(0)
                        .inspect_err(|err| error!(?err))
                        .map_err(Into::into)
                })
                .and_then(|id| Uuid::parse_str(id.as_str()).map_err(Into::into))
        }
        .inspect(|uuid| debug!(?uuid))
        .inspect_err(|err| error!(?err))?;

        for partition in 0..topic.num_partitions {
            let params = (self.cluster.as_str(), topic.name.as_str(), partition);

            _ = pc
                .query_opt("topition_insert.sql", params)
                .await
                .map(|row| row.map(|row| row.get_value(0)).transpose())
                .inspect(|topition| debug!(?topition))?;

            _ = pc
                .query_opt("watermark_insert.sql", params)
                .await
                .map(|row| row.map(|row| row.get_value(0)).transpose())
                .inspect(|watermark| debug!(?watermark))?;
        }

        if let Some(configs) = topic.configs {
            for config in configs {
                debug!(?config);

                let params = (
                    self.cluster.as_str(),
                    topic.name.as_str(),
                    config.name.as_str(),
                    config.value.as_deref(),
                );

                _ = pc
                    .query_one("topic_configuration_upsert.sql", params)
                    .await
                    .map(|row| row.get_value(0))
                    .inspect_err(|err| error!(?err, ?config))
                    .inspect(|id| debug!(?id, ?config))?;
            }
        }

        pc.commit(tx).await.and(Ok(uuid)).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "create_topic")],
            )
        })
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(?topics);
        todo!()
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let start = SystemTime::now();
        debug!(cluster = self.cluster, ?topic);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let mut rows = match topic {
            TopicId::Id(id) => {
                pc.query(
                    "topic_select_uuid.sql",
                    (self.cluster.as_str(), id.to_string().as_str()),
                )
                .await?
            }

            TopicId::Name(name) => {
                pc.query(
                    "topic_select_name.sql",
                    (self.cluster.as_str(), name.as_str()),
                )
                .await?
            }
        };

        let Some(row) = rows.next().await? else {
            return Ok(ErrorCode::UnknownTopicOrPartition);
        };

        let topic_name = row.get_str(1)?;

        for sql in [
            "consumer_offset_delete_by_topic.sql",
            "topic_configuration_delete_by_topic.sql",
            "watermark_delete_by_topic.sql",
            "header_delete_by_topic.sql",
            "record_delete_by_topic.sql",
            "txn_offset_commit_tp_delete_by_topic.sql",
            "txn_produce_offset_delete_by_topic.sql",
            "txn_topition_delete_by_topic.sql",
            "producer_detail_delete_by_topic.sql",
            "topition_delete_by_topic.sql",
        ] {
            let rows = pc.execute(sql, (self.cluster.as_str(), topic_name)).await?;

            debug!(?topic, rows, sql)
        }

        _ = pc
            .execute("topic_delete_by.sql", (self.cluster.as_str(), topic_name))
            .await?;

        pc.commit(tx).await.and(Ok(ErrorCode::None)).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_topic")],
            )
        })
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let start = SystemTime::now();
        debug!(?resource);

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

                            if c.query(
                                "topic_configuration_upsert.sql",
                                (
                                    self.cluster.as_str(),
                                    resource.resource_name.as_str(),
                                    config.name.as_str(),
                                    config.value.as_deref(),
                                ),
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

                            if c.query(
                                "topic_configuration_delete.sql",
                                (
                                    self.cluster.as_str(),
                                    resource.resource_name.as_str(),
                                    config.name.as_str(),
                                ),
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
                .inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[KeyValue::new("operation", "incremental_alter_resource")],
                    )
                })
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "incremental_alter_resource")],
                )
            }),
        }
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let start = SystemTime::now();

        let pc = self.connection().await?;

        let tx = pc.transaction().await.inspect(|_| {
            debug!(after_produce_transaction = elapsed_millis(start));
        })?;

        let high = self
            .produce_in_tx(transaction_id, topition, deflated, &pc)
            .await
            .inspect(|_| {
                debug!(after_produce_in_tx = elapsed_millis(start));
            })
            .inspect_err(|err| match err {
                Error::Api(_) => debug!(?err),
                err => error!(?err),
            })?;

        pc.commit(tx)
            .await
            .and(Ok(high))
            .inspect_err(|err| error!(?err))
            .inspect(|_| {
                debug!(after_produce_commit = elapsed_millis(start));

                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "produce")],
                )
            })
    }

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

        debug!(
            ?topition,
            offset,
            min_bytes,
            max_bytes,
            ?isolation_level,
            ?max_wait
        );

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

        let c = self.connection().await?;

        let mut records = if let Some(key) = key_filter {
            let key_bytes = key.as_bytes().to_vec();
            c.query(
                "record_fetch_keyed.sql",
                (
                    self.cluster.as_str(),
                    base_topic,
                    topition.partition(),
                    offset,
                    (max_bytes as i64),
                    high_watermark,
                    key_bytes,
                ),
            )
            .await
            .inspect_err(|err| error!(?err))?
        } else {
            c.query(
                "record_fetch.sql",
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                    offset,
                    (max_bytes as i64),
                    high_watermark,
                ),
            )
            .await
            .inspect_err(|err| error!(?err))?
        };

        let mut batches = vec![];

        if let Some(row) = records.next().await? {
            let offset_delta = 0;
            let timestamp_delta = 0;

            let base_offset = row
                .get::<i64>(0)
                .inspect(|base_offset| debug!(base_offset))
                .inspect_err(|err| error!(?err))?;

            let record_builder = {
                let mut record_builder = Record::builder()
                    .offset_delta(offset_delta)
                    .timestamp_delta(timestamp_delta)
                    .key(
                        row.get::<Option<Vec<u8>>>(3)
                            .map(|o| o.map(Bytes::from))
                            .inspect(|k| debug!(?k))
                            .inspect_err(|err| error!(?err))?,
                    )
                    .value(
                        row.get::<Option<Vec<u8>>>(4)
                            .map(|o| o.map(Bytes::from))
                            .inspect(|v| debug!(?v))
                            .inspect_err(|err| error!(?err))?,
                    );

                let mut headers = c
                    .query(
                        "header_fetch.sql",
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            base_offset,
                        ),
                    )
                    .await?;

                while let Some(header) = headers.next().await? {
                    let mut header_builder = Header::builder();

                    if let Some(k) = header
                        .get::<Option<Vec<u8>>>(0)
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.key(Bytes::from(k));
                    }

                    if let Some(v) = header
                        .get::<Option<Vec<u8>>>(1)
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.value(Bytes::from(v));
                    }

                    record_builder = record_builder.header(header_builder);
                }

                record_builder
            };

            let mut batch_builder = inflated::Batch::builder()
                .base_offset(base_offset)
                .attributes(
                    row.get::<Option<i32>>(1)
                        .map(|attributes| attributes.unwrap_or(0))
                        .inspect_err(|err| error!(?err))? as i16,
                )
                .base_timestamp(
                    row.get_value(2)
                        .map_err(Error::from)
                        .and_then(LiteTimestamp::try_from)
                        .and_then(|system_time| to_timestamp(&system_time.0).map_err(Into::into))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_id(
                    row.get::<Option<i64>>(6)
                        .map(|producer_id| producer_id.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_epoch(
                    row.get::<Option<i32>>(7)
                        .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))? as i16,
                )
                .record(record_builder)
                .last_offset_delta(offset_delta);

            while let Some(row) = records.next().await?
                && !has_deadline_expired()
            {
                let attributes = row
                    .get::<Option<i32>>(1)
                    .map(|attributes| attributes.unwrap_or(0))
                    .inspect_err(|err| error!(?err))? as i16;

                let producer_id = row
                    .get::<Option<i64>>(6)
                    .map(|producer_id| producer_id.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))?;
                let producer_epoch = row
                    .get::<Option<i32>>(7)
                    .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))? as i16;

                if batch_builder.attributes != attributes
                    || batch_builder.producer_id != producer_id
                    || batch_builder.producer_epoch != producer_epoch
                {
                    batches.push(batch_builder.build().and_then(TryInto::try_into)?);

                    batch_builder = inflated::Batch::builder()
                        .base_offset(
                            row.get::<i64>(0)
                                .inspect(|base_offset| debug!(base_offset))
                                .inspect_err(|err| error!(?err))?,
                        )
                        .base_timestamp(
                            row.get_value(2)
                                .map_err(Error::from)
                                .and_then(LiteTimestamp::try_from)
                                .and_then(|system_time| {
                                    to_timestamp(&system_time.0).map_err(Into::into)
                                })
                                .inspect_err(|err| error!(?err))?,
                        )
                        .attributes(attributes)
                        .producer_id(producer_id)
                        .producer_epoch(producer_epoch);
                }

                let offset = row
                    .get::<i64>(0)
                    .inspect(|offset| debug!(offset))
                    .inspect_err(|err| error!(?err))?;
                let offset_delta = i32::try_from(offset - batch_builder.base_offset)?;

                let timestamp_delta = row
                    .get_value(2)
                    .map_err(Error::from)
                    .and_then(LiteTimestamp::try_from)
                    .and_then(|system_time| {
                        to_timestamp(&system_time.0)
                            .map(|timestamp| timestamp - batch_builder.base_timestamp)
                            .map_err(Into::into)
                    })
                    .inspect(|timestamp| debug!(?timestamp))
                    .inspect_err(|err| error!(?err))?;

                let record_builder = {
                    let mut record_builder = Record::builder()
                        .offset_delta(offset_delta)
                        .timestamp_delta(timestamp_delta)
                        .key(
                            row.get::<Option<Vec<u8>>>(3)
                                .map(|o| o.map(Bytes::from))
                                .inspect(|k| debug!(?k))
                                .inspect_err(|err| error!(?err))?,
                        )
                        .value(
                            row.get::<Option<Vec<u8>>>(4)
                                .map(|o| o.map(Bytes::from))
                                .inspect(|v| debug!(?v))
                                .inspect_err(|err| error!(?err))?,
                        );

                    let mut headers = c
                        .query(
                            "header_fetch.sql",
                            (
                                self.cluster.as_str(),
                                topition.topic(),
                                topition.partition(),
                                offset,
                            ),
                        )
                        .await?;

                    while let Some(header) = headers.next().await? {
                        let mut header_builder = Header::builder();

                        if let Some(k) = header
                            .get::<Option<Vec<u8>>>(0)
                            .inspect_err(|err| error!(?err))?
                        {
                            header_builder = header_builder.key(Bytes::from(k));
                        }

                        if let Some(v) = header
                            .get::<Option<Vec<u8>>>(1)
                            .inspect_err(|err| error!(?err))?
                        {
                            header_builder = header_builder.value(Bytes::from(v));
                        }

                        record_builder = record_builder.header(header_builder);
                    }

                    record_builder
                };

                batch_builder = batch_builder
                    .record(record_builder)
                    .last_offset_delta(offset_delta);
            }

            batches.push(batch_builder.build().and_then(TryInto::try_into)?);
        } else {
            batches.push(
                inflated::Batch::builder()
                    .build()
                    .and_then(TryInto::try_into)?,
            );
        }

        Ok(batches).inspect(|_| {
            if let Ok(elapsed) = started_at.elapsed() {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed.as_millis() as u64,
                    &[KeyValue::new("operation", "fetch")],
                )
            }
        })
    }

    #[instrument(skip(self))]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?topition);

        let c = self.connection().await?;

        let row = c
            .query_one(
                "watermark_select.sql",
                (
                    self.cluster.as_str(),
                    self.base_topic(topition.topic()).await?,
                    topition.partition(),
                ),
            )
            .await
            .inspect_err(|err| error!(?topition, ?err))?;

        let log_start = row
            .get::<Option<i64>>(0)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or_default();

        let high_watermark = row
            .get::<Option<i64>>(1)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or_default();

        let last_stable = row
            .get::<Option<i64>>(2)
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(high_watermark);

        debug!(cluster = self.cluster, ?topition, log_start, high_watermark,);

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
        })
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "offset_stage")],
            )
        })
    }

    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?group, ?retention, ?offsets);

        let c = self.connection().await?;
        let tx = c.transaction().await?;

        let mut cg_inserted = false;

        let mut responses = vec![];

        for (topition, offset) in offsets {
            debug!(?topition, ?offset);

            let mut rows = c
                .query(
                    "topition_select.sql",
                    (
                        self.cluster.as_str(),
                        self.base_topic(topition.topic()).await?,
                        topition.partition(),
                    ),
                )
                .await
                .inspect_err(|err| error!(?err))?;

            if rows.next().await.inspect_err(|err| error!(?err))?.is_some() {
                if !cg_inserted {
                    let rows = c
                        .execute("consumer_group_insert.sql", (self.cluster.as_str(), group))
                        .await?;
                    debug!(rows);

                    cg_inserted = true;
                }

                let rows = c
                    .execute(
                        "consumer_offset_insert.sql",
                        (
                            self.cluster.as_str(),
                            self.base_topic(topition.topic()).await?,
                            topition.partition(),
                            group,
                            offset.offset,
                            offset.leader_epoch,
                            offset.timestamp.map(LiteTimestamp),
                            offset.metadata.as_deref(),
                        ),
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

        c.commit(tx).await.inspect_err(|err| error!(?err))?;

        Ok(responses).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "offset_commit")],
            )
        })
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let start = SystemTime::now();

        debug!(group_id);

        let mut results = BTreeMap::new();

        let c = self.connection().await?;

        let mut rows = c
            .query(
                "consumer_offset_select_by_group.sql",
                (self.cluster.as_str(), group_id),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            let topic = row.get_str(0)?;
            let partition = row.get::<i32>(1)?;
            let offset = row.get::<i64>(2)?;

            debug!(group_id, topic, partition, offset);

            assert_eq!(
                None,
                results.insert(Topition::new(topic, partition), offset)
            );
        }

        Ok(results).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "committed_offset_topitions")],
            )
        })
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?group_id, ?topics, ?require_stable);

        let c = self.connection().await?;

        let mut offsets = BTreeMap::new();

        for topic in topics {
            let mut rows = c
                .query(
                    "consumer_offset_select.sql",
                    (
                        self.cluster.as_str(),
                        group_id,
                        self.base_topic(topic.topic()).await?,
                        topic.partition(),
                    ),
                )
                .await
                .inspect_err(|err| {
                    error!(
                        ?err,
                        cluster = self.cluster,
                        group_id,
                        topic = topic.topic,
                        partition = topic.partition
                    )
                })?;

            let offset = rows
                .next()
                .await
                .and_then(|maybe| maybe.map_or(Ok(-1), |row| row.get::<i64>(0)))
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

        Ok(offsets).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "offset_fetch")],
            )
        })
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let start = SystemTime::now();

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
                ListOffset::Earliest | ListOffset::Latest => c
                    .query_opt(
                        query,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition)),

                ListOffset::Timestamp(timestamp) => c
                    .query_opt(
                        query,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            LiteTimestamp::from(timestamp),
                        ),
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

                    row.get::<i64>(0)
                        .map_err(Into::into)
                        .map(Some)
                        .and_then(|offset| {
                            row.get_value(1)
                                .map_err(Into::into)
                                .and_then(LiteTimestamp::try_from)
                                .map(SystemTime::from)
                                .map(Some)
                                .map(|timestamp| {
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

        Ok(responses).inspect(|r| {
            debug!(?r);
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "list_offsets")],
            )
        })
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let start = SystemTime::now();

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

                            let mut rows = c
                                .query("topic_select_name.sql", (self.cluster.as_str(), base_topic))
                                .await?;

                            match rows.next().await.inspect_err(|err| error!(?err)) {
                                Ok(Some(row)) => {
                                    let error_code = ErrorCode::None.into();

                                    let topic_id = vtid.or(row
                                        .get_str(0)
                                        .map_err(Error::from)
                                        .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                                        .map(|uuid| uuid.into_bytes())
                                        .map(Some)?);

                                    let is_internal = row.get::<bool>(2).map(Some)?;
                                    let partitions = row.get::<i32>(3)?;
                                    let replication_factor = row.get::<i32>(4)?;

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

                            let mut rows = c
                                .query(
                                    "topic_select_uuid.sql",
                                    (self.cluster.as_str(), id.to_string().as_str()),
                                )
                                .await?;

                            match rows.next().await {
                                Ok(Some(row)) => {
                                    let error_code = ErrorCode::None.into();
                                    let topic_id = row
                                        .get_str(0)
                                        .map_err(Error::from)
                                        .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                                        .map(|uuid| uuid.into_bytes())
                                        .map(Some)?;
                                    let name = row.get::<String>(1).map(Some)?;
                                    let is_internal = row.get::<bool>(2).map(Some)?;
                                    let partitions = row.get::<i32>(3)?;
                                    let replication_factor = row.get::<i32>(4)?;

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
                                Ok(None) => MetadataResponseTopic::default()
                                    .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                    .name(None)
                                    .topic_id(Some(id.into_bytes()))
                                    .is_internal(Some(false))
                                    .partitions(Some([].into()))
                                    .topic_authorized_operations(Some(-2147483648)),
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

                let mut rows = c
                    .query("topic_by_cluster.sql", &[self.cluster.as_str()])
                    .await?;

                while let Some(row) = rows.next().await? {
                    let error_code = ErrorCode::None.into();
                    let topic_id = row
                        .get_str(0)
                        .map_err(Error::from)
                        .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                        .map(|uuid| uuid.into_bytes())
                        .map(Some)?;
                    let name = row.get::<String>(1).map(Some)?;
                    let is_internal = row.get::<bool>(2).map(Some)?;
                    let partitions = row.get::<i32>(3)?;
                    let replication_factor = row.get::<i32>(4)?;

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

                responses
            }
        };

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: responses,
        })
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "metadata")],
            )
        })
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, name, ?resource, ?keys);

        let c = self.connection().await?;

        let mut rows = c
            .query("topic_select.sql", (self.cluster.as_str(), name))
            .await?;

        if rows.next().await?.is_some() {
            let mut rows = c
                .query(
                    "topic_configuration_select.sql",
                    (self.cluster.as_str(), name),
                )
                .await?;

            let mut configs = vec![];

            while let Some(row) = rows.next().await? {
                let name = row.get_str(0).inspect_err(|err| error!(?err))?;
                let value = row
                    .get::<Option<String>>(1)
                    .map(|value| value.unwrap_or_default())
                    .map(Some)
                    .inspect_err(|err| error!(?err))?;

                configs.push(
                    DescribeConfigsResourceResult::default()
                        .name(name.to_owned())
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
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_config")],
                )
            })
        } else {
            let error_code = ErrorCode::UnknownTopicOrPartition;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some([].into())))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "describe_config")],
                )
            })
        }
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let start = SystemTime::now();

        debug!(?topics, partition_limit, ?cursor);

        let c = self.connection().await?;

        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            responses.push(match topic {
                TopicId::Name(name) => {
                    match c
                        .query_opt(
                            "topic_select_name.sql",
                            (self.cluster.as_str(), name.as_str()),
                        )
                        .await
                        .inspect_err(|err| error!(?err))
                    {
                        Ok(Some(row)) => {
                            let topic_id = row
                                .get_str(0)
                                .map_err(Error::from)
                                .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                                .map(|uuid| uuid.into_bytes())?;
                            let name = row.get::<String>(1).map(Some)?;
                            let is_internal = row.get::<bool>(2).map(Some)?;
                            let partitions = row.get::<i32>(3)?;
                            let replication_factor = row.get::<i32>(4)?;

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
                    match c
                        .query_one(
                            "topic_select_uuid.sql",
                            (self.cluster.as_str(), id.to_string().as_str()),
                        )
                        .await
                    {
                        Ok(row) => {
                            let topic_id = row
                                .get_str(0)
                                .map_err(Error::from)
                                .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                                .map(|uuid| uuid.into_bytes())?;
                            let name = row.get::<String>(1).map(Some)?;
                            let is_internal = row.get::<bool>(2).map(Some)?;
                            let partitions = row.get::<i32>(3)?;
                            let replication_factor = row.get::<i32>(4)?;

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

        Ok(responses).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "describe_topic_partitions")],
            )
        })
    }

    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        let start = SystemTime::now();

        debug!(?states_filter);

        let c = self.connection().await?;

        let mut listed_groups = vec![];

        let mut rows = c
            .query("consumer_group_select.sql", &[self.cluster.as_str()])
            .await?;

        while let Some(row) = rows.next().await? {
            let group_id = row.get_str(0)?;

            listed_groups.push(
                ListedGroup::default()
                    .group_id(group_id.to_owned())
                    .protocol_type("consumer".into())
                    .group_state(Some("unknown".into()))
                    .group_type(Some("classic".into())),
            );
        }

        Ok(listed_groups).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "list_groups")],
            )
        })
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let start = SystemTime::now();

        debug!(?group_ids);

        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let c = self.connection().await?;

            for group_id in group_ids {
                _ = c
                    .execute(
                        "consumer_offset_delete_by_cg.sql",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = c
                    .execute(
                        "consumer_group_detail_delete_by_cg.sql",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                let rows = c
                    .execute(
                        "consumer_group_delete.sql",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
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

        Ok(results).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_groups")],
            )
        })
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();

        debug!(?group_ids, include_authorized_operations);

        let mut results = vec![];
        let c = self.connection().await?;

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                if let Some(row) = c
                    .query_opt(
                        "consumer_group_select_by_name.sql",
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err, group_id))?
                {
                    let current = row
                        .get_str(1)
                        .map_err(Error::from)
                        .and_then(|s| serde_json::from_str::<GroupDetail>(s).map_err(Into::into))
                        .inspect(|current| debug!(?current))
                        .inspect_err(|err| error!(?err, group_id))?;

                    results.push(NamedGroupDetail::found(group_id.into(), current));
                } else {
                    results.push(NamedGroupDetail::error_code(
                        group_id.into(),
                        ErrorCode::GroupIdNotFound,
                    ));
                }
            }
        }

        Ok(results).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "describe_groups")],
            )
        })
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, group_id, ?detail, ?version);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        _ = pc
            .execute(
                "consumer_group_insert.sql",
                (self.cluster.as_str(), group_id),
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

        let detail = serde_json::to_value(detail).inspect(|detail| debug!(%detail))?;

        let outcome = if let Some(row) = pc
            .query_opt(
                "consumer_group_detail_insert.sql",
                (
                    self.cluster.as_str(),
                    group_id,
                    existing_e_tag.to_string().as_str(),
                    new_e_tag.to_string().as_str(),
                    detail.to_string().as_str(),
                ),
            )
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| error!(?err))?
        {
            row.get_str(2)
                .map_err(Error::from)
                .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
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
            let row = pc
                .query_one(
                    "consumer_group_detail.sql",
                    (group_id, self.cluster.as_str()),
                )
                .await
                .inspect(|row| debug!(?row))
                .inspect_err(|err| error!(?err))?;

            let version = row
                .get_str(0)
                .map_err(Error::from)
                .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                .inspect_err(|err| error!(?err))
                .map(|uuid| uuid.to_string())
                .map(Some)
                .map(|e_tag| Version {
                    e_tag,
                    version: None,
                })
                .inspect(|version| debug!(?version))?;

            let value = row
                .get_str(1)
                .map_err(Error::from)
                .inspect(|value| debug!(%value))
                .and_then(|value| serde_json::from_str(value).map_err(Into::into))
                .inspect(|value| debug!(%value))?;

            let current = serde_json::from_value::<GroupDetail>(value)
                .inspect(|current| debug!(?current))
                .inspect_err(|err| error!(?err))
                .map(Box::new)?;

            Err(UpdateError::Outdated { current, version })
        };

        pc.commit(tx).await?;

        debug!(?outcome);

        outcome.inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "update_group")],
            )
        })
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let start = SystemTime::now();

        debug!(
            cluster = self.cluster,
            transaction_id, transaction_timeout_ms, producer_id, producer_epoch
        );

        match (producer_id, producer_epoch, transaction_id) {
            (Some(-1), Some(-1), Some(transaction_id)) => {
                let pc = self.connection().await?;
                let tx = pc.transaction().await?;

                if let Some(row) = pc
                    .query_opt(
                        "producer_epoch_for_current_txn.sql",
                        (self.cluster.as_str(), transaction_id),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    let id = row.get::<i64>(0).inspect_err(|err| error!(?err))?;
                    let epoch = row.get::<i32>(1).inspect_err(|err| error!(?err))? as i16;
                    let status = row
                        .get::<Option<String>>(2)
                        .inspect_err(|err| error!(?err))?
                        .map_or(Ok(None), |status| {
                            TxnState::from_str(status.as_str()).map(Some)
                        })?;

                    debug!(transaction_id, id, epoch, ?status);

                    if let Some(TxnState::Begin) = status {
                        let error = self
                            .end_in_tx(transaction_id, id, epoch, false, &pc)
                            .await?;

                        if error != ErrorCode::None {
                            _ = tx
                                .rollback()
                                .await
                                .inspect_err(|err| error!(?err, ?transaction_id, id, epoch));

                            return Ok(ProducerIdResponse { error, id, epoch }).inspect(|_| {
                                DELEGATE_REQUEST_DURATION.record(
                                    elapsed_millis(start),
                                    &[KeyValue::new("operation", "init_producer")],
                                )
                            });
                        }
                    }
                }

                let (producer, epoch) = if let Some(row) = pc
                    .query_opt(
                        "txn_select_name.sql",
                        (self.cluster.as_str(), transaction_id),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    let producer: i64 = row
                        .get(0)
                        .inspect_err(|err| error!(?err))
                        .inspect(|producer| debug!(producer))?;

                    let row = pc
                        .query_one(
                            "producer_epoch_insert.sql",
                            (self.cluster.as_str(), producer),
                        )
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    let epoch = row
                        .get::<i32>(0)
                        .inspect(|epoch| debug!(epoch))
                        .inspect_err(|err| error!(?err))? as i16;

                    (producer, epoch)
                } else {
                    let row = pc
                        .query_one("producer_insert.sql", &[self.cluster.as_str()])
                        .await
                        .inspect_err(|err| error!(?err))?;

                    let producer: i64 = row.get(0).inspect_err(|err| error!(?err))?;

                    let row = pc
                        .query_one(
                            "producer_epoch_insert.sql",
                            (self.cluster.as_str(), producer),
                        )
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    let epoch = row.get::<i32>(0)? as i16;

                    assert_eq!(
                        1,
                        pc.execute(
                            "txn_insert.sql",
                            (self.cluster.as_str(), transaction_id, producer),
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
                    pc.execute(
                        "txn_detail_insert.sql",
                        (
                            self.cluster.as_str(),
                            transaction_id,
                            producer,
                            epoch,
                            transaction_timeout_ms
                        ),
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

                let error = match pc.commit(tx).await.inspect_err(|err| {
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
            }

            (Some(-1), Some(-1), None) => {
                let pc = self.connection().await?;
                let tx = pc.transaction().await?;

                let mut rows = pc
                    .query("producer_insert.sql", &[self.cluster.as_str()])
                    .await?;

                if let Some(row) = rows.next().await? {
                    let producer = row.get::<i64>(0).inspect(|producer| debug!(producer))?;

                    while let Some(row) = rows.next().await? {
                        debug!(?row)
                    }

                    let mut rows = pc
                        .query(
                            "producer_epoch_insert.sql",
                            (self.cluster.as_str(), producer),
                        )
                        .await?;

                    if let Some(row) = rows.next().await? {
                        let epoch = row
                            .get::<i32>(0)
                            .map(|epoch| epoch as i16)
                            .inspect(|epoch| debug!(epoch))?;

                        while let Some(row) = rows.next().await? {
                            debug!(?row)
                        }

                        let error = match pc
                            .commit(tx)
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
                        .inspect(|response| debug!(?response))
                        .inspect(|_| {
                            DELEGATE_REQUEST_DURATION.record(
                                elapsed_millis(start),
                                &[KeyValue::new("operation", "init_producer")],
                            )
                        })
                    } else {
                        Ok(ProducerIdResponse {
                            error: ErrorCode::UnknownServerError,
                            id: producer,
                            epoch: -1,
                        })
                        .inspect(|response| debug!(?response))
                        .inspect(|_| {
                            DELEGATE_REQUEST_DURATION.record(
                                elapsed_millis(start),
                                &[KeyValue::new("operation", "init_producer")],
                            )
                        })
                    }
                } else {
                    Ok(ProducerIdResponse {
                        error: ErrorCode::UnknownServerError,
                        id: -1,
                        epoch: -1,
                    })
                    .inspect(|response| debug!(?response))
                    .inspect(|_| {
                        DELEGATE_REQUEST_DURATION.record(
                            elapsed_millis(start),
                            &[KeyValue::new("operation", "init_producer")],
                        )
                    })
                }
            }

            (_, _, _) => todo!(),
        }
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();

        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, group_id
        );

        Ok(ErrorCode::None).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "txn_add_offsets")],
            )
        })
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?partitions);

        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                topics,
            } => {
                debug!(?transaction_id, ?producer_id, ?producer_epoch, ?topics);

                let pc = self.connection().await?;
                let tx = pc.transaction().await?;

                let mut results = vec![];

                for topic in topics {
                    let mut results_by_partition = vec![];

                    for partition_index in topic.partitions.unwrap_or(vec![]) {
                        _ = pc
                            .execute(
                                "txn_topition_insert.sql",
                                (
                                    self.cluster.as_str(),
                                    topic.name.as_str(),
                                    partition_index,
                                    transaction_id.as_str(),
                                    producer_id,
                                    producer_epoch,
                                ),
                            )
                            .await
                            .inspect_err(|err| {
                                error!(
                                    ?err,
                                    cluster = self.cluster,
                                    topic = topic.name,
                                    partition_index,
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

                _ = pc
                    .execute(
                        "txn_detail_update_started_at.sql",
                        (
                            self.cluster.as_str(),
                            transaction_id.as_str(),
                            producer_id,
                            producer_epoch,
                        ),
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

                pc.commit(tx).await?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results)).inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[KeyValue::new("operation", "txn_add_partitions")],
                    )
                })
            }

            TxnAddPartitionsRequest::VersionFourPlus { .. } => {
                todo!()
            }
        }
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?offsets);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let (producer_id, producer_epoch) = if let Some(row) = pc
            .query_opt(
                "producer_epoch_for_current_txn.sql",
                (self.cluster.as_str(), offsets.transaction_id.as_str()),
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let producer_id = row
                .get::<i64>(0)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            let epoch = row
                .get::<i32>(1)
                .map(|epoch| epoch as i16)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            (producer_id, epoch)
        } else {
            (None, None)
        };

        _ = pc
            .execute(
                "consumer_group_insert.sql",
                (self.cluster.as_str(), offsets.group_id.as_str()),
            )
            .await?;

        debug!(?producer_id, ?producer_epoch);

        _ = pc
            .execute(
                "txn_offset_commit_insert.sql",
                (
                    self.cluster.as_str(),
                    offsets.transaction_id.as_str(),
                    offsets.group_id.as_str(),
                    offsets.producer_id,
                    offsets.producer_epoch,
                    offsets.generation_id,
                    offsets.member_id,
                ),
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
                        _ = pc
                            .execute(
                                "txn_offset_commit_tp_insert.sql",
                                (
                                    self.cluster.as_str(),
                                    offsets.transaction_id.as_str(),
                                    offsets.group_id.as_str(),
                                    offsets.producer_id,
                                    offsets.producer_epoch,
                                    topic.name.as_str(),
                                    partition.partition_index,
                                    partition.committed_offset,
                                    partition.committed_leader_epoch,
                                    partition.committed_metadata,
                                ),
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

        pc.commit(tx).await?;

        Ok(topics).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "txn_offset_commit")],
            )
        })
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();

        debug!(cluster = ?self.cluster, transaction_id, producer_id, producer_epoch, committed);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let error_code = self
            .end_in_tx(transaction_id, producer_id, producer_epoch, committed, &pc)
            .await?;

        pc.commit(tx).await.and(Ok(error_code)).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "txn_end")],
            )
        })
    }

    async fn maintain_transactions(&self, _now: SystemTime) -> Result<()> {
        Ok(())
    }

    async fn aborted_transactions(
        &self,
        _topition: &Topition,
        _offset: i64,
        _last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        Ok(vec![])
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.vacuum_into().await?;

        let Ok(_permit) = self.maintenance.try_acquire() else {
            return Ok(());
        };

        let start = SystemTime::now();

        let deleted = self.policy_delete(now).await?;
        debug!(deleted);

        let compacted = self.policy_compact().await?;
        debug!(compacted);

        {
            let connection = self.pool.get().await?;

            let mut rows = connection.query("maintain-vacuum.sql", ()).await?;

            if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
                debug!(
                    freelist_count = row.get_str(0)?,
                    page_size = row.get_str(1)?
                );
            }
        }

        Ok(()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "maintain")],
            )
        })
    }

    async fn cluster_id(&self) -> Result<String> {
        let start = SystemTime::now();

        Ok(self.cluster.clone()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "cluster_id")],
            )
        })
    }

    async fn node(&self) -> Result<i32> {
        let start = SystemTime::now();

        Ok(self.node).inspect(|_| {
            DELEGATE_REQUEST_DURATION
                .record(elapsed_millis(start), &[KeyValue::new("operation", "node")])
        })
    }

    async fn advertised_listener(&self) -> Result<Url> {
        let start = SystemTime::now();

        Ok(self.advertised_listener.clone()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "advertised_listener")],
            )
        })
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let start = SystemTime::now();
        let pc = self.connection().await?;

        pc.execute(
            "scram_credential_delete.sql",
            (self.cluster.as_str(), user, i32::from(mechanism)),
        )
        .await
        .inspect_err(|err| error!(?err))
        .map_err(Into::into)
        .and(Ok(()))
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_user_scram_credential")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let start = SystemTime::now();
        let pc = self.connection().await?;

        pc.execute(
            "scram_credential_insert.sql",
            (
                self.cluster.as_str(),
                user,
                i32::from(mechanism),
                &credential.salt[..],
                credential.iterations,
                &credential.stored_key[..],
                &credential.server_key[..],
            ),
        )
        .await
        .inspect_err(|err| error!(?err))
        .map_err(Into::into)
        .and(Ok(()))
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "upsert_user_scram_credential")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let start = SystemTime::now();
        let pc = self.connection().await?;

        pc.query_opt(
            "scram_credential_select.sql",
            (self.cluster.as_str(), user, i32::from(mechanism)),
        )
        .await
        .inspect_err(|err| error!(?err))
        .map_err(Into::into)
        .and_then(|row| {
            if let Some(row) = row {
                let salt = row.get::<Vec<u8>>(0).map(Bytes::from)?;
                let iterations = row.get::<i32>(1)?;
                let stored_key = row.get::<Vec<u8>>(2).map(Bytes::from)?;
                let server_key = row.get::<Vec<u8>>(3).map(Bytes::from)?;

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
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "upsert_user_scram_credential")],
            )
        })
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        let start = SystemTime::now();
        let c = self.pool.get().await?;
        let _ = c.query("ping.sql", ()).await?;
        DELEGATE_REQUEST_DURATION
            .record(elapsed_millis(start), &[KeyValue::new("operation", "ping")]);
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct LiteTimestamp(SystemTime);

impl Deref for LiteTimestamp {
    type Target = SystemTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<SystemTime> for LiteTimestamp {
    fn from(value: SystemTime) -> Self {
        Self(value)
    }
}

impl From<&SystemTime> for LiteTimestamp {
    fn from(value: &SystemTime) -> Self {
        Self(*value)
    }
}

impl From<LiteTimestamp> for SystemTime {
    fn from(value: LiteTimestamp) -> Self {
        value.0
    }
}

impl From<LiteTimestamp> for Value {
    fn from(value: LiteTimestamp) -> Self {
        Value::Integer(to_timestamp(&value.0).unwrap_or_default())
    }
}

impl TryFrom<Value> for LiteTimestamp {
    type Error = Error;

    fn try_from(value: Value) -> result::Result<Self, Self::Error> {
        match value {
            Value::Integer(timestamp) => to_system_time(timestamp)
                .map_err(Into::into)
                .map(LiteTimestamp::from),

            Value::Text(text) => NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f")
                .map(|date_time| date_time.and_utc())
                .inspect(|dt| debug!(?dt))
                .map(SystemTime::from)
                .map(LiteTimestamp::from)
                .map_err(Into::into),

            Value::Real(_) => unimplemented!("{value:?}"),
            Value::Null => unimplemented!("{value:?}"),
            Value::Blob(_) => unimplemented!("{value:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use tempfile::tempdir;
    use tokio::fs::remove_file;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::StorageContainer;

    use super::*;

    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(EnvFilter::from_default_env().add_directive(
                    format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
                ))
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[tokio::test]
    async fn insert_with_returning() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");
        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        let sql = "create table xyz (
            id integer primary key autoincrement,
            name text not null
            )";

        _ = connection.execute(sql, ()).await?;

        let sql = "insert into xyz (name) values (?1) returning xyz.name";

        let statement = connection.prepare(sql).await?;
        let mut rows = statement.query(&["abc"]).await?;
        let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
        let name = row.get_str(0).inspect(|name| debug!(name))?;
        assert_eq!("abc", name);

        Ok(())
    }

    #[tokio::test]
    async fn insert_select_with_returning() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");
        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        let sql = "create table pqr (
            id integer primary key autoincrement,
            name text not null
            )";

        _ = connection.execute(sql, ()).await?;

        let sql = "insert into pqr (name) values (?1)";
        let statement = connection.prepare(sql).await?;
        assert_eq!(1, statement.execute(&["fgh"]).await?);

        let sql = "create table xyz (
            id integer primary key autoincrement,
            pqr integer references pqr (id) not null,
            name text not null
            )";

        _ = connection.execute(sql, ()).await?;

        let sql = "insert into xyz (pqr, name)
            select pqr.id, ?2
            from pqr
            where pqr.name = ?1
            returning xyz.name";

        let expected = "abc";
        let mut rows = connection.query(sql, &["fgh", expected]).await?;
        let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
        let actual = row.get_str(0).inspect(|name| debug!(name))?;
        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn cte_insert_with_returning() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");
        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        let sql = "create table pqr (
            id integer primary key autoincrement,
            name text not null
            )";

        _ = connection.execute(sql, ()).await?;

        let sql = "insert into pqr (name) values (?1)";
        let statement = connection.prepare(sql).await?;
        assert_eq!(1, statement.execute(&["fgh"]).await?);

        let sql = "create table xyz (
            id integer primary key autoincrement,
            pqr integer references pqr (id) not null,
            name text not null
            )";

        _ = connection.execute(sql, ()).await?;

        let sql = "with qwe as (
                select pqr.id, ?2
                from pqr
                where pqr.name = ?1
            )
            insert into xyz (pqr, name)
            select * from qwe
            returning xyz.name";

        let expected = "abc";
        let mut rows = connection.query(sql, &["fgh", expected]).await?;
        let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
        let actual = row.get_str(0).inspect(|name| debug!(name))?;
        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simpler_insert_select_with_returning() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");
        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        let sql = "create table xyz (
            id integer primary key autoincrement,
            pqr integer not null,
            name text not null
            )";

        _ = connection.execute(sql, ()).await?;

        let expected = "abc";

        let sql = "insert into xyz (pqr, name)
            select 41 + 1, ?1
            returning xyz.pqr, xyz.name";

        let mut rows = connection.query(sql, &[expected]).await?;
        let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();

        assert_eq!(42, row.get::<i32>(0)?);

        let actual = row.get_str(1).inspect(|name| debug!(name))?;
        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn create_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");
        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        assert_eq!(
            0,
            connection
                .execute(&include_sql!("ddl/010-cluster.sql"), ())
                .await?
        );

        assert_eq!(
            0,
            connection
                .execute(&include_sql!("ddl/020-topic.sql"), ())
                .await?
        );

        let cluster = "nisshi";

        assert_eq!(
            1,
            connection
                .execute(
                    &fix_parameters(&include_sql!("sql/register_broker.sql"))?,
                    &[cluster]
                )
                .await?
        );

        let name = "test";
        let uuid = Uuid::new_v4();
        let partitions = 3;
        let replication_factor = 3;

        let mut rows = connection
            .query(
                &fix_parameters(&include_sql!("sql/topic_insert.sql"))?,
                (
                    cluster,
                    name,
                    uuid.to_string(),
                    partitions,
                    replication_factor,
                ),
            )
            .await?;

        let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
        assert_eq!(uuid.to_string().as_str(), row.get_str(0)?);
        Ok(())
    }

    #[tokio::test]
    async fn create_topic_in_tx() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let db = libsql::Builder::new_local(file_path).build().await?;
        let tx = db.connect()?.transaction().await?;

        assert_eq!(
            0,
            tx.execute(&include_sql!("ddl/010-cluster.sql"), ()).await?
        );

        assert_eq!(0, tx.execute(&include_sql!("ddl/020-topic.sql"), ()).await?);

        let cluster = "nisshi";

        assert_eq!(
            1,
            tx.execute(
                &fix_parameters(&include_sql!("sql/register_broker.sql"))?,
                &[cluster]
            )
            .await?
        );

        let name = "test";
        let uuid = Uuid::new_v4();
        let partitions = 3;
        let replication_factor = 3;

        let mut rows = tx
            .query(
                &fix_parameters(&include_sql!("sql/topic_insert.sql"))?,
                (
                    cluster,
                    name,
                    uuid.to_string(),
                    partitions,
                    replication_factor,
                ),
            )
            .await?;

        let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
        assert_eq!(uuid.to_string().as_str(), row.get_str(0)?);
        Ok(())
    }

    #[tokio::test]
    async fn lite_system_time() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        assert_eq!(
            0,
            connection
                .execute(&include_sql!("ddl/010-cluster.sql"), ())
                .await?
        );

        let name = "lite";

        _ = connection
            .execute(&include_sql!("sql/register_broker.sql"), &[name])
            .await?;

        let mut rows = connection
            .query("select last_updated from cluster where name = ?1", &[name])
            .await?;
        let row = rows.next().await?.unwrap();
        let _timestamp = LiteTimestamp::try_from(row.get_value(0)?)?;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn register_broker() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = Url::parse(&format!("file://{}", file_path.display()))?;
        let cluster = "nisshi";
        let node = 12321;

        {
            let engine = Engine::builder()
                .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
                .cluster(cluster.to_owned())
                .storage(storage)
                .node(node)
                .build()
                .await?;

            engine
                .register_broker(BrokerRegistrationRequest {
                    broker_id: node,
                    cluster_id: cluster.to_owned(),
                    incarnation_id: Uuid::new_v4(),
                    rack: None,
                })
                .await?;
        }

        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        let mut rows = connection
            .query("select id, name from cluster where name = ?1", &[cluster])
            .await?;

        let row = rows.next().await?.unwrap();
        assert_eq!(cluster, row.get_str(1)?);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn storage_create_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = Url::parse(&format!("file://{}", file_path.display()))?;
        let cluster = "nisshi";
        let node = 12321;

        let topic = "test";
        let num_partitions = 5;
        let replication_factor = 3;

        let uuid = {
            let engine = Engine::builder()
                .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
                .cluster(cluster.to_owned())
                .storage(storage)
                .node(node)
                .build()
                .await?;

            engine
                .register_broker(BrokerRegistrationRequest {
                    broker_id: node,
                    cluster_id: cluster.to_owned(),
                    incarnation_id: Uuid::new_v4(),
                    rack: None,
                })
                .await?;

            let creatable_topic = CreatableTopic::default()
                .name(topic.to_owned())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(Some([].into()))
                .configs(Some([].into()));

            engine
                .create_topic(creatable_topic, false)
                .await
                .inspect(|uuid| debug!(?uuid))
        }?;

        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        let mut rows = connection
            .query(
                "select t.uuid, t.partitions, t.replication_factor from
                cluster c
                join topic t on t.cluster = c.id
                where c.name = ?1
                and t.name = ?2",
                &[cluster, topic],
            )
            .await?;

        let row = rows.next().await?.unwrap();
        assert_eq!(uuid, Uuid::parse_str(row.get_str(0)?)?);
        assert_eq!(num_partitions, row.get::<i32>(1)?);
        assert_eq!(replication_factor, row.get::<i32>(2)? as i16);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn produce() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = Url::parse(&format!("file://{}", file_path.display()))?;
        let cluster = "nisshi";
        let node = 12321;

        let topic = "test";
        let num_partitions = 5;
        let replication_factor = 3;

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(storage)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        let creatable_topic = CreatableTopic::default()
            .name(topic.to_owned())
            .num_partitions(num_partitions)
            .replication_factor(replication_factor)
            .assignments(Some([].into()))
            .configs(Some([].into()));

        let _uuid = engine
            .create_topic(creatable_topic, false)
            .await
            .inspect(|uuid| debug!(?uuid))?;

        Ok(())
    }

    #[allow(dead_code)]
    async fn clean_up() -> Result<()> {
        let relative = db_path().map(|path| format!("{path}*"))?;

        let mut path = env::current_dir()?;
        path.push(relative);
        debug!(?path);

        if let Some(pattern) = path.to_str() {
            debug!(pattern);

            let paths = glob::glob(pattern)?;

            for path in paths.flatten() {
                debug!(?path);

                remove_file(path).await?;
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    async fn storage_container(cluster: &str, node: i32) -> Result<Arc<Box<dyn Storage>>> {
        StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .schema_registry(None)
            .storage(
                db_path()
                    .map(|path| format!("sqlite://{path}"))
                    .inspect(|url| debug!(url))
                    .and_then(|url| Url::parse(&url).map_err(Into::into))?,
            )
            .build()
            .await
    }

    #[allow(dead_code)]
    fn db_path() -> Result<String> {
        thread::current()
            .name()
            .ok_or(Error::Message(String::from("unnamed thread")))
            .map(|name| {
                format!(
                    "../logs/{}/{}::{name}.db",
                    env!("CARGO_PKG_NAME"),
                    env!("CARGO_CRATE_NAME")
                )
            })
    }

    fn txn_batch(producer_id: i64) -> deflated::Batch {
        inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"x").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_id)
            .producer_epoch(0)
            .base_sequence(0)
            .last_offset_delta(0)
            .build()
            .and_then(TryInto::try_into)
            .expect("deflated txn batch")
    }

    /// An open (uncommitted) transaction must pin the read_committed last stable
    /// offset below the high watermark. Regressed when `offset_stage` read the
    /// high-watermark column instead of the stable-offset column, so
    /// `read_committed` consumers saw uncommitted records.
    #[tokio::test]
    async fn read_committed_last_stable_offset_pinned_by_open_transaction() -> Result<()> {
        use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

        let _guard = init_tracing()?;

        // lite's build() resolves the URL path relative to the crate dir (it
        // strips the leading "/" and pushes onto cwd), so use a crate-local db
        // file. A tempdir won't work here. The guard removes the file (and its
        // WAL/SHM sidecars) on drop -- including on panic -- so no artifacts are
        // left behind, while the up-front removal keeps re-runs deterministic.
        struct DbFileGuard(&'static str);
        impl Drop for DbFileGuard {
            fn drop(&mut self) {
                for suffix in ["", "-wal", "-shm"] {
                    let _ = std::fs::remove_file(format!("{}{suffix}", self.0));
                }
            }
        }

        let db_file = "read-committed-last-stable-offset-test.db";
        let _db_file_guard = DbFileGuard(db_file);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{db_file}{suffix}"));
        }
        let storage = Url::parse(&format!("file:///{db_file}"))?;
        let cluster = "nisshi";
        let node = 12321;

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(storage)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        _ = engine
            .create_topic(
                CreatableTopic::default()
                    .name("t".into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        // a transactional producer begins a transaction and produces, never commits
        let producer = engine
            .init_producer(Some("tx"), 0, Some(-1), Some(-1))
            .await?;
        let pid = producer.id;

        _ = engine
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: "tx".into(),
                producer_id: pid,
                producer_epoch: 0,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name("t".into())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        _ = engine
            .produce(Some("tx"), &Topition::new("t", 0), txn_batch(pid))
            .await?;

        let stage = engine.offset_stage(&Topition::new("t", 0)).await?;

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
}
