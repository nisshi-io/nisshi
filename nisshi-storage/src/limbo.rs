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
    env,
    fmt::Debug,
    marker::PhantomData,
    ops::Deref,
    result,
    str::FromStr as _,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime},
};

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetRequest, ListOffsetResponse, METER,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
    Result, ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState, UpdateError, Version,
    sql::{Cache, default_hash, idempotent_sequence_check, remove_comments},
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::NaiveDateTime;
use nisshi_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, NULL_TOPIC_ID, OpType, ScramMechanism,
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
use rand::{rng, seq::SliceRandom as _};
use regex::Regex;
use tracing::{debug, error};
use turso::{
    Connection, Database, Row, Value, params::IntoParams, transaction::Transaction,
    transaction::TransactionBehavior,
};
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
            .get_value(0)
            .map_err(Into::into)
            .and_then(|value| {
                value
                    .as_text()
                    .cloned()
                    .ok_or(Error::UnexpectedValue(value))
            })
            .inspect_err(|err| error!(?err))?;

        let producer_id = row
            .get_value(1)
            .map_err(Into::into)
            .and_then(|value| {
                value
                    .as_integer()
                    .copied()
                    .ok_or(Error::UnexpectedValue(value))
            })
            .inspect_err(|err| error!(?err))?;

        let producer_epoch = row
            .get_value(2)
            .map_err(Into::into)
            .and_then(|value| {
                value
                    .as_integer()
                    .copied()
                    .map(|value| value as i16)
                    .ok_or(Error::UnexpectedValue(value))
            })
            .inspect_err(|err| error!(?err))?;

        let status = row
            .get_value(3)
            .map(|value| value.as_text().cloned())
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

/// Turso storage engine
///
#[derive(Clone, Debug)]
pub struct Engine {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    db: Arc<Mutex<Database>>,

    schemas: Option<Registry>,
    lake: Option<House>,
}

impl Engine {
    pub fn builder()
    -> Builder<PhantomData<String>, PhantomData<i32>, PhantomData<Url>, PhantomData<Url>> {
        Builder::default()
    }

    async fn connection(&self) -> Result<Connection> {
        let db = self.db.lock()?;
        db.connect().map_err(Into::into)
    }

    fn attributes_for_error(&self, sql: &str, error: &turso::Error) -> Vec<KeyValue> {
        debug!(sql, ?error);

        let _attributes = [
            KeyValue::new("sql", sql.to_owned()),
            KeyValue::new("cluster_id", self.cluster.clone()),
        ];

        debug!(?error);
        todo!();
    }

    async fn prepare_execute<P>(
        &self,
        connection: &Connection,
        sql: &str,
        params: P,
    ) -> result::Result<u64, turso::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(?connection, sql, ?params);

        let mut statement = connection.prepare(sql).await?;

        let execute_start = SystemTime::now();

        statement
            .execute(params)
            .await
            .inspect(|rows| {
                debug!(rows);

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
    }

    async fn prepare_query_opt<P>(
        &self,
        connection: &Connection,
        sql: &str,
        params: P,
    ) -> result::Result<Option<Row>, turso::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(?connection, sql, ?params);

        let mut statement = connection.prepare(sql).await?;

        let execute_start = SystemTime::now();

        let mut rows = statement.query(params).await.inspect_err(|err| {
            SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
        })?;

        let row = rows.next().await.inspect_err(|err| {
            SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
        })?;

        let attributes = [
            KeyValue::new("sql", sql.to_owned()),
            KeyValue::new("cluster_id", self.cluster.clone()),
        ];

        SQL_DURATION.record(
            execute_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &attributes,
        );

        SQL_REQUESTS.add(1, &attributes);

        Ok(row)
    }

    async fn prepare_query_one<P>(
        &self,
        connection: &Connection,
        sql: &str,
        params: P,
    ) -> result::Result<Row, turso::Error>
    where
        P: IntoParams,
        P: Debug,
    {
        debug!(?connection, sql, ?params);

        let mut statement = connection
            .prepare(sql)
            .await
            .inspect_err(|err| error!(?err, sql))?;

        let execute_start = SystemTime::now();

        let mut rows = statement.query(params).await.inspect_err(|err| {
            error!(?err, sql);
            SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
        })?;

        if let Some(row) = rows
            .next()
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| {
                error!(?err, sql);
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })?
        {
            let attributes = [
                KeyValue::new("sql", sql.to_owned()),
                KeyValue::new("cluster_id", self.cluster.clone()),
            ];

            SQL_DURATION.record(
                execute_start
                    .elapsed()
                    .map_or(0, |duration| duration.as_millis() as u64),
                &attributes,
            );

            SQL_REQUESTS.add(1, &attributes);

            Ok(row).inspect(|row| debug!(?row))
        } else {
            panic!("more or less than one row");
        }
    }

    async fn idempotent_message_check(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &Connection,
    ) -> Result<()> {
        debug!(transaction_id, ?deflated);

        let mut rows = connection
            .query(
                &sql_lookup("producer_epoch_current_for_producer.sql")?,
                (self.cluster.as_str(), deflated.producer_id),
            )
            .await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            let current_epoch = row
                .get_value(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .map(|value| value as i16)
                        .ok_or(Error::UnexpectedValue(value))
                })?;

            let row = self
                .prepare_query_one(
                    connection,
                    &sql_lookup("producer_select_for_update.sql")?,
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

            let sequence = row
                .get_value(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .map(|value| value as i32)
                        .ok_or(Error::UnexpectedValue(value))
                })?;

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
                self.prepare_execute(
                    connection,
                    &sql_lookup("producer_detail_insert.sql")?,
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

    async fn watermark_select_for_update(
        &self,
        topition: &Topition,
        tx: &Connection,
    ) -> Result<(Option<i64>, Option<i64>)> {
        debug!(?topition, ?tx);

        let mut rows = tx
            .query(
                &sql_lookup("watermark_select_no_update.sql")?,
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
                row.get_value(0)
                    .map(|value| value.as_integer().copied())
                    .inspect_err(|err| error!(?err))?,
                row.get_value(1)
                    .map(|value| value.as_integer().copied())
                    .inspect_err(|err| error!(?err))?,
            ))
        } else {
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        }
    }

    async fn produce_in_tx<'conn>(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        tx: &Transaction<'conn>,
    ) -> Result<i64> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?topition, ?deflated);

        let topic = topition.topic();
        let partition = topition.partition();

        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, tx.deref())
                .await
                .inspect_err(|err| error!(?err))?;
        }

        let (low, high) = self.watermark_select_for_update(topition, tx).await?;

        debug!(?low, ?high);

        let inflated = inflated::Batch::try_from(deflated).inspect_err(|err| error!(?err))?;

        let attributes = BatchAttribute::try_from(inflated.attributes)?;

        if !attributes.control
            && let Some(ref schemas) = self.schemas
        {
            schemas.validate(topition.topic(), &inflated).await?;
        }

        let last_offset_delta = i64::from(inflated.last_offset_delta);

        for (delta, record) in inflated.records.iter().enumerate() {
            let delta = i64::try_from(delta)?;
            let offset = high.unwrap_or_default() + delta;
            let key = record.key.as_deref();
            let value = record.value.as_deref();

            debug!(?delta, ?record, ?offset);

            _ = self
                .prepare_execute(
                    tx,
                    &sql_lookup("record_insert.sql")?,
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

            for header in record.headers.iter().as_ref() {
                let key = header.key.as_deref();
                let value = header.value.as_deref();

                _ = self
                    .prepare_execute(
                        tx,
                        &sql_lookup("header_insert.sql")?,
                        (self.cluster.as_str(), topic, partition, offset, key, value),
                    )
                    .await
                    .inspect_err(|err| {
                        error!(?err, ?topic, ?partition, ?offset, ?key, ?value);
                    });
            }
        }

        if let Some(transaction_id) = transaction_id
            && attributes.transaction
        {
            let offset_start = high.unwrap_or_default();
            let offset_end = high.map_or(last_offset_delta, |high| high + last_offset_delta);

            _ = self
                    .prepare_execute(tx,
                        &sql_lookup("txn_produce_offset_insert.sql")?,
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

        _ = self
            .prepare_execute(
                tx,
                &sql_lookup("watermark_update.sql")?,
                (
                    self.cluster.as_str(),
                    topic,
                    partition,
                    low.unwrap_or_default(),
                    high.map_or(last_offset_delta + 1, |high| high + last_offset_delta + 1),
                ),
            )
            .await
            .inspect(|n| debug!(?n))
            .inspect_err(|err| error!(?err))?;

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
                &inflated,
                config,
            )
            .await
            .inspect(|store| debug!(?store))
            .inspect_err(|err| debug!(?err))?;
        }

        Ok(high.unwrap_or_default())
    }

    async fn end_in_tx<'conn>(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        tx: &Transaction<'conn>,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?committed);

        let mut overlaps = vec![];

        let mut rows = tx
            .deref()
            .query(
                &sql_lookup("txn_select_produced_topitions.sql")?,
                (
                    self.cluster.as_str(),
                    transaction_id,
                    producer_id,
                    producer_epoch,
                ),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            let topic = row.get_value(0).map_err(Into::into).and_then(|value| {
                value
                    .as_text()
                    .cloned()
                    .ok_or(Error::UnexpectedValue(value))
            })?;

            let partition = row.get_value(1).map_err(Into::into).and_then(|value| {
                value
                    .as_integer()
                    .map(|i| *i as i32)
                    .ok_or(Error::UnexpectedValue(value))
            })?;

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
                .produce_in_tx(Some(transaction_id), &topition, batch, tx)
                .await?;

            debug!(offset, ?topition);

            let mut rows = tx
                .query(
                    &sql_lookup("txn_produce_offset_select_offset_range.sql")?,
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
                let offset_start = row.get_value(0).map_err(Into::into).and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .ok_or(Error::UnexpectedValue(value))
                })?;

                let offset_end = row.get_value(1).map_err(Into::into).and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .ok_or(Error::UnexpectedValue(value))
                })?;

                debug!(offset_start, offset_end);

                let mut rows = tx
                    .query(
                        &sql_lookup("txn_produce_offset_select_overlapping_txn.sql")?,
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

                _ = tx
                    .execute(
                        &sql_lookup("txn_produce_offset_delete_by_txn.sql")?,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = tx
                    .execute(
                        &sql_lookup("txn_topition_delete_by_txn.sql")?,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                if txn.status == TxnState::PrepareCommit {
                    _ = tx
                        .execute(
                            &sql_lookup("consumer_offset_insert_from_txn.sql")?,
                            (
                                self.cluster.as_str(),
                                txn.name.as_str(),
                                txn.producer_id,
                                txn.producer_epoch,
                            ),
                        )
                        .await?;
                }

                _ = tx
                    .execute(
                        &sql_lookup("txn_offset_commit_tp_delete_by_txn.sql")?,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = tx
                    .execute(
                        &sql_lookup("txn_offset_commit_delete_by_txn.sql")?,
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

                _ = tx
                    .execute(
                        &sql_lookup("txn_status_update.sql")?,
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

            _ = tx
                .execute(
                    &sql_lookup("txn_status_update.sql")?,
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
}

#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, L, D> {
    cluster: C,
    node: N,
    advertised_listener: L,
    storage: D,
    schemas: Option<Registry>,
    lake: Option<House>,
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
        }
    }

    pub(crate) fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, D> {
        Self { schemas, ..self }
    }

    pub(crate) fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
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

fn fix_parameters(sql: &str) -> Result<String> {
    Regex::new(r"\$(?<i>\d+)")
        .map(|re| re.replace_all(sql, "?$i").into_owned())
        .map_err(Into::into)
}

fn sql_lookup(key: &str) -> Result<String> {
    crate::sql::SQL
        .get(key)
        .and_then(|sql| fix_parameters(sql).inspect(|sql| debug!(key, sql)))
}

impl Builder<String, i32, Url, Url> {
    pub(crate) async fn build(self) -> Result<Engine> {
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

        let db = turso::Builder::new_local(path.to_str().unwrap())
            .build()
            .await?;

        let connection = db.connect()?;

        for (name, ddl) in DDL.iter() {
            _ = connection
                .execute(ddl.as_str(), ())
                .await
                .inspect(|rows| debug!(name, rows))
                .inspect_err(|err| error!(name, ?err));
        }

        Ok(Engine {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            db: Arc::new(Mutex::new(db)),
            schemas: self.schemas,
            lake: self.lake,
        })
    }
}

fn unique_constraint(error_code: ErrorCode) -> impl Fn(turso::Error) -> Error {
    let _ = error_code;
    move |err| {
        let _ = err;
        todo!()
        // if let turso::Error::SqliteFailure(code, ref reason) = err {
        //     debug!(code, reason);

        //     if code == 2067 {
        //         Error::Api(error_code)
        //     } else {
        //         err.into()
        //     }
        // } else {
        //     err.into()
        // }
    }
}

#[async_trait]
impl Storage for Engine {
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        debug!(?broker_registration);

        let connection = self.connection().await?;

        self.prepare_execute(
            &connection,
            &sql_lookup("register_broker.sql")?,
            &[broker_registration.cluster_id],
        )
        .await
        .map_err(Into::into)
        .and(Ok(()))
    }

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

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        debug!(cluster = self.cluster, ?topic, validate_only);

        let mut connection = self.connection().await.inspect_err(|err| error!(?err))?;

        let tx = connection.transaction().await?;

        let uuid = {
            let uuid = Uuid::new_v4();

            let parameters = (
                self.cluster.as_str(),
                topic.name.as_str(),
                uuid.to_string(),
                topic.num_partitions,
                (topic.replication_factor as i32),
            );

            self.prepare_query_one(&tx, &sql_lookup("topic_insert.sql")?, parameters.clone())
                .await
                .inspect_err(|err| error!(?err))
                .map_err(unique_constraint(ErrorCode::TopicAlreadyExists))
                .inspect(|row| debug!(?parameters, ?row))
                .and_then(|row| {
                    row.get_value(0)
                        .map(|value| value.as_text().cloned().unwrap())
                        .inspect_err(|err| error!(?err))
                        .map_err(Into::into)
                })
                .and_then(|id| Uuid::parse_str(id.as_str()).map_err(Into::into))
        }
        .inspect(|uuid| debug!(?uuid))
        .inspect_err(|err| error!(?err))?;

        for partition in 0..topic.num_partitions {
            let params = (self.cluster.as_str(), topic.name.as_str(), partition);

            _ = self
                .prepare_query_one(&tx, &sql_lookup("topition_insert.sql")?, params)
                .await
                .map(|row| row.get_value(0))
                .inspect(|topition| debug!(?topition))?;

            _ = self
                .prepare_query_one(&tx, &sql_lookup("watermark_insert.sql")?, params)
                .await
                .map(|row| row.get_value(0))
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

                _ = self
                    .prepare_query_one(&tx, &sql_lookup("topic_configuration_upsert.sql")?, params)
                    .await
                    .map(|row| row.get_value(0))
                    .inspect_err(|err| error!(?err, ?config))
                    .inspect(|id| debug!(?id, ?config))?;
            }
        }

        tx.commit().await.map_err(Into::into).and(Ok(uuid))
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(?topics);
        todo!()
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        debug!(cluster = self.cluster, ?topic);

        let mut connection = self.connection().await?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;

        let mut rows = match topic {
            TopicId::Id(id) => {
                tx.query(
                    &sql_lookup("topic_select_uuid.sql")?,
                    (self.cluster.as_str(), id.to_string().as_str()),
                )
                .await?
            }

            TopicId::Name(name) => {
                tx.query(
                    &sql_lookup("topic_select_name.sql")?,
                    (self.cluster.as_str(), name.as_str()),
                )
                .await?
            }
        };

        let Some(row) = rows.next().await? else {
            return Ok(ErrorCode::UnknownTopicOrPartition);
        };

        let value = row.get_value(1)?;
        let topic_name = value
            .as_text()
            .map(|topic_name| topic_name.as_str())
            .ok_or(Error::UnexpectedValue(value.clone()))?;

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
            let rows = self
                .prepare_execute(&tx, &sql_lookup(sql)?, (self.cluster.as_str(), topic_name))
                .await?;

            debug!(?topic, rows, sql)
        }

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup("topic_delete_by.sql")?,
                (self.cluster.as_str(), topic_name),
            )
            .await?;

        tx.commit()
            .await
            .map_err(Into::into)
            .and(Ok(ErrorCode::None))
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
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
                                &sql_lookup("topic_configuration_upsert.sql")?,
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
                                &sql_lookup("topic_configuration_delete.sql")?,
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
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        debug!(cluster = self.cluster, transaction_id, ?topition, ?deflated);

        let mut connection = self.connection().await?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;

        let high = self
            .produce_in_tx(transaction_id, topition, deflated, &tx)
            .await?;

        tx.commit().await.map_err(Into::into).and(Ok(high))
    }

    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        _max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        debug!(?topition, offset, min_bytes, max_bytes, ?isolation_level);
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

        let mut records = c
            .query(
                &sql_lookup("record_fetch.sql")?,
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
            .inspect_err(|err| error!(?err))?;

        let mut batches = vec![];

        if let Some(row) = records.next().await? {
            let offset_delta = 0;
            let timestamp_delta = 0;

            let base_offset = row
                .get_value(0)
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .ok_or(Error::UnexpectedValue(value))
                })
                .inspect(|base_offset| debug!(base_offset))
                .inspect_err(|err| error!(?err))?;

            let record_builder = {
                let mut record_builder = Record::builder()
                    .offset_delta(offset_delta)
                    .timestamp_delta(timestamp_delta)
                    .key(
                        row.get_value(3)
                            .map(|o| o.as_blob().map(|blob| Bytes::copy_from_slice(blob)))
                            .inspect(|k| debug!(?k))
                            .inspect_err(|err| error!(?err))?,
                    )
                    .value(
                        row.get_value(4)
                            .map(|o| o.as_blob().map(|blob| Bytes::copy_from_slice(blob)))
                            .inspect(|v| debug!(?v))
                            .inspect_err(|err| error!(?err))?,
                    );

                let mut headers = c
                    .query(
                        &sql_lookup("header_fetch.sql")?,
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
                        .get_value(0)
                        .map(|value| value.as_blob().cloned())
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.key(Bytes::from(k));
                    }

                    if let Some(v) = header
                        .get_value(1)
                        .map(|value| value.as_blob().cloned())
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
                    row.get_value(1)
                        .map(|value| {
                            value
                                .as_integer()
                                .copied()
                                .map(|attributes| attributes as i32)
                        })
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
                    row.get_value(6)
                        .map(|value| value.as_integer().copied())
                        .map(|producer_id| producer_id.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_epoch(
                    row.get_value(7)
                        .map(|value| {
                            value
                                .as_integer()
                                .copied()
                                .map(|producer_epoch| producer_epoch as i32)
                        })
                        .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))? as i16,
                )
                .record(record_builder)
                .last_offset_delta(offset_delta);

            while let Some(row) = records.next().await? {
                let attributes = row
                    .get_value(1)
                    .map(|value| {
                        value
                            .as_integer()
                            .copied()
                            .map(|attributes| attributes as i16)
                    })
                    .map(|attributes| attributes.unwrap_or(0))
                    .inspect_err(|err| error!(?err))?;

                let producer_id = row
                    .get_value(6)
                    .map(|value| value.as_integer().copied())
                    .map(|producer_id| producer_id.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))?;

                let producer_epoch = row
                    .get_value(7)
                    .map(|value| {
                        value
                            .as_integer()
                            .copied()
                            .map(|producer_epoch| producer_epoch as i16)
                    })
                    .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))?;

                if batch_builder.attributes != attributes
                    || batch_builder.producer_id != producer_id
                    || batch_builder.producer_epoch != producer_epoch
                {
                    batches.push(batch_builder.build().and_then(TryInto::try_into)?);

                    batch_builder = inflated::Batch::builder()
                        .base_offset(
                            row.get_value(0)
                                .map_err(Into::into)
                                .and_then(|value| {
                                    value
                                        .as_integer()
                                        .copied()
                                        .ok_or(Error::UnexpectedValue(value))
                                })
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
                    .get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_integer()
                            .copied()
                            .ok_or(Error::UnexpectedValue(value))
                    })
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
                            row.get_value(3)
                                .map(|value| value.as_blob().cloned())
                                .map(|o| o.map(Bytes::from))
                                .inspect(|k| debug!(?k))
                                .inspect_err(|err| error!(?err))?,
                        )
                        .value(
                            row.get_value(4)
                                .map(|value| value.as_blob().cloned())
                                .map(|o| o.map(Bytes::from))
                                .inspect(|v| debug!(?v))
                                .inspect_err(|err| error!(?err))?,
                        );

                    let mut headers = c
                        .query(
                            &sql_lookup("header_fetch.sql")?,
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
                            .get_value(0)
                            .map(|value| value.as_blob().cloned())
                            .map(|o| o.map(Bytes::from))
                            .inspect_err(|err| error!(?err))?
                        {
                            header_builder = header_builder.key(k);
                        }

                        if let Some(v) = header
                            .get_value(1)
                            .map(|value| value.as_blob().cloned())
                            .map(|o| o.map(Bytes::from))
                            .inspect_err(|err| error!(?err))?
                        {
                            header_builder = header_builder.value(v);
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

        Ok(batches)
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        debug!(cluster = self.cluster, ?topition);
        let c = self.connection().await?;

        let row = self
            .prepare_query_one(
                &c,
                &sql_lookup("watermark_select.sql")?,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await
            .inspect_err(|err| error!(?topition, ?err))?;

        let log_start = row
            .get_value(0)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or_default();

        let high_watermark = row
            .get_value(1)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or_default();

        let last_stable = row
            .get_value(2)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(high_watermark);

        debug!(cluster = self.cluster, ?topition, log_start, high_watermark,);

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
        })
    }

    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        debug!(cluster = self.cluster, ?group, ?retention, ?offsets);
        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let mut cg_inserted = false;

        let mut responses = vec![];

        for (topition, offset) in offsets {
            debug!(?topition, ?offset);

            let mut rows = tx
                .query(
                    &sql_lookup("topition_select.sql")?,
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                    ),
                )
                .await
                .inspect_err(|err| error!(?err))?;

            if rows.next().await.inspect_err(|err| error!(?err))?.is_some() {
                if !cg_inserted {
                    let rows = self
                        .prepare_execute(
                            &tx,
                            &sql_lookup("consumer_group_insert.sql")?,
                            (self.cluster.as_str(), group),
                        )
                        .await?;
                    debug!(rows);

                    cg_inserted = true;
                }

                let rows = self
                    .prepare_execute(
                        &tx,
                        &sql_lookup("consumer_offset_insert.sql")?,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
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

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(responses)
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        debug!(group_id);

        let mut results = BTreeMap::new();

        let c = self.connection().await?;

        let mut rows = c
            .query(
                &sql_lookup("consumer_offset_select_by_group.sql")?,
                (self.cluster.as_str(), group_id),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            let topic = row.get_value(0).map_err(Into::into).and_then(|value| {
                value
                    .as_text()
                    .cloned()
                    .ok_or(Error::UnexpectedValue(value))
            })?;

            let partition = row.get_value(1).map_err(Into::into).and_then(|value| {
                value
                    .as_integer()
                    .copied()
                    .map(|partition| partition as i32)
                    .ok_or(Error::UnexpectedValue(value))
            })?;

            let offset = row.get_value(2).map_err(Into::into).and_then(|value| {
                value
                    .as_integer()
                    .copied()
                    .ok_or(Error::UnexpectedValue(value))
            })?;

            debug!(group_id, topic, partition, offset);

            assert_eq!(
                None,
                results.insert(Topition::new(topic, partition), offset)
            );
        }

        Ok(results)
    }

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
            let mut rows = c
                .query(
                    &sql_lookup("consumer_offset_select.sql")?,
                    (
                        self.cluster.as_str(),
                        group_id,
                        topic.topic(),
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
                .and_then(|row| {
                    row.and_then(|row| {
                        row.get_value(0)
                            .map(|value| value.as_integer().copied())
                            .transpose()
                    })
                    .unwrap_or(Ok(-1))
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

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(cluster = self.cluster, ?isolation_level, ?offsets);
        let c = self.connection().await?;

        let mut responses = vec![];

        for (topition, offset_type) in offsets {
            let query = match (offset_type, isolation_level) {
                (ListOffsetRequest::Earliest, _) => sql_lookup("list_earliest_offset.sql")?,
                (ListOffsetRequest::Latest, IsolationLevel::ReadCommitted) => {
                    sql_lookup("list_latest_offset_committed.sql")?
                }
                (ListOffsetRequest::Latest, IsolationLevel::ReadUncommitted) => {
                    sql_lookup("list_latest_offset_uncommitted.sql")?
                }
                (ListOffsetRequest::Timestamp(_), _) => {
                    sql_lookup("list_latest_offset_timestamp.sql")?
                }
            };

            debug!(?query);

            let list_offset = match offset_type {
                ListOffsetRequest::Earliest | ListOffsetRequest::Latest => self
                    .prepare_query_opt(
                        &c,
                        query.as_str(),
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition)),

                ListOffsetRequest::Timestamp(timestamp) => self
                    .prepare_query_opt(
                        &c,
                        query.as_str(),
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

                    row.get_value(0)
                        .map_err(Into::into)
                        .map(|value| value.as_integer().copied())
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

        Ok(responses).inspect(|r| debug!(?r))
    }

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
                            let mut rows = c
                                .query(
                                    &sql_lookup("topic_select_name.sql")?,
                                    (self.cluster.as_str(), name.as_str()),
                                )
                                .await?;

                            match rows.next().await.inspect_err(|err| error!(?err)) {
                                Ok(Some(row)) => {
                                    let error_code = ErrorCode::None.into();

                                    let topic_id = row.get_value(0).map_err(Error::from).and_then(
                                        |value| {
                                            value
                                                .as_text()
                                                .map(|value| {
                                                    Uuid::parse_str(value)
                                                        .map(|uuid| uuid.into_bytes())
                                                        .map_err(Into::into)
                                                })
                                                .transpose()
                                        },
                                    )?;

                                    let name =
                                        row.get_value(1).map(|value| value.as_text().cloned())?;

                                    let is_internal =
                                        row.get_value(2).map_err(Into::into).and_then(|value| {
                                            value
                                                .as_integer()
                                                .map(|i| match *i {
                                                    0 => Ok(false),
                                                    1 => Ok(true),
                                                    _ => Err(Error::UnexpectedValue(value.clone())),
                                                })
                                                .transpose()
                                        })?;

                                    let partitions =
                                        row.get_value(3).map_err(Into::into).and_then(|value| {
                                            value
                                                .as_integer()
                                                .map(|i| *i as i32)
                                                .ok_or(Error::UnexpectedValue(value))
                                        })?;

                                    let replication_factor =
                                        row.get_value(4).map_err(Into::into).and_then(|value| {
                                            value
                                                .as_integer()
                                                .map(|i| *i as i32)
                                                .ok_or(Error::UnexpectedValue(value))
                                        })?;

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
                                                    .leader_epoch(Some(-1))
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
                                    &sql_lookup("topic_select_uuid.sql")?,
                                    (self.cluster.as_str(), id.to_string().as_str()),
                                )
                                .await?;

                            match rows.next().await {
                                Ok(Some(row)) => {
                                    let error_code = ErrorCode::None.into();
                                    let topic_id = row.get_value(0).map_err(Error::from).and_then(
                                        |value| {
                                            value
                                                .as_text()
                                                .map(|value| {
                                                    Uuid::parse_str(value)
                                                        .map(|uuid| uuid.into_bytes())
                                                        .map_err(Into::into)
                                                })
                                                .transpose()
                                        },
                                    )?;

                                    let name =
                                        row.get_value(1).map(|value| value.as_text().cloned())?;

                                    let is_internal =
                                        row.get_value(2).map_err(Into::into).and_then(|value| {
                                            value
                                                .as_integer()
                                                .map(|i| match *i {
                                                    0 => Ok(false),
                                                    1 => Ok(true),
                                                    _ => Err(Error::UnexpectedValue(value.clone())),
                                                })
                                                .transpose()
                                        })?;

                                    let partitions =
                                        row.get_value(3).map_err(Into::into).and_then(|value| {
                                            value
                                                .as_integer()
                                                .map(|i| *i as i32)
                                                .ok_or(Error::UnexpectedValue(value))
                                        })?;

                                    let replication_factor =
                                        row.get_value(4).map_err(Into::into).and_then(|value| {
                                            value
                                                .as_integer()
                                                .map(|i| *i as i32)
                                                .ok_or(Error::UnexpectedValue(value))
                                        })?;

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
                                                    .leader_epoch(Some(-1))
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
                    .query(
                        &sql_lookup("topic_by_cluster.sql")?,
                        &[self.cluster.as_str()],
                    )
                    .await?;

                while let Some(row) = rows.next().await? {
                    let error_code = ErrorCode::None.into();
                    let topic_id = row.get_value(0).map_err(Error::from).and_then(|value| {
                        value
                            .as_text()
                            .map(|value| {
                                Uuid::parse_str(value)
                                    .map(|uuid| uuid.into_bytes())
                                    .map_err(Into::into)
                            })
                            .transpose()
                    })?;

                    let name = row.get_value(1).map(|value| value.as_text().cloned())?;

                    let is_internal = row.get_value(2).map_err(Into::into).and_then(|value| {
                        value
                            .as_integer()
                            .map(|i| match *i {
                                0 => Ok(false),
                                1 => Ok(true),
                                _ => Err(Error::UnexpectedValue(value.clone())),
                            })
                            .transpose()
                    })?;

                    let partitions = row.get_value(3).map_err(Into::into).and_then(|value| {
                        value
                            .as_integer()
                            .map(|i| *i as i32)
                            .ok_or(Error::UnexpectedValue(value))
                    })?;

                    let replication_factor =
                        row.get_value(4).map_err(Into::into).and_then(|value| {
                            value
                                .as_integer()
                                .map(|i| *i as i32)
                                .ok_or(Error::UnexpectedValue(value))
                        })?;

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
                                    .leader_epoch(Some(-1))
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
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        debug!(cluster = self.cluster, name, ?resource, ?keys);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut rows = c
            .query(
                &sql_lookup("topic_select.sql")?,
                (self.cluster.as_str(), name),
            )
            .await?;

        if rows.next().await?.is_some() {
            let mut rows = c
                .query(
                    &sql_lookup("topic_configuration_select.sql")?,
                    (self.cluster.as_str(), name),
                )
                .await?;

            let mut configs = vec![];

            while let Some(row) = rows.next().await? {
                let name = row
                    .get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_text()
                            .cloned()
                            .ok_or(Error::UnexpectedValue(value))
                    })
                    .inspect_err(|err| error!(?err))?;

                let value = row
                    .get_value(1)
                    .map(|value| value.as_text().cloned())
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

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        debug!(?topics, partition_limit, ?cursor);
        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            responses.push(match topic {
                TopicId::Name(name) => {
                    match self
                        .prepare_query_opt(
                            &c,
                            &sql_lookup("topic_select_name.sql")?,
                            (self.cluster.as_str(), name.as_str()),
                        )
                        .await
                        .inspect_err(|err| error!(?err))
                    {
                        Ok(Some(row)) => {
                            let topic_id =
                                row.get_value(0).map_err(Error::from).and_then(|value| {
                                    value.as_text().map_or(
                                        Err(Error::UnexpectedValue(value.clone())),
                                        |value| {
                                            Uuid::parse_str(value)
                                                .map(|uuid| uuid.into_bytes())
                                                .map_err(Into::into)
                                        },
                                    )
                                })?;

                            let name = row.get_value(1).map(|value| value.as_text().cloned())?;

                            let is_internal =
                                row.get_value(2).map_err(Into::into).and_then(|value| {
                                    value
                                        .as_integer()
                                        .map(|i| match *i {
                                            0 => Ok(false),
                                            1 => Ok(true),
                                            _ => Err(Error::UnexpectedValue(value.clone())),
                                        })
                                        .transpose()
                                })?;

                            let partitions =
                                row.get_value(3).map_err(Into::into).and_then(|value| {
                                    value
                                        .as_integer()
                                        .map(|i| *i as i32)
                                        .ok_or(Error::UnexpectedValue(value))
                                })?;

                            let replication_factor =
                                row.get_value(4).map_err(Into::into).and_then(|value| {
                                    value
                                        .as_integer()
                                        .map(|i| *i as i32)
                                        .ok_or(Error::UnexpectedValue(value))
                                })?;

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
                                                .leader_epoch(-1)
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
                        .prepare_query_one(
                            &c,
                            &sql_lookup("topic_select_uuid.sql")?,
                            (self.cluster.as_str(), id.to_string().as_str()),
                        )
                        .await
                    {
                        Ok(row) => {
                            let topic_id =
                                row.get_value(0).map_err(Error::from).and_then(|value| {
                                    value.as_text().map_or(
                                        Err(Error::UnexpectedValue(value.clone())),
                                        |value| {
                                            Uuid::parse_str(value)
                                                .map(|uuid| uuid.into_bytes())
                                                .map_err(Into::into)
                                        },
                                    )
                                })?;

                            let name = row.get_value(1).map(|value| value.as_text().cloned())?;

                            let is_internal =
                                row.get_value(2).map_err(Into::into).and_then(|value| {
                                    value
                                        .as_integer()
                                        .map(|i| match *i {
                                            0 => Ok(false),
                                            1 => Ok(true),
                                            _ => Err(Error::UnexpectedValue(value.clone())),
                                        })
                                        .transpose()
                                })?;

                            let partitions =
                                row.get_value(3).map_err(Into::into).and_then(|value| {
                                    value
                                        .as_integer()
                                        .map(|i| *i as i32)
                                        .ok_or(Error::UnexpectedValue(value))
                                })?;

                            let replication_factor =
                                row.get_value(4).map_err(Into::into).and_then(|value| {
                                    value
                                        .as_integer()
                                        .map(|i| *i as i32)
                                        .ok_or(Error::UnexpectedValue(value))
                                })?;

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
                                                .leader_epoch(-1)
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

    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        debug!(?states_filter);
        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut listed_groups = vec![];

        let mut rows = c
            .query(
                &sql_lookup("consumer_group_select.sql")?,
                &[self.cluster.as_str()],
            )
            .await?;

        while let Some(row) = rows.next().await? {
            let group_id = row.get_value(0).map_err(Into::into).and_then(|value| {
                value
                    .as_text()
                    .cloned()
                    .ok_or(Error::UnexpectedValue(value.clone()))
            })?;

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

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        debug!(?group_ids);
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let c = self.connection().await?;

            let mut consumer_offset = c
                .prepare(&sql_lookup("consumer_offset_delete_by_cg.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            let mut group_detail = c
                .prepare(&sql_lookup("consumer_group_detail_delete_by_cg.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            let mut group = c
                .prepare(&sql_lookup("consumer_group_delete.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            for group_id in group_ids {
                _ = consumer_offset
                    .execute((self.cluster.as_str(), group_id.as_str()))
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = group_detail
                    .execute((self.cluster.as_str(), group_id.as_str()))
                    .await
                    .inspect_err(|err| error!(?err))?;

                let rows = group
                    .execute((self.cluster.as_str(), group_id.as_str()))
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
                        &sql_lookup("consumer_group_select_by_name.sql")?,
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err, group_id))?
                {
                    let current = row
                        .get_value(1)
                        .map_err(Error::from)
                        .and_then(|value| {
                            value
                                .as_text()
                                .cloned()
                                .ok_or(Error::UnexpectedValue(value.clone()))
                        })
                        .and_then(|s| serde_json::from_str::<GroupDetail>(&s).map_err(Into::into))
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

        Ok(results)
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        debug!(cluster = self.cluster, group_id, ?detail, ?version);
        debug!(cluster = self.cluster, group_id, ?detail, ?version);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup("consumer_group_insert.sql")?,
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

        let detail = serde_json::to_value(detail).inspect(|detail| debug!(?detail))?;

        let outcome = if let Some(row) = self
            .prepare_query_opt(
                &tx,
                &sql_lookup("consumer_group_detail_insert.sql")?,
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
            row.get_value(2)
                .map_err(Error::from)
                .and_then(|value| {
                    value
                        .as_text()
                        .ok_or(Error::UnexpectedValue(value.clone()))
                        .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                })
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
                .prepare_query_one(
                    &tx,
                    &sql_lookup("consumer_group_detail.sql")?,
                    (group_id, self.cluster.as_str()),
                )
                .await
                .inspect(|row| debug!(?row))
                .inspect_err(|err| error!(?err))?;

            let version = row
                .get_value(0)
                .map_err(Error::from)
                .and_then(|value| {
                    value
                        .as_text()
                        .ok_or(Error::UnexpectedValue(value.clone()))
                        .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                })
                .inspect_err(|err| error!(?err))
                .map(|uuid| uuid.to_string())
                .map(Some)
                .map(|e_tag| Version {
                    e_tag,
                    version: None,
                })
                .inspect(|version| debug!(?version))?;

            let current = row
                .get_value(1)
                .map_err(Error::from)
                .and_then(|value| {
                    value
                        .as_text()
                        .map(|v| v.as_str())
                        .ok_or(Error::UnexpectedValue(value.clone()))
                        .and_then(|s| serde_json::from_str::<GroupDetail>(s).map_err(Into::into))
                })
                .inspect(|current| debug!(?current))
                .map(Box::new)?;

            Err(UpdateError::Outdated { current, version })
        };

        tx.commit().await.inspect_err(|err| error!(?err))?;

        debug!(?outcome);

        outcome
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        debug!(
            cluster = self.cluster,
            transaction_id, transaction_timeout_ms, producer_id, producer_epoch
        );
        match (producer_id, producer_epoch, transaction_id) {
            (Some(-1), Some(-1), Some(transaction_id)) => {
                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                if let Some(row) = self
                    .prepare_query_opt(
                        &tx,
                        &sql_lookup("producer_epoch_for_current_txn.sql")?,
                        (self.cluster.as_str(), transaction_id),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    let id = row
                        .get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .copied()
                                .ok_or(Error::UnexpectedValue(value))
                        })
                        .inspect_err(|err| error!(?err))?;
                    let epoch = row
                        .get_value(1)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .map(|i| *i as i16)
                                .ok_or(Error::UnexpectedValue(value))
                        })
                        .inspect_err(|err| error!(?err))?;
                    let status = row
                        .get_value(2)
                        .map_err(Error::from)
                        .and_then(|value| {
                            value.as_text().map_or(Ok(None), |status| {
                                TxnState::from_str(status.as_str()).map(Some)
                            })
                        })
                        .inspect_err(|err| error!(?err))?;

                    debug!(transaction_id, id, epoch, ?status);

                    if let Some(TxnState::Begin) = status {
                        let error = self
                            .end_in_tx(transaction_id, id, epoch, false, &tx)
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
                    .prepare_query_opt(
                        &tx,
                        &sql_lookup("txn_select_name.sql")?,
                        (self.cluster.as_str(), transaction_id),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    let producer = row
                        .get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .cloned()
                                .ok_or(Error::UnexpectedValue(value.clone()))
                        })
                        .inspect_err(|err| error!(?err))
                        .inspect(|producer| debug!(producer))?;

                    let row = self
                        .prepare_query_one(
                            &tx,
                            &sql_lookup("producer_epoch_insert.sql")?,
                            (self.cluster.as_str(), producer),
                        )
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    let epoch = row
                        .get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .map(|i| *i as i16)
                                .ok_or(Error::UnexpectedValue(value.clone()))
                        })
                        .inspect(|epoch| debug!(epoch))
                        .inspect_err(|err| error!(?err))? as i16;

                    (producer, epoch)
                } else {
                    let row = self
                        .prepare_query_one(
                            &tx,
                            &sql_lookup("producer_insert.sql")?,
                            &[self.cluster.as_str()],
                        )
                        .await
                        .inspect_err(|err| error!(?err))?;

                    let producer = row
                        .get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .copied()
                                .ok_or(Error::UnexpectedValue(value))
                        })
                        .inspect_err(|err| error!(?err))?;

                    let row = self
                        .prepare_query_one(
                            &tx,
                            &sql_lookup("producer_epoch_insert.sql")?,
                            (self.cluster.as_str(), producer),
                        )
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    let epoch = row.get_value(0).map_err(Into::into).and_then(|value| {
                        value
                            .as_integer()
                            .map(|i| *i as i16)
                            .ok_or(Error::UnexpectedValue(value))
                    })?;

                    assert_eq!(
                        1,
                        self.prepare_execute(
                            &tx,
                            &sql_lookup("txn_insert.sql")?,
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
                    self.prepare_execute(
                        &tx,
                        &sql_lookup("txn_detail_insert.sql")?,
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
            }

            (Some(-1), Some(-1), None) => {
                let mut connection = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .await?;

                let mut rows = tx
                    .query(
                        &sql_lookup("producer_insert.sql")?,
                        &[self.cluster.as_str()],
                    )
                    .await?;

                if let Some(row) = rows.next().await? {
                    let producer = row
                        .get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .copied()
                                .ok_or(Error::UnexpectedValue(value))
                        })
                        .inspect(|producer| debug!(producer))
                        .inspect_err(|err| error!(?err))?;

                    while let Some(row) = rows
                        .next()
                        .await
                        .inspect(|row| debug!(?row))
                        .inspect_err(|err| error!(?err))?
                    {
                        debug!(?row)
                    }

                    let mut rows = tx
                        .query(
                            &sql_lookup("producer_epoch_insert.sql")?,
                            (self.cluster.as_str(), producer),
                        )
                        .await
                        .inspect_err(|err| error!(?err, cluster = self.cluster, producer))?;

                    if let Some(row) = rows.next().await? {
                        let epoch = row
                            .get_value(0)
                            .map_err(Into::into)
                            .and_then(|value| {
                                value
                                    .as_integer()
                                    .map(|i| *i as i16)
                                    .ok_or(Error::UnexpectedValue(value))
                            })
                            .inspect(|epoch| debug!(epoch))?;

                        while let Some(row) = rows.next().await? {
                            debug!(?row)
                        }

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
                        .inspect(|response| debug!(?response))
                    } else {
                        Ok(ProducerIdResponse {
                            error: ErrorCode::UnknownServerError,
                            id: producer,
                            epoch: -1,
                        })
                        .inspect(|response| debug!(?response))
                    }
                } else {
                    Ok(ProducerIdResponse {
                        error: ErrorCode::UnknownServerError,
                        id: -1,
                        epoch: -1,
                    })
                    .inspect(|response| debug!(?response))
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
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, group_id
        );

        Ok(ErrorCode::None)
    }

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
                            .prepare_execute(
                                &tx,
                                &sql_lookup("txn_topition_insert.sql")?,
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

                _ = self
                    .prepare_execute(
                        &tx,
                        &sql_lookup("txn_detail_update_started_at.sql")?,
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

                tx.commit().await?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
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
        debug!(cluster = self.cluster, ?offsets);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let (producer_id, producer_epoch) = if let Some(row) = self
            .prepare_query_opt(
                &tx,
                &sql_lookup("producer_epoch_for_current_txn.sql")?,
                (self.cluster.as_str(), offsets.transaction_id.as_str()),
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let producer_id = row
                .get_value(0)
                .map(|value| value.as_integer().copied())
                .inspect_err(|err| error!(?err))?;

            let epoch = row
                .get_value(1)
                .map(|value| value.as_integer().map(|i| *i as i16))
                .inspect_err(|err| error!(?err))?;

            (producer_id, epoch)
        } else {
            (None, None)
        };

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup("consumer_group_insert.sql")?,
                (self.cluster.as_str(), offsets.group_id.as_str()),
            )
            .await?;

        debug!(?producer_id, ?producer_epoch);

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup("txn_offset_commit_insert.sql")?,
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
                        _ = self
                            .prepare_execute(
                                &tx,
                                &sql_lookup("txn_offset_commit_tp_insert.sql")?,
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

        tx.commit().await?;

        Ok(topics)
    }

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
            .end_in_tx(transaction_id, producer_id, producer_epoch, committed, &tx)
            .await?;

        tx.commit().await?;

        Ok(error_code)
    }

    async fn maintain(&self, _now: SystemTime) -> Result<()> {
        Ok(())
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

    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        todo!()
    }

    async fn upsert_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        todo!()
    }

    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        todo!()
    }

    async fn ping(&self) -> Result<()> {
        let c = self.connection().await?;
        let _ = c.query("ping.sql", ()).await?;
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
        todo!("{value:?}")
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
    use tempfile::tempdir;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

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

    #[ignore]
    #[tokio::test]
    async fn lite_system_time() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let db = turso::Builder::new_local(file_path.to_str().unwrap())
            .build()
            .await?;
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

    /// An open (uncommitted) transaction must pin the read_committed last stable
    /// offset below the high watermark. Regressed when `offset_stage` read the
    /// high-watermark column instead of the stable-offset column, so
    /// `read_committed` consumers saw uncommitted records.
    ///
    /// `#[ignore]`d like every other engine-level test here: turso/limbo cannot
    /// yet run the full produce flow in CI. Run manually against turso with
    /// `cargo nextest run --features turso -- --ignored`.
    #[ignore]
    #[tokio::test]
    async fn read_committed_last_stable_offset_pinned_by_open_transaction() -> Result<()> {
        use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = Url::parse(&format!("file://{}", file_path.display()))?;
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

        let topition = Topition::new("t", 0);

        // a transactional producer begins a transaction and produces, never commits
        let producer = engine
            .init_producer(Some("tx"), 10_000, Some(-1), Some(-1))
            .await?;

        _ = engine
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: "tx".into(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name("t".into())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"uncommitted").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = engine.produce(Some("tx"), &topition, batch).await?;

        let stage = engine.offset_stage(&topition).await?;

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
