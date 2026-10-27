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

/// Declares `Row`, one row of a query's result, and `RowReader`, which reads it, in the query's
/// module. The macro takes one line for each column, such as `low: Option<i64>`: the field and its
/// type. The field's name is the column's name in the query's result, so a query that needs another
/// Rust name renames the column with `as`.
///
/// `RowReader::new` finds each column's index by its name once, before the first row, and
/// `RowReader::read_row` reads each row by those indexes through any
/// [`DriverRow`](driver::DriverRow). Each field's type implements [`FromColumn`](driver::FromColumn).
/// A read error names the query and the column. The macro implements [`QueryRow`] for `Row`, so
/// that an engine's query helpers read each row of a result as `Row`.
macro_rules! sql_row {
    (
        $(#[$row_attribute:meta])*
        pub(crate) struct Row {
            $(
                $(#[$field_attribute:meta])*
                $field:ident: $type:ty
            ),+ $(,)?
        }
    ) => {
        $(#[$row_attribute])*
        pub(crate) struct Row {
            $(
                $(#[$field_attribute])*
                pub(crate) $field: $type,
            )+
        }

        /// Reads each [`Row`] of the query's result by the index of each of its columns.
        #[derive(Clone, Debug)]
        pub(crate) struct RowReader {
            query: &'static str,
            $($field: usize,)+
        }

        impl RowReader {
            /// Returns a reader for the result with `columns`, or
            /// [`Error::MissingColumn`](nisshi_storage::Error::MissingColumn) for the first
            /// column of [`Row`] that the result does not have.
            pub(crate) fn new(
                columns: &$crate::sql::result_columns::ResultColumns,
            ) -> ::nisshi_storage::Result<Self> {
                Ok(Self {
                    query: columns.query(),
                    $($field: columns.index(stringify!($field))?,)+
                })
            }

            /// Reads `row`, or returns [`Error::UnreadableColumn`](nisshi_storage::Error::UnreadableColumn) for the
            /// first column that the reader can't read as its field's type.
            pub(crate) fn read_row<R>(&self, row: &R) -> ::nisshi_storage::Result<Row>
            where
                R: $crate::sql::driver::DriverRow,
            {
                Ok(Row {
                    $(
                        $field: <$type as $crate::sql::driver::FromColumn>::from_column(
                            row,
                            self.$field,
                        )
                            .map_err(|source| ::nisshi_storage::Error::UnreadableColumn {
                                query: self.query,
                                column: stringify!($field),
                                source: Box::new(source),
                            })?,
                    )+
                })
            }
        }

        impl $crate::sql::QueryRow for Row {
            type Reader = RowReader;

            fn reader(
                columns: &$crate::sql::result_columns::ResultColumns,
            ) -> ::nisshi_storage::Result<RowReader> {
                RowReader::new(columns)
            }

            fn read<R>(reader: &RowReader, row: &R) -> ::nisshi_storage::Result<Self>
            where
                R: $crate::sql::driver::DriverRow,
            {
                reader.read_row(row)
            }
        }
    };
}

/// A row of one query's result, which [`sql_row!`] declares, read by column name. An engine's query
/// helpers take the row type, as in `query_first::<topic_select_name::Row>`, and return each row
/// read as it.
pub(crate) trait QueryRow: Sized {
    /// The reader that finds each column of a result by its name, before the first row.
    type Reader;

    /// Returns the reader for the result with `columns`.
    fn reader(columns: &result_columns::ResultColumns) -> Result<Self::Reader>;

    /// Reads `row` with `reader`.
    fn read<R: driver::DriverRow>(reader: &Self::Reader, row: &R) -> Result<Self>;
}

/// A cursor over one query's result on libSQL or Turso, which returns each of its rows as a
/// [`QueryRow`], in order. It reads a row only when the caller reaches it.
pub(crate) struct QueryRowCursor<R: driver::DriverRowCursor, T: QueryRow> {
    driver_cursor: R,
    reader: T::Reader,
}

impl<R: driver::DriverRowCursor, T: QueryRow> QueryRowCursor<R, T> {
    /// Returns a cursor over the result with `columns`, which reads each row from
    /// `driver_cursor`.
    pub(crate) fn new(columns: &result_columns::ResultColumns, driver_cursor: R) -> Result<Self> {
        T::reader(columns).map(|reader| Self {
            driver_cursor,
            reader,
        })
    }

    /// Returns the next row as `T`, or `None` after the last row.
    pub(crate) async fn next(&mut self) -> Result<Option<T>> {
        self.driver_cursor
            .next()
            .await?
            .map(|row| T::read(&self.reader, &row))
            .transpose()
    }
}

pub(crate) mod consumer_group_select;
pub(crate) mod driver;
pub(crate) mod producer_epochs_select;
pub(crate) mod result_columns;
pub(crate) mod topic_configuration_select;
pub(crate) mod topic_configuration_select_keys;
pub(crate) mod topic_select;
pub(crate) mod watermark_select_for_write;

use nisshi_sans_io::{ErrorCode, record::deflated};
use nisshi_storage::{Error, ProduceEpoch, Result, StoredEpochs};
use std::{
    cmp::Ordering,
    hash::{DefaultHasher, Hash, Hasher as _},
};
use tracing::debug;
use uuid::Uuid;

use std::{collections::BTreeMap, ops::Deref, sync::LazyLock};

pub(crate) struct Cache(pub BTreeMap<&'static str, String>);

impl Deref for Cache {
    type Target = BTreeMap<&'static str, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Cache {
    pub(crate) fn new(inner: BTreeMap<&'static str, String>) -> Self {
        Self(inner)
    }

    pub(crate) fn get(&self, key: &str) -> Result<&str> {
        self.0
            .get(key)
            .map(|s| s.as_str())
            .ok_or(Error::UnknownCacheKey(key.to_owned()))
    }
}

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

/// Returns the SQL cache key of the file `$file` in the folder `$folder` of this crate's `src`: the
/// file's name for a file in `sql`, and its path for a file in another folder.
macro_rules! cache_key {
    ("sql" / $file:literal) => {
        $file
    };
    ($folder:literal / $file:literal) => {
        concat!($folder, "/", $file)
    };
}

/// Declares the SQL cache, [`SQL`], which holds every query that the engines run, keyed by
/// [`cache_key!`].
///
/// Each line names a query by its folder and file in this crate's `src`. A line in `rows` names
/// the constant that a query's module declares, because an engine reads the query's rows through
/// that module, and a compile-time assertion checks that the constant holds the query's key. A
/// line in `plain` declares the constant here.
macro_rules! queries {
    (
        rows {
            $($module:ident :: $module_constant:ident = $row_folder:tt / $row_file:literal,)+
        }
        plain {
            $($constant:ident = $folder:tt / $file:literal,)+
        }
    ) => {
        $(
            #[doc = concat!("The `", $folder, "/", $file, "` query's name in the SQL cache.")]
            pub(crate) const $constant: &str = cache_key!($folder / $file);
        )+

        $(
            const _: () = assert!(
                const_str_eq($module::$module_constant, cache_key!($row_folder / $row_file)),
                concat!(
                    stringify!($module::$module_constant),
                    " must be the cache key of ",
                    $row_folder,
                    "/",
                    $row_file
                ),
            );
        )+

        pub(crate) static SQL: LazyLock<Cache> = LazyLock::new(|| {
            Cache::new(BTreeMap::from([
                $(
                    (
                        $module::$module_constant,
                        include_sql!(concat!($row_folder, "/", $row_file)),
                    ),
                )+
                $(($constant, include_sql!(concat!($folder, "/", $file))),)+
            ]))
        });
    };
}

/// Returns whether `left` and `right` hold the same text. `str`'s `==` can't run in a constant.
const fn const_str_eq(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());

    if left.len() != right.len() {
        return false;
    }

    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }

    true
}

queries! {
    rows {
        consumer_group_select::QUERY = "sql" / "consumer_group_select.sql",
        producer_epochs_select::QUERY = "sql" / "producer_epochs_select.sql",
        topic_configuration_select::QUERY = "sql" / "topic_configuration_select.sql",
        topic_configuration_select_keys::QUERY = "pg" / "topic_configuration_select_keys.sql",
        topic_select::QUERY = "sql" / "topic_select.sql",
        watermark_select_for_write::LITE_QUERY = "lite" / "watermark_select_for_write.sql",
        watermark_select_for_write::QUERY = "sql" / "watermark_select_for_write.sql",
    }
    plain {
        CONSUMER_GROUP_DELETE = "sql" / "consumer_group_delete.sql",
        CONSUMER_GROUP_DETAIL = "sql" / "consumer_group_detail.sql",
        CONSUMER_GROUP_DETAIL_DELETE_BY_CG = "sql" / "consumer_group_detail_delete_by_cg.sql",
        CONSUMER_GROUP_DETAIL_INSERT = "sql" / "consumer_group_detail_insert.sql",
        CONSUMER_GROUP_INSERT = "sql" / "consumer_group_insert.sql",
        CONSUMER_GROUP_SELECT_BY_NAME = "sql" / "consumer_group_select_by_name.sql",
        CONSUMER_OFFSET_DELETE_BY_CG = "sql" / "consumer_offset_delete_by_cg.sql",
        CONSUMER_OFFSET_DELETE_BY_TOPIC = "sql" / "consumer_offset_delete_by_topic.sql",
        CONSUMER_OFFSET_INSERT = "sql" / "consumer_offset_insert.sql",
        CONSUMER_OFFSET_INSERT_FROM_TXN = "sql" / "consumer_offset_insert_from_txn.sql",
        CONSUMER_OFFSET_SELECT = "sql" / "consumer_offset_select.sql",
        CONSUMER_OFFSET_SELECT_BY_GROUP = "sql" / "consumer_offset_select_by_group.sql",
        HEADER_COPY = "sql" / "header_copy.sql",
        HEADER_DELETE_BY_TOPIC = "sql" / "header_delete_by_topic.sql",
        HEADER_FETCH = "sql" / "header_fetch.sql",
        HEADER_FETCH_RANGE = "sql" / "header_fetch_range.sql",
        HEADER_INSERT = "sql" / "header_insert.sql",
        LIST_EARLIEST_OFFSET = "sql" / "list_earliest_offset.sql",
        LIST_LATEST_OFFSET_COMMITTED = "sql" / "list_latest_offset_committed.sql",
        LIST_LATEST_OFFSET_TIMESTAMP = "sql" / "list_latest_offset_timestamp.sql",
        LIST_LATEST_OFFSET_UNCOMMITTED = "sql" / "list_latest_offset_uncommitted.sql",
        LITE_POLICY_COMPACT_COMPACTION = "lite" / "policy_compact_compaction.sql",
        LITE_POLICY_COMPACT_DELETE = "lite" / "policy_compact_delete.sql",
        LITE_POLICY_COMPACT_DISTINCT_K = "lite" / "policy_compact_distinct_k.sql",
        LITE_POLICY_COMPACT_MAX_OFFSET_ID = "lite" / "policy_compact_max_offset_id.sql",
        LITE_POLICY_COMPACT_TOPITIONS = "lite" / "policy_compact_topitions.sql",
        LITE_POLICY_DELETE = "lite" / "policy_delete.sql",
        LITE_POLICY_DELETE_BYTES = "lite" / "policy_delete_bytes.sql",
        LITE_VACUUM_INTO = "lite" / "vacuum_into.sql",
        MAINTAIN_VACUUM = "sql" / "maintain-vacuum.sql",
        NISSHI_SCHEMA_VERSION_CREATE = "sql" / "nisshi_schema_version_create.sql",
        NISSHI_SCHEMA_VERSION_INSERT = "sql" / "nisshi_schema_version_insert.sql",
        NISSHI_SCHEMA_VERSION_SELECT = "sql" / "nisshi_schema_version_select.sql",
        PG_NISSHI_SCHEMA_VERSION_EXISTS = "pg" / "nisshi_schema_version_exists.sql",
        PG_RECORD_FETCH = "pg" / "record_fetch.sql",
        PG_RECORD_FETCH_KEYED = "pg" / "record_fetch_keyed.sql",
        PG_SEARCH_PATH_SET_TO_CLUSTER_NAMESPACE = "pg" / "search_path_set_to_cluster_namespace.sql",
        PG_TOPIC_SELECT_UUID = "pg" / "topic_select_uuid.sql",
        PING = "sql" / "ping.sql",
        POLICY_COMPACT = "sql" / "policy_compact.sql",
        POLICY_DELETE = "sql" / "policy_delete.sql",
        POLICY_DELETE_BYTES = "sql" / "policy_delete_bytes.sql",
        PRODUCER_DETAIL_DELETE_BY_TOPIC = "sql" / "producer_detail_delete_by_topic.sql",
        PRODUCER_DETAIL_INSERT = "sql" / "producer_detail_insert.sql",
        PRODUCER_EPOCH_CURRENT_FOR_PRODUCER = "sql" / "producer_epoch_current_for_producer.sql",
        PRODUCER_EPOCH_FOR_CURRENT_TXN = "sql" / "producer_epoch_for_current_txn.sql",
        PRODUCER_EPOCH_INSERT = "sql" / "producer_epoch_insert.sql",
        PRODUCER_EPOCH_INSERT_GIVEN = "sql" / "producer_epoch_insert_given.sql",
        PRODUCER_INSERT = "sql" / "producer_insert.sql",
        PRODUCER_SELECT_FOR_UPDATE = "sql" / "producer_select_for_update.sql",
        PRODUCER_UPDATE_EPOCH_WITH_TXN = "sql" / "producer_update_epoch_with_txn.sql",
        PRODUCER_UPDATE_SEQUENCE = "sql" / "producer_update_sequence.sql",
        RECORD_COPY = "sql" / "record_copy.sql",
        RECORD_DELETE_BELOW_LOG_START = "sql" / "record_delete_below_log_start.sql",
        RECORD_DELETE_BY_OFFSET = "sql" / "record_delete_by_offset.sql",
        RECORD_DELETE_BY_TOPIC = "sql" / "record_delete_by_topic.sql",
        RECORD_FETCH = "sql" / "record_fetch.sql",
        RECORD_FETCH_KEYED = "sql" / "record_fetch_keyed.sql",
        RECORD_INSERT = "sql" / "record_insert.sql",
        REGISTER_BROKER = "sql" / "register_broker.sql",
        SCRAM_CREDENTIAL_DELETE = "sql" / "scram_credential_delete.sql",
        SCRAM_CREDENTIAL_INSERT = "sql" / "scram_credential_insert.sql",
        SCRAM_CREDENTIAL_SELECT = "sql" / "scram_credential_select.sql",
        TOPIC_BY_CLUSTER = "sql" / "topic_by_cluster.sql",
        TOPIC_BY_UUID = "sql" / "topic_by_uuid.sql",
        TOPIC_CONFIGURATION_DELETE = "sql" / "topic_configuration_delete.sql",
        TOPIC_CONFIGURATION_DELETE_BY_TOPIC = "sql" / "topic_configuration_delete_by_topic.sql",
        TOPIC_CONFIGURATION_SELECT_NAME = "sql" / "topic_configuration_select_name.sql",
        TOPIC_CONFIGURATION_UPSERT = "sql" / "topic_configuration_upsert.sql",
        TOPIC_DELETE_BY = "sql" / "topic_delete_by.sql",
        TOPIC_INSERT = "sql" / "topic_insert.sql",
        TOPIC_SELECT_NAME = "sql" / "topic_select_name.sql",
        TOPIC_SELECT_NAME_FOR_UPDATE = "sql" / "topic_select_name_for_update.sql",
        TOPIC_SELECT_UUID = "sql" / "topic_select_uuid.sql",
        TOPITION_DELETE_BY_TOPIC = "sql" / "topition_delete_by_topic.sql",
        TOPITION_INSERT = "sql" / "topition_insert.sql",
        TOPITION_SELECT = "sql" / "topition_select.sql",
        TOPITION_SELECT_ID = "sql" / "topition_select_id.sql",
        TXN_DETAIL_INSERT = "sql" / "txn_detail_insert.sql",
        TXN_DETAIL_SELECT = "sql" / "txn_detail_select.sql",
        TXN_DETAIL_SELECT_CURRENT = "sql" / "txn_detail_select_current.sql",
        TXN_DETAIL_SELECT_FOR_UPDATE = "sql" / "txn_detail_select_for_update.sql",
        TXN_DETAIL_SELECT_STATUS_FOR_UPDATE = "sql" / "txn_detail_select_status_for_update.sql",
        TXN_DETAIL_SELECT_TIMED_OUT = "sql" / "txn_detail_select_timed_out.sql",
        TXN_DETAIL_UPDATE_SEQUENCE = "sql" / "txn_detail_update_sequence.sql",
        TXN_DETAIL_UPDATE_STARTED_AT = "sql" / "txn_detail_update_started_at.sql",
        TXN_INSERT = "sql" / "txn_insert.sql",
        TXN_OFFSET_COMMIT_DELETE_BY_TXN = "sql" / "txn_offset_commit_delete_by_txn.sql",
        TXN_OFFSET_COMMIT_INSERT = "sql" / "txn_offset_commit_insert.sql",
        TXN_OFFSET_COMMIT_TP_DELETE_BY_TOPIC = "sql" / "txn_offset_commit_tp_delete_by_topic.sql",
        TXN_OFFSET_COMMIT_TP_DELETE_BY_TXN = "sql" / "txn_offset_commit_tp_delete_by_txn.sql",
        TXN_OFFSET_COMMIT_TP_INSERT = "sql" / "txn_offset_commit_tp_insert.sql",
        TXN_PRODUCE_OFFSET_DELETE_BY_TOPIC = "sql" / "txn_produce_offset_delete_by_topic.sql",
        TXN_PRODUCE_OFFSET_DELETE_BY_TXN = "sql" / "txn_produce_offset_delete_by_txn.sql",
        TXN_PRODUCE_OFFSET_INSERT = "sql" / "txn_produce_offset_insert.sql",
        TXN_PRODUCE_OFFSET_SELECT_OFFSET_RANGE = "sql" / "txn_produce_offset_select_offset_range.sql",
        TXN_PRODUCE_OFFSET_SELECT_OVERLAPPING_TXN = "sql" / "txn_produce_offset_select_overlapping_txn.sql",
        TXN_SELECT_NAME = "sql" / "txn_select_name.sql",
        TXN_SELECT_NAME_FOR_UPDATE = "sql" / "txn_select_name_for_update.sql",
        TXN_SELECT_PRODUCED_TOPITIONS = "sql" / "txn_select_produced_topitions.sql",
        TXN_SELECT_PRODUCER_EPOCH = "sql" / "txn_select_producer_epoch.sql",
        TXN_STATUS_UPDATE = "sql" / "txn_status_update.sql",
        TXN_TOPITION_DELETE_BY_TOPIC = "sql" / "txn_topition_delete_by_topic.sql",
        TXN_TOPITION_DELETE_BY_TXN = "sql" / "txn_topition_delete_by_txn.sql",
        TXN_TOPITION_INSERT = "sql" / "txn_topition_insert.sql",
        TXN_TOPITION_SELECT = "sql" / "txn_topition_select.sql",
        TXN_TOPITION_SELECT_TXNS = "sql" / "txn_topition_select_txns.sql",
        TXN_UPDATE_PRODUCER = "sql" / "txn_update_producer.sql",
        VIRTUAL_TOPIC_UPSERT = "sql" / "virtual_topic_upsert.sql",
        WATERMARK_DELETE_BY_TOPIC = "sql" / "watermark_delete_by_topic.sql",
        WATERMARK_INSERT = "sql" / "watermark_insert.sql",
        WATERMARK_INSERT_FROM_TXN = "sql" / "watermark_insert_from_txn.sql",
        WATERMARK_SELECT = "sql" / "watermark_select.sql",
        WATERMARK_SELECT_STABLE = "sql" / "watermark_select_stable.sql",
        WATERMARK_UPDATE = "sql" / "watermark_update.sql",
    }
}

pub(crate) fn remove_comments(commented: &str) -> String {
    commented.lines().fold(String::new(), |uncommented, line| {
        if let Some(position) = line.find("--") {
            match line.split_at(position) {
                ("", _) => uncommented,
                (before, _) => {
                    if uncommented.is_empty() {
                        before.trim().into()
                    } else {
                        format!("{uncommented} {before}")
                    }
                }
            }
        } else if line.trim().is_empty() {
            uncommented
        } else if uncommented.is_empty() {
            line.trim().into()
        } else {
            format!("{uncommented} {}", line.trim())
        }
    })
}

/// Checks a batch's epoch against the epochs stored for its producer, read with
/// [`producer_epochs_select`]. The batch's producer is transactional when `stored` says so,
/// when the request has a transactional ID, or when the batch is transactional.
pub(crate) fn idempotent_epoch_check(
    transaction_id: Option<&str>,
    stored: StoredEpochs,
    deflated: &deflated::Batch,
) -> Result<ProduceEpoch> {
    let stored = StoredEpochs {
        transactional: stored.transactional
            || transaction_id.is_some()
            || deflated.is_transactional(),
        ..stored
    };

    stored
        .check_batch(deflated.producer_epoch, deflated.base_sequence)
        .inspect_err(|error| debug!(?error, ?stored, deflated.producer_epoch))
        .map_err(Error::Api)
}

/// Checks a batch's first sequence against `sequence`, the next sequence stored for its
/// producer's epoch on the partition, and returns how far the batch advances it.
///
/// The caller checks the batch's epoch first, with [`idempotent_epoch_check`].
pub(crate) fn idempotent_sequence_check(sequence: i32, deflated: &deflated::Batch) -> Result<i32> {
    match sequence.cmp(&deflated.base_sequence) {
        Ordering::Equal => Ok(deflated.last_offset_delta + 1),

        Ordering::Greater => {
            debug!(?sequence, ?deflated.base_sequence);
            Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
        }

        Ordering::Less => {
            debug!(?sequence, ?deflated.base_sequence);
            Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
        }
    }
}

pub(crate) fn default_hash<H>(h: &H) -> Uuid
where
    H: Hash,
{
    let mut s = DefaultHasher::new();
    h.hash(&mut s);
    Uuid::from_u128(s.finish() as u128)
}

#[cfg(test)]
mod tests {
    use nisshi_storage::{Error, Result};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

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

    #[test]
    fn remove_comments() -> Result<()> {
        let _guard = init_tracing()?;

        assert_eq!(String::from(""), super::remove_comments(""));
        assert_eq!(String::from("pqr"), super::remove_comments("-- abc\npqr"));
        assert_eq!(String::from("abc"), super::remove_comments("abc -- def"));
        assert_eq!(String::from("abc def"), super::remove_comments("abc\ndef"));
        assert_eq!(
            String::from("abc def"),
            super::remove_comments("abc\n\ndef")
        );
        assert_eq!(
            String::from("abc def"),
            super::remove_comments("abc \ndef ")
        );

        Ok(())
    }
}
