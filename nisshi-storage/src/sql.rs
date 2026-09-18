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

use nisshi_sans_io::{ErrorCode, record::deflated};
use tracing::debug;
use uuid::Uuid;

use crate::{Error, Result};
use std::{
    cmp::Ordering,
    hash::{DefaultHasher, Hash, Hasher as _},
};

#[cfg(any(feature = "libsql", feature = "postgres", feature = "turso"))]
use std::{collections::BTreeMap, ops::Deref, sync::LazyLock};

#[cfg(any(feature = "libsql", feature = "postgres", feature = "turso"))]
pub(crate) struct Cache(pub BTreeMap<&'static str, String>);

#[cfg(any(feature = "libsql", feature = "postgres", feature = "turso"))]
impl Deref for Cache {
    type Target = BTreeMap<&'static str, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(any(feature = "libsql", feature = "postgres", feature = "turso"))]
impl Cache {
    pub(crate) fn new(inner: BTreeMap<&'static str, String>) -> Self {
        Self(inner)
    }

    #[cfg(any(feature = "postgres", feature = "turso"))]
    pub(crate) fn get(&self, key: &str) -> Result<&str> {
        self.0
            .get(key)
            .map(|s| s.as_str())
            .ok_or(Error::UnknownCacheKey(key.to_owned()))
    }
}

#[cfg(any(feature = "libsql", feature = "postgres", feature = "turso"))]
macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

#[cfg(any(feature = "libsql", feature = "postgres", feature = "turso"))]
pub(crate) static SQL: LazyLock<Cache> = LazyLock::new(|| {
    let mapping = [
        (
            "maintain-vacuum.sql",
            include_sql!("sql/maintain-vacuum.sql"),
        ),
        (
            "consumer_group_delete.sql",
            include_sql!("sql/consumer_group_delete.sql"),
        ),
        (
            "consumer_group_detail_delete_by_cg.sql",
            include_sql!("sql/consumer_group_detail_delete_by_cg.sql"),
        ),
        (
            "consumer_group_detail_insert.sql",
            include_sql!("sql/consumer_group_detail_insert.sql"),
        ),
        (
            "consumer_group_detail.sql",
            include_sql!("sql/consumer_group_detail.sql"),
        ),
        (
            "consumer_group_insert.sql",
            include_sql!("sql/consumer_group_insert.sql"),
        ),
        (
            "consumer_group_select_by_name.sql",
            include_sql!("sql/consumer_group_select_by_name.sql"),
        ),
        (
            "consumer_group_select.sql",
            include_sql!("sql/consumer_group_select.sql"),
        ),
        (
            "consumer_offset_delete_by_cg.sql",
            include_sql!("sql/consumer_offset_delete_by_cg.sql"),
        ),
        (
            "consumer_offset_delete_by_topic.sql",
            include_sql!("sql/consumer_offset_delete_by_topic.sql"),
        ),
        (
            "consumer_offset_insert_from_txn.sql",
            include_sql!("sql/consumer_offset_insert_from_txn.sql"),
        ),
        (
            "consumer_offset_insert.sql",
            include_sql!("sql/consumer_offset_insert.sql"),
        ),
        (
            "consumer_offset_select_by_group.sql",
            include_sql!("sql/consumer_offset_select_by_group.sql"),
        ),
        (
            "consumer_offset_select.sql",
            include_sql!("sql/consumer_offset_select.sql"),
        ),
        ("header_copy.sql", include_sql!("sql/header_copy.sql")),
        (
            "header_delete_by_topic.sql",
            include_sql!("sql/header_delete_by_topic.sql"),
        ),
        ("header_fetch.sql", include_sql!("sql/header_fetch.sql")),
        ("header_insert.sql", include_sql!("sql/header_insert.sql")),
        (
            "list_earliest_offset.sql",
            include_sql!("sql/list_earliest_offset.sql"),
        ),
        (
            "list_latest_offset_committed.sql",
            include_sql!("sql/list_latest_offset_committed.sql"),
        ),
        (
            "list_latest_offset_timestamp.sql",
            include_sql!("sql/list_latest_offset_timestamp.sql"),
        ),
        (
            "list_latest_offset_uncommitted.sql",
            include_sql!("sql/list_latest_offset_uncommitted.sql"),
        ),
        (
            "lite/policy_compact_compaction.sql",
            include_sql!("lite/policy_compact_compaction.sql"),
        ),
        (
            "lite/policy_compact_delete.sql",
            include_sql!("lite/policy_compact_delete.sql"),
        ),
        (
            "lite/policy_compact_distinct_k.sql",
            include_sql!("lite/policy_compact_distinct_k.sql"),
        ),
        (
            "lite/policy_compact_max_offset_id.sql",
            include_sql!("lite/policy_compact_max_offset_id.sql"),
        ),
        (
            "lite/policy_compact_topitions.sql",
            include_sql!("lite/policy_compact_topitions.sql"),
        ),
        (
            "lite/policy_delete.sql",
            include_sql!("lite/policy_delete.sql"),
        ),
        ("lite/vacuum_into.sql", include_sql!("lite/vacuum_into.sql")),
        ("policy_compact.sql", include_sql!("sql/policy_compact.sql")),
        ("policy_delete.sql", include_sql!("sql/policy_delete.sql")),
        ("ping.sql", "select 1 + 1".to_string()),
        (
            "producer_detail_delete_by_topic.sql",
            include_sql!("sql/producer_detail_delete_by_topic.sql"),
        ),
        (
            "producer_detail_insert.sql",
            include_sql!("sql/producer_detail_insert.sql"),
        ),
        (
            "producer_epoch_current_for_producer.sql",
            include_sql!("sql/producer_epoch_current_for_producer.sql"),
        ),
        (
            "producer_epoch_for_current_txn.sql",
            include_sql!("sql/producer_epoch_for_current_txn.sql"),
        ),
        (
            "producer_epoch_insert.sql",
            include_sql!("sql/producer_epoch_insert.sql"),
        ),
        (
            "producer_insert.sql",
            include_sql!("sql/producer_insert.sql"),
        ),
        (
            "producer_select_for_update.sql",
            include_sql!("sql/producer_select_for_update.sql"),
        ),
        (
            "producer_update_epoch_with_txn.sql",
            include_sql!("sql/producer_update_epoch_with_txn.sql"),
        ),
        (
            "producer_update_sequence.sql",
            include_sql!("sql/producer_update_sequence.sql"),
        ),
        ("record_copy.sql", include_sql!("sql/record_copy.sql")),
        (
            "record_delete_by_topic.sql",
            include_sql!("sql/record_delete_by_topic.sql"),
        ),
        ("record_fetch.sql", include_sql!("sql/record_fetch.sql")),
        (
            "record_fetch_keyed.sql",
            include_sql!("sql/record_fetch_keyed.sql"),
        ),
        ("record_fetch_pg.sql", include_sql!("pg/record_fetch.sql")),
        (
            "record_fetch_pg_keyed.sql",
            include_sql!("pg/record_fetch_keyed.sql"),
        ),
        ("record_insert.sql", include_sql!("sql/record_insert.sql")),
        (
            "register_broker.sql",
            include_sql!("sql/register_broker.sql"),
        ),
        (
            "scram_credential_delete.sql",
            include_sql!("sql/scram_credential_delete.sql"),
        ),
        (
            "scram_credential_insert.sql",
            include_sql!("sql/scram_credential_insert.sql"),
        ),
        (
            "scram_credential_select.sql",
            include_sql!("sql/scram_credential_select.sql"),
        ),
        (
            "topic_by_cluster.sql",
            include_sql!("sql/topic_by_cluster.sql"),
        ),
        ("topic_by_uuid.sql", include_sql!("sql/topic_by_uuid.sql")),
        (
            "topic_configuration_delete_by_topic.sql",
            include_sql!("sql/topic_configuration_delete_by_topic.sql"),
        ),
        (
            "topic_configuration_delete.sql",
            include_sql!("sql/topic_configuration_delete.sql"),
        ),
        (
            "topic_configuration_select.sql",
            include_sql!("sql/topic_configuration_select.sql"),
        ),
        (
            "topic_configuration_upsert.sql",
            include_sql!("sql/topic_configuration_upsert.sql"),
        ),
        (
            "topic_delete_by.sql",
            include_sql!("sql/topic_delete_by.sql"),
        ),
        ("topic_insert.sql", include_sql!("sql/topic_insert.sql")),
        ("topic_select.sql", include_sql!("sql/topic_select.sql")),
        (
            "topic_select_name.sql",
            include_sql!("sql/topic_select_name.sql"),
        ),
        (
            "topic_select_uuid.sql",
            include_sql!("sql/topic_select_uuid.sql"),
        ),
        (
            "pg/topic_select_uuid.sql",
            include_sql!("pg/topic_select_uuid.sql"),
        ),
        (
            "topition_delete_by_topic.sql",
            include_sql!("sql/topition_delete_by_topic.sql"),
        ),
        (
            "topition_insert.sql",
            include_sql!("sql/topition_insert.sql"),
        ),
        (
            "topition_select.sql",
            include_sql!("sql/topition_select.sql"),
        ),
        (
            "topition_select_id.sql",
            include_sql!("sql/topition_select_id.sql"),
        ),
        (
            "txn_detail_insert.sql",
            include_sql!("sql/txn_detail_insert.sql"),
        ),
        (
            "txn_detail_select_current.sql",
            include_sql!("sql/txn_detail_select_current.sql"),
        ),
        (
            "txn_detail_select_for_update.sql",
            include_sql!("sql/txn_detail_select_for_update.sql"),
        ),
        (
            "txn_detail_select_status_for_update.sql",
            include_sql!("sql/txn_detail_select_status_for_update.sql"),
        ),
        (
            "txn_detail_select_timed_out.sql",
            include_sql!("sql/txn_detail_select_timed_out.sql"),
        ),
        (
            "txn_detail_select.sql",
            include_sql!("sql/txn_detail_select.sql"),
        ),
        (
            "txn_detail_update_sequence.sql",
            include_sql!("sql/txn_detail_update_sequence.sql"),
        ),
        (
            "txn_detail_update_started_at.sql",
            include_sql!("sql/txn_detail_update_started_at.sql"),
        ),
        ("txn_insert.sql", include_sql!("sql/txn_insert.sql")),
        (
            "txn_offset_commit_delete_by_txn.sql",
            include_sql!("sql/txn_offset_commit_delete_by_txn.sql"),
        ),
        (
            "txn_offset_commit_insert.sql",
            include_sql!("sql/txn_offset_commit_insert.sql"),
        ),
        (
            "txn_offset_commit_tp_delete_by_topic.sql",
            include_sql!("sql/txn_offset_commit_tp_delete_by_topic.sql"),
        ),
        (
            "txn_offset_commit_tp_delete_by_txn.sql",
            include_sql!("sql/txn_offset_commit_tp_delete_by_txn.sql"),
        ),
        (
            "txn_offset_commit_tp_insert.sql",
            include_sql!("sql/txn_offset_commit_tp_insert.sql"),
        ),
        (
            "txn_produce_offset_delete_by_topic.sql",
            include_sql!("sql/txn_produce_offset_delete_by_topic.sql"),
        ),
        (
            "txn_produce_offset_delete_by_txn.sql",
            include_sql!("sql/txn_produce_offset_delete_by_txn.sql"),
        ),
        (
            "txn_produce_offset_insert.sql",
            include_sql!("sql/txn_produce_offset_insert.sql"),
        ),
        (
            "txn_produce_offset_select_offset_range.sql",
            include_sql!("sql/txn_produce_offset_select_offset_range.sql"),
        ),
        (
            "txn_produce_offset_select_overlapping_txn.sql",
            include_sql!("sql/txn_produce_offset_select_overlapping_txn.sql"),
        ),
        (
            "txn_select_name.sql",
            include_sql!("sql/txn_select_name.sql"),
        ),
        (
            "txn_select_produced_topitions.sql",
            include_sql!("sql/txn_select_produced_topitions.sql"),
        ),
        (
            "txn_select_producer_epoch.sql",
            include_sql!("sql/txn_select_producer_epoch.sql"),
        ),
        (
            "txn_status_update.sql",
            include_sql!("sql/txn_status_update.sql"),
        ),
        (
            "txn_topition_delete_by_topic.sql",
            include_sql!("sql/txn_topition_delete_by_topic.sql"),
        ),
        (
            "txn_topition_delete_by_txn.sql",
            include_sql!("sql/txn_topition_delete_by_txn.sql"),
        ),
        (
            "txn_topition_insert.sql",
            include_sql!("sql/txn_topition_insert.sql"),
        ),
        (
            "txn_topition_select_txns.sql",
            include_sql!("sql/txn_topition_select_txns.sql"),
        ),
        (
            "txn_topition_select.sql",
            include_sql!("sql/txn_topition_select.sql"),
        ),
        (
            "virtual_topic_upsert.sql",
            include_sql!("sql/virtual_topic_upsert.sql"),
        ),
        (
            "watermark_delete_by_topic.sql",
            include_sql!("sql/watermark_delete_by_topic.sql"),
        ),
        (
            "watermark_insert_from_txn.sql",
            include_sql!("sql/watermark_insert_from_txn.sql"),
        ),
        (
            "watermark_insert.sql",
            include_sql!("sql/watermark_insert.sql"),
        ),
        (
            "watermark_select_for_update.sql",
            include_sql!("sql/watermark_select_for_update.sql"),
        ),
        (
            "watermark_select_no_update.sql",
            include_sql!("sql/watermark_select_no_update.sql"),
        ),
        (
            "watermark_select_stable.sql",
            include_sql!("sql/watermark_select_stable.sql"),
        ),
        (
            "watermark_select.sql",
            include_sql!("sql/watermark_select.sql"),
        ),
        (
            "watermark_update.sql",
            include_sql!("sql/watermark_update.sql"),
        ),
    ];

    Cache::new(BTreeMap::from(mapping))
});

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

pub(crate) fn idempotent_sequence_check(
    producer_epoch: &i16,
    sequence: &i32,
    deflated: &deflated::Batch,
) -> Result<i32> {
    match producer_epoch.cmp(&deflated.producer_epoch) {
        Ordering::Equal => match sequence.cmp(&deflated.base_sequence) {
            Ordering::Equal => Ok(deflated.last_offset_delta + 1),

            Ordering::Greater => {
                debug!(?sequence, ?deflated.base_sequence);
                Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
            }

            Ordering::Less => {
                debug!(?sequence, ?deflated.base_sequence);
                Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
            }
        },

        Ordering::Greater => Err(Error::Api(ErrorCode::ProducerFenced)),

        Ordering::Less => Err(Error::Api(ErrorCode::InvalidProducerEpoch)),
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
    use crate::{Error, Result};
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
