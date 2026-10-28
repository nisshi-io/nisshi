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

//! The query that reads a partition's watermarks and its topic's `max.message.bytes` before a write
//! to the partition. A change to either version's `select` list must keep the column names [`LOW`],
//! [`HIGH`] and [`MAX_MESSAGE_BYTES`].
//!
//! The query takes three parameters: `$1` is the cluster's name, `$2` the topic's name, and `$3`
//! the partition's index.

/// The PostgreSQL version's name in the SQL cache. This version locks the partition's watermark
/// row.
pub(crate) const QUERY: &str = "watermark_select_for_write.sql";

/// The libSQL and Turso version's name in the SQL cache. This version does not lock rows, because
/// SQLite does not lock rows.
pub(crate) const LITE_QUERY: &str = "lite/watermark_select_for_write.sql";

/// The column that holds the partition's low watermark.
pub(crate) const LOW: &str = "low";

/// The column that holds the partition's high watermark.
pub(crate) const HIGH: &str = "high";

/// The column that holds the topic's stored `max.message.bytes`.
pub(crate) const MAX_MESSAGE_BYTES: &str = "max_message_bytes";

/// One row of the query's result: a partition's watermarks and its topic's stored
/// `max.message.bytes`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Row {
    /// The partition's low watermark.
    pub(crate) low: Option<i64>,
    /// The partition's high watermark.
    pub(crate) high: Option<i64>,
    /// The topic's stored `max.message.bytes`, or `None` when the topic does not set the key.
    pub(crate) max_message_bytes: Option<String>,
}
