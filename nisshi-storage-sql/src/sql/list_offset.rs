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

//! The queries that answer a ListOffsets request for one partition, and [`Row`], their result.
//! An engine runs one of the four versions and reads its row the same way, so a change to one
//! version's `select` list must keep the column names that [`Row`] declares.
//!
//! Each version takes three parameters: `$1` is the cluster's name, `$2` the topic's name, and `$3`
//! the partition's index. The timestamp version also takes the timestamp as `$4`. The earliest and
//! latest versions return no row for a partition that does not exist. The timestamp version also
//! returns no row when no record of the partition has a timestamp at or after `$4`.

/// The earliest version's name in the SQL cache. This version answers the offset of the first
/// stored record at or above the low watermark, or the high watermark when no such record exists.
pub(crate) const EARLIEST_QUERY: &str = "list_earliest_offset.sql";

/// The `read_committed` latest version's name in the SQL cache. This version answers the last
/// stable offset.
pub(crate) const LATEST_COMMITTED_QUERY: &str = "list_latest_offset_committed.sql";

/// The timestamp version's name in the SQL cache. This version answers the first record whose
/// timestamp is at or after `$4`.
pub(crate) const LATEST_TIMESTAMP_QUERY: &str = "list_latest_offset_timestamp.sql";

/// The `read_uncommitted` latest version's name in the SQL cache. This version answers the high
/// watermark.
pub(crate) const LATEST_UNCOMMITTED_QUERY: &str = "list_latest_offset_uncommitted.sql";

sql_row! {
    /// The offset that answers the request.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The offset.
        offset_id: i64,
        /// The timestamp of the record at the offset, or `None` when that record is not stored.
        timestamp: Option<std::time::SystemTime>,
    }
}
