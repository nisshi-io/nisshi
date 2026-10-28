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

//! The query that finds a topic or a virtual topic by its ID, and [`Row`], one row of its result.
//! A change to either version's `select` list must keep the column names that [`Row`] declares.
//!
//! The query takes two parameters: `$1` is the cluster's name, and `$2` the ID. It returns one row
//! for a topic or a virtual topic that exists, and none for an ID that the cluster does not hold.
//! A virtual topic's name is its topic's name, a `/`, and its key.

/// The query's name in the SQL cache.
pub(crate) const QUERY: &str = "topic_select_uuid.sql";

/// The PostgreSQL version's name in the SQL cache. This version decodes the virtual topic's key
/// from UTF-8, because PostgreSQL stores the key as `bytea`.
pub(crate) const PG_QUERY: &str = "pg/topic_select_uuid.sql";

sql_row! {
    /// One topic or virtual topic of the cluster.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The topic's or the virtual topic's ID.
        uuid: uuid::Uuid,
        /// The topic's or the virtual topic's name.
        name: String,
        /// Whether the topic is internal.
        is_internal: bool,
        /// The topic's partition count.
        partitions: i32,
        /// The topic's replication factor.
        replication_factor: i32,
    }
}
