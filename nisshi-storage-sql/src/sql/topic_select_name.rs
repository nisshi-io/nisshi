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

//! The `topic_select_name.sql` query and [`Row`], one row of its result. A change to the query's
//! `select` list must keep the column names that [`Row`] declares.
//!
//! The query takes two parameters: `$1` is the cluster's name, and `$2` the topic's name. It
//! returns one row for a topic that exists, and none for a topic that does not.

/// The query's name in the SQL cache.
pub(crate) const QUERY: &str = "topic_select_name.sql";

sql_row! {
    /// One topic of the cluster.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The topic's ID.
        uuid: uuid::Uuid,
        /// The topic's name.
        name: String,
        /// Whether the topic is internal.
        is_internal: bool,
        /// The topic's partition count.
        partitions: i32,
        /// The topic's replication factor.
        replication_factor: i32,
    }
}
