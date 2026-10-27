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

//! The `consumer_group_select.sql` query and [`Row`], one row of its result. A change to the
//! query's `select` list must keep the column names that [`Row`] declares.
//!
//! The query takes one parameter, `$1`: the cluster's name.
//!
//! An engine that needs its own version of the query gets another key here, such as
//! `lite/consumer_group_select.sql`, and its version returns the same columns, so every engine
//! reads them through [`RowReader`]. A query that returns other columns gets a module of its own.

/// The query's name in the SQL cache.
pub(crate) const QUERY: &str = "consumer_group_select.sql";

sql_row! {
    /// One consumer group of the cluster.
    #[derive(Debug)]
    pub(crate) struct Row {
        /// The group's name.
        name: String = "name",
        /// The group's stored detail, or the error from parsing it as JSON. It is `None` for a
        /// group that has only committed offsets.
        detail: Option<serde_json::Result<serde_json::Value>> = "detail",
    }
}
