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

//! The `topic_configuration_upsert.sql` query, which sets one config of a topic, and [`Row`], the
//! row that it returns. A change to the query's `returning` list must keep the column names that
//! [`Row`] declares.
//!
//! The query takes four parameters: `$1` is the cluster's name, `$2` the topic's name, `$3` the
//! config's key, and `$4` its value.

/// The query's name in the SQL cache.
pub(crate) const QUERY: &str = "topic_configuration_upsert.sql";

sql_row! {
    /// The config that the query inserted or updated.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The config's row ID.
        id: i64,
    }
}
