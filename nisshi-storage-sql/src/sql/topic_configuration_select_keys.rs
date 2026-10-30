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

//! The `pg/topic_configuration_select_keys.sql` query and the names of the columns that PostgreSQL
//! reads from its result. A change to the query's `select` list must keep these names.
//!
//! The query takes three parameters: `$1` is the cluster's name, `$2` the topic's name, and `$3`
//! the array of config keys to read.

/// The query's name in the SQL cache.
pub(crate) const QUERY: &str = "pg/topic_configuration_select_keys.sql";

/// The config's key. It is null in the one row for a topic that sets none of the keys.
#[cfg(feature = "postgres")]
pub(crate) const NAME: &str = "name";

/// The config's stored value.
#[cfg(feature = "postgres")]
pub(crate) const VALUE: &str = "value";
