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

//! The `topic_select.sql` query, which finds a topic by its name. An engine reads no column of its
//! result: a row means that the topic exists.
//!
//! The query takes two parameters: `$1` is the cluster's name, and `$2` the topic's name.

/// The query's name in the SQL cache.
pub(crate) const QUERY: &str = "topic_select.sql";
