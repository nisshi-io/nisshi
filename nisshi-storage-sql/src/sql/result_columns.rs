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

//! The index of each column in a query's result, found by the column's name.

use nisshi_storage::{Error, Result};

/// The column names of one query's result, in order, as the engine's driver reports them.
#[derive(Clone, Debug)]
pub(crate) struct ResultColumns {
    query: &'static str,
    names: Vec<String>,
}

impl ResultColumns {
    fn new(query: &'static str, names: Vec<String>) -> Self {
        Self { query, names }
    }

    /// Returns the column names of `rows`, libSQL's result of `query`.
    #[cfg(feature = "libsql")]
    pub(crate) fn libsql(query: &'static str, rows: &libsql::Rows) -> Self {
        Self::new(
            query,
            (0..rows.column_count())
                .map(|index| rows.column_name(index).unwrap_or_default().to_owned())
                .collect(),
        )
    }

    /// Returns the column names of `rows`, Turso's result of `query`.
    #[cfg(feature = "turso")]
    pub(crate) fn turso(query: &'static str, rows: &turso::Rows) -> Self {
        Self::new(query, rows.column_names())
    }

    /// Returns the index of the column `name`, or an error if the result has no such column.
    pub(crate) fn index(&self, name: &str) -> Result<usize> {
        self.names
            .iter()
            .position(|found| found == name)
            .ok_or_else(|| {
                Error::Message(format!(
                    "{}: no column {name:?} in {:?}",
                    self.query, self.names
                ))
            })
    }
}
