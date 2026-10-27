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

//! Reads a column's value from a row of each SQL driver: the only driver-specific code that a
//! query module's `RowReader` uses.

use nisshi_storage::Result;
use serde_json::Value;

/// A row of one SQL driver's result, read by column index.
///
/// Each method except [`is_null`](Self::is_null) reads one kind of value that the queries store,
/// and returns an error for a value of another type or a null. [`Column`] builds each Rust type
/// that a query module declares on these methods.
pub(crate) trait DriverRow {
    /// Returns whether the value at `index` is null.
    fn is_null(&self, index: usize) -> Result<bool>;

    /// Returns the integer at `index`, widened to `i64`.
    fn integer(&self, index: usize) -> Result<i64>;

    /// Returns the text at `index`.
    fn text(&self, index: usize) -> Result<String>;

    /// Returns the JSON document at `index`, or the error from parsing it.
    ///
    /// On libSQL and Turso, which store the document as text, the parse error is a value, not a
    /// read error, so that the engine decides what an undecodable document does to the rest of
    /// the result. PostgreSQL rejects an invalid `json` value when it is stored, and
    /// tokio-postgres decodes the document itself, so a decode failure there is a read error.
    fn json(&self, index: usize) -> Result<serde_json::Result<Value>>;
}

/// A Rust type that a query module declares for a column, read through any [`DriverRow`].
pub(crate) trait Column: Sized {
    /// Reads the value at `index` of `row`.
    fn read<R: DriverRow>(row: &R, index: usize) -> Result<Self>;
}

impl Column for i64 {
    fn read<R: DriverRow>(row: &R, index: usize) -> Result<Self> {
        row.integer(index)
    }
}

impl Column for i32 {
    fn read<R: DriverRow>(row: &R, index: usize) -> Result<Self> {
        Self::try_from(row.integer(index)?).map_err(Into::into)
    }
}

impl Column for i16 {
    fn read<R: DriverRow>(row: &R, index: usize) -> Result<Self> {
        Self::try_from(row.integer(index)?).map_err(Into::into)
    }
}

impl Column for String {
    fn read<R: DriverRow>(row: &R, index: usize) -> Result<Self> {
        row.text(index)
    }
}

impl Column for serde_json::Result<Value> {
    fn read<R: DriverRow>(row: &R, index: usize) -> Result<Self> {
        row.json(index)
    }
}

impl<T: Column> Column for Option<T> {
    fn read<R: DriverRow>(row: &R, index: usize) -> Result<Self> {
        if row.is_null(index)? {
            Ok(None)
        } else {
            T::read(row, index).map(Some)
        }
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use super::DriverRow;
    use nisshi_storage::Result;
    use serde_json::Value;
    use std::error::Error as StdError;
    use tokio_postgres::{
        Row,
        types::{FromSql, Type},
    };

    /// A value of any type, read only to find out whether it is null.
    struct AnyValue;

    impl<'a> FromSql<'a> for AnyValue {
        fn from_sql(_: &Type, _: &'a [u8]) -> Result<Self, Box<dyn StdError + Sync + Send>> {
            Ok(Self)
        }

        fn accepts(_: &Type) -> bool {
            true
        }
    }

    /// A `smallint`, `integer` or `bigint`, widened to `i64`.
    ///
    /// tokio-postgres reads each width only as its own Rust type, and a query's integer columns
    /// have different widths, such as a `smallint` epoch and a `bigint` offset.
    struct Integer(i64);

    impl<'a> FromSql<'a> for Integer {
        fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn StdError + Sync + Send>> {
            match *ty {
                Type::INT2 => i16::from_sql(ty, raw).map(i64::from),
                Type::INT4 => i32::from_sql(ty, raw).map(i64::from),
                _ => i64::from_sql(ty, raw),
            }
            .map(Self)
        }

        fn accepts(ty: &Type) -> bool {
            matches!(*ty, Type::INT2 | Type::INT4 | Type::INT8)
        }
    }

    impl DriverRow for Row {
        fn is_null(&self, index: usize) -> Result<bool> {
            self.try_get::<_, Option<AnyValue>>(index)
                .map(|value| value.is_none())
                .map_err(Into::into)
        }

        fn integer(&self, index: usize) -> Result<i64> {
            self.try_get::<_, Integer>(index)
                .map(|Integer(integer)| integer)
                .map_err(Into::into)
        }

        fn text(&self, index: usize) -> Result<String> {
            self.try_get(index).map_err(Into::into)
        }

        fn json(&self, index: usize) -> Result<serde_json::Result<Value>> {
            self.try_get(index).map(Ok).map_err(Into::into)
        }
    }
}

#[cfg(feature = "libsql")]
mod libsql {
    use super::DriverRow;
    use libsql::{Row, Value};
    use nisshi_storage::Result;

    /// Returns `index` as the `i32` that libSQL takes.
    fn libsql_index(index: usize) -> Result<i32> {
        i32::try_from(index).map_err(Into::into)
    }

    // This reads each value with `get_value` and matches its type, instead of the typed
    // `Row::get`, because libSQL 0.10's typed `get` panics on a value of another type.
    impl DriverRow for Row {
        fn is_null(&self, index: usize) -> Result<bool> {
            self.get_value(libsql_index(index)?)
                .map(|value| matches!(value, Value::Null))
                .map_err(Into::into)
        }

        fn integer(&self, index: usize) -> Result<i64> {
            match self.get_value(libsql_index(index)?)? {
                Value::Integer(integer) => Ok(integer),
                Value::Null => Err(libsql::Error::NullValue.into()),
                _ => Err(libsql::Error::InvalidColumnType.into()),
            }
        }

        fn text(&self, index: usize) -> Result<String> {
            match self.get_value(libsql_index(index)?)? {
                Value::Text(text) => Ok(text),
                Value::Null => Err(libsql::Error::NullValue.into()),
                _ => Err(libsql::Error::InvalidColumnType.into()),
            }
        }

        fn json(&self, index: usize) -> Result<serde_json::Result<serde_json::Value>> {
            self.text(index).map(|text| serde_json::from_str(&text))
        }
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::DriverRow;
    use nisshi_storage::Result;
    use turso::Row;

    impl DriverRow for Row {
        fn is_null(&self, index: usize) -> Result<bool> {
            self.get_value(index)
                .map(|value| value.is_null())
                .map_err(Into::into)
        }

        fn integer(&self, index: usize) -> Result<i64> {
            self.get(index).map_err(Into::into)
        }

        fn text(&self, index: usize) -> Result<String> {
            self.get(index).map_err(Into::into)
        }

        fn json(&self, index: usize) -> Result<serde_json::Result<serde_json::Value>> {
            self.text(index).map(|text| serde_json::from_str(&text))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::result_columns::ResultColumns;
    use nisshi_storage::{Error, Result};
    use serde_json::json;
    use std::assert_matches;

    /// A query that reads one column of each kind that [`DriverRow`](super::DriverRow) reads.
    mod probe {
        /// The query's name in the errors that its reader returns.
        pub(super) const QUERY: &str = "probe.sql";

        sql_row! {
            #[derive(Debug)]
            pub(crate) struct Row {
                epoch: i16 = "epoch",
                high: Option<i64> = "high",
                name: String = "name",
                detail: Option<serde_json::Result<serde_json::Value>> = "detail",
            }
        }
    }

    /// The probe queries in one driver's SQL dialect.
    struct ProbeSql {
        /// Returns one row, with the columns in the order of [`probe::Row`].
        in_order: &'static str,
        /// Returns the row of `in_order`, with its columns in the reverse order.
        reordered: &'static str,
        /// Returns no rows, and names the `name` column `nom`.
        renamed_without_rows: &'static str,
        /// Returns text in the `epoch` column.
        epoch_is_text: &'static str,
        /// Returns null in the `name` column.
        name_is_null: &'static str,
        /// Returns an `epoch` larger than `i16::MAX`.
        epoch_overflows: &'static str,
    }

    const SQLITE: ProbeSql = ProbeSql {
        in_order: r#"select 3 as epoch, 7 as high, 'a' as name, '{"k":1}' as detail"#,
        reordered: r#"select '{"k":1}' as detail, 'a' as name, 7 as high, 3 as epoch"#,
        renamed_without_rows: "select 3 as epoch, 7 as high, 'a' as nom, null as detail \
                               where 1 = 0",
        epoch_is_text: "select 'x' as epoch, 7 as high, 'a' as name, null as detail",
        name_is_null: "select 3 as epoch, 7 as high, null as name, null as detail",
        epoch_overflows: "select 40000 as epoch, 7 as high, 'a' as name, null as detail",
    };

    const POSTGRES: ProbeSql = ProbeSql {
        in_order: r#"select 3::int2 as epoch, 7::int4 as high, 'a'::text as name,
                     '{"k":1}'::json as detail"#,
        reordered: r#"select '{"k":1}'::json as detail, 'a'::text as name, 7::int4 as high,
                      3::int2 as epoch"#,
        renamed_without_rows: "select 3::int2 as epoch, 7::int8 as high, 'a'::text as nom, \
                               null::json as detail where 1 = 0",
        epoch_is_text: "select 'x'::text as epoch, 7::int8 as high, 'a'::text as name, \
                        null::json as detail",
        name_is_null: "select 3::int2 as epoch, 7::int8 as high, null::text as name, \
                       null::json as detail",
        epoch_overflows: "select 40000::int4 as epoch, 7::int8 as high, 'a'::text as name, \
                          null::json as detail",
    };

    /// Runs probe queries through one driver.
    trait ProbeDriver {
        /// Runs `sql`, and reads each row of its result with [`probe::RowReader`].
        async fn probe_rows(sql: &str) -> Result<Vec<probe::Row>>;
    }

    #[cfg(feature = "postgres")]
    struct Postgres;

    #[cfg(feature = "postgres")]
    impl ProbeDriver for Postgres {
        async fn probe_rows(sql: &str) -> Result<Vec<probe::Row>> {
            let (client, connection) = tokio_postgres::connect(
                "host=localhost user=postgres password=postgres",
                tokio_postgres::NoTls,
            )
            .await?;
            let connection = tokio::spawn(connection);

            let statement = client.prepare(sql).await?;
            let reader = probe::RowReader::new(&ResultColumns::postgres(probe::QUERY, &statement))?;
            let rows = client
                .query(&statement, &[])
                .await?
                .iter()
                .map(|row| reader.read_row(row))
                .collect();

            drop(client);
            _ = connection.await;
            rows
        }
    }

    #[cfg(feature = "libsql")]
    struct Libsql;

    #[cfg(feature = "libsql")]
    impl ProbeDriver for Libsql {
        async fn probe_rows(sql: &str) -> Result<Vec<probe::Row>> {
            let connection = libsql::Builder::new_local(":memory:")
                .build()
                .await?
                .connect()?;
            let mut rows = connection.query(sql, ()).await?;
            let reader = probe::RowReader::new(&ResultColumns::libsql(probe::QUERY, &rows))?;

            let mut probes = vec![];
            while let Some(row) = rows.next().await? {
                probes.push(reader.read_row(&row)?);
            }
            Ok(probes)
        }
    }

    #[cfg(feature = "turso")]
    struct Turso;

    #[cfg(feature = "turso")]
    impl ProbeDriver for Turso {
        async fn probe_rows(sql: &str) -> Result<Vec<probe::Row>> {
            let connection = turso::Builder::new_local(":memory:")
                .build()
                .await?
                .connect()?;
            let mut rows = connection.query(sql, ()).await?;
            let reader = probe::RowReader::new(&ResultColumns::turso(probe::QUERY, &rows))?;

            let mut probes = vec![];
            while let Some(row) = rows.next().await? {
                probes.push(reader.read_row(&row)?);
            }
            Ok(probes)
        }
    }

    /// Returns the one row of `sql`'s result.
    async fn probe_row<D: ProbeDriver>(sql: &str) -> Result<probe::Row> {
        let mut rows = D::probe_rows(sql).await?;
        assert_eq!(1, rows.len(), "{sql}");
        Ok(rows.remove(0))
    }

    /// The reader finds each column by its name, so a query that returns its columns in another
    /// order gives the engine the same values.
    async fn reordered_select_reads_the_same_values<D: ProbeDriver>(sql: &ProbeSql) -> Result<()> {
        for query in [sql.in_order, sql.reordered] {
            let row = probe_row::<D>(query).await?;

            assert_eq!(3, row.epoch, "{query}");
            assert_eq!(Some(7), row.high, "{query}");
            assert_eq!("a", row.name, "{query}");
            assert_matches!(row.detail, Some(Ok(detail)) if detail == json!({"k": 1}));
        }

        Ok(())
    }

    /// A query whose result lacks a column fails before the first row, so a renamed column is
    /// found even when the result is empty.
    async fn renamed_column_is_missing_without_rows<D: ProbeDriver>(sql: &ProbeSql) -> Result<()> {
        assert_matches!(
            D::probe_rows(sql.renamed_without_rows).await,
            Err(Error::MissingColumn { query: probe::QUERY, column: "name", found })
                if found.contains(&"nom".to_owned())
        );
        Ok(())
    }

    /// A value that the driver can't read as its field's type is an error that names the query
    /// and the column, instead of a value read as missing.
    async fn unreadable_value_names_its_column<D: ProbeDriver>(sql: &ProbeSql) -> Result<()> {
        for (query, column) in [
            (sql.epoch_is_text, "epoch"),
            (sql.name_is_null, "name"),
            (sql.epoch_overflows, "epoch"),
        ] {
            assert_matches!(
                D::probe_rows(query).await,
                Err(Error::Column { query: probe::QUERY, column: found, .. }) if found == column,
                "{query}"
            );
        }
        Ok(())
    }

    /// A document that isn't JSON is kept in the row as a parse error, so that the engine decides
    /// what it does to the rest of the result.
    async fn invalid_json_is_kept_in_the_row<D: ProbeDriver>() -> Result<()> {
        let row =
            probe_row::<D>("select 3 as epoch, 7 as high, 'a' as name, '{' as detail").await?;
        assert_matches!(row.detail, Some(Err(_)));
        Ok(())
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_reordered_select_reads_the_same_values() -> Result<()> {
        reordered_select_reads_the_same_values::<Postgres>(&POSTGRES).await
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_renamed_column_is_missing_without_rows() -> Result<()> {
        renamed_column_is_missing_without_rows::<Postgres>(&POSTGRES).await
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_unreadable_value_names_its_column() -> Result<()> {
        unreadable_value_names_its_column::<Postgres>(&POSTGRES).await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_reordered_select_reads_the_same_values() -> Result<()> {
        reordered_select_reads_the_same_values::<Libsql>(&SQLITE).await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_renamed_column_is_missing_without_rows() -> Result<()> {
        renamed_column_is_missing_without_rows::<Libsql>(&SQLITE).await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_unreadable_value_names_its_column() -> Result<()> {
        unreadable_value_names_its_column::<Libsql>(&SQLITE).await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_invalid_json_is_kept_in_the_row() -> Result<()> {
        invalid_json_is_kept_in_the_row::<Libsql>().await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_reordered_select_reads_the_same_values() -> Result<()> {
        reordered_select_reads_the_same_values::<Turso>(&SQLITE).await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_renamed_column_is_missing_without_rows() -> Result<()> {
        renamed_column_is_missing_without_rows::<Turso>(&SQLITE).await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_unreadable_value_names_its_column() -> Result<()> {
        unreadable_value_names_its_column::<Turso>(&SQLITE).await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_invalid_json_is_kept_in_the_row() -> Result<()> {
        invalid_json_is_kept_in_the_row::<Turso>().await
    }
}
