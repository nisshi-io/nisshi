---
paths:
  - "nisshi-storage-sql/**"
---

## SQL Queries

The PostgreSQL, libSQL and Turso engines run the same queries, from `nisshi-storage-sql/src/sql/`.
An engine that reads a column by a hand-written index reads the wrong column when the query's
`select` list changes, and nothing fails. An engine therefore reads each column by its name,
through code that the query's module declares once for all three drivers.

The examples below are illustrative, not taken from a real file, so they stay true when the code
changes.

## Rules

- **Give each query that returns rows a module beside it.** `sql/topic_select.sql` gets
  `sql/topic_select.rs`, declared in `sql.rs`. The module holds a `QUERY` constant with the
  query's name in the SQL cache, and declares one row of the result with `sql_row!`, one line for
  each column. A change to the `select` list and a change to the row then show in the same folder
  and the same diff.

  ```rust
  /// The query's name in the SQL cache.
  pub(crate) const QUERY: &str = "topic_select.sql";

  sql_row! {
      /// One topic of the cluster.
      #[derive(Clone, Debug, Eq, PartialEq)]
      pub(crate) struct Row {
          /// The topic's name.
          name: String,
          /// The topic's partition count.
          partitions: i32,
      }
  }
  ```

  The macro declares `Row` and a `RowReader`. A query whose engines read no column, because a row
  only means that something exists, needs only `QUERY`.

- **Name a field as its column, and rename a column in the SQL.** The macro reads each field from
  the column with the field's name. When the Rust name should differ from a table's column, the
  query renames the column with `as`, as in `p.id as producer_id`. The column's name then appears
  once, in the query, and the reader checks it against the field.
- **Share one row type between the queries that return the same thing.** When two or more queries
  return the same columns with the same meaning, such as a topic found by its name and a topic
  found by its ID, declare the row once, in an inline module of `sql/row.rs` named for what one row
  holds, and re-export it from each query's module:

  ```rust
  pub(crate) use super::row::topic::{Row, RowReader};
  ```

  Each `sql_row!` declares types named `Row` and `RowReader`, so each shared row needs a module of
  its own. Re-export only `RowReader` when no engine names `Row`, because the compiler rejects an
  unused re-export. Each engine still names the query's own module, `topic_select::RowReader`, so
  the query's module stays the one place that says what the query returns. A row that only one
  query returns stays in that query's module. Variants of one query, such as a PostgreSQL version
  and a SQLite version, are constants in one module, not separate modules.
- **Run the query by `QUERY` through the engine's helper, and read each row with the module's
  `RowReader`.** Never write the query's file name or a column's index at a call site. Each helper
  returns the result's column names with its rows. Build the reader once, before the first row,
  and read every row with it:

  ```rust
  let (columns, mut rows) = connection.query_rows(topic_select::QUERY, params).await?;
  let reader = topic_select::RowReader::new(&columns)?;

  while let Some(row) = rows.next().await? {
      let topic_select::Row { name, partitions } = reader.read_row(&row)?;
  }
  ```

  For a result of at most one row, use the engine's one-row helper, which records the same SQL
  metrics as a many-row query. On PostgreSQL, a query that reads at most one row uses
  `prepare_query_optional_row`, for none or one row, or `prepare_query_one_row`, for exactly one
  row. Both fail on a second row. `prepare_query_rows` is for a result of any number of rows.
  libSQL and Turso have `query_rows` and `query_first`. PostgreSQL gets the column names from the
  prepared statement, so a result without rows has them too.
- **Write the row type qualified, as `topic_select::Row`.** The module already names the query, so
  the type needs no other name. Each engine file also imports its driver's own `Row`, and the
  qualified name shows which of the two a line means.
- **Declare each field as the driver reads it, and decode it in the engine.** A field's type
  implements `FromColumn` in `sql/driver.rs`, which reads it through the `DriverRow` of each
  driver. A field of `Option` of a type reads a null as `None`, and a field of the type itself
  returns `Error::UnexpectedNull` for one. A JSON column is `serde_json::Result<serde_json::Value>`.
  On libSQL and Turso the parse error is then a value, and the engine decides what an undecodable
  document does to the rest of the result. PostgreSQL rejects an invalid `json` value when it is
  stored. A new kind of column gets one `FromColumn` implementation, and a new kind of stored value
  gets one `DriverRow` method for each driver. A query module has no driver-specific code.
- **Put what a row means in the query's module.** When a result has a row that is not data, such
  as a marker row for a topic that sets none of the requested keys, the module gives `Row` a method
  that returns `None` for it, and each engine calls that method instead of testing the field.
- **Name every column that an engine reads with `as`, unless it is a plain column.** PostgreSQL
  names `count(*)` `count`, and a parameter `?column?`. SQLite names them by the expression's text.
  `cg.name` is `name` on all three drivers, but `count(*) as partitions` is the only way to give an
  expression one name everywhere. This applies to `returning` lists too.
- **Do not write `select *` in a query that an engine reads.** Its column names come from the
  tables, and a change to a table changes the result without a change to the query.
- **Give each column of a result a unique, lower-case name.** PostgreSQL folds an unquoted name to
  lower case and SQLite does not, and a reader returns `Error::DuplicateColumn` for a name that
  appears twice. Do not quote a column name.

## The Test

Change the order of a query's `select` list. Every engine still reads the same values. Rename a
column in the query. Every engine returns `Error::MissingColumn`, which names the column, even when
the result has no rows. Store a value of the wrong type. Every engine returns
`Error::UnreadableColumn`, which names the query and the column. Find two row types with the same
columns and the same meaning. They should be one type in `sql/row.rs`.
