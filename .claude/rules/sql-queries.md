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
  each column: the field, its type, and the column's name. A change to the `select` list and a
  change to the row then show in the same folder and the same diff.

  ```rust
  /// The query's name in the SQL cache.
  pub(crate) const QUERY: &str = "topic_select.sql";

  sql_row! {
      /// One topic of the cluster.
      #[derive(Clone, Debug, Eq, PartialEq)]
      pub(crate) struct Row {
          /// The topic's name.
          name: String = "name",
          /// The topic's partition count.
          partitions: i32 = "partitions",
      }
  }
  ```

  The macro declares `Row` and a `RowReader`. A query whose engines read no column, because a row
  only means that something exists, needs only `QUERY`.

- **Run the query by `QUERY`, and read each row with the module's `RowReader`.** Never write the
  query's file name or a column's index at a call site. Build the reader once, before the first
  row, from the result's column names, and read every row with it:

  ```rust
  let reader = topic_select::RowReader::new(&ResultColumns::libsql(topic_select::QUERY, &rows))?;

  while let Some(row) = rows.next().await? {
      let topic_select::Row { name, partitions } = reader.read_row(&row)?;
  }
  ```

  PostgreSQL gets the names from the prepared statement, through `prepare_query_rows`, so a result
  without rows has them too. libSQL and Turso get them from the result, with
  `ResultColumns::libsql` and `ResultColumns::turso`.
- **Write the row type qualified, as `topic_select::Row`.** The module already names the query, so
  the type needs no other name. Each engine file also imports its driver's own `Row`, and the
  qualified name shows which of the two a line means.
- **Declare each field as the driver reads it, and decode it in the engine.** A field's type
  implements `Column` in `sql/driver.rs`, which reads it through the `DriverRow` of each driver. A
  JSON column is `serde_json::Result<serde_json::Value>`. On libSQL and Turso the parse error is
  then a value, and the engine decides what an undecodable document does to the rest of the
  result. PostgreSQL rejects an invalid `json` value when it is stored. A new kind of
  column gets one `Column` implementation, and a new kind of stored value gets one `DriverRow`
  method for each driver. A query module has no driver-specific code.
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
  lower case and SQLite does not, and a name that appears twice finds only its first column. Do not
  quote a column name.

## The Test

Change the order of a query's `select` list. Every engine still reads the same values. Rename a
column in the query. Every engine returns `Error::MissingColumn`, which names the column, even when
the result has no rows. Store a value of the wrong type. Every engine returns `Error::Column`,
which names the query and the column.
