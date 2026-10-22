---
paths:
  - "nisshi-storage-sql/**"
---

## SQL Queries

The PostgreSQL, libSQL and Turso engines run the same queries, from `nisshi-storage-sql/src/sql/`.
An engine that reads a column by a hand-written index reads the wrong column when the query's
`select` list changes, and nothing fails. An engine therefore reads each column by its name.

The examples below are illustrative, not taken from a real file, so they stay true when the code
changes.

## Rules

- **Give each query that returns rows a module beside it.** `sql/topic_select.sql` gets
  `sql/topic_select.rs`, declared in `sql.rs`. The module holds a `QUERY` constant with the
  query's name in the SQL cache, and one constant for each column that an engine reads, holding
  the column's name. A change to the `select` list and a change to its constants then show in the
  same folder and the same diff.

  ```rust
  /// The query's name in the SQL cache.
  pub(crate) const QUERY: &str = "topic_select.sql";

  /// The topic's name.
  pub(crate) const NAME: &str = "name";
  ```

- **Run the query by `QUERY`, and read each column by its constant.** Never write the query's file
  name or a column's index at a call site.
  - PostgreSQL reads by name: `row.try_get::<_, String>(topic_select::NAME)?`.
  - libSQL and Turso read only by index. Build a `ResultColumns` from the result's column names
    once, before the first row, and look up each index with `columns.index(topic_select::NAME)?`.
    Then read every row by the looked-up index, so each row costs no more than before.
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
column in the query. Every engine returns an error that names the missing column.
