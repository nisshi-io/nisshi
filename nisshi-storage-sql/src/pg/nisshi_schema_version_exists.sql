-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Returns whether a namespace on the search path holds the nisshi_schema_version table.
--
-- This query reads pg_class with the statement's snapshot, instead of calling to_regclass.
-- to_regclass looks the name up in the session's catalog cache without a lock, and the session
-- applies the changes of other sessions to that cache only at certain points, such as the start of
-- a transaction. A broker that waits for the setup lock runs this query in a transaction that began
-- before the lock holder committed. to_regclass can then miss the table that the holder created,
-- and the broker creates the schema a second time. A new statement snapshot sees every transaction
-- that committed before the statement started.

select exists (
    select
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    where c.relname = 'nisshi_schema_version'
    and n.nspname = any (current_schemas(false))
)
