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

-- prepare record_fetch (text, text, integer, integer, integer, integer, bigint) as
-- A fetch reads at most $7 rows past the fetch offset (ROWS_PER_FETCH in pg.rs, 10000). The running byte total below is
-- computed over every row that this query reads, so without the limit, one fetch costs as much
-- as the whole backlog between the fetch offset and the high watermark, however few rows the
-- client can take. The fetch service reads again from the last offset that it received, so a
-- limit that ends a read early costs one more query.
--
-- The record table is read directly, with the topition id looked up in a scalar subquery. A join
-- between the two hides from the planner that the rows come out in offset order, and it then sorts
-- all of them, which for 10000 rows of 1 KiB spills to a temp file. Without the join, the limit
-- stops an ordered walk of the primary key, and the running byte total reads that order as it is.
--
-- The fetch runs with the settings in FETCH_PLAN_SETTINGS (pg.rs), which turn off bitmap and
-- sequential scans, so that this query keeps the ordered scan that the limit relies on. Run it
-- with them to see the plan that a fetch uses: `run.sh explain` does.
with candidates as (
select

r.offset_id,
r.attributes,
r.timestamp,
r.k,
r.v,
r.producer_id,
r.producer_epoch

from

record r

where

r.topition = (
    select tp.id
    from cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    where c.name = $1
    and t.name = $2
    and tp.partition = $3)
and r.offset_id >= $4
and r.offset_id < $6

order by r.offset_id
limit $7),

sized as (
select

offset_id,
attributes,
timestamp,
k,
v,
sum(coalesce(length(k), 0) + coalesce(length(v), 0)) over (order by offset_id) as bytes,
producer_id,
producer_epoch

from

candidates)

select * from sized where bytes < $5 order by offset_id;
