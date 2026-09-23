-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

with

deletion as (
    select tp.id as topition
    from topition tp
    join topic t on t.id = tp.topic
    join cluster c on t.cluster = c.id
    join topic_configuration tc on tc.topic = t.id
    where c.name = $1
    and tc.name = 'cleanup.policy'
    and tc.value like '%delete%'
),

retention as (
    select tp.id as topition, tc.value
    from topition tp
    join topic t on t.id = tp.topic
    join cluster c on t.cluster = c.id
    left join topic_configuration tc on tc.topic = t.id
    where c.name = $1
    and tc.name = 'retention.ms'
),

ancient as (
    select tp.id as topition, r.offset_id as offset_id
    from record r
    join topition tp on tp.id = r.topition
    join topic t on t.id = tp.topic
    join cluster c on t.cluster = c.id
    join deletion del on del.topition = tp.id
    left join retention ret on ret.topition = tp.id
    where c.name = $1
    and $2 - r.timestamp > coalesce(cast(ret.value as integer), $3)
)

delete from record
where (record.topition, record.offset_id) in (select * from ancient);
