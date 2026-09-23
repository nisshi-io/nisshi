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

with

dup as (
    select
    tp.id as topition, r.k as k, max(r.offset_id) as offset_id
    from record r
    join topition tp on tp.id = r.topition
    join topic t on tp.topic = t.id
    join cluster c on t.cluster = c.id
    join topic_configuration tc on tc.topic = t.id
    where c.name = $1
    and tc.name = 'cleanup.policy'
    and tc.value like '%compact%'
    and r.k is not null
    group by tp.id,r.k having count(r.k) > 1
),

compaction as (
    select
    r.topition as topition, r.offset_id as offset_id
    from record r
    join dup on r.topition = dup.topition and r.k = dup.k
    where dup.offset_id > r.offset_id
)

delete from record
where (record.topition, record.offset_id) in (select * from compaction);
