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

-- Kafka's retention.bytes is a per-partition limit: delete a partition's oldest records until the
-- bytes of its keys and values are within it, and always keep the newest record.
with

capped as (
    select tp.id as topition, cast(tc.value as bigint) as cap
    from topition tp
    join topic t on t.id = tp.topic
    join cluster c on t.cluster = c.id
    join topic_configuration tc on tc.topic = t.id
    left join topic_configuration pol on pol.topic = t.id and pol.name = 'cleanup.policy'
    where c.name = $1
    and tc.name = 'retention.bytes'
    -- A negative retention.bytes is no limit. The pattern allows at most 18 digits, so the cast to
    -- bigint never overflows and fails this statement for every topic. The broker rejects a value
    -- that isn't a long, but a topic created before the broker checked the value can hold one, and
    -- that topic keeps every record.
    and tc.value ~ '^[0-9]{1,18}$'
    -- A topic without a cleanup.policy has Kafka's default policy, delete.
    and coalesce(pol.value, 'delete') like '%delete%'
),

sized as (
    select r.topition, r.offset_id, capped.cap,
    sum(coalesce(octet_length(r.k), 0) + coalesce(octet_length(r.v), 0))
        over (partition by r.topition order by r.offset_id desc) as retained,
    row_number() over (partition by r.topition order by r.offset_id desc) as newest
    from record r
    join capped on capped.topition = r.topition
),

excess as (
    select topition, offset_id
    from sized
    where retained > cap
    and newest > 1
)

delete from record
where (record.topition, record.offset_id) in (select topition, offset_id from excess);
