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

-- Computes, for every control-batch marker (commit or abort) inside a
-- requested Fetch range, the first offset of the transaction that marker
-- closes -- used to populate the Fetch response's aborted_transactions
-- field under read_committed isolation. Real Kafka semantics: the broker
-- still returns the raw records (see record_fetch_pg.sql) -- it additionally
-- reports which ranges the client should discard itself, using the control
-- marker that's already permanently in the log (see nisshi-storage/src/
-- service/fetch.rs and pg.rs's end_in_tx, which writes these markers).
--
-- IMPORTANT: "the previous marker" is computed per producer_id, NOT per
-- (producer_id, producer_epoch). producer_epoch is a liveness/fencing
-- generation counter (bumped when a producer reconnects with the same
-- transactional.id) -- it is NOT a separate logical transaction stream.
-- Partitioning by epoch as well was an earlier bug in this query: a
-- producer that commits under epoch 0, then reconnects and aborts under
-- epoch 1, would have no "previous marker" visible from within epoch 1
-- alone, so lag() came back NULL and the abort's reported range collapsed
-- to the fetch's own start offset -- wrongly reporting epoch 0's already-
-- COMMITTED data as part of the aborted range. Partitioning by producer_id
-- alone (ordering by offset across every epoch that producer has ever
-- used) fixes this: the abort under epoch 1 correctly looks back to its
-- own true previous marker, whichever epoch wrote it.
--
-- Three ways to compute "the previous marker for this producer" (i.e.
-- where the aborted transaction's data actually starts) were benchmarked
-- (300k rows / 499 markers seeded into a scratch partition, Postgres 17,
-- EXPLAIN ANALYZE, unindexed vs. with the partial index in
-- etc/initdb.d/010-schema.sql):
--
--   1) Two queries: this one to list markers, then a second query run once
--      PER marker to find that producer's previous marker.
--      ~4,724ms unindexed / ~5.3ms indexed, across 499 round trips either way.
--
--   2) One query: same marker list, then a correlated subquery (with a
--      coalesce fallback to the producer's all-time earliest record) run
--      once per marker row, inline.
--      ~5,337ms unindexed / ~974ms indexed -- slower than (1) in BOTH cases.
--      A correlated subquery re-executes once per outer row, same as a
--      round trip does -- packaging it as one statement doesn't remove the
--      repeated-lookup cost, it just moves where it happens. Its fallback
--      branch also scans ALL of a producer's records (not just markers),
--      which the partial index below can't cover, so it stays slow even
--      once indexed.
--
--   3) One query, one pass (this file): lag() over each producer/epoch's
--      own markers, ordered by offset -- computed once for the whole set
--      instead of once per row, no correlated subquery at all.
--      ~20.5ms unindexed / ~0.71ms indexed.
--
-- (3) won on every axis measured (round trips, raw time, plan shape) and
-- isn't affected by the index blind spot that hurt (2) -- that's what's
-- implemented below. Full details/numbers in the plan doc for this feature.

-- prepare record_control_marker_select (text, text, integer, bigint, bigint) as

with markers as (
    select

    r.offset_id,
    r.producer_id,
    r.producer_epoch,
    r.k, -- decode with ControlBatch::try_from in Rust to tell commit vs abort

    -- The offset of the row immediately before this one within its own
    -- producer_id group (across every epoch that producer has used -- see
    -- the header comment on why epoch is deliberately NOT part of this
    -- partition) -- i.e. this producer's previous marker on this
    -- partition. Computed once, in a single sorted pass over every marker
    -- this producer has ever written here, rather than one lookup per row
    -- (see the benchmark notes above).
    lag(r.offset_id) over (
        partition by r.producer_id
        order by r.offset_id
    ) as previous_marker_offset

    from

    cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join record r on r.topition = tp.id

    where

    c.name = $1
    and t.name = $2
    and tp.partition = $3
    and (r.attributes & 32) = 32 -- BatchAttribute::CONTROL_BITMASK -- only control-batch markers, never data rows
    -- Deliberately NOT bounded by the fetch's own offset range here: lag()
    -- needs to see markers before the fetch's start offset too, to find the
    -- true previous one for producers whose earlier transaction predates
    -- this particular fetch.
)

select

producer_id,
k,
-- previous_marker_offset is NULL when this is a producer's first-ever
-- marker on this partition (lag() has nothing behind it to look at).
-- Either way, clamp to at least the fetch's own start offset ($4) --
-- nothing earlier than that matters for this particular response.
greatest(coalesce(previous_marker_offset + 1, $4), $4) as first_offset

from

markers

where

-- NOW filter down to markers actually inside this fetch's requested range.
offset_id >= $4
and offset_id < $5;
