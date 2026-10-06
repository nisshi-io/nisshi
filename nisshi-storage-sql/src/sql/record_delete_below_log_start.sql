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

-- prepare record_delete_below_log_start (text) as

-- Removes the records that DeleteRecords left below a partition's log start
-- (watermark.low). It keeps the record at watermark.high - 1, which the
-- Latest queries join. Postgres's least() ignores a null, so both
-- watermarks must be present.

delete from record

using cluster c, topic t, topition tp, watermark w

where

c.name = $1
and t.cluster = c.id
and tp.topic = t.id
and w.topition = tp.id
and record.topition = tp.id
and w.low is not null
and w.high is not null
and record.offset_id < least(w.low, w.high - 1);
