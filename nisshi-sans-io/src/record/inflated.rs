// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{
    BatchAttribute, ByteSize, Compression, Error, Result, TimestampType,
    record::{Record, codec::Sequence, deflated},
    ser::RecordBatchEncoder,
    to_timestamp,
};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::debug;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Frame {
    pub batches: Vec<Batch>,
}

impl TryFrom<deflated::Frame> for Frame {
    type Error = Error;

    fn try_from(deflated: deflated::Frame) -> Result<Self, Self::Error> {
        deflated
            .batches
            .into_iter()
            .map(Batch::try_from)
            .collect::<Result<Vec<_>>>()
            .map(|batches| Self { batches })
    }
}

impl TryFrom<&deflated::Frame> for Frame {
    type Error = Error;

    fn try_from(deflated: &deflated::Frame) -> Result<Self, Self::Error> {
        deflated
            .batches
            .iter()
            .try_fold(Vec::new(), |mut acc, batch| {
                Batch::try_from(batch).map(|inflated| {
                    acc.push(inflated);
                    acc
                })
            })
            .map(|batches| Self { batches })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "deflated::Batch")]
pub struct Batch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,

    #[serde(serialize_with = "Sequence::<Record>::serialize")]
    pub records: Vec<Record>,
}

const MAGIC: i8 = 2;

impl Default for Batch {
    fn default() -> Self {
        let base_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default();

        Self {
            base_offset: 0,
            batch_length: Default::default(),
            partition_leader_epoch: -1,
            magic: MAGIC,
            crc: Default::default(),
            attributes: BatchAttribute::default()
                .compression(Compression::None)
                .timestamp(TimestampType::CreateTime)
                .into(),
            last_offset_delta: Default::default(),
            base_timestamp,
            max_timestamp: base_timestamp,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: Default::default(),
        }
    }
}

impl TryFrom<deflated::Batch> for Batch {
    type Error = Error;

    fn try_from(value: deflated::Batch) -> Result<Self, Self::Error> {
        let base_offset = value.base_offset;
        let batch_length = value.batch_length;
        let partition_leader_epoch = value.partition_leader_epoch;
        let magic = value.magic;
        let crc = value.crc;
        let attributes =
            BatchAttribute::try_from(value.attributes).inspect(|attribute| debug!(?attribute))?;
        let last_offset_delta = value.last_offset_delta;
        let base_timestamp = value.base_timestamp;
        let max_timestamp = value.max_timestamp;
        let producer_id = value.producer_id;
        let producer_epoch = value.producer_epoch;
        let base_sequence = value.base_sequence;

        let records: Vec<Record> = value.try_into()?;

        Ok(Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes: attributes.compression(Compression::None).into(),
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}

impl TryFrom<&deflated::Batch> for Batch {
    type Error = Error;

    fn try_from(value: &deflated::Batch) -> Result<Self, Self::Error> {
        let base_offset = value.base_offset;
        let batch_length = value.batch_length;
        let partition_leader_epoch = value.partition_leader_epoch;
        let magic = value.magic;
        let crc = value.crc;
        let attributes = value.attributes;
        let last_offset_delta = value.last_offset_delta;
        let base_timestamp = value.base_timestamp;
        let max_timestamp = value.max_timestamp;
        let producer_id = value.producer_id;
        let producer_epoch = value.producer_epoch;
        let base_sequence = value.base_sequence;

        let records: Vec<Record> = value.try_into()?;

        Ok(Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Compaction {
    pub batch: Batch,
    pub records: usize,
}

impl Batch {
    pub fn compression(&self) -> Result<Compression> {
        Compression::try_from(self.attributes)
    }

    #[must_use]
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub fn into_builder(self) -> Builder {
        self.into()
    }

    pub fn max_offset(&self) -> i64 {
        self.base_offset + i64::from(self.last_offset_delta)
    }

    pub fn keys(&self) -> BTreeSet<Bytes> {
        self.records
            .iter()
            .fold(BTreeSet::new(), |mut acc, record| {
                if let Some(key) = record.key() {
                    _ = acc.insert(key);
                }

                acc
            })
    }

    /// Compact this batch, retaining only the most recent record per key.
    ///
    /// `head` contains the keys of records in newer batches: a record whose
    /// key is in `head` is dropped, as is a record whose key reappears later
    /// in this batch. Records without a key are always retained. The batch
    /// structure (base offset, offset deltas, timestamps) is preserved, so
    /// compaction leaves gaps in the offsets, as in Apache Kafka.
    ///
    /// The returned [`Compaction`] holds the compacted batch and the number
    /// of records that were removed.
    pub fn compact(mut self, head: &BTreeSet<Bytes>) -> Result<Compaction> {
        let mut last_delta_offset_for_key = BTreeMap::new();
        let mut records = 0;

        for record in self.records.iter() {
            if let Some(key) = record.key() {
                if head.contains(&key) {
                    records += 1;
                    continue;
                }

                if last_delta_offset_for_key
                    .insert(key, record.offset_delta)
                    .is_some()
                {
                    records += 1;
                }
            }
        }

        debug!(?records);

        if records > 0 {
            let delta_offsets_to_retain: BTreeSet<i32> =
                last_delta_offset_for_key.into_values().collect();
            debug!(?delta_offsets_to_retain);

            self.records.retain(|record| {
                record.key().is_none() || delta_offsets_to_retain.contains(&record.offset_delta)
            });

            self.into_builder()
                .build()
                .map(|batch| Compaction { batch, records })
        } else {
            Ok(Compaction {
                batch: self,
                records,
            })
        }
    }
}

impl From<Batch> for Builder {
    fn from(value: Batch) -> Self {
        Self {
            base_offset: value.base_offset,
            partition_leader_epoch: value.partition_leader_epoch,
            magic: value.magic,
            attributes: value.attributes,
            last_offset_delta: value.last_offset_delta,
            base_timestamp: value.base_timestamp,
            max_timestamp: value.max_timestamp,
            producer_id: value.producer_id,
            producer_epoch: value.producer_epoch,
            base_sequence: value.base_sequence,
            records: value.records.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Sequence<super::Builder>,
}

impl Default for Builder {
    fn default() -> Self {
        let base_timestamp = to_timestamp(&SystemTime::now()).unwrap_or_default();

        Self {
            base_offset: 0,
            partition_leader_epoch: -1,
            magic: 2,
            attributes: 0,
            last_offset_delta: 0,
            base_timestamp,
            max_timestamp: base_timestamp,
            producer_id: -1,
            producer_epoch: 0,
            base_sequence: 0,
            records: Sequence::default(),
        }
    }
}

impl ByteSize for Builder {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(size_of_val(&self.partition_leader_epoch)
            + size_of_val(&self.magic)
            // size of crc is u32
            + size_of::<u32>()
            + size_of_val(&self.attributes)
            + size_of_val(&self.last_offset_delta)
            + size_of_val(&self.base_timestamp)
            + size_of_val(&self.max_timestamp)
            + size_of_val(&self.producer_id)
            + size_of_val(&self.producer_epoch)
            + size_of_val(&self.base_sequence)
            + self.records.size_in_bytes()?)
    }
}

impl Builder {
    #[must_use]
    pub fn base_offset(self, base_offset: i64) -> Self {
        Self {
            base_offset,
            ..self
        }
    }

    #[must_use]
    pub fn partition_leader_epoch(self, partition_leader_epoch: i32) -> Self {
        Self {
            partition_leader_epoch,
            ..self
        }
    }

    #[must_use]
    pub fn magic(self, magic: i8) -> Self {
        Self { magic, ..self }
    }

    #[must_use]
    pub fn attributes(self, attributes: i16) -> Self {
        Self { attributes, ..self }
    }

    #[must_use]
    pub fn last_offset_delta(self, last_offset_delta: i32) -> Self {
        Self {
            last_offset_delta,
            ..self
        }
    }

    #[must_use]
    pub fn base_timestamp(self, base_timestamp: i64) -> Self {
        Self {
            base_timestamp,
            ..self
        }
    }

    #[must_use]
    pub fn max_timestamp(self, max_timestamp: i64) -> Self {
        Self {
            max_timestamp,
            ..self
        }
    }

    #[must_use]
    pub fn producer_id(self, producer_id: i64) -> Self {
        Self {
            producer_id,
            ..self
        }
    }

    #[must_use]
    pub fn producer_epoch(self, producer_epoch: i16) -> Self {
        Self {
            producer_epoch,
            ..self
        }
    }

    #[must_use]
    pub fn base_sequence(self, base_sequence: i32) -> Self {
        Self {
            base_sequence,
            ..self
        }
    }

    #[must_use]
    pub fn record(mut self, record: super::Builder) -> Self {
        self.records.extend(vec![record]);
        self
    }

    fn crc(&self) -> Result<u32> {
        let mut encoder = RecordBatchEncoder::new(BytesMut::with_capacity(self.size_in_bytes()?));
        self.attributes.serialize(&mut encoder)?;
        self.last_offset_delta.serialize(&mut encoder)?;
        self.base_timestamp.serialize(&mut encoder)?;
        self.max_timestamp.serialize(&mut encoder)?;
        self.producer_id.serialize(&mut encoder)?;
        self.producer_epoch.serialize(&mut encoder)?;
        self.base_sequence.serialize(&mut encoder)?;
        self.records.serialize(&mut encoder)?;

        let encoded = Bytes::from(encoder);

        let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
        digest.update(&encoded[..]);

        Ok(digest.finalize() as u32)
    }

    pub fn build(self) -> Result<Batch> {
        let batch_length = self
            .size_in_bytes()
            .and_then(|size| i32::try_from(size).map_err(Into::into))?;

        let crc = self.crc()?;

        let records = self
            .records
            .0
            .into_iter()
            .try_fold(Vec::new(), |mut acc, record| {
                Record::try_from(record).map(|record| {
                    acc.push(record);
                    acc
                })
            })?;

        Ok(Batch {
            base_offset: self.base_offset,
            batch_length,
            partition_leader_epoch: self.partition_leader_epoch,
            magic: self.magic,
            crc,
            attributes: self.attributes,
            last_offset_delta: self.last_offset_delta,
            base_timestamp: self.base_timestamp,
            max_timestamp: self.max_timestamp,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self.base_sequence,
            records,
        })
    }
}

#[cfg(test)]
mod tests {
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;
    use crate::{MaximumAllocationSize, Result, de::BatchDecoder};

    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(EnvFilter::from_default_env().add_directive(
                    format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
                ))
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[test]
    fn record_maximum_allocation() -> Result<()> {
        let record = Record::builder()
            .value(Some(Bytes::from_static(&[100, 101, 102])))
            .build()?;

        let mut encoder = RecordBatchEncoder::new(BytesMut::new());
        record.serialize(&mut encoder)?;

        let encoded = BytesMut::from(encoder);
        assert_eq!(record.maximum_allocation_size()?, encoded.len());

        Ok(())
    }

    #[test]
    fn batch() -> Result<()> {
        let _guard = init_tracing()?;

        let decoded = Batch::builder()
            .base_offset(0)
            .partition_leader_epoch(-1)
            .magic(2)
            .attributes(0)
            .last_offset_delta(0)
            .base_timestamp(1_707_058_170_165)
            .max_timestamp(1_707_058_170_165)
            .producer_id(1)
            .producer_epoch(0)
            .base_sequence(1)
            .record(Record::builder().value(Some(Bytes::from_static(&[100, 101, 102]))))
            .build()?;

        assert_eq!(decoded.batch_length, 59);
        assert_eq!(decoded.crc, 1_126_819_645);

        Ok(())
    }

    #[test]
    fn batch_decode() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 255, 255, 255, 255, 2, 67, 41, 231, 61, 0, 0, 0,
            0, 0, 0, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 0, 0,
            0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 18, 0, 0, 0, 1, 6, 100, 101, 102, 0,
        ];

        let decoded = Batch::builder()
            .base_offset(0)
            .partition_leader_epoch(-1)
            .magic(2)
            .attributes(0)
            .last_offset_delta(0)
            .base_timestamp(1_707_058_170_165)
            .max_timestamp(1_707_058_170_165)
            .producer_id(1)
            .producer_epoch(0)
            .base_sequence(1)
            .record(Record::builder().value(Some(Bytes::from(vec![100, 101, 102]))))
            .build()?;

        let decoder = BatchDecoder::new(Bytes::copy_from_slice(&encoded[..]));
        let actual = Batch::deserialize(decoder)?;

        assert_eq!(decoded, actual);

        Ok(())
    }

    #[test]
    fn batch_encode() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = Bytes::from_static(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 255, 255, 255, 255, 2, 67, 41, 231, 61, 0, 0, 0,
            0, 0, 0, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 0, 0,
            0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 18, 0, 0, 0, 1, 6, 100, 101, 102, 0,
        ]);

        let decoded = Batch::builder()
            .base_offset(0)
            .partition_leader_epoch(-1)
            .magic(2)
            .attributes(0)
            .last_offset_delta(0)
            .base_timestamp(1_707_058_170_165)
            .max_timestamp(1_707_058_170_165)
            .producer_id(1)
            .producer_epoch(0)
            .base_sequence(1)
            .record(Record::builder().value(Some(Bytes::from(vec![100, 101, 102]))))
            .build()?;

        let decoder = BatchDecoder::new(encoded);
        let actual = Batch::deserialize(decoder)?;

        assert_eq!(decoded, actual);

        Ok(())
    }

    #[test]
    fn build_batch_records() -> Result<()> {
        let _guard = init_tracing()?;

        let keys: Vec<_> = (0..=6).map(|i| format!("k{i}")).map(Bytes::from).collect();
        let values: Vec<_> = (0..=11).map(|i| format!("v{i}")).map(Bytes::from).collect();

        let mut builder = Batch::builder();
        let indexes = [
            (1, 1),
            (2, 2),
            (1, 3),
            (1, 4),
            (3, 5),
            (2, 6),
            (4, 7),
            (5, 8),
            (5, 9),
            (2, 10),
            (6, 11),
        ];

        for (offset_delta, (key_index, value_index)) in indexes.into_iter().enumerate() {
            builder = builder.record(
                Record::builder()
                    .offset_delta(i32::try_from(offset_delta)?)
                    .key(Some(keys[key_index].clone()))
                    .value(Some(values[value_index].clone())),
            );
        }

        let batch = builder.build()?;
        assert_eq!(indexes.len(), batch.records.len());

        Ok(())
    }

    fn keyed(offset_delta: i32, key: &'static [u8], value: &'static [u8]) -> super::super::Builder {
        Record::builder()
            .offset_delta(offset_delta)
            .key(Some(Bytes::from_static(key)))
            .value(Some(Bytes::from_static(value)))
    }

    #[test]
    fn compact_noop_without_duplicates() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch::builder()
            .record(keyed(0, b"k1", b"v1"))
            .record(keyed(1, b"k2", b"v2"))
            .last_offset_delta(1)
            .build()?;

        let compaction = batch.clone().compact(&BTreeSet::new())?;

        assert_eq!(0, compaction.records);
        assert_eq!(batch, compaction.batch);

        Ok(())
    }

    #[test]
    fn compact_within_batch_keeps_last_occurrence_per_key() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch::builder()
            .base_offset(5)
            .record(keyed(0, b"k1", b"v1"))
            .record(keyed(1, b"k2", b"v2"))
            .record(keyed(2, b"k1", b"v3"))
            .last_offset_delta(2)
            .build()?;

        let compaction = batch.compact(&BTreeSet::new())?;

        assert_eq!(1, compaction.records);

        let records = &compaction.batch.records;
        assert_eq!(2, records.len());
        assert_eq!(1, records[0].offset_delta);
        assert_eq!(Some(Bytes::from_static(b"k2")), records[0].key);
        assert_eq!(2, records[1].offset_delta);
        assert_eq!(Some(Bytes::from_static(b"v3")), records[1].value);

        // batch structure is preserved so offsets keep their gaps
        assert_eq!(5, compaction.batch.base_offset);
        assert_eq!(2, compaction.batch.last_offset_delta);

        Ok(())
    }

    #[test]
    fn compact_drops_keys_in_head() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch::builder()
            .record(keyed(0, b"k1", b"v1"))
            .record(keyed(1, b"k2", b"v2"))
            .last_offset_delta(1)
            .build()?;

        let head = BTreeSet::from([Bytes::from_static(b"k1"), Bytes::from_static(b"k2")]);
        let compaction = batch.compact(&head)?;

        assert_eq!(2, compaction.records);
        assert!(compaction.batch.records.is_empty());

        Ok(())
    }

    #[test]
    fn compact_retains_keyless_records() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .offset_delta(0)
                    .value(Some(Bytes::from_static(b"v0"))),
            )
            .record(keyed(1, b"k1", b"v1"))
            .record(keyed(2, b"k1", b"v2"))
            .last_offset_delta(2)
            .build()?;

        let compaction = batch.compact(&BTreeSet::new())?;

        assert_eq!(1, compaction.records);

        let records = &compaction.batch.records;
        assert_eq!(2, records.len());
        assert_eq!(0, records[0].offset_delta);
        assert_eq!(None, records[0].key);
        assert_eq!(2, records[1].offset_delta);
        assert_eq!(Some(Bytes::from_static(b"k1")), records[1].key);

        Ok(())
    }

    #[test]
    fn compact_retains_keyless_when_all_keyed_records_drop() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .offset_delta(0)
                    .value(Some(Bytes::from_static(b"v0"))),
            )
            .record(keyed(1, b"k1", b"v1"))
            .last_offset_delta(1)
            .build()?;

        let head = BTreeSet::from([Bytes::from_static(b"k1")]);
        let compaction = batch.compact(&head)?;

        assert_eq!(1, compaction.records);

        let records = &compaction.batch.records;
        assert_eq!(1, records.len());
        assert_eq!(0, records[0].offset_delta);
        assert_eq!(None, records[0].key);

        Ok(())
    }
}
