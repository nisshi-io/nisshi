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
//
//! Deflated (compressed) Kafka Records
use std::{fmt::Formatter, io::Write, result};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::write::GzEncoder;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, Visitor},
};
use tracing::{debug, error, instrument};

use crate::{ByteSize, Compression, Decode as _, Decoder, Encode, Error, Result, record::Record};

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Frame {
    pub batches: Vec<Batch>,
}

impl ByteSize for Frame {
    #[instrument(skip_all, ret)]
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(size_of::<i32>()
            + self
                .batches
                .iter()
                .map(|batch| {
                    // base_offset
                    size_of::<i64>()
                // batch length
                + size_of::<i32>()
                + FIXED_BATCH_LENGTH
                + batch.record_data.len()
                })
                .sum::<usize>())
    }
}

impl TryFrom<crate::record::inflated::Frame> for Frame {
    type Error = Error;

    fn try_from(inflated: crate::record::inflated::Frame) -> Result<Self, Self::Error> {
        inflated
            .batches
            .into_iter()
            .map(Batch::try_from)
            .collect::<Result<Vec<_>>>()
            .map(|batches| Self { batches })
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
/// A deflated (compressed) batch of Kafka records
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
    pub record_count: u32,
    pub record_data: Bytes,
}

impl From<Batch> for Bytes {
    fn from(value: Batch) -> Self {
        let mut encoded =
            BytesMut::with_capacity(value.batch_length as usize + size_of_val(&value.base_offset));

        encoded.put_i64(value.base_offset);
        encoded.put_i32(value.batch_length);
        encoded.put_i32(value.partition_leader_epoch);
        encoded.put_i8(value.magic);
        encoded.put_u32(value.crc);
        encoded.put_i16(value.attributes);
        encoded.put_i32(value.last_offset_delta);
        encoded.put_i64(value.base_timestamp);
        encoded.put_i64(value.max_timestamp);
        encoded.put_i64(value.producer_id);
        encoded.put_i16(value.producer_epoch);
        encoded.put_i32(value.base_sequence);
        encoded.put_u32(value.record_count);
        encoded.put(value.record_data);

        Bytes::from(encoded)
    }
}

impl TryFrom<Bytes> for Batch {
    type Error = Error;
    fn try_from(mut encoded: Bytes) -> result::Result<Self, Self::Error> {
        let base_offset = encoded.try_get_i64()?;
        let batch_length = encoded.try_get_i32()?;

        let partition_leader_epoch = encoded.try_get_i32()?;
        let magic = encoded.try_get_i8()?;
        let crc = encoded.try_get_u32()?;

        debug!(base_offset, batch_length);

        let crc_data_size = usize::try_from(batch_length)
            .map_err(Into::into)
            .and_then(|batch_length| {
                batch_length
                    .checked_sub(size_of_val(&partition_leader_epoch))
                    .ok_or(Error::Overflow)
                    .inspect_err(|err| {
                        debug!(
                            batch_length,
                            size_of_val = size_of_val(&partition_leader_epoch),
                            ?err
                        )
                    })
            })
            .and_then(|batch_length| {
                batch_length
                    .checked_sub(size_of_val(&magic))
                    .ok_or(Error::Overflow)
                    .inspect_err(|err| {
                        debug!(batch_length, size_of_val = size_of_val(&magic), ?err)
                    })
            })
            .and_then(|batch_length| {
                batch_length
                    .checked_sub(size_of_val(&crc))
                    .ok_or(Error::Overflow)
                    .inspect_err(|err| debug!(batch_length, size_of_val = size_of_val(&crc), ?err))
            })?;

        debug!(crc_data_size, encoded = encoded.len(), crc);

        if crc_data_size > encoded.len() {
            return Err(Error::Overflow);
        }

        let crc_data = &encoded[..crc_data_size];

        let computed = {
            let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
            digest.update(crc_data);

            digest.finalize() as u32
        };

        if computed != crc {
            error!(crc, computed);
        }

        let attributes = encoded.try_get_i16()?;
        let last_offset_delta = encoded.try_get_i32()?;
        let base_timestamp = encoded.try_get_i64()?;
        let max_timestamp = encoded.try_get_i64()?;
        let producer_id = encoded.try_get_i64()?;
        let producer_epoch = encoded.try_get_i16()?;
        let base_sequence = encoded.try_get_i32()?;
        let record_count = encoded.try_get_u32()?;

        let record_data_size =
            usize::try_from(batch_length)
                .map_err(Into::into)
                .and_then(|batch_length| {
                    batch_length
                        .checked_sub(FIXED_BATCH_LENGTH)
                        .ok_or(Error::Overflow)
                })?;

        debug!(record_data_size, encoded = encoded.len(), computed);

        if record_data_size > encoded.len() {
            return Err(Error::Overflow);
        }

        let record_data = encoded.slice(..record_data_size);

        let batch = Batch {
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
            record_count,
            record_data,
        };

        Ok(batch)
    }
}

impl Batch {
    const TRANSACTIONAL_BITMASK: i16 = 0b1_0000i16;
    const CONTROL_BITMASK: i16 = 0b10_0000i16;

    pub fn is_transactional(&self) -> bool {
        self.attributes & Self::TRANSACTIONAL_BITMASK == Self::TRANSACTIONAL_BITMASK
    }

    pub fn is_control(&self) -> bool {
        self.attributes & Self::CONTROL_BITMASK == Self::CONTROL_BITMASK
    }

    pub fn is_idempotent(&self) -> bool {
        self.producer_id != -1 && self.base_sequence != -1
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct CrcData {
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: u32,
    pub record_data: Bytes,
}

impl TryFrom<&CrcData> for Bytes {
    type Error = Error;

    fn try_from(value: &CrcData) -> result::Result<Self, Self::Error> {
        let mut encoded = value.size_in_bytes().map(BytesMut::with_capacity)?;
        encoded.put_i16(value.attributes);
        encoded.put_i32(value.last_offset_delta);
        encoded.put_i64(value.base_timestamp);
        encoded.put_i64(value.max_timestamp);
        encoded.put_i64(value.producer_id);
        encoded.put_i16(value.producer_epoch);
        encoded.put_i32(value.base_sequence);
        encoded.put_u32(value.record_count);
        encoded.put(&value.record_data[..]);

        Ok(Bytes::from(encoded))
    }
}

impl ByteSize for CrcData {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(size_of_val(&self.attributes)
            + size_of_val(&self.last_offset_delta)
            + size_of_val(&self.base_timestamp)
            + size_of_val(&self.max_timestamp)
            + size_of_val(&self.producer_id)
            + size_of_val(&self.producer_epoch)
            + size_of_val(&self.base_sequence)
            + size_of_val(&self.record_count)
            + self.record_data.len())
    }
}

impl From<&Batch> for CrcData {
    fn from(batch: &Batch) -> Self {
        Self {
            attributes: batch.attributes,
            last_offset_delta: batch.last_offset_delta,
            base_timestamp: batch.base_timestamp,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            record_count: batch.record_count,
            record_data: batch.record_data.clone(),
        }
    }
}

impl CrcData {
    fn into_batch(self, base_offset: i64, partition_leader_epoch: i32, magic: i8) -> Result<Batch> {
        let crc = self
            .crc()
            .inspect(|crc| debug!(?self, base_offset, partition_leader_epoch, magic, crc))?;

        Ok(Batch {
            base_offset,
            batch_length: i32::try_from(FIXED_BATCH_LENGTH + self.record_data.len())?,
            partition_leader_epoch,
            magic,
            crc,
            attributes: self.attributes,
            last_offset_delta: self.last_offset_delta,
            base_timestamp: self.base_timestamp,
            max_timestamp: self.max_timestamp,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self.base_sequence,
            record_count: self.record_count,
            record_data: self.record_data,
        })
    }

    fn crc(&self) -> Result<u32> {
        let encoded = Bytes::try_from(self)?;
        debug!(encoded = ?&encoded[..]);

        let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
        digest.update(&encoded[..]);

        Ok(digest.finalize() as u32)
    }
}

fn into_record_data(records: &[Record], compression: Compression) -> Result<Bytes> {
    let sizing = records
        .iter()
        .map(|record| record.size_in_bytes())
        .collect::<Result<Vec<_>>>()
        .map(|sizes| sizes.iter().sum::<usize>())?;

    debug!(sizing);

    match compression {
        Compression::None => records.encode(),

        Compression::Gzip => {
            let uncompressed = records
                .encode()
                .inspect(|inflated| debug!(inflated_len = inflated.len()))?;

            let mut gz = GzEncoder::new(
                BytesMut::with_capacity(uncompressed.len()).writer(),
                flate2::Compression::default(),
            );

            gz.write_all(&uncompressed)?;

            gz.finish()
                .map(|w| w.into_inner())
                .map(Bytes::from)
                .inspect(|deflated| debug!(deflated_len = deflated.len()))
                .map_err(Into::into)
        }

        Compression::Lz4 => {
            let uncompressed = records.encode()?;

            let mut lz4 = lz4::EncoderBuilder::new()
                .build(BytesMut::with_capacity(uncompressed.len()).writer())?;

            lz4.write_all(&uncompressed[..])?;

            let (w, _) = lz4.finish();
            Ok(Bytes::from(w.into_inner()))
        }

        Compression::Zstd => {
            let uncompressed = records.encode()?;

            let mut zstd = zstd::stream::write::Encoder::new(
                BytesMut::with_capacity(uncompressed.len()).writer(),
                0,
            )?;

            zstd.write_all(&uncompressed[..])?;

            zstd.finish()
                .map(|w| w.into_inner())
                .map(Bytes::from)
                .map_err(Into::into)
        }

        unexpected => Err(Error::UnexpectedType(format!("{unexpected:?}",))),
    }
}

impl TryFrom<crate::record::inflated::Batch> for Batch {
    type Error = Error;

    fn try_from(batch: crate::record::inflated::Batch) -> std::result::Result<Self, Self::Error> {
        CrcData {
            attributes: batch.attributes,
            last_offset_delta: batch.last_offset_delta,
            base_timestamp: batch.base_timestamp,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            record_count: u32::try_from(batch.records.len())?,
            record_data: into_record_data(&batch.records[..], batch.compression()?)?,
        }
        .into_batch(batch.base_offset, batch.partition_leader_epoch, batch.magic)
    }
}

impl Batch {
    pub fn max_offset(&self) -> i64 {
        self.base_offset + i64::from(self.last_offset_delta)
    }

    fn compression(&self) -> Result<Compression> {
        Compression::try_from(self.attributes)
    }
}

impl TryFrom<Batch> for Vec<Record> {
    type Error = Error;

    #[instrument(skip_all)]
    fn try_from(mut batch: Batch) -> Result<Self, Self::Error> {
        let record_count = usize::try_from(batch.record_count)?;

        debug!(?record_count);
        debug!(?batch.record_data);

        if batch
            .compression()
            .is_ok_and(|compression| compression == Compression::None)
        {
            let mut records = Vec::with_capacity(record_count);

            for _ in 0..record_count {
                let record = Record::decode(&mut batch.record_data)?;
                records.push(record);
            }

            Ok(records)
        } else {
            let mut reader = batch
                .compression()
                .and_then(|compression| compression.inflator(batch.record_data.reader()))?;

            let mut decoder = Decoder::new(&mut reader);
            let mut records = Vec::with_capacity(record_count);

            for _ in 0..record_count {
                let record = Record::deserialize(&mut decoder)?;
                records.push(record);
            }

            Ok(records)
        }
    }
}

impl TryFrom<&Batch> for Vec<Record> {
    type Error = Error;

    fn try_from(batch: &Batch) -> Result<Self, Self::Error> {
        let record_count = usize::try_from(batch.record_count)?;

        debug!(?record_count);
        debug!(?batch.record_data);

        let mut reader = batch
            .compression()
            .and_then(|compression| compression.inflator(batch.record_data.clone().reader()))?;

        let mut decoder = Decoder::new(&mut reader);
        let mut records = Vec::with_capacity(record_count);

        for _ in 0..record_count {
            let record = Record::deserialize(&mut decoder)?;
            records.push(record);
        }

        Ok(records)
    }
}

const FIXED_BATCH_LENGTH: usize =
    // partition leader epoch
    size_of::<i32>()
    // magic
    + size_of::<i8>()
    // CRC
    + size_of::<u32>()
    // attributes
    + size_of::<i16>()
    // last_offset_delta
    + size_of::<i32>()
    // base timestamp
    + size_of::<i64>()
    // max timestamp
    + size_of::<i64>()
    // producer id
    + size_of::<i64>()
    // producer epoch
    + size_of::<i16>()
    // base sequence
    + size_of::<i32>()
    // record count
    + size_of::<u32>();

impl<'de> Deserialize<'de> for Batch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Batch;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(Batch))
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                debug!(v = ?v[..]);
                Batch::try_from(Bytes::from(v)).map_err(|err| de::Error::custom(err.to_string()))
            }
        }

        deserializer.deserialize_byte_buf(V)
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        BatchAttribute, ControlBatch, EndTransactionMarker, de::BatchDecoder, record::inflated,
    };

    use super::*;

    use tracing::subscriber::DefaultGuard;

    #[cfg(miri)]
    fn init_tracing() -> Result<()> {
        Ok(())
    }

    #[cfg(not(miri))]
    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        use tracing_subscriber::fmt::format::FmtSpan;

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_max_level(tracing::Level::DEBUG)
                .with_span_events(FmtSpan::ACTIVE)
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    const LOREM: &[u8] = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
    eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad \
    minim veniam, quis nostrud exercitation ullamco laboris nisi ut \
    aliquip ex ea commodo consequat. Duis aute irure dolor in \
    reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla \
    pariatur. Excepteur sint occaecat cupidatat non proident, sunt in \
    culpa qui officia deserunt mollit anim id est laborum.";

    #[test]
    fn decode_gzip() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 89, 0, 0, 0, 0, 2, 198, 48, 56, 83, 0, 1, 0, 0, 0, 0,
            0, 0, 1, 145, 183, 231, 239, 158, 0, 0, 1, 145, 183, 231, 239, 158, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 31, 139, 8, 0, 0, 0, 0,
            0, 0, 19, 53, 144, 205, 81, 67, 49, 12, 132, 31, 23, 104, 99, 11, 200, 188, 42, 224,
            198, 149, 2, 132, 172, 4, 205, 248, 47, 182, 148, 73, 9, 169, 153, 19, 50, 15, 110,
            150, 37, 173, 118, 191, 199, 203, 182, 109, 79, 223, 207, 239, 109, 72, 129, 246, 233,
            5, 169, 229, 54, 48, 213, 64, 69, 236, 4, 110, 117, 10, 155, 152, 15, 80, 210, 174,
            147, 181, 94, 32, 89, 163, 57, 37, 197, 2, 68, 125, 150, 150, 96, 82, 122, 44, 107,
            101, 77, 154, 188, 26, 220, 144, 233, 51, 228, 33, 118, 72, 11, 10, 93, 42, 129, 178,
            94, 157, 118, 124, 24, 164, 106, 9, 109, 20, 93, 143, 91, 148, 84, 78, 184, 186, 78,
            212, 54, 109, 120, 130, 220, 101, 176, 26, 153, 182, 10, 207, 153, 10, 183, 67, 121,
            13, 233, 212, 117, 233, 87, 82, 123, 12, 67, 40, 140, 151, 240, 212, 142, 0, 113, 202,
            118, 188, 46, 73, 114, 19, 232, 240, 112, 114, 100, 213, 138, 33, 125, 200, 151, 212,
            36, 35, 130, 199, 199, 173, 101, 239, 113, 78, 194, 78, 36, 133, 204, 41, 96, 205, 249,
            159, 80, 4, 114, 156, 253, 162, 100, 168, 203, 16, 58, 141, 40, 124, 236, 120, 187,
            179, 116, 19, 95, 24, 131, 65, 99, 38, 225, 152, 99, 239, 154, 200, 214, 70, 164, 232,
            163, 105, 146, 186, 40, 46, 82, 113, 148, 61, 119, 90, 185, 209, 206, 103, 101, 37, 36,
            153, 50, 86, 183, 180, 188, 108, 208, 2, 164, 129, 99, 254, 113, 245, 178, 111, 63,
            143, 62, 223, 101, 198, 1, 0, 0, 0, 0, 0,
        ];

        let decoder = BatchDecoder::new(Bytes::from_static(encoded));
        let decoded = Batch::deserialize(decoder)?;

        assert_eq!(
            Compression::Gzip,
            Compression::try_from(decoded.attributes)?
        );

        let mut inflated = crate::record::inflated::Batch::try_from(decoded.clone())
            .inspect(|inflated| debug!(?inflated))?;

        assert_eq!(
            Compression::None,
            Compression::try_from(inflated.attributes)?
        );

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            inflated.records
        );

        inflated.attributes = BatchAttribute::try_from(inflated.attributes)
            .map(|attribute| attribute.compression(Compression::Gzip).into())?;

        let deflated = Batch::try_from(inflated)?;
        assert_eq!(decoded.base_offset, deflated.base_offset);
        assert_eq!(
            decoded.partition_leader_epoch,
            deflated.partition_leader_epoch
        );
        assert_eq!(decoded.magic, deflated.magic);
        assert_eq!(decoded.attributes, deflated.attributes);
        assert_eq!(decoded.last_offset_delta, deflated.last_offset_delta);
        assert_eq!(decoded.base_timestamp, deflated.base_timestamp);

        let records: Vec<Record> = deflated.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    fn decode_zstd() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 91, 0, 0, 0, 0, 2, 200, 21, 172, 244, 0, 4, 0, 0, 0,
            0, 0, 0, 1, 145, 183, 250, 201, 221, 0, 0, 1, 145, 183, 250, 201, 221, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 40, 181, 47, 253, 0,
            88, 13, 9, 0, 70, 217, 64, 36, 160, 37, 73, 7, 255, 255, 255, 255, 143, 174, 211, 102,
            147, 114, 239, 182, 165, 188, 148, 244, 91, 75, 123, 39, 146, 211, 241, 3, 167, 201,
            64, 245, 234, 24, 45, 9, 56, 0, 53, 0, 53, 0, 197, 44, 90, 146, 147, 15, 123, 209, 29,
            99, 44, 57, 147, 242, 238, 145, 18, 167, 14, 240, 197, 53, 216, 71, 250, 57, 169, 162,
            68, 227, 112, 178, 27, 29, 160, 77, 21, 159, 138, 174, 28, 169, 201, 116, 94, 99, 116,
            4, 0, 36, 40, 40, 138, 34, 0, 130, 229, 38, 220, 115, 204, 62, 221, 154, 126, 195, 84,
            41, 89, 187, 99, 225, 7, 114, 194, 37, 137, 48, 157, 53, 61, 15, 29, 152, 186, 25, 81,
            115, 187, 41, 169, 154, 155, 139, 5, 74, 168, 60, 188, 84, 203, 12, 101, 106, 116, 141,
            206, 64, 149, 40, 177, 142, 234, 180, 74, 73, 43, 214, 13, 89, 104, 186, 46, 229, 163,
            78, 201, 197, 23, 35, 106, 44, 60, 89, 14, 81, 123, 241, 200, 196, 124, 192, 232, 24,
            213, 94, 163, 68, 175, 49, 171, 233, 41, 160, 4, 172, 60, 28, 57, 99, 110, 52, 5, 193,
            100, 114, 201, 107, 110, 85, 124, 41, 103, 138, 150, 200, 201, 164, 245, 241, 66, 225,
            250, 60, 180, 78, 201, 97, 153, 58, 64, 51, 249, 161, 83, 59, 43, 22, 22, 249, 201, 81,
            172, 225, 26, 227, 138, 246, 84, 148, 198, 227, 145, 24, 80, 66, 8, 0, 220, 218, 44,
            15, 30, 45, 186, 100, 95, 73, 49, 124, 17, 109, 0, 43, 70, 43, 93, 140, 227, 122, 67,
            136, 2, 0, 0, 0,
        ];

        let decoder = BatchDecoder::new(Bytes::from_static(encoded));
        let decoded = Batch::deserialize(decoder)?;

        assert_eq!(
            Compression::Zstd,
            Compression::try_from(decoded.attributes)?
        );

        let mut inflated = crate::record::inflated::Batch::try_from(decoded.clone())?;

        assert_eq!(
            Compression::None,
            Compression::try_from(inflated.attributes)?
        );

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            inflated.records
        );

        inflated.attributes = BatchAttribute::try_from(inflated.attributes)
            .map(|attribute| attribute.compression(Compression::Zstd).into())?;

        let deflated = Batch::try_from(inflated)?;
        assert_eq!(decoded.base_offset, deflated.base_offset);
        assert_eq!(
            decoded.partition_leader_epoch,
            deflated.partition_leader_epoch
        );
        assert_eq!(decoded.magic, deflated.magic);
        assert_eq!(decoded.attributes, deflated.attributes);
        assert_eq!(decoded.last_offset_delta, deflated.last_offset_delta);
        assert_eq!(decoded.base_timestamp, deflated.base_timestamp);

        let records: Vec<Record> = deflated.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    fn decode_lz4() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 237, 0, 0, 0, 0, 2, 43, 216, 167, 237, 0, 3, 0, 0, 0,
            0, 0, 0, 1, 145, 184, 77, 37, 242, 0, 0, 1, 145, 184, 77, 37, 242, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 4, 34, 77, 24, 96, 64,
            130, 173, 1, 0, 0, 242, 95, 136, 7, 0, 0, 0, 1, 250, 6, 76, 111, 114, 101, 109, 32,
            105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 105, 116, 32, 97, 109,
            101, 116, 44, 32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32, 97, 100,
            105, 112, 105, 115, 99, 105, 110, 103, 32, 101, 108, 105, 116, 44, 32, 115, 101, 100,
            32, 100, 111, 32, 101, 105, 117, 115, 109, 111, 100, 32, 116, 101, 109, 112, 111, 114,
            32, 105, 110, 99, 105, 100, 105, 100, 117, 110, 116, 32, 117, 116, 32, 108, 97, 98,
            111, 114, 101, 32, 101, 116, 91, 0, 240, 14, 101, 32, 109, 97, 103, 110, 97, 32, 97,
            108, 105, 113, 117, 97, 46, 32, 85, 116, 32, 101, 110, 105, 109, 32, 97, 100, 32, 109,
            105, 9, 0, 242, 26, 118, 101, 110, 105, 97, 109, 44, 32, 113, 117, 105, 115, 32, 110,
            111, 115, 116, 114, 117, 100, 32, 101, 120, 101, 114, 99, 105, 116, 97, 116, 105, 111,
            110, 32, 117, 108, 108, 97, 109, 99, 111, 90, 0, 0, 37, 0, 98, 105, 115, 105, 32, 117,
            116, 83, 0, 242, 1, 105, 112, 32, 101, 120, 32, 101, 97, 32, 99, 111, 109, 109, 111,
            100, 111, 193, 0, 112, 113, 117, 97, 116, 46, 32, 68, 83, 0, 162, 97, 117, 116, 101,
            32, 105, 114, 117, 114, 101, 145, 0, 240, 2, 32, 105, 110, 32, 114, 101, 112, 114, 101,
            104, 101, 110, 100, 101, 114, 105, 116, 17, 0, 176, 118, 111, 108, 117, 112, 116, 97,
            116, 101, 32, 118, 234, 0, 164, 32, 101, 115, 115, 101, 32, 99, 105, 108, 108, 34, 1,
            208, 101, 32, 101, 117, 32, 102, 117, 103, 105, 97, 116, 32, 110, 145, 0, 240, 4, 32,
            112, 97, 114, 105, 97, 116, 117, 114, 46, 32, 69, 120, 99, 101, 112, 116, 101, 117, 71,
            1, 240, 4, 110, 116, 32, 111, 99, 99, 97, 101, 99, 97, 116, 32, 99, 117, 112, 105, 100,
            97, 116, 50, 0, 160, 111, 110, 32, 112, 114, 111, 105, 100, 101, 110, 70, 1, 0, 42, 1,
            128, 105, 110, 32, 99, 117, 108, 112, 97, 248, 0, 224, 32, 111, 102, 102, 105, 99, 105,
            97, 32, 100, 101, 115, 101, 114, 30, 0, 64, 109, 111, 108, 108, 147, 1, 0, 33, 1, 240,
            1, 105, 100, 32, 101, 115, 116, 32, 108, 97, 98, 111, 114, 117, 109, 46, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];

        let decoder = BatchDecoder::new(Bytes::from_static(encoded));
        let decoded = Batch::deserialize(decoder)?;
        assert_eq!(Compression::Lz4, Compression::try_from(decoded.attributes)?);

        let mut inflated = crate::record::inflated::Batch::try_from(decoded.clone())?;

        assert_eq!(
            Compression::None,
            Compression::try_from(inflated.attributes)?
        );

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            inflated.records
        );

        inflated.attributes = BatchAttribute::try_from(inflated.attributes)
            .map(|attribute| attribute.compression(Compression::Lz4).into())?;

        let deflated = Batch::try_from(inflated)?;
        assert_eq!(decoded.base_offset, deflated.base_offset);
        assert_eq!(
            decoded.partition_leader_epoch,
            deflated.partition_leader_epoch
        );
        assert_eq!(decoded.magic, deflated.magic);
        assert_eq!(decoded.attributes, deflated.attributes);
        assert_eq!(decoded.last_offset_delta, deflated.last_offset_delta);
        assert_eq!(decoded.base_timestamp, deflated.base_timestamp);

        let records: Vec<Record> = deflated.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    fn decode_snappy() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 218, 0, 0, 0, 0, 2, 228, 189, 111, 249, 0, 2, 0, 0, 0,
            0, 0, 0, 1, 145, 184, 92, 90, 192, 0, 0, 1, 145, 184, 92, 90, 192, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 198, 3, 240, 111, 136, 7,
            0, 0, 0, 1, 250, 6, 76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100, 111,
            108, 111, 114, 32, 115, 105, 116, 32, 97, 109, 101, 116, 44, 32, 99, 111, 110, 115,
            101, 99, 116, 101, 116, 117, 114, 32, 97, 100, 105, 112, 105, 115, 99, 105, 110, 103,
            32, 101, 108, 105, 116, 44, 32, 115, 101, 100, 32, 100, 111, 32, 101, 105, 117, 115,
            109, 111, 100, 32, 116, 101, 109, 112, 111, 114, 32, 105, 110, 99, 105, 100, 105, 100,
            117, 110, 116, 32, 117, 116, 32, 108, 97, 98, 111, 114, 101, 32, 101, 116, 32, 100, 1,
            91, 112, 101, 32, 109, 97, 103, 110, 97, 32, 97, 108, 105, 113, 117, 97, 46, 32, 85,
            116, 32, 101, 110, 105, 109, 32, 97, 100, 32, 109, 105, 1, 9, 160, 118, 101, 110, 105,
            97, 109, 44, 32, 113, 117, 105, 115, 32, 110, 111, 115, 116, 114, 117, 100, 32, 101,
            120, 101, 114, 99, 105, 116, 97, 116, 105, 111, 110, 32, 117, 108, 108, 97, 109, 99,
            111, 9, 90, 1, 37, 8, 105, 115, 105, 1, 106, 5, 83, 60, 105, 112, 32, 101, 120, 32,
            101, 97, 32, 99, 111, 109, 109, 111, 100, 111, 9, 193, 24, 113, 117, 97, 116, 46, 32,
            68, 1, 83, 36, 97, 117, 116, 101, 32, 105, 114, 117, 114, 101, 13, 236, 60, 105, 110,
            32, 114, 101, 112, 114, 101, 104, 101, 110, 100, 101, 114, 105, 116, 1, 17, 40, 118,
            111, 108, 117, 112, 116, 97, 116, 101, 32, 118, 1, 234, 36, 32, 101, 115, 115, 101, 32,
            99, 105, 108, 108, 49, 34, 232, 101, 32, 101, 117, 32, 102, 117, 103, 105, 97, 116, 32,
            110, 117, 108, 108, 97, 32, 112, 97, 114, 105, 97, 116, 117, 114, 46, 32, 69, 120, 99,
            101, 112, 116, 101, 117, 114, 32, 115, 105, 110, 116, 32, 111, 99, 99, 97, 101, 99, 97,
            116, 32, 99, 117, 112, 105, 100, 97, 116, 1, 50, 60, 111, 110, 32, 112, 114, 111, 105,
            100, 101, 110, 116, 44, 32, 115, 117, 110, 5, 117, 88, 99, 117, 108, 112, 97, 32, 113,
            117, 105, 32, 111, 102, 102, 105, 99, 105, 97, 32, 100, 101, 115, 101, 114, 1, 30, 12,
            109, 111, 108, 108, 33, 147, 33, 33, 60, 105, 100, 32, 101, 115, 116, 32, 108, 97, 98,
            111, 114, 117, 109, 46, 0, 0, 0, 0,
        ];

        let decoder = BatchDecoder::new(Bytes::from_static(encoded));
        let decoded = Batch::deserialize(decoder)?;
        assert_eq!(
            Compression::Snappy,
            Compression::try_from(decoded.attributes)?
        );

        let records: Vec<Record> = decoded.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    pub fn is_transactional() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch {
            base_offset: 0,
            batch_length: 68,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 3650210183,
            attributes: 16,
            last_offset_delta: 0,
            base_timestamp: 1729509915759,
            max_timestamp: 1729509915759,
            producer_id: 5,
            producer_epoch: 0,
            base_sequence: 0,
            record_count: 1,
            record_data: Bytes::from_static(b"$\0\0\0\x08\0\0\0\0\x10test0-ok\0"),
        };

        assert!(batch.is_transactional());

        Ok(())
    }

    #[test]
    pub fn is_transactional_control() -> Result<()> {
        use crate::record::inflated;

        let _guard = init_tracing()?;

        let deflated = Batch {
            base_offset: 1,
            batch_length: 66,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 820655041,
            attributes: 48,
            last_offset_delta: 0,
            base_timestamp: 1729509916024,
            max_timestamp: 1729509916024,
            producer_id: 5,
            producer_epoch: 0,
            base_sequence: -1,
            record_count: 1,
            record_data: Bytes::from_static(b" \0\0\0\x08\0\0\0\x01\x0c\0\0\0\0\0\0\0"),
        };

        assert!(deflated.is_transactional());
        assert!(deflated.is_control());

        let inflated = inflated::Batch::try_from(deflated)?;

        assert_eq!(1, inflated.records.len());
        assert_eq!(
            Some(Bytes::from_static(b"\0\0\0\x01")),
            inflated.records[0].key
        );

        let control_batch = ControlBatch::try_from(inflated.records[0].clone().key().unwrap())?;
        assert_eq!(0, control_batch.version);
        assert!(control_batch.is_commit());
        assert!(!control_batch.is_abort());

        assert_eq!(
            Some(Bytes::from_static(b"\0\0\0\0\0\0")),
            inflated.records[0].value
        );

        let txn_marker =
            EndTransactionMarker::try_from(inflated.records[0].clone().value.unwrap())?;
        assert_eq!(0, txn_marker.version);
        assert_eq!(0, txn_marker.coordinator_epoch);

        Ok(())
    }

    #[test]
    fn deflate() -> Result<()> {
        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
        let value = Bytes::from_static(b"consectetur adipiscing elit");

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_sequence = 78987;
        let base_offset = 9876789;
        let attributes: i16 = BatchAttribute::default().transaction(true).into();

        let batch: Batch = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .attributes(attributes)
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_offset(base_offset)
            .base_sequence(base_sequence)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        assert_eq!(base_sequence, batch.base_sequence);
        assert_eq!(producer_id, batch.producer_id);
        assert_eq!(producer_epoch, batch.producer_epoch);
        assert_eq!(attributes, batch.attributes);
        assert_eq!(1, batch.record_count);
        assert_eq!(base_offset, batch.base_offset);

        Ok(())
    }

    #[test]
    fn encode_decode() -> Result<()> {
        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
        let value = Bytes::from_static(b"consectetur adipiscing elit");

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_sequence = 78987;
        let base_offset = 9876789;
        let attributes: i16 = BatchAttribute::default().transaction(true).into();

        let batch: Batch = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .attributes(attributes)
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_offset(base_offset)
            .base_sequence(base_sequence)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        let expected = batch.batch_length as usize;

        let encoded = Bytes::from(batch);

        let deflated = Batch::try_from(encoded)?;

        assert_eq!(deflated.producer_id, producer_id);
        assert_eq!(deflated.producer_epoch, producer_epoch);
        assert_eq!(deflated.base_sequence, base_sequence);
        assert_eq!(deflated.base_offset, base_offset);
        assert_eq!(deflated.attributes, attributes);

        let inflated = inflated::Batch::try_from(deflated)?;
        assert_eq!(1, inflated.records.len());

        assert_eq!(Some(key), inflated.records[0].key);
        assert_eq!(Some(value), inflated.records[0].value);

        Ok(())
    }
}
