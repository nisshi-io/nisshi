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

//! Decodes the records of a batch for every compression codec.
//!
//! Input layout, so the fuzzer spends its time in the decompressors and the
//! record decoder rather than rediscovering batch header fields:
//!
//! | bytes      | meaning                                          |
//! |------------|--------------------------------------------------|
//! | `[0] % 5`  | codec: none, gzip, snappy, lz4, zstd              |
//! | `[1..3]`   | `record_count`, big-endian `u16`                  |
//! | `[3..]`    | `record_data`, the (possibly compressed) records  |

#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use nisshi_sans_io::{
    Compression,
    record::{Record, deflated, inflated},
};

const CODECS: [Compression; 5] = [
    Compression::None,
    Compression::Gzip,
    Compression::Snappy,
    Compression::Lz4,
    Compression::Zstd,
];

fuzz_target!(|data: &[u8]| {
    let [selector, count_hi, count_lo, record_data @ ..] = data else {
        return;
    };

    let compression = CODECS[usize::from(*selector) % CODECS.len()].clone();
    let uncompressed = matches!(compression, Compression::None);

    let batch = deflated::Batch {
        attributes: i16::from(compression),
        record_count: u32::from(u16::from_be_bytes([*count_hi, *count_lo])),
        record_data: Bytes::copy_from_slice(record_data),
        ..Default::default()
    };

    // The by-reference and by-value conversions are separate
    // implementations (they differ for uncompressed batches), and the
    // storage backends use both, so exercise each.
    let by_reference = Vec::<Record>::try_from(&batch);
    let by_value = Vec::<Record>::try_from(batch.clone());

    // The two must agree on whether the batch decodes and, when it does, on
    // the records. Error variants are not compared.
    //
    // For a compressed batch both conversions call the same
    // `inflate_records`, so agreement there shows only that it is
    // deterministic. For an uncompressed batch the by-value conversion
    // decodes each record with `Record::decode` and the by-reference
    // conversion with the serde visitors, so there the comparison is a
    // differential between the two record decoders.
    //
    // The `(Ok, Err)` arm for an uncompressed batch exists because
    // `Record::decode` rejects what the serde visitors accept in two known
    // ways: bytes left after the last record, and a record `length` that
    // does not match its body. Without the arm the fuzzer reports those
    // within seconds and finds nothing else. The arm accepts more than those
    // two, though: it accepts every batch that the serde visitors decode and
    // `Record::decode` rejects. So this target cannot catch a serde visitor
    // that is more lenient than `Decode`; `fuzz_varint` covers the varint
    // visitors by comparing the two varint decoders directly, and nothing
    // covers the key, value and header visitors that way. Remove the arm
    // once the by-reference conversion rejects trailing bytes and a
    // mismatched `length`.
    match (&by_reference, &by_value) {
        (Ok(by_reference), Ok(by_value)) => assert_eq!(by_reference, by_value),
        (Err(_), Err(_)) => {}
        (Ok(_), Err(_)) if uncompressed => {}
        _ => panic!(
            "by-reference and by-value decoding disagree: {:?} vs {:?}",
            by_reference.as_ref().map(Vec::len),
            by_value.as_ref().map(Vec::len),
        ),
    }

    let _ = inflated::Batch::try_from(&batch);
    let _ = inflated::Batch::try_from(batch);
});
