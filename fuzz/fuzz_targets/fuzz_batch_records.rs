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

    // Differential oracle: the two must agree on whether the batch decodes
    // and, when it does, on the records. Error variants are not compared.
    //
    // One known difference is allowed. For an uncompressed batch, the
    // by-value conversion rejects bytes left after the last record, and the
    // by-reference conversion accepts them. Without this exception the
    // fuzzer reports it within seconds and finds nothing else. Compressed
    // batches share one implementation, so they must agree. Remove the
    // exception once the by-reference conversion rejects trailing bytes.
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
