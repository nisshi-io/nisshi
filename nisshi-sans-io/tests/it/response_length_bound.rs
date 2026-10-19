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

//! `Frame::response_from_bytes` bounds every length-prefixed field to the
//! frame's own length, as `Frame::request_from_bytes` already does.
//!
//! A response comes from an upstream broker, so a malicious or buggy broker
//! can claim a length far larger than the frame it sent. Without the bound
//! the decoder reserves a zeroed buffer of the claimed size (up to the 1 GiB
//! fallback) before the read fails.

use std::assert_matches;

use crate::common::init_tracing;
use bytes::{BufMut, Bytes, BytesMut};
use nisshi_sans_io::{ApiKey, Body, Error, Frame, Result, SaslAuthenticateResponse};

/// Larger than any frame these tests build, well under the 1 GiB fallback.
const CLAIMED_LENGTH: usize = 100_000_000;

/// `SaslAuthenticateResponse` v0, non-flexible: `auth_bytes` has a 4-byte
/// length followed by `auth_bytes`.
fn non_flexible(auth_bytes_length: i32, auth_bytes: &[u8]) -> Bytes {
    let mut encoded = BytesMut::new();
    encoded.put_i32(0); // size: unused by Frame::deserialize itself
    encoded.put_i32(1); // correlation_id
    encoded.put_i16(0); // error_code
    encoded.put_i16(-1); // error_message: null
    encoded.put_i32(auth_bytes_length);
    encoded.put_slice(auth_bytes);
    encoded.freeze()
}

/// `SaslAuthenticateResponse` v2, flexible: `auth_bytes` has an unsigned
/// varint length plus one, and the frame ends with the session lifetime and
/// an empty tag buffer.
fn flexible(auth_bytes_varint: &[u8], auth_bytes: &[u8]) -> Bytes {
    let mut encoded = BytesMut::new();
    encoded.put_i32(0); // size: unused by Frame::deserialize itself
    encoded.put_i32(1); // correlation_id
    encoded.put_u8(0); // header tag buffer: empty
    encoded.put_i16(0); // error_code
    encoded.put_u8(0); // error_message: compact nullable string, null
    encoded.put_slice(auth_bytes_varint);
    encoded.put_slice(auth_bytes);
    encoded.put_i64(0); // session_lifetime_ms
    encoded.put_u8(0); // body tag buffer: empty
    encoded.freeze()
}

/// Unsigned varint encoding of `value`.
fn varint(mut value: u32) -> Vec<u8> {
    let mut encoded = Vec::new();
    while value >= 0x80 {
        encoded.push((value as u8) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
    encoded
}

fn auth_bytes(frame: Frame) -> Result<Bytes> {
    assert_matches!(frame.body, Body::SaslAuthenticateResponse(_));
    SaslAuthenticateResponse::try_from(frame.body).map(|response| response.auth_bytes)
}

/// A non-flexible `auth_bytes` length claim larger than the frame must be
/// rejected with `MessageMaxSizeExceeded`, not by the read failing at the end
/// of the frame after the buffer was already reserved.
#[test]
fn non_flexible_bytes_claim_beyond_the_frame_is_rejected() -> Result<()> {
    let _guard = init_tracing()?;

    let encoded = non_flexible(i32::try_from(CLAIMED_LENGTH)?, &[]);

    let err = Frame::response_from_bytes(encoded, SaslAuthenticateResponse::KEY, 0)
        .expect_err("a claimed length beyond the frame must be rejected");

    assert_matches!(err, Error::MessageMaxSizeExceeded(CLAIMED_LENGTH));
    Ok(())
}

/// The flexible length branch must be bounded the same way.
#[test]
fn flexible_bytes_claim_beyond_the_frame_is_rejected() -> Result<()> {
    let _guard = init_tracing()?;

    let encoded = flexible(&varint(u32::try_from(CLAIMED_LENGTH)? + 1), &[]);

    let err = Frame::response_from_bytes(encoded, SaslAuthenticateResponse::KEY, 2)
        .expect_err("a claimed length beyond the frame must be rejected");

    assert_matches!(err, Error::MessageMaxSizeExceeded(CLAIMED_LENGTH));
    Ok(())
}

/// Bounding by the frame's length must not reject a valid response, in
/// either encoding.
#[test]
fn valid_response_still_decodes() -> Result<()> {
    let _guard = init_tracing()?;

    let expected = Bytes::from_static(b"server-final-message");

    let encoded = non_flexible(i32::try_from(expected.len())?, &expected);
    let frame = Frame::response_from_bytes(encoded, SaslAuthenticateResponse::KEY, 0)?;
    assert_eq!(expected, auth_bytes(frame)?);

    let encoded = flexible(&varint(u32::try_from(expected.len())? + 1), &expected);
    let frame = Frame::response_from_bytes(encoded, SaslAuthenticateResponse::KEY, 2)?;
    assert_eq!(expected, auth_bytes(frame)?);

    Ok(())
}
