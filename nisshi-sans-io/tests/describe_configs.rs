// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use common::init_tracing;
use nisshi_model::{MessageKind, VersionRange};
use nisshi_sans_io::MESSAGE_META;
use std::collections::BTreeMap;

pub mod common;

#[test]
fn response() {
    let _guard = init_tracing().unwrap();

    let message_name = "DescribeConfigsResponse";
    assert!(BTreeMap::from(MESSAGE_META).contains_key(message_name));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get(message_name).unwrap();
    assert_eq!(32, message.api_key);
    assert_eq!(MessageKind::Response, message.message_kind);

    let structures = message.structures();
    let mut keys: Vec<_> = structures.into_iter().map(|(name, _)| name).collect();
    keys.sort();

    assert_eq!(
        vec![
            "DescribeConfigsResourceResult",
            "DescribeConfigsResult",
            "DescribeConfigsSynonym",
        ],
        keys
    );

    assert_eq!(
        "DescribeConfigsResult",
        message
            .field("results")
            .and_then(|field| field.kind.kind_of_sequence())
            .map(|kind_meta| kind_meta.0)
            .unwrap()
    );

    assert_eq!(
        "DescribeConfigsResourceResult",
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|field| field.kind.kind_of_sequence())
            .map(|kind_meta| kind_meta.0)
            .unwrap()
    );

    assert_eq!(
        "bool",
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|configs| configs.field("read_only"))
            .map(|read_only| read_only.kind.0)
            .unwrap()
    );

    assert_eq!(
        VersionRange {
            start: 0,
            end: i16::MAX
        },
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|configs| configs.field("read_only"))
            .map(|read_only| read_only.version)
            .unwrap()
    );

    assert_eq!(
        "bool",
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|configs| configs.field("is_default"))
            .map(|read_only| read_only.kind.0)
            .unwrap()
    );

    assert_eq!(
        VersionRange { start: 0, end: 0 },
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|configs| configs.field("is_default"))
            .map(|read_only| read_only.version)
            .unwrap()
    );
}
