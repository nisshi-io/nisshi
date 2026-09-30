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

//! Fuzzes [`CreateTopicsService`] (the [`Storage::create_topic`] request
//! handler) end-to-end against the in-memory storage backend, bypassing the
//! wire protocol entirely: fuzzed bytes are turned directly into a
//! [`CreateTopicsRequest`] via `nisshi-sans-io`'s `arbitrary` feature, then
//! run through the same service the broker routes `CreateTopics` requests
//! to. This targets bugs in the storage/business logic (panics, mismatched
//! invariants) rather than the wire decoder, which `fuzz_request_decode`
//! already covers.

#![no_main]

use std::sync::{Arc, LazyLock};

use libfuzzer_sys::fuzz_target;
use nisshi_sans_io::CreateTopicsRequest;
use nisshi_storage::{CreateTopicsService, Error, StorageContainer};
use nisshi_storage_dynostore::MemoryEngineFactory;
use rama::Service as _;
use tokio::runtime::Runtime;
use url::Url;

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().expect("tokio runtime"));

// `nisshi-storage-dynostore`'s `create_topic` runs `for _ in 0..topic.num_partitions { .. }`
// with no upper bound check, so an unclamped `i32::MAX` here would hang a fuzz run rather
// than exercise new logic; clamping keeps every execution fast while still covering the
// interesting boundaries (negative, zero, the `-1` "default" sentinel, and "many").
const MIN_PARTITIONS: i32 = -3;
const MAX_PARTITIONS: i32 = 256;
const MIN_REPLICATION: i16 = -3;
const MAX_REPLICATION: i16 = 16;

fuzz_target!(|request: CreateTopicsRequest| {
    let mut request = request;

    if let Some(topics) = request.topics.as_mut() {
        for topic in topics.iter_mut() {
            topic.num_partitions = topic.num_partitions.clamp(MIN_PARTITIONS, MAX_PARTITIONS);
            topic.replication_factor = topic
                .replication_factor
                .clamp(MIN_REPLICATION, MAX_REPLICATION);
        }
    }

    RUNTIME.block_on(async {
        let mut builder = StorageContainer::builder()
            .cluster_id("fuzz")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092").expect("static url"));
        builder.with_factory(Arc::new(MemoryEngineFactory));

        let storage = builder
            .storage(Url::parse("memory://fuzz").expect("static url"))
            .build()
            .await
            .expect("in-memory storage always builds");

        let service = CreateTopicsService { storage };

        // A `Error::Api` is a well-formed Kafka error response and is
        // expected/uninteresting; any other error means something went
        // wrong internally (not just "client sent a bad request"), so it's
        // treated as a fuzz failure alongside panics.
        match service.serve(request).await {
            Ok(_) | Err(Error::Api(_)) => {}
            Err(error) => panic!("non-API error from CreateTopicsService: {error:?}"),
        }
    });
});
