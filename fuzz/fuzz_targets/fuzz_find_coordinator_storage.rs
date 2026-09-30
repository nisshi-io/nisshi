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

//! Fuzzes [`FindCoordinatorService`] end-to-end against the in-memory storage backend,
//! bypassing the wire protocol entirely: fuzzed bytes are turned directly
//! into a [`FindCoordinatorRequest`] via `nisshi-sans-io`'s `arbitrary` feature, then run
//! through the same service the broker routes `FindCoordinator` requests to. This
//! targets bugs in the storage/business logic (panics, mismatched
//! invariants) rather than the wire decoder, which `fuzz_request_decode`
//! already covers.
//!
//! Storage starts empty for every execution (no topics/groups/ACLs are
//! pre-seeded), so this mainly exercises not-found/empty-state handling;
//! seeding realistic state first is a possible follow-up.

#![no_main]

use std::sync::{Arc, LazyLock};

use libfuzzer_sys::fuzz_target;
use nisshi_sans_io::FindCoordinatorRequest;
use nisshi_storage::{Error, FindCoordinatorService, StorageContainer};
use nisshi_storage_dynostore::MemoryEngineFactory;
use rama::Service as _;
use tokio::runtime::Runtime;
use url::Url;

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().expect("tokio runtime"));

fuzz_target!(|request: FindCoordinatorRequest| {
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

        let service = FindCoordinatorService { storage };

        // A `Error::Api` is a well-formed Kafka error response and is
        // expected/uninteresting; any other error means something went
        // wrong internally (not just "client sent a bad request"), so it's
        // treated as a fuzz failure alongside panics.
        match service.serve(request).await {
            Ok(_) | Err(Error::Api(_)) => {}
            Err(error) => panic!("non-API error from FindCoordinatorService: {error:?}"),
        }
    });
});
