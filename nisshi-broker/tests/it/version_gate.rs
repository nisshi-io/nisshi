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

//! Drift tests for the capped-route version gate (`SupportedApiVersions`).
//!
//! `capped_range_snapshot` pins both the protocol's own valid range and nisshi's declared
//! `SUPPORTED` range for each capped API, so a future Kafka protocol bump (changing the
//! former) and an accidental cap change (changing the latter, in either direction) both fail
//! this test instead of silently drifting.
//!
//! `full_route_table_has_the_expected_route_count` builds the real route table the broker
//! serves -- the same `storage::services`, `coordinator::services` and `auth::services`
//! composition `nisshi_broker::service::services` uses in production, minus the raw TCP layers
//! -- so a future refactor that drops a route-registration call fails loudly here rather than
//! only at runtime.

use nisshi_broker::{
    Error, Result,
    coordinator::group::administrator::Controller,
    service::{auth, coordinator, storage},
};
use nisshi_sans_io::{
    AddPartitionsToTxnRequest, ApiKey as _, ListOffsetsRequest, ProduceRequest, RootMessageMeta,
};
use nisshi_service::{FrameRouteService, SupportedApiVersions};
use uuid::Uuid;

use crate::common::{self, StorageType};

#[test]
fn capped_range_snapshot() {
    let requests = RootMessageMeta::messages().requests();

    let produce_protocol = requests
        .get(&ProduceRequest::KEY)
        .expect("ProduceRequest protocol metadata")
        .version
        .valid;
    assert_eq!(
        (0, 11),
        (produce_protocol.start, produce_protocol.end),
        "Produce's own protocol range moved; re-check ProduceRequest::SUPPORTED still excludes \
         only the pre-RecordBatch-v2 versions"
    );
    assert_eq!(
        (3, 11),
        (
            *ProduceRequest::SUPPORTED.start(),
            *ProduceRequest::SUPPORTED.end()
        )
    );

    let list_offsets_protocol = requests
        .get(&ListOffsetsRequest::KEY)
        .expect("ListOffsetsRequest protocol metadata")
        .version
        .valid;
    assert_eq!(
        (0, 9),
        (list_offsets_protocol.start, list_offsets_protocol.end),
        "ListOffsets' own protocol range moved; re-check ListOffsetsRequest::SUPPORTED still \
         excludes only v7-9 (unimplemented sentinel timestamps) -- v0 is in scope (SOL-155187 \
         classifies the OldStyleOffsets population gap as a response-shape bug to fix, not a \
         reason to cap the version)"
    );
    assert_eq!(
        (0, 6),
        (
            *ListOffsetsRequest::SUPPORTED.start(),
            *ListOffsetsRequest::SUPPORTED.end()
        )
    );

    let add_partitions_protocol = requests
        .get(&AddPartitionsToTxnRequest::KEY)
        .expect("AddPartitionsToTxnRequest protocol metadata")
        .version
        .valid;
    assert_eq!(
        (0, 5),
        (add_partitions_protocol.start, add_partitions_protocol.end),
        "AddPartitionsToTxn's own protocol range moved; re-check \
         AddPartitionsToTxnRequest::SUPPORTED still excludes only the multi-transaction (v4+) \
         shape"
    );
    assert_eq!(
        (0, 3),
        (
            *AddPartitionsToTxnRequest::SUPPORTED.start(),
            *AddPartitionsToTxnRequest::SUPPORTED.end()
        )
    );
}

#[tokio::test]
async fn full_route_table_has_the_expected_route_count() -> Result<()> {
    let cluster_id = Uuid::new_v4().to_string();

    let storage = common::storage_container(
        StorageType::InMemory,
        cluster_id,
        111,
        "tcp://localhost:9092".parse()?,
        None,
    )
    .await?;

    let coordinator = Controller::with_storage(storage.clone())?;

    let with_storage_routes = storage::services(FrameRouteService::<Error>::builder(), storage)?;
    // storage.rs no longer registers GetTelemetrySubscriptions (SOL-155187 removed it), so this
    // is 26, not the 27 it was while that route existed.
    assert_eq!(26, with_storage_routes.len());

    let with_coordinator_routes = coordinator::services(with_storage_routes, coordinator)?;
    assert_eq!(26 + 6, with_coordinator_routes.len());

    let with_auth_routes = auth::services(with_coordinator_routes)?;
    assert_eq!(26 + 6 + 2, with_auth_routes.len());

    let route_table = with_auth_routes.build().map_err(Error::from)?;
    // `build` adds the `ApiVersions` route itself.
    assert_eq!(26 + 6 + 2 + 1, route_table.len());

    Ok(())
}
