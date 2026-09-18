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

//! Regression test for `describe_groups` on the storage backends that
//! persist `GroupDetail` as JSON text.
//!
//! Both the libsql (`lite.rs`) and turso (`limbo.rs`) backends used to
//! call `serde_json::Value::from(&str)` on the JSON column, which wraps
//! the raw JSON text as a `Value::String` instead of parsing it. The
//! subsequent `serde_json::from_value::<GroupDetail>` then always
//! failed with `invalid type: string "...", expected struct
//! GroupDetail`. The native-JSON Postgres backend was not affected.

mod common;

use std::{slice::from_ref, sync::Arc, thread};

use nisshi_storage::{BrokerRegistrationRequest, GroupDetail, Storage, StorageContainer};
use rand::{prelude::*, rng};
use tracing::debug;
use url::Url;
use uuid::Uuid;

use crate::common::{Error, init_tracing};

fn storage_url(scheme: &str) -> Result<Url, Error> {
    thread::current()
        .name()
        .ok_or_else(|| Error::Message("unnamed thread".into()))
        .map(|name| {
            format!(
                "{scheme}://../logs/{}/{}::{name}.db",
                env!("CARGO_PKG_NAME"),
                env!("CARGO_CRATE_NAME"),
            )
        })
        .and_then(|url| Url::parse(&url).map_err(Into::into))
}

async fn build_storage(
    scheme: &str,
    cluster: &str,
    node: i32,
) -> Result<Arc<Box<dyn Storage>>, Error> {
    StorageContainer::builder()
        .cluster_id(cluster)
        .node_id(node)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
        .storage(storage_url(scheme)?)
        .build()
        .await
        .map_err(Into::into)
}

async fn round_trip(scheme: &str) -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7().to_string();
    let node_id = rng().random_range(0..i32::MAX);
    let group_id = format!("test-group-{}", Uuid::now_v7());

    let storage = build_storage(scheme, &cluster_id, node_id).await?;

    storage
        .register_broker(BrokerRegistrationRequest {
            broker_id: node_id,
            cluster_id: cluster_id.clone(),
            incarnation_id: Uuid::new_v4(),
            rack: None,
        })
        .await?;

    let detail = GroupDetail::default();

    let _version = storage
        .update_group(&group_id, detail.clone(), None)
        .await
        .inspect(|version| debug!(?version))
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let described = storage
        .describe_groups(Some(from_ref(&group_id)), false)
        .await?;

    assert_eq!(1, described.len(), "describe_groups must return one entry");

    Ok(())
}

#[cfg(feature = "libsql")]
#[tokio::test]
async fn libsql_describe_groups_round_trip() -> Result<(), Error> {
    round_trip("sqlite").await
}

// The turso end-to-end harness is unstable (existing turso tests in
// `nisshi-broker` are all `#[ignore]`); run this by hand with
// `cargo test --features turso -- --ignored` once the harness lands.
#[cfg(feature = "turso")]
#[ignore]
#[tokio::test]
async fn turso_describe_groups_round_trip() -> Result<(), Error> {
    round_trip("turso").await
}
