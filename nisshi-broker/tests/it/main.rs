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

mod common;

// `pub` keeps each file's `pub` helpers reachable, as when every file was its own crate.
pub mod auth;
pub mod cg;
pub mod cg_dynamic;
pub mod cg_latency;
pub mod cg_static;
pub mod consumer_group_describe;
pub mod create_topics;
pub mod delete_groups;
pub mod delete_records;
pub mod delete_topics;
pub mod describe_cluster;
pub mod describe_configs;
pub mod describe_groups;
pub mod describe_groups_round_trip;
pub mod describe_topic_partitions;
pub mod fetch;
pub mod find_coordinator;
pub mod get_telemetry_subscriptions;
pub mod group_cache_miss;
pub mod incremental_alter_configs;
pub mod init_producer_id;
pub mod list_groups;
pub mod list_offsets;
pub mod metadata;
pub mod new_cg;
pub mod person;
pub mod pg_init_producer;
pub mod pg_txn;
pub mod policy_compact_delete;
pub mod produce;
pub mod produce_fetch;
pub mod sasl_scram_enforcement;
pub mod storage_describe_cluster;
pub mod storage_describe_configs;
pub mod storage_fetch;
pub mod storage_list_offsets;
pub mod storage_metadata;
pub mod tls;
pub mod topic;
pub mod topic_lifecycle;
pub mod txn;
pub mod update_group_conditional;

// Cargo only builds the modules declared above, so a file in `tests/it/` without a
// `pub mod` line would never compile or run. Fail instead of skipping it silently.
#[test]
fn every_test_file_is_declared() -> std::io::Result<()> {
    let declared = include_str!("main.rs");
    let mut undeclared = Vec::new();

    for entry in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/it"))? {
        let path = entry?.path();
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };

        if path.extension().is_some_and(|extension| extension == "rs")
            && stem != "main"
            && !declared.contains(&format!("\npub mod {stem};\n"))
        {
            undeclared.push(stem.to_owned());
        }
    }

    assert!(
        undeclared.is_empty(),
        "add `pub mod <name>;` to tests/it/main.rs for: {undeclared:?}"
    );
    Ok(())
}
