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

//! Harness for the smoke suite: run the real Kafka CLI tools against a
//! broker on a chosen storage engine, the way a user would.
//!
//! Configuration comes from the environment, which `just smoke <engine>` sets up:
//!
//! - `NISSHI_SMOKE_BOOTSTRAP`: the shared broker's address
//! - `NISSHI_SMOKE_STORAGE`: the storage engine URL
//! - `NISSHI_SMOKE_BIN` or `NISSHI_SMOKE_IMAGE`: what [`Broker::isolated`] launches
//! - `NISSHI_SMOKE_KAFKA`: the running container with the Kafka CLI tools
//! - `NISSHI_SMOKE_RUN`: the run's id, which labels every container and
//!   volume the suite creates, so a run removes only its own

use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

mod broker;
mod kafka_cli;
mod record;
mod timed_command;

pub use broker::{Broker, LaunchOptions, free_port};
pub use kafka_cli::{KafkaCli, Output};
pub use record::Record;

/// A name no other test, process or earlier run uses, for topics, groups,
/// clusters and containers, so tests can share one broker and one database.
pub fn unique_name(prefix: &str) -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos())
        .unwrap_or_default();

    format!(
        "{prefix}-{}-{}-{nanos}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// The `docker --label` on every container and volume this run creates.
fn label() -> String {
    format!(
        "--label=nisshi-smoke={}",
        env("NISSHI_SMOKE_RUN").unwrap_or_else(|| "local".to_owned())
    )
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}
