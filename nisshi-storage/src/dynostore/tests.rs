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

use dotenv::dotenv;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::File, sync::Arc, thread};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

use crate::{Error, Result};

mod latency;

pub(crate) fn init_tracing() -> Result<DefaultGuard, Error> {
    _ = dotenv().ok();

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                            .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

#[test]
fn range_check() {
    let map = BTreeMap::from([(3, "a"), (5, "b"), (8, "c")]);

    assert_eq!(Some((&3, &"a")), map.range(2..).next());
    assert_eq!(Some((&5, &"b")), map.range(4..).next());
    assert_eq!(None, map.range(9..).next());
}

#[test]
fn schema_change() -> Result<()> {
    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X0 {
        low: Option<i64>,
        high: Option<i64>,
    }

    let low = Some(6);
    let high = Some(66);

    let x0 = X0 { low, high };

    let encoded = serde_json::to_string(&x0)?;

    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X1 {
        low: Option<i64>,
        high: Option<i64>,
        timestamps: Option<BTreeMap<i64, i64>>,
    }

    let x1: X1 = serde_json::from_str(&encoded[..])?;

    assert_eq!(low, x1.low);
    assert_eq!(high, x1.high);
    assert!(x1.timestamps.is_none());

    Ok(())
}
