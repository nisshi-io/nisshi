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

//! SOL-155184: the factory-level AWS credential pre-check added to
//! [`S3OptimisticConcurrencyEngineFactory::build`] has no credential source
//! configured, it should fail fast with [`Error::NoCredentials`] rather than
//! starting successfully and failing later on the first real request.
//!
//! A real integration test against local MinIO (both credential tiers, bad
//! keys, a missing bucket, a wrong endpoint, zero credentials) was run by
//! hand and is not reproduced here. Driving `object_store`'s real IMDS
//! lookup in CI is what this test avoids: an unreachable `169.254.169.254`
//! behaves differently depending on the host (some environments, notably
//! Azure, answer that address with something other than "unreachable"),
//! so a test that relies on IMDS being absent is not reliably deterministic.
//!
//! `object_store` instead honors an `AWS_METADATA_ENDPOINT` config key
//! (`object_store::aws::builder`), which redirects the IMDS lookup
//! itself to an arbitrary URL. Pointing it at a port nothing listens on
//! gives a deterministic, fast (connection-refused, not a timeout) failure
//! with no dependency on the host's real network environment.

use nisshi_storage::{Error, StorageFactory as _, StorageFactoryConfiguration};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::factory::S3OptimisticConcurrencyEngineFactory;

/// An address nothing listens on: connections to it are refused immediately,
/// so this test fails fast instead of waiting out a real IMDS timeout.
const UNREACHABLE_METADATA_ENDPOINT: &str = "http://127.0.0.1:1";

#[tokio::test]
async fn build_fails_fast_with_no_resolvable_credentials() {
    let configuration = StorageFactoryConfiguration {
        node_id: 111,
        cluster: "nisshi".to_owned(),
        advertised_listener: Url::parse("tcp://localhost:9092").expect("url"),
        storage: Url::parse("s3://nisshi-test-bucket").expect("url"),
        schema_registry: None,
        lake_house: None,
        cancellation: CancellationToken::new(),
    };

    // No static keys, no web identity, no task role: the only credential
    // source left is the (redirected) metadata endpoint, and nothing is
    // listening there, so resolution must fail.
    let result = temp_env::async_with_vars(
        [
            ("AWS_ACCESS_KEY_ID", None::<&str>),
            ("AWS_SECRET_ACCESS_KEY", None::<&str>),
            ("AWS_SESSION_TOKEN", None::<&str>),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", None::<&str>),
            ("AWS_ROLE_ARN", None::<&str>),
            ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", None::<&str>),
            ("AWS_CONTAINER_CREDENTIALS_FULL_URI", None::<&str>),
            ("AWS_METADATA_ENDPOINT", Some(UNREACHABLE_METADATA_ENDPOINT)),
        ],
        S3OptimisticConcurrencyEngineFactory.build(configuration),
    )
    .await;

    match result {
        Err(Error::NoCredentials(_)) => {}
        Err(other) => panic!("expected Error::NoCredentials, got a different error: {other}"),
        Ok(_) => panic!("expected Error::NoCredentials, but build() succeeded"),
    }
}
