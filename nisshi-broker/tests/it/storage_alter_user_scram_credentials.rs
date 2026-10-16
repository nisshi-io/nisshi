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

use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    AlterUserScramCredentialsRequest, ErrorCode, RequestInput, ScramMechanism,
    alter_user_scram_credentials_request::{ScramCredentialDeletion, ScramCredentialUpsertion},
};
use nisshi_storage::{AlterUserScramCredentialsService, ArcDynStorage, Storage};
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

use crate::common::{alphanumeric_string, init_tracing};

const SCRAM_SHA_256: i8 = 1;
const UNKNOWN_MECHANISM: i8 = 111;

fn upsertion(name: &str, mechanism: i8) -> ScramCredentialUpsertion {
    ScramCredentialUpsertion::default()
        .name(name.into())
        .mechanism(mechanism)
        .iterations(4096)
        .salt(Bytes::from_static(b"salt"))
        .salted_password(Bytes::from_static(b"salted password"))
}

/// An unrecognized mechanism is answered `UNSUPPORTED_SASL_MECHANISM` on that item alone:
/// the valid upsertion beside it is still stored.
///
/// The `memory://` engine accepts SCRAM upserts without keeping them, so `persists` is false
/// there and only the per-item answers are checked.
async fn bad_and_good_mechanism(storage: ArcDynStorage, persists: bool) -> Result<()> {
    let bad = alphanumeric_string(15);
    let good = alphanumeric_string(15);

    let response = AlterUserScramCredentialsService {
        storage: storage.clone(),
    }
    .serve(RequestInput {
        request: AlterUserScramCredentialsRequest::default()
            .deletions(Some([].into()))
            .upsertions(Some(
                [
                    upsertion(&bad, UNKNOWN_MECHANISM),
                    upsertion(&good, SCRAM_SHA_256),
                ]
                .into(),
            )),
        extensions: Extensions::default(),
    })
    .await?;

    let results = response.results.as_deref().unwrap_or_default();
    assert_eq!(2, results.len());

    assert_eq!(bad, results[0].user);
    assert_eq!(
        ErrorCode::UnsupportedSaslMechanism,
        ErrorCode::try_from(results[0].error_code)?
    );

    assert_eq!(good, results[1].user);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[1].error_code)?);

    if persists {
        assert!(
            storage
                .user_scram_credential(&good, ScramMechanism::Scram256)
                .await?
                .is_some()
        );
    }

    Ok(())
}

/// A deletion with an unrecognized mechanism is answered `UNSUPPORTED_SASL_MECHANISM`, with
/// Kafka's message, on that item alone: the valid deletion beside it is still answered.
async fn bad_and_good_deletion(storage: ArcDynStorage) -> Result<()> {
    let bad = alphanumeric_string(15);
    let good = alphanumeric_string(15);

    let service = AlterUserScramCredentialsService {
        storage: storage.clone(),
    };

    _ = service
        .serve(RequestInput {
            request: AlterUserScramCredentialsRequest::default()
                .deletions(Some([].into()))
                .upsertions(Some([upsertion(&good, SCRAM_SHA_256)].into())),
            extensions: Extensions::default(),
        })
        .await?;

    let response = service
        .serve(RequestInput {
            request: AlterUserScramCredentialsRequest::default()
                .upsertions(Some([].into()))
                .deletions(Some(
                    [
                        ScramCredentialDeletion::default()
                            .name(bad.clone())
                            .mechanism(UNKNOWN_MECHANISM),
                        ScramCredentialDeletion::default()
                            .name(good.clone())
                            .mechanism(SCRAM_SHA_256),
                    ]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let results = response.results.as_deref().unwrap_or_default();
    assert_eq!(2, results.len());

    assert_eq!(bad, results[0].user);
    assert_eq!(
        ErrorCode::UnsupportedSaslMechanism,
        ErrorCode::try_from(results[0].error_code)?
    );
    assert_eq!(
        Some("Unknown SCRAM mechanism"),
        results[0].error_message.as_deref()
    );

    assert_eq!(good, results[1].user);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[1].error_code)?);

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage:path, $persists:literal) => {
        #[cfg(feature = $feature)]
        mod $name {
            use super::*;

            #[tokio::test]
            async fn bad_and_good_mechanism() -> Result<()> {
                let _guard = init_tracing()?;

                let storage = $storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?;

                super::bad_and_good_mechanism(storage, $persists).await
            }

            #[tokio::test]
            async fn bad_and_good_deletion() -> Result<()> {
                let _guard = init_tracing()?;

                let storage = $storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?;

                super::bad_and_good_deletion(storage).await
            }
        }
    };
}

backend!(in_memory, "dynostore", crate::common::memory_storage, false);
backend!(lite, "libsql", crate::common::lite_storage, true);
backend!(slatedb, "slatedb", crate::common::slate_storage, true);
backend!(pg, "postgres", crate::common::postgres_storage, true);
