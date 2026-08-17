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

use crate::{Error, Result, ScramCredential, Storage};
use bytes::Bytes;
use nisshi_sans_io::{
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, ApiKey, ErrorCode,
    ScramMechanism, alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
};
use rama::{Context, Service};
use rsasl::mechanisms::scram::tools::derive_keys;
use sha2::{Digest, Sha256, Sha512};
use tracing::{debug, instrument};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AlterUserScramCredentialsService;

impl ApiKey for AlterUserScramCredentialsService {
    const KEY: i16 = AlterUserScramCredentialsRequest::KEY;
}

impl<G> Service<G, AlterUserScramCredentialsRequest> for AlterUserScramCredentialsService
where
    G: Storage,
{
    type Response = AlterUserScramCredentialsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: AlterUserScramCredentialsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let mut results = vec![];

        if let Some(deletions) = req.deletions {
            for deletion in deletions {
                let mechanism = ScramMechanism::try_from(deletion.mechanism)?;

                results.push(
                    ctx.state()
                        .delete_user_scram_credential(&deletion.name, mechanism)
                        .await
                        .map_or(
                            AlterUserScramCredentialsResult::default()
                                .user(deletion.name.clone())
                                .error_code(ErrorCode::UnsupportedSaslMechanism.into())
                                .error_message(Some("".into())),
                            |()| {
                                AlterUserScramCredentialsResult::default()
                                    .user(deletion.name.clone())
                                    .error_code(ErrorCode::None.into())
                                    .error_message(Some("".into()))
                            },
                        ),
                );
            }
        }

        if let Some(upsertions) = req.upsertions {
            for upsertion in upsertions {
                let (mechanism, stored_key, server_key) =
                    ScramMechanism::try_from(upsertion.mechanism)
                        .inspect(|mechanism| debug!(?mechanism))
                        .map(|mechanism| {
                            if mechanism == ScramMechanism::Scram256 {
                                let (client_key, server_key) =
                                    derive_keys::<Sha256>(&upsertion.salted_password);

                                (
                                    mechanism,
                                    Bytes::copy_from_slice(&Sha256::digest(client_key)[..]),
                                    Bytes::copy_from_slice(&server_key[..]),
                                )
                            } else {
                                let (client_key, server_key) =
                                    derive_keys::<Sha512>(&upsertion.salted_password);

                                (
                                    mechanism,
                                    Bytes::copy_from_slice(&Sha512::digest(client_key)[..]),
                                    Bytes::copy_from_slice(&server_key[..]),
                                )
                            }
                        })?;

                let credential = ScramCredential {
                    salt: upsertion.salt,
                    iterations: upsertion.iterations,
                    stored_key,
                    server_key,
                };

                results.push(
                    ctx.state()
                        .upsert_user_scram_credential(
                            upsertion.name.as_str(),
                            mechanism,
                            credential,
                        )
                        .await
                        .map_or(
                            AlterUserScramCredentialsResult::default()
                                .user(upsertion.name.clone())
                                .error_code(ErrorCode::UnsupportedSaslMechanism.into())
                                .error_message(Some("".into())),
                            |()| {
                                AlterUserScramCredentialsResult::default()
                                    .user(upsertion.name.clone())
                                    .error_code(ErrorCode::None.into())
                                    .error_message(Some("".into()))
                            },
                        ),
                );
            }
        }

        Ok(AlterUserScramCredentialsResponse::default()
            .throttle_time_ms(0)
            .results(Some(results)))
    }
}
