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

use crate::{Authentication, Error, Stage};
use nisshi_sans_io::{ApiKey, ErrorCode, SaslHandshakeRequest, SaslHandshakeResponse};
use rama::{Context, Service};
use rsasl::prelude::Mechname;
use tracing::{debug, instrument};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SaslHandshakeService;

impl ApiKey for SaslHandshakeService {
    const KEY: i16 = SaslHandshakeRequest::KEY;
}

impl<S> Service<S, SaslHandshakeRequest> for SaslHandshakeService
where
    S: Send + Sync + 'static,
{
    type Response = SaslHandshakeResponse;
    type Error = Error;

    #[instrument(skip(self, ctx), ret)]
    async fn serve(
        &self,
        ctx: Context<S>,
        req: SaslHandshakeRequest,
    ) -> Result<Self::Response, Self::Error> {
        if let Some(authentication) = ctx.get::<Authentication>().cloned() {
            authentication.stage
            .lock()
            .map_err(Into::into)
            .and_then(|mut guard| {
                // Re-authentication (KIP-368): a Java kafka-clients
                // connection periodically issues another SaslHandshake
                // on the same TCP socket. The previous handshake left
                // the Stage in `Session`/`Finished`, so a stale
                // `take()` would fall into the else branch and reject
                // a perfectly valid mechanism with
                // `UnsupportedSaslMechanism`. Always start with a
                // fresh `SASLServer` built from the stored config.
                if guard.as_ref().is_none_or(|guard|!matches!(guard, Stage::Server(_))) {
                    debug!(?guard);
                    _ = guard.replace(authentication.fresh_server());
                }

                if let Some(Stage::Server(server)) = guard.take() && let Ok(mechanism) = Mechname::parse(req.mechanism.as_bytes()) {
                    debug!(available = ?server.get_available().into_iter().map(|mechanism|mechanism.mechanism.as_str()).collect::<Vec<_>>());

                    server
                        .start_suggested(mechanism)
                        .inspect_err(|err| debug!(?err, ?mechanism))
                        .map_err(Into::into)
                        .map(|session| {
                            let mechanisms = [session.get_mechname().to_string()];

                            _ = guard.replace(Stage::Session(session));

                            SaslHandshakeResponse::default()
                                .error_code(ErrorCode::None.into())
                                .mechanisms(Some(mechanisms.into()))
                        })
                } else {
                    Ok(SaslHandshakeResponse::default()
                        .error_code(ErrorCode::UnsupportedSaslMechanism.into())
                        .mechanisms(Some([req.mechanism].into())))
                }
            })
        } else {
            Ok(SaslHandshakeResponse::default()
                .error_code(ErrorCode::UnsupportedSaslMechanism.into())
                .mechanisms(Some([req.mechanism].into())))
        }
    }
}
