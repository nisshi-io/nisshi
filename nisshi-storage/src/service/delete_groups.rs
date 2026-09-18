// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use nisshi_sans_io::{ApiKey, DeleteGroupsRequest, DeleteGroupsResponse};
use rama::{Context, Service};
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`DeleteGroupsRequest`] returning [`DeleteGroupsResponse`].
/// ```
/// use rama::{Context, Layer, Service as _, layer::MapStateLayer};
/// use nisshi_sans_io::{DeleteGroupsRequest, ErrorCode};
/// use nisshi_storage::{DeleteGroupsService, Error, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(111)
///     .advertised_listener(Url::parse("tcp://localhost:9092")?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let service = MapStateLayer::new(|_| storage).into_layer(DeleteGroupsService);
///
/// let group_id = "abcba";
///
/// let response = service
///     .serve(
///         Context::default(),
///         DeleteGroupsRequest::default().groups_names(Some([group_id.into()].into())),
///     )
///     .await?;
///
/// let results = response.results.unwrap_or_default();
/// assert_eq!(1, results.len());
/// assert_eq!(group_id, results[0].group_id.as_str());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeleteGroupsService;

impl ApiKey for DeleteGroupsService {
    const KEY: i16 = DeleteGroupsRequest::KEY;
}

impl<G> Service<G, DeleteGroupsRequest> for DeleteGroupsService
where
    G: Storage,
{
    type Response = DeleteGroupsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: DeleteGroupsRequest,
    ) -> Result<Self::Response, Self::Error> {
        ctx.state()
            .delete_groups(req.groups_names.as_deref())
            .await
            .map(Some)
            .map(|results| {
                DeleteGroupsResponse::default()
                    .throttle_time_ms(0)
                    .results(results)
            })
    }
}

#[cfg(all(test, feature = "dynostore"))]
mod tests {
    use nisshi_sans_io::{DeleteGroupsRequest, ErrorCode};
    use rama::{Context, Layer as _, Service, layer::MapStateLayer};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;
    use url::Url;

    use crate::{DeleteGroupsService, Error, StorageContainer};

    fn init_tracing() -> Result<DefaultGuard, Error> {
        use std::{fs::File, sync::Arc, thread};

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(EnvFilter::from_default_env().add_directive(
                    format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
                ))
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

    #[tokio::test]
    async fn delete_non_existent() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = StorageContainer::builder()
            .cluster_id("nisshi")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://localhost:9092")?)
            .storage(Url::parse("memory://nisshi/")?)
            .build()
            .await?;

        let service = MapStateLayer::new(|_| storage).into_layer(DeleteGroupsService);

        let group_id = "abcba";

        let response = service
            .serve(
                Context::default(),
                DeleteGroupsRequest::default().groups_names(Some([group_id.into()].into())),
            )
            .await?;

        let results = response.results.unwrap_or_default();
        assert_eq!(1, results.len());
        assert_eq!(group_id, results[0].group_id.as_str());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);

        Ok(())
    }
}
