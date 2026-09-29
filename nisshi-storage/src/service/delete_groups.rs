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

use nisshi_sans_io::{
    ApiKey, DeleteGroupsRequest, DeleteGroupsResponse, ErrorCode, RequestInput,
    delete_groups_response::DeletableGroupResult,
};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`DeleteGroupsRequest`] returning [`DeleteGroupsResponse`].
/// ```no_run
/// use rama::Service as _;
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
/// let service = DeleteGroupsService { storage };
///
/// let group_id = "abcba";
///
/// let response = service
///     .serve(
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
#[derive(Clone, Debug)]
pub struct DeleteGroupsService<G> {
    pub storage: G,
}

impl<G> ApiKey for DeleteGroupsService<G> {
    const KEY: i16 = DeleteGroupsRequest::KEY;
}

impl<G, I> Service<I> for DeleteGroupsService<G>
where
    G: Storage,
    I: Into<RequestInput<DeleteGroupsRequest>> + Send + 'static,
{
    type Output = DeleteGroupsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let results = match input.request.groups_names {
            None => self.storage.delete_groups(None).await?,

            Some(group_ids) => {
                // An empty group id can't name a single group in every
                // backend's key layout (on dynostore it widens the delete to
                // every group), and Kafka has deprecated empty group ids
                // (KIP-289). Reject it without calling storage. Clients match
                // results by group_id, so order doesn't matter.
                let (invalid, non_empty): (Vec<String>, Vec<String>) = group_ids
                    .into_iter()
                    .partition(|group_id| group_id.is_empty());

                let mut results: Vec<DeletableGroupResult> = invalid
                    .into_iter()
                    .map(|group_id| {
                        DeletableGroupResult::default()
                            .group_id(group_id)
                            .error_code(ErrorCode::InvalidGroupId.into())
                    })
                    .collect();

                if !non_empty.is_empty() {
                    results.extend(self.storage.delete_groups(Some(&non_empty)).await?);
                }

                results
            }
        };

        Ok(DeleteGroupsResponse::default()
            .throttle_time_ms(0)
            .results(Some(results)))
    }
}
