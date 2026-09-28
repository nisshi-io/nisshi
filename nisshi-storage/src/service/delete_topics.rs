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
    ApiKey, DeleteTopicsRequest, DeleteTopicsResponse, RequestInput,
    delete_topics_response::DeletableTopicResult,
};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`DeleteTopicsRequest`] returning [`DeleteTopicsResponse`].
/// ```no_run
/// use rama::Service as _;
/// use nisshi_sans_io::{DeleteTopicsRequest, DeleteTopicsResponse,
///     delete_topics_response::DeletableTopicResult, ErrorCode};
/// use nisshi_storage::{DeleteTopicsService, Error, StorageContainer};
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
/// let service = DeleteTopicsService { storage };
///
/// let topic = "pqr";
///
/// let error_code = ErrorCode::UnknownTopicOrPartition;
///
/// assert_eq!(
///     DeleteTopicsResponse::default()
///         .throttle_time_ms(Some(0))
///         .responses(Some(vec![
///             DeletableTopicResult::default()
///                 .error_code(error_code.into())
///                 .error_message(Some(error_code.to_string()))
///                 .name(Some(topic.into())),
///         ])),
///     service
///         .serve(
///             DeleteTopicsRequest::default().topic_names(Some(vec![topic.into()]))
///         )
///         .await?
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct DeleteTopicsService<G> {
    pub storage: G,
}

impl<G> ApiKey for DeleteTopicsService<G> {
    const KEY: i16 = DeleteTopicsRequest::KEY;
}

impl<G, I> Service<I> for DeleteTopicsService<G>
where
    G: Storage,
    I: Into<RequestInput<DeleteTopicsRequest>> + Send + 'static,
{
    type Output = DeleteTopicsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let mut responses = vec![];

        for topic in input.request.topics.unwrap_or_default() {
            let error_code = self.storage.delete_topic(&topic.clone().into()).await?;
            responses.push(
                DeletableTopicResult::default()
                    .name(topic.name.clone())
                    .topic_id(Some(topic.topic_id))
                    .error_code(i16::from(error_code))
                    .error_message(Some(error_code.to_string())),
            );
        }

        for topic in input.request.topic_names.unwrap_or_default() {
            let error_code = self.storage.delete_topic(&topic.clone().into()).await?;

            responses.push(
                DeletableTopicResult::default()
                    .name(Some(topic))
                    .topic_id(None)
                    .error_code(i16::from(error_code))
                    .error_message(Some(error_code.to_string())),
            );
        }

        Ok(DeleteTopicsResponse::default()
            .throttle_time_ms(Some(0))
            .responses(Some(responses)))
    }
}
