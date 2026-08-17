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

use nisshi_sans_io::{
    ApiKey, ErrorCode, MetadataRequest, MetadataResponse, create_topics_request::CreatableTopic,
};
use rama::{Context, Service};
use tracing::{debug, error, instrument};

use crate::{Error, Result, Storage, TopicId};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`MetadataRequest`] returning [`MetadataRequest`].
/// ```
/// use rama::{Context, Layer as _, Service, layer::MapStateLayer};
/// use nisshi_sans_io::MetadataRequest;
/// use nisshi_storage::{Error, MetadataService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
/// const NODE_ID: i32 = 111;
///
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let service = MapStateLayer::new(|_| storage).into_layer(MetadataService);
///
/// let response = service
///     .serve(
///         Context::default(),
///         MetadataRequest::default()
///             .allow_auto_topic_creation(Some(false))
///             .include_cluster_authorized_operations(Some(false))
///             .include_topic_authorized_operations(Some(false))
///             .topics(Some([].into())),
///     )
///     .await?;
///
/// let brokers = response.brokers.as_deref().unwrap_or_default();
/// assert_eq!(1, brokers.len());
/// assert_eq!(HOST, brokers[0].host);
/// assert_eq!(PORT, brokers[0].port);
/// assert_eq!(NODE_ID, brokers[0].node_id);
/// assert!(brokers[0].rack.is_none());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetadataService;

/// Defaults for topics created via auto creation: four partitions is the
/// `num.partitions` that the Apache Kafka client test suites assume.
const AUTO_CREATE_NUM_PARTITIONS: i32 = 4;
const AUTO_CREATE_REPLICATION_FACTOR: i16 = 1;

/// The Apache Kafka topic name rules: 1 to 249 characters from
/// `[a-zA-Z0-9._-]`, and not "." or "..".
fn is_valid_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name.len() <= 249
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

impl ApiKey for MetadataService {
    const KEY: i16 = MetadataRequest::KEY;
}

impl<G> Service<G, MetadataRequest> for MetadataService
where
    G: Storage,
{
    type Response = MetadataResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: MetadataRequest,
    ) -> Result<Self::Response, Self::Error> {
        let topics = req
            .topics
            .map(|topics| topics.iter().map(TopicId::from).collect::<Vec<_>>());

        let mut response = ctx
            .state()
            .metadata(topics.as_deref())
            .await
            .inspect_err(|err| error!(?err))?;

        // versions prior to 4 do not have allow_auto_topic_creation,
        // and behave as if it were true
        if req.allow_auto_topic_creation.unwrap_or(true) {
            let unknown = response
                .topics()
                .iter()
                .filter(|topic| topic.error_code == i16::from(ErrorCode::UnknownTopicOrPartition))
                .filter_map(|topic| topic.name.clone())
                .filter(|name| is_valid_topic_name(name))
                .collect::<Vec<_>>();

            let mut created = false;

            for name in unknown {
                match ctx
                    .state()
                    .create_topic(
                        CreatableTopic::default()
                            .name(name)
                            .num_partitions(AUTO_CREATE_NUM_PARTITIONS)
                            .replication_factor(AUTO_CREATE_REPLICATION_FACTOR)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                        false,
                    )
                    .await
                {
                    Ok(topic_id) => {
                        debug!(?topic_id);
                        created = true;
                    }

                    // concurrent metadata requests can race to create
                    Err(Error::Api(ErrorCode::TopicAlreadyExists)) => created = true,

                    Err(err) => error!(?err),
                }
            }

            if created {
                response = ctx
                    .state()
                    .metadata(topics.as_deref())
                    .await
                    .inspect_err(|err| error!(?err))?;
            }
        }
        let brokers = Some(response.brokers().to_owned());
        let cluster_id = response.cluster().map(|s| s.into());
        let controller_id = response.controller();
        let topics = Some(
            response
                .topics()
                .iter()
                .map(|topic| {
                    if topic.error_code == i16::from(ErrorCode::UnknownTopicOrPartition)
                        && topic
                            .name
                            .as_deref()
                            .is_some_and(|name| !is_valid_topic_name(name))
                    {
                        topic
                            .clone()
                            .error_code(ErrorCode::InvalidTopicException.into())
                    } else {
                        topic.clone()
                    }
                })
                .collect::<Vec<_>>(),
        );
        let cluster_authorized_operations = Some(-1);

        let throttle_time_ms = Some(0);

        Ok(MetadataResponse::default()
            .throttle_time_ms(throttle_time_ms)
            .brokers(brokers)
            .cluster_id(cluster_id)
            .controller_id(controller_id)
            .topics(topics)
            .cluster_authorized_operations(cluster_authorized_operations))
    }
}
