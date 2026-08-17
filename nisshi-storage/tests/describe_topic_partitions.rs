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

use crate::common::{Error, init_tracing};
use nisshi_sans_io::{
    DescribeTopicPartitionsRequest, ErrorCode, describe_topic_partitions_request::TopicRequest,
};
use nisshi_storage::{DescribeTopicPartitionsService, StorageContainer};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use url::Url;

mod common;

#[tokio::test]
async fn req() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = StorageContainer::builder()
        .cluster_id("nisshi")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://nisshi/")?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(DescribeTopicPartitionsService);

    let topic = "abcba";

    let response = service
        .serve(
            Context::default(),
            DescribeTopicPartitionsRequest::default()
                .topics(Some([TopicRequest::default().name(topic.into())].into())),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(topics[0].error_code)?
    );
    assert_eq!(Some(topic), topics[0].name.as_deref());

    Ok(())
}
