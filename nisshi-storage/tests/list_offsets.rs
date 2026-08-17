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
    ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
};
use nisshi_storage::{ListOffsetsService, StorageContainer};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use url::Url;

mod common;

#[tokio::test]
async fn req() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id("nisshi")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(Url::parse("memory://nisshi/")?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(ListOffsetsService);

    let topic = "abcba";

    let response = service
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .replica_id(NODE_ID)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic.into())
                        .partitions(Some(
                            [ListOffsetsPartition::default()
                                .current_leader_epoch(Some(-1))
                                .max_num_offsets(Some(3))
                                .partition_index(0)
                                .timestamp(ListOffset::Earliest.try_into()?)]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic, topics[0].name);

    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(0, partitions[0].partition_index);
    assert!(partitions[0].old_style_offsets.is_none());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(Some(-1), partitions[0].timestamp);
    assert_eq!(Some(0), partitions[0].offset);
    assert_eq!(Some(0), partitions[0].leader_epoch);

    Ok(())
}
