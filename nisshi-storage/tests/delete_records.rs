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
    DeleteRecordsRequest,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
};
use nisshi_storage::{DeleteRecordsService, StorageContainer};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use tracing::debug;
use url::Url;

mod common;

#[ignore]
#[tokio::test]
async fn delete_non_existent_records() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = StorageContainer::builder()
        .cluster_id("nisshi")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://nisshi/")?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(DeleteRecordsService);

    let topic = "abcba";

    let response = service
        .serve(
            Context::default(),
            DeleteRecordsRequest::default().topics(Some(
                [DeleteRecordsTopic::default()
                    .name(topic.into())
                    .partitions(Some(
                        [DeleteRecordsPartition::default()
                            .offset(32123)
                            .partition_index(6)]
                        .into(),
                    ))]
                .into(),
            )),
        )
        .await
        .inspect(|response| debug!(?response))?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());

    Ok(())
}
