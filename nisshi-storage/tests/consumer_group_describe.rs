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
use nisshi_sans_io::{ConsumerGroupDescribeRequest, ErrorCode};
use nisshi_storage::{ConsumerGroupDescribeService, StorageContainer};
use rama::{Context, Layer, Service as _, layer::MapStateLayer};
use tracing::debug;
use url::Url;

mod common;

#[tokio::test]
async fn describe_non_existent_group() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = StorageContainer::builder()
        .cluster_id("nisshi")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://nisshi/")?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(ConsumerGroupDescribeService);

    let group_id = "abc";

    let response = service
        .serve(
            Context::default(),
            ConsumerGroupDescribeRequest::default()
                .group_ids(Some([group_id.into()].into()))
                .include_authorized_operations(false),
        )
        .await
        .inspect(|response| debug!(?response))?;

    let groups = response.groups.unwrap_or_default();
    assert_eq!(1, groups.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
    assert_eq!(group_id, groups[0].group_id.as_str());
    assert_eq!("Empty", groups[0].group_state.as_str());

    Ok(())
}
