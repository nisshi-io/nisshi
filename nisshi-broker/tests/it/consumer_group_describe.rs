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

use nisshi_broker::Result;
use nisshi_sans_io::{ConsumerGroupDescribeRequest, ErrorCode, RequestInput};
use nisshi_storage::{ConsumerGroupDescribeService, Storage};
use rama::{Service as _, extensions::Extensions};
use tracing::debug;
use url::Url;

use crate::common::{self, register_broker};

pub async fn describe_non_existent_group<C, G>(
    cluster_id: C,
    broker_id: i32,
    advertised_listener: Url,
    storage: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    debug!(broker_id, %advertised_listener);
    register_broker(cluster_id, broker_id, &storage).await?;

    let service = ConsumerGroupDescribeService { storage };

    let group_id = "abc";

    let response = service
        .serve(RequestInput {
            request: ConsumerGroupDescribeRequest::default()
                .group_ids(Some([group_id.into()].into()))
                .include_authorized_operations(false),
            extensions: Extensions::default(),
        })
        .await
        .inspect(|response| debug!(?response))?;

    let groups = response.groups.unwrap_or_default();
    assert_eq!(1, groups.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
    assert_eq!(group_id, groups[0].group_id.as_str());
    assert_eq!("Empty", groups[0].group_state.as_str());

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use nisshi_storage::ArcDynStorage;

    use crate::common::{StorageType, init_tracing};
    use rand::{prelude::*, rng};
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
        advertised_listener: Url,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            advertised_listener,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn describe_non_existent_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = rng().random_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::describe_non_existent_group(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener).await?,
        )
        .await
    }
}
