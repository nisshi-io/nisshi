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
use nisshi_sans_io::{DescribeClusterRequest, EndpointType};
use nisshi_storage::{DescribeClusterService, StorageContainer};
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

    let service = MapStateLayer::new(|_| storage).into_layer(DescribeClusterService);

    let response = service
        .serve(
            Context::default(),
            DescribeClusterRequest::default()
                .endpoint_type(Some(EndpointType::Broker.into()))
                .include_cluster_authorized_operations(false),
        )
        .await?;

    let brokers = response.brokers.unwrap_or_default();
    assert_eq!(1, brokers.len());
    assert_eq!(NODE_ID, brokers[0].broker_id);
    assert_eq!(HOST, brokers[0].host.as_str());
    assert_eq!(PORT, brokers[0].port);

    Ok(())
}
