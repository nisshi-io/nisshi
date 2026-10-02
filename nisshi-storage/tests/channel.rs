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

// use rama::{Context, Layer, Service};
// use nisshi_storage::{
//     ChannelRequestLayer, RequestChannelService, RequestReceiver, RequestStorageService, Storage,
//     StorageContainer, bounded_channel,
// };
// use tokio::task::JoinSet;
// use tokio_util::sync::CancellationToken;
// use tracing::debug;
// use url::Url;

// use crate::common::{Error, init_tracing};

// mod common;

// fn server<G>(
//     cancellation: CancellationToken,
//     request_receiver: RequestReceiver,
//     storage: G,
// ) -> impl Future<Output = Result<(), Error>>
// where
//     G: Storage + Clone,
// {
//     async move {
//         let server =
//             ChannelRequestLayer::new(cancellation).into_layer(RequestStorageService::new(storage));

//         server
//             .serve(Context::default(), request_receiver)
//             .await
//             .map_err(Into::into)
//     }
// }

// #[tokio::test]
// async fn channel_storage_layer() -> Result<(), Error> {
//     let _guard = init_tracing()?;

//     let (sender, receiver) = bounded_channel(10);
//     let cancellation = CancellationToken::new();

//     const HOST: &str = "localhost";
//     const PORT: i32 = 9092;
//     const NODE_ID: i32 = 111;
//     const CLUSTER_ID: &str = "nisshi";

//     let storage = StorageContainer::builder()
//         .cluster_id(CLUSTER_ID)
//         .node_id(NODE_ID)
//         .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
//         .storage(Url::parse("memory://nisshi/")?)
//         .build()
//         .await?;

//     let mut join = JoinSet::new();

//     let _server = {
//         let cancellation = cancellation.clone();
//         let storage = storage.clone();
//         join.spawn(async move { server(cancellation, receiver, storage).await })
//     };

//     let client = RequestChannelService::new(sender);

//     let cluster_id = client.cluster_id().await?;
//     assert_eq!(CLUSTER_ID, &cluster_id);

//     cancellation.cancel();

//     let joined = join.join_all().await;
//     debug!(?joined);

//     Ok(())
// }
