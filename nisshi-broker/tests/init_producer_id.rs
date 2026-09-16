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

use crate::common::{init_tracing, lite_storage, memory_storage, postgres_storage, slate_storage};
use nisshi_broker::Result;
use nisshi_sans_io::{ErrorCode, InitProducerIdRequest, RequestInput};
use nisshi_storage::{ArcDynStorage, InitProducerIdService, Storage};
use rama::{Service, extensions::Extensions};
use rand::{RngExt as _, rng};
use uuid::Uuid;

mod common;

async fn no_txn_init_producer_id(storage: impl Storage + Clone) -> Result<()> {
    let service = InitProducerIdService {
        storage: storage.clone(),
    };

    let transactional_id = None;
    let transaction_timeout_ms = 0;
    let producer_id = Some(-1);
    let producer_epoch = Some(-1);

    let extensions = Extensions::default();

    let r0 = service
        .serve(RequestInput {
            request: InitProducerIdRequest::default()
                .transactional_id(transactional_id.clone())
                .transaction_timeout_ms(transaction_timeout_ms)
                .producer_id(producer_id)
                .producer_epoch(producer_epoch),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(r0.error_code, i16::from(ErrorCode::None));
    assert_eq!(r0.producer_epoch, 0);
    assert!(r0.producer_id > 0);

    let r1 = service
        .serve(RequestInput {
            request: InitProducerIdRequest::default()
                .transactional_id(transactional_id.clone())
                .transaction_timeout_ms(transaction_timeout_ms)
                .producer_id(producer_id)
                .producer_epoch(producer_epoch),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(r1.error_code, i16::from(ErrorCode::None));
    assert_eq!(r1.producer_epoch, 0);
    assert!(r1.producer_id > r0.producer_id);

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        memory_storage(cluster, node).await
    }

    #[tokio::test]
    async fn no_txn_init_producer_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::no_txn_init_producer_id(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        lite_storage(cluster, node).await
    }

    #[tokio::test]
    async fn no_txn_init_producer_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::no_txn_init_producer_id(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        slate_storage(cluster, node).await
    }

    #[tokio::test]
    async fn no_txn_init_producer_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::no_txn_init_producer_id(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        postgres_storage(cluster, node).await
    }

    #[tokio::test]
    async fn no_txn_init_producer_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::no_txn_init_producer_id(storage).await?;

        Ok(())
    }
}
