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

//! A produce checks an idempotent batch's sequence before it writes the record, and advances the
//! sequence after. A retry of the same batch can pass the check while the first attempt waits
//! on its record write. Whichever attempt advances second must find the other's batch and
//! advance nothing, or the producer's next batch would be out of order.

use std::{
    assert_matches,
    fmt::{self, Debug, Display},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use nisshi_sans_io::{
    ErrorCode,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{Error, Storage as _, Topition};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio::sync::{Notify, oneshot};

use crate::dynostore::{DynoStore, tests::init_tracing};

/// Holds the first record write until it is released, and passes everything else through.
#[derive(Clone)]
struct HeldRecordWriteStore {
    object_store: Arc<InMemory>,
    record_write_held: Arc<Notify>,
    release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
}

impl Debug for HeldRecordWriteStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeldRecordWriteStore").finish()
    }
}

impl Display for HeldRecordWriteStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeldRecordWriteStore").finish()
    }
}

#[async_trait]
impl ObjectStore for HeldRecordWriteStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        let release = location
            .as_ref()
            .ends_with(".batch")
            .then(|| {
                self.release
                    .lock()
                    .ok()
                    .and_then(|mut release| release.take())
            })
            .flatten();

        if let Some(release) = release {
            self.record_write_held.notify_one();
            _ = release.await;
        }

        self.object_store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.object_store.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.object_store.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.object_store.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.object_store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.object_store.copy_opts(from, to, opts).await
    }
}

fn idempotent_batch(producer_id: i64, base_sequence: i32) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .producer_id(producer_id)
        .producer_epoch(0)
        .base_sequence(base_sequence)
        .record(Record::builder().value(Bytes::from_static(b"value").into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

#[tokio::test]
async fn retry_that_advances_first_leaves_the_sequence_advanced_once() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let record_write_held = Arc::new(Notify::new());
    let (release_sender, release_receiver) = oneshot::channel();

    let storage = DynoStore::new(
        "nisshi",
        111,
        HeldRecordWriteStore {
            object_store: Arc::new(InMemory::new()),
            record_write_held: record_write_held.clone(),
            release: Arc::new(Mutex::new(Some(release_receiver))),
        },
    )?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("abc".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new("abc", 0);
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;

    let first_attempt = storage.produce(None, &topition, idempotent_batch(producer.id, 0)?);

    let retry = async {
        record_write_held.notified().await;

        let retry = storage
            .produce(None, &topition, idempotent_batch(producer.id, 0)?)
            .await;

        _ = release_sender.send(());
        retry
    };

    let (first_attempt, retry) = tokio::join!(first_attempt, retry);

    assert_eq!(1, retry?);
    assert_matches!(
        first_attempt,
        Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
    );

    assert_eq!(
        2,
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 1)?)
            .await?
    );

    Ok(())
}
