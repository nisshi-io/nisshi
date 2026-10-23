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

//! The error code that `DynoStore::describe_groups` answers when it can't read a group. Kafka
//! clients retry `COORDINATOR_NOT_AVAILABLE`, so an object store error answers that code, and a
//! group that can't be decoded answers `UNKNOWN_SERVER_ERROR`, because a retry does not fix it.

use std::fmt::{Debug, Display};

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nisshi_sans_io::ErrorCode;
use nisshi_storage::{Error, NamedGroupDetail, Result, Storage as _};
use object_store::{
    CopyOptions, GetOptions, GetResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};

use crate::dynostore::{DynoStore, tests::init_tracing};

const GROUP_ID: &str = "abc";

/// Wraps an [`ObjectStore`] and makes each `get()` or each `list()` call fail.
#[derive(Clone)]
struct FailingObjectStore<O> {
    object_store: O,
    get_fails: bool,
    list_fails: bool,
}

impl<O> FailingObjectStore<O> {
    fn failing_get(object_store: O) -> Self {
        Self {
            object_store,
            get_fails: true,
            list_fails: false,
        }
    }

    fn failing_list(object_store: O) -> Self {
        Self {
            object_store,
            get_fails: false,
            list_fails: true,
        }
    }
}

fn forced_failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "test",
        source: "forced failure".into(),
    }
}

impl<O> Debug for FailingObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailingObjectStore").finish()
    }
}

impl<O> Display for FailingObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailingObjectStore").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for FailingObjectStore<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
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
        if self.get_fails {
            return Err(forced_failure());
        }

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
        if self.list_fails {
            return Box::pin(stream::once(async { Err(forced_failure()) }));
        }

        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<object_store::ListResult, object_store::Error> {
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

async fn describe(storage: &DynoStore) -> Result<Vec<NamedGroupDetail>> {
    storage
        .describe_groups(Some(&[GROUP_ID.to_owned()]), false)
        .await
}

#[tokio::test]
async fn failed_state_read_answers_coordinator_not_available() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(
        "nisshi",
        111,
        FailingObjectStore::failing_get(InMemory::new()),
    )?;

    assert_eq!(
        vec![NamedGroupDetail::error_code(
            GROUP_ID.into(),
            ErrorCode::CoordinatorNotAvailable
        )],
        describe(&storage).await?
    );

    Ok(())
}

#[tokio::test]
async fn failed_offsets_list_answers_coordinator_not_available() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(
        "nisshi",
        111,
        FailingObjectStore::failing_list(InMemory::new()),
    )?;

    assert_eq!(
        vec![NamedGroupDetail::error_code(
            GROUP_ID.into(),
            ErrorCode::CoordinatorNotAvailable
        )],
        describe(&storage).await?
    );

    Ok(())
}

#[tokio::test]
async fn undecodable_state_answers_unknown_server_error() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = InMemory::new();
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?;

    _ = object_store
        .put(
            &storage.group_state_location(GROUP_ID)?,
            PutPayload::from_static(b"not json"),
        )
        .await?;

    assert_eq!(
        vec![NamedGroupDetail::error_code(
            GROUP_ID.into(),
            ErrorCode::UnknownServerError
        )],
        describe(&storage).await?
    );

    Ok(())
}
