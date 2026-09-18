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

use std::{
    fmt::Debug,
    sync::{Arc, LazyLock, Mutex},
};

use crate::{Result, dynostore::object_store_error_name};
use bytes::Bytes;
use object_store::{
    Attributes, GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, TagSet,
    UpdateVersion, path::Path,
};
use opentelemetry::{KeyValue, metrics::Counter};
use serde::{Serialize, de::DeserializeOwned};
use tracing::{debug, instrument};

use super::METER;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DataVersion<D> {
    data: D,
    version: Option<UpdateVersion>,
}

impl<D> From<&DataVersion<D>> for PutMode {
    fn from(value: &DataVersion<D>) -> Self {
        value
            .version
            .clone()
            .map_or(PutMode::Create, PutMode::Update)
    }
}

static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_opticon_requests")
        .with_description("OptiCon requests")
        .build()
});

static ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_opticon_errors")
        .with_description("OptiCon requests")
        .build()
});

#[derive(Clone, Debug, Default)]
pub(super) struct OptiCon<D> {
    path: Path,
    tags: TagSet,
    attributes: Attributes,
    data_version: Arc<Mutex<Option<DataVersion<D>>>>,
}

impl<D> OptiCon<D> {
    pub(super) fn path(path: impl Into<Path>) -> Self {
        Self {
            path: path.into(),
            tags: Default::default(),
            attributes: Default::default(),
            data_version: Default::default(),
        }
    }
}

impl<D> OptiCon<D>
where
    D: Clone + Debug + Default + DeserializeOwned + PartialEq + Serialize,
{
    #[instrument(skip_all, fields(path = %self.path))]
    async fn get(&self, object_store: &impl ObjectStore) -> Result<()> {
        const METHOD: &str = "get";
        REQUESTS.add(1, &[KeyValue::new("method", METHOD)]);

        let on_error = |error: &object_store::Error| {
            ERRORS.add(
                1,
                &[
                    KeyValue::new("method", METHOD),
                    KeyValue::new("error", object_store_error_name(error)),
                ],
            );
        };

        match object_store.get(&self.path).await.inspect_err(|error| {
            debug!(?error);
            on_error(error)
        }) {
            Ok(get_result) => {
                let version = Some(UpdateVersion {
                    e_tag: get_result.meta.e_tag.clone(),
                    version: get_result.meta.version.clone(),
                });

                let encoded = get_result.bytes().await.inspect_err(|error| {
                    debug!(?error);
                    on_error(error)
                })?;
                let data = serde_json::from_slice::<D>(&encoded)?;

                debug!(?version);

                self.data_version
                    .lock()
                    .map_err(Into::into)
                    .map(|mut lock| lock.replace(DataVersion { data, version }))
                    .and(Ok(()))
            }

            Err(object_store::Error::NotFound { .. }) => self
                .data_version
                .lock()
                .map_err(Into::into)
                .map(|mut lock| lock.take())
                .and(Ok(())),

            Err(otherwise) => Err(otherwise.into()),
        }
    }

    #[instrument(skip_all, fields(path = %self.path))]
    pub(super) async fn with<E, F>(&self, object_store: &impl ObjectStore, f: F) -> Result<E>
    where
        F: Fn(&D) -> Result<E>,
    {
        const METHOD: &str = "with";
        REQUESTS.add(1, &[KeyValue::new("method", METHOD)]);

        let on_error = |error: &object_store::Error| {
            ERRORS.add(
                1,
                &[
                    KeyValue::new("method", METHOD),
                    KeyValue::new("error", object_store_error_name(error)),
                ],
            );
        };

        let version = self
            .data_version
            .lock()
            .map(|guard| guard.as_ref().and_then(|dv| dv.version.clone()))?;
        debug!(?version);

        match object_store
            .get_opts(
                &self.path,
                GetOptions {
                    if_none_match: version.as_ref().and_then(|version| version.e_tag.clone()),
                    ..GetOptions::default()
                },
            )
            .await
            .inspect_err(|error| {
                debug!(?error);
                on_error(error)
            }) {
            Ok(get_result) => {
                let version = Some(UpdateVersion {
                    e_tag: get_result.meta.e_tag.clone(),
                    version: get_result.meta.version.clone(),
                });

                debug!(action = "out of date", ?version);

                get_result
                    .bytes()
                    .await
                    .inspect_err(|error| {
                        debug!(?error);
                        on_error(error)
                    })
                    .map_err(Into::into)
                    .and_then(|encoded| serde_json::from_slice::<D>(&encoded).map_err(Into::into))
                    .and_then(|data| {
                        self.data_version
                            .lock()
                            .map_err(Into::into)
                            .map(|mut guard| guard.replace(DataVersion { data, version }))
                    })
                    .and(Ok(()))
            }

            Err(object_store::Error::NotFound { .. }) => {
                debug!(action = "not found");
                self.data_version
                    .lock()
                    .map_err(Into::into)
                    .map(|mut guard| guard.take())
                    .and(Ok(()))
            }

            Err(object_store::Error::NotModified { .. }) => {
                debug!(action = "not modified");
                Ok(())
            }

            Err(otherwise) => Err(otherwise.into()),
        }
        .and(
            self.data_version
                .lock()
                .map_err(Into::into)
                .and_then(|lock| {
                    if let Some(dv @ DataVersion { data, .. }) = lock.as_ref() {
                        debug!(?dv);
                        f(data)
                    } else {
                        let data = D::default();
                        debug!(?data);
                        f(&data)
                    }
                }),
        )
    }

    #[instrument(skip_all, fields(path = %self.path))]
    pub(super) async fn with_mut<E, F>(&self, object_store: &impl ObjectStore, f: F) -> Result<E>
    where
        E: Debug,
        F: Fn(&mut D) -> Result<E>,
    {
        const METHOD: &str = "with_mut";
        REQUESTS.add(1, &[KeyValue::new("method", METHOD)]);

        let on_error = |error: &object_store::Error| {
            ERRORS.add(
                1,
                &[
                    KeyValue::new("method", METHOD),
                    KeyValue::new("error", object_store_error_name(error)),
                ],
            );
        };

        loop {
            REQUESTS.add(1, &[KeyValue::new("method", "with_mut_loop")]);

            let (outcome, dv) = self.data_version.lock().map(|guard| {
                let mut dv = guard.clone().unwrap_or_default();
                let outcome = f(&mut dv.data);
                (outcome, dv)
            })?;

            let payload = serde_json::to_vec(&dv.data)
                .map(Bytes::from)
                .map(PutPayload::from)?;

            let opts = PutOptions {
                mode: PutMode::from(&dv),
                tags: self.tags.clone(),
                attributes: self.attributes.clone(),
                ..Default::default()
            };

            match object_store
                .put_opts(&self.path, payload, opts)
                .await
                .inspect_err(|error| {
                    debug!(?error);
                    on_error(error)
                }) {
                Ok(put_result) => {
                    return self
                        .data_version
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| {
                            guard.replace(DataVersion {
                                data: dv.data,
                                version: Some(UpdateVersion {
                                    e_tag: put_result.e_tag,
                                    version: put_result.version,
                                }),
                            })
                        })
                        .and(outcome);
                }

                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                ) => {
                    self.get(object_store).await?;
                    continue;
                }

                Err(err) => return Err(err.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::{PutPayload, memory::InMemory};
    use serde::{Deserialize, Serialize};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::Error;

    use super::*;

    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X(i32);

    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(EnvFilter::from_default_env().add_directive(
                    format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
                ))
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[tokio::test]
    async fn with_does_not_exist() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        let o = OptiCon::path(path.clone());

        assert_eq!(1, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        assert!(matches!(
            object_store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        assert_eq!(1, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        assert!(matches!(
            object_store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        Ok(())
    }

    #[tokio::test]
    async fn with_mut_does_not_exist() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        let o = OptiCon::path(path.clone());

        let expected = 1;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        let expected = 2;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        Ok(())
    }

    #[tokio::test]
    async fn with_did_exist() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let o = OptiCon::path(path.clone());

        assert_eq!(7, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        object_store.delete(&path).await?;

        assert_eq!(1, o.with(&object_store, |x| Ok(x.0 + 1)).await?);

        assert!(matches!(
            object_store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        assert_eq!(1, o.with(&object_store, |x| Ok(x.0 + 1)).await?);

        assert!(matches!(
            object_store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        Ok(())
    }

    #[tokio::test]
    async fn with_mut_did_exist() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let o = OptiCon::path(path.clone());

        let expected = 7;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        object_store.delete(&path).await?;

        let expected = 1;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        let expected = 2;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        Ok(())
    }

    #[tokio::test]
    async fn with_already_exists() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let o = OptiCon::path(path.clone());

        assert_eq!(7, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(6, data.0);

        assert_eq!(7, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(6, data.0);

        Ok(())
    }

    #[tokio::test]
    async fn with_mut_already_exists() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let o = OptiCon::path(path.clone());

        assert_eq!(
            42,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;

                Ok(6 * x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(7, data.0);

        assert_eq!(
            48,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;

                Ok(6 * x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(8, data.0);

        Ok(())
    }
}
