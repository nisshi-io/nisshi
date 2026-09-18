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
    fmt::{Debug, Display},
    num::NonZero,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use cached::stores::ExpiringSizedCache;
use futures::stream::BoxStream;
use governor::{DefaultDirectRateLimiter, Jitter, Quota, RateLimiter};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use tracing::{debug, error, instrument, warn};

use crate::Result;

const DEFAULT_JITTER: Duration = Duration::from_millis(0);

#[derive(Clone)]
pub(crate) struct PutRateLimiter<O> {
    entries: Arc<Mutex<ExpiringSizedCache<Path, Arc<DefaultDirectRateLimiter>>>>,
    rate_per_second: Option<NonZero<u32>>,
    jitter: Option<Duration>,
    object_store: O,
}

impl<O> Debug for PutRateLimiter<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutRateLimiter").finish()
    }
}

impl<O> Display for PutRateLimiter<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutRateLimiter").finish()
    }
}

impl<O> PutRateLimiter<O> {
    pub(crate) fn new(object_store: O, ttl: Duration) -> Self {
        Self {
            object_store,
            entries: Arc::new(Mutex::new(ExpiringSizedCache::new(ttl))),
            rate_per_second: Default::default(),
            jitter: Default::default(),
        }
    }

    pub(crate) fn with_rate_per_second(self, rate_per_second: Option<NonZero<u32>>) -> Self {
        Self {
            rate_per_second,
            ..self
        }
    }

    pub(crate) fn with_jitter(self, jitter: Option<Duration>) -> Self {
        Self { jitter, ..self }
    }

    fn rate_limiter(&self) -> Option<Arc<DefaultDirectRateLimiter>> {
        self.rate_per_second
            .map(Quota::per_second)
            .map(RateLimiter::direct)
            .map(Arc::new)
    }

    #[instrument(skip_all, fields(location = %location))]
    fn location_rate_limiter(&self, location: &Path) -> Option<Arc<DefaultDirectRateLimiter>> {
        self.entries.lock().ok().and_then(|mut entries| {
            entries
                .get(location)
                .cloned()
                .or_else(|| self.rate_limiter())
                .and_then(|rate_limiter| {
                    entries
                        .insert_evict(location.to_owned(), rate_limiter.clone(), true)
                        .ok()
                        .map(|_| rate_limiter)
                })
        })
    }

    #[instrument(skip_all, fields(location = %location))]
    async fn rate_limit(&self, location: &Path) {
        if let Some(rate_per_second) = self.rate_per_second
            && let Some(rate_limiter) = self.location_rate_limiter(location)
        {
            let rate_limit_start = SystemTime::now();

            _ = rate_limiter
                .until_n_ready_with_jitter(
                    rate_per_second,
                    Jitter::up_to(self.jitter.unwrap_or(DEFAULT_JITTER)),
                )
                .await
                .inspect_err(|err| error!(%location, ?err))
                .inspect(|_| {
                    let rate_limited_ms = rate_limit_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64);
                    debug!(rate_limited_ms);
                })
                .ok();
        } else {
            warn!("no_rate_limit");
        }
    }
}

#[async_trait]
impl<O> ObjectStore for PutRateLimiter<O>
where
    O: ObjectStore,
{
    #[instrument(skip_all, fields(location = %location))]
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.rate_limit(location).await;
        self.object_store.put_opts(location, payload, opts).await
    }

    #[instrument(skip_all, fields(location = %location))]
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.rate_limit(location).await;
        self.object_store.put_multipart_opts(location, opts).await
    }

    #[instrument(skip_all, fields(%location, if_none_match = options.if_none_match), ret)]
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.object_store.get_opts(location, options.clone()).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.object_store.delete_stream(locations)
    }

    #[instrument(skip_all, fields(prefix))]
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list(prefix)
    }

    #[instrument(skip_all, fields(prefix))]
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.object_store.list_with_delimiter(prefix).await
    }

    #[instrument(skip_all, fields(from = %from, to = %to))]
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.object_store.copy_opts(from, to, opts).await
    }
}

#[cfg(test)]
mod tests {

    use std::num::NonZeroU32;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::Error;

    use super::*;

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
    async fn test() -> Result<()> {
        let _guard = init_tracing()?;

        const EXPECTED_DELAY: u64 = 900;

        let prl = PutRateLimiter::new(InMemory::new(), Duration::from_mins(5))
            .with_rate_per_second(NonZeroU32::new(1));

        let location = Path::from("a");

        let delay = {
            let now = SystemTime::now();
            _ = prl
                .put_opts(
                    &location,
                    PutPayload::from(Bytes::from_static(b"12321")),
                    PutOptions::default(),
                )
                .await?;

            now.elapsed()
                .map_or(0, |duration| duration.as_millis() as u64)
        };

        assert!(delay < EXPECTED_DELAY, "{delay}");

        let delay = {
            let now = SystemTime::now();
            _ = prl
                .put_opts(
                    &location,
                    PutPayload::from(Bytes::from_static(b"12321")),
                    PutOptions::default(),
                )
                .await?;

            now.elapsed()
                .map_or(0, |duration| duration.as_millis() as u64)
        };

        assert!(delay >= EXPECTED_DELAY, "{delay}");

        let location = Path::from("b");

        let delay = {
            let now = SystemTime::now();
            _ = prl
                .put_opts(
                    &location,
                    PutPayload::from(Bytes::from_static(b"12321")),
                    PutOptions::default(),
                )
                .await?;

            now.elapsed()
                .map_or(0, |duration| duration.as_millis() as u64)
        };

        assert!(delay < EXPECTED_DELAY, "{delay}");

        let location = Path::from("a");

        let delay = {
            let now = SystemTime::now();
            _ = prl
                .put_opts(
                    &location,
                    PutPayload::from(Bytes::from_static(b"12321")),
                    PutOptions::default(),
                )
                .await?;

            now.elapsed()
                .map_or(0, |duration| duration.as_millis() as u64)
        };

        assert!(delay >= EXPECTED_DELAY, "{delay}");

        Ok(())
    }
}
