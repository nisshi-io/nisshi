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

//! Run CPU-bound batch decoding off the async runtime's worker threads.
//!
//! Inflating a produced batch decompresses and parses every record in it.
//! For a large compressed batch that takes long enough that running it on a
//! runtime worker thread stalls every other connection scheduled on that
//! worker. [`offload`] moves the work onto Tokio's blocking thread pool, and
//! [`inflate`] does so for the common case of inflating one batch.

use std::sync::{Arc, OnceLock};

use nisshi_sans_io::record::{deflated, inflated};
use tokio::{runtime::Handle, sync::Semaphore, task};

use crate::{Error, Result};

/// Bounds how many offloaded decodes run at once.
///
/// Before decoding moved to the blocking pool, it ran on the runtime's worker
/// threads, so at most one decode per worker ran at a time. The blocking pool
/// allows up to 512 threads by default, so without this bound a burst of
/// large produce requests would start one decode thread each. Sizing the
/// semaphore from the worker count keeps the old ceiling on decode
/// concurrency.
///
/// The semaphore is created once per process, sized from the runtime that
/// first calls [`offload`]. The broker runs a single runtime, so that is its
/// worker count, including any `TOKIO_WORKER_THREADS` override.
static PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();

fn permits() -> Arc<Semaphore> {
    PERMITS
        .get_or_init(|| {
            Arc::new(Semaphore::new(
                Handle::current().metrics().num_workers().max(1),
            ))
        })
        .clone()
}

/// Run `f` on Tokio's blocking thread pool, at most one call per runtime
/// worker thread at a time.
///
/// Use this for CPU-bound work, such as decoding a record batch, that would
/// otherwise block a runtime worker thread. A panic in `f` is returned as
/// [`Error::Join`].
pub async fn offload<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let permit = permits().acquire_owned().await?;

    task::spawn_blocking(move || {
        let _permit = permit;
        f()
    })
    .await?
}

/// Inflate `deflated` on Tokio's blocking thread pool, as [`offload`] does.
pub async fn inflate(deflated: deflated::Batch) -> Result<inflated::Batch> {
    offload(move || inflated::Batch::try_from(deflated).map_err(Error::from)).await
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, time::Duration};

    use tokio::task::yield_now;

    use super::*;

    /// How long the offloaded closure waits for the signal before giving up.
    /// Only reached when the closure runs inline on the runtime thread, where
    /// the signal can never arrive while it waits.
    const SIGNAL_TIMEOUT: Duration = Duration::from_secs(3);

    // The test runtime has a single thread. The offloaded closure waits for
    // a signal that only this task, on that thread, can send. If `offload`
    // ran the closure inline, it would block the only runtime thread, the
    // signal would never be sent while it waited, and it would time out.
    #[tokio::test]
    async fn offload_runs_off_the_runtime_thread() -> Result<()> {
        let (tx, rx) = mpsc::channel::<()>();

        let task = tokio::spawn(offload(move || Ok(rx.recv_timeout(SIGNAL_TIMEOUT).is_ok())));

        yield_now().await;
        _ = tx.send(());

        assert!(
            task.await??,
            "the offloaded closure blocked the runtime thread"
        );

        Ok(())
    }

    #[tokio::test]
    async fn offload_returns_a_panic_as_join_error() {
        let outcome = offload::<_, ()>(|| panic!("decode panicked")).await;

        assert!(matches!(outcome, Err(Error::Join(_))), "{outcome:?}");
    }
}
