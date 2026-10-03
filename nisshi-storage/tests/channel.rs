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

//! Exercises `ChannelRequestService::serve`, the mpsc-mode storage supervisor loop
//! (SOL-155270), directly against a minimal test double - rather than a real storage
//! backend - so a panic or a dropped response receiver can be triggered
//! deterministically, independent of any backend's own behaviour.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use nisshi_storage::{
    ChannelRequestLayer, Error, Request, RequestChannelService, Response, Storage, bounded_channel,
};
use rama::{Layer, Service};
use tokio::{sync::oneshot, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::common::{Error as TestError, init_tracing};

mod common;

/// A minimal [`Service<Request>`] standing in for real storage, so the supervisor
/// loop's own behaviour (what SOL-155270 fixes) can be tested independently of any
/// storage backend. Panics exactly once - on the first request it serves, unless
/// built with [`FlakyService::benign`] - then answers every request with
/// `Response::Ping(Ok(()))`.
#[derive(Clone, Default)]
struct FlakyService {
    panic_once: Arc<AtomicBool>,
}

impl FlakyService {
    /// A `FlakyService` that never panics - its one-shot flag starts already spent.
    fn benign() -> Self {
        Self {
            panic_once: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl Service<Request> for FlakyService {
    type Output = Response;
    type Error = Error;

    async fn serve(&self, _req: Request) -> Result<Self::Output, Self::Error> {
        if self.panic_once.swap(true, Ordering::SeqCst) {
            Ok(Response::Ping(Ok(())))
        } else {
            panic!("synthetic panic injected for SOL-155270 test coverage")
        }
    }
}

/// A well-behaved server and client must round-trip a request successfully - basic
/// sanity coverage for the plumbing the two tests below build on.
#[tokio::test]
async fn request_response_round_trip() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let (sender, receiver) = bounded_channel(10);
    let cancellation = CancellationToken::new();

    let mut join = JoinSet::new();

    {
        let cancellation = cancellation.clone();
        let server = FlakyService::benign();

        let _ = join.spawn(async move {
            ChannelRequestLayer::new(cancellation)
                .into_layer(server)
                .serve(receiver)
                .await
        });
    }

    let client = RequestChannelService::new(sender);
    client.ping().await?;

    cancellation.cancel();
    let joined = join.join_all().await;
    debug!(?joined);

    Ok(())
}

/// F2: a client that sends a request and then drops its response receiver (simulating
/// a cancelled/disconnected caller) must not end the shared supervisor loop - a second,
/// well-behaved request on the same channel must still succeed.
#[tokio::test]
async fn dropped_response_receiver_does_not_end_server_loop() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let (sender, receiver) = bounded_channel(10);
    let cancellation = CancellationToken::new();

    let mut join = JoinSet::new();

    {
        let cancellation = cancellation.clone();
        let server = FlakyService::benign();

        let _ = join.spawn(async move {
            ChannelRequestLayer::new(cancellation)
                .into_layer(server)
                .serve(receiver)
                .await
        });
    }

    // Send a request directly on the raw channel (bypassing `RequestChannelService`,
    // which always keeps its own receiver alive) so the test controls the oneshot and
    // can drop it - simulating a caller that cancelled or disconnected mid-request.
    let (resp_tx, resp_rx) = oneshot::channel();
    sender
        .send((Request::Ping, resp_tx))
        .await
        .map_err(|_| TestError::Message(String::from("send failed")))?;
    drop(resp_rx);

    // Give the supervisor loop a generous window to process the request and observe
    // the failed send before asserting it is still alive - and assert that directly,
    // rather than inferring survival only from a later request succeeding, so a
    // regression fails for the right reason instead of an ambiguous timeout.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        join.try_join_next().is_none(),
        "supervisor loop ended after a dropped response receiver"
    );

    // A second, well-behaved request on the same channel must still succeed.
    let client = RequestChannelService::new(sender);
    client.ping().await?;

    cancellation.cancel();
    let joined = join.join_all().await;
    debug!(?joined);

    Ok(())
}

/// F1: a panic while handling one request must not end the shared supervisor loop -
/// a second, well-behaved request on the same channel must still succeed.
#[tokio::test]
async fn panicking_request_does_not_end_server_loop() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let (sender, receiver) = bounded_channel(10);
    let cancellation = CancellationToken::new();

    let mut join = JoinSet::new();

    {
        let cancellation = cancellation.clone();
        let flaky = FlakyService::default();

        let _ = join.spawn(async move {
            ChannelRequestLayer::new(cancellation)
                .into_layer(flaky)
                .serve(receiver)
                .await
        });
    }

    let client = RequestChannelService::new(sender);

    // First request panics inside its per-request child task; the caller observes a
    // channel failure (its oneshot `tx` was dropped without a send), not a hang.
    assert!(client.ping().await.is_err());

    // The supervisor loop itself must have survived the panic.
    assert!(
        join.try_join_next().is_none(),
        "supervisor loop ended after a panicking request"
    );

    // A second request (the flaky service's panic-once flag is now consumed) must
    // still succeed - proving the shared loop kept serving other callers.
    client.ping().await?;

    cancellation.cancel();
    let joined = join.join_all().await;
    debug!(?joined);

    Ok(())
}
