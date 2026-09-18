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

use nisshi_sans_io::{ApiKey, Body, Frame};
use rama::{Context, Layer, Service};

use crate::coordinator::group::Coordinator;

pub mod group;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CoordinatorService<C, S> {
    coordinator: C,
    inner: S,
}

impl<C, S> ApiKey for CoordinatorService<C, S>
where
    S: ApiKey,
{
    const KEY: i16 = S::KEY;
}

impl<C, S, State> Service<State, Frame> for CoordinatorService<C, S>
where
    S: Service<C, Frame>,
    S::Response: Into<Body>,
    C: Coordinator,
    State: Send + Sync + 'static,
{
    type Response = Body;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let (ctx, _) = ctx.swap_state(self.coordinator.clone());
        self.inner.serve(ctx, req).await.map(Into::into)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CoordinatorLayer<C> {
    coordinator: C,
}

impl<C> CoordinatorLayer<C> {
    pub fn new(coordinator: C) -> Self {
        Self { coordinator }
    }
}

impl<C, S> Layer<S> for CoordinatorLayer<C>
where
    C: Coordinator,
{
    type Service = CoordinatorService<C, S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            coordinator: self.coordinator.clone(),
            inner,
        }
    }
}
