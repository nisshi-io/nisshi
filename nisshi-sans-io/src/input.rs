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

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;
use rama::{
    ServiceInput,
    extensions::{Extension, Extensions, ExtensionsRef},
};

use crate::{Body, Frame, Request};

#[derive(Clone, Debug)]
pub struct FrameInput {
    pub frame: Frame,
    pub extensions: Extensions,
}

impl AsRef<Frame> for FrameInput {
    fn as_ref(&self) -> &Frame {
        &self.frame
    }
}

impl ExtensionsRef for FrameInput {
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl From<Frame> for FrameInput {
    fn from(frame: Frame) -> Self {
        Self {
            frame,
            extensions: Extensions::default(),
        }
    }
}

impl From<ServiceInput<Frame>> for FrameInput {
    fn from(value: ServiceInput<Frame>) -> Self {
        Self {
            frame: value.input,
            extensions: value.extensions,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BodyInput {
    pub body: Body,
    pub extensions: Extensions,
}

impl ExtensionsRef for BodyInput {
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl From<Body> for BodyInput {
    fn from(body: Body) -> Self {
        Self {
            body,
            extensions: Extensions::default(),
        }
    }
}

impl From<ServiceInput<Body>> for BodyInput {
    fn from(value: ServiceInput<Body>) -> Self {
        Self {
            body: value.input,
            extensions: value.extensions,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RequestInput<Q: Request> {
    pub request: Q,
    pub extensions: Extensions,
}

impl<Q> ExtensionsRef for RequestInput<Q>
where
    Q: Request,
{
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl<Q> From<Q> for RequestInput<Q>
where
    Q: Request,
{
    fn from(request: Q) -> Self {
        Self {
            request,
            extensions: Extensions::default(),
        }
    }
}

impl<Q> From<ServiceInput<Q>> for RequestInput<Q>
where
    Q: Request,
{
    fn from(value: ServiceInput<Q>) -> Self {
        Self {
            request: value.input,
            extensions: value.extensions,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BytesInput {
    pub bytes: Bytes,
    pub extensions: Extensions,
}

impl ExtensionsRef for BytesInput {
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl From<Bytes> for BytesInput {
    fn from(bytes: Bytes) -> Self {
        Self {
            bytes,
            extensions: Extensions::default(),
        }
    }
}

impl From<ServiceInput<Bytes>> for BytesInput {
    fn from(value: ServiceInput<Bytes>) -> Self {
        Self {
            bytes: value.input,
            extensions: value.extensions,
        }
    }
}

/// Marks that the response currently being assembled for a connection's
/// in-flight request must not be written to the peer.
///
/// A connection's request/response loop processes one request at a time and
/// shares one [`Extensions`] store across every request on it. `Extensions`
/// has no way to remove an entry once inserted, so a plain one-shot insert
/// would leak: every later request on the same connection would also find it
/// present and be wrongly suppressed. Wrapping the flag in an [`AtomicBool`]
/// lets [`Self::take`] read and clear it in one atomic step, so only the
/// request that set it is ever suppressed.
#[derive(Clone, Debug, Extension)]
pub struct SuppressResponseExtension(Arc<AtomicBool>);

impl SuppressResponseExtension {
    /// Marks the in-flight request's response for suppression, inserting this
    /// extension into `extensions` if this is the first request on the
    /// connection to need it.
    pub fn mark(extensions: &Extensions) {
        if let Some(marker) = extensions.get_ref::<Self>() {
            marker.0.store(true, Ordering::Release);
        } else {
            _ = extensions.insert(Self(Arc::new(AtomicBool::new(true))));
        }
    }

    /// Reads and clears the marker, reporting whether it was set.
    ///
    /// A connection that never carries a suppressed response never gets this
    /// extension inserted, so absence counts as unset.
    #[must_use]
    pub fn take(extensions: &Extensions) -> bool {
        extensions
            .get_ref::<Self>()
            .is_some_and(|marker| marker.0.swap(false, Ordering::AcqRel))
    }
}
