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

use crate::{ByteSize, Decode, Encode, Result, record::codec::Octets};
use bytes::{BufMut as _, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tracing::instrument;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Header {
    #[serde(serialize_with = "Octets::serialize")]
    #[serde(deserialize_with = "Octets::deserialize")]
    pub key: Option<Bytes>,

    #[serde(serialize_with = "Octets::serialize")]
    #[serde(deserialize_with = "Octets::deserialize")]
    pub value: Option<Bytes>,
}

impl ByteSize for Header {
    fn size_in_bytes(&self) -> Result<usize> {
        Octets(self.key.clone())
            .size_in_bytes()
            .and_then(|key_octets| {
                Octets(self.value.clone())
                    .size_in_bytes()
                    .map(|value_octets| key_octets + value_octets)
            })
    }
}

impl Encode for Header {
    #[instrument(skip_all)]
    fn encode(&self) -> Result<Bytes> {
        let mut encoded = self.size_in_bytes().map(BytesMut::with_capacity)?;

        encoded.put(Octets(self.key.clone()).encode()?);
        encoded.put(Octets(self.value.clone()).encode()?);

        Ok(encoded.into())
    }
}

impl Decode for Header {
    #[instrument(skip_all)]
    fn decode(encoded: &mut Bytes) -> Result<Self> {
        Octets::decode(encoded).and_then(|key| {
            Octets::decode(encoded).map(|value| Self {
                key: key.into(),
                value: value.into(),
            })
        })
    }
}

impl Header {
    pub fn builder() -> Builder {
        Builder::default()
    }
}

impl From<Builder> for Header {
    fn from(value: Builder) -> Self {
        Self {
            key: value.key.0,
            value: value.value.0,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Builder {
    key: Octets,
    value: Octets,
}

impl Builder {
    #[must_use]
    pub fn key(mut self, key: Bytes) -> Self {
        self.key = Octets::from(Some(key));
        self
    }

    #[must_use]
    pub fn value(mut self, value: Bytes) -> Self {
        self.value = Octets::from(Some(value));
        self
    }

    #[must_use]
    pub fn build(self) -> Header {
        Header {
            key: self.key.0,
            value: self.value.0,
        }
    }
}

impl From<Header> for Builder {
    fn from(value: Header) -> Self {
        Self {
            key: value.key.into(),
            value: value.value.into(),
        }
    }
}

impl ByteSize for Builder {
    fn size_in_bytes(&self) -> Result<usize> {
        self.key
            .size_in_bytes()
            .and_then(|ksz| self.value.size_in_bytes().map(|vsz| ksz + vsz))
    }
}
