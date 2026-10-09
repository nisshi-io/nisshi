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

//! Nisshi Cat
//!
//! Fetch or Produce (with validation when backed by a schema) messages to a topic

use std::{fmt, io, result, sync::Arc};

use consume::Consume;
use nisshi_sans_io::ErrorCode;
use produce::Produce;
use tokio_util::codec::LinesCodecError;

mod consume;
mod produce;

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    Client(#[from] nisshi_client::Error),
    Io(Arc<io::Error>),
    LinesCodec(#[from] LinesCodecError),
    Protocol(#[from] nisshi_sans_io::Error),
    Schema(#[from] nisshi_schema::Error),
    SerdeJson(#[from] serde_json::Error),
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Cat {
    Consume(Box<consume::Configuration>),
    Produce(Box<produce::Configuration>),
}

impl Cat {
    pub fn produce() -> produce::PhantomBuilder {
        Builder::produce()
    }

    pub fn consume() -> consume::PhantomBuilder {
        Builder::consume()
    }

    pub async fn main(self) -> Result<ErrorCode> {
        match self {
            Self::Produce(configuration) => Produce::try_from(*configuration)?.main().await,
            Self::Consume(configuration) => Consume::try_from(*configuration)?.main().await,
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder;

impl Builder {
    pub fn produce() -> produce::PhantomBuilder {
        produce::Builder::default()
    }

    pub fn consume() -> consume::PhantomBuilder {
        consume::Builder::default()
    }
}
