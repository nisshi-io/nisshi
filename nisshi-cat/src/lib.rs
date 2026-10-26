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

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
//! Nisshi Cat
//!
//! Fetch or Produce (with validation when backed by a schema) messages to a topic

use std::{fmt, io, result, sync::Arc, time::Duration};

use consume::Consume;
pub use location::Location;
use nisshi_sans_io::ErrorCode;
use produce::Produce;
use tokio_util::codec::LinesCodecError;

mod consume;
mod location;
mod produce;

pub type Result<T, E = Error> = result::Result<T, E>;

/// How long a command keeps trying to connect to the broker before it fails.
///
/// The client's default is 30 s, the default of Kafka's
/// `socket.connection.setup.timeout.max.ms`. A command that a person runs uses a shorter limit,
/// so that it reports a broker that isn't running within seconds.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    Client(#[from] nisshi_client::Error),
    InvalidJson {
        location: Location,
        source: serde_json::Error,
    },

    Io(Arc<io::Error>),
    LinesCodec(#[from] LinesCodecError),
    Message(String),

    /// A message has neither a `key` nor a `value` field, so `cat produce` can't make a record
    /// of it without a schema.
    MessageWithoutKeyOrValue {
        location: Location,
        message: String,
    },

    NoMessages {
        location: Location,
    },

    /// A file holds JSON that isn't an array, so `cat produce` can't read messages from it.
    NotAnArray {
        location: Location,
    },

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
        match self {
            Self::InvalidJson { location, source } => write!(
                f,
                "{location} is not valid JSON: {source}. {}",
                location.message_format()
            ),
            Self::MessageWithoutKeyOrValue { location, message } => write!(
                f,
                r#"{location} has no "key" or "value" field: {message}. {}"#,
                location.message_format()
            ),
            Self::NotAnArray { location } => write!(
                f,
                "{location} is not a JSON array of messages. {}",
                location.message_format()
            ),
            Self::NoMessages { location } => {
                write!(
                    f,
                    "{location} has no messages. {}",
                    location.message_format()
                )
            }
            _ => write!(f, "{self:?}"),
        }
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

#[cfg(test)]
mod tests {
    use super::{Cat, Error};
    use std::{assert_matches, fs, net::TcpListener, process, time::Duration};
    use tokio::time::timeout;
    use url::Url;

    /// Returns the URL of a local port that nothing listens on.
    fn closed_port() -> Url {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        Url::parse(&format!("tcp://127.0.0.1:{port}")).unwrap()
    }

    /// Asserts that `cat` fails to connect well within the client's default connect timeout
    /// of 30 s, which shows that the command sets the shorter `CONNECT_TIMEOUT`.
    async fn assert_stops_connecting_early(cat: Cat) {
        assert_matches!(
            timeout(Duration::from_secs(15), cat.main()).await,
            Ok(Err(Error::Client(_)))
        );
    }

    #[tokio::test]
    async fn produce_stops_connecting_after_the_connect_timeout() {
        let file_name = std::env::temp_dir()
            .join(format!("nisshi-cat-connect-timeout-{}.json", process::id()))
            .to_string_lossy()
            .into_owned();
        fs::write(&file_name, r#"[{"value": "hello world"}]"#).unwrap();

        assert_stops_connecting_early(
            Cat::produce()
                .broker(closed_port())
                .topic("greetings")
                .partition(0)
                .schema_registry(None)
                .file_name(file_name.clone())
                .build(),
        )
        .await;

        fs::remove_file(file_name).unwrap();
    }

    #[tokio::test]
    async fn consume_stops_connecting_after_the_connect_timeout() {
        assert_stops_connecting_early(
            Cat::consume()
                .broker(closed_port())
                .topic("greetings")
                .partition(0)
                .schema_registry(None)
                .build(),
        )
        .await;
    }
}
