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

use std::{
    fmt::{self, Display},
    io::Write,
    marker::PhantomData,
};

use crate::{Error, Result};

use bytes::Bytes;
use futures::StreamExt;
use nisshi_client::{Client, ConnectionManager};
use nisshi_sans_io::{
    ErrorCode, MetadataRequest, NULL_TOPIC_ID, ProduceRequest, ProduceResponse,
    metadata_request::MetadataRequestTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::{Record, deflated, inflated},
};
use nisshi_schema::{AsKafkaRecord, Registry, Schema};
use serde_json::Value;
use tokio::{
    fs::File,
    io::{self, AsyncReadExt},
};
use tokio_util::codec::{FramedRead, LinesCodec};
use tracing::{debug, warn};
use url::Url;

#[derive(Clone, Debug, Default)]
pub struct Builder<B, T, P, S, F> {
    broker: B,
    topic: T,
    partition: P,
    schema_registry: S,
    file_name: F,
    confirm: bool,
}

pub(crate) type PhantomBuilder = Builder<
    PhantomData<Url>,
    PhantomData<String>,
    PhantomData<i32>,
    PhantomData<Option<Url>>,
    PhantomData<String>,
>;

impl<B, T, P, S, F> Builder<B, T, P, S, F> {
    pub fn broker(self, broker: impl Into<Url>) -> Builder<Url, T, P, S, F> {
        Builder {
            broker: broker.into(),
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            file_name: self.file_name,
            confirm: self.confirm,
        }
    }

    pub fn topic(self, topic: impl Into<String>) -> Builder<B, String, P, S, F> {
        Builder {
            broker: self.broker,
            topic: topic.into(),
            partition: self.partition,
            schema_registry: self.schema_registry,
            file_name: self.file_name,
            confirm: self.confirm,
        }
    }

    pub fn partition(self, partition: i32) -> Builder<B, T, i32, S, F> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition,
            schema_registry: self.schema_registry,
            file_name: self.file_name,
            confirm: self.confirm,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Url>) -> Builder<B, T, P, Option<Url>, F> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry,
            file_name: self.file_name,
            confirm: self.confirm,
        }
    }

    pub fn file_name(self, file_name: String) -> Builder<B, T, P, S, String> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            file_name,
            confirm: self.confirm,
        }
    }

    /// Print a confirmation line to stdout on success. Off by default, so a library caller gets no output.
    pub fn confirm(self, confirm: bool) -> Builder<B, T, P, S, F> {
        Self { confirm, ..self }
    }
}

impl Builder<Url, String, i32, Option<Url>, String> {
    pub fn build(self) -> super::Cat {
        super::Cat::Produce(Box::new(Configuration {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            file_name: self.file_name,
            confirm: self.confirm,
        }))
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Configuration {
    pub broker: Url,
    pub topic: String,
    pub partition: i32,
    pub schema_registry: Option<Url>,
    pub file_name: String,
    pub confirm: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct Produce {
    configuration: Configuration,
    registry: Option<Registry>,
}

impl TryFrom<Configuration> for Produce {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        configuration
            .schema_registry
            .as_ref()
            .map(|url| Registry::builder_try_from_url(url).map(|builder| builder.build()))
            .transpose()
            .map(|registry| Self {
                configuration,
                registry,
            })
            .map_err(Into::into)
    }
}

impl Produce {
    fn add_to_batch(
        &self,
        mut batch: inflated::Builder,
        schema: Option<&Schema>,
        data: &Value,
        offset_delta: &mut i32,
    ) -> Result<inflated::Builder> {
        if let Some(schema) = schema {
            batch = batch.record(
                schema
                    .as_kafka_record(data)
                    .map(|record| record.offset_delta(*offset_delta))?,
            );

            *offset_delta += 1;
        } else {
            let key = data
                .get("key")
                .and_then(|key| serde_json::to_vec(key).map(Bytes::from).ok());

            let value = data
                .get("value")
                .and_then(|value| serde_json::to_vec(value).map(Bytes::from).ok());

            if key.is_some() || value.is_some() {
                batch = batch.record(
                    Record::builder()
                        .key(key)
                        .value(value)
                        .offset_delta(*offset_delta),
                );

                *offset_delta += 1;
            } else {
                warn!(ignored = %data);
            }
        }

        Ok(batch)
    }

    pub(crate) async fn main(self) -> Result<ErrorCode> {
        debug!(%self.configuration.file_name);

        let schema = if let Some(ref registry) = self.registry {
            registry
                .schema(&self.configuration.topic)
                .await
                .inspect(|schema| debug!(?schema))?
        } else {
            None
        };

        let mut offset_delta = 0;

        let frame = {
            let mut batch = inflated::Batch::builder();

            if self.configuration.file_name == "-" {
                let stdin = io::stdin();
                let mut reader = FramedRead::new(stdin, LinesCodec::new());

                while let Some(line) = reader.next().await.transpose()? {
                    if line.trim().is_empty() {
                        continue;
                    }

                    debug!(%line);

                    batch = serde_json::from_str::<Value>(&line)
                        .inspect(|data| debug!(%data))
                        .map_err(Into::into)
                        .and_then(|data| {
                            self.add_to_batch(batch, schema.as_ref(), &data, &mut offset_delta)
                        })?;
                }
            } else {
                let mut file = File::open(self.configuration.file_name.as_str()).await?;

                let mut contents = vec![];
                _ = file.read_to_end(&mut contents).await?;

                let v = serde_json::from_slice::<Value>(&contents[..])?;

                if let Some(records) = v.as_array() {
                    for record in records {
                        debug!(%record);

                        batch =
                            self.add_to_batch(batch, schema.as_ref(), record, &mut offset_delta)?;
                    }
                }
            }

            batch
                .last_offset_delta(offset_delta - 1)
                .build()
                .map(|batch| inflated::Frame {
                    batches: vec![batch],
                })
                .and_then(deflated::Frame::try_from)?
        };

        debug!(?frame);

        let client = ConnectionManager::builder(self.configuration.broker.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        // A real Kafka client's first Metadata request allows auto-creation, so a produce to a
        // topic that doesn't exist yet creates it instead of failing with UnknownTopicOrPartition.
        // Mirrors the Metadata request in `consume`, but with auto-creation allowed instead of
        // refused. It goes after the input is read, so that a missing or malformed input file
        // leaves no empty topic behind.
        _ = client
            .call(
                MetadataRequest::default()
                    .allow_auto_topic_creation(Some(true))
                    .include_cluster_authorized_operations(Some(false))
                    .include_topic_authorized_operations(Some(false))
                    .topics(Some(
                        [MetadataRequestTopic::default()
                            .name(Some(self.configuration.topic.clone()))
                            .topic_id(Some(NULL_TOPIC_ID))]
                        .into(),
                    )),
            )
            .await?;

        let req = ProduceRequest::default()
            .transactional_id(None)
            .acks(-1)
            .timeout_ms(1_500)
            .topic_data(Some(
                [TopicProduceData::default()
                    .name(self.configuration.topic.clone())
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(self.configuration.partition)
                            .records(Some(frame))]
                        .into(),
                    ))]
                .into(),
            ));

        let ProduceResponse { responses, .. } = client.call(req).await?;
        let responses = responses.unwrap_or_default();
        assert_eq!(1, responses.len());

        let TopicProduceResponse {
            partition_responses,
            ..
        } = responses.first().ok_or_else(|| {
            Error::Message(format!("expected exactly one response: {responses:?}"))
        })?;
        let partition_responses = partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partition_responses.len());

        let PartitionProduceResponse {
            error_code,
            base_offset,
            ..
        } = partition_responses.first().ok_or_else(|| {
            Error::Message(format!(
                "expected exactly one partition response: {partition_responses:?}"
            ))
        })?;

        let error_code = ErrorCode::try_from(*error_code)?;

        if error_code == ErrorCode::None && self.configuration.confirm {
            let produced = Produced {
                topic: self.configuration.topic,
                partition: self.configuration.partition,
                base_offset: *base_offset,
                count: offset_delta,
            };

            // The broker has already written the messages. A failure to print the line, such as
            // a closed pipe, is ignored instead of panicking in `println!`, because a non-zero
            // exit would make a caller that retries write the messages again.
            _ = writeln!(std::io::stdout(), "{produced}").inspect_err(|error| debug!(?error));
        }

        Ok(error_code)
    }
}

/// The line `cat produce` prints when the broker has written the messages.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Produced {
    topic: String,
    partition: i32,
    /// The offset of the first message written.
    base_offset: i64,
    /// The number of messages written.
    count: i32,
}

impl Display for Produced {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            topic,
            partition,
            base_offset,
            count,
        } = self;

        match count {
            0 => write!(f, "Produced 0 messages to {topic} partition {partition}."),
            1 => write!(
                f,
                "Produced 1 message to {topic} partition {partition}, offset {base_offset}."
            ),
            _ => write!(
                f,
                "Produced {count} messages to {topic} partition {partition}, offsets \
                {base_offset} to {}.",
                base_offset + i64::from(count - 1)
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Produced;

    fn produced(base_offset: i64, count: i32) -> String {
        Produced {
            topic: "taxi".into(),
            partition: 0,
            base_offset,
            count,
        }
        .to_string()
    }

    #[test]
    fn produced_gives_the_range_of_offsets() {
        assert_eq!(
            "Produced 4 messages to taxi partition 0, offsets 0 to 3.",
            produced(0, 4)
        );
    }

    #[test]
    fn produced_one_message_gives_its_offset() {
        assert_eq!(
            "Produced 1 message to taxi partition 0, offset 4.",
            produced(4, 1)
        );
    }

    #[test]
    fn produced_no_messages_gives_no_offset() {
        assert_eq!("Produced 0 messages to taxi partition 0.", produced(-1, 0));
    }
}
