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

mod de;
mod ser;

use super::varint::UnsignedVarInt;
use crate::{ByteSize, Result};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{SeqAccess, Visitor},
    ser::SerializeSeq,
};
use std::{
    any::{type_name, type_name_of_val},
    fmt::Formatter,
    io::Cursor,
    iter::{chain, once},
    ops::Deref,
};
use tracing::{debug, instrument};

const MAXIMUM_TAGGED_FIELDS: usize = 128;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TagField(pub u32, pub Vec<u8>);

impl TagField {
    pub(crate) fn tag(&self) -> u32 {
        self.0
    }

    pub(crate) fn data(&self) -> &[u8] {
        &self.1[..]
    }

    #[instrument(skip(field))]
    pub fn encode(tag: u32, field: &impl Serialize) -> Result<Self> {
        ser::Encoder::encode(field).map(|encoded| Self(tag, encoded))
    }
}

impl ByteSize for TagField {
    fn size_in_bytes(&self) -> Result<usize> {
        [
            UnsignedVarInt(self.tag()),
            UnsignedVarInt::try_from(self.data().len())?,
        ]
        .iter()
        .try_fold(self.data().len(), |acc, uvi| {
            uvi.size_in_bytes().map(|size_in_bytes| acc + size_in_bytes)
        })
    }
}

impl Serialize for TagField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        debug!(?self);

        let mut s = serializer.serialize_seq(None)?;

        s.serialize_element(&UnsignedVarInt::from(self.tag()))?;

        UnsignedVarInt::try_from(self.data().len())
            .map_err(|e| serde::ser::Error::custom(format!("length too big: {e:?}")))
            .and_then(|length| s.serialize_element(&length))?;

        for byte in self.data() {
            s.serialize_element(byte)?;
        }

        s.end()
    }
}

impl<'de> Deserialize<'de> for TagField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = TagField;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(Tag))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                debug!("seq={}", type_name_of_val(&seq));

                let tag: u32 = seq
                    .next_element::<UnsignedVarInt>()?
                    .ok_or_else(|| serde::de::Error::custom("tag"))?
                    .into();

                let length: usize = seq
                    .next_element::<UnsignedVarInt>()?
                    .ok_or_else(|| serde::de::Error::custom("length"))?
                    .into();

                if length > MAXIMUM_TAGGED_FIELDS {
                    return Err(serde::de::Error::custom(format!(
                        "maximum tagged fields exceeded {length}"
                    )));
                }

                (0..length)
                    .try_fold(Vec::with_capacity(length), |mut acc, _| {
                        seq.next_element::<u8>()?
                            .ok_or_else(|| serde::de::Error::custom("byte"))
                            .map(|byte| {
                                acc.push(byte);
                                acc
                            })
                    })
                    .inspect(|data| debug!(?tag, ?data))
                    .map(|data| TagField(tag, data))
            }
        }

        debug!("deserializer={}", type_name_of_val(&deserializer));
        deserializer.deserialize_seq(V)
    }
}

impl From<(u32, Vec<u8>)> for TagField {
    fn from(value: (u32, Vec<u8>)) -> Self {
        Self(value.0, value.1)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TagBuffer(pub Vec<TagField>);

impl TagBuffer {
    #[must_use]
    pub fn empty() -> Self {
        Self(vec![])
    }

    #[must_use]
    pub fn builder() -> TagBufferBuilder {
        TagBufferBuilder::default()
    }

    pub fn decode<T>(&self, tag: &u32) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        debug!("tag={tag} T={}", type_name::<T>());

        self.0
            .iter()
            .find(|TagField(found, _)| found == tag)
            .map_or_else(
                || Ok(None),
                |TagField(_, encoded)| {
                    let mut r = Cursor::new(encoded);
                    let mut decoder = de::Decoder::new(&mut r);
                    T::deserialize(&mut decoder).map(Some)
                },
            )
    }

    pub fn encode(tags: &[(u32, impl Serialize)]) -> Result<Self> {
        tags.iter()
            .map(|(tag, field)| {
                ser::Encoder::encode(field)
                    .map(|encoded| TagField(*tag, encoded))
                    .inspect(|tag_field| debug!(?tag_field))
            })
            .collect::<Result<Vec<_>>>()
            .map(Self)
    }
}

impl Deref for TagBuffer {
    type Target = Vec<TagField>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TagBufferBuilder {
    tags: Vec<TagField>,
}

impl TagBufferBuilder {
    #[must_use]
    pub fn tag(mut self, tag: u32, data: Vec<u8>) -> Self {
        self.tags.push(TagField(tag, data));
        self
    }

    #[must_use]
    pub fn build(self) -> TagBuffer {
        TagBuffer(self.tags)
    }
}

impl From<Vec<TagField>> for TagBuffer {
    fn from(value: Vec<TagField>) -> Self {
        Self(value)
    }
}

impl ByteSize for TagBuffer {
    fn size_in_bytes(&self) -> Result<usize> {
        chain(
            once(UnsignedVarInt::try_from(self.0.len()).and_then(|uvi| uvi.size_in_bytes())),
            self.0.iter().map(|tag| tag.size_in_bytes()),
        )
        .collect::<Result<Vec<_>>>()
        .map(|length| length.iter().sum::<usize>())
    }
}

impl Serialize for TagBuffer {
    #[instrument(skip_all)]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(None)?;

        UnsignedVarInt::try_from(self.0.len())
            .map_err(|e| serde::ser::Error::custom(format!("length too big: {e:?}")))
            .inspect(|length| debug!(?length))
            .and_then(|length| s.serialize_element(&length))?;

        for tagged_field in &self.0 {
            s.serialize_element(tagged_field)?;
        }

        s.end()
    }
}

impl<'de> Deserialize<'de> for TagBuffer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = TagBuffer;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(TagBuffer))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                debug!("seq={}", type_name_of_val(&seq));

                let number_of_tagged_fields: usize = seq
                    .next_element::<UnsignedVarInt>()?
                    .ok_or_else(|| serde::de::Error::custom("tag"))?
                    .into();

                debug!(?number_of_tagged_fields);

                if number_of_tagged_fields > MAXIMUM_TAGGED_FIELDS {
                    return Err(serde::de::Error::custom(format!(
                        "maximum tagged fields exceeded {number_of_tagged_fields}"
                    )));
                }

                (0..number_of_tagged_fields)
                    .try_fold(Vec::with_capacity(number_of_tagged_fields), |mut acc, _| {
                        seq.next_element::<TagField>()?
                            .ok_or_else(|| serde::de::Error::custom("tagged field"))
                            .inspect(|tag| debug!(?tag))
                            .map(|tag| {
                                acc.push(tag);
                                acc
                            })
                    })
                    .map(TagBuffer)
            }
        }

        debug!("deserializer={}", type_name_of_val(&deserializer));
        deserializer.deserialize_seq(V)
    }
}

#[cfg(test)]
mod tests {
    use super::{de::Decoder, ser::Encoder, *};

    #[ignore]
    #[test]
    fn empty() -> Result<()> {
        let expected = TagBuffer::builder().build();

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);

        expected.serialize(&mut encoder)?;

        let mut decoder = Decoder::new(&mut c);
        let actual = TagBuffer::deserialize(&mut decoder)?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[ignore]
    #[test]
    fn buffer_of1() -> Result<()> {
        let expected = TagBuffer::builder().tag(55, b"pqr".into()).build();

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);

        expected.serialize(&mut encoder)?;

        let mut decoder = Decoder::new(&mut c);
        let actual = TagBuffer::deserialize(&mut decoder)?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[ignore]
    #[test]
    fn buffer_of2() -> Result<()> {
        let expected = TagBuffer::builder()
            .tag(66, vec![5, 4, 3, 2, 1])
            .tag(88, b"abc".into())
            .build();

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);

        expected.serialize(&mut encoder)?;

        let mut decoder = Decoder::new(&mut c);
        let actual = TagBuffer::deserialize(&mut decoder)?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[ignore]
    #[test]
    fn api_versions_response_v3_000() -> Result<()> {
        let expected = TagBuffer::builder()
            .tag(
                0,
                vec![
                    2, 17, 109, 101, 116, 97, 100, 97, 116, 97, 46, 118, 101, 114, 115, 105, 111,
                    110, 0, 1, 0, 14, 0,
                ],
            )
            .tag(1, vec![0, 0, 0, 0, 0, 0, 0, 76])
            .tag(
                2,
                vec![
                    2, 17, 109, 101, 116, 97, 100, 97, 116, 97, 46, 118, 101, 114, 115, 105, 111,
                    110, 0, 14, 0, 14, 0,
                ],
            )
            .build();

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);

        expected.serialize(&mut encoder)?;

        let mut decoder = Decoder::new(&mut c);
        let actual = TagBuffer::deserialize(&mut decoder)?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[test]
    fn compact_string() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            value: String,
        }

        let expected = Example {
            value: "Hello World!".to_owned(),
        };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(
            encoded,
            vec![13, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 33]
        );

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Example::deserialize(&mut decoder)?);

        Ok(())
    }

    #[test]
    fn compact_string_empty() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            value: String,
        }

        let expected = Example {
            value: "".to_owned(),
        };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(encoded, vec![1]);

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Example::deserialize(&mut decoder)?);

        Ok(())
    }

    #[test]
    fn compact_nullable_string_none() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            value: Option<String>,
        }

        let expected = Example { value: None };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(encoded, vec![0]);

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Example::deserialize(&mut decoder)?);

        Ok(())
    }

    #[test]
    fn compact_nullable_string_some() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            value: Option<String>,
        }

        let expected = Example {
            value: Some("Hello World!".to_owned()),
        };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(
            encoded,
            vec![13, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 33]
        );

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Example::deserialize(&mut decoder)?);

        Ok(())
    }

    #[test]
    fn compact_array_of() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            value: Vec<i32>,
        }

        let expected = Example {
            value: [12321, 54345, 78987].into(),
        };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(encoded, vec![4, 0, 0, 48, 33, 0, 0, 212, 73, 0, 1, 52, 139]);

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Example::deserialize(&mut decoder)?);

        Ok(())
    }

    #[test]
    fn compact_array_of_empty() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            value: Vec<i32>,
        }

        let expected = Example { value: [].into() };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(encoded, vec![1]);

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Example::deserialize(&mut decoder)?);

        Ok(())
    }

    #[test]
    fn array_of() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Wrapper {
            value: Vec<Example>,
        }

        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            node_id: i32,
            host: String,
            port: i32,
            rack: String,
        }

        let expected = Wrapper {
            value: vec![Example {
                node_id: 32123,
                host: "abc".to_owned(),
                port: 98789,
                rack: "pqr".to_owned(),
            }],
        };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(
            encoded,
            vec![
                2, 0, 0, 125, 123, 4, 97, 98, 99, 0, 1, 129, 229, 4, 112, 113, 114
            ]
        );

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Wrapper::deserialize(&mut decoder)?);

        Ok(())
    }
}
