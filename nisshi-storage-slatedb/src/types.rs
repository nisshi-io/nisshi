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

//! Type definitions for SlateDB storage engine
//!
//! # Key Design for LSM-tree
//!
//! All keys follow a consistent pattern optimized for LSM-tree storage:
//!
//! ```text
//! {type_prefix}/{hierarchy...}/{leaf_id}
//! ```
//!
//! ## Key Prefixes (sorted by access pattern)
//!
//! | Prefix | Description | Key Structure |
//! |--------|-------------|---------------|
//! | `b/` | Batch data | `b/{topic_uuid}/{partition:be32}/{offset:be64}` |
//! | `c/` | Consumer group commits | `c/{group}/{topic}/{partition:be32}` |
//! | `g/` | Group state | `g/{group_id}` |
//! | `t/` | Time index | `t/{topic_uuid}/{partition:be32}/{max_timestamp:be64}` |
//! | `u/` | SCRAM credentials | `u/{user}/{mechanism:be32}` |
//! | `w/` | Watermarks | `w/{topic_uuid}/{partition:be32}` |
//!
//! ## Design Principles
//!
//! 1. **Prefix-first**: Type prefix comes first for efficient filtering
//! 2. **Big-endian integers**: Preserves numeric ordering in lexicographic sort
//! 3. **Fixed-width encoding**: Ensures consistent key ordering
//! 4. **Hierarchical structure**: Enables efficient prefix scans
//!
//! ## LSM-tree Considerations
//!
//! - Keys with same prefix are stored together → better compaction
//! - Bloom filters can efficiently skip unrelated key types
//! - Range scans for a partition only touch relevant SSTable blocks

use std::{collections::BTreeMap, time::SystemTime};

use bytes::Bytes;
use nisshi_sans_io::{ScramMechanism, create_topics_request::CreatableTopic};
use nisshi_storage::{GroupDetail, ScramCredential, TxnState, Version};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Type aliases
pub(super) type Group = String;
pub(super) type Offset = i64;
pub(super) type Partition = i32;
pub(super) type ProducerEpoch = i16;
pub(super) type ProducerId = i64;
pub(super) type Sequence = i32;
pub(super) type Topic = String;

// Collection types
pub(super) type Topics = BTreeMap<Topic, TopicMetadata>;
pub(super) type Producers = BTreeMap<ProducerId, ProducerDetail>;
pub(super) type Brokers = BTreeMap<i32, BrokerInfo>;
pub(super) type Transactions = BTreeMap<String, Txn>;

/// Transaction produce offset range
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub(super) struct TxnProduceOffset {
    pub offset_start: Offset,
    pub offset_end: Offset,
}

/// Transaction commit offset
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TxnCommitOffset {
    pub committed_offset: Offset,
    pub leader_epoch: Option<i32>,
    pub metadata: Option<String>,
}

/// Topic metadata
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TopicMetadata {
    pub id: Uuid,
    pub topic: CreatableTopic,
}

/// Watermark for a topic partition
///
/// `timestamps` is a deprecated field, kept at its original (3rd) position
/// purely so that decoding legacy (pre-time-index) bytes can be detected: a
/// postcard decode of this (now 5-field) shape against legacy 3-field bytes
/// fails, which is the signal `Engine::partition_watermark`/`decode_watermark`
/// use to fall back to [`WatermarkLegacy`] and (on a write path) backfill the
/// time index. Nothing writes to `timestamps` any more; the time index lives
/// in the `t/` keyspace (see [`TimeIndexKey`]), keyed correctly by
/// `max_timestamp` rather than this field's old (and buggy) `base_timestamp`
/// keying.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct Watermark {
    pub low: Option<i64>,
    pub high: Option<i64>,
    pub timestamps: Option<BTreeMap<i64, i64>>,
    /// The greatest `max_timestamp` ever appended to the `t/` time index for
    /// this partition, via the monotonic "maybeAppend" rule matching Kafka's
    /// own `TimeIndex` (floored at `NO_TIMESTAMP = -1`, so a negative
    /// timestamp is never indexed/never advances this). `None` means the
    /// index holds nothing for this partition: either it is genuinely empty,
    /// or every batch so far had a `max_timestamp <= -1`.
    pub latest_indexed_timestamp: Option<i64>,
    /// The `max_timestamp` header of the most recently appended batch,
    /// unconditionally (not floored, not gated by the monotonic rule above).
    /// Lets `ListOffsets(Latest)` answer its timestamp in O(1) from the
    /// watermark alone, instead of a `batch_base_at_or_before` binary search
    /// on every call (`ListOffsets(Latest)` is a consumer hot path).
    pub last_batch_max_timestamp: Option<i64>,
}

/// The pre-time-index, 3-field shape of [`Watermark`]. A stored watermark
/// that fails to decode as the current shape is re-parsed as this shape
/// (ANY decode error, not just a specific error kind: any shape mismatch
/// means "not the current format"). See [`Watermark`] for why this works.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct WatermarkLegacy {
    pub low: Option<i64>,
    pub high: Option<i64>,
    pub timestamps: Option<BTreeMap<i64, i64>>,
}

/// Key for watermark storage: `w/{topic_uuid}/{partition:be32}`
///
/// Watermarks are accessed per-partition, so we use topic+partition as the key.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct WatermarkKey {
    /// Type prefix for LSM-tree grouping
    pub prefix: char,
    /// Topic UUID (16 bytes, fixed)
    pub topic: Uuid,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
}

impl Default for WatermarkKey {
    fn default() -> Self {
        Self {
            prefix: 'w',
            topic: Uuid::nil(),
            partition: 0,
        }
    }
}

impl WatermarkKey {
    pub(super) fn new(topic: Uuid, partition: Partition) -> Self {
        Self {
            prefix: 'w',
            topic,
            partition,
        }
    }
}

/// Group detail with version
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct GroupDetailVersion {
    pub detail: GroupDetail,
    pub version: Version,
}

impl GroupDetailVersion {
    pub(super) fn detail(self, detail: GroupDetail) -> Self {
        Self { detail, ..self }
    }

    pub(super) fn version(self, version: Version) -> Self {
        Self { version, ..self }
    }
}

/// Producer detail with sequence tracking
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct ProducerDetail {
    pub sequences: BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, Sequence>>>,
}

/// Transaction identifier
#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TxnId {
    pub transaction: String,
    pub producer_id: ProducerId,
    pub producer_epoch: ProducerEpoch,
    pub state: TxnState,
}

/// Transaction state
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct Txn {
    pub producer: ProducerId,
    pub epochs: BTreeMap<ProducerEpoch, TxnDetail>,
}

/// Transaction detail
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TxnDetail {
    pub transaction_timeout_ms: i32,
    pub started_at: Option<SystemTime>,
    pub state: Option<TxnState>,
    pub produces: BTreeMap<Topic, BTreeMap<Partition, Option<TxnProduceOffset>>>,
    pub offsets: BTreeMap<Group, BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>>,
}

/// Key for batch storage: `b/{topic_uuid}/{partition:be32}/{offset:be64}`
///
/// Batches are the most frequently accessed data. The key structure enables:
/// - Efficient sequential reads by offset (big-endian preserves order)
/// - Prefix scan for all batches in a partition
/// - Bloom filter can quickly skip non-batch keys
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct BatchKey {
    /// Type prefix 'b' for batch
    pub prefix: char,
    /// Topic UUID (16 bytes, fixed)
    pub topic: Uuid,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
    /// Offset within partition (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub offset: Offset,
}

impl Default for BatchKey {
    fn default() -> Self {
        Self {
            prefix: 'b',
            topic: Uuid::nil(),
            partition: 0,
            offset: 0,
        }
    }
}

impl BatchKey {
    pub(super) fn new(topic: Uuid, partition: Partition, offset: Offset) -> Self {
        Self {
            prefix: 'b',
            topic,
            partition,
            offset,
        }
    }

    /// Create a key for range scan starting from this offset
    pub(super) fn scan_from(topic: Uuid, partition: Partition, offset: Offset) -> Self {
        Self::new(topic, partition, offset)
    }
}

/// Prefix key for scanning all batches in a topic partition: `b/{topic_uuid}/{partition}`
///
/// This is a separate struct from `BatchKey` because we need to check if a scanned key
/// still belongs to the same topic/partition before decoding the batch data.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct BatchKeyPrefix {
    /// Type prefix 'b' for batch
    pub prefix: char,
    /// Topic UUID (16 bytes, fixed)
    pub topic: Uuid,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
}

impl BatchKeyPrefix {
    pub(super) fn new(topic: Uuid, partition: Partition) -> Self {
        Self {
            prefix: 'b',
            topic,
            partition,
        }
    }
}

/// Key for the time index: `t/{topic_uuid}/{partition:be32}/{max_timestamp:be64}`
///
/// Maps a batch's `max_timestamp` to its `base_offset` (the value, a plain
/// postcard-encoded `i64`). Maintained by the monotonic "maybeAppend" rule
/// from Kafka's own `TimeIndex`: an entry is appended only when its
/// `max_timestamp` is strictly greater than every one appended so far for
/// the partition (see `Engine::append_time_index`). A negative timestamp is
/// never indexed (floored at Kafka's `NO_TIMESTAMP = -1`): with raw
/// `fixint::be` two's-complement encoding, a negative i64 sorts AFTER every
/// positive i64 in byte order, so indexing one would corrupt the ceiling
/// range-scan that `ListOffsets(Timestamp)` depends on.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TimeIndexKey {
    /// Type prefix 't' for time index
    pub prefix: char,
    /// Topic UUID (16 bytes, fixed)
    pub topic: Uuid,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
    /// Batch max_timestamp (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub timestamp: i64,
}

impl TimeIndexKey {
    pub(super) fn new(topic: Uuid, partition: Partition, timestamp: i64) -> Self {
        Self {
            prefix: 't',
            topic,
            partition,
            timestamp,
        }
    }

    /// Create a key for range scan starting from this timestamp
    pub(super) fn scan_from(topic: Uuid, partition: Partition, timestamp: i64) -> Self {
        Self::new(topic, partition, timestamp)
    }
}

/// Prefix key for scanning all time index entries in a topic partition:
/// `t/{topic_uuid}/{partition}`
///
/// Separate from `TimeIndexKey` for the same reason as `BatchKeyPrefix`: a
/// scanned key must be checked against this prefix before being decoded as
/// a full `TimeIndexKey`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TimeIndexKeyPrefix {
    /// Type prefix 't' for time index
    pub prefix: char,
    /// Topic UUID (16 bytes, fixed)
    pub topic: Uuid,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
}

impl TimeIndexKeyPrefix {
    pub(super) fn new(topic: Uuid, partition: Partition) -> Self {
        Self {
            prefix: 't',
            topic,
            partition,
        }
    }
}

/// Key for storing committed offsets: `c/{group}/{topic}/{partition:be32}`
///
/// Consumer group offsets are accessed by group, then by topic-partition.
/// This enables efficient "get all offsets for a group" scans.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct OffsetCommitKey {
    /// Type prefix 'c' for commit
    pub prefix: char,
    /// Consumer group ID
    pub group: String,
    /// Topic name
    pub topic: String,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
}

impl Default for OffsetCommitKey {
    fn default() -> Self {
        Self {
            prefix: 'c',
            group: String::new(),
            topic: String::new(),
            partition: 0,
        }
    }
}

impl OffsetCommitKey {
    pub(super) fn new(
        group: impl Into<String>,
        topic: impl Into<String>,
        partition: Partition,
    ) -> Self {
        Self {
            prefix: 'c',
            group: group.into(),
            topic: topic.into(),
            partition,
        }
    }
}

/// Prefix key for scanning all offsets in a consumer group: `c/{group}`
///
/// This is a separate struct from `OffsetCommitKey` because postcard serialization
/// includes length prefixes for strings, so we can't use an `OffsetCommitKey` with
/// empty topic as a scan prefix - it would include the empty string's length marker.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct OffsetCommitKeyPrefix {
    /// Type prefix 'c' for commit
    pub prefix: char,
    /// Consumer group ID
    pub group: String,
}

impl OffsetCommitKeyPrefix {
    pub(super) fn new(group: impl Into<String>) -> Self {
        Self {
            prefix: 'c',
            group: group.into(),
        }
    }
}

/// Value stored for offset commits
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct OffsetCommitValue {
    pub offset: i64,
    pub leader_epoch: Option<i32>,
    pub metadata: Option<String>,
}

/// Key for storing group state: `g/{group_id}`
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct GroupKey {
    /// Type prefix 'g' for group
    pub prefix: char,
    /// Group ID
    pub group_id: String,
}

impl Default for GroupKey {
    fn default() -> Self {
        Self {
            prefix: 'g',
            group_id: String::new(),
        }
    }
}

impl GroupKey {
    pub(super) fn new(group_id: impl Into<String>) -> Self {
        Self {
            prefix: 'g',
            group_id: group_id.into(),
        }
    }
}

/// Prefix key for scanning all groups: `g`
///
/// This is a separate struct from `GroupKey` because postcard serialization
/// includes length prefixes for strings, so we can't use a `GroupKey` with
/// empty group_id as a scan prefix.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct GroupKeyPrefix {
    /// Type prefix 'g' for group
    pub prefix: char,
}

impl GroupKeyPrefix {
    pub(super) fn new() -> Self {
        Self { prefix: 'g' }
    }
}

/// Stored broker information
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct BrokerInfo {
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

/// Key for storing SASL/SCRAM credentials: `u/{user}/{mechanism:be32}`
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct UserScramCredentialKey {
    /// Type prefix 'u' for user credential
    pub prefix: char,
    /// SASL user name
    pub user: String,
    /// SCRAM mechanism (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub mechanism: i32,
}

impl UserScramCredentialKey {
    pub(super) fn new(user: impl Into<String>, mechanism: ScramMechanism) -> Self {
        Self {
            prefix: 'u',
            user: user.into(),
            mechanism: i32::from(mechanism),
        }
    }
}

/// Value stored for SASL/SCRAM credentials
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct StoredScramCredential {
    pub salt: Vec<u8>,
    pub iterations: i32,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

impl From<ScramCredential> for StoredScramCredential {
    fn from(credential: ScramCredential) -> Self {
        Self {
            salt: credential.salt.to_vec(),
            iterations: credential.iterations,
            stored_key: credential.stored_key.to_vec(),
            server_key: credential.server_key.to_vec(),
        }
    }
}

impl From<StoredScramCredential> for ScramCredential {
    fn from(stored: StoredScramCredential) -> Self {
        Self {
            salt: Bytes::from(stored.salt),
            iterations: stored.iterations,
            stored_key: Bytes::from(stored.stored_key),
            server_key: Bytes::from(stored.server_key),
        }
    }
}
