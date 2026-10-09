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

//! List-typed configuration, as altered by the `APPEND` and `SUBTRACT`
//! operations of [`IncrementalAlterConfigsRequest`](nisshi_sans_io::IncrementalAlterConfigsRequest).

use std::collections::{HashMap, HashSet};

use nisshi_sans_io::{ConfigResource, ErrorCode, OpType};

use crate::{Error, Result};

/// The topic configuration keys whose type is `LIST`.
///
/// Kafka defines them in `LogConfig`:
/// [`cleanup.policy`](https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java#L295),
/// [`leader.replication.throttled.replicas` and `follower.replication.throttled.replicas`](https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java#L320-L323).
pub const TOPIC_LIST_CONFIGS: [&str; 3] = [
    "cleanup.policy",
    "leader.replication.throttled.replicas",
    "follower.replication.throttled.replicas",
];

/// Whether `key` is a `LIST` configuration of `resource`, the only kind that
/// `APPEND` and `SUBTRACT` may alter.
pub fn is_list_config(resource: ConfigResource, key: &str) -> bool {
    resource == ConfigResource::Topic && TOPIC_LIST_CONFIGS.contains(&key)
}

/// Returns the value of the configuration `key` of `resource` after applying
/// `op` with `value` to `current`, or `None` when `op` removes the key.
///
/// `APPEND` and `SUBTRACT` follow Kafka's
/// [`incrementalAlterConfigResource`](https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java#L232-L250)
/// and [`getParts`](https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java#L389-L404):
///
/// - `APPEND` adds each item of `value` that isn't already present;
/// - `SUBTRACT` removes the first occurrence of each item of `value`.
///
/// An unset `current` is an empty list. Kafka uses the default of `key`
/// instead, which makes `APPEND compact` to an unset `cleanup.policy` give
/// `delete,compact`. Nisshi deletes old records only when `cleanup.policy`
/// is set and contains `delete`, so that default would turn on deletion the
/// client didn't ask for.
///
/// # Errors
///
/// Returns [`ErrorCode::InvalidConfig`] for `APPEND` or `SUBTRACT` on a key
/// that [isn't a list](is_list_config).
pub fn apply_op(
    resource: ConfigResource,
    key: &str,
    current: Option<&str>,
    op: OpType,
    value: Option<&str>,
) -> Result<Option<String>> {
    let append = match op {
        OpType::Set => return Ok(value.map(ToOwned::to_owned)),
        OpType::Delete => return Ok(None),
        OpType::Append => true,
        OpType::Subtract => false,
    };

    if !is_list_config(resource, key) {
        return Err(Error::Api(ErrorCode::InvalidConfig));
    }

    let mut items: Vec<&str> = current
        .unwrap_or_default()
        .split(',')
        .filter(|item| !item.is_empty())
        .collect();

    // Kafka keeps an empty item of `value` (from `a,,b`) and then rejects the
    // result when it validates the new value. We ignore an empty item instead,
    // because Nisshi doesn't validate values and would store it.
    let changes = value
        .unwrap_or_default()
        .split(',')
        .filter(|item| !item.is_empty());

    // The sets keep the cost linear in the number of items: a client sets the
    // size of `value`, and the backends apply it inside a write transaction.
    if append {
        let mut present: HashSet<&str> = items.iter().copied().collect();

        for item in changes {
            if present.insert(item) {
                items.push(item);
            }
        }
    } else {
        let mut removals: HashMap<&str, usize> = HashMap::new();

        for item in changes {
            *removals.entry(item).or_default() += 1;
        }

        items.retain(|item| match removals.get_mut(item) {
            Some(remaining) if *remaining > 0 => {
                *remaining -= 1;
                false
            }
            _ => true,
        });
    }

    Ok(Some(items.join(",")))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLEANUP_POLICY: &str = "cleanup.policy";

    fn apply(current: Option<&str>, op: OpType, value: &str) -> Result<Option<String>> {
        apply_op(
            ConfigResource::Topic,
            CLEANUP_POLICY,
            current,
            op,
            Some(value),
        )
    }

    #[test]
    fn list_configs_are_topic_only() {
        assert!(is_list_config(ConfigResource::Topic, CLEANUP_POLICY));
        assert!(is_list_config(
            ConfigResource::Topic,
            "follower.replication.throttled.replicas"
        ));
        assert!(!is_list_config(ConfigResource::Topic, "retention.ms"));
        assert!(!is_list_config(ConfigResource::Broker, CLEANUP_POLICY));
    }

    #[test]
    fn set_and_delete() -> Result<()> {
        assert_eq!(
            Some("compact"),
            apply(Some("delete"), OpType::Set, "compact")?.as_deref()
        );
        assert_eq!(None, apply(Some("delete"), OpType::Delete, "compact")?);
        Ok(())
    }

    #[test]
    fn set_and_delete_any_key() -> Result<()> {
        assert_eq!(
            Some("1000"),
            apply_op(
                ConfigResource::Topic,
                "retention.ms",
                None,
                OpType::Set,
                Some("1000")
            )?
            .as_deref()
        );
        Ok(())
    }

    #[test]
    fn append_or_subtract_non_list_is_invalid_config() {
        for op in [OpType::Append, OpType::Subtract] {
            assert!(matches!(
                apply_op(
                    ConfigResource::Topic,
                    "retention.ms",
                    None,
                    op,
                    Some("1000")
                ),
                Err(Error::Api(ErrorCode::InvalidConfig))
            ));
        }
    }

    #[test]
    fn append_to_unset_does_not_add_kafka_default() -> Result<()> {
        assert_eq!(
            Some("compact"),
            apply(None, OpType::Append, "compact")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn append_present_item_is_unchanged() -> Result<()> {
        assert_eq!(
            Some("delete,compact"),
            apply(Some("delete,compact"), OpType::Append, "compact")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn append_multiple_items() -> Result<()> {
        assert_eq!(
            Some("x,a,b"),
            apply(Some("x"), OpType::Append, "a,b,a")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn append_keeps_existing_duplicates() -> Result<()> {
        assert_eq!(
            Some("a,a,b"),
            apply(Some("a,a"), OpType::Append, "a,b")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn subtract_to_empty() -> Result<()> {
        assert_eq!(
            Some(""),
            apply(Some("delete"), OpType::Subtract, "delete")?.as_deref()
        );
        assert_eq!(
            Some(""),
            apply(None, OpType::Subtract, "delete")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn subtract_multiple_items() -> Result<()> {
        assert_eq!(
            Some("b"),
            apply(Some("a,b,c"), OpType::Subtract, "a,c")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn subtract_removes_first_occurrence() -> Result<()> {
        assert_eq!(
            Some("b,a"),
            apply(Some("a,b,a"), OpType::Subtract, "a")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn subtract_repeated_item_removes_as_many_occurrences() -> Result<()> {
        assert_eq!(
            Some("b,a"),
            apply(Some("a,b,a,a"), OpType::Subtract, "a,a")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn subtract_absent_item_is_unchanged() -> Result<()> {
        assert_eq!(
            Some("compact"),
            apply(Some("compact"), OpType::Subtract, "delete")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn empty_items_are_ignored() -> Result<()> {
        assert_eq!(
            Some("a,b"),
            apply(None, OpType::Append, "a,,b,")?.as_deref()
        );

        assert_eq!(
            Some("delete"),
            apply(Some("delete,,"), OpType::Subtract, ",")?.as_deref()
        );
        Ok(())
    }

    #[test]
    fn large_append_and_subtract() -> Result<()> {
        let value = (0..100_000)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let appended = apply(None, OpType::Append, &value)?;
        assert_eq!(Some(value.as_str()), appended.as_deref());

        assert_eq!(
            Some(""),
            apply(appended.as_deref(), OpType::Subtract, &value)?.as_deref()
        );
        Ok(())
    }
}
