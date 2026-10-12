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

//! Tests that the broker's maintenance run deletes the records that a topic's retention configs
//! expire, and keeps the others. A user sets `retention.ms` or `retention.bytes` to bound how much
//! a topic stores. A broker that deletes too little fills its storage, and a broker that deletes
//! too much loses records that consumers haven't read yet.
//!
//! These tests run on PostgreSQL and SQLite, the engines that apply retention. Memory storage is
//! for development, and it doesn't apply retention. Each test launches a broker that runs
//! maintenance every 2 seconds, and produces records with a timestamp an hour old, so that
//! retention has expired them already.

#![cfg(any(feature = "postgres", feature = "sqlite"))]

use std::time::Duration;

use nisshi_smoke_test::{Broker, KafkaCli, now_in_millis, wait_until};

/// How long a test waits for a maintenance run to give the result it expects.
const MAINTENANCE_TIMEOUT: Duration = Duration::from_secs(60);

/// A retention that has expired a record an hour old, and keeps a record produced during the test
/// for ten minutes, longer than any test runs.
const TEN_MINUTE_RETENTION: &str = "retention.ms=600000";

const DELETE_POLICY: &str = "cleanup.policy=delete";

/// How many expired records a test produces first, at offsets 0, 1 and 2.
const EXPIRED_RECORD_COUNT: usize = 3;

/// Produces `count` records to `topic` with a timestamp an hour old.
fn produce_hour_old_records(cli: &KafkaCli, topic: &str, count: usize) {
    let an_hour_ago = now_in_millis() - 3_600_000;
    let produced = cli.verifiable_produce_created_at(topic, count, an_hour_ago, None);

    assert_eq!(produced.acknowledged_record_count(), count, "{produced}");
}

fn earliest_offset(cli: &KafkaCli, topic: &str) -> i64 {
    cli.partition_offsets_at(topic, "earliest")[0]
}

fn latest_offset(cli: &KafkaCli, topic: &str) -> i64 {
    cli.partition_offsets_at(topic, "latest")[0]
}

/// Waits until `topic`'s earliest offset is `expected`, which shows that a maintenance run deleted
/// the records before it. Panics if that doesn't happen within [`MAINTENANCE_TIMEOUT`].
fn wait_until_earliest_offset_is(cli: &KafkaCli, topic: &str, expected: i64) {
    wait_until(MAINTENANCE_TIMEOUT, || match earliest_offset(cli, topic) {
        earliest if earliest == expected => Ok(()),
        earliest => Err(format!(
            "{topic}'s earliest offset is {earliest}, not {expected}"
        )),
    });
}

/// Asserts that `topic` still holds the `count` records produced to it, at offsets from 0.
///
/// The test reads the records instead of checking that the earliest offset is still 0, because
/// the broker also reports 0 for a partition that retention emptied.
#[track_caller]
fn assert_every_record_kept(cli: &KafkaCli, topic: &str, count: usize) {
    let offsets = cli
        .read_records(topic, 0, 0, count)
        .into_iter()
        .map(|record| record.offset)
        .collect::<Vec<_>>();

    assert_eq!(offsets, (0..count as i64).collect::<Vec<_>>());
}

/// Waits until a maintenance run has deleted an expired record from a topic of its own.
///
/// A test that expects a topic to keep its records can't wait for the records to go. It calls
/// this after it produces them, and then checks them. Each maintenance run applies retention to
/// every topic of the cluster at once, so the run that deleted this record also checked the
/// records that the test produced before it.
fn wait_for_maintenance_run(cli: &KafkaCli) {
    let topic = cli.create_unique_topic(1, &[DELETE_POLICY, TEN_MINUTE_RETENTION]);
    produce_hour_old_records(cli, &topic, 1);
    // The topic keeps a newer record, so retention never empties it. The broker reports the
    // earliest offset of an empty partition as 0, and the wait would then never end.
    _ = cli.produce_values(&topic, &["new-0"]);
    wait_until_earliest_offset_is(cli, &topic, 1);
}

/// Retention must delete the records older than `retention.ms`, and keep the newer ones that
/// consumers still need. The earliest offset moves to the first record it keeps.
#[test]
fn expired_records_are_deleted_and_newer_records_kept() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.create_unique_topic(1, &[DELETE_POLICY, TEN_MINUTE_RETENTION]);
    produce_hour_old_records(&cli, &topic, EXPIRED_RECORD_COUNT);
    _ = cli.produce_values(&topic, &["new-0", "new-1"]);

    wait_until_earliest_offset_is(&cli, &topic, EXPIRED_RECORD_COUNT as i64);

    let kept_records = cli.read_records_from_earliest(&topic, 0, 2);
    let kept_offsets = kept_records.iter().map(|record| record.offset);
    let kept_values = kept_records.iter().map(|record| record.value.as_str());
    assert_eq!(kept_offsets.collect::<Vec<_>>(), [3, 4]);
    assert_eq!(kept_values.collect::<Vec<_>>(), ["new-0", "new-1"]);
}

/// After retention deletes every record of a partition, the broker must give the next record the
/// offset after the last one it stored. A broker that numbers from the records that remain starts
/// again at 0, and a consumer that already read offsets 0 to 2 skips the new records.
#[test]
fn record_produced_after_retention_gets_the_next_offset() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.create_unique_topic(1, &[DELETE_POLICY, TEN_MINUTE_RETENTION]);
    produce_hour_old_records(&cli, &topic, EXPIRED_RECORD_COUNT);
    wait_for_maintenance_run(&cli);

    _ = cli.produce_values(&topic, &["after-retention"]);

    // The read starts at the earliest offset, so it also shows that retention deleted the
    // expired records, and the partition was empty before the new record.
    assert_eq!(
        cli.read_records_from_earliest(&topic, 0, 1)
            .into_iter()
            .map(|record| (record.offset, record.value))
            .collect::<Vec<_>>(),
        [(EXPIRED_RECORD_COUNT as i64, "after-retention".to_owned())]
    );
}

/// When retention deletes every record of a partition, the earliest and the latest offset must
/// both be the offset after the last deleted record, as Kafka reports them. A consumer group's
/// committed offset is at that offset, and a latest offset that goes back makes the group's
/// offset look out of range, so the group starts again from the beginning or the end.
#[test]
#[ignore = "the broker reports offsets 0 for a partition that retention emptied (#864)"]
fn offsets_are_kept_when_retention_empties_a_partition() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.create_unique_topic(1, &[DELETE_POLICY, TEN_MINUTE_RETENTION]);
    produce_hour_old_records(&cli, &topic, EXPIRED_RECORD_COUNT);

    wait_for_maintenance_run(&cli);

    assert_eq!(
        (earliest_offset(&cli, &topic), latest_offset(&cli, &topic)),
        (EXPIRED_RECORD_COUNT as i64, EXPIRED_RECORD_COUNT as i64)
    );
}

/// Kafka's default `cleanup.policy` is `delete`, so a topic that sets only `retention.ms` must be
/// cleaned the same way. Users often set only `retention.ms`.
#[test]
#[ignore = "retention deletes records only from a topic that sets cleanup.policy (#918)"]
fn topic_without_cleanup_policy_is_cleaned_by_retention() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.create_unique_topic(1, &[TEN_MINUTE_RETENTION]);
    produce_hour_old_records(&cli, &topic, EXPIRED_RECORD_COUNT);
    _ = cli.produce_values(&topic, &["new-0"]);

    wait_until_earliest_offset_is(&cli, &topic, EXPIRED_RECORD_COUNT as i64);
}

/// `retention.ms=-1` means that the topic keeps every record, however old. A user sets it on a
/// topic that is the only copy of its data.
#[test]
#[ignore = "retention.ms=-1 makes retention delete every record (#919)"]
fn retention_ms_minus_1_keeps_every_record() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.create_unique_topic(1, &[DELETE_POLICY, "retention.ms=-1"]);
    produce_hour_old_records(&cli, &topic, EXPIRED_RECORD_COUNT);

    wait_for_maintenance_run(&cli);

    assert_every_record_kept(&cli, &topic, EXPIRED_RECORD_COUNT);
}

/// A topic's `retention.ms` of 30 days must not stop retention on the broker's other topics. The
/// value is larger than a 32-bit integer holds, and a broker that fails on it never deletes
/// another expired record, so its storage grows until it is full.
#[test]
#[cfg_attr(
    feature = "postgres",
    ignore = "a retention.ms larger than a 32-bit integer stops retention on every topic (#920)"
)]
fn long_retention_on_one_topic_does_not_stop_retention_on_another() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let thirty_day_topic = cli.create_unique_topic(1, &[DELETE_POLICY, "retention.ms=2592000000"]);
    // The topic holds a record, so that the maintenance run checks the record's age against the
    // topic's `retention.ms`.
    _ = cli.produce_values(&thirty_day_topic, &["new-0"]);

    wait_for_maintenance_run(&cli);
}

/// A topic with a `retention.ms` of 30 days must keep a new record. A broker that reads the value
/// into a 32-bit integer gets a negative or a small retention, and deletes the record at once.
#[test]
#[cfg_attr(
    feature = "postgres",
    ignore = "a retention.ms larger than a 32-bit integer stops retention on every topic (#920)"
)]
fn thirty_day_retention_keeps_a_new_record() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let thirty_day_topic = cli.create_unique_topic(1, &[DELETE_POLICY, "retention.ms=2592000000"]);
    _ = cli.produce_values(&thirty_day_topic, &["new-0"]);

    wait_for_maintenance_run(&cli);

    assert_every_record_kept(&cli, &thirty_day_topic, 1);
}

/// A `retention.ms` added to an existing topic with `kafka-configs` must apply from the next
/// maintenance run. A user lowers it to free storage at once. The topic keeps its records under
/// the default retention of 7 days first, so the test shows that the added config deleted them.
#[test]
fn added_retention_ms_deletes_older_records() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.create_unique_topic(1, &[DELETE_POLICY]);
    produce_hour_old_records(&cli, &topic, EXPIRED_RECORD_COUNT);
    _ = cli.produce_values(&topic, &["new-0"]);
    wait_for_maintenance_run(&cli);
    assert_every_record_kept(&cli, &topic, EXPIRED_RECORD_COUNT + 1);

    _ = cli
        .add_topic_configs(&topic, &[TEN_MINUTE_RETENTION])
        .succeeded();

    wait_until_earliest_offset_is(&cli, &topic, EXPIRED_RECORD_COUNT as i64);
}

/// Deleting a topic's `retention.ms` with `kafka-configs` must restore the default of 7 days, which
/// keeps a record an hour old. A broker that keeps applying the deleted value deletes records the
/// user meant to keep.
#[test]
#[ignore = "a topic's own configs are reported as defaults, so kafka-configs refuses to \
            delete them (#904)"]
fn deleted_retention_ms_restores_the_default() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.create_unique_topic(1, &[DELETE_POLICY, TEN_MINUTE_RETENTION]);
    _ = cli
        .delete_topic_configs(&topic, &["retention.ms"])
        .succeeded();
    produce_hour_old_records(&cli, &topic, EXPIRED_RECORD_COUNT);

    wait_for_maintenance_run(&cli);

    assert_every_record_kept(&cli, &topic, EXPIRED_RECORD_COUNT);
}

/// Retention must keep a partition within its topic's `retention.bytes`, by deleting the oldest
/// records, and keep the newest ones. A user sets it to bound a topic's storage, whatever the
/// records' age.
#[test]
#[ignore = "retention ignores retention.bytes (#921)"]
fn retention_bytes_keeps_partition_under_the_cap() {
    /// The size of each record's value. The broker stores more bytes than this per record, so a
    /// partition under the cap holds at most `RETENTION_BYTES / VALUE_BYTES` records.
    const VALUE_BYTES: usize = 1000;
    const RETENTION_BYTES: usize = 5 * VALUE_BYTES;
    const PRODUCED_RECORD_COUNT: usize = 20;

    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let retention_bytes = format!("retention.bytes={RETENTION_BYTES}");
    let topic = cli.create_unique_topic(1, &[DELETE_POLICY, &retention_bytes]);
    let values = (0..PRODUCED_RECORD_COUNT)
        .map(|n| format!("{n:0>VALUE_BYTES$}"))
        .collect::<Vec<_>>();
    _ = cli.produce_values(&topic, &values);

    // A broker can delete the records in several runs, as Kafka does a segment at a time, so the
    // test waits until the partition is under the cap, not for the first deletion.
    wait_until(MAINTENANCE_TIMEOUT, || {
        let kept_record_count = PRODUCED_RECORD_COUNT as i64 - earliest_offset(&cli, &topic);

        if kept_record_count <= (RETENTION_BYTES / VALUE_BYTES) as i64 {
            Ok(())
        } else {
            Err(format!(
                "{topic} keeps {kept_record_count} records of {VALUE_BYTES} bytes, with \
                 retention.bytes of {RETENTION_BYTES}"
            ))
        }
    });

    let newest_offset = PRODUCED_RECORD_COUNT as u64 - 1;
    assert_eq!(
        cli.read_records(&topic, 0, newest_offset, 1)[0].value,
        values[PRODUCED_RECORD_COUNT - 1]
    );
}
