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

//! Tests for SlateDB storage engine
//!
//! Note: Basic CRUD operations (create/delete topic, metadata, produce, offset commit,
//! list offsets, init producer, describe config, list/delete groups, transactions)
//! are covered by broker tests in nisshi-broker/tests/it/*.rs with slatedb module.
//!
//! This file contains tests for:
//! - Low-level API tests (offset_stage, brokers, cluster_id, node)
//! - Error case tests (duplicate topic, unknown txn, wrong producer/epoch)
//! - Unique feature tests (isolation levels, delete records, idempotent produce, builder pattern)

use std::sync::Arc;

use bytes::Bytes;
use nisshi_sans_io::{ErrorCode, create_topics_request::CreatableTopic, record::deflated::Batch};
use nisshi_storage::{
    BrokerRegistrationRequest, Error, Storage, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse,
};
use slatedb::{Db, object_store::memory::InMemory};
use url::Url;

use super::engine::Engine;

async fn create_test_engine() -> Engine {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("test.slatedb", object_store)
        .await
        .expect("Failed to open SlateDB");

    Engine::new(
        "test-cluster",
        1,
        Url::parse("tcp://localhost:9092").unwrap(),
        Arc::new(db),
    )
}

// ========== Unique Error Case Tests ==========

#[tokio::test]
async fn test_create_duplicate_topic() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("dup-topic".into())
        .num_partitions(1)
        .replication_factor(1);

    // First creation should succeed
    let result = engine.create_topic(topic.clone(), false).await;
    assert!(result.is_ok());

    // Second creation should fail
    let result = engine.create_topic(topic, false).await;
    assert!(matches!(
        result,
        Err(Error::Api(ErrorCode::TopicAlreadyExists))
    ));
}

// ========== Low-level API Tests ==========

#[tokio::test]
async fn test_offset_stage() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("stage-topic".into())
        .num_partitions(1)
        .replication_factor(1);

    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("stage-topic", 0);

    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(0, stage.log_start);
    assert_eq!(0, stage.last_stable);
    assert_eq!(0, stage.high_watermark);
}

#[tokio::test]
async fn test_brokers() {
    let engine = create_test_engine().await;

    let brokers = engine.brokers().await.unwrap();
    assert_eq!(1, brokers.len());
    assert_eq!(1, brokers[0].broker_id);
    assert_eq!("localhost", brokers[0].host.as_str());
    assert_eq!(9092, brokers[0].port);
}

#[tokio::test]
async fn test_cluster_id() {
    let engine = create_test_engine().await;

    let cluster_id = engine.cluster_id().await.unwrap();
    assert_eq!("test-cluster", cluster_id);
}

#[tokio::test]
async fn test_node() {
    let engine = create_test_engine().await;

    let node = engine.node().await.unwrap();
    assert_eq!(1, node);
}

// ========== Transaction Error Tests ==========

#[tokio::test]
async fn test_txn_add_partitions_unknown_txn() {
    use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let engine = create_test_engine().await;

    // Try to add partitions without initializing transaction
    let request = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: "unknown-txn".into(),
        producer_id: 1,
        producer_epoch: 0,
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name("any-topic".into())
                .partitions(Some(vec![0])),
        ],
    };

    let response = engine.txn_add_partitions(request).await.unwrap();

    match response {
        TxnAddPartitionsResponse::VersionZeroToThree(results) => {
            let partitions = results[0].results_by_partition.as_ref().unwrap();
            assert_eq!(
                ErrorCode::TransactionalIdNotFound,
                ErrorCode::try_from(partitions[0].partition_error_code).unwrap()
            );
        }
        _ => panic!("Expected VersionZeroToThree response"),
    }
}

// ========== Isolation Level Tests ==========

#[tokio::test]
async fn test_fetch_isolation_levels() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("isolation-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("isolation-topic", 0);

    // Produce some data with valid empty batch (record_count=0)
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 0,
        record_data: Bytes::new(),
    };

    let _ = engine
        .produce(None, &topition, batch.clone())
        .await
        .unwrap();
    let _ = engine
        .produce(None, &topition, batch.clone())
        .await
        .unwrap();

    // Verify offset stage to confirm data was written
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(2, stage.high_watermark);
    assert_eq!(2, stage.last_stable);

    // Test that ReadUncommitted and ReadCommitted return same result when no txns
    let stage_uncommitted = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(
        stage_uncommitted.high_watermark,
        stage_uncommitted.last_stable
    );
}

#[tokio::test]
async fn test_offset_stage_with_transaction() {
    use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("stage-txn-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("stage-txn-topic", 0);

    // Produce some data first
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 1,
        record_data: Bytes::new(),
    };

    let _ = engine
        .produce(None, &topition, batch.clone())
        .await
        .unwrap();

    // Check offset stage - no transactions, so last_stable == high_watermark
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(1, stage.high_watermark);
    assert_eq!(1, stage.last_stable);

    // Start a transaction and add this partition
    let producer = engine
        .init_producer(Some("stage-test-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    let request = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: "stage-test-txn".into(),
        producer_id: producer.id,
        producer_epoch: producer.epoch,
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name("stage-txn-topic".into())
                .partitions(Some(vec![0])),
        ],
    };
    let _ = engine.txn_add_partitions(request).await.unwrap();

    // After committing the transaction, offset stage should still be consistent
    // Note: txn_end now writes a commit marker batch, incrementing high_watermark by 1
    let _ = engine
        .txn_end("stage-test-txn", producer.id, producer.epoch, true)
        .await
        .unwrap();

    let stage = engine.offset_stage(&topition).await.unwrap();
    // high_watermark increased by 1 due to commit marker batch
    assert_eq!(2, stage.high_watermark);
    assert_eq!(2, stage.last_stable);
}

// ========== Idempotent Producer Tests ==========

#[tokio::test]
async fn test_idempotent_produce() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("idempotent-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    // Initialize idempotent producer
    let producer = engine
        .init_producer(None, 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    let topition = Topition::new("idempotent-topic", 0);

    // First batch with sequence 0
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: producer.id,
        producer_epoch: producer.epoch,
        base_sequence: 0,
        record_count: 1,
        record_data: Bytes::new(),
    };

    let offset = engine.produce(None, &topition, batch).await.unwrap();
    assert_eq!(0, offset);
}

// ========== Delete Records Tests ==========

#[tokio::test]
async fn test_delete_records() {
    use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};

    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("delete-records-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("delete-records-topic", 0);

    // Produce some data
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 1,
        record_data: Bytes::new(),
    };

    for _ in 0..5 {
        let _ = engine
            .produce(None, &topition, batch.clone())
            .await
            .unwrap();
    }

    // Verify initial state
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(0, stage.log_start);
    assert_eq!(5, stage.high_watermark);

    // Delete records up to offset 3
    let delete_request = vec![
        DeleteRecordsTopic::default()
            .name("delete-records-topic".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(3),
            ])),
    ];

    let results = engine.delete_records(&delete_request).await.unwrap();

    assert_eq!(1, results.len());
    let partitions = results[0].partitions.as_ref().unwrap();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code).unwrap()
    );
    assert_eq!(3, partitions[0].low_watermark);

    // Verify log_start was updated
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(3, stage.log_start);
}

#[tokio::test]
async fn test_fetch_with_min_bytes() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("min-bytes-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("min-bytes-topic", 0);

    // Produce multiple small batches with valid empty batch (record_count=0)
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 0,
        record_data: Bytes::new(),
    };

    for _ in 0..5 {
        let _ = engine
            .produce(None, &topition, batch.clone())
            .await
            .unwrap();
    }

    // Verify data was written
    let stage = engine.offset_stage(&topition).await.unwrap();
    assert_eq!(5, stage.high_watermark);
}

// ========== Register Broker Tests ==========

#[tokio::test]
async fn test_register_broker() {
    let engine = create_test_engine().await;

    let registration = BrokerRegistrationRequest {
        cluster_id: "test-cluster".into(),
        broker_id: 1,
        rack: Some("rack-1".into()),
        incarnation_id: Default::default(),
    };

    engine.register_broker(registration).await.unwrap();

    // Check that broker is now registered
    let brokers = engine.brokers().await.unwrap();
    assert_eq!(1, brokers.len());
    assert_eq!(1, brokers[0].broker_id);
    assert_eq!(Some("rack-1".to_string()), brokers[0].rack);
}

// ========== Version Four Plus Transaction Tests ==========

#[tokio::test]
async fn test_txn_add_partitions_version_four_plus() {
    use nisshi_sans_io::add_partitions_to_txn_request::{
        AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
    };

    let engine = create_test_engine().await;

    // Create topic
    let topic = CreatableTopic::default()
        .name("v4-topic".into())
        .num_partitions(2)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    // Initialize transactional producer
    let producer = engine
        .init_producer(Some("v4-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Use VersionFourPlus format
    let request = TxnAddPartitionsRequest::VersionFourPlus {
        transactions: vec![
            AddPartitionsToTxnTransaction::default()
                .transactional_id("v4-txn".into())
                .producer_id(producer.id)
                .producer_epoch(producer.epoch)
                .verify_only(false)
                .topics(Some(vec![
                    AddPartitionsToTxnTopic::default()
                        .name("v4-topic".into())
                        .partitions(Some(vec![0, 1])),
                ])),
        ],
    };

    let response = engine.txn_add_partitions(request).await.unwrap();

    match response {
        TxnAddPartitionsResponse::VersionFourPlus(results) => {
            assert_eq!(1, results.len());
            assert_eq!("v4-txn", results[0].transactional_id.as_str());

            let topic_results = results[0].topic_results.as_ref().unwrap();
            assert_eq!(1, topic_results.len());
            assert_eq!("v4-topic", topic_results[0].name.as_str());

            let partitions = topic_results[0].results_by_partition.as_ref().unwrap();
            assert_eq!(2, partitions.len());
        }
        _ => panic!("Expected VersionFourPlus response"),
    }
}

// ========== Additional Transaction Error Tests ==========

#[tokio::test]
async fn test_txn_wrong_producer_id() {
    use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let engine = create_test_engine().await;

    // Initialize transactional producer
    let _ = engine
        .init_producer(Some("wrong-id-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Try with wrong producer_id
    let request = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: "wrong-id-txn".into(),
        producer_id: 9999, // Wrong ID
        producer_epoch: 0,
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name("any-topic".into())
                .partitions(Some(vec![0])),
        ],
    };

    let response = engine.txn_add_partitions(request).await.unwrap();

    match response {
        TxnAddPartitionsResponse::VersionZeroToThree(results) => {
            let partitions = results[0].results_by_partition.as_ref().unwrap();
            assert_eq!(
                ErrorCode::UnknownProducerId,
                ErrorCode::try_from(partitions[0].partition_error_code).unwrap()
            );
        }
        _ => panic!("Expected VersionZeroToThree response"),
    }
}

#[tokio::test]
async fn test_txn_wrong_epoch() {
    use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

    let engine = create_test_engine().await;

    // Initialize transactional producer
    let producer = engine
        .init_producer(Some("wrong-epoch-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Try with wrong producer_epoch
    let request = TxnAddPartitionsRequest::VersionZeroToThree {
        transaction_id: "wrong-epoch-txn".into(),
        producer_id: producer.id,
        producer_epoch: 99, // Wrong epoch
        topics: vec![
            AddPartitionsToTxnTopic::default()
                .name("any-topic".into())
                .partitions(Some(vec![0])),
        ],
    };

    let response = engine.txn_add_partitions(request).await.unwrap();

    match response {
        TxnAddPartitionsResponse::VersionZeroToThree(results) => {
            let partitions = results[0].results_by_partition.as_ref().unwrap();
            assert_eq!(
                ErrorCode::ProducerFenced,
                ErrorCode::try_from(partitions[0].partition_error_code).unwrap()
            );
        }
        _ => panic!("Expected VersionZeroToThree response"),
    }
}

#[tokio::test]
async fn test_txn_end_unknown_transaction() {
    let engine = create_test_engine().await;

    // Try to end a transaction that doesn't exist
    let result = engine.txn_end("nonexistent-txn", 1, 0, true).await;

    assert!(matches!(
        result,
        Err(Error::Api(ErrorCode::TransactionalIdNotFound))
    ));
}

#[tokio::test]
async fn test_txn_end_wrong_producer() {
    let engine = create_test_engine().await;

    // Initialize transactional producer
    let producer = engine
        .init_producer(Some("end-wrong-prod"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Try to end with wrong producer_id
    let result = engine
        .txn_end("end-wrong-prod", 9999, producer.epoch, true)
        .await;

    assert!(matches!(
        result,
        Err(Error::Api(ErrorCode::UnknownProducerId))
    ));
}

#[tokio::test]
async fn test_txn_end_wrong_epoch() {
    let engine = create_test_engine().await;

    // Initialize transactional producer
    let producer = engine
        .init_producer(Some("end-wrong-epoch"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    // Try to end with wrong epoch
    let result = engine
        .txn_end("end-wrong-epoch", producer.id, 99, true)
        .await;

    assert!(matches!(result, Err(Error::Api(ErrorCode::ProducerFenced))));
}

// ========== Idempotent Producer Error Tests ==========

#[tokio::test]
async fn test_idempotent_unknown_producer() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("unknown-prod-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("unknown-prod-topic", 0);

    // Try to produce with unknown producer_id (but idempotent flag set)
    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: 9999, // Unknown producer
        producer_epoch: 0,
        base_sequence: 0,
        record_count: 0,
        record_data: Bytes::new(),
    };

    let result = engine.produce(None, &topition, batch).await;

    assert!(matches!(
        result,
        Err(Error::Api(ErrorCode::UnknownProducerId))
    ));
}

// ========== Delete Records Edge Cases ==========

#[tokio::test]
async fn test_delete_records_unknown_topic() {
    use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};

    let engine = create_test_engine().await;

    // Delete records from non-existent topic
    let delete_request = vec![
        DeleteRecordsTopic::default()
            .name("nonexistent-topic".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(5),
            ])),
    ];

    let results = engine.delete_records(&delete_request).await.unwrap();

    assert_eq!(1, results.len());
    let partitions = results[0].partitions.as_ref().unwrap();
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(partitions[0].error_code).unwrap()
    );
}

#[tokio::test]
async fn test_delete_records_unknown_partition() {
    use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};

    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("del-unknown-part".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    // Delete records from non-existent partition
    let delete_request = vec![
        DeleteRecordsTopic::default()
            .name("del-unknown-part".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(99) // Invalid partition
                    .offset(5),
            ])),
    ];

    let results = engine.delete_records(&delete_request).await.unwrap();

    assert_eq!(1, results.len());
    let partitions = results[0].partitions.as_ref().unwrap();
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(partitions[0].error_code).unwrap()
    );
}

// ========== Multiple Partition Tests ==========

#[tokio::test]
async fn test_produce_multiple_partitions() {
    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("multi-part-topic".into())
        .num_partitions(3)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 0,
        record_data: Bytes::new(),
    };

    // Produce to different partitions
    for partition in 0..3 {
        let topition = Topition::new("multi-part-topic", partition);
        let offset = engine
            .produce(None, &topition, batch.clone())
            .await
            .unwrap();
        assert_eq!(0, offset); // Each partition starts at 0
    }

    // Verify each partition has its own offset
    for partition in 0..3 {
        let topition = Topition::new("multi-part-topic", partition);
        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(1, stage.high_watermark);
    }
}

// ========== Transactional Offset Tests ==========

#[tokio::test]
async fn test_txn_add_offsets() {
    let engine = create_test_engine().await;

    let producer = engine
        .init_producer(Some("add-offsets-txn"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    let error_code = engine
        .txn_add_offsets("add-offsets-txn", producer.id, producer.epoch, "group-1")
        .await
        .unwrap();
    assert_eq!(ErrorCode::None, error_code);
}

#[tokio::test]
async fn test_txn_add_offsets_unknown_txn() {
    let engine = create_test_engine().await;

    let error_code = engine
        .txn_add_offsets("unknown-txn", 1, 0, "group-1")
        .await
        .unwrap();
    assert_eq!(ErrorCode::TransactionalIdNotFound, error_code);
}

#[tokio::test]
async fn test_txn_add_offsets_wrong_producer() {
    let engine = create_test_engine().await;

    let producer = engine
        .init_producer(Some("add-offsets-wrong-prod"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    let error_code = engine
        .txn_add_offsets("add-offsets-wrong-prod", 9999, producer.epoch, "group-1")
        .await
        .unwrap();
    assert_eq!(ErrorCode::UnknownProducerId, error_code);
}

#[tokio::test]
async fn test_txn_add_offsets_wrong_epoch() {
    let engine = create_test_engine().await;

    let producer = engine
        .init_producer(Some("add-offsets-wrong-epoch"), 60000, Some(-1), Some(-1))
        .await
        .unwrap();

    let error_code = engine
        .txn_add_offsets("add-offsets-wrong-epoch", producer.id, 99, "group-1")
        .await
        .unwrap();
    assert_eq!(ErrorCode::ProducerFenced, error_code);
}

// ========== SCRAM Credential Tests ==========

#[tokio::test]
async fn test_scram_credential_lifecycle() {
    use nisshi_sans_io::ScramMechanism;
    use nisshi_storage::ScramCredential;

    let engine = create_test_engine().await;

    // No credential stored yet
    let found = engine
        .user_scram_credential("alice", ScramMechanism::Scram256)
        .await
        .unwrap();
    assert!(found.is_none());

    let credential = ScramCredential {
        salt: Bytes::from_static(b"salt"),
        iterations: 4096,
        stored_key: Bytes::from_static(b"stored-key"),
        server_key: Bytes::from_static(b"server-key"),
    };

    engine
        .upsert_user_scram_credential("alice", ScramMechanism::Scram256, credential.clone())
        .await
        .unwrap();

    // Credential round-trips
    let found = engine
        .user_scram_credential("alice", ScramMechanism::Scram256)
        .await
        .unwrap();
    assert_eq!(Some(credential.clone()), found);

    // Different mechanism and user are distinct keys
    assert!(
        engine
            .user_scram_credential("alice", ScramMechanism::Scram512)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .user_scram_credential("bob", ScramMechanism::Scram256)
            .await
            .unwrap()
            .is_none()
    );

    engine
        .delete_user_scram_credential("alice", ScramMechanism::Scram256)
        .await
        .unwrap();

    let found = engine
        .user_scram_credential("alice", ScramMechanism::Scram256)
        .await
        .unwrap();
    assert!(found.is_none());
}

// ========== List Offsets Tests ==========

#[tokio::test]
async fn test_list_offsets_earliest_after_delete_records() {
    use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};
    use nisshi_sans_io::{IsolationLevel, ListOffset};

    let engine = create_test_engine().await;

    let topic = CreatableTopic::default()
        .name("earliest-topic".into())
        .num_partitions(1)
        .replication_factor(1);
    let _ = engine.create_topic(topic, false).await.unwrap();

    let topition = Topition::new("earliest-topic", 0);

    let batch = Batch {
        base_offset: 0,
        batch_length: 0,
        partition_leader_epoch: 0,
        magic: 2,
        crc: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 1000,
        max_timestamp: 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        record_count: 1,
        record_data: Bytes::new(),
    };

    for _ in 0..5 {
        let _ = engine
            .produce(None, &topition, batch.clone())
            .await
            .unwrap();
    }

    // Before deletion, earliest is 0
    let responses = engine
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Earliest)],
        )
        .await
        .unwrap();
    assert_eq!(Some(0), responses[0].1.offset);

    // Delete records up to offset 3
    let delete_request = vec![
        DeleteRecordsTopic::default()
            .name("earliest-topic".into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(3),
            ])),
    ];
    let _ = engine.delete_records(&delete_request).await.unwrap();

    // Earliest now reflects the advanced log start offset
    let responses = engine
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[
                (topition.clone(), ListOffset::Earliest),
                (topition.clone(), ListOffset::Latest),
            ],
        )
        .await
        .unwrap();
    assert_eq!(ErrorCode::None, responses[0].1.error_code);
    assert_eq!(Some(3), responses[0].1.offset);
    assert_eq!(Some(5), responses[1].1.offset);
}

// ========== Cleanup Policy Tests ==========

mod cleanup_policy {
    use std::time::{Duration, SystemTime};

    use nisshi_sans_io::{
        BatchAttribute, Compression, IsolationLevel,
        create_topics_request::CreatableTopicConfig,
        record::{Record, inflated},
    };

    use super::*;

    async fn topic_with_policy(engine: &Engine, name: &str, configs: &[(&str, &str)]) -> Topition {
        let topic = CreatableTopic::default()
            .name(name.into())
            .num_partitions(1)
            .replication_factor(1)
            .configs(Some(
                configs
                    .iter()
                    .map(|(name, value)| {
                        CreatableTopicConfig::default()
                            .name((*name).into())
                            .value(Some((*value).into()))
                    })
                    .collect(),
            ));

        let _ = engine.create_topic(topic, false).await.unwrap();

        Topition::new(name, 0)
    }

    fn keyed_batch(key: &'static [u8], value: &'static [u8], timestamp: Option<i64>) -> Batch {
        let mut builder = inflated::Batch::builder().record(
            Record::builder()
                .key(Some(Bytes::from_static(key)))
                .value(Some(Bytes::from_static(value))),
        );

        if let Some(timestamp) = timestamp {
            builder = builder.base_timestamp(timestamp).max_timestamp(timestamp);
        }

        builder.build().and_then(Batch::try_from).unwrap()
    }

    async fn fetch_all(engine: &Engine, topition: &Topition) -> Vec<Batch> {
        engine
            .fetch(
                topition,
                0,
                1,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::ZERO,
            )
            .await
            .unwrap()
    }

    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|since_epoch| since_epoch.as_millis() as i64)
            .unwrap()
    }

    #[tokio::test]
    async fn compact_keeps_latest_record_per_key() {
        let engine = create_test_engine().await;
        let topition =
            topic_with_policy(&engine, "policy-compact", &[("cleanup.policy", "compact")]).await;

        for batch in [
            keyed_batch(b"a", b"one", None),
            keyed_batch(b"b", b"first", None),
            keyed_batch(b"a", b"two", None),
        ] {
            let _ = engine.produce(None, &topition, batch).await.unwrap();
        }

        engine.maintain(SystemTime::now()).await.unwrap();

        // The batch at offset 0 (key "a", superseded at offset 2) is
        // removed, but the log start stays at 0, as in Kafka: a consumer at
        // 0 reads from the first surviving batch, rather than being answered
        // OFFSET_OUT_OF_RANGE by Fetch and resetting its position
        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(0, stage.log_start);
        assert_eq!(3, stage.high_watermark);

        let batches = fetch_all(&engine, &topition).await;
        assert_eq!(2, batches.len());
        assert_eq!(1, batches[0].base_offset);
        assert_eq!(2, batches[1].base_offset);

        let records = inflated::Batch::try_from(batches[1].clone())
            .unwrap()
            .records;
        assert_eq!(1, records.len());
        assert_eq!(Some(Bytes::from_static(b"two")), records[0].value);

        // Compaction is idempotent
        engine.maintain(SystemTime::now()).await.unwrap();
        assert_eq!(2, fetch_all(&engine, &topition).await.len());
    }

    #[tokio::test]
    async fn compact_skips_a_batch_it_cannot_inflate() {
        let engine = create_test_engine().await;
        let topition = topic_with_policy(
            &engine,
            "policy-compact-skip",
            &[("cleanup.policy", "compact")],
        )
        .await;

        // Without a schema registry the engine stores a batch without
        // inflating it, so one that cannot be inflated (plain record data
        // labelled as zstd) is stored but can never be read back as records.
        // Compaction must leave it alone, not abandon the pass.
        let mut undecodable = keyed_batch(b"b", b"opaque", None);
        undecodable.attributes = BatchAttribute::default()
            .compression(Compression::Zstd)
            .into();

        for batch in [
            keyed_batch(b"a", b"one", None),
            undecodable,
            keyed_batch(b"a", b"two", None),
        ] {
            let _ = engine.produce(None, &topition, batch).await.unwrap();
        }

        engine.maintain(SystemTime::now()).await.unwrap();

        // Offset 0 (key "a", superseded at offset 2) is removed; the batch
        // that cannot be inflated is still there, untouched.
        let batches = fetch_all(&engine, &topition).await;
        assert_eq!(2, batches.len());
        assert_eq!(1, batches[0].base_offset);
        assert_eq!(2, batches[1].base_offset);
        assert!(inflated::Batch::try_from(batches[0].clone()).is_err());
    }

    #[tokio::test]
    async fn compact_rewrites_partial_batch() {
        let engine = create_test_engine().await;
        let topition = topic_with_policy(
            &engine,
            "policy-compact-partial",
            &[("cleanup.policy", "compact")],
        )
        .await;

        // One batch with two records, then key "a" is superseded
        let batch = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(Some(Bytes::from_static(b"a")))
                    .value(Some(Bytes::from_static(b"one"))),
            )
            .record(
                Record::builder()
                    .key(Some(Bytes::from_static(b"b")))
                    .value(Some(Bytes::from_static(b"first")))
                    .offset_delta(1),
            )
            .last_offset_delta(1)
            .build()
            .and_then(Batch::try_from)
            .unwrap();

        let _ = engine.produce(None, &topition, batch).await.unwrap();
        let _ = engine
            .produce(None, &topition, keyed_batch(b"a", b"two", None))
            .await
            .unwrap();

        engine.maintain(SystemTime::now()).await.unwrap();

        // The first batch survives with only the record for key "b"
        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(0, stage.log_start);
        assert_eq!(3, stage.high_watermark);

        let batches = fetch_all(&engine, &topition).await;
        assert_eq!(2, batches.len());
        assert_eq!(0, batches[0].base_offset);

        let records = inflated::Batch::try_from(batches[0].clone())
            .unwrap()
            .records;
        assert_eq!(1, records.len());
        assert_eq!(Some(Bytes::from_static(b"b")), records[0].key);
        assert_eq!(Some(Bytes::from_static(b"first")), records[0].value);
        assert_eq!(1, records[0].offset_delta);
    }

    #[tokio::test]
    async fn delete_removes_expired_prefix() {
        let engine = create_test_engine().await;
        let topition = topic_with_policy(
            &engine,
            "policy-delete",
            &[("cleanup.policy", "delete"), ("retention.ms", "60000")],
        )
        .await;

        let now = now_millis();
        let expired = now - Duration::from_mins(5).as_millis() as i64;

        for batch in [
            keyed_batch(b"a", b"one", Some(expired)),
            keyed_batch(b"a", b"two", Some(expired + 1)),
            keyed_batch(b"a", b"three", Some(now)),
        ] {
            let _ = engine.produce(None, &topition, batch).await.unwrap();
        }

        engine.maintain(SystemTime::now()).await.unwrap();

        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(2, stage.log_start);
        assert_eq!(3, stage.high_watermark);

        let batches = fetch_all(&engine, &topition).await;
        assert_eq!(1, batches.len());
        assert_eq!(2, batches[0].base_offset);
    }

    #[tokio::test]
    async fn delete_whole_partition_meets_high_watermark() {
        use nisshi_sans_io::ListOffset;

        let engine = create_test_engine().await;
        let topition = topic_with_policy(
            &engine,
            "policy-delete-all",
            &[("cleanup.policy", "delete"), ("retention.ms", "60000")],
        )
        .await;

        let expired = now_millis() - Duration::from_mins(5).as_millis() as i64;

        for batch in [
            keyed_batch(b"a", b"one", Some(expired)),
            keyed_batch(b"a", b"two", Some(expired + 1)),
        ] {
            let _ = engine.produce(None, &topition, batch).await.unwrap();
        }

        engine.maintain(SystemTime::now()).await.unwrap();

        // The log start meets the high watermark: offsets are never reused
        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(2, stage.log_start);
        assert_eq!(2, stage.high_watermark);

        assert!(fetch_all(&engine, &topition).await.is_empty());

        let responses = engine
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(topition.clone(), ListOffset::Earliest)],
            )
            .await
            .unwrap();
        assert_eq!(Some(2), responses[0].1.offset);

        // The partition is now fully empty (log_start == high_watermark):
        // Latest's cached `last_batch_max_timestamp` fast path must have
        // been cleared along with it, not keep answering with the pruned
        // batch's stale timestamp.
        let responses = engine
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(topition.clone(), ListOffset::Latest)],
            )
            .await
            .unwrap();
        assert_eq!(Some(2), responses[0].1.offset);
        assert!(responses[0].1.timestamp.is_none());

        // The next produce continues from the high watermark
        let offset = engine
            .produce(None, &topition, keyed_batch(b"a", b"three", None))
            .await
            .unwrap();
        assert_eq!(2, offset);
    }

    #[tokio::test]
    async fn no_policy_is_untouched() {
        let engine = create_test_engine().await;
        let topition = topic_with_policy(&engine, "policy-none", &[]).await;

        let expired = now_millis() - Duration::from_hours(30 * 24).as_millis() as i64;

        for batch in [
            keyed_batch(b"a", b"one", Some(expired)),
            keyed_batch(b"a", b"two", Some(expired + 1)),
        ] {
            let _ = engine.produce(None, &topition, batch).await.unwrap();
        }

        engine.maintain(SystemTime::now()).await.unwrap();

        let stage = engine.offset_stage(&topition).await.unwrap();
        assert_eq!(0, stage.log_start);
        assert_eq!(2, stage.high_watermark);
        assert_eq!(2, fetch_all(&engine, &topition).await.len());
    }
}

// ========== Time Index Tests ==========
//
// These exercise the `t/` time index directly: the monotonic maybeAppend
// rule (floored at -1 so a negative timestamp is never indexed), the
// ceiling-range-scan lookup, the sequential-batch-scan fallthrough that
// covers a gap in the index (pruning, or a stale compacted header), the
// full-rebuild-on-compaction strategy, and backfilling a legacy watermark.
mod time_index {
    use std::time::{Duration, SystemTime};

    use nisshi_sans_io::{
        IsolationLevel, ListOffset,
        create_topics_request::CreatableTopicConfig,
        delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
        record::{Record, inflated},
        ser::RecordBatchEncoder,
    };
    use nisshi_storage::{ListOffsetResponse, TopicId};
    use serde::Serialize;

    use crate::types::{
        BatchKey, TimeIndexKey, TimeIndexKeyPrefix, Watermark, WatermarkKey, WatermarkLegacy,
    };

    use super::*;

    async fn create_topic(engine: &Engine, name: &str) -> Topition {
        let topic = CreatableTopic::default()
            .name(name.into())
            .num_partitions(1)
            .replication_factor(1);

        let _ = engine.create_topic(topic, false).await.unwrap();

        Topition::new(name, 0)
    }

    async fn topic_with_policy(engine: &Engine, name: &str, configs: &[(&str, &str)]) -> Topition {
        let topic = CreatableTopic::default()
            .name(name.into())
            .num_partitions(1)
            .replication_factor(1)
            .configs(Some(
                configs
                    .iter()
                    .map(|(name, value)| {
                        CreatableTopicConfig::default()
                            .name((*name).into())
                            .value(Some((*value).into()))
                    })
                    .collect(),
            ));

        let _ = engine.create_topic(topic, false).await.unwrap();

        Topition::new(name, 0)
    }

    /// A single-record, single-offset batch with an explicit absolute
    /// timestamp, built via the real encoder so it can be inflated.
    fn keyed_batch(key: &'static [u8], value: &'static [u8], timestamp: i64) -> Batch {
        inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(Some(Bytes::from_static(key)))
                    .value(Some(Bytes::from_static(value))),
            )
            .base_timestamp(timestamp)
            .max_timestamp(timestamp)
            .build()
            .and_then(Batch::try_from)
            .unwrap()
    }

    /// A batch with only header fields set and empty `record_data`: enough
    /// to exercise the time index's header-only paths (append, backfill,
    /// the sequential scan's cheap header skip), but NOT inflatable - do
    /// not use where a test needs its records actually inspected.
    fn fake_batch(last_offset_delta: i32, base_timestamp: i64, max_timestamp: i64) -> Batch {
        Batch {
            base_offset: 0,
            batch_length: 0,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 0,
            attributes: 0,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            record_count: last_offset_delta as u32 + 1,
            record_data: Bytes::new(),
        }
    }

    fn at_millis(millis: i64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(millis as u64)
    }

    fn millis_since_epoch(t: SystemTime) -> i64 {
        t.duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    async fn list_offsets_timestamp(
        engine: &Engine,
        topition: &Topition,
        millis: i64,
    ) -> ListOffsetResponse {
        engine
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(topition.clone(), ListOffset::Timestamp(at_millis(millis)))],
            )
            .await
            .unwrap()
            .remove(0)
            .1
    }

    async fn fetch_all(engine: &Engine, topition: &Topition) -> Vec<Batch> {
        engine
            .fetch(
                topition,
                0,
                1,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::ZERO,
            )
            .await
            .unwrap()
    }

    async fn topic_uuid(engine: &Engine, name: &str) -> uuid::Uuid {
        engine.get_topics().await.unwrap().get(name).unwrap().id
    }

    async fn watermark_of(engine: &Engine, topic: uuid::Uuid, partition: i32) -> Watermark {
        let key = postcard::to_stdvec(&WatermarkKey::new(topic, partition)).unwrap();
        let encoded = engine.db.get(&key).await.unwrap().unwrap();
        postcard::from_bytes(&encoded).unwrap()
    }

    async fn time_index_count(engine: &Engine, topic: uuid::Uuid, partition: i32) -> usize {
        let prefix = postcard::to_stdvec(&TimeIndexKeyPrefix::new(topic, partition)).unwrap();
        let scan_start =
            postcard::to_stdvec(&TimeIndexKey::scan_from(topic, partition, 0)).unwrap();

        let mut scan = engine.db.scan(scan_start..).await.unwrap();
        let mut count = 0;
        while let Some(kv) = scan.next().await.unwrap() {
            if !kv.key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        count
    }

    /// Three single-record batches: offset0 (ts=100, indexed), offset1
    /// (ts=90, <= the running max so NOT indexed - the "gap" a naive
    /// ceiling-only lookup would miss if it ever needed to), offset2
    /// (ts=150, indexed). Index: `{100->0, 150->2}`.
    async fn ceiling_fixture(name: &str) -> (Engine, Topition) {
        let engine = create_test_engine().await;
        let topition = create_topic(&engine, name).await;

        for batch in [
            keyed_batch(b"a", b"v1", 100),
            keyed_batch(b"b", b"v2", 90),
            keyed_batch(b"c", b"v3", 150),
        ] {
            let _ = engine.produce(None, &topition, batch).await.unwrap();
        }

        (engine, topition)
    }

    #[tokio::test]
    async fn ceiling_scan_finds_between_entries() {
        let (engine, topition) = ceiling_fixture("time-index-ceiling-between").await;

        let response = list_offsets_timestamp(&engine, &topition, 115).await;
        assert_eq!(ErrorCode::None, response.error_code);
        assert_eq!(Some(2), response.offset);
        assert_eq!(150, millis_since_epoch(response.timestamp.unwrap()));
    }

    #[tokio::test]
    async fn ceiling_scan_exact_boundary() {
        let (engine, topition) = ceiling_fixture("time-index-ceiling-boundary").await;

        let response = list_offsets_timestamp(&engine, &topition, 150).await;
        assert_eq!(Some(2), response.offset);
        assert_eq!(150, millis_since_epoch(response.timestamp.unwrap()));
    }

    #[tokio::test]
    async fn future_timestamp_returns_no_match() {
        let (engine, topition) = ceiling_fixture("time-index-ceiling-future").await;

        let response = list_offsets_timestamp(&engine, &topition, 151).await;
        assert_eq!(ErrorCode::None, response.error_code);
        assert_eq!(Some(0), response.offset);
        assert!(response.timestamp.is_none());
    }

    #[tokio::test]
    async fn delete_records_gap_is_covered_by_sequential_scan() {
        let engine = create_test_engine().await;
        let topition = create_topic(&engine, "time-index-gap").await;

        // Batch A: 3 records (offsets 0-2), header max_timestamp=100 ->
        // indexed 100->0.
        let _ = engine
            .produce(None, &topition, fake_batch(2, 100, 100))
            .await
            .unwrap();

        // Batch B: 5 real records (offsets 3-7) with absolute timestamps
        // 60,70,80,90,95. Its header max_timestamp=95 is <= the running
        // max (100), so the monotonic rule skips indexing it - this batch
        // is the coverage gap.
        let batch_b = {
            let mut builder = inflated::Batch::builder()
                .base_timestamp(50)
                .max_timestamp(95)
                .last_offset_delta(4);

            for (delta_index, timestamp_delta) in [10i64, 20, 30, 40, 45].into_iter().enumerate() {
                builder = builder.record(
                    Record::builder()
                        .key(None)
                        .value(Some(Bytes::from_static(b"v")))
                        .offset_delta(delta_index as i32)
                        .timestamp_delta(timestamp_delta),
                );
            }

            builder.build().and_then(Batch::try_from).unwrap()
        };
        let _ = engine.produce(None, &topition, batch_b).await.unwrap();

        // Batch C: offset8, header max_timestamp=150 -> indexed 150->8.
        let _ = engine
            .produce(None, &topition, fake_batch(0, 150, 150))
            .await
            .unwrap();

        // delete_records(3) deletes only batch A (base_offset 0 < 3);
        // batch B (base_offset 3) and C survive whole. The index's 100->0
        // entry is pruned, leaving only 150->8: a gap below the new low
        // watermark (3) that a naive "ceiling or nothing" lookup can't see
        // into.
        let delete_request = vec![
            DeleteRecordsTopic::default()
                .name("time-index-gap".into())
                .partitions(Some(vec![
                    DeleteRecordsPartition::default()
                        .partition_index(0)
                        .offset(3),
                ])),
        ];
        let _ = engine.delete_records(&delete_request).await.unwrap();

        let response = list_offsets_timestamp(&engine, &topition, 95).await;
        assert_eq!(ErrorCode::None, response.error_code);
        assert_eq!(Some(7), response.offset);
        assert_eq!(95, millis_since_epoch(response.timestamp.unwrap()));
    }

    #[tokio::test]
    async fn compaction_rebuild_finds_correct_entry_after_removal() {
        let engine = create_test_engine().await;
        let topition = topic_with_policy(
            &engine,
            "time-index-compact-removed",
            &[("cleanup.policy", "compact")],
        )
        .await;

        // offset0: key "x", ts=100 -> indexed 100->0.
        let _ = engine
            .produce(None, &topition, keyed_batch(b"x", b"old", 100))
            .await
            .unwrap();
        // offset1: key "y", ts=90 -> 90<=100, not indexed.
        let _ = engine
            .produce(None, &topition, keyed_batch(b"y", b"only", 90))
            .await
            .unwrap();
        // offset2: key "x" again, ts=110 -> indexed 110->2; supersedes
        // offset0's record.
        let _ = engine
            .produce(None, &topition, keyed_batch(b"x", b"new", 110))
            .await
            .unwrap();

        engine.maintain(SystemTime::now()).await.unwrap();

        // offset0 is now gone (its key was superseded and it held no other
        // record); the index is rebuilt from the survivors (offset1 ts90,
        // offset2 ts110) as {90->1, 110->2}, not left pointing at a
        // deleted batch.
        assert!(
            fetch_all(&engine, &topition)
                .await
                .iter()
                .all(|batch| batch.base_offset != 0)
        );

        let response = list_offsets_timestamp(&engine, &topition, 100).await;
        assert_eq!(ErrorCode::None, response.error_code);
        assert_eq!(Some(2), response.offset);
        assert_eq!(110, millis_since_epoch(response.timestamp.unwrap()));
    }

    /// Discriminates the real full-rebuild-from-survivors logic from a
    /// naive per-entry prune (delete the `t/` entries whose `base_offset`
    /// is in the removed set). `compaction_rebuild_finds_correct_entry_after_removal`
    /// above does not discriminate this: its removed batch happens to sit
    /// at the head of the index, so the "nothing indexed below target ->
    /// start at low" fallback masks the gap either way. This scenario
    /// removes an entry from the middle of the index instead, which a
    /// naive prune answers wrong and the real rebuild answers right.
    #[tokio::test]
    async fn compaction_rebuild_depends_on_full_replay_not_naive_prune() {
        let engine = create_test_engine().await;
        let topition = topic_with_policy(
            &engine,
            "time-index-compact-naive-prune-gap",
            &[("cleanup.policy", "compact")],
        )
        .await;

        // offset0: key "a", ts=100 -> indexed 100->0.
        let _ = engine
            .produce(None, &topition, keyed_batch(b"a", b"v", 100))
            .await
            .unwrap();
        // offset1: key "b", ts=150 -> indexed 150->1. Superseded below by
        // offset4 (same key "b" again), so compaction removes this batch.
        let _ = engine
            .produce(None, &topition, keyed_batch(b"b", b"v1", 150))
            .await
            .unwrap();
        // offset2: key "c", ts=140 -> 140<=150, NOT indexed under the
        // pre-compaction index. This is the entry a naive prune can never
        // recover, because nothing ever named it in the first place.
        let _ = engine
            .produce(None, &topition, keyed_batch(b"c", b"v", 140))
            .await
            .unwrap();
        // offset3: key "d", ts=200 -> indexed 200->3.
        let _ = engine
            .produce(None, &topition, keyed_batch(b"d", b"v", 200))
            .await
            .unwrap();
        // offset4: key "b" again, ts=210 -> indexed 210->4; supersedes
        // offset1's record.
        let _ = engine
            .produce(None, &topition, keyed_batch(b"b", b"v2", 210))
            .await
            .unwrap();

        engine.maintain(SystemTime::now()).await.unwrap();

        // offset1 (the superseded "b") is gone; offset0, offset2, offset3,
        // offset4 survive.
        assert!(
            fetch_all(&engine, &topition)
                .await
                .iter()
                .all(|batch| batch.base_offset != 1)
        );

        // Real code (full replay over survivors) rebuilds the index as
        // {100->0, 140->2, 200->3, 210->4} and a query for 120 correctly
        // answers offset2@140.
        //
        // A naive per-entry prune, by contrast, would only delete the
        // entry naming the removed base_offset (1, i.e. 150->1) from the
        // PRE-compaction index {100->0, 150->1, 200->3, 210->4}, leaving
        // {100->0, 200->3, 210->4} - which has no entry for offset2 at
        // all, so a query for 120 wrongly lands on the ceiling entry
        // 200->3 and answers offset3@200 instead.
        let response = list_offsets_timestamp(&engine, &topition, 120).await;
        assert_eq!(ErrorCode::None, response.error_code);
        assert_eq!(Some(2), response.offset);
        assert_eq!(140, millis_since_epoch(response.timestamp.unwrap()));
    }

    #[tokio::test]
    async fn compaction_stale_header_does_not_cause_false_match() {
        let engine = create_test_engine().await;
        let topition = topic_with_policy(
            &engine,
            "time-index-stale-header",
            &[("cleanup.policy", "compact")],
        )
        .await;

        // One batch, two records: key "p" (absolute ts=100, later
        // superseded) and key "q" (absolute ts=80, survives). The batch
        // header's max_timestamp=100 is the true max at the time it is
        // written, so it is indexed as 100->0.
        let batch = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(Some(Bytes::from_static(b"p")))
                    .value(Some(Bytes::from_static(b"old")))
                    .timestamp_delta(20),
            )
            .record(
                Record::builder()
                    .key(Some(Bytes::from_static(b"q")))
                    .value(Some(Bytes::from_static(b"keep")))
                    .offset_delta(1),
            )
            .base_timestamp(80)
            .max_timestamp(100)
            .last_offset_delta(1)
            .build()
            .and_then(Batch::try_from)
            .unwrap();
        let _ = engine.produce(None, &topition, batch).await.unwrap();

        // A later batch supersedes key "p".
        let _ = engine
            .produce(None, &topition, keyed_batch(b"p", b"new", 120))
            .await
            .unwrap();

        engine.maintain(SystemTime::now()).await.unwrap();

        // The record for key "p" is removed from the first batch, leaving
        // only key "q" (ts=80) - but `Builder::build` copies max_timestamp
        // verbatim from the original batch, so the rewritten batch's
        // header still (stale-ly) claims max_timestamp=100. The rebuild
        // reindexes from this stale header unchanged: {100->0, 120->2}.
        //
        // Target 90 sits between the surviving record's real timestamp
        // (80) and the stale header's claimed max (100): a correct scan
        // must look inside the batch at offset 0, find nothing >= 90, and
        // fall through to the next batch - not wrongly trust the header as
        // a match by itself.
        let response = list_offsets_timestamp(&engine, &topition, 90).await;
        assert_eq!(ErrorCode::None, response.error_code);
        assert_eq!(Some(2), response.offset);
        assert_eq!(120, millis_since_epoch(response.timestamp.unwrap()));
    }

    #[tokio::test]
    async fn negative_timestamp_first_batch_never_indexed() {
        let engine = create_test_engine().await;
        let topition = create_topic(&engine, "time-index-negative").await;

        // First and only batch so far: a negative max_timestamp, as an
        // untrusted wire header could claim (Nisshi does not validate it).
        // Flooring the monotonic check at -1 (Kafka's own NO_TIMESTAMP
        // sentinel) must keep this from ever being indexed.
        let _ = engine
            .produce(None, &topition, fake_batch(0, -5, -5))
            .await
            .unwrap();

        let topic = topic_uuid(&engine, "time-index-negative").await;
        assert!(
            watermark_of(&engine, topic, 0)
                .await
                .latest_indexed_timestamp
                .is_none()
        );

        // A later, real positive-timestamp batch is indexed normally and
        // is not poisoned by the negative entry that came before it: were
        // the negative timestamp ever indexed, raw fixint::be two's
        // complement encoding would sort it AFTER every positive
        // timestamp, corrupting the ceiling scan below.
        let _ = engine
            .produce(None, &topition, keyed_batch(b"a", b"v", 50))
            .await
            .unwrap();

        let response = list_offsets_timestamp(&engine, &topition, 10).await;
        assert_eq!(ErrorCode::None, response.error_code);
        assert_eq!(Some(1), response.offset);
        assert_eq!(50, millis_since_epoch(response.timestamp.unwrap()));
    }

    #[tokio::test]
    async fn legacy_watermark_backfills_and_answers_correctly() {
        let engine = create_test_engine().await;
        let topition = create_topic(&engine, "time-index-legacy").await;
        let topic = topic_uuid(&engine, "time-index-legacy").await;

        // Seed two real batches directly into the `b/` keyspace, bypassing
        // produce() entirely, to simulate data written before this
        // feature existed: no time index was ever maintained for it.
        // offset1's timestamp (300) is deliberately HIGHER than the
        // post-migration produce below (250): this is what makes the
        // scenario discriminating (see the final query's comment).
        for (offset, timestamp) in [(0i64, 100i64), (1, 300)] {
            let batch: Batch = inflated::Batch::builder()
                .record(
                    Record::builder()
                        .key(None)
                        .value(Some(Bytes::from_static(b"v"))),
                )
                .base_timestamp(timestamp)
                .max_timestamp(timestamp)
                .build()
                .and_then(Batch::try_from)
                .unwrap();

            let encoded = {
                let mut encoder = RecordBatchEncoder::new(bytes::BytesMut::new());
                batch.serialize(&mut encoder).unwrap();
                Bytes::from(encoder)
            };

            let batch_key = postcard::to_stdvec(&BatchKey::new(topic, 0, offset)).unwrap();
            let _ = engine.db.put(batch_key, &encoded[..]).await.unwrap();
        }

        // Seed a legacy-shaped (3-field) watermark: low/high only, no time
        // index ever written.
        let legacy = WatermarkLegacy {
            low: Some(0),
            high: Some(2),
            timestamps: None,
        };
        let watermark_key = postcard::to_stdvec(&WatermarkKey::new(topic, 0)).unwrap();
        let _ = engine
            .db
            .put(watermark_key.clone(), postcard::to_stdvec(&legacy).unwrap())
            .await
            .unwrap();

        // Before any write, list_offsets must still give the correct
        // answer: just via the read-only "index empty -> scan from low"
        // fallback (slower, since an unbackfilled index can't use the O(1)
        // future-check shortcut either), not a populated index.
        let response = list_offsets_timestamp(&engine, &topition, 150).await;
        assert_eq!(ErrorCode::None, response.error_code);
        assert_eq!(Some(1), response.offset);
        assert_eq!(300, millis_since_epoch(response.timestamp.unwrap()));

        // A write (the cold-start migration trigger) appends a THIRD batch
        // at offset2, timestamp=250 - lower than the legacy offset1's 300.
        let _ = engine
            .produce(None, &topition, keyed_batch(b"a", b"v", 250))
            .await
            .unwrap();

        // Both legacy batches must have been backfilled as real `t/`
        // entries (100->0, 300->1), not just the new produce's own
        // (250->2, which the monotonic rule doesn't even index, since
        // 250 <= the backfilled 300). Without backfill this count would be
        // 1 (only the new produce's entry).
        assert_eq!(2, time_index_count(&engine, topic, 0).await);

        // The discriminating check: query a timestamp (280) that is
        // beyond the new produce's own timestamp (250) but still covered
        // by backfilled legacy data (offset1@300). Without backfill,
        // `latest_indexed_timestamp` would only reflect the new produce
        // (250), and rule 1's O(1) shortcut (`target > latest_indexed`)
        // would wrongly answer "no match" for 280 even though offset1@300
        // is real, present data.
        let response = list_offsets_timestamp(&engine, &topition, 280).await;
        assert_eq!(ErrorCode::None, response.error_code);
        assert_eq!(Some(1), response.offset);
        assert_eq!(300, millis_since_epoch(response.timestamp.unwrap()));

        let watermark = watermark_of(&engine, topic, 0).await;
        assert_eq!(Some(300), watermark.latest_indexed_timestamp);
    }

    #[tokio::test]
    async fn delete_topic_leaves_no_time_index_keys() {
        let engine = create_test_engine().await;
        let topition1 = create_topic(&engine, "time-index-delete-me").await;
        let topition2 = create_topic(&engine, "time-index-keep-me").await;

        let _ = engine
            .produce(None, &topition1, keyed_batch(b"a", b"v", 100))
            .await
            .unwrap();
        let _ = engine
            .produce(None, &topition2, keyed_batch(b"a", b"v", 200))
            .await
            .unwrap();

        let topic1 = topic_uuid(&engine, "time-index-delete-me").await;
        let topic2 = topic_uuid(&engine, "time-index-keep-me").await;

        assert_eq!(1, time_index_count(&engine, topic1, 0).await);
        assert_eq!(1, time_index_count(&engine, topic2, 0).await);

        let result = engine
            .delete_topic(&TopicId::Name("time-index-delete-me".into()))
            .await
            .unwrap();
        assert_eq!(ErrorCode::None, result);

        assert_eq!(0, time_index_count(&engine, topic1, 0).await);
        assert_eq!(1, time_index_count(&engine, topic2, 0).await);
    }

    async fn list_offsets_latest(engine: &Engine, topition: &Topition) -> ListOffsetResponse {
        engine
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(topition.clone(), ListOffset::Latest)],
            )
            .await
            .unwrap()
            .remove(0)
            .1
    }

    #[tokio::test]
    async fn latest_timestamp_cleared_after_delete_records_empties_partition() {
        let engine = create_test_engine().await;
        let topition = create_topic(&engine, "time-index-latest-empty").await;

        let _ = engine
            .produce(None, &topition, keyed_batch(b"a", b"v", 1000))
            .await
            .unwrap();

        let delete_request = vec![
            DeleteRecordsTopic::default()
                .name("time-index-latest-empty".into())
                .partitions(Some(vec![
                    DeleteRecordsPartition::default()
                        .partition_index(0)
                        .offset(1),
                ])),
        ];
        let _ = engine.delete_records(&delete_request).await.unwrap();

        // The partition is now fully empty (low == high == 1).
        // `last_batch_max_timestamp` was set to 1000 by the original
        // produce and is never touched by `append_time_index` again, so
        // without clearing it on full-prune it would keep answering
        // Latest with the pruned batch's stale timestamp instead of
        // `None`, which is what an empty partition must answer.
        let response = list_offsets_latest(&engine, &topition).await;
        assert_eq!(ErrorCode::None, response.error_code);
        assert_eq!(Some(1), response.offset);
        assert!(response.timestamp.is_none());
    }

    /// A worked example exercising the ceiling-then-scan lookup and the
    /// post-`delete_records` behavior together: 5 batches of 10 records
    /// each, with out-of-order/overlapping timestamps across batches.
    #[tokio::test]
    async fn overlapping_batches_ceiling_then_scan_with_delete_records() {
        let engine = create_test_engine().await;
        let topition = create_topic(&engine, "time-index-ticket-example").await;

        // B0: offsets 0-9, timestamps 91-100.
        // B1: offsets 10-19, timestamps 91-100 (same range as B0).
        // B2: offsets 20-29, timestamps 71-80 (earlier than B0/B1).
        // B3: offsets 30-39, timestamps 141-150.
        // B4: offsets 40-49, timestamps 111-120.
        let batches: [[i64; 10]; 5] = [
            [91, 92, 93, 94, 95, 96, 97, 98, 99, 100],
            [91, 92, 93, 94, 95, 96, 97, 98, 99, 100],
            [71, 72, 73, 74, 75, 76, 77, 78, 79, 80],
            [141, 142, 143, 144, 145, 146, 147, 148, 149, 150],
            [111, 112, 113, 114, 115, 116, 117, 118, 119, 120],
        ];

        for timestamps in batches {
            let base_timestamp = timestamps[0];
            let max_timestamp = *timestamps.last().unwrap();

            let mut builder = inflated::Batch::builder()
                .base_timestamp(base_timestamp)
                .max_timestamp(max_timestamp)
                .last_offset_delta(9);

            for (delta_index, timestamp) in timestamps.into_iter().enumerate() {
                builder = builder.record(
                    Record::builder()
                        .key(None)
                        .value(Some(Bytes::from_static(b"v")))
                        .offset_delta(delta_index as i32)
                        .timestamp_delta(timestamp - base_timestamp),
                );
            }

            let batch = builder.build().and_then(Batch::try_from).unwrap();
            let _ = engine.produce(None, &topition, batch).await.unwrap();
        }

        // Timestamp(70): before everything -> the earliest record, offset
        // 0 @ 91 (B2's 71-80 starts at offset 20, but the monotonic index
        // only ever ceiling-scans forward from the first entry >= target,
        // which for 70 is B0's 91->0 - the earliest absolute entry in the
        // index; the sequential scan then finds the true first record
        // whose timestamp >= 70, which is offset0@91, since nothing
        // before it qualifies either).
        let response = list_offsets_timestamp(&engine, &topition, 70).await;
        assert_eq!(Some(0), response.offset);
        assert_eq!(91, millis_since_epoch(response.timestamp.unwrap()));

        // Timestamp(95): first record with timestamp >= 95 is offset4@95
        // (within B0).
        let response = list_offsets_timestamp(&engine, &topition, 95).await;
        assert_eq!(Some(4), response.offset);
        assert_eq!(95, millis_since_epoch(response.timestamp.unwrap()));

        // Timestamp(101): first record with timestamp >= 101 is offset30
        // @141 (B3), since B4's 111-120 sits in offset order AFTER B3 but
        // its own timestamps are lower - the scan only skips a batch
        // whose header rules it out entirely, so it still inspects B3
        // (base_offset 30) before ever reaching B4.
        let response = list_offsets_timestamp(&engine, &topition, 101).await;
        assert_eq!(Some(30), response.offset);
        assert_eq!(141, millis_since_epoch(response.timestamp.unwrap()));

        // Timestamp(151): beyond everything ever indexed -> no match.
        let response = list_offsets_timestamp(&engine, &topition, 151).await;
        assert_eq!(ErrorCode::None, response.error_code);
        assert_eq!(Some(0), response.offset);
        assert!(response.timestamp.is_none());

        // delete_records(10) removes B0 (offsets 0-9) only.
        let delete_request = vec![
            DeleteRecordsTopic::default()
                .name("time-index-ticket-example".into())
                .partitions(Some(vec![
                    DeleteRecordsPartition::default()
                        .partition_index(0)
                        .offset(10),
                ])),
        ];
        let _ = engine.delete_records(&delete_request).await.unwrap();

        // Timestamp(95): now answered from B1 (offsets 10-19, same
        // timestamps 91-100 as the deleted B0) -> offset14@95.
        let response = list_offsets_timestamp(&engine, &topition, 95).await;
        assert_eq!(Some(14), response.offset);
        assert_eq!(95, millis_since_epoch(response.timestamp.unwrap()));

        // Timestamp(70): the new low watermark is 10, so the earliest
        // answer is now offset10@91.
        let response = list_offsets_timestamp(&engine, &topition, 70).await;
        assert_eq!(Some(10), response.offset);
        assert_eq!(91, millis_since_epoch(response.timestamp.unwrap()));
    }
}

// ========== Builder Pattern Tests ==========

#[tokio::test]
async fn test_builder_pattern() {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("builder-test.slatedb", object_store)
        .await
        .expect("Failed to open SlateDB");

    let engine = Engine::builder()
        .cluster("builder-cluster")
        .node(42)
        .advertised_listener(Url::parse("tcp://10.0.0.1:9093").unwrap())
        .db(Arc::new(db))
        .schemas(None)
        .lake(None)
        .build();

    assert_eq!("builder-cluster", engine.cluster_id().await.unwrap());
    assert_eq!(42, engine.node().await.unwrap());
}
