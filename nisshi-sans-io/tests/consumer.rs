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

use std::sync::Arc;

use bytes::Bytes;
use common::init_tracing;
use nisshi_sans_io::{
    Body, Error, ErrorCode, HeartbeatResponse, JoinGroupResponse, MetadataResponse, Result,
    SyncGroupResponse,
    consumer::{
        Assignor, CONSUMER, ConsumerProtocolAssignment, ConsumerProtocolSubscription,
        GroupConsumer, MemberAssignment, MemberMetadata, TopicPartition,
    },
    join_group_response::JoinGroupResponseMember,
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
};
use tracing::debug;

pub mod common;

#[test]
fn decode_range_metadata_001() -> Result<()> {
    let _guard = init_tracing()?;

    let encoded =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");

    let decoded = MemberMetadata::try_from(encoded.clone())?;

    assert_eq!(3, decoded.version);
    assert_eq!(vec![String::from("benchmark")], decoded.subscription.topics);
    assert_eq!(
        Some(Bytes::from_static(b"")),
        decoded.subscription.user_data
    );
    assert!(
        decoded
            .subscription
            .owned_partitions
            .as_ref()
            .is_some_and(|owned_partitions| owned_partitions.is_empty())
    );

    assert_eq!(Some(-1), decoded.subscription.generation_id);
    assert_eq!(Some(""), decoded.subscription.rack_id.as_deref());

    assert_eq!(encoded, Bytes::try_from(&decoded)?);

    Ok(())
}

#[test]
fn decode_range_metadata_002() -> Result<()> {
    let _guard = init_tracing().unwrap();

    let encoded = Bytes::from_static(
        b"\0\x03\0\0\0\x01\0\x04test\xff\xff\xff\xff\0\0\0\0\xff\xff\xff\xff\xff\xff",
    );

    let decoded = MemberMetadata::try_from(encoded.clone())?;

    assert_eq!(3, decoded.version);
    assert_eq!(vec![String::from("test")], decoded.subscription.topics);
    assert_eq!(None, decoded.subscription.user_data);
    assert!(
        decoded
            .subscription
            .owned_partitions
            .as_ref()
            .is_some_and(|owned_partitions| owned_partitions.is_empty())
    );

    assert_eq!(Some(-1), decoded.subscription.generation_id);
    assert_eq!(None, decoded.subscription.rack_id.as_deref());

    assert_eq!(encoded, Bytes::try_from(&decoded)?);

    Ok(())
}

#[test]
fn decode_range_metadata_003() -> Result<()> {
    let _guard = init_tracing().unwrap();

    let encoded = Bytes::from_static(
        b"\0\x03\0\0\0\x01\0\x14telemetry/\"PL20 UXA\"\xff\xff\xff\xff\0\0\0\0\xff\xff\xff\xff\xff\xff"
    );

    let decoded = MemberMetadata::try_from(encoded.clone())?;

    assert_eq!(3, decoded.version);
    assert_eq!(
        vec![String::from("telemetry/\"PL20 UXA\"")],
        decoded.subscription.topics
    );
    assert_eq!(None, decoded.subscription.user_data);
    assert!(
        decoded
            .subscription
            .owned_partitions
            .as_ref()
            .is_some_and(|owned_partitions| owned_partitions.is_empty())
    );

    assert_eq!(Some(-1), decoded.subscription.generation_id);
    assert_eq!(None, decoded.subscription.rack_id.as_deref());

    assert_eq!(encoded, Bytes::try_from(&decoded)?);

    Ok(())
}

#[test]
fn cooperative_sticky_001() -> Result<()> {
    let _guard = init_tracing().unwrap();

    let encoded = Bytes::from_static(
        b"\0\x03\0\0\0\x01\0\x04test\0\0\0\x04\xff\xff\xff\xff\0\0\0\0\xff\xff\xff\xff\xff\xff",
    );

    let decoded = MemberMetadata::try_from(encoded.clone())?;

    assert_eq!(3, decoded.version);
    assert_eq!(vec![String::from("test")], decoded.subscription.topics);
    assert_eq!(
        Some(Bytes::from_static(b"\xff\xff\xff\xff")),
        decoded.subscription.user_data
    );
    assert!(
        decoded
            .subscription
            .owned_partitions
            .as_ref()
            .is_some_and(|owned_partitions| owned_partitions.is_empty())
    );

    assert_eq!(Some(-1), decoded.subscription.generation_id);
    assert_eq!(None, decoded.subscription.rack_id.as_deref());

    assert_eq!(encoded, Bytes::try_from(&decoded)?);

    Ok(())
}

#[test]
fn cooperative_sticky_002() -> Result<()> {
    let _guard = init_tracing().unwrap();

    let encoded = Bytes::from_static(
        b"\0\x03\0\0\0\x01\0\x14telemetry/\"PL20 UXA\"\0\0\0\x04\xff\xff\xff\xff\0\0\0\0\xff\xff\xff\xff\xff\xff"
    );

    let decoded = MemberMetadata::try_from(encoded.clone())?;

    assert_eq!(3, decoded.version);
    assert_eq!(
        vec![String::from("telemetry/\"PL20 UXA\"")],
        decoded.subscription.topics
    );
    assert_eq!(
        Some(Bytes::from_static(b"\xff\xff\xff\xff")),
        decoded.subscription.user_data
    );
    assert!(
        decoded
            .subscription
            .owned_partitions
            .as_ref()
            .is_some_and(|owned_partitions| owned_partitions.is_empty())
    );

    assert_eq!(Some(-1), decoded.subscription.generation_id);
    assert_eq!(None, decoded.subscription.rack_id.as_deref());

    assert_eq!(encoded, Bytes::try_from(&decoded)?);

    Ok(())
}

#[test]
fn member_assignment_001() -> Result<()> {
    let _guard = init_tracing().unwrap();

    let encoded = Bytes::from_static(
        b"\0\x03\0\0\0\x01\0\x04test\0\0\0\x03\0\0\0\0\0\0\0\x01\0\0\0\x02\xff\xff\xff\xff",
    );

    let decoded = MemberAssignment::try_from(encoded.clone())?;

    assert_eq!(3, decoded.version);
    assert_eq!(1, decoded.assignment.assigned_partitions.len());
    assert_eq!(
        Some(TopicPartition {
            topic: "test".into(),
            partitions: vec![0, 1, 2]
        }),
        decoded.assignment.assigned_partitions.first().cloned()
    );
    assert!(decoded.assignment.user_data.is_none());

    assert_eq!(encoded, Bytes::try_from(&decoded)?);

    Ok(())
}

#[test]
fn member_assignment_002() -> Result<()> {
    let _guard = init_tracing().unwrap();

    let encoded = Bytes::from_static(
        b"\0\x03\0\0\0\x01\0\x04test\0\0\0\x03\0\0\0\0\0\0\0\x01\0\0\0\x02\xff\xff\xff\xff",
    );

    let decoded = MemberAssignment::try_from(encoded.clone())?;

    assert_eq!(3, decoded.version);
    assert_eq!(1, decoded.assignment.assigned_partitions.len());
    assert_eq!(
        Some(TopicPartition {
            topic: "test".into(),
            partitions: vec![0, 1, 2]
        }),
        decoded.assignment.assigned_partitions.first().cloned()
    );
    assert!(decoded.assignment.user_data.is_none());

    assert_eq!(encoded, Bytes::try_from(&decoded)?);

    Ok(())
}

#[test]
fn member_assignment_003() -> Result<()> {
    let _guard = init_tracing().unwrap();

    let encoded = Bytes::from_static(
        b"\0\x03\0\0\0\x01\0\x14telemetry/\"PL20 UXA\"\0\0\0\x03\0\0\0\0\0\0\0\x01\0\0\0\x02\xff\xff\xff\xff"
    );

    let decoded = MemberAssignment::try_from(encoded.clone())?;

    assert_eq!(3, decoded.version);
    assert_eq!(1, decoded.assignment.assigned_partitions.len());
    assert_eq!(
        Some(TopicPartition {
            topic: "telemetry/\"PL20 UXA\"".into(),
            partitions: vec![0, 1, 2]
        }),
        decoded.assignment.assigned_partitions.first().cloned()
    );
    assert!(decoded.assignment.user_data.is_none());

    assert_eq!(encoded, Bytes::try_from(&decoded)?);

    Ok(())
}

#[test]
fn empty_assignment() -> Result<(), Error> {
    let assignment =
        MemberAssignment::try_from(Bytes::from_static(b"\0\x03\0\0\0\0\xff\xff\xff\xff"))?;

    assert_eq!(
        MemberAssignment::default().version(ConsumerProtocolSubscription::V3),
        assignment
    );

    Ok(())
}

#[test]
fn build_cpa() -> Result<(), Error> {
    let user_data = Bytes::from_static(b"abc");

    let abc = TopicPartition::default().topic("pqr").partitions(0..3);

    let cpa = ConsumerProtocolAssignment::default().user_data(Some(user_data.clone()));

    assert_eq!(Some(user_data), cpa.user_data);

    Ok(())
}

#[test]
fn metadata_from_bytes_001() -> Result<(), Error> {
    let expected = MemberMetadata {
        version: 3,
        subscription: ConsumerProtocolSubscription {
            topics: ["test-topic".into()].into(),
            user_data: Some(Bytes::from_static(
                b"\0\0\0\x01\0\ntest-topic\0\0\0\x01\0\0\0\x02\0\0\0\x11",
            )),
            owned_partitions: Some(
                [TopicPartition {
                    topic: "test-topic".into(),
                    partitions: [2].into(),
                }]
                .into(),
            ),
            generation_id: Some(17),
            rack_id: None,
        },
    };

    let decoded = MemberMetadata::try_from(Bytes::from_static(b"\0\x03\0\0\0\x01\0\ntest-topic\0\0\0\x1c\0\0\0\x01\0\ntest-topic\0\0\0\x01\0\0\0\x02\0\0\0\x11\0\0\0\x01\0\ntest-topic\0\0\0\x01\0\0\0\x02\0\0\0\x11\xff\xff"))?;

    assert_eq!(expected, decoded);
    Ok(())
}

#[test]
fn metadata_from_bytes_002() -> Result<(), Error> {
    let expected = MemberMetadata {
        version: 3,
        subscription: ConsumerProtocolSubscription {
            topics: ["test-topic".into()].into(),
            user_data: Some(Bytes::from_static(
                b"\0\0\0\x01\0\ntest-topic\0\0\0\x01\0\0\0\x02\0\0\0\x0e",
            )),
            owned_partitions: Some(
                [TopicPartition {
                    topic: "test-topic".into(),
                    partitions: [2].into(),
                }]
                .into(),
            ),
            generation_id: Some(14),
            rack_id: None,
        },
    };

    let decoded = MemberMetadata::try_from(Bytes::from_static(b"\0\x03\0\0\0\x01\0\ntest-topic\0\0\0\x1c\0\0\0\x01\0\ntest-topic\0\0\0\x01\0\0\0\x02\0\0\0\x0e\0\0\0\x01\0\ntest-topic\0\0\0\x01\0\0\0\x02\0\0\0\x0e\xff\xff"))?;

    assert_eq!(expected, decoded);
    Ok(())
}

fn make_metadata(topics: &[(&str, i32)]) -> MetadataResponse {
    MetadataResponse::default().topics(Some(
        topics
            .iter()
            .map(|(name, count)| {
                MetadataResponseTopic::default()
                    .name(Some((*name).to_string()))
                    .partitions(Some(
                        (0..*count)
                            .map(|p| MetadataResponsePartition::default().partition_index(p))
                            .collect(),
                    ))
            })
            .collect(),
    ))
}

fn member_meta(topics: &[&str]) -> Result<Bytes, Error> {
    Bytes::try_from(
        &MemberMetadata::default()
            .version(ConsumerProtocolSubscription::V3)
            .subscription(
                ConsumerProtocolSubscription::default()
                    .topics(topics.iter().map(|t| t.to_string())),
            ),
    )
}

#[test]
fn initial_state() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let group = "g1";
    let topic = "t";
    let consumer = GroupConsumer::builder(group)
        .topics([topic])
        .metadata(make_metadata(&[(topic, 3)]))
        .build();

    assert_eq!(group, consumer.group_id);

    assert!(consumer.is_outsider());

    assert_eq!(1, consumer.topics.len());
    assert!(consumer.topics.contains(&topic.into()));

    Ok(())
}

#[test]
fn outsider_sends_join_request() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let mut consumer = GroupConsumer::builder("g1")
        .topics(["t"])
        .metadata(make_metadata(&[("t", 3)]))
        .on_assignment(Some(Arc::new(
            |group: String, assignment: MemberAssignment| debug!(group, %assignment),
        )))
        .build();

    let Body::JoinGroupRequest(req) = consumer.next_action(None)? else {
        panic!("expected JoinGroupRequest");
    };

    assert_eq!("g1", req.group_id);
    assert_eq!("", req.member_id);
    assert_eq!(CONSUMER, req.protocol_type);

    let names: Vec<&str> = req
        .protocols
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|p| p.name.as_str())
        .collect();
    assert_eq!(4, names.len(), "all four assignors must be advertised");
    assert!(names.contains(&Assignor::RANGE));
    assert!(names.contains(&Assignor::ROUND_ROBIN));
    assert!(names.contains(&Assignor::UNIFORM));
    assert!(names.contains(&Assignor::COOPERATIVE_STICKY));

    Ok(())
}

#[test]
fn outsider_member_id_required_rejoins_with_assigned_id() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let mut consumer = GroupConsumer::builder("g1")
        .topics(["t"])
        .metadata(make_metadata(&[("t", 1)]))
        .on_assignment(Some(Arc::new(
            |group: String, assignment: MemberAssignment| debug!(group, %assignment),
        )))
        .build();

    let Body::JoinGroupRequest(req) = consumer.next_action(Some(Body::JoinGroupResponse(
        JoinGroupResponse::default()
            .error_code(i16::from(ErrorCode::MemberIdRequired))
            .member_id("consumer-abc-123".into()),
    )))?
    else {
        panic!("expected JoinGroupRequest");
    };

    assert_eq!("consumer-abc-123", req.member_id);

    Ok(())
}

#[test]
fn outsider_becomes_follower() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let mut consumer = GroupConsumer::builder("g1")
        .topics(["t"])
        .metadata(make_metadata(&[("t", 2)]))
        .on_assignment(Some(Arc::new(
            |group: String, assignment: MemberAssignment| debug!(group, %assignment),
        )))
        .build();

    let Body::SyncGroupRequest(req) = consumer.next_action(Some(Body::JoinGroupResponse(
        JoinGroupResponse::default()
            .error_code(i16::from(ErrorCode::None))
            .generation_id(3)
            .member_id("me".into())
            .leader("leader".into())
            .protocol_name(Some(Assignor::RANGE.into()))
            .protocol_type(Some(CONSUMER.into())),
    )))?
    else {
        panic!("expected SyncGroupRequest");
    };

    assert_eq!("g1", req.group_id);
    assert_eq!("me", req.member_id);
    assert_eq!(3, req.generation_id);
    assert_eq!(Some(vec![]), req.assignments);

    Ok(())
}

#[test]
fn outsider_becomes_leader_range() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let mut consumer = GroupConsumer::builder("g1")
        .topics(["t"])
        .metadata(make_metadata(&[("t", 4)]))
        .on_assignment(Some(Arc::new(
            |group: String, assignment: MemberAssignment| debug!(group, %assignment),
        )))
        .build();

    let meta = member_meta(&["t"])?;
    let Body::SyncGroupRequest(req) = consumer.next_action(Some(Body::JoinGroupResponse(
        JoinGroupResponse::default()
            .error_code(i16::from(ErrorCode::None))
            .generation_id(2)
            .member_id("C0".into())
            .leader("C0".into())
            .protocol_name(Some(Assignor::RANGE.into()))
            .protocol_type(Some(CONSUMER.into()))
            .members(Some(vec![
                JoinGroupResponseMember::default()
                    .member_id("C0".into())
                    .metadata(meta.clone()),
                JoinGroupResponseMember::default()
                    .member_id("C1".into())
                    .metadata(meta),
            ])),
    )))?
    else {
        panic!("expected SyncGroupRequest");
    };

    assert_eq!("C0", req.member_id);
    assert_eq!(2, req.generation_id);
    assert_eq!(Some(Assignor::RANGE.to_string()), req.protocol_name);

    let assignments = req.assignments.unwrap();
    assert_eq!(2, assignments.len());

    let c0 = MemberAssignment::try_from(
        assignments
            .iter()
            .find(|a| a.member_id == "C0")
            .unwrap()
            .assignment
            .clone(),
    )?;
    assert_eq!(
        vec![TopicPartition {
            topic: "t".into(),
            partitions: vec![0, 1],
        }],
        c0.assignment.assigned_partitions
    );

    let c1 = MemberAssignment::try_from(
        assignments
            .iter()
            .find(|a| a.member_id == "C1")
            .unwrap()
            .assignment
            .clone(),
    )?;
    assert_eq!(
        vec![TopicPartition {
            topic: "t".into(),
            partitions: vec![2, 3],
        }],
        c1.assignment.assigned_partitions
    );

    Ok(())
}

#[test]
fn leader_uses_protocol_from_join_response() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // With 2 members and topic "t" with 4 partitions:
    // range    → C0:[0,1], C1:[2,3]
    // roundrobin → C0:[0,2], C1:[1,3]
    // Passing protocol_name="roundrobin" verifies dispatch ignores self.assignor.
    let mut consumer = GroupConsumer::builder("g1")
        .topics(["t"])
        .metadata(make_metadata(&[("t", 4)]))
        .on_assignment(Some(Arc::new(
            |group: String, assignment: MemberAssignment| debug!(group, %assignment),
        )))
        .build();

    let meta = member_meta(&["t"])?;
    let Body::SyncGroupRequest(req) = consumer.next_action(Some(Body::JoinGroupResponse(
        JoinGroupResponse::default()
            .error_code(i16::from(ErrorCode::None))
            .generation_id(1)
            .member_id("C0".into())
            .leader("C0".into())
            .protocol_name(Some(Assignor::ROUND_ROBIN.into()))
            .protocol_type(Some(CONSUMER.into()))
            .members(Some(vec![
                JoinGroupResponseMember::default()
                    .member_id("C0".into())
                    .metadata(meta.clone()),
                JoinGroupResponseMember::default()
                    .member_id("C1".into())
                    .metadata(meta),
            ])),
    )))?
    else {
        panic!("expected SyncGroupRequest");
    };

    let assignments = req.assignments.unwrap();
    let c0 = MemberAssignment::try_from(
        assignments
            .iter()
            .find(|a| a.member_id == "C0")
            .unwrap()
            .assignment
            .clone(),
    )?;
    // roundrobin: t/0→C0, t/1→C1, t/2→C0, t/3→C1 → C0 gets [0,2]
    assert_eq!(
        vec![TopicPartition {
            topic: "t".into(),
            partitions: vec![0, 2],
        }],
        c0.assignment.assigned_partitions
    );

    Ok(())
}

#[test]
fn leader_sync_response_yields_heartbeat() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let mut consumer = GroupConsumer::builder("g1")
        .topics(["t"])
        .metadata(make_metadata(&[("t", 1)]))
        .on_assignment(Some(Arc::new(
            |group: String, assignment: MemberAssignment| debug!(group, %assignment),
        )))
        .build();

    let meta = member_meta(&["t"])?;
    _ = consumer.next_action(Some(Body::JoinGroupResponse(
        JoinGroupResponse::default()
            .error_code(i16::from(ErrorCode::None))
            .generation_id(5)
            .member_id("me".into())
            .leader("me".into())
            .protocol_name(Some(Assignor::RANGE.into()))
            .protocol_type(Some(CONSUMER.into()))
            .members(Some(vec![
                JoinGroupResponseMember::default()
                    .member_id("me".into())
                    .metadata(meta),
            ])),
    )))?;

    let assignment = Bytes::try_from(
        &MemberAssignment::default()
            .version(ConsumerProtocolSubscription::V3)
            .assignment(ConsumerProtocolAssignment::default().assigned_partitions([
                TopicPartition {
                    topic: "t".into(),
                    partitions: vec![0],
                },
            ])),
    )?;

    let Body::HeartbeatRequest(req) = consumer.next_action(Some(Body::SyncGroupResponse(
        SyncGroupResponse::default()
            .error_code(i16::from(ErrorCode::None))
            .assignment(assignment),
    )))?
    else {
        panic!("expected HeartbeatRequest");
    };

    assert_eq!("g1", req.group_id);
    assert_eq!("me", req.member_id);
    assert_eq!(5, req.generation_id);

    Ok(())
}

#[test]
fn follower_heartbeat_ok_yields_heartbeat() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let mut consumer = GroupConsumer::builder("g1")
        .topics(["t"])
        .metadata(make_metadata(&[("t", 2)]))
        .on_assignment(Some(Arc::new(
            |group: String, assignment: MemberAssignment| debug!(group, %assignment),
        )))
        .build();

    _ = consumer.next_action(Some(Body::JoinGroupResponse(
        JoinGroupResponse::default()
            .error_code(i16::from(ErrorCode::None))
            .generation_id(3)
            .member_id("me".into())
            .leader("leader".into())
            .protocol_name(Some(Assignor::RANGE.into()))
            .protocol_type(Some(CONSUMER.into())),
    )))?;

    let Body::HeartbeatRequest(req) = consumer.next_action(Some(Body::HeartbeatResponse(
        HeartbeatResponse::default().error_code(i16::from(ErrorCode::None)),
    )))?
    else {
        panic!("expected HeartbeatRequest");
    };

    assert_eq!("me", req.member_id);
    assert_eq!(3, req.generation_id);

    Ok(())
}

#[test]
fn follower_rebalance_in_progress_yields_join() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let mut consumer = GroupConsumer::builder("g1")
        .topics(["t"])
        .metadata(make_metadata(&[("t", 2)]))
        .on_assignment(Some(Arc::new(
            |group: String, assignment: MemberAssignment| debug!(group, %assignment),
        )))
        .build();

    _ = consumer
        .next_action(Some(Body::JoinGroupResponse(
            JoinGroupResponse::default()
                .error_code(i16::from(ErrorCode::None))
                .generation_id(4)
                .member_id("me".into())
                .leader("leader".into())
                .protocol_name(Some(Assignor::RANGE.into()))
                .protocol_type(Some(CONSUMER.into())),
        )))
        .inspect(|req| debug!(?req))?;

    let Body::JoinGroupRequest(req) = consumer
        .next_action(Some(Body::HeartbeatResponse(
            HeartbeatResponse::default().error_code(i16::from(ErrorCode::RebalanceInProgress)),
        )))
        .inspect(|req| debug!(?req))?
    else {
        panic!("expected HeartbeatRequest");
    };

    assert_eq!("me", req.member_id);

    Ok(())
}
