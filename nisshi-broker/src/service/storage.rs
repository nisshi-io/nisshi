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

use nisshi_sans_io::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, AlterUserScramCredentialsRequest,
    ApiKey as _, ConsumerGroupDescribeRequest, CreateAclsRequest, CreateTopicsRequest,
    DeleteGroupsRequest, DeleteRecordsRequest, DeleteTopicsRequest, DescribeAclsRequest,
    DescribeClusterRequest, DescribeConfigsRequest, DescribeGroupsRequest,
    DescribeTopicPartitionsRequest, DescribeUserScramCredentialsRequest, EndTxnRequest,
    FetchRequest, FindCoordinatorRequest, GetTelemetrySubscriptionsRequest,
    IncrementalAlterConfigsRequest, InitProducerIdRequest, ListGroupsRequest, ListOffsetsRequest,
    ListPartitionReassignmentsRequest, MetadataRequest, ProduceRequest, TxnOffsetCommitRequest,
};
use nisshi_service::{FrameRequestLayer, FrameRouteBuilder};
use nisshi_storage::{
    AlterUserScramCredentialsService, ConsumerGroupDescribeService, CreateAclsService,
    CreateTopicsService, DeleteGroupsService, DeleteRecordsService, DeleteTopicsService,
    DescribeAclsService, DescribeClusterService, DescribeConfigsService, DescribeGroupsService,
    DescribeTopicPartitionsService, DescribeUserScramCredentialsService, FetchService,
    FindCoordinatorService, GetTelemetrySubscriptionsService, IncrementalAlterConfigsService,
    InitProducerIdService, ListGroupsService, ListOffsetsService,
    ListPartitionReassignmentsService, MetadataService, ProduceService, Storage,
    TxnAddOffsetsService, TxnAddPartitionService, TxnEndService, TxnOffsetCommitService,
};
use rama::{
    Layer as _, Service as _,
    layer::{MapErrLayer, MapStateLayer},
};

use crate::Error;

pub fn services<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    [
        add_offsets_to_txn,
        add_partitions_to_txn,
        alter_user_scram_credentials,
        consumer_group_describe,
        create_acls,
        create_topics,
        delete_groups,
        delete_records,
        delete_topics,
        describe_acls,
        describe_cluster,
        describe_configs,
        describe_groups,
        describe_topic_partitions,
        describe_user_scram_credentials,
        fetch,
        find_coordinator,
        get_telemetry_subscriptions,
        incremental_alter_configs,
        init_producer_id,
        list_groups,
        list_offsets,
        list_partition_reassignments,
        metadata,
        produce,
        end_txn,
        txn_offset_commit,
    ]
    .iter()
    .try_fold(builder, |builder, service| {
        service(builder, storage.clone())
    })
}

pub fn alter_user_scram_credentials<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            AlterUserScramCredentialsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<AlterUserScramCredentialsRequest>::new(),
            )
                .into_layer(AlterUserScramCredentialsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_user_scram_credentials<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            DescribeUserScramCredentialsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DescribeUserScramCredentialsRequest>::new(),
            )
                .into_layer(DescribeUserScramCredentialsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn create_acls<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            CreateAclsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<CreateAclsRequest>::new(),
            )
                .into_layer(CreateAclsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_acls<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            DescribeAclsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DescribeAclsRequest>::new(),
            )
                .into_layer(DescribeAclsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn consumer_group_describe<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            ConsumerGroupDescribeRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<ConsumerGroupDescribeRequest>::new(),
            )
                .into_layer(ConsumerGroupDescribeService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn create_topics<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            CreateTopicsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<CreateTopicsRequest>::new(),
            )
                .into_layer(CreateTopicsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn delete_groups<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            DeleteGroupsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DeleteGroupsRequest>::new(),
            )
                .into_layer(DeleteGroupsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn delete_records<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            DeleteRecordsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DeleteRecordsRequest>::new(),
            )
                .into_layer(DeleteRecordsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn delete_topics<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            DeleteTopicsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DeleteTopicsRequest>::new(),
            )
                .into_layer(DeleteTopicsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_cluster<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            DescribeClusterRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DescribeClusterRequest>::new(),
            )
                .into_layer(DescribeClusterService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_configs<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            DescribeConfigsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DescribeConfigsRequest>::new(),
            )
                .into_layer(DescribeConfigsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_groups<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            DescribeGroupsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DescribeGroupsRequest>::new(),
            )
                .into_layer(DescribeGroupsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_topic_partitions<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            DescribeTopicPartitionsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<DescribeTopicPartitionsRequest>::new(),
            )
                .into_layer(DescribeTopicPartitionsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn fetch<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            FetchRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<FetchRequest>::new(),
            )
                .into_layer(FetchService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn find_coordinator<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            FindCoordinatorRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<FindCoordinatorRequest>::new(),
            )
                .into_layer(FindCoordinatorService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn incremental_alter_configs<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            IncrementalAlterConfigsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<IncrementalAlterConfigsRequest>::new(),
            )
                .into_layer(IncrementalAlterConfigsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn init_producer_id<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            InitProducerIdRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<InitProducerIdRequest>::new(),
            )
                .into_layer(InitProducerIdService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn list_groups<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            ListGroupsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<ListGroupsRequest>::new(),
            )
                .into_layer(ListGroupsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn list_offsets<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            ListOffsetsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<ListOffsetsRequest>::new(),
            )
                .into_layer(ListOffsetsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn list_partition_reassignments<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            ListPartitionReassignmentsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<ListPartitionReassignmentsRequest>::new(),
            )
                .into_layer(ListPartitionReassignmentsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn metadata<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            MetadataRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<MetadataRequest>::new(),
            )
                .into_layer(MetadataService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn produce<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            ProduceRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<ProduceRequest>::new(),
            )
                .into_layer(ProduceService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn get_telemetry_subscriptions<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            GetTelemetrySubscriptionsRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<GetTelemetrySubscriptionsRequest>::new(),
            )
                .into_layer(GetTelemetrySubscriptionsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn add_offsets_to_txn<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            AddOffsetsToTxnRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<AddOffsetsToTxnRequest>::new(),
            )
                .into_layer(TxnAddOffsetsService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn add_partitions_to_txn<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            AddPartitionsToTxnRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<AddPartitionsToTxnRequest>::new(),
            )
                .into_layer(TxnAddPartitionService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn txn_offset_commit<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            TxnOffsetCommitRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<TxnOffsetCommitRequest>::new(),
            )
                .into_layer(TxnOffsetCommitService)
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn end_txn<S>(
    builder: FrameRouteBuilder<(), Error>,
    storage: S,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    S: Storage + Clone,
{
    builder
        .with_route(
            EndTxnRequest::KEY,
            (
                MapErrLayer::new(Error::from),
                MapStateLayer::new(|_| storage),
                FrameRequestLayer::<EndTxnRequest>::new(),
            )
                .into_layer(TxnEndService)
                .boxed(),
        )
        .map_err(Into::into)
}
