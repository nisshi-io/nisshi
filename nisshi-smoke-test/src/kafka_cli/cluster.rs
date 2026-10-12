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

//! `kafka-cluster` and `kafka-broker-api-versions`, which describe the cluster and its broker.

use super::{KafkaCli, Output, Tool};

/// How `kafka-cluster cluster-id` starts the line that holds the cluster's id.
const CLUSTER_ID_LINE_PREFIX: &str = "Cluster ID: ";
/// What `kafka-broker-api-versions` prints before the node id, on its first line.
const NODE_ID_MARKER: &str = "(id: ";

/// The command of [`KafkaCli::cluster_id`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum ClusterId {}

/// The command of [`KafkaCli::api_versions`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum ApiVersions {}

/// Runs `kafka-cluster`, which describes the cluster.
impl KafkaCli {
    /// Prints the cluster's id, as `Cluster ID: <id>`.
    ///
    /// Sends `DescribeCluster`.
    pub fn cluster_id(&self) -> Output<ClusterId> {
        self.run(Tool::Cluster, &["cluster-id"])
    }
}

/// Runs `kafka-broker-api-versions`, which lists the APIs each broker supports.
impl KafkaCli {
    /// Lists the API versions the broker supports, one `<name>(<key>): <min> to <max> [usable:
    /// <version>]` line per API, after a line naming the broker.
    ///
    /// Sends `ApiVersions` and `Metadata`.
    pub fn api_versions(&self) -> Output<ApiVersions> {
        self.run(Tool::BrokerApiVersions, &[])
    }
}

impl Output<ClusterId> {
    /// Returns the cluster's id.
    pub fn cluster_id(&self) -> Option<&str> {
        self.stdout
            .lines()
            .find_map(|line| line.strip_prefix(CLUSTER_ID_LINE_PREFIX))
    }
}

impl Output<ApiVersions> {
    /// Returns the node id that the listing names on its first line,
    /// `<host>:<port> (id: <node> rack: ...) -> (`.
    pub fn node_id(&self) -> Option<i32> {
        self.stdout
            .lines()
            .next()?
            .split_once(NODE_ID_MARKER)?
            .1
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    }
}
