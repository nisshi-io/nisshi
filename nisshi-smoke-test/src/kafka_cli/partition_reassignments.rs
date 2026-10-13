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

//! `kafka-reassign-partitions`, which moves partitions between brokers and lists the moves in
//! progress.

use super::{KafkaCli, Output, Tool};

impl KafkaCli {
    /// Lists the partition reassignments in progress.
    ///
    /// Sends `ListPartitionReassignments`.
    pub fn list_partition_reassignments(&self) -> Output {
        self.run(Tool::ReassignPartitions, &["--list"])
    }
}
