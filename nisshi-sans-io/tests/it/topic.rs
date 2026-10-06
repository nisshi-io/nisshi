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

use nisshi_sans_io::topic::is_valid_topic_name;

#[test]
fn accepts_legal_names() {
    assert!(is_valid_topic_name("a"));
    assert!(is_valid_topic_name(&"a".repeat(249)));
    assert!(is_valid_topic_name("a.b_c-d"));

    // Kafka rejects a name only when it is exactly "." or ".."; three or
    // more dots is a legal name.
    assert!(is_valid_topic_name("..."));
}

#[test]
fn rejects_illegal_names() {
    assert!(!is_valid_topic_name(""));
    assert!(!is_valid_topic_name("."));
    assert!(!is_valid_topic_name(".."));
    assert!(!is_valid_topic_name(&"a".repeat(250)));
    assert!(!is_valid_topic_name("a/b"));
    assert!(!is_valid_topic_name("a b"));

    // The rule checks bytes, not chars, so every multi-byte character is
    // rejected; swapping the check to `.chars()` would silently widen it.
    assert!(!is_valid_topic_name("é"));
}
