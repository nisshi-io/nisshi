# Plan: Implement Remaining Consumer Partition Assignors

## Background

`consumer.rs` defines four assignors in the `Assignor` enum — `Range`, `RoundRobin`, `Uniform`,
and `CooperativeSticky` — and the `ConsumerAssignor` trait. Only `RangeAssignor` is implemented
(in `consumer/assignor/range.rs`). The other three are named but have no backing implementation,
so the broker can only honour clients that negotiate the `range` protocol.

The `Assignor` enum strings (`"range"`, `"roundrobin"`, `"uniform"`,
`"cooperative-sticky"`) are already wired into `GroupConsumer::ASSIGNORS` (currently hard-coded
to `&[Assignor::Range]`); once the implementations exist that constant should be expanded.

The `assignor.rs` module already defines a useful `Topition` struct (topic + partition_index as a
sorted key) and a `From<&MetadataResponse> for BTreeSet<Topition>` conversion — these should be
shared helpers for all assignors.

---

## Kafka Assignor Semantics

### Range (already implemented)

For each topic independently, sort partitions, divide them into consecutive ranges. Members get
*allocation* or *allocation + 1* partitions. Members are processed in sorted order.

**Key property**: consecutive partitions, per-topic, members with lower sort order get slightly
more when the division is uneven. Both topics independently → a member may get the same-indexed
partition across all its subscribed topics.

The existing implementation has a subtle bug: it collapses all partition indices across all topics
into one `BTreeSet<i32>` (losing topic identity), then distributes the raw index numbers. The
current tests pass only because both topics in the test have the same partition count with
indices 0..N. For true range assignment each topic must be handled independently. This is noted
below but is **out of scope** for this plan — fix it in a follow-up.

---

### RoundRobin

**Protocol name**: `"roundrobin"`

1. Collect the union of all (topic, partition) pairs subscribed by *any* member.
2. Sort the full set (by topic name then partition index) — i.e., iterate
   `BTreeSet<Topition>`.
3. Distribute them across sorted members in round-robin order: topition[0] → member[0],
   topition[1] → member[1], …, topition[N] → member[N % len].
4. Each member only receives partitions for topics it has subscribed to; skip a topition
   if the current member is not subscribed to that topic and move to the next member.

**Result**: partitions interleaved across members. Every member gets roughly the same
count of partitions.

When all members subscribe to the same topics, round-robin produces the most balanced
spread possible. When members have different subscriptions the skip logic is needed.

---

### Uniform (Sticky)

**Protocol name**: `"uniform"` (Tansu-specific alias, not in standard Kafka)

Semantically identical to round-robin for the initial assignment. The "sticky" part matters
only during rebalance: try to keep existing assignments (`owned_partitions` from each member's
`ConsumerProtocolSubscription`) unchanged and only move partitions that are unbalanced or
belong to members who left the group.

For a first implementation, treat uniform as round-robin (ignore stickiness). A follow-up
can add the rebalance-aware logic.

---

### CooperativeSticky

**Protocol name**: `"cooperative-sticky"`

This is the incremental rebalance protocol (KIP-429). Its distinguishing feature is that
existing assignments are preserved across rebalances — the broker never revokes more than
it must.

Algorithm:
1. Read each member's `owned_partitions` from their `ConsumerProtocolSubscription`.
2. Build a `target` assignment using round-robin over the new member list.
3. For each topition in the target assignment, if it is already owned by the target member,
   keep it. Otherwise it must be moved, but only after the previous owner has revoked it in
   a prior rebalance round (handled by the broker's join/sync state machine, not the
   assignor itself).
4. For a first implementation: produce the same output as round-robin. The
   `owned_partitions` data is available in `MemberMetadata` for future use.

---

## Implementation Plan

### Step 1 — Refactor `assignor.rs`: extract shared helpers

The `Topition` struct and `BTreeSet<Topition>` builder are private to `assignor.rs`. Move
them (or re-export) so all assignors can use them.

**File**: `nisshi-sans-io/src/consumer/assignor.rs`

Add a public(crate) helper:

```rust
pub(super) fn sorted_members(members: &[JoinGroupResponseMember]) -> Vec<&JoinGroupResponseMember> {
    let mut v: Vec<_> = members.iter().collect();
    v.sort_by_key(|m| m.member_id.as_str());
    v
}
```

The `Topition` struct and its `From<&MetadataResponse>` impl should stay here or be lifted
to the parent `assignor` module (currently a single `assignor.rs` file; consider splitting
into `assignor/mod.rs` if the file grows).

Also add a helper to build assignments from a `HashMap<&str, Vec<Topition>>`:

```rust
pub(super) fn build_assignments(
    assignments: impl IntoIterator<Item = (String, Vec<Topition>)>,
) -> Result<Vec<SyncGroupRequestAssignment>, Error>
```

### Step 2 — Implement `RoundRobinAssignor`

**File**: `nisshi-sans-io/src/consumer/assignor/round_robin.rs`

```rust
pub struct RoundRobinAssignor;

impl ConsumerAssignor for RoundRobinAssignor {
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error> {
        // 1. sort members by member_id
        // 2. parse each member's MemberMetadata to get subscription topics
        // 3. build BTreeSet<Topition> from metadata
        // 4. for each topition in sorted order, assign to the next member
        //    (round-robin) that is subscribed to that topic
        // 5. encode MemberAssignment per member and return SyncGroupRequestAssignment
    }
}
```

Export from `assignor.rs` as `pub use round_robin::RoundRobinAssignor`.

### Step 3 — Implement `UniformAssignor`

**File**: `nisshi-sans-io/src/consumer/assignor/uniform.rs`

For v1 this is a thin wrapper around `RoundRobinAssignor::assign`.

```rust
pub struct UniformAssignor;

impl ConsumerAssignor for UniformAssignor {
    fn assign(...) -> ... {
        RoundRobinAssignor.assign(members, metadata)
    }
}
```

### Step 4 — Implement `CooperativeStickyAssignor`

**File**: `nisshi-sans-io/src/consumer/assignor/cooperative_sticky.rs`

For v1, same delegate-to-round-robin approach. Add a `TODO` comment for the incremental
rebalance logic that reads `owned_partitions`.

### Step 5 — Wire up `consumer.rs`

In `consumer.rs`:

- `pub use` the new assignors.
- Update `GroupConsumer::ASSIGNORS` from `&[Assignor::Range]` to all four variants
  once the implementations exist.

---

## Test Plan (using `range.rs` as template)

Each assignor module gets a `#[cfg(test)] mod tests { ... }` block mirroring `range.rs`.

### Shared test helpers (in `assignor.rs` or a `tests/` helper module)

```rust
fn metadata_response_topic(topic: &str, partitions: Range<i32>) -> MetadataResponseTopic
fn member(id: &str, topics: &[&str]) -> JoinGroupResponseMember
```

### `RoundRobinAssignor` tests

| Test name | Setup | Expected result |
|---|---|---|
| `even_single_topic` | 3 members, topic t0 with 3 partitions | each member gets exactly 1 partition |
| `even_two_topics` | 3 members, t0 and t1 each 3 partitions | each member gets 1 partition from each topic |
| `uneven_single_topic` | 2 members, t0 with 3 partitions | member[0] gets [0,2], member[1] gets [1] |
| `interleaved_two_topics` | 2 members, t0 3p + t1 3p | C0 gets t0:[0,2] t1:[1], C1 gets t0:[1] t1:[0,2] |
| `different_subscriptions` | 2 members: C0→[t0,t1], C1→[t1], t0 2p + t1 2p | C0 gets t0:[0,1] t1:[0], C1 gets t1:[1] |

The "different subscriptions" test validates the skip logic (topition is skipped if the
next round-robin member is not subscribed to that topic).

### `UniformAssignor` tests

Same matrix as round-robin (since v1 delegates). Just confirm the output is identical to
`RoundRobinAssignor` for the same inputs.

### `CooperativeStickyAssignor` tests

Same matrix as round-robin for v1. Additionally:

| Test name | Setup | Expected result |
|---|---|---|
| `rebalance_no_change` | 2 members with owned_partitions matching balanced state | assignments are unchanged (stickiness) |

The rebalance test exercises that `owned_partitions` is parsed from `MemberMetadata` — even
if v1 doesn't honour it yet, it validates the data round-trip.

---

## File Layout After Changes

```
nisshi-sans-io/src/consumer/
├── assignor.rs              # Topition, sorted_members helper, module declarations
├── assignor/
│   ├── range.rs             # existing RangeAssignor (unchanged)
│   ├── round_robin.rs       # new RoundRobinAssignor
│   ├── uniform.rs           # new UniformAssignor (delegates to round-robin)
│   └── cooperative_sticky.rs # new CooperativeStickyAssignor (delegates for v1)
├── codec.rs                 # existing (unchanged)
└── consumer.rs              # updated: new pub uses + ASSIGNORS constant
```

Note: `assignor.rs` currently has both `mod range;` and the `Topition` struct. When adding
more submodules, converting `assignor.rs` → `assignor/mod.rs` is the natural refactor.

---

## Out of Scope

- **Range bug**: the existing `RangeAssignor` merges partition indices across all topics.
  Kafka's range assignor is per-topic. Fix in a separate PR.
- **Incremental rebalance logic** for cooperative-sticky (KIP-429 full implementation).
- **Rack-aware assignment** (uses `rack_id` in `ConsumerProtocolSubscription`).
- **Changing `GroupConsumer::ASSIGNORS`** — depends on whether the broker should advertise
  all four protocols. Expand it once tests pass.
