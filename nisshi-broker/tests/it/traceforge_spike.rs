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

//! Phase 0 feasibility spike for the `traceforge` crate.
//!
//! This models the race in `update_group_conditional::concurrent_same_version`
//! against the real in-memory (dynostore) storage engine: two writers racing
//! `Storage::update_group` on the same group id and version, of which exactly
//! one must win and the other must see `Outdated`.
//!
//! An earlier version of this spike spawned two real `thread::spawn` closures
//! that each called `future::block_on(storage.update_group(..))` concurrently,
//! expecting `verify()` to discover both possible outcomes. It didn't: neither
//! closure contains any TraceForge-visible scheduling point (no
//! `send_msg`/`recv_msg`, no `traceforge::sync` primitive, no `nondet()`), so
//! TraceForge has nothing to fork the search on. It ran the two closures in a
//! fixed order every time (`stats.execs == 1`), and swapping which thread was
//! spawned first flipped who won but never explored the other order. Real,
//! uninstrumented async code is opaque to TraceForge: it executes correctly,
//! but concurrency hidden inside it (real `std::sync::Mutex`, real
//! `object_store` CAS, ...) is not discovered automatically, however many
//! real OS/tokio threads it actually runs on.
//!
//! The fix is to make the contended decision explicit as a TraceForge
//! `nondet()` choice, then apply it by *sequencing* the (still real) calls
//! into storage accordingly, instead of running them concurrently. That
//! gives TraceForge an actual decision point to fork the search on, so
//! `verify()` explores both orders (`stats.execs == 2`) while every step
//! still runs the genuine `Storage::update_group` logic.
//!
//! Note what this does and doesn't prove. It is not TraceForge discovering a
//! race by interleaving two truly concurrent calls — it's enumerating the two
//! candidate *orderings* of a decision we identified by hand (who commits
//! first) and running the real logic once per ordering. For two participants
//! that's no more than a hand-written test for each order would give you. The
//! payoff shows up once there are enough independent decision points that
//! hand-enumeration stops being practical (e.g. the 3-member join/sync/
//! heartbeat interleavings in `traceforge_group_join.rs`'s Phase 1) —
//! `nondet()` and
//! TraceForge's search handle the combinatorics automatically, provided each
//! contended decision is exposed to it explicitly like `write_two_first`
//! below. TraceForge only explores what it can see: it will not find a race
//! in unmodified nisshi code on its own.

#![cfg(feature = "dynostore")]

use crate::common::{alphanumeric_string, memory_storage};
use nisshi_storage::{GroupDetail, Storage};
use traceforge::{Config, TypeNondet, cover, future, verify};

fn generation(generation_id: i32) -> GroupDetail {
    GroupDetail {
        generation_id,
        ..Default::default()
    }
}

#[test]
fn concurrent_update_group_race_is_exhaustively_explored() {
    let stats = verify(Config::builder().build(), || {
        let storage = future::block_on(memory_storage("spike", 111)).expect("storage");
        let group_id = alphanumeric_string(15);

        let version = future::block_on(storage.update_group(&group_id, generation(1), None))
            .expect("initial create");

        // Which writer's compare-and-swap reaches storage first decides the
        // race; make that decision a TraceForge choice so `verify()` explores
        // both orders, instead of a real concurrent call TraceForge can't see
        // into.
        let generation_two_first = bool::nondet();

        let (first, second) = if generation_two_first {
            (
                future::block_on(storage.update_group(
                    &group_id,
                    generation(2),
                    Some(version.clone()),
                )),
                future::block_on(storage.update_group(&group_id, generation(3), Some(version))),
            )
        } else {
            (
                future::block_on(storage.update_group(
                    &group_id,
                    generation(3),
                    Some(version.clone()),
                )),
                future::block_on(storage.update_group(&group_id, generation(2), Some(version))),
            )
        };

        // Whichever call is sequenced first always wins a fresh compare-and-
        // swap - that part is a tautology, not a finding. What's actually
        // being checked is that the invariant ("exactly one writer commits,
        // the other sees `Outdated`") holds under both nondet()-selected
        // orderings, and `cover!` records that TraceForge really did visit
        // both of them rather than just the one its default schedule prefers.
        match (generation_two_first, first.is_ok(), second.is_ok()) {
            (true, true, false) => {
                cover!("generation_2_committed_first");
            }
            (false, true, false) => {
                cover!("generation_3_committed_first");
            }
            otherwise => panic!("expected exactly one writer to win, got: {otherwise:?}"),
        }
    });

    println!(
        "traceforge stats: execs={} blocked={}",
        stats.execs, stats.block
    );

    assert_eq!(
        stats.execs, 2,
        "expected both nondet() branches to be explored"
    );
    assert!(
        stats
            .coverage
            .is_covered("generation_2_committed_first".to_string())
    );
    assert!(
        stats
            .coverage
            .is_covered("generation_3_committed_first".to_string())
    );
}
