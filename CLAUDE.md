# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Nisshi is a stateless, Apache Kafka-compatible broker written in Rust. It is a drop-in replacement for Apache Kafka with pluggable storage backends: PostgreSQL, libSQL (SQLite), S3/object store, and memory. Schema-backed topics (Avro, JSON Schema, Protocol Buffers) can be written as Apache Iceberg or Delta Lake tables.

- Rust edition 2024, toolchain pinned to 1.98 (`rust-toolchain.toml`)
- License: Apache-2.0
- `unsafe_code` is forbidden workspace-wide

## Build & Test Commands

The project uses `just` as a task runner (loads `.env` automatically).

```shell
just                 # default: fmt, build, test, clippy
just build           # build with all features (dev profile)
just build-all       # build every target (bins, examples, tests, benches) with all features - use this to verify a change builds cleanly workspace-wide
just test            # nextest + doc tests - use this to rerun the full test suite after a change
just test-workspace  # cargo nextest run --workspace --all-targets --all-features
just test-doc        # cargo test --workspace --doc --all-features
just clippy          # cargo clippy --workspace --all-features --all-targets -- -D warnings
just fmt             # cargo fmt --all --check
just check           # cargo check --workspace --all-features --all-targets
just ci              # (re)starts the docker compose services (postgres, minio, lakehouse) that integration tests depend on - safe to rerun if services are in a bad state
```

Run a single test with nextest:
```shell
cargo nextest run --workspace --all-features -E 'test(test_name_here)'
```

### Local Development Environment

```shell
cp example.env .env    # then edit .env as needed (AWS_ENDPOINT for local minio, etc.)
just ci                # starts minio, postgres, lakehouse via docker compose
just broker            # build + start broker with full infrastructure
just broker-postgres   # broker with postgres backend only
just broker-sqlite     # broker with sqlite backend only
just broker-memory     # broker with in-memory backend only
just broker-s3         # broker with S3/minio backend only
```

Note: when running nisshi directly (not via docker compose), set `AWS_ENDPOINT="http://localhost:9000"` in `.env`.

### Worktrees

- **Build output stays per worktree.** Each worktree builds into its own `target/`. Don't point worktrees at a shared one (`CARGO_TARGET_DIR`, `build.target-dir`). Cargo gives a workspace crate the same artifact names in every checkout and decides whether to rebuild by comparing file modification times, so a checkout whose sources are older than another checkout's last build reuses that build, and `cargo test` runs the other worktree's code.
- **`.env` is copied into Claude Code worktrees.** `.worktreeinclude` lists `.env`, so `claude --worktree` and subagent worktrees get a copy. For a worktree created any other way, copy `.env` yourself.

## Architecture

Cargo workspace with 22 member crates (including the dev-only `fuzz` crate), producing a single binary (`nisshi`) with subcommands: `broker` (default), `cat`, `topic`, `generator`, `perf`, `proxy`.

### Key Crates

| Crate | Role |
|-------|------|
| `nisshi` | Binary entry point, subcommand dispatch |
| `nisshi-broker` | Kafka API broker: `Broker<G, S>` generic over Coordinator + Storage |
| `nisshi-sans-io` | **Code-generated** Kafka wire protocol (pure serde, no I/O) |
| `nisshi-service` | Network service layers built on `rama` (Layer/Service composition) |
| `nisshi-storage` | Storage abstraction: `StorageContainer` enum over backends |
| `nisshi-schema` | Schema registry + Iceberg/Delta/Parquet lake integration |
| `nisshi-client` | Async Kafka protocol client (rama service layers) |
| `nisshi-model` | Kafka JSON protocol definitions (used in build.rs) |
| `nisshi-cat` | CLI: produce/consume Avro, JSON, Protobuf messages |
| `nisshi-cli` | Clap-based CLI argument parsing |

### Sans-I/O Code Generation (`nisshi-sans-io`)

`nisshi-sans-io/build.rs` reads ~185 official Kafka JSON message descriptors from `nisshi-sans-io/message/*.json` and generates typed Rust structs for every request/response pair. **Do not manually edit generated files.** The message JSON files are from upstream Apache Kafka.

The generator also emits `#[derive(arbitrary::Arbitrary)]` on every generated type, gated behind the `arbitrary` feature flag (`dep:arbitrary`, see `fn arbitrary_field_attribute` in `build.rs`) — this is what lets the `fuzz` crate construct arbitrary `Request`/`Response` values directly from fuzzer bytes instead of fuzzing only the wire decoder. A handful of fields need a hand-written `#[arbitrary(with = ...)]`/`#[arbitrary(default)]` attribute to stay sound (e.g. raw-byte fields route through `nisshi-sans-io/src/arbitrary_support.rs` rather than deriving); see the doc comment on `arbitrary_field_attribute` for the full list and why each one is needed.

### Service Layer Pattern (`nisshi-service`)

Uses `rama` crate for Layer/Service composition:
- `TcpBytesLayer` (TCP) -> `BytesFrameLayer` (bytes -> Kafka Frame) -> `FrameRouteService` (route to typed handlers) -> `FrameBytesLayer` -> `BytesTcpService`
- Same layering pattern used for broker, proxy, and CLI clients

#### rama 0.4.0: `Context<State>` replaced by `Extensions`

As of rama 0.4.0, `Service::serve` takes a single `req` parameter — the old `Service<State, Req>::serve(&self, ctx: Context<State>, req: Req)` two-parameter form and generic `State` are gone. Any ambient data (auth state, cluster id, maximum frame size, etc.) that used to travel via `Context<State>` now travels as a `rama::extensions::Extensions` bag carried *inside* the request wrapper type — see `nisshi-sans-io/src/input.rs` for `FrameInput`, `BodyInput`, `RequestInput<Q>`, and `BytesInput`, each a `{ value, extensions: Extensions }` pair.

Two things about `Extensions` are easy to get wrong when touching this code:

- **It is not fresh per request.** `Extensions` is `Arc`-backed; `.clone()` shares the same underlying store, and it is typically created once per connection (or per long-lived session, e.g. a consumer group) and reused/cloned across every request on it — not reconstructed per request the way the old per-call `Context` was. Code that inserts something once (e.g. `AuthenticationExtension` in `BytesFrameService`) must check `extensions.contains::<T>()` before inserting, since on the 2nd+ request on the same connection it will already be there. Don't assert it's absent — that's a real invariant violation waiting to break multi-round-trip flows like SCRAM.
- **`.fork()` vs `.clone()`**: `.fork()` creates an isolated child scope — reads fall through to the parent, but inserts land only on the child and don't leak back up. Use `.fork()` when issuing an internal/side request that should see the caller's extensions but not mutate them (e.g. `nisshi-proxy`'s internal `DescribeConfigsRequest` lookup). Use `.clone()` (sharing the same store) when the request is part of the same logical session and should accumulate/observe state alongside sibling requests (e.g. `ConsumerGroupService`'s per-session `Extensions` field).
- **`Extension` requires an explicit impl** (or `#[derive(Extension)]`) — there is no blanket impl for arbitrary `T`, so a generic type can't automatically be stored as an extension unless its concrete implementors opt in.

Per-handler dependencies (the `Coordinator` in `nisshi-broker/src/broker/group/*.rs`, the `Storage` handle `G` in `nisshi-storage/src/service/*.rs`) are still injected as plain struct fields (`struct FooService<C> { coordinator: C }`) on each handler rather than through the `Extensions` bag — that's deliberate, not a leftover of the migration.

### Storage Backends (`nisshi-storage`)

Selected at compile time via feature flags, dispatched at runtime through `StorageContainer` enum:
- `memory://` - in-memory (feature: `dynostore`)
- `s3://` - S3/MinIO (feature: `dynostore`)
- `postgres://` - PostgreSQL (feature: `postgres`)
- `sqlite://` - libSQL/SQLite (feature: `libsql`)
- `slatedb://` - SlateDB KV store (feature: `slatedb`)

`RequestChannelService`'s `Storage` trait impl (`nisshi-storage/src/service.rs`) sends a `Request` over a channel and extracts the matching `Response` variant; every method does this via the `serve_and_extract!(self, request_expr, ResponseVariant)` macro rather than repeating the match/unwrap boilerplate - add new methods the same way.

### Broker Specifics

- Node ID is always **111** (single-node, stateless design - this is intentional)
- Group coordination in `nisshi-broker/src/coordinator/group/`
- `EnvVarExp<T>` wrapper allows CLI args with `${VAR}` references expanded at parse time
- All `Error` types implement `Clone` (non-Clone errors wrapped in `Arc`)

## Feature Flags

Default: `dynostore`, `postgres`, `libsql`, `slatedb`. Full build: `delta,dynostore,iceberg,libsql,parquet,postgres,slatedb`.

Lake features: `parquet`, `iceberg`, `delta` - enable writing schema-backed topics to data lake tables.

## Testing Notes

- Tests use `cargo-nextest` (not `cargo test` for workspace tests)
- CI runs nextest with the `ci` profile in `.config/nextest.toml` (`NEXTEST_PROFILE=ci`), which kills a test still running after 5 minutes; local runs use the default profile
- Test logs go to `logs/<crate-name>/` (one file per test thread, dirs must exist)
- Integration tests require external services started via `just ci` (postgres, minio, lakehouse); rerun `just ci` if those services are in a bad state, then `just test` to rerun the suite
- Tests load `.env` via `dotenv().ok()`
- Tests in `nisshi-broker` run against multiple backends: InMemory, Lite (libSQL), Postgres, SlateDb
- `nisshi-broker`, `nisshi-sans-io` and `nisshi-service` each build one integration-test binary, `it`. To add a test file, create `tests/it/<name>.rs` and declare it with `pub mod <name>;` in `tests/it/main.rs`; Cargo ignores undeclared files, and the `every_test_file_is_declared` test fails if one is missed. Gate backend-specific tests with `#[cfg(feature = "...")]` on a module, not `required-features`. Run one file's tests with a name filter, e.g. `cargo nextest run -p nisshi-broker --all-features -E 'test(/^fetch::/)'`
- Single-file test targets with specific feature requirements (e.g. `nisshi-schema`'s `berg`) use `required-features` in their `Cargo.toml`

## Fuzz Testing (`fuzz`)

The `fuzz` crate (`cargo-fuzz` + `libfuzzer-sys`) is a dev-only workspace member, excluded from `just test-workspace` and not run in CI — it's a local/manual tool. It requires the nightly toolchain, which `just` targets invoke explicitly (`cargo +nightly fuzz ...`); you don't need to switch your default toolchain.

```shell
just cargo-fuzz +args        # thin wrapper: cargo +nightly fuzz {{args}}
just fuzz-request-decode     # fuzz_request_decode, 60s
just fuzz-member-metadata    # fuzz_member_metadata, 60s
just fuzz-generate-seed      # (re)generate the corpus seed files, see below
just fuzz-all seconds="60"   # every target in turn, {{seconds}} each, stops at the first crash

cargo +nightly fuzz list                             # list all targets
cargo +nightly fuzz run <target> -- -max_total_time=60   # fuzz one target
cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<crash-file>  # replay a saved crash
```

Three target shapes, all in `fuzz/fuzz_targets/`:
- **`fuzz_request_decode` / `fuzz_response_decode` / `fuzz_varint` / `fuzz_deflated_batch` / `fuzz_member_metadata`** — fuzz raw bytes straight through the wire decoder/encoder, no `Storage` involved.
- **`fuzz_<api>_storage`** (the majority, ~23 targets, one per Kafka API like `fuzz_fetch_storage`, `fuzz_create_topics_storage`) — build an arbitrary typed request (via `nisshi-sans-io`'s `arbitrary` feature) and run it end-to-end through the real `*Service` against an in-memory `StorageContainer` (storage starts empty every execution), to catch panics/invariant violations in service and storage logic rather than the decoder. One known exception: `fuzz_delete_records_storage` is excluded from `fuzz-all` because `nisshi-storage-dynostore`'s `delete_records` is currently an unconditional `todo!()`, so it panics on its first execution — a known pre-existing gap, kept for parity and ready to start finding real bugs once that stub is filled in.
- **`generate_seeds`** (plain `fn main`, not a fuzz target) — writes hand-built binary seed files into `fuzz/corpus/<target>/`; rerun it after adding a new seed case.

`fuzz/src/lib.rs` provides the shared `check_request`/`check_response` helpers used by most storage targets: they encode a value at its API's latest version, decode it, re-encode the decoded value, and decode again — then assert the two decodes are equal. They deliberately do **not** assert the first decode equals the original fuzzed value: a sans-io codec is not required to be lossless against an arbitrary in-memory value (e.g. every generated sequence field is `Option<Vec<T>>` regardless of wire nullability, so a mandatory array left at `None` is wire-equivalent to `Some(vec![])` but not `PartialEq`). The round-trip property that must hold is that re-encoding an already-decoded (version-normalized) value decodes back to exactly the same thing.

`fuzz/corpus/`, `fuzz/artifacts/` (crash reproducers) and `fuzz/.gitignore`'s `target`/`coverage` are all untracked — rebuild the corpus locally with `just fuzz-generate-seed` plus whatever `cargo +nightly fuzz run` accumulates. If you fix a bug found by fuzzing, replay every existing artifact for that target (`cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<crash-file>`) to confirm the fix, not just the one input that was being worked on.

## CI Pipeline

GitHub Actions (`.github/workflows/ci.yml`) runs in two tiers, gated by `ci-gate`, the single required check that fans in every other job:

- **Tier A, every pull_request push:** `fmt`, `clippy`, `typos`, `third-party-license`, `test` (postgres:17 only), one non-experimental leg each of `compat-librdkafka` / `compat-franz-go`.
- **Tier B, once per merge-queue entry (`merge_group`) and on push to `main`:** the full `build-storage` / `build-storage-lake` feature matrix, `test` on postgres:16/17/18, the experimental compat legs, `cargo-publish-dry-run`, `src`, `release`, `package`, `smoke` (Java Kafka client, Kafka 3.7/3.8/3.9).

Merging goes through a merge queue: "Merge when ready" queues the PR, the queue re-runs CI on it against the current tip of `main`, and merges with a merge commit if everything is green. Tier B is skipped on PRs only while the `MERGE_QUEUE` repository variable is `on`; with it unset, PRs run everything. The other required checks come from `codeql.yml`, `workflow-lint.yml` and `dependencies.yml`.

## Key Files

| File | Purpose |
|------|---------|
| `justfile` | All build/test/run tasks |
| `example.env` | Template for local `.env` config |
| `compose.yaml` | Docker Compose: postgres, minio, grafana, jaeger, prometheus, lakehouse |
| `etc/initdb.d/010-schema.sql` | PostgreSQL schema DDL |
| `etc/schema/` | Sample schemas: `.avsc` (Avro), `.json` (JSON Schema), `.proto` (Protobuf) |
| `nisshi-sans-io/message/` | Kafka JSON protocol descriptors (upstream, ~185 files) |
| `nisshi-sans-io/build.rs` | Code generator: JSON descriptors -> Rust types |
| `fuzz/` | `cargo-fuzz` targets exercising the sans-io codec and storage services (see Fuzz Testing) |

## Lint Configuration

Workspace-level in `Cargo.toml`: `clippy::all = warn`, `unsafe_code = forbid`, `non_ascii_idents = forbid`, `rust_2018_idioms = deny`, `unreachable_pub = warn`, `broken_intra_doc_links = deny`. CI runs `clippy -- -D warnings` (all warnings are errors).
