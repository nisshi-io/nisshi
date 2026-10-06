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
just test-workspace  # cargo nextest run --workspace --all-targets --all-features, excluding fuzz and nisshi-smoke-test
just test-doc        # cargo test --workspace --doc --all-features
just doc             # rustdoc, warnings denied, private items too; pass --open to browse
just clippy          # cargo clippy --workspace --all-features --all-targets -- -D warnings
just fmt             # cargo fmt --all --check
just check           # cargo check --workspace --all-features --all-targets
just smoke <engine>  # Kafka CLI smoke suite (nisshi-smoke-test) against postgres, sqlite, memory or s3; starts and removes its own broker and services
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
- **Seed a new worktree with `just seed-target` before its first build.** The recipe clones the main checkout's `target/debug` copy-on-write, so the clone takes no extra disk until cargo rewrites a file. It then runs `cargo clean --workspace` on the clone, so only dependency artifacts carry over and the workspace crates rebuild from this worktree's sources. The recipe seeds no release, profiling or cross-target build, because that clean covers only the dev profile. It does nothing when `target/debug` already exists, and an interrupted seed leaves no `target/debug`, so a rerun starts again. Don't run it while the main checkout builds. Don't bring `target/` into a worktree any other way (`.worktreeinclude`, `worktree.symlinkDirectories`, a plain copy): those carry workspace-crate artifacts over, and cargo then reuses another checkout's code as the previous bullet describes.
- **`.env` is copied into Claude Code worktrees.** `.worktreeinclude` lists `.env`, so `claude --worktree` and subagent worktrees get a copy. For a worktree created any other way, copy `.env` yourself.

## Architecture

Cargo workspace (members, including the `fuzz` harness, are listed in the root `Cargo.toml`), producing a single binary (`nisshi`) with subcommands: `broker` (default), `cat`, `topic`, `user`, `generator`, `perf`, `proxy`.

### Key Crates

| Crate | Role |
|-------|------|
| `nisshi` | Binary entry point, subcommand dispatch |
| `nisshi-broker` | Kafka API broker: `Broker<G, S>` generic over Coordinator + Storage |
| `nisshi-sans-io` | **Code-generated** Kafka wire protocol (pure serde, no I/O) |
| `nisshi-service` | Network service layers built on `rama` (Layer/Service composition) |
| `nisshi-storage` | Storage abstraction: `Storage`/`StorageFactory` traits, `StorageContainer` builder |
| `nisshi-storage-dynostore` | Storage factories for `memory://`, `s3://`, `gs://` (ObjectStore-backed) |
| `nisshi-storage-null` | Storage factory for `null://`, a stub backend with no message persistence |
| `nisshi-storage-slatedb` | Storage factory for `slatedb://` (SlateDB, a KV store on object storage) |
| `nisshi-storage-sql` | Storage factories for `postgres://` and `sqlite://` (PostgreSQL, libSQL) |
| `nisshi-schema` | Schema registry + Iceberg/Delta/Parquet lake integration |
| `nisshi-client` | Async Kafka protocol client (rama service layers) |
| `nisshi-model` | Kafka JSON protocol definitions (used in build.rs) |
| `nisshi-auth` | SASL authentication: mechanism handshake, SCRAM credential verification |
| `nisshi-cat` | CLI: produce/consume Avro, JSON, Protobuf messages |
| `nisshi-cli` | Clap-based CLI argument parsing |
| `nisshi-generator` | CLI: generate fake data for schema-backed topics |
| `nisshi-otel` | OpenTelemetry common configuration |
| `nisshi-perf` | CLI: performance producer/consumer |
| `nisshi-proxy` | Kafka API-compatible proxy |
| `nisshi-topic` | CLI: create, list, delete topics |

### Sans-I/O Code Generation (`nisshi-sans-io`)

`nisshi-sans-io/build.rs` reads ~185 official Kafka JSON message descriptors from `nisshi-sans-io/message/*.json` and generates typed Rust structs for every request/response pair. **Do not manually edit generated files.** The message JSON files are from upstream Apache Kafka.

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

### Storage Backends (`nisshi-storage`, `nisshi-storage-dynostore`, `nisshi-storage-null`, `nisshi-storage-slatedb`, `nisshi-storage-sql`)

Selected at compile time via feature flags, dispatched at runtime through a `StorageFactory` trait, not an enum: `StorageContainer::builder()` (`nisshi-storage/src/lib.rs`) collects one `Arc<dyn StorageFactory>` per enabled backend, and `.build()` picks the first whose `scheme()` regex matches the configured storage URL. Each backend crate implements the trait in its own `factory.rs`:
- `memory://`, `s3://`, `gs://` - ObjectStore-backed (`nisshi-storage-dynostore`, feature: `dynostore`)
- `null://` - stub backend, tracks topic metadata only, no message storage (`nisshi-storage-null`, always compiled in)
- `postgres://` - PostgreSQL (`nisshi-storage-sql`, feature: `postgres`)
- `sqlite://` - libSQL/SQLite (`nisshi-storage-sql`, feature: `libsql`)
- `slatedb://` - SlateDB KV store (`nisshi-storage-slatedb`, feature: `slatedb`)

`RequestChannelService`'s `Storage` trait impl (`nisshi-storage/src/service.rs`) sends a `Request` over a channel and extracts the matching `Response` variant; every method does this via the `serve_and_extract!(self, request_expr, ResponseVariant)` macro rather than repeating the match/unwrap boilerplate - add new methods the same way.

### Broker Specifics

- Node ID is always **111** (single-node, stateless design - this is intentional)
- Group coordination in `nisshi-broker/src/coordinator/group/`
- `EnvVarExp<T>` wrapper allows CLI args with `${VAR}` references expanded at parse time
- All `Error` types implement `Clone` (non-Clone errors wrapped in `Arc`)

## Feature Flags

Default: `dynostore`, `postgres`, `libsql`, `slatedb`. The `just build`/`release` feature lists below omit `turso`, which the broker and storage crates enable by default. Full build: `delta,dynostore,iceberg,libsql,parquet,postgres,slatedb`.

Lake features: `parquet`, `iceberg`, `delta` - enable writing schema-backed topics to data lake tables.

## Testing Notes

- Tests use `cargo-nextest` (not `cargo test` for workspace tests)
- CI runs nextest with the `ci` profile in `.config/nextest.toml` (`NEXTEST_PROFILE=ci`), which kills a test still running after 5 minutes; local runs use the default profile
- Test logs go to `logs/<crate-name>/` (one file per test thread, dirs must exist)
- Integration tests require external services started via `just ci` (postgres, minio, lakehouse); rerun `just ci` if those services are in a bad state, then `just test` to rerun the suite
- Tests load `.env` via `dotenv().ok()`
- Tests in `nisshi-broker` run against multiple backends: InMemory, Lite (libSQL), Postgres, SlateDb
- `nisshi-broker`, `nisshi-sans-io` and `nisshi-service` each build one integration-test binary, `it`. To add a test file, create `tests/it/<name>.rs` and declare it with `pub mod <name>;` in `tests/it/main.rs`; Cargo ignores undeclared files, and the `every_test_file_is_declared` test fails if one is missed. Gate backend-specific tests with `#[cfg(feature = "...")]` on a module, not `required-features`. Run one file's tests with a name filter, e.g. `cargo nextest run -p nisshi-broker --all-features -E 'test(/^fetch::/)'`
- Give each feature its own test file. Put the tests of one feature or one Kafka API in one file under `tests/it/`, named after it. Tests for a new feature go in a new file, not at the end of a file about another subject. A reader then finds them by name, and a name filter on the module selects them.
- `nisshi-smoke-test` runs the real Kafka CLI tools (in a `$KAFKA_IMAGE` container) against a broker, and stays out of `just test`; run it with `just smoke <engine>`. Its containers use the host network, so on macOS it needs Docker Desktop with host networking turned on. Its tests talk to the shared broker through `Broker::shared()`, or start their own with `Broker::isolated()`, and each uses its own topic and group names (`nisshi_smoke_test::unique_name`). A test that fails because of an open bug is marked `#[ignore = "<what is broken>"]`, or `#[cfg_attr(feature = "<engine>", ignore = "...")]` when it fails only on some engines. CI skips it, local runs still run it (`run.sh` passes `--run-ignored all` outside CI), and the change that fixes the bug removes the attribute
- Single-file test targets with specific feature requirements (e.g. `nisshi-schema`'s `berg`) use `required-features` in their `Cargo.toml`

## CI Pipeline

GitHub Actions (`.github/workflows/ci.yml`) runs in two tiers, gated by `ci-gate`, the single required check that fans in every other job:

- **Tier A, every pull_request push:** `fmt`, `clippy` (which also runs `just doc`), `typos`, `third-party-license`, `test` (postgres:17 only), one non-experimental leg each of `compat-librdkafka` / `compat-franz-go`.
- **Tier B, once per merge-queue entry (`merge_group`) and on push to `main`:** the `build-storage` matrix (one build per storage engine, `turso` included) and `build-storage-lake` (one build per lake format on `dynostore`), `test` on postgres:16/17/18, the experimental compat legs, `cargo-publish-dry-run`, `src`, `release`, `package`, `smoke` (the `nisshi-smoke-test` suite: Kafka CLI tools from Kafka 3.9 and 4.3 against the packaged image on postgres, memory and s3, and against a source build on sqlite until #796 is fixed, on x86 and arm; `smoke-report` puts every leg's results in one pass/fail grid in the step summary).

Merging goes through a merge queue: "Merge when ready" queues the PR, the queue re-runs CI on it against the current tip of `main`, and squash-merges it if everything is green: the PR title becomes the commit's subject and the PR description its body. Tier B is skipped on same-repo PRs only while the `MERGE_QUEUE` repository variable is `on`; with it unset, they run everything. Fork PRs can't read the variable, so Tier B is always skipped on them and runs in the queue. The other required checks come from `codeql.yml`, `workflow-lint.yml`, `dependencies.yml` and `pr-title.yml` (the PR title must follow Conventional Commits; see `CONTRIBUTING.md`).

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

## Lint Configuration

Workspace-level in `Cargo.toml`: `clippy::all = warn`, `unsafe_code = forbid`, `non_ascii_idents = forbid`, `rust_2018_idioms = deny`, `unreachable_pub = warn`, `broken_intra_doc_links = deny`, `private_intra_doc_links = deny`. CI runs `clippy -- -D warnings` (all warnings are errors) and `just doc`, which runs rustdoc with warnings denied.
