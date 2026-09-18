# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Nisshi is a stateless, Apache Kafka-compatible broker written in Rust. It is a drop-in replacement for Apache Kafka with pluggable storage backends: PostgreSQL, libSQL (SQLite), S3/object store, and memory. Schema-backed topics (Avro, JSON Schema, Protocol Buffers) can be written as Apache Iceberg or Delta Lake tables.

- Rust edition 2024, toolchain pinned to 1.93 (`rust-toolchain.toml`)
- License: Apache-2.0
- `unsafe_code` is forbidden workspace-wide

## Build & Test Commands

The project uses `just` as a task runner (loads `.env` automatically).

```shell
just                 # default: fmt, build, test, clippy
just build           # build with all features (dev profile)
just test            # nextest + doc tests
just test-workspace  # cargo nextest run --workspace --all-targets --all-features
just test-doc        # cargo test --workspace --doc --all-features
just clippy          # cargo clippy --workspace --all-features --all-targets -- -D warnings
just fmt             # cargo fmt --all --check
just check           # cargo check --workspace --all-features --all-targets
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

## Architecture

Cargo workspace with 15 member crates, producing a single binary (`nisshi`) with subcommands: `broker` (default), `cat`, `topic`, `generator`, `perf`, `proxy`.

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

### Service Layer Pattern (`nisshi-service`)

Uses `rama` crate for Layer/Service composition:
- `TcpBytesLayer` (TCP) -> `BytesFrameLayer` (bytes -> Kafka Frame) -> `FrameRouteService` (route to typed handlers) -> `FrameBytesLayer` -> `BytesTcpService`
- Same layering pattern used for broker, proxy, and CLI clients

### Storage Backends (`nisshi-storage`)

Selected at compile time via feature flags, dispatched at runtime through `StorageContainer` enum:
- `memory://` - in-memory (feature: `dynostore`)
- `s3://` - S3/MinIO (feature: `dynostore`)
- `postgres://` - PostgreSQL (feature: `postgres`)
- `sqlite://` - libSQL/SQLite (feature: `libsql`)
- `slatedb://` - SlateDB KV store (feature: `slatedb`)

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
- Test logs go to `logs/<crate-name>/` (one file per test thread, dirs must exist)
- Integration tests require external services started via `just ci`
- Tests load `.env` via `dotenv().ok()`
- Tests in `nisshi-broker` run against multiple backends: InMemory, Lite (libSQL), Postgres, SlateDb
- Tests with specific feature requirements use `required-features` in their `Cargo.toml`

## CI Pipeline

GitHub Actions (`.github/workflows/ci.yml`): check -> fmt -> clippy -> build-storage (each feature independently) -> build-storage-lake (feature combinations) -> test (PostgreSQL 16/17/18 matrix) -> publish dry-run -> typos -> smoke tests (Java Kafka client, Kafka 3.7/3.8/3.9).

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

Workspace-level in `Cargo.toml`: `clippy::all = warn`, `unsafe_code = forbid`, `non_ascii_idents = forbid`, `rust_2018_idioms = deny`, `unreachable_pub = warn`, `broken_intra_doc_links = deny`. CI runs `clippy -- -D warnings` (all warnings are errors).
