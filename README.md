<div align="center">

# Nisshi (née Tansu) 🗃️
stateless Apache Kafka®-compatible broker with pluggable storage (PostgreSQL, SQLite, S3, memory)

<br>

[![License](https://img.shields.io/badge/License-Apache-165dfc.svg)](https://github.com/nisshi-io/nisshi/blob/main/LICENSE)
&nbsp;
[![Built with Rust](https://img.shields.io/badge/built_with-Rust-165dfc.svg?logo=rust)](https://www.rust-lang.org/)
&nbsp;
<br>
[![Docs](https://img.shields.io/badge/📖%20docs-docs.nisshi.io-165dfc.svg)](https://docs.nisshi.io/)
&nbsp;
[![Blog](https://img.shields.io/badge/%F0%9F%93%98%20blog-blog.nisshi.io-165dfc.svg)](https://blog.nisshi.io/articles)
&nbsp;
<br>
[![GitHub stars](https://img.shields.io/github/stars/nisshi-io/nisshi?style=social)](https://github.com/nisshi-io/nisshi)
&nbsp;
[![Bluesky](https://img.shields.io/bluesky/followers/nisshi.io)](https://bsky.app/profile/tansu.io)

<br>

</div>

# What is Nisshi?

[Nisshi][github-com-nisshi-io] (née Tansu) is an Apache Kafka®-compatible broker
with PostgreSQL, libSQL (SQLite), S3 or memory storage engines.
Schema backed topics (Avro, JSON or Protocol buffers) can
be written as [Apache Iceberg](https://iceberg.apache.org) or [Delta Lake](https://delta.io) tables.

Features:

- Apache Kafka®-compatible
- Available with [PostgreSQL](https://www.postgresql.org), [libSQL](https://docs.turso.tech/libsql), [S3](https://en.wikipedia.org/wiki/Amazon_S3) or memory storage engines
- Topics [validated](docs/schema-registry.md) by [JSON Schema][json-schema-org], [Apache Avro](https://avro.apache.org)
  or [Protocol buffers](protocol-buffers) can be written as [Apache Iceberg](https://iceberg.apache.org) or [Delta Lake](https://delta.io) tables

## Get Started

> [!NOTE]
> Want the quickest way to start using Nisshi? Use a prebuilt binary. See [Get Started with Nisshi](docs/quickstart.md).

Other ways to run Nisshi:

- Docker Compose — if you have Docker installed. See [Running with Docker Compose](#running-with-docker-compose).
- cargo — if you have Rust installed, or there is no prebuilt binary for your platform, such as Windows or an Intel Mac. See [Installing with crates.io](#installing-with-cratesio).

To see Nisshi write topics to a data lake, try the examples for [pyiceberg](https://github.com/nisshi-io/example-pyiceberg), [Apache Spark](https://github.com/nisshi-io/example-spark), or [Delta Lake](https://github.com/nisshi-io/example-delta-lake).

To learn more about Nisshi, see the full documentation at [docs.nisshi.io](https://docs.nisshi.io/).

## Table of contents

- [Which release to use](#which-release-to-use)
- [Installing with crates.io](#installing-with-cratesio)
- [Running with Docker Compose](#running-with-docker-compose)
- [Storage options](#storage-options)
  - [Running Docker Compose with S3 (MinIO)](#running-docker-compose-with-s3-minio)
  - [Running Docker Compose with PostgreSQL](#running-docker-compose-with-postgresql)
- [Commands](#commands)
  - [broker](#broker)
  - [topic](#topic)
  - [cat](#cat)
  - [proxy](#proxy)
- [Securing and monitoring](#securing-and-monitoring-a-broker)
- [Compatibility](#compatibility)

## Which release to use

[![Latest release](https://img.shields.io/github/v/release/nisshi-io/nisshi?label=latest%20release)][nisshi-latest-release]

Use the [latest release][nisshi-latest-release], the one marked **Latest** on the Releases page. `cargo install nisshi` and the `latest` container image both skip pre-releases.

Releases marked **Pre-release**, with tags such as `v0.8.0-pre.1`, are builds of `main` published ahead of the next release. Their behaviour and storage formats can still change, and a later version may not read the data they wrote (upgrading from 0.7.0-pre.1 or pre.2 to 0.7.0 needs the manual steps in the 0.7.0 upgrade notes). Use them for testing, not for data you want to keep. To install one, name its exact version, for example `ghcr.io/nisshi-io/nisshi:0.8.0-pre.1`.

This README describes `main`, which can be ahead of the latest release. To read the README of a release, open it at its tag, for example [v0.7.0](https://github.com/nisshi-io/nisshi/blob/v0.7.0/README.md).

## Installing with crates.io

To build and install Nisshi from [crates.io](https://crates.io/crates/nisshi), run:

```shell
cargo install nisshi --locked --features dynostore,libsql,postgres
```

The build takes several minutes. It compiles some C libraries as well as the Rust code,
so it needs:

- Rust, installed with [rustup](https://rustup.rs). The project builds and tests with
  the Rust version in [rust-toolchain.toml](rust-toolchain.toml).
- A C compiler. On macOS, install the Xcode Command Line Tools with
  `xcode-select --install`. On Debian or Ubuntu, run
  `sudo apt-get install build-essential`.

`--locked` builds with the dependency versions in the release's `Cargo.lock`, instead of
the newest compatible versions.

The `--features` list adds the `memory://`, `s3://`, `gs://`, `sqlite://`, and `postgres://` storage engines.
To add another storage engine or a data lake format, see [Storage options](#storage-options).

To check the install, run `nisshi --help`. Its last lines list the storage engines in
the build:

```shell
Storage engines: dynostore, libsql, postgres
Data lakes:
```

By default, the broker accepts connections from other computers on your network, and does not require clients to authenticate.
To start a broker that only listens to your computer, run:

```shell
nisshi --listener-url tcp://127.0.0.1:9092
```

To protect a broker that other computers connect to, see [Securing and monitoring a broker](#securing-and-monitoring-a-broker) for TLS and authentication.

The broker keeps topics and messages in memory, and loses them when it stops. To keep them, see [Storage options](#storage-options).

In a second terminal, create a topic, send a message, and read it:

```shell
nisshi topic create hello
echo '{"value": "hello world"}' | nisshi cat produce hello
nisshi cat consume hello
```

The `cat consume` command prints the message:

```json
[{"key":null,"value":"hello world"}]
```

Releases before 0.8.0 print nothing for `topic create` and `cat produce`, so on those releases only the last command prints anything.

## Running with Docker Compose

Run the broker with [Docker Compose](https://docs.docker.com/compose/) from a
clone of this repository. You don't need a `.env`: by default,
[compose.yaml](compose.yaml) runs the latest release on PostgreSQL.

```shell
docker compose up -d nisshi
```

This starts PostgreSQL and the broker, which listens on `localhost:9092`.
Compose publishes their ports on 127.0.0.1 only, so other computers can't connect.

If PostgreSQL already runs on your computer, port 5432 is taken and the command
fails with "port is already allocated" or "address already in use". Publish the
`db` service on another port:

```shell
NISSHI_DB_PORTS=127.0.0.1:15432:5432 docker compose up -d nisshi
```

To create a topic, produce a message to it, and consume it:

```shell
docker compose exec nisshi /nisshi topic create hello
echo '{"value":"hello world"}' | docker compose exec -T nisshi /nisshi cat produce hello
docker compose exec nisshi /nisshi cat consume hello
```

The consume command prints the message:

```json
[{"key":null,"value":"hello world"}]
```

If a command fails, `docker compose ps` shows whether the broker is running,
and `docker compose logs nisshi` shows why it stopped.

Stop the broker and PostgreSQL with `docker compose down`. Add `--volumes` to
also delete the stored messages.

Next steps:

- Connect any Kafka client to `localhost:9092`.
- Use another storage engine: set `STORAGE_ENGINE` in a `.env` file, for example `STORAGE_ENGINE=memory://nisshi/`. [example.env](example.env) lists the other settings, and it selects `s3://nisshi/`, which needs the bucket that [Running Docker Compose with S3 (MinIO)](#running-docker-compose-with-s3-minio) creates.
- Read the documentation at [docs.nisshi.io](https://docs.nisshi.io/).

## Storage options

Choose the engine with `--storage-engine` (or `STORAGE_ENGINE`) using the URL scheme below.

| Engine | URL scheme | Cargo feature | Status |
| --- | --- | --- | --- |
| PostgreSQL | `postgres://` | `postgres` | Suitable for production. Tested, with bugs and security issues fixed. Help comes from the community. |
| S3 and Google Cloud Storage | `s3://`, `gs://` | `dynostore` | Experimental. Available for continuity. Not recommended for production. |
| SQLite (libSQL) | `sqlite://` | `libsql` | Suitable for development. |
| In-memory | `memory://` | `dynostore` | Suitable for development and testing. Data is lost when the broker stops. |
| Turso | `turso://` | `turso` | Experimental. |
| SlateDB | `slatedb://` | `slatedb` | Deprecated. Not recommended for new deployments, because it allows a single writer. Only security fixes are accepted. The engine may be removed in a future release. Use PostgreSQL for production, or SQLite or in-memory for development. The broker logs a warning at startup. |

A build contains only the storage engines whose features you enable. To add an engine, add its feature to the `--features` list, for example
`--features dynostore,libsql,postgres,turso`. Apache Parquet, Apache Iceberg, and Delta Lake output for schema backed
topics needs the `parquet`, `iceberg`, or `delta` feature.

The engines differ in what they support. For example, only PostgreSQL and SQLite apply
retention and compaction. [Compatibility and feature status](docs/compatibility.md) lists what
each engine supports, which Kafka APIs the broker answers, and which Kafka clients CI tests.

Switching storage engines does not move data: topics, messages, and consumer group offsets stay in the old engine, and the new one starts empty. The release binary starts on `memory://`, and Docker Compose runs on PostgreSQL unless you set `STORAGE_ENGINE`.

PostgreSQL can stream transaction logs to an archive with
[continuous archiving][continuous-archiving].

Nisshi OSS is provided as-is, with community support only.

### Running Docker Compose with S3 (MinIO)

The following will configure a S3 storage engine
using the "nisshi" bucket (full context is in
[compose.yaml](compose.yaml) and [example.env](example.env)):

Copy `example.env` into `.env` so that you have a local working copy:

```shell
cp example.env .env
```

Edit `.env` so that `STORAGE_ENGINE` is defined as:

```shell
STORAGE_ENGINE="s3://nisshi/"
```

First time startup, you'll need to create a bucket, an access key
and a secret in minio.

Just bring minio up, without nisshi:

```shell
docker compose up -d minio
```

Create a minio `local` alias representing `http://localhost:9000` with the default credentials of `minioadmin`:

```shell
docker compose exec minio \
   /usr/bin/mc \
   alias \
   set \
   local \
   http://localhost:9000 \
   minioadmin \
   minioadmin
```

Create a `nisshi` bucket in minio using the `local` alias:

```shell
docker compose exec minio \
   /usr/bin/mc mb local/nisshi
```

Once this is done, you can start nisshi with:

```shell
docker compose up -d nisshi
```

Using the regular Apache Kafka CLI you can create topics, produce and consume
messages with Nisshi:

```shell
kafka-topics \
  --bootstrap-server localhost:9092 \
  --partitions=3 \
  --replication-factor=1 \
  --create --topic test
```

Describe the `test` topic:

```shell
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic test
```

Note that node 111 is the leader and ISR for each topic partition.
This node represents the broker handling your request. Every Nisshi broker
reports the same node ID, 111; it does not identify a particular machine.

Producer:

```shell
echo "hello world" | kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic test
```

Group consumer using `test-consumer-group`:

```shell
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --group test-consumer-group \
  --topic test \
  --from-beginning \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.offset=true \
  --property print.partition=true \
  --property print.headers=true \
  --property print.value=true
```

Describe the consumer `test-consumer-group` group:

```shell
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group test-consumer-group \
  --describe
```

### Running Docker Compose with PostgreSQL

To switch between the minio and PostgreSQL examples, firstly
shutdown Nisshi:

```shell
docker compose down nisshi
```

Switch to the PostgreSQL storage engine by updating [.env](.env):

```env
# minio storage engine
# STORAGE_ENGINE="s3://nisshi/"

# PostgreSQL storage engine -- use the compose service name `db`, not `localhost`
STORAGE_ENGINE="postgres://postgres:postgres@db"
```

Start PostgreSQL:

```shell
docker compose up -d db
```

The `db` service runs PostgreSQL 18, and that image does not start on a `db` volume that holds PostgreSQL 17 data. Run `docker compose down --volumes` once before the first start, or set `POSTGRES_IMAGE=postgres:17` in [.env](.env) to stay on 17.

Nisshi creates its tables when it starts on an empty database, and records the schema version there. On a PostgreSQL server of your own, give Nisshi a role that owns its database:

```sql
create role nisshi login password '...';
create database nisshi owner nisshi;
```

A broker changes the schema only when the database doesn't record the broker's schema version: on the first start, and on the first start of a release that changes the schema. To run brokers with a role that can only read and write rows, start one broker once with the credentials of the role that owns the tables, then start every broker with the read/write role. Do the same before you upgrade to a release that changes the schema. A broker whose role can't change the schema refuses to start until that has happened.

Nisshi is tested on PostgreSQL 16 and later. On PostgreSQL 16 or later, an administrator who isn't a superuser, such as the admin user of a managed service, must first run `grant nisshi to <admin>`. Nisshi supports a database that 0.7.0-pre.3 or a later release set up.

Bring Nisshi back up:

```shell
docker compose up -d nisshi
```

Using the regular Apache Kafka CLI you can create topics, produce and consume
messages with Nisshi:

```shell
kafka-topics \
  --bootstrap-server localhost:9092 \
  --partitions=3 \
  --replication-factor=1 \
  --create --topic test
```

Producer:

```shell
echo "hello world" | kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic test
```

Consumer:

```shell
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --group test-consumer-group \
  --topic test \
  --from-beginning \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.offset=true \
  --property print.partition=true \
  --property print.headers=true \
  --property print.value=true
```

Or using [librdkafka][librdkafka] to produce:

```shell
echo "Lorem ipsum dolor..." | \
  ./examples/rdkafka_example -P \
  -t test -p 1 \
  -b localhost:9092 \
  -z gzip
```

Consumer:

```shell
./examples/rdkafka_example \
  -C \
  -t test -p 1 \
  -b localhost:9092
```

## Commands

Nisshi is a single statically linked binary containing the following:

- [broker](#broker) — an Apache Kafka®-compatible broker and schema registry
- [topic](#topic) — a command line interface to create or delete topics
- [cat](#cat) — a command line interface to consume or produce Avro, JSON, or Protobuf messages to a topic
- [proxy](#proxy) — an Apache Kafka®-compatible proxy

## broker

The broker subcommand is default if no other command is supplied.

```shell
Usage: nisshi [OPTIONS]
       nisshi <COMMAND>

Commands:
  broker  Apache Kafka®-compatible broker with Avro, JSON, Protobuf schema validation [default if no command supplied]
  cat     Easily consume or produce Avro, JSON or Protobuf messages to a topic
  topic   Create or delete topics managed by the broker
  proxy   Apache Kafka®-compatible proxy
  help    Print this message or the help of the given subcommand(s)

Options:
      --kafka-cluster-id <KAFKA_CLUSTER_ID>
          All members of the same cluster should use the same id [env: CLUSTER_ID=RvQwrYegSUCkIPkaiAZQlQ] [default: nisshi_cluster]
      --kafka-listener-url <KAFKA_LISTENER_URL>
          The broker will listen on this address [env: LISTENER_URL=] [default: tcp://[::]:9092]
      --kafka-advertised-listener-url <KAFKA_ADVERTISED_LISTENER_URL>
          This location is advertised to clients in metadata [env: ADVERTISED_LISTENER_URL=tcp://127.0.0.1:9092] [default: tcp://127.0.0.1:9092]
      --storage-engine <STORAGE_ENGINE>
          Storage engine examples are: postgres://postgres:postgres@localhost, memory://nisshi/ or s3://nisshi/ [env: STORAGE_ENGINE=s3://nisshi/] [default: memory://nisshi/]
      --schema-registry <SCHEMA_REGISTRY>
          Schema registry examples are: file://./etc/schema or s3://nisshi/, containing: topic.json, topic.proto or topic.avsc [env: SCHEMA_REGISTRY=file://./etc/schema]
  -h, --help
          Print help
  -V, --version
          Print version
```

A broker can be started by simply running `nisshi`, all options have defaults. Nisshi pickup any existing environment,
loading any found in `.env`. An [example.env](example.env) is provided as part of the distribution
and can be copied into `.env` for local modification. Sample schemas can be found in [etc/schema](etc/schema), used in the examples.

If an Apache Avro, Protobuf or JSON schema has been assigned to a topic, the
broker will reject any messages that are invalid. Schema backed topics are written
as Apache Parquet when the broker runs with the `parquet` subcommand, for example
`nisshi broker --schema-registry file://./etc/schema parquet --location file://./lake`
(`--location` can also come from `DATA_LAKE`). A `file://` location must be an existing directory.
The `iceberg` and `delta` subcommands write Apache Iceberg and Delta Lake tables;
run `nisshi broker iceberg --help` or `nisshi broker delta --help` for their options.

## topic

Nisshi settings for a topic are listed in [topic configuration](docs/topic-configuration.md).

The `nisshi topic` command has the following subcommands:

```shell
nisshi topic --help
Create, list or delete topics managed by the broker

Usage: nisshi topic [OPTIONS] <COMMAND>

Commands:
  create  Create a topic
  delete  Delete an existing topic
  list    List existing topics
  help    Print this message or the help of the given subcommand(s)

Options:
      --schema-registry <SCHEMA_REGISTRY>
          Schema registry examples are: file://./etc/schema or s3://nisshi/, containing: topic.json, topic.proto or topic.avsc [env: SCHEMA_REGISTRY=]
  -h, --help
          Print help
```

To create a topic use:

```shell
nisshi topic create taxi
```

The command prints the topic and its number of partitions:

```shell
Created topic taxi with 3 partitions.
```

`nisshi topic create` and `nisshi cat produce` exit with status 0 on success,
and with a non-zero status on failure. A script that only checks the exit
status can pass `--silent`, so that the command prints no confirmation line.
Errors still print.

## cat

The `nisshi cat` command, has the following subcommands:

```shell
nisshi cat --help
Easily consume or produce Avro, JSON or Protobuf messages to a topic

Usage: nisshi cat <COMMAND>

Commands:
  produce  Produce Avro/JSON/Protobuf messages to a topic
  consume  Consume Avro/JSON/Protobuf messages from a topic
  help     Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

The `produce` subcommand reads JSON formatted messages encoding them into
Apache Avro, Protobuf or JSON depending on the schema used by the topic.

For example, the `taxi` topic is backed by [taxi.proto](etc/schema/taxi.proto).
Using [trips.json](etc/data/trips.json) containing a JSON array of objects,
`nisshi cat produce` encodes each message into protobuf into the broker:

```
nisshi cat produce taxi etc/data/trips.json
```

The command prints the number of messages and the offsets that the broker
gave them:

```shell
Produced 4 messages to taxi partition 0, offsets 0 to 3.
```

Using [duckdb](https://duckdb.org) we can read the
[Apache Parquet](https://parquet.apache.org) files
created by the broker:

```shell
duckdb :memory: "SELECT * FROM 'data/taxi/*/*.parquet'"
```

Results in the following output:

```shell
|-----------+---------+---------------+-------------+---------------|
| vendor_id | trip_id | trip_distance | fare_amount | store_and_fwd |
|     int64 |   int64 |         float |      double |         int32 |
|-----------+---------+---------------+-------------+---------------|
|         1 | 1000371 |           1.8 |       15.32 |             0 |
|         2 | 1000372 |           2.5 |       22.15 |             0 |
|         2 | 1000373 |           0.9 |        9.01 |             0 |
|         1 | 1000374 |           8.4 |       42.13 |             1 |
|-----------+---------+---------------+-------------+---------------|
```


## proxy

The `nisshi proxy` command runs a Kafka API proxy in front of an origin broker:

```shell
nisshi proxy --help
Apache Kafka®-compatible proxy

Usage: nisshi proxy [OPTIONS]

Options:
      --listener-url <LISTENER_URL>
          The proxy will listen on this address [env: LISTENER_URL=] [default: tcp://0.0.0.0:9092]
      --advertised-listener-url <ADVERTISED_LISTENER_URL>
          This location is advertised to clients in metadata [env: ADVERTISED_LISTENER_URL=] [default: tcp://127.0.0.1:9092]
      --origin-url <ORIGIN_URL>
          The proxy will forward traffic to this origin broker, which must not require authentication [env: ORIGIN_URL=] [default: tcp://127.0.0.1:9092]
      --otlp-endpoint-url <OTLP_ENDPOINT_URL>
          OTEL Exporter OTLP endpoint [env: OTEL_EXPORTER_OTLP_ENDPOINT=]
      --schema-registry <SCHEMA_REGISTRY>
          Schema registry examples are: file://./etc/schema or s3://nisshi/, containing: topic.json, topic.proto or topic.avsc [env: SCHEMA_REGISTRY=]
  -h, --help
          Print help
```

To proxy a broker that listens on `localhost:19092`:

```shell
nisshi proxy --origin-url tcp://localhost:19092
```

The proxy does not authenticate its clients, and it connects to the origin
over plain TCP without SASL. So the origin listener must not require
authentication, as with a Kafka `PLAINTEXT` listener. Every client that can
reach the proxy listener gets the proxy's access to the origin.

The proxy answers SaslHandshake and SaslAuthenticate itself with
`ILLEGAL_SASL_STATE`, as a Kafka `PLAINTEXT` listener does, and does not
forward them to the origin. So a client configured for SASL fails to connect.

## Securing and monitoring a broker

### TLS

The listener speaks TLS when both `--cert` (the certificate chain) and `--key`
(the private key) are supplied as PEM files, and clap rejects one without the other:

```shell
nisshi broker --cert broker.pem --key broker-key.pem
```

With TLS configured the listener is TLS only: plaintext clients are refused
during the handshake, so clients must be configured with `security.protocol=SSL`
(and a truststore containing the certificate, if it is self-signed). The
`--listener-url` keeps its `tcp://` scheme. TLS here provides encryption only;
client authentication is still SASL (see `--authentication`). The private key
is a PKCS#8, SEC1 or RSA PEM key. `--cert` and `--key` may point at the same
file when the certificate chain and key are kept in one PEM bundle. Any
problem loading the certificate or key, or a key that does not match the
certificate, fails startup rather than falling back to plaintext.

A passphrase protected key (the counterpart of Kafka's `ssl.key.password`) is
read with `--key-passphrase-file`, a file holding the passphrase; a trailing
newline is ignored:

```shell
nisshi broker --cert broker.pem --key broker-key.pem --key-passphrase-file broker-key.passphrase
```

Only PKCS#8 encryption (`ENCRYPTED PRIVATE KEY`, PBES2 with PBKDF2-HMAC-SHA2 or
scrypt and AES-CBC or Triple DES, which is what `openssl pkcs8 -topk8` emits) is
supported. Legacy OpenSSL PEM encryption (`Proc-Type: 4,ENCRYPTED`) and keys
derived with a SHA-1 PRF (older OpenSSL releases' default) are rejected at
startup, naming the algorithm; re-encrypt the key first, keeping the passphrase:

```shell
openssl pkcs8 -topk8 -in broker-key.pem -out broker-key-pkcs8.pem -v2 aes-256-cbc -v2prf hmacWithSHA256
```

An empty passphrase file means no passphrase, so a mounted secret that is
absent for unencrypted keys works unchanged. A passphrase given for a key that
is not encrypted is ignored with a warning.

Note for existing deployments: before 0.7 these flags were accepted but had no
effect, so a broker started with `--cert` and `--key` was serving plaintext.
After upgrading, that same command line serves TLS only, and `--cert` without
`--key` (or the reverse) is rejected. The bundled `nisshi cat`, `topic`, `perf`
and `proxy` subcommands connect in plaintext and cannot yet talk to a TLS
listener. `--key-passphrase-file` is unknown to earlier releases, so rolling
the binary back means removing that flag from the command line as well.

### Metrics

Metrics are exported over OTLP/HTTP when `--otlp-endpoint-url` (or
`OTEL_EXPORTER_OTLP_ENDPOINT`) is set. `v1/metrics` is appended to the URL's
path, so `https://collector:4318` and `https://gateway/otlp` export to
`https://collector:4318/v1/metrics` and `https://gateway/otlp/v1/metrics`.
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` is not read. Collector credentials go
in `OTEL_EXPORTER_OTLP_HEADERS`.

The exported resource honours the standard OpenTelemetry environment
variables. Every `key=value` pair in `OTEL_RESOURCE_ATTRIBUTES` is attached
as given; values are not percent-decoded, and entries without a `=` are
ignored with a warning. `service.name` is resolved as `OTEL_SERVICE_NAME`,
then `service.name` in `OTEL_RESOURCE_ATTRIBUTES`, then the default for the
subcommand (`nisshi-broker`, or `nisshi-proxy` / `nisshi-generator` for
`proxy` and `generator`). Unset, empty or whitespace-only values fall through
to the next source. The endpoint, `service.name` and attribute names are
logged at `info` on startup. Resource attributes are sent with every export,
so use an `https` endpoint when they carry anything sensitive.

```shell
OTEL_EXPORTER_OTLP_ENDPOINT=https://collector:4318/ \
OTEL_SERVICE_NAME=kafka-broker \
OTEL_RESOURCE_ATTRIBUTES=service.version=0.7.0,deployment.environment.name=staging \
nisshi broker
```

Note for existing deployments: before 0.7 `service.name` was always
`nisshi-broker` and these variables were ignored. If `OTEL_SERVICE_NAME` or
`OTEL_RESOURCE_ATTRIBUTES` is already set in the broker's environment, for
example injected pod-wide for another agent, its series move to that name on
upgrade and dashboards or alerts keyed on `nisshi-broker` go quiet. Unset the
inherited variable or set `OTEL_SERVICE_NAME=nisshi-broker`. Rolling back
reverts to `nisshi-broker` regardless of these variables. On shutdown the
broker now flushes pending metrics before exiting.

## Compatibility

Nisshi implements the Apache Kafka® wire protocol, generated from the Apache Kafka 3.9.1
message descriptors in [`nisshi-sans-io/message`](nisshi-sans-io/message). Clients on
Kafka 4.x negotiate down to those protocol versions. CI tests the broker with the librdkafka and
franz-go integration test suites and the Apache Kafka command line tools.
[Kafka clients tested](docs/compatibility.md#kafka-clients-tested) lists the versions and the
known client gaps. Please [raise an issue][nisshi-issues] if a client or Kafka version you rely on
behaves differently.

Transactions are not supported: a producer that calls `initTransactions()`, including Kafka Streams with `exactly_once_v2`, fails with `TransactionalIdAuthorizationException`. Idempotent producers work.

## Feedback

Please [raise an issue][nisshi-issues] if you encounter a problem.

## Contributing

Contributions are welcome. See [CONTRIBUTING][nisshi-contributing] for how to get started,
and [GOVERNANCE][nisshi-governance] for how decisions get made. Everyone participating is
expected to follow our [Code of Conduct][nisshi-code-of-conduct]. To report a security
vulnerability, see [SECURITY][nisshi-security].

## License

Nisshi is licensed under [Apache 2.0][apache-license].

Apache, Apache Kafka, Kafka and the other Apache project names in this document are
trademarks of The Apache Software Foundation.

[apache-license]: https://www.apache.org/licenses/LICENSE-2.0
[apache-zookeeper]: https://en.wikipedia.org/wiki/Apache_ZooKeeper
[aws-s3-conditional-requests]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html
[aws-s3-conditional-writes]: https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/
[cloudflare-r2]: https://developers.cloudflare.com/r2/
[continuous-archiving]: https://www.postgresql.org/docs/current/continuous-archiving.html
[crates-io-object-store]: https://crates.io/crates/object_store
[github-com-nisshi-io]: https://github.com/nisshi-io/nisshi
[json-schema-org]: https://json-schema.org/
[librdkafka]: https://github.com/confluentinc/librdkafka
[min-io]: https://min.io
[minio-create-access-key]: https://min.io/docs/minio/container/administration/console/security-and-access.html#id1
[minio-create-bucket]: https://min.io/docs/minio/container/administration/console/managing-objects.html#creating-buckets
[object-store-dynamo-conditional-put]: https://docs.rs/object_store/0.11.0/object_store/aws/struct.DynamoCommit.html
[protocol-buffers]: https://protobuf.dev
[raft-consensus]: https://raft.github.io
[rust-lang-org]: https://www.rust-lang.org
[nisshi-issues]: https://github.com/nisshi-io/nisshi/issues
[nisshi-latest-release]: https://github.com/nisshi-io/nisshi/releases/latest
[nisshi-contributing]: CONTRIBUTING.md
[nisshi-code-of-conduct]: CODE_OF_CONDUCT.md
[nisshi-security]: SECURITY.md
[nisshi-governance]: GOVERNANCE.md
[tigris-conditional-writes]: https://www.tigrisdata.com/blog/s3-conditional-writes/
