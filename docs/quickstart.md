# Get Started with Nisshi

Start a broker, create a topic, then send and read a message. This page uses a prebuilt release, so you need no Rust toolchain or Docker. Nisshi is open source, under the Apache License 2.0.

## Step 1: Download a Release

Select the release file for your platform from the [releases page](https://github.com/nisshi-io/nisshi/releases/latest).

| Platform | `uname -sm` prints | File |
|---|---|---|
| macOS, Apple silicon | `Darwin arm64` | `nisshi-aarch64-apple-darwin.tar.gz` |
| Linux, x86-64 | `Linux x86_64` | `nisshi-x86_64-unknown-linux-musl.tar.gz` |
| Linux, Arm64 | `Linux aarch64` | `nisshi-aarch64-unknown-linux-musl.tar.gz` |

macOS blocks a file that you download with a browser and displays "Not Opened". Downloading with `curl` avoids the block. To unblock a file that a browser downloaded, extract the archive, and then run `xattr -d com.apple.quarantine bin/nisshi`.

For other operating systems, you can build Nisshi from source by following the steps in [Build from Source](#build-from-source).

To download a release and extract it into a new directory, run the following commands in a terminal:

```shell
mkdir nisshi && cd nisshi
FILE=nisshi-aarch64-apple-darwin.tar.gz   # change this line for your platform
curl -fLO https://github.com/nisshi-io/nisshi/releases/latest/download/$FILE
tar -xzf $FILE
```

The archive extracts into the current directory, which is why the commands above start in a new one. The broker is `bin/nisshi`.

To verify the download, compare the output of `shasum -a 256 $FILE` on macOS or `sha256sum $FILE` on Linux with the SHA-256 digest that the releases page lists for the file.

## Step 2: Start the Broker

By default, other computers on your network can reach the broker on port 9092, and the broker does not require clients to authenticate. To start the broker so that only your own computer can connect, run the following command:

```shell
./bin/nisshi --listener-url tcp://127.0.0.1:9092
```

The broker prints its address and stays in the foreground. The following example shows the output. Press Ctrl-C to stop the broker.

```text
nisshi broker 0.7.0
listening on: tcp://127.0.0.1:9092 (advertised: tcp://127.0.0.1:9092)
storage: memory://nisshi/ ["dynostore", "libsql", "postgres", "slatedb", "turso"]
ready in 12ms
```

Your version number, list of storage engines, and startup time can differ.

By default, the broker keeps topics and messages in memory, and loses them when it stops. To keep them, you can start the broker with a SQLite file by running the following command:

```shell
./bin/nisshi --listener-url tcp://127.0.0.1:9092 --storage-engine sqlite://nisshi.db
```

To start again with no data, stop the broker and delete `nisshi.db`, `nisshi.db-shm`, and `nisshi.db-wal`.

If another program already uses port 9092, the broker stops with the error "Address already in use". To use a different port, set both the `--listener-url` and `--advertised-listener-url` options. Then add `--broker` with the same address to each command in Step 3. For example, to use port 9093, run the following command:

```shell
./bin/nisshi --listener-url tcp://127.0.0.1:9093 --advertised-listener-url tcp://127.0.0.1:9093
```

The release includes several storage engines. The `./bin/nisshi --help` command lists the available storage engines and includes the options for TLS (`--cert` and `--key`) and authentication (`--authentication`).

## Step 3: Send and Read a Message

To create a topic, send a message, and read it, perform these steps in a second terminal, in the same directory:

```shell
./bin/nisshi topic create hello
echo '{"value": "hello world"}' | ./bin/nisshi cat produce hello
./bin/nisshi cat consume hello
```

The `nisshi cat` command sends and reads JSON messages from the terminal. The `cat produce` subcommand reads one JSON object per line. Each object has a `key`, a `value`, or both. When the broker accepts the message, the subcommand prints one line, for example `Produced 1 message to hello partition 0, offset 0.` (`topic create` prints a line too, such as `Created topic hello with 3 partitions.`). Releases before 0.8.0 print nothing on success for these two commands; a command that succeeds exits with status 0. The `cat consume` subcommand reads the topic from the start of partition 0, prints the messages it gets, and exits. If the topic is empty, it waits up to 5 seconds first:

```text
[{"key":null,"value":"hello world"}]
```

## Step 4: Connect Your Own Client

The broker implements the Apache Kafka protocol. To connect your own application, point any Kafka client at the bootstrap server `127.0.0.1:9092`.

A client connects to the address that the broker advertises. To connect from another machine or a container, start the broker without the `--listener-url` option, which allows other computers to connect. Then set `--advertised-listener-url tcp://<address>:9092`, where `<address>` is the one that the client can reach. Anyone who can reach that address can use the broker without authentication.

## Next Steps

You now have a running broker that serves Kafka clients. To continue:

- Connect your applications — Point your Kafka clients at the broker to start sending and receiving messages. For a client example, see [Sarama (Go)](https://docs.nisshi.io/docs/golang-sarama). For an example that uses Kafka Mirror Maker, see [Mirror Maker](https://docs.nisshi.io/docs/example-mirror-maker).
- Explore the command-line options — Run `./bin/nisshi --help`, `./bin/nisshi topic --help`, and `./bin/nisshi cat --help`.
- Learn how Nisshi works — Every broker is stateless and keeps its state in the storage engine (for example, PostgreSQL or S3). For more information, see [Stateless Brokers](https://docs.nisshi.io/docs/stateless-brokers).

Nisshi also has capabilities beyond this quickstart:

- Validate messages with a schema — The broker checks each message against an Avro, JSON Schema, or Protocol Buffers schema and rejects messages that do not conform. For more information, see [Schema Registry](https://docs.nisshi.io/docs/schema-registry).
- Write topic data to a data lake — Nisshi can write a topic that has a schema as Apache Iceberg tables, Delta Lake tables, or Apache Parquet files. For more information, see [Data Lake](https://docs.nisshi.io/docs/data-lake).
- Choose where Nisshi stores data — The storage options include memory, SQLite, PostgreSQL, and S3. For more information, see [Storage Engine](https://docs.nisshi.io/docs/storage-engine).
- Secure the broker — The broker supports TLS, and it supports authentication with Simple Authentication and Security Layer (SASL). For more information about the options, see [TLS](https://github.com/nisshi-io/nisshi#tls) in the README.
- Run a proxy — The `nisshi proxy` command runs a Kafka API proxy in front of another broker. For more information, see [Proxy CLI](https://docs.nisshi.io/docs/cli-proxy).

## Build from Source

If there is no release for your operating system or architecture, for example Windows or an Intel-based Mac (`Darwin x86_64`), you can build Nisshi from source. To build it, perform these steps:

1. Install Rust with [rustup](https://rustup.rs).
2. Restart your shell so that `cargo` is on your `PATH`.
3. Run the following command:

   ```shell
   cargo install nisshi
   ```

The build installs the `nisshi` command on your `PATH`. In the other steps, run `nisshi` instead of `./bin/nisshi`.
