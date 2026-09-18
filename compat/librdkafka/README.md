# librdkafka compatibility suite

Runs the [librdkafka](https://github.com/confluentinc/librdkafka)
integration test suite against nisshi, as a client-compatibility check.
librdkafka underpins kcat, confluent-kafka-python, confluent-kafka-go and
confluent-kafka-dotnet, so passing its suite covers a large share of the
non-Java Kafka client ecosystem.

## Running

```shell
just compat-librdkafka
```

This builds nisshi (`dynostore` feature), starts a `memory://` broker
advertising `tcp://127.0.0.1:9092`, then runs `run.sh`, which:

1. clones librdkafka at the tag pinned in `run.sh` (`LIBRDKAFKA_VERSION`)
   into `target/compat/librdkafka` and builds its `test-runner` (a C
   toolchain is the only build requirement),
2. points `tests/test.conf` at the broker (`BOOTSTRAP_SERVERS` to
   override),
3. runs each test listed in `tests.allow` in quick mode, and fails if any
   test fails.

To run against a different storage engine, pass it along with the
matching feature, e.g.:

```shell
just compat-librdkafka 'sqlite://nisshi.db' libsql
just compat-librdkafka 'postgres://postgres:postgres@localhost' postgres
just compat-librdkafka 's3://nisshi/' dynostore
```

To run against an already-running broker, call `./run.sh` directly. The
broker must advertise an IPv4 address (librdkafka may resolve `localhost`
to `::1`, and nisshi listens on IPv4 only). Set `RESULTS_FILE` to also
record a `test,PASS|FAIL` line per test, e.g. for building a report card
across storage engines (see the `compat-librdkafka` job in
`.github/workflows/ci.yml`).

To investigate a single test:

```shell
cd target/compat/librdkafka/tests
DYLD_LIBRARY_PATH=../src:../src-cpp TESTS=0019 TEST_DEBUG=broker,protocol \
    ./test-runner -p1 -Q -E
```

(`LD_LIBRARY_PATH` on Linux.)

## The allowlist

`tests.allow` lists tests known to pass. When broker compatibility
improves, re-sweep the excluded tests and grow the allowlist.
`FINDINGS.md` records the gaps behind the tests that are still excluded,
and the ones already fixed (automatic topic creation on MetadataRequest,
which most of the suite assumes, was implemented 2026-06-11).
