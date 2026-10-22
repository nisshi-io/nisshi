# franz-go compatibility suite

Runs the [franz-go](https://github.com/twmb/franz-go) `pkg/kgo`
integration test suite against nisshi, as a client-compatibility check.
franz-go is the most widely used pure-Go Kafka client and exercises
newer protocol surface than librdkafka (flexible versions throughout,
KIP-345 static membership, KIP-848 next-gen groups, KIP-932 share
groups), so it complements the librdkafka suite.

## Running

```shell
just compat-franz-go
```

This builds nisshi (`dynostore` feature), starts a `memory://` broker
advertising `tcp://127.0.0.1:9092`, then runs `run.sh`, which:

1. clones franz-go at the tag pinned in `run.sh` (`FRANZ_GO_VERSION`)
   into `target/compat/franz-go` (a Go toolchain is the only build
   requirement),
2. runs the tests listed in `tests.allow` in a single `go test`
   invocation against the broker (`BOOTSTRAP_SERVERS` to override),
   with `KGO_TEST_RF=1` (nisshi is single-node) and
   `KGO_TEST_RECORDS=10000` to keep the ETL-style tests quick.

To run against an already-running broker, call `./run.sh` directly.

To investigate a single test:

```shell
just compat-franz-go-test '^TestAddRemovePartitions$' 120s
```

For an older version, pass its `WORK_DIR` (see below) as the fourth
argument, e.g. `just compat-franz-go-test '^TestIssue325$' 120s 1
target/compat-v1.16.1`.

## The allowlist

`tests.allow` lists tests known to pass. When broker compatibility
improves, re-sweep the excluded tests and grow the allowlist.
`FINDINGS.md` records the gaps behind the tests that are excluded.

CI also tests franz-go v1.16.1, the newest release on or before Kafka
3.7.0, because applications upgrade franz-go slowly. Its test names and
passing tests differ, so it has its own allowlist,
`tests-v1.16.1.allow`, which `run.sh` picks from `FRANZ_GO_VERSION`. To
run it locally:

```shell
FRANZ_GO_VERSION=v1.16.1 WORK_DIR=target/compat-v1.16.1 just compat-franz-go
```

`run.sh` stops if the clone in `WORK_DIR` is at another version, so give
each version its own `WORK_DIR`. It also fails a test that the allowlist
names but the version does not run, because `go test` passes when `-run`
matches nothing.

Some allowlisted tests self-skip against nisshi: the SCRAM tests
(no `KGO_TEST_SCRAM` configured) and the share-group / KIP-848 tests
(nisshi does not advertise ShareFetch v2 or ConsumerGroupHeartbeat v1).
They are kept in the allowlist so they light up automatically when the
broker grows support. Others never contact the broker at all: they are
unit tests of the client, such as address parsing, compression and the
group balancers.
