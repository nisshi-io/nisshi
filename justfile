set dotenv-load

default: fmt build test clippy

about:
    cargo about generate about.hbs > license.html

cargo-build +args:
    cargo build {{ args }}

clean-workspace:
    cargo clean --workspace

license:
    cargo about generate about.hbs > license.html

build-all profile="dev" features="delta,dynostore,iceberg,libsql,parquet,postgres,slatedb": (cargo-build "--profile" profile "--timings" "--no-default-features" "--features" features)

build profile="dev" features="delta,dynostore,iceberg,libsql,parquet,postgres,slatedb" bin="nisshi": (cargo-build "--profile" profile "--timings" "--bin" bin "--no-default-features" "--features" features)

build-storage: clean-workspace (build "dev" "libsql") (build "dev" "postgres") (build "dev" "slatedb")

build-examples: (cargo-build "--examples")

release: (cargo-build "--release" "--bin" "nisshi" "--no-default-features" "--features" "delta,dynostore,iceberg,libsql,parquet,postgres,slatedb")

release-sqlite: (cargo-build "--release" "--bin" "nisshi" "--no-default-features" "--features" "libsql")

test: test-workspace test-doc

test-workspace *args: (nextest "run" "--workspace" "--all-targets" "--all-features" "--no-fail-fast" "--exclude" "fuzz" args)

nextest *args:
    cargo nextest {{ args }}

test-doc:
    cargo test --workspace --doc --all-features

doc:
    cargo doc --all-features --open

cargo-fuzz +args:
    cargo +nightly fuzz {{ args }}

fuzz-request-decode: (cargo-fuzz "run" "fuzz_request_decode" "--" "-max_total_time=60")

fuzz-member-metadata: (cargo-fuzz "run" "fuzz_member_metadata" "--" "-max_total_time=60")

fuzz-generate-seed: (cargo-fuzz "run" "--package" "fuzz" "--bin" "generate_seeds")

check:
    cargo check --workspace --all-features --all-targets

clippy:
    cargo clippy --workspace --all-features --all-targets -- -D warnings

fmt:
    cargo fmt --all --check

miri:
    cargo +nightly miri test --no-fail-fast --all-features

docker-build:
    docker build --tag ghcr.io/nisshi-io/nisshi --no-cache --progress plain --debug .

docker-build-cross:
    docker build --tag ghcr.io/nisshi-io/nisshi --no-cache --progress plain --platform linux/amd64,linux/arm64 --debug .

minio-up: (docker-compose-up "minio")

minio-down: (docker-compose-down "minio")

minio-mc +args:
    docker compose exec minio /usr/bin/mc {{ args }}

minio-local-alias: (minio-mc "alias" "set" "local" "http://localhost:9000" "minioadmin" "minioadmin")

minio-nisshi-bucket: (minio-mc "mb" "local/nisshi")

minio-lake-bucket: (minio-mc "mb" "local/lake")

minio-ready-local: (minio-mc "ready" "local")

nisshi-up: (docker-compose-up "nisshi")

nisshi-down: (docker-compose-down "nisshi")

db-up: (docker-compose-up "db")

db-down: (docker-compose-down "db")

jaeger-up: (docker-compose-up "jaeger")

jaeger-down: (docker-compose-down "jaeger")

prometheus-up: (docker-compose-up "prometheus")

prometheus-down: (docker-compose-down "prometheus")

grafana-up: (docker-compose-up "grafana")

grafana-down: (docker-compose-down "grafana")

grafana-ui:
    open http://localhost:3000

lakehouse-catalog-up: (docker-compose-up "lakehouse-catalog")

lakehouse-catalog-down: (docker-compose-down "lakehouse-catalog")

lakehouse-accept-terms-of-use:
    curl http://localhost:8181/management/v1/bootstrap -H "Content-Type: application/json" --data '{"accept-terms-of-use": true}'

lakehouse-create-warehouse:
    curl http://localhost:8181/management/v1/warehouse -H "Content-Type: application/json" --data @etc/lakekeeper/create-default-warehouse.json

lakehouse-migrate:
    docker compose exec lakehouse-catalog /home/nonroot/iceberg-catalog migrate

docker-compose-up *args:
    docker compose --ansi never --progress plain up --no-color --quiet-pull --wait --detach {{ args }}

docker-compose-down *args:
    docker compose down --remove-orphans --volumes {{ args }}

ps:
    docker compose ps

docker-compose-logs *args:
    docker compose logs {{ args }}

psql:
    docker compose exec db psql $*

docker-run-postgres:
    docker run \
        --detach \
        --name postgres \
        --publish 5432:5432 \
        --env PGUSER=postgres \
        --env POSTGRES_PASSWORD=postgres \
        --volume ./etc/initdb.d/:/docker-entrypoint-initdb.d/ \
        postgres:16.4

docker-prune:
    docker system prune --force

docker-run:
    docker run --detach --name nisshi --publish 9092:9092 nisshi

docker-rm-f:
    docker rm --force nisshi

list-topics:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --command-config command.properties --list

list-topics-plain:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --command-config command-plain.properties --list

list-topics-scram-256:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --command-config command-scram-256.properties --list

list-topics-scram-512:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --command-config command-scram-512.properties --list

user-create user password profile mechanism="scram512":
    target/{{ replace(profile, "dev", "debug") }}/nisshi user create {{ user }} {{ password }} --mechanism {{ mechanism }}

add-alice-user profile="dev": (user-create "alice" "secret" profile "scram256") (user-create "alice" "secret" profile "scram512")

user-delete user profile mechanism="scram512":
    target/{{ replace(profile, "dev", "debug") }}/nisshi user delete {{ user }} --mechanism {{ mechanism }}

delete-alice-user profile="dev": (user-delete "alice" profile "scram256") (user-delete "alice" profile "scram512")

# add-alice-user:
#    kafka-configs --alter --add-config "SCRAM-SHA-256=[iterations=8192,password=secret],SCRAM-SHA-512=[iterations=8192,password=secret]" --entity-type users --entity-name alice --bootstrap-server localhost:9092

test-topic-describe:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --describe --topic test

test-topic-create:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --config cleanup.policy=compact --partitions=3 --replication-factor=1 --create --topic test

test-topic-create-1m-retention:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --config cleanup.policy=delete --config retention.ms=60000 --partitions=3 --replication-factor=1 --create --topic test

test-topic-alter:
    kafka-configs --bootstrap-server ${ADVERTISED_LISTENER} --alter --entity-type topics --entity-name test --add-config retention.ms=3600000,retention.bytes=524288000

test-topic-delete:
    kafka-topics --bootstrap-server ${ADVERTISED_LISTENER} --delete --topic test

test-topic-get-offsets-earliest:
    kafka-get-offsets --bootstrap-server ${ADVERTISED_LISTENER} --topic test --time earliest

test-topic-get-offsets-latest:
    kafka-get-offsets --bootstrap-server ${ADVERTISED_LISTENER} --topic test --time latest

test-topic-produce:
    echo "h1:pqr,h2:jkl,h3:uio	qwerty	poiuy\nh1:def,h2:lmn,h3:xyz	asdfgh	lkj\nh1:stu,h2:fgh,h3:ijk	zxcvbn	mnbvc" | kafka-console-producer --bootstrap-server ${ADVERTISED_LISTENER} --topic test --property parse.headers=true --property parse.key=true

test-topic-consume:
    kafka-console-consumer --bootstrap-server ${ADVERTISED_LISTENER} --consumer-property fetch.max.wait.ms=15000 --group test-consumer-group --topic test --from-beginning --property print.timestamp=true --property print.key=true --property print.offset=true --property print.partition=true --property print.headers=true --property print.value=true

test-consumer-group-describe:
    kafka-consumer-groups --bootstrap-server ${ADVERTISED_LISTENER} --group test-consumer-group --describe

consumer-group-list:
    kafka-consumer-groups --bootstrap-server ${ADVERTISED_LISTENER} --list

test-reset-offsets-to-earliest:
    kafka-consumer-groups --bootstrap-server ${ADVERTISED_LISTENER} --group test-consumer-group --topic test:0 --reset-offsets --to-earliest --execute

topic-create topic *args:
    target/debug/nisshi topic create {{ topic }} {{ args }}

topic-delete topic:
    target/debug/nisshi topic delete {{ topic }}

cat-produce topic file:
    target/debug/nisshi cat produce {{ topic }} {{ file }}

cat-consume topic:
    target/debug/nisshi cat consume {{ topic }} --max-wait-time-ms=5000

generator topic *args:
    target/debug/nisshi generator {{ args }} {{ topic }} 2>&1 >generator.log

duckdb-k-unnest-v-parquet topic:
    duckdb -init duckdb-init.sql :memory: "SELECT key,unnest(value) FROM '{{ replace(env("DATA_LAKE"), "file://./", "") }}/{{ topic }}/*/*.parquet'"

duckdb-parquet topic:
    duckdb -init duckdb-init.sql :memory: "SELECT * FROM '{{ replace(env("DATA_LAKE"), "file://./", "") }}/{{ topic }}/*/*.parquet'"

# create person topic with schema etc/schema/person.json
person-topic-create: (topic-create "person")

# delete person topic
person-topic-delete: (topic-delete "person")

# produce etc/data/persons.json with schema etc/schema/person.json
person-topic-populate: (cat-produce "person" "etc/data/persons.json")

# produce valid data, that is accepted by the broker
person-topic-produce-valid:
    echo '{"key": "345-67-6543", "value": {"firstName": "John", "lastName": "Doe", "age": 21}}' | target/debug/nisshi cat produce person

# produce invalid data, that is rejected by the broker
person-topic-produce-invalid:
    echo '{"key": "ABC-12-4242", "value": {"firstName": "John", "lastName": "Doe", "age": -1}}' | target/debug/nisshi cat produce person

# person parquet
person-duckdb-parquet: (duckdb-k-unnest-v-parquet "person")

person-topic-consume:
    kafka-console-consumer \
        --bootstrap-server ${ADVERTISED_LISTENER} \
        --timeout-ms=15000 \
        --group person-consumer-group \
        --topic person \
        --from-beginning \
        --formatter-property print.timestamp=true \
        --formatter-property print.key=true \
        --formatter-property print.offset=true \
        --formatter-property print.partition=true \
        --formatter-property print.headers=true \
        --formatter-property print.value=true

# create search topic with etc/schema/search.proto
search-topic-create: (topic-create "search")

# delete search topic
search-topic-delete: (topic-delete "search")

# produce data to search topic with etc/schema/search.proto
search-topic-produce:
    echo '{"value": {"query": "abc/def", "page_number": 6, "results_per_page": 13, "corpus": "CORPUS_WEB"}}' | target/debug/nisshi cat produce search

# search parquet
search-duckdb-parquet: (duckdb-parquet "search")

nisshi-server:
    target/debug/nisshi broker --schema-registry file://./etc/schema 2>&1 | tee broker.log

kafka39:
    docker run --d -p 9092:9092 apache/kafka:3.9.0

proxy-kafka39:
    docker run --rm -p 19092:9092 apache/kafka:3.9.0

proxy-kafka41:
    docker run --rm -p 19092:9092 apache/kafka:4.1.0

codespace-create:
    gh codespace create \
        --repo $(gh repo view --json nameWithOwner --jq .nameWithOwner) \
        --branch $(git branch --show-current) \
        --machine basicLinux32gb

codespace-delete:
    gh codespace ls \
        --repo $(gh repo view \
            --json nameWithOwner \
            --jq .nameWithOwner) \
        --json name \
        --jq '.[].name' | xargs --no-run-if-empty -n1 gh codespace delete --codespace

codespace-logs:
    gh codespace logs \
        --codespace $(gh codespace ls \
            --repo $(gh repo view \
                --json nameWithOwner \
                --jq .nameWithOwner) \
            --json name \
            --jq '.[].name')

codespace-ls:
    gh codespace list \
        --repo $(gh repo view \
            --json nameWithOwner \
            --jq .nameWithOwner)

codespace-ssh:
    gh codespace ssh \
        --codespace $(gh codespace ls \
            --repo $(gh repo view \
                --json nameWithOwner \
                --jq .nameWithOwner) \
            --json name \
            --jq '.[].name')

all: test miri

flamegraph *args:
    cargo flamegraph {{ args }}

benchmark-flamegraph: build docker-compose-down minio-up minio-ready-local minio-local-alias minio-nisshi-bucket prometheus-up grafana-up
    flamegraph -- target/debug/nisshi broker 2>&1  | tee broker.log

benchmark: build docker-compose-down minio-up minio-ready-local minio-local-alias minio-nisshi-bucket prometheus-up grafana-up
    target/debug/nisshi broker 2>&1  | tee broker.log

otel profile="dev" *args: build docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket prometheus-up grafana-up
    OTEL_METRIC_EXPORT_INTERVAL=5000 OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:9090/api/v1/otlp/" target/{{ replace(profile, "dev", "debug") }}/nisshi broker {{ args }}  | tee broker.log

otel-up: docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket prometheus-up grafana-up nisshi-up

nisshi-broker profile *args:
    target/{{ replace(profile, "dev", "debug") }}/nisshi broker {{ args }} 2>&1 >broker.log

flamegraph-nisshi-broker profile *args:
    #!/usr/bin/env zsh
    unset SCHEMA_REGISTRY
    export RUST_LOG=warn
    flamegraph --verbose -- ./target/{{ replace(profile, "dev", "debug") }}/nisshi broker {{ args }}

# run a debug broker with configuration from .env
broker *args: build docker-compose-down prometheus-up grafana-up db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket minio-lake-bucket lakehouse-catalog-up (nisshi-broker "debug" args)

# run a release broker with configuration from .env
broker-release *args: release docker-compose-down prometheus-up grafana-up db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket minio-lake-bucket lakehouse-catalog-up (nisshi-broker "release" args)

# run a proxy with configuration from .env
proxy *args:
    target/debug/nisshi proxy {{ args }} 2>&1 | tee proxy.log

# teardown compose, rebuild: minio, db, nisshi and lake buckets
server: (cargo-build "--bin" "nisshi") docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket minio-lake-bucket lakehouse-catalog-up
    target/debug/nisshi broker 2>&1  | tee broker.log

gdb: (cargo-build "--bin" "nisshi") docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket minio-lake-bucket
    rust-gdb --args target/debug/nisshi broker

lldb: (cargo-build "--bin" "nisshi") docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket minio-lake-bucket lakehouse-catalog-up
    rust-lldb target/debug/nisshi broker

ci: docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket minio-lake-bucket lakehouse-catalog-up lakehouse-accept-terms-of-use lakehouse-create-warehouse

# bring up postgres and minio (with the nisshi bucket), without the lakehouse catalog
compat-infra-up: docker-compose-down db-up minio-up minio-ready-local minio-local-alias minio-nisshi-bucket

# produce etc/data/observations.json with schema etc/schema/observation.avsc
observation-produce: (cat-produce "observation" "etc/data/observations.json")

# consume observation topic with schema etc/schema/observation.avsc
observation-consume: (cat-consume "observation")

# create observation topic with schema etc/schema/observation.avsc
observation-topic-create: (topic-create "observation")

# observation parquet
observation-duckdb-parquet: (duckdb-k-unnest-v-parquet "observation")

duckdb *sql:
    duckdb -init duckdb-init.sql -markdown :memory: {{ sql }}

# produce etc/data/trips.json with schema etc/schema/taxi.proto
taxi-topic-populate: (cat-produce "taxi" "etc/data/trips.json")

# consume taxi topic with schema etc/schema/taxi.proto
taxi-topic-consume: (cat-consume "taxi")

# create taxi topic with generated fields with schema etc/schema/taxi.proto
taxi-topic-create: (topic-create "taxi" "--partitions=1" "--config=tansu.lake.normalize=true" "--config=tansu.lake.partition=meta.day" "--config=tansu.lake.z_order=vendor_id" "--config=tansu.lake.sink=true" "--config=tansu.batch=true" "--config=tansu.batch.max_records=200" "--config=tansu.batch.timeout_ms=1000")

# create taxi topic with schema etc/schema/taxi.proto
taxi-topic-create-plain: (topic-create "taxi" "--partitions" "1" "--config" "tansu.lake.sink=true")

# create taxi topic with a flattened schema etc/schema/taxi.proto
taxi-topic-create-normalize: (topic-create "taxi" "--partitions" "1" "--config" "tansu.lake.sink=true" "--config" "tansu.lake.normalize=true" "--config" "tansu.lake.normalize.separator=_" "--config" "tansu.lake.z_order=value_vendor_id")

taxi-topic-generator: (generator "taxi" "--broker=tcp://localhost:9092" "--per-second=10" "--producers=16" "--batch-size=1" "--duration-seconds=60")

# delete taxi topic
taxi-topic-delete: (topic-delete "taxi")

# taxi parquet
taxi-duckdb-parquet: (duckdb-parquet "taxi")

# taxi duckdb delta lake
taxi-duckdb-delta: (duckdb "\"select * from delta_scan('s3://lake/nisshi.taxi');\"")

# create employee topic with etc/schema/employee.proto
employee-topic-create: (topic-create "employee")

# produce etc/data/persons.json with etc/schema/person.json
employee-produce: (cat-produce "employee" "etc/data/employees.json")

# employee duckdb delta lake
employee-duckdb-delta: (duckdb "\"select * from delta_scan('s3://lake/nisshi.employee');\"")

# create customer topic with schema etc/schema/customer.proto
customer-topic-create *args: (topic-create "customer" "--partitions=1" "--config=tansu.lake.normalize=true" "--config=tansu.lake.partition=meta.day" "--config=tansu.lake.sink=true" "--config=tansu.batch=true" "--config=tansu.batch.max_records=200" "--config=tansu.batch.timeout_ms=1000" args)

customer-topic-generator *args: (generator "customer" args)

customer-duckdb-delta: (duckdb "\"select * from delta_scan('s3://lake/nisshi.customer');\"")

broker-memory profile="profiling": (build profile "dynostore") (nisshi-broker profile "--storage-engine=memory://" "--advertised-listener-url=tcp://127.0.0.1:9092/" "--silent")

# run the librdkafka integration test suite against the given storage engine
compat-librdkafka storage="memory://" features="dynostore": clean-nisshi-db (build "dev" features)
    #!/usr/bin/env bash
    set -euo pipefail
    ./target/debug/nisshi broker --storage-engine={{ storage }} \
        --advertised-listener-url=tcp://127.0.0.1:9092 &
    broker=$!
    trap 'kill ${broker}' EXIT
    ./compat/librdkafka/run.sh

# run the franz-go integration test suite against the given storage engine
compat-franz-go storage="memory://" features="dynostore": clean-nisshi-db (build "dev" features)
    #!/usr/bin/env bash
    set -euo pipefail
    ./target/debug/nisshi broker --storage-engine={{ storage }} \
        --advertised-listener-url=tcp://127.0.0.1:9092 &
    broker=$!
    trap 'kill ${broker}' EXIT
    ./compat/franz-go/run.sh

compat-franz-go-test tests timeout="600s" count="1":
    #!/usr/bin/env bash
    cd target/compat/franz-go/pkg/kgo
    export KGO_TEST_RF=1
    export KGO_LOG_LEVEL=debug
    go test -count={{ count }} -timeout {{ timeout }} -v -run {{ tests }} .

broker-null profile="profiling": (build profile "default") (nisshi-broker profile "--storage-engine=null://")

clean-nisshi-db:
    rm -f nisshi.db* snapshot.db

clean-lake-dir:
    rm -rf lake/*

broker-sqlite-parquet profile="dev": clean-nisshi-db clean-lake-dir (build profile "libsql,parquet") (nisshi-broker profile "--storage-engine=sqlite://nisshi.db" "parquet" "--location=file://./lake")

broker-sqlite-delta profile="profiling": docker-compose-down minio-up minio-ready-local minio-local-alias minio-lake-bucket clean-nisshi-db (build profile "libsql,delta") (nisshi-broker profile "--storage-engine=sqlite://nisshi.db" "delta")

broker-sqlite profile="profiling": clean-nisshi-db (build profile "libsql") (nisshi-broker profile "--silent" "--storage-engine=sqlite://nisshi.db" "--advertised-listener-url=tcp://127.0.0.1:9092")

broker-sqlite-existing profile="profiling": (build profile "libsql") (nisshi-broker profile "--silent" "--storage-engine=sqlite://nisshi.db")

broker-sqlite-no-maintenance profile="profiling": clean-nisshi-db (build profile "libsql") (nisshi-broker profile "--silent" "--storage-engine='sqlite://nisshi.db'")

broker-sqlite-authentication profile="profiling": (build profile "libsql") (nisshi-broker profile "--authentication" "--storage-engine=sqlite://nisshi.db")

broker-sqlite-maintenance-1m profile="profiling": clean-nisshi-db (build profile "libsql") (nisshi-broker profile "--storage-engine=sqlite://nisshi.db?maintenance_interval=1m")

broker-sqlite-vacuum-into profile="profiling": clean-nisshi-db (build profile "libsql") (nisshi-broker profile "--storage-engine=sqlite://nisshi.db?vacuum_into=snapshot.db")

s3-up: docker-compose-down minio-up minio-ready-local minio-local-alias minio-nisshi-bucket

broker-s3 profile="profiling": (build profile "dynostore") s3-up (nisshi-broker profile "--storage-engine=s3://nisshi/" "--silent" "--advertised-listener-url=tcp://127.0.0.1:9092")

broker-postgres profile="profiling": (build profile "postgres") docker-compose-down db-up (nisshi-broker profile "--storage-engine=postgres://postgres:postgres@localhost" "--silent" "--advertised-listener-url=tcp://127.0.0.1:9092")

broker-postgres-existing profile="profiling": (build profile "postgres") (nisshi-broker profile "--silent" "--storage-engine=postgres://postgres:postgres@localhost")

broker-postgres-local profile="profiling": (build profile "postgres") (nisshi-broker profile "--silent" "--storage-engine=postgres://pmorgan@localhost/pmorgan")

broker-postgres-authentication profile="profiling": (build profile "postgres") (nisshi-broker profile "--authentication" "--storage-engine=postgres://postgres:postgres@localhost")

broker-postgres-maintenance-1m profile="profiling": (build profile "postgres") (nisshi-broker profile "--storage-engine=postgres://postgres:postgres@localhost?maintenance_interval=1m")

samply-null profile="profiling":
    cargo build --profile {{ profile }} --bin nisshi
    RUST_LOG=warn samply record ./target/{{ replace(profile, "dev", "debug") }}/nisshi --storage-engine=null://sink

flamegraph-null profile="profiling": (build profile "default") (flamegraph-nisshi-broker profile "--storage-engine=null://sink")

flamegraph-sqlite profile="profiling": (build profile "libsql") clean-nisshi-db (flamegraph-nisshi-broker profile "--storage-engine=sqlite://nisshi.db")

flamegraph-postgres profile="profiling": (build profile "postgres") docker-compose-down db-up (flamegraph-nisshi-broker profile "--storage-engine=postgres://postgres:postgres@localhost")

flamegraph-memory profile="profiling": (build profile "dynostore") (flamegraph-nisshi-broker profile "--storage-engine=memory://nisshi/")

flamegraph-s3 profile="profiling": (build profile "dynostore") docker-compose-down minio-up minio-ready-local minio-local-alias minio-nisshi-bucket (flamegraph-nisshi-broker profile "--storage-engine=s3://nisshi/")

samply-produce profile="profiling":
    cargo build --profile {{ profile }} --bin bench_produce_v11
    RUST_LOG=warn samply record ./target/{{ replace(profile, "dev", "debug") }}/bench_produce_v11

flamegraph-produce profile="profiling":
    cargo build --profile {{ profile }} --bin bench_produce_v11
    RUST_LOG=warn flamegraph -- ./target/{{ replace(profile, "dev", "debug") }}/bench_produce_v11

bench-hyperfine iterations="100000" profile="release": (build profile "libsql" "bench")
    hyperfine -N './target/{{ replace(profile, "dev", "debug") }}/bench --iterations {{ iterations }}'

bench-dhat mode="heap" profile="release": (build profile "libsql" "bench")
    valgrind --tool=dhat --mode={{ mode }} ./target/{{ replace(profile, "dev", "debug") }}/bench

bench-flamegraph profile="profiling": (build profile "libsql" "bench")
    RUST_LOG=warn flamegraph -- ./target/{{ replace(profile, "dev", "debug") }}/bench

bench-flamegraph-produce profile="profiling": (build profile "libsql" "bench")
    RUST_LOG=warn flamegraph -- ./target/{{ replace(profile, "dev", "debug") }}/bench --api-key=0

bench-flamegraph-fetch iterations="100000" profile="profiling": (build profile "libsql" "bench")
    RUST_LOG=warn flamegraph -- ./target/{{ replace(profile, "dev", "debug") }}/bench  --iterations {{ iterations }} --api-key=1

bench-perf profile="profiling": (build profile "libsql" "bench")
    RUST_LOG=warn perf record --call-graph dwarf ./target/{{ replace(profile, "dev", "debug") }}/bench 2>&1 >/dev/null

consumer-perf num_records="1000" topic="test":
    kafka-consumer-perf-test --topic {{ topic }} --num-records {{ num_records }} --bootstrap-server ${ADVERTISED_LISTENER}

soak-producer-perf seconds throughput="1000" record_size="1024":
    kafka-producer-perf-test --topic test --warmup-records {{ throughput }} --num-records $(({{ seconds }} * {{ throughput }})) --record-size {{ record_size }} --throughput {{ throughput }} --command-property bootstrap.servers=${ADVERTISED_LISTENER}

soak-producer-perf-500: (soak-producer-perf "600" "500" "1024")

soak-producer-perf-1000: (soak-producer-perf "3600" "1000" "1024")

producer-perf throughput="1000" record_size="1024" num_records="100000" topic="test":
    kafka-producer-perf-test --topic {{ topic }} --warmup-records {{ throughput }} --num-records $(({{ num_records }} + {{ throughput }})) --record-size {{ record_size }} --throughput {{ throughput }} --command-property bootstrap.servers=${ADVERTISED_LISTENER}

producer-perf-10: (producer-perf "10")

producer-perf-1000: (producer-perf "1000" "1024" "25000")

producer-perf-2000: (producer-perf "2000" "1024" "50000")

producer-perf-3000: (producer-perf "3000" "1024" "75000")

producer-perf-4000: (producer-perf "4000" "1024" "100000")

producer-perf-5000: (producer-perf "5000" "1024" "125000")

producer-perf-6000: (producer-perf "6000" "1024" "150000")

producer-perf-7000: (producer-perf "7000" "1024" "175000")

producer-perf-8000: (producer-perf "8000" "1024" "200000")

producer-perf-9000: (producer-perf "9000" "1024" "225000")

producer-perf-10000: (producer-perf "10000" "1024" "250000")

producer-perf-15000: (producer-perf "15000" "1024" "375000")

producer-perf-20000: (producer-perf "20000" "1024" "500000")

producer-perf-30000: (producer-perf "30000" "1024" "750000")

producer-perf-40000: (producer-perf "40000" "1024" "1000000")

producer-perf-45000: (producer-perf "45000" "1024" "1100000")

producer-perf-50000: (producer-perf "50000" "1024" "1250000")

producer-perf-60000: (producer-perf "60000" "1024" "1500000")

producer-perf-70000: (producer-perf "70000" "1024" "1750000")

producer-perf-80000: (producer-perf "80000" "1024" "2000000")

producer-perf-90000: (producer-perf "90000" "1024" "2250000")

producer-perf-100000: (producer-perf "100000" "1024" "2500000")

producer-perf-200000: (producer-perf "200000" "1024" "5000000")

producer-perf-300000: (producer-perf "300000" "1024" "7500000")

producer-perf-400000: (producer-perf "400000" "1024" "10000000")

producer-perf-500000: (producer-perf "500000" "1024" "12500000")

producer-perf-600000: (producer-perf "600000" "1024" "15000000")

producer-perf-1000000: (producer-perf "1000000" "1024" "25000000")

ps-nisshi-rss:
    ps -p $(pgrep nisshi) -o rss= | awk '{print $1/1024 " MB"}'

telemetry-topic-create: (topic-create "telemetry" "--config" "tansu.virtual=true")

telemetry-produce-valid profile="dev":
    echo '{"key": "SK06 YPM", "value": {"latitude":52.930412156530465,"longitude":-4.894550244518114,"altitude":158.06766871179406}}' | target/{{ replace(profile, "dev", "debug") }}/nisshi cat produce telemetry

telemetry-consume:
    kafka-console-consumer \
        --bootstrap-server ${ADVERTISED_LISTENER} \
        --timeout-ms=15000 \
        --group telemetry-consumer-group \
        --topic telemetry \
        --from-beginning \
        --formatter-property print.timestamp=true \
        --formatter-property print.key=true \
        --formatter-property print.offset=true \
        --formatter-property print.partition=true \
        --formatter-property print.headers=true \
        --formatter-property print.value=true

telemetry-vrm-consume vrm="SK06 YPM":
    kafka-console-consumer \
        --bootstrap-server ${ADVERTISED_LISTENER} \
        --timeout-ms=15000 \
        --group telemetry-sk06-consumer-group \
        --topic 'telemetry/"{{ vrm }}"' \
        --from-beginning \
        --formatter-property print.timestamp=true \
        --formatter-property print.key=true \
        --formatter-property print.offset=true \
        --formatter-property print.partition=true \
        --formatter-property print.headers=true \
        --formatter-property print.value=true

postgres-local:
    LC_ALL="en_US.UTF-8" /opt/homebrew/opt/postgresql@18/bin/postgres -D /opt/homebrew/var/postgresql@18

group-consumer-example topics="test":
    cargo run --package nisshi-client --example group_consumer -- --topics {{ topics }}

la profile="dev" *args:
    target/{{ replace(profile, "dev", "debug") }}/la {{ args }}
