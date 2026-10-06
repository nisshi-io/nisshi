#!/usr/bin/env bash
#
# Runs the smoke suite (nisshi-smoke-test) against one storage engine:
#
#   nisshi-smoke-test/run.sh <postgres|sqlite|memory|s3>
#
# It starts what the engine needs under its own compose project, then the
# shared broker (NISSHI_SMOKE_IMAGE when set, otherwise NISSHI_SMOKE_BIN or a
# binary built with only the engine's feature) and the Kafka CLI tools
# (KAFKA_IMAGE), runs the tests, and removes everything it started. It
# writes the results to target/smoke/results-<leg>.csv, where the leg is
# NISSHI_SMOKE_LEG or the engine. The exit status is non-zero if a test
# failed, or if the shared broker exited, restarted, panicked or didn't exit
# with 0 on SIGTERM.
#
# The Kafka CLI tools and each broker container use the host network. On
# macOS, that needs Docker Desktop with host networking turned on.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

engine=${1:-}
case "${engine}" in
    postgres) feature=postgres storage=postgres://postgres:postgres@localhost services=db ports=5432 ;;
    sqlite) feature=libsql storage=sqlite:// services= ports= ;;
    memory) feature=dynostore storage=memory://nisshi/ services= ports= ;;
    s3) feature=dynostore storage=s3://nisshi/ services=minio ports="9000 9001" ;;
    *) echo "usage: nisshi-smoke-test/run.sh <postgres|sqlite|memory|s3>" >&2; exit 2 ;;
esac

target_dir=$(cargo metadata --format-version 1 --no-deps | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
results=${target_dir}/smoke
leg=${NISSHI_SMOKE_LEG:-${engine}}
mkdir -p "${results}"

# Labels every container and volume this run creates, so cleanup removes
# this run's and leaves another run's alone.
export NISSHI_SMOKE_RUN=${engine}-$$

# The engine's own compose project, so a run never touches the volumes of
# the default one, nor the services of a run on another engine. compose.yaml
# interpolates these images for services this script doesn't start.
export NISSHI_IMAGE=${NISSHI_IMAGE:-unused} LAKEKEEPER_IMAGE=${LAKEKEEPER_IMAGE:-unused}
project=nisshi-smoke-${engine}
compose=(docker compose --project-name "${project}" --ansi never)

# Set to true when this run starts the compose services, so cleanup only stops services it started.
started=false

# Cleanup runs every step even when one fails, so a failed removal can't
# leave the compose services running. It runs in a subshell, so its
# `set +e` doesn't turn off errexit for the rest of the script.
#
# The harness removes each broker container that it stops, so a broker
# container that is still here belongs to a run that hung or was cancelled.
# Cleanup saves its log before it removes it.
cleanup() (
    set +e
    docker ps --all --filter "label=nisshi-smoke=${NISSHI_SMOKE_RUN}" --filter name=nisshi-smoke-broker --format '{{.Names}}' |
        while read -r name; do
            docker logs "${name}" >"${results}/${name}.log" 2>&1
        done
    docker ps --all --quiet --filter "label=nisshi-smoke=${NISSHI_SMOKE_RUN}" | xargs -r docker rm --force >/dev/null
    docker volume ls --quiet --filter "label=nisshi-smoke=${NISSHI_SMOKE_RUN}" | xargs -r docker volume rm --force >/dev/null
    if [[ "${started}" == true ]]; then
        "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1
    fi
)
trap cleanup EXIT

# Build before starting anything, so a slow build doesn't count against
# a broker or a service that is already running.
if [[ -z "${NISSHI_SMOKE_IMAGE:-}" && -z "${NISSHI_SMOKE_BIN:-}" ]]; then
    cargo build -p nisshi --bin nisshi --no-default-features --features "${feature}"
    export NISSHI_SMOKE_BIN=${target_dir}/debug/nisshi
fi
# Building the tests also builds smoke-broker.
cargo nextest run -p nisshi-smoke-test --features "${engine}" --no-run

# The services publish the same host ports as the default compose project,
# so if the services from `just ci`, or another run on this engine, are
# still running, `compose up` fails with "port is already allocated".
for port in ${ports}; do
    if (: </dev/tcp/127.0.0.1/"${port}") 2>/dev/null; then
        echo "port ${port} is already in use, probably by the services from \`just ci\` or by another \`just smoke ${engine}\`: stop them with \`docker compose stop\`, or end that run (if it was killed, \`docker compose --project-name ${project} down --volumes\`), then rerun" >&2
        exit 1
    fi
done

if [[ -n "${services}" ]]; then
    started=true
    "${compose[@]}" up --detach --wait --quiet-pull ${services}
fi

case "${engine}" in
    postgres)
        # The healthcheck's pg_isready uses the socket, so it passes
        # while the init scripts still run on a server that doesn't
        # listen on TCP yet. pg_isready doesn't retry a refused
        # connection, so this loop does.
        for attempt in $(seq 60); do
            if "${compose[@]}" exec -T db pg_isready --host=localhost --quiet; then
                break
            elif [[ ${attempt} == 60 ]]; then
                echo "postgres did not accept TCP connections within 60s" >&2
                exit 1
            fi
            sleep 1
        done
        ;;
    s3)
        "${compose[@]}" exec minio /usr/bin/mc ready local
        "${compose[@]}" exec minio /usr/bin/mc alias set local http://localhost:9000 minioadmin minioadmin
        "${compose[@]}" exec minio /usr/bin/mc mb local/nisshi
        export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
        export AWS_ENDPOINT=http://localhost:9000 AWS_ALLOW_HTTP=true AWS_DEFAULT_REGION=auto
        ;;
esac

export KAFKA_IMAGE=${KAFKA_IMAGE:-apache/kafka:3.9.2}
NISSHI_SMOKE_KAFKA=$(docker run --detach --rm --label="nisshi-smoke=${NISSHI_SMOKE_RUN}" --network=host --entrypoint=sleep "${KAFKA_IMAGE}" infinity)
export NISSHI_SMOKE_KAFKA
export NISSHI_SMOKE_STORAGE=${storage}
export NISSHI_SMOKE_LOG=${results}/broker-${leg}.log

# CI skips the tests marked #[ignore] because an open bug makes them fail;
# locally every test runs.
run_ignored=all
if [[ "${CI:-}" == true ]]; then
    run_ignored=default
fi

set +e
"${target_dir}/debug/smoke-broker" -- \
    cargo nextest run -p nisshi-smoke-test --features "${engine}" --profile smoke --run-ignored "${run_ignored}" \
        --color never --status-level all --final-status-level none \
    2>&1 | tee "${results}/nextest-${leg}.log"
status=${PIPESTATUS[0]}
set -e

# One `name,PASS|FAIL|SKIP` row per test, the format the compat suites'
# report reads. nextest prints a status line per test; the last one wins.
# smoke-broker adds a shared_broker row for the broker and a suite row for
# the nextest run as a whole. A run that stopped before it could write
# them gets FAIL for both, so the file never shows a failed leg as green.
awk '$1 ~ /^(PASS|FAIL|FLAKY|SKIP|TIMEOUT|ABORT|LEAK|LEAK-FAIL|SIG[A-Z]+)$/ && $2 ~ /^\[/ {
        outcome = ($1 == "PASS" || $1 == "FLAKY") ? "PASS" : ($1 == "SKIP" ? "SKIP" : "FAIL")
        result[$NF] = outcome
    }
    $1 == "smoke-broker:" && $2 == "result" { split($3, row, ","); result[row[1]] = row[2] }
    END {
        if (!("shared_broker" in result)) result["shared_broker"] = "FAIL"
        if (!("suite" in result)) result["suite"] = "FAIL"
        for (name in result) print name "," result[name]
    }' \
    "${results}/nextest-${leg}.log" | sort > "${results}/results-${leg}.csv"

exit "${status}"
