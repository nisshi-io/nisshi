#!/usr/bin/env bash
#
# Runs the smoke suite (nisshi-smoke-test) against one storage engine:
#
#   nisshi-smoke-test/run.sh <postgres|sqlite|memory|s3> [nextest arguments...]
#
# The script passes extra arguments to `cargo nextest run`, such as a filter
# that selects some of the tests: -E 'test(/^groups::group_rebalance::/)'.
#
# It starts what the engine needs under its own compose project, then the
# shared broker (NISSHI_SMOKE_IMAGE if set, otherwise NISSHI_SMOKE_BIN or a
# binary built with only the engine's feature) and the Kafka CLI tools
# (KAFKA_IMAGE), runs the tests, and removes everything it started. It
# writes the results to target/smoke/results-<leg>.csv, where the leg is
# NISSHI_SMOKE_LEG or the engine. The exit status is non-zero if a test
# failed, or if the shared broker exited, panicked or didn't exit
# with 0 on SIGTERM.
#
# Everything the run writes goes in target/smoke:
#
#   results-<leg>.csv    one name,PASS|FAIL|SKIP row per test
#   nextest-<leg>.log    what nextest printed, with each failed test's output
#   broker-<leg>.log     the shared broker's log
#   brokers-<leg>/       the logs of each broker that a failed test launched
#   services-<leg>.log   the logs of the compose services, such as postgres
#   <container>.log      the log of a broker container left by a test that
#                        hung or was killed
#
# The Kafka CLI tools and each broker container use the host network. On
# macOS, the suite needs Docker Desktop with host networking turned on, or,
# under colima, NISSHI_SMOKE_DOCKER_HOST_IP=192.168.5.2, the address at which
# colima's containers reach this machine. That setting covers a broker run as
# a process here, the default, and not NISSHI_SMOKE_IMAGE. It installs
# iptables in an alpine container with apk, so it needs access to an Alpine
# package mirror.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

engine=${1:-}
case "${engine}" in
    # The postgres leg's storage URL names the port that Docker picks for the
    # service, so the script sets it once the service has started.
    postgres) feature=postgres storage='' services=db ;;
    sqlite) feature=libsql storage=sqlite://nisshi.db services='' ;;
    memory) feature=dynostore storage=memory://nisshi/ services='' ;;
    s3) feature=dynostore storage=s3://nisshi/ services=minio ;;
    *) echo "usage: nisshi-smoke-test/run.sh <postgres|sqlite|memory|s3> [nextest arguments...]" >&2; exit 2 ;;
esac
shift

target_dir=$(cargo metadata --format-version 1 --no-deps | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
results=${target_dir}/smoke
leg=${NISSHI_SMOKE_LEG:-${engine}}
mkdir -p "${results}"
# Cleanup writes the results file from the nextest log, so the files of an
# earlier run on this leg must not remain.
rm -f "${results}/results-${leg}.csv" "${results}/nextest-${leg}.log" \
    "${results}/services-${leg}.log"

# Labels every container and volume this run creates, so cleanup removes
# this run's and leaves another run's alone.
export NISSHI_SMOKE_RUN=${engine}-$$

# The run's own compose project, so a run never touches the services and
# volumes of the default project or of another run. compose.yaml interpolates
# these images for services this script doesn't start.
export NISSHI_IMAGE=${NISSHI_IMAGE:-unused} LAKEKEEPER_IMAGE=${LAKEKEEPER_IMAGE:-unused}
project_prefix=nisshi-smoke-$(id -u)-
project=${project_prefix}${NISSHI_SMOKE_RUN}
compose=(docker compose --project-name "${project}" --ansi never)

# Set to true when this run starts the compose services, so cleanup only stops services it started.
started=false

# What the run is doing. Cleanup writes it to the results file as a failed
# row when the run stops before nextest starts.
step="build the broker and the tests"

# Writes one `name,PASS|FAIL|SKIP` row per test, the format the compat
# suites' report reads. nextest prints a status line per test; a test with a
# FAIL line stays failed whatever follows it. smoke-broker adds a shared_broker row for the broker and a suite row
# for the nextest run as a whole. A run that stopped before it could write
# them gets FAIL for both, so the file never shows a failed leg as green. A
# run that stopped before nextest started also gets a FAIL row named for its step.
# Only cleanup calls write_results, and ShellCheck doesn't see the EXIT trap's call to cleanup
# (koalaman/shellcheck#2542).
# shellcheck disable=SC2329
write_results() {
    local log=${results}/nextest-${leg}.log
    local failed_step=
    if [[ ! -f "${log}" ]]; then
        log=/dev/null
        failed_step=${step}
    fi
    awk -v failed_step="${failed_step}" '
        $1 ~ /^(PASS|FAIL|FLAKY|SKIP|TIMEOUT|ABORT|LEAK|LEAK-FAIL|SIG[A-Z]+)$/ && $2 ~ /^\[/ {
            outcome = ($1 == "PASS" || $1 == "FLAKY") ? "PASS" : ($1 == "SKIP" ? "SKIP" : "FAIL")
            # A test that failed once stays failed, even if a later line for the same name passes.
            if (result[$NF] != "FAIL") result[$NF] = outcome
            if (outcome == "PASS") passed++
            tests++
        }
        $1 == "smoke-broker:" && $2 == "result" { split($3, row, ","); result[row[1]] = row[2] }
        END {
            if (failed_step != "") result["setup: " failed_step] = "FAIL"
            # nextest writes the status lines for people, and a nextest
            # release can change them. The leg fails when no line matches,
            # instead of showing green with no tests.
            else if (tests == 0) result["results: no test status line in the nextest log"] = "FAIL"
            # A leg whose tests were all skipped checked nothing.
            else if (passed == 0) result["results: no test passed"] = "FAIL"
            if (!("shared_broker" in result)) result["shared_broker"] = "FAIL"
            if (!("suite" in result)) result["suite"] = "FAIL"
            for (name in result) print name "," result[name]
        }' \
        "${log}" | sort >"${results}/results-${leg}.csv"
}

# Cleanup runs every step even when one fails, so a failed removal can't
# leave the compose services running. It runs in a subshell, so its
# `set +e` doesn't turn off errexit for the rest of the script.
#
# The harness removes each broker container that it stops, so a broker
# container that is still here belongs to a run that hung or was cancelled.
# Cleanup saves its log before it removes it.
#
# Cleanup writes the results file, so a run that fails or is cancelled at
# any step still reports the leg.
#
# The EXIT trap calls cleanup. ShellCheck doesn't see that call when the script runs `exit`
# (koalaman/shellcheck#2542).
# shellcheck disable=SC2329
cleanup() (
    set +e
    write_results
    stop_leftover_brokers
    docker ps --all --filter "label=nisshi-smoke=${NISSHI_SMOKE_RUN}" --filter name=nisshi-smoke-broker --format '{{.Names}}' |
        while read -r name; do
            docker logs "${name}" >"${results}/${name}.log" 2>&1
        done
    # --volumes also removes each container's anonymous volumes, which the next line misses because
    # they have no label. A test's own PostgreSQL server keeps its data in one.
    docker ps --all --quiet --filter "label=nisshi-smoke=${NISSHI_SMOKE_RUN}" | xargs -r docker rm --force --volumes >/dev/null
    docker volume ls --quiet --filter "label=nisshi-smoke=${NISSHI_SMOKE_RUN}" | xargs -r docker volume rm --force >/dev/null
    if [[ "${started}" == true ]]; then
        "${compose[@]}" logs --no-color --timestamps >"${results}/services-${leg}.log" 2>&1
        "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1
    fi
)
trap cleanup EXIT

# nextest kills a test that runs too long with SIGKILL, so the harness can't stop the broker
# processes the test launched. Each process broker keeps its process id in a file in its directory
# until the harness has stopped it, so a file still there names a broker that is still running.
# Only cleanup calls stop_leftover_brokers (koalaman/shellcheck#2542).
# shellcheck disable=SC2329
stop_leftover_brokers() {
    local pid_file pid
    for pid_file in "${NISSHI_SMOKE_WORK_DIR:-${results}/brokers-${leg}}"/*/broker.pid; do
        [[ -f "${pid_file}" ]] || continue
        pid=$(<"${pid_file}")
        # The process id can belong to another process by now, so only a broker is stopped.
        case "$(ps -p "${pid}" -o command= 2>/dev/null)" in
            *" broker --cluster-id="*)
                echo "a test was killed while its broker ran as process ${pid} (${pid_file%/*}): stopping it" >&2
                kill -KILL "${pid}"
                ;;
        esac
    done
}

# Build before starting anything, so a slow build doesn't count against
# a broker or a service that is already running.
#
# Each leg runs its own copy of the broker, so that two legs running at once
# never run each other's build. Every leg builds target/debug/nisshi with only
# its own engine's feature. If the postgres leg started target/debug/nisshi just
# after the memory leg rebuilt it, the broker would lack PostgreSQL support and
# refuse to start. The lock stops another leg from rebuilding the file between
# this leg's build and its copy. macOS lacks flock(1), so there the legs must
# run one at a time.
if [[ -z "${NISSHI_SMOKE_IMAGE:-}" && -z "${NISSHI_SMOKE_BIN:-}" ]]; then
    (
        if command -v flock >/dev/null; then
            flock 9
        fi
        cargo build -p nisshi --bin nisshi --no-default-features --features "${feature}"
        cp "${target_dir}/debug/nisshi" "${results}/nisshi-${leg}"
    ) 9>"${results}/build.lock"
    export NISSHI_SMOKE_BIN=${results}/nisshi-${leg}
fi
# Building the tests also builds smoke-broker.
cargo nextest run -p nisshi-smoke-test --features "${engine}" --no-run

# The image of a PostgreSQL server that a test starts for itself. It is the
# image of the db service.
export NISSHI_SMOKE_POSTGRES_IMAGE=${POSTGRES_IMAGE:-postgres:17}

# The directory of the files that reserve ports for brokers and PostgreSQL
# servers. Every run of the same user shares it. Each user has their own,
# because the first user to create a shared one would own it, and the others
# couldn't create their lock files in it.
NISSHI_SMOKE_PORT_LOCKS=${TMPDIR:-/tmp}/nisshi-smoke-$(id -u)/ports
export NISSHI_SMOKE_PORT_LOCKS

# The harness gives `docker run` a minute to start a broker container, which isn't enough to pull
# the image as well, so the image is pulled here.
step="pull the broker image"
if [[ -n "${NISSHI_SMOKE_IMAGE:-}" ]] && ! docker image inspect "${NISSHI_SMOKE_IMAGE}" >/dev/null 2>&1; then
    docker pull --quiet "${NISSHI_SMOKE_IMAGE}"
fi

# The services publish their ports on loopback ports that Docker picks, so that
# a run can start while the services of `just ci` or of another run hold the
# fixed ports. compose.yaml publishes the fixed ports when these are unset.
export NISSHI_DB_PORTS=127.0.0.1::5432
export NISSHI_MINIO_PORTS=127.0.0.1::9000 NISSHI_MINIO_CONSOLE_PORTS=127.0.0.1::9001

# A run that was killed skips its cleanup and leaves its compose project
# behind. The project's name ends with the process id of that run's script.
#
# The script removes only the projects of its own user. Docker shows the
# projects of every user, but `ps` can't see the processes of another user
# when /proc hides them, so a live run of another user would look killed.
step="remove the services of killed runs"
for stale in $(docker compose ls --all --quiet --filter "name=${project_prefix}"); do
    if [[ "${stale}" == "${project_prefix}"* ]] && ! ps -p "${stale##*-}" -o command= 2>/dev/null | grep -q 'run.sh'; then
        docker compose --project-name "${stale}" down --volumes --remove-orphans >/dev/null 2>&1
    fi
done

step="start the services"
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
        storage=postgres://postgres:postgres@$("${compose[@]}" port db 5432)
        ;;
    s3)
        "${compose[@]}" exec minio /usr/bin/mc ready local
        "${compose[@]}" exec minio /usr/bin/mc alias set local http://localhost:9000 minioadmin minioadmin
        "${compose[@]}" exec minio /usr/bin/mc mb local/nisshi
        export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
        AWS_ENDPOINT=http://$("${compose[@]}" port minio 9000)
        export AWS_ENDPOINT AWS_ALLOW_HTTP=true AWS_DEFAULT_REGION=auto
        ;;
esac

step="start the Kafka tools"
export KAFKA_IMAGE=${KAFKA_IMAGE:-apache/kafka:3.9.2}
# Each tool call starts a JVM that runs for seconds, so these options make the
# JVM start faster and use less CPU: the JIT compiles only its fast first tier,
# and one GC thread collects. They don't change what the Kafka client sends.
jvm_options="-XX:TieredStopAtLevel=1 -XX:+UseSerialGC -Djava.awt.headless=true"
# Under a Docker VM whose host network is the VM's own, not this machine's
# (colima: NISSHI_SMOKE_DOCKER_HOST_IP=192.168.5.2), the tools get a network of
# their own, where each connection to 127.0.0.1 goes to that address, which
# reaches this machine's loopback.
if [[ -n "${NISSHI_SMOKE_DOCKER_HOST_IP:-}" ]]; then
    kafka_network=(--sysctl net.ipv4.conf.all.route_localnet=1)
else
    kafka_network=(--network=host)
fi
NISSHI_SMOKE_KAFKA=$(docker run --detach --rm --label="nisshi-smoke=${NISSHI_SMOKE_RUN}" "${kafka_network[@]}" \
    --env "KAFKA_JVM_PERFORMANCE_OPTS=${jvm_options}" --entrypoint=sleep "${KAFKA_IMAGE}" infinity)
if [[ -n "${NISSHI_SMOKE_DOCKER_HOST_IP:-}" ]]; then
    docker run --rm --label="nisshi-smoke=${NISSHI_SMOKE_RUN}" --network="container:${NISSHI_SMOKE_KAFKA}" \
        --cap-add=NET_ADMIN alpine:3.20 sh -ec "apk add --quiet iptables
        iptables -t nat -A OUTPUT -d 127.0.0.1/32 -p tcp -j DNAT --to-destination ${NISSHI_SMOKE_DOCKER_HOST_IP}
        iptables -t nat -A POSTROUTING -s 127.0.0.1/32 -p tcp -j MASQUERADE"
fi
export NISSHI_SMOKE_KAFKA
export NISSHI_SMOKE_STORAGE=${storage}
export NISSHI_SMOKE_LOG=${results}/broker-${leg}.log
# The harness keeps the files of each broker that a test launches here, and
# deletes them if the test passes. The script empties the directory first, so
# the directory holds only this run's broker files.
export NISSHI_SMOKE_WORK_DIR=${results}/brokers-${leg}
rm -rf "${NISSHI_SMOKE_WORK_DIR}"
mkdir -p "${NISSHI_SMOKE_WORK_DIR}"

# CI skips the tests marked #[ignore] because an open bug makes them fail;
# locally every test runs.
run_ignored=all
if [[ "${CI:-}" == true ]]; then
    run_ignored=default
fi

profile=smoke
if [[ "${engine}" == s3 ]]; then
    profile=smoke-s3
fi

set +e
"${target_dir}/debug/smoke-broker" -- \
    cargo nextest run -p nisshi-smoke-test --features "${engine}" --profile "${profile}" --run-ignored "${run_ignored}" \
        --no-tests=fail \
        --color never --status-level all --final-status-level none "$@" \
    2>&1 | tee "${results}/nextest-${leg}.log"
status=${PIPESTATUS[0]}
set -e

exit "${status}"
