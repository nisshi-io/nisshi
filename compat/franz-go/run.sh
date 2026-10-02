#!/usr/bin/env bash
#
# Run the franz-go (pkg/kgo) integration test suite against a running
# tansu broker.
#
# The broker must be reachable at ${BOOTSTRAP_SERVERS} (default
# 127.0.0.1:9092):
#
#   tansu broker --storage-engine=memory:// \
#       --advertised-listener-url=tcp://127.0.0.1:9092
#
# Only tests listed in tests.allow are run; FINDINGS.md records the
# compatibility gaps behind the excluded tests. Grow the allowlist as
# broker compatibility improves.
#
# KGO_TEST_RF=1 is forced: tansu is a single-node broker and the suite
# creates topics with replication factor 3 by default.

set -euo pipefail

FRANZ_GO_VERSION="${FRANZ_GO_VERSION:-v1.21.3}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-127.0.0.1:9092}"
# when set (the compat-franz-go justfile recipe does), the PID of the broker
# started for this run: the readiness wait fails immediately if it exits
BROKER_PID="${BROKER_PID:-}"
COMPAT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${WORK_DIR:-${COMPAT_DIR}/../../target/compat}"
SRC_DIR="${WORK_DIR}/franz-go"

# when set, a "test,PASS|FAIL" line is appended for every top-level test,
# so CI can build a report card across storage engines
RESULTS_FILE="${RESULTS_FILE:-}"
if [[ -n "${RESULTS_FILE}" ]]; then
    mkdir -p "$(dirname "${RESULTS_FILE}")"
    : > "${RESULTS_FILE}"
    # resolve to an absolute path: the test suite runs from a different
    # working directory than this script started in
    RESULTS_FILE="$(cd "$(dirname "${RESULTS_FILE}")" && pwd)/$(basename "${RESULTS_FILE}")"
fi

mkdir -p "${WORK_DIR}"

if [[ ! -d "${SRC_DIR}" ]]; then
    git clone --depth 1 --branch "${FRANZ_GO_VERSION}" \
        https://github.com/twmb/franz-go.git "${SRC_DIR}"
fi

# wait for the broker to listen, failing fast if it never does: falling
# through to the suite against a dead broker buries the real cause under
# dozens of "connection refused" test failures. The report card row keeps
# this storage engine's column visible in CI when that happens.
broker_not_ready() {
    echo "$@" >&2
    if [[ -n "${RESULTS_FILE}" ]]; then
        printf 'broker-startup,FAIL\n' >> "${RESULTS_FILE}"
    fi
    exit 1
}

ready=""
started="${SECONDS}"
while (( SECONDS - started < 30 )); do
    if [[ -n "${BROKER_PID}" ]] && ! kill -0 "${BROKER_PID}" 2> /dev/null; then
        broker_not_ready "broker process ${BROKER_PID} exited during startup," \
                         "see broker output above"
    fi
    if (exec 3<> "/dev/tcp/${BOOTSTRAP_SERVERS%:*}/${BOOTSTRAP_SERVERS##*:}") \
           2> /dev/null; then
        ready=1
        break
    fi
    sleep 0.1
done

if [[ -z "${ready}" ]]; then
    broker_not_ready "broker did not start listening on ${BOOTSTRAP_SERVERS}" \
                     "after $((SECONDS - started))s"
fi

tests=$(grep -Ev '^[[:space:]]*(#|$)' "${COMPAT_DIR}/tests.allow" |
            awk '{print $1}' | paste -s -d '|' -)

count=$(grep -cEv '^[[:space:]]*(#|$)' "${COMPAT_DIR}/tests.allow")
echo "running ${count// /} franz-go test(s) against ${BOOTSTRAP_SERVERS}"

cd "${SRC_DIR}/pkg/kgo"

output="${WORK_DIR}/franz-go.log"

set +e
KGO_SEEDS="${BOOTSTRAP_SERVERS}" \
KGO_TEST_RF=1 \
KGO_TEST_RECORDS="${KGO_TEST_RECORDS:-10000}" \
KGO_LOG_LEVEL="${KGO_LOG_LEVEL:-none}" \
    go test -count=1 -timeout 600s -v -run "^(${tests})\$" . | tee "${output}"
status="${PIPESTATUS[0]}"
set -e

if [[ -n "${RESULTS_FILE}" ]]; then
    grep -E '^--- (PASS|FAIL): ' "${output}" |
        sed -E 's/^--- (PASS|FAIL): ([^ ]+).*/\2,\1/' >> "${RESULTS_FILE}"
fi

exit "${status}"
