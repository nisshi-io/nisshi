#!/usr/bin/env bash
#
# Run the librdkafka integration test suite against a running tansu broker.
#
# The broker must be reachable at ${BOOTSTRAP_SERVERS} (default
# 127.0.0.1:9092) and must advertise an IPv4 address: librdkafka resolves
# "localhost" to ::1 first on some hosts and tansu only listens on IPv4.
#
#   tansu broker --storage-engine=memory:// \
#       --advertised-listener-url=tcp://127.0.0.1:9092
#
# Only tests listed in tests.allow are run: tansu does not implement
# automatic topic creation on MetadataRequest (auto.create.topics.enable),
# which most of the suite depends on, so the allowlist holds the tests that
# create their topics explicitly through the Admin API and are known to
# pass. Grow it as broker compatibility improves.

set -euo pipefail

LIBRDKAFKA_VERSION="${LIBRDKAFKA_VERSION:-v2.14.2}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-127.0.0.1:9092}"
COMPAT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${WORK_DIR:-${COMPAT_DIR}/../../target/compat}"
SRC_DIR="${WORK_DIR}/librdkafka"
JOBS="$(getconf _NPROCESSORS_ONLN)"

# when set, a "test,PASS|FAIL" line is appended for every test, so CI can
# build a report card across storage engines
RESULTS_FILE="${RESULTS_FILE:-}"
if [[ -n "${RESULTS_FILE}" ]]; then
    mkdir -p "$(dirname "${RESULTS_FILE}")"
    : > "${RESULTS_FILE}"
fi

mkdir -p "${WORK_DIR}"

if [[ ! -d "${SRC_DIR}" ]]; then
    git clone --depth 1 --branch "${LIBRDKAFKA_VERSION}" \
        https://github.com/confluentinc/librdkafka.git "${SRC_DIR}"
fi

if [[ ! -x "${SRC_DIR}/tests/test-runner" ]]; then
    (cd "${SRC_DIR}" &&
         ./configure --disable-curl --disable-sasl &&
         make -j "${JOBS}" libs &&
         make -j "${JOBS}" -C tests build)
fi

printf 'bootstrap.servers=%s\n' "${BOOTSTRAP_SERVERS}" \
       > "${SRC_DIR}/tests/test.conf"

for _ in $(seq 1 100); do
    if (exec 3<> "/dev/tcp/${BOOTSTRAP_SERVERS%:*}/${BOOTSTRAP_SERVERS##*:}") \
           2> /dev/null; then
        break
    fi
    sleep 0.1
done

export DYLD_LIBRARY_PATH="${SRC_DIR}/src:${SRC_DIR}/src-cpp"
export LD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}"

tests=$(grep -Ev '^[[:space:]]*(#|$)' "${COMPAT_DIR}/tests.allow" |
            awk '{print $1}')

# timeout(1) is coreutils: present on Linux, optional on macOS
timeout=""
if command -v timeout > /dev/null; then
    timeout="timeout 300"
fi

count=0
failed=""

for test in ${tests}; do
    count=$((count + 1))
    echo "=== ${test} ==="
    status="PASS"
    if ! (cd "${SRC_DIR}/tests" &&
              TESTS="${test}" ${timeout} ./test-runner -p1 -Q -E); then
        status="FAIL"
        failed="${failed} ${test}"
    fi
    if [[ -n "${RESULTS_FILE}" ]]; then
        printf '%s,%s\n' "${test}" "${status}" >> "${RESULTS_FILE}"
    fi
done

echo
if [[ -n "${failed}" ]]; then
    echo "ran ${count} test(s), FAILED:${failed}"
    exit 1
fi
echo "ran ${count} test(s), all passed"
