#!/usr/bin/env bash
#
# Runs the services that the tests need (db, minio and lakehouse-catalog) in a
# compose project of this worktree's own, so that worktrees testing at once
# never share or remove each other's services:
#
#   etc/worktree-services.sh up      start or update the project, and write .env
#   etc/worktree-services.sh down    remove the project and its volumes
#   etc/worktree-services.sh prune   remove the project of each removed worktree
#
# The script does nothing in the main checkout or when CI is set: there the
# services run in the default project, on the fixed ports in compose.yaml,
# and `just ci` starts them.
#
# The project is named nisshi-wt-<worktree directory>, and its services
# publish their ports on loopback ports that Docker picks. `up` writes those
# ports to .env, as AWS_ENDPOINT, ICEBERG_CATALOG and NISSHI_TEST_DATABASE_URL,
# and writes COMPOSE_PROJECT_NAME and the NISSHI_*_PORTS variables there too, so
# that a later `docker compose` or `just` command in the worktree uses this
# project. When nextest runs `up` as a setup script, it also writes them to
# $NEXTEST_ENV, because a test process doesn't take a variable from .env that
# is already in its environment.
#
# NISSHI_WORKTREE_PROJECT names another project instead, for a test of this
# script that runs a second project beside the first.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

prefix=nisshi-wt-

is_linked_worktree() {
    [[ "$(git rev-parse --path-format=absolute --git-dir)" != "$(git rev-parse --path-format=absolute --git-common-dir)" ]]
}

if [[ -n "${CI:-}" ]] || ! is_linked_worktree; then
    exit 0
fi

# Compose accepts only lowercase letters, digits, dashes and underscores in a
# project name.
worktree=$(basename "$(git rev-parse --show-toplevel)" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_\n-' '-')
project=${NISSHI_WORKTREE_PROJECT:-${prefix}${worktree}}
compose=(docker compose --project-name "${project}" --ansi never)

# Docker picks each host port, and keeps it while the container runs. A
# container that is created again can get another port, so `up` writes .env
# again each time.
export NISSHI_DB_PORTS=127.0.0.1::5432
export NISSHI_MINIO_PORTS=127.0.0.1::9000
export NISSHI_MINIO_CONSOLE_PORTS=127.0.0.1::9001
export NISSHI_LAKEHOUSE_PORTS=127.0.0.1::8181

# Removes the project of each worktree whose directory is gone. A project
# keeps the path of its compose file, and the path is in the worktree. The
# function removes only projects named with this script's prefix, so it never
# touches the default project, the smoke suite's projects or another tool's.
prune() {
    local name config_file
    docker compose ls --all --format json |
        jq -r --arg prefix "${prefix}" '.[] | select(.Name | startswith($prefix)) | "\(.Name)\t\(.ConfigFiles | split(",")[0])"' |
        while IFS=$'\t' read -r name config_file; do
            if [[ ! -e "${config_file}" ]]; then
                echo "worktree-services: removing ${name}, whose worktree ${config_file%/*} is gone" >&2
                # This worktree's compose.yaml names the same services and
                # volumes, so down finds them by the project's name.
                docker compose --project-name "${name}" --ansi never down --volumes --remove-orphans >/dev/null 2>&1 ||
                    echo "worktree-services: could not remove ${name}" >&2
            fi
        done
}

# Replaces each KEY=... line of the file with the new value, and adds each key
# that the file doesn't have. dotenv takes the first line of a key, so every
# line of the key gets the new value.
write_env() {
    local file=$1
    shift
    local tmp
    tmp=$(mktemp "${file}.XXXXXX")
    awk '
        BEGIN {
            for (i = 1; i < ARGC; i++) {
                split(ARGV[i], kv, "=")
                key = kv[1]
                value[key] = substr(ARGV[i], length(key) + 2)
                order[i] = key
                ARGV[i] = ""
            }
            count = ARGC - 1
        }
        {
            line = $0
            sub(/^export[ \t]+/, "", line)
            split(line, kv, "=")
            if (index(line, "=") > 0 && (kv[1] in value)) {
                print kv[1] "=" value[kv[1]]
                seen[kv[1]] = 1
                next
            }
            print
        }
        END {
            for (i = 1; i <= count; i++)
                if (!(order[i] in seen)) print order[i] "=" value[order[i]]
        }' "$@" <"${file}" >"${tmp}"
    mv "${tmp}" "${file}"
}

# Takes a lock on this worktree, so two runs of `up` at once, such as two
# nextest runs, don't start the project or write .env at the same time.
# macOS lacks flock(1), so the lock is a directory, and it holds the owner's
# process id, so a lock that a killed run left behind is taken over.
lock_dir=$(git rev-parse --path-format=absolute --git-dir)/worktree-services.lock

release_lock() {
    rm -rf "${lock_dir}"
}

take_lock() {
    local attempt owner
    for attempt in $(seq 600); do
        if mkdir "${lock_dir}" 2>/dev/null; then
            echo $$ >"${lock_dir}/pid"
            trap release_lock EXIT
            return
        fi
        owner=$(cat "${lock_dir}/pid" 2>/dev/null || true)
        if [[ -n "${owner}" ]] && ! kill -0 "${owner}" 2>/dev/null; then
            rm -rf "${lock_dir}"
            continue
        fi
        if [[ ${attempt} == 1 ]]; then
            echo "worktree-services: waiting for ${lock_dir}" >&2
        fi
        sleep 1
    done
    echo "worktree-services: ${lock_dir} still held after 10 minutes" >&2
    exit 1
}

up() {
    prune
    take_lock

    "${compose[@]}" up --detach --wait --quiet-pull db minio lakehouse-catalog >&2

    "${compose[@]}" exec -T minio /usr/bin/mc ready local >/dev/null
    "${compose[@]}" exec -T minio /usr/bin/mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null
    "${compose[@]}" exec -T minio /usr/bin/mc mb --ignore-existing local/nisshi local/lake >/dev/null

    local db minio catalog
    db=$("${compose[@]}" port db 5432)
    minio=$("${compose[@]}" port minio 9000)
    catalog=$("${compose[@]}" port lakehouse-catalog 8181)

    # The catalog accepts its terms of use once, and answers each later
    # bootstrap with an error, so the script checks whether that happened first.
    if ! curl --silent --fail "http://${catalog}/management/v1/info" | jq -e '.bootstrapped' >/dev/null; then
        curl --silent --show-error --fail "http://${catalog}/management/v1/bootstrap" \
            -H "Content-Type: application/json" --data '{"accept-terms-of-use": true}' >/dev/null
    fi
    # The catalog answers a second create of the warehouse with a 400 error
    # that it also gives for an invalid request, so the script lists the
    # warehouses first.
    local warehouse_file=etc/lakekeeper/create-default-warehouse.json
    if ! curl --silent --show-error --fail "http://${catalog}/management/v1/warehouse" |
        jq -e --arg name "$(jq -r '."warehouse-name"' "${warehouse_file}")" 'any(.warehouses[]; .name == $name)' >/dev/null; then
        curl --silent --show-error --fail "http://${catalog}/management/v1/warehouse" \
            -H "Content-Type: application/json" --data @"${warehouse_file}" >/dev/null
    fi

    local settings=(
        "COMPOSE_PROJECT_NAME=${project}"
        "NISSHI_DB_PORTS=${NISSHI_DB_PORTS}"
        "NISSHI_MINIO_PORTS=${NISSHI_MINIO_PORTS}"
        "NISSHI_MINIO_CONSOLE_PORTS=${NISSHI_MINIO_CONSOLE_PORTS}"
        "NISSHI_LAKEHOUSE_PORTS=${NISSHI_LAKEHOUSE_PORTS}"
        "AWS_ENDPOINT=http://${minio}"
        "ICEBERG_CATALOG=http://${catalog}/catalog"
        "NISSHI_TEST_DATABASE_URL=postgres://postgres:postgres@${db}"
    )

    if [[ ! -f .env ]]; then
        cp example.env .env
    fi
    write_env .env "${settings[@]}"

    if [[ -n "${NEXTEST_ENV:-}" ]]; then
        printf '%s\n' "${settings[@]}" >>"${NEXTEST_ENV}"
    fi

    echo "worktree-services: ${project} is up: postgres ${db}, minio ${minio}, catalog ${catalog}" >&2
}

down() {
    "${compose[@]}" down --volumes --remove-orphans
}

case "${1:-}" in
    up) up ;;
    down) down ;;
    prune) prune ;;
    *) echo "usage: etc/worktree-services.sh <up|down|prune>" >&2; exit 2 ;;
esac
