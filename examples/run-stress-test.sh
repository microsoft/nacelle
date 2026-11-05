#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

BIND="127.0.0.1:7878"
SERVER_THREADS="$(nproc)"
DURATION="30"
CONNECTIONS="256"
PIPELINE="8"
PAYLOAD="256"
CONFIG_PATH="config.toml"

config_bool_value() {
    local config_path="$1"
    local key="$2"
    if [[ ! -f "$config_path" ]]; then
        return
    fi

    awk '
        BEGIN {
            key = ARGV[1]
            ARGV[1] = ""
        }
        {
            sub(/[[:space:]]*#.*/, "", $0)
            pattern = "^[[:space:]]*" key "[[:space:]]*="
            if ($0 ~ pattern) {
                value = $0
                sub(/^[^=]*=/, "", value)
                gsub(/[[:space:]]/, "", value)
                print tolower(value)
                exit
            }
        }
    ' "$key" "$config_path"
}

effective_tls_self_signed() {
    local value
    value="$(config_bool_value "config.toml" "tls_self_signed")"
    if [[ -z "$value" ]]; then
        value="false"
    fi

    if [[ "$CONFIG_PATH" != "config.toml" && "$CONFIG_PATH" != "./config.toml" ]]; then
        local override
        override="$(config_bool_value "$CONFIG_PATH" "tls_self_signed")"
        if [[ -n "$override" ]]; then
            value="$override"
        fi
    fi

    echo "$value"
}

usage() {
    echo "Usage: $0 [--config PATH] [--bind ADDR] [--server-threads N] [--connections N] [--pipeline N] [--duration-secs S] [--payload-bytes N]"
    echo "Uses ./config.toml by default; client TLS mode follows the effective tls_self_signed value."
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --bind)           BIND="$2";           shift 2 ;;
        --server-threads) SERVER_THREADS="$2"; shift 2 ;;
        --connections)    CONNECTIONS="$2";    shift 2 ;;
        --pipeline)       PIPELINE="$2";       shift 2 ;;
        --duration-secs)  DURATION="$2";       shift 2 ;;
        --payload-bytes)  PAYLOAD="$2";        shift 2 ;;
        --config)         CONFIG_PATH="$2";    shift 2 ;;
        --help|-h)        usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

PORT="${BIND##*:}"
command -v ss >/dev/null || { echo "ss is required" >&2; exit 1; }
check_bind_available() {
    if ss -H -ltn "sport = :$PORT" | grep . >/dev/null; then
        echo "Bind address is already in use: $BIND" >&2
        return 1
    fi
}
check_bind_available

TARGET_DIR="${CARGO_TARGET_DIR:-target}"
HOST_TARGET="$(rustc -vV | sed -n 's/^host: //p')"
if [[ -z "$HOST_TARGET" ]]; then
    echo "Unable to determine the Rust host target" >&2
    exit 1
fi
RELEASE_DIR="$TARGET_DIR/$HOST_TARGET/release"
cargo build --release --target "$HOST_TARGET" --target-dir "$TARGET_DIR" \
    --package nacelle-stress-server --package nacelle-stress-test
check_bind_available

SERVER_ARGS=(
    --bind "$BIND"
    --server-threads "$SERVER_THREADS"
)

if [[ "$CONFIG_PATH" != "config.toml" && "$CONFIG_PATH" != "./config.toml" ]]; then
    SERVER_ARGS+=(--config "$CONFIG_PATH")
fi

SERVER_PID=""
CLIENT_PID=""
cleanup() {
    local status=$?
    local pid
    trap - EXIT INT TERM
    for pid in "$CLIENT_PID" "$SERVER_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$RELEASE_DIR/nacelle-stress-server" "${SERVER_ARGS[@]}" &
SERVER_PID=$!

ready="false"
for _ in $(seq 1 100); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Stress server exited before listening on $BIND" >&2
        exit 1
    fi
    if ss -H -ltnp "sport = :$PORT" | grep -F "pid=$SERVER_PID," >/dev/null; then
        ready="true"
        break
    fi
    sleep 0.1
done
if [[ "$ready" != "true" ]]; then
    echo "Stress server did not listen on $BIND within 10 seconds" >&2
    exit 1
fi

echo "--- nacelle threads=$SERVER_THREADS connections=$CONNECTIONS pipeline=$PIPELINE ---"

CLIENT_ARGS=(
    --addr "$BIND"
    --connections "$CONNECTIONS"
    --pipeline "$PIPELINE"
    --duration-secs "$DURATION"
    --payload-bytes "$PAYLOAD"
)

if [[ "$(effective_tls_self_signed)" == "true" ]]; then
    CLIENT_ARGS+=(--tls-insecure)
fi

"$RELEASE_DIR/nacelle-stress-test" "${CLIENT_ARGS[@]}" &
CLIENT_PID=$!
wait "$CLIENT_PID"
CLIENT_PID=""
