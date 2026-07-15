#!/usr/bin/env bash
set -euo pipefail

TOOL="perf"
CONFIG="examples/nacelle-stress-server/configs/tcp.toml"
BIND="127.0.0.1:17878"
SERVER_THREADS="8"
FEATURE_SET="minimal"
HANDLER_MODE="shared"
DISABLE_TIMEOUTS="false"
DISABLE_HANDLER_TIMEOUT="false"
DISABLE_TCP_TIMEOUTS="false"
RESPONSE_WRITE_MODE="immediate"
TLS_INSECURE="false"
CONNECTIONS="256"
PIPELINE="8"
WARMUP_SECS="5"
DURATION_SECS="10"
PAYLOAD_BYTES="256"
RUNS="3"
SERVER_CPUS=""
CLIENT_CPUS=""
OUTPUT_DIRECTORY=""
SKIP_BUILD="false"

usage() {
    cat <<'EOF'
Usage: scripts/profile-linux.sh [options]

Options:
  --tool baseline|perf|heaptrack
  --config PATH
  --bind ADDR
  --server-threads N
    --feature-set minimal|default
    --handler-mode shared|serial
    --disable-timeouts
    --disable-handler-timeout
    --disable-tcp-timeouts
    --response-write-mode immediate|coalesce-buffered
    --tls-insecure
  --connections N
  --pipeline N
  --warmup-secs N
  --duration-secs N
  --payload-bytes N
  --runs N
  --server-cpus LIST
  --client-cpus LIST
  --output-directory PATH
  --skip-build
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tool) TOOL="$2"; shift 2 ;;
        --config) CONFIG="$2"; shift 2 ;;
        --bind) BIND="$2"; shift 2 ;;
        --server-threads) SERVER_THREADS="$2"; shift 2 ;;
        --feature-set) FEATURE_SET="$2"; shift 2 ;;
        --handler-mode) HANDLER_MODE="$2"; shift 2 ;;
        --disable-timeouts) DISABLE_TIMEOUTS="true"; shift ;;
        --disable-handler-timeout) DISABLE_HANDLER_TIMEOUT="true"; shift ;;
        --disable-tcp-timeouts) DISABLE_TCP_TIMEOUTS="true"; shift ;;
        --response-write-mode) RESPONSE_WRITE_MODE="$2"; shift 2 ;;
        --tls-insecure) TLS_INSECURE="true"; shift ;;
        --connections) CONNECTIONS="$2"; shift 2 ;;
        --pipeline) PIPELINE="$2"; shift 2 ;;
        --warmup-secs) WARMUP_SECS="$2"; shift 2 ;;
        --duration-secs) DURATION_SECS="$2"; shift 2 ;;
        --payload-bytes) PAYLOAD_BYTES="$2"; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        --server-cpus) SERVER_CPUS="$2"; shift 2 ;;
        --client-cpus) CLIENT_CPUS="$2"; shift 2 ;;
        --output-directory) OUTPUT_DIRECTORY="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD="true"; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

EFFECTIVE_DISABLE_HANDLER_TIMEOUT="$DISABLE_HANDLER_TIMEOUT"
EFFECTIVE_DISABLE_TCP_TIMEOUTS="$DISABLE_TCP_TIMEOUTS"
if [[ "$DISABLE_TIMEOUTS" == "true" ]]; then
    EFFECTIVE_DISABLE_HANDLER_TIMEOUT="true"
    EFFECTIVE_DISABLE_TCP_TIMEOUTS="true"
fi

if [[ "$(uname -s)" != "Linux" ]]; then
    echo "profile-linux.sh requires Linux" >&2
    exit 1
fi

case "$TOOL" in
    baseline) ;;
    perf) command -v perf >/dev/null || { echo "perf is required" >&2; exit 1; } ;;
    heaptrack)
        command -v heaptrack >/dev/null || { echo "heaptrack is required" >&2; exit 1; }
        command -v heaptrack_print >/dev/null || { echo "heaptrack_print is required" >&2; exit 1; }
        ;;
    *) echo "--tool must be baseline, perf, or heaptrack" >&2; exit 2 ;;
esac

case "$HANDLER_MODE" in
    shared|serial) ;;
    *) echo "--handler-mode must be shared or serial" >&2; exit 2 ;;
esac

case "$FEATURE_SET" in
    minimal|default) ;;
    *) echo "--feature-set must be minimal or default" >&2; exit 2 ;;
esac

case "$RESPONSE_WRITE_MODE" in
    immediate|coalesce-buffered) ;;
    *) echo "--response-write-mode must be immediate or coalesce-buffered" >&2; exit 2 ;;
esac

if [[ "$TLS_INSECURE" == "true" && "$FEATURE_SET" != "default" ]]; then
    echo "--tls-insecure requires --feature-set default so the client includes Rustls" >&2
    exit 2
fi
if [[ "$TOOL" == "heaptrack" && "$FEATURE_SET" == "default" ]]; then
    echo "Heaptrack cannot observe the default mimalloc allocator; use --feature-set minimal" >&2
    exit 2
fi

if [[ -n "$SERVER_CPUS" || -n "$CLIENT_CPUS" ]]; then
    command -v taskset >/dev/null || { echo "taskset is required for CPU pinning" >&2; exit 1; }
fi
command -v ss >/dev/null || { echo "ss is required" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -f "$CONFIG" ]]; then
    echo "Config not found: $CONFIG" >&2
    exit 1
fi

if [[ -z "$OUTPUT_DIRECTORY" ]]; then
    OUTPUT_DIRECTORY="target/linux-profiles/$(date -u +%Y%m%dT%H%M%SZ)-$TOOL"
fi
if [[ -e "$OUTPUT_DIRECTORY" ]]; then
    echo "Output directory already exists: $OUTPUT_DIRECTORY" >&2
    exit 1
fi
mkdir -p "$OUTPUT_DIRECTORY"

if [[ "$SKIP_BUILD" != "true" ]]; then
    PROFILE_RUSTFLAGS="${RUSTFLAGS:-}"
    if [[ -n "$PROFILE_RUSTFLAGS" ]]; then
        PROFILE_RUSTFLAGS+=" "
    fi
    PROFILE_RUSTFLAGS+="-C force-frame-pointers=yes"
    BUILD_FEATURE_ARGS=()
    if [[ "$FEATURE_SET" == "minimal" ]]; then
        BUILD_FEATURE_ARGS+=(--no-default-features)
    fi
    RUSTFLAGS="$PROFILE_RUSTFLAGS" cargo build --profile profiling \
        -p nacelle-stress-server "${BUILD_FEATURE_ARGS[@]}"
    RUSTFLAGS="$PROFILE_RUSTFLAGS" cargo build --profile profiling \
        -p nacelle-stress-test "${BUILD_FEATURE_ARGS[@]}"
fi

SERVER_BINARY="target/profiling/nacelle-stress-server"
CLIENT_BINARY="target/profiling/nacelle-stress-test"
if [[ ! -x "$SERVER_BINARY" || ! -x "$CLIENT_BINARY" ]]; then
    echo "Profiling binaries are missing; rerun without --skip-build" >&2
    exit 1
fi

{
    echo "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit=$(git rev-parse HEAD)"
    echo "working_tree_changes=$(git status --short | wc -l)"
    echo "kernel=$(uname -srmo)"
    echo "rustc=$(rustc --version)"
    echo "cargo=$(cargo --version)"
    echo "tool=$TOOL"
    echo "config=$CONFIG"
    echo "bind=$BIND"
    echo "server_threads=$SERVER_THREADS"
    echo "feature_set=$FEATURE_SET"
    echo "handler_mode=$HANDLER_MODE"
    echo "disable_timeouts=$DISABLE_TIMEOUTS"
    echo "disable_handler_timeout=$EFFECTIVE_DISABLE_HANDLER_TIMEOUT"
    echo "disable_tcp_timeouts=$EFFECTIVE_DISABLE_TCP_TIMEOUTS"
    echo "response_write_mode=$RESPONSE_WRITE_MODE"
    echo "tls_insecure=$TLS_INSECURE"
    echo "connections=$CONNECTIONS"
    echo "pipeline=$PIPELINE"
    echo "warmup_secs=$WARMUP_SECS"
    echo "duration_secs=$DURATION_SECS"
    echo "payload_bytes=$PAYLOAD_BYTES"
    echo "runs=$RUNS"
    echo "server_cpus=${SERVER_CPUS:-unbound}"
    echo "client_cpus=${CLIENT_CPUS:-unbound}"
    lscpu
} > "$OUTPUT_DIRECTORY/metadata.txt"

PORT="${BIND##*:}"
if ss -H -ltn "sport = :$PORT" | grep -q .; then
    echo "Bind address is already in use: $BIND" >&2
    exit 1
fi

SERVER_PID=""
PROFILER_PID=""
SERVER_ARGS=(
    --config "$CONFIG"
    --bind "$BIND"
    --server-threads "$SERVER_THREADS"
    --handler-mode "$HANDLER_MODE"
    --response-write-mode "$RESPONSE_WRITE_MODE"
)
if [[ "$DISABLE_TIMEOUTS" == "true" ]]; then
    SERVER_ARGS+=(--disable-timeouts)
fi
if [[ "$DISABLE_HANDLER_TIMEOUT" == "true" ]]; then
    SERVER_ARGS+=(--disable-handler-timeout)
fi
if [[ "$DISABLE_TCP_TIMEOUTS" == "true" ]]; then
    SERVER_ARGS+=(--disable-tcp-timeouts)
fi

stop_process() {
    local pid="$1"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill -INT "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    stop_process "$SERVER_PID"
    stop_process "$PROFILER_PID"
    exit "$status"
}
trap cleanup EXIT INT TERM

wait_for_listener() {
    local launcher_pid="$1"
    local listener_pid=""
    for _ in $(seq 1 100); do
        listener_pid="$(ss -H -ltnp "sport = :$PORT" | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | sed -n '1p')"
        if [[ -n "$listener_pid" ]]; then
            echo "$listener_pid"
            return 0
        fi
        if ! kill -0 "$launcher_pid" 2>/dev/null; then
            return 1
        fi
        sleep 0.05
    done
    return 1
}

start_server() {
    local server_log="$1"
    local command=(
        "$SERVER_BINARY"
        "${SERVER_ARGS[@]}"
    )
    if [[ -n "$SERVER_CPUS" ]]; then
        command=(taskset -c "$SERVER_CPUS" "${command[@]}")
    fi
    "${command[@]}" > "$server_log" 2>&1 &
    local launcher_pid=$!
    SERVER_PID="$(wait_for_listener "$launcher_pid")" || {
        sed -n '1,160p' "$server_log" >&2
        return 1
    }
}

run_client() {
    local duration="$1"
    local output="$2"
    local command=(
        "$CLIENT_BINARY"
        --addr "$BIND"
        --connections "$CONNECTIONS"
        --pipeline "$PIPELINE"
        --duration-secs "$duration"
        --payload-bytes "$PAYLOAD_BYTES"
    )
    if [[ "$TLS_INSECURE" == "true" ]]; then
        command+=(--tls-insecure)
    fi
    if [[ -n "$CLIENT_CPUS" ]]; then
        command=(taskset -c "$CLIENT_CPUS" "${command[@]}")
    fi
    "${command[@]}" | tee "$output"
}

run_warmup() {
    if [[ "$WARMUP_SECS" -gt 0 ]]; then
        run_client "$WARMUP_SECS" "$OUTPUT_DIRECTORY/warmup.log" >/dev/null
    fi
}

case "$TOOL" in
    baseline)
        start_server "$OUTPUT_DIRECTORY/server.log"
        run_warmup
        for run in $(seq 1 "$RUNS"); do
            run_client "$DURATION_SECS" "$OUTPUT_DIRECTORY/run-$run.log"
        done
        stop_process "$SERVER_PID"
        SERVER_PID=""
        awk '/effective_rps/{print $2}' "$OUTPUT_DIRECTORY"/run-*.log | sort -n > "$OUTPUT_DIRECTORY/rps.txt"
        ;;
    perf)
        if [[ "$(< /proc/sys/kernel/perf_event_paranoid)" -gt 2 ]]; then
            echo "perf access is blocked; set kernel.perf_event_paranoid to 2 or lower" >&2
            exit 1
        fi
        start_server "$OUTPUT_DIRECTORY/server.log"
        run_warmup
        perf record -o "$OUTPUT_DIRECTORY/perf.data" -e cycles:u -F 999 \
            --call-graph fp -p "$SERVER_PID" > "$OUTPUT_DIRECTORY/perf-record.log" 2>&1 &
        PROFILER_PID=$!
        run_client "$DURATION_SECS" "$OUTPUT_DIRECTORY/client.log"
        stop_process "$PROFILER_PID"
        PROFILER_PID=""
        stop_process "$SERVER_PID"
        SERVER_PID=""
        perf report -i "$OUTPUT_DIRECTORY/perf.data" --stdio --no-children \
            -g none --sort comm,dso,symbol --percent-limit 0.25 \
            > "$OUTPUT_DIRECTORY/perf-report-self.txt"
        perf report -i "$OUTPUT_DIRECTORY/perf.data" --stdio --children \
            -g none --sort comm,dso,symbol --percent-limit 0.5 \
            > "$OUTPUT_DIRECTORY/perf-report-inclusive.txt"
        ;;
    heaptrack)
        heaptrack -o "$OUTPUT_DIRECTORY/heaptrack" "$SERVER_BINARY" "${SERVER_ARGS[@]}" \
            > "$OUTPUT_DIRECTORY/server.log" 2>&1 &
        PROFILER_PID=$!
        SERVER_PID="$(wait_for_listener "$PROFILER_PID")" || {
            sed -n '1,160p' "$OUTPUT_DIRECTORY/server.log" >&2
            exit 1
        }
        if [[ -n "$SERVER_CPUS" ]]; then
            taskset -apc "$SERVER_CPUS" "$SERVER_PID" > "$OUTPUT_DIRECTORY/affinity.log"
        fi
        run_warmup
        run_client "$DURATION_SECS" "$OUTPUT_DIRECTORY/client.log"
        stop_process "$SERVER_PID"
        SERVER_PID=""
        wait "$PROFILER_PID" 2>/dev/null || true
        PROFILER_PID=""
        heaptrack_print "$OUTPUT_DIRECTORY/heaptrack.zst" \
            > "$OUTPUT_DIRECTORY/heaptrack-report.txt"
        ;;
esac

trap - EXIT INT TERM
echo "Profile output: $OUTPUT_DIRECTORY"