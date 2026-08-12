#!/usr/bin/env bash
set -euo pipefail

baseline="${1:-428a364b9e7fd815143acd80473f60adb2ca2e38}"
candidate="${2:-HEAD}"
output_directory="${3:-target/public-api}"

command -v cargo-public-api >/dev/null 2>&1 || {
    echo "cargo-public-api is required" >&2
    exit 1
}

if ! git diff --quiet || ! git diff --cached --quiet || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
    echo "public API comparison requires a clean working tree" >&2
    exit 1
fi

resolve_commit() {
    local revision="$1"
    local description="$2"
    git rev-parse --verify "${revision}^{commit}" 2>/dev/null || {
        echo "public API ${description} revision is unavailable: ${revision}" >&2
        exit 1
    }
}

baseline_commit="$(resolve_commit "$baseline" "baseline")"
candidate_commit="$(resolve_commit "$candidate" "candidate")"
starting_commit="$(git rev-parse HEAD)"
mkdir -p "$output_directory"

cleanup() {
    if [[ "$(git rev-parse HEAD)" != "$starting_commit" ]]; then
        git checkout --detach "$starting_commit" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

compare() {
    local artifact="$1"
    local package="$2"
    local default_features="$3"
    local features="$4"
    local args=(public-api -p "$package")

    if [[ "$default_features" == "false" ]]; then
        args+=(--no-default-features)
    fi
    if [[ -n "$features" ]]; then
        args+=(--features "$features")
    fi
    args+=(
        diff "${baseline_commit}..${candidate_commit}"
        --color never
        --deny changed
        --deny removed
    )

    echo "==> $artifact"
    cargo "${args[@]}" > "$output_directory/$artifact.diff"
}

compare nacelle-codec nacelle-codec true ""
compare nacelle-core-rustls nacelle-core false "error-hints,experimental-memory,phase-timing,rustls"
compare nacelle-core-openssl nacelle-core false "error-hints,experimental-memory,openssl,phase-timing"
compare nacelle-openssl nacelle-openssl true "vendored"
compare nacelle-rustls nacelle-rustls true "self-signed"
compare nacelle-tcp-rustls nacelle-tcp false "buffer-rotation,experimental-memory,phase-timing,rustls,tls-self-signed"
compare nacelle-tcp-openssl nacelle-tcp false "buffer-rotation,experimental-memory,experimental-openssl-detection,openssl,phase-timing"
compare nacelle-http nacelle-http true "experimental-memory,tls-self-signed"
compare nacelle-rustls-facade nacelle false "buffer-rotation,error-hints,experimental-memory,experimental-thread-per-core,http,phase-timing,rustls,tcp,tls-self-signed"
compare nacelle-openssl-facade nacelle false "buffer-rotation,error-hints,experimental-memory,experimental-openssl-detection,experimental-thread-per-core,http,openssl,phase-timing,tcp"

if [[ "$(git rev-parse HEAD)" != "$starting_commit" ]]; then
    echo "cargo-public-api did not restore the starting commit" >&2
    exit 1
fi

printf 'baseline=%s\ncandidate=%s\n' "$baseline_commit" "$candidate_commit" > "$output_directory/revisions.txt"
echo "==> Public API artifacts: $output_directory"
