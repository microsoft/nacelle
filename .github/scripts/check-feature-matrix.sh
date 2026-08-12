#!/usr/bin/env bash
set -euo pipefail

command="${1:-check}"
shift || true
cargo_args=("$@")

cargo hack "${command}" --workspace --each-feature --optional-deps --exclude-all-features "${cargo_args[@]}"
cargo hack "${command}" --workspace --all-features \
    --exclude nacelle-core \
    --exclude nacelle-examples \
    --exclude nacelle-openssl \
    --exclude nacelle-tcp \
    --exclude nacelle \
    "${cargo_args[@]}"

cargo "${command}" -p nacelle-core \
    --features error-hints,experimental-memory,phase-timing,rustls \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle-core \
    --features error-hints,experimental-memory,openssl,phase-timing \
    "${cargo_args[@]}"

cargo "${command}" -p nacelle-tcp \
    --features buffer-rotation,experimental-memory,phase-timing,rustls,tls-self-signed \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle-tcp \
    --features buffer-rotation,experimental-memory,experimental-openssl-detection,openssl-vendored,phase-timing \
    "${cargo_args[@]}"

cargo "${command}" -p nacelle \
    --no-default-features \
    --features bench,buffer-rotation,error-hints,experimental-memory,experimental-thread-per-core,fuzzing,http,phase-timing,rustls,tcp,tls-self-signed \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle \
    --no-default-features \
    --features bench,buffer-rotation,error-hints,experimental-memory,experimental-openssl-detection,experimental-thread-per-core,fuzzing,http,openssl-vendored,phase-timing,tcp \
    "${cargo_args[@]}"

cargo "${command}" -p nacelle-examples \
    --no-default-features \
    --features bench,experimental-memory,http,tcp,tls-self-signed \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle-examples \
    --no-default-features \
    --features bench,experimental-memory,http,openssl,tcp \
    "${cargo_args[@]}"
