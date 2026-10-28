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
    --features error-hints,phase-timing,rustls \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle-core \
    --features error-hints,openssl,phase-timing \
    "${cargo_args[@]}"

cargo "${command}" -p nacelle-tcp \
    --features buffer-rotation,phase-timing,policies,rustls,tls-self-signed \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle-tcp \
    --features buffer-rotation,experimental-openssl-detection,openssl-vendored,phase-timing,policies \
    "${cargo_args[@]}"

cargo "${command}" -p nacelle \
    --no-default-features \
    --features bench,buffer-rotation,error-hints,experimental-thread-per-core,fuzzing,http,phase-timing,policies,rustls,tcp,tcp-policies,tls-self-signed \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle \
    --no-default-features \
    --features bench,buffer-rotation,error-hints,experimental-openssl-detection,experimental-thread-per-core,fuzzing,http,openssl-vendored,phase-timing,policies,tcp,tcp-policies \
    "${cargo_args[@]}"

cargo "${command}" -p nacelle-examples \
    --no-default-features \
    --features bench,http,policies,tcp,tls-self-signed \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle-examples \
    --no-default-features \
    --features bench,http,openssl,policies,tcp \
    "${cargo_args[@]}"
