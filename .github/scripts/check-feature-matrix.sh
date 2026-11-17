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
    --features error-hints,peer-limits,phase-timing,rustls \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle-core \
    --features error-hints,openssl,peer-limits,phase-timing \
    "${cargo_args[@]}"

cargo "${command}" -p nacelle-tcp \
    --features buffer-rotation,compact-connections,peer-limits,phase-timing,rustls,tls-self-signed \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle-tcp \
    --features buffer-rotation,compact-connections,experimental-openssl-detection,openssl-vendored,peer-limits,phase-timing \
    "${cargo_args[@]}"

cargo "${command}" -p nacelle \
    --no-default-features \
    --features bench,buffer-rotation,compact-connections,error-hints,experimental-thread-per-core,fuzzing,http,peer-limits,phase-timing,rustls,tcp,tls-self-signed \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle \
    --no-default-features \
    --features bench,buffer-rotation,compact-connections,error-hints,experimental-openssl-detection,experimental-thread-per-core,fuzzing,http,openssl-vendored,peer-limits,phase-timing,tcp \
    "${cargo_args[@]}"

cargo "${command}" -p nacelle-examples \
    --no-default-features \
    --features bench,http,tcp,tls-self-signed \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle-examples \
    --no-default-features \
    --features bench,http,peer-limits,tcp,tls-self-signed \
    "${cargo_args[@]}"
cargo "${command}" -p nacelle-examples \
    --no-default-features \
    --features bench,http,openssl,peer-limits,tcp \
    "${cargo_args[@]}"

bash .github/scripts/check-peer-limit-unification.sh "${command}" "${cargo_args[@]}"
