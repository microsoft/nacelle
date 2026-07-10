#!/bin/bash

set -ex

toolchain="${nightly:-nightly-2026-04-16}"

rustup toolchain install "${toolchain}" --component rust-src

export ASAN_OPTIONS="detect_odr_violation=0 detect_leaks=0"
packages=(
    -p nacelle-core
    -p nacelle-tcp
    -p nacelle-http
    -p nacelle
    -p nacelle-stress-common
)

# Run address sanitizer
RUSTFLAGS="-Z sanitizer=address" \
cargo +"${toolchain}" test "${packages[@]}" --lib --target x86_64-unknown-linux-gnu

# Run thread sanitizer
RUSTFLAGS="-Z sanitizer=thread" \
cargo +"${toolchain}" -Zbuild-std test "${packages[@]}" --lib --target x86_64-unknown-linux-gnu
