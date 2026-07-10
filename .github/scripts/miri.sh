#!/bin/bash
set -ex

toolchain="${nightly:-nightly-2026-04-16}"

rustup toolchain install "${toolchain}" --component miri
cargo +"${toolchain}" miri setup

export MIRIFLAGS="-Zmiri-strict-provenance"

cargo +"${toolchain}" miri test \
    -p nacelle-core \
    -p nacelle \
    --lib

# run with wrapping integer overflow instead of panic
cargo +"${toolchain}" miri test --release \
    -p nacelle-core \
    -p nacelle \
    --lib
