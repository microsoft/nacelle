#!/bin/bash

toolchain="${nightly:-nightly-2026-04-16}"

rustup toolchain install "${toolchain}" --component miri
cargo +"${toolchain}" miri setup

set -ex
RUSTFLAGS="$RUSTFLAGS -Cpanic=abort -Zpanic-abort-tests" cargo +"${toolchain}" test --all-features --test '*'