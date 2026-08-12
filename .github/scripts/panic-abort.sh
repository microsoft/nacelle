#!/bin/bash

toolchain="${nightly:-nightly-2026-04-16}"

rustup toolchain install "${toolchain}" --component miri
cargo +"${toolchain}" miri setup

set -ex
RUSTUP_TOOLCHAIN="${toolchain}" RUSTFLAGS="$RUSTFLAGS -Cpanic=abort -Zpanic-abort-tests" \
	.github/scripts/check-feature-matrix.sh test