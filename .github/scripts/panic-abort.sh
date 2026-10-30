#!/bin/bash

toolchain="${nightly:-nightly-2026-04-16}"

rustup toolchain install "${toolchain}" --component miri
cargo +"${toolchain}" miri setup

set -ex
# Rustdoc needs matching panic settings when its examples link these dependencies.
RUSTUP_TOOLCHAIN="${toolchain}" RUSTFLAGS="$RUSTFLAGS -Cpanic=abort -Zpanic-abort-tests" \
	RUSTDOCFLAGS="${RUSTDOCFLAGS:-} -Cpanic=abort -Zpanic-abort-tests" \
	.github/scripts/check-feature-matrix.sh test
