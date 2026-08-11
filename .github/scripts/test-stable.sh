#!/bin/bash

set -ex

cmd="${1:-test}"

.github/scripts/check-feature-matrix.sh "${cmd}"

if [[ "${RUST_VERSION}" == "nightly"* ]]; then
    # Check benchmarks
    cargo check --benches

    # Check minimal versions
    # Remove dev-dependencies from Cargo.toml to prevent the next `cargo update`
    # from determining minimal versions based on dev-dependencies.
    cargo hack --remove-dev-deps --workspace
    # Update Cargo.lock to minimal version dependencies.
    cargo update -Z minimal-versions
    .github/scripts/check-feature-matrix.sh check
fi