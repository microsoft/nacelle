#!/bin/bash
set -ex

toolchain="${nightly:-nightly-2026-04-16}"

rustup toolchain install "${toolchain}" --component miri
cargo +"${toolchain}" miri setup

export MIRIFLAGS="-Zmiri-strict-provenance"

# Miri does not support Tokio's non-blocking network I/O. Networked tests run
# in the ordinary test matrix; keep this job focused on memory-safe tests.
# The metric schema test enters crossbeam-epoch through metrics-util's dev-only
# histogram recorder, whose integer pointer casts strict-provenance Miri rejects.
cargo +"${toolchain}" miri test -p nacelle-core --lib -- \
	--test-threads=1 \
	--skip telemetry::tests::metric_schema_uses_singular_names_and_base_units
cargo +"${toolchain}" miri test -p nacelle --lib app::tests::app_starts_without_listeners -- --test-threads=1

# run with wrapping integer overflow instead of panic
cargo +"${toolchain}" miri test --release -p nacelle-core --lib -- \
	--test-threads=1 \
	--skip telemetry::tests::metric_schema_uses_singular_names_and_base_units
cargo +"${toolchain}" miri test --release -p nacelle --lib app::tests::app_starts_without_listeners -- --test-threads=1
