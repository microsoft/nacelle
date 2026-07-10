#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p nacelle-core --features http-types,tls-self-signed,tower,otel --all-targets
cargo clippy -p nacelle-core --features http-types,tls-self-signed,tower,otel --all-targets -- -D warnings
cargo test -p nacelle-tcp --all-targets
cargo clippy -p nacelle-tcp --all-targets -- -D warnings
cargo test -p nacelle-tcp --features tls-self-signed --all-targets
cargo clippy -p nacelle-tcp --features tls-self-signed --all-targets -- -D warnings
cargo test -p nacelle-http --features tls-self-signed --all-targets
cargo clippy -p nacelle-http --features tls-self-signed --all-targets -- -D warnings
cargo test -p nacelle-reference-protocol --all-targets
cargo clippy -p nacelle-reference-protocol --all-targets -- -D warnings
cargo check -p nacelle-examples --all-features --all-targets
cargo clippy -p nacelle-examples --all-features --all-targets -- -D warnings
cargo test -p nacelle --features http,tower,otel --all-targets
cargo clippy -p nacelle --features http,tower,otel --all-targets -- -D warnings
cargo test -p nacelle --no-default-features --features http --all-targets
cargo test -p nacelle --no-default-features --features tls --all-targets
cargo clippy -p nacelle --no-default-features --features tcp --all-targets -- -D warnings
cargo test -p nacelle --no-default-features --features tls-self-signed --all-targets
cargo test -p nacelle --no-default-features --features http,tls-self-signed --all-targets
cargo test -p nacelle --features tls-self-signed --all-targets
cargo clippy -p nacelle --features http,tower,otel,tls-self-signed --all-targets -- -D warnings
cargo test -p nacelle --no-default-features --all-targets
cargo test -p nacelle-stress-test --all-targets
cargo test -p nacelle-stress-test --no-default-features --all-targets
cargo test -p nacelle-stress-server --all-targets
cargo test -p nacelle-stress-server --no-default-features --all-targets
cargo tree -i serde_yaml >/dev/null 2>&1 && {
  echo "serde_yaml is still present" >&2
  exit 1
} || true
cargo tree -i unsafe-libyaml >/dev/null 2>&1 && {
  echo "unsafe-libyaml is still present" >&2
  exit 1
} || true
if cargo tree -p nacelle --no-default-features --features tcp,openssl -i rustls >/dev/null 2>&1; then
  echo "rustls is selected by the tcp,openssl feature set" >&2
  exit 1
fi
if cargo tree --workspace --no-default-features -i rustls >/dev/null 2>&1; then
  echo "rustls is selected by the workspace no-default feature set" >&2
  exit 1
fi
if command -v cargo-audit >/dev/null 2>&1; then
  cargo audit
else
  echo "cargo-audit not installed; skipping audit" >&2
fi
