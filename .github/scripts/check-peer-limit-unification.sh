#!/usr/bin/env bash
set -euo pipefail

command="${1:-test}"
shift || true
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
fixture="$(mktemp -d)"
trap 'rm -f -- "$fixture/Cargo.toml" "$fixture/Cargo.lock"; rmdir -- "$fixture"' EXIT

cp -- "$repo_root/Cargo.lock" "$fixture/Cargo.lock"
cat > "$fixture/Cargo.toml" <<EOF
[package]
name = "nacelle-peer-limit-unification"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]
resolver = "2"

[features]
core-peer-limits = ["nacelle-core/peer-limits"]
tcp-peer-limits = ["nacelle-tcp/peer-limits"]

[dependencies]
nacelle = { path = "$repo_root/nacelle", default-features = false, features = ["experimental-thread-per-core"] }
nacelle-core = { path = "$repo_root/nacelle-core", default-features = false }
nacelle-tcp = { path = "$repo_root/nacelle-tcp", default-features = false }

[lib]
path = "$repo_root/.github/fixtures/peer-limit-unification.rs"
EOF

rustfmt --edition 2024 --check "$repo_root/.github/fixtures/peer-limit-unification.rs"
for feature in core-peer-limits tcp-peer-limits; do
    args=(--manifest-path "$fixture/Cargo.toml" --no-default-features --features "$feature")
    facade_features="$(cargo tree "${args[@]}" -i nacelle --depth 0 --prefix none --format '{f}')"
    if [[ ",$facade_features," == *",peer-limits,"* ]]; then
        echo "mixed-dependency fixture unexpectedly enabled nacelle/peer-limits" >&2
        exit 1
    fi
    echo "==> Peer limits through $feature without nacelle/peer-limits"
    cargo "$command" "${args[@]}" --target-dir "${CARGO_TARGET_DIR:-"$repo_root/target"}" "$@"
done
