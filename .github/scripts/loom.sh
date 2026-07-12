#!/bin/bash
set -ex

toolchain="${1:-}"
cargo=(cargo)
if [[ -n "${toolchain}" ]]; then
    cargo=(cargo +"${toolchain}")
fi

RUSTFLAGS="--cfg loom -Dwarnings" "${cargo[@]}" test -p nacelle-core --lib
