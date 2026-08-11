#!/usr/bin/env bash
set -euo pipefail

if output=$(cargo check -p nacelle --no-default-features --features tcp,rustls,openssl 2>&1); then
    echo "dual-provider build unexpectedly succeeded" >&2
    exit 1
fi

if ! grep -Fq "Nacelle supports exactly one TLS backend" <<<"${output}"; then
    echo "dual-provider build failed without the expected Nacelle provider guard" >&2
    printf '%s\n' "${output}" >&2
    exit 1
fi
