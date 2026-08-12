#!/usr/bin/env bash
set -euo pipefail

files=(
    nacelle/src/app.rs
    nacelle/src/host.rs
    nacelle-tcp/src/connection.rs
    nacelle-tcp/src/server.rs
    nacelle-tcp/src/serial_server.rs
    nacelle-tcp/src/server/listeners.rs
    nacelle-tcp/src/runtime/tcp.rs
    nacelle-tcp/src/runtime/unix.rs
    nacelle-tcp/src/runtime/rustls.rs
    nacelle-tcp/src/runtime/openssl.rs
    nacelle-http/src/server.rs
)

missing="$({
    perl -0777 -ne '
        while (/((?:\s*#\[[^\n]+\]\n|\s*\/\/\/[^\n]*\n)*)\s*pub (?:async )?fn ((?:enable_|serve)[A-Za-z0-9_]*|run|wait|shutdown_and_wait(?:_timeout)?)(?=\s*(?:<|\())/mg) {
            my ($attributes, $name) = ($1, $2);
            next if $attributes =~ /doc\(hidden\)/;
            print "$ARGV:$name\n" unless $attributes =~ /\/\/\//;
        }
    ' "${files[@]}"
} || true)"

if [[ -n "$missing" ]]; then
    echo "visible serving APIs without item-level rustdoc:" >&2
    printf '%s\n' "$missing" >&2
    exit 1
fi

require_section() {
    local file="$1"
    local section="$2"
    if ! grep -Fq "$section" "$file"; then
        echo "$file is missing $section" >&2
        exit 1
    fi
}

contract_files=(
    nacelle/src/app.rs
    nacelle/src/host.rs
    nacelle-tcp/src/connection.rs
    nacelle-tcp/src/runtime.rs
    nacelle-tcp/src/server.rs
    nacelle-tcp/src/serial_server.rs
    nacelle-http/src/server.rs
)
for file in "${contract_files[@]}"; do
    require_section "$file" "# Errors"
    require_section "$file" "# Panics"
    require_section "$file" "# Example"
done

require_section nacelle/src/app.rs "# Serving contract"
require_section nacelle/src/host.rs "# Serving contract"
require_section docs/reference/api-stability.md 'Public exports marked `#[doc(hidden)]`'

examples=(
    echo
    manual_host
    direct_tcp
    listener_tcp
    unix_echo
    http_echo
    direct_http
    tls_echo
    tls_http_echo
    openssl_echo
)
for example in "${examples[@]}"; do
    require_section examples/nacelle-examples/Cargo.toml "name = \"$example\""
done

echo "serving rustdoc audit passed"