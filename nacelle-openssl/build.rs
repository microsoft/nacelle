//! Detect TLS 1.3 support in the linked OpenSSL library for version-specific tests.

use std::env;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(nacelle_openssl_tls13)");

    if env::var("DEP_OPENSSL_VERSION_NUMBER")
        .ok()
        .and_then(|version| u64::from_str_radix(&version, 16).ok())
        .is_some_and(|version| version >= 0x1010_1000)
    {
        println!("cargo::rustc-cfg=nacelle_openssl_tls13");
    }
}
