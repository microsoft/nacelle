---
name: Rust
description: "Use when writing, reviewing, testing, or changing Rust code in the Nacelle Cargo workspace. Covers crate boundaries, feature flags, lint expectations, tests, and validation commands."
applyTo: "**/*.rs"
---

# Rust Guidelines

- Follow the workspace lints in `Cargo.toml`; do not suppress a lint without a narrow, documented reason.
- Keep public APIs documented and preserve `Send`/`Sync`, streaming, cancellation, and error semantics when changing shared types or traits.
- Avoid new allocations, locks, timers, logging, or metric writes on request and connection hot paths unless measured and justified.
- Keep feature-gated code valid for the smallest supported feature set. In particular, `openssl` must not implicitly select `rustls`.
- Put unit tests near the owning module and integration or property tests in the existing crate test targets. Use Tokio's paused-time facilities for timing-sensitive tests when the local tests do so.
- For certificate/key parsing in tests, bind the parsing result first, then use an explicit `match` with a constant panic message in `Err(_)`. Reuse the crate's test-only `test_certificate` helper for Rustls certificates. Do not chain `unwrap`, `expect`, or `unwrap_or_else` onto TLS parsing, even with a constant panic closure: `expect`/`unwrap` print parser diagnostics, and CodeQL can still flag the fluent closure form as a sensitive-data logging path. Never format the PEM, parsed certificate/key, result, or parser error in assertions, panics, or logs. See [safe TLS test diagnostics](../../docs/how-to/security-scanning.md#safe-tls-test-diagnostics).
- Run a focused crate or test-target check immediately after editing.
- Before finalizing Rust changes, run `cargo fmt --all`, `.github/scripts/check-feature-matrix.sh clippy --all-targets -- -D warnings`, and `.github/scripts/check-feature-matrix.sh test`.
- Run `./scripts/validate-all.sh` as the broader feature-matrix check when feature wiring, TLS, cross-crate behavior, or release readiness is affected.