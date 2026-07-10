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
- Run a focused crate or test-target check immediately after editing.
- Before finalizing Rust changes, run `cargo fmt --all`, `cargo clippy --workspace --all-features --all-targets -- -D warnings`, and `cargo test --workspace --all-features`.
- Run `./scripts/validate-all.sh` as the broader feature-matrix check when feature wiring, TLS, cross-crate behavior, or release readiness is affected.