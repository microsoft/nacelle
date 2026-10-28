---
name: nacelle-review
description: "Review Nacelle Rust and Cargo changes for decoder progress, buffer ownership, transport and Handler contracts, cancellation, backpressure, TLS separation, features, and validation."
---

# Reviewing Nacelle Rust and Runtime Changes

Use this domain workflow with the general review rules. Review the affected
contracts; do not require every check below for unrelated changes.

## Preparation

Read the [repository instructions](../../copilot-instructions.md) and
[Rust instructions](../../instructions/rust.instructions.md). For the affected
area, consult the architecture guide, owning crate, callers, tests, manifests,
and public reference documentation. Apply the
[security](../../instructions/security.instructions.md) and
[performance](../../instructions/performance.instructions.md) guidance when
relevant.

## Review Procedure

### 1. Check Ownership and Compatibility

- Keep reusable byte/message codecs and reader/writer buffers in `nacelle-codec`.
  Keep transport-neutral handlers, lifecycle, limits, telemetry, and TLS
  primitives in `nacelle-core`.
- Keep TCP/Unix policy in `nacelle-tcp`, HTTP policy in `nacelle-http`, provider
  implementations in their TLS crates, and convenience re-exports in `nacelle`.
- Keep the reference protocol in the unpublished
  `examples/nacelle-reference-protocol` package, outside the published library
  API.
- Trace public type, trait, error, configuration, default, feature, and wire
  changes through consumers and adapters. Confirm intended changes have
  compatibility documentation and focused coverage.

### 2. Check Decoding, Encoding, and Buffer Ownership

- Successful decoding must consume input; requesting more input must leave the
  cumulative buffer unchanged. Check partial input, multiple frames, malformed
  input, EOF, and oversized input where the change affects those paths.
- Verify size limits reject input before unbounded growth, and that reader and
  writer adapters preserve bounded buffering and backpressure.
- Preserve shared backing allocations and zero-copy behavior where promised.
  Trace slices and retained messages rather than assuming an extra clone or
  borrowed value is either necessary or safe.
- Check encoding and partial writes for dropped, duplicated, reordered, or
  incorrectly flushed data.

### 3. Check Async Lifecycle and Resource Bounds

- Trace handler errors, cancellation, deadlines, connection teardown, and
  graceful shutdown through the affected tasks.
- Verify task, queue, connection, and buffer bounds; ensure slow peers or
  handlers cannot bypass the intended backpressure.
- Check blocking operations, lock scope across `.await`, task ownership,
  `Send`/`Sync` requirements, and cleanup when a future is dropped.
- Require deterministic coverage for changed timing behavior where practical;
  follow existing paused-time tests rather than adding sleep-based assumptions.

### 4. Check Features, TLS, and Diagnostics

- Read manifests and the feature matrix. Rustls serves TCP and HTTP; OpenSSL is
  TCP-specific. Negotiated TLS metadata belongs in `nacelle-core`.
- Preserve mutual exclusion of Rustls and OpenSSL; do not use workspace
  `--all-features` as a valid provider combination or let `openssl` implicitly
  select `rustls`.
- Verify changed feature-gated paths under the smallest relevant feature set
  and the supported provider combinations.
- Keep peer-controlled data out of metric labels. Preserve low cardinality,
  stable rejection reasons, and opt-in diagnostic overhead.
- For hot-path changes, identify allocations, retained buffers, locks, timers,
  and instrumentation. Require comparable measurements before accepting a
  throughput, latency, or memory improvement claim.

### 5. Evaluate Evidence and Report

Start with focused tests for the owning crate. Codec changes start with
`cargo test -p nacelle-codec --all-features`; performance claims also need the
existing benchmark comparison under comparable conditions.

Use the Rust instructions for formatting, lint, and test gates. Feature wiring,
TLS, cross-crate changes, and readiness work require the broader
`./scripts/validate-all.sh` evidence. A review does not itself authorize running
or installing every check.

Verify corresponding API, feature, configuration, security, and performance
documentation updates. Report findings using the general review severity and
evidence rules, with skipped checks and remaining uncertainty stated explicitly.
