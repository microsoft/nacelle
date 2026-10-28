# Nacelle Project Guidelines

Nacelle is an experimental Tokio-based Rust library for streaming services across TCP, Unix sockets, HTTP/1, and TLS-enabled listeners. Keep changes small, validated, and consistent with the existing crate boundaries.

## Architecture

- Read `README.md` and `docs/topics/architecture.md` before changing public behavior or crate boundaries.
- Keep transport-agnostic byte/message decoding, encoding, asynchronous reader/writer adapters, and reusable framing buffers in `nacelle-codec`.
- Keep transport-neutral request, response, handler, lifecycle, limits, telemetry, and TLS primitives in `nacelle-core`.
- Keep TCP and Unix socket behavior in `nacelle-tcp`, all HTTP behavior in `nacelle-http`, and convenience re-exports in `nacelle`.
- Keep the reference protocol in the unpublished `examples/nacelle-reference-protocol` package, outside the published library API.
- Treat TCP `Protocol` implementations as consumers of the `nacelle-codec` decoder contract and, together with HTTP conversion, as adapters around the transport-neutral `Handler` boundary.
- Preserve public APIs, feature behavior, configuration defaults, and wire behavior unless the task explicitly requires a change. Update the corresponding reference docs when these contracts change.
- Keep generated files, dependency updates, formatting churn, and metadata edits out of scope unless required. Allow Cargo to update `Cargo.lock` only when dependency resolution changes.

## Implementation

- Inspect the nearby implementation, tests, manifests, and documentation before editing, then follow the local pattern.
- Make the smallest coherent change in the crate that owns the behavior. Avoid moving transport-specific policy into `nacelle-core`.
- Preserve the `MessageDecoder` progress contract: successful decoding consumes input, while requesting more input leaves the cumulative buffer unchanged. Custom decoders must reject oversized input before buffers can grow without bound.
- Preserve `nacelle-codec` buffer ownership and zero-copy behavior unless the task explicitly changes that contract; decoded messages may share the reader's backing allocation.
- Add or update focused tests for behavior changes and regressions. Cover feature-gated behavior under the relevant feature combinations.
- Preserve bounded resource use, backpressure, graceful shutdown, and low-cardinality telemetry.
- Update `nacelle-codec/README.md` and `docs/reference/nacelle-codec.md` when codec APIs or contracts change. Update the corresponding documentation for other public APIs, features, configuration, operator behavior, compatibility, or performance guidance.
- `docs/internal/` is intentionally blanket-ignored. Keep documents local and untracked; do not add `.gitignore` exceptions for internal docs.
- `.github/plans/` is intentionally blanket-ignored. Keep documents local and untracked; do not add `.gitignore` exceptions for plans.

## Validation

- Run the narrowest relevant test or check first, then broaden validation to match the change's blast radius.
- Do not make unqualified production-readiness claims. Scope them by audience, environment, workload, and rollback posture, and report correctness, security, performance, operability, compatibility, and documentation evidence separately.
- For codec changes, start with `cargo test -p nacelle-codec --all-features` and run its benchmark comparison when making performance claims.
- Follow `.github/instructions/rust.instructions.md` for Rust formatting, linting, and workspace tests.
- Run `./scripts/validate-all.sh` when changes affect feature combinations, TLS providers, crate integration, or release readiness.
- Build the mdBook documentation when changing documentation structure, links, or rendered content.

## Agent Workflows and Style

- Apply [communication style](instructions/style/communication-style.instructions.md) and [documentation style](instructions/style/documentation-style.instructions.md) throughout repository work, including responses, reviews, plans, comments, and documentation. These are general instructions, not opt-in skills.
- Style must not change technical meaning or omit required errors, safety conditions, compatibility details, or validation evidence. Preserve exact identifiers, protocol tokens, quotations, and externally required wording. Nacelle's correctness, security, and compatibility requirements take precedence over presentation preferences.
- For reviews, use the [general review workflow](skills/code-review/SKILL.md) with the [Nacelle review workflow](skills/nacelle-review/SKILL.md) for Rust, Cargo, transport, codec, and runtime changes, or the [documentation review workflow](skills/docs-review/SKILL.md) for documentation changes.
- Follow [documentation instructions](instructions/docs.instructions.md) when changing Markdown, rustdoc, or mdBook configuration. Use the [planning workflow](skills/planning/SKILL.md) for broad or risky work; create plan files only when requested.
- Review agents are optional tools for explicit requests or substantial independent review work, not mandatory steps for every change. Do not recursively delegate reviews.

## Commits

When asked to commit, use Conventional Commits with a short, scoped message such as `fix: ...`, `feat: ...`, `perf: ...`, `docs: ...`, or `test: ...`. Commit only the coherent change requested; do not include unrelated working-tree changes.

Use the [PR and commit drafting workflow](skills/pr-description/SKILL.md) when asked for titles, PR descriptions, commit descriptions, or squash commit messages. Commit bodies, including PR descriptions intended for reuse as commit bodies, must omit validation sections and equivalent test-results summaries. Keep necessary evidence and known failures in the PR or a separate handoff note. Remove filler throughout authored prose.