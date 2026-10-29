---
name: Documentation
description: "Use when writing or reviewing Nacelle Markdown, crate READMEs, rustdoc, documentation links, mdBook navigation, or book configuration."
applyTo: "**/*.md,**/*.rs,book.toml"
---

# Nacelle Documentation

- Describe Nacelle as an experimental Rust/Tokio library. Keep readiness claims scoped to an audience, environment, workload, evidence, and rollback posture; documentation polish is not proof of readiness.
- Use [the architecture guide](../../docs/topics/architecture.md) and the owning crate's implementation, manifests, tests, and examples to verify behavior. Preserve the distinctions between codecs, transport-neutral core types, TCP/Unix, HTTP, TLS providers, and facade re-exports.
- Keep entry points in the root and crate READMEs concise. Place tutorials, how-to guides, explanations, and API/configuration references in the existing sections under [docs](../../docs/SUMMARY.md); link to authoritative detail instead of copying it.
- The documentation site uses mdBook. Maintain [SUMMARY.md](../../docs/SUMMARY.md) when adding, moving, or removing published pages, and verify paths against [book.toml](../../book.toml).
- Use relative links in Markdown files. Check targets, heading anchors, and the paths used by commands from their documented working directory. Do not hand-edit generated book or rustdoc output.
- Document public API contracts, decoder progress and buffer ownership, feature requirements, defaults, errors, limits, and compatibility consequences when affected. Rustdoc examples must use supported feature combinations; Rustls and OpenSSL cannot be enabled together.
- For codec contract changes, update both [the codec README](../../nacelle-codec/README.md) and [the codec reference](../../docs/reference/nacelle-codec.md). Update the corresponding transport, TLS, configuration, security, or performance pages for changes in those areas.
- Keep internal working documents under ignored `docs/internal/`, requested plans under ignored `.github/plans/`, and review provenance and long-running review ledgers under ignored `.github/ledger/`. Keep these local and untracked; never force-add them or add ignore exceptions. Do not promote working assessments into public documentation.
- Apply the general [communication](style/communication-style.instructions.md) and [documentation](style/documentation-style.instructions.md) styles. Preserve exact technical wording and required safety details when presentation preferences conflict.
- Run `mdbook build` after changing book structure, links, or rendered content. For rustdoc or API changes, use the owning crate's doc tests or documented `cargo doc` commands under the affected feature set. Report unavailable tooling or skipped checks explicitly; prose-only instruction edits do not require Rust workspace tests.
