---
name: Documentation style
description: "Use throughout Nacelle work when writing documentation, rustdoc, comments, plans, review findings, or change descriptions."
applyTo: "**"
---

# Documentation and technical writing style

Apply this guidance to authored technical prose throughout repository work.
Follow the technical-contract precedence in the
[repository instructions](../../copilot-instructions.md#agent-workflows-and-style).
Use the [documentation instructions](../docs.instructions.md) for Nacelle's
documentation layout and validation.

## Rules

### GEN-WRITE-002: Separate requirements from proposals

State the problem and observable behavior, then inputs, outputs, invariants,
failures, acceptance examples, and unresolved decisions. Keep implementation
suggestions and future work distinct from the contract.

Why: a suggestion must not accidentally become required behavior.

### GEN-DOC-002: Document caller-facing contracts

Document externally public APIs, invariants, errors, safety conditions, and useful
examples where not evident. Document internal items whose purpose or failure
behavior is unclear from names and types.

Why: callers need the behavior they may rely on, not implementation narration.

### GEN-DOC-003: Maintain authoritative documentation

Update contracts, architecture, configuration, operations, and diagrams alongside
behavior. Distill accepted external collaboration into repository documentation;
retain working artifacts with their work item as history rather than current truth.
Follow existing placement, navigation, linking, and validation requirements.

Why: the repository should remain sufficient to understand and maintain the system.

### GEN-WRITE-004: Make change descriptions reviewable

Describe the concrete problem, resulting behavior, design choices, verification,
and limitations. Include abandoned approaches only for relevant trade-offs.
Preserve required public/internal sections and describe results, not editing history.

Why: reviewers need behavior and evidence rather than a transcript.

## Preferences

### GEN-DOC-101: Keep entry documentation focused

Describe current software in README files with a representative invocation or
integration example. Link deeper material rather than copying it; put future work
in a backlog and durable designs in the existing documentation structure.

Why: an entry point should explain current use without speculative detail.

### GEN-DOC-102: Comment intent and constraints

Explain rationale, invariants, surprising constraints, safety, and consequences,
not adjacent code. Preserve language-specific documentation requirements.

Why: comments earn their maintenance cost by explaining what code cannot.

### GEN-DOC-103: Retain consequential design decisions

Record costly-to-reverse, surprising decisions with genuine trade-offs in the
existing architecture or topic documentation. Include context, decision,
rationale, and consequences; use a separate ADR when requested. Keep internal
working notes in ignored locations, not public reference documentation. Use
maintained diagrams beside explanatory text when relationships are harder to
understand linearly.

Why: maintainers need enough context to preserve or deliberately revisit decisions.

### WRITE-101: Follow the project's commit convention

Follow repository commit-title requirements; otherwise prefer Conventional Commit
titles with a type, optional scope, and concise result. Add a body for rationale,
migration concerns, or non-obvious trade-offs.

Why: consistency aids scanning without introducing a competing repository policy.
