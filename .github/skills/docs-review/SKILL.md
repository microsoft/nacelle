---
name: docs-review
description: "Review Nacelle README, rustdoc, Markdown, mdBook navigation, commands, links, API contracts, and readiness wording against implementation and validation evidence."
---

# Reviewing Nacelle Documentation

Use this workflow for documentation-only changes and the documentation portion
of mixed changes. Apply the general review evidence and severity rules without
calling another review agent.

## Procedure

1. Read the [documentation instructions](../../instructions/docs.instructions.md)
   and general [communication](../../instructions/style/communication-style.instructions.md)
   and [documentation](../../instructions/style/documentation-style.instructions.md)
   styles.
2. Identify the audience and contract: introductory use, tutorial, operator
   procedure, architecture explanation, or API/configuration reference. Keep
   working assessments and requested plans in their ignored locations.
3. Compare claims with the owning crate, manifests, tests, examples, and scripts.
   Verify names, defaults, feature gates, errors, limits, and wire behavior.
   Check decoder progress and buffer ownership when codec documentation changes.
4. Verify commands from their stated working directory. Confirm package names,
   binary targets, flags, config paths, prerequisites, and supported TLS
   combinations. Do not suggest workspace `--all-features` where it enables
   mutually exclusive providers.
5. Check relative file links and heading anchors. For published pages, trace
   navigation from `docs/SUMMARY.md` and verify mdBook paths in `book.toml`.
   Do not assume a successful render proves external links or commands work.
6. Check that public API and behavior changes update the relevant crate README
   and reference pages. Avoid duplicate authoritative descriptions that can
   diverge.
7. Assess performance and readiness language against the stated platform,
   feature flags, workload, security mode, measurements, and rollback posture.
   Keep correctness, security, performance, operability, compatibility, and
   documentation evidence distinct. Do not import deployment requirements from
   another product or treat experimental status as a reason to omit safety
   contracts.
8. Apply style to authored prose without changing identifiers, protocol tokens,
   quotations, or required technical details. Report consequential ambiguity or
   explicit rule violations, not subjective rewrites.
9. Inspect or, when authorized, run `mdbook build` for book changes. For rustdoc,
   assess owning-crate doc tests or the documented `cargo doc` command under
   the affected feature set. Report checks not run and unavailable tooling.

## Output

Lead with verified findings: location, inaccurate or missing contract, affected
reader or operation, evidence, and a concrete correction. Separate optional
wording suggestions and unresolved evidence gaps from defects. If no verified
findings remain, say so and state the validation scope.
