---
name: code-review
description: "Review Nacelle pull requests, diffs, and local changes for verified correctness, compatibility, resource-use, performance, and documentation problems."
---

# Reviewing Nacelle Changes

Review the requested change without modifying it. Use this workflow for pull
requests, branch diffs, staged or unstaged changes, and pre-submission reviews.

## Scope and Applicable Guidance

- Read the [repository instructions](../../copilot-instructions.md), including
  the general communication and documentation styles.
- For Rust, Cargo, codecs, transports, handlers, TLS, or runtime behavior, apply
  the [Nacelle review workflow](../nacelle-review/SKILL.md).
- For Markdown, rustdoc, mdBook, or documentation links, apply the
  [documentation review workflow](../docs-review/SKILL.md).
- Use both workflows for mixed changes, without reporting the same issue twice.
  These workflows supplement this one; they do not call it recursively.
- Perform the review directly when the evidence fits the current context.
  Delegate only explicitly requested or substantial independent work; do not
  create a reviewer for every file or recursively delegate reviews.

## Procedure

### 1. Establish the Review Target

Identify the requested base, head, commit, pull request, or local change set.
Inspect the working-tree state without reverting or staging changes. For GitHub
reviews, prefer `gh` and disable command pagers.

Read the diff and relevant implementation before adopting the author's account
of its behavior. Read enough surrounding code to establish the affected
invariants, then trace callers, consumers, helpers, sibling implementations, and
tests where they can change the conclusion. Use history when it resolves a
specific compatibility question or explains a surprising constraint, not as a
mandatory scan of every file.

### 2. Establish Behavior and Requirements

State the old and new observable behavior. Distinguish established contracts
from inferred motivation. Read the PR description, linked requirements, and
existing review comments to reconcile the implementation with the requested
outcome and avoid duplicate findings. Treat performance and readiness claims as
claims requiring evidence.

Do not expand the review into unrelated cleanup or a repository-wide audit.
Check sibling paths when a changed contract affects them, but identify
pre-existing issues separately from regressions caused by the change.

### 3. Verify Candidate Findings

Prioritize reachable correctness failures, compatibility breaks, resource
growth, cancellation and concurrency errors, security regressions, and missing
coverage for changed behavior.

Before reporting a finding:

1. Trace the relevant input and execution path through callers and guards.
2. Verify the violated contract or requirement in code, tests, or authoritative
   documentation.
3. Identify the observable consequence and affected conditions.
4. Check whether a focused test, reproduction, or existing validation result
   confirms or disproves the concern.
5. Suggest a correction that preserves the surrounding contracts.

Do not treat an unproven scenario, unmeasured optimization, or personal style
preference as a verified defect. Keep unresolved evidence gaps separate from
findings. Apply explicit style rules to newly authored prose, but do not elevate
cosmetic suggestions into correctness blockers.

### 4. Assess Validation

Use the checks required by the applicable instructions and the change's blast
radius. Distinguish checks inspected from checks executed; never claim an
unexecuted check passed.

In a read-only review, run checks only when authorized by the review scope.
Do not install tooling, update dependencies, or modify source files. Report
relevant failed checks rather than assuming CI will catch them, and avoid
duplicating routine formatter diagnostics as separate review findings.

### 5. Report Results

Lead with the findings, ordered by severity and then confidence:

- **error**: A verified correctness, security, compatibility, or explicit
  acceptance failure that must be fixed before merge.
- **warning**: A verified bounded performance/resource regression or established
  convention violation that should be corrected.
- **suggestion**: An optional improvement, clearly separate from required fixes.

For each finding, include a concise title, exact file and line location, the
triggering conditions, the consequence, evidence, and a concrete correction.
Consolidate repeated instances of the same root cause, listing affected paths.
Do not invent findings to fill a quota.

Conclude with validation performed, evidence gaps, and residual risks. If no
verified findings remain, say so without claiming the change is risk-free or
production ready.

Do not post comments, approve or reject a PR, create commits, or push changes
unless the user explicitly requests the corresponding action.
