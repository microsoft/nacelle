---
name: code-review
description: "Review Nacelle pull requests, whole branches, diffs, and local changes for verified correctness, compatibility, resource-use, performance, and documentation problems. Coordinate multiple reviewers for substantial independent branch-review scopes."
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
  Coordinate multiple reviewers for substantial independent scopes as described
  below. Do not split one continuous trace into nominal review angles, create a
  reviewer for every file, or recursively delegate reviews.

## Procedure

### 1. Establish the Review Target

Identify the requested base, head, commit, pull request, or local change set.
Inspect the working-tree state without reverting or staging changes. For GitHub
reviews, prefer `gh` and disable command pagers.

For a branch or pull request, use the requested target or an existing local
target ref such as `origin/main`, and establish its merge-base with the reviewed
head. Inspect the complete merge-base-to-head diff and commits, not only the
latest fix, an attachment, or an earlier summary. Distinguish committed changes
from staged and unstaged changes; include the latter only when requested. State
any unavailable refs or differences between the checkout and the requested PR.

Inventory added, modified, deleted, and renamed files, including tests,
manifests, tooling, and documentation. Keep a compact coverage record in session
state: reviewed areas and critical paths, delegated owners, and deferred areas
with reasons. Account for every changed file or coherent file group; a partial
review must not be reported as a whole-branch review.

Read the diff and relevant implementation before adopting the author's account
of its behavior. Read enough surrounding code to establish the affected
invariants, then trace callers, consumers, helpers, sibling implementations, and
tests where they can change the conclusion. Use history when it resolves a
specific compatibility question or explains a surprising constraint, not as a
mandatory scan of every file.

#### Coordinate Independent Reviews

For a substantial branch with independent code and contract contexts, use
multiple reviewers with bounded assignments. A branch spanning transports and
shared policy primitives can warrant separate TCP delivery/lifecycle, HTTP
admission/body-handling, and core policy-contract reviews. Do not add agents
merely because evidence spans several files or fits different labels.

The coordinator must:

- Give every reviewer the same base and head, its owned scope, relevant shared
  contracts and dependencies, known findings, and explicit stopping criteria.
  Prefer the existing specialized review agents.
- Require inspected files and paths, checked invariants, reachable failure
  examples, finding locations and confidence, test coverage, and unresolved
  questions. A bare "no issues found" is not a sufficient review result.
- Retain cross-scope integration work, including feature wiring, adapter
  contracts, and interactions between shared primitives and transports.
  Specialists must not recursively launch more reviewers.
- Reconcile reports rather than concatenate them. Resolve ownership gaps,
  conflicting conclusions, duplicate findings, and cross-scope consequences.
  Use the assigned reviewer for needed follow-up instead of repeating its
  investigation.
- Wait for every assigned review before declaring its scope complete. Report
  unfinished or unreviewed areas explicitly.

### 2. Establish Behavior and Requirements

State the old and new observable behavior. Distinguish established contracts
from inferred motivation. Read the PR description, linked requirements, and
existing review comments to reconcile the implementation with the requested
outcome and avoid duplicate findings. Treat performance and readiness claims as
claims requiring evidence.

State the affected invariants before treating existing tests as coverage.
Trace success, rejection, ordinary error, cancellation, and teardown paths where
relevant, including unchanged helpers, sibling call sites, and adapters. Follow
ownership, partial progress, cleanup, and telemetry attribution across those
paths. Record which cases were inspected and why omitted cases do not apply;
do not infer sibling-path correctness from one passing path.

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

When reviewing a fix or follow-up, inspect sibling paths for the same defect
family and check that the correction does not move the failure elsewhere.
Preserve the requested branch scope rather than narrowing it to the latest
regression test or changed line.

Do not treat an unproven scenario, unmeasured optimization, or personal style
preference as a verified defect. Keep unresolved evidence gaps separate from
findings. Apply explicit style rules to newly authored prose, but do not elevate
cosmetic suggestions into correctness blockers.

### 4. Assess Validation

Use the checks required by the applicable instructions and the change's blast
radius. Distinguish checks inspected from checks executed; never claim an
unexecuted check passed.

Map validation evidence to the relevant behavioral cases. A green feature
matrix establishes the behavior exercised by its tests, not coverage of every
failure transition. Distinguish tested cases, source-backed reasoning, missing
coverage, and checks left to CI. More tests or more agents do not substitute for
tracing the affected invariants.

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

Conclude with the reviewed base/head, covered and deferred scopes, validation
performed, evidence gaps, and residual risks. Qualify any "no verified findings"
conclusion by the inspected scope; do not claim a partial review covers the
whole branch or that the change is risk-free or production ready.

Do not post comments, approve or reject a PR, create commits, or push changes
unless the user explicitly requests the corresponding action.
